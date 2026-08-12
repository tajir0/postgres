#ifndef RELATION_ROW_CACHE_H
#define RELATION_ROW_CACHE_H

#include "postgres.h"

#include "access/htup.h"
#include "executor/tuptable.h"
#include "port/atomics.h"
#include "utils/dsa.h"
#include "utils/rel.h"
#include "utils/snapshot.h"

/*
 * 行缓存哈希桶数:编译期定值 2^24 = 16,777,216,不再暴露为 GUC。
 *
 * 定死的理由:桶数唯一的效果是决定链长,而链长只在"条目数 / 桶数"过大时
 * 才影响性能。2^24 个桶足以让上限规模(256GB 池 ≈ 2.6 亿条目)的平均链长
 * 保持在个位数,再往下调只省内存、不改变任何语义,却多一个极易配错的旋钮
 * ——桶头数组的开销与 row_cache_size 完全无关,分开配置时几乎必然被漏算。
 *
 * 代价是桶头数组固定占 2^24 × 8B = 128MB 共享内存(仅在 row_cache_size > 0
 * 即特性启用时才分配)。规划实例内存时必须把它与 row_cache_size 分开计入。
 *
 * 必须是 2 的幂:bucket = hash & ROW_CACHE_BUCKET_MASK 靠低位掩码定位。
 */
#define ROW_CACHE_HASH_BUCKETS		(1 << 24)	/* 16,777,216 */
#define ROW_CACHE_BUCKET_MASK		((uint32) (ROW_CACHE_HASH_BUCKETS - 1))
StaticAssertDecl((ROW_CACHE_HASH_BUCKETS & (ROW_CACHE_HASH_BUCKETS - 1)) == 0,
				 "ROW_CACHE_HASH_BUCKETS must be a power of two");

/*
 * GUC:行缓存数据池总大小(MB,PGC_POSTMASTER)。
 *
 * S1 内存模型:启动时在传统共享内存里一次性划出固定大小的段池
 * (row_cache_size / 1MB 个段),运行期零动态分配——写入是段内 bump
 * 指针推进,回收以整段为单位。"池满"不是失败,走整段淘汰(FIFO,
 * LRU 近似)腾地方;彻底腾不出(池全被当前加载中的表占用)时 load
 * 报错回滚,绝不阻塞等内存。
 *
 * 0 = 特性整体关闭(默认),语义对齐 max_prepared_transactions:
 * 共享内存一个字节都不划(**含桶头数组**——它按 row_cache_hash_buckets
 * 计算,与本参数无关,默认就是 64MB),washer 不注册,RowCacheCtl 保持
 * NULL,于是模块内全部入口经 `RowCacheCtl == NULL` 自动短路,行为与
 * 未打本补丁的原生 PG 一致。
 *
 * 取值是"0 或 >= ROW_CACHE_MIN_ACTIVE_SIZE_MB"的二段式:GUC 下界放到 0
 * 只为容纳"关闭",启用时另有实用下界。连续区间表达不了,故由
 * check_row_cache_size 把关。
 *
 * 该下界是**经验值而非正确性边界**:池再小也不会卡死——ACTIVE 段装满后
 * used 停止增长,5 轮内即被 washer 封存成 FULL 并正常参与淘汰。真正的问题
 * 是太小的池存不住东西:washer 的空闲水位 target = Max(1, n_segments/16),
 * n_segments = 1 时 target 等于总段数,回填刚取走那唯一的段,free 就低于
 * 水位,washer 随即把它淘汰回来补位——刚填进去的行当场丢失(自噬)。
 * 实测 1MB 池 6 万次点查:段一直在轮换(seq_num 持续增长)但命中率 0%,
 * 段内 n_entries 恒为 1。16MB(16 段,target=1,15 段可实际装数据)是能
 * 观察到稳定命中的最小规模。
 */
#define ROW_CACHE_DEFAULT_SIZE_MB	0		/* 0 = 关闭 */
#define ROW_CACHE_MIN_SIZE_MB		0
#define ROW_CACHE_MIN_ACTIVE_SIZE_MB 16		/* 启用时的实用下界(经验值) */
#define ROW_CACHE_MAX_SIZE_MB		(256 * 1024)	/* 256GB */
extern PGDLLIMPORT int row_cache_size_mb;

/* 特性总开关:池大小为 0 即整体关闭。 */
#define RowCacheIsEnabled()			(row_cache_size_mb > 0)

/* 共享内存大小与初始化(由 ipci.c 调用)。 */
extern Size RowCacheShmemSize(void);
extern void RowCacheShmemInit(void);

/* S3:后台洗段 bgworker(注册由 postmaster.c 调用)。 */
extern void RowCacheWasherRegister(void);
extern void RowCacheWasherMain(Datum main_arg);

extern void RowCacheOnHeapUpdate(Relation rel,
								 HeapTuple oldtup,
								 HeapTuple newtup);
extern void RowCacheOnHeapDelete(Relation rel,
								 HeapTuple oldtup);

/* 加载 / 卸载一张关系的缓存。 */
/*
 * scan_rows = true  : load 语义(注册 + 全表灌入),调用方须持 ShareLock。
 * scan_rows = false : enable 语义(只注册, 行靠按需回填), AccessShareLock 足够。
 */
extern void RelationRowCacheLoadRelation(Relation rel, bool scan_rows);
extern void RelationRowCacheDropRelation(Oid relid);

/* DROP DATABASE 钩子:清掉目标库的全部缓存槽与段(命令及 WAL redo 调用)。 */
extern void RelationRowCacheDropDatabase(Oid dboid);

/*
 *
 * RelationRowCachePkeyFetch       — 单列 pkey 关系。
 * RelationRowCachePkeyFetchComposite — 复合 pkey 关系。
 */
extern bool RelationRowCachePkeyFetch(Oid relid,
									  Datum pkey_val,
									  Snapshot snapshot,
									  TupleTableSlot *slot,
									  bool *is_visible,
									  bool *has_hot_chain);
extern bool RelationRowCachePkeyFetchComposite(Oid relid,
											   const Datum *vals,
											   int nvals,
											   Snapshot snapshot,
											   TupleTableSlot *slot,
											   bool *is_visible,
											   bool *has_hot_chain);


extern AttrNumber RelationRowCachePkeyAttno(Oid relid);

#define ROWCACHE_NOT_CACHED		((struct RelMeta *) 0x1)

extern void RelationRowCacheBindRelation(Relation rel);

extern bool RelationRowCachePkeyFetchBound(struct RelMeta *rm,
										   Oid expected_relid,
										   const RowCachePkeyDesc *descs,
										   const Datum *vals,
										   int nvals,
										   Snapshot snapshot,
										   TupleTableSlot *slot,
										   bool *is_visible,
										   bool *has_hot_chain);

/*
 * S3:点查 miss 按需回填。GUC row_cache_backfill 总开关;
 * InvalGen 是回填竞态屏障的代数读取(探测 miss 时、读堆之前记下)。
 */
extern PGDLLIMPORT bool row_cache_backfill;
extern uint64 RelationRowCacheInvalGen(struct RelMeta *rm);
extern bool RelationRowCacheBackfillBound(struct RelMeta *rm,
										  Oid expected_relid,
										  const RowCachePkeyDesc *descs,
										  int nvals,
										  TupleTableSlot *slot,
										  uint64 gen_seen);

extern int RelationRowCachePkeyDescriptor(Oid relid,
										   AttrNumber *out_attnos,
										   int max_attnos);


#endif							/* RELATION_ROW_CACHE_H */
