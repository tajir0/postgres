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
 * GUC:行缓存哈希桶数(PGC_POSTMASTER,改需重启,必须是 2 的幂)。
 * S1 起桶头数组在启动时随共享内存一并分配;实际生效值固化进 RowCacheControl。
 */
#define ROW_CACHE_DEFAULT_HASH_BUCKETS	(1 << 23)	/* 8,388,608 */
#define ROW_CACHE_MIN_HASH_BUCKETS		(1 << 10)	/* 1,024 */
#define ROW_CACHE_MAX_HASH_BUCKETS		(1 << 24)	/* 134,217,728 */
extern PGDLLIMPORT int row_cache_hash_buckets;

/*
 * GUC:行缓存数据池总大小(MB,PGC_POSTMASTER)。
 *
 * S1 内存模型:启动时在传统共享内存里一次性划出固定大小的段池
 * (row_cache_size / 1MB 个段),运行期零动态分配——写入是段内 bump
 * 指针推进,回收以整段为单位。"池满"不是失败,走整段淘汰(FIFO,
 * LRU 近似)腾地方;彻底腾不出(池全被当前加载中的表占用)时 load
 * 报错回滚,绝不阻塞等内存。
 */
#define ROW_CACHE_DEFAULT_SIZE_MB	64
#define ROW_CACHE_MIN_SIZE_MB		16
#define ROW_CACHE_MAX_SIZE_MB		(256 * 1024)	/* 256GB */
extern PGDLLIMPORT int row_cache_size_mb;

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
