/*-------------------------------------------------------------------------
 *
 * relation_row_cache.c
 *	  行缓存:分区锁保护的全局哈希 (relid, pkey) -> 扁平化 tuple。
 *
 * 架构(S1:段式固定池,借鉴 OceanBase KVCache 的内存组织):
 *
 *   内存模型:启动时在传统共享内存里一次性划出
 *     [RowCacheControl][桶头数组][段描述符数组][数据池(N × 1MB 段)]
 *   运行期零动态分配:行(GlobalEntry + 内联 FlatCachedTuple)在段内
 *   bump-pointer 追加分配;段内永不删除单条,回收只以整段为单位。
 *   传统 shmem 在所有 backend 映射到相同地址,桶链直接用真实指针。
 *
 *   GlobalCache:按 (relid, pkey) 键控的链地址哈希表。桶头数组
 *                GlobalEntry*[n_hash_buckets],链元素是 GlobalEntry。
 *
 *   段(RowCacheSegment):1MB,归属单一 relation。RelMeta 持有本表段链
 *   (first_seg,受 build_lock 保护);空闲段挂全局 free 链(受 seg_lock
 *   保护)。池满时按 alloc_seq FIFO 淘汰别的表的最老 FULL 段(LRU 近似,
 *   S3 换衰减打分);淘汰对 victim 表只造成部分行 miss(回退原生路径,
 *   正确性无损)。彻底腾不出时 load 报错回滚——任何路径不等内存。
 *
 *   RelMeta[64]:固定 shmem 数组,保存 per-relation 元数据(DISABLED/
 *                LOADING/ENABLED 状态、pkey 描述符、build_lock、段链)。
 *
 *   并发模型(锁序:build_lock → seg_lock → 分区锁):
 *     - 128 个 LWLock 分区,每组桶一个。
 *     - 读路径:在桶的分区锁上取 LW_SHARED,走链,拷贝载荷到 slot,释放。
 *     - DML 失效:分区锁 EXCLUSIVE 下摘链;载荷留在段内成死数据,等整段
 *       回收(不再逐条释放)。
 *     - Load / Drop 通过 per-RelMeta 的 build_lock 彼此串行化;段的
 *       分配/归还/淘汰选段由全局 seg_lock 串行化。
 *     - 淘汰者持 seg_lock 后对 victim 表用 ConditionalAcquire(build_lock),
 *       拿不到就换下一个候选——与"build_lock → seg_lock"的正向锁序
 *       不构成死锁。正在 LOADING 的表 build_lock 被持有,其 ACTIVE 段
 *       因此永远不会被淘汰。
 *
 *   S2(本阶段):
 *     - 读路径 pin 化:分区锁内只做"查链 + pin 所在段",拷贝移到锁外,
 *       锁持有时长从"整行拷贝"缩到"链查找"。段物理复用前等 pin 排空
 *       (微秒级,函数作用域引用;abort 由事务回调兜底释放)。
 *     - 段 seq_num 换代计数:回收时 +1;读路径作绊线校验,为 S3 洗段
 *       与 S6 无锁读预置失效原语。
 *     - DDL schema 指纹:RelMeta 存 (relfilenumber, tupdesc_hash),
 *       bind 慢路径复核——vacuum/analyze/GRANT 等触发的 relcache 失效
 *       不再误伤缓存,真 DDL(改列/重写)才失效。指纹必须纯内存可算
 *       (bind 在 RelationBuildDesc 的禁 catalog 区域内运行);
 *       "删主键约束"由执行器侧 indisunique 门槛封堵(execRowcache.c)。
 *
 *   S4:
 *     - 主键编码支持确定性 collation 的 text/varchar/bpchar、uuid 与
 *       bytea。varlena 统一 detoast 后规范化,短键内联、长键尾随条目。
 *
 *   Pkey 限制:1..ROW_CACHE_PKEY_MAX_ATTS 列;除原有传值类型外,传引用
 *   类型仅接受 S4 P1 白名单。其它类型在 Load 时静默跳过。
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/detoast.h"
#include "access/genam.h"
#include "access/htup_details.h"
#include "access/relation.h"
#include "access/tableam.h"
#include "access/tupmacs.h"
#include "access/xact.h"
#include "common/hashfn.h"
#include "catalog/pg_index.h"
#include "catalog/pg_type_d.h"
#include "executor/tuptable.h"
#include "lib/relation_row_cache.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/bufmgr.h"
#include "libpq/pqsignal.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/itemptr.h"
#include "storage/latch.h"
#include "storage/lockdefs.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/shmem.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/guc_hooks.h"
#include "utils/builtins.h"
#include "utils/wait_event.h"
#include "utils/dsa.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/datum.h"


#define ROW_CACHE_MAX_RELATIONS	64
#define ROW_CACHE_NUM_PARTITIONS	128
/*
 * 桶数:2 << 22 = 8,388,608(~8M)。必须是 2 的幂,这样
 * `bucket_mask = N - 1` 才是一个干净的低位掩码。
 *
 * 定值理由:
 *
 *   8M 桶可支持高达 3000 万 entry 的工作集、平均链长 < 4。
 *   更小的负载会浪费一些桶头内存,但没有时间代价。
 *
 *   桶头数组占用:
 *     8M * sizeof(GlobalEntry *) = 8M * 8 B = 64 MB 传统共享内存。
 *   S1 起它在启动时随段池一并划出(固定池全预分配)。
 *
 *   桶数现为 PGC_POSTMASTER 的 GUC row_cache_hash_buckets(改需重启,默认仍
 *   8M,必须是 2 的幂);运维可按工作集大小调整。
 *
 */
/*
 * 桶数与掩码现在是运行期值:由 GUC row_cache_hash_buckets 决定桶数,生效的
 * 桶数与掩码(桶数 - 1)在 RowCacheShmemInit 里固化进
 * RowCacheControl.{n_hash_buckets, bucket_mask}。GUC 默认/范围见
 * lib/relation_row_cache.h;check_row_cache_hash_buckets 保证它是 2 的幂,
 * 所以 `hash & bucket_mask` 仍是干净的低位掩码。
 */
int			row_cache_hash_buckets = ROW_CACHE_DEFAULT_HASH_BUCKETS;

/* 数据池大小(MB)。段数 = row_cache_size_mb(段固定 1MB)。 */
int			row_cache_size_mb = ROW_CACHE_DEFAULT_SIZE_MB;

/* GUC:点查 miss 后按需回填开关(S3)。 */
bool		row_cache_backfill = true;

/* RelMeta.state 取值。 */
#define RELMETA_DISABLED	0
#define RELMETA_LOADING		1
#define RELMETA_ENABLED		2

/* ----------------------------------------------------------------
 * 数据结构
 * ---------------------------------------------------------------- */

/*
 * RelMeta:per-relation 元数据,住在 shmem 里(固定数组)。
 *
 * 查找是对至多 ROW_CACHE_MAX_RELATIONS 个槽的线性扫描;一个 per-backend
 * 的 sticky 缓存对同一 relid 的重复查找做短路。
 */

/*
 * 段大小:固定 1MB。足够容纳千列宽行(超过段容量的单行在 load 时跳过,
 * 读路径 miss 回退原生,正确性无损),又保持淘汰粒度精细。
 */
#define ROW_CACHE_SEGMENT_SIZE	((uint32) (1024 * 1024))

/*
 * Pkey 存储参数。
 *
 * ROW_CACHE_PKEY_MAX_ATTS  — Load 资格检查时接受的最大 pkey 列数。
 *
 * ROW_CACHE_PKEY_INLINE_BYTES — GlobalEntry 内联的短键容量。32 字节覆盖:
 *                              - 1 列 int8        (8B)
 *                              - 2 列 int8       (16B)
 *                              - 3 列 int8       (24B)
 *                              - 4 列 int8       (32B)
 *                              - 4 列 int4       (16B)
 *                              - 8 列 int4       (32B)
 *                            更长的规范化键完整存放在条目头之后。
 *
 * ROW_CACHE_PKEY_MAX_BYTES — 单键编码上限(16KB)。桶链判等靠 pkey_hash
 * 前滤 + 逐字节 memcmp,超长键让哈希碰撞时的比较成本失控,且几百 KB 的
 * "主键"没有点查语义上的合理性。超限的行在所有路径同值拒绝:load 跳过
 * 该行、probe/回填按 miss 回退、DML 失效因该行从未入缓存而无需动作——
 * cap 在四条路径一致,不存在"缓存了却失效不掉"的组合。
 */
#define ROW_CACHE_PKEY_INLINE_BYTES	32
#define ROW_CACHE_PKEY_MAX_BYTES	((uint32) (16 * 1024))

StaticAssertDecl(ROW_CACHE_PKEY_MAX_ATTS <= INDEX_MAX_KEYS,
				 "ROW_CACHE_PKEY_MAX_ATTS must not exceed INDEX_MAX_KEYS");

typedef struct RelMeta
{
	/*
	 * 槽身份 = (dboid, relid)。主共享内存是实例级的,而 relid /
	 * relfilenode 都是数据库内标识——模板克隆库(CREATE DATABASE ...
	 * TEMPLATE)中两库的表 OID 与 filenode 完全相同,schema 指纹也
	 * 无法区分,必须以数据库 OID 参与完整键,否则跨库串数据。
	 */
	Oid				dboid;			/* 所属数据库;InvalidOid = 空闲槽 */
	Oid				relid;			/* InvalidOid = 空闲槽 */
	pg_atomic_uint32 state;			/* RELMETA_{DISABLED,LOADING,ENABLED} */
	LWLock			build_lock;		/* 串行化 Load / Drop;不在读路径上 */

	/*
	 * Pkey 描述符。
	 *
	 * 支持单列与复合键,最多 ROW_CACHE_PKEY_MAX_ATTS 列。描述符同时保存
	 * 类型、传值方式与主索引 collation,供四条键路径共享规范化规则。
	 */
	int				n_pkey_attrs;	/* 不合格 / 未加载时为 0 */
	RowCachePkeyDesc pkey_descs[ROW_CACHE_PKEY_MAX_ATTS];

	/*
	 * 本关系持有的段链(seg id,经 RowCacheSegment.next_seg 串联)。
	 * 受 build_lock 保护:loader 自己持有;淘汰者要动别人的链必须
	 * ConditionalAcquire 对方的 build_lock。
	 */
	int32			first_seg;		/* 段链头;-1 = 无 */
	int32			cur_seg;		/* 当前写入段(LOADING 或回填);-1 = 无 */

	/*
	 * schema 指纹(S2):load 时拍下,bind 慢路径(relcache 重建后的
	 * 首次绑定)复核。relfilenumber 抓 TRUNCATE / VACUUM FULL / CLUSTER
	 * 等重写(TID 全变);tupdesc_hash 抓加删改列。两者都纯内存可算——
	 * bind 运行在 RelationBuildDesc 的禁 catalog 区域,不能开索引。
	 */
	RelFileNumber	fp_relfilenumber;
	uint64			fp_tupdesc_hash;

	/*
	 * 失效计数(S3):DML 钩子对本表每次行级失效前无条件 +1。
	 *
	 * 按需回填的竞态屏障:探测 miss 时执行器记下 c1(早于堆读),回填
	 * 在分区锁内插入前比对 c2——期间本表有任何 DML 则放弃回填。两种
	 * 交错都安全:钩子先执行(counter++ 先于其摘链的分区锁临界区,
	 * 回填者进锁后必见新值 → 放弃);回填先插入(钩子随后的摘链必然
	 * 命中刚插入的条目 → 摘除)。堵住"读到 xmax 干净的旧版本 → 并发
	 * UPDATE 摘链扑空 → 回填复活死行"的窗口。
	 */
	pg_atomic_uint64 inval_counter;
} RelMeta;

typedef struct GlobalEntry
{
	Oid				dboid;			/* 所属数据库(条目键的一部分,防跨库串数据) */
	Oid				relid;
	uint32			pkey_hash;
	uint32			pkey_len;		/* 规范化键长度 */
	uint8			pkey_inline[ROW_CACHE_PKEY_INLINE_BYTES];
	ItemPointerData	tid;			/* Load 时的 heap TID(填 slot 用) */
	uint32			span;			/* 条目头 + 可选长键 + 内联 flat 的总字节数 */
	struct GlobalEntry *next;		/* 桶链上的下一个 GlobalEntry(真实指针) */
	/* 长键(若有)与 FlatCachedTuple 尾随其后,见 EntryPkeyData/ENTRY_FLAT。 */
} GlobalEntry;

/*
 * 一次键序列化的 backend 本地工作区。短键零分配;超过 32 字节才
 * palloc/repalloc,调用方用 SerializedPkeyRelease 配平。
 */
typedef struct SerializedPkey
{
	uint8		   *data;
	uint32		len;
	uint32		capacity;
	uint8		inline_data[ROW_CACHE_PKEY_INLINE_BYTES];
} SerializedPkey;

/* RowCacheSegment.state 取值。 */
#define RC_SEG_FREE		0		/* 在全局空闲链上 */
#define RC_SEG_ACTIVE	1		/* 某关系的当前写入段(LOADING 期间或回填段) */
#define RC_SEG_FULL		2		/* 已封存,可被整段淘汰 */

/*
 * S3 打分/洗段参数(对应 OB CACHE_SCORE_DECAY_FACTOR = 0.9 与
 * _cache_wash_interval = 200ms)。
 *
 * 空闲段水位 = max(1, n_segments/16):washer 周期性把 score 最低的
 * FULL 段洗回空闲链,使按需回填在池满后仍能持续进行(热点轮换);
 * 回填路径自身只从空闲链取段、绝不淘汰(两不等纪律)。
 */
#define ROW_CACHE_SCORE_DECAY		0.9
#define ROW_CACHE_WASH_INTERVAL_MS	200
#define ROW_CACHE_SEAL_BASE_SCORE	1.0		/* 封存时的起步分,防新段被立即误杀 */
#define ROW_CACHE_SEAL_IDLE_ROUNDS	5		/* ACTIVE 段空闲 N 轮(约 1s)后封存 */

/*
 * 段描述符(池外定长数组)。字段无原子:分配/封存由持 build_lock 的
 * loader 单写;归还/淘汰在 seg_lock(+victim build_lock)下串行。
 */
typedef struct RowCacheSegment
{
	Oid			dboid;			/* 归属数据库(淘汰者按 (dboid,relid) 反查属主) */
	Oid			relid;			/* 归属关系;InvalidOid = 空闲 */
	uint32		state;			/* RC_SEG_* */

	/*
	 * 段内 bump 指针与条目数。S3 起为原子:load 单写者(build_lock 排他)
	 * 之外,按需回填允许多 backend 并发 bump 同一 ENABLED 表的回填段
	 * (各自 CAS 出不重叠区间,各写各的)。
	 */
	pg_atomic_uint32 used;
	pg_atomic_uint32 n_entries;

	uint64		alloc_seq;		/* 全局分配序号;打分相同时的淘汰决胜 */
	int32		next_seg;		/* 本表段链 / 空闲链的下一段;-1 = 无 */

	/*
	 * S2:换代计数 + 在途读者 pin。
	 *
	 * seq_num 在段被物理回收(SegPrepareReuse)时 +1——持旧引用者靠
	 * 失配自查发现换代(读路径绊线;S3 洗段/S6 无锁读的失效原语)。
	 * pin_cnt > 0 表示有读者正在锁外拷贝本段数据,禁止物理复用;
	 * pin 在分区锁内获取(此时 entry 仍在链上 ⇒ 段必然活着),释放
	 * 无需任何锁。
	 */
	uint32		seq_num;
	pg_atomic_uint32 pin_cnt;

	/*
	 * S3:衰减 LFU 打分(对应 OB score = score×0.9 + recent_get_cnt)。
	 * recent_get_cnt 由命中读者原子累加;score 只在 seg_lock 下由打分
	 * 器(washer / 同步淘汰者)读写。淘汰选 score 最低的 FULL 段。
	 */
	pg_atomic_uint32 recent_get_cnt;
	double		score;

	/*
	 * washer 私有的空闲检测(seg_lock 下读写):ACTIVE 段(主要是回填
	 * 段)连续 ROW_CACHE_SEAL_IDLE_ROUNDS 轮 used 无增长即被封存为
	 * FULL 参与淘汰——否则每表一个常驻 ACTIVE 回填段,少量回填就能把
	 * 整个段池占成不可淘汰。
	 */
	uint32		wash_seen_used;
	uint16		wash_idle_rounds;
} RowCacheSegment;

/*
 * 全局代数计数器
 *
 * 每次 Load / Drop 对它 +1(冷路径);每次读 / DML 在 RelationRowCacheBindRelation
 * 里对它做一次原子读 + 比较
 */
typedef union RowCacheGenPadded
{
	pg_atomic_uint32 value;
	char			pad[PG_CACHE_LINE_SIZE];
} RowCacheGenPadded;

/*
 * RowCacheControl:顶层 shmem 段。桶头数组、段描述符数组、数据池紧随
 * 本结构之后,在同一次 ShmemInitStruct 里划出(见 RowCacheShmemInit)。
 */
typedef struct RowCacheControl
{
	/*
	 * 桶数与掩码:由 GUC row_cache_hash_buckets 决定,在 RowCacheShmemInit
	 * 设一次、之后只读。bucket_mask = n_hash_buckets - 1(必为 2 的幂)。
	 */
	int				n_hash_buckets;
	uint32			bucket_mask;

	/* 段池:段数在启动时由 row_cache_size 固化,之后只读。 */
	int32			n_segments;
	int32			free_seg_head;		/* 全局空闲段链头;-1 = 空 */
	uint64			seg_alloc_counter;	/* 单调递增,发放 alloc_seq */
	TimestampTz		last_score_refresh;	/* 上次衰减打分时刻(seg_lock 下) */

	/*
	 * 段需求计数(S3):空闲链耗尽时由取段者 +1(load 同步淘汰前 /
	 * 回填放弃前)。washer 每轮取出清零——近期无需求就不洗段,避免
	 * 水位机制无故蚕食一张合法占满池子的常驻表。
	 */
	pg_atomic_uint32 seg_demand;

	RowCacheGenPadded global_gen pg_attribute_aligned(PG_CACHE_LINE_SIZE);

	LWLock			seg_lock;				/* 保护空闲链/段归属/淘汰选段 */
	LWLock			relmeta_alloc_lock;		/* 保护 RelMeta 槽分配 */
	LWLock			partition_locks[ROW_CACHE_NUM_PARTITIONS];


	RelMeta			relmetas[ROW_CACHE_MAX_RELATIONS];
} RowCacheControl;

/*
 * FlatCachedTuple: 单次分配、扁平存储的缓存 tuple。
 *
 * 把每行的全部数据(values[]、isnull[]、HeapTupleHeader、以及按引用传递的
 * Datum 载荷)打包进一块连续内存——S1 起直接在段内 bump 出的空间上
 * 原地构建(GlobalEntry 之后内联),运行期零动态分配。
 *
 * 固定头之后的内存布局:
 *   Datum   values[natts]       -- 起于 MAXALIGN(sizeof(FlatCachedTuple))
 *   bool    isnull[natts]       -- 紧跟 values 之后
 *   char    htup_data[htup_len] -- HeapTupleHeaderData + tuple 载荷
 *   char    varlen_data[...]    -- 按引用传递的 Datum 二进制载荷
 */
typedef struct FlatCachedTuple
{
	uint32		total_size;		/* 本块总字节数 */
	int			natts;			/* 属性(列)个数 */
	uint32		htup_offset;	/* HeapTupleHeader 在本块内的字节偏移 */
	uint32		htup_len;		/* HeapTuple 数据长度(t_len) */
} FlatCachedTuple;

#define FLAT_TUPLE_VALUES(ft) \
	((Datum *)((char *)(ft) + MAXALIGN(sizeof(FlatCachedTuple))))
#define FLAT_TUPLE_ISNULL(ft) \
	((bool *)((char *)FLAT_TUPLE_VALUES(ft) + sizeof(Datum) * (ft)->natts))
#define FLAT_TUPLE_HTUP_DATA(ft) \
	((char *)(ft) + (ft)->htup_offset)

/* 长键完整存于固定头之后;短键直接使用头内的 pkey_inline。 */
static inline uint8 *
EntryPkeyData(GlobalEntry *e)
{
	if (e->pkey_len <= ROW_CACHE_PKEY_INLINE_BYTES)
		return e->pkey_inline;
	return (uint8 *) e + MAXALIGN(sizeof(GlobalEntry));
}

static inline const uint8 *
EntryPkeyDataConst(const GlobalEntry *e)
{
	return EntryPkeyData((GlobalEntry *) e);
}

/* FlatCachedTuple 起点取决于长键长度,并始终保持 MAXALIGN。 */
static inline Size
EntryFlatOffset(uint32 pkey_len)
{
	Size		off = MAXALIGN(sizeof(GlobalEntry));

	if (pkey_len > ROW_CACHE_PKEY_INLINE_BYTES)
		off = MAXALIGN(off + pkey_len);
	return off;
}

#define ENTRY_FLAT(e) \
	((FlatCachedTuple *) ((char *) (e) + EntryFlatOffset((e)->pkey_len)))

static Size FlatTupleComputeSize(TupleTableSlot *slot, HeapTuple htup,
								 uint32 *htup_offset_out,
								 uint32 *varlen_offset_out);
static void FlatTupleFillInto(FlatCachedTuple *ft, Size total_size,
							  uint32 htup_offset, uint32 varlen_offset,
							  TupleTableSlot *slot, HeapTuple htup);
static bool RowCacheUnflattenToSlot(const FlatCachedTuple *flat,
									Oid relid,
									ItemPointer tid,
									TupleTableSlot *slot);

/*
 * 共享内存各区域的进程本地基址。传统 shmem 在所有 backend 映射到相同
 * 地址,四个指针在 RowCacheShmemInit 里(创建者与 attach 者一致地)按
 * 偏移算出;此后进程内只读。
 */
static RowCacheControl *RowCacheCtl = NULL;
static GlobalEntry **RowCacheBuckets = NULL;	/* 长度 n_hash_buckets */
static RowCacheSegment *RowCacheSegs = NULL;	/* 长度 n_segments */
static char *RowCachePool = NULL;				/* n_segments × 1MB */

static Oid			LastLookupRelid = InvalidOid;
static RelMeta	   *LastLookupRelMeta = NULL;

/*
 * 本 backend 在途的段 pin(至多一个:pin 是 DoPkeyFetchBytes 的函数
 * 作用域引用,无嵌套)。唯一的中途逃逸路径是锁外拷贝时 ereport(如
 * palloc OOM)——由事务/子事务 abort 回调兜底释放。
 */
static int32		PendingPinnedSeg = -1;

/* 前向声明。 */
static void RowCacheEnsureBackendInit(void);
static RelMeta *FindRelMeta(Oid dboid, Oid relid);

static bool	RowCacheRelcacheCallbackRegistered = false;

static void rowcache_relcache_callback(Datum arg, Oid relid);

/*
 * 每个 backend 注册 relcache 失效回调，DDL 驱动的失效。
 */
static void
RowCacheRegisterRelcacheCallback(void)
{
	if (RowCacheRelcacheCallbackRegistered)
		return;
	CacheRegisterRelcacheCallback(rowcache_relcache_callback, (Datum) 0);
	RowCacheRelcacheCallbackRegistered = true;
}

static void
rowcache_relcache_callback(Datum arg, Oid relid)
{
	if (RowCacheCtl == NULL)
		return;

	/*
	 * relid == InvalidOid 表示 sinval 队列溢出(全量失效):清掉整个
	 * 本地查找缓存。具体某表失效则只清对应项。真正的"要不要 DISABLED"
	 * 裁决留给 S2 的 schema 指纹复核;这里只保证本地指针不悬空。
	 */
	if (!OidIsValid(relid) || LastLookupRelid == relid)
	{
		LastLookupRelid = InvalidOid;
		LastLookupRelMeta = NULL;
	}
}

static RelMeta *AllocateOrFindRelMeta(Oid dboid, Oid relid);
static bool CheckEligiblePkey(Relation rel, RelMeta *rm);
static bool SerializePkeyFromSlot(TupleTableSlot *slot, int n_pkey_attrs,
									  const RowCachePkeyDesc *descs,
									  SerializedPkey *key);
static bool SerializePkeyFromTuple(HeapTuple tuple, TupleDesc desc,
									   int n_pkey_attrs,
									   const RowCachePkeyDesc *descs,
									   SerializedPkey *key);
static bool SerializePkeyFromDatumArray(const Datum *vals, int nvals,
										const RowCachePkeyDesc *descs,
										SerializedPkey *key);
static void SerializedPkeyRelease(SerializedPkey *key);
static inline uint32 ComputePkeyHashBytes(const uint8 *buf, uint32 len);
static void SegUnlinkEntries(int32 sid);
static void ReleaseAllSegmentsForRel(RelMeta *rm);
static void SegPrepareReuse(int32 sid);
static GlobalEntry *SegAllocEntry(RelMeta *rm, Size need);
static uint64 RowCacheTupDescHash(TupleDesc desc);

/* ----------------------------------------------------------------
 * 段 pin(S2)
 * ---------------------------------------------------------------- */

/* entry 所在段号(entry 必在池内,O(1) 算术)。 */
static inline int32
EntrySegId(const GlobalEntry *e)
{
	return (int32) (((const char *) e - RowCachePool) / ROW_CACHE_SEGMENT_SIZE);
}

static inline void
SegPin(int32 sid)
{
	Assert(PendingPinnedSeg == -1);
	pg_atomic_fetch_add_u32(&RowCacheSegs[sid].pin_cnt, 1);
	PendingPinnedSeg = sid;
}

static inline void
SegUnpin(int32 sid)
{
	PendingPinnedSeg = -1;
	pg_atomic_fetch_sub_u32(&RowCacheSegs[sid].pin_cnt, 1);
}

/*
 * 事务/子事务 abort 兜底:把在途 pin 放掉,保证段回收不因错误路径卡死。
 * FATAL 走 AbortOutOfAnyTransaction 同样到达这里;kill -9 由 postmaster
 * 重建共享内存兜底。
 */
static void
RowCacheXactCallback(XactEvent event, void *arg)
{
	if (event == XACT_EVENT_ABORT ||
		event == XACT_EVENT_PARALLEL_ABORT)
	{
		if (PendingPinnedSeg >= 0)
			SegUnpin(PendingPinnedSeg);
	}
#ifdef USE_ASSERT_CHECKING
	else if (event == XACT_EVENT_PRE_COMMIT)
		Assert(PendingPinnedSeg == -1);
#endif
}

static void
RowCacheSubXactCallback(SubXactEvent event, SubTransactionId mySubid,
						SubTransactionId parentSubid, void *arg)
{
	if (event == SUBXACT_EVENT_ABORT_SUB && PendingPinnedSeg >= 0)
		SegUnpin(PendingPinnedSeg);
}

/* ----------------------------------------------------------------
 * 共享内存大小与初始化
 * ---------------------------------------------------------------- */

bool
check_row_cache_hash_buckets(int *newval, void **extra, GucSource source)
{
	int			v = *newval;

	if (v <= 0 || (v & (v - 1)) != 0)
	{
		GUC_check_errdetail("\"row_cache_hash_buckets\" must be a power of two.");
		return false;
	}
	return true;
}

/* 段数:池大小(MB)/ 段大小(1MB)。 */
static inline int32
RowCacheNSegments(void)
{
	return (int32) (((Size) row_cache_size_mb * 1024 * 1024) /
					ROW_CACHE_SEGMENT_SIZE);
}

Size
RowCacheShmemSize(void)
{
	Size		sz = MAXALIGN(sizeof(RowCacheControl));
	Size		nsegs = (Size) RowCacheNSegments();

	/* 桶头数组 */
	sz = add_size(sz, MAXALIGN(sizeof(GlobalEntry *) *
							   (Size) row_cache_hash_buckets));
	/* 段描述符数组 */
	sz = add_size(sz, MAXALIGN(sizeof(RowCacheSegment) * nsegs));
	/* 数据池(前置缓存行对齐余量) */
	sz = add_size(sz, PG_CACHE_LINE_SIZE);
	sz = add_size(sz, mul_size(nsegs, ROW_CACHE_SEGMENT_SIZE));

	return sz;
}

void
RowCacheShmemInit(void)
{
	bool		found;
	char	   *base;
	Size		off;
	int32		nsegs = RowCacheNSegments();

	base = (char *) ShmemInitStruct("Row Cache Control V6",
									RowCacheShmemSize(),
									&found);

	/*
	 * 各区域基址对创建者与 attach 者(EXEC_BACKEND)都要设置;传统 shmem
	 * 映射地址一致,直接按偏移切分。
	 */
	RowCacheCtl = (RowCacheControl *) base;
	off = MAXALIGN(sizeof(RowCacheControl));
	RowCacheBuckets = (GlobalEntry **) (base + off);
	off += MAXALIGN(sizeof(GlobalEntry *) * (Size) row_cache_hash_buckets);
	RowCacheSegs = (RowCacheSegment *) (base + off);
	off += MAXALIGN(sizeof(RowCacheSegment) * (Size) nsegs);
	off = TYPEALIGN(PG_CACHE_LINE_SIZE, off);
	RowCachePool = base + off;

	if (found)
		return;

	RowCacheCtl->n_hash_buckets = row_cache_hash_buckets;
	RowCacheCtl->bucket_mask = (uint32) row_cache_hash_buckets - 1;
	RowCacheCtl->n_segments = nsegs;
	RowCacheCtl->seg_alloc_counter = 0;
	RowCacheCtl->last_score_refresh = 0;
	pg_atomic_init_u32(&RowCacheCtl->seg_demand, 0);
	pg_atomic_init_u32(&RowCacheCtl->global_gen.value, 0);

	LWLockInitialize(&RowCacheCtl->seg_lock, LWTRANCHE_ROW_CACHE_CTL);
	LWLockInitialize(&RowCacheCtl->relmeta_alloc_lock,
					 LWTRANCHE_ROW_CACHE_RELMETA);

	for (int i = 0; i < ROW_CACHE_NUM_PARTITIONS; i++)
		LWLockInitialize(&RowCacheCtl->partition_locks[i],
						 LWTRANCHE_ROW_CACHE_PART);

	for (int i = 0; i < ROW_CACHE_MAX_RELATIONS; i++)
	{
		RelMeta    *rm = &RowCacheCtl->relmetas[i];

		rm->dboid = InvalidOid;
		rm->relid = InvalidOid;
		pg_atomic_init_u32(&rm->state, RELMETA_DISABLED);
		LWLockInitialize(&rm->build_lock, LWTRANCHE_ROW_CACHE_RELMETA);
		rm->n_pkey_attrs = 0;
		rm->first_seg = -1;
		rm->cur_seg = -1;
		rm->fp_relfilenumber = InvalidRelFileNumber;
		rm->fp_tupdesc_hash = 0;
		pg_atomic_init_u64(&rm->inval_counter, 0);
	}

	/* 桶头全空。 */
	memset(RowCacheBuckets, 0,
		   sizeof(GlobalEntry *) * (Size) row_cache_hash_buckets);

	/* 所有段串成空闲链。 */
	for (int32 s = 0; s < nsegs; s++)
	{
		RowCacheSegment *seg = &RowCacheSegs[s];

		seg->dboid = InvalidOid;
		seg->relid = InvalidOid;
		seg->state = RC_SEG_FREE;
		pg_atomic_init_u32(&seg->used, 0);
		pg_atomic_init_u32(&seg->n_entries, 0);
		seg->alloc_seq = 0;
		seg->next_seg = (s + 1 < nsegs) ? (s + 1) : -1;
		seg->seq_num = 0;
		pg_atomic_init_u32(&seg->pin_cnt, 0);
		pg_atomic_init_u32(&seg->recent_get_cnt, 0);
		seg->score = 0.0;
		seg->wash_seen_used = 0;
		seg->wash_idle_rounds = 0;
	}
	RowCacheCtl->free_seg_head = (nsegs > 0) ? 0 : -1;
}

/* ----------------------------------------------------------------
 * backend 一次性初始化:注册 relcache 失效回调 + 事务回调(pin 兜底)。
 * S1 之后不再有 DSA,共享内存基址在 RowCacheShmemInit 时已设好。
 * ---------------------------------------------------------------- */

static bool RowCacheBackendInitDone = false;

static void
RowCacheEnsureBackendInit(void)
{
	if (RowCacheBackendInitDone)
		return;
	RowCacheRegisterRelcacheCallback();
	RegisterXactCallback(RowCacheXactCallback, NULL);
	RegisterSubXactCallback(RowCacheSubXactCallback, NULL);
	RowCacheBackendInitDone = true;
}

/* ----------------------------------------------------------------
 * RelMeta 查找与槽分配
 * ---------------------------------------------------------------- */

static RelMeta *
FindRelMeta(Oid dboid, Oid relid)
{
	if (!OidIsValid(dboid) || !OidIsValid(relid))
		return NULL;

	for (int i = 0; i < ROW_CACHE_MAX_RELATIONS; i++)
	{
		RelMeta    *rm = &RowCacheCtl->relmetas[i];

		if (rm->dboid == dboid && rm->relid == relid)
			return rm;
	}
	return NULL;
}

static RelMeta *
AllocateOrFindRelMeta(Oid dboid, Oid relid)
{
	RelMeta    *rm;

	rm = FindRelMeta(dboid, relid);
	if (rm != NULL)
		return rm;

	LWLockAcquire(&RowCacheCtl->relmeta_alloc_lock, LW_EXCLUSIVE);

	/* 持锁下重新检查。 */
	for (int i = 0; i < ROW_CACHE_MAX_RELATIONS; i++)
	{
		RelMeta    *cand = &RowCacheCtl->relmetas[i];

		if (cand->dboid == dboid && cand->relid == relid)
		{
			LWLockRelease(&RowCacheCtl->relmeta_alloc_lock);
			return cand;
		}
	}

	/* 找一个空闲槽。 */
	for (int i = 0; i < ROW_CACHE_MAX_RELATIONS; i++)
	{
		RelMeta    *cand = &RowCacheCtl->relmetas[i];

		if (cand->relid == InvalidOid)
		{
			cand->dboid = dboid;
			cand->relid = relid;
			pg_atomic_write_u32(&cand->state, RELMETA_DISABLED);
			cand->n_pkey_attrs = 0;
			cand->first_seg = -1;
			cand->cur_seg = -1;
			rm = cand;
			break;
		}
	}

	LWLockRelease(&RowCacheCtl->relmeta_alloc_lock);
	return rm;
}

/* ----------------------------------------------------------------
 * Pkey 资格检查 / 序列化
 * ---------------------------------------------------------------- */

/*
 * 检查 rel 的主键,若合格则填充 `rm` 的 pkey 描述符部分:
 *
 *   - n_pkey_attrs       — pkey 列数(1..ROW_CACHE_PKEY_MAX_ATTS)
 *   - pkey_descs[]       — heap attno、类型、typlen/byval 与 collation
 *
 * 被拒情形:
 *   - 关系没有主键(或主键是 deferrable)
 *   - 任一 pkey 列是系统列 / 表达式(attno <= 0)
 *   - 传值列 attlen 不是 1/2/4/8
 *   - 传引用列不在 S4 P1 白名单(text/varchar/bpchar/uuid/bytea)
 *   - 字符串列使用非确定性 collation 或非内置字节等值语义
 *   - 列数超过 ROW_CACHE_PKEY_MAX_ATTS
 */
static bool
CheckEligiblePkey(Relation rel, RelMeta *rm)
{
	Oid			pkindex_oid;
	Relation	pkindex;
	Form_pg_index ind;
	int			natts;
	RowCachePkeyDesc descs[ROW_CACHE_PKEY_MAX_ATTS];
	bool		eligible = false;

	rm->n_pkey_attrs = 0;

	pkindex_oid = RelationGetPrimaryKeyIndex(rel, false);
	if (!OidIsValid(pkindex_oid))
		return false;

	pkindex = index_open(pkindex_oid, AccessShareLock);
	ind = pkindex->rd_index;

	if (ind == NULL || ind->indnkeyatts <= 0 ||
		ind->indnkeyatts > ROW_CACHE_PKEY_MAX_ATTS)
		goto out;

	natts = ind->indnkeyatts;
	for (int i = 0; i < natts; i++)
	{
		AttrNumber	attno = ind->indkey.values[i];
		Form_pg_attribute attr;
		Oid			collation;
		RegProcedure expected_eq = InvalidOid;

		if (attno <= 0)
			goto out;			/* 系统列 / 表达式键 */

		attr = TupleDescAttr(RelationGetDescr(rel), attno - 1);
		collation = pkindex->rd_indcollation[i];

		if (attr->attbyval)
		{
			if (attr->attlen != 1 && attr->attlen != 2 &&
				attr->attlen != 4 && attr->attlen != 8)
				goto out;
		}
		else
		{
			switch (attr->atttypid)
			{
				case TEXTOID:
				case VARCHAROID:
					expected_eq = F_TEXTEQ;
					break;
				case BPCHAROID:
					expected_eq = F_BPCHAREQ;
					break;
				case BYTEAOID:
					expected_eq = F_BYTEAEQ;
					break;
				case UUIDOID:
					expected_eq = F_UUID_EQ;
					break;
				default:
					goto out;
			}

			if (attr->atttypid == UUIDOID)
			{
				if (attr->attlen != 16)
					goto out;
			}
			else if (attr->attlen != -1)
				goto out;

			if (attr->atttypid == TEXTOID ||
				attr->atttypid == VARCHAROID ||
				attr->atttypid == BPCHAROID)
			{
				if (!OidIsValid(collation) ||
					!get_collation_isdeterministic(collation))
					goto out;
			}

			/*
			 * 白名单类型仍可能使用自定义 opclass。只有等值操作最终
			 * 落到相应内置字节等值函数时,下面的规范化编码才成立。
			 */
			{
				Oid			eqop;

				eqop = get_opfamily_member(pkindex->rd_opfamily[i],
									   pkindex->rd_opcintype[i],
									   pkindex->rd_opcintype[i],
									   BTEqualStrategyNumber);
				if (!OidIsValid(eqop) || get_opcode(eqop) != expected_eq)
					goto out;
			}
		}

		descs[i].attno = attno;
		descs[i].typlen = attr->attlen;
		descs[i].byval = attr->attbyval;
		descs[i].typid = attr->atttypid;
		descs[i].collation = collation;
	}

	/* 把描述符提交进 RelMeta。 */
	rm->n_pkey_attrs = natts;
	memcpy(rm->pkey_descs, descs, sizeof(RowCachePkeyDesc) * natts);
	eligible = true;

out:
	index_close(pkindex, AccessShareLock);
	return eligible;
}

static inline void
SerializedPkeyInit(SerializedPkey *key)
{
	key->data = key->inline_data;
	key->len = 0;
	key->capacity = ROW_CACHE_PKEY_INLINE_BYTES;
}

static void
SerializedPkeyRelease(SerializedPkey *key)
{
	if (key->data != key->inline_data)
		pfree(key->data);
	SerializedPkeyInit(key);
}

static bool
SerializedPkeyEnsure(SerializedPkey *key, uint32 addlen)
{
	uint32		needed;
	uint32		newcap;
	uint8	   *newdata;

	if (addlen > ROW_CACHE_PKEY_MAX_BYTES - key->len)
		return false;
	needed = key->len + addlen;
	if (needed <= key->capacity)
		return true;

	newcap = key->capacity;
	while (newcap < needed)
	{
		if (newcap > ROW_CACHE_PKEY_MAX_BYTES / 2)
		{
			newcap = ROW_CACHE_PKEY_MAX_BYTES;
			break;
		}
		newcap *= 2;
	}

	if (key->data == key->inline_data)
	{
		newdata = palloc(newcap);
		memcpy(newdata, key->inline_data, key->len);
	}
	else
		newdata = repalloc(key->data, newcap);

	key->data = newdata;
	key->capacity = newcap;
	return true;
}

static bool
SerializedPkeyAppend(SerializedPkey *key, const void *data, uint32 len)
{
	if (!SerializedPkeyEnsure(key, len))
		return false;
	if (len > 0)
		memcpy(key->data + key->len, data, len);
	key->len += len;
	return true;
}

/*
 * 单列规范化规则:
 *   - by-value:保持原有 attlen 字节编码;
 *   - uuid:固定 16 字节;
 *   - text/varchar/bytea:去 TOAST/varlena 头,编码 uint32 长度 + payload;
 *   - bpchar:在上述基础上去掉等值语义忽略的尾部空格。
 */
static bool
SerializePkeyDatum(Datum value, const RowCachePkeyDesc *desc,
						SerializedPkey *key)
{
	if (desc->byval)
	{
		if (desc->typlen != 1 && desc->typlen != 2 &&
			desc->typlen != 4 && desc->typlen != 8)
			return false;
		if (!SerializedPkeyEnsure(key, desc->typlen))
			return false;
		store_att_byval(key->data + key->len, value, desc->typlen);
		key->len += desc->typlen;
		return true;
	}

	if (desc->typid == UUIDOID && desc->typlen == 16)
		return SerializedPkeyAppend(key, DatumGetPointer(value), 16);

	if (desc->typid == TEXTOID || desc->typid == VARCHAROID ||
		desc->typid == BPCHAROID || desc->typid == BYTEAOID)
	{
		struct varlena *original = (struct varlena *) DatumGetPointer(value);
		struct varlena *detoasted;
		Size		raw_size;
		uint32		payload_len;
		bool		ok;

		if (desc->typlen != -1)
			return false;

		raw_size = toast_raw_datum_size(value);
		if (raw_size > (Size) ROW_CACHE_PKEY_MAX_BYTES + VARHDRSZ)
			return false;

		detoasted = PG_DETOAST_DATUM_PACKED(value);
		payload_len = (uint32) VARSIZE_ANY_EXHDR(detoasted);
		if (desc->typid == BPCHAROID)
			payload_len = (uint32) bpchartruelen(VARDATA_ANY(detoasted),
												 (int) payload_len);

		ok = SerializedPkeyAppend(key, &payload_len, sizeof(payload_len)) &&
			SerializedPkeyAppend(key, VARDATA_ANY(detoasted), payload_len);
		if (detoasted != original)
			pfree(detoasted);
		return ok;
	}

	return false;
}

/*
 * 共用编码器。变长列带长度前缀,避免复合键 ('ab','c') 与 ('a','bc')
 * 拼接成相同字节串。nulls == NULL 表示调用方已保证全部非空。
 */
static bool
SerializePkeyDatums(const Datum *values, const bool *nulls,
						int n_pkey_attrs, const RowCachePkeyDesc *descs,
						SerializedPkey *key)
{
	SerializedPkeyInit(key);
	if (values == NULL || descs == NULL || n_pkey_attrs <= 0 ||
		n_pkey_attrs > ROW_CACHE_PKEY_MAX_ATTS)
		return false;

	for (int i = 0; i < n_pkey_attrs; i++)
	{
		if ((nulls != NULL && nulls[i]) ||
			!SerializePkeyDatum(values[i], &descs[i], key))
		{
			SerializedPkeyRelease(key);
			return false;
		}
	}
	return true;
}

static bool
SerializePkeyFromSlot(TupleTableSlot *slot, int n_pkey_attrs,
					  const RowCachePkeyDesc *descs,
					  SerializedPkey *key)
{
	Datum		values[ROW_CACHE_PKEY_MAX_ATTS];
	bool		nulls[ROW_CACHE_PKEY_MAX_ATTS];

	for (int i = 0; i < n_pkey_attrs; i++)
		values[i] = slot_getattr(slot, descs[i].attno, &nulls[i]);

	return SerializePkeyDatums(values, nulls, n_pkey_attrs, descs, key);
}

/* DML 使用 RelationData 的本地描述符,不触碰可能换代的共享 RelMeta。 */
static bool
SerializePkeyFromTuple(HeapTuple tuple, TupleDesc desc,
					   int n_pkey_attrs, const RowCachePkeyDesc *descs,
					   SerializedPkey *key)
{
	Datum		values[ROW_CACHE_PKEY_MAX_ATTS];
	bool		nulls[ROW_CACHE_PKEY_MAX_ATTS];

	for (int i = 0; i < n_pkey_attrs; i++)
		values[i] = heap_getattr(tuple, descs[i].attno, desc, &nulls[i]);

	return SerializePkeyDatums(values, nulls, n_pkey_attrs, descs, key);
}

static bool
SerializePkeyFromDatumArray(const Datum *vals, int nvals,
							const RowCachePkeyDesc *descs,
							SerializedPkey *key)
{
	return SerializePkeyDatums(vals, NULL, nvals, descs, key);
}

static inline uint32
ComputePkeyHashBytes(const uint8 *buf, uint32 len)
{
	return hash_bytes(buf, (int) len);
}

static inline void
StoreEntryPkey(GlobalEntry *entry, const SerializedPkey *key)
{
	Assert(key->len <= ROW_CACHE_PKEY_MAX_BYTES);
	entry->pkey_len = key->len;
	memcpy(EntryPkeyData(entry), key->data, key->len);
}

static inline LWLock *
PartitionLockForBucket(uint32 bucket)
{
	return &RowCacheCtl->partition_locks[bucket % ROW_CACHE_NUM_PARTITIONS];
}

/* 段 payload 基址。 */
static inline char *
SegBase(int32 sid)
{
	return RowCachePool + (Size) sid * ROW_CACHE_SEGMENT_SIZE;
}

/* ----------------------------------------------------------------
 * MVCC 可见性检查
 *
 * 判定:
 *   1. xmin 必须已提交(命中位);未提交 / 已 abort 时返回 false(交回堆)。
 *   2. xmin 必须对本快照可见:frozen 对所有快照可见;否则当 xmin 落在快照
 *      活动窗口(>= snapshot->xmin)时用 XidInMVCCSnapshot 做权威判定。这一
 *      步挡住 RR/SERIALIZABLE 回溯快照看到"快照之后才提交的行"。
 *   3. xmax:未删除(XMAX_INVALID)或仅被锁(LOCKED_ONLY)即可见;真正的
 *      删除/更新返回 false(是否对本快照仍可见交回堆路径)。
 * ---------------------------------------------------------------- */

static pg_attribute_always_inline bool
RowCacheTupleVisibleMVCC(HeapTuple tuple, Snapshot snapshot)
{
	HeapTupleHeader td = tuple->t_data;
	uint16		infomask = td->t_infomask;

	if (unlikely(!(infomask & HEAP_XMIN_COMMITTED)))
		return false;

	if (!HeapTupleHeaderXminFrozen(td))
	{
		TransactionId xmin = HeapTupleHeaderGetRawXmin(td);

		if (!TransactionIdPrecedes(xmin, snapshot->xmin) &&
			XidInMVCCSnapshot(xmin, snapshot))
			return false;		/* 插入事务对本快照不可见 */
	}

	/* xmax:未删除或仅被锁 -> 可见。 */
	if (likely(infomask & HEAP_XMAX_INVALID))
		return true;
	if (HEAP_XMAX_IS_LOCKED_ONLY(infomask))
		return true;

	return false;
}


/*
 * 把一个已完全初始化的 GlobalEntry 插到它所属桶的链头。
 * 调用方必须持有该桶的分区锁 EXCLUSIVE。
 */
static void
BucketInsertHead(uint32 bucket, GlobalEntry *entry)
{
	entry->next = RowCacheBuckets[bucket];
	pg_write_barrier();
	RowCacheBuckets[bucket] = entry;
}

/*
 * 走一条桶链,找匹配 (dboid, relid, pkey_hash, pkey_len, pkey bytes) 的
 * entry。
 *
 * 匹配条件:
 *   - dboid + relid 相等(模板克隆库的表 OID 可能相同,库号必须参与判等)
 *   - pkey_hash 相等(廉价的哈希碰撞过滤)
 *   - pkey_len 相等
 *   - memcmp(规范化键, ..., pkey_len) 做最终判等
 */
static GlobalEntry *
BucketLookup(uint32 bucket, Oid dboid, Oid relid, uint32 pkey_hash,
			 const uint8 *pkey_buf, uint32 pkey_len)
{
	GlobalEntry *e;

	for (e = RowCacheBuckets[bucket]; e != NULL; e = e->next)
	{
		if (e->dboid == dboid &&
				e->relid == relid &&
				e->pkey_hash == pkey_hash &&
				e->pkey_len == pkey_len &&
				memcmp(EntryPkeyDataConst(e), pkey_buf, pkey_len) == 0)
			return e;
	}
	return NULL;
}

/* ----------------------------------------------------------------
 * 段分配 / 回收 / 淘汰
 *
 * 锁序:build_lock → seg_lock → 分区锁。淘汰者反向需要 victim 的
 * build_lock 时只用 ConditionalAcquire,失败换候选,不构成死锁。
 * ---------------------------------------------------------------- */

/*
 * 把段 sid 里所有仍挂在哈希桶链上的 entry 摘掉。按 entry 地址匹配,
 * 幂等:已被 DML 失效摘掉的、或分配后从未插链的条目自然找不到,跳过。
 * 调用方保证没有并发写者会往该段追加(持有属主 build_lock,或段属主
 * 已不存在)。
 */
static void
SegUnlinkEntries(int32 sid)
{
	RowCacheSegment *seg = &RowCacheSegs[sid];
	char	   *base = SegBase(sid);
	uint32		used = pg_atomic_read_u32(&seg->used);
	uint32		off = 0;

	while (off < used)
	{
		GlobalEntry *e = (GlobalEntry *) (base + off);
		uint32		bucket;
		LWLock	   *part;
		GlobalEntry *cur;
		GlobalEntry *prev;

		/* span 在 bump 时立即写入;为 0 说明段元数据损坏,防御退出。 */
		if (e->span == 0)
		{
			elog(WARNING, "row cache: corrupted segment %d at offset %u",
				 sid, off);
			break;
		}

		bucket = e->pkey_hash & RowCacheCtl->bucket_mask;
		part = PartitionLockForBucket(bucket);

		LWLockAcquire(part, LW_EXCLUSIVE);
		prev = NULL;
		for (cur = RowCacheBuckets[bucket]; cur != NULL; cur = cur->next)
		{
			if (cur == e)
			{
				if (prev != NULL)
					prev->next = e->next;
				else
					RowCacheBuckets[bucket] = e->next;
				break;
			}
			prev = cur;
		}
		LWLockRelease(part);

		off += e->span;
	}
}

/*
 * 段物理回收前的最后一步(S2):等在途 pin 排空,然后换代(seq_num++)
 * 并清空段元数据。调用方随后自行处理 next_seg 归属(空闲链/悬空)。
 *
 * 等待有界的依据:pin 是 DoPkeyFetchBytes 的函数作用域引用,持有窗口
 * 是锁外拷贝一行的时长(微秒级),中途 ereport 由事务 abort 回调兜底
 * 释放;性质等同自旋锁,不违反"分配路径不等内存"的纪律(等的不是
 * 内存,是必然排空的在途读者)。持续 10s 不排空说明 pin 泄漏(bug),
 * 打 WARNING 便于诊断,继续等。
 *
 * 死锁论证:两个调用语境分别持有 (build_lock + seg_lock) 或
 * (seg_lock + victim build_lock),而 pin 的释放不需要任何锁
 * (SegUnpin 是纯原子减),不存在循环等待。
 */
static void
SegPrepareReuse(int32 sid)
{
	RowCacheSegment *seg = &RowCacheSegs[sid];
	long		waited_us = 0;

	while (pg_atomic_read_u32(&seg->pin_cnt) != 0)
	{
		pg_usleep(10);
		waited_us += 10;
		if (waited_us % (10L * 1000000L) == 0)
			elog(WARNING, "row cache: segment %d still pinned after %ld s (possible pin leak)",
				 sid, waited_us / 1000000L);
	}

	seg->seq_num++;
	seg->dboid = InvalidOid;
	seg->relid = InvalidOid;
	seg->state = RC_SEG_FREE;
	pg_atomic_write_u32(&seg->used, 0);
	pg_atomic_write_u32(&seg->n_entries, 0);
	pg_atomic_write_u32(&seg->recent_get_cnt, 0);
	seg->score = 0.0;
	seg->wash_seen_used = 0;
	seg->wash_idle_rounds = 0;
}

/* 调用方持 seg_lock EXCLUSIVE。保留 ACTIVE 段已经积累的热点分数。 */
static inline void
SegSeal(RowCacheSegment *seg)
{
	Assert(seg->state == RC_SEG_ACTIVE || seg->state == RC_SEG_FULL);
	seg->state = RC_SEG_FULL;
	seg->score = Max(seg->score, ROW_CACHE_SEAL_BASE_SCORE);
	seg->wash_idle_rounds = 0;
}

/*
 * 释放 rm 段链上的全部段:摘净桶链引用后整链归还空闲链。
 * 调用方持有 rm->build_lock。幂等(中途被中断后重跑安全)。
 */
static void
ReleaseAllSegmentsForRel(RelMeta *rm)
{
	int32		sid;

	for (sid = rm->first_seg; sid >= 0; sid = RowCacheSegs[sid].next_seg)
	{
		CHECK_FOR_INTERRUPTS();
		SegUnlinkEntries(sid);
	}

	LWLockAcquire(&RowCacheCtl->seg_lock, LW_EXCLUSIVE);
	sid = rm->first_seg;
	while (sid >= 0)
	{
		RowCacheSegment *seg = &RowCacheSegs[sid];
		int32		next = seg->next_seg;

		SegPrepareReuse(sid);
		seg->next_seg = RowCacheCtl->free_seg_head;
		RowCacheCtl->free_seg_head = sid;
		sid = next;
	}
	rm->first_seg = -1;
	rm->cur_seg = -1;
	LWLockRelease(&RowCacheCtl->seg_lock);
}

/*
 * 段周期维护(S3),调用方持 seg_lock EXCLUSIVE:
 *   1. 刷新衰减 LFU 分数 score = score×0.9 + recent_get_cnt;
 *   2. ACTIVE 段连续若干轮 used 无增长后封存为 FULL,使少量回填形成的
 *      尾段也能参与淘汰。
 *
 * washer 每周期调用;同步淘汰路径在维护时间戳陈旧时补跑。两项维护合并
 * 为一次全段扫描,避免在全局段锁下重复遍历大段池。
 */
static void
RowCacheRefreshSegmentsLocked(void)
{
	for (int32 s = 0; s < RowCacheCtl->n_segments; s++)
	{
		RowCacheSegment *seg = &RowCacheSegs[s];
		uint32		recent = pg_atomic_exchange_u32(&seg->recent_get_cnt, 0);
		uint32		used_now;

		if (seg->state == RC_SEG_FREE)
		{
			seg->score = 0.0;
			continue;
		}

		seg->score = seg->score * ROW_CACHE_SCORE_DECAY + (double) recent;

		if (seg->state != RC_SEG_ACTIVE || !OidIsValid(seg->relid))
			continue;

		used_now = pg_atomic_read_u32(&seg->used);
		if (used_now != seg->wash_seen_used)
		{
			seg->wash_seen_used = used_now;
			seg->wash_idle_rounds = 0;
		}
		else if (++seg->wash_idle_rounds >= ROW_CACHE_SEAL_IDLE_ROUNDS)
			SegSeal(seg);
	}
	RowCacheCtl->last_score_refresh = GetCurrentTimestamp();
}

/*
 * 在 FULL 段中挑 score 最低者(同分取 alloc_seq 最小,退化为 FIFO)。
 * exempt_relid 的段被豁免(load 不淘汰自己);skip_relid 排除二次扫描
 * 时 build_lock 拿不到的表。调用方持 seg_lock EXCLUSIVE。
 */
static int32
SegPickVictim(Oid exempt_dboid, Oid exempt_relid,
			  const Oid *skip_dboids, const Oid *skip_relids, int nskip)
{
	int32		victim = -1;
	double		victim_score = 0.0;
	uint64		victim_seq = 0;

	for (int32 s = 0; s < RowCacheCtl->n_segments; s++)
	{
		RowCacheSegment *seg = &RowCacheSegs[s];
		bool		skipped = false;

		if (seg->state != RC_SEG_FULL)
			continue;
		if (seg->dboid == exempt_dboid && seg->relid == exempt_relid)
			continue;
		for (int k = 0; k < nskip; k++)
		{
			if (seg->dboid == skip_dboids[k] && seg->relid == skip_relids[k])
			{
				skipped = true;
				break;
			}
		}
		if (skipped)
			continue;
		if (victim < 0 ||
			seg->score < victim_score ||
			(seg->score == victim_score && seg->alloc_seq < victim_seq))
		{
			victim = s;
			victim_score = seg->score;
			victim_seq = seg->alloc_seq;
		}
	}
	return victim;
}

/*
 * 淘汰一个 FULL 段:打分选最冷 → conditional 拿 victim 表的 build_lock
 * → 摘净桶链引用 → 从属主段链摘除 → 等 pin 排空并换代。
 *
 * 候选遍历纪律(对应 OB try_wash_mb 的"失败即跳过、穷尽为止"):属主
 * build_lock 拿不到(正在 load/drop)就把该 (dboid, relid) 记入 skip
 * 集合换下一个属主,每个属主至多一次条件锁;所有持段属主都忙时才返回
 * -1——绝不因为前两个候选不顺利就误报池耗尽。skip 集合上限即 RelMeta
 * 槽数,循环有界。
 *
 * (exempt_dboid, exempt_relid) 的段被豁免(load 不淘汰自己)。
 * 调用方持 seg_lock EXCLUSIVE。返回段号(已不在任何链上)。
 */
static int32
SegEvictOne(Oid exempt_dboid, Oid exempt_relid)
{
	Oid			skip_dboids[ROW_CACHE_MAX_RELATIONS];
	Oid			skip_relids[ROW_CACHE_MAX_RELATIONS];
	int			nskip = 0;

	/* 打分陈旧时补一轮刷新(washer 常态每周期维护)。 */
	if (TimestampDifferenceExceeds(RowCacheCtl->last_score_refresh,
								   GetCurrentTimestamp(),
								   ROW_CACHE_WASH_INTERVAL_MS))
		RowCacheRefreshSegmentsLocked();

	for (;;)
	{
		int32		victim = SegPickVictim(exempt_dboid, exempt_relid,
										   skip_dboids, skip_relids, nskip);
		RowCacheSegment *vseg;
		RelMeta    *vrm;
		int32	   *linkp;
		int32		cur;

		if (victim < 0)
			return -1;			/* 候选已穷尽:真·无可淘汰段 */

		vseg = &RowCacheSegs[victim];
		vrm = FindRelMeta(vseg->dboid, vseg->relid);
		if (vrm == NULL || vrm->dboid != vseg->dboid ||
			vrm->relid != vseg->relid)
		{
			/* 不应发生:有主的段必有 RelMeta。防御:直接回收。 */
			elog(WARNING, "row cache: segment %d owned by relation %u/%u without metadata",
				 victim, vseg->dboid, vseg->relid);
			SegUnlinkEntries(victim);
			SegPrepareReuse(victim);
			vseg->next_seg = -1;
			return victim;
		}

		if (!LWLockConditionalAcquire(&vrm->build_lock, LW_EXCLUSIVE))
		{
			/* 属主正被 load/drop:记入 skip 集合,换下一个属主。 */
			if (nskip >= ROW_CACHE_MAX_RELATIONS)
				return -1;		/* 防御:不应发生(属主数受槽数限制) */
			skip_dboids[nskip] = vseg->dboid;
			skip_relids[nskip] = vseg->relid;
			nskip++;
			continue;
		}

		/* 持有 victim 的 build_lock:摘桶链引用 + 从其段链摘除。 */
		SegUnlinkEntries(victim);

		linkp = &vrm->first_seg;
		cur = vrm->first_seg;
		while (cur >= 0)
		{
			if (cur == victim)
			{
				*linkp = RowCacheSegs[cur].next_seg;
				break;
			}
			linkp = &RowCacheSegs[cur].next_seg;
			cur = RowCacheSegs[cur].next_seg;
		}
		if (vrm->cur_seg == victim)
			vrm->cur_seg = -1;

		LWLockRelease(&vrm->build_lock);

		/*
		 * 此刻 victim 已摘净桶链引用且不在任何段链上,不可能再有新 pin;
		 * 等存量读者(锁外拷贝中)排空后换代复用。
		 */
		SegPrepareReuse(victim);
		RowCacheSegs[victim].next_seg = -1;
		return victim;
	}
}

/*
 * 弹出一个可用段:优先空闲链(washer 负责维持水位),空了同步淘汰
 * (打分最冷)。调用方持 seg_lock EXCLUSIVE。
 */
static int32
SegPopOrEvict(Oid loading_dboid, Oid loading_relid)
{
	if (RowCacheCtl->free_seg_head >= 0)
	{
		int32		sid = RowCacheCtl->free_seg_head;

		RowCacheCtl->free_seg_head = RowCacheSegs[sid].next_seg;
		RowCacheSegs[sid].next_seg = -1;
		return sid;
	}
	/* 空闲链耗尽 = 段需求信号,washer 据此维持水位。 */
	pg_atomic_fetch_add_u32(&RowCacheCtl->seg_demand, 1);
	return SegEvictOne(loading_dboid, loading_relid);
}

/*
 * 段内 CAS bump 出一块 need 字节的空间并写好占位头(S3:允许多写者——
 * 并发回填者各自拿到不重叠区间,各写各的)。段剩余不足返回 NULL。
 *
 * 立即写 span / 占位字段:此后即使写者中途异常,SegUnlinkEntries 也能
 * 按 span 安全遍历本段。
 */
static GlobalEntry *
SegTryBump(int32 sid, uint32 need)
{
	RowCacheSegment *seg = &RowCacheSegs[sid];
	uint32		old = pg_atomic_read_u32(&seg->used);
	GlobalEntry *e;

	for (;;)
	{
		if ((Size) old + need > ROW_CACHE_SEGMENT_SIZE)
			return NULL;
		if (pg_atomic_compare_exchange_u32(&seg->used, &old, old + need))
			break;
		/* CAS 失败时 old 已被更新为当前值,重试。 */
	}

	pg_atomic_fetch_add_u32(&seg->n_entries, 1);

	e = (GlobalEntry *) (SegBase(sid) + old);
	e->span = need;
	e->dboid = InvalidOid;
	e->relid = InvalidOid;		/* 插链前的未完成标记 */
	e->pkey_hash = 0;
	e->pkey_len = 0;
	e->next = NULL;
	return e;
}

/*
 * 回填分配(S3):从本表当前写入段 CAS bump;段满则在 seg_lock 下换段,
 * 但只从空闲链取——绝不同步淘汰、绝不等待(查询路径纪律)。空闲链空
 * 时记一次段需求(washer 下轮补水位)并放弃。
 *
 * 调用方持 rm->build_lock SHARED:挡住 drop/load/指纹失效/淘汰(它们
 * 取 EXCLUSIVE)对段链与段内存的并发变更;多个回填者之间靠 SegTryBump
 * 的原子 CAS 与 seg_lock 串行。陈旧的 cur_seg 读值无害:bump 进一个
 * 刚被封存(FULL)的段仍然合法——FULL 只表示"可淘汰",而淘汰需要
 * EXCLUSIVE build_lock,被我们的 SHARED 挡住。
 */
static GlobalEntry *
SegBackfillAlloc(RelMeta *rm, Size need)
{
	need = MAXALIGN(need);

	if (need > ROW_CACHE_SEGMENT_SIZE)
		return NULL;

	for (;;)
	{
		int32		sid = rm->cur_seg;
		int32		nid;

		if (sid >= 0)
		{
			GlobalEntry *e = SegTryBump(sid, (uint32) need);

			if (e != NULL)
				return e;
		}

		LWLockAcquire(&RowCacheCtl->seg_lock, LW_EXCLUSIVE);

		/* 并发回填者可能已换好新段:重试 bump。 */
		if (rm->cur_seg != sid)
		{
			LWLockRelease(&RowCacheCtl->seg_lock);
			continue;
		}

		if (RowCacheCtl->free_seg_head < 0)
		{
			pg_atomic_fetch_add_u32(&RowCacheCtl->seg_demand, 1);
			LWLockRelease(&RowCacheCtl->seg_lock);
			return NULL;
		}

		nid = RowCacheCtl->free_seg_head;
		RowCacheCtl->free_seg_head = RowCacheSegs[nid].next_seg;

		{
			RowCacheSegment *nseg = &RowCacheSegs[nid];

			nseg->dboid = rm->dboid;
			nseg->relid = rm->relid;
			nseg->state = RC_SEG_ACTIVE;
			pg_atomic_write_u32(&nseg->used, 0);
			pg_atomic_write_u32(&nseg->n_entries, 0);
			nseg->alloc_seq = ++RowCacheCtl->seg_alloc_counter;
			nseg->next_seg = rm->first_seg;
		}
		if (sid >= 0)
			SegSeal(&RowCacheSegs[sid]);
		rm->first_seg = nid;
		rm->cur_seg = nid;
		LWLockRelease(&RowCacheCtl->seg_lock);
	}
}

/*
 * 从本表当前写入段 bump 出一个 entry(含内联 flat 载荷的总空间 need)。
 * 段不够就换新段(空闲链/淘汰);单行超过段容量返回 NULL(调用方跳过
 * 该行);池彻底腾不出则 ereport(调用方 PG_CATCH 回滚整个 load)。
 * 调用方持有 rm->build_lock EXCLUSIVE。
 */
static GlobalEntry *
SegAllocEntry(RelMeta *rm, Size need)
{
	need = MAXALIGN(need);

	if (need > ROW_CACHE_SEGMENT_SIZE)
		return NULL;			/* 单行超段容量:跳过 */

	for (;;)
	{
		int32		sid = rm->cur_seg;

		if (sid >= 0)
		{
			GlobalEntry *e = SegTryBump(sid, (uint32) need);

			if (e != NULL)
				return e;
		}

		/* 换新段。 */
		{
			int32		nid;

			LWLockAcquire(&RowCacheCtl->seg_lock, LW_EXCLUSIVE);
			nid = SegPopOrEvict(rm->dboid, rm->relid);
			if (nid < 0)
			{
				LWLockRelease(&RowCacheCtl->seg_lock);
				ereport(ERROR,
						(errcode(ERRCODE_OUT_OF_MEMORY),
						 errmsg("row cache: segment pool exhausted"),
						 errdetail_internal("row_cache_size = %dMB (%d segments); "
											"relation already holds all reclaimable segments",
											row_cache_size_mb,
											RowCacheCtl->n_segments)));
			}

			{
				RowCacheSegment *nseg = &RowCacheSegs[nid];

				nseg->dboid = rm->dboid;	/* 淘汰者按 (dboid,relid) 反查属主 */
				nseg->relid = rm->relid;
				nseg->state = RC_SEG_ACTIVE;
				pg_atomic_write_u32(&nseg->used, 0);
				pg_atomic_write_u32(&nseg->n_entries, 0);
				nseg->alloc_seq = ++RowCacheCtl->seg_alloc_counter;
				nseg->next_seg = rm->first_seg;
			}
			if (sid >= 0)
			{
				/* 封存旧段:给起步分,防"刚写满还没被读过"的段被立即误杀。 */
				SegSeal(&RowCacheSegs[sid]);
			}
			rm->first_seg = nid;
			rm->cur_seg = nid;
			LWLockRelease(&RowCacheCtl->seg_lock);
		}
	}
}

/* ----------------------------------------------------------------
 * FlatCachedTuple:行的连续物化存储格式
 * ---------------------------------------------------------------- */

/*
 * 计算 slot 扁平化后 FlatCachedTuple 的总字节数。htup 是调用方已物化的
 * heap tuple 副本(测量与写入两步共用,避免重复物化)。
 */
static Size
FlatTupleComputeSize(TupleTableSlot *slot, HeapTuple htup,
					 uint32 *htup_offset_out, uint32 *varlen_offset_out)
{
	TupleDesc	desc = slot->tts_tupleDescriptor;
	int			natts = desc->natts;
	uint32		values_offset,
				isnull_offset,
				htup_offset,
				varlen_offset;
	Size		total_size;

	values_offset = MAXALIGN(sizeof(FlatCachedTuple));
	isnull_offset = values_offset + sizeof(Datum) * natts;
	htup_offset = MAXALIGN(isnull_offset + sizeof(bool) * natts);
	varlen_offset = htup_offset + MAXALIGN(htup->t_len);
	total_size = varlen_offset;

	for (int i = 0; i < natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(desc, i);

		if (!slot->tts_isnull[i] && !attr->attbyval)
			total_size += MAXALIGN(datumGetSize(slot->tts_values[i],
												false, attr->attlen));
	}

	*htup_offset_out = htup_offset;
	*varlen_offset_out = varlen_offset;
	return total_size;
}

/*
 * 把 slot 扁平化写进调用方给定的缓冲(段内 bump 出的空间,S1 起原地
 * 写入,不再经过临时分配 + memcpy)。参数均来自 FlatTupleComputeSize。
 * 全程纯 memcpy,无 ereport 点。
 */
static void
FlatTupleFillInto(FlatCachedTuple *ft, Size total_size,
				  uint32 htup_offset, uint32 varlen_offset,
				  TupleTableSlot *slot, HeapTuple htup)
{
	TupleDesc	desc = slot->tts_tupleDescriptor;
	int			natts = desc->natts;
	Datum	   *dst_values;
	bool	   *dst_isnull;
	char	   *varlen_cursor;

	ft->total_size = (uint32) total_size;
	ft->natts = natts;
	ft->htup_offset = htup_offset;
	ft->htup_len = htup->t_len;

	dst_values = FLAT_TUPLE_VALUES(ft);
	dst_isnull = FLAT_TUPLE_ISNULL(ft);
	varlen_cursor = (char *) ft + varlen_offset;

	for (int i = 0; i < natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(desc, i);

		dst_isnull[i] = slot->tts_isnull[i];

		if (slot->tts_isnull[i])
		{
			dst_values[i] = (Datum) 0;
		}
		else if (attr->attbyval)
		{
			dst_values[i] = slot->tts_values[i];
		}
		else
		{
			Size		datum_size = datumGetSize(slot->tts_values[i],
												  false, attr->attlen);

			memcpy(varlen_cursor,
				   DatumGetPointer(slot->tts_values[i]),
				   datum_size);
			/* 存相对 ft 基址的偏移,不存进程本地指针 */
			dst_values[i] = (Datum) (varlen_cursor - (char *) ft);
			varlen_cursor += MAXALIGN(datum_size);
		}
	}

	memcpy((char *) ft + htup_offset, htup->t_data, htup->t_len);
}

bool
RowCacheUnflattenToSlot(const FlatCachedTuple *flat,
						Oid relid,
						ItemPointer tid,
						TupleTableSlot *slot)
{
	TupleDesc	desc = slot->tts_tupleDescriptor;
	HeapTuple	copy;
	Size		payload_len;
	char	   *local_htup;
	Datum	   *src_values;
	bool	   *src_isnull;

	Assert(flat != NULL);
	Assert(slot != NULL);
	Assert(tid != NULL);

	if (desc->natts != flat->natts)
		return false;

	payload_len = flat->total_size - flat->htup_offset;
	copy = (HeapTuple) palloc(HEAPTUPLESIZE + payload_len);

	copy->t_len = flat->htup_len;
	copy->t_tableOid = relid;
	ItemPointerCopy(tid, &copy->t_self);
	copy->t_data = (HeapTupleHeader) ((char *) copy + HEAPTUPLESIZE);

	memcpy((char *) copy + HEAPTUPLESIZE,
		   (char *) flat + flat->htup_offset,
		   payload_len);

	ExecForceStoreHeapTupleNoCopy(copy, slot, true);

	src_values = FLAT_TUPLE_VALUES(flat);
	src_isnull = FLAT_TUPLE_ISNULL(flat);

	memcpy(slot->tts_values, src_values, sizeof(Datum) * flat->natts);
	memcpy(slot->tts_isnull, src_isnull, sizeof(bool) * flat->natts);

	local_htup = (char *) copy->t_data;
	for (int i = 0; i < flat->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(desc, i);

		if (!src_isnull[i] && !attr->attbyval)
			slot->tts_values[i] = PointerGetDatum(
				local_htup + (Size) src_values[i] - flat->htup_offset);
	}

	slot->tts_nvalid = flat->natts;

	return true;
}

void
RelationRowCacheLoadRelation(Relation rel)
{
	Oid			relid = RelationGetRelid(rel);
	RelMeta    *rm;
	TableScanDesc scan;
	TupleTableSlot *slot;
	bool		pushed_snapshot = false;

	if (RowCacheCtl == NULL)
		elog(ERROR, "row cache shared memory not initialized");

	RowCacheEnsureBackendInit();

	rm = AllocateOrFindRelMeta(MyDatabaseId, relid);
	if (rm == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("row cache: out of relation slots"),
				 errdetail_internal("row_cache.max_relations = %d",
									ROW_CACHE_MAX_RELATIONS)));

	LWLockAcquire(&rm->build_lock, LW_EXCLUSIVE);

	if (pg_atomic_read_u32(&rm->state) != RELMETA_DISABLED)
	{
		pg_atomic_write_u32(&rm->state, RELMETA_DISABLED);
		pg_memory_barrier();
	}

	/*
	 * 无论 state 如何,只要还挂着段就先释放——覆盖上次 load/drop 中途
	 * 被中断留下的残段(ReleaseAllSegmentsForRel 幂等)。
	 */
	if (rm->first_seg >= 0)
		ReleaseAllSegmentsForRel(rm);

	pg_atomic_write_u32(&rm->state, RELMETA_LOADING);

	if (!CheckEligiblePkey(rel, rm))
	{
		pg_atomic_write_u32(&rm->state, RELMETA_DISABLED);
		LastLookupRelMeta = NULL;
		LastLookupRelid = InvalidOid;
		LWLockRelease(&rm->build_lock);
		return;
	}

	/* schema 指纹:bind 慢路径复核用(S2)。 */
	rm->fp_relfilenumber = rel->rd_locator.relNumber;
	rm->fp_tupdesc_hash = RowCacheTupDescHash(RelationGetDescr(rel));

	if (!ActiveSnapshotSet())
	{
		PushActiveSnapshot(GetTransactionSnapshot());
		pushed_snapshot = true;
	}

	scan = table_beginscan(rel, GetActiveSnapshot(), 0, NULL);
	slot = table_slot_create(rel, NULL);

	/*
	 * 中途若段池耗尽(或其他 ereport),在 PG_CATCH 里清理;scan / slot /
	 * snapshot 是事务资源,由事务 abort 自动回收,PG_CATCH 只需清理行缓存
	 * 的 shmem 状态(整段释放,不再逐条 free)。
	 */
	PG_TRY();
	{
		int64		nloaded = 0;
		int64		nskipped_pkey = 0;
		int64		nskipped_big = 0;

		while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
		{
			SerializedPkey pkey;
			uint32		pkey_hash;
			uint32		bucket;
			HeapTuple	htup;
			Size		flat_size;
			uint32		htup_off;
			uint32		varlen_off;
			GlobalEntry *e;
			LWLock	   *part;

			slot_getallattrs(slot);

			if (!SerializePkeyFromSlot(slot, rm->n_pkey_attrs,
									rm->pkey_descs, &pkey))
			{
				nskipped_pkey++;
				continue;
			}

			pkey_hash = ComputePkeyHashBytes(pkey.data, pkey.len);
			bucket = pkey_hash & RowCacheCtl->bucket_mask;

			htup = ExecCopySlotHeapTuple(slot);
			flat_size = FlatTupleComputeSize(slot, htup, &htup_off, &varlen_off);

			e = SegAllocEntry(rm, EntryFlatOffset(pkey.len) + flat_size);
			if (e == NULL)
			{
				/* 单行超过段容量:不缓存该行(读路径 miss 回退)。 */
				heap_freetuple(htup);
				SerializedPkeyRelease(&pkey);
				nskipped_big++;
				continue;
			}

			StoreEntryPkey(e, &pkey);
			SerializedPkeyRelease(&pkey);
			FlatTupleFillInto(ENTRY_FLAT(e), flat_size, htup_off, varlen_off,
							  slot, htup);
			heap_freetuple(htup);

			e->dboid = MyDatabaseId;
			e->relid = relid;
			e->pkey_hash = pkey_hash;
			ItemPointerCopy(&slot->tts_tid, &e->tid);

			part = PartitionLockForBucket(bucket);
			LWLockAcquire(part, LW_EXCLUSIVE);
			BucketInsertHead(bucket, e);
			LWLockRelease(part);

			nloaded++;
		}

		{
			int			nsegs = 0;

			for (int32 s = rm->first_seg; s >= 0; s = RowCacheSegs[s].next_seg)
				nsegs++;
			elog(DEBUG1, "row cache: loaded relation %u: %lld rows, %d segments"
				 " (skipped: %lld null-pkey, %lld oversized)",
				 relid, (long long) nloaded, nsegs,
				 (long long) nskipped_pkey, (long long) nskipped_big);
		}
	}
	PG_CATCH();
	{
		/*
		 * 加载中途失败(通常是段池耗尽):整段释放已灌入的数据、把 state
		 * 回滚到 DISABLED、释放 RelMeta 槽位、推进代数,然后重新抛出。
		 * 此处只持有 build_lock;ReleaseAllSegmentsForRel 自取/放
		 * seg_lock 与分区锁。
		 *
		 * 注意:errfinish() 在 longjmp 之前把 InterruptHoldoffCount 清零,
		 * 而 build_lock 仍被本 backend 持有(错误恢复的 LWLockReleaseAll
		 * 尚未运行)。必须先补一个 HOLD_INTERRUPTS 与下面手动
		 * LWLockRelease(build_lock) 内部的 RESUME 配平——这是
		 * LWLockReleaseAll 的标准做法;顺带让清理期间的
		 * CHECK_FOR_INTERRUPTS 保持无操作。(V4 的 PG_CATCH 缺这一步,
		 * 只是其 DSA OOM 路径从未真正执行过,没暴露。)
		 */
		HOLD_INTERRUPTS();
		pg_atomic_write_u32(&rm->state, RELMETA_DISABLED);
		pg_memory_barrier();
		ReleaseAllSegmentsForRel(rm);
		rm->n_pkey_attrs = 0;
		rm->dboid = InvalidOid;
		rm->relid = InvalidOid;
		LastLookupRelMeta = NULL;
		LastLookupRelid = InvalidOid;
		LWLockRelease(&rm->build_lock);
		pg_atomic_fetch_add_u32(&RowCacheCtl->global_gen.value, 1);
		PG_RE_THROW();
	}
	PG_END_TRY();

	table_endscan(scan);
	ExecDropSingleTupleTableSlot(slot);

	if (pushed_snapshot)
		PopActiveSnapshot();

	/* 封存当前写入段:ENABLED 后所有段均为 FULL(可淘汰),带起步分。 */
	if (rm->cur_seg >= 0)
	{
		LWLockAcquire(&RowCacheCtl->seg_lock, LW_EXCLUSIVE);
		SegSeal(&RowCacheSegs[rm->cur_seg]);
		rm->cur_seg = -1;
		LWLockRelease(&RowCacheCtl->seg_lock);
	}

	pg_write_barrier();
	pg_atomic_write_u32(&rm->state, RELMETA_ENABLED);

	LastLookupRelMeta = NULL;
	LastLookupRelid = InvalidOid;

	LWLockRelease(&rm->build_lock);
	pg_atomic_fetch_add_u32(&RowCacheCtl->global_gen.value, 1);

	RelationRowCacheBindRelation(rel);
}

/*
 * DML 行级失效:按 pkey 从桶链上摘除对应 entry。
 *
 * S1 起摘链即完成——载荷留在段内成为死数据,等所在段被整段回收
 * (drop / 淘汰)时一并消失。失效路径上不再有任何内存释放调用。
 */
static void
InvalidateEntryByPkeyBytes(Oid dboid, Oid relid,
						   const uint8 *pkey_buf, uint32 pkey_len)
{
	uint32			pkey_hash;
	uint32			bucket;
	LWLock		   *part;
	GlobalEntry	   *prev;
	GlobalEntry	   *cur;

	pkey_hash = ComputePkeyHashBytes(pkey_buf, pkey_len);
	bucket = pkey_hash & RowCacheCtl->bucket_mask;
	part = PartitionLockForBucket(bucket);

	LWLockAcquire(part, LW_EXCLUSIVE);

	prev = NULL;
	for (cur = RowCacheBuckets[bucket]; cur != NULL; cur = cur->next)
	{
		if (cur->dboid == dboid &&
			cur->relid == relid &&
			cur->pkey_hash == pkey_hash &&
			cur->pkey_len == pkey_len &&
			memcmp(EntryPkeyDataConst(cur), pkey_buf, pkey_len) == 0)
		{
			if (prev != NULL)
				prev->next = cur->next;
			else
				RowCacheBuckets[bucket] = cur->next;
			break;
		}
		prev = cur;
	}

	LWLockRelease(part);
}

static void
InvalidateByHeapTuple(Relation rel, HeapTuple tuple)
{
	RelMeta	   *rm;
	Oid			relid;
	SerializedPkey pkey;

	if (RowCacheCtl == NULL)
		return;
	if (rel == NULL || tuple == NULL || tuple->t_data == NULL)
		return;

	RelationRowCacheBindRelation(rel);

	rm = rel->rd_rowcache_meta;
	if (rm == NULL || rm == ROWCACHE_NOT_CACHED)
		return;
	if (pg_atomic_read_u32(&rm->state) != RELMETA_ENABLED)
		return;
	if (rel->rd_rowcache_pkey_n <= 0)
		return;

	relid = RelationGetRelid(rel);

	/*
	 * 回填竞态屏障(S3):先推进失效计数,再做摘链。顺序保证两种交错
	 * 都安全——回填者在其分区锁临界区内比对计数:若本钩子先走完摘链
	 * (锁的 acquire 语义使回填者必见新计数)则回填放弃;若回填先插入,
	 * 下面的摘链必然命中刚插入的条目并将其摘除。
	 */
	pg_atomic_fetch_add_u64(&rm->inval_counter, 1);

	/*
	 * 用 reldata 的本地 pkey schema 快照序列化。
	 *
	 * S4 起 by-ref 主键列在此处 detoast,两个调用点的上下文前提必须
	 * 保持(挪动 heapam.c 的钩子位置前先读这里):
	 *
	 * 1. tuple 指向共享 buffer 页内数据,钩子必须在 ReleaseBuffer 之前
	 *    调用(仅靠 pin 保命;content lock 已释放,detoast 打开 TOAST
	 *    表读页是合法的)。
	 * 2. heap_delete 路径上 heap_toast_delete 先于本钩子执行——旧行的
	 *    out-of-line TOAST 行此刻已被本事务标删。detoast 仍能读回,依
	 *    赖的是 SnapshotToast 的可见性规则不检查 xmax(删除只是标记,
	 *    页上数据仍在,且删除事务尚未提交)。若未来 TOAST 可见性语义
	 *    收紧,这里必须改为在 heap_toast_delete 之前取键。
	 */
	if (!SerializePkeyFromTuple(tuple, RelationGetDescr(rel),
								 rel->rd_rowcache_pkey_n,
								 rel->rd_rowcache_pkey_descs, &pkey))
		return;

	InvalidateEntryByPkeyBytes(MyDatabaseId, relid, pkey.data, pkey.len);
	SerializedPkeyRelease(&pkey);
}


void
RowCacheOnHeapUpdate(Relation rel, HeapTuple oldtup, HeapTuple newtup)
{
	(void) newtup;
	InvalidateByHeapTuple(rel, oldtup);
}

void
RowCacheOnHeapDelete(Relation rel, HeapTuple oldtup)
{
	InvalidateByHeapTuple(rel, oldtup);
}

void
RelationRowCacheDropRelation(Oid relid)
{
	RelMeta    *rm;

	if (RowCacheCtl == NULL)
		return;

	rm = FindRelMeta(MyDatabaseId, relid);
	if (rm == NULL)
		return;

	RowCacheEnsureBackendInit();

	LWLockAcquire(&rm->build_lock, LW_EXCLUSIVE);


	pg_atomic_write_u32(&rm->state, RELMETA_DISABLED);
	pg_memory_barrier();

	ReleaseAllSegmentsForRel(rm);

	rm->n_pkey_attrs = 0;
	rm->dboid = InvalidOid;
	rm->relid = InvalidOid;

	LastLookupRelMeta = NULL;
	LastLookupRelid = InvalidOid;

	LWLockRelease(&rm->build_lock);

	pg_atomic_fetch_add_u32(&RowCacheCtl->global_gen.value, 1);
}

/*
 * DROP DATABASE 钩子:清掉目标库在共享缓存里的全部 RelMeta 槽与段。
 *
 * 必须由 dropdb(dbcommands.c)调用:缓存身份是 (dboid, relid),库删
 * 掉后不再有任何 backend 能以该库身份 bind/drop 这些槽——不清理则
 * 每删一个已加载缓存的库就永久泄漏若干 RelMeta 槽(上限 64,耗尽后
 * 只能重启)。调用时 dropdb 已确保目标库无活跃连接,build_lock 至多
 * 被别库的淘汰者短暂 conditional 持有,常规 Acquire 等待即可。
 */
void
RelationRowCacheDropDatabase(Oid dboid)
{
	int			ndropped = 0;

	if (RowCacheCtl == NULL || !OidIsValid(dboid))
		return;

	for (int i = 0; i < ROW_CACHE_MAX_RELATIONS; i++)
	{
		RelMeta    *rm = &RowCacheCtl->relmetas[i];

		if (rm->dboid != dboid || !OidIsValid(rm->relid))
			continue;

		LWLockAcquire(&rm->build_lock, LW_EXCLUSIVE);

		/* 持锁重验:期间槽可能已被并发 drop/复用。 */
		if (rm->dboid == dboid && OidIsValid(rm->relid))
		{
			pg_atomic_write_u32(&rm->state, RELMETA_DISABLED);
			pg_memory_barrier();

			ReleaseAllSegmentsForRel(rm);

			rm->n_pkey_attrs = 0;
			rm->dboid = InvalidOid;
			rm->relid = InvalidOid;
			ndropped++;
		}
		LWLockRelease(&rm->build_lock);
	}

	if (ndropped > 0)
	{
		LastLookupRelMeta = NULL;
		LastLookupRelid = InvalidOid;
		pg_atomic_fetch_add_u32(&RowCacheCtl->global_gen.value, 1);
		elog(DEBUG1, "row cache: dropped %d relation(s) of database %u",
			 ndropped, dboid);
	}
}

static bool
DoPkeyFetchBytes(RelMeta *rm, Oid relid,
				 const uint8 *pkey_buf, uint32 pkey_len,
				 Snapshot snapshot, TupleTableSlot *slot,
				 bool *is_visible, bool *has_hot_chain)
{
	uint32			hash;
	uint32			bucket;
	LWLock		   *part;
	GlobalEntry	   *entry;
	FlatCachedTuple *flat;
	HeapTupleData	htup;
	ItemPointerData	tid;
	int32			sid;
	uint32			seq_seen;

	/*
	 * 槽位换代(ABA)防御:probe 无锁,rm 可能已被释放并复用给另一个
	 * (dboid, relid) 组合——长生命周期执行器(游标/缓存计划)持有的
	 * 绑定不会因 global_gen 变化而立即重建。这里做快速拒绝,而真正的
	 * 硬保证在下面:查桶键锚定在不可变的 MyDatabaseId 上,即使 rm 在
	 * 校验后被并发换代,也只可能命中(本库, 本表, 本键)的真条目。
	 */
	if (rm->dboid != MyDatabaseId || rm->relid != relid)
		return false;
	if (pg_atomic_read_u32(&rm->state) != RELMETA_ENABLED)
		return false;
	pg_read_barrier();

	hash = ComputePkeyHashBytes(pkey_buf, pkey_len);
	bucket = hash & RowCacheCtl->bucket_mask;
	part = PartitionLockForBucket(bucket);

	/*
	 * S2 读协议:锁内只做"查链 + pin 所在段",可见性判断与整行拷贝移到
	 * 锁外——分区锁持有时长从"整行拷贝"缩到"链查找"。
	 *
	 * 安全论证:持分区锁期间 entry 在链上 ⇒ 其所在段必然活着(段物理
	 * 复用前必须先摘净全部条目,摘链需要本分区排他锁,会被我们挡住);
	 * pin 在锁内完成 ⇒ 放锁后段的物理复用被 pin_cnt 阻止,锁外读到的
	 * 字节稳定。并发 DML 失效只改链指针、不改 entry 内容,不影响在途
	 * 拷贝——语义等同 V4 的"锁内拷贝刚结束,行随即被更新"。
	 */
	LWLockAcquire(part, LW_SHARED);

	/* 查桶键用不可变的 MyDatabaseId,绝不用可变的 rm->dboid(ABA)。 */
	entry = BucketLookup(bucket, MyDatabaseId, relid, hash, pkey_buf, pkey_len);
	if (entry == NULL)
	{
		LWLockRelease(part);
		return false;
	}

	sid = EntrySegId(entry);
	seq_seen = RowCacheSegs[sid].seq_num;
	SegPin(sid);

	/* S3:段级命中计数,washer 打分(衰减 LFU)的输入。 */
	pg_atomic_fetch_add_u32(&RowCacheSegs[sid].recent_get_cnt, 1);

	LWLockRelease(part);

	/* ---- 以下在锁外,段内存由 pin 保护 ---- */

	flat = ENTRY_FLAT(entry);

	ItemPointerCopy(&entry->tid, &tid);
	htup.t_data = (HeapTupleHeader) FLAT_TUPLE_HTUP_DATA(flat);
	htup.t_len = flat->htup_len;
	htup.t_tableOid = relid;
	ItemPointerCopy(&tid, &htup.t_self);

	*has_hot_chain = HeapTupleIsHotUpdated(&htup);
	*is_visible = RowCacheTupleVisibleMVCC(&htup, snapshot);

	if (*is_visible)
	{
		slot->tts_tableOid = relid;
		ItemPointerCopy(&tid, &slot->tts_tid);

		/*
		 * 唯一可能 ereport 的点(palloc OOM):在途 pin 由事务 abort
		 * 回调(RowCacheXactCallback)兜底释放,无需 PG_TRY。
		 *
		 * 展开失败(natts 与缓存时不符——正常情况下 schema 指纹会先
		 * 一步失效整表,这里是纵深防御)按 miss 处理,绝不带着未填充
		 * 的 slot 报命中。
		 */
		if (!RowCacheUnflattenToSlot(flat, relid, &tid, slot))
		{
			SegUnpin(sid);
			*is_visible = false;
			*has_hot_chain = false;
			return false;
		}
	}

	/*
	 * 换代绊线:pin 协议成立时段在 pin 期间绝不换代;失配说明回收纪律
	 * 被破坏(bug)。防御性按 miss 处理——调用方回退原生路径,slot 中
	 * 的可疑内容会被真实读取覆盖。
	 */
	if (unlikely(RowCacheSegs[sid].seq_num != seq_seen))
	{
		Assert(false);
		SegUnpin(sid);
		*is_visible = false;
		*has_hot_chain = false;
		return false;
	}

	SegUnpin(sid);
	return true;
}


/* ----------------------------------------------------------------
 * schema 指纹(S2)
 *
 * 信号与裁决分离:relcache inval 是脏信号(vacuum/analyze 的 pg_class
 * inplace 更新、GRANT、建索引、sinval 溢出都会触发),不能拿来直接
 * DISABLED。真正的裁决在 bind 慢路径用指纹比对完成:指纹没变(例行
 * 维护)缓存零损失;指纹变了(真 DDL)才失效。
 * ---------------------------------------------------------------- */

static uint64
RowCacheTupDescHash(TupleDesc desc)
{
	uint64		h = (uint64) desc->natts;

	for (int i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, i);
		uint32		x[5];

		x[0] = (uint32) att->atttypid;
		x[1] = (uint32) att->atttypmod;
		x[2] = (uint32) (uint16) att->attlen |
			((uint32) att->attbyval << 16) |
			((uint32) att->attisdropped << 17);
		x[3] = (uint32) att->attnum;
		x[4] = (uint32) att->attcollation;
		h = hash_combine64(h, hash_bytes_extended((const unsigned char *) x,
												  sizeof(x), 0));
	}
	return h;
}

static inline bool
RowCacheFingerprintMatches(Relation rel, RelMeta *rm)
{
	return rm->fp_relfilenumber == rel->rd_locator.relNumber &&
		rm->fp_tupdesc_hash == RowCacheTupDescHash(RelationGetDescr(rel));
}

static RelMeta *
LookupRelMetaForFetch(Oid relid)
{
	RelMeta	   *rm;

	if (RowCacheCtl == NULL || !OidIsValid(relid))
		return NULL;

	if (relid == LastLookupRelid)
		rm = LastLookupRelMeta;
	else
	{
		rm = FindRelMeta(MyDatabaseId, relid);
		LastLookupRelid = relid;
		LastLookupRelMeta = rm;
	}

	if (rm == NULL ||
		pg_atomic_read_u32(&rm->state) != RELMETA_ENABLED)
		return NULL;

	return rm;
}

bool
RelationRowCachePkeyFetch(Oid relid,
						  Datum pkey_val,
						  Snapshot snapshot,
						  TupleTableSlot *slot,
						  bool *is_visible,
						  bool *has_hot_chain)
{
	RelMeta	   *rm;
	RowCachePkeyDesc desc;
	SerializedPkey pkey;
	Datum		value = pkey_val;
	bool		found;

	Assert(is_visible != NULL && has_hot_chain != NULL);
	*is_visible = false;
	*has_hot_chain = false;

	if (!IsMVCCSnapshot(snapshot))
		return false;

	rm = LookupRelMetaForFetch(relid);
	if (rm == NULL)
		return false;

	/* 旧的非 bound API 不是执行器热路径;条件共享锁下取稳定描述符。 */
	if (!LWLockConditionalAcquire(&rm->build_lock, LW_SHARED))
		return false;
	if (rm->dboid != MyDatabaseId || rm->relid != relid ||
		pg_atomic_read_u32(&rm->state) != RELMETA_ENABLED ||
		rm->n_pkey_attrs != 1)
	{
		LWLockRelease(&rm->build_lock);
		return false;
	}
	desc = rm->pkey_descs[0];
	LWLockRelease(&rm->build_lock);

	if (!SerializePkeyFromDatumArray(&value, 1, &desc, &pkey))
		return false;
	found = DoPkeyFetchBytes(rm, relid, pkey.data, pkey.len,
							snapshot, slot, is_visible, has_hot_chain);
	SerializedPkeyRelease(&pkey);
	return found;
}

bool
RelationRowCachePkeyFetchComposite(Oid relid,
								   const Datum *vals,
								   int nvals,
								   Snapshot snapshot,
								   TupleTableSlot *slot,
								   bool *is_visible,
								   bool *has_hot_chain)
{
	RelMeta	   *rm;
	RowCachePkeyDesc descs[ROW_CACHE_PKEY_MAX_ATTS];
	SerializedPkey pkey;
	bool		found;

	Assert(is_visible != NULL && has_hot_chain != NULL);
	*is_visible = false;
	*has_hot_chain = false;

	if (vals == NULL || nvals <= 0)
		return false;
	if (!IsMVCCSnapshot(snapshot))
		return false;

	rm = LookupRelMetaForFetch(relid);
	if (rm == NULL)
		return false;

	if (!LWLockConditionalAcquire(&rm->build_lock, LW_SHARED))
		return false;
	if (rm->dboid != MyDatabaseId || rm->relid != relid ||
		pg_atomic_read_u32(&rm->state) != RELMETA_ENABLED ||
		rm->n_pkey_attrs != nvals)
	{
		LWLockRelease(&rm->build_lock);
		return false;
	}
	memcpy(descs, rm->pkey_descs, sizeof(RowCachePkeyDesc) * nvals);
	LWLockRelease(&rm->build_lock);

	if (!SerializePkeyFromDatumArray(vals, nvals, descs, &pkey))
		return false;
	found = DoPkeyFetchBytes(rm, relid, pkey.data, pkey.len,
							snapshot, slot, is_visible, has_hot_chain);
	SerializedPkeyRelease(&pkey);
	return found;
}

void
RelationRowCacheBindRelation(Relation rel)
{
	Oid			relid;
	RelMeta	   *rm;
	int			n;
	uint32		cur_gen;

	if (rel == NULL)
		return;

	if (RowCacheCtl == NULL)
		return;

	/* 注册 relcache 失效回调(每 backend 一次,冷路径)。 */
	RowCacheEnsureBackendInit();

	cur_gen = pg_atomic_read_u32(&RowCacheCtl->global_gen.value);

	if (rel->rd_rowcache_meta != NULL && rel->rd_rowcache_gen == cur_gen)
		return;

	rel->rd_rowcache_gen = cur_gen;

	relid = RelationGetRelid(rel);
	if (!OidIsValid(relid))
	{
		rel->rd_rowcache_meta = ROWCACHE_NOT_CACHED;
		return;
	}

	rm = FindRelMeta(MyDatabaseId, relid);
	if (rm == NULL ||
		pg_atomic_read_u32(&rm->state) != RELMETA_ENABLED)
	{
		rel->rd_rowcache_meta = ROWCACHE_NOT_CACHED;
		return;
	}
	pg_read_barrier();

	/*
	 * S4 描述符含 typid/byval,不能在槽位并发换代时无锁复制。绑定是
	 * 冷路径,用 conditional SHARED 取得身份、指纹与描述符的一致快照。
	 * 拿不到表示正被 load/drop/淘汰,当前查询回退,但保留 NULL 让下次
	 * 绑定重试,不能把一次短暂锁竞争永久粘成 NOT_CACHED。
	 */
	if (!LWLockConditionalAcquire(&rm->build_lock, LW_SHARED))
	{
		rel->rd_rowcache_meta = NULL;
		return;
	}
	if (rm->dboid != MyDatabaseId || rm->relid != relid ||
		pg_atomic_read_u32(&rm->state) != RELMETA_ENABLED)
	{
		LWLockRelease(&rm->build_lock);
		rel->rd_rowcache_meta = NULL;
		return;
	}

	/*
	 * schema 指纹复核(S2):relcache 重建后的首次绑定走到这里。指纹
	 * 相同(vacuum/analyze/GRANT 等例行维护触发的重建)则正常绑定;
	 * 不同(TRUNCATE/重写/加删改列等真 DDL)则升级为排他锁失效整表。
	 */
	if (!RowCacheFingerprintMatches(rel, rm))
	{
		LWLockRelease(&rm->build_lock);

		if (LWLockConditionalAcquire(&rm->build_lock, LW_EXCLUSIVE))
		{
			bool		invalidated = false;

			if (rm->dboid == MyDatabaseId && rm->relid == relid &&
				pg_atomic_read_u32(&rm->state) == RELMETA_ENABLED &&
				!RowCacheFingerprintMatches(rel, rm))
			{
				elog(DEBUG1, "row cache: schema fingerprint mismatch for relation %u, invalidating",
					 relid);
				pg_atomic_write_u32(&rm->state, RELMETA_DISABLED);
				pg_memory_barrier();
				ReleaseAllSegmentsForRel(rm);
				rm->n_pkey_attrs = 0;
				rm->dboid = InvalidOid;
				rm->relid = InvalidOid;
				LastLookupRelMeta = NULL;
				LastLookupRelid = InvalidOid;
				invalidated = true;
			}
			LWLockRelease(&rm->build_lock);
			if (invalidated)
				pg_atomic_fetch_add_u32(&RowCacheCtl->global_gen.value, 1);
		}

		/* 当前查询回退;若排他锁竞争失败,后续绑定继续尝试裁决。 */
		rel->rd_rowcache_meta = NULL;
		return;
	}

	n = rm->n_pkey_attrs;
	if (n <= 0 || n > ROW_CACHE_PKEY_MAX_ATTS)
	{
		LWLockRelease(&rm->build_lock);
		rel->rd_rowcache_meta = ROWCACHE_NOT_CACHED;
		return;
	}

	rel->rd_rowcache_pkey_n = n;
	memcpy(rel->rd_rowcache_pkey_descs, rm->pkey_descs,
		   sizeof(RowCachePkeyDesc) * n);
	rel->rd_rowcache_meta = rm;
	LWLockRelease(&rm->build_lock);
}

bool
RelationRowCachePkeyFetchBound(RelMeta *rm,
							   Oid expected_relid,
							   const RowCachePkeyDesc *descs,
							   const Datum *vals,
							   int nvals,
							   Snapshot snapshot,
							   TupleTableSlot *slot,
							   bool *is_visible,
							   bool *has_hot_chain)
{
	SerializedPkey pkey;
	bool		found;

	Assert(is_visible != NULL && has_hot_chain != NULL);
	*is_visible = false;
	*has_hot_chain = false;

	if (rm == NULL || rm == ROWCACHE_NOT_CACHED)
		return false;
	if (descs == NULL || vals == NULL || nvals <= 0)
		return false;
	if (!IsMVCCSnapshot(snapshot))
		return false;

	/* 槽位换代防御:身份 = (dboid, relid),缺一不可。 */
	if (rm->dboid != MyDatabaseId || rm->relid != expected_relid)
		return false;

	if (pg_atomic_read_u32(&rm->state) != RELMETA_ENABLED)
		return false;

	if (!SerializePkeyFromDatumArray(vals, nvals, descs, &pkey))
		return false;

	found = DoPkeyFetchBytes(rm, expected_relid, pkey.data, pkey.len,
							snapshot, slot, is_visible, has_hot_chain);
	SerializedPkeyRelease(&pkey);
	return found;
}

/*
 * 探测 miss 时执行器记下的失效代数(回填竞态屏障的 c1)。
 * 必须在原生路径读堆之前取得——调用点在 ExecIndexNextRowCache 开头。
 */
uint64
RelationRowCacheInvalGen(RelMeta *rm)
{
	if (rm == NULL || rm == ROWCACHE_NOT_CACHED)
		return 0;
	return pg_atomic_read_u64(&rm->inval_counter);
}

/*
 * 点查 miss 按需回填(S3,对应 OB get_block_row 的 miss 后回填)。
 *
 * best-effort:任何检查不满足直接放弃,绝不阻塞、绝不淘汰、绝不影响
 * 查询结果。gen_seen 是探测 miss 时(读堆之前)记下的失效代数,插入
 * 前在分区锁内终检——期间本表有任何 DML 即放弃,堵住"读到 xmax 干净
 * 的旧版本、并发 UPDATE 摘链扑空、回填复活死行"的窗口(与 DML 钩子
 * 的 counter++ 先于摘链共同构成完整屏障,见 RelMeta.inval_counter)。
 *
 * 行状态三关(对应 OB 的 !have_uncommited_row / read_with_same_schema):
 *   1. xmin 已提交且 hint 已设(排除本事务未提交行;hint 未设保守放弃,
 *      不做 clog 查询);
 *   2. 未被删/改(xmax invalid 或仅行锁);
 *   3. 非 HOT 更新链的中间版本。
 */
bool
RelationRowCacheBackfillBound(RelMeta *rm, Oid expected_relid,
							  TupleTableSlot *slot, uint64 gen_seen)
{
	SerializedPkey pkey;
	uint32		pkey_hash;
	uint32		bucket;
	HeapTuple	src = NULL;
	bool		src_should_free = false;
	HeapTuple	htup;
	Size		flat_size;
	uint32		htup_off;
	uint32		varlen_off;
	GlobalEntry *e;
	LWLock	   *part;
	bool		inserted = false;

	SerializedPkeyInit(&pkey);

	if (!row_cache_backfill)
		return false;
	if (rm == NULL || rm == ROWCACHE_NOT_CACHED || slot == NULL)
		return false;
	if (RowCacheCtl == NULL)
		return false;

	/* 快速预检:期间已有 DML,不必白做 flatten。 */
	if (pg_atomic_read_u64(&rm->inval_counter) != gen_seen)
		return false;

	/*
	 * build_lock SHARED(conditional):挡住 drop/load/指纹失效/淘汰
	 * (均取 EXCLUSIVE)对段链与段内存的并发变更;表正被维护时直接
	 * 放弃。多个回填者 SHARED 共享,互相靠原子 bump 并行。
	 */
	if (!LWLockConditionalAcquire(&rm->build_lock, LW_SHARED))
		return false;

	if (rm->dboid != MyDatabaseId || rm->relid != expected_relid ||
		pg_atomic_read_u32(&rm->state) != RELMETA_ENABLED ||
		rm->n_pkey_attrs <= 0 ||
		rm->n_pkey_attrs > ROW_CACHE_PKEY_MAX_ATTS)
		goto out;

	/* 行状态三关。 */
	src = ExecFetchSlotHeapTuple(slot, false, &src_should_free);
	if (src == NULL || src->t_data == NULL)
		goto out;
	{
		uint16		infomask = src->t_data->t_infomask;

		if (!(infomask & HEAP_XMIN_COMMITTED))
			goto out;
		if (!(infomask & HEAP_XMAX_INVALID) &&
			!HEAP_XMAX_IS_LOCKED_ONLY(infomask))
			goto out;
		if (HeapTupleIsHotUpdated(src))
			goto out;
	}

	/*
	 * S4:按真实返回行的主键编码,不使用查询参数。即使其它索引的
	 * collation 认为不同字节等价,回填条目仍与 DML 失效键完全一致。
	 */
	if (!SerializePkeyFromSlot(slot, rm->n_pkey_attrs,
							rm->pkey_descs, &pkey))
		goto out;
	pkey_hash = ComputePkeyHashBytes(pkey.data, pkey.len);
	bucket = pkey_hash & RowCacheCtl->bucket_mask;

	slot_getallattrs(slot);
	htup = ExecCopySlotHeapTuple(slot);
	flat_size = FlatTupleComputeSize(slot, htup, &htup_off, &varlen_off);

	e = SegBackfillAlloc(rm, EntryFlatOffset(pkey.len) + flat_size);
	if (e == NULL)
	{
		heap_freetuple(htup);
		goto out;
	}

	StoreEntryPkey(e, &pkey);
	FlatTupleFillInto(ENTRY_FLAT(e), flat_size, htup_off, varlen_off,
					  slot, htup);
	heap_freetuple(htup);

	e->pkey_hash = pkey_hash;
	ItemPointerCopy(&slot->tts_tid, &e->tid);

	part = PartitionLockForBucket(bucket);
	LWLockAcquire(part, LW_EXCLUSIVE);

	/*
	 * 竞态屏障终检 + 查重(锁内):期间本表有 DML → 放弃;并发回填者
	 * 已插同键 → 放弃。放弃时 bump 出的空间成为死数据,随段回收消失。
	 */
	if (pg_atomic_read_u64(&rm->inval_counter) == gen_seen &&
		BucketLookup(bucket, MyDatabaseId, expected_relid, pkey_hash,
					 pkey.data, pkey.len) == NULL)
	{
		e->dboid = MyDatabaseId;
		e->relid = expected_relid;
		BucketInsertHead(bucket, e);
		inserted = true;
	}
	LWLockRelease(part);

	if (inserted)
		elog(DEBUG2, "row cache: backfilled one row of relation %u",
			 expected_relid);

out:
	SerializedPkeyRelease(&pkey);
	if (src_should_free && src != NULL)
		heap_freetuple(src);
	LWLockRelease(&rm->build_lock);
	return inserted;
}

AttrNumber
RelationRowCachePkeyAttno(Oid relid)
{
	RelMeta    *rm;

	if (RowCacheCtl == NULL || !OidIsValid(relid))
		return 0;

	if (relid == LastLookupRelid)
		rm = LastLookupRelMeta;
	else
	{
		rm = FindRelMeta(MyDatabaseId, relid);
		LastLookupRelid = relid;
		LastLookupRelMeta = rm;
	}

	if (rm == NULL ||
		pg_atomic_read_u32(&rm->state) != RELMETA_ENABLED)
		return 0;

	if (rm->n_pkey_attrs != 1)
		return 0;

	return rm->pkey_descs[0].attno;
}

int
RelationRowCachePkeyDescriptor(Oid relid, AttrNumber *out_attnos,
							   int max_attnos)
{
	RelMeta	   *rm;
	int			n;

	if (RowCacheCtl == NULL || !OidIsValid(relid))
		return 0;
	if (out_attnos == NULL || max_attnos <= 0)
		return 0;

	if (relid == LastLookupRelid)
		rm = LastLookupRelMeta;
	else
	{
		rm = FindRelMeta(MyDatabaseId, relid);
		LastLookupRelid = relid;
		LastLookupRelMeta = rm;
	}

	if (rm == NULL ||
		pg_atomic_read_u32(&rm->state) != RELMETA_ENABLED)
		return 0;

	n = rm->n_pkey_attrs;
	if (n <= 0 || n > max_attnos)
		return 0;

	for (int i = 0; i < n; i++)
		out_attnos[i] = rm->pkey_descs[i].attno;

	return n;
}

/* ----------------------------------------------------------------
 * 后台洗段进程(S3,对应 OceanBase 的 wash 定时线程)
 *
 * 每 ROW_CACHE_WASH_INTERVAL_MS 一轮:
 *   1. 衰减打分并封存空闲的 ACTIVE 尾段;
 *   2. 若此前出现过段需求(seg_demand > 0),把 score 最低的
 *      FULL 段洗回空闲链,直至水位 max(1, n_segments/16)。
 *
 * "无需求不洗段"避免蚕食合法占满池子的常驻表;洗段遵循与同步淘汰
 * 相同的锁序与 pin 排空纪律(复用 SegEvictOne)。
 * ---------------------------------------------------------------- */

static void
RowCacheWashRound(void)
{
	int32		free_cnt;
	int32		target;
	uint32		demand;

	if (RowCacheCtl == NULL)
		return;

	LWLockAcquire(&RowCacheCtl->seg_lock, LW_EXCLUSIVE);

	RowCacheRefreshSegmentsLocked();

	demand = pg_atomic_exchange_u32(&RowCacheCtl->seg_demand, 0);
	if (demand > 0)
	{
		free_cnt = 0;
		for (int32 s = RowCacheCtl->free_seg_head; s >= 0;
			 s = RowCacheSegs[s].next_seg)
			free_cnt++;

		target = Max(1, RowCacheCtl->n_segments / 16);

		while (free_cnt < target)
		{
			int32		sid = SegEvictOne(InvalidOid, InvalidOid);

			if (sid < 0)
				break;			/* 没有可淘汰段(全 FREE/ACTIVE/表被锁) */
			RowCacheSegs[sid].next_seg = RowCacheCtl->free_seg_head;
			RowCacheCtl->free_seg_head = sid;
			free_cnt++;
		}

		/*
		 * 暂时全是 ACTIVE 段或属主正忙时保留一个需求信号。待尾段
		 * 达到空闲阈值或 build_lock 释放后,后续轮次会自动补足水位,
		 * 不要求前台查询再次 miss 才能推动回收。
		 */
		if (free_cnt < target)
			pg_atomic_fetch_add_u32(&RowCacheCtl->seg_demand, 1);
	}

	LWLockRelease(&RowCacheCtl->seg_lock);
}

void
RowCacheWasherRegister(void)
{
	BackgroundWorker bgw;

	if (IsBinaryUpgrade)
		return;

	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS;
	bgw.bgw_start_time = BgWorkerStart_PostmasterStart;
	snprintf(bgw.bgw_library_name, MAXPGPATH, "postgres");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "RowCacheWasherMain");
	snprintf(bgw.bgw_name, BGW_MAXLEN, "row cache washer");
	snprintf(bgw.bgw_type, BGW_MAXLEN, "row cache washer");
	bgw.bgw_restart_time = 5;
	bgw.bgw_notify_pid = 0;
	bgw.bgw_main_arg = (Datum) 0;

	RegisterBackgroundWorker(&bgw);
}

void
RowCacheWasherMain(Datum main_arg)
{
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	pqsignal(SIGHUP, SIG_IGN);
	BackgroundWorkerUnblockSignals();

	elog(DEBUG1, "row cache washer started");

	for (;;)
	{
		int			rc;

		if (ShutdownRequestPending)
			proc_exit(0);

		RowCacheWashRound();

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   ROW_CACHE_WASH_INTERVAL_MS,
					   WAIT_EVENT_ROW_CACHE_WASHER_MAIN);
		if (rc & WL_LATCH_SET)
			ResetLatch(MyLatch);
	}
}
