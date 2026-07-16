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
 *   Todo:
 *     - S3:衰减 LFU 打分 + 后台洗段 + 点查 miss 按需回填
 *
 *   Pkey 限制:1..ROW_CACHE_PKEY_MAX_ATTS 个传值列。pkey 为传引用的表在
 *   Load 时被静默跳过(不报错,不建任何 entry)。
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/relation.h"
#include "access/tableam.h"
#include "access/tupmacs.h"
#include "access/xact.h"
#include "common/hashfn.h"
#include "catalog/pg_index.h"
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
#include "utils/wait_event.h"
#include "utils/dsa.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
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
 * Pkey 存储参数。
 *
 * ROW_CACHE_PKEY_MAX_ATTS  — Load 资格检查时接受的最大 pkey 列数。
 *
 * ROW_CACHE_PKEY_INLINE_BYTES — 内嵌进 GlobalEntry 的序列化(拼接)pkey
 *                            的最大字节长度。32 字节覆盖以下任一:
 *                              - 1 列 int8        (8B)
 *                              - 2 列 int8       (16B)
 *                              - 3 列 int8       (24B)
 *                              - 4 列 int8       (32B)
 *                              - 4 列 int4       (16B)
 *                              - 8 列 int4       (32B)
 *                            传引用pkey的表资格检查失败、保持不缓存。
 */
#define ROW_CACHE_PKEY_INLINE_BYTES	32

StaticAssertDecl(ROW_CACHE_PKEY_MAX_ATTS <= INDEX_MAX_KEYS,
				 "ROW_CACHE_PKEY_MAX_ATTS must not exceed INDEX_MAX_KEYS");

typedef struct RelMeta
{
	Oid				relid;			/* InvalidOid = 空闲槽 */
	pg_atomic_uint32 state;			/* RELMETA_{DISABLED,LOADING,ENABLED} */
	LWLock			build_lock;		/* 串行化 Load / Drop;不在读路径上 */

	/*
	 * Pkey 描述符。
	 *
	 * 支持单列与复合传值键,最多 ROW_CACHE_PKEY_MAX_ATTS 列、序列化总长
	 * <= ROW_CACHE_PKEY_INLINE_BYTES。
	 *
	 * 传引用 pkey 不在范围内:故资格检查成功后 pkey_byvals[] 恒为 true;
	 * 这个字段保留是为将来支持传引用留一个干净的扩展点
	 */
	int				n_pkey_attrs;	/* 不合格 / 未加载时为 0 */
	AttrNumber		pkey_attnos[ROW_CACHE_PKEY_MAX_ATTS];
	int16			pkey_typlens[ROW_CACHE_PKEY_MAX_ATTS];
	bool			pkey_byvals[ROW_CACHE_PKEY_MAX_ATTS];
	int				pkey_total_len;	/* pkey_typlens 之和, <= ROW_CACHE_PKEY_INLINE_BYTES */

	/*
	 * 本关系持有的段链(seg id,经 RowCacheSegment.next_seg 串联)。
	 * 受 build_lock 保护:loader 自己持有;淘汰者要动别人的链必须
	 * ConditionalAcquire 对方的 build_lock。
	 */
	int32			first_seg;		/* 段链头;-1 = 无 */
	int32			cur_seg;		/* 当前写入段(仅 LOADING 期间);-1 = 无 */

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
	Oid				relid;
	uint32			pkey_hash;
	uint8			pkey_len;		/* 序列化长度, <= ROW_CACHE_PKEY_INLINE_BYTES */
	uint8			pkey_buf[ROW_CACHE_PKEY_INLINE_BYTES];
	ItemPointerData	tid;			/* Load 时的 heap TID(填 slot 用) */
	uint32			span;			/* 本条目在段内占用的总字节数(含内联 flat) */
	struct GlobalEntry *next;		/* 桶链上的下一个 GlobalEntry(真实指针) */
	/* FlatCachedTuple 载荷内联紧随其后(MAXALIGN 对齐),见 ENTRY_FLAT */
} GlobalEntry;

/*
 * 段大小:固定 1MB。足够容纳千列宽行(超过段容量的单行在 load 时跳过,
 * 读路径 miss 回退原生,正确性无损),又保持淘汰粒度精细。
 */
#define ROW_CACHE_SEGMENT_SIZE	((uint32) (1024 * 1024))

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

/*
 * 段描述符(池外定长数组)。字段无原子:分配/封存由持 build_lock 的
 * loader 单写;归还/淘汰在 seg_lock(+victim build_lock)下串行。
 */
typedef struct RowCacheSegment
{
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

/* GlobalEntry 之后内联的 FlatCachedTuple。 */
#define ENTRY_FLAT(e) \
	((FlatCachedTuple *) ((char *) (e) + MAXALIGN(sizeof(GlobalEntry))))

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
static RelMeta *FindRelMeta(Oid relid);

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

static RelMeta *AllocateOrFindRelMeta(Oid relid);
static bool CheckEligiblePkey(Relation rel, RelMeta *rm);
static int SerializePkeyFromSlot(TupleTableSlot *slot, RelMeta *rm,
								 uint8 *out_buf);
static int SerializePkeyFromTuple(HeapTuple tuple, TupleDesc desc,
								  int n_pkey_attrs, const AttrNumber *attnos,
								  const int16 *typlens, uint8 *out_buf);
static int SerializePkeyFromDatum(Datum d, RelMeta *rm, uint8 *out_buf);
static int SerializePkeyFromDatumArray(const Datum *vals, int nvals,
									   RelMeta *rm, uint8 *out_buf);
static inline uint32 ComputePkeyHashBytes(const uint8 *buf, int len);
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

	base = (char *) ShmemInitStruct("Row Cache Control V5",
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

		rm->relid = InvalidOid;
		pg_atomic_init_u32(&rm->state, RELMETA_DISABLED);
		LWLockInitialize(&rm->build_lock, LWTRANCHE_ROW_CACHE_RELMETA);
		rm->n_pkey_attrs = 0;
		rm->pkey_total_len = 0;
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
FindRelMeta(Oid relid)
{
	if (!OidIsValid(relid))
		return NULL;

	for (int i = 0; i < ROW_CACHE_MAX_RELATIONS; i++)
	{
		RelMeta    *rm = &RowCacheCtl->relmetas[i];

		if (rm->relid == relid)
			return rm;
	}
	return NULL;
}

static RelMeta *
AllocateOrFindRelMeta(Oid relid)
{
	RelMeta    *rm;

	rm = FindRelMeta(relid);
	if (rm != NULL)
		return rm;

	LWLockAcquire(&RowCacheCtl->relmeta_alloc_lock, LW_EXCLUSIVE);

	/* 持锁下重新检查。 */
	for (int i = 0; i < ROW_CACHE_MAX_RELATIONS; i++)
	{
		if (RowCacheCtl->relmetas[i].relid == relid)
		{
			rm = &RowCacheCtl->relmetas[i];
			LWLockRelease(&RowCacheCtl->relmeta_alloc_lock);
			return rm;
		}
	}

	/* 找一个空闲槽。 */
	for (int i = 0; i < ROW_CACHE_MAX_RELATIONS; i++)
	{
		RelMeta    *cand = &RowCacheCtl->relmetas[i];

		if (cand->relid == InvalidOid)
		{
			cand->relid = relid;
			pg_atomic_write_u32(&cand->state, RELMETA_DISABLED);
			cand->n_pkey_attrs = 0;
			cand->pkey_total_len = 0;
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
 *   - pkey_attnos[]      — 每个 pkey 列的 heap attno,按索引顺序
 *   - pkey_typlens[]     — 每个 pkey 列的 attlen(1/2/4/8)
 *   - pkey_byvals[]      — 恒为 true(传引用不在范围内)
 *   - pkey_total_len     — pkey_typlens 之和, <= ROW_CACHE_PKEY_INLINE_BYTES
 *
 * 被拒情形:
 *   - 关系没有主键(或主键是 deferrable)
 *   - 任一 pkey 列是系统列 / 表达式(attno <= 0)
 *   - 任一 pkey 列是传引用、或 attlen 异常(<=0 或 > 8)
 *   - 列数超过 ROW_CACHE_PKEY_MAX_ATTS
 *   - 序列化总长 > ROW_CACHE_PKEY_INLINE_BYTES
 */
static bool
CheckEligiblePkey(Relation rel, RelMeta *rm)
{
	Oid			pkindex_oid;
	Relation	pkindex;
	Form_pg_index ind;
	int			natts;
	int			total = 0;
	AttrNumber	attnos[ROW_CACHE_PKEY_MAX_ATTS];
	int16		typlens[ROW_CACHE_PKEY_MAX_ATTS];

	rm->n_pkey_attrs = 0;
	rm->pkey_total_len = 0;

	pkindex_oid = RelationGetPrimaryKeyIndex(rel, false);
	if (!OidIsValid(pkindex_oid))
		return false;

	pkindex = index_open(pkindex_oid, AccessShareLock);
	ind = pkindex->rd_index;

	if (ind == NULL || ind->indnkeyatts <= 0 ||
		ind->indnkeyatts > ROW_CACHE_PKEY_MAX_ATTS)
	{
		index_close(pkindex, AccessShareLock);
		return false;
	}

	natts = ind->indnkeyatts;
	for (int i = 0; i < natts; i++)
	{
		AttrNumber	attno = ind->indkey.values[i];
		Form_pg_attribute attr;

		if (attno <= 0)
		{
			/* 系统列 / 表达式键 —— 不支持 */
			index_close(pkindex, AccessShareLock);
			return false;
		}

		attr = TupleDescAttr(RelationGetDescr(rel), attno - 1);

		/* 仅传值。拒绝传引用或异常 attlen。 */
		if (!attr->attbyval ||
			attr->attlen <= 0 ||
			attr->attlen > (int) sizeof(uint64))
		{
			index_close(pkindex, AccessShareLock);
			return false;
		}

		if (total + attr->attlen > ROW_CACHE_PKEY_INLINE_BYTES)
		{
			index_close(pkindex, AccessShareLock);
			return false;
		}

		attnos[i] = attno;
		typlens[i] = attr->attlen;
		total += attr->attlen;
	}

	index_close(pkindex, AccessShareLock);

	/* 把描述符提交进 RelMeta。 */
	rm->n_pkey_attrs = natts;
	rm->pkey_total_len = total;
	for (int i = 0; i < natts; i++)
	{
		rm->pkey_attnos[i] = attnos[i];
		rm->pkey_typlens[i] = typlens[i];
		rm->pkey_byvals[i] = true;
	}
	return true;
}

static int
SerializePkeyFromSlot(TupleTableSlot *slot, RelMeta *rm, uint8 *out_buf)
{
	int			total = 0;

	for (int i = 0; i < rm->n_pkey_attrs; i++)
	{
		AttrNumber	attno = rm->pkey_attnos[i];
		int16		typlen = rm->pkey_typlens[i];

		if (slot->tts_isnull[attno - 1])
			return -1;

		if (total + typlen > ROW_CACHE_PKEY_INLINE_BYTES)
			return -1;

		store_att_byval(out_buf + total,
						slot->tts_values[attno - 1],
						typlen);
		total += typlen;
	}
	return total;
}

/*
 * 从 HeapTuple 序列化 pkey。schema(列数 / attno / typlen)由调用方
 * 显式传入,而不是从 RelMeta 读——DML 钩子据此可直接用 RelationData
 * 上的本地快照(rd_rowcache_pkey_*)序列化,无需触碰 shmem RelMeta。
 */
static int
SerializePkeyFromTuple(HeapTuple tuple, TupleDesc desc,
					   int n_pkey_attrs, const AttrNumber *attnos,
					   const int16 *typlens, uint8 *out_buf)
{
	int			total = 0;

	for (int i = 0; i < n_pkey_attrs; i++)
	{
		AttrNumber	attno = attnos[i];
		int16		typlen = typlens[i];
		Datum		d;
		bool		isnull;

		d = heap_getattr(tuple, attno, desc, &isnull);
		if (isnull)
			return -1;

		if (total + typlen > ROW_CACHE_PKEY_INLINE_BYTES)
			return -1;

		store_att_byval(out_buf + total, d, typlen);
		total += typlen;
	}
	return total;
}

static int
SerializePkeyFromDatum(Datum d, RelMeta *rm, uint8 *out_buf)
{
	if (rm->n_pkey_attrs != 1)
		return -1;
	if (rm->pkey_typlens[0] > ROW_CACHE_PKEY_INLINE_BYTES)
		return -1;
	store_att_byval(out_buf, d, rm->pkey_typlens[0]);
	return rm->pkey_typlens[0];
}


static int
SerializePkeyFromDatumArray(const Datum *vals, int nvals,
							RelMeta *rm, uint8 *out_buf)
{
	int		total = 0;

	if (nvals != rm->n_pkey_attrs)
		return -1;
	if (rm->pkey_total_len > ROW_CACHE_PKEY_INLINE_BYTES)
		return -1;

	for (int i = 0; i < nvals; i++)
	{
		int16	typlen = rm->pkey_typlens[i];

		store_att_byval(out_buf + total, vals[i], typlen);
		total += typlen;
	}
	return total;
}

static inline uint32
ComputePkeyHashBytes(const uint8 *buf, int len)
{
	return hash_bytes(buf, len);
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
 * 走一条桶链,找匹配 (relid, pkey_hash, pkey_len, pkey_buf) 的 entry。
 *
 * 匹配条件:
 *   - relid 相等(4 字节比较)
 *   - pkey_hash 相等(廉价的哈希碰撞过滤)
 *   - pkey_len 相等(单字节比较)
 *   - memcmp(pkey_buf, ..., pkey_len) 做最终判等
 */
static GlobalEntry *
BucketLookup(uint32 bucket, Oid relid, uint32 pkey_hash,
			 const uint8 *pkey_buf, int pkey_len)
{
	GlobalEntry *e;

	for (e = RowCacheBuckets[bucket]; e != NULL; e = e->next)
	{
		if (e->relid == relid &&
			e->pkey_hash == pkey_hash &&
			e->pkey_len == pkey_len &&
			memcmp(e->pkey_buf, pkey_buf, pkey_len) == 0)
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
	seg->relid = InvalidOid;
	seg->state = RC_SEG_FREE;
	pg_atomic_write_u32(&seg->used, 0);
	pg_atomic_write_u32(&seg->n_entries, 0);
	pg_atomic_write_u32(&seg->recent_get_cnt, 0);
	seg->score = 0.0;
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
 * 衰减打分刷新(S3):score = score×0.9 + recent_get_cnt(取出清零)。
 * 调用方持 seg_lock EXCLUSIVE。washer 每个洗段周期跑一轮;同步淘汰
 * 路径在打分陈旧(超过一个周期)时补跑。
 */
static void
RowCacheRefreshScoresLocked(void)
{
	for (int32 s = 0; s < RowCacheCtl->n_segments; s++)
	{
		RowCacheSegment *seg = &RowCacheSegs[s];
		uint32		recent = pg_atomic_exchange_u32(&seg->recent_get_cnt, 0);

		if (seg->state == RC_SEG_FREE)
			seg->score = 0.0;
		else
			seg->score = seg->score * ROW_CACHE_SCORE_DECAY + (double) recent;
	}
	RowCacheCtl->last_score_refresh = GetCurrentTimestamp();
}

/*
 * 在 FULL 段中挑 score 最低者(同分取 alloc_seq 最小,退化为 FIFO)。
 * exempt_relid 的段被豁免(load 不淘汰自己);skip_relid 排除二次扫描
 * 时 build_lock 拿不到的表。调用方持 seg_lock EXCLUSIVE。
 */
static int32
SegPickVictim(Oid exempt_relid, Oid skip_relid)
{
	int32		victim = -1;
	double		victim_score = 0.0;
	uint64		victim_seq = 0;

	for (int32 s = 0; s < RowCacheCtl->n_segments; s++)
	{
		RowCacheSegment *seg = &RowCacheSegs[s];

		if (seg->state != RC_SEG_FULL)
			continue;
		if (seg->relid == exempt_relid ||
			(OidIsValid(skip_relid) && seg->relid == skip_relid))
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
 * (拿不到换一次候选)→ 摘净桶链引用 → 从属主段链摘除 → 等 pin 排空
 * 并换代。exempt_relid 的段被豁免。调用方持 seg_lock EXCLUSIVE。
 * 返回段号(已不在任何链上),拿不到返回 -1(绝不等待)。
 */
static int32
SegEvictOne(Oid exempt_relid)
{
	/* 打分陈旧时补一轮刷新(washer 常态每周期维护)。 */
	if (TimestampDifferenceExceeds(RowCacheCtl->last_score_refresh,
								   GetCurrentTimestamp(),
								   ROW_CACHE_WASH_INTERVAL_MS))
		RowCacheRefreshScoresLocked();

	for (;;)
	{
		int32		victim = SegPickVictim(exempt_relid, InvalidOid);
		RowCacheSegment *vseg;
		RelMeta    *vrm;
		int32	   *linkp;
		int32		cur;

		if (victim < 0)
			return -1;			/* 无可淘汰段 */

		vseg = &RowCacheSegs[victim];
		vrm = FindRelMeta(vseg->relid);
		if (vrm == NULL || vrm->relid != vseg->relid)
		{
			/* 不应发生:有主的段必有 RelMeta。防御:直接回收。 */
			elog(WARNING, "row cache: segment %d owned by relation %u without metadata",
				 victim, vseg->relid);
			SegUnlinkEntries(victim);
			SegPrepareReuse(victim);
			vseg->next_seg = -1;
			return victim;
		}

		if (!LWLockConditionalAcquire(&vrm->build_lock, LW_EXCLUSIVE))
		{
			/*
			 * victim 表正在 load/drop,跳过它:把该表的段全部临时排除
			 * 太复杂,简单起见本轮直接放弃淘汰这张表——把它的最老段
			 * 从候选里排除的办法是换一张表。为避免死循环,这里改为
			 * 线性扫描下一个次老候选:重扫时跳过该 relid。
			 */
			Oid			busy_relid = vseg->relid;
			int32		alt = SegPickVictim(exempt_relid, busy_relid);

			if (alt < 0)
				return -1;

			vseg = &RowCacheSegs[alt];
			vrm = FindRelMeta(vseg->relid);
			if (vrm == NULL || vrm->relid != vseg->relid ||
				!LWLockConditionalAcquire(&vrm->build_lock, LW_EXCLUSIVE))
				return -1;		/* 两次都不顺利:放弃,让 load 失败 */
			victim = alt;
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
SegPopOrEvict(Oid loading_relid)
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
	return SegEvictOne(loading_relid);
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
	e->relid = InvalidOid;		/* 插链前的未完成标记 */
	e->pkey_hash = 0;
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

			nseg->relid = rm->relid;
			nseg->state = RC_SEG_ACTIVE;
			pg_atomic_write_u32(&nseg->used, 0);
			pg_atomic_write_u32(&nseg->n_entries, 0);
			nseg->alloc_seq = ++RowCacheCtl->seg_alloc_counter;
			nseg->next_seg = rm->first_seg;
		}
		if (sid >= 0)
		{
			RowCacheSegs[sid].state = RC_SEG_FULL;
			RowCacheSegs[sid].score = ROW_CACHE_SEAL_BASE_SCORE;
		}
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
			nid = SegPopOrEvict(rm->relid);
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
				RowCacheSegs[sid].state = RC_SEG_FULL;
				RowCacheSegs[sid].score = ROW_CACHE_SEAL_BASE_SCORE;
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

	rm = AllocateOrFindRelMeta(relid);
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
			uint8		pkey_buf[ROW_CACHE_PKEY_INLINE_BYTES];
			int			pkey_len;
			uint32		pkey_hash;
			uint32		bucket;
			HeapTuple	htup;
			Size		flat_size;
			uint32		htup_off;
			uint32		varlen_off;
			GlobalEntry *e;
			LWLock	   *part;

			slot_getallattrs(slot);

			pkey_len = SerializePkeyFromSlot(slot, rm, pkey_buf);
			if (pkey_len < 0)
			{
				nskipped_pkey++;
				continue;
			}

			pkey_hash = ComputePkeyHashBytes(pkey_buf, pkey_len);
			bucket = pkey_hash & RowCacheCtl->bucket_mask;

			htup = ExecCopySlotHeapTuple(slot);
			flat_size = FlatTupleComputeSize(slot, htup, &htup_off, &varlen_off);

			e = SegAllocEntry(rm, MAXALIGN(sizeof(GlobalEntry)) + flat_size);
			if (e == NULL)
			{
				/* 单行超过段容量:不缓存该行(读路径 miss 回退)。 */
				heap_freetuple(htup);
				nskipped_big++;
				continue;
			}

			FlatTupleFillInto(ENTRY_FLAT(e), flat_size, htup_off, varlen_off,
							  slot, htup);
			heap_freetuple(htup);

			e->relid = relid;
			e->pkey_hash = pkey_hash;
			e->pkey_len = (uint8) pkey_len;
			memcpy(e->pkey_buf, pkey_buf, pkey_len);
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
		rm->pkey_total_len = 0;
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
		RowCacheSegs[rm->cur_seg].state = RC_SEG_FULL;
		RowCacheSegs[rm->cur_seg].score = ROW_CACHE_SEAL_BASE_SCORE;
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
InvalidateEntryByPkeyBytes(Oid relid, const uint8 *pkey_buf, int pkey_len)
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
		if (cur->relid == relid &&
			cur->pkey_hash == pkey_hash &&
			cur->pkey_len == pkey_len &&
			memcmp(cur->pkey_buf, pkey_buf, pkey_len) == 0)
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
	uint8		pkey_buf[ROW_CACHE_PKEY_INLINE_BYTES];
	int			pkey_len;

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

	/* 用 reldata 的本地 pkey schema 快照序列化 */
	pkey_len = SerializePkeyFromTuple(tuple, RelationGetDescr(rel),
									  rel->rd_rowcache_pkey_n,
									  rel->rd_rowcache_pkey_attnos,
									  rel->rd_rowcache_pkey_typlens,
									  pkey_buf);
	if (pkey_len < 0)
		return;

	InvalidateEntryByPkeyBytes(relid, pkey_buf, pkey_len);
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

	rm = FindRelMeta(relid);
	if (rm == NULL)
		return;

	RowCacheEnsureBackendInit();

	LWLockAcquire(&rm->build_lock, LW_EXCLUSIVE);


	pg_atomic_write_u32(&rm->state, RELMETA_DISABLED);
	pg_memory_barrier();

	ReleaseAllSegmentsForRel(rm);

	rm->n_pkey_attrs = 0;
	rm->pkey_total_len = 0;
	rm->relid = InvalidOid;

	LastLookupRelMeta = NULL;
	LastLookupRelid = InvalidOid;

	LWLockRelease(&rm->build_lock);

	pg_atomic_fetch_add_u32(&RowCacheCtl->global_gen.value, 1);
}

static bool
DoPkeyFetchBytes(RelMeta *rm, Oid relid,
				 const uint8 *pkey_buf, int pkey_len,
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

	entry = BucketLookup(bucket, relid, hash, pkey_buf, pkey_len);
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
		uint32		x[4];

		x[0] = (uint32) att->atttypid;
		x[1] = (uint32) att->atttypmod;
		x[2] = (uint32) (uint16) att->attlen |
			((uint32) att->attbyval << 16) |
			((uint32) att->attisdropped << 17);
		x[3] = (uint32) att->attnum;
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
		rm = FindRelMeta(relid);
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
	uint8		pkey_buf[ROW_CACHE_PKEY_INLINE_BYTES];
	int			pkey_len;

	Assert(is_visible != NULL && has_hot_chain != NULL);
	*is_visible = false;
	*has_hot_chain = false;

	if (!IsMVCCSnapshot(snapshot))
		return false;

	rm = LookupRelMetaForFetch(relid);
	if (rm == NULL)
		return false;

	if (rm->n_pkey_attrs != 1)
		return false;

	pkey_len = SerializePkeyFromDatum(pkey_val, rm, pkey_buf);
	if (pkey_len < 0)
		return false;

	return DoPkeyFetchBytes(rm, relid, pkey_buf, pkey_len,
							snapshot, slot, is_visible, has_hot_chain);
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
	uint8		pkey_buf[ROW_CACHE_PKEY_INLINE_BYTES];
	int			pkey_len;

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

	if (rm->n_pkey_attrs != nvals)
		return false;

	pkey_len = SerializePkeyFromDatumArray(vals, nvals, rm, pkey_buf);
	if (pkey_len < 0)
		return false;

	return DoPkeyFetchBytes(rm, relid, pkey_buf, pkey_len,
							snapshot, slot, is_visible, has_hot_chain);
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

	rm = FindRelMeta(relid);
	if (rm == NULL ||
		pg_atomic_read_u32(&rm->state) != RELMETA_ENABLED)
	{
		rel->rd_rowcache_meta = ROWCACHE_NOT_CACHED;
		return;
	}
	pg_read_barrier();

	/*
	 * schema 指纹复核(S2):relcache 重建后的首次绑定走到这里。指纹
	 * 相同(vacuum/analyze/GRANT 等例行维护触发的重建)→ 正常绑定,
	 * 缓存零损失;不同(TRUNCATE/重写/加删改列等真 DDL)→ 失效整表。
	 *
	 * 用条件锁:拿不到(正被 load/drop/淘汰)就本轮按未缓存绑定,失效
	 * 由下一个拿到锁的绑定者完成。期间无脏读窗口:本 backend 已绑
	 * NOT_CACHED;其它 backend 要么还没消费 inval(受 DDL 的
	 * AccessExclusiveLock 锁序保护,不可能正在查这张表),要么同样走到
	 * 这里。
	 */
	if (!RowCacheFingerprintMatches(rel, rm))
	{
		if (LWLockConditionalAcquire(&rm->build_lock, LW_EXCLUSIVE))
		{
			if (rm->relid == relid &&
				pg_atomic_read_u32(&rm->state) == RELMETA_ENABLED &&
				!RowCacheFingerprintMatches(rel, rm))
			{
				elog(DEBUG1, "row cache: schema fingerprint mismatch for relation %u, invalidating",
					 relid);
				pg_atomic_write_u32(&rm->state, RELMETA_DISABLED);
				pg_memory_barrier();
				ReleaseAllSegmentsForRel(rm);
				rm->n_pkey_attrs = 0;
				rm->pkey_total_len = 0;
				rm->relid = InvalidOid;
				LastLookupRelMeta = NULL;
				LastLookupRelid = InvalidOid;
				LWLockRelease(&rm->build_lock);
				pg_atomic_fetch_add_u32(&RowCacheCtl->global_gen.value, 1);
			}
			else
				LWLockRelease(&rm->build_lock);
		}
		rel->rd_rowcache_meta = ROWCACHE_NOT_CACHED;
		return;
	}

	n = rm->n_pkey_attrs;
	if (n <= 0 || n > ROW_CACHE_PKEY_MAX_ATTS)
	{
		rel->rd_rowcache_meta = ROWCACHE_NOT_CACHED;
		return;
	}

	rel->rd_rowcache_pkey_n = n;
	for (int i = 0; i < n; i++)
	{
		rel->rd_rowcache_pkey_attnos[i] = rm->pkey_attnos[i];
		rel->rd_rowcache_pkey_typlens[i] = rm->pkey_typlens[i];
	}

	rel->rd_rowcache_meta = rm;
}

bool
RelationRowCachePkeyFetchBound(RelMeta *rm,
							   Oid expected_relid,
							   const Datum *vals,
							   int nvals,
							   Snapshot snapshot,
							   TupleTableSlot *slot,
							   bool *is_visible,
							   bool *has_hot_chain)
{
	uint8		pkey_buf[ROW_CACHE_PKEY_INLINE_BYTES];
	int			pkey_len;

	Assert(is_visible != NULL && has_hot_chain != NULL);
	*is_visible = false;
	*has_hot_chain = false;

	if (rm == NULL || rm == ROWCACHE_NOT_CACHED)
		return false;
	if (vals == NULL || nvals <= 0)
		return false;
	if (!IsMVCCSnapshot(snapshot))
		return false;

	if (rm->relid != expected_relid)
		return false;

	if (pg_atomic_read_u32(&rm->state) != RELMETA_ENABLED)
		return false;

	if (rm->n_pkey_attrs != nvals)
		return false;

	pkey_len = SerializePkeyFromDatumArray(vals, nvals, rm, pkey_buf);
	if (pkey_len < 0)
		return false;

	return DoPkeyFetchBytes(rm, expected_relid, pkey_buf, pkey_len,
							snapshot, slot, is_visible, has_hot_chain);
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
							  const Datum *vals, int nvals,
							  TupleTableSlot *slot, uint64 gen_seen)
{
	uint8		pkey_buf[ROW_CACHE_PKEY_INLINE_BYTES];
	int			pkey_len;
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

	if (rm->relid != expected_relid ||
		pg_atomic_read_u32(&rm->state) != RELMETA_ENABLED ||
		rm->n_pkey_attrs != nvals)
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

	pkey_len = SerializePkeyFromDatumArray(vals, nvals, rm, pkey_buf);
	if (pkey_len < 0)
		goto out;
	pkey_hash = ComputePkeyHashBytes(pkey_buf, pkey_len);
	bucket = pkey_hash & RowCacheCtl->bucket_mask;

	slot_getallattrs(slot);
	htup = ExecCopySlotHeapTuple(slot);
	flat_size = FlatTupleComputeSize(slot, htup, &htup_off, &varlen_off);

	e = SegBackfillAlloc(rm, MAXALIGN(sizeof(GlobalEntry)) + flat_size);
	if (e == NULL)
	{
		heap_freetuple(htup);
		goto out;
	}

	FlatTupleFillInto(ENTRY_FLAT(e), flat_size, htup_off, varlen_off,
					  slot, htup);
	heap_freetuple(htup);

	e->pkey_hash = pkey_hash;
	e->pkey_len = (uint8) pkey_len;
	memcpy(e->pkey_buf, pkey_buf, pkey_len);
	ItemPointerCopy(&slot->tts_tid, &e->tid);

	part = PartitionLockForBucket(bucket);
	LWLockAcquire(part, LW_EXCLUSIVE);

	/*
	 * 竞态屏障终检 + 查重(锁内):期间本表有 DML → 放弃;并发回填者
	 * 已插同键 → 放弃。放弃时 bump 出的空间成为死数据,随段回收消失。
	 */
	if (pg_atomic_read_u64(&rm->inval_counter) == gen_seen &&
		BucketLookup(bucket, expected_relid, pkey_hash,
					 pkey_buf, pkey_len) == NULL)
	{
		e->relid = expected_relid;
		BucketInsertHead(bucket, e);
		inserted = true;
	}
	LWLockRelease(part);

	if (inserted)
		elog(DEBUG2, "row cache: backfilled one row of relation %u",
			 expected_relid);

out:
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
		rm = FindRelMeta(relid);
		LastLookupRelid = relid;
		LastLookupRelMeta = rm;
	}

	if (rm == NULL ||
		pg_atomic_read_u32(&rm->state) != RELMETA_ENABLED)
		return 0;

	if (rm->n_pkey_attrs != 1)
		return 0;

	return rm->pkey_attnos[0];
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
		rm = FindRelMeta(relid);
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
		out_attnos[i] = rm->pkey_attnos[i];

	return n;
}

/* ----------------------------------------------------------------
 * 后台洗段进程(S3,对应 OceanBase 的 wash 定时线程)
 *
 * 每 ROW_CACHE_WASH_INTERVAL_MS 一轮:
 *   1. 衰减打分刷新(score = score×0.9 + recent_get_cnt);
 *   2. 若上个周期出现过段需求(seg_demand > 0),把 score 最低的
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

	RowCacheRefreshScoresLocked();

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
			int32		sid = SegEvictOne(InvalidOid);

			if (sid < 0)
				break;			/* 没有可淘汰段(全 FREE/ACTIVE/表被锁) */
			RowCacheSegs[sid].next_seg = RowCacheCtl->free_seg_head;
			RowCacheCtl->free_seg_head = sid;
			free_cnt++;
		}
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
