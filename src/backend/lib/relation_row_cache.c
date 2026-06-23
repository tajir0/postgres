/*-------------------------------------------------------------------------
 *
 * relation_row_cache.c
 *	  行缓存:分区锁保护的全局哈希 (relid, pkey) -> 扁平化 tuple。
 *
 * 架构:
 *
 *   GlobalCache:一张 DSA 常驻、按 (relid, pkey) 键控的链地址哈希表。
 *                桶头存在一个扁平的 dsa_pointer[n_hash_buckets]
 *                数组里((1<<23) = 8M 个桶)。每个链元素是一个 GlobalEntry,
 *                持有 pkey、hash、载荷指针和 next_dp 链。
 *
 *   RelMeta[64]:固定 shmem 数组,保存 per-relation 元数据(DISABLED/
 *                LOADING/ENABLED 状态、pkey 描述符、build_lock)。没有
 *                rel_gen 字段;DDL 失效直接在 build_lock 下把 state 置为
 *                DISABLED。
 *
 *   并发模型:
 *     - 128 个 LWLock 分区,每组桶一个。
 *     - 读路径:在桶的分区锁上取 LW_SHARED,走链,拷贝载荷,释放。同一
 *       分区上任意键的并发读者完全并行(SHARED 不阻塞 SHARED)。
 *     - 写路径(Load / Drop / DML 失效):在桶的分区锁上取 LW_EXCLUSIVE,
 *       摘链,同步 dsa_free 载荷与 entry,释放。EX 会等所有在途 SHARED
 *       读者排空,所以 dsa_free 安全。
 *     - Load / Drop 通过 per-RelMeta 的 build_lock(LWLock,非分区锁)
 *       彼此串行化。
 *
 *   Todo:
 *     - INSERT 钩子(对新行没有 LRU/填充策略)
 *     - LRU 驱逐
 *     
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
 *     8M * sizeof(dsa_pointer) = 8M * 8 B = 预留 64 MB DSA。
 *   它在首次 Load 时于 DSA 内惰性分配;从不碰缓存的 backend 不付任何成本。
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
} RelMeta;

typedef struct GlobalEntry
{
	Oid				relid;
	uint32			pkey_hash;
	uint8			pkey_len;		/* 序列化长度, <= ROW_CACHE_PKEY_INLINE_BYTES */
	uint8			pkey_buf[ROW_CACHE_PKEY_INLINE_BYTES];
	ItemPointerData	tid;			/* Load 时的 heap TID(填 slot 用) */
	dsa_pointer		payload_dp;		/* FlatCachedTuple(永远是块内内部指针) */
	dsa_pointer		next_dp;		/* 桶链上的下一个 GlobalEntry */
} GlobalEntry;

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
 * RowCacheControl:顶层 shmem 段。
 */
typedef struct RowCacheControl
{
	dsa_handle		global_dsa_handle;

	/*
	 * 桶头数组(长度 n_hash_buckets)。
	 */
	dsa_pointer		hash_buckets_dp;

	/*
	 * 桶数与掩码:由 GUC row_cache_hash_buckets 决定,在 RowCacheShmemInit
	 * 设一次、之后只读。bucket_mask = n_hash_buckets - 1(必为 2 的幂)。
	 */
	int				n_hash_buckets;
	uint32			bucket_mask;

	RowCacheGenPadded global_gen pg_attribute_aligned(PG_CACHE_LINE_SIZE);

	LWLock			control_lock;			/* 保护 DSA 初始化 */
	LWLock			relmeta_alloc_lock;		/* 保护 RelMeta 槽分配 */
	LWLock			partition_locks[ROW_CACHE_NUM_PARTITIONS];


	RelMeta			relmetas[ROW_CACHE_MAX_RELATIONS];
} RowCacheControl;

/*
 * FlatCachedTuple: 单次分配、扁平存储的缓存 tuple。
 *
 * 把每行的全部数据(values[]、isnull[]、HeapTupleHeader、以及按引用传递的
 * Datum 载荷)打包进一块由 dsa_allocate 分配的连续内存。取代了此前的
 * TidRowCacheEntry——后者把数据散在三次独立 palloc 里。
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

static dsa_pointer RowCacheFlattenTuple(dsa_area *area, TupleTableSlot *slot);
static bool RowCacheUnflattenToSlot(const FlatCachedTuple *flat,
									Oid relid,
									ItemPointer tid,
									TupleTableSlot *slot);

static RowCacheControl *RowCacheCtl = NULL;
static dsa_area *LocalDsa = NULL;

/*
 * 桶头数组的进程局部地址的 backend 本地缓存。
 *
 * 桶头数组(dsa_pointer[n_hash_buckets])在 shmem 初始化时分配一次，
 * dsa_get_address 在本 backend 的 DSA 附着生命周期内返回稳定地址。解析
 * 一次后复用,省掉每次缓存探测的一次 dsa_get_address(段映射查找 + 加法)。
 * 在 EnsureRowCacheDsa 里每次(重新)设置 LocalDsa 时把它重置为 NULL。
 */
static dsa_pointer *LocalBucketHeads = NULL;

static Oid			LastLookupRelid = InvalidOid;
static RelMeta	   *LastLookupRelMeta = NULL;

/* rowcache_relcache_callback 需要的前向声明。 */
static void EnsureRowCacheDsa(void);
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
	RelMeta	   *rm;

	if (RowCacheCtl == NULL)
		return;
	if (!OidIsValid(relid))
		return;					

	// rm = FindRelMeta(relid);
	// if (rm == NULL)
	// 	return;

	// pg_atomic_write_u32(&rm->state, RELMETA_DISABLED);

	if (LastLookupRelid == relid)
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
static void DropAllEntriesForRelid(Oid relid);

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

Size
RowCacheShmemSize(void)
{
	return MAXALIGN(sizeof(RowCacheControl));
}

void
RowCacheShmemInit(void)
{
	bool		found;

	RowCacheCtl = (RowCacheControl *)
		ShmemInitStruct("Row Cache Control V4",
						sizeof(RowCacheControl),
						&found);

	if (found)
		return;

	RowCacheCtl->global_dsa_handle = DSA_HANDLE_INVALID;
	RowCacheCtl->hash_buckets_dp = InvalidDsaPointer;
	RowCacheCtl->n_hash_buckets = row_cache_hash_buckets;
	RowCacheCtl->bucket_mask = (uint32) row_cache_hash_buckets - 1;
	pg_atomic_init_u32(&RowCacheCtl->global_gen.value, 0);

	LWLockInitialize(&RowCacheCtl->control_lock, LWTRANCHE_ROW_CACHE_CTL);
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
	}
}

/* ----------------------------------------------------------------
 * DSA 惰性初始化。桶头数组在任何 backend 首次碰缓存时、在 control_lock
 * 下于 DSA 里分配。
 * ---------------------------------------------------------------- */

static void
EnsureRowCacheDsa(void)
{
	MemoryContext old_ctx;

	if (LocalDsa != NULL)
		return;

	old_ctx = MemoryContextSwitchTo(TopMemoryContext);

	LWLockAcquire(&RowCacheCtl->control_lock, LW_EXCLUSIVE);

	if (RowCacheCtl->global_dsa_handle == DSA_HANDLE_INVALID)
	{
		dsa_area   *dsa = dsa_create(LWTRANCHE_ROW_CACHE_DSA);
		dsa_pointer dp;
		dsa_pointer *buckets;

		dsa_pin(dsa);
		dsa_pin_mapping(dsa);

		dp = dsa_allocate0(dsa,
						   sizeof(dsa_pointer) * RowCacheCtl->n_hash_buckets);
		if (!DsaPointerIsValid(dp))
		{
			LWLockRelease(&RowCacheCtl->control_lock);
			MemoryContextSwitchTo(old_ctx);
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory"),
					 errdetail_internal("row cache: cannot allocate hash buckets")));
		}
		buckets = (dsa_pointer *) dsa_get_address(dsa, dp);
		for (int i = 0; i < RowCacheCtl->n_hash_buckets; i++)
			buckets[i] = InvalidDsaPointer;

		RowCacheCtl->global_dsa_handle = dsa_get_handle(dsa);
		RowCacheCtl->hash_buckets_dp = dp;
		LocalDsa = dsa;
		LocalBucketHeads = NULL;	
	}
	else
	{
		LocalDsa = dsa_attach(RowCacheCtl->global_dsa_handle);
		dsa_pin_mapping(LocalDsa);
		LocalBucketHeads = NULL;	
	}

	LWLockRelease(&RowCacheCtl->control_lock);

	MemoryContextSwitchTo(old_ctx);

	/*
	 * relcache 失效回调,使本 backend 能观察到对缓存关系的 DDL。
	 */
	RowCacheRegisterRelcacheCallback();
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

static inline dsa_pointer *
BucketHeads(void)
{
	if (likely(LocalBucketHeads != NULL))
		return LocalBucketHeads;

	LocalBucketHeads = (dsa_pointer *) dsa_get_address(LocalDsa,
												   RowCacheCtl->hash_buckets_dp);
	return LocalBucketHeads;
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
BucketInsertHead(uint32 bucket, dsa_pointer entry_dp, GlobalEntry *entry)
{
	dsa_pointer *heads = BucketHeads();

	entry->next_dp = heads[bucket];
	pg_write_barrier();
	heads[bucket] = entry_dp;
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
			 const uint8 *pkey_buf, int pkey_len,
			 dsa_pointer *out_entry_dp)
{
	dsa_pointer *heads = BucketHeads();
	dsa_pointer cur_dp = heads[bucket];

	while (DsaPointerIsValid(cur_dp))
	{
		GlobalEntry *e = (GlobalEntry *) dsa_get_address(LocalDsa, cur_dp);

		if (e->relid == relid &&
			e->pkey_hash == pkey_hash &&
			e->pkey_len == pkey_len &&
			memcmp(e->pkey_buf, pkey_buf, pkey_len) == 0)
		{
			if (out_entry_dp)
				*out_entry_dp = cur_dp;
			return e;
		}
		cur_dp = e->next_dp;
	}
	return NULL;
}

/* ----------------------------------------------------------------
 * FlatCachedTuple:行的连续物化存储格式
 * ---------------------------------------------------------------- */

dsa_pointer
RowCacheFlattenTuple(dsa_area *area, TupleTableSlot *slot)
{
	 TupleDesc	desc = slot->tts_tupleDescriptor;
	 int			natts = desc->natts;
	 HeapTuple	htup;
	 Size		total_size;
	 uint32		values_offset,
				 isnull_offset,
				 htup_offset,
				 varlen_offset;
	 FlatCachedTuple *ft;
	 dsa_pointer dp;
	 Datum	   *dst_values;
	 bool	   *dst_isnull;
	 char	   *varlen_cursor;
 
	 slot_getallattrs(slot);
	 htup = ExecCopySlotHeapTuple(slot);
 
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
 
	 dp = dsa_allocate(area, total_size);
	 ft = (FlatCachedTuple *) dsa_get_address(area, dp);
	 memset(ft, 0, total_size);
 
	 ft->total_size = total_size;
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
			 /* Store offset from ft base, not a process-local pointer */
			 dst_values[i] = (Datum) (varlen_cursor - (char *) ft);
			 varlen_cursor += MAXALIGN(datum_size);
		 }
	 }
 
	 memcpy((char *) ft + htup_offset, htup->t_data, htup->t_len);
 
	 heap_freetuple(htup);
	 return dp;
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

	EnsureRowCacheDsa();

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
		DropAllEntriesForRelid(relid);
	}

	pg_atomic_write_u32(&rm->state, RELMETA_LOADING);

	if (!CheckEligiblePkey(rel, rm))
	{
		pg_atomic_write_u32(&rm->state, RELMETA_DISABLED);
		LastLookupRelMeta = NULL;
		LastLookupRelid = InvalidOid;
		LWLockRelease(&rm->build_lock);
		return;
	}

	if (!ActiveSnapshotSet())
	{
		PushActiveSnapshot(GetTransactionSnapshot());
		pushed_snapshot = true;
	}

	scan = table_beginscan(rel, GetActiveSnapshot(), 0, NULL);
	slot = table_slot_create(rel, NULL);

	/*
	 * 中途若 dsa_allocate OOM(或其他 ereport),在 PG_CATCH 里清理，scan / slot / snapshot 是事务资源,由事务
	 * abort 自动回收,PG_CATCH 只需清理行缓存的 shmem 状态。
	 */
	PG_TRY();
	{
		while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
		{
			uint8		pkey_buf[ROW_CACHE_PKEY_INLINE_BYTES];
			int			pkey_len;
			uint32		pkey_hash;
			uint32		bucket;
			dsa_pointer payload_dp;
			dsa_pointer entry_dp;
			GlobalEntry *e;
			LWLock	   *part;

			slot_getallattrs(slot);

			pkey_len = SerializePkeyFromSlot(slot, rm, pkey_buf);
			if (pkey_len < 0)
				continue;				

			pkey_hash = ComputePkeyHashBytes(pkey_buf, pkey_len);
			bucket = pkey_hash & RowCacheCtl->bucket_mask;

			{
				dsa_pointer	tmp_dp = RowCacheFlattenTuple(LocalDsa, slot);
				FlatCachedTuple *tmp_flat;
				Size		flat_size;
				Size		combined_size;
				char	   *inline_flat;

				if (!DsaPointerIsValid(tmp_dp))
					continue;

				tmp_flat = (FlatCachedTuple *) dsa_get_address(LocalDsa, tmp_dp);
				flat_size = tmp_flat->total_size;
				combined_size = MAXALIGN(sizeof(GlobalEntry)) + flat_size;

				entry_dp = dsa_allocate_extended(LocalDsa, combined_size,
													DSA_ALLOC_NO_OOM);
				if (!DsaPointerIsValid(entry_dp))
				{
					dsa_free(LocalDsa, tmp_dp);
					ereport(ERROR,
							(errcode(ERRCODE_OUT_OF_MEMORY),
								errmsg("out of memory"),
								errdetail_internal("row cache: cannot allocate GlobalEntry")));
				}

				e = (GlobalEntry *) dsa_get_address(LocalDsa, entry_dp);
				inline_flat = (char *) e + MAXALIGN(sizeof(GlobalEntry));
				memcpy(inline_flat, tmp_flat, flat_size);
				dsa_free(LocalDsa, tmp_dp);

				payload_dp = entry_dp + MAXALIGN(sizeof(GlobalEntry));
			}

			e->relid = relid;
			e->pkey_hash = pkey_hash;
			e->pkey_len = (uint8) pkey_len;
			memcpy(e->pkey_buf, pkey_buf, pkey_len);
			ItemPointerCopy(&slot->tts_tid, &e->tid);
			e->payload_dp = payload_dp;
			e->next_dp = InvalidDsaPointer;

			part = PartitionLockForBucket(bucket);
			LWLockAcquire(part, LW_EXCLUSIVE);
			BucketInsertHead(bucket, entry_dp, e);
			LWLockRelease(part);
	}
	}
	PG_CATCH();
	{
		/*
		 * 加载中途失败(通常是 DSA OOM):清掉已插入的部分 entry、把 state
		 * 回滚到 DISABLED、释放 RelMeta 槽位、推进代数,然后重新抛出。
		 * 此处只持有 build_lock(OOM 发生在分区锁之外的 dsa_allocate),
		 * DropAllEntriesForRelid 自取/放分区锁,dsa_free 在 OOM 后仍可用。
		 */
		// pg_atomic_write_u32(&rm->state, RELMETA_DISABLED);
		pg_memory_barrier();
		DropAllEntriesForRelid(relid);
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

	pg_write_barrier();
	pg_atomic_write_u32(&rm->state, RELMETA_ENABLED);

	LastLookupRelMeta = NULL;
	LastLookupRelid = InvalidOid;

	LWLockRelease(&rm->build_lock);
	pg_atomic_fetch_add_u32(&RowCacheCtl->global_gen.value, 1);

	RelationRowCacheBindRelation(rel);
}

static void
DropAllEntriesForRelid(Oid relid)
{
	dsa_pointer *heads;

	if (!DsaPointerIsValid(RowCacheCtl->hash_buckets_dp))
		return;

	heads = BucketHeads();

	for (uint32 b = 0; b < RowCacheCtl->n_hash_buckets; b++)
	{
		LWLock	   *part = PartitionLockForBucket(b);
		dsa_pointer prev_dp = InvalidDsaPointer;
		dsa_pointer cur_dp;
		GlobalEntry *prev = NULL;

		CHECK_FOR_INTERRUPTS();

		LWLockAcquire(part, LW_EXCLUSIVE);

		cur_dp = heads[b];
		while (DsaPointerIsValid(cur_dp))
		{
			GlobalEntry *e = (GlobalEntry *) dsa_get_address(LocalDsa, cur_dp);
			dsa_pointer next_dp = e->next_dp;

			if (e->relid == relid)
			{
				if (DsaPointerIsValid(prev_dp))
					prev->next_dp = next_dp;
				else
					heads[b] = next_dp;

				dsa_free(LocalDsa, cur_dp);

				cur_dp = next_dp;
			}
			else
			{
				prev_dp = cur_dp;
				prev = e;
				cur_dp = next_dp;
			}
		}

		LWLockRelease(part);
	}
}


static void
InvalidateEntryByPkeyBytes(Oid relid, const uint8 *pkey_buf, int pkey_len)
{
	uint32			pkey_hash;
	uint32			bucket;
	LWLock		   *part;
	dsa_pointer	   *heads;
	dsa_pointer		prev_dp;
	dsa_pointer		cur_dp;
	GlobalEntry	   *prev;
	dsa_pointer		victim_dp = InvalidDsaPointer;

	/*
	 * backend 至少要跑过一次 EnsureRowCacheDsa,LocalDsa 才有效、
	 * BucketHeads 才能解析。DML 钩子路径可能是本 backend 第一次碰
	 * 缓存,这里惰性 attach。
	 */
	EnsureRowCacheDsa();

	pkey_hash = ComputePkeyHashBytes(pkey_buf, pkey_len);
	bucket = pkey_hash & RowCacheCtl->bucket_mask;
	part = PartitionLockForBucket(bucket);

	LWLockAcquire(part, LW_EXCLUSIVE);

	if (!DsaPointerIsValid(RowCacheCtl->hash_buckets_dp))
	{
		LWLockRelease(part);
		return;
	}

	heads = BucketHeads();
	prev_dp = InvalidDsaPointer;
	prev = NULL;
	cur_dp = heads[bucket];

	while (DsaPointerIsValid(cur_dp))
	{
		GlobalEntry *e = (GlobalEntry *) dsa_get_address(LocalDsa, cur_dp);

		if (e->relid == relid &&
			e->pkey_hash == pkey_hash &&
			e->pkey_len == pkey_len &&
			memcmp(e->pkey_buf, pkey_buf, pkey_len) == 0)
		{
			/* 从链上摘除。 */
			if (DsaPointerIsValid(prev_dp))
				prev->next_dp = e->next_dp;
			else
				heads[bucket] = e->next_dp;

			victim_dp = cur_dp;

			break;
		}

		prev_dp = cur_dp;
		prev = e;
		cur_dp = e->next_dp;
	}

	LWLockRelease(part);

	if (DsaPointerIsValid(victim_dp))
		dsa_free(LocalDsa, victim_dp);
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

	EnsureRowCacheDsa();

	LWLockAcquire(&rm->build_lock, LW_EXCLUSIVE);


	pg_atomic_write_u32(&rm->state, RELMETA_DISABLED);
	pg_memory_barrier();

	DropAllEntriesForRelid(relid);

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
	bool			result = false;

	if (pg_atomic_read_u32(&rm->state) != RELMETA_ENABLED)
		return false;
	pg_read_barrier();

	EnsureRowCacheDsa();

	hash = ComputePkeyHashBytes(pkey_buf, pkey_len);
	bucket = hash & RowCacheCtl->bucket_mask;
	part = PartitionLockForBucket(bucket);

	LWLockAcquire(part, LW_SHARED);

	entry = BucketLookup(bucket, relid, hash, pkey_buf, pkey_len, NULL);
	if (entry == NULL)
		goto out;

	pg_read_barrier();

	if (!DsaPointerIsValid(entry->payload_dp))
		goto out;
	flat = (FlatCachedTuple *) dsa_get_address(LocalDsa, entry->payload_dp);

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
		RowCacheUnflattenToSlot(flat, relid, &tid, slot);
	}

	result = true;

out:
	LWLockRelease(part);
	return result;
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
