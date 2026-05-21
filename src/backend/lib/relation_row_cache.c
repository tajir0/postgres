/*-------------------------------------------------------------------------
 *
 * relation_row_cache.c
 *	  V4 Phase 1 implementation of the shared row cache.
 *
 * Architecture (see V4设计文档 sections 2-5):
 *
 *   GlobalCache: a single DSA-resident chained hash table keyed by
 *                (relid, pkey).  Bucket heads live in a flat
 *                dsa_pointer[ROW_CACHE_HASH_BUCKETS] array; each chain
 *                element is a GlobalEntry holding the pkey, hash, payload
 *                pointer and a `next_dp` to the next entry in the bucket.
 *
 *   RelMeta[64]: fixed shmem array indexing per-relation metadata
 *                (state machine + pkey descriptor + rel_gen + build_lock).
 *
 *   Concurrency:
 *     - Write side (Load / Drop) takes the per-relation build_lock and
 *       the per-bucket partition lock as needed.
 *     - Read side takes NO locks; it relies on the V3 caller contract
 *       (no concurrent Load/Drop with readers).  Phase 2 will introduce
 *       EBR; until then, callers must serialize externally.
 *
 *   Phase 1 limits: single-column byval pk (int2/int4/int8/oid).  Tables
 *   with composite or pass-by-reference pks are silently skipped by
 *   RelationRowCacheLoadRelation (no error, no entries created).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/relation.h"
#include "access/tableam.h"
#include "catalog/pg_index.h"
#include "executor/tuptable.h"
#include "lib/relation_row_cache.h"
#include "lib/tid_row_cache.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/bufmgr.h"
#include "storage/itemptr.h"
#include "storage/lockdefs.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "storage/shmem.h"
#include "utils/dsa.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/snapmgr.h"

/* ----------------------------------------------------------------
 * Constants
 * ---------------------------------------------------------------- */

#define ROW_CACHE_MAX_RELATIONS	64
#define ROW_CACHE_NUM_PARTITIONS	128
#define ROW_CACHE_HASH_BUCKETS	8192	/* must be a power of two */
#define ROW_CACHE_BUCKET_MASK	(ROW_CACHE_HASH_BUCKETS - 1)

StaticAssertDecl((ROW_CACHE_HASH_BUCKETS & ROW_CACHE_BUCKET_MASK) == 0,
				 "ROW_CACHE_HASH_BUCKETS must be a power of two");

/* RelMeta.state values. */
#define RELMETA_DISABLED	0
#define RELMETA_LOADING		1
#define RELMETA_ENABLED		2

/* GlobalEntry.state values.  Phase 1 only ever sets FRESH. */
#define ROW_CACHE_ENTRY_FRESH	0
#define ROW_CACHE_ENTRY_STALE	1
#define ROW_CACHE_ENTRY_DELETED	2

/* ----------------------------------------------------------------
 * Data structures
 * ---------------------------------------------------------------- */

/*
 * RelMeta: per-relation metadata, lives in shmem (fixed array).
 *
 * Lookup is a linear scan of up to ROW_CACHE_MAX_RELATIONS slots; a sticky
 * per-backend cache short-circuits repeated lookups for the same relid.
 */
typedef struct RelMeta
{
	Oid				relid;			/* InvalidOid = unused slot */
	pg_atomic_uint32 state;			/* RELMETA_{DISABLED,LOADING,ENABLED} */
	pg_atomic_uint64 rel_gen;		/* bumped on Drop / DDL invalidation */
	LWLock			build_lock;		/* serializes Load / Drop; not on read path */

	/* Pkey descriptor.  Phase 1: single-col byval only. */
	AttrNumber		pkey_attno;		/* 1-based heap attno of pkey column */
	int16			pkey_typlen;
	bool			pkey_byval;
} RelMeta;

/*
 * GlobalEntry: one element of the chained global hash.  Lives in DSA.
 *
 * For Phase 1 the pkey is always a single-col byval Datum which fits in
 * 64 bits; pkey_val stores it directly (no separate dsa_pointer needed).
 * Phase 4 will widen this when composite/byref support lands.
 */
typedef struct GlobalEntry
{
	Oid				relid;
	uint32			pkey_hash;
	uint64			pkey_val;		/* packed byval Datum */
	uint64			rel_gen_at_load;
	pg_atomic_uint32 state;			/* ROW_CACHE_ENTRY_{FRESH,STALE,DELETED} */
	ItemPointerData	tid;			/* heap TID at load time (for slot fill) */
	dsa_pointer		payload_dp;		/* FlatCachedTuple */
	dsa_pointer		next_dp;		/* next GlobalEntry in this bucket */
} GlobalEntry;

/*
 * RowCacheControl: top-level shmem segment.
 */
typedef struct RowCacheControl
{
	dsa_handle		global_dsa_handle;
	LWLock			control_lock;			/* protects DSA init */
	LWLock			relmeta_alloc_lock;		/* protects RelMeta slot allocation */
	LWLock			partition_locks[ROW_CACHE_NUM_PARTITIONS];

	/*
	 * Bucket-head array (length ROW_CACHE_HASH_BUCKETS) lives in DSA;
	 * pointer below is published after dsa_create.
	 */
	dsa_pointer		hash_buckets_dp;

	/*
	 * EBR (Epoch-Based Reclamation) infrastructure, Phase 2.
	 *
	 *   global_epoch         — monotonically increasing counter.  Writers
	 *                          bump it when they retire DSA blocks; readers
	 *                          snapshot it into PGPROC.rowcache_local_epoch
	 *                          on entering a read critical section.
	 *
	 *   safe_epoch_published — published by the GC worker after scanning
	 *                          ProcArray.  Any retire batch whose recorded
	 *                          epoch is strictly less than this value has
	 *                          no live reader left and can be dsa_free'd.
	 *
	 * Both are monotonic, plain uint64 atomics.  They never roll over in
	 * any realistic timescale (would take ~5800 millennia at 10k bumps/s).
	 *
	 * Phase 2 only wires the storage + initialization.  The GC bgworker
	 * (which publishes safe_epoch_published) and the retire-list machinery
	 * are added in subsequent Phase 2 commits.
	 */
	pg_atomic_uint64 global_epoch;
	pg_atomic_uint64 safe_epoch_published;

	RelMeta			relmetas[ROW_CACHE_MAX_RELATIONS];
} RowCacheControl;

/* ----------------------------------------------------------------
 * Globals
 * ---------------------------------------------------------------- */

static RowCacheControl *RowCacheCtl = NULL;
static dsa_area *LocalDsa = NULL;

/* ----------------------------------------------------------------
 * Phase 2: EBR accessor implementations.
 *
 * RowCacheEpochEnter / Exit (in the header) write the per-backend slot in
 * PGPROC; the helpers below operate on the shared counters.
 *
 * All counters are pg_atomic_uint64; reads / writes are single-instruction
 * on x86_64 / aarch64.  Bump uses fetch_add to advance the global counter
 * and returns the *previous* value (matching epoch semantics: writers
 * advance the global counter, but the value handed to a retire batch is
 * the one observed before retiring).
 *
 * ComputeSafeEpoch scans ProcArray under SHARED ProcArrayLock and returns
 * min(rowcache_local_epoch) over all non-idle slots.  If every backend is
 * idle (local_epoch == 0), there are no live readers and the current
 * global_epoch is itself safe.  Callers should publish the result via
 * RowCacheSafeEpochPublish so backend-local GC can consume it cheaply.
 * ---------------------------------------------------------------- */

uint64
RowCacheGlobalEpochRead(void)
{
	Assert(RowCacheCtl != NULL);
	return pg_atomic_read_u64(&RowCacheCtl->global_epoch);
}

uint64
RowCacheGlobalEpochBump(void)
{
	Assert(RowCacheCtl != NULL);
	return pg_atomic_fetch_add_u64(&RowCacheCtl->global_epoch, 1);
}

uint64
RowCacheSafeEpochRead(void)
{
	Assert(RowCacheCtl != NULL);
	return pg_atomic_read_u64(&RowCacheCtl->safe_epoch_published);
}

void
RowCacheSafeEpochPublish(uint64 safe)
{
	Assert(RowCacheCtl != NULL);
	pg_atomic_write_u64(&RowCacheCtl->safe_epoch_published, safe);
}

uint64
RowCacheComputeSafeEpoch(void)
{
	uint64		oldest = UINT64_MAX;

	Assert(RowCacheCtl != NULL);

	LWLockAcquire(ProcArrayLock, LW_SHARED);
	for (int i = 0; i < ProcGlobal->allProcCount; i++)
	{
		uint64	le = pg_atomic_read_u64(&ProcGlobal->allProcs[i].rowcache_local_epoch);

		if (le > 0 && le < oldest)
			oldest = le;
	}
	LWLockRelease(ProcArrayLock);

	return (oldest == UINT64_MAX)
		? pg_atomic_read_u64(&RowCacheCtl->global_epoch)
		: oldest;
}

/* ----------------------------------------------------------------
 * Phase 2 (2/4): Backend-local retire list
 *
 * Writers (DML hooks, Drop) do not call dsa_free directly.  Instead they
 * call RowCacheEpochRetire with up to three DSA pointers (payload, entry,
 * pkey buffer), which pushes a RetireNode onto a backend-local list along
 * with a snapshot of global_epoch.
 *
 * Backend-local reclaim (RowCacheLocalGC) consumes safe_epoch_published
 * (set by the GC bgworker — future commit) and dsa_free's every node
 * whose recorded epoch is strictly less than safe_epoch.  This decouples
 * write throughput from GC latency: writers never block waiting for
 * readers to drain.
 *
 * Memory:
 *   - RetireNodes are palloc'd in TopMemoryContext so they survive
 *     across statements / transactions until LocalGC reclaims them.
 *   - dp[0..2] hold up to three DSA pointers per node.  Unused slots
 *     are InvalidDsaPointer and skipped at free time.  This 3-slot
 *     layout matches the common case (DELETE of a single entry retires
 *     payload + entry + pkey buffer in one node).
 *
 * Epoch bumping:
 *   We bump the global epoch opportunistically every RETIRE_BUMP_THRESHOLD
 *   retires.  Bumping too aggressively contends fetch_add on the shared
 *   counter; bumping too lazily lets retire lists grow before any GC can
 *   make progress.  64 strikes a reasonable balance for OLTP workloads.
 *
 * Backend exit:
 *   On crash / disconnect the retire list is leaked along with the
 *   backend's palloc memory; the underlying DSA blocks are likewise
 *   leaked.  Phase 2 (3/4) will install before_shmem_exit cleanup to
 *   move the list to a global orphan list reclaimable by the GC worker.
 * ---------------------------------------------------------------- */

#define RETIRE_BUMP_THRESHOLD	64
#define RETIRE_NODE_SLOTS		3

typedef struct RetireNode
{
	uint64				epoch;
	dsa_pointer			dp[RETIRE_NODE_SLOTS];
	struct RetireNode  *next;
} RetireNode;

static RetireNode  *MyRetireHead = NULL;
static size_t		MyRetireListLen = 0;

/* Forward-declared here so RowCacheLocalGC can call it before the main
 * forward-declaration block (which lives further down with the rest of
 * the V4 Phase 1 helpers). */
static void EnsureRowCacheDsa(void);

void
RowCacheEpochRetire(dsa_pointer dp_a, dsa_pointer dp_b, dsa_pointer dp_c)
{
	RetireNode	   *n;
	MemoryContext	old;

	Assert(RowCacheCtl != NULL);

	/* Nothing to retire — skip allocation. */
	if (!DsaPointerIsValid(dp_a) &&
		!DsaPointerIsValid(dp_b) &&
		!DsaPointerIsValid(dp_c))
		return;

	old = MemoryContextSwitchTo(TopMemoryContext);
	n = (RetireNode *) palloc(sizeof(RetireNode));
	MemoryContextSwitchTo(old);

	n->epoch = pg_atomic_read_u64(&RowCacheCtl->global_epoch);
	n->dp[0] = dp_a;
	n->dp[1] = dp_b;
	n->dp[2] = dp_c;
	n->next = MyRetireHead;
	MyRetireHead = n;
	MyRetireListLen++;

	/*
	 * Opportunistic global_epoch bump.  Not strictly required (the GC
	 * worker bumps on its own schedule) but lets a hot writer drive the
	 * counter forward so its own retires can be reclaimed sooner.
	 *
	 * fetch_add returns the prior value; we discard it.
	 */
	if (MyRetireListLen % RETIRE_BUMP_THRESHOLD == 0)
		(void) pg_atomic_fetch_add_u64(&RowCacheCtl->global_epoch, 1);
}

/*
 * RowCacheLocalGC: free every RetireNode in this backend's list whose
 * recorded epoch is strictly less than the published safe_epoch.
 *
 * Should be called at convenient quiescent points (CommitTransaction
 * tail, CHECK_FOR_INTERRUPTS, idle).  Phase 2 (4/4) wires the actual
 * call sites; this commit only provides the function.
 *
 * Cheap when no work to do: one atomic_load + one list-head check.
 */
void
RowCacheLocalGC(void)
{
	uint64			safe;
	RetireNode	  **pp;
	RetireNode	   *node;

	if (MyRetireHead == NULL || RowCacheCtl == NULL)
		return;

	/*
	 * dsa_free needs LocalDsa.  This backend must already be attached
	 * (otherwise it could not have called EpochRetire), but EnsureRowCacheDsa
	 * is idempotent and cheap on the attached-fast-path.
	 */
	EnsureRowCacheDsa();

	safe = pg_atomic_read_u64(&RowCacheCtl->safe_epoch_published);

	/*
	 * Walk the singly-linked list with a back-pointer so we can splice
	 * out reclaimable nodes in O(1) per visit without a separate pass.
	 */
	pp = &MyRetireHead;
	while ((node = *pp) != NULL)
	{
		if (node->epoch < safe)
		{
			*pp = node->next;
			for (int i = 0; i < RETIRE_NODE_SLOTS; i++)
			{
				if (DsaPointerIsValid(node->dp[i]))
					dsa_free(LocalDsa, node->dp[i]);
			}
			pfree(node);
			Assert(MyRetireListLen > 0);
			MyRetireListLen--;
		}
		else
		{
			pp = &node->next;
		}
	}
}

/* Diagnostics: current length of this backend's retire list. */
size_t
RowCacheLocalRetireCount(void)
{
	return MyRetireListLen;
}

/*
 * Sticky per-backend cache for relmeta lookup.  Invalidated by Load/Drop
 * on the same backend (LastCachedRelMeta=NULL) so a subsequent call
 * re-walks the array.  Cross-backend Load/Drop is not invalidated here;
 * the read path re-checks relmeta->state and rel_gen on every fetch, so
 * a stale sticky pointer still produces a safe "miss" outcome.
 */
static Oid			LastLookupRelid = InvalidOid;
static RelMeta	   *LastLookupRelMeta = NULL;

/* ----------------------------------------------------------------
 * Forward declarations
 * ---------------------------------------------------------------- */

static void EnsureRowCacheDsa(void);
static RelMeta *FindRelMeta(Oid relid);
static RelMeta *AllocateOrFindRelMeta(Oid relid);
static AttrNumber CheckEligibleByvalPkey(Relation rel,
										 int16 *out_typlen,
										 bool *out_byval);
static inline uint64 PackPkeyDatum(Datum d, int16 typlen);
static inline uint32 ComputePkeyHash(uint64 pkey_val);
static void DropAllEntriesForRelid(Oid relid);

/* ----------------------------------------------------------------
 * Shared-memory sizing and init
 * ---------------------------------------------------------------- */

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

	/*
	 * EBR counters start at 1 so that 0 in PGPROC.rowcache_local_epoch
	 * unambiguously means "not in a critical section".  Any value >= 1 is
	 * a real snapshot of global_epoch taken by some reader.
	 */
	pg_atomic_init_u64(&RowCacheCtl->global_epoch, 1);
	pg_atomic_init_u64(&RowCacheCtl->safe_epoch_published, 0);

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
		pg_atomic_init_u64(&rm->rel_gen, 0);
		LWLockInitialize(&rm->build_lock, LWTRANCHE_ROW_CACHE_RELMETA);
		rm->pkey_attno = 0;
		rm->pkey_typlen = 0;
		rm->pkey_byval = false;
	}
}

/* ----------------------------------------------------------------
 * DSA lazy init.  The bucket-head array is allocated in DSA the first
 * time any backend touches the cache, under the control_lock.
 * ---------------------------------------------------------------- */

static void
EnsureRowCacheDsa(void)
{
	MemoryContext old_ctx;

	if (LocalDsa != NULL)
		return;

	/*
	 * dsa_create / dsa_attach palloc the per-backend dsa_area handle in
	 * CurrentMemoryContext; switch to TopMemoryContext so it survives the
	 * statement that triggered initialisation.
	 */
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
						   sizeof(dsa_pointer) * ROW_CACHE_HASH_BUCKETS);
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
		for (int i = 0; i < ROW_CACHE_HASH_BUCKETS; i++)
			buckets[i] = InvalidDsaPointer;

		RowCacheCtl->global_dsa_handle = dsa_get_handle(dsa);
		RowCacheCtl->hash_buckets_dp = dp;
		LocalDsa = dsa;
	}
	else
	{
		LocalDsa = dsa_attach(RowCacheCtl->global_dsa_handle);
		dsa_pin_mapping(LocalDsa);
	}

	LWLockRelease(&RowCacheCtl->control_lock);

	MemoryContextSwitchTo(old_ctx);
}

/* ----------------------------------------------------------------
 * RelMeta lookup and slot allocation
 * ---------------------------------------------------------------- */

/*
 * Linear scan of the RelMeta array for a slot whose relid matches.
 * Returns NULL if not found.  Caller must NOT rely on the slot staying
 * registered: a concurrent Drop can flip state to DISABLED, but the slot
 * (and its embedded build_lock) is never deallocated.
 */
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

/*
 * Find an existing slot for `relid` or allocate a fresh one.  Returns
 * NULL only if the array is full.  Slot allocation is serialised under
 * relmeta_alloc_lock so two backends don't both grab the same empty
 * slot for different relids.
 */
static RelMeta *
AllocateOrFindRelMeta(Oid relid)
{
	RelMeta    *rm;

	rm = FindRelMeta(relid);
	if (rm != NULL)
		return rm;

	LWLockAcquire(&RowCacheCtl->relmeta_alloc_lock, LW_EXCLUSIVE);

	/* Re-check under the lock. */
	for (int i = 0; i < ROW_CACHE_MAX_RELATIONS; i++)
	{
		if (RowCacheCtl->relmetas[i].relid == relid)
		{
			rm = &RowCacheCtl->relmetas[i];
			LWLockRelease(&RowCacheCtl->relmeta_alloc_lock);
			return rm;
		}
	}

	/* Find an unused slot. */
	for (int i = 0; i < ROW_CACHE_MAX_RELATIONS; i++)
	{
		RelMeta    *cand = &RowCacheCtl->relmetas[i];

		if (cand->relid == InvalidOid)
		{
			cand->relid = relid;
			pg_atomic_write_u32(&cand->state, RELMETA_DISABLED);
			cand->pkey_attno = 0;
			cand->pkey_typlen = 0;
			cand->pkey_byval = false;
			rm = cand;
			break;
		}
	}

	LWLockRelease(&RowCacheCtl->relmeta_alloc_lock);
	return rm;
}

/* ----------------------------------------------------------------
 * Pkey eligibility / serialization (Phase 1: single-col byval only)
 * ---------------------------------------------------------------- */

/*
 * Return the 1-based heap attno of the relation's pkey column iff the
 * pkey is single-column and pass-by-value (int2/int4/int8/oid).
 * Returns 0 otherwise (composite, byref, deferrable, no pk, etc.).
 */
static AttrNumber
CheckEligibleByvalPkey(Relation rel, int16 *out_typlen, bool *out_byval)
{
	Oid			pkindex_oid;
	Relation	pkindex;
	Form_pg_index ind;
	Form_pg_attribute attr;
	AttrNumber	attno = 0;

	*out_typlen = 0;
	*out_byval = false;

	pkindex_oid = RelationGetPrimaryKeyIndex(rel, false);
	if (!OidIsValid(pkindex_oid))
		return 0;

	pkindex = index_open(pkindex_oid, AccessShareLock);
	ind = pkindex->rd_index;

	if (ind == NULL || ind->indnkeyatts != 1)
	{
		index_close(pkindex, AccessShareLock);
		return 0;
	}

	attno = ind->indkey.values[0];
	index_close(pkindex, AccessShareLock);

	if (attno <= 0)
		return 0;

	attr = TupleDescAttr(RelationGetDescr(rel), attno - 1);
	if (!attr->attbyval || attr->attlen <= 0 ||
		attr->attlen > (int) sizeof(uint64))
		return 0;

	*out_typlen = attr->attlen;
	*out_byval = true;
	return attno;
}

/*
 * Pack a byval pkey Datum into a 64-bit canonical representation.
 *
 * For typlen <= 8 the Datum is already zero-extended to 64 bits per the
 * PG byval convention, but we mask to typlen-relevant bits so that, e.g.,
 * a high-bit-set int4 with junk in the upper 32 bits compares equal to
 * the canonical zero-extended form.
 */
static inline uint64
PackPkeyDatum(Datum d, int16 typlen)
{
	uint64		v = (uint64) d;

	switch (typlen)
	{
		case 1:
			return v & UINT64CONST(0xff);
		case 2:
			return v & UINT64CONST(0xffff);
		case 4:
			return v & UINT64CONST(0xffffffff);
		case 8:
		default:
			return v;
	}
}

/*
 * Splitmix64-style finalizer; lifted from the V3 pkey_row_cache.c.  We
 * keep it as a 32-bit return because that's all the bucket index needs.
 */
static inline uint32
ComputePkeyHash(uint64 pkey_val)
{
	uint64		x = pkey_val;

	x ^= x >> 30;
	x *= UINT64CONST(0xbf58476d1ce4e5b9);
	x ^= x >> 27;
	x *= UINT64CONST(0x94d49bb133111eb1);
	x ^= x >> 31;
	return (uint32) x;
}

static inline LWLock *
PartitionLockForBucket(uint32 bucket)
{
	return &RowCacheCtl->partition_locks[bucket % ROW_CACHE_NUM_PARTITIONS];
}

static inline dsa_pointer *
BucketHeads(void)
{
	return (dsa_pointer *) dsa_get_address(LocalDsa,
										   RowCacheCtl->hash_buckets_dp);
}

/* ----------------------------------------------------------------
 * MVCC visibility check (lifted from V3)
 * ---------------------------------------------------------------- */

static bool
RowCacheTupleVisibleMVCC(HeapTuple tuple, Snapshot snapshot)
{
	uint16		infomask;
	HeapTupleHeaderData *thdr;

	if (!IsMVCCSnapshot(snapshot))
		return false;

	thdr = tuple->t_data;
	infomask = thdr->t_infomask;

	if (HeapTupleHeaderXminInvalid(thdr))
		return false;
	if (!HeapTupleHeaderXminCommitted(thdr))
		return false;

	if (infomask & HEAP_XMAX_INVALID)
		return true;
	if (HEAP_XMAX_IS_LOCKED_ONLY(infomask))
		return true;
	if (infomask & HEAP_XMAX_COMMITTED)
		return false;

	return false;
}

/* ----------------------------------------------------------------
 * Bucket-chain helpers
 * ---------------------------------------------------------------- */

/*
 * Insert a fully-initialised GlobalEntry at the head of its bucket.
 * Caller must hold the bucket's partition lock EXCLUSIVE.
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
 * Walk a bucket chain looking for an entry matching (relid, pkey_val).
 * Returns the entry pointer (via dsa_get_address) and stores the
 * dsa_pointer in *out_entry_dp when found; otherwise returns NULL.
 *
 * Read path: NO LOCK (per Phase 1 caller contract).
 * Write path: caller holds the partition lock.
 */
static GlobalEntry *
BucketLookup(uint32 bucket, Oid relid, uint64 pkey_val,
			 dsa_pointer *out_entry_dp)
{
	dsa_pointer *heads = BucketHeads();
	dsa_pointer cur_dp = heads[bucket];

	while (DsaPointerIsValid(cur_dp))
	{
		GlobalEntry *e = (GlobalEntry *) dsa_get_address(LocalDsa, cur_dp);

		if (e->relid == relid && e->pkey_val == pkey_val)
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
 * Public API: Load
 * ----------------------------------------------------------------
 *
 * 1. Find / allocate RelMeta slot.
 * 2. build_lock EXCLUSIVE.
 * 3. If already ENABLED, drop first (idempotent).
 * 4. State -> LOADING.
 * 5. Eligibility check; bail out (and leave state DISABLED) if pk is not
 *    a supported shape.
 * 6. Heap scan; for each tuple, allocate GlobalEntry + flatten payload,
 *    take partition lock, push at bucket head.
 * 7. write_barrier; state -> ENABLED.
 * 8. Release build_lock.
 */
void
RelationRowCacheLoadRelation(Relation rel)
{
	Oid			relid = RelationGetRelid(rel);
	RelMeta    *rm;
	AttrNumber	pkey_attno;
	int16		pkey_typlen = 0;
	bool		pkey_byval = false;
	TableScanDesc scan;
	TupleTableSlot *slot;
	bool		pushed_snapshot = false;
	uint64		rel_gen_at_load;

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

	/*
	 * Idempotent reload: if already ENABLED, drop the previous contents
	 * first.  This keeps Load(rel) safe to call twice without leaving
	 * duplicate entries in the global hash.
	 */
	if (pg_atomic_read_u32(&rm->state) == RELMETA_ENABLED)
	{
		pg_atomic_write_u32(&rm->state, RELMETA_DISABLED);
		pg_atomic_fetch_add_u64(&rm->rel_gen, 1);
		pg_memory_barrier();
		DropAllEntriesForRelid(relid);
	}

	pg_atomic_write_u32(&rm->state, RELMETA_LOADING);

	pkey_attno = CheckEligibleByvalPkey(rel, &pkey_typlen, &pkey_byval);
	if (pkey_attno == 0)
	{
		/*
		 * Phase 1 limitation: composite / byref / no-pk tables are not
		 * cacheable.  Leave the slot in DISABLED state and reset the
		 * descriptor so a future Phase 4 reload finds a clean slate.
		 */
		rm->pkey_attno = 0;
		rm->pkey_typlen = 0;
		rm->pkey_byval = false;
		pg_atomic_write_u32(&rm->state, RELMETA_DISABLED);
		LastLookupRelMeta = NULL;
		LastLookupRelid = InvalidOid;
		LWLockRelease(&rm->build_lock);
		return;
	}

	rm->pkey_attno = pkey_attno;
	rm->pkey_typlen = pkey_typlen;
	rm->pkey_byval = pkey_byval;
	rel_gen_at_load = pg_atomic_read_u64(&rm->rel_gen);

	if (!ActiveSnapshotSet())
	{
		PushActiveSnapshot(GetTransactionSnapshot());
		pushed_snapshot = true;
	}

	scan = table_beginscan(rel, GetActiveSnapshot(), 0, NULL);
	slot = table_slot_create(rel, NULL);

	while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		Datum		pkey_datum;
		uint64		pkey_packed;
		uint32		pkey_hash;
		uint32		bucket;
		dsa_pointer payload_dp;
		dsa_pointer entry_dp;
		GlobalEntry *e;
		LWLock	   *part;

		slot_getallattrs(slot);
		if (slot->tts_isnull[pkey_attno - 1])
			continue;				/* defensive; pk columns are NOT NULL */

		pkey_datum = slot->tts_values[pkey_attno - 1];
		pkey_packed = PackPkeyDatum(pkey_datum, pkey_typlen);
		pkey_hash = ComputePkeyHash(pkey_packed);
		bucket = pkey_hash & ROW_CACHE_BUCKET_MASK;

		/* Flatten and allocate before taking the partition lock. */
		payload_dp = RowCacheFlattenTuple(LocalDsa, slot);
		if (!DsaPointerIsValid(payload_dp))
			continue;

		entry_dp = dsa_allocate(LocalDsa, sizeof(GlobalEntry));
		if (!DsaPointerIsValid(entry_dp))
		{
			dsa_free(LocalDsa, payload_dp);
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory"),
					 errdetail_internal("row cache: cannot allocate GlobalEntry")));
		}
		e = (GlobalEntry *) dsa_get_address(LocalDsa, entry_dp);
		e->relid = relid;
		e->pkey_hash = pkey_hash;
		e->pkey_val = pkey_packed;
		e->rel_gen_at_load = rel_gen_at_load;
		pg_atomic_init_u32(&e->state, ROW_CACHE_ENTRY_FRESH);
		ItemPointerCopy(&slot->tts_tid, &e->tid);
		e->payload_dp = payload_dp;
		e->next_dp = InvalidDsaPointer;

		part = PartitionLockForBucket(bucket);
		LWLockAcquire(part, LW_EXCLUSIVE);
		BucketInsertHead(bucket, entry_dp, e);
		LWLockRelease(part);
	}

	table_endscan(scan);
	ExecDropSingleTupleTableSlot(slot);

	if (pushed_snapshot)
		PopActiveSnapshot();

	/* Publish ENABLED with a write barrier. */
	pg_write_barrier();
	pg_atomic_write_u32(&rm->state, RELMETA_ENABLED);

	/* Invalidate this backend's sticky cache to force a fresh lookup. */
	LastLookupRelMeta = NULL;
	LastLookupRelid = InvalidOid;

	LWLockRelease(&rm->build_lock);
}

/* ----------------------------------------------------------------
 * Public API: Drop
 * ----------------------------------------------------------------
 *
 * 1. Find RelMeta; bail out if none.
 * 2. build_lock EXCLUSIVE.
 * 3. State -> DISABLED, bump rel_gen.
 * 4. Sweep every bucket: under partition lock, unlink + dsa_free all
 *    entries whose relid matches.
 * 5. Release build_lock.
 *
 * Phase 1 frees DSA storage immediately, relying on the V3 caller
 * contract (no readers concurrent with Drop).  Phase 2 will defer frees
 * via EBR.
 */
static void
DropAllEntriesForRelid(Oid relid)
{
	dsa_pointer *heads;

	if (!DsaPointerIsValid(RowCacheCtl->hash_buckets_dp))
		return;

	heads = BucketHeads();

	for (uint32 b = 0; b < ROW_CACHE_HASH_BUCKETS; b++)
	{
		LWLock	   *part = PartitionLockForBucket(b);
		dsa_pointer prev_dp = InvalidDsaPointer;
		dsa_pointer cur_dp;
		GlobalEntry *prev = NULL;

		LWLockAcquire(part, LW_EXCLUSIVE);

		cur_dp = heads[b];
		while (DsaPointerIsValid(cur_dp))
		{
			GlobalEntry *e = (GlobalEntry *) dsa_get_address(LocalDsa, cur_dp);
			dsa_pointer next_dp = e->next_dp;

			if (e->relid == relid)
			{
				/* Unlink. */
				if (DsaPointerIsValid(prev_dp))
					prev->next_dp = next_dp;
				else
					heads[b] = next_dp;

				/* Free payload + entry.  No EBR yet (Phase 2). */
				if (DsaPointerIsValid(e->payload_dp))
					dsa_free(LocalDsa, e->payload_dp);
				dsa_free(LocalDsa, cur_dp);

				cur_dp = next_dp;
				/* prev / prev_dp unchanged. */
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

	if (pg_atomic_read_u32(&rm->state) == RELMETA_DISABLED)
	{
		LWLockRelease(&rm->build_lock);
		return;
	}

	pg_atomic_write_u32(&rm->state, RELMETA_DISABLED);
	pg_atomic_fetch_add_u64(&rm->rel_gen, 1);
	pg_memory_barrier();

	DropAllEntriesForRelid(relid);

	rm->pkey_attno = 0;
	rm->pkey_typlen = 0;
	rm->pkey_byval = false;

	LastLookupRelMeta = NULL;
	LastLookupRelid = InvalidOid;

	LWLockRelease(&rm->build_lock);
}

/* ----------------------------------------------------------------
 * Public API: PkeyFetch
 * ----------------------------------------------------------------
 *
 * Read path is lock-free (Phase 1 contract).  See section 4.1 of the
 * design doc for the protocol; Phase 1 omits the EBR enter/exit calls,
 * everything else is identical:
 *
 *   1. Sticky lookup -> RelMeta.
 *   2. Atomic-read state; bail if not ENABLED.
 *   3. Pack pkey, compute hash, walk bucket chain.
 *   4. If found: validate rel_gen and entry state, do MVCC check, fill slot.
 */
bool
RelationRowCachePkeyFetch(Oid relid,
						  Datum pkey_val,
						  Snapshot snapshot,
						  TupleTableSlot *slot,
						  bool *is_visible,
						  bool *has_hot_chain)
{
	RelMeta    *rm;
	uint64		packed;
	uint32		hash;
	uint32		bucket;
	GlobalEntry *entry;
	FlatCachedTuple *flat;
	HeapTupleData htup;
	ItemPointerData tid;

	Assert(is_visible != NULL && has_hot_chain != NULL);
	*is_visible = false;
	*has_hot_chain = false;

	if (RowCacheCtl == NULL || !OidIsValid(relid))
		return false;
	if (!IsMVCCSnapshot(snapshot))
		return false;

	/* Sticky lookup. */
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
		return false;
	pg_read_barrier();

	EnsureRowCacheDsa();

	packed = PackPkeyDatum(pkey_val, rm->pkey_typlen);
	hash = ComputePkeyHash(packed);
	bucket = hash & ROW_CACHE_BUCKET_MASK;

	entry = BucketLookup(bucket, relid, packed, NULL);
	if (entry == NULL)
		return false;

	/* Validate entry: must be FRESH and same generation. */
	if (pg_atomic_read_u32(&entry->state) != ROW_CACHE_ENTRY_FRESH)
		return false;
	if (entry->rel_gen_at_load != pg_atomic_read_u64(&rm->rel_gen))
		return false;
	pg_read_barrier();

	if (!DsaPointerIsValid(entry->payload_dp))
		return false;
	flat = (FlatCachedTuple *) dsa_get_address(LocalDsa, entry->payload_dp);

	/*
	 * Reconstruct an in-memory HeapTuple pointing into the flat payload
	 * so we can run the MVCC visibility check.  The TID is recovered
	 * from the embedded HeapTupleHeader (set when we flattened) for the
	 * has_hot_chain / slot fill.
	 */
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

	return true;
}

/* ----------------------------------------------------------------
 * Public API: PkeyAttno
 * ---------------------------------------------------------------- */

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

	return rm->pkey_attno;
}

/* ----------------------------------------------------------------
 * Legacy TID-keyed API: stubs.  Phase 1 has no TID-keyed path; the
 * tableam.h hook sites still call these but get a clean "miss" so they
 * fall through to the native AM path.
 * ---------------------------------------------------------------- */

bool
RelationRowCacheFillSlot(TupleTableSlot *slot)
{
	(void) slot;
	return false;
}

bool
RelationRowCacheFetchWithVisibility(Oid relid,
									ItemPointer tid,
									Snapshot snapshot,
									TupleTableSlot *slot,
									bool *is_visible,
									bool *has_hot_chain)
{
	(void) relid;
	(void) tid;
	(void) snapshot;
	(void) slot;

	if (is_visible)
		*is_visible = false;
	if (has_hot_chain)
		*has_hot_chain = false;
	return false;
}
