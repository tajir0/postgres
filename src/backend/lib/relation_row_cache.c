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
#include "utils/wait_event.h"
#include "utils/dsa.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
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

	/*
	 * Orphan retire list (Phase 2 4/4 prerequisite).
	 *
	 * When a backend exits (normally or abnormally) with retires still
	 * pending in its backend-local list, the before_shmem_exit hook moves
	 * them here so the GC worker can reclaim them once safe_epoch passes.
	 *
	 *   orphan_list_lock     — protects head pointer updates
	 *   orphan_list_head_dp  — head of a chain of OrphanBatch nodes in DSA;
	 *                          InvalidDsaPointer when empty
	 */
	LWLock			orphan_list_lock;
	dsa_pointer		orphan_list_head_dp;

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

/* Forward-declared here so rowcache_relcache_callback (defined below in
 * the EBR section) can touch the sticky-lookup statics and FindRelMeta
 * helper that physically live further down with the Phase 1 globals. */
static Oid			LastLookupRelid;
static RelMeta	   *LastLookupRelMeta;
static RelMeta *FindRelMeta(Oid relid);

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

/* ----------------------------------------------------------------
 * Phase 2 (4/4 prerequisite): backend-exit cleanup + orphan list
 *
 * If a backend dies (clean disconnect, FATAL, postmaster signal) while
 * its retire list is non-empty, the palloc'd RetireNodes evaporate with
 * the backend's memory context — but the DSA blocks they point at do
 * NOT.  Without intervention those DSA blocks leak forever.
 *
 * Mitigation:
 *
 *   1. Register a before_shmem_exit hook the first time this backend
 *      attaches to the row-cache DSA.  Backends that never touched the
 *      cache pay nothing.
 *
 *   2. On exit the hook walks MyRetireHead and moves each entry into
 *      the shmem-anchored orphan list as an OrphanBatch node allocated
 *      in DSA (this backend is still attached to DSA at before_shmem_exit
 *      time — DSA detach is sequenced after, in on_shmem_exit).  After
 *      splicing it also forces MyProc->rowcache_local_epoch to 0 so a
 *      dead backend can never permanently block safe_epoch advance.
 *
 *   3. The GC worker drains the orphan list each tick under the freshly
 *      published safe_epoch, dsa_free'ing each batch's payload pointers
 *      and freeing the OrphanBatch node itself.
 *
 * Lock-order: orphan_list_lock is leaf — no other lock is acquired
 * while held, so it cannot deadlock against partition / build locks.
 * ---------------------------------------------------------------- */

typedef struct OrphanBatch
{
	uint64			epoch;
	dsa_pointer		dp[RETIRE_NODE_SLOTS];
	dsa_pointer		next_dp;	/* InvalidDsaPointer at list tail */
} OrphanBatch;

static bool	RowCacheExitCallbackRegistered = false;
static bool	RowCacheRelcacheCallbackRegistered = false;

static void rowcache_backend_exit_cleanup(int code, Datum arg);
static void rowcache_relcache_callback(Datum arg, Oid relid);

/*
 * Idempotent: install before_shmem_exit once per backend, the first time
 * the backend touches the cache (i.e. inside EnsureRowCacheDsa).  Backends
 * that never call EnsureRowCacheDsa skip this entirely.
 */
static void
RowCacheRegisterExitCallback(void)
{
	if (RowCacheExitCallbackRegistered)
		return;
	before_shmem_exit(rowcache_backend_exit_cleanup, (Datum) 0);
	RowCacheExitCallbackRegistered = true;
}

/*
 * Idempotent: install a relcache invalidation callback once per backend.
 * Called from EnsureRowCacheDsa alongside the exit hook so any backend
 * that touches the cache also wires up DDL-driven invalidation.
 *
 * sinval is delivered to every backend, so each cache-touching backend
 * runs the callback independently when its AcceptInvalidationMessages
 * fires.  All updates land on shmem (RelMeta.state / rel_gen) which is
 * idempotent under concurrent writes.
 */
static void
RowCacheRegisterRelcacheCallback(void)
{
	if (RowCacheRelcacheCallbackRegistered)
		return;
	CacheRegisterRelcacheCallback(rowcache_relcache_callback, (Datum) 0);
	RowCacheRelcacheCallbackRegistered = true;
}

/*
 * Splice MyRetireHead onto the shmem orphan list, then drop our
 * rowcache_local_epoch so we never block safe_epoch advance after exit.
 *
 * Runs during proc_exit_prepare before shmem teardown, so:
 *   - DSA is still attached (dsa_allocate / dsa_get_address are safe).
 *   - LWLockAcquire is safe (not yet in lwlock cleanup).
 *   - TopMemoryContext is still alive (pfree on local RetireNode is safe).
 *
 * If a DSA allocation fails (out of segment space) we LOG and leak that
 * one node — orphans are best-effort, the cluster must not crash here.
 */
static void
rowcache_backend_exit_cleanup(int code, Datum arg)
{
	/* Always release our epoch slot, even when we have nothing to splice. */
	if (MyProc != NULL)
		pg_atomic_write_u64(&MyProc->rowcache_local_epoch, 0);

	if (MyRetireHead == NULL || RowCacheCtl == NULL || LocalDsa == NULL)
	{
		MyRetireHead = NULL;
		MyRetireListLen = 0;
		return;
	}

	LWLockAcquire(&RowCacheCtl->orphan_list_lock, LW_EXCLUSIVE);

	while (MyRetireHead != NULL)
	{
		RetireNode	   *node = MyRetireHead;
		dsa_pointer		ob_dp;
		OrphanBatch	   *ob;

		MyRetireHead = node->next;

		ob_dp = dsa_allocate_extended(LocalDsa, sizeof(OrphanBatch),
									  DSA_ALLOC_NO_OOM);
		if (DsaPointerIsValid(ob_dp))
		{
			ob = (OrphanBatch *) dsa_get_address(LocalDsa, ob_dp);
			ob->epoch = node->epoch;
			ob->dp[0] = node->dp[0];
			ob->dp[1] = node->dp[1];
			ob->dp[2] = node->dp[2];
			ob->next_dp = RowCacheCtl->orphan_list_head_dp;
			RowCacheCtl->orphan_list_head_dp = ob_dp;
		}
		else
		{
			/*
			 * DSA out of memory during exit cleanup.  Log and skip; the
			 * three dpts on this node will leak but the cluster must not
			 * fail to shut a backend down.
			 */
			ereport(LOG,
					(errmsg_internal("row cache: orphan-list dsa_allocate failed during backend exit; leaking %d DSA block(s)",
									 (DsaPointerIsValid(node->dp[0]) ? 1 : 0) +
									 (DsaPointerIsValid(node->dp[1]) ? 1 : 0) +
									 (DsaPointerIsValid(node->dp[2]) ? 1 : 0))));
		}

		pfree(node);
	}

	LWLockRelease(&RowCacheCtl->orphan_list_lock);

	MyRetireListLen = 0;
}

/*
 * Relcache invalidation callback (Phase 3 (5/5)).
 *
 * Fired in every backend when a relation's catalog state changes
 * (DROP TABLE, ALTER TABLE that rewrites tuples, schema changes,
 * etc.).  Our job is to unpublish the cached entries for `relid` so
 * future readers in this and every other backend stop using them.
 *
 * Two-level invariant:
 *
 *   - state = DISABLED  → atomic_load in the read path's fast-fail
 *     short-circuit will bail before even entering the EBR critical
 *     section.
 *
 *   - rel_gen ++         → any reader that did enter the critical
 *     section and grabbed a GlobalEntry whose rel_gen_at_load matched
 *     the OLD value will fail the rel_gen check after our bump and
 *     treat it as a miss.
 *
 * Note: we do NOT sweep + retire entries here.  sinval is broadcast to
 * every backend, so a sweep would happen in each backend independently
 * — O(N_backends × N_buckets) work for what is conceptually a single
 * O(N_buckets) job.  Physical reclamation is deferred to the next
 * explicit Drop, the next Load (which detects stale state via build_lock
 * + double-check), or future LRU eviction.
 *
 * Idempotence + race-safety: pg_atomic_write_u32 and fetch_add_u64 are
 * safe under concurrent firings of this callback in multiple backends;
 * the final state and rel_gen value are well-defined.
 *
 * relid == InvalidOid means "everything is potentially invalid"; this
 * happens e.g. after CREATE/DROP DATABASE.  We don't currently support
 * a bulk-bump, but the cache is anyway DROP'd at database boundaries
 * (via sinval on each individual relation that gets dropped), so the
 * miss is harmless in practice.  Wire up a per-RelMeta scan here later
 * if a workload actually triggers it.
 */
static void
rowcache_relcache_callback(Datum arg, Oid relid)
{
	RelMeta	   *rm;

	if (RowCacheCtl == NULL)
		return;
	if (!OidIsValid(relid))
		return;					/* see note above */

	rm = FindRelMeta(relid);
	if (rm == NULL)
		return;

	/*
	 * Two-step unpublish, atomic store + atomic add; safe to race
	 * across backends.  The exact order doesn't matter for correctness
	 * because readers fail on EITHER signal; we write state first so a
	 * fast-fail reader bails before paying for EpochEnter.
	 */
	pg_atomic_write_u32(&rm->state, RELMETA_DISABLED);
	(void) pg_atomic_fetch_add_u64(&rm->rel_gen, 1);

	/*
	 * Invalidate this backend's sticky-lookup cache.  Other backends
	 * each receive their own sinval and will invalidate their own
	 * sticky during their own callback firing.
	 */
	if (LastLookupRelid == relid)
	{
		LastLookupRelid = InvalidOid;
		LastLookupRelMeta = NULL;
	}
}

/*
 * GC-side drain.  Called from the rowcache-gc worker after publishing
 * safe_epoch.  Walks the orphan list under orphan_list_lock and reclaims
 * every batch whose epoch < safe.
 *
 * Holding orphan_list_lock for the entire walk is acceptable because:
 *   - The list is only mutated at backend exit (rare) and during this
 *     drain (single worker, single-threaded inside the lock).
 *   - dsa_free is the heavy operation; if contention ever becomes real
 *     we can split the list into shards or copy-out-then-free.
 *
 * Returns the number of batches freed (for diagnostics / future logging).
 */
static int
RowCacheReclaimOrphans(uint64 safe)
{
	dsa_pointer	   *pp;
	dsa_pointer		cur_dp;
	int				freed = 0;

	if (RowCacheCtl == NULL)
		return 0;

	/*
	 * GC needs DSA attached to call dsa_free on payload pointers and on
	 * the OrphanBatch nodes themselves.  EnsureRowCacheDsa is idempotent.
	 */
	EnsureRowCacheDsa();

	LWLockAcquire(&RowCacheCtl->orphan_list_lock, LW_EXCLUSIVE);

	pp = &RowCacheCtl->orphan_list_head_dp;
	cur_dp = *pp;
	while (DsaPointerIsValid(cur_dp))
	{
		OrphanBatch *ob = (OrphanBatch *) dsa_get_address(LocalDsa, cur_dp);

		if (ob->epoch < safe)
		{
			dsa_pointer	next = ob->next_dp;

			*pp = next;
			for (int i = 0; i < RETIRE_NODE_SLOTS; i++)
			{
				if (DsaPointerIsValid(ob->dp[i]))
					dsa_free(LocalDsa, ob->dp[i]);
			}
			dsa_free(LocalDsa, cur_dp);
			cur_dp = next;
			freed++;
		}
		else
		{
			pp = &ob->next_dp;
			cur_dp = ob->next_dp;
		}
	}

	LWLockRelease(&RowCacheCtl->orphan_list_lock);

	return freed;
}

/* ----------------------------------------------------------------
 * Phase 2 (3/4): "rowcache-gc" background worker
 *
 * One commonly-shared, postmaster-monitored bgworker that periodically:
 *   1. Computes safe_epoch = min over ProcArray of every backend's
 *      rowcache_local_epoch (skipping idle slots == 0).
 *   2. Publishes the result via RowCacheSafeEpochPublish.
 *   3. Sleeps GC_INTERVAL_MS (latch-interruptible).
 *
 * Backends consume the published value in their own RowCacheLocalGC
 * pass — see Phase 2 (2/4).  This worker therefore does not directly
 * dsa_free anything; it is purely the safe-epoch publisher.
 *
 * Registration (RowCacheGCRegister) is called from postmaster startup
 * just after ApplyLauncherRegister, before process_shared_preload_libraries
 * gets a chance to consume worker slots.  Uses BGWORKER_SHMEM_ACCESS but
 * NOT BGWORKER_BACKEND_DATABASE_CONNECTION — the GC only touches shmem
 * (ProcArray + RowCacheCtl), no catalog access required.
 *
 * Restart policy:
 *   bgw_restart_time = 5 seconds.  A crash here is fixable without
 *   bringing down the cluster: until a new GC instance comes up,
 *   safe_epoch_published stays frozen and retire lists grow, but the
 *   cluster keeps serving reads.
 *
 * Future Phase 2 (3/4 cont.):
 *   When the orphan list (for crashed-backend cleanup) lands, this
 *   worker will also drain it under the published safe_epoch.
 * ---------------------------------------------------------------- */

#define ROWCACHE_GC_INTERVAL_MS	100

PGDLLEXPORT void RowCacheGCMain(Datum main_arg);

void
RowCacheGCMain(Datum main_arg)
{
	ereport(DEBUG1,
			(errmsg_internal("row-cache GC worker started")));

	/* Standard bgworker signal handlers. */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	for (;;)
	{
		int		rc;
		uint64	safe;

		CHECK_FOR_INTERRUPTS();

		/*
		 * Core duty: compute and publish safe_epoch.  Even if no backend
		 * has anything to retire, publishing keeps the counter fresh so
		 * any future retire is reclaimable promptly.
		 *
		 * Then drain the orphan list under the freshly published safe
		 * epoch.  Orphans come from backends that exited (clean or crash)
		 * with retires still pending — see rowcache_backend_exit_cleanup.
		 */
		if (RowCacheCtl != NULL)
		{
			safe = RowCacheComputeSafeEpoch();
			RowCacheSafeEpochPublish(safe);
			(void) RowCacheReclaimOrphans(safe);
		}

		/* Sleep until SIGHUP / SIGTERM / interval timeout / postmaster death. */
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   ROWCACHE_GC_INTERVAL_MS,
					   WAIT_EVENT_ROWCACHE_GC_MAIN);

		if (rc & WL_LATCH_SET)
		{
			ResetLatch(MyLatch);
			CHECK_FOR_INTERRUPTS();
		}

		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}
	}
	/* unreachable; die() and proc_exit() handle exit paths */
}

/*
 * Register the rowcache-gc worker.  Called from postmaster startup.
 *
 * Built-in worker, library_name = "postgres" so the postmaster looks the
 * function up in its own symbol table rather than dlopening a .so.
 */
void
RowCacheGCRegister(void)
{
	BackgroundWorker bgw;

	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	bgw.bgw_restart_time = 5;
	snprintf(bgw.bgw_library_name, MAXPGPATH, "postgres");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "RowCacheGCMain");
	snprintf(bgw.bgw_name, BGW_MAXLEN, "rowcache-gc");
	snprintf(bgw.bgw_type, BGW_MAXLEN, "rowcache-gc");
	bgw.bgw_notify_pid = 0;
	bgw.bgw_main_arg = (Datum) 0;

	RegisterBackgroundWorker(&bgw);
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

	/* Orphan list starts empty. */
	RowCacheCtl->orphan_list_head_dp = InvalidDsaPointer;

	LWLockInitialize(&RowCacheCtl->control_lock, LWTRANCHE_ROW_CACHE_CTL);
	LWLockInitialize(&RowCacheCtl->relmeta_alloc_lock,
					 LWTRANCHE_ROW_CACHE_RELMETA);
	LWLockInitialize(&RowCacheCtl->orphan_list_lock,
					 LWTRANCHE_ROW_CACHE_CTL);

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

	/*
	 * Lazily install the backend-exit cleanup hook here so that backends
	 * which never touch the cache pay zero exit-time overhead.  Registered
	 * once per backend (idempotent inside RowCacheRegisterExitCallback).
	 */
	RowCacheRegisterExitCallback();

	/*
	 * Lazily install the relcache invalidation callback so DDL on cached
	 * relations is observed by this backend.  Same rationale as above:
	 * skip the cost in backends that never touch the cache.
	 */
	RowCacheRegisterRelcacheCallback();
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
 * Public API: Drop  (Phase 3 (3/3): unpublish + EBR retire)
 * ----------------------------------------------------------------
 *
 * Two-phase teardown decouples the "make new readers miss" half from
 * the "reclaim DSA blocks" half:
 *
 *   Unpublish (writer-visible, completes immediately):
 *     1. Find RelMeta; bail out if none.
 *     2. build_lock EXCLUSIVE (serialise concurrent Load / Drop / Drop).
 *     3. Atomic-store state -> DISABLED + fetch_add rel_gen + memory
 *        barrier.  Any reader that enters the EBR critical section
 *        AFTER this point either sees state != ENABLED (early miss)
 *        or grabs an entry whose rel_gen_at_load mismatches (gen miss).
 *
 *   Retire (deferred reclaim, runs under EBR safe_epoch):
 *     4. Sweep every bucket; under partition lock, unlink each entry
 *        whose relid matches, atomic-store entry->state=DELETED,
 *        push (payload_dp, entry_dp) onto this backend's EBR retire
 *        list.  Backend-local LocalGC (Phase 2 2/4) + the rowcache-gc
 *        worker (Phase 2 3/4) actually dsa_free them once safe_epoch
 *        exceeds the retire epoch.
 *     5. Clear RelMeta pkey descriptor (allows the slot to be reused
 *        by a future Load of a different relation or a re-Load with
 *        a different pkey shape).
 *     6. Invalidate per-backend sticky-lookup cache.
 *     7. Release build_lock.
 *
 * Concurrent-reader safety: once Phase 4 wires EpochEnter/Exit into
 * the read path, this Drop is safe under any number of in-flight
 * readers.  EBR keeps the freshly-unlinked entry's backing memory
 * alive until those readers leave their critical sections, and the
 * unpublish step guarantees no NEW reader can reach the entry through
 * the bucket chain.
 *
 * Until Phase 4 lands, the V3 caller contract (no readers concurrent
 * with Drop) still applies; Drop's behavior is observably identical
 * to the prior immediate-dsa_free implementation — just with the
 * actual free deferred by one GC tick.
 *
 * Phase 3 (3/3) upgrade:
 *   - Caller (RelationRowCacheDropRelation) has already set
 *     rel->state = DISABLED and bumped rel_gen, so any reader that
 *     enters the EBR critical section AFTER this point will either
 *     observe state != ENABLED (early miss) or load an entry whose
 *     rel_gen_at_load != current rel_gen (gen-mismatch miss).
 *   - Readers that were ALREADY inside an EBR critical section before
 *     the unpublish may still hold a pointer to a GlobalEntry we're
 *     about to unlink.  EBR-retiring the entry + payload (rather than
 *     dsa_free'ing) keeps their backing memory alive until safe_epoch
 *     exceeds the retire epoch, at which point all in-flight readers
 *     have left the critical section.
 *
 * Net effect: Drop becomes safe under concurrent readers once Phase 4
 * wires EpochEnter/Exit into the read path.  Until then (i.e. through
 * Phase 1's caller contract), retire is just a slightly delayed
 * dsa_free with identical observable behavior.
 *
 * Each unlinked entry pushes (payload_dp, entry_dp, Invalid) onto this
 * backend's retire list.  RETIRE_NODE_SLOTS' third slot is reserved
 * for the per-entry pkey buffer that Phase 4 (composite / byref pk)
 * will introduce.
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
				dsa_pointer	victim_payload;

				/* Unlink. */
				if (DsaPointerIsValid(prev_dp))
					prev->next_dp = next_dp;
				else
					heads[b] = next_dp;

				/*
				 * Mark DELETED so any future-EBR reader that already
				 * grabbed `e` from the bucket chain bails out instead
				 * of using its payload.  Chain-unlink above prevents
				 * NEW readers from finding it; this state flip protects
				 * readers that already have the pointer in hand.
				 */
				victim_payload = e->payload_dp;
				pg_atomic_write_u32(&e->state, ROW_CACHE_ENTRY_DELETED);

				/*
				 * Retire payload + entry via EBR instead of dsa_free.
				 * GC bgworker (Phase 2 3/4) + backend-local reclaim
				 * (Phase 2 2/4) will free them once safe_epoch passes.
				 */
				RowCacheEpochRetire(victim_payload, cur_dp,
									InvalidDsaPointer);

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

/* ----------------------------------------------------------------
 * Phase 3 (1/2): DML hooks — invalidate-only strategy
 *
 * On UPDATE / DELETE of a cached row, the cached payload is now stale.
 * Strategy choice for Phase 3:
 *
 *   invalidate-only (this commit):
 *     Unlink the matching GlobalEntry from its bucket chain and retire
 *     payload + entry + (pkey buffer, when extended in Phase 4) via the
 *     EBR retire list.  Future reads for the same pkey miss the cache
 *     and fall back to the native btree path.  Cache content is NEVER
 *     refreshed by DML; only an explicit Drop+Load (or future LRU
 *     eviction) repopulates.
 *
 *   write-through (NOT this commit):
 *     Would re-flatten the new tuple and atomic-swap the payload_dp.
 *     Skipped here because the corresponding INSERT hook is out of
 *     scope, so we cannot keep cache contents in sync end-to-end
 *     anyway.  Defer to a later phase when LRU + INSERT land together.
 *
 * UPDATE handling note: we ALWAYS evict by the OLD pkey value.  Cases:
 *   - non-key-update: old pkey == new pkey, old entry evicted, future
 *     reads fall back to native (correct).
 *   - key-update (rare): old pkey != new pkey, old entry evicted; new
 *     pkey simply has no cache entry until next Load (correct, no
 *     stale data possible).
 *
 * HOT update also routes through heap_update, so this hook fires for
 * HOT too.  That's intentional: HOT changes the tuple content even
 * when btree is untouched, so the cached payload is equally stale.
 *
 * Call-site contract (heapam.c):
 *   - Must be called BEFORE ReleaseBuffer(buffer) — we read tuple->t_data
 *     via heap_getattr which dereferences buffer memory.
 *   - Must be called AFTER END_CRIT_SECTION — we palloc inside
 *     RowCacheEpochRetire which can throw OOM.
 *   - The "between CacheInvalidateHeapTuple and ReleaseBuffer" slot in
 *     both heap_update and heap_delete satisfies both constraints.
 * ---------------------------------------------------------------- */

/*
 * Internal: locate (relid, pkey) in the global hash, unlink the entry,
 * and retire payload + entry via EBR.  No-op if not found.
 *
 * Caller must already have determined that `rm` is the live RelMeta
 * for `relid` and that `pkey_datum` is non-null.
 */
static void
InvalidateEntryByPkey(RelMeta *rm, Oid relid, Datum pkey_datum)
{
	uint64			packed;
	uint32			pkey_hash;
	uint32			bucket;
	LWLock		   *part;
	dsa_pointer	   *heads;
	dsa_pointer		prev_dp;
	dsa_pointer		cur_dp;
	GlobalEntry	   *prev;
	dsa_pointer		victim_dp = InvalidDsaPointer;
	dsa_pointer		victim_payload_dp = InvalidDsaPointer;

	Assert(rm != NULL);

	/*
	 * EnsureRowCacheDsa needs to have run at least once for this backend
	 * so that LocalDsa is valid and BucketHeads can be resolved.  In the
	 * DML hook path we may be the first to touch the cache from this
	 * backend; attach lazily.
	 */
	EnsureRowCacheDsa();

	packed = PackPkeyDatum(pkey_datum, rm->pkey_typlen);
	pkey_hash = ComputePkeyHash(packed);
	bucket = pkey_hash % ROW_CACHE_HASH_BUCKETS;
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
			e->pkey_val == packed)
		{
			/* Unlink from chain. */
			if (DsaPointerIsValid(prev_dp))
				prev->next_dp = e->next_dp;
			else
				heads[bucket] = e->next_dp;

			victim_dp = cur_dp;
			victim_payload_dp = e->payload_dp;

			/*
			 * Mark DELETED so any reader that already grabbed `e` (under
			 * future Phase 2 EBR enter) bails out instead of using its
			 * payload.  Plain atomic store; the bucket-chain unlink above
			 * makes the entry unreachable to new readers.
			 */
			pg_atomic_write_u32(&e->state, ROW_CACHE_ENTRY_DELETED);
			break;
		}

		prev_dp = cur_dp;
		prev = e;
		cur_dp = e->next_dp;
	}

	LWLockRelease(part);

	/*
	 * Retire OUTSIDE the partition lock.  EpochRetire may palloc and
	 * could in theory ereport on OOM; holding the lock across it would
	 * widen the critical section unnecessarily.
	 */
	if (DsaPointerIsValid(victim_dp))
		RowCacheEpochRetire(victim_payload_dp, victim_dp, InvalidDsaPointer);
}

/*
 * Shared front-end used by RowCacheOnHeapUpdate / RowCacheOnHeapDelete.
 *
 * Steps:
 *   1. Cheap pre-checks (RowCacheCtl set, relmeta enabled).  Sticky-relmeta
 *      friendly: in the common case of repeated DML on the same relation,
 *      FindRelMeta will hit and the only work is a couple of atomic loads.
 *   2. Extract the pkey Datum from `tuple` using rel's tuple descriptor.
 *      Bail if null (defensive; pkeys are NOT NULL by definition).
 *   3. Defer to InvalidateEntryByPkey.
 */
static void
InvalidateByHeapTuple(Relation rel, HeapTuple tuple)
{
	RelMeta	   *rm;
	Datum		pkey_datum;
	bool		isnull;
	Oid			relid;

	if (RowCacheCtl == NULL)
		return;
	if (rel == NULL || tuple == NULL || tuple->t_data == NULL)
		return;

	relid = RelationGetRelid(rel);
	rm = FindRelMeta(relid);
	if (rm == NULL)
		return;
	if (pg_atomic_read_u32(&rm->state) != RELMETA_ENABLED)
		return;
	if (rm->pkey_attno <= 0)
		return;

	pkey_datum = heap_getattr(tuple, rm->pkey_attno,
							  RelationGetDescr(rel), &isnull);
	if (isnull)
		return;

	InvalidateEntryByPkey(rm, relid, pkey_datum);
}

/* ----------------------------------------------------------------
 * Phase 3 (2/2): write-through for UPDATE
 *
 * Replaces the prior invalidate-only handling of heap_update so that
 * the cached payload stays in sync with the heap.  Keeps the cache
 * "hot" across a stream of UPDATEs to the same pkey — the typical OLTP
 * pattern for warehouse / district / customer / profile-table rows.
 *
 * Steps for the common case (non-key-update on a cached row):
 *   1. Flatten newtup to a fresh DSA block (allocations happen OUTSIDE
 *      the partition lock; slot-create / flatten / dsa_allocate are
 *      the expensive bits).
 *   2. Acquire partition lock EXCLUSIVE for the target bucket.
 *   3. Walk bucket chain; on (relid, pkey_hash, pkey_val) match:
 *        old_dp = e->payload_dp;
 *        pg_write_barrier();
 *        e->payload_dp = new_dp;     -- atomic, single-word
 *      The barrier guarantees any reader who loads the new pointer
 *      sees the fully-initialised FlatCachedTuple it points to.
 *   4. Release partition lock.
 *   5. RowCacheEpochRetire(old_dp) — outside the lock; lets the GC
 *      reclaim old payload once no EBR critical section can still hold
 *      a pointer into it.
 *
 * Degraded paths (still correct, just less optimal):
 *
 *   key-update (old_pkey != new_pkey):
 *     Cannot write-through because the new pkey has no entry to update
 *     (no INSERT hook in this phase).  Falls back to invalidate-only on
 *     the OLD pkey.  Both old and new pkey will miss cache thereafter
 *     until Drop+Load; safe but cache-miss.
 *
 *   entry-not-cached (UPDATE to a relation that's cache-enabled but
 *   the specific pkey is not in the bucket chain — e.g. concurrently
 *   evicted, or the row was inserted after Load):
 *     Walk completes without a match.  The freshly-flattened payload
 *     is dsa_free'd directly (no reader ever observed it, so EBR
 *     retire is unnecessary — direct free is safe and a tiny bit
 *     faster).
 *
 *   flatten failure (OOM):
 *     RowCacheFlattenTuple ereports on dsa_allocate failure.  That's
 *     fine: heap_update is past END_CRIT_SECTION and the transaction
 *     can abort cleanly.  Cache contents remain consistent (the entry
 *     keeps its previous payload).
 *
 * NOT bumping entry_gen: Phase 3 readers (added in Phase 4) tolerate
 * "saw old payload after writer swapped" via MVCC visibility check,
 * exactly as for in-heap stale reads.  If a future reader needs
 * stronger "did this entry get replaced under me" detection, add a
 * pg_atomic_fetch_add_u64 on entry->entry_gen here.
 * ---------------------------------------------------------------- */

/*
 * Wrap RowCacheFlattenTuple so callers that hold only a HeapTuple
 * (heap_update / heap_delete hooks) can flatten without manually
 * setting up a slot.
 *
 * Allocates a single-tuple slot in CurrentMemoryContext (caller
 * guarantees we're not in a CRIT_SECTION; allocation may ereport on
 * OOM).  Slot is dropped before return so we never leak across calls.
 */
static dsa_pointer
FlattenHeapTupleToDsa(Relation rel, HeapTuple htup)
{
	TupleTableSlot *slot;
	dsa_pointer		dp;

	Assert(rel != NULL && htup != NULL);

	EnsureRowCacheDsa();

	slot = MakeSingleTupleTableSlot(RelationGetDescr(rel), &TTSOpsHeapTuple);
	ExecStoreHeapTuple(htup, slot, false);
	dp = RowCacheFlattenTuple(LocalDsa, slot);
	ExecDropSingleTupleTableSlot(slot);

	return dp;
}

/*
 * Find entry by (relid, pkey) and atomically swap its payload_dp to
 * `new_payload_dp`.  On success returns the previous payload_dp (which
 * the caller must retire).  On miss returns InvalidDsaPointer and
 * `*found` is set to false; caller should dsa_free the wasted new
 * payload directly.
 *
 * Uses the same lock + walk discipline as InvalidateEntryByPkey.
 */
static dsa_pointer
ReplaceEntryPayloadByPkey(RelMeta *rm, Oid relid, Datum pkey_datum,
						  dsa_pointer new_payload_dp, bool *found)
{
	uint64			packed;
	uint32			pkey_hash;
	uint32			bucket;
	LWLock		   *part;
	dsa_pointer	   *heads;
	dsa_pointer		cur_dp;
	dsa_pointer		old_payload_dp = InvalidDsaPointer;

	Assert(rm != NULL && found != NULL);
	*found = false;

	EnsureRowCacheDsa();

	packed = PackPkeyDatum(pkey_datum, rm->pkey_typlen);
	pkey_hash = ComputePkeyHash(packed);
	bucket = pkey_hash & ROW_CACHE_BUCKET_MASK;
	part = PartitionLockForBucket(bucket);

	LWLockAcquire(part, LW_EXCLUSIVE);

	if (!DsaPointerIsValid(RowCacheCtl->hash_buckets_dp))
	{
		LWLockRelease(part);
		return InvalidDsaPointer;
	}

	heads = BucketHeads();
	cur_dp = heads[bucket];

	while (DsaPointerIsValid(cur_dp))
	{
		GlobalEntry *e = (GlobalEntry *) dsa_get_address(LocalDsa, cur_dp);

		if (e->relid == relid &&
			e->pkey_hash == pkey_hash &&
			e->pkey_val == packed)
		{
			old_payload_dp = e->payload_dp;
			/*
			 * write_barrier before publishing the new pointer so a
			 * reader that loads new_payload_dp afterwards sees the
			 * fully-initialised FlatCachedTuple at that address.
			 */
			pg_write_barrier();
			e->payload_dp = new_payload_dp;
			*found = true;
			break;
		}
		cur_dp = e->next_dp;
	}

	LWLockRelease(part);

	return old_payload_dp;
}

/*
 * Public DML hooks.  Called from heap_update / heap_delete after
 * END_CRIT_SECTION + CacheInvalidateHeapTuple but before ReleaseBuffer.
 *
 * UPDATE uses write-through (Phase 3 (2/2)); DELETE uses invalidate-
 * only (Phase 3 (1/2) — DELETE removes the entry entirely, so write-
 * through is not meaningful).
 */
void
RowCacheOnHeapUpdate(Relation rel, HeapTuple oldtup, HeapTuple newtup)
{
	RelMeta		   *rm;
	Datum			old_pkey;
	Datum			new_pkey;
	bool			old_isnull;
	bool			new_isnull;
	Oid				relid;
	uint64			old_packed;
	uint64			new_packed;
	dsa_pointer		new_payload_dp;
	dsa_pointer		old_payload_dp;
	bool			found;

	if (RowCacheCtl == NULL)
		return;
	if (rel == NULL || oldtup == NULL || oldtup->t_data == NULL)
		return;
	if (newtup == NULL || newtup->t_data == NULL)
		return;

	relid = RelationGetRelid(rel);
	rm = FindRelMeta(relid);
	if (rm == NULL)
		return;
	if (pg_atomic_read_u32(&rm->state) != RELMETA_ENABLED)
		return;
	if (rm->pkey_attno <= 0)
		return;

	old_pkey = heap_getattr(oldtup, rm->pkey_attno,
							RelationGetDescr(rel), &old_isnull);
	if (old_isnull)
		return;

	new_pkey = heap_getattr(newtup, rm->pkey_attno,
							RelationGetDescr(rel), &new_isnull);
	if (new_isnull)
	{
		/* Defensive: pkeys are NOT NULL.  If somehow null, treat the
		 * update as an invalidation of the old pkey. */
		InvalidateEntryByPkey(rm, relid, old_pkey);
		return;
	}

	old_packed = PackPkeyDatum(old_pkey, rm->pkey_typlen);
	new_packed = PackPkeyDatum(new_pkey, rm->pkey_typlen);

	/*
	 * Key-update degrade path: pkey changed.  Without an INSERT hook
	 * we cannot populate the new pkey, so the most-consistent thing
	 * we can do is evict the old entry and leave both old and new
	 * pkey to fall back to native btree.
	 */
	if (old_packed != new_packed)
	{
		InvalidateEntryByPkey(rm, relid, old_pkey);
		return;
	}

	/* Non-key-update: write-through. */
	new_payload_dp = FlattenHeapTupleToDsa(rel, newtup);
	if (!DsaPointerIsValid(new_payload_dp))
		return;					/* unlikely; flatten ereports on OOM */

	old_payload_dp = ReplaceEntryPayloadByPkey(rm, relid, old_pkey,
											   new_payload_dp, &found);

	if (found)
	{
		/* Retire old payload — no reader who already loaded it can be
		 * harmed because EBR keeps the dpts alive until safe_epoch. */
		if (DsaPointerIsValid(old_payload_dp))
			RowCacheEpochRetire(old_payload_dp,
								InvalidDsaPointer,
								InvalidDsaPointer);
	}
	else
	{
		/*
		 * Entry not in cache (concurrent eviction / never loaded).  The
		 * freshly-flattened payload was never published to any reader,
		 * so direct dsa_free is safe and avoids one EBR cycle of delay.
		 */
		dsa_free(LocalDsa, new_payload_dp);
	}
}

void
RowCacheOnHeapDelete(Relation rel, HeapTuple oldtup)
{
	InvalidateByHeapTuple(rel, oldtup);
}

/* ----------------------------------------------------------------
 * Phase 3 (4/4): VACUUM LP_UNUSED fallback
 *
 * Called by lazy_vacuum_heap_page after it converts LP_DEAD line
 * pointers to LP_UNUSED (the point at which the (block, offnum) pair
 * becomes available for INSERT to reuse).
 *
 * Why we need this hook:
 *
 *   V4's cache key is (relid, pkey), so TID reuse — the silent
 *   stale-read bug that haunts the V1/V2 TID-keyed design — is NOT a
 *   correctness problem here.  A new INSERT into a recycled (block,
 *   offnum) has a brand-new pkey; a cache lookup by that pkey misses
 *   naturally (no entry exists; INSERT hook is intentionally absent).
 *
 *   What this hook DOES address is the secondary concern of stale
 *   cache-entry contents whose embedded HeapTupleHeader / t_self
 *   reference an (offnum) that has since been LP_UNUSED'd and possibly
 *   recycled.  Because read paths return FlatCachedTuple content
 *   directly (not via heap), this is not a correctness risk either —
 *   but the cached row is no longer reachable through the live heap,
 *   so its cache occupancy is effectively dead weight.
 *
 *   Bumping rel_gen makes every existing entry of this relation appear
 *   stale to future readers (rel_gen_at_load mismatch); they fall back
 *   to native btree.  Physical reclamation is deferred to the next
 *   explicit Drop, or to future LRU eviction.  This is the same
 *   "simple but heavy" design the V4 doc calls out.
 *
 * Cost / call shape:
 *   - One sticky relmeta lookup + one atomic_load + (if cached) one
 *     fetch_add_u64.  Tens of ns; well under the latency of one
 *     lazy_vacuum_heap_page invocation.
 *   - Idempotent: bumping rel_gen multiple times during a single
 *     VACUUM is fine, even desirable (each bump shifts the cutoff
 *     forward so concurrent readers re-evaluate).
 *
 * Caller contract (vacuumlazy.c):
 *   - Must be called only when nunused > 0 (page actually produced
 *     LP_UNUSED items).  Caller already gates on this.
 *   - Must be called AFTER END_CRIT_SECTION — fetch_add is safe in
 *     crit section but the sticky-cache update path would not be.
 *
 * Out of scope (deliberately not hooked):
 *   - heap_page_prune: HOT-chain compression rarely produces LP_UNUSED
 *     (mostly LP_REDIRECT).  When it does, the next VACUUM picks up
 *     the slack.  Adding the hook here would invalidate cache on
 *     ordinary SELECTs that trigger opportunistic pruning, which is
 *     too aggressive.
 *   - Toast vacuum: toast rels are not pkey-cached in V4 (no single
 *     byval pk).
 */
void
RowCacheOnVacuumLPUnused(Relation rel)
{
	RelMeta	   *rm;

	if (RowCacheCtl == NULL)
		return;
	if (rel == NULL)
		return;

	rm = FindRelMeta(RelationGetRelid(rel));
	if (rm == NULL)
		return;
	if (pg_atomic_read_u32(&rm->state) != RELMETA_ENABLED)
		return;

	/*
	 * Bump rel_gen.  Readers will see the new value via atomic_load on
	 * their next fetch and treat any entry with rel_gen_at_load != new
	 * value as a miss, falling back to native btree.
	 */
	(void) pg_atomic_fetch_add_u64(&rm->rel_gen, 1);
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
	bool		result = false;

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

	/*
	 * Fast-fail: out-of-EBR atomic_load of state.  Avoids paying for
	 * EpochEnter on every fetch when the relation is not cache-enabled
	 * (the overwhelming common case for non-cached tables in TPC-C-like
	 * workloads).
	 */
	if (rm == NULL ||
		pg_atomic_read_u32(&rm->state) != RELMETA_ENABLED)
		return false;

	/*
	 * Phase 4 EBR enter: from here until EpochExit we are a "reader" in
	 * EBR terms.  Any GlobalEntry / FlatCachedTuple we touch is
	 * guaranteed to remain physically backed by DSA — even if Drop /
	 * DML / VACUUM retires it concurrently, GC won't dsa_free until
	 * after we EpochExit.
	 *
	 * EpochEnter cost: 1 atomic_load (global_epoch) + 1 atomic_store
	 * (MyProc->rowcache_local_epoch) + 1 memory_barrier ≈ 3-5 ns.  No
	 * RMW, no lock, no cache-line ping-pong.
	 */
	RowCacheEpochEnter();

	/*
	 * Re-check state after EpochEnter.  Without this, a Drop that
	 * completed between our fast-fail check above and EpochEnter would
	 * be invisible: its retire epoch could be < our local_epoch, GC
	 * would dsa_free the entry, and our subsequent dsa_get_address
	 * could land on freed memory.  Re-reading state under our
	 * critical-section flag guarantees the (state, retire) ordering
	 * Drop established with its memory_barrier still applies.
	 */
	if (pg_atomic_read_u32(&rm->state) != RELMETA_ENABLED)
		goto out;
	pg_read_barrier();

	EnsureRowCacheDsa();

	packed = PackPkeyDatum(pkey_val, rm->pkey_typlen);
	hash = ComputePkeyHash(packed);
	bucket = hash & ROW_CACHE_BUCKET_MASK;

	entry = BucketLookup(bucket, relid, packed, NULL);
	if (entry == NULL)
		goto out;

	/* Validate entry: must be FRESH and same generation. */
	if (pg_atomic_read_u32(&entry->state) != ROW_CACHE_ENTRY_FRESH)
		goto out;
	if (entry->rel_gen_at_load != pg_atomic_read_u64(&rm->rel_gen))
		goto out;
	pg_read_barrier();

	if (!DsaPointerIsValid(entry->payload_dp))
		goto out;
	flat = (FlatCachedTuple *) dsa_get_address(LocalDsa, entry->payload_dp);

	/*
	 * Reconstruct an in-memory HeapTuple pointing into the flat payload
	 * so we can run the MVCC visibility check.  The TID is recovered
	 * from the embedded HeapTupleHeader (set when we flattened) for the
	 * has_hot_chain / slot fill.
	 *
	 * EBR guarantees `flat` stays valid until EpochExit even if a
	 * concurrent write-through swapped entry->payload_dp to a new
	 * block and retired this one — the retired block's dsa_free is
	 * gated on safe_epoch > our local_epoch.
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

	result = true;

out:
	RowCacheEpochExit();
	return result;
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
