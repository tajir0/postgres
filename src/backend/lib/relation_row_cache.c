/*-------------------------------------------------------------------------
 *
 * relation_row_cache.c
 *	  V4 row cache (dml_lock branch): partition-locked global hash of
 *	  (relid, pkey) -> flattened tuple.
 *
 * Architecture:
 *
 *   GlobalCache: a single DSA-resident chained hash table keyed by
 *                (relid, pkey).  Bucket heads live in a flat
 *                dsa_pointer[ROW_CACHE_HASH_BUCKETS] array ((1<<23) = 8M
 *                buckets).  Each chain element is a GlobalEntry holding
 *                the pkey, hash, payload pointer, and next_dp link.
 *
 *   RelMeta[64]: fixed shmem array for per-relation metadata
 *                (DISABLED/LOADING/ENABLED state, pkey descriptor,
 *                build_lock).  No rel_gen field; DDL invalidation flips
 *                state=DISABLED directly under the build_lock.
 *
 *   Concurrency model (dml_lock branch):
 *     - 128 LWLock partitions, one per group of buckets.
 *     - Read path: acquire LW_SHARED on the bucket's partition lock,
 *       walk the chain, copy payload, release.  Concurrent readers on
 *       any key in the same partition are fully parallel (SHARED does
 *       not block SHARED).
 *     - Write path (Load / Drop / DML invalidate): acquire LW_EXCLUSIVE
 *       on the bucket's partition lock, unlink entry, dsa_free payload
 *       and entry synchronously, release.  EX waits for all in-flight
 *       SHARED readers to drain, so dsa_free is safe without deferred GC.
 *     - Load / Drop serialized against each other via per-RelMeta
 *       build_lock (LWLock, not partition lock).
 *
 *   What is NOT in this implementation:
 *     - EBR / retire list / GC bgworker (removed in dml_lock commits 4-5)
 *     - VACUUM hook / rel_gen soft invalidation (removed in commit 6)
 *     - INSERT hook (no LRU/population strategy for new rows)
 *     - LRU eviction (future work)
 *     - Composite / byref pkey (future work)
 *
 *   Pkey limits: 1..ROW_CACHE_PKEY_MAX_ATTS pass-by-value columns.
 *   Tables with pass-by-reference pkeys are silently skipped at Load
 *   time (no error, no entries created).
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
/*
 * Bucket count: 2 << 22 = 8,388,608 (~8M).  Must be a power of two so
 * `ROW_CACHE_BUCKET_MASK = N - 1` gives a clean low-bit mask.
 *
 * Sizing rationale (Phase 5 (dml_lock) 7/8):
 *   Old value 8192 was catastrophically too small.  100-warehouse
 *   TPC-C customer table has ~3M rows; with 8192 buckets the
 *   average chain length was ~366 nodes, which turned each miss
 *   into a ~50 us cache-walk before BTree fallback (observed:
 *   load-after EXPLAIN 35x slower than load-before for a query
 *   that 99.7% missed the cache).
 *
 *   8M buckets supports a working set up to ~30M entries with
 *   average chain length < 4.  For TPC-C 1000-warehouse customer
 *   (30M rows) that's the rough upper bound; smaller workloads
 *   waste some bucket-head memory but pay no time cost.
 *
 *   Bucket-head array footprint:
 *     8M * sizeof(dsa_pointer) = 8M * 8 B = 64 MB DSA reserved.
 *   This is allocated lazily inside DSA on first Load; backends
 *   that never touch the cache pay nothing.
 *
 *   Future work (out of scope for this commit): make this a
 *   PGC_POSTMASTER GUC `row_cache.hash_buckets` so operators can
 *   tune by workload.
 *
 * CRITICAL: the parentheses around `(1 << 23)` are mandatory.
 * Without them the unparenthesised expansion of
 * `(ROW_CACHE_HASH_BUCKETS - 1)` becomes `(1 << 23 - 1)` which C
 * parses as `(1 << 22)` (operator precedence: `-` binds tighter
 * than `<<`).  That would silently halve the mask and break
 * bucket distribution.
 */
#define ROW_CACHE_HASH_BUCKETS	(1 << 23)	/* 2 << 22 = 8,388,608 */
#define ROW_CACHE_BUCKET_MASK	(ROW_CACHE_HASH_BUCKETS - 1)

StaticAssertDecl((ROW_CACHE_HASH_BUCKETS & ROW_CACHE_BUCKET_MASK) == 0,
				 "ROW_CACHE_HASH_BUCKETS must be a power of two");

/* RelMeta.state values. */
#define RELMETA_DISABLED	0
#define RELMETA_LOADING		1
#define RELMETA_ENABLED		2

/* ----------------------------------------------------------------
 * Data structures
 * ---------------------------------------------------------------- */

/*
 * RelMeta: per-relation metadata, lives in shmem (fixed array).
 *
 * Lookup is a linear scan of up to ROW_CACHE_MAX_RELATIONS slots; a sticky
 * per-backend cache short-circuits repeated lookups for the same relid.
 */

/*
 * Pkey-storage parameters.
 *
 * ROW_CACHE_PKEY_MAX_ATTS  — max # of pkey columns we accept at Load
 *                            eligibility.  TPC-C uses up to 4 columns
 *                            (bmsql_order_line); we round up to 8 for
 *                            headroom without bloating RelMeta.
 *
 * ROW_CACHE_PKEY_INLINE_BYTES — max byte length of the serialized
 *                            (concatenated) pkey we'll embed in
 *                            GlobalEntry.  32 bytes covers any of:
 *                              - 1 column int8        (8B)
 *                              - 2 columns int8       (16B)
 *                              - 3 columns int8       (24B)
 *                              - 4 columns int8       (32B)
 *                              - 4 columns int4       (16B)
 *                              - 8 columns int4       (32B)
 *                            Tables exceeding this fail eligibility
 *                            and stay un-cached.  Byref pks are out of
 *                            scope for this commit (still rejected at
 *                            Load time, same as before).
 */
#define ROW_CACHE_PKEY_MAX_ATTS		8
#define ROW_CACHE_PKEY_INLINE_BYTES	32

StaticAssertDecl(ROW_CACHE_PKEY_MAX_ATTS <= INDEX_MAX_KEYS,
				 "ROW_CACHE_PKEY_MAX_ATTS must not exceed INDEX_MAX_KEYS");

typedef struct RelMeta
{
	Oid				relid;			/* InvalidOid = unused slot */
	pg_atomic_uint32 state;			/* RELMETA_{DISABLED,LOADING,ENABLED} */
	LWLock			build_lock;		/* serializes Load / Drop; not on read path */

	/*
	 * Pkey descriptor.
	 *
	 * Phase 1 supported only single-column byval; this commit (Phase 4
	 * subset) extends to composite byval keys up to ROW_CACHE_PKEY_MAX_ATTS
	 * columns with total serialized length <= ROW_CACHE_PKEY_INLINE_BYTES.
	 *
	 * Byref pkeys remain out of scope: pkey_byvals[] is therefore always
	 * true after a successful eligibility check; the field is kept (rather
	 * than dropped) to leave a clean extension point for future byref
	 * support, and so the eligibility check can early-fail by clearing it.
	 */
	int				n_pkey_attrs;	/* 0 if not eligible / not loaded */
	AttrNumber		pkey_attnos[ROW_CACHE_PKEY_MAX_ATTS];
	int16			pkey_typlens[ROW_CACHE_PKEY_MAX_ATTS];
	bool			pkey_byvals[ROW_CACHE_PKEY_MAX_ATTS];
	int				pkey_total_len;	/* sum of pkey_typlens, <= ROW_CACHE_PKEY_INLINE_BYTES */
} RelMeta;

/*
 * GlobalEntry: one element of the chained global hash.  Lives in DSA.
 *
 * Pkey storage:
 *   pkey_len + pkey_buf hold the byte-serialised key (concatenation of
 *   per-column store_att_byval payloads in attno order).  All cache
 *   lookups compare by (relid, pkey_hash, pkey_len, memcmp(pkey_buf)).
 *
 *   Phase 4 single-col case: pkey_len == typlen (1/2/4/8 bytes), buf
 *   holds the canonical byval representation.  Tail bytes up to
 *   ROW_CACHE_PKEY_INLINE_BYTES are unused (not zeroed; equality check
 *   gates on pkey_len so stale tail bytes are harmless).
 *
 *   Future byref support would either spill pkey to DSA via a
 *   pkey_dp pointer or extend pkey_buf — both extension points are
 *   intentionally left clean here.
 */
typedef struct GlobalEntry
{
	Oid				relid;
	uint32			pkey_hash;
	uint8			pkey_len;		/* serialized length, <= ROW_CACHE_PKEY_INLINE_BYTES */
	uint8			pkey_buf[ROW_CACHE_PKEY_INLINE_BYTES];
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
	 * Phase 5 (dml_lock) 5/8: removed EBR (Epoch-Based Reclamation)
	 * shmem fields — global_epoch / safe_epoch_published / orphan_list_*.
	 * The synchronous dsa_free model installed in commits 2-3 has no
	 * use for them.  PGPROC.rowcache_local_epoch has also been removed
	 * in this commit.
	 */

	RelMeta			relmetas[ROW_CACHE_MAX_RELATIONS];
} RowCacheControl;

/* ----------------------------------------------------------------
 * Globals
 * ---------------------------------------------------------------- */

static RowCacheControl *RowCacheCtl = NULL;
static dsa_area *LocalDsa = NULL;

/*
 * Phase 5 (dml_lock) 5/8: deleted backend-local retire list +
 * RowCacheEpochRetire + RowCacheLocalGC + RowCacheLocalRetireCount.
 * Writers now dsa_free synchronously under EX partition lock
 * (commits 2-3); there is no longer a deferred-reclaim path.
 */

/*
 * Sticky per-backend cache for relmeta lookup.  Invalidated by Load/Drop
 * on the same backend so a subsequent call re-walks the array.
 * Declared up here so rowcache_relcache_callback (defined below) can
 * touch them; physically the rest of the V4 Phase 1 globals live
 * further down.
 */
static Oid			LastLookupRelid = InvalidOid;
static RelMeta	   *LastLookupRelMeta = NULL;

/* Forward declarations needed by rowcache_relcache_callback. */
static void EnsureRowCacheDsa(void);
static RelMeta *FindRelMeta(Oid relid);

/* ----------------------------------------------------------------
 * Per-backend callbacks: relcache invalidation
 *
 * Phase 5 (dml_lock) 5/8: removed the orphan-list machinery
 * (OrphanBatch struct, RowCacheExitCallbackRegistered flag,
 * RowCacheRegisterExitCallback, rowcache_backend_exit_cleanup) along
 * with the rest of the EBR retire infrastructure.  Without a
 * deferred-reclaim path there is no list to splice at backend exit.
 *
 * The relcache callback below stays — it implements DDL-driven
 * unpublish (state=DISABLED + rel_gen bump) and is independent of
 * EBR.
 * ---------------------------------------------------------------- */

static bool	RowCacheRelcacheCallbackRegistered = false;

static void rowcache_relcache_callback(Datum arg, Oid relid);

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
	 * Flip state to DISABLED.  Future readers short-circuit on the
	 * state check before walking the bucket chain; entries linger in
	 * the hash until manual Drop / re-Load / LRU eviction.
	 *
	 * Phase 5 (dml_lock) 6/8: removed the accompanying rel_gen bump.
	 * Without rel_gen there is no per-entry gen check in the read
	 * path, but DDL operations that fire this callback take
	 * AccessExclusiveLock at the relation level, which prevents any
	 * concurrent SELECT from running.  In-flight readers cannot race
	 * with DDL, so flipping state alone is sufficient.
	 */
	pg_atomic_write_u32(&rm->state, RELMETA_DISABLED);

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

/* ----------------------------------------------------------------
 * Forward declarations
 *
 * EnsureRowCacheDsa and FindRelMeta also have early forward decls
 * above (for use by rowcache_relcache_callback); these are the
 * canonical declarations.
 * ---------------------------------------------------------------- */

static RelMeta *AllocateOrFindRelMeta(Oid relid);
static bool CheckEligiblePkey(Relation rel, RelMeta *rm);
static int SerializePkeyFromSlot(TupleTableSlot *slot, RelMeta *rm,
								 uint8 *out_buf);
static int SerializePkeyFromTuple(HeapTuple tuple, TupleDesc desc,
								  RelMeta *rm, uint8 *out_buf);
static int SerializePkeyFromDatum(Datum d, RelMeta *rm, uint8 *out_buf);
static int SerializePkeyFromDatumArray(const Datum *vals, int nvals,
									   RelMeta *rm, uint8 *out_buf);
static inline uint32 ComputePkeyHashBytes(const uint8 *buf, int len);
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
	 * Lazily install the relcache invalidation callback so DDL on cached
	 * relations is observed by this backend.  Skip the cost in backends
	 * that never touch the cache.
	 *
	 * Phase 5 (dml_lock) 5/8: the backend-exit cleanup hook
	 * (RowCacheRegisterExitCallback) was removed in this commit along
	 * with the retire / orphan list machinery — there is no longer any
	 * per-backend state that needs explicit teardown at proc_exit.
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
 * Pkey eligibility / serialization (Phase 1: single-col byval only)
 * ---------------------------------------------------------------- */

/*
 * Return the 1-based heap attno of the relation's pkey column iff the
 * pkey is single-column and pass-by-value (int2/int4/int8/oid).
 * Returns 0 otherwise (composite, byref, deferrable, no pk, etc.).
 */
/*
 * Inspect rel's primary key and, if eligible, populate the pkey
 * descriptor portion of `rm`:
 *
 *   - n_pkey_attrs       — # of pkey columns (1..ROW_CACHE_PKEY_MAX_ATTS)
 *   - pkey_attnos[]      — heap attno of each pkey column, in index order
 *   - pkey_typlens[]     — attlen of each pkey column (1/2/4/8)
 *   - pkey_byvals[]      — always true (byref is out of scope this commit)
 *   - pkey_total_len     — sum of pkey_typlens, <= ROW_CACHE_PKEY_INLINE_BYTES
 *
 * Returns true on success, false on rejection.  On rejection `rm`'s
 * pkey descriptor is left in the "not eligible" state (n_pkey_attrs=0)
 * so the read path's pkey_attno checks naturally see "no pkey".
 *
 * Rejection cases:
 *   - relation has no primary key (or it's deferrable)
 *   - any pkey column is system / expression (attno <= 0)
 *   - any pkey column is byref or has weird attlen (<=0 or > 8)
 *   - more than ROW_CACHE_PKEY_MAX_ATTS columns
 *   - serialized total length > ROW_CACHE_PKEY_INLINE_BYTES
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
			/* system column / expression-key — not supported */
			index_close(pkindex, AccessShareLock);
			return false;
		}

		attr = TupleDescAttr(RelationGetDescr(rel), attno - 1);

		/* Phase 4 subset: byval-only.  Reject byref or odd attlen. */
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

	/* Commit descriptor into RelMeta. */
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

/*
 * Pkey serialization helpers (composite byval, Phase 4 subset).
 *
 * The serialized form is the byte-wise concatenation of each pkey
 * column's store_att_byval result, in attno order, with no padding
 * or length prefix between columns (length is fixed by typlen and
 * implicit in the RelMeta descriptor).
 *
 * Equality compares (pkey_len, memcmp(pkey_buf)); identical byval
 * Datums always produce identical byte sequences inside a single PG
 * instance (one endianness, one Datum width).
 *
 * Three input flavors:
 *
 *   FromSlot   — Load path; slot already has tts_values[] filled by
 *                slot_getallattrs.
 *   FromTuple  — DML hook path; tuple's t_data is buffer-mapped, use
 *                heap_getattr on each pkey column.
 *   FromDatum  — single-Datum convenience for the legacy Fetch API;
 *                returns -1 if RelMeta is not single-column (the
 *                caller is supposed to gate on n_pkey_attrs == 1).
 *
 * All three return the total serialized length on success, -1 on
 * defensive failure (null pkey column — shouldn't happen, PK columns
 * are NOT NULL — or buffer overrun which the eligibility check should
 * have prevented).
 */

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

static int
SerializePkeyFromTuple(HeapTuple tuple, TupleDesc desc, RelMeta *rm,
					   uint8 *out_buf)
{
	int			total = 0;

	for (int i = 0; i < rm->n_pkey_attrs; i++)
	{
		AttrNumber	attno = rm->pkey_attnos[i];
		int16		typlen = rm->pkey_typlens[i];
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

/*
 * Composite Datum-array flavor.  Used by the executor IndexNext gate
 * (Phase 4 (2/2)) after it has gathered one Datum per cached pkey
 * column from the scan's equality ScanKeys.
 *
 * Caller must pass nvals == rm->n_pkey_attrs and supply Datums in the
 * exact attno order recorded in rm->pkey_attnos[].  Returns -1 if those
 * preconditions don't hold or buffer would overflow.
 */
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

/*
 * Hash a serialized pkey byte buffer to a 32-bit bucket-selector.
 * Uses PG's hash_bytes (same family as hash join / hash agg, no Datum
 * boxing) which is specifically tuned for short byte sequences.
 */
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
 * Walk a bucket chain looking for an entry matching
 * (relid, pkey_hash, pkey_len, pkey_buf).
 *
 * Match criteria progressively widen the cheap-cmp filter:
 *   - relid equality (4-byte cmp)
 *   - pkey_hash equality (cheap hash collision filter)
 *   - pkey_len equality (single-byte cmp)
 *   - memcmp(pkey_buf, ..., pkey_len) for definitive equality
 *
 * Returns the entry pointer (via dsa_get_address) and stores the
 * dsa_pointer in *out_entry_dp when found; NULL otherwise.
 *
 * Phase 5 (dml_lock) lock discipline: BOTH read and write paths now
 * hold the bucket's partition lock while calling BucketLookup —
 * SHARED on the read side, EXCLUSIVE on the write side.  EBR is
 * still entered as a transitional double safety net, but the
 * partition lock alone is now sufficient to bound the lifetime of
 * any chain-walked entry / payload.
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

	/*
	 * Idempotent reload: if already ENABLED, drop the previous contents
	 * first.  This keeps Load(rel) safe to call twice without leaving
	 * duplicate entries in the global hash.
	 */
	if (pg_atomic_read_u32(&rm->state) == RELMETA_ENABLED)
	{
		pg_atomic_write_u32(&rm->state, RELMETA_DISABLED);
		pg_memory_barrier();
		DropAllEntriesForRelid(relid);
	}

	pg_atomic_write_u32(&rm->state, RELMETA_LOADING);

	if (!CheckEligiblePkey(rel, rm))
	{
		/*
		 * Composite > ROW_CACHE_PKEY_MAX_ATTS, byref, total length >
		 * ROW_CACHE_PKEY_INLINE_BYTES, or no/deferrable pk.  Leave the
		 * slot in DISABLED state; descriptor is already cleared inside
		 * CheckEligiblePkey on rejection so a future re-eligible Load
		 * finds a clean slate.
		 */
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
			continue;				/* defensive; pk columns are NOT NULL */

		pkey_hash = ComputePkeyHashBytes(pkey_buf, pkey_len);
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
 *        barrier.  Any reader that enters AFTER this point either sees
 *        state != ENABLED (early miss) or grabs an entry whose
 *        rel_gen_at_load mismatches (gen miss).
 *
 *   Reclaim (synchronous, Phase 5 dml_lock commits 2-3 of 8):
 *     4. Sweep every bucket; under partition lock EXCLUSIVE, unlink
 *        each entry whose relid matches and dsa_free its payload +
 *        entry synchronously.  No retire list, no GC bgworker.
 *     5. Clear RelMeta pkey descriptor (allows the slot to be reused
 *        by a future Load of a different relation or a re-Load with
 *        a different pkey shape).
 *     6. Invalidate per-backend sticky-lookup cache.
 *     7. Release build_lock.
 *
 * Concurrent-reader safety (Phase 5 dml_lock, commit 2/8):
 *
 * Each bucket is processed under its own LW_EXCLUSIVE partition
 * lock.  Acquire(EX) waits for all in-flight LW_SHARED readers on
 * the same partition to release; so once we hold EX, no reader is
 * walking this bucket and no reader holds a pointer into any entry
 * in this bucket's chain.  We can therefore unlink + dsa_free the
 * matching entries synchronously, inside the lock, with no need
 * for EBR retire / safe_epoch coordination.
 *
 * Readers that walked into the bucket BEFORE we acquired EX have
 * already released their SHARED on us; readers that race to acquire
 * SHARED after our EX release will walk the post-unlink chain (the
 * target entries are gone, so they either find a non-matching
 * entry or fall through to BTree fallback).
 *
 * The caller (RelationRowCacheDropRelation) has already set
 * rm->state = DISABLED + bumped rel_gen + memory_barrier before
 * calling us, which acts as a defense-in-depth unpublish: any
 * reader that enters AFTER the unpublish step short-circuits on
 * the state check before walking the chain.  But this is now
 * redundant with the partition-lock discipline — Phase 5's later
 * commits will simplify the unpublish protocol.
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
				 * Phase 5 (dml_lock) 2/8: synchronous dsa_free, no
				 * retire list.  Safe because:
				 *   - We hold partition[b] EXCLUSIVE, so no concurrent
				 *     reader can be inside the SHARED critical section
				 *     for this bucket.
				 *   - The entry was just unlinked above, so no future
				 *     reader can reach it via the chain.
				 * dsa_free is called inside the lock for simplicity;
				 * Drop is a cold path (manual SQL / DDL only) and the
				 * per-bucket EX section is already short.
				 *
				 * Phase 5 (dml_lock) 3/8: removed the
				 * ROW_CACHE_ENTRY_DELETED tombstone store.  The
				 * SHARED/EX pairing already guarantees no reader holds
				 * a pointer into a freshly-unlinked entry.
				 */
				victim_payload = e->payload_dp;
				if (DsaPointerIsValid(victim_payload))
					dsa_free(LocalDsa, victim_payload);
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
 *   - Must be called AFTER END_CRIT_SECTION — we may dsa_allocate /
 *     dsa_free which can ereport on OOM.
 *   - The "between CacheInvalidateHeapTuple and ReleaseBuffer" slot in
 *     both heap_update and heap_delete satisfies both constraints.
 * ---------------------------------------------------------------- */

/*
 * Internal: locate (relid, pkey_buf) in the global hash, unlink the entry,
 * and dsa_free payload + entry synchronously (Phase 5 dml_lock 2/8).
 * No-op if not found.
 *
 * Caller must already have determined that `rm` is the live RelMeta
 * for `relid` and that `pkey_buf` is a valid serialized pkey of length
 * `pkey_len`.
 */
static void
InvalidateEntryByPkeyBytes(RelMeta *rm, Oid relid,
						   const uint8 *pkey_buf, int pkey_len)
{
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

	pkey_hash = ComputePkeyHashBytes(pkey_buf, pkey_len);
	bucket = pkey_hash & ROW_CACHE_BUCKET_MASK;
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
			/* Unlink from chain. */
			if (DsaPointerIsValid(prev_dp))
				prev->next_dp = e->next_dp;
			else
				heads[bucket] = e->next_dp;

			victim_dp = cur_dp;
			victim_payload_dp = e->payload_dp;

			/*
			 * Phase 5 (dml_lock) 3/8: removed the
			 * ROW_CACHE_ENTRY_DELETED tombstone store.  The
			 * SHARED/EX partition lock pairing already guarantees no
			 * reader can hold a pointer into a freshly-unlinked
			 * entry — a writer holds EX while unlinking, which only
			 * proceeds after all SHARED readers have released.  The
			 * chain-unlink above prevents new readers from reaching
			 * the entry.
			 */
			break;
		}

		prev_dp = cur_dp;
		prev = e;
		cur_dp = e->next_dp;
	}

	LWLockRelease(part);

	/*
	 * Phase 5 (dml_lock) 2/8: synchronous dsa_free OUTSIDE the
	 * partition lock.  Safe for the same reason as in
	 * DropAllEntriesForRelid: writer held EX while unlinking, so no
	 * reader can have walked into `victim_dp` after the lock was
	 * acquired; readers that walked it BEFORE the EX acquire have
	 * since released their SHARED.  No EBR retire required.
	 *
	 * Keeping the free OUTSIDE the lock (instead of inside, as Drop
	 * does) shortens the DML hook's critical section — heap_update /
	 * heap_delete fire this on every row mutation, so per-row µs
	 * savings add up.
	 */
	if (DsaPointerIsValid(victim_dp))
	{
		if (DsaPointerIsValid(victim_payload_dp))
			dsa_free(LocalDsa, victim_payload_dp);
		dsa_free(LocalDsa, victim_dp);
	}
}

/*
 * Shared front-end used by RowCacheOnHeapUpdate / RowCacheOnHeapDelete.
 *
 * Steps:
 *   1. Cheap pre-checks (RowCacheCtl set, relmeta enabled, has pkey).
 *      Sticky-relmeta friendly.
 *   2. Serialize the composite (or single) pkey columns from `tuple`
 *      using rel's tuple descriptor.  Bail if any column is null
 *      (defensive; pkeys are NOT NULL by definition).
 *   3. Defer to InvalidateEntryByPkeyBytes.
 */
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

	relid = RelationGetRelid(rel);
	rm = FindRelMeta(relid);
	if (rm == NULL)
		return;
	if (pg_atomic_read_u32(&rm->state) != RELMETA_ENABLED)
		return;
	if (rm->n_pkey_attrs <= 0)
		return;

	pkey_len = SerializePkeyFromTuple(tuple, RelationGetDescr(rel),
									  rm, pkey_buf);
	if (pkey_len < 0)
		return;

	InvalidateEntryByPkeyBytes(rm, relid, pkey_buf, pkey_len);
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
 *   5. dsa_free(old_dp) — outside the lock (Phase 5 dml_lock 2/8).
 *      Safe because all readers that loaded old_dp held SHARED on
 *      this partition; our EXCLUSIVE acquire in step 2 waited for
 *      them to release, so by the time we release in step 4 no
 *      reader holds the old pointer.  Post-step-3 readers will load
 *      new_dp instead.
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
 *     is dsa_free'd directly (no reader ever observed it, so direct
 *     free is trivially safe).
 *
 *   flatten failure (OOM):
 *     RowCacheFlattenTuple ereports on dsa_allocate failure.  That's
 *     fine: heap_update is past END_CRIT_SECTION and the transaction
 *     can abort cleanly.  Cache contents remain consistent (the entry
 *     keeps its previous payload).
 *
 * NOT bumping entry_gen: readers tolerate "saw old payload after
 * writer swapped" via MVCC visibility check, exactly as for in-heap
 * stale reads.  If a future reader needs stronger "did this entry
 * get replaced under me" detection, add a pg_atomic_fetch_add_u64
 * on entry->entry_gen here.
 *
 * pg_write_barrier between the old payload_dp snapshot and the
 * publish of new payload_dp is still useful even with the partition
 * lock: it guarantees that any reader who acquires SHARED after our
 * EX release and loads new_payload_dp sees the fully-initialised
 * FlatCachedTuple at that address.  LWLock release/acquire pairing
 * provides this barrier on most architectures, but we keep the
 * explicit pg_write_barrier as belt-and-suspenders.
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
 * Find entry by (relid, pkey_buf) and atomically swap its payload_dp to
 * `new_payload_dp`.  On success returns the previous payload_dp (which
 * the caller must retire) and sets *found=true.  On miss returns
 * InvalidDsaPointer and *found=false; caller should dsa_free the
 * wasted new payload directly.
 *
 * Uses the same lock + walk discipline as InvalidateEntryByPkeyBytes.
 */
static dsa_pointer
ReplaceEntryPayloadByPkeyBytes(RelMeta *rm, Oid relid,
							   const uint8 *pkey_buf, int pkey_len,
							   dsa_pointer new_payload_dp, bool *found)
{
	uint32			pkey_hash;
	uint32			bucket;
	LWLock		   *part;
	dsa_pointer	   *heads;
	dsa_pointer		cur_dp;
	dsa_pointer		old_payload_dp = InvalidDsaPointer;

	Assert(rm != NULL && found != NULL);
	*found = false;

	EnsureRowCacheDsa();

	pkey_hash = ComputePkeyHashBytes(pkey_buf, pkey_len);
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
			e->pkey_len == pkey_len &&
			memcmp(e->pkey_buf, pkey_buf, pkey_len) == 0)
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
	Oid				relid;
	uint8			old_buf[ROW_CACHE_PKEY_INLINE_BYTES];
	uint8			new_buf[ROW_CACHE_PKEY_INLINE_BYTES];
	int				old_len;
	int				new_len;
	TupleDesc		desc;
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
	if (rm->n_pkey_attrs <= 0)
		return;

	desc = RelationGetDescr(rel);

	old_len = SerializePkeyFromTuple(oldtup, desc, rm, old_buf);
	if (old_len < 0)
		return;					/* defensive */

	new_len = SerializePkeyFromTuple(newtup, desc, rm, new_buf);
	if (new_len < 0)
	{
		/* Defensive: new pkey null shouldn't happen.  Fall back to
		 * invalidating the old. */
		InvalidateEntryByPkeyBytes(rm, relid, old_buf, old_len);
		return;
	}

	/*
	 * Key-update degrade path: pkey columns changed.  Without an INSERT
	 * hook we cannot populate the new pkey, so the most-consistent
	 * thing we can do is evict the old entry and leave both old and
	 * new pkey to fall back to native btree.  For composite pkeys we
	 * compare both length and bytes since either could change in
	 * principle (though length usually doesn't).
	 */
	if (old_len != new_len || memcmp(old_buf, new_buf, old_len) != 0)
	{
		InvalidateEntryByPkeyBytes(rm, relid, old_buf, old_len);
		return;
	}

	/* Non-key-update: write-through. */
	new_payload_dp = FlattenHeapTupleToDsa(rel, newtup);
	if (!DsaPointerIsValid(new_payload_dp))
		return;					/* unlikely; flatten ereports on OOM */

	old_payload_dp = ReplaceEntryPayloadByPkeyBytes(rm, relid,
													old_buf, old_len,
													new_payload_dp, &found);

	if (found)
	{
		/*
		 * Phase 5 (dml_lock) 2/8: synchronous dsa_free of old payload.
		 * ReplaceEntryPayloadByPkeyBytes swapped entry->payload_dp
		 * under EXCLUSIVE partition lock, so no reader can have
		 * loaded old_payload_dp after that swap (post-swap readers
		 * see new_payload_dp); pre-swap readers held SHARED and
		 * have since released, so they no longer hold the pointer.
		 * Direct free is safe.
		 */
		if (DsaPointerIsValid(old_payload_dp))
			dsa_free(LocalDsa, old_payload_dp);
	}
	else
	{
		/*
		 * Entry not in cache (concurrent eviction / never loaded).  The
		 * freshly-flattened payload was never published to any reader,
		 * so direct dsa_free is trivially safe.
		 */
		dsa_free(LocalDsa, new_payload_dp);
	}
}

void
RowCacheOnHeapDelete(Relation rel, HeapTuple oldtup)
{
	InvalidateByHeapTuple(rel, oldtup);
}

/*
 * Phase 5 (dml_lock) 6/8: removed RowCacheOnVacuumLPUnused.
 *
 * The hook bumped RelMeta.rel_gen on every LP_UNUSED transition to
 * "soft-invalidate" all cached entries for the relation, on the
 * theory that VACUUM might clear up stale-content entries the DML
 * hooks missed.  Analysis showed this was solving a phantom problem:
 *
 *   - Dead tuples never enter the cache (Load skips them; DML hooks
 *     unlink + dsa_free on UPDATE / DELETE before VACUUM ever sees
 *     the dead tuple).
 *   - pkey-keyed cache is immune to TID reuse: a new INSERT into a
 *     recycled (block,offnum) has a fresh pkey that doesn't collide
 *     with any cached entry.
 *   - VACUUM's tuple-freezing / hint-bit updates change heap layout
 *     but not pkey value or cached payload visibility.
 *
 * The hook's only observable effect was bulk soft-invalidating the
 * entire table's cache on every autovacuum (default naptime 1 min),
 * silently leaking memory of the soft-invalidated entries until the
 * next manual Drop+Load.  Net result: cache hit rate periodically
 * collapsed without anyone noticing.
 *
 * RelMeta.rel_gen and GlobalEntry.rel_gen_at_load are also deleted
 * in this commit (no remaining caller bumps rel_gen — relcache
 * callback now flips state=DISABLED only).
 */

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
	pg_memory_barrier();

	DropAllEntriesForRelid(relid);

	rm->n_pkey_attrs = 0;
	rm->pkey_total_len = 0;

	LastLookupRelMeta = NULL;
	LastLookupRelid = InvalidOid;

	LWLockRelease(&rm->build_lock);
}

/* ----------------------------------------------------------------
 * Public API: PkeyFetch / PkeyFetchComposite
 * ----------------------------------------------------------------
 *
 * Shared protocol (Phase 5 (dml_lock) commit 3/8 — EBR removed
 * from read path):
 *
 *   1. Sticky lookup -> RelMeta.
 *   2. Out-of-lock fast-fail atomic_load(state).
 *   3. Caller serializes pkey from its own input shape (single Datum,
 *      Datum[], future byref bytes, ...) and verifies n_pkey_attrs.
 *   4. Re-check state (guards against Drop-after-fast-fail).
 *   5. LWLockAcquire(partition, LW_SHARED) — bounds the read critical
 *      section against concurrent writers (DML hook / Drop / Load
 *      insert), all of which acquire LW_EXCLUSIVE on the same lock.
 *   6. Bucket lookup, validate same rel_gen.
 *   7. dsa_get_address payload, MVCC visibility check, slot fill.
 *   8. LWLockRelease(partition).
 *
 * Steps 4-8 are identical for single-col and composite paths; we
 * factor them into DoPkeyFetchBytes so the two public entry points
 * only differ in how they produce the serialized pkey buffer.
 *
 * Phase 5 lock discipline rationale: the read path now relies
 * solely on the partition lock for concurrent-writer safety.  A
 * writer that wants to unlink / dsa_free an entry must acquire
 * LW_EXCLUSIVE on the partition lock, which blocks until all
 * SHARED readers have released.  No reader can therefore observe
 * a freed entry / payload.  The previous EBR retire +
 * safe_epoch + GC bgworker machinery has been removed in
 * commits 2-5 of this branch.  The previous entry->state ==
 * FRESH check has been removed in this commit because the
 * DELETED tombstone write it guarded against is gone (writers
 * unlink + dsa_free synchronously under EX lock).
 *
 * Cost of LW_SHARED acquire/release on uncontended fast path is
 * ~30-50 ns, a small fraction of total hook overhead and paid
 * back by simpler memory management and lower steady-state DSA
 * footprint.
 */

/*
 * Inner read-side workhorse.  Caller MUST have already:
 *   - validated rm is non-null and state was ENABLED at fast-fail time
 *   - validated rm->n_pkey_attrs matches the call shape
 *   - serialized the pkey into pkey_buf[0..pkey_len-1] using
 *     SerializePkeyFromDatum / SerializePkeyFromDatumArray
 *
 * Returns true when a cache entry was found (regardless of MVCC
 * visibility).  Sets *is_visible / *has_hot_chain accordingly.
 * Returns false when:
 *   - state flipped to non-ENABLED inside EBR critical section
 *   - bucket lookup found nothing
 *   - entry state was not FRESH or rel_gen mismatch
 *   - entry payload was unexpectedly invalid
 *
 * Always exits EBR before returning.
 */
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

	/*
	 * Re-check state right before the lock acquire.  A Drop that
	 * flipped state to DISABLED between the caller's fast-fail and
	 * here will be caught; if state is still ENABLED we proceed
	 * under the partition lock, which is sufficient by itself to
	 * keep the chain-walked entry / payload alive across concurrent
	 * writers (Phase 5 dml_lock).  Even if state flips to DISABLED
	 * mid-flight, a concurrent Drop must acquire the same partition
	 * lock EXCLUSIVE to unlink entries — it will block until we
	 * release SHARED, so anything we observe under the lock is
	 * physically valid.
	 */
	if (pg_atomic_read_u32(&rm->state) != RELMETA_ENABLED)
		return false;
	pg_read_barrier();

	EnsureRowCacheDsa();

	hash = ComputePkeyHashBytes(pkey_buf, pkey_len);
	bucket = hash & ROW_CACHE_BUCKET_MASK;
	part = PartitionLockForBucket(bucket);

	/*
	 * SHARED partition lock bounds the read critical section.  Once
	 * acquired, no concurrent writer (DML hook / Drop / Load insert)
	 * can mutate this bucket chain or free its entries / payloads,
	 * because all writers acquire EXCLUSIVE on the same lock before
	 * unlink / payload swap / dsa_free.
	 *
	 * Held across BucketLookup + entry validation + payload deref +
	 * MVCC visibility check + slot fill.
	 */
	LWLockAcquire(part, LW_SHARED);

	entry = BucketLookup(bucket, relid, hash, pkey_buf, pkey_len, NULL);
	if (entry == NULL)
		goto out;

	/*
	 * Phase 5 (dml_lock) 6/8: removed the rel_gen check.  No bulk
	 * soft-invalidation channel exists anymore — VACUUM no longer
	 * touches the cache, and DDL invalidates via state=DISABLED
	 * (caught by the pre-lock fast-fail) plus AccessExclusiveLock
	 * on the relation (which prevents concurrent SELECT).
	 *
	 * The previous entry->state == FRESH check was removed in
	 * commit 3/8 (no more DELETED tombstone writes).
	 */
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

/*
 * Shared per-call entry sequence used by the two public Fetch APIs.
 * Looks up sticky RelMeta + does the out-of-EBR state fast-fail.
 * Returns the RelMeta if eligible to proceed, NULL otherwise.
 */
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

/*
 * Single-Datum entry point.  Legacy API used by the executor when the
 * cached relation has exactly one pkey column.  Returns false (no hit)
 * for composite-pkey tables; callers route composite via
 * RelationRowCachePkeyFetchComposite.
 */
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

/*
 * Composite entry point (Phase 4 (2/2)).  Caller passes one Datum per
 * pkey column in attno-order as recorded by
 * RelationRowCachePkeyDescriptor.  nvals must equal the descriptor's
 * natts; mismatch returns false.
 *
 * This is the entry point the executor's IndexNext composite dispatch
 * uses after gathering equality ScanKeys; it also serves any future
 * caller that needs to probe by multi-column pkey value.
 */
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

/* ----------------------------------------------------------------
 * Public API: PkeyAttno + PkeyDescriptor
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

	/*
	 * Legacy single-Datum dispatch path returns 0 for composite tables.
	 * Composite-aware callers should use RelationRowCachePkeyDescriptor
	 * to learn the full attno list and then dispatch via
	 * RelationRowCachePkeyFetchComposite.
	 */
	if (rm->n_pkey_attrs != 1)
		return 0;

	return rm->pkey_attnos[0];
}

/*
 * Snapshot the relation's cached pkey descriptor into caller-supplied
 * buffers.  Used by the executor IndexNext gate (Phase 4 (2/2)) to
 * decide whether to dispatch to RelationRowCachePkeyFetchComposite
 * and, if so, in what column order to assemble the Datum array.
 *
 * On success returns the number of pkey columns (>= 1) and writes
 * `out_attnos[0..return-1]` with heap attno of each column, in the
 * order the cache expects to receive Datums.
 *
 * Returns 0 when:
 *   - cache not initialised
 *   - relid invalid
 *   - no RelMeta slot for this relid
 *   - RelMeta state != ENABLED
 *   - max_attnos < cached n_pkey_attrs (caller buffer too small)
 *
 * Cheap: one sticky lookup + a handful of atomic loads + a memcpy of
 * up to 8 AttrNumbers.  Safe to call on the executor hot path.
 */
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
