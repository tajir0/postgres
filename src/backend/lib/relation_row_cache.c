#include "postgres.h"

#include "access/htup_details.h"
#include "access/tableam.h"
#include "executor/tuptable.h"
#include "lib/pkey_row_cache.h"
#include "lib/relation_row_cache.h"
#include "lib/tid_row_cache.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/bufmgr.h"
#include "storage/lwlock.h"
#include "storage/procnumber.h"
#include "storage/shmem.h"
#include "utils/dsa.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"

/* ----------------------------------------------------------------
 * Data structures
 * ---------------------------------------------------------------- */

#define ROW_CACHE_MAX_RELATIONS 128
#define ROW_CACHE_NUM_PARTITIONS 16

/*
 * Per-relation state values for RowCacheRelEntry.state.
 *
 * State transitions are only performed by Load / Drop while holding
 * rel_lock(EXCLUSIVE).  Readers only ever read this field atomically,
 * never write it.
 */
#define ROW_CACHE_STATE_UNLOADED	0
#define ROW_CACHE_STATE_LOADED		1

typedef struct RowCacheShmemControl
{
	dsa_handle	global_dsa_handle;
	LWLock		control_lock;
	LWLock		rel_hash_locks[ROW_CACHE_NUM_PARTITIONS];
} RowCacheShmemControl;

/*
 * RowCacheRelEntry: per-relation slot in the shared hash table.
 *
 * Concurrency protocol (atomic state, lock-free read path):
 *
 *   - state: LOADED / UNLOADED.  Readers only read atomically.  Load and
 *     Drop write it while holding rel_lock(EXCLUSIVE).
 *   - rel_lock: EXCLUSIVE-only, serializes Load ↔ Drop ↔ Load.  The read
 *     path never acquires it, eliminating cache-line bouncing on the
 *     hot path under high concurrency.
 *
 * IMPORTANT LIFETIME CONTRACT:
 *   The caller MUST guarantee that no concurrent readers exist while
 *   Load or Drop runs.  This matches the intended use case (read-only
 *   tables such as TPC-C ITEM: load once before the workload, drop only
 *   after every worker has finished).  Enforcing this at the application
 *   level lets us omit the per-backend pin/unpin RCU-style protocol on
 *   the hot read path — which materially improved multi-process
 *   throughput.
 *
 * Once an entry is inserted into RowCacheRelHash its memory address
 * (and the embedded rel_lock) is stable for the lifetime of the
 * postmaster: Drop never HASH_REMOVEs.  Entry addresses remain valid
 * for the lifetime of the postmaster (no dangling pointers after Drop).
 */
typedef struct RowCacheRelEntry
{
	Oid			relid;			/* hash key */
	dsa_pointer blocks_dp;		/* DSA: TidBlockEntry[nblocks], Invalid if none */
	BlockNumber nblocks;		/* length of blocks_dp; 0 if empty relation */
	LWLock		rel_lock;		/* serializes Load ↔ Drop; readers don't take it */
	pg_atomic_uint32 state;		/* ROW_CACHE_STATE_{UNLOADED,LOADED} */
	int			natts;
	/*
	 * Pkey-driven secondary index.  Valid only when pkey_attno > 0.
	 * Both fields are set under rel_lock together with blocks_dp and
	 * published with the same write barrier before state=LOADED.
	 */
	dsa_pointer pkey_idx_dp;	/* DSA: PkeyIndex, Invalid if not built */
	AttrNumber	pkey_attno;		/* 1-based attno of pkey column, 0 if none */
} RowCacheRelEntry;

/*
 * Per-heap-block cached tuple pointers.  Indexed by block number in the flat
 * blocks array (RelationGetNumberOfBlocks snapshot at load time).
 */
typedef struct TidBlockEntry
{
	int			capacity;
	dsa_pointer entries_dp;		/* → dsa_pointer[capacity] array */
} TidBlockEntry;

/* ----------------------------------------------------------------
 * Global / backend-local state
 * ---------------------------------------------------------------- */

static RowCacheShmemControl *RowCacheCtl = NULL;
static HTAB *RowCacheRelHash = NULL;
static dsa_area *LocalDsa = NULL;

/*
 * Per-backend last-used relation cache.
 *
 * Avoids the partition lock (LW_SHARED hash lookup) on repeated accesses to
 * the same relation within one backend.  Both variables are backend-local and
 * require no synchronisation.  They are invalidated whenever Load or Drop
 * change the entry (set LastCachedEntry = NULL).
 *
 * Safety: RowCacheRelHash entries are never HASH_REMOVE'd, so the pointer
 * remains valid for the lifetime of the postmaster even after a Drop.
 * The read path re-checks entry->relid and entry->state atomically, so a
 * stale cached pointer after a Drop + Load still yields a safe "not loaded"
 * outcome.
 */
static Oid				LastCachedRelid = InvalidOid;
static RowCacheRelEntry *LastCachedEntry = NULL;

/* ----------------------------------------------------------------
 * Partition lock helpers for RowCacheRelHash
 *
 * ShmemInitHash with HASH_PARTITION does NOT manage partition locks
 * internally — the caller must acquire the appropriate partition lock
 * before every hash_search call.  This is the same pattern used by
 * buf_table.c (BufMappingPartitionLock) and lock.c.
 * ---------------------------------------------------------------- */

static inline uint32
RowCacheRelHashCode(const Oid *relid)
{
	return get_hash_value(RowCacheRelHash, relid);
}

static inline LWLock *
RowCacheRelPartitionLock(uint32 hashcode)
{
	return &RowCacheCtl->rel_hash_locks[hashcode % ROW_CACHE_NUM_PARTITIONS];
}

static Size
RowCacheRelEntryAllocSize(void)
{
	return MAXALIGN(sizeof(RowCacheRelEntry));
}

/* ----------------------------------------------------------------
 * Shared-memory sizing and initialization
 * ---------------------------------------------------------------- */

Size
RowCacheShmemSize(void)
{
	Size		size = 0;

	size = add_size(size, MAXALIGN(sizeof(RowCacheShmemControl)));
	size = add_size(size, hash_estimate_size(ROW_CACHE_MAX_RELATIONS,
											 RowCacheRelEntryAllocSize()));
	return size;
}

void
RowCacheShmemInit(void)
{
	bool		found;
	HASHCTL		ctl;

	RowCacheCtl = (RowCacheShmemControl *)
		ShmemInitStruct("Row Cache Control",
						sizeof(RowCacheShmemControl),
						&found);
	if (!found)
	{
		RowCacheCtl->global_dsa_handle = DSA_HANDLE_INVALID;
		LWLockInitialize(&RowCacheCtl->control_lock, LWTRANCHE_ROW_CACHE_CTL);

		for (int i = 0; i < ROW_CACHE_NUM_PARTITIONS; i++)
			LWLockInitialize(&RowCacheCtl->rel_hash_locks[i],
							 LWTRANCHE_ROW_CACHE_RELHASH);
	}

	ctl.keysize = sizeof(Oid);
	ctl.entrysize = RowCacheRelEntryAllocSize();
	ctl.num_partitions = ROW_CACHE_NUM_PARTITIONS;

	RowCacheRelHash = ShmemInitHash("Row Cache Relation Hash",
									ROW_CACHE_MAX_RELATIONS,
									ROW_CACHE_MAX_RELATIONS,
									&ctl,
									HASH_ELEM | HASH_BLOBS | HASH_PARTITION);
}

/* ----------------------------------------------------------------
 * DSA lazy initialization
 * ---------------------------------------------------------------- */

static void
EnsureRowCacheDsa(void)
{
	MemoryContext old_ctx;

	if (LocalDsa != NULL)
		return;

	/*
	 * dsa_create() and dsa_attach() palloc the backend-local dsa_area struct
	 * in CurrentMemoryContext.  LocalDsa is a backend-local global that must
	 * outlive any single statement; allocate it in TopMemoryContext so it is
	 * not freed when a query's es_query_cxt is deleted.
	 */
	old_ctx = MemoryContextSwitchTo(TopMemoryContext);

	LWLockAcquire(&RowCacheCtl->control_lock, LW_EXCLUSIVE);

	if (RowCacheCtl->global_dsa_handle == DSA_HANDLE_INVALID)
	{
		dsa_area   *dsa = dsa_create(LWTRANCHE_ROW_CACHE_DSA);

		dsa_pin(dsa);
		dsa_pin_mapping(dsa);
		RowCacheCtl->global_dsa_handle = dsa_get_handle(dsa);
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
 * Block-level helpers
 * ---------------------------------------------------------------- */

static int
TidRowBlockInitialCapacity(OffsetNumber off)
{
	int			cap = 16;

	while (cap <= (int) off)
		cap <<= 1;
	return cap;
}

/* ----------------------------------------------------------------
 * Store one tuple into the flat per-block arrays (Load only).
 * ---------------------------------------------------------------- */

static void
RowCacheStoreTupleShared(TidBlockEntry *blocks, BlockNumber nblocks,
						 TupleTableSlot *slot)
{
	BlockNumber blockno = ItemPointerGetBlockNumberNoCheck(&slot->tts_tid);
	OffsetNumber off = ItemPointerGetOffsetNumberNoCheck(&slot->tts_tid);
	TidBlockEntry *block;
	dsa_pointer *entries_arr;
	dsa_pointer flat_dp;

	if (blockno >= nblocks)
		return;

	block = &blocks[blockno];

	if (block->capacity == 0)
	{
		int			cap = TidRowBlockInitialCapacity(off);

		block->capacity = cap;
		block->entries_dp = dsa_allocate0(LocalDsa,
										  sizeof(dsa_pointer) * cap);
	}
	else if ((int) off >= block->capacity)
	{
		int			oldcap = block->capacity;
		int			newcap = oldcap;
		dsa_pointer new_dp;
		dsa_pointer *old_arr;
		dsa_pointer *new_arr;

		while (newcap <= (int) off)
			newcap <<= 1;

		new_dp = dsa_allocate0(LocalDsa, sizeof(dsa_pointer) * newcap);
		old_arr = (dsa_pointer *) dsa_get_address(LocalDsa, block->entries_dp);
		new_arr = (dsa_pointer *) dsa_get_address(LocalDsa, new_dp);
		memcpy(new_arr, old_arr, sizeof(dsa_pointer) * oldcap);

		dsa_free(LocalDsa, block->entries_dp);
		block->entries_dp = new_dp;
		block->capacity = newcap;
	}

	flat_dp = RowCacheFlattenTuple(LocalDsa, slot);

	entries_arr = (dsa_pointer *) dsa_get_address(LocalDsa, block->entries_dp);
	if (DsaPointerIsValid(entries_arr[off]))
		dsa_free(LocalDsa, entries_arr[off]);
	entries_arr[off] = flat_dp;
}

/* ----------------------------------------------------------------
 * Destroy the flat block array + all DSA allocations for a relation
 * ---------------------------------------------------------------- */

static void
RowCacheDestroyBlocks(RowCacheRelEntry *entry)
{
	TidBlockEntry *blocks;
	BlockNumber blk;

	EnsureRowCacheDsa();

	if (!DsaPointerIsValid(entry->blocks_dp))
		return;

	blocks = (TidBlockEntry *) dsa_get_address(LocalDsa, entry->blocks_dp);

	for (blk = 0; blk < entry->nblocks; blk++)
	{
		TidBlockEntry *block = &blocks[blk];

		if (DsaPointerIsValid(block->entries_dp))
		{
			dsa_pointer *arr = (dsa_pointer *)
				dsa_get_address(LocalDsa, block->entries_dp);

			for (int i = 0; i < block->capacity; i++)
			{
				if (DsaPointerIsValid(arr[i]))
					dsa_free(LocalDsa, arr[i]);
			}
			dsa_free(LocalDsa, block->entries_dp);
			block->entries_dp = InvalidDsaPointer;
			block->capacity = 0;
		}
	}

	dsa_free(LocalDsa, entry->blocks_dp);
	entry->blocks_dp = InvalidDsaPointer;
	entry->nblocks = 0;
}

/* ----------------------------------------------------------------
 * MVCC visibility (same logic as the old demo, pure in-memory check)
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
 * Internal lookup: find FlatCachedTuple by (relid, tid)
 * ---------------------------------------------------------------- */

static FlatCachedTuple *
RowCacheLookupFlat(RowCacheRelEntry *entry, ItemPointer tid)
{
	TidBlockEntry *blocks;
	TidBlockEntry *block;
	dsa_pointer *entries_arr;
	BlockNumber blockno;
	OffsetNumber off;

	EnsureRowCacheDsa();

	blockno = ItemPointerGetBlockNumberNoCheck(tid);
	off = ItemPointerGetOffsetNumberNoCheck(tid);

	if (!DsaPointerIsValid(entry->blocks_dp) || blockno >= entry->nblocks)
		return NULL;

	blocks = (TidBlockEntry *) dsa_get_address(LocalDsa, entry->blocks_dp);
	block = &blocks[blockno];

	if ((int) off >= block->capacity || !DsaPointerIsValid(block->entries_dp))
		return NULL;

	entries_arr = (dsa_pointer *) dsa_get_address(LocalDsa, block->entries_dp);
	if (!DsaPointerIsValid(entries_arr[off]))
		return NULL;

	return (FlatCachedTuple *) dsa_get_address(LocalDsa, entries_arr[off]);
}

/* ----------------------------------------------------------------
 * Public API: Load
 *
 * Lock order: partition_lock → rel_lock (never reverse).
 *
 * Concurrency: state=UNLOADED is written first (via pg_atomic_write_u32)
 * so any reader that samples state afterwards bails out.  Once rebuilding
 * finishes, a write barrier is issued before state=LOADED to publish
 * blocks_dp, nblocks, and natts to readers that see the new state.
 *
 * Caller contract: no readers may be executing concurrently with Load.
 * (See RowCacheRelEntry for the full lifetime contract.)
 * ---------------------------------------------------------------- */

void
RelationRowCacheLoadRelation(Relation rel)
{
	Oid			relid = RelationGetRelid(rel);
	RowCacheRelEntry *entry;
	bool		found;
	uint32		hashcode;
	LWLock	   *partlock;
	BlockNumber nblocks;
	TidBlockEntry *blocks;
	dsa_pointer blocks_dp;

	if (RowCacheRelHash == NULL)
		elog(ERROR, "row cache shared memory not initialized");

	EnsureRowCacheDsa();

	hashcode = RowCacheRelHashCode(&relid);
	partlock = RowCacheRelPartitionLock(hashcode);

	LWLockAcquire(partlock, LW_EXCLUSIVE);

	entry = hash_search_with_hash_value(RowCacheRelHash, &relid, hashcode,
										HASH_ENTER, &found);
	if (!found)
	{
		LWLockInitialize(&entry->rel_lock, LWTRANCHE_ROW_CACHE_REL);
		pg_atomic_init_u32(&entry->state, ROW_CACHE_STATE_UNLOADED);
		entry->blocks_dp = InvalidDsaPointer;
		entry->nblocks = 0;
		entry->natts = 0;
		entry->pkey_idx_dp = InvalidDsaPointer;
		entry->pkey_attno = 0;
	}

	/* Acquire rel_lock while still holding partition lock to prevent a
	 * concurrent Drop from sneaking in between the hash lookup and our
	 * serialisation lock. */
	LWLockAcquire(&entry->rel_lock, LW_EXCLUSIVE);
	LWLockRelease(partlock);

	/*
	 * If a previous load exists, tear it down.  The caller contract
	 * guarantees no readers are inside the old block array, so we do not
	 * need to drain pin counts.
	 *
	 *   1. Flip state=UNLOADED so any late reader bails out on the
	 *      atomic state check.
	 *   2. Publish the state change with a memory barrier before freeing.
	 *   3. Destroy the old flat block array.
	 */
	if (pg_atomic_read_u32(&entry->state) == ROW_CACHE_STATE_LOADED)
	{
		pg_atomic_write_u32(&entry->state, ROW_CACHE_STATE_UNLOADED);
		pg_memory_barrier();

		if (DsaPointerIsValid(entry->blocks_dp))
			RowCacheDestroyBlocks(entry);
		if (DsaPointerIsValid(entry->pkey_idx_dp))
		{
			RowCachePkeyIndexFree(LocalDsa, entry->pkey_idx_dp);
			entry->pkey_idx_dp = InvalidDsaPointer;
			entry->pkey_attno = 0;
		}
	}

	nblocks = RelationGetNumberOfBlocks(rel);

	if (nblocks > 0)
	{
		Size		bytes = mul_size(sizeof(TidBlockEntry), (Size) nblocks);

		blocks_dp = dsa_allocate0(LocalDsa, bytes);
		if (!DsaPointerIsValid(blocks_dp))
			ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory"),
					 errdetail_internal("row cache: cannot allocate TidBlockEntry array.")));
		blocks = (TidBlockEntry *) dsa_get_address(LocalDsa, blocks_dp);
	}
	else
	{
		blocks_dp = InvalidDsaPointer;
		blocks = NULL;
	}

	entry->natts = RelationGetDescr(rel)->natts;

	{
		TableScanDesc scan;
		TupleTableSlot *slot;
		bool		pushed_snapshot = false;
		dsa_pointer pkey_idx_dp = InvalidDsaPointer;
		AttrNumber	pkey_attno = 0;
		uint64		row_estimate;

		if (!ActiveSnapshotSet())
		{
			PushActiveSnapshot(GetTransactionSnapshot());
			pushed_snapshot = true;
		}

		/*
		 * Sizing the pkey index.  Prefer rel->rd_rel->reltuples when
		 * positive (post-ANALYZE estimate); otherwise fall back to a
		 * crude upper bound from the heap-page count (best-effort).
		 * Over-sizing is harmless; under-sizing degrades to long probe
		 * chains but never fails (we cap at PKEY_MAX_BUCKETS internally).
		 */
		row_estimate = (rel->rd_rel->reltuples > 0)
			? (uint64) rel->rd_rel->reltuples
			: (uint64) nblocks * 100;

		pkey_idx_dp = RowCachePkeyIndexAlloc(rel, LocalDsa,
											 row_estimate, &pkey_attno);

		scan = table_beginscan(rel, GetActiveSnapshot(), 0, NULL);
		slot = table_slot_create(rel, NULL);

		while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
		{
			RowCacheStoreTupleShared(blocks, nblocks, slot);

			/*
			 * Build the pkey -> TID mapping.  Pkey columns are NOT NULL
			 * by definition; we still defensively check tts_isnull[]
			 * before treating the Datum as a valid key.
			 */
			if (DsaPointerIsValid(pkey_idx_dp))
			{
				int			zb_attno = pkey_attno - 1;

				/* slot_getallattrs already invoked inside Flatten */
				if (!slot->tts_isnull[zb_attno])
					RowCachePkeyIndexInsert(LocalDsa, pkey_idx_dp,
											slot->tts_values[zb_attno],
											&slot->tts_tid);
			}
		}

		table_endscan(scan);
		ExecDropSingleTupleTableSlot(slot);

		if (pushed_snapshot)
			PopActiveSnapshot();

		entry->pkey_idx_dp = pkey_idx_dp;
		entry->pkey_attno = pkey_attno;
	}

	/*
	 * Publish: write barrier ensures blocks_dp / nblocks / natts are visible
	 * to any reader that observes state=LOADED.
	 */
	entry->blocks_dp = blocks_dp;
	entry->nblocks = nblocks;
	pg_write_barrier();
	pg_atomic_write_u32(&entry->state, ROW_CACHE_STATE_LOADED);

	/*
	 * Invalidate this backend's last-used cache.  If we just reloaded the
	 * same relation the stale pointer is still valid (entries never move),
	 * but forcing a fresh lookup on the next read ensures LastCachedEntry
	 * is always set via the slow path after a Load, which also re-populates
	 * it correctly for callers that call Load then immediately Read.
	 */
	LastCachedEntry = NULL;
	LastCachedRelid = InvalidOid;

	LWLockRelease(&entry->rel_lock);
}

/* ----------------------------------------------------------------
 * Public API: Drop
 *
 * Lock order: partition_lock(EXCLUSIVE) → rel_lock(EXCLUSIVE).
 * partition_lock is released after rel_lock is acquired (same pattern
 * as Load).  The entry is NOT removed from RowCacheRelHash: the slot
 * (and its embedded rel_lock) stays alive for the lifetime of the
 * postmaster.  Read paths take partition_lock(SHARED) on each lookup.
 *
 * Caller contract: no readers may be executing concurrently with Drop.
 * (See RowCacheRelEntry for the full lifetime contract.)
 *
 * Concurrency:
 *   1. Acquire rel_lock EXCLUSIVE (serialises with Load / other Drop).
 *   2. Flip state to UNLOADED — any late reader that samples state
 *      afterwards bails out before touching the block array.
 *   3. Publish the state change with a memory barrier before freeing.
 *   4. Destroy the block array.
 * ---------------------------------------------------------------- */

void
RelationRowCacheDropRelation(Oid relid)
{
	RowCacheRelEntry *entry;
	uint32		hashcode;
	LWLock	   *partlock;

	if (RowCacheRelHash == NULL)
		return;

	EnsureRowCacheDsa();

	hashcode = RowCacheRelHashCode(&relid);
	partlock = RowCacheRelPartitionLock(hashcode);

	LWLockAcquire(partlock, LW_EXCLUSIVE);

	entry = hash_search_with_hash_value(RowCacheRelHash, &relid, hashcode,
										HASH_FIND, NULL);
	if (entry == NULL ||
		pg_atomic_read_u32(&entry->state) != ROW_CACHE_STATE_LOADED)
	{
		LWLockRelease(partlock);
		return;
	}

	/* Lock order: partition_lock → rel_lock. */
	LWLockAcquire(&entry->rel_lock, LW_EXCLUSIVE);
	LWLockRelease(partlock);

	/*
	 * Double-check state under rel_lock: another Drop may have raced with
	 * us and already torn the cache down.
	 */
	if (pg_atomic_read_u32(&entry->state) == ROW_CACHE_STATE_LOADED)
	{
		/* Publish UNLOADED before freeing. */
		pg_atomic_write_u32(&entry->state, ROW_CACHE_STATE_UNLOADED);
		pg_memory_barrier();

		if (DsaPointerIsValid(entry->blocks_dp))
			RowCacheDestroyBlocks(entry);
		if (DsaPointerIsValid(entry->pkey_idx_dp))
		{
			RowCachePkeyIndexFree(LocalDsa, entry->pkey_idx_dp);
			entry->pkey_idx_dp = InvalidDsaPointer;
			entry->pkey_attno = 0;
		}
	}

	/*
	 * Invalidate this backend's last-used cache so the next read goes
	 * through the slow path and gets the correct (unloaded) state.
	 */
	LastCachedEntry = NULL;
	LastCachedRelid = InvalidOid;

	LWLockRelease(&entry->rel_lock);
}

/* ----------------------------------------------------------------
 * Public API: FillSlot (simple cache lookup + slot fill)
 *
 * Read-path concurrency (no rel_lock, no pin!):
 *   1. Locate the entry (backend-local fast path, or partition_lock(SHARED)
 *      + HASH_FIND on miss).
 *   2. Verify entry->relid matches and state == LOADED.  Both checks are
 *      atomic scalar reads — no RMWs, no locks.
 *   3. Access the flat block array / flat tuple.
 *
 * Safety relies on the caller contract (see RowCacheRelEntry): Load and
 * Drop never run concurrently with readers, so the block array cannot be
 * freed under our feet.
 * ---------------------------------------------------------------- */

/*
 * RowCacheLookupRelEntry -- locate the hash entry for a relation.
 *
 * Hot path: if this backend already looked up the same relid in a prior call
 * the entry pointer is cached in LastCachedEntry, bypassing the partition lock
 * entirely.  The cached pointer is valid for the lifetime of the postmaster
 * (entries are never HASH_REMOVE'd), so no additional lifetime check is needed
 * here; callers validate entry->relid and entry->state atomically before use.
 */
static RowCacheRelEntry *
RowCacheLookupRelEntry(Oid relid)
{
	RowCacheRelEntry *entry;
	uint32		hashcode;
	LWLock	   *partlock;

	/* Fast path: same relation as last lookup — skip partition lock. */
	if (relid == LastCachedRelid && LastCachedEntry != NULL)
		return LastCachedEntry;

	/* Slow path: partition-locked hash lookup. */
	hashcode = RowCacheRelHashCode(&relid);
	partlock = RowCacheRelPartitionLock(hashcode);

	LWLockAcquire(partlock, LW_SHARED);
	entry = hash_search_with_hash_value(RowCacheRelHash, &relid,
										hashcode, HASH_FIND, NULL);
	LWLockRelease(partlock);

	/* Cache the result (even NULL, but only if OidIsValid to avoid confusion). */
	if (entry != NULL)
	{
		LastCachedRelid = relid;
		LastCachedEntry = entry;
	}

	return entry;
}

/*
 * Validate a looked-up entry for the read path.  Returns true if the
 * entry is loaded for the requested relid.  Both checks are plain
 * atomic/scalar reads — no RMWs, no locks.
 */
static inline bool
RowCacheEntryIsLoadedFor(RowCacheRelEntry *entry, Oid relid)
{
	return entry->relid == relid &&
		pg_atomic_read_u32(&entry->state) == ROW_CACHE_STATE_LOADED;
}

bool
RelationRowCacheFillSlot(TupleTableSlot *slot)
{
	RowCacheRelEntry *entry;
	FlatCachedTuple *flat;
	Oid			relid;

	Assert(slot != NULL);
	if (RowCacheRelHash == NULL)
		return false;
	if (!OidIsValid(slot->tts_tableOid) || !ItemPointerIsValid(&slot->tts_tid))
		return false;

	relid = slot->tts_tableOid;

	entry = RowCacheLookupRelEntry(relid);
	if (entry == NULL || !RowCacheEntryIsLoadedFor(entry, relid))
		return false;

	flat = RowCacheLookupFlat(entry, &slot->tts_tid);
	if (flat == NULL)
		return false;

	return RowCacheUnflattenToSlot(flat, slot->tts_tableOid,
								   &slot->tts_tid, slot);
}

/* ----------------------------------------------------------------
 * Public API: FetchWithVisibility (MVCC-aware cache lookup)
 *
 * Same lock protocol as FillSlot.
 * ---------------------------------------------------------------- */

bool
RelationRowCacheFetchWithVisibility(Oid relid,
									ItemPointer tid,
									Snapshot snapshot,
									TupleTableSlot *slot,
									bool *is_visible,
									bool *has_hot_chain)
{
	RowCacheRelEntry *entry;
	FlatCachedTuple *flat;
	HeapTupleData htup;

	Assert(slot != NULL && is_visible != NULL && has_hot_chain != NULL);
	*is_visible = false;
	*has_hot_chain = false;

	if (RowCacheRelHash == NULL)
		return false;
	if (!OidIsValid(relid) || !ItemPointerIsValid(tid))
		return false;

	entry = RowCacheLookupRelEntry(relid);
	if (entry == NULL || !RowCacheEntryIsLoadedFor(entry, relid))
		return false;

	flat = RowCacheLookupFlat(entry, tid);
	if (flat == NULL)
		return false;

	htup.t_data = (HeapTupleHeader) FLAT_TUPLE_HTUP_DATA(flat);
	htup.t_len = flat->htup_len;
	htup.t_tableOid = relid;
	ItemPointerCopy(tid, &htup.t_self);

	*has_hot_chain = HeapTupleIsHotUpdated(&htup);
	*is_visible = RowCacheTupleVisibleMVCC(&htup, snapshot);

	if (*is_visible)
		RowCacheUnflattenToSlot(flat, relid, tid, slot);

	return true;
}

/* ----------------------------------------------------------------
 * Public API: PkeyFetch -- pkey-driven fast path
 *
 * Looks up `pkey_val` in the relation's PkeyIndex (if any) to obtain a
 * TID, then performs the same MVCC visibility + unflatten dance as
 * FetchWithVisibility.
 *
 * Returns true if a matching cache entry exists (regardless of
 * visibility); the caller must inspect *is_visible to decide whether
 * to use `slot`.  Returns false when:
 *   - The relation has no cache entry / not loaded
 *   - The relation has no pkey index built (composite, non-byval, etc.)
 *   - The pkey value is not present in the index
 * In all "false" cases the caller should fall back to the regular
 * IndexScan path (which still benefits from the TID-cache hook in
 * table_index_fetch_tuple).
 *
 * Concurrency: identical to FetchWithVisibility — pure reads, no
 * locks.  Caller contract requires that no concurrent Load/Drop runs.
 * ---------------------------------------------------------------- */
bool
RelationRowCachePkeyFetch(Oid relid,
						  Datum pkey_val,
						  Snapshot snapshot,
						  TupleTableSlot *slot,
						  bool *is_visible,
						  bool *has_hot_chain)
{
	RowCacheRelEntry *entry;
	ItemPointerData tid;
	FlatCachedTuple *flat;
	HeapTupleData htup;

	Assert(slot != NULL && is_visible != NULL && has_hot_chain != NULL);
	*is_visible = false;
	*has_hot_chain = false;

	if (RowCacheRelHash == NULL)
		return false;
	if (!OidIsValid(relid))
		return false;
	if (!IsMVCCSnapshot(snapshot))
		return false;

	entry = RowCacheLookupRelEntry(relid);
	if (entry == NULL || !RowCacheEntryIsLoadedFor(entry, relid))
		return false;
	if (!DsaPointerIsValid(entry->pkey_idx_dp))
		return false;

	if (!RowCachePkeyIndexLookup(LocalDsa, entry->pkey_idx_dp,
								 pkey_val, &tid))
		return false;

	flat = RowCacheLookupFlat(entry, &tid);
	if (flat == NULL)
		return false;

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

/*
 * Return the 1-based pkey attno if a pkey index is currently loaded for
 * the given relation, or 0 otherwise.  Cheap read-only check intended
 * for executor plan-init to decide whether to wire up the fast path.
 */
AttrNumber
RelationRowCachePkeyAttno(Oid relid)
{
	RowCacheRelEntry *entry;

	if (RowCacheRelHash == NULL || !OidIsValid(relid))
		return 0;

	entry = RowCacheLookupRelEntry(relid);
	if (entry == NULL || !RowCacheEntryIsLoadedFor(entry, relid))
		return 0;
	if (!DsaPointerIsValid(entry->pkey_idx_dp))
		return 0;

	return entry->pkey_attno;
}
