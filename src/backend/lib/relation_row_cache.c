#include "postgres.h"

#include "access/htup_details.h"
#include "access/tableam.h"
#include "executor/tuptable.h"
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
 * Concurrency protocol (atomic state + pin count, lock-free read path):
 *
 *   - state: LOADED / UNLOADED.  Readers only read atomically.  Load and
 *     Drop write it while holding rel_lock(EXCLUSIVE).
 *   - pin_slots[MaxBackends]: per-backend pin depth.  Each reader only
 *     RMWs its own slot (MyProcNumber), avoiding cache-line bouncing on
 *     a global counter under multi-process load.  Drop / Load spin until
 *     every slot is zero before tearing down the DSA block array.
 *   - rel_lock: EXCLUSIVE-only, serializes Load ↔ Drop ↔ Load.  The read
 *     path never acquires it, eliminating cache-line bouncing on the
 *     hot path under high concurrency.
 *
 * Once an entry is inserted into RowCacheRelHash its memory address
 * (and the embedded rel_lock) is stable for the lifetime of the
 * postmaster: Drop never HASH_REMOVEs.  Readers may therefore cache
 * the pointer in a backend-local map (LocalRelPtrCache) without risk
 * of lock re-initialization races.
 */
typedef struct RowCacheRelEntry
{
	Oid			relid;			/* hash key */
	dsa_pointer blocks_dp;		/* DSA: TidBlockEntry[nblocks], Invalid if none */
	BlockNumber nblocks;		/* length of blocks_dp; 0 if empty relation */
	LWLock		rel_lock;		/* serializes Load ↔ Drop; readers don't take it */
	pg_atomic_uint32 state;		/* ROW_CACHE_STATE_{UNLOADED,LOADED} */
	int			natts;
	/* Per-backend pin slots follow in allocated tail; see RowCacheRelPinSlots. */
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
 * Backend-local pointer cache: relid → RowCacheRelEntry *
 *
 * RowCacheRelEntry slots in ShmemInitHash are pre-allocated and never
 * physically moved, and Drop never HASH_REMOVEs them, so the cached
 * pointer is stable for the lifetime of the postmaster.  On the hot
 * path we skip partition_lock entirely and go straight to pin+state.
 * A mismatched relid (reserved for a future recycling scheme) or
 * state != LOADED is detected by RowCachePinEntry and causes fall-back
 * to the cold path via LocalRelPtrInvalidate.
 */
typedef struct LocalRelPtrEntry
{
	Oid				 relid;		/* hash key */
	RowCacheRelEntry *entry;	/* pointer into ShmemInitHash (stable) */
} LocalRelPtrEntry;

static HTAB *LocalRelPtrCache = NULL;

/*
 * Single-slot bypass for repeated lookups of the same relation (tight loops
 * on one table).  Avoids LocalRelPtrLookup's hash_search when relid matches.
 * Cleared in LocalRelPtrInvalidate() together with the HTAB entry.
 */
static Oid			LastRelPtrRelid = InvalidOid;
static RowCacheRelEntry *LastRelPtrEntry = NULL;

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

/*
 * Hash entry allocation includes RowCacheRelEntry header plus MaxBackends
 * atomic pin slots (one cache line per backend index in the hot path).
 */
static Size
RowCacheRelEntryAllocSize(void)
{
	return add_size(MAXALIGN(sizeof(RowCacheRelEntry)),
					mul_size((Size) MaxBackends, sizeof(pg_atomic_uint32)));
}

static inline pg_atomic_uint32 *
RowCacheRelPinSlots(const RowCacheRelEntry *entry)
{
	return (pg_atomic_uint32 *) ((char *) entry + MAXALIGN(sizeof(RowCacheRelEntry)));
}

static void
RowCacheInitEntryPins(RowCacheRelEntry *entry)
{
	pg_atomic_uint32 *slots = RowCacheRelPinSlots(entry);

	for (int i = 0; i < MaxBackends; i++)
		pg_atomic_init_u32(&slots[i], 0);
}

static void
RowCacheWaitPinsDrained(RowCacheRelEntry *entry)
{
	pg_atomic_uint32 *slots = RowCacheRelPinSlots(entry);

	for (int i = 0; i < MaxBackends; i++)
		while (pg_atomic_read_u32(&slots[i]) != 0)
			pg_usleep(1);
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
 * Backend-local rel-pointer cache helpers
 * ---------------------------------------------------------------- */

static void
EnsureLocalRelPtrCache(void)
{
	HASHCTL		ctl;

	if (LocalRelPtrCache != NULL)
		return;

	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(LocalRelPtrEntry);
	ctl.hcxt = TopMemoryContext;
	LocalRelPtrCache = hash_create("Row Cache Local Rel Ptr",
								   32, &ctl,
								   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static RowCacheRelEntry *
LocalRelPtrLookup(Oid relid)
{
	LocalRelPtrEntry *local;

	if (LocalRelPtrCache == NULL)
		return NULL;
	local = hash_search(LocalRelPtrCache, &relid, HASH_FIND, NULL);
	return local ? local->entry : NULL;
}

static void
LocalRelPtrInsert(Oid relid, RowCacheRelEntry *entry)
{
	LocalRelPtrEntry *local;
	bool		found;

	EnsureLocalRelPtrCache();
	local = hash_search(LocalRelPtrCache, &relid, HASH_ENTER, &found);
	local->entry = entry;
}

static void
LocalRelPtrInvalidate(Oid relid)
{
	/* Clear single-slot even when LocalRelPtrCache is still NULL. */
	if (LocalRelPtrCache != NULL)
		hash_search(LocalRelPtrCache, &relid, HASH_REMOVE, NULL);

	if (relid == LastRelPtrRelid)
	{
		LastRelPtrRelid = InvalidOid;
		LastRelPtrEntry = NULL;
	}
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
 * so any reader that samples state afterwards bails out.  We then spin
 * until every per-backend pin slot is zero, which guarantees no reader is
 * inside the old block array when it is freed.  Once rebuilding finishes,
 * a write barrier is issued before state=LOADED to publish blocks_dp,
 * nblocks, and natts to readers that see the new state.
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
		RowCacheInitEntryPins(entry);
		entry->blocks_dp = InvalidDsaPointer;
		entry->nblocks = 0;
		entry->natts = 0;
	}

	/* Acquire rel_lock while still holding partition lock to prevent a
	 * concurrent Drop from sneaking in before we pin the entry. */
	LWLockAcquire(&entry->rel_lock, LW_EXCLUSIVE);
	LWLockRelease(partlock);

	/*
	 * If a previous load exists, tear it down safely:
	 *   1. Flip state=UNLOADED so new readers bail out.
	 *   2. Wait for in-flight readers (all per-backend pin slots zero).
	 *   3. Destroy the old flat block array — no reader can use it anymore.
	 */
	if (pg_atomic_read_u32(&entry->state) == ROW_CACHE_STATE_LOADED)
	{
		pg_atomic_write_u32(&entry->state, ROW_CACHE_STATE_UNLOADED);
		pg_memory_barrier();

		RowCacheWaitPinsDrained(entry);

		if (DsaPointerIsValid(entry->blocks_dp))
			RowCacheDestroyBlocks(entry);
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

		if (!ActiveSnapshotSet())
		{
			PushActiveSnapshot(GetTransactionSnapshot());
			pushed_snapshot = true;
		}

		scan = table_beginscan(rel, GetActiveSnapshot(), 0, NULL);
		slot = table_slot_create(rel, NULL);

		while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
			RowCacheStoreTupleShared(blocks, nblocks, slot);

		table_endscan(scan);
		ExecDropSingleTupleTableSlot(slot);

		if (pushed_snapshot)
			PopActiveSnapshot();
	}

	/*
	 * Publish: write barrier ensures blocks_dp / nblocks / natts are visible
	 * to any reader that observes state=LOADED.
	 */
	entry->blocks_dp = blocks_dp;
	entry->nblocks = nblocks;
	pg_write_barrier();
	pg_atomic_write_u32(&entry->state, ROW_CACHE_STATE_LOADED);

	LWLockRelease(&entry->rel_lock);
}

/* ----------------------------------------------------------------
 * Public API: Drop
 *
 * Lock order: partition_lock(EXCLUSIVE) → rel_lock(EXCLUSIVE).
 * partition_lock is released after rel_lock is acquired (same pattern
 * as Load).  The entry is NOT removed from RowCacheRelHash: the slot
 * (and its embedded rel_lock) stays alive for the lifetime of the
 * postmaster.  This lets readers cache RowCacheRelEntry pointers in
 * LocalRelPtrCache and know the pointer will never dangle.
 *
 * Concurrency with readers:
 *   1. Acquire rel_lock EXCLUSIVE (serialises with Load / other Drop).
 *   2. Flip state to UNLOADED — readers that sample state afterwards
 *      bail out before touching the block array.
 *   3. Spin until every per-backend pin slot is zero — no reader is inside
 *      the cached data.
 *   4. Destroy the block array; future readers will see state=UNLOADED and
 *      never observe the freed DSA data.
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
		/* Publish UNLOADED before inspecting pins. */
		pg_atomic_write_u32(&entry->state, ROW_CACHE_STATE_UNLOADED);
		pg_memory_barrier();

		/* Wait for readers that pinned before our state flip to finish. */
		RowCacheWaitPinsDrained(entry);

		if (DsaPointerIsValid(entry->blocks_dp))
			RowCacheDestroyBlocks(entry);
	}

	LWLockRelease(&entry->rel_lock);

	LocalRelPtrInvalidate(relid);
}

/* ----------------------------------------------------------------
 * Public API: FillSlot (simple cache lookup + slot fill)
 *
 * Read-path concurrency (no rel_lock!):
 *   1. Locate the entry (via LocalRelPtrCache hot path or partition_lock
 *      cold path).
 *   2. Bump this backend's pin slot (MyProcNumber) to pin the entry.
 *   3. Read state.  If not LOADED, unpin and bail.  (fetch_add on the slot
 *      is a full barrier, so the state read is ordered after the pin.)
 *   4. Access the flat block array / flat tuple.  Drop cannot free blocks
 *      while any pin slot is non-zero.
 *   5. Unpin (decrement this backend's slot).
 *
 * Since the entry slot is never recycled, pointers cached in
 * LocalRelPtrCache (and the single-slot below) are stable: we only need
 * to re-check entry->relid to protect against the (currently impossible)
 * future case where an entry might be reused for a different relation.
 *
 * Hot path order: single-slot compare → LocalRelPtrLookup hash →
 * partition_lock + shared hash find.
 * ---------------------------------------------------------------- */

static RowCacheRelEntry *
RowCacheLookupRelEntry(Oid relid)
{
	RowCacheRelEntry *entry;
	uint32		hashcode;
	LWLock	   *partlock;

	/* Fastest: last relation in this backend (no hash_search). */
	if (relid == LastRelPtrRelid && LastRelPtrEntry != NULL)
		entry = LastRelPtrEntry;
	else
	{
		entry = LocalRelPtrLookup(relid);
		LastRelPtrRelid = relid;
		LastRelPtrEntry = entry;
	}

	if (entry != NULL)
		return entry;

	/* Cold path: partition_lock SHARED + HASH_FIND, then cache pointer. */
	hashcode = RowCacheRelHashCode(&relid);
	partlock = RowCacheRelPartitionLock(hashcode);

	LWLockAcquire(partlock, LW_SHARED);
	entry = hash_search_with_hash_value(RowCacheRelHash, &relid,
										hashcode, HASH_FIND, NULL);
	LWLockRelease(partlock);

	if (entry != NULL)
	{
		LocalRelPtrInsert(relid, entry);
		LastRelPtrRelid = relid;
		LastRelPtrEntry = entry;
	}

	return entry;
}

/*
 * Pin an entry for the read path.  On success (returns true) the caller
 * must pair this with a RowCachePinReleaseEntry() before returning.
 * On failure the entry is not pinned and the caller must not access its
 * blocks_dp / nblocks / natts fields.
 */
static inline bool
RowCachePinEntry(RowCacheRelEntry *entry, Oid relid)
{
	pg_atomic_uint32 *pinslot;

	Assert(MyProcNumber >= 0 && MyProcNumber < MaxBackends);
	pinslot = &RowCacheRelPinSlots(entry)[MyProcNumber];

	pg_atomic_fetch_add_u32(pinslot, 1);

	if (entry->relid != relid ||
		pg_atomic_read_u32(&entry->state) != ROW_CACHE_STATE_LOADED)
	{
		pg_atomic_fetch_sub_u32(pinslot, 1);
		return false;
	}

	return true;
}

static inline void
RowCachePinReleaseEntry(RowCacheRelEntry *entry)
{
	Assert(MyProcNumber >= 0 && MyProcNumber < MaxBackends);
	pg_atomic_fetch_sub_u32(&RowCacheRelPinSlots(entry)[MyProcNumber], 1);
}

bool
RelationRowCacheFillSlot(TupleTableSlot *slot)
{
	RowCacheRelEntry *entry;
	FlatCachedTuple *flat;
	bool		ok;
	Oid			relid;

	Assert(slot != NULL);
	if (RowCacheRelHash == NULL)
		return false;
	if (!OidIsValid(slot->tts_tableOid) || !ItemPointerIsValid(&slot->tts_tid))
		return false;

	relid = slot->tts_tableOid;

	entry = RowCacheLookupRelEntry(relid);
	if (entry == NULL)
		return false;

	if (!RowCachePinEntry(entry, relid))
	{
		/*
		 * Entry is not loaded (or relid mismatch from a future recycling
		 * scheme).  Drop our stale local pointer so we don't hammer the
		 * same dead entry again.
		 */
		LocalRelPtrInvalidate(relid);
		return false;
	}

	flat = RowCacheLookupFlat(entry, &slot->tts_tid);
	if (flat == NULL)
	{
		RowCachePinReleaseEntry(entry);
		return false;
	}

	ok = RowCacheUnflattenToSlot(flat, slot->tts_tableOid,
								 &slot->tts_tid, slot);
	RowCachePinReleaseEntry(entry);
	return ok;
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
	if (entry == NULL)
		return false;

	if (!RowCachePinEntry(entry, relid))
	{
		LocalRelPtrInvalidate(relid);
		return false;
	}

	flat = RowCacheLookupFlat(entry, tid);
	if (flat == NULL)
	{
		RowCachePinReleaseEntry(entry);
		return false;
	}

	htup.t_data = (HeapTupleHeader) FLAT_TUPLE_HTUP_DATA(flat);
	htup.t_len = flat->htup_len;
	htup.t_tableOid = relid;
	ItemPointerCopy(tid, &htup.t_self);

	*has_hot_chain = HeapTupleIsHotUpdated(&htup);
	*is_visible = RowCacheTupleVisibleMVCC(&htup, snapshot);

	if (*is_visible)
		RowCacheUnflattenToSlot(flat, relid, tid, slot);

	RowCachePinReleaseEntry(entry);
	return true;
}
