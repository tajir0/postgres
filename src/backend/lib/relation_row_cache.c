#include "postgres.h"

#include "access/htup_details.h"
#include "access/tableam.h"
#include "executor/tuptable.h"
#include "lib/dshash.h"
#include "lib/relation_row_cache.h"
#include "lib/tid_row_cache.h"
#include "storage/lwlock.h"
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

typedef struct RowCacheShmemControl
{
	dsa_handle	global_dsa_handle;
	LWLock		control_lock;
	LWLock		rel_hash_locks[ROW_CACHE_NUM_PARTITIONS];
} RowCacheShmemControl;

typedef struct RowCacheRelEntry
{
	Oid			relid;			/* hash key */
	dshash_table_handle tid_hash_handle;
	LWLock		rel_lock;
	int			natts;
	bool		loaded;
} RowCacheRelEntry;

typedef struct TidBlockEntry
{
	BlockNumber blockno;		/* hash key */
	int			capacity;
	dsa_pointer entries_dp;		/* → dsa_pointer[capacity] array */
} TidBlockEntry;

typedef struct LocalRelAttachEntry
{
	Oid			relid;			/* hash key */
	dshash_table_handle attach_handle;	/* must match RowCacheRelEntry */
	dshash_table *tid_hash;
} LocalRelAttachEntry;

/* ----------------------------------------------------------------
 * Global / backend-local state
 * ---------------------------------------------------------------- */

static RowCacheShmemControl *RowCacheCtl = NULL;
static HTAB *RowCacheRelHash = NULL;
static dsa_area *LocalDsa = NULL;
static HTAB *LocalAttachCache = NULL;

/*
 * Backend-local pointer cache: relid → RowCacheRelEntry *
 *
 * RowCacheRelEntry slots in ShmemInitHash are pre-allocated and never
 * physically moved, so the pointer is stable as long as the entry exists
 * in the hash table.  On the hot path we skip partition_lock entirely
 * and jump straight to rel_lock.  A stale entry is detected under
 * rel_lock by re-checking entry->relid and entry->loaded.
 */
typedef struct LocalRelPtrEntry
{
	Oid				 relid;		/* hash key */
	RowCacheRelEntry *entry;	/* pointer into ShmemInitHash (stable) */
} LocalRelPtrEntry;

static HTAB *LocalRelPtrCache = NULL;

/* ----------------------------------------------------------------
 * dshash parameters for the inner (BlockNumber → TidBlockEntry) hash
 * ---------------------------------------------------------------- */

static const dshash_parameters tid_block_dsh_params = {
	.key_size = sizeof(BlockNumber),
	.entry_size = sizeof(TidBlockEntry),
	.compare_function = dshash_memcmp,
	.hash_function = dshash_memhash,
	.copy_function = dshash_memcpy,
	.tranche_id = LWTRANCHE_ROW_CACHE_HASH
};

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

/* ----------------------------------------------------------------
 * Shared-memory sizing and initialization
 * ---------------------------------------------------------------- */

Size
RowCacheShmemSize(void)
{
	Size		size = 0;

	size = add_size(size, MAXALIGN(sizeof(RowCacheShmemControl)));
	size = add_size(size, hash_estimate_size(ROW_CACHE_MAX_RELATIONS,
											 sizeof(RowCacheRelEntry)));
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
	ctl.entrysize = sizeof(RowCacheRelEntry);
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
 * Backend-local attach cache (avoids repeated dshash_attach)
 * ---------------------------------------------------------------- */

static void
EnsureLocalAttachCache(void)
{
	HASHCTL		ctl;

	if (LocalAttachCache != NULL)
		return;

	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(LocalRelAttachEntry);
	/*
	 * Must allocate in TopMemoryContext: this table outlives any single
	 * statement/portal.  Otherwise CurrentMemoryContext may be freed between
	 * calls (e.g. pg_drop after a prior query), leaving LocalAttachCache
	 * dangling and causing segfaults in LocalAttachInvalidate.
	 */
	ctl.hcxt = TopMemoryContext;
	LocalAttachCache = hash_create("Row Cache Local Attach",
								   32, &ctl,
								   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static dshash_table *
LocalAttachGetOrCreate(Oid relid, RowCacheRelEntry *entry)
{
	LocalRelAttachEntry *local;
	bool		found;

	EnsureLocalAttachCache();
	EnsureRowCacheDsa();

	local = hash_search(LocalAttachCache, &relid, HASH_ENTER, &found);

	/*
	 * Another backend may have destroyed/replaced the inner dshash while we
	 * still hold a stale dshash_table *.  dshash_attach/find Assert on magic
	 * if we use the old pointer — treat handle mismatch like a cache miss.
	 *
	 * For HASH_ENTER on a brand-new key, value fields are uninitialized; do
	 * not call dshash_detach on garbage tid_hash.
	 */
	if (found)
	{
		if (local->tid_hash != NULL &&
			(entry->tid_hash_handle != local->attach_handle ||
			 entry->tid_hash_handle == DSHASH_HANDLE_INVALID))
		{
			dshash_detach(local->tid_hash);
			local->tid_hash = NULL;
			local->attach_handle = DSHASH_HANDLE_INVALID;
		}
	}
	else
	{
		local->tid_hash = NULL;
		local->attach_handle = DSHASH_HANDLE_INVALID;
	}

	if (local->tid_hash != NULL)
		return local->tid_hash;

	{
		/*
		 * dshash_attach() pallocs the backend-local dshash_table struct in
		 * CurrentMemoryContext.  Since local->tid_hash is stored in
		 * LocalAttachCache (TopMemoryContext), the struct must also live in
		 * TopMemoryContext; otherwise the query's es_query_cxt will be freed
		 * at statement end, leaving a dangling pointer that trips
		 * Assert(magic == DSHASH_MAGIC) on the next access.
		 */
		MemoryContext old_ctx = MemoryContextSwitchTo(TopMemoryContext);

		local->tid_hash = dshash_attach(LocalDsa, &tid_block_dsh_params,
										entry->tid_hash_handle, NULL);
		MemoryContextSwitchTo(old_ctx);
	}
	local->attach_handle = entry->tid_hash_handle;
	return local->tid_hash;
}

static void
LocalAttachInvalidate(Oid relid)
{
	LocalRelAttachEntry *local;

	if (LocalAttachCache == NULL)
		return;

	local = hash_search(LocalAttachCache, &relid, HASH_FIND, NULL);
	if (local != NULL && local->tid_hash != NULL)
	{
		dshash_detach(local->tid_hash);
		local->tid_hash = NULL;
		local->attach_handle = DSHASH_HANDLE_INVALID;
	}
	hash_search(LocalAttachCache, &relid, HASH_REMOVE, NULL);
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
	if (LocalRelPtrCache == NULL)
		return;
	hash_search(LocalRelPtrCache, &relid, HASH_REMOVE, NULL);
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
 * Store one tuple into the inner dshash
 * ---------------------------------------------------------------- */

static void
RowCacheStoreTupleShared(dshash_table *tid_hash, TupleTableSlot *slot)
{
	BlockNumber blockno = ItemPointerGetBlockNumberNoCheck(&slot->tts_tid);
	OffsetNumber off = ItemPointerGetOffsetNumberNoCheck(&slot->tts_tid);
	TidBlockEntry *block;
	dsa_pointer *entries_arr;
	dsa_pointer flat_dp;
	bool		found;

	block = dshash_find_or_insert(tid_hash, &blockno, &found);

	if (!found)
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

	dshash_release_lock(tid_hash, block);
}

/* ----------------------------------------------------------------
 * Destroy the inner dshash + all DSA allocations for a relation
 * ---------------------------------------------------------------- */

static void
RowCacheDestroyTidHash(RowCacheRelEntry *entry)
{
	dshash_table *tid_hash;
	TidBlockEntry *block;
	dshash_seq_status seq;

	EnsureRowCacheDsa();

	tid_hash = dshash_attach(LocalDsa, &tid_block_dsh_params,
							 entry->tid_hash_handle, NULL);

	dshash_seq_init(&seq, tid_hash, true);
	while ((block = dshash_seq_next(&seq)) != NULL)
	{
		dsa_pointer *arr = (dsa_pointer *)
			dsa_get_address(LocalDsa, block->entries_dp);

		for (int i = 0; i < block->capacity; i++)
		{
			if (DsaPointerIsValid(arr[i]))
				dsa_free(LocalDsa, arr[i]);
		}
		dsa_free(LocalDsa, block->entries_dp);
	}
	dshash_seq_term(&seq);

	dshash_destroy(tid_hash);
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
 * Caller must hold rel_lock LW_SHARED.
 * ---------------------------------------------------------------- */

static FlatCachedTuple *
RowCacheLookupFlat(RowCacheRelEntry *entry, ItemPointer tid)
{
	dshash_table *tid_hash;
	TidBlockEntry *block;
	dsa_pointer *entries_arr;
	BlockNumber blockno;
	OffsetNumber off;

	tid_hash = LocalAttachGetOrCreate(entry->relid, entry);

	blockno = ItemPointerGetBlockNumberNoCheck(tid);
	off = ItemPointerGetOffsetNumberNoCheck(tid);

	block = dshash_find(tid_hash, &blockno, false);
	if (block == NULL)
		return NULL;

	if ((int) off >= block->capacity)
	{
		dshash_release_lock(tid_hash, block);
		return NULL;
	}

	entries_arr = (dsa_pointer *) dsa_get_address(LocalDsa, block->entries_dp);
	if (!DsaPointerIsValid(entries_arr[off]))
	{
		dshash_release_lock(tid_hash, block);
		return NULL;
	}

	{
		FlatCachedTuple *flat = (FlatCachedTuple *)
			dsa_get_address(LocalDsa, entries_arr[off]);

		dshash_release_lock(tid_hash, block);
		return flat;
	}
}

/* ----------------------------------------------------------------
 * Public API: Load
 *
 * Lock order: partition_lock → rel_lock (never reverse).
 * We hold the partition lock only long enough for HASH_ENTER +
 * rel_lock acquisition, then release it so readers on the same
 * partition are not blocked during the (potentially long) table scan.
 * ---------------------------------------------------------------- */

void
RelationRowCacheLoadRelation(Relation rel)
{
	Oid			relid = RelationGetRelid(rel);
	RowCacheRelEntry *entry;
	dshash_table *tid_hash;
	bool		found;
	uint32		hashcode;
	LWLock	   *partlock;

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
		entry->tid_hash_handle = DSHASH_HANDLE_INVALID;
		entry->loaded = false;
		entry->natts = 0;
	}

	/* Acquire rel_lock while still holding partition lock to prevent a
	 * concurrent Drop from removing the entry before we pin it. */
	LWLockAcquire(&entry->rel_lock, LW_EXCLUSIVE);
	LWLockRelease(partlock);

	/* Drop stale backend-local dshash pointers before destroying shared hash. */
	LocalAttachInvalidate(relid);

	if (entry->loaded && entry->tid_hash_handle != DSHASH_HANDLE_INVALID)
		RowCacheDestroyTidHash(entry);

	tid_hash = dshash_create(LocalDsa, &tid_block_dsh_params, NULL);
	entry->tid_hash_handle = dshash_get_hash_table_handle(tid_hash);
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
			RowCacheStoreTupleShared(tid_hash, slot);

		table_endscan(scan);
		ExecDropSingleTupleTableSlot(slot);

		if (pushed_snapshot)
			PopActiveSnapshot();
	}

	entry->loaded = true;

	dshash_detach(tid_hash);
	LWLockRelease(&entry->rel_lock);

	LocalAttachInvalidate(relid);
}

/* ----------------------------------------------------------------
 * Public API: Drop
 *
 * Lock order: partition_lock(EXCLUSIVE) → rel_lock(EXCLUSIVE).
 * partition_lock is released after rel_lock is acquired (same pattern
 * as Load).  The entry is NOT removed from RowCacheRelHash: keeping the
 * slot alive ensures its embedded rel_lock is never re-initialized via
 * LWLockInitialize while another backend may hold a stale pointer to it
 * from LocalRelPtrCache.  The entry simply stays with loaded=false.
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
	if (entry == NULL || !entry->loaded)
	{
		LWLockRelease(partlock);
		return;
	}

	/* Lock order: partition_lock → rel_lock. */
	LWLockAcquire(&entry->rel_lock, LW_EXCLUSIVE);

	/*
	 * Release partition_lock before the (potentially expensive) DSA teardown.
	 * We intentionally do NOT HASH_REMOVE the entry: leaving it in the table
	 * with loaded=false means the rel_lock slot is never recycled and
	 * LWLockInitialize is never called again for this slot.  That eliminates
	 * the race between a backend holding a stale LocalRelPtrCache pointer
	 * (and about to call LWLockAcquire) and a concurrent Load that would
	 * re-initialize the same lock via LWLockInitialize.
	 *
	 * The table is bounded by ROW_CACHE_MAX_RELATIONS entries, so leaving
	 * entries in place is acceptable.
	 */
	LWLockRelease(partlock);

	LocalAttachInvalidate(relid);

	if (entry->loaded)
	{
		RowCacheDestroyTidHash(entry);
		entry->loaded = false;
		entry->tid_hash_handle = DSHASH_HANDLE_INVALID;
	}

	LWLockRelease(&entry->rel_lock);

	LocalRelPtrInvalidate(relid);
}

/* ----------------------------------------------------------------
 * Public API: FillSlot (simple cache lookup + slot fill)
 *
 * Lock order: partition_lock(SHARED) → rel_lock(SHARED).
 * Release partition_lock as soon as rel_lock is held.
 * ---------------------------------------------------------------- */

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

	/*
	 * Hot path: backend-local pointer cache hit → skip partition_lock.
	 *
	 * ShmemInitHash slots are pre-allocated and never physically moved, so
	 * the cached pointer remains valid as a memory address.  We validate it
	 * under rel_lock by re-checking entry->relid and entry->loaded; if the
	 * slot was reused for a different relation or dropped, we evict the cache
	 * entry and fall through to the cold path.
	 */
	entry = LocalRelPtrLookup(relid);
	if (entry != NULL)
	{
		LWLockAcquire(&entry->rel_lock, LW_SHARED);
		if (entry->relid != relid || !entry->loaded)
		{
			LWLockRelease(&entry->rel_lock);
			LocalRelPtrInvalidate(relid);
			entry = NULL;		/* fall through to cold path */
		}
	}

	/*
	 * Cold path: acquire partition_lock, look up the shared hash table, then
	 * cache the entry pointer for subsequent hot-path lookups.
	 */
	if (entry == NULL)
	{
		uint32		hashcode = RowCacheRelHashCode(&relid);
		LWLock	   *partlock = RowCacheRelPartitionLock(hashcode);

		LWLockAcquire(partlock, LW_SHARED);
		entry = hash_search_with_hash_value(RowCacheRelHash, &relid,
											hashcode, HASH_FIND, NULL);
		if (entry == NULL || !entry->loaded)
		{
			LWLockRelease(partlock);
			return false;
		}
		LWLockAcquire(&entry->rel_lock, LW_SHARED);
		LWLockRelease(partlock);

		if (!entry->loaded)
		{
			LWLockRelease(&entry->rel_lock);
			return false;
		}

		LocalRelPtrInsert(relid, entry);
	}

	flat = RowCacheLookupFlat(entry, &slot->tts_tid);
	if (flat == NULL)
	{
		LWLockRelease(&entry->rel_lock);
		return false;
	}

	ok = RowCacheUnflattenToSlot(flat, slot->tts_tableOid,
								 &slot->tts_tid, slot);
	LWLockRelease(&entry->rel_lock);
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

	/* Hot path: backend-local pointer cache hit → skip partition_lock */
	entry = LocalRelPtrLookup(relid);
	if (entry != NULL)
	{
		LWLockAcquire(&entry->rel_lock, LW_SHARED);
		if (entry->relid != relid || !entry->loaded)
		{
			LWLockRelease(&entry->rel_lock);
			LocalRelPtrInvalidate(relid);
			entry = NULL;
		}
	}

	/* Cold path: acquire partition_lock, look up, cache for next time */
	if (entry == NULL)
	{
		uint32		hashcode = RowCacheRelHashCode(&relid);
		LWLock	   *partlock = RowCacheRelPartitionLock(hashcode);

		LWLockAcquire(partlock, LW_SHARED);
		entry = hash_search_with_hash_value(RowCacheRelHash, &relid, hashcode,
											HASH_FIND, NULL);
		if (entry == NULL || !entry->loaded)
		{
			LWLockRelease(partlock);
			return false;
		}
		LWLockAcquire(&entry->rel_lock, LW_SHARED);
		LWLockRelease(partlock);

		if (!entry->loaded)
		{
			LWLockRelease(&entry->rel_lock);
			return false;
		}

		LocalRelPtrInsert(relid, entry);
	}

	flat = RowCacheLookupFlat(entry, tid);
	if (flat == NULL)
	{
		LWLockRelease(&entry->rel_lock);
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

	LWLockRelease(&entry->rel_lock);
	return true;
}
