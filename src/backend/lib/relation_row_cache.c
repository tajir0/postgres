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

typedef struct RowCacheShmemControl
{
	dsa_handle	global_dsa_handle;
	LWLock		control_lock;
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
	dshash_table *tid_hash;
} LocalRelAttachEntry;

/* ----------------------------------------------------------------
 * Global / backend-local state
 * ---------------------------------------------------------------- */

static RowCacheShmemControl *RowCacheCtl = NULL;
static HTAB *RowCacheRelHash = NULL;
static dsa_area *LocalDsa = NULL;
static HTAB *LocalAttachCache = NULL;

static inline bool
row_cache_shmem_ready(void)
{
	return RowCacheCtl != NULL && RowCacheRelHash != NULL;
}

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
	}

	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(RowCacheRelEntry);
	ctl.num_partitions = NUM_BUFFER_PARTITIONS < 16 ? NUM_BUFFER_PARTITIONS : 16;

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
	if (LocalDsa != NULL)
		return;

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
	if (found && local->tid_hash != NULL)
		return local->tid_hash;

	local->tid_hash = dshash_attach(LocalDsa, &tid_block_dsh_params,
									entry->tid_hash_handle, NULL);
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
	}
	hash_search(LocalAttachCache, &relid, HASH_REMOVE, NULL);
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
 * ---------------------------------------------------------------- */

void
RelationRowCacheLoadRelation(Relation rel)
{
	Oid			relid = RelationGetRelid(rel);
	RowCacheRelEntry *entry;
	dshash_table *tid_hash;
	bool		found;

	if (!row_cache_shmem_ready())
		return;

	EnsureRowCacheDsa();

	entry = hash_search(RowCacheRelHash, &relid, HASH_ENTER, &found);

	if (!found)
	{
		LWLockInitialize(&entry->rel_lock, LWTRANCHE_ROW_CACHE_REL);
		entry->tid_hash_handle = DSHASH_HANDLE_INVALID;
		entry->loaded = false;
		entry->natts = 0;
	}

	LWLockAcquire(&entry->rel_lock, LW_EXCLUSIVE);

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
 * ---------------------------------------------------------------- */

void
RelationRowCacheDropRelation(Oid relid)
{
	RowCacheRelEntry *entry;

	if (!row_cache_shmem_ready())
		return;

	entry = hash_search(RowCacheRelHash, &relid, HASH_FIND, NULL);
	if (entry == NULL || !entry->loaded)
		return;

	LWLockAcquire(&entry->rel_lock, LW_EXCLUSIVE);

	if (entry->loaded)
	{
		RowCacheDestroyTidHash(entry);
		entry->loaded = false;
		entry->tid_hash_handle = DSHASH_HANDLE_INVALID;
	}

	LWLockRelease(&entry->rel_lock);

	hash_search(RowCacheRelHash, &relid, HASH_REMOVE, NULL);

	LocalAttachInvalidate(relid);
}

/* ----------------------------------------------------------------
 * Public API: FillSlot (simple cache lookup + slot fill)
 * ---------------------------------------------------------------- */

bool
RelationRowCacheFillSlot(TupleTableSlot *slot)
{
	RowCacheRelEntry *entry;
	FlatCachedTuple *flat;
	bool		ok;

	Assert(slot != NULL);
	if (!OidIsValid(slot->tts_tableOid) || !ItemPointerIsValid(&slot->tts_tid))
		return false;

	if (!row_cache_shmem_ready())
		return false;

	entry = hash_search(RowCacheRelHash, &slot->tts_tableOid, HASH_FIND, NULL);
	if (entry == NULL || !entry->loaded)
		return false;

	LWLockAcquire(&entry->rel_lock, LW_SHARED);

	if (!entry->loaded)
	{
		LWLockRelease(&entry->rel_lock);
		return false;
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

	if (!OidIsValid(relid) || !ItemPointerIsValid(tid))
		return false;

	if (!row_cache_shmem_ready())
		return false;

	entry = hash_search(RowCacheRelHash, &relid, HASH_FIND, NULL);
	if (entry == NULL || !entry->loaded)
		return false;

	LWLockAcquire(&entry->rel_lock, LW_SHARED);

	if (!entry->loaded)
	{
		LWLockRelease(&entry->rel_lock);
		return false;
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
