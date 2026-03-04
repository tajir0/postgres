#include "postgres.h"

#include "access/htup_details.h"
#include "access/tableam.h"
#include "common/hashfn.h"
#include "executor/tuptable.h"
#include "lib/relation_row_cache.h"
#include "lib/tid_row_cache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"

typedef struct RelationRowCacheEntry
{
	char		status;
	Oid			relid;
	/* Per-relation context to drop the entire second-level cache at once. */
	MemoryContext row_ctx;
	TidRowCache *tid_cache;
	int			natts;
} RelationRowCacheEntry;

#define SH_PREFIX relation_row_cache
#define SH_ELEMENT_TYPE RelationRowCacheEntry
#define SH_KEY_TYPE Oid
#define SH_KEY relid
#define SH_SCOPE static inline
#define SH_DECLARE
#include "lib/simplehash.h"

static MemoryContext RelationRowCacheContext = NULL;
static relation_row_cache_hash *RelationRowCacheHash = NULL;
static Oid RelationRowCacheFastRelid = InvalidOid;
static RelationRowCacheEntry *RelationRowCacheFastEntry = NULL;

static inline uint32
RelationRowCacheHashKey(Oid relid)
{
	return murmurhash32(relid);
}

static inline bool
RelationRowCacheKeysEqual(Oid a, Oid b)
{
	return a == b;
}

#define SH_PREFIX relation_row_cache
#define SH_ELEMENT_TYPE RelationRowCacheEntry
#define SH_KEY_TYPE Oid
#define SH_KEY relid
#define SH_HASH_KEY(tb, key) RelationRowCacheHashKey(key)
#define SH_EQUAL(tb, a, b) RelationRowCacheKeysEqual(a, b)
#define SH_SCOPE static inline
#define SH_DEFINE
#include "lib/simplehash.h"

static inline RelationRowCacheEntry *
RelationRowCacheLookupRelEntry(Oid relid)
{
	RelationRowCacheEntry *entry;

	if (RelationRowCacheFastRelid == relid)
		return RelationRowCacheFastEntry;

	entry = relation_row_cache_lookup(RelationRowCacheHash, relid);
	RelationRowCacheFastRelid = relid;
	RelationRowCacheFastEntry = entry;
	return entry;
}

/*
 * Estimate second-level hash size from reltuples and keep slack to reduce
 * probe length and table growth.
 */
static uint32
RelationRowCacheEstimateElements(Relation rel)
{
	double		reltuples = rel->rd_rel->reltuples;
	uint64		est;

	if (reltuples > 1.0)
		est = (uint64) (reltuples * 1.3) + 1;
	else
		est = 1024;

	if (est < 128)
		est = 128;
	if (est > (PG_UINT32_MAX / 2))
		est = (PG_UINT32_MAX / 2);

	return (uint32) est;
}

/* Drop one relation entry and all second-level memory in one shot. */
static void
RelationRowCacheResetEntry(RelationRowCacheEntry *entry)
{
	if (RelationRowCacheFastRelid == entry->relid)
	{
		RelationRowCacheFastRelid = InvalidOid;
		RelationRowCacheFastEntry = NULL;
	}

	if (entry->row_ctx != NULL)
	{
		MemoryContextDelete(entry->row_ctx);
		entry->row_ctx = NULL;
		entry->tid_cache = NULL;
	}
	entry->natts = 0;
}

void
RelationRowCacheBackendInit(void)
{
	if (RelationRowCacheHash != NULL)
		return;

	RelationRowCacheContext = AllocSetContextCreate(TopMemoryContext,
												"BackendRelationRowCache",
												ALLOCSET_DEFAULT_SIZES);
	RelationRowCacheHash = relation_row_cache_create(RelationRowCacheContext,
											 16,
											 NULL);
}

void
RelationRowCacheDropRelation(Oid relid)
{
	RelationRowCacheEntry *entry;

	RelationRowCacheBackendInit();

	entry = RelationRowCacheLookupRelEntry(relid);
	if (entry == NULL)
		return;

	RelationRowCacheResetEntry(entry);
	relation_row_cache_delete(RelationRowCacheHash, relid);
}

void
RelationRowCacheLoadRelation(Relation rel)
{
	RelationRowCacheEntry *entry;
	TableScanDesc scan;
	TupleTableSlot *scan_slot;
	uint32		est_nelements;
	bool		found;
	bool		pushed_snapshot = false;

	Assert(rel != NULL);

	RelationRowCacheBackendInit();

	entry = relation_row_cache_insert(RelationRowCacheHash,
								  RelationGetRelid(rel),
								  &found);
	if (!found)
	{
		entry->row_ctx = NULL;
		entry->tid_cache = NULL;
		entry->natts = 0;
	}

	RelationRowCacheResetEntry(entry);

	entry->row_ctx = AllocSetContextCreate(RelationRowCacheContext,
									   "RelationTidRowCache",
									   ALLOCSET_DEFAULT_SIZES);
	est_nelements = RelationRowCacheEstimateElements(rel);
	entry->tid_cache = TidRowCacheCreate(entry->row_ctx, est_nelements);
	entry->natts = RelationGetDescr(rel)->natts;
	RelationRowCacheFastRelid = entry->relid;
	RelationRowCacheFastEntry = entry;

	if (!ActiveSnapshotSet())
	{
		PushActiveSnapshot(GetTransactionSnapshot());
		pushed_snapshot = true;
	}

	scan = table_beginscan(rel, GetActiveSnapshot(), 0, NULL);
	scan_slot = table_slot_create(rel, NULL);

	while (table_scan_getnextslot(scan, ForwardScanDirection, scan_slot))
		TidRowCacheStoreFromSlot(entry->tid_cache, scan_slot, entry->row_ctx);

	table_endscan(scan);
	ExecDropSingleTupleTableSlot(scan_slot);

	if (pushed_snapshot)
		PopActiveSnapshot();
}

bool
RelationRowCacheFillSlot(TupleTableSlot *slot)
{
	RelationRowCacheEntry *rel_entry;
	TidRowCacheEntry *tid_entry;

	Assert(slot != NULL);

	if (!OidIsValid(slot->tts_tableOid) || !ItemPointerIsValid(&slot->tts_tid))
		return false;

	RelationRowCacheBackendInit();

	rel_entry = RelationRowCacheLookupRelEntry(slot->tts_tableOid);
	if (rel_entry == NULL || rel_entry->tid_cache == NULL)
		return false;

	tid_entry = TidRowCacheLookupEntry(rel_entry->tid_cache, &slot->tts_tid);
	if (tid_entry == NULL)
		return false;

	return TidRowCacheFillSlot(tid_entry, rel_entry->relid, &slot->tts_tid, slot);
}

/*
 * Conservative MVCC visibility check based on tuple header bits already in
 * cache. Unknown states are treated as "not visible" to preserve correctness.
 */
static bool
RelationRowCacheTupleVisibleMVCC(HeapTuple tuple, Snapshot snapshot)
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

bool
RelationRowCacheFetchWithVisibility(Oid relid,
									ItemPointer tid,
									Snapshot snapshot,
									TupleTableSlot *slot,
									bool *is_visible,
									bool *has_hot_chain)
{
	RelationRowCacheEntry *rel_entry;
	TidRowCacheEntry *tid_entry;

	Assert(slot != NULL);
	Assert(is_visible != NULL);
	Assert(has_hot_chain != NULL);

	*is_visible = false;
	*has_hot_chain = false;

	if (!OidIsValid(relid) || !ItemPointerIsValid(tid))
		return false;

	RelationRowCacheBackendInit();

	rel_entry = RelationRowCacheLookupRelEntry(relid);
	if (rel_entry == NULL || rel_entry->tid_cache == NULL)
		return false;

	tid_entry = TidRowCacheLookupEntry(rel_entry->tid_cache, tid);
	if (tid_entry == NULL)
		return false;

	*has_hot_chain = HeapTupleIsHotUpdated(tid_entry->heap_tuple);
	*is_visible = RelationRowCacheTupleVisibleMVCC(tid_entry->heap_tuple, snapshot);

	if (*is_visible)
		return TidRowCacheFillSlot(tid_entry, rel_entry->relid, tid, slot);

	return true;
}
