#include "postgres.h"

#include <string.h>

#include "common/hashfn.h"
#include "executor/tuptable.h"
#include "lib/tid_row_cache.h"
#include "storage/itemptr.h"
#include "storage/off.h"
#include "utils/datum.h"

typedef struct TidRowBlock
{
	int			capacity;
	TidRowCacheEntry **entries;
} TidRowBlock;

typedef struct TidRowBlockMapEntry
{
	char		status;
	BlockNumber blockno;
	uint32		hash;
	TidRowBlock block;
} TidRowBlockMapEntry;

#define SH_PREFIX tid_row_block_map
#define SH_ELEMENT_TYPE TidRowBlockMapEntry
#define SH_KEY_TYPE BlockNumber
#define SH_KEY blockno
#define SH_SCOPE static inline
#define SH_DECLARE
#include "lib/simplehash.h"

static inline uint32
TidRowBlockHashKey(const BlockNumber key)
{
	/* BlockNumber is 32bit, hashing it directly is enough here. */
	return murmurhash32((uint32) key);
}

static inline bool
TidRowBlockKeysEqual(const BlockNumber a, const BlockNumber b)
{
	return a == b;
}

#define SH_PREFIX tid_row_block_map
#define SH_ELEMENT_TYPE TidRowBlockMapEntry
#define SH_KEY_TYPE BlockNumber
#define SH_KEY blockno
#define SH_HASH_KEY(tb, key) TidRowBlockHashKey(key)
#define SH_EQUAL(tb, a, b) TidRowBlockKeysEqual(a, b)
#define SH_STORE_HASH
#define SH_GET_HASH(tb, a) ((a)->hash)
#define SH_SCOPE static inline
#define SH_DEFINE
#include "lib/simplehash.h"

struct TidRowCache
{
	MemoryContext ctx;
	tid_row_block_map_hash *blocks;
};

static inline int
TidRowBlockInitialCapacity(OffsetNumber off)
{
	int			cap = 16;

	while (cap <= (int) off)
		cap <<= 1;

	return cap;
}

static void
TidRowBlockEnsureCapacity(TidRowBlock *block, OffsetNumber off)
{
	int			newcap;
	TidRowCacheEntry **newentries;

	if ((int) off < block->capacity)
		return;

	newcap = block->capacity;
	while (newcap <= (int) off)
		newcap <<= 1;

	newentries = (TidRowCacheEntry **) repalloc(block->entries,
												sizeof(TidRowCacheEntry *) * newcap);
	MemSet(newentries + block->capacity,
		   0,
		   sizeof(TidRowCacheEntry *) * (newcap - block->capacity));
	block->entries = newentries;
	block->capacity = newcap;
}

TidRowCache *
TidRowCacheCreate(MemoryContext ctx, uint32 nelements)
{
	TidRowCache *cache;
	uint32		block_estimate;

	cache = (TidRowCache *) MemoryContextAllocZero(ctx, sizeof(TidRowCache));
	cache->ctx = ctx;

	/*
	 * nelements is tuple-level estimate from caller. Convert it to rough block
	 * count (about tens of tuples per page), keep a floor to avoid frequent grow.
	 */
	block_estimate = nelements / 64;
	if (block_estimate < 128)
		block_estimate = 128;

	cache->blocks = tid_row_block_map_create(ctx, block_estimate, NULL);
	return cache;
}

TidRowCacheEntry *
TidRowCacheLookupEntry(TidRowCache *cache, ItemPointer tid)
{
	BlockNumber blockno;
	OffsetNumber off;
	TidRowBlockMapEntry *block_entry;

	Assert(cache != NULL);
	Assert(tid != NULL);

	blockno = ItemPointerGetBlockNumberNoCheck(tid);
	off = ItemPointerGetOffsetNumberNoCheck(tid);

	block_entry = tid_row_block_map_lookup(cache->blocks, blockno);
	if (block_entry == NULL)
		return NULL;
	if ((int) off >= block_entry->block.capacity)
		return NULL;

	return block_entry->block.entries[off];
}

void
TidRowCacheStoreFromSlot(TidRowCache *cache,
						 TupleTableSlot *slot,
						 MemoryContext target_ctx)
{
	TidRowBlockMapEntry *block_entry;
	TidRowCacheEntry *entry;
	TupleDesc	desc = slot->tts_tupleDescriptor;
	MemoryContext old_ctx;
	BlockNumber blockno;
	OffsetNumber off;
	bool		found;
	int			i;

	Assert(cache != NULL);
	Assert(slot != NULL);
	Assert(ItemPointerIsValid(&slot->tts_tid));
	Assert(!TTS_EMPTY(slot));

	slot_getallattrs(slot);
	blockno = ItemPointerGetBlockNumberNoCheck(&slot->tts_tid);
	off = ItemPointerGetOffsetNumberNoCheck(&slot->tts_tid);

	old_ctx = MemoryContextSwitchTo(target_ctx);

	block_entry = tid_row_block_map_insert(cache->blocks, blockno, &found);
	if (!found)
	{
		block_entry->block.capacity = TidRowBlockInitialCapacity(off);
		block_entry->block.entries =
			(TidRowCacheEntry **) palloc0(sizeof(TidRowCacheEntry *) *
										  block_entry->block.capacity);
	}
	else
		TidRowBlockEnsureCapacity(&block_entry->block, off);

	entry = block_entry->block.entries[off];
	if (entry == NULL)
	{
		entry = (TidRowCacheEntry *) palloc0(sizeof(TidRowCacheEntry));
		block_entry->block.entries[off] = entry;
	}
	else
	{
		if (entry->tts_values)
			pfree(entry->tts_values);
		if (entry->tts_isnull)
			pfree(entry->tts_isnull);
		if (entry->heap_tuple)
			heap_freetuple(entry->heap_tuple);
	}

	entry->natts = desc->natts;
	entry->tts_values = (Datum *) palloc0(sizeof(Datum) * desc->natts);
	entry->tts_isnull = (bool *) palloc(sizeof(bool) * desc->natts);

	for (i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(desc, i);

		entry->tts_isnull[i] = slot->tts_isnull[i];
		if (slot->tts_isnull[i])
			entry->tts_values[i] = (Datum) 0;
		else
			entry->tts_values[i] = datumCopy(slot->tts_values[i],
											 attr->attbyval,
											 attr->attlen);
	}

	entry->heap_tuple = ExecCopySlotHeapTuple(slot);

	MemoryContextSwitchTo(old_ctx);
}

bool
TidRowCacheFillSlot(const TidRowCacheEntry *entry,
					Oid relid,
					ItemPointer tid,
					TupleTableSlot *slot)
{
	HeapTuple	tuple_copy;

	Assert(entry != NULL);
	Assert(slot != NULL);
	Assert(tid != NULL);

	if (slot->tts_tupleDescriptor->natts != entry->natts)
		return false;

	tuple_copy = heap_copytuple(entry->heap_tuple);
	tuple_copy->t_tableOid = relid;

	ExecForceStoreHeapTuple(tuple_copy, slot, true);

	memcpy(slot->tts_values,
		   entry->tts_values,
		   sizeof(Datum) * entry->natts);
	memcpy(slot->tts_isnull,
		   entry->tts_isnull,
		   sizeof(bool) * entry->natts);
	slot->tts_nvalid = entry->natts;
	ItemPointerCopy(tid, &slot->tts_tid);
	slot->tts_tableOid = relid;

	return true;
}
