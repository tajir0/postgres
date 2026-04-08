#include "postgres.h"

#include <string.h>

#include "access/htup_details.h"
#include "common/hashfn.h"
#include "executor/tuptable.h"
#include "lib/tid_row_cache.h"
#include "storage/itemptr.h"
#include "storage/off.h"
#include "utils/datum.h"

typedef struct TidRowBlock
{
	int			capacity;
	FlatCachedTuple **entries;
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
	FlatCachedTuple **newentries;

	if ((int) off < block->capacity)
		return;

	newcap = block->capacity;
	while (newcap <= (int) off)
		newcap <<= 1;

	newentries = (FlatCachedTuple **) repalloc(block->entries,
											   sizeof(FlatCachedTuple *) * newcap);
	MemSet(newentries + block->capacity,
		   0,
		   sizeof(FlatCachedTuple *) * (newcap - block->capacity));
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

	block_estimate = nelements / 64;
	if (block_estimate < 128)
		block_estimate = 128;

	cache->blocks = tid_row_block_map_create(ctx, block_estimate, NULL);
	return cache;
}

FlatCachedTuple *
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

/*
 * Flatten a slot's tuple data into a single contiguous palloc'd block.
 *
 * Pass-by-val Datums are stored inline in the values[] array.
 * Pass-by-ref Datums have their payloads copied to the varlen area at
 * the end of the block, and values[] stores a pointer into that area.
 */
static FlatCachedTuple *
FlattenTupleFromSlot(TupleTableSlot *slot, MemoryContext target_ctx)
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
	Datum	   *dst_values;
	bool	   *dst_isnull;
	char	   *varlen_cursor;
	MemoryContext old_ctx;

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

	old_ctx = MemoryContextSwitchTo(target_ctx);
	ft = (FlatCachedTuple *) palloc0(total_size);
	MemoryContextSwitchTo(old_ctx);

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
			dst_values[i] = PointerGetDatum(varlen_cursor);
			varlen_cursor += MAXALIGN(datum_size);
		}
	}

	memcpy((char *) ft + htup_offset, htup->t_data, htup->t_len);

	heap_freetuple(htup);
	return ft;
}

void
TidRowCacheStoreFromSlot(TidRowCache *cache,
						 TupleTableSlot *slot,
						 MemoryContext target_ctx)
{
	TidRowBlockMapEntry *block_entry;
	FlatCachedTuple *ft;
	BlockNumber blockno;
	OffsetNumber off;
	bool		found;

	Assert(cache != NULL);
	Assert(slot != NULL);
	Assert(ItemPointerIsValid(&slot->tts_tid));
	Assert(!TTS_EMPTY(slot));

	slot_getallattrs(slot);
	blockno = ItemPointerGetBlockNumberNoCheck(&slot->tts_tid);
	off = ItemPointerGetOffsetNumberNoCheck(&slot->tts_tid);

	block_entry = tid_row_block_map_insert(cache->blocks, blockno, &found);
	if (!found)
	{
		MemoryContext old_ctx = MemoryContextSwitchTo(target_ctx);

		block_entry->block.capacity = TidRowBlockInitialCapacity(off);
		block_entry->block.entries =
			(FlatCachedTuple **) palloc0(sizeof(FlatCachedTuple *) *
										 block_entry->block.capacity);
		MemoryContextSwitchTo(old_ctx);
	}
	else
		TidRowBlockEnsureCapacity(&block_entry->block, off);

	if (block_entry->block.entries[off] != NULL)
		pfree(block_entry->block.entries[off]);

	ft = FlattenTupleFromSlot(slot, target_ctx);
	block_entry->block.entries[off] = ft;
}

bool
TidRowCacheFillSlot(const FlatCachedTuple *flat,
					Oid relid,
					ItemPointer tid,
					TupleTableSlot *slot)
{
	HeapTupleData htup;
	Datum	   *src_values;
	bool	   *src_isnull;

	Assert(flat != NULL);
	Assert(slot != NULL);
	Assert(tid != NULL);

	if (slot->tts_tupleDescriptor->natts != flat->natts)
		return false;

	htup.t_data = (HeapTupleHeader) FLAT_TUPLE_HTUP_DATA(flat);
	htup.t_len = flat->htup_len;
	htup.t_tableOid = relid;
	ItemPointerCopy(tid, &htup.t_self);

	ExecForceStoreHeapTupleNoCopy(&htup, slot, false);

	src_values = FLAT_TUPLE_VALUES(flat);
	src_isnull = FLAT_TUPLE_ISNULL(flat);

	memcpy(slot->tts_values, src_values, sizeof(Datum) * flat->natts);
	memcpy(slot->tts_isnull, src_isnull, sizeof(bool) * flat->natts);
	slot->tts_nvalid = flat->natts;
	ItemPointerCopy(tid, &slot->tts_tid);
	slot->tts_tableOid = relid;

	return true;
}
