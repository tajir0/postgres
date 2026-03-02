#include "postgres.h"

#include <string.h>

#include "common/hashfn.h"
#include "executor/tuptable.h"
#include "lib/tid_row_cache.h"

#define SH_PREFIX tid_row_cache
#define SH_ELEMENT_TYPE TidRowCacheEntry
#define SH_KEY_TYPE ItemPointerData
#define SH_KEY tid
#define SH_SCOPE static inline
#define SH_DECLARE
#include "lib/simplehash.h"

/* 计算二级 hash（tid）的哈希值。 */
static inline uint32
TidRowCacheHashKey(const ItemPointerData key)
{
	return hash_bytes((const unsigned char *) &key, sizeof(ItemPointerData));
}

/* 比较两个 tid 是否相等。 */
static inline bool
TidRowCacheKeysEqual(const ItemPointerData a, const ItemPointerData b)
{
	return ItemPointerEquals(&a, &b);
}

#define SH_PREFIX tid_row_cache
#define SH_ELEMENT_TYPE TidRowCacheEntry
#define SH_KEY_TYPE ItemPointerData
#define SH_KEY tid
#define SH_HASH_KEY(tb, key) TidRowCacheHashKey(key)
#define SH_EQUAL(tb, a, b) TidRowCacheKeysEqual(a, b)
#define SH_SCOPE static inline
#define SH_DEFINE
#include "lib/simplehash.h"

/* 创建二级 tid 缓存。 */
TidRowCache *
TidRowCacheCreate(MemoryContext ctx, uint32 nelements)
{
	return tid_row_cache_create(ctx, nelements, NULL);
}

/* 按 tid 查询缓存条目。 */
TidRowCacheEntry *
TidRowCacheLookupEntry(TidRowCache *cache, ItemPointer tid)
{
	return tid_row_cache_lookup(cache, *tid);
}

/*
 * 将 slot 中的当前元组复制到二级缓存。
 * tts_values/tts_isnull/HeapTuple 全部复制到 target_ctx 中，保证长期可用。
 */
void
TidRowCacheStoreFromSlot(TidRowCache *cache,
						 TupleTableSlot *slot,
						 MemoryContext target_ctx)
{
	TidRowCacheEntry *entry;
	TupleDesc	desc = slot->tts_tupleDescriptor;
	MemoryContext old_ctx;
	bool		found;
	int			i;

	Assert(cache != NULL);
	Assert(slot != NULL);
	Assert(ItemPointerIsValid(&slot->tts_tid));
	Assert(!TTS_EMPTY(slot));

	slot_getallattrs(slot);

	/* 所有拷贝出的负载都放入调用方提供的子上下文。 */
	old_ctx = MemoryContextSwitchTo(target_ctx);

	entry = tid_row_cache_insert(cache, slot->tts_tid, &found);
	if (found)
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

	/* 保留自有 HeapTuple 副本，便于快速重建 slot 物理态。 */
	entry->heap_tuple = ExecCopySlotHeapTuple(slot);

	MemoryContextSwitchTo(old_ctx);
}

/*
 * 将缓存条目写回到已有 slot。
 * 同步回填物理 HeapTuple 与 tts_values/tts_isnull。
 */
bool
TidRowCacheFillSlot(const TidRowCacheEntry *entry, Oid relid, TupleTableSlot *slot)
{
	HeapTuple	tuple_copy;

	Assert(entry != NULL);
	Assert(slot != NULL);

	if (slot->tts_tupleDescriptor->natts != entry->natts)
		return false;

	/* 先重建物理元组态，再覆盖 Datum/isnull 数组。 */
	tuple_copy = heap_copytuple(entry->heap_tuple);
	tuple_copy->t_tableOid = relid;

	ExecForceStoreHeapTuple(tuple_copy, slot, true);

	slot_getallattrs(slot);
	memcpy(slot->tts_values,
		   entry->tts_values,
		   sizeof(Datum) * entry->natts);
	memcpy(slot->tts_isnull,
		   entry->tts_isnull,
		   sizeof(bool) * entry->natts);
	slot->tts_nvalid = entry->natts;
	ItemPointerCopy(&entry->tid, &slot->tts_tid);
	slot->tts_tableOid = relid;

	return true;
}
