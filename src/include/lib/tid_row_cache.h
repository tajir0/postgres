#ifndef TID_ROW_CACHE_H
#define TID_ROW_CACHE_H

#include "postgres.h"

#include "executor/tuptable.h"

typedef struct TidRowCache TidRowCache;

typedef struct TidRowCacheEntry
{
	/* 完整拷贝的元组负载，供后端生命周期内读取。 */
	Datum	   *tts_values;
	bool	   *tts_isnull;
	HeapTuple	heap_tuple;
	int			natts;
} TidRowCacheEntry;

extern TidRowCache *TidRowCacheCreate(MemoryContext ctx, uint32 nelements);
extern TidRowCacheEntry *TidRowCacheLookupEntry(TidRowCache *cache,
												ItemPointer tid);
/* 将一个 slot 的内容复制到 target_ctx，并按 tid 执行插入/更新。 */
extern void TidRowCacheStoreFromSlot(TidRowCache *cache,
									 TupleTableSlot *slot,
									 MemoryContext target_ctx);
/* 将缓存中的元组负载写回到现有 slot。 */
extern bool TidRowCacheFillSlot(const TidRowCacheEntry *entry,
								Oid relid,
								ItemPointer tid,
								TupleTableSlot *slot);

#endif							/* TID_ROW_CACHE_H */
