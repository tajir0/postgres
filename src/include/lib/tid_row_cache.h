#ifndef TID_ROW_CACHE_H
#define TID_ROW_CACHE_H

#include "postgres.h"

#include "executor/tuptable.h"

typedef struct TidRowCache TidRowCache;

/*
 * FlatCachedTuple: single-allocation flat storage for a cached tuple.
 *
 * All per-tuple data (values[], isnull[], HeapTupleHeader, and pass-by-ref
 * Datum payloads) are packed into one contiguous palloc'd block.  This
 * replaces the previous TidRowCacheEntry which scattered data across
 * three independent palloc calls (values, isnull, heap_tuple).
 *
 * Memory layout after the fixed header:
 *   Datum   values[natts]       -- starts at MAXALIGN(sizeof(FlatCachedTuple))
 *   bool    isnull[natts]       -- immediately after values
 *   char    htup_data[htup_len] -- HeapTupleHeaderData + tuple payload
 *   char    varlen_data[...]    -- pass-by-ref Datum binary payloads
 */
typedef struct FlatCachedTuple
{
	uint32		total_size;		/* total byte size of this block */
	int			natts;			/* number of attributes */
	uint32		htup_offset;	/* byte offset to HeapTupleHeader within block */
	uint32		htup_len;		/* HeapTuple data length (t_len) */
} FlatCachedTuple;

#define FLAT_TUPLE_VALUES(ft) \
	((Datum *)((char *)(ft) + MAXALIGN(sizeof(FlatCachedTuple))))
#define FLAT_TUPLE_ISNULL(ft) \
	((bool *)((char *)FLAT_TUPLE_VALUES(ft) + sizeof(Datum) * (ft)->natts))
#define FLAT_TUPLE_HTUP_DATA(ft) \
	((char *)(ft) + (ft)->htup_offset)

extern TidRowCache *TidRowCacheCreate(MemoryContext ctx, uint32 nelements);
extern FlatCachedTuple *TidRowCacheLookupEntry(TidRowCache *cache,
											   ItemPointer tid);
extern void TidRowCacheStoreFromSlot(TidRowCache *cache,
									 TupleTableSlot *slot,
									 MemoryContext target_ctx);
extern bool TidRowCacheFillSlot(const FlatCachedTuple *flat,
								Oid relid,
								ItemPointer tid,
								TupleTableSlot *slot);

#endif							/* TID_ROW_CACHE_H */
