#ifndef RELATION_ROW_CACHE_H
#define RELATION_ROW_CACHE_H

#include "postgres.h"

#include "executor/tuptable.h"
#include "utils/rel.h"
#include "utils/snapshot.h"

/* Shared memory sizing and initialization (called from ipci.c). */
extern Size RowCacheShmemSize(void);
extern void RowCacheShmemInit(void);

/* Load all visible tuples of a relation into the shared row cache. */
extern void RelationRowCacheLoadRelation(Relation rel);
/* Fill slot from the shared cache using slot->tts_tableOid + slot->tts_tid. */
extern bool RelationRowCacheFillSlot(TupleTableSlot *slot);
/*
 * Lookup (relid, tid) in the shared cache and report visibility / HOT-chain
 * status.  Returns true if the cache entry was found; *is_visible and
 * *has_hot_chain carry the MVCC results.
 */
extern bool RelationRowCacheFetchWithVisibility(Oid relid,
												ItemPointer tid,
												Snapshot snapshot,
												TupleTableSlot *slot,
												bool *is_visible,
												bool *has_hot_chain);
/* Drop the shared cache for a relation. */
extern void RelationRowCacheDropRelation(Oid relid);

#endif							/* RELATION_ROW_CACHE_H */
