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

/*
 * Pkey-driven fast path: lookup a tuple by primary-key value, bypassing
 * the btree.  Only succeeds when the relation was loaded with a pkey
 * index (single-column, byval primary key) and the entry is in the
 * LOADED state.
 *
 * On hit: fills `slot` with the visible tuple and returns true.
 * On miss / not eligible / not visible: returns false.  Caller should
 * fall back to the regular IndexScan + TID-cache path.
 *
 * `pkey_val` must be a non-null Datum of the same type as the
 * relation's primary key column.
 */
extern bool RelationRowCachePkeyFetch(Oid relid,
									  Datum pkey_val,
									  Snapshot snapshot,
									  TupleTableSlot *slot,
									  bool *is_visible,
									  bool *has_hot_chain);

/*
 * Return the 1-based attno of the relation's pkey column if the cache
 * entry has a pkey index loaded.  Returns 0 if no entry, not loaded, or
 * no pkey index.  Used by the executor at plan-init time to decide
 * whether to enable the pkey fast path for a given IndexScan.
 */
extern AttrNumber RelationRowCachePkeyAttno(Oid relid);

#endif							/* RELATION_ROW_CACHE_H */
