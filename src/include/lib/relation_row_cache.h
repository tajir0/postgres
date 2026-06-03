/*-------------------------------------------------------------------------
 *
 * relation_row_cache.h
 *	  V4 row cache (dml_lock branch): partition-locked global hash of
 *	  (relid, pkey) -> flattened tuple + fixed-size RelMeta array for
 *	  per-relation metadata.
 *
 * Architecture:
 *   - Single flat global hash in DSA, (1 << 23) = 8,388,608 buckets.
 *   - 128 LWLock partitions; readers hold LW_SHARED, writers hold
 *     LW_EXCLUSIVE.  LW_EXCLUSIVE waits for all in-flight SHARED readers
 *     to drain, making synchronous dsa_free safe — no deferred GC needed.
 *   - RelMeta array (64 slots) with DISABLED / LOADING / ENABLED state.
 *     No rel_gen field; DDL invalidation flips state=DISABLED directly
 *     under the per-relation build_lock.
 *   - Pkey: 1..ROW_CACHE_PKEY_MAX_ATTS pass-by-value columns.
 *     Pass-by-reference pkeys bail out silently at Load time.
 *   - IndexNext hook (nodeIndexscan.c) calls RelationRowCachePkeyFetch /
 *     RelationRowCachePkeyFetchComposite.
 *   - FlatCachedTuple (tid_row_cache.{c,h}) reused as payload.
 *
 * What is NOT in this implementation:
 *   - EBR / retire list / GC bgworker (removed; partition locks suffice)
 *   - VACUUM hook / rel_gen soft invalidation (removed; pkey-keyed cache
 *     is immune to TID reuse, and DML hooks handle UPDATE/DELETE before
 *     VACUUM can see the dead tuple)
 *   - INSERT hook (no LRU / population strategy for new rows)
 *   - LRU eviction (future work)
 *   - Composite / byref pkey (future work)
 *
 * Concurrent safety:
 *   Read path: acquire LW_SHARED on bucket's partition lock, walk chain,
 *   copy payload, release.  Multiple readers on any key in the same
 *   partition are fully parallel (SHARED does not block SHARED).
 *
 *   Write path (Drop / DML invalidate): acquire LW_EXCLUSIVE on bucket's
 *   partition lock, unlink entry, dsa_free synchronously, release.
 *
 *   Load serialized against itself and Drop via per-RelMeta build_lock.
 *   Concurrent readers see either the old or the new fully-published
 *   state, never an intermediate.
 *
 * Legacy TID-keyed API: RelationRowCacheFillSlot /
 * RelationRowCacheFetchWithVisibility are stubs returning false so
 * existing tableam.h call sites compile unchanged.
 *
 *-------------------------------------------------------------------------
 */
#ifndef RELATION_ROW_CACHE_H
#define RELATION_ROW_CACHE_H

#include "postgres.h"

#include "access/htup.h"
#include "executor/tuptable.h"
#include "port/atomics.h"
#include "utils/dsa.h"
#include "utils/rel.h"
#include "utils/snapshot.h"

/* Shared memory sizing and initialization (called from ipci.c). */
extern Size RowCacheShmemSize(void);
extern void RowCacheShmemInit(void);

/* ----------------------------------------------------------------
 * DML hooks — invalidate-only
 *
 * Called from heap_update / heap_delete after END_CRIT_SECTION +
 * CacheInvalidateHeapTuple but BEFORE ReleaseBuffer (so oldtup->t_data
 * is still mapped for heap_getattr-based pkey extraction).
 *
 * Both hooks early-exit cheaply (~5-15 ns) when:
 *   - RowCacheCtl is uninitialised, or
 *   - the relation has no RelMeta slot, or
 *   - its RelMeta is not ENABLED, or
 *   - the relation's pkey is not a single byval column.
 *
 * When invalidation fires, the old GlobalEntry is unlinked from its
 * bucket chain under partition lock EXCLUSIVE, then its payload and
 * entry are dsa_free'd synchronously (safe because LW_EXCLUSIVE waits
 * for all in-flight LW_SHARED readers to drain before proceeding).
 * Cache content is never refreshed by DML; subsequent reads for the
 * same pkey miss and fall back to the native btree path until the next
 * explicit Drop+Load.
 *
 * INSERT is intentionally not hooked: with no LRU / population strategy
 * for newly inserted rows, an INSERT hook would only add overhead with
 * no possible cache-hit benefit.  Pre-loaded rows are unaffected.
 */
extern void RowCacheOnHeapUpdate(Relation rel,
								 HeapTuple oldtup,
								 HeapTuple newtup);
extern void RowCacheOnHeapDelete(Relation rel,
								 HeapTuple oldtup);

/* Load / Drop a relation's cache. */
extern void RelationRowCacheLoadRelation(Relation rel);
extern void RelationRowCacheDropRelation(Oid relid);

/*
 * V4 pkey-driven fast path (used by nodeIndexscan.c).
 *
 * RelationRowCachePkeyFetch       — single-column pkey relation.
 * RelationRowCachePkeyFetchComposite — composite pkey relation.
 *
 * On hit returns true; *is_visible tells the caller whether the tuple was
 * MVCC-visible to `snapshot`.  On not-loaded / not-found / not-eligible
 * returns false (caller must fall back to btree).
 *
 * For composite, `vals[0..nvals-1]` must be supplied in the attno order
 * reported by RelationRowCachePkeyDescriptor (which is the order the
 * cache recorded at Load time, i.e. the index's indkey order).  nvals
 * must equal the cached n_pkey_attrs; mismatch returns false.
 */
extern bool RelationRowCachePkeyFetch(Oid relid,
									  Datum pkey_val,
									  Snapshot snapshot,
									  TupleTableSlot *slot,
									  bool *is_visible,
									  bool *has_hot_chain);
extern bool RelationRowCachePkeyFetchComposite(Oid relid,
											   const Datum *vals,
											   int nvals,
											   Snapshot snapshot,
											   TupleTableSlot *slot,
											   bool *is_visible,
											   bool *has_hot_chain);

/*
 * Return the 1-based pkey attno currently registered for this relation
 * (matching the cache's pkey column).  Returns 0 if the relation is not
 * cached, not loaded, or its cache uses a composite pkey.  Cheap; safe
 * on the executor hot path.
 */
extern AttrNumber RelationRowCachePkeyAttno(Oid relid);

/*
 * Relation-bound fast path.
 *
 * ROWCACHE_NOT_CACHED is the sentinel stored in RelationData.rd_rowcache_meta
 * once a relation has been checked and confirmed to have no cache.  It lets a
 * single pointer test ("== NULL ? not bound : == NOT_CACHED ? reject : use")
 * cover both "never looked" and "looked, nothing there" without a second
 * field.  Any non-NULL, non-sentinel value is a live shmem RelMeta pointer.
 */
#define ROWCACHE_NOT_CACHED		((struct RelMeta *) 0x1)

/*
 * Bind (or refresh) rel->rd_rowcache_{meta,pkey_*} from the shared RelMeta
 * array.  Idempotent and cheap when already bound (rd_rowcache_meta != NULL
 * returns immediately).  Called from RelationBuildDesc so an SI-driven
 * rebuild refreshes the snapshot, and lazily from executor / DML hooks the
 * first time a freshly built relation is touched.  No-op (leaves the field
 * NULL) when the cache module is not yet initialised (e.g. bootstrap).
 */
extern void RelationRowCacheBindRelation(Relation rel);

/*
 * Bound composite/single fetch: probe the cache for a relation whose live
 * RelMeta pointer the caller already holds (from rd_rowcache_meta), skipping
 * the global RelMeta-array scan and sticky lookup entirely.  vals[0..nvals-1]
 * are the pkey column Datums in cache attno order; nvals must equal the
 * cache's n_pkey_attrs.  Semantics otherwise match RelationRowCachePkeyFetch.
 */
extern bool RelationRowCachePkeyFetchBound(struct RelMeta *rm,
										   const Datum *vals,
										   int nvals,
										   Snapshot snapshot,
										   TupleTableSlot *slot,
										   bool *is_visible,
										   bool *has_hot_chain);

/*
 * Snapshot the relation's cached pkey descriptor: writes the cache's
 * full attno list (in load-time / index order) into out_attnos[] and
 * returns the count.  Returns 0 when the relation is not cached, not
 * loaded, or the caller's buffer is too small.
 *
 * Used by nodeIndexscan.c IndexNext to verify a runtime composite-
 * dispatch is safe (cache attnos match index attnos position-for-
 * position) and to know how many ScanKey arguments to gather.
 */
extern int RelationRowCachePkeyDescriptor(Oid relid,
										   AttrNumber *out_attnos,
										   int max_attnos);

/*
 * Legacy TID-keyed API.  These are stubs returning false so existing
 * tableam.h hook sites compile and harmlessly fall through to the
 * native path.
 */
extern bool RelationRowCacheFillSlot(TupleTableSlot *slot);
extern bool RelationRowCacheFetchWithVisibility(Oid relid,
												ItemPointer tid,
												Snapshot snapshot,
												TupleTableSlot *slot,
												bool *is_visible,
												bool *has_hot_chain);

#endif							/* RELATION_ROW_CACHE_H */
