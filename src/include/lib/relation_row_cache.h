/*-------------------------------------------------------------------------
 *
 * relation_row_cache.h
 *	  V4 Phase 1 row cache: single flat global hash (relid, pkey) -> entry
 *	  + fixed-size RelMeta array for per-relation metadata.
 *
 * Phase 1 scope:
 *   - Global chained hash in DSA, 128 partition locks for write serialization.
 *   - RelMeta array (64 slots) with DISABLED / LOADING / ENABLED state.
 *   - Pkey serialization: single-column, pass-by-value only (int2/int4/int8/oid).
 *     Composite / byref pks bail out at Load time; design leaves room for
 *     Phase 4 extensions.
 *   - IndexNext hook (in nodeIndexscan.c) calls RelationRowCachePkeyFetch.
 *   - FlatCachedTuple (from tid_row_cache.{c,h}) is reused as the payload.
 *
 * Concurrent-reader safety (as of Phase 3 (5/5)):
 *   The Phase 1 lifetime contract that required "no readers concurrent
 *   with Load / Drop" has been LIFTED.  Drop now goes through unpublish
 *   (state=DISABLED + rel_gen bump) + EBR-retire (Phase 3 (3/4)), and
 *   the read path (RelationRowCachePkeyFetch) wraps its critical
 *   section with RowCacheEpochEnter/Exit (Phase 3 (5/5)).  Together
 *   these guarantee that any reader in flight when Drop / DML / DDL
 *   fires keeps a valid view of its already-loaded GlobalEntry /
 *   FlatCachedTuple until it exits the EBR critical section; physical
 *   reclamation by the rowcache-gc worker is gated on safe_epoch
 *   advancing past every in-flight reader's local_epoch.
 *
 *   Load remains serialized with itself and with Drop via per-RelMeta
 *   build_lock; concurrent readers see either "old, fully-published
 *   state" or "new, fully-published state" but never an intermediate.
 *
 * Phase 1 OUT of scope (left as stubs):
 *   - EBR / retire-list / bgworker GC (Phase 2)
 *   - DML hooks (heap_insert/update/delete) (Phase 3)
 *   - Composite / byref pk (Phase 4)
 *
 * Legacy V1/V2 TID-keyed API: RelationRowCacheFillSlot /
 * RelationRowCacheFetchWithVisibility are kept as stubs returning false so
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

/*
 * Phase 5 (dml_lock) 4-5/8: removed all EBR (Epoch-Based Reclamation)
 * primitives.  The read path now uses LW_SHARED partition locks and the
 * write path dsa_free's synchronously under LW_EXCLUSIVE.  Deleted:
 *
 *   - RowCacheGlobalEpoch{Read,Bump}, RowCacheSafeEpoch{Read,Publish},
 *     RowCacheComputeSafeEpoch
 *   - RowCacheEpochRetire, RowCacheLocalGC, RowCacheLocalRetireCount
 *   - RowCacheEpochEnter / Exit static inlines
 *   - RowCacheGCRegister, RowCacheGCMain
 *   - PGPROC.rowcache_local_epoch field
 *   - RowCacheControl shmem fields: global_epoch, safe_epoch_published,
 *     orphan_list_lock, orphan_list_head_dp
 *   - GlobalEntry.state field + ROW_CACHE_ENTRY_{FRESH,STALE,DELETED}
 *     macros
 */

/* ----------------------------------------------------------------
 * Phase 3 (1/2): DML hooks — invalidate-only
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
 * When invalidation actually fires, the old GlobalEntry is unlinked from
 * its bucket chain (under partition lock EXCLUSIVE) and its payload +
 * entry are pushed to the EBR retire list (under no lock).  Cache
 * content is never refreshed by DML; subsequent reads for the same pkey
 * miss the cache and fall back to the native btree path until the next
 * explicit Drop+Load (or, post Phase 4 / 5, LRU eviction triggers).
 *
 * The newtup parameter on RowCacheOnHeapUpdate is reserved for a future
 * write-through strategy and is ignored under the invalidate-only path
 * shipped in this commit.
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

/*
 * VACUUM LP_UNUSED fallback (Phase 3 (4/4)).
 *
 * Called from lazy_vacuum_heap_page once it has converted at least one
 * LP_DEAD slot to LP_UNUSED.  Bumps the relation's rel_gen so any cache
 * entry whose recorded rel_gen_at_load no longer matches is treated as
 * a miss by future readers.
 *
 * Strictly a "defense in depth" hook for V4: pkey-keyed cache is
 * naturally immune to TID reuse (new INSERT into a recycled TID has a
 * fresh pkey and won't collide).  This hook prevents the secondary
 * issue of stale-content entries lingering after rows they describe
 * are gone from the live heap.
 *
 * Cost is ~tens of ns (sticky relmeta lookup + atomic_load + atomic_add)
 * for cache-enabled tables, and ~5-15 ns early-out otherwise.  Caller
 * must gate on "this page actually produced LP_UNUSED items".
 */
extern void RowCacheOnVacuumLPUnused(Relation rel);

/* Load / Drop a relation's cache.  See lifetime contract above. */
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
 * Legacy TID-keyed API.  V4 Phase 1 has no TID-keyed path; these are
 * stubs returning false so existing tableam.h hook sites compile and
 * harmlessly fall through to the native path.
 */
extern bool RelationRowCacheFillSlot(TupleTableSlot *slot);
extern bool RelationRowCacheFetchWithVisibility(Oid relid,
												ItemPointer tid,
												Snapshot snapshot,
												TupleTableSlot *slot,
												bool *is_visible,
												bool *has_hot_chain);

#endif							/* RELATION_ROW_CACHE_H */
