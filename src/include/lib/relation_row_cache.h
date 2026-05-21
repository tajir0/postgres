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
#include "storage/proc.h"
#include "utils/dsa.h"
#include "utils/rel.h"
#include "utils/snapshot.h"

/* Shared memory sizing and initialization (called from ipci.c). */
extern Size RowCacheShmemSize(void);
extern void RowCacheShmemInit(void);

/* ----------------------------------------------------------------
 * Phase 2: EBR (Epoch-Based Reclamation) primitives
 *
 * Read path declares its critical section with RowCacheEpochEnter/Exit.
 * Writers retire DSA blocks instead of dsa_free'ing immediately; the GC
 * worker (subsequent commit) computes safe_epoch = min over all backends'
 * rowcache_local_epoch and publishes it for retire-list reclamation.
 *
 * This commit only wires the counters + the enter/exit primitives.  The
 * retire list, GC bgworker, and read-path integration land in later
 * Phase 2 commits.
 * ---------------------------------------------------------------- */

/* Accessors backed by the shmem RowCacheControl.  Defined in the .c file. */
extern uint64 RowCacheGlobalEpochRead(void);
extern uint64 RowCacheGlobalEpochBump(void);
extern uint64 RowCacheSafeEpochRead(void);
extern void   RowCacheSafeEpochPublish(uint64 safe);
extern uint64 RowCacheComputeSafeEpoch(void);

/*
 * Retire / reclaim API (Phase 2 2/4).
 *
 * Writers (DML / Drop / future LRU eviction) call RowCacheEpochRetire with
 * up to 3 DSA pointers — typically (payload, entry, pkey_buffer) — instead
 * of dsa_free'ing them directly.  Each retire snapshots the current
 * global_epoch and pushes a node onto a backend-local list.  Pass
 * InvalidDsaPointer for unused slots; passing all three Invalid is a no-op
 * (no allocation).
 *
 * RowCacheLocalGC walks this backend's retire list and dsa_free's every
 * node whose recorded epoch < safe_epoch_published (set by the future GC
 * bgworker; until then it remains 0 and LocalGC is effectively a no-op).
 *
 * RowCacheLocalRetireCount returns the current pending count for
 * diagnostics / pg_row_cache_stat (Phase 5).
 */
extern void   RowCacheEpochRetire(dsa_pointer dp_a,
								  dsa_pointer dp_b,
								  dsa_pointer dp_c);
extern void   RowCacheLocalGC(void);
extern size_t RowCacheLocalRetireCount(void);

/*
 * Register the rowcache-gc background worker.  Called once from postmaster
 * startup (see src/backend/postmaster/postmaster.c) before
 * process_shared_preload_libraries runs, so the worker slot is reserved
 * before extensions get a chance to consume one.
 */
extern void RowCacheGCRegister(void);

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

/*
 * Enter a row-cache read critical section.
 *
 * Snapshots the current global_epoch into MyProc->rowcache_local_epoch
 * with release-store semantics so any subsequent atomic_load(payload_dp)
 * inside the section is ordered after this store.
 *
 * Hot path: 1 atomic_load + 1 atomic_store + 1 memory barrier (~3-5 ns).
 * No RMW, no lock, no cache-line ping-pong (each backend writes its own
 * PGPROC cache line).
 */
static inline void
RowCacheEpochEnter(void)
{
	uint64		e = RowCacheGlobalEpochRead();

	pg_atomic_write_u64(&MyProc->rowcache_local_epoch, e);
	pg_memory_barrier();
}

/*
 * Exit a row-cache read critical section.
 *
 * Stores 0, telling the GC that this backend no longer holds a reference
 * to any retire-list block whose recorded epoch is <= the previously
 * observed value.
 */
static inline void
RowCacheEpochExit(void)
{
	pg_atomic_write_u64(&MyProc->rowcache_local_epoch, 0);
}

/* Load / Drop a relation's cache.  See lifetime contract above. */
extern void RelationRowCacheLoadRelation(Relation rel);
extern void RelationRowCacheDropRelation(Oid relid);

/*
 * V4 pkey-driven fast path (used by nodeIndexscan.c).
 * On hit returns true; *is_visible tells the caller whether the tuple was
 * MVCC-visible to `snapshot`.  On not-loaded / not-found / not-eligible
 * returns false (caller must fall back to btree).
 *
 * `pkey_val` is a non-null Datum of the relation's pkey column type.
 */
extern bool RelationRowCachePkeyFetch(Oid relid,
									  Datum pkey_val,
									  Snapshot snapshot,
									  TupleTableSlot *slot,
									  bool *is_visible,
									  bool *has_hot_chain);

/*
 * Return the 1-based pkey attno currently registered for this relation
 * (matching the cache's pkey column).  Returns 0 if the relation is not
 * cached, not loaded, or its cache uses a key shape this Phase doesn't
 * support.  Cheap; safe on the executor hot path.
 */
extern AttrNumber RelationRowCachePkeyAttno(Oid relid);

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
