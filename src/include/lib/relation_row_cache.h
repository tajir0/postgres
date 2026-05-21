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
 * Phase 1 lifetime contract (same as V3):
 *   Callers must guarantee no readers run concurrently with Load / Drop.
 *   This phase has no EBR (deferred to Phase 2): Drop frees DSA entries
 *   immediately, relying on the caller contract to avoid use-after-free.
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

#include "executor/tuptable.h"
#include "port/atomics.h"
#include "storage/proc.h"
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
