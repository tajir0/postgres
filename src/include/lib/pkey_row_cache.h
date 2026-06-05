/*-------------------------------------------------------------------------
 *
 * pkey_row_cache.h
 *	  Primary-key-driven secondary index for the relation row cache.
 *
 * The TID-keyed row cache (tid_row_cache.{c,h}) stores tuples indexed by
 * (blockno, off).  To answer a `WHERE pkey = ?` lookup that path still
 * has to traverse the btree first to obtain a TID, which dominates the
 * cost under multi-process contention.
 *
 * PkeyIndex is a read-only, lock-free hash table built once at Load time
 * that maps `pkey_val -> (blockno, off)`, allowing the executor to skip
 * the btree entirely for simple pkey equality lookups.
 *
 * V1 scope:
 *   - Single-column primary key
 *   - Pass-by-value attribute (attbyval=true, attlen<=8): int2/int4/int8/oid
 *
 * Out of scope (returns "unsupported" -> caller falls back to btree path):
 *   - Composite primary keys
 *   - Pass-by-reference types (text, varchar, numeric, etc.)
 *   - Tables without a primary key
 *   - Deferrable primary keys
 *
 * Concurrency:
 *   - The PkeyIndex is allocated in DSA and never modified after Load
 *     publishes it (same lifetime contract as the TID block array).
 *   - Lookups perform pure reads + linear probing, no atomics, no locks.
 *   - Cross-process safety: only Datums of byval types are stored in
 *     buckets; no process-local pointers, so the index is valid in any
 *     backend that has dsa_attach'd to the global row cache area.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PKEY_ROW_CACHE_H
#define PKEY_ROW_CACHE_H

#include "postgres.h"

#include "storage/itemptr.h"
#include "utils/dsa.h"
#include "utils/rel.h"

/*
 * PkeyHashEntry: one bucket in the open-addressing hash table.
 *
 * Layout is exactly 16 bytes for cache-line friendliness; 4 buckets fit in
 * one 64-byte line.  `occupied` is treated as a small flag (0 = empty,
 * 1 = filled); we never tombstone because the index is read-only after
 * Load.
 */
typedef struct PkeyHashEntry
{
	Datum		pkey_val;		/* exact Datum representation of the key */
	uint32		blockno;		/* heap block of the corresponding tuple */
	uint16		off;			/* offset within block (1-based) */
	uint8		occupied;		/* 0 = empty slot, 1 = filled */
	uint8		_pad;
} PkeyHashEntry;

StaticAssertDecl(sizeof(PkeyHashEntry) == 16,
				 "PkeyHashEntry must stay 16 bytes for cache alignment");

/*
 * PkeyIndex: header followed inline by PkeyHashEntry buckets[n_buckets].
 *
 * The whole structure is a single dsa_allocate so that it can be located
 * by a single dsa_pointer and torn down with a single dsa_free.
 */
typedef struct PkeyIndex
{
	uint32		n_buckets;		/* power-of-2 */
	uint32		mask;			/* n_buckets - 1 */
	int			pkey_attno;		/* 1-based attno (matches pg_attribute) */
	int16		pkey_typlen;	/* attlen of the pkey column */
	bool		pkey_byval;		/* must be true in V1 */
	bool		_pad1;
	uint32		n_entries;		/* actual number of inserted keys */
	/* PkeyHashEntry buckets[n_buckets] follows immediately, MAXALIGN'd */
} PkeyIndex;

#define PKEY_INDEX_BUCKETS(pi) \
	((PkeyHashEntry *)((char *)(pi) + MAXALIGN(sizeof(PkeyIndex))))

/*
 * Build a PkeyIndex for `rel` if its primary key meets V1 constraints.
 *
 * Caller must:
 *   - Hold AccessShareLock (or stronger) on `rel`
 *   - Have already attached to the global row cache DSA (`area` valid)
 *   - Provide an estimated row count for sizing (typically equal to the
 *     number of tuples that will be loaded; pass the relation's
 *     reltuples or the actual row count if known)
 *
 * On success returns a valid dsa_pointer with empty buckets ready for
 * `RowCachePkeyIndexInsert` calls.  `*out_attno` receives the 1-based
 * attribute number of the pkey column.
 *
 * Returns InvalidDsaPointer (and sets *out_attno = 0) when:
 *   - Relation has no primary key (or it is deferrable)
 *   - Primary key is composite
 *   - Primary key column is not pass-by-value
 */
extern dsa_pointer RowCachePkeyIndexAlloc(Relation rel,
										  dsa_area *area,
										  uint64 estimated_rows,
										  AttrNumber *out_attno);

/*
 * Insert one (pkey_val, tid) pair.  Must only be called from the Load
 * path while holding rel_lock(EXCLUSIVE), with no concurrent readers.
 *
 * `pkey_val` is the Datum extracted from the tuple via
 * `slot->tts_values[pkey_attno - 1]`; caller must already have checked
 * that the corresponding `tts_isnull[]` flag is false.
 */
extern void RowCachePkeyIndexInsert(dsa_area *area,
									dsa_pointer idx_dp,
									Datum pkey_val,
									ItemPointer tid);

/*
 * Lookup `pkey_val` in the index.  On hit returns true and fills
 * *out_tid; on miss returns false.  Pure read path: no atomics, no
 * locks; safe to call concurrently from any number of backends.
 */
extern bool RowCachePkeyIndexLookup(dsa_area *area,
									dsa_pointer idx_dp,
									Datum pkey_val,
									ItemPointer out_tid);

/*
 * Free the entire PkeyIndex.  Single dsa_free since the buckets are
 * inlined in the same allocation.
 */
extern void RowCachePkeyIndexFree(dsa_area *area, dsa_pointer idx_dp);

#endif							/* PKEY_ROW_CACHE_H */
