/*-------------------------------------------------------------------------
 *
 * pkey_row_cache.c
 *	  Read-only DSA hash table mapping pkey_val -> (blockno, off).
 *
 * See pkey_row_cache.h for the high-level design.  This file provides
 * Build / Insert / Lookup / Free for a single relation's PkeyIndex.
 *
 * Hashing strategy (V1):
 *   We support only pass-by-value types whose Datum representation is
 *   canonical (zero-extended to 64 bits, e.g. int4 -> uint64).  Two
 *   equal byval Datums always have identical 64-bit byte patterns, so
 *   we can:
 *     - Compare keys with a plain `==` on Datum
 *     - Hash keys by mixing the 64 bits with a fast finalizer
 *
 *   This avoids per-lookup typcache / fmgr indirection and keeps the
 *   hot path branch-free.  Pass-by-reference support (text, numeric,
 *   composite keys) is deferred to V2 and will require storing
 *   additional payload + a typcache-driven hash/eq callback.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/relation.h"
#include "catalog/pg_index.h"
#include "lib/pkey_row_cache.h"
#include "storage/itemptr.h"
#include "storage/lockdefs.h"
#include "utils/dsa.h"
#include "utils/rel.h"
#include "utils/relcache.h"

/* Min/max bucket count to keep the index sane. */
#define PKEY_MIN_BUCKETS	16
#define PKEY_MAX_BUCKETS	(1u << 30)	/* ~1 billion, far above any realistic load */

/* ----------------------------------------------------------------
 * Internal helpers
 * ---------------------------------------------------------------- */

/*
 * Splitmix64-style finalizer.  Mixes 64 bits of input into a high-quality
 * 64-bit hash.  Used because:
 *   - Constant-time, branch-free, ~3 cycles
 *   - No table lookups, fits in registers
 *   - Avalanches well even for low-entropy keys (e.g. small integers)
 */
static inline uint64
pkey_mix64(uint64 x)
{
	x ^= x >> 30;
	x *= UINT64CONST(0xbf58476d1ce4e5b9);
	x ^= x >> 27;
	x *= UINT64CONST(0x94d49bb133111eb1);
	x ^= x >> 31;
	return x;
}

/*
 * Round n up to the next power of 2, clamped to [PKEY_MIN_BUCKETS,
 * PKEY_MAX_BUCKETS].
 */
static uint32
pkey_next_pow2(uint64 n)
{
	uint32		v = PKEY_MIN_BUCKETS;

	while ((uint64) v < n && v < PKEY_MAX_BUCKETS)
		v <<= 1;
	return v;
}

/*
 * Decide whether `rel` is eligible for pkey-driven indexing under V1
 * constraints.  Returns the 1-based attno of the pkey column on
 * success, 0 on rejection.
 */
static AttrNumber
pkey_eligible_attno(Relation rel)
{
	Oid			pkindex_oid;
	Relation	pkindex;
	AttrNumber	attno = 0;
	Form_pg_index ind;
	Form_pg_attribute attr;

	/* Reject deferrable PKs: they aren't immediately enforced. */
	pkindex_oid = RelationGetPrimaryKeyIndex(rel, false);
	if (!OidIsValid(pkindex_oid))
		return 0;

	pkindex = index_open(pkindex_oid, AccessShareLock);

	/*
	 * Defensive: index_open should never return NULL (it errors on
	 * invalid relations), but guard the dereference anyway.
	 */
	if (pkindex == NULL)
		return 0;

	ind = pkindex->rd_index;
	if (ind == NULL)
	{
		index_close(pkindex, AccessShareLock);
		return 0;
	}

	/* V1: single-column primary key only. */
	if (ind->indnkeyatts != 1)
	{
		index_close(pkindex, AccessShareLock);
		return 0;
	}

	attno = ind->indkey.values[0];
	if (attno <= 0)				/* defensive: system or expression key */
	{
		index_close(pkindex, AccessShareLock);
		return 0;
	}

	index_close(pkindex, AccessShareLock);

	/* V1: pass-by-value attribute only. */
	attr = TupleDescAttr(RelationGetDescr(rel), attno - 1);
	if (!attr->attbyval || attr->attlen <= 0 || attr->attlen > (int) sizeof(Datum))
		return 0;

	return attno;
}

/* ----------------------------------------------------------------
 * Public API
 * ---------------------------------------------------------------- */

dsa_pointer
RowCachePkeyIndexAlloc(Relation rel,
					   dsa_area *area,
					   uint64 estimated_rows,
					   AttrNumber *out_attno)
{
	AttrNumber	attno;
	Form_pg_attribute attr;
	uint32		n_buckets;
	Size		alloc_size;
	dsa_pointer dp;
	PkeyIndex  *pi;

	Assert(area != NULL);
	Assert(out_attno != NULL);

	*out_attno = 0;

	attno = pkey_eligible_attno(rel);
	if (attno == 0)
		return InvalidDsaPointer;

	attr = TupleDescAttr(RelationGetDescr(rel), attno - 1);

	/*
	 * Size buckets to keep load factor <= 0.5 (n_buckets >= 2 * rows).
	 * If reltuples is 0 (empty relation, freshly built table that hasn't
	 * been ANALYZE'd), fall back to PKEY_MIN_BUCKETS.
	 */
	if (estimated_rows == 0)
		n_buckets = PKEY_MIN_BUCKETS;
	else
		n_buckets = pkey_next_pow2(estimated_rows * 2);

	alloc_size = MAXALIGN(sizeof(PkeyIndex)) +
		(Size) n_buckets * sizeof(PkeyHashEntry);

	dp = dsa_allocate0(area, alloc_size);
	if (!DsaPointerIsValid(dp))
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail_internal("row cache: cannot allocate PkeyIndex (%zu bytes).",
									alloc_size)));

	pi = (PkeyIndex *) dsa_get_address(area, dp);
	pi->n_buckets = n_buckets;
	pi->mask = n_buckets - 1;
	pi->pkey_attno = attno;
	pi->pkey_typlen = attr->attlen;
	pi->pkey_byval = true;
	pi->n_entries = 0;

	*out_attno = attno;
	return dp;
}

void
RowCachePkeyIndexInsert(dsa_area *area,
						dsa_pointer idx_dp,
						Datum pkey_val,
						ItemPointer tid)
{
	PkeyIndex  *pi;
	PkeyHashEntry *buckets;
	uint64		h;
	uint32		pos;
	uint32		probe_count = 0;

	Assert(DsaPointerIsValid(idx_dp));
	Assert(tid != NULL);

	pi = (PkeyIndex *) dsa_get_address(area, idx_dp);
	buckets = PKEY_INDEX_BUCKETS(pi);

	h = pkey_mix64((uint64) pkey_val);
	pos = (uint32) h & pi->mask;

	/*
	 * Linear probing.  Because we sized the table for load factor <= 0.5
	 * the expected probe length is < 1.5; we still cap iterations at
	 * n_buckets to fail loudly if the caller oversubscribes the index.
	 */
	while (buckets[pos].occupied)
	{
		/* Defensive: detect duplicate-key insert (shouldn't happen for
		 * a real PK).  Overwrite silently to keep behaviour idempotent.
		 */
		if (buckets[pos].pkey_val == pkey_val)
			break;

		pos = (pos + 1) & pi->mask;

		if (++probe_count >= pi->n_buckets)
			elog(ERROR, "row cache pkey index full (%u buckets, n_entries=%u)",
				 pi->n_buckets, pi->n_entries);
	}

	if (!buckets[pos].occupied)
		pi->n_entries++;

	buckets[pos].pkey_val = pkey_val;
	buckets[pos].blockno = ItemPointerGetBlockNumberNoCheck(tid);
	buckets[pos].off = ItemPointerGetOffsetNumberNoCheck(tid);
	buckets[pos].occupied = 1;
}

bool
RowCachePkeyIndexLookup(dsa_area *area,
						dsa_pointer idx_dp,
						Datum pkey_val,
						ItemPointer out_tid)
{
	PkeyIndex  *pi;
	PkeyHashEntry *buckets;
	uint64		h;
	uint32		pos;
	uint32		probe_count = 0;

	if (!DsaPointerIsValid(idx_dp))
		return false;

	pi = (PkeyIndex *) dsa_get_address(area, idx_dp);
	buckets = PKEY_INDEX_BUCKETS(pi);

	h = pkey_mix64((uint64) pkey_val);
	pos = (uint32) h & pi->mask;

	/*
	 * Linear probing until we hit either:
	 *   - The matching key  -> return its TID
	 *   - An empty slot     -> miss (open addressing invariant: a key
	 *                          inserted with starting bucket b is in
	 *                          buckets[b..first_empty_after_b])
	 */
	while (buckets[pos].occupied)
	{
		if (buckets[pos].pkey_val == pkey_val)
		{
			ItemPointerSet(out_tid,
						   buckets[pos].blockno,
						   buckets[pos].off);
			return true;
		}
		pos = (pos + 1) & pi->mask;

		/* Safety net: if we wrap fully, the table is corrupted. */
		if (++probe_count >= pi->n_buckets)
			return false;
	}
	return false;
}

void
RowCachePkeyIndexFree(dsa_area *area, dsa_pointer idx_dp)
{
	if (!DsaPointerIsValid(idx_dp))
		return;
	dsa_free(area, idx_dp);
}
