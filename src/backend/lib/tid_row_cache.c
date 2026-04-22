#include "postgres.h"

#include <string.h>

#include "access/htup_details.h"
#include "executor/tuptable.h"
#include "lib/tid_row_cache.h"
#include "utils/datum.h"
#include "utils/dsa.h"

/*
 * Flatten a slot's tuple data into a single contiguous DSA block.
 *
 * Pass-by-val Datums are stored inline in the values[] array.
 * Pass-by-ref Datums have their payloads copied to the varlen area at
 * the end of the block; values[] stores the byte offset from the start
 * of the FlatCachedTuple block to that payload (NOT a pointer).
 * Storing offsets rather than pointers makes the block valid across
 * processes, because DSA segments are mapped at different virtual
 * addresses in each backend.
 *
 * Returns a dsa_pointer to the allocated FlatCachedTuple block.
 */
dsa_pointer
RowCacheFlattenTuple(dsa_area *area, TupleTableSlot *slot)
{
	TupleDesc	desc = slot->tts_tupleDescriptor;
	int			natts = desc->natts;
	HeapTuple	htup;
	Size		total_size;
	uint32		values_offset,
				isnull_offset,
				htup_offset,
				varlen_offset;
	FlatCachedTuple *ft;
	dsa_pointer dp;
	Datum	   *dst_values;
	bool	   *dst_isnull;
	char	   *varlen_cursor;

	slot_getallattrs(slot);
	htup = ExecCopySlotHeapTuple(slot);

	values_offset = MAXALIGN(sizeof(FlatCachedTuple));
	isnull_offset = values_offset + sizeof(Datum) * natts;
	htup_offset = MAXALIGN(isnull_offset + sizeof(bool) * natts);

	varlen_offset = htup_offset + MAXALIGN(htup->t_len);
	total_size = varlen_offset;

	for (int i = 0; i < natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(desc, i);

		if (!slot->tts_isnull[i] && !attr->attbyval)
			total_size += MAXALIGN(datumGetSize(slot->tts_values[i],
												false, attr->attlen));
	}

	dp = dsa_allocate(area, total_size);
	ft = (FlatCachedTuple *) dsa_get_address(area, dp);
	memset(ft, 0, total_size);

	ft->total_size = total_size;
	ft->natts = natts;
	ft->htup_offset = htup_offset;
	ft->htup_len = htup->t_len;

	dst_values = FLAT_TUPLE_VALUES(ft);
	dst_isnull = FLAT_TUPLE_ISNULL(ft);
	varlen_cursor = (char *) ft + varlen_offset;

	for (int i = 0; i < natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(desc, i);

		dst_isnull[i] = slot->tts_isnull[i];

		if (slot->tts_isnull[i])
		{
			dst_values[i] = (Datum) 0;
		}
		else if (attr->attbyval)
		{
			dst_values[i] = slot->tts_values[i];
		}
		else
		{
			Size		datum_size = datumGetSize(slot->tts_values[i],
												 false, attr->attlen);

			memcpy(varlen_cursor,
				   DatumGetPointer(slot->tts_values[i]),
				   datum_size);
			/* Store offset from ft base, not a process-local pointer */
			dst_values[i] = (Datum) (varlen_cursor - (char *) ft);
			varlen_cursor += MAXALIGN(datum_size);
		}
	}

	memcpy((char *) ft + htup_offset, htup->t_data, htup->t_len);

	heap_freetuple(htup);
	return dp;
}

/*
 * Unflatten a FlatCachedTuple back into a TupleTableSlot.
 *
 * Fast-path design: single palloc + single bulk memcpy for the physical
 * tuple payload (htup_data + varlen), bulk memcpy for values[]/isnull[],
 * and O(natts) pointer arithmetic to fix up pass-by-ref Datums.  No
 * per-column palloc or datumCopy.
 *
 * Memory layout of the palloc'd block:
 *
 *   [ HeapTupleData (HEAPTUPLESIZE bytes) ]
 *   [ htup_data  (flat->htup_len bytes)   ]  ← copy->t_data points here
 *   [ pad to MAXALIGN                     ]
 *   [ varlen payloads                     ]
 *
 * Because the (htup_data + varlen) region has identical internal layout
 * in the FlatCachedTuple block and in the local copy, any byte offset
 * that was relative to htup_data in the DSA block is still the same
 * relative offset in the local copy.  So fixing up tts_values[i] for a
 * pass-by-ref column is just:
 *
 *     tts_values[i] = copy->t_data + (src_values[i] - flat->htup_offset)
 *
 * Lifetime safety:
 *   - The slot owns the palloc'd block (shouldFree=true) and pfrees it on
 *     ExecClearTuple.  All tts_values[] pointers point into that block, so
 *     they remain valid for the lifetime of the slot, completely decoupled
 *     from the DSA pin window.
 *
 * This replaces earlier buggy implementations:
 *   1. HeapTupleData on the C stack + ExecForceStoreHeapTupleNoCopy left a
 *      dangling pointer; generic plans triggered SIGSEGV in heap_deform.
 *   2. values[] stored process-local DSA pointers, invalid cross-process.
 */
bool
RowCacheUnflattenToSlot(const FlatCachedTuple *flat,
						Oid relid,
						ItemPointer tid,
						TupleTableSlot *slot)
{
	TupleDesc	desc = slot->tts_tupleDescriptor;
	HeapTuple	copy;
	Size		payload_len;
	char	   *local_htup;
	Datum	   *src_values;
	bool	   *src_isnull;

	Assert(flat != NULL);
	Assert(slot != NULL);
	Assert(tid != NULL);

	if (desc->natts != flat->natts)
		return false;

	/*
	 * 1. Single palloc: HeapTupleData header + (htup_data + varlen) region.
	 *
	 * payload_len covers all bytes from htup_offset to the end of the flat
	 * block, i.e. both the raw HeapTuple data and any trailing varlen
	 * payloads.  One palloc + one memcpy replaces heap_copytuple + N
	 * per-column datumCopy's.
	 */
	payload_len = flat->total_size - flat->htup_offset;
	copy = (HeapTuple) palloc(HEAPTUPLESIZE + payload_len);

	copy->t_len = flat->htup_len;
	copy->t_tableOid = relid;
	ItemPointerCopy(tid, &copy->t_self);
	copy->t_data = (HeapTupleHeader) ((char *) copy + HEAPTUPLESIZE);

	memcpy((char *) copy + HEAPTUPLESIZE,
		   (char *) flat + flat->htup_offset,
		   payload_len);

	/*
	 * ExecForceStoreHeapTupleNoCopy accepts any slot ops type (including
	 * TTSOpsBufferHeapTuple used by index scans).  shouldFree=true lets the
	 * slot pfree the tuple when cleared.  Note: this resets tts_nvalid to 0
	 * internally; we set it to natts below after filling tts_values[].
	 */
	ExecForceStoreHeapTupleNoCopy(copy, slot, true);

	/*
	 * 2. Bulk-copy pre-decoded values[]/isnull[] in one memcpy each.
	 *
	 * Pass-by-val Datums are already correct.  Pass-by-ref Datums still
	 * contain the DSA-relative offsets stored by RowCacheFlattenTuple; we
	 * fix those up in step 3.
	 */
	src_values = FLAT_TUPLE_VALUES(flat);
	src_isnull = FLAT_TUPLE_ISNULL(flat);

	memcpy(slot->tts_values, src_values, sizeof(Datum) * flat->natts);
	memcpy(slot->tts_isnull, src_isnull, sizeof(bool) * flat->natts);

	/*
	 * 3. Fix up pass-by-ref Datum pointers.
	 *
	 * For each non-null pass-by-ref column, src_values[i] is a byte offset
	 * relative to the flat block's base.  The corresponding payload in the
	 * local copy sits at (local_htup + (offset - htup_offset)), because we
	 * copied the entire htup_data+varlen region verbatim starting from
	 * local_htup.  Only arithmetic, no allocations.
	 */
	local_htup = (char *) copy->t_data;
	for (int i = 0; i < flat->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(desc, i);

		if (!src_isnull[i] && !attr->attbyval)
			slot->tts_values[i] = PointerGetDatum(
				local_htup + (Size) src_values[i] - flat->htup_offset);
	}

	slot->tts_nvalid = flat->natts;

	return true;
}
