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
 * the end of the block; values[] stores a pointer into that area
 * (valid in the current process's DSA mapping).
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
			dst_values[i] = PointerGetDatum(varlen_cursor);
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
 * Allocates a HeapTuple in the current memory context (via heap_copytuple)
 * and stores it in the slot with shouldFree=true so the slot owns the memory.
 * This avoids two previous bugs:
 *
 *   1. HeapTupleData on stack + NoCopy: after RowCacheUnflattenToSlot returns
 *      the stack frame is reused, leaving slot->tts_heaptuple pointing at
 *      garbage.  Generic plans (triggered by PL/pgSQL variables after 5
 *      executions) defer slot consumption past the return, reliably
 *      triggering a SIGSEGV in heap_deform_tuple / fill_val.
 *
 *   2. DSA pointer lifetime: t_data pointed directly into the DSA block;
 *      once the caller releases its pin another backend can Drop/Reload the
 *      cache and free that memory.
 *
 * heap_copytuple produces a palloc'd copy that is independent of the DSA
 * block, so both hazards are eliminated.  The extra palloc + memcpy costs
 * ~60-100 ns per cache hit, which is well within the savings from avoiding
 * a shared-buffer pin + heap_deform_tuple on a cold page.
 */
bool
RowCacheUnflattenToSlot(const FlatCachedTuple *flat,
						Oid relid,
						ItemPointer tid,
						TupleTableSlot *slot)
{
	HeapTupleData tmp;
	HeapTuple	copy;

	Assert(flat != NULL);
	Assert(slot != NULL);
	Assert(tid != NULL);

	if (slot->tts_tupleDescriptor->natts != flat->natts)
		return false;

	/*
	 * Build a temporary HeapTupleData pointing into the DSA block just long
	 * enough to copy it out.  heap_copytuple pallocs
	 * HEAPTUPLESIZE + t_len bytes and memcpys t_data, so the result is
	 * completely independent of the DSA block and of this stack frame.
	 */
	tmp.t_len = flat->htup_len;
	tmp.t_data = (HeapTupleHeader) FLAT_TUPLE_HTUP_DATA(flat);
	tmp.t_tableOid = relid;
	ItemPointerCopy(tid, &tmp.t_self);

	copy = heap_copytuple(&tmp);	/* palloc'd; owned by caller's mcxt */

	/*
	 * ExecStoreHeapTuple with shouldFree=true: the slot takes ownership and
	 * will pfree copy when the slot is cleared or replaced.
	 */
	ExecStoreHeapTuple(copy, slot, true);

	return true;
}
