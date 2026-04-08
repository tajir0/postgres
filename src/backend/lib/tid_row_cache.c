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
 * Reconstructs a HeapTupleData on the stack and stores it via
 * ExecForceStoreHeapTupleNoCopy, then copies the pre-decoded
 * values[] and isnull[] arrays into the slot.
 */
bool
RowCacheUnflattenToSlot(const FlatCachedTuple *flat,
						Oid relid,
						ItemPointer tid,
						TupleTableSlot *slot)
{
	HeapTupleData htup;
	Datum	   *src_values;
	bool	   *src_isnull;

	Assert(flat != NULL);
	Assert(slot != NULL);
	Assert(tid != NULL);

	if (slot->tts_tupleDescriptor->natts != flat->natts)
		return false;

	htup.t_data = (HeapTupleHeader) FLAT_TUPLE_HTUP_DATA(flat);
	htup.t_len = flat->htup_len;
	htup.t_tableOid = relid;
	ItemPointerCopy(tid, &htup.t_self);

	ExecForceStoreHeapTupleNoCopy(&htup, slot, false);

	src_values = FLAT_TUPLE_VALUES(flat);
	src_isnull = FLAT_TUPLE_ISNULL(flat);

	memcpy(slot->tts_values, src_values, sizeof(Datum) * flat->natts);
	memcpy(slot->tts_isnull, src_isnull, sizeof(bool) * flat->natts);
	slot->tts_nvalid = flat->natts;
	ItemPointerCopy(tid, &slot->tts_tid);
	slot->tts_tableOid = relid;

	return true;
}
