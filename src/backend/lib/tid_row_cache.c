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
 * Restores both the physical HeapTuple and the pre-decoded values[]/isnull[]
 * arrays so the executor can skip heap_deform_tuple entirely (tts_nvalid is
 * set to natts on return).
 *
 * Lifetime safety:
 *   - The HeapTuple is produced by heap_copytuple, so it lives in the current
 *     memory context and is independent of the DSA block.  The slot owns it
 *     (shouldFree=true) and pfrees it on ExecClearTuple.
 *   - Pass-by-val Datums are stored by value; no pointer issues.
 *   - Pass-by-ref Datums: the FlatCachedTuple block stores byte offsets from
 *     its own base (set by RowCacheFlattenTuple).  On unflatten we call
 *     datumCopy to produce a palloc'd local copy, so tts_values[] never
 *     points back into the DSA block.  The pin can therefore be released
 *     immediately after this function returns.
 *
 * This fixes two bugs present in earlier revisions:
 *   1. HeapTupleData on the C stack passed to ExecForceStoreHeapTupleNoCopy:
 *      the stack frame was reused after return, leaving a dangling pointer.
 *      Generic plans (PL/pgSQL variables → plan cache switch after 5 execs)
 *      triggered this reliably, causing SIGSEGV in heap_deform_tuple/fill_val.
 *   2. values[] contained process-local pointers into the DSA mapping, which
 *      are meaningless in any other backend's address space.
 */
bool
RowCacheUnflattenToSlot(const FlatCachedTuple *flat,
						Oid relid,
						ItemPointer tid,
						TupleTableSlot *slot)
{
	TupleDesc	desc = slot->tts_tupleDescriptor;
	HeapTupleData tmp;
	HeapTuple	copy;
	Datum	   *src_values;
	bool	   *src_isnull;

	Assert(flat != NULL);
	Assert(slot != NULL);
	Assert(tid != NULL);

	if (desc->natts != flat->natts)
		return false;

	/*
	 * 1. Restore the physical HeapTuple.
	 *
	 * Build a temporary HeapTupleData that points into the DSA block just
	 * long enough to copy it out.  heap_copytuple pallocs
	 * HEAPTUPLESIZE + htup_len bytes and memcpys t_data, producing a copy
	 * that is completely independent of the DSA block and of this stack frame.
	 *
	 * ExecForceStoreHeapTupleNoCopy accepts any slot ops type (including
	 * TTSOpsBufferHeapTuple used by index scans).  shouldFree=true lets the
	 * slot pfree the tuple when it is cleared or replaced.
	 * Note: this call resets tts_nvalid to 0 internally; we set it to natts
	 * below after filling tts_values[].
	 */
	tmp.t_len = flat->htup_len;
	tmp.t_data = (HeapTupleHeader) FLAT_TUPLE_HTUP_DATA(flat);
	tmp.t_tableOid = relid;
	ItemPointerCopy(tid, &tmp.t_self);

	copy = heap_copytuple(&tmp);
	ExecForceStoreHeapTupleNoCopy(copy, slot, true);

	/*
	 * 2. Restore pre-decoded values[]/isnull[].
	 *
	 * Pass-by-val columns: copy the Datum value directly.
	 * Pass-by-ref columns: src_values[i] holds the byte offset from the
	 *   FlatCachedTuple base to the varlen payload (written by
	 *   RowCacheFlattenTuple).  We call datumCopy to palloc a local copy so
	 *   that tts_values[i] is independent of the DSA block.
	 *
	 * Setting tts_nvalid = natts tells the executor the slot is fully decoded;
	 * it will use tts_values[] directly and skip heap_deform_tuple.
	 */
	src_values = FLAT_TUPLE_VALUES(flat);
	src_isnull = FLAT_TUPLE_ISNULL(flat);

	for (int i = 0; i < flat->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(desc, i);

		slot->tts_isnull[i] = src_isnull[i];

		if (src_isnull[i] || attr->attbyval)
		{
			slot->tts_values[i] = src_values[i];
		}
		else
		{
			/*
			 * src_values[i] is a byte offset from the FlatCachedTuple base.
			 * Reconstruct the pointer, then datumCopy into local memory.
			 */
			Datum		dsa_datum =
				PointerGetDatum((char *) flat + (Size) src_values[i]);

			slot->tts_values[i] = datumCopy(dsa_datum, false, attr->attlen);
		}
	}

	slot->tts_nvalid = flat->natts;

	return true;
}
