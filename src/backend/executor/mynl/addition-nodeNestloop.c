#include <unistd.h>
#include <pthread.h>

#include "socket.h"
#include "TupleArray.c"
#include "ResultBuffer.c"

static inline 
void
my_getTypeOutputInfo(Oid type, Form_pg_type pt)
{
	HeapTuple   typeTuple;
	
	typeTuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type));
	if (!HeapTupleIsValid(typeTuple))
		elog(ERROR, "cache lookup failed for type %u", type);
	
	memcpy(pt, (Form_pg_type)GETSTRUCT(typeTuple), sizeof(FormData_pg_type));
		
	ReleaseSysCache(typeTuple);
}


static 
void
readAttsInfoTupleArray(TupleArray *ta, TupleTableSlot *slot)
{
	int i;
	FormData_pg_type fd_pt;
	TupleDesc typeinfo = slot->tts_tupleDescriptor;
	int natts = typeinfo->natts;
	int offset = 0;
	
	allocAttsRegionTupleArray(ta, natts);
	
	for (i = 0; i < natts; ++i) {
		Datum attr;
		bool isnull;
		
		ArrayType *array;
		Oid etype;
		int elen;
		bool ebyval;
		char ealign;
		Datum *array_vals = NULL;
		bool *array_nulls = NULL;
		int  array_nelems;
		
		
		attr = slot_getattr(slot, i + 1, &isnull);
		if (isnull) {
			puts("there is null attribute");
			exit(1);
 			continue;
		}
		
		my_getTypeOutputInfo(typeinfo->attrs[i]->atttypid, &fd_pt);
		
		/* non-array */
		switch (typeinfo->attrs[i]->atttypid) {
		case BOOLOID:
			//puts("S:BOOLOID");
			offset = setNthAttInfoTupleArray(ta, i, BOOLOID, sizeof(bool), 0, offset);
			continue;
		case INT8OID: // (long long)
			//puts("S:INT8OID");
			offset = setNthAttInfoTupleArray(ta, i, INT8OID, sizeof(int64), 0, offset);
			continue;
		case INT2OID: // short
			//puts("S:INT2OID");
			offset = setNthAttInfoTupleArray(ta, i, INT2OID, sizeof(int16), 0, offset);
			continue;
		case INT4OID: // long
			//puts("S:INT4OID");
			offset = setNthAttInfoTupleArray(ta, i, INT4OID, sizeof(int32), 0, offset);
			continue;
		case FLOAT4OID: // float
			//puts("S:FLOAT4OID");
			offset = setNthAttInfoTupleArray(ta, i, FLOAT4OID, sizeof(float4), 0, offset);
			continue;
		case FLOAT8OID: // double
			//puts("S:FLOAT8OID");
			offset = setNthAttInfoTupleArray(ta, i, FLOAT8OID, sizeof(float8), 0, offset);
			continue;
		}
		
		
		/* array */
		array = DatumGetArrayTypeP(attr);
		etype = ARR_ELEMTYPE(array);
		
		if (etype == InvalidOid) {
			etype = typeinfo->attrs[i]->atttypid;
			if (etype == InvalidOid) {
				fprintf(stderr, "There is Invalid Oid\n");
				exit(1);
				continue;
			}
		}
				
		my_getTypeOutputInfo(etype, &fd_pt);
		
		elen = fd_pt.typlen;
		ebyval = fd_pt.typbyval;
		ealign = fd_pt.typalign;
		
		deconstruct_array(array, etype,
				  elen, ebyval, ealign,
				  &array_vals, &array_nulls, &array_nelems);
		
		/* set array size */
		offset = setNthAttInfoTupleArray(ta, i, etype, elen, array_nelems, offset);
	}
	
	allocDataRegionTupleArray(ta);
}

static inline 
void
insertDataTupleArray(TupleArray *ta, TupleTableSlot *slot)
{
	int i, k;
	FormData_pg_type fd_pt;
	TupleDesc typeinfo = slot->tts_tupleDescriptor;
	int natts = typeinfo->natts;
	
	for (i = 0; i < natts; ++i) {
		Datum       attr;
		bool        isnull;
		
		char *data;
		
		ArrayType *array;
		Oid etype;
		int elen;
		bool ebyval;
		char ealign;
		Datum *array_vals = NULL;
		bool *array_nulls = NULL;
		int array_nelems;
		
		
		attr = slot_getattr(slot, i + 1, &isnull);
		if (isnull) {
			puts("there is null attribute");
			exit(1);
 			continue;
		}
		
		my_getTypeOutputInfo(typeinfo->attrs[i]->atttypid, &fd_pt);   
		
		data = getPtrFromTupleArray(ta, ta->num_rows, i + 1);
		
		/* non-array */
		switch (typeinfo->attrs[i]->atttypid) {
		case BOOLOID:
			//puts("S:BOOLOID");
			*(bool *)data = DatumGetBool(attr);
			continue;
		case INT8OID: // (long long)
			//puts("S:INT8OID");
			*(int64 *)data = DatumGetInt64(attr);
			continue;
		case INT2OID: // short
			//puts("S:INT2OID");
			*(int16 *)data = DatumGetInt16(attr);
			continue;
		case INT4OID: // long
			//puts("S:INT4OID");
			*(int32 *)data = DatumGetInt32(attr);
			continue;
		case FLOAT4OID:
			//puts("S:FLOAT4OID");
			*(float4 *)data = DatumGetFloat4(attr);
			continue;
		case FLOAT8OID:
			//puts("S:FLOAT8OID");
			*(float8 *)data = DatumGetFloat8(attr);
			continue;
		}
		
		/* array */
		array = DatumGetArrayTypeP(attr);
		etype = ARR_ELEMTYPE(array);
		
		if (etype == InvalidOid) {
			etype = typeinfo->attrs[i]->atttypid;
			if (etype == InvalidOid) {
				fprintf(stderr, "There is Invalid Oid\n");
				exit(1);
				continue;
			}
		}
		
		my_getTypeOutputInfo(etype, &fd_pt);
		
		elen = fd_pt.typlen;
		ebyval = fd_pt.typbyval;
		ealign = fd_pt.typalign;
		
		deconstruct_array(array, etype,
				  elen, ebyval, ealign,
				  &array_vals, &array_nulls, &array_nelems);
				
		*(Datum *)data = attr;
		data += sizeof(Datum);
		
		for (k = 0; k < array_nelems; k++) {
			if (array_nulls != NULL && array_nulls[k]) {
				printf("att[%d][%d]=null\n", i, k);
				exit(1);
				continue;
			}
			
			switch (etype) {
			case BOOLOID:
				//puts("A:BOOLOID");
				((bool *)data)[k] = *((bool *)DatumGetPointer(array_vals)+k);
				break;
			case INT8OID: // (long long)
				//puts("A:INT8OID");
				((int64 *)data)[k] = *((int64 *)DatumGetPointer(array_vals)+k);
				break;
			case INT2OID: // short
				//puts("A:INT2OID");
				((int16 *)data)[k] = *((int16 *)DatumGetPointer(array_vals)+k);
				break;
			case INT4OID: // long
				//puts("A:INT4OID");
				((int32 *)data)[k] = *((int32 *)DatumGetPointer(array_vals)+k);
				break;
			case FLOAT4OID:
				//puts("A:FLOAT4OID");
				((float4 *)data)[k] = *((float4 *)DatumGetPointer(array_vals)+k);
				break;
			case FLOAT8OID:
				//puts("A:FLOAT8OID");
				((float8 *)data)[k] = *((float8 *)DatumGetPointer(array_vals)+k);
				break;
			}
		}
		
		if (needReallocTupleArray(ta, data))
			reallocDataRegionTupleArray(ta);
	}
	
	ta->num_rows++;
}

static
void *
receiveJoinResult(void *arg)
{
	ResultBuffer *rb = (ResultBuffer *)arg;
	int cur_idx = (rb->idx + 1) % 2;
	
	/* to make the thread canceled any time */
	while (true) {
		long size;
		
		if (rb->size[rb->idx] != 0 || cur_idx == rb->idx) {
			__sync_synchronize();
			pthread_testcancel();
			continue;
		}
		
		cur_idx = rb->idx;
		
		if (cur_idx < 0)
			break;
		
		if ((size = recvData(rb->sock, rb->buf[cur_idx], rb->buf_size)) < 0)
			MY_ERROR();
						
		if (size == 0) {
			puts("remote closed.");
			
			while (true) {
				long old = rb->size[cur_idx];
				if (__sync_bool_compare_and_swap(&rb->size[cur_idx], old, -1) == true)
					break;
			}
			
			break;
		}		
		
		while (true) {
			long old = rb->size[cur_idx];
			
			if (__sync_bool_compare_and_swap(&rb->size[cur_idx], old, size) == true)
				break;
		}
	}
	
        return NULL;
}
	

