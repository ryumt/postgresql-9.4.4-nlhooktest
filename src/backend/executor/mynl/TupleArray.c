#ifndef HEAD_TUPLEARRAY__
#define HEAD_TUPLEARRAY__

#include "socket.h"
#include "myerror.h"


typedef struct AttributeInfo {
	/* number of attributes */
        int num;
        /* type per attribute */
        int *types;
	/* size per attribute */
	int *sizes;
	/*
	 * number of repetition per attribute,
         * if > 0, the attribute is an array following Datum ptr.
         */
	int *repeats;
	/* offset per attribute */
        int *offsets;
} AttributeInfo;

typedef struct TupleArray {
        /* buffer allocation unit size */
        const long alloc_size;
	
        /* pointer to buffer */
        char *data;
        /* current buffer size */
        long cur_size;
        /* size of tuple */
        int tuple_size;
        /* number of tuples in buffer */
        long num_rows;

        /* information of attributes */
        AttributeInfo att_info;
} TupleArray;


static TupleArray *constructTupleArray(void);
static void destructTupleArray(TupleArray *ta);
static void allocDataRegionTupleArray(TupleArray *ta);
static void allocAttsRegionTupleArray(TupleArray *ta, const int natts);
static int setNthAttInfoTupleArray(TupleArray *ta, const int n, 
				   const int type, const int size,
				   const int repeat, const int offset);
static int needReallocTupleArray(TupleArray *ta, char *cur_ptr);
static void reallocDataRegionTupleArray(TupleArray *ta);
static char *getPtrFromTupleArray(TupleArray *ta, const long row, const long col);

static void sendHeaderTupleArray(int sock, TupleArray *ta);
static void recvHeaderTupleArray(int sock, TupleArray *ta);
static void sendSizeTupleArray(int sock, long data_size);
static void sendDataTupleArray(int sock, TupleArray *ta, long data_size);
static long recvSizeTupleArray(int sock);
static void recvDataTupleArray(int sock, TupleArray *ta, long data_size);
static void sendPartialTupleArray(int sock, TupleArray *ta, long offset, long num);
static void sendTupleArray(int sock, TupleArray *ta);
static void recvTupleArray(int sock, TupleArray *ta);
static void sendWholeTupleArray(int sock, TupleArray *ta);
static void recvWholeTupleArray(int sock, TupleArray *ta);

static void printTupleArray(TupleArray *ta);




static 
TupleArray *
constructTupleArray(void)
{
        TupleArray *ta;
	
	ta = (TupleArray *)palloc(sizeof(TupleArray));
	if (ta == NULL) {
		// error
		fprintf(stderr, "tuple array palloc error\n");
		exit(1);
	}
	
	/* zero initialize */
        bzero(ta, sizeof(TupleArray));
	/* 
	 * const cast
	 * buffer allocation unit size -> 128MB
	 */
	*(long *)&ta->alloc_size = 1024 * 1024 * 128;
	// *(long *)&ta->alloc_size = 1024 * 128;
	
	allocDataRegionTupleArray(ta);
	
        return ta;
}

static 
void
destructTupleArray(TupleArray *ta)
{
	if (ta->att_info.types != NULL) {
		pfree(ta->att_info.types);
		ta->att_info.types = NULL;
	}
		
	if (ta->data != NULL) {
		pfree(ta->data);
		ta->data = NULL;
	}
	
	pfree(ta);
}

static inline 
void
allocDataRegionTupleArray(TupleArray *ta)
{
	ta->data = (char *)palloc(ta->alloc_size);
	if (ta->data == NULL) {
		// error
		fprintf(stderr, "data palloc error\n");
		exit(1);
	}
	
	ta->cur_size = ta->alloc_size;
}

static inline
void
allocAttsRegionTupleArray(TupleArray *ta, const int natts)
{
	AttributeInfo *ai = &ta->att_info;
	
	/* for *types, *sizes, *repeats, *offsets */
	const int array_size = sizeof(int) * natts;
	static const int array_num = 4;
	char *buffer = (char *)palloc(array_size * array_num);
	
	if (buffer == NULL) {
		// error
		fprintf(stderr, "att palloc error\n");
		exit(1);
	}
	
	ai->num = natts;
	ai->types = (int *)(buffer + 0 * array_size);
	ai->sizes = (int *)(buffer + 1 * array_size);
	ai->repeats = (int *)(buffer + 2 * array_size);
	ai->offsets = (int *)(buffer + 3 * array_size);
}


static inline 
int 
setNthAttInfoTupleArray(TupleArray *ta, const int n, 
			const int type, const int size,
			const int repeat, const int offset)
{
	int size_inc = 0;
	AttributeInfo *ai = &ta->att_info;
	
	if (repeat > 0)  {
		/* add size of Datum that points to array */
		size_inc += sizeof(Datum);
		/* add size of elements in array */
		size_inc += size * repeat;
	}
	else  {
		/* add size of element */
		size_inc += size;
	}
	
	ai->types[n] = type;
	ai->sizes[n] = size_inc;
	ai->repeats[n] = repeat;
	ai->offsets[n] = offset;
	
	/* add tuple size */
	ta->tuple_size += size_inc;
	/* return new offset */
	return offset + size_inc;
}

static inline 
int 
needReallocTupleArray(TupleArray *ta, char *cur_ptr)
{
	char *written_ptr = cur_ptr + ta->tuple_size;
	char *end_ptr = ta->data + ta->cur_size;
	
	return (written_ptr > end_ptr);	
}

static inline 
void
reallocDataRegionTupleArray(TupleArray *ta)
{
	char *new_ptr;
	long new_size;
	
	new_size = ta->cur_size = ta->cur_size * 2;
	new_ptr = (char *)repalloc(ta->data, new_size);
	if (new_ptr == NULL) {
		// error
		fprintf(stderr, "data repalloc error\n");
		pfree(ta->data);
		ta->data = NULL;
		exit(1);
	}
	
	// printf("realloc %zu -> %zu\n", new_size/2, new_size);
	// printf("realloc %p -> %p\n", ta->data, new_ptr);
		
	ta->data = new_ptr;
}

static inline 
char *
getPtrFromTupleArray(TupleArray *ta, const long row, const long col)
{
        long i;
        char *ptr = ta->data + row * ta->tuple_size;
	
	for (i = 0; i < col; i++)
                ptr += ta->att_info.offsets[i];
                	
        return ptr;
}



static inline 
void
sendHeaderTupleArray(int sock, TupleArray *ta)
{
	/* for checking returned value */
	long check_size;
	
	int natts = ta->att_info.num;
	const int array_size = sizeof(int) * natts;
	static const int array_num = 4;
	
	
	/* send size of tuple */
	check_size = sizeof(ta->tuple_size);
        if (sendData(sock, &ta->tuple_size, check_size) < check_size)
                MY_ERROR();
	
	/* send number of attributes */
	check_size = sizeof(natts);
        if (sendData(sock, &natts, check_size) < check_size)
                MY_ERROR();
	
	/* send attribute info */
	check_size = array_num * array_size;
        if (sendData(sock, ta->att_info.types, check_size) < check_size)
                MY_ERROR();
}

static inline 
void
recvHeaderTupleArray(int sock, TupleArray *ta)
{
	/* for checking returned value */
	long check_size;
	
	int natts;
	int array_size;
	static const int array_num = 4;
	
	
	/* receive size of tuple */
	check_size = sizeof(ta->tuple_size);
        if (recvData(sock, &ta->tuple_size, check_size) < check_size)
                MY_ERROR();
		
        /* receive number of attributes */
	check_size = sizeof(natts);
        if (recvData(sock, &natts, check_size) < check_size)
                MY_ERROR();
	
	allocAttsRegionTupleArray(ta, natts);
		
	array_size = sizeof(int) * natts;
	
	/* receive attribute info */
	check_size = array_num * array_size;
        if (recvData(sock, ta->att_info.types, check_size) < check_size)
                MY_ERROR();
}



static inline 
void  
sendSizeTupleArray(int sock, long data_size)
{
	/* for checking returned value */
	long check_size;
		
	/* send size of tuple buffer */
	check_size = sizeof(data_size);
        if (sendData(sock, &data_size, check_size) < check_size)
                MY_ERROR();
}

static inline 
void
sendDataTupleArray(int sock, TupleArray *ta, long data_size)
{
	/* for checking returned value */
	long check_size;
		
	/* send contents of tuple buffer */
	check_size = sizeof(char) * data_size;
        if (sendData(sock, ta->data, check_size) < check_size)
                MY_ERROR();
}

static inline 
long 
recvSizeTupleArray(int sock)
{
	/* for checking returned value */
	long check_size;
	long data_size;
	
	/* receive size of tuple buffer */
	check_size = sizeof(data_size);
        if (recvData(sock, &data_size, check_size) < check_size)
                MY_ERROR();
	
	return data_size;
}

static inline 
void
recvDataTupleArray(int sock, TupleArray *ta, long data_size)
{
	/* for checking returned value */
	long check_size;
	char *ptr;
	
	/* 
	 * check if there is enough space in buffer
	 * if isn't, re-allocate buffer region.
	 */
	while (1) {
		ptr = ta->data + data_size;
		if (needReallocTupleArray(ta, ptr)) {
			reallocDataRegionTupleArray(ta);
			continue;
		}
		break;
	}
	
	/* receive contents of buffer */
	check_size = sizeof(char) * data_size;
        if (recvData(sock, ta->data, check_size) < check_size)
                MY_ERROR();
	
	/* calculate number of received rows */
	ta->num_rows = data_size / ta->tuple_size;	
}


static inline 
void
sendPartialTupleArray(int sock, TupleArray *ta, long offset, long num)
{
	/* for checking returned value */
	long check_size;
	
	char *data = ta->data + offset;
	long data_size = num * ta->tuple_size;
	
	sendSizeTupleArray(sock, data_size);
	check_size = sizeof(char) * data_size;
        if (sendData(sock, data, check_size) < check_size)
                MY_ERROR();
}

static inline 
void
sendTupleArray(int sock, TupleArray *ta)
{
	long data_size = ta->num_rows * ta->tuple_size;
	
	sendSizeTupleArray(sock, data_size);
	sendDataTupleArray(sock, ta, data_size);
}

static inline 
void
recvTupleArray(int sock, TupleArray *ta)
{
	long size;
	
	size = recvSizeTupleArray(sock);
	recvDataTupleArray(sock, ta, size);
}



static inline 
void
sendWholeTupleArray(int sock, TupleArray *ta)
{
	sendHeaderTupleArray(sock, ta);
	
	/* send tuple buffer */
	sendTupleArray(sock, ta);
}


static inline 
void
recvWholeTupleArray(int sock, TupleArray *ta)
{
	recvHeaderTupleArray(sock, ta);
	
	/* receive tuple buffer */
	recvTupleArray(sock, ta);
}



static void
printTupleArray(TupleArray *ta)
{
	long row, col;
	int idx;
	char *data;
	AttributeInfo *ai = &ta->att_info;
	

	for (row = 0; row < ta->num_rows; row++) {
		for (col = 0; col < ai->num; col++) {
			int repeat;
			
			printf("col=%ld, repeat=%d\n", col, ai->repeats[col]);
			
			data = getPtrFromTupleArray(ta, row, col + 1);
			
			if (ai->repeats[col] != 0) {
				repeat = ai->repeats[col];
				printf("Datum for [%ld][..]=%p\n", col, *((Datum *)data));
				data += sizeof(Datum);
			}			
			else
				repeat = 1;
			
			
			for (idx = 0; idx < repeat; idx++) {
				switch (ai->types[col]) {
				case BOOLOID:
					printf("BOOLOID");
					printf("[%ld][%d]=%d\n", col, idx, ((bool *)data)[idx]);
					break;
				case INT8OID: // (long long)
					printf("INT8OID");
					printf("[%ld][%d]=%lld\n", col, idx, ((int64 *)data)[idx]);
					break;
				case INT2OID: // short
					printf("INT2OID");
					printf("[%ld][%d]=%hd\n", col, idx, ((int16 *)data)[idx]);
					break;
				case INT4OID: // long 
					printf("INT4OID"); 
					printf("[%ld][%d]=%d\n", col, idx, ((int32 *)data)[idx]);
					break;
				case FLOAT4OID:
					printf("FLOAT4OID");
					printf("[%ld][%d]=%f\n", col, idx, ((float4 *)data)[idx]);
					break;
				case FLOAT8OID:
					printf("FLOAT8OID");
					printf("[%ld][%d]=%f\n", col, idx, ((float8 *)data)[idx]);
					break;
				default:
					printf("Invalid");
					printf("Cannot print value\n");
					break;
				}
			}	      	
		}
	}
}

#endif // HEAD_TUPLEARRAY__

