#ifndef HEAD_RESULTBUFFER__
#define HEAD_RESULTBUFFER__

typedef struct ResultBuffer {
	int sock;
	
	int idx;
	char *buf[2];
	long size[2];
	
	long buf_size;
	long elm_size;
} ResultBuffer;


static inline 
ResultBuffer * 
constructResultBuffer(int sock, int buf_size, int elm_size)
{
	ResultBuffer *rb = (ResultBuffer *)palloc(sizeof(ResultBuffer));
	
	rb->sock = sock;
	rb->idx = 0;
	rb->buf[0] = (char *)palloc(buf_size);
	rb->buf[1] = (char *)palloc(buf_size);
	
	// cause deadlock
	// bzero(rb->size, sizeof(rb->size));
	rb->size[0] = 0;
	rb->size[1] = -1;
	
	rb->buf_size = buf_size;
	rb->elm_size = elm_size;
	
	return rb;
}

static inline 
void 
destructResultBuffer(ResultBuffer *rb)
{
	pfree(rb->buf[0]);
	pfree(rb->buf[1]);
	
	pfree(rb);
}


static inline 
void 
rewriteIndexResultBuffer(ResultBuffer *rb, int v)
{
        while (true) {
                int old = rb->idx;
		
		if (__sync_bool_compare_and_swap(&rb->idx, old, v) == true)
                        break;
        }
}

static inline 
void 
switchIndexResultBuffer(ResultBuffer *rb)
{
	int next_idx = (rb->idx + 1) % 2;
	
	while (true) {
                long old = rb->size[next_idx];
		
		if (__sync_bool_compare_and_swap(&rb->size[next_idx], old, 0) == true)
			break;
	}
	
	rewriteIndexResultBuffer(rb, next_idx);
}


#endif //HEAD_RESULTBUFFER__

