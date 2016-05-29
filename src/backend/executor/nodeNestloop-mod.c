/*-------------------------------------------------------------------------
 *
 * nodeNestloop.c
 *	  routines to support nest-loop joins
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeNestloop.c
 *
 *-------------------------------------------------------------------------
 */
/*
 *	 INTERFACE ROUTINES
 *		ExecNestLoop	 - process a nestloop join of two plans
 *		ExecInitNestLoop - initialize the join
 *		ExecEndNestLoop  - shut down the join
 */

#include "postgres.h"

#include "executor/execdebug.h"
#include "executor/nodeNestloop.h"
#include "utils/memutils.h"

#include "access/htup_details.h"



/* ----------------------------------------------------------------
 *		ExecNestLoop(node)
 *
 * old comments
 *		Returns the tuple joined from inner and outer tuples which
 *		satisfies the qualification clause.
 *
 *		It scans the inner relation to join with current outer tuple.
 *
 *		If none is found, next tuple from the outer relation is retrieved
 *		and the inner relation is scanned from the beginning again to join
 *		with the outer tuple.
 *
 *		NULL is returned if all the remaining outer tuples are tried and
 *		all fail to join with the inner tuples.
 *
 *		NULL is also returned if there is no tuple from inner relation.
 *
 *		Conditions:
 *		  -- outerTuple contains current tuple from outer relation and
 *			 the right son(inner relation) maintains "cursor" at the tuple
 *			 returned previously.
 *				This is achieved by maintaining a scan position on the outer
 *				relation.
 *
 *		Initial States:
 *		  -- the outer child and the inner child
 *			   are prepared to return the first tuple.
 * ----------------------------------------------------------------
 */

#include "catalog/pg_type.h"
#include "utils/syscache.h"
#include "utils/array.h"

#include "mynl/addition-nodeNestloop.c"


static int Sock;
static char *Address = "localhost";
static int CJPort = 59999;

static TupleArray *OuterTupleArray;
static TupleArray *InnerTupleArray;
static const long MAX_RESULT = 10000000;
//static const long MAX_RESULT = 100000;


static int VarNum = 0;
static int OpNum = 0;

static int CJState = 0;

static ProjectionInfo * ProjInfo;


static MemoryContext oldContext;
static MemoryContext queryContext;



TupleTableSlot *
ExecNestLoop(NestLoopState *node)
{
	NestLoop   *nl;
	PlanState  *innerPlan;
	PlanState  *outerPlan;
	TupleTableSlot *outerTupleSlot;
	TupleTableSlot *innerTupleSlot;
	// List	   *joinqual;
	List	   *otherqual;
	ExprContext *econtext;
	// ListCell   *lc;
	
	//Plan *out, *inn;
	//out = node->js.ps.plan->righttree;
	//inn = node->js.ps.plan->lefttree;
	
	// needed for result
	static const int RESERVE_SIZE = sizeof(long);
	
	static pthread_t recv_thread;
	static ResultBuffer *rb;
	
	static int cur_idx;
	static char *cur_ptr, *end_ptr;
	static long data_size;
		
	static Datum *values;
	static bool  *isnull;
	static Datum *data;
	static TupleTableSlot *slot;
	
	static int i, j;
		
		
	
	/*
	 * get information from the node
	 */
	ENL1_printf("getting info from node");
	
	nl = (NestLoop *) node->js.ps.plan;
	// joinqual = node->js.joinqual;
	otherqual = node->js.ps.qual;
	//outerPlan = outerPlanState(node);
	//innerPlan = innerPlanState(node);
	outerPlan = innerPlanState(node);
	innerPlan = outerPlanState(node);
	econtext = node->js.ps.ps_ExprContext;
	
	
	/*
	 * Check to see if we're still projecting out tuples from a previous join
	 * tuple (because there is a function-returning-set in the projection
	 * expressions).  If so, try to project another one.
	 */
	if (node->js.ps.ps_TupFromTlist) {
		TupleTableSlot *result;
		ExprDoneCond isDone;
		
		result = ExecProject(node->js.ps.ps_ProjInfo, &isDone);
		if (isDone == ExprMultipleResult)
			return result;
		/* Done with that source tuple... */
		node->js.ps.ps_TupFromTlist = false;
	}
	
	
	/*
	 * Ok, everything is setup for the join so now loop until we return a
	 * qualifying join tuple.
	 */
	ENL1_printf("entering main loop");
	
	for (;;) {
		/*
		 * Reset per-tuple memory context to free any expression evaluation
		 * storage allocated in the previous tuple cycle.  Note this can't happen
		 * until we're done projecting out tuples from a join tuple.
		 */
		ResetExprContext(econtext);
		
		switch (CJState) {
		case 0: 
			oldContext = MemoryContextSwitchTo(queryContext);
			
			/* fetch first outer tuple */
			outerTupleSlot = ExecProcNode(outerPlan);
			
			readAttsInfoTupleArray(OuterTupleArray, outerTupleSlot);
			insertDataTupleArray(OuterTupleArray, outerTupleSlot);
			
			CJState++;
			
			MemoryContextSwitchTo(oldContext);
			continue;
		case 1:
			oldContext = MemoryContextSwitchTo(queryContext);
			
			/* fetch first inner tuple */			
			innerTupleSlot = ExecProcNode(innerPlan);
			
			readAttsInfoTupleArray(InnerTupleArray, innerTupleSlot);
			insertDataTupleArray(InnerTupleArray, innerTupleSlot);
						
			CJState++;
			MemoryContextSwitchTo(oldContext);
			continue;
		case 2:
			oldContext = MemoryContextSwitchTo(queryContext);
			/* fill outer buffer */
			ENL1_printf("getting new outer tuple");
			outerTupleSlot = ExecProcNode(outerPlan);
			
			/*
			 * if there are no more outer tuples, finish to fill outer buffer.
			 */
			if (TupIsNull(outerTupleSlot)) {
				ENL1_printf("no outer tuple, ending fetch");
				CJState++;
			}				
			else {
				ENL1_printf("Saving new outer tuple information");
				insertDataTupleArray(OuterTupleArray, outerTupleSlot);
			}
			
			MemoryContextSwitchTo(oldContext);
			continue;
		case 3:
			oldContext = MemoryContextSwitchTo(queryContext);
			/* fill inner buffer */
			ENL1_printf("getting new inner tuple");
			innerTupleSlot = ExecProcNode(innerPlan);
			
			/*
			 * if there are no more outer tuples, finish to fill outer buffer.
			 */
			if (TupIsNull(innerTupleSlot)) {
				ENL1_printf("no iner tuple, ending fetch");
				CJState++;
			}
			else {
				ENL1_printf("saving new inner tuple information");
				insertDataTupleArray(InnerTupleArray, innerTupleSlot);
			}
			
			MemoryContextSwitchTo(oldContext);
			continue;
		case 4:
			/*
			 * at this point we have a new pair of inner and outer tuples so we
			 * test the inner and outer tuples to see if they satisfy the node's
			 * qualification.
			 *
			 * Only the joinquals determine MatchedOuter status, but all quals
			 * must pass to actually return the tuple.
			 */
			
			/*
			puts("--------------outer_ta----------------");
			printTupleArray(OuterTupleArray);
			puts("--------------inner_ta----------------");
			printTupleArray(InnerTupleArray);
			*/

			/*
			  sendOpInfo(Sock, OpNum);
			  sendResultOpInfo(Sock, VarNum);
			*/
			
			sendWholeTupleArray(Sock, OuterTupleArray);
			sendWholeTupleArray(Sock, InnerTupleArray);
			
			puts("table data sended.");
			CJState++;
			
			/* Result Receiving */
		case 5: 
			rb = constructResultBuffer(Sock, sizeof(Datum) * MAX_RESULT, sizeof(Datum) * VarNum);
			
			pthread_create(&recv_thread, NULL, receiveJoinResult, rb);
			data_size = 0;
			i = 0;
			
			CJState++;
		case 6: 
			/* wait until results arive */
			cur_idx = rb->idx;
			printf("cur_idx = %d\n", cur_idx);
			while (rb->size[cur_idx] == 0)
				__sync_synchronize();
			
			switchIndexResultBuffer(rb);
			printf("buffer[%d] received\n", cur_idx);
			
			data = (Datum *)rb->buf[cur_idx];
			data_size = rb->size[cur_idx];
			printf("buffer[%d] size = %ld\n", cur_idx, rb->size[cur_idx]);
			
			/* terminate */
			if (data_size == -1) {
				puts("terminator found");
				rewriteIndexResultBuffer(rb, -1);
				pthread_cancel(recv_thread);
				puts("pthread_cancel");
				pthread_join(recv_thread, NULL);
				puts("pthread_join");
				destructResultBuffer(rb);
				
				puts("received all results");
				return NULL;
			}
			
			CJState++;
		case 7: 
			ProjInfo = node->js.ps.ps_ProjInfo;
			slot = ProjInfo->pi_slot;
			ExecClearTuple(slot);
			
			values = slot->tts_values;
			isnull = slot->tts_isnull;
			
			/* terminator found */
			/*
			if (Int32GetDatum(*((int *)data + i)) == 0)
				return NULL;
			*/			

			for (i = 0; i < VarNum; i++) {
				values[i] =  Int32GetDatum(*((int *)data + i));
				isnull[i] = false; 		
			}
			data += VarNum / 2;
			data_size -= sizeof(int) * VarNum;
			{
				TupleTableSlot *tts = ExecStoreVirtualTuple(slot);
				
				/* done all results in current buffer */
				if (data_size == 0)
					CJState--;
				
				/* return result */
				//puts("return tts");
				return tts;
			}
			break;
		}
	}
}
/* ----------------------------------------------------------------
 *		ExecInitNestLoop
 * ----------------------------------------------------------------
 */
NestLoopState *
ExecInitNestLoop(NestLoop *node, EState *estate, int eflags)
{
	List *joinqual = node->join.joinqual;
	List *targetlist = node->join.plan.targetlist;
	NestLoopState *nlstate;
	
	queryContext = estate->es_query_cxt;
	
	
	// pprint(node);
	if (joinqual != NULL) {
		Expr *root = (Expr *)joinqual->head->data.ptr_value;
		puts("joinqual converting...");
		/*
		  allocOpInfo();
		  OpNum = convertQualTree(root, OpInfo, SortedOpInfo);
		*/
	}
	else
		puts("joinqual is null, skip converting.");
	
	/*
	  puts("------checking--------");
	  for (int i = 0; i < OpNum; i++) {
	  printf("node[%d]: ord = %d, type = %d, opno = %d\n", i, SortedOpInfo[i].order, SortedOpInfo[i].type, SortedOpInfo[i].opid);
	  printf("\t left = %ld, right = %ld\n", SortedOpInfo[i].left, SortedOpInfo[i].right);
	  printf("\t dst = %d, pos = %d\n", SortedOpInfo[i].dst_idx, SortedOpInfo[i].dst_pos);
	  }
	*/
	
	if (targetlist != NULL) {
		puts("targetlist checking...");
		
		VarNum = targetlist->length;

		/*
		  allocResultOpInfo(VarNum);
		  convertTargetList(targetlist, VarNum);
		*/
	}
	
	CJState = 0;
	OuterTupleArray = constructTupleArray();
	InnerTupleArray = constructTupleArray();
	
	if ((Sock = connectSock(Address, CJPort)) < 0) {
		// error
		fprintf(stderr, "error connectSock\n");
		exit(1);
	}
	puts("connected to external process.");
	
	
	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));
	
	NL1_printf("ExecInitNestLoop: %s\n",
		   "initializing node");
	
	/*
	 * create state structure
	 */
	nlstate = makeNode(NestLoopState);
	nlstate->js.ps.plan = (Plan *) node;
	nlstate->js.ps.state = estate;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &nlstate->js.ps);
	
	/*
	 * initialize child expressions
	 */
	nlstate->js.ps.targetlist = (List *)
		ExecInitExpr((Expr *) node->join.plan.targetlist,
					 (PlanState *) nlstate);
	nlstate->js.ps.qual = (List *)
		ExecInitExpr((Expr *) node->join.plan.qual,
					 (PlanState *) nlstate);
	nlstate->js.jointype = node->join.jointype;
	nlstate->js.joinqual = (List *)
		ExecInitExpr((Expr *) node->join.joinqual,
					 (PlanState *) nlstate);
	
	/*
	 * initialize child nodes
	 *
	 * If we have no parameters to pass into the inner rel from the outer,
	 * tell the inner child that cheap rescans would be good.  If we do have
	 * such parameters, then there is no point in REWIND support at all in the
	 * inner child, because it will always be rescanned with fresh parameter
	 * values.
	 */
	outerPlanState(nlstate) = ExecInitNode(outerPlan(node), estate, eflags);
	if (node->nestParams == NIL)
		eflags |= EXEC_FLAG_REWIND;
	else
		eflags &= ~EXEC_FLAG_REWIND;
	innerPlanState(nlstate) = ExecInitNode(innerPlan(node), estate, eflags);
	
	/*
	 * tuple table initialization
	 */
	ExecInitResultTupleSlot(estate, &nlstate->js.ps);

	switch (node->join.jointype)
	{
		case JOIN_INNER:
		case JOIN_SEMI:
			break;
		case JOIN_LEFT:
		case JOIN_ANTI:
			nlstate->nl_NullInnerTupleSlot =
				ExecInitNullTupleSlot(estate,
						      ExecGetResultType(innerPlanState(nlstate)));
			break;
		default:
			elog(ERROR, "unrecognized join type: %d",
				 (int) node->join.jointype);
	}
	
	/*
	 * initialize tuple type and projection info
	 */
	ExecAssignResultTypeFromTL(&nlstate->js.ps);
	ExecAssignProjectionInfo(&nlstate->js.ps, NULL);
	
	/*
	 * finally, wipe the current outer tuple clean.
	 */
	nlstate->js.ps.ps_TupFromTlist = false;
	nlstate->nl_NeedNewOuter = true;
	nlstate->nl_MatchedOuter = false;

	NL1_printf("ExecInitNestLoop: %s\n",
		   "node initialized");
	
	return nlstate;
}

/* ----------------------------------------------------------------
 *		ExecEndNestLoop
 *
 *		closes down scans and frees allocated storage
 * ----------------------------------------------------------------
 */
void
ExecEndNestLoop(NestLoopState *node)
{
	int j;
	
	/*
	pfree(OpInfo);
	pfree(SortedOpInfo);
	
	for (j = 0; j < VarNum; j++) {
		pfree(ResultOpInfo[j]);
		pfree(SortedResultOpInfo[j]);
	}
	pfree(ResultOpNum);
	pfree(ResultOpInfo);
	pfree(SortedResultOpInfo);
	*/	

	destructTupleArray(OuterTupleArray);
	destructTupleArray(InnerTupleArray);
	
	closeSock(Sock);
	puts("close connection to external process.");
	
	
	NL1_printf("ExecEndNestLoop: %s\n",
		   "ending node processing");
	
	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->js.ps);
	
	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->js.ps.ps_ResultTupleSlot);
	
	/*
	 * close down subplans
	 */
	ExecEndNode(outerPlanState(node));
	ExecEndNode(innerPlanState(node));

	NL1_printf("ExecEndNestLoop: %s\n",
			   "node processing ended");
}

/* ----------------------------------------------------------------
 *		ExecReScanNestLoop
 * ----------------------------------------------------------------
 */
void
ExecReScanNestLoop(NestLoopState *node)
{
	PlanState  *outerPlan = outerPlanState(node);
	
	/*
	 * If outerPlan->chgParam is not null then plan will be automatically
	 * re-scanned by first ExecProcNode.
	 */
	if (outerPlan->chgParam == NULL)
		ExecReScan(outerPlan);
	
	/*
	 * innerPlan is re-scanned for each new outer tuple and MUST NOT be
	 * re-scanned from here or you'll get troubles from inner index scans when
	 * outer Vars are used as run-time keys...
	 */
	
	node->js.ps.ps_TupFromTlist = false;
	node->nl_NeedNewOuter = true;
	node->nl_MatchedOuter = false;
}
