PostgreSQL NestLoopJoinHookTest
====

## Description

When NestLoopJoin executor is called, this process will connect to port 59999 on localhost and send tuple data.
After sending tuples, receive result tuples from port 59999.
NestLoopJoin ends when the connection losts.

## Requirement

Database directory that was initialized by original postgresql-9.4.4 is needed.
ExecNestLoop is overwritten in this code, so cannot initialize database directory by initdb or something.
Tab completion is also unavailable because it uses NestLoopJoin.

## Install

If executor/nodeNestloop.o: undefined reference to symbol 'pthread_cancel@@GLIBC_2.2.5' error occurs, use
   LIBS=-pthread ./configure
