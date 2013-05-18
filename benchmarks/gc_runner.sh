#!/bin/bash

set -x

BENCH=./dbtest
NTHREADS=28
PREFIX=$1
YCSB_MEM=`python -c "print int(100+1.4*$NTHREADS)"`

$BENCH \
  --verbose \
  --bench ycsb \
  --db-type ndb-proto2 \
  --scale-factor 320000 \
  --num-threads $NTHREADS \
  --bench-opts '--workload-mix 80,0,20,0' \
  --numa-memory ${YCSB_MEM}G \
  --parallel-loading \
  --runtime 60 \
  --slow-exit 2>&1 | grep chain_ > results/$PREFIX-ycsb-chains.txt

$BENCH \
  --verbose \
  --bench tpcc \
  --db-type ndb-proto2 \
  --scale-factor $NTHREADS \
  --num-threads $NTHREADS \
  --numa-memory $((4 * $NTHREADS))G \
  --parallel-loading \
  --runtime 60 \
  --slow-exit 2>&1 | grep chain_ > results/$PREFIX-tpcc-chains.txt
