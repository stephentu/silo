#!/bin/bash

for ncores in 1 2 4 8 16 24 32; do
  #for wset in 4 8 16; do
  for wset in 18; do
    echo -n "$ncores $wset "
    ./persist_test \
      --logfile data.log \
      --logfile /data/scidb/001/2/stephentu/data.log \
      --logfile /data/scidb/001/3/stephentu/data.log \
      --num-threads $ncores --strategy epoch --writeset $wset --valuesize 32
    sleep 1
  done
done
