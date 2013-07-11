#!/bin/bash

for ncores in 1 2 4 8 16 24 32; do
  for wset in 4 8 16; do
    echo -n "$ncores $wset "
    ./persist_test --num-threads $ncores --strategy epoch --writeset $wset --valuesize 32
    sleep 1
  done
done
