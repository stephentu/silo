Silo
=====

This project contains the prototype of the database system described in 

    Speedy Transactions in Multicore In-Memory Databases 
    Stephen Tu, Wenting Zheng, Eddie Kohler, Barbara Liskov, Samuel Madden 
    SOSP 2013. 
    http://people.csail.mit.edu/stephentu/papers/silo.pdf

This code is an ongoing work in progress.

Build
-----

There are several options to build. `MODE` is an important variable
governing the type of build. The default is `MODE=perf`, see the
Makefile for more options. `DEBUG=1` triggers a debug build (off by
default). `CHECK_INVARIANTS=1` enables invariant checking. There are
two targets: the default target which builds the test suite, and
`dbtest` which builds the benchmark suite. Examples:

    MODE=perf DEBUG=1 CHECK_INVARIANTS=1 make -j
    MODE=perf make -j dbtest

Each different combination of `MODE`, `DEBUG`, and `CHECK_INVARIANTS` triggers
a unique output directory; for example, the first command above builds to
`out-perf.debug.check.masstree`.

Silo now uses [Masstree](https://github.com/kohler/masstree-beta) by default as
the default index tree. To use the old tree, set `MASSTREE=0`.

Running
-------

To run the tests, simply invoke `<outdir>/test` with no arguments. To run the
benchmark suite, invoke `<outdir>/benchmarks/dbtest`. For now, look in
`benchmarks/dbtest.cc` for documentation on the command line arguments. An
example invocation for TPC-C is:

    <outdir>/benchmarks/dbtest \
        --verbose \
        --bench tpcc \
        --num-threads 28 \
        --scale-factor 28 \
        --runtime 30 \
        --numa-memory 112G 

Benchmarks
----------

To reproduce the graphs from the paper:

    $ cd benchmarks
    $ python runner.py /unused-dir <results-file-prefix>

If you set `DRYRUN=True` in `runner.py`, then you get to see all the
commands that would be issued by the benchmark script.
