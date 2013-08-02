#!/usr/bin/env python

import itertools as it
import platform
import math
import subprocess
import sys

#DBS = ('mysql', 'bdb', 'ndb-proto1', 'ndb-proto2')
#DBS = ('ndb-proto1', 'ndb-proto2')
#DBS = ('ndb-proto2', 'kvdb')
#DBS = ('kvdb', 'ndb-proto2')
DBS = ('ndb-proto1',)

# config for tom
#THREADS = (1, 2, 4, 8, 12, 18, 24, 30, 36, 42, 48)
#THREADS = (1,)

# config for ben
#THREADS = (1, 2, 4, 8, 16, 24, 32, 40, 48, 56, 64, 72, 80)

# config for istc*
THREADS = (1, 4, 8, 12, 16, 20, 24, 28, 32)
#THREADS = (28, 32)

#TXN_FLAGS = (0x0, 0x1)
#TXN_FLAGS = (0x1,)

#SCALE_FACTORS = (10,)

# tuples of (benchname, amplification-factor)
#BENCHMARKS = ( ('ycsb', 1000), ('tpcc', 1), )

DRYRUN = False

NTRIALS = 1 if DRYRUN else 3

### NOTE: for TPC-C, in general, allocate 4GB of memory per thread for the experiments.
### this is over-conservative

KNOB_ENABLE_YCSB_SCALE=False
KNOB_ENABLE_TPCC_SCALE=False
KNOB_ENABLE_TPCC_MULTIPART=False
KNOB_ENABLE_TPCC_RO_SNAPSHOTS=False

## debugging runs
KNOB_ENABLE_TPCC_SCALE_ALLPERSIST=False
KNOB_ENABLE_TPCC_SCALE_ALLPERSIST_NOFSYNC=True
KNOB_ENABLE_TPCC_SCALE_FAKEWRITES=False

grids = []

# exp 1:
#   scale graph: kvdb VS ndb on ycsb 80/20 w/ fixed scale factor 320000

#grids += [
#  {
#    'name' : 'scale',
#    'dbs' : DBS,
#    'threads' : THREADS,
#    'scale_factors' : [320000],
#    'benchmarks' : ['ycsb'],
#    'bench_opts' : ['--workload-mix 80,20,0,0'],
#    'par_load' : [True],
#    'retry' : [False],
#  },
#  {
#    'name' : 'scale_rmw',
#    'dbs' : DBS,
#    'threads' : THREADS,
#    'scale_factors' : [320000],
#    'benchmarks' : ['ycsb'],
#    'bench_opts' : ['--workload-mix 80,0,20,0'],
#    'par_load' : [True],
#    'retry' : [False],
#  },
#]

MACHINE_LOG_CONFIG = {
  'modis2' : (
        ('data.log', 1.),
        ('/data/scidb/001/2/stephentu/data.log', 1.),
        ('/data/scidb/001/3/stephentu/data.log', 1.),
      ),
  'istc3' : (
        ('data.log', 3./24.),
        ('/f0/stephentu/data.log', 7./24.),
        ('/f1/stephentu/data.log', 7./24.),
        ('/f2/stephentu/data.log', 7./24.),
      ),
}

### helpers for log allocation
def normalize(x):
  denom = math.fsum(x)
  return [e / denom for e in x]

def scale(x, a):
  return [e * a for e in x]

# a - b
def sub(a, b):
  assert len(a) == len(b)
  return [x - y for x, y in zip(a, b)]

def twonorm(x):
  return math.sqrt(math.fsum([e * e for e in x]))

def onenorm(x):
  return math.fsum([abs(e) for e in x])

def argcmp(x, comp, predicate):
  idx = None
  val = None
  for i in xrange(len(x)):
    if not predicate(x[i]):
      continue
    if idx is None or comp(x[i], val):
      idx = i
      val = x[i]
  if idx is None:
    # couldn't find it
    raise Exception("no argmin satisfiying predicate")
  return idx

def argmin(x, predicate=lambda x: True):
  return argcmp(x, lambda a, b: a < b, predicate)

def argmax(x, predicate=lambda x: True):
  return argcmp(x, lambda a, b: a > b, predicate)

def allocate(nworkers, weights):
  def score(allocation):
    #print "score(): allocation=", allocation, "weighted=", normalize(allocation), \
    #    "score=",onenorm(sub(normalize(allocation), weights))
    return onenorm(sub(normalize(allocation), weights))

  # assumes weights are normalized
  approx = map(int, map(math.ceil, scale(weights, nworkers)))
  diff = sum(approx) - nworkers
  if diff > 0:
    #print "OVER"
    #print approx
    #print normalize(approx)
    while diff > 0:
      best, bestValue = None, None
      for idx in xrange(len(approx)):
        if not approx[idx]:
          continue
        cpy = approx[:]
        cpy[idx] -= 1
        s = score(cpy)
        if bestValue is None or s < bestValue:
          best, bestValue = cpy, s
      assert best is not None
      approx = best
      diff -= 1

  elif diff < 0:
    #print "UNDER"
    #print approx
    #print normalize(approx)
    while diff < 0:
      best, bestValue = None, None
      for idx in xrange(len(approx)):
        cpy = approx[:]
        cpy[idx] += 1
        s = score(cpy)
        if bestValue is None or s < bestValue:
          best, bestValue = cpy, s
      assert best is not None
      approx = best
      diff += 1

  #print "choice      =", approx
  #print "weights     =", weights
  #print "allocweights=", normalize(approx)

  acc = 0
  ret = []
  for x in approx:
    ret.append(range(acc, acc + x))
    acc += x
  return ret

def mk_ycsb_entries(nthds):
  return [
    {
      'name' : 'scale',
      'dbs' : DBS,
      'threads' : [nthds],
      'scale_factors' : [320000],
      'benchmarks' : ['ycsb'],
      'bench_opts' : ['--workload-mix 80,20,0,0'],
      'par_load' : [True],
      'retry' : [False],
      'persist' : [False],
      'numa_memory' : ['%dG' % int(100 + 1.4*nthds)],
    },
    {
      'name' : 'scale_rmw',
      'dbs' : DBS,
      'threads' : [nthds],
      'scale_factors' : [320000],
      'benchmarks' : ['ycsb'],
      'bench_opts' : ['--workload-mix 80,0,20,0'],
      'par_load' : [True],
      'retry' : [False],
      'persist' : [False],
      'numa_memory' : ['%dG' % int(100 + 1.4*nthds)],
    },
  ]

if KNOB_ENABLE_YCSB_SCALE:
  for nthds in THREADS:
    grids += mk_ycsb_entries(nthds)

# exp 2:
if KNOB_ENABLE_TPCC_SCALE:
  def mk_grid(name, bench, nthds):
    return {
      'name' : name,
      'dbs' : ['ndb-proto2'],
      'threads' : [nthds],
      'scale_factors' : [nthds],
      'benchmarks' : [bench],
      'bench_opts' : [''],
      'par_load' : [False],
      'retry' : [False],
      'persist' : [True, False],
      'numa_memory' : ['%dG' % (4 * nthds)],
    }
  grids += [mk_grid('scale_tpcc', 'tpcc', t) for t in THREADS]

# exp 3:
#   x-axis varies the % multi-partition for new order. hold scale_factor constant @ 28,
#   nthreads also constant at 28
if KNOB_ENABLE_TPCC_MULTIPART:
  D_RANGE = range(0, 11)
  grids += [
    {
      'name' : 'multipart:pct',
      'dbs' : ['ndb-proto2'],
      'threads' : [28],
      'scale_factors': [28],
      'benchmarks' : ['tpcc'],
      'bench_opts' : ['--workload-mix 100,0,0,0,0 --new-order-remote-item-pct %d' % d for d in D_RANGE],
      'par_load' : [False],
      'retry' : [False],
      'persist' : [False],
      'numa_memory' : ['%dG' % (4 * 28)],
    },
    {
      'name' : 'multipart:pct',
      'dbs' : ['kvdb-st'],
      'threads' : [28],
      'scale_factors': [28],
      'benchmarks' : ['tpcc'],
      'bench_opts' :
        ['--workload-mix 100,0,0,0,0 --enable-separate-tree-per-partition --enable-partition-locks --new-order-remote-item-pct %d' % d for d in D_RANGE],
      'par_load' : [True],
      'retry' : [False],
      'persist' : [False],
      'numa_memory' : ['%dG' % (4 * 28)],
    },
  ]

# exp 4:
#  * standard workload mix
#  * fix the tpc-c scale factor at 8
#  * for volt, do one run @ 8-threads
#  * for ndb, vary threads [8, 12, 16, 20, 24, 28, 32]
#grids += [
#  {
#    'name' : 'multipart:cpu',
#    'dbs' : ['kvdb'],
#    'threads' : [8],
#    'scale_factors': [8],
#    'benchmarks' : ['tpcc'],
#    'bench_opts' : ['--enable-separate-tree-per-partition --enable-partition-locks'],
#    'par_load' : [True],
#    'retry' : [False],
#  },
#  {
#    'name' : 'multipart:cpu',
#    'dbs' : ['ndb-proto2'],
#    'threads' : [8, 12, 16, 20, 24, 28, 32],
#    'scale_factors': [8],
#    'benchmarks' : ['tpcc'],
#    'bench_opts' : [''],
#    'par_load' : [False],
#    'retry' : [False],
#  },
#]

# exp 5:
#  * 50% new order, 50% stock level
#  * scale factor 8, n-threads 16
#  * x-axis is --new-order-remote-item-pct from [0, 20, 40, 60, 80, 100]
if KNOB_ENABLE_TPCC_RO_SNAPSHOTS:
  RO_DRANGE = [0, 20, 40, 60, 80, 100]
  grids += [
    {
      'name' : 'readonly',
      'dbs' : ['ndb-proto2'],
      'threads' : [16],
      'scale_factors': [8],
      'benchmarks' : ['tpcc'],
      'bench_opts' : ['--workload-mix 50,0,0,0,50 --new-order-remote-item-pct %d' % d for d in RO_DRANGE],
      'par_load' : [False],
      'retry' : [True],
      'persist' : [False],
      'numa_memory' : ['%dG' % (4 * 16)],
    },
    {
      'name' : 'readonly',
      'dbs' : ['ndb-proto2'],
      'threads' : [16],
      'scale_factors': [8],
      'benchmarks' : ['tpcc'],
      'bench_opts' : ['--disable-read-only-snapshots --workload-mix 50,0,0,0,50 --new-order-remote-item-pct %d' % d for d in RO_DRANGE],
      'par_load' : [False],
      'retry' : [True],
      'persist' : [False],
      'numa_memory' : ['%dG' % (4 * 16)],
    },
  ]

if KNOB_ENABLE_TPCC_SCALE_ALLPERSIST:
  def mk_grid(name, bench, nthds):
    return {
      'name' : name,
      'dbs' : ['ndb-proto2'],
      'threads' : [nthds],
      'scale_factors' : [nthds],
      'benchmarks' : [bench],
      'bench_opts' : [''],
      'par_load' : [False],
      'retry' : [False],
      'persist' : [True],
      'numa_memory' : ['%dG' % (4 * nthds)],
    }
  grids += [mk_grid('scale_tpcc', 'tpcc', t) for t in THREADS]

if KNOB_ENABLE_TPCC_SCALE_ALLPERSIST_NOFSYNC:
  def mk_grid(name, bench, nthds):
    return {
      'name' : name,
      'dbs' : ['ndb-proto2'],
      'threads' : [nthds],
      'scale_factors' : [nthds],
      'benchmarks' : [bench],
      'bench_opts' : [''],
      'par_load' : [False],
      'retry' : [False],
      'persist' : [True],
      'numa_memory' : ['%dG' % (4 * nthds)],
      'log_nofsync' : [True],
    }
  grids += [mk_grid('scale_tpcc', 'tpcc', t) for t in THREADS]

if KNOB_ENABLE_TPCC_SCALE_FAKEWRITES:
  def mk_grid(name, bench, nthds):
    return {
      'name' : name,
      'dbs' : ['ndb-proto2'],
      'threads' : [nthds],
      'scale_factors' : [nthds],
      'benchmarks' : [bench],
      'bench_opts' : [''],
      'par_load' : [False],
      'retry' : [False],
      'persist' : [True],
      'numa_memory' : ['%dG' % (4 * nthds)],
      'log_fake_writes' : [True],
    }
  grids += [mk_grid('scale_tpcc', 'tpcc', t) for t in THREADS]

def run_configuration(
    basedir, dbtype, bench, scale_factor, nthreads, bench_opts,
    par_load, retry_aborted_txn, numa_memory, logfiles,
    assignments, log_fake_writes, log_nofsync):
  # Note: assignments is a list of list of ints
  assert len(logfiles) == len(assignments)
  assert not log_fake_writes or len(logfiles)
  assert not log_nofsync or len(logfiles)
  args = [
      './dbtest',
      '--bench', bench,
      '--basedir', basedir,
      '--db-type', dbtype,
      '--num-threads', str(nthreads),
      '--scale-factor', str(scale_factor),
      '--txn-flags', '1',
      '--runtime', '60',
  ] + ([] if not bench_opts else ['--bench-opts', bench_opts]) \
    + ([] if not par_load else ['--parallel-loading']) \
    + ([] if not retry_aborted_txn else ['--retry-aborted-transactions']) \
    + ([] if not numa_memory else ['--numa-memory', numa_memory]) \
    + ([] if not logfiles else list(it.chain.from_iterable([['--logfile', f] for f in logfiles]))) \
    + ([] if not assignments else list(it.chain.from_iterable([['--assignment', ','.join(map(str, x))] for x in assignments]))) \
    + ([] if not log_fake_writes else ['--log-fake-writes']) \
    + ([] if not log_nofsync else ['--log-nofsync'])
  print >>sys.stderr, '[INFO] running command:'
  print >>sys.stderr, ' '.join(args)
  if not DRYRUN:
    p = subprocess.Popen(args, stdin=open('/dev/null', 'r'), stdout=subprocess.PIPE)
    r = p.stdout.read()
    p.wait()
    toks = r.strip().split(' ')
  else:
    toks = [0,0,0,0,0]
  assert len(toks) == 5
  return tuple(map(float, toks))

if __name__ == '__main__':
  (_, basedir, outfile) = sys.argv

  # iterate over all configs
  results = []
  for grid in grids:
    for (db, bench, scale_factor, threads, bench_opts,
         par_load, retry, numa_memory, persist, log_fake_writes, log_nofsync) in it.product(
        grid['dbs'], grid['benchmarks'], grid['scale_factors'], \
        grid['threads'], grid['bench_opts'], grid['par_load'], grid['retry'],
        grid['numa_memory'], grid['persist'],
        grid.get('log_fake_writes', [False]),
        grid.get('log_nofsync', [False])):
      config = {
        'name'            : grid['name'],
        'db'              : db,
        'bench'           : bench,
        'scale_factor'    : scale_factor,
        'threads'         : threads,
        'bench_opts'      : bench_opts,
        'par_load'        : par_load,
        'retry'           : retry,
        'persist'         : persist,
        'numa_memory'     : numa_memory,
        'log_fake_writes' : log_fake_writes,
        'log_nofsync' : log_nofsync,
      }
      print >>sys.stderr, '[INFO] running config %s' % (str(config))
      if persist:
        node = platform.node()
        inf = MACHINE_LOG_CONFIG[node]
        logfiles = [x[0] for x in inf]
        weights = normalize([x[1] for x in inf])
        assignments = allocate(threads, weights)
      else:
        logfiles, assignments = [], []
      values = []
      for _ in range(NTRIALS):
        value = run_configuration(
            basedir, db, bench, scale_factor, threads,
            bench_opts, par_load, retry, numa_memory,
            logfiles, assignments, log_fake_writes,
            log_nofsync)
        values.append(value)
      results.append((config, values))

    # write intermediate results
    with open(outfile + '.py', 'w') as fp:
      print >>fp, 'RESULTS = %s' % (repr(results))

  # write results
  with open(outfile + '.py', 'w') as fp:
    print >>fp, 'RESULTS = %s' % (repr(results))
