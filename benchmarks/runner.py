#!/usr/bin/env python

import itertools as it
import platform
import math
import subprocess
import sys
import multiprocessing as mp
import os
import re

DRYRUN = True
USE_MASSTREE = True

NTRIALS = 1 if DRYRUN else 3

PERSIST_REAL='persist-real'
PERSIST_TEMP='persist-temp'
PERSIST_NONE='persist-none'

MACHINE_CONFIG = {
  'modis2' : {
      'logfiles' : (
          ('data.log', 1.),
          ('/data/scidb/001/2/stephentu/data.log', 1.),
          ('/data/scidb/001/3/stephentu/data.log', 1.),
      ),
      'tempprefix' : '/tmp',
      'disable_madv_willneed' : False,
  },
  'istc3' : {
      'logfiles' : (
          ('data.log', 3./24.),
          ('/f0/stephentu/data.log', 7./24.),
          ('/f1/stephentu/data.log', 7./24.),
          ('/f2/stephentu/data.log', 7./24.),
      ),
      'tempprefix' : '/run/shm',
      'disable_madv_willneed' : True,
  },
  'istc4' : {
      'logfiles' : (
          ('data.log', 1.),
      ),
      'tempprefix' : '/run/shm',
      'disable_madv_willneed' : False,
  },
}

NCPUS = mp.cpu_count()

TPCC_STANDARD_MIX='45,43,4,4,4'
TPCC_REALISTIC_MIX='39,37,4,10,10'

KNOB_ENABLE_YCSB_SCALE=True
KNOB_ENABLE_TPCC_SCALE=True
KNOB_ENABLE_TPCC_MULTIPART=True
KNOB_ENABLE_TPCC_MULTIPART_SKEW=True
KNOB_ENABLE_TPCC_FACTOR_ANALYSIS=True
KNOB_ENABLE_TPCC_PERSIST_FACTOR_ANALYSIS=True
KNOB_ENABLE_TPCC_RO_SNAPSHOTS=True

## debugging runs
KNOB_ENABLE_TPCC_SCALE_ALLPERSIST=False
KNOB_ENABLE_TPCC_SCALE_ALLPERSIST_COMPRESS=False
KNOB_ENABLE_TPCC_SCALE_ALLPERSIST_NOFSYNC=False
KNOB_ENABLE_TPCC_SCALE_FAKEWRITES=False
KNOB_ENABLE_TPCC_SCALE_GC=False
KNOB_ENABLE_TPCC_FACTOR_ANALYSIS_1=False

def binary_path(tpe):
  prog_suffix= '.masstree' if USE_MASSTREE else '.silotree'
  return '../%s%s/benchmarks/dbtest' % (tpe, prog_suffix)

grids = []

def get_scale_threads(stride):
  thds = range(0, NCPUS + 1, stride)
  thds[0] = 1
  return thds

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

if KNOB_ENABLE_YCSB_SCALE:
  def mk_ycsb_entries(nthds):
    return [
      {
        'name' : 'scale_rmw',
        'dbs' : ['kvdb', 'ndb-proto1', 'ndb-proto2'],
        'threads' : [nthds],
        'scale_factors' : [160000],
        'benchmarks' : ['ycsb'],
        'bench_opts' : ['--workload-mix 80,0,20,0'],
        'par_load' : [True],
        'retry' : [False],
        'persist' : [PERSIST_NONE],
        'numa_memory' : ['%dG' % (40 + 2 * nthds)],
      },
    ]
  THREADS = get_scale_threads(4)
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
      'par_load' : [False],
      'retry' : [False],
      'persist' : [PERSIST_REAL, PERSIST_TEMP, PERSIST_NONE],
      'numa_memory' : ['%dG' % (4 * nthds)],
    }
  THREADS = get_scale_threads(4)
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
      'bench_opts' :
          ['--workload-mix 100,0,0,0,0 --new-order-remote-item-pct %d' % d for d in D_RANGE],
      'par_load' : [False],
      'retry' : [False],
      'persist' : [PERSIST_NONE],
      'numa_memory' : ['%dG' % (4 * 28)],
    },
    {
      'binary' : [binary_path('out-factor-gc')],
      'name' : 'multipart:pct',
      'dbs' : ['ndb-proto2'],
      'threads' : [28],
      'scale_factors': [28],
      'benchmarks' : ['tpcc'],
      'bench_opts' :
          ['--workload-mix 100,0,0,0,0 --new-order-remote-item-pct %d' % d for d in D_RANGE],
      'par_load' : [False],
      'retry' : [False],
      'persist' : [PERSIST_NONE],
      'disable_snapshots' : [True],
      'numa_memory' : ['%dG' % (4 * 28)],
    },
    {
      'binary' : [binary_path('out-factor-gc')],
      'name' : 'multipart:pct',
      'dbs' : ['ndb-proto2'],
      'threads' : [28],
      'scale_factors': [28],
      'benchmarks' : ['tpcc'],
      'bench_opts' :
          ['--enable-separate-tree-per-partition --workload-mix 100,0,0,0,0 --new-order-remote-item-pct %d' % d for d in D_RANGE],
      'par_load' : [False],
      'retry' : [False],
      'persist' : [PERSIST_NONE],
      'disable_snapshots' : [True],
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
      'par_load' : [False],
      'retry' : [False],
      'persist' : [PERSIST_NONE],
      'numa_memory' : ['%dG' % (4 * 28)],
    },
  ]

if KNOB_ENABLE_TPCC_MULTIPART_SKEW:
  def mk_grids(nthds):
    return [
      {
        'name' : 'multipart:skew',
        'dbs' : ['ndb-proto2'],
        'threads' : [nthds],
        'scale_factors': [4],
        'benchmarks' : ['tpcc'],
        'bench_opts' : [
          '--workload-mix 100,0,0,0,0',
        ],
        'par_load' : [False],
        'retry' : [True],
        'backoff' : [True],
        'persist' : [PERSIST_NONE],
        'numa_memory' : ['%dG' % (4 * nthds)],
      },
      {
        'name' : 'multipart:skew',
        'dbs' : ['ndb-proto2'],
        'threads' : [nthds],
        'scale_factors': [4],
        'benchmarks' : ['tpcc'],
        'bench_opts' : [
          '--workload-mix 100,0,0,0,0 --new-order-fast-id-gen'
        ],
        'par_load' : [False],
        'retry' : [True],
        'persist' : [PERSIST_NONE],
        'numa_memory' : ['%dG' % (4 * nthds)],
      },
    ]
  grids += [
    {
      'name' : 'multipart:skew',
      'dbs' : ['kvdb-st'],
      'threads' : [1],
      'scale_factors': [4],
      'benchmarks' : ['tpcc'],
      'bench_opts' :
        ['--workload-mix 100,0,0,0,0 --enable-separate-tree-per-partition --enable-partition-locks'],
      'par_load' : [False],
      'retry' : [False],
      'persist' : [PERSIST_NONE],
      'numa_memory' : ['%dG' % (4 * 4)],
    },
  ]
  thds = [1,2,4,6,8,10,12,16,20,24,28,32]
  grids += list(it.chain.from_iterable([mk_grids(t) for t in thds]))

if KNOB_ENABLE_TPCC_FACTOR_ANALYSIS:
  # order is:
  # baseline (jemalloc, no-overwrites, gc, snapshots)
  # +allocator
  # +insert
  # -snapshots
  # -gc
  grids += [
    {
      'binary' : [binary_path('out-factor-gc-nowriteinplace')],
      'name' : 'factoranalysis',
      'dbs' : ['ndb-proto2'],
      'threads' : [28],
      'scale_factors': [28],
      'benchmarks' : ['tpcc'],
      'par_load' : [False],
      'retry' : [False],
      'persist' : [PERSIST_NONE],
      'numa_memory' : [None, '%dG' % (4 * 28)],
    },
    {
      'binary' : [binary_path('out-factor-gc')],
      'name' : 'factoranalysis',
      'dbs' : ['ndb-proto2'],
      'threads' : [28],
      'scale_factors': [28],
      'benchmarks' : ['tpcc'],
      'par_load' : [False],
      'retry' : [False],
      'persist' : [PERSIST_NONE],
      'numa_memory' : ['%dG' % (4 * 28)],
      'disable_snapshots' : [False],
    },
    {
      'binary' : [binary_path('out-factor-gc')],
      'name' : 'factoranalysis',
      'dbs' : ['ndb-proto2'],
      'threads' : [28],
      'scale_factors': [28],
      'benchmarks' : ['tpcc'],
      'bench_opts' : ['--disable-read-only-snapshots'],
      'par_load' : [False],
      'retry' : [False],
      'persist' : [PERSIST_NONE],
      'numa_memory' : ['%dG' % (4 * 28)],
      'disable_snapshots' : [True],
    },
    {
      'binary' : [binary_path('out-factor-gc')],
      'name' : 'factoranalysis',
      'dbs' : ['ndb-proto2'],
      'threads' : [28],
      'scale_factors': [28],
      'benchmarks' : ['tpcc'],
      'bench_opts' : ['--disable-read-only-snapshots'],
      'par_load' : [False],
      'retry' : [False],
      'persist' : [PERSIST_NONE],
      'numa_memory' : ['%dG' % (4 * 28)],
      'disable_snapshots' : [True],
      'disable_gc' : [True],
    },
  ]

if KNOB_ENABLE_TPCC_FACTOR_ANALYSIS_1:
  # order is:
  # baseline (jemalloc, no-overwrites, gc, no-snapshots)
  # +allocator
  # +insert
  # +snapshots
  # -gc
  grids += [
    {
      'binary' : [binary_path('out-factor-gc-nowriteinplace')],
      'name' : 'factoranalysis',
      'dbs' : ['ndb-proto2'],
      'threads' : [28],
      'scale_factors': [28],
      'benchmarks' : ['tpcc'],
      'bench_opts' : ['--workload-mix %s --disable-read-only-snapshots' % TPCC_REALISTIC_MIX],
      'par_load' : [False],
      'retry' : [False],
      'persist' : [PERSIST_NONE],
      'numa_memory' : [None, '%dG' % (4 * 28)],
      'disable_snapshots': [True],
    },
    {
      'binary' : [binary_path('out-factor-gc')],
      'name' : 'factoranalysis',
      'dbs' : ['ndb-proto2'],
      'threads' : [28],
      'scale_factors': [28],
      'benchmarks' : ['tpcc'],
      'bench_opts' : ['--workload-mix %s --disable-read-only-snapshots' % TPCC_REALISTIC_MIX],
      'par_load' : [False],
      'retry' : [False],
      'persist' : [PERSIST_NONE],
      'numa_memory' : ['%dG' % (4 * 28)],
      'disable_snapshots' : [True],
    },
    {
      'binary' : [binary_path('out-factor-gc')],
      'name' : 'factoranalysis',
      'dbs' : ['ndb-proto2'],
      'threads' : [28],
      'scale_factors': [28],
      'benchmarks' : ['tpcc'],
      'bench_opts' : ['--workload-mix %s' % TPCC_REALISTIC_MIX],
      'par_load' : [False],
      'retry' : [False],
      'persist' : [PERSIST_NONE],
      'numa_memory' : ['%dG' % (4 * 28)],
      'disable_gc' : [False, True],
    },
  ]

if KNOB_ENABLE_TPCC_PERSIST_FACTOR_ANALYSIS:
  # write zero length log records (perfect/fake compression)
  # lz4-compress buffers
  grids += [
    {
      'binary' : [binary_path('out-factor-fake-compression')],
      'name' : 'persistfactoranalysis',
      'dbs' : ['ndb-proto2'],
      'threads' : [28],
      'scale_factors': [28],
      'benchmarks' : ['tpcc'],
      'par_load' : [False],
      'retry' : [False],
      'persist' : [PERSIST_REAL],
      'numa_memory' : ['%dG' % (4 * 28)],
    },
    {
      'binary' : [binary_path('out-perf')],
      'name' : 'persistfactoranalysis',
      'dbs' : ['ndb-proto2'],
      'threads' : [28],
      'scale_factors': [28],
      'benchmarks' : ['tpcc'],
      'par_load' : [False],
      'retry' : [False],
      'persist' : [PERSIST_REAL],
      'numa_memory' : ['%dG' % (4 * 28)],
      'log_compress' : [True],
    },
  ]

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
      'persist' : [PERSIST_NONE],
      'numa_memory' : ['%dG' % (4 * 16)],
      'disable_snapshots' : [False],
    },
    {
      'name' : 'readonly',
      'binary' : [binary_path('out-factor-gc')],
      'dbs' : ['ndb-proto2'],
      'threads' : [16],
      'scale_factors': [8],
      'benchmarks' : ['tpcc'],
      'bench_opts' : ['--disable-read-only-snapshots --workload-mix 50,0,0,0,50 --new-order-remote-item-pct %d' % d for d in RO_DRANGE],
      'par_load' : [False],
      'retry' : [True],
      'persist' : [PERSIST_NONE],
      'numa_memory' : ['%dG' % (4 * 16)],
      'disable_snapshots' : [True],
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
      'persist' : [PERSIST_REAL],
      'numa_memory' : ['%dG' % (4 * nthds)],
    }
  THREADS = get_scale_threads(4)
  grids += [mk_grid('scale_tpcc', 'tpcc', t) for t in THREADS]

if KNOB_ENABLE_TPCC_SCALE_ALLPERSIST_COMPRESS:
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
      'persist' : [PERSIST_REAL],
      'numa_memory' : ['%dG' % (4 * nthds)],
      'log_compress' : [True],
    }
  THREADS = get_scale_threads(4)
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
      'persist' : [PERSIST_REAL],
      'numa_memory' : ['%dG' % (4 * nthds)],
      'log_nofsync' : [True],
    }
  THREADS = get_scale_threads(4)
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
      'persist' : [PERSIST_REAL],
      'numa_memory' : ['%dG' % (4 * nthds)],
      'log_fake_writes' : [True],
    }
  THREADS = get_scale_threads(4)
  grids += [mk_grid('scale_tpcc', 'tpcc', t) for t in THREADS]

if KNOB_ENABLE_TPCC_SCALE_GC:
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
      'persist' : [PERSIST_NONE],
      'numa_memory' : ['%dG' % (4 * nthds)],
      'disable_gc' : [False, True],
    }
  THREADS = get_scale_threads(4)
  grids += [mk_grid('scale_tpcc', 'tpcc', t) for t in THREADS]

def check_binary_executable(binary):
  return os.path.isfile(binary) and os.access(binary, os.X_OK)

def run_configuration(
    binary, disable_madv_willneed,
    basedir, dbtype, bench, scale_factor, nthreads, bench_opts,
    par_load, retry_aborted_txn, backoff_aborted_txn, numa_memory, logfiles,
    assignments, log_fake_writes, log_nofsync, log_compress,
    disable_gc, disable_snapshots, ntries=5):
  # Note: assignments is a list of list of ints
  assert len(logfiles) == len(assignments)
  assert not log_fake_writes or len(logfiles)
  assert not log_nofsync or len(logfiles)
  assert not log_compress or len(logfiles)
  args = [
      binary,
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
    + ([] if not backoff_aborted_txn else ['--backoff-aborted-transactions']) \
    + ([] if not numa_memory else ['--numa-memory', numa_memory]) \
    + ([] if not logfiles else list(it.chain.from_iterable([['--logfile', f] for f in logfiles]))) \
    + ([] if not assignments else list(it.chain.from_iterable([['--assignment', ','.join(map(str, x))] for x in assignments]))) \
    + ([] if not log_fake_writes else ['--log-fake-writes']) \
    + ([] if not log_nofsync else ['--log-nofsync']) \
    + ([] if not log_compress else ['--log-compress']) \
    + ([] if not disable_gc else ['--disable-gc']) \
    + ([] if not disable_snapshots else ['--disable-snapshots'])
  print >>sys.stderr, '[INFO] running command:'
  print >>sys.stderr, ('DISABLE_MADV_WILLNEED=1' if disable_madv_willneed else ''), ' '.join([x.replace(' ', r'\ ') for x in args])
  if not DRYRUN:
    with open('stderr.log', 'w') as err:
      env = dict(os.environ)
      if disable_madv_willneed:
        env['DISABLE_MADV_WILLNEED'] = '1'
      p = subprocess.Popen(args, stdin=open('/dev/null', 'r'), stdout=subprocess.PIPE, stderr=err, env=env)
      print >>sys.stderr, 'pid=', p.pid
      r = p.stdout.read()
      retcode = p.wait()
      toks = r.strip().split(' ')
  else:
    assert check_binary_executable(binary)
    toks = [0,0,0,0,0]
  if len(toks) != 5:
    print 'Failure: retcode=', retcode, ', stdout=', r
    import shutil
    shutil.copyfile('stderr.log', 'stderr.%d.log' % p.pid)
    if ntries:
      return run_configuration(
          binary, disable_madv_willneed,
          basedir, dbtype, bench, scale_factor, nthreads, bench_opts,
          par_load, retry_aborted_txn, backoff_aborted_txn, numa_memory, logfiles,
          assignments, log_fake_writes, log_nofsync, log_compress,
          disable_gc, disable_snapshots, ntries - 1)
    else:
      print "Out of tries!"
      assert False
  return tuple(map(float, toks))

if __name__ == '__main__':
  (_, basedir, outfile) = sys.argv

  DEFAULT_BINARY=binary_path('out-perf')
  # list all the binaries needed
  binaries = set(it.chain.from_iterable([grid.get('binary', [DEFAULT_BINARY]) for grid in grids]))
  failed = []
  for binary in binaries:
    if not check_binary_executable(binary):
      print >>sys.stderr, '[ERROR] cannot find binary %s' % binary
      failed.append(binary)
  if failed:
    r = re.compile(r'out-(.*)\.(masstree|silotree)')
    print >>sys.stderr, \
        '[INFO] Try running the following commands in the root source directory:'
    for binary in failed:
      folder = binary.split(os.sep)[1]
      m = r.match(folder)
      if not m:
        print >>sys.stderr, '[ERROR] bad binary name %s' % binary
      else:
        print >>sys.stderr, 'MASSTREE=%d MODE=%s make -j dbtest' % (1 if m.group(2) == 'masstree' else 0, m.group(1))
    sys.exit(1)

  # iterate over all configs
  results = []
  for grid in grids:
    for (binary, db, bench, scale_factor, threads, bench_opts,
         par_load, retry, backoff, numa_memory, persist,
         log_fake_writes, log_nofsync, log_compress,
         disable_gc, disable_snapshots) in it.product(
        grid.get('binary', [DEFAULT_BINARY]),
        grid['dbs'], grid['benchmarks'], grid['scale_factors'],
        grid['threads'], grid.get('bench_opts', ['']), grid['par_load'],
        grid['retry'], grid.get('backoff', [False]),
        grid['numa_memory'], grid['persist'],
        grid.get('log_fake_writes', [False]),
        grid.get('log_nofsync', [False]),
        grid.get('log_compress', [False]),
        grid.get('disable_gc', [False]),
        grid.get('disable_snapshots', [False])):
      node = platform.node()
      disable_madv_willneed = MACHINE_CONFIG[node]['disable_madv_willneed']
      config = {
        'binary'                : binary,
        'disable_madv_willneed' : disable_madv_willneed,
        'name'                  : grid['name'],
        'db'                    : db,
        'bench'                 : bench,
        'scale_factor'          : scale_factor,
        'threads'               : threads,
        'bench_opts'            : bench_opts,
        'par_load'              : par_load,
        'retry'                 : retry,
        'backoff'               : backoff,
        'persist'               : persist,
        'numa_memory'           : numa_memory,
        'log_fake_writes'       : log_fake_writes,
        'log_nofsync'           : log_nofsync,
        'log_compress'          : log_compress,
        'disable_gc'            : disable_gc,
        'disable_snapshots'     : disable_snapshots,
      }
      print >>sys.stderr, '[INFO] running config %s' % (str(config))
      if persist != PERSIST_NONE:
        info = MACHINE_CONFIG[node]['logfiles']
        tempprefix = MACHINE_CONFIG[node]['tempprefix']
        logfiles = \
            [x[0] for x in info] if persist == PERSIST_REAL \
              else [os.path.join(tempprefix, 'data%d.log' % (idx)) for idx in xrange(len(info))]
        weights = \
          normalize([x[1] for x in info]) if persist == PERSIST_REAL else \
          normalize([1.0 for _ in info])
        assignments = allocate(threads, weights)
      else:
        logfiles, assignments = [], []
      values = []
      for _ in range(NTRIALS):
        value = run_configuration(
            binary, disable_madv_willneed,
            basedir, db, bench, scale_factor, threads,
            bench_opts, par_load, retry, backoff, numa_memory,
            logfiles, assignments, log_fake_writes,
            log_nofsync, log_compress, disable_gc,
            disable_snapshots)
        values.append(value)
      results.append((config, values))

    # write intermediate results
    with open(outfile + '.py', 'w') as fp:
      print >>fp, 'RESULTS = %s' % (repr(results))

  # write results
  with open(outfile + '.py', 'w') as fp:
    print >>fp, 'RESULTS = %s' % (repr(results))
