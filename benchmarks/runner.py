#!/usr/bin/env python

import itertools
import platform
import subprocess
import sys

#DBS = ('mysql', 'bdb', 'ndb-proto1', 'ndb-proto2')
#DBS = ('ndb-proto1', 'ndb-proto2')
#DBS = ('ndb-proto2', 'kvdb')
DBS = ('kvdb', 'ndb-proto2')

# config for tom
#THREADS = (1, 2, 4, 8, 12, 18, 24, 30, 36, 42, 48)
#THREADS = (1,)

# config for ben
#THREADS = (1, 2, 4, 8, 16, 24, 32, 40, 48, 56, 64, 72, 80)

# config for istc*
THREADS = (1, 4, 8, 12, 16, 20, 24, 28, 32)

#TXN_FLAGS = (0x0, 0x1)
TXN_FLAGS = (0x1,)

#SCALE_FACTORS = (10,)

# tuples of (benchname, amplification-factor)
BENCHMARKS = ( ('ycsb', 1000), ('tpcc', 1), )

def mk_grid(bench, nthds):
  # bench is tuple (name, factor)
  return {
      'dbs' : DBS,
      'threads': [nthds],
      'scale_factors' : [nthds * bench[1]],
      'benchmarks' : [bench[0]],
      'txn_flags' : TXN_FLAGS,
  }

grids = [mk_grid(b, n) for b in BENCHMARKS for n in THREADS]

def run_configuration(basedir, dbtype, bench, scale_factor, txn_flags, nthreads):
  args = [
      './dbtest',
      '--bench', bench,
      '--basedir', basedir,
      '--db-type', dbtype,
      '--num-threads', str(nthreads),
      '--scale-factor', str(scale_factor),
      '--txn-flags', '%d' % (txn_flags),
      '--runtime', '60',
  ]
  p = subprocess.Popen(args, stdin=open('/dev/null', 'r'), stdout=subprocess.PIPE)
  r = p.stdout.read()
  p.wait()
  toks = r.strip().split(' ')
  assert len(toks) == 2
  return float(toks[0]), float(toks[1])

if __name__ == '__main__':
  (_, basedir, outfile) = sys.argv

  # iterate over all configs
  results = []
  for grid in grids:
    for (db, bench, scale_factor, txn_flags, threads) in itertools.product(
        grid['dbs'], grid['benchmarks'], grid['scale_factors'], grid['txn_flags'], grid['threads']):
      config = {
        'db'           : db,
        'bench'        : bench,
        'scale_factor' : scale_factor,
        'txn_flags'    : txn_flags,
        'threads'      : threads,
      }
      print >>sys.stderr, '[INFO] running config %s' % (str(config))
      value = run_configuration(basedir, db, bench, scale_factor, txn_flags, threads)
      results.append((config, value))

  # write results
  with open(outfile + '.py', 'w') as fp:
    print >>fp, 'RESULTS = %s' % (repr(results))
