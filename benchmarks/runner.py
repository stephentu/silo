#!/usr/bin/env python

import itertools
import platform
import subprocess
import sys

#DBS = ('mysql', 'bdb', 'ndb-proto1', 'ndb-proto2')
DBS = ('ndb-proto1', 'ndb-proto2')
#DBS = ('ndb-proto2',)

THREADS = (1, 2, 4, 8, 16, 24, 32, 40, 48)
#THREADS = (1,)

#TXN_FLAGS = (0x0, 0x1)
TXN_FLAGS = (0x1,)

SCALE_FACTORS = (10,)

#BENCHMARKS = ('ycsb', 'tpcc')
BENCHMARKS = ('tpcc',)

def run_configuration(basedir, dbtype, bench, scale_factor, txn_flags, nthreads):
  args = [
      './bench',
      '--bench', bench,
      '--basedir', basedir,
      '--db-type', dbtype,
      '--num-threads', str(nthreads),
      '--scale-factor', str(scale_factor),
      '--txn-flags', '%x' % (txn_flags),
  ]
  p = subprocess.Popen(args, stdin=open('/dev/null', 'r'), stdout=subprocess.PIPE)
  r = p.stdout.read()
  p.wait()
  return float(r.strip())

if __name__ == '__main__':
  (_, basedir, outfile) = sys.argv

  # iterate over all configs
  results = []
  for (db, bench, scale_factor, txn_flags, threads) in itertools.product(
      DBS, BENCHMARKS, SCALE_FACTORS, TXN_FLAGS, THREADS):
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
    print >>fp, 'DBS = %s' % (repr(DBS))
    print >>fp, 'THREADS = %s' % (repr(THREADS))
    print >>fp, 'TXN_FLAGS = %s' % (repr(TXN_FLAGS))
    print >>fp, 'SCALE_FACTORS = %s' % (repr(SCALE_FACTORS))
    print >>fp, 'BENCHMARKS = %s' % (repr(BENCHMARKS))
    print >>fp, 'RESULTS = %s' % (repr(results))
