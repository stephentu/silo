#!/usr/bin/env python

try:
  import matplotlib
  matplotlib.use('Agg')
  import pylab as plt
  do_plot = True
except ImportError:
  do_plot = False

import platform
import subprocess
import sys

DBS = ('mysql', 'bdb', 'ndb-proto1', 'ndb-proto2')
THREADS = (1, 2, 4, 8, 16, 24, 32, 40, 48)

def run_configuration(basedir, dbtype, nthreads):
  args = ['./ycsb', '--basedir', basedir, '--db-type', dbtype, '--num-threads', str(nthreads), '--num-keys', '1000000']
  p = subprocess.Popen(args, stdin=open('/dev/null', 'r'), stdout=subprocess.PIPE)
  r = p.stdout.read()
  p.wait()
  return float(r.strip())

if __name__ == '__main__':
  (_, basedir, outfile) = sys.argv
  if do_plot:
    print "matplotlib found, will draw plots"
  else:
    print "matplotlib not found"
  all_values = []
  for db in DBS:
    values = [run_configuration(basedir, db, n) for n in THREADS]
    all_values.append(values)
    print db, values
    if do_plot:
      plt.plot(THREADS, values)
  if do_plot:
    plt.xlabel('num threads')
    plt.ylabel('ops/sec')
    plt.title(platform.node())
    plt.legend(DBS)
    plt.savefig(outfile + '.eps')
  with open(outfile + '.py', 'w') as fp:
    print >>fp, 'DBS = %s' % (repr(DBS))
    print >>fp, 'THREADS = %s' % (repr(THREADS))
    print >>fp, 'RESULTS = %s' % (repr(all_values))
