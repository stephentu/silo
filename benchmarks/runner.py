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

DBS = ('bdb', 'ndb')
THREADS = (1, 2, 4, 8, 16, 24)

def run_configuration(dbtype, nthreads):
  args = ['./ycsb', '--db-type', dbtype, '--num-threads', str(nthreads)]
  p = subprocess.Popen(args, stdin=open('/dev/null', 'r'), stdout=subprocess.PIPE)
  r = p.stdout.read()
  p.wait()
  return float(r.strip())

if __name__ == '__main__':
  (_, outfile) = sys.argv
  if do_plot:
    print "matplotlib found, will draw plots"
  else:
    print "matplotlib not found"
  for db in DBS:
    values = [run_configuration(db, n) for n in THREADS]
    print db, values
    if do_plot:
      plt.plot(THREADS, values)
  if do_plot:
    plt.xlabel('num threads')
    plt.ylabel('ops/sec')
    plt.title(platform.node())
    plt.legend(DBS)
    plt.savefig(outfile)
