#!/usr/bin/env python

import itertools as it
import platform
import math
import subprocess
import sys
import time
import multiprocessing as mp
import os

BUILDDIR='../out-perf.ectrs'
if __name__ == '__main__':
  (_, out) = sys.argv

  args = [
      os.path.join(BUILDDIR, 'benchmarks/dbtest'),
      '--bench-opts', '--workload-mix 0,0,100,0',
      '--stats-server-sockfile' , '/tmp/silo.sock',
      '--num-threads', '28',
      '--numa-memory', '96G',
      '--scale-factor', '160000',
      '--parallel-loading',
      '--runtime', '30',
  ]
  env = dict(os.environ)
  env['DISABLE_MADV_WILLNEED'] = '1'
  p0 = subprocess.Popen(args, stdin=open('/dev/null', 'r'), stdout=open('/dev/null', 'w'), env=env)
  time.sleep(1.0) # XXX: hacky
  args = [os.path.join(BUILDDIR, 'stats_client'), '/tmp/silo.sock', 'dbtuple_bytes_allocated:dbtuple_bytes_freed']
  with open(out, 'w') as fp:
    p1 = subprocess.Popen(args, stdin=open('/dev/null', 'r'), stdout=fp)
    p0.wait()
    p1.wait()
