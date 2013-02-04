#!/usr/bin/env python

import matplotlib
import pylab as plt
import numpy as np

import os
import sys

if __name__ == '__main__':
  files = sys.argv[1:]
  for f in files:
    execfile(f)
    for (db, res) in zip(DBS[:-1], RESULTS):
      #plt.plot(THREADS, np.log(np.array(res)))
      #plt.plot(THREADS, res)
      #plt.plot(THREADS, np.array(res)/np.array(THREADS)) # per-core
      plt.plot(THREADS, np.log10(np.array(res)/np.array(THREADS))) # per-core
    plt.xlabel('num threads')
    #plt.ylabel('ops/sec')
    #plt.ylabel('ops/sec/core')
    plt.ylabel('$log_{10}$ ops/sec/thread')
    plt.legend(DBS[:-1], loc='right')
    plt.title('YCSB workload 95/4/1 read/rmw/write 10M keys')
    plt.savefig('.'.join(os.path.basename(f).split('.')[:-1] + ['pdf']))
    plt.close()
