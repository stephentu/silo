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
    for db in DBS:
      values = []
      for t in THREADS:
        # find config in results...
        V = None
        for (r, v) in RESULTS:
          if r['db'] == db and r['threads'] == t:
            V = v
            break
        assert V
        values.append(V)
      plt.plot(THREADS, values)
    #for (db, res) in zip(DBS[:-1], RESULTS):
    #  #plt.plot(THREADS, np.log(np.array(res)))
    #  #plt.plot(THREADS, res)
    #  #plt.plot(THREADS, np.array(res)/np.array(THREADS)) # per-core
    #  plt.plot(THREADS, np.log10(np.array(res)/np.array(THREADS))) # per-core
    plt.xlabel('num threads')
    plt.ylabel('txns/sec')
    #plt.ylabel('ops/sec/core')
    #plt.ylabel('$log_{10}$ ops/sec/thread')
    plt.legend(DBS, loc='right')
    plt.title('TPCC workload scale factor 10')
    plt.savefig('.'.join(os.path.basename(f).split('.')[:-1] + ['pdf']))
    plt.close()
