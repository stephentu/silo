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
    for (db, res) in zip(DBS, RESULTS):
      #plt.plot(THREADS, np.log(np.array(res)))
      plt.plot(THREADS, res)
    plt.xlabel('num threads')
    plt.ylabel('ops/sec')
    plt.legend(DBS, loc='right')
    plt.title('get/put workload 95/4/1 read/rmw/write')
    plt.savefig('.'.join(os.path.basename(f).split('.')[:-1] + ['pdf']))
    plt.close()
