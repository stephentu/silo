import matplotlib
matplotlib.use('Agg')
import pylab as plt

import sys

if __name__ == '__main__':
  (_, fname, oname) = sys.argv
  execfile(fname)
  for name, data in zip(DBS, RESULTS):
    plt.plot(THREADS, data)
  plt.xlabel('num threads')
  plt.ylabel('ops/sec')
  plt.title('')
  plt.legend(DBS, loc='lower right')
  plt.savefig(oname)
