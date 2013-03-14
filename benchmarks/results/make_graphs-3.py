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

    # configs
    descs = [
      {
        'x-axis' : 'threads',
        'y-axis' : lambda x: x[0],
        'lines' : ['db'], # each line holds this constant
        'x-label' : 'num threads',
        'y-label' : 'txns/sec',
        'title' : 'TPCC throughput graph',
        'name' : 'throughput',
      },
      {
        'x-axis' : 'threads',
        'y-axis' : lambda x: x[1],
        'lines' : ['db'], # each line holds this constant
        'x-label' : 'num threads',
        'y-label' : 'aborts/sec',
        'title' : 'TPCC abort graph',
        'name' : 'abort',
      }]

    for desc in descs:
      lines = {}
      for (config, result) in RESULTS:
        key = tuple(config[x] for x in desc['lines'])
        pts = lines.get(key, {})
        assert not config[desc['x-axis']] in pts
        pts[config[desc['x-axis']]] = desc['y-axis'](result)
        lines[key] = pts

      labels = []
      for (name, pts) in lines.iteritems():
        spts = sorted(pts.iteritems(), key=lambda x: x[0])
        plt.plot([x[0] for x in spts], [x[1] for x in spts])
        labels.append('-'.join(name))

      plt.xlabel(desc['x-label'])
      plt.ylabel(desc['y-label'])
      plt.title(desc['title'])
      plt.legend(labels, loc='upper left')
      bname = '.'.join(os.path.basename(f).split('.')[:-1])
      plt.savefig('.'.join([bname + '-' + desc['name'], 'pdf']))
      plt.close()
