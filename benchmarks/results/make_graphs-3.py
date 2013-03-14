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

    benchmarks = set([d[0]['bench'] for d in RESULTS])

    # configs
    def mk_descs(bench):
      descs = [
        {
          'x-axis' : 'threads',
          'y-axis' : lambda x: x[0],
          'lines' : ['db'], # each line holds this constant
          'x-label' : 'num threads',
          'y-label' : 'txns/sec',
          'title' : '%s throughput graph' % bench,
          'name' : 'throughput',
        },
        {
          'x-axis' : 'threads',
          'y-axis' : lambda x: x[1],
          'lines' : ['db'], # each line holds this constant
          'x-label' : 'num threads',
          'y-label' : 'aborts/sec',
          'title' : '%s abort graph' % bench,
          'name' : 'abort',
        }]
      return descs

    for bench in benchmarks:
      bench_results = [d for d in RESULTS if d[0]['bench'] == bench]
      for desc in mk_descs(bench):
        lines = {}
        for (config, result) in bench_results:
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
        plt.savefig('.'.join([bname + '-' + bench + '-' + desc['name'], 'pdf']))
        plt.close()
