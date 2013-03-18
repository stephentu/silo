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

    def deal_with_pos0_res(x):
      if type(x) == list:
        return [e[0] for e in x]
      return x[0]

    def deal_with_pos1_res(x):
      if type(x) == list:
        return [e[1] for e in x]
      return x[1]

    # configs
    def mk_descs(bench):
      descs = [
        {
          'x-axis' : 'threads',
          'y-axis' : deal_with_pos0_res,
          'lines' : ['db'], # each line holds this constant
          'x-label' : 'num threads',
          'y-label' : 'txns/sec',
          'title' : '%s throughput graph' % bench,
          'name' : 'throughput',
        },
        {
          'x-axis' : 'threads',
          'y-axis' : deal_with_pos1_res,
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

        def mean(x): return sum(x)/len(x)
        def median(x): return x[len(x)/2]

        labels = []
        for (name, pts) in lines.iteritems():
          spts = sorted(pts.iteritems(), key=lambda x: x[0])
          ypts = [sorted(x[1]) for x in spts]
          ymins = np.array([min(x) for x in ypts])
          ymaxs = np.array([max(x) for x in ypts])
          ymid = np.array([median(x) for x in ypts])
          yerr=np.array([ymid - ymins, ymaxs - ymid])
          plt.errorbar([x[0] for x in spts], ymid, yerr=yerr)
          labels.append('-'.join(name))

        plt.xlabel(desc['x-label'])
        plt.ylabel(desc['y-label'])
        plt.title(desc['title'])
        plt.legend(labels, loc='upper left')
        bname = '.'.join(os.path.basename(f).split('.')[:-1])
        plt.savefig('.'.join([bname + '-' + bench + '-' + desc['name'], 'pdf']))
        plt.close()
