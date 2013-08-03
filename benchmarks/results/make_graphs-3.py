#!/usr/bin/env python

import matplotlib
import pylab as plt
import numpy as np

import os
import sys
import math

if __name__ == '__main__':
  files = sys.argv[1:]
  for f in files:
    execfile(f)

    #names = ['scale', 'multipart:pct', 'multipart:cpu']

    def deal_with_posK_res(k, x):
      if type(x) == list:
        return [e[k] for e in x]
      return x[k]

    def deal_with_pos0_res(x):
      return deal_with_posK_res(0, x)

    def deal_with_pos1_res(x):
      return deal_with_posK_res(1, x)

    import re
    RGX = re.compile(r'--new-order-remote-item-pct (\d+)')
    def extract_pct(x):
      m = RGX.search(x)
      assert m
      p = int(m.group(1))
      assert p >= 0 and p <= 100
      def pn(n, p):
        return 1.0 - (1.0 - p)**n
      def ex(p):
        import math
        return math.fsum([(1.0/11.0)*pn(float(n), p) for n in range(5, 16)])
      return ex(p/100.0) * 100.0

    def extract_p(x):
      m = RGX.search(x)
      assert m
      p = int(m.group(1))
      assert p >= 0 and p <= 100
      return p

    def multipart_cpu_process(config):
      assert config['db'] == 'ndb-proto2' or \
             config['db'] == 'kvdb'
      if config['db'] == 'ndb-proto2':
        return config['threads']
      else:
        return 8

    def readonly_lines_func(config):
      if 'disable-read-only-snapshots' in config['bench_opts']:
        return 'No-Snapshots'
      else:
        return 'Snapshots'

    def MFormatter(x, p):
      if x == 0:
        return '0'
      v = float(x)/float(10**6)
      if math.ceil(v) == v:
        return '%dM' % v
      return '%.1fM' % v

    def KFormatter(x, p):
      if x == 0:
        return '0'
      v = float(x)/float(10**3)
      if math.ceil(v) == v:
        return '%dK' % v
      return '%.1fK' % v

    descs = [
      {
        'name' : 'scale',
        'x-axis' : 'threads',
        'x-axis-func' : lambda x: x,
        'y-axis' : deal_with_pos0_res,
        'lines' : ['db'], # each line holds this constant
        'x-label' : 'threads',
        'y-label' : 'throughput (txns/sec)',
        'y-axis-major-formatter' : matplotlib.ticker.FuncFormatter(MFormatter),
        'x-axis-set-major-locator' : True,
        #'title' : 'ycsb throughput graph',
      },
      {
        'name' : 'scale_tpcc',
        'x-axis' : 'threads',
        'x-axis-func' : lambda x: x,
        'y-axis' : deal_with_pos0_res,
        'lines' : ['db'], # each line holds this constant
        'x-label' : 'threads',
        'y-label' : 'throughput (txns/sec)',
        'y-axis-major-formatter' : matplotlib.ticker.FuncFormatter(MFormatter),
        'x-axis-set-major-locator' : True,
        #'title' : 'tpcc throughput graph',
      },
      {
        'name' : 'multipart:pct',
        'x-axis' : 'bench_opts',
        'x-axis-func' : extract_pct,
        'y-axis' : deal_with_pos0_res,
        'lines' : ['db'], # each line holds this constant
        'x-label' : '% cross-partition',
        'y-label' : 'throughput (txns/sec)',
        'y-axis-major-formatter' : matplotlib.ticker.FuncFormatter(MFormatter),
        'x-axis-set-major-locator' : False,
        #'title' : 'tpcc new-order throughput graph',
        'legend' : 'upper right',
      },
      #{
      #  'name' : 'multipart:cpu',
      #  'x-axis-process' : multipart_cpu_process,
      #  'y-axis' : deal_with_pos0_res,
      #  'lines' : ['db'], # each line holds this constant
      #  'x-label' : 'num threads',
      #  'y-label' : 'txns/sec',
      #  'title' : 'tpcc full workload throughput graph',
      #},
      {
        'name' : 'readonly',
        'x-axis' : 'bench_opts',
        'x-axis-func' : extract_p,
        'y-axis' : deal_with_pos0_res,
        'lines-func' : readonly_lines_func,
        'x-label' : '% remote warehouse stock',
        'y-label' : 'throughput (txns/sec)',
        'y-axis-major-formatter' : matplotlib.ticker.FuncFormatter(KFormatter),
        'x-axis-set-major-locator' : True,
        #'title' : 'tpcc read only throughput graph',
        'legend' : 'right',
      },
    ]

    def label_transform(x):
      if x == 'kvdb':
        return 'Key-Value'
      if x == 'ndb-proto1':
        return 'Malflingo-Star'
      if x == 'ndb-proto2':
        return 'Malflingo'
      if x == 'kvdb-st':
        return 'Partitioned-Store'
      return x

    for desc in descs:
      bench = desc['name']
      bench_results = [d for d in RESULTS if d[0]['name'] == bench]
      if not bench_results:
        print >>sys.stderr, 'skipping bench %s' % bench
        continue
      lines = {}
      for (config, result) in bench_results:
        if 'lines-func' in desc:
          key = desc['lines-func'](config)
        else:
          key = tuple(config[x] for x in desc['lines'])
        pts = lines.get(key, {})
        if 'x-axis-process' in desc:
          xpt = desc['x-axis-process'](config)
        else:
          xpt = desc['x-axis-func'](config[desc['x-axis']])
        assert xpt not in pts
        pts[xpt] = desc['y-axis'](result)
        lines[key] = pts

      def mean(x): return sum(x)/len(x)
      def median(x): return sorted(x)[len(x)/2]

      # find min/max of xpts
      xmin = min([e for l in lines.values() for e in l])
      xmax = max([e for l in lines.values() for e in l])
      #print xmin, xmax

      labels = []
      for (name, pts) in lines.iteritems():
        spts = sorted(pts.iteritems(), key=lambda x: x[0])
        ypts = [sorted(x[1]) for x in spts]
        ymins = np.array([min(x) for x in ypts])
        ymaxs = np.array([max(x) for x in ypts])
        ymid = np.array([median(x) for x in ypts])
        yerr=np.array([ymid - ymins, ymaxs - ymid])
        xpts = [x[0] for x in spts]
        assert len(xpts)
        if len(xpts) == 1:
          xpts = range(xmin, xmax + 1)
          assert len(ymins) == 1
          assert len(ymaxs) == 1
          assert len(ymid) == 1
          ymins = np.array([ymins[0] for _ in xpts])
          ymaxs = np.array([ymaxs[0] for _ in xpts])
          ymid = np.array([ymid[0] for _ in xpts])
          yerr=np.array([ymid - ymins, ymaxs - ymid])

        plt.errorbar(xpts, ymid, yerr=yerr)
        if type(name) == str:
          labels.append(label_transform(name))
        else:
          labels.append(label_transform('-'.join(name)))

      ax = plt.gca()
      if desc['x-axis-set-major-locator']:
        ax.xaxis.set_major_locator(matplotlib.ticker.FixedLocator(sorted(lines.values()[0].keys())))
      if 'y-axis-major-formatter' in desc:
        ax.yaxis.set_major_formatter(desc['y-axis-major-formatter'])

      plt.xlabel(desc['x-label'])
      plt.ylabel(desc['y-label'])
      if 'title' in desc:
        plt.title(desc['title'])

      plt.xlim(xmin = xmin, xmax = xmax)
      plt.ylim(ymin = 0)

      placement = 'upper left' if not 'legend' in desc else desc['legend']
      plt.legend(labels, loc=placement)
      bname = '.'.join(os.path.basename(f).split('.')[:-1])
      plt.savefig('.'.join([bname + '-' + bench, 'pdf']))
      plt.close()
