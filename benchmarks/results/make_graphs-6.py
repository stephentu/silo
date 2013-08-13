#!/usr/bin/env python

import matplotlib
import pylab as plt
import numpy as np

import re
import os
import sys
import math

def predicate(fn):
    def pred(x):
        if 'persist' not in x[0]:
            return not persist
        return x[0]['persist'] if persist else not x[0]['persist']
    return pred

NEW_ORDER_RGX = re.compile(r'--new-order-remote-item-pct (\d+)')
def extract_pct(x):
  x = x[0]['bench_opts']
  m = NEW_ORDER_RGX.search(x)
  assert m
  p = int(m.group(1))
  assert p >= 0 and p <= 100
  def pn(n, p):
    return 1.0 - (1.0 - p)**n
  def ex(p):
    import math
    return math.fsum([(1.0/11.0)*pn(float(n), p) for n in range(5, 16)])
  return ex(p/100.0) * 100.0

def deal_with_posK_res(k):
    def fn(x):
        x = x[1]
        if type(x) == list:
            return [e[k] for e in x]
        return x[k]
    return fn

def mkplot(results, desc, outfilename):
    def mean(x):   return sum(x)/len(x)
    def median(x): return sorted(x)[len(x)/2]
    fig = plt.figure()
    ax = plt.subplot(111)
    lines = []
    for line_desc in desc['lines']:
        predfn = line_desc['extractor']
        line_results = [d for d in results if predfn(d)]
        xpts = map(desc['x-axis'], line_results)
        ypts = map(desc['y-axis'], line_results)
        lines.append({ 'xpts' : xpts, 'ypts' : ypts })
    if not desc['show-error-bars']:
        for l in lines:
            ax.plot(l['xpts'], [median(y) for y in l['ypts']])
    else:
        for l in lines:
            ymins = np.array([min(y) for y in l['ypts']])
            ymaxs = np.array([max(y) for y in l['ypts']])
            ymid = np.array([median(y) for y in l['ypts']])
            yerr = np.array([ymid - ymins, ymaxs - ymid])
            ax.errorbar(l['xpts'], ymid, yerr=yerr)

    ax.set_xlabel(desc['x-label'])
    ax.set_ylabel(desc['y-label'])
    ax.set_ylim(ymin = 0)
    ax.legend([l['label'] for l in desc['lines']], loc=desc['legend'])
    fig.savefig(outfilename, format='pdf')

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

if __name__ == '__main__':
    (_, inp) = sys.argv
    execfile(inp)

    def maflingo_regular_extractor(x):
        if x[0]['db'] != 'ndb-proto2':
            return False
        return x[0]['bench_opts'].find('--enable-separate-tree-per-partition') == -1

    def maflingo_sep_trees_extractor(x):
        if x[0]['db'] != 'ndb-proto2':
            return False
        return x[0]['bench_opts'].find('--enable-separate-tree-per-partition') != -1

    config = \
      {
        'x-axis' : extract_pct,
        'y-axis' : deal_with_posK_res(0),
        'lines' : [
            {
                'label' : 'Partition-Store',
                'extractor' : lambda x: x[0]['db'] == 'kvdb-st',
            },
            {
                'label' : 'Maflingo',
                'extractor' : maflingo_regular_extractor,
            },
            {
                'label' : 'Partition-Maflingo',
                'extractor' : maflingo_sep_trees_extractor,
            },
        ],
        'x-label' : '% cross-partition',
        'y-label' : 'throughput (txns/sec)',
        'y-axis-major-formatter' : matplotlib.ticker.FuncFormatter(MFormatter),
        'x-axis-set-major-locator' : False,
        'show-error-bars' : True,
        'legend' : 'upper right',
      }

    mkplot(RESULTS, config, 'istc3-8-12-13_multipart.pdf')
