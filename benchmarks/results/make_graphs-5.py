#!/usr/bin/env python

import matplotlib
import pylab as plt
import numpy as np

import os
import sys
import math

NAMEPAT='istc3-8-1-13%s.py'
def N(x):
    return NAMEPAT % x

def split_results_by_predicate(results, pred):
  s0, s1 = [], []
  for res in results:
    if pred(res):
      s0.append(res)
    else:
      s1.append(res)
  return s0, s1

FILES = (
    (N(''), False),
    (N('_fake_compress'), True),
    (N(''), True),
    (N('_newbench'), True),
    (N('_compress'), True),

    #(N('_fake_writes'), True),
    #(N('_fake_writes_stride'), True),
    #(N('_fake_writes_stride1'), True),
    #(N('_log_reduce_size'), True),
)

def datafromfile(f, persist):
    g, l = {}, {}
    execfile(f, g, l)
    res, _ = split_results_by_predicate(
        l['RESULTS'],
        lambda x: x[0]['persist'] if persist else not x[0]['persist'])
    return res

if __name__ == '__main__':
    def order_results_by_threads(results):
        # res is list[(config, results)], change to
        # list[(num_threads, results)]
        def trfm(ent):
            return (ent[0]['threads'], ent[1])
        return map(trfm, results)

    def extract_result_position(k, res):
        if type(res) == list:
            return [x[k] for x in res]
        return res[k]

    def extract_throughput(results, persist):
        def trfm(ent):
            return (ent[0], extract_result_position(0 if not persist else 1, ent[1]))
        return map(trfm, results)

    def extract_latency(results, persist):
        def trfm(ent):
            return (ent[0], extract_result_position(2 if not persist else 3, ent[1]))
        return map(trfm, results)

    def filter_name(results, name):
        def match(ent):
            return ent[0]['name'] == name
        return [x for x in results if match(x)]

    def XX(x):
        return [e[0] for e in x]

    def perturb(x):
        return [np.random.normal(loc=0.0, scale=0.2) + e for e in x]

    def scalaradd(x, s):
        return [e + s for e in x]

    def scale(x, s):
        return [e / s for e in x]

    def median(x): return sorted(x)[len(x)/2]

    def percorify(x):
        return [ (e[0], [ee/e[0] for ee in e[1]]) for e in x ]

    def YY(x):
        def checked(e):
            if type(e) == list:
                return median(e)
            return e
        return [checked(e[1]) for e in x]

    def YYPC(x):
        def checked(e):
            if type(e) == list:
                return median(e)
            return e
        return [checked(e[1])/float(e[0]) for e in x]

    def YERR(x):
        ypts = [e[1] for e in x]
        ymins = np.array([min(x) for x in ypts])
        ymaxs = np.array([max(x) for x in ypts])
        ymid = np.array([median(x) for x in ypts])
        yerr=np.array([ymid - ymins, ymaxs - ymid])
        return yerr

    def YERRPC(x):
        ypts = [[ee/float(e[0]) for ee in e[1]] for e in x]
        ymins = np.array([min(x) for x in ypts])
        ymaxs = np.array([max(x) for x in ypts])
        ymid = np.array([median(x) for x in ypts])
        yerr=np.array([ymid - ymins, ymaxs - ymid])
        return yerr

    def nameit(x):
        fname, persist = x
        fname = fname.replace('istc3-8-1-13', '').replace('.py', '')
        if not fname:
            return 'base-persist' if persist else 'base'
        return fname

    fig, fig1 = plt.figure(), plt.figure()
    ax, ax1 = fig.add_subplot(111), fig1.add_subplot(111)

    from matplotlib.font_manager import FontProperties
    fontP = FontProperties()
    fontP.set_size('small')

    off = 0.0
    for fname, persist in FILES:
        res = datafromfile(fname, persist)
        res = order_results_by_threads(res)
        throughput = extract_throughput(res, persist)
        percorethroughput = percorify(throughput)
        print percorethroughput
        ax.errorbar(scalaradd(XX(throughput), off), YY(percorethroughput), yerr=YERR(percorethroughput))
        off += 0.1

    ax.legend(map(nameit, FILES), loc='lower right', prop=fontP)
    ax.set_xlabel('threads')
    ax.set_ylabel('throughput (txns/sec/core)')
    ax.set_ylim([0, 32000])
    fig.savefig('istc3-8-1-13_summary.pdf')
