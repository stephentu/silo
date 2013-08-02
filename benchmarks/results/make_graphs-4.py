#!/usr/bin/env python

import matplotlib
import pylab as plt
import numpy as np

import os
import sys
import math

def filter_name(results, name):
  def match(ent):
    return ent[0]['name'] == name
  return [x for x in results if match(x)]

def order_results_by_threads(results):
  # res is list[(config, results)], change to
  # list[(num_threads, results)]
  def trfm(ent):
    return (ent[0]['threads'], ent[1])
  return map(trfm, results)

def split_results_by_predicate(results, pred):
  s0, s1 = [], []
  for res in results:
    if pred(res):
      s0.append(res)
    else:
      s1.append(res)
  return s0, s1

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

def XX(x):
  return [e[0] for e in x]

def median(x): return sorted(x)[len(x)/2]

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

def handle_scale_tpcc(f, results):
  # two graphs
  # x-axis is num threads on both
  # y-axis[0] is throughput
  # y-axis[1] is latency

  no_persist, with_persist = \
      split_results_by_predicate(results, lambda x: not x[0]['persist'])
  no_persist, with_persist = \
      order_results_by_threads(no_persist), order_results_by_threads(with_persist)

  no_persist_throughput, no_persist_latency = \
      extract_throughput(no_persist, False), extract_latency(no_persist, False)
  with_persist_throughput, with_persist_latency = \
      extract_throughput(with_persist, True), extract_latency(with_persist, True)

  fig = plt.figure()
  ax = plt.subplot(111)
  ax.errorbar(XX(no_persist_throughput), YY(no_persist_throughput), yerr=YERR(no_persist_throughput))
  ax.errorbar(XX(with_persist_throughput), YY(with_persist_throughput), yerr=YERR(with_persist_throughput))
  ax.legend(('No-Persist', 'Persist'), loc='upper left')
  ax.set_xlabel('threads')
  ax.set_ylabel('throughput (txns/sec)')
  bname = '.'.join(os.path.basename(f).split('.')[:-1])
  fig.savefig('.'.join([bname + '-scale_tpcc-throughput', 'pdf']))

  fig = plt.figure()
  ax = plt.subplot(111)
  ax.errorbar(XX(no_persist_throughput), YYPC(no_persist_throughput), yerr=YERRPC(no_persist_throughput))
  ax.errorbar(XX(with_persist_throughput), YYPC(with_persist_throughput), yerr=YERRPC(with_persist_throughput))
  ax.legend(('No-Persist', 'Persist'), loc='upper left')
  ax.set_xlabel('threads')
  ax.set_ylabel('throughput (txns/sec/core)')
  ax.set_ylim([20000, 32000])
  bname = '.'.join(os.path.basename(f).split('.')[:-1])
  fig.savefig('.'.join([bname + '-scale_tpcc-per-core-throughput', 'pdf']))

  fig = plt.figure()
  ax = plt.subplot(111)
  ax.errorbar(XX(no_persist_latency), YY(no_persist_latency), yerr=YERR(no_persist_latency))
  ax.errorbar(XX(with_persist_latency), YY(with_persist_latency), yerr=YERR(with_persist_latency))
  ax.legend(('No-Persist', 'Persist'), loc='upper left')
  ax.set_xlabel('threads')
  ax.set_ylabel('latency (ms/txn)')
  bname = '.'.join(os.path.basename(f).split('.')[:-1])
  fig.savefig('.'.join([bname + '-scale_tpcc-latency', 'pdf']))

if __name__ == '__main__':
  files = sys.argv[1:]
  for f in files:
    execfile(f)

    # scale_tpcc
    scale_tpcc = filter_name(RESULTS, 'scale_tpcc')
    if scale_tpcc:
      handle_scale_tpcc(f, scale_tpcc)
