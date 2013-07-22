#!/usr/bin/env python

import itertools
import platform
import subprocess
import sys
import math
import pickle

def normalize(x):
  denom = sum(x)
  return [e/denom for e in x]

def scale(x, a):
  return [e * a for e in x]

def argcmp(x, comp, predicate):
  idx = None
  val = None
  for i in xrange(len(x)):
    if not predicate(x[i]):
      continue
    if idx is None or comp(x[i], val):
      idx = i
      val = x[i]
  if idx is None:
    # couldn't find it
    raise Exception("no argmin satisfiying predicate")
  return idx

def argmin(x, predicate):
  return argcmp(x, lambda a, b: a < b, predicate)

def argmax(x, predicate):
  return argcmp(x, lambda a, b: a > b, predicate)

def allocate(nworkers, weights):
  approx = map(int, map(math.ceil, scale(weights, nworkers)))
  diff = sum(approx) - nworkers
  if diff > 0:
    while diff > 0:
      i = argmin(approx, predicate=lambda x: x > 0)
      approx[i] -= 1
      diff -= 1
  elif diff < 0:
    i = argmax(approx, lambda x: True)
    approx[i] += -diff
  acc = 0
  ret = []
  for x in approx:
    ret.append(range(acc, acc + x))
    acc += x
  return ret

def run(cmd):
  print >>sys.stderr, '[INFO] running command %s' % str(cmd)
  p = subprocess.Popen(cmd, stdin=open('/dev/null', 'r'), stdout=subprocess.PIPE)
  r = p.stdout.read()
  p.wait()
  return r

if __name__ == '__main__':
  (_, outfile) = sys.argv

  STRATEGIES = ['epoch', 'epoch-compress']
  NCORES = [1, 2, 4, 8, 16, 24, 32]
  WSET = [18]

  #STRATEGIES = ['epoch']
  #NCORES = [1]
  #WSET = [18]

  node = platform.node()

  if node == 'modis2':
    LOGGERS = [
        ('data.log', 1.),
        ('/data/scidb/001/2/stephentu/data.log', 1.),
        ('/data/scidb/001/3/stephentu/data.log', 1.),
    ]
  elif node == 'istc3':
    LOGGERS = [
        ('data.log', 1./3.),
        ('/f0/stephentu/data.log', 2./3.),
    ]
  else:
    print "unknown node", node
    assert False, "Unknown node!"

  weights = normalize([x[1] for x in LOGGERS])
  logfile_cmds = list(itertools.chain.from_iterable([['--logfile', f] for f, _ in LOGGERS]))

  results = []
  for strat, ncores, ws in itertools.product(STRATEGIES, NCORES, WSET):
    allocations = allocate(ncores, weights)
    alloc_cmds = list(
        itertools.chain.from_iterable([['--assignment', ','.join(map(str, alloc))] for alloc in allocations]))
    cmd = ['./persist_test'] + \
        logfile_cmds + \
        alloc_cmds + \
        ['--num-threads', str(ncores),
         '--strategy', strat,
         '--writeset', str(ws),
         '--valuesize', '32']
    output = run(cmd)
    res = float(output.strip())
    results.append(((strat, ncores, ws), res))

  with open(outfile, 'w') as fp:
    pickle.dump(results, fp)
