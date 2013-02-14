#include <iostream>
#include <sstream>
#include <vector>
#include <utility>
#include <string>

#include <stdlib.h>
#include <unistd.h>

#include "../macros.h"
#include "../varkey.h"
#include "../thread.h"
#include "../util.h"
#include "../spinbarrier.h"

#include "bench.h"

using namespace std;
using namespace util;

static size_t nkeys;

class ycsb_worker : public bench_worker {
public:
  ycsb_worker(unsigned long seed, abstract_db *db,
              const map<string, abstract_ordered_index *> &open_tables,
              spin_barrier *barrier_a, spin_barrier *barrier_b)
    : bench_worker(seed, db, open_tables, barrier_a, barrier_b),
      tbl(open_tables.at("USERTABLE"))
  {
  }

  void
  txn_read()
  {
    void *txn = db->new_txn(txn_flags);
    const bool direct_mem = db->index_supports_direct_mem_access();
    string k = u64_varkey(r.next() % nkeys).str();
    try {
      char *v = 0;
      size_t vlen = 0;
      ALWAYS_ASSERT(tbl->get(txn, k.data(), k.size(), v, vlen));
      if (!direct_mem) free(v);
      if (db->commit_txn(txn))
        ntxn_commits++;
    } catch (abstract_db::abstract_abort_exception &ex) {
      db->abort_txn(txn);
      ntxn_aborts++;
    }
  }

  static void
  TxnRead(bench_worker *w)
  {
    static_cast<ycsb_worker *>(w)->txn_read();
  }

  void
  txn_write()
  {
    void *txn = db->new_txn(txn_flags);
    string k = u64_varkey(r.next() % nkeys).str();
    try {
      string v(128, 'b');
      tbl->put(txn, k.data(), k.size(), v.data(), v.size());
      if (db->commit_txn(txn))
        ntxn_commits++;
    } catch (abstract_db::abstract_abort_exception &ex) {
      db->abort_txn(txn);
      ntxn_aborts++;
    }
  }

  static void
  TxnWrite(bench_worker *w)
  {
    static_cast<ycsb_worker *>(w)->txn_write();
  }

  void
  txn_rmw()
  {
    void *txn = db->new_txn(txn_flags);
    const bool direct_mem = db->index_supports_direct_mem_access();
    string k = u64_varkey(r.next() % nkeys).str();
    try {
      char *v = 0;
      size_t vlen = 0;
      ALWAYS_ASSERT(tbl->get(txn, k.data(), k.size(), v, vlen));
      if (!direct_mem) free(v);
      string vnew(128, 'c');
      tbl->put(txn, k.data(), k.size(), vnew.data(), vnew.size());
      if (db->commit_txn(txn))
        ntxn_commits++;
    } catch (abstract_db::abstract_abort_exception &ex) {
      db->abort_txn(txn);
      ntxn_aborts++;
    }
  }

  static void
  TxnRmw(bench_worker *w)
  {
    static_cast<ycsb_worker *>(w)->txn_rmw();
  }

  class worker_scan_callback : public abstract_ordered_index::scan_callback {
  public:
    virtual bool
    invoke(const char *key, size_t key_len,
           const char *value, size_t value_len)
    {
      return true;
    }
  };

  void
  txn_scan()
  {
    void *txn = db->new_txn(txn_flags);
    size_t kstart = r.next() % nkeys;
    string kbegin = u64_varkey(kstart).str();
    string kend = u64_varkey(kstart + 100).str();
    worker_scan_callback c;
    try {
      tbl->scan(txn, kbegin.data(), kbegin.size(),
                kend.data(), kend.size(), true, c);
      if (db->commit_txn(txn))
        ntxn_commits++;
    } catch (abstract_db::abstract_abort_exception &ex) {
      db->abort_txn(txn);
      ntxn_aborts++;
    }
  }

  static void
  TxnScan(bench_worker *w)
  {
    static_cast<ycsb_worker *>(w)->txn_scan();
  }

  virtual workload_desc
  get_workload()
  {
    workload_desc w;
    //w.push_back(make_pair(1.00, TxnRead));

    //w.push_back(make_pair(0.85, TxnRead));
    //w.push_back(make_pair(0.10, TxnScan));
    //w.push_back(make_pair(0.04, TxnRmw));
    //w.push_back(make_pair(0.01, TxnWrite));

    w.push_back(make_pair(0.95, TxnRead));
    w.push_back(make_pair(0.04, TxnRmw));
    w.push_back(make_pair(0.01, TxnWrite));
    return w;
  }

private:
  abstract_ordered_index *tbl;
};

void
ycsb_do_test(abstract_db *db)
{
  nkeys = size_t(scale_factor * 1000.0);
  assert(nkeys > 0);

  spin_barrier barrier_a(nthreads);
  spin_barrier barrier_b(1);

  db->thread_init();

  abstract_ordered_index *tbl = db->open_index("USERTABLE");
  map<string, abstract_ordered_index *> open_tables;
  open_tables["USERTABLE"] = tbl;

  // load
  const size_t batchsize = (db->txn_max_batch_size() == -1) ?
    10000 : db->txn_max_batch_size();
  ALWAYS_ASSERT(batchsize > 0);
  const size_t nbatches = nkeys / batchsize;
  if (nbatches == 0) {
    void *txn = db->new_txn(txn_flags);
    for (size_t j = 0; j < nkeys; j++) {
      string k = u64_varkey(j).str();
      string v(128, 'a');
      tbl->insert(txn, k.data(), k.size(), v.data(), v.size());
    }
    if (verbose)
      cerr << "batch 1/1 done" << endl;
    ALWAYS_ASSERT(db->commit_txn(txn));
  } else {
    for (size_t i = 0; i < nbatches; i++) {
      size_t keyend = (i == nbatches - 1) ? nkeys : (i + 1) * batchsize;
      void *txn = db->new_txn(txn_flags);
      for (size_t j = i * batchsize; j < keyend; j++) {
        string k = u64_varkey(j).str();
        string v(128, 'a');
        tbl->insert(txn, k.data(), k.size(), v.data(), v.size());
      }
      if (verbose)
        cerr << "batch " << (i + 1) << "/" << nbatches << " done" << endl;
      ALWAYS_ASSERT(db->commit_txn(txn));
    }
  }

  db->do_txn_epoch_sync();

  fast_random r(8544290);
  vector<ycsb_worker *> workers;
  for (size_t i = 0; i < nthreads; i++)
    workers.push_back(new ycsb_worker(r.next(), db, open_tables, &barrier_a, &barrier_b));
  for (size_t i = 0; i < nthreads; i++)
    workers[i]->start();
  barrier_a.wait_for();
  barrier_b.count_down();
  timer t;
  sleep(runtime);
  running = false;
  __sync_synchronize();
  unsigned long elapsed = t.lap();
  size_t n_commits = 0;
  size_t n_aborts = 0;
  for (size_t i = 0; i < nthreads; i++) {
    workers[i]->join();
    n_commits += workers[i]->get_ntxn_commits();
    n_aborts += workers[i]->get_ntxn_aborts();
  }

  double agg_throughput = double(n_commits) / (double(elapsed) / 1000000.0);
  double avg_per_core_throughput = agg_throughput / double(nthreads);

  double agg_abort_rate = double(n_aborts) / (double(elapsed) / 1000000.0);
  double avg_per_core_abort_rate = agg_abort_rate / double(nthreads);

  if (verbose) {
    cerr << "agg_throughput: " << agg_throughput << " ops/sec" << endl;
    cerr << "avg_per_core_throughput: " << avg_per_core_throughput << " ops/sec/core" << endl;
    cerr << "agg_abort_rate: " << agg_abort_rate << " aborts/sec" << endl;
    cerr << "avg_per_core_abort_rate: " << avg_per_core_abort_rate << " aborts/sec/core" << endl;
  }
  cout << agg_throughput << endl;

  db->do_txn_finish();
  db->thread_end();
}
