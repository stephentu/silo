#include <iostream>
#include <vector>
#include <utility>

#include "../macros.h"
#include "../varkey.h"
#include "../thread.h"
#include "../util.h"

#include "bdb_wrapper.h"
#include "ndb_wrapper.h"

using namespace std;
using namespace util;

//static const size_t nkeys = 1000;
//static const size_t nkeys = 140000000;
static const size_t nkeys = 100000;
static size_t nthreads = 1;
static volatile bool running = true;

class worker;
typedef void (*txn_fn_t)(worker *);
typedef vector<pair<double, txn_fn_t> > workload_desc;
static workload_desc *MakeWorkloadDesc();
static workload_desc *ycsb_workload = MakeWorkloadDesc();

class worker : public ndb_thread {
public:
  worker(unsigned long seed, abstract_db *db)
    : r(seed), db(db), ntxns(0)
  {}

  void
  txn_read()
  {
    void *txn = db->new_txn();
    string k = u64_varkey(r.next() % nkeys).str();
    try {
      char *v = 0;
      size_t vlen = 0;
      ALWAYS_ASSERT(db->get(txn, k.data(), k.size(), v, vlen));
      free(v);
      if (db->commit_txn(txn))
        ntxns++;
    } catch (abstract_db::abstract_abort_exception &ex) {
      db->abort_txn(txn);
    }
  }
  static void TxnRead(worker *w) { w->txn_read(); }

  void
  txn_write()
  {
    void *txn = db->new_txn();
    string k = u64_varkey(r.next() % nkeys).str();
    try {
      string v(128, 'b');
      db->put(txn, k.data(), k.size(), v.data(), v.size());
      if (db->commit_txn(txn))
        ntxns++;
    } catch (abstract_db::abstract_abort_exception &ex) {
      db->abort_txn(txn);
    }
  }
  static void TxnWrite(worker *w) { w->txn_write(); }

  void
  txn_rmw()
  {
    void *txn = db->new_txn();
    string k = u64_varkey(r.next() % nkeys).str();
    try {
      char *v = 0;
      size_t vlen = 0;
      ALWAYS_ASSERT(db->get(txn, k.data(), k.size(), v, vlen));
      free(v);
      string vnew(128, 'c');
      db->put(txn, k.data(), k.size(), vnew.data(), vnew.size());
      if (db->commit_txn(txn))
        ntxns++;
    } catch (abstract_db::abstract_abort_exception &ex) {
      db->abort_txn(txn);
    }
  }
  static void TxnRmw(worker *w) { w->txn_rmw(); }

  virtual void run()
  {
    while (running) {
      double d = r.next_uniform();
      for (size_t i = 0; i < ycsb_workload->size(); i++) {
        if ((i + 1) == ycsb_workload->size() || d < ycsb_workload->operator[](i).first) {
          ycsb_workload->operator[](i).second(this);
          break;
        }
      }
    }
  }

  fast_random r;
  abstract_db *db;
  size_t ntxns;
};

static workload_desc *
MakeWorkloadDesc()
{
  workload_desc *w = new workload_desc;
  w->push_back(make_pair(0.95, worker::TxnRead));
  w->push_back(make_pair(0.04, worker::TxnRmw));
  w->push_back(make_pair(0.01, worker::TxnWrite));
  return w;
}

static void
do_test(abstract_db *db, double read_ratio)
{
  // load
  for (size_t i = 0; i < nkeys; i++) {
    void *txn = db->new_txn();
    string k = u64_varkey(i).str();
    string v(128, 'a');
    db->put(txn, k.data(), k.size(), v.data(), v.size());
    ALWAYS_ASSERT(db->commit_txn(txn));
  }

  fast_random r(8544290);
  vector<worker *> workers;
  for (size_t i = 0; i < nthreads; i++)
    workers.push_back(new worker(r.next(), db));
  timer t;
  for (size_t i = 0; i < nthreads; i++)
    workers[i]->start();
  sleep(10);
  running = false;
  size_t n = 0;
  for (size_t i = 0; i < nthreads; i++) {
    workers[i]->join();
    n += workers[i]->ntxns;
  }

  double agg_throughput = double(n) / (double(t.lap()) / 1000000.0);
  double avg_per_core_throughput = agg_throughput / double(nthreads);

  cerr << "agg_throughput: " << agg_throughput << " ops/sec" << endl;
  cerr << "avg_per_core_throughput: " << avg_per_core_throughput << " ops/sec/core" << endl;
}

int main(void)
{
  //int ret UNUSED = system("rm -rf db/*");
  //bdb_wrapper w("db", "ycsb.db");

  ndb_wrapper w;

  do_test(&w, 0.95);
  return 0;
}
