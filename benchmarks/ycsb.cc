#include <iostream>

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
static size_t nthreads = 24;
static volatile bool running = true;

class worker : public ndb_thread {
public:
  worker(unsigned long seed, double read_ratio, abstract_db *db)
    : seed(seed), read_ratio(read_ratio), db(db), ntxns(0) {}

  virtual void run()
  {
    fast_random r(seed);
    while (running) {
      void *txn = db->new_txn();
      string k = u64_varkey(r.next()).str();
      try {
        double p = r.next_uniform();
        if (p < read_ratio) {
          char *v = 0;
          size_t vlen = 0;
          if (db->get(txn, k.data(), k.size(), v, vlen))
            free(v);
        } else {
          string v(128, 'b');
          db->put(txn, k.data(), k.size(), v.data(), v.size());
        }
        if (db->commit_txn(txn))
          ntxns++;
      } catch (abstract_db::abstract_abort_exception &ex) {
        db->abort_txn(txn);
      }
    }
  }

  unsigned long seed;
  double read_ratio;
  abstract_db *db;
  size_t ntxns;
};

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
    workers.push_back(new worker(r.next(), read_ratio, db));
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
  int ret UNUSED = system("rm -rf db/*");
  bdb_wrapper w("db", "ycsb.db");

  //ndb_wrapper w;

  do_test(&w, 0.95);
  return 0;
}
