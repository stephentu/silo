#include <iostream>
#include <vector>
#include <utility>
#include <string>

#include <getopt.h>
#include <stdlib.h>

#include "../macros.h"
#include "../varkey.h"
#include "../thread.h"
#include "../util.h"

#include "bdb_wrapper.h"
#include "ndb_wrapper.h"

using namespace std;
using namespace util;

static size_t nkeys = 100000;
static size_t nthreads = 1;
static volatile bool running = true;
static int verbose = 0;

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
do_test(abstract_db *db)
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
  sleep(30);
  running = false;
  size_t n = 0;
  for (size_t i = 0; i < nthreads; i++) {
    workers[i]->join();
    n += workers[i]->ntxns;
  }

  double agg_throughput = double(n) / (double(t.lap()) / 1000000.0);
  double avg_per_core_throughput = agg_throughput / double(nthreads);

  if (verbose) {
    cerr << "agg_throughput: " << agg_throughput << " ops/sec" << endl;
    cerr << "avg_per_core_throughput: " << avg_per_core_throughput << " ops/sec/core" << endl;
  }
  cout << agg_throughput << endl;
}

int
main(int argc, char **argv)
{
  abstract_db *db = NULL;
  string db_type;
  while (1) {
    static struct option long_options[] =
    {
      {"verbose", no_argument,       &verbose, 1},
      {"num-keys",  required_argument, 0, 'k'},
      {"num-threads",  required_argument, 0, 't'},
      {"db-type",    required_argument, 0, 'd'},
      {0, 0, 0, 0}
    };
    int option_index = 0;
    int c = getopt_long(argc, argv, "vk:t:d:", long_options, &option_index);
    if (c == -1)
      break;

    switch (c) {
    case 0:
      if (long_options[option_index].flag != 0)
        break;
      abort();
      break;

    case 'k':
      nkeys = strtoul(optarg, NULL, 10);
      ALWAYS_ASSERT(nkeys > 0);
      break;

    case 't':
      nthreads = strtoul(optarg, NULL, 10);
      ALWAYS_ASSERT(nthreads > 0);
      break;

    case 'd':
      db_type = optarg;
      if (db_type == "bdb") {
        int ret UNUSED = system("rm -rf db/*");
        db = new bdb_wrapper("db", "ycsb.db");
      } else if (db_type == "ndb-proto1") {
        db = new ndb_wrapper(ndb_wrapper::PROTO_1);
      } else if (db_type == "ndb-proto2") {
        db = new ndb_wrapper(ndb_wrapper::PROTO_2);
      } else
        ALWAYS_ASSERT(false);
      break;

    case '?':
      /* getopt_long already printed an error message. */
      break;

    default:
      abort();
    }
  }

  if (verbose) {
    cerr << "settings:" << endl;
    cerr << "  num-keys: " << nkeys << endl;
    cerr << "  num-threads: " << nthreads << endl;
    cerr << "  db-type: " << db_type << endl;
  }

  do_test(db);
  return 0;
}
