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

static inline string
queue_key(uint64_t id0, uint64_t id1)
{
  big_endian_trfm<uint64_t> t;
  string buf(2 * sizeof(uint64_t), 0);
  uint64_t *p = (uint64_t *) &buf[0];
  *p++ = t(id0);
  *p++ = t(id1);
  return buf;
}

static const string queue_values("ABCDEFGH");

class queue_worker : public bench_worker {
public:
  queue_worker(unsigned long seed, abstract_db *db,
               const map<string, abstract_ordered_index *> &open_tables,
               spin_barrier *barrier_a, spin_barrier *barrier_b,
               uint64_t id, bool consumer)
    : bench_worker(seed, db, open_tables, barrier_a, barrier_b),
      tbl(open_tables.at("table")), id(id), consumer(consumer),
      ctr(consumer ? 0 : nkeys)
  {
  }

  void
  txn_produce()
  {
    vector<char *> delete_me;
    void *txn = db->new_txn(txn_flags);
    //const bool direct_mem = db->index_supports_direct_mem_access();
    try {
      const string k = queue_key(id, ctr);
      tbl->insert(txn, k.data(), k.size(), queue_values.data(), queue_values.size());
      if (db->commit_txn(txn)) {
        ctr++;
        ntxn_commits++;
      } else {
        ntxn_aborts++;
      }
    } catch (abstract_db::abstract_abort_exception &ex) {
      db->abort_txn(txn);
      ntxn_aborts++;
    }
    for (vector<char *>::iterator it = delete_me.begin();
         it != delete_me.end(); ++it)
      free(*it);
  }

  static void
  TxnProduce(bench_worker *w)
  {
    static_cast<queue_worker *>(w)->txn_produce();
  }

  void
  txn_consume()
  {
    void *txn = db->new_txn(txn_flags);
    //const bool direct_mem = db->index_supports_direct_mem_access();
    try {
      const string lowk = queue_key(id, 0);
      const string highk = queue_key(id, numeric_limits<uint64_t>::max());
      limit_callback c(1);
      tbl->scan(txn, lowk.data(), lowk.size(),
                highk.data(), highk.size(), true, c);
      if (likely(!c.values.empty())) {
        ALWAYS_ASSERT(c.values.size() == 1);
        const string &k = c.values.front().first;
        tbl->remove(txn, k.data(), k.size());
      }
      if (db->commit_txn(txn))
        ntxn_commits++;
      else
        ntxn_aborts++;
    } catch (abstract_db::abstract_abort_exception &ex) {
      db->abort_txn(txn);
      ntxn_aborts++;
    }
  }

  static void
  TxnConsume(bench_worker *w)
  {
    static_cast<queue_worker *>(w)->txn_consume();
  }

  void
  txn_consume_scanhint()
  {
    void *txn = db->new_txn(txn_flags);
    //const bool direct_mem = db->index_supports_direct_mem_access();
    try {
      const string lowk = queue_key(id, ctr);
      const string highk = queue_key(id, numeric_limits<uint64_t>::max());
      limit_callback c(1);
      tbl->scan(txn, lowk.data(), lowk.size(),
                highk.data(), highk.size(), true, c);
      const bool found = !c.values.empty();
      if (likely(found)) {
        ALWAYS_ASSERT(c.values.size() == 1);
        const string &k = c.values.front().first;
        tbl->remove(txn, k.data(), k.size());
      }
      if (db->commit_txn(txn)) {
        if (likely(found)) ctr++;
        ntxn_commits++;
      } else {
        ntxn_aborts++;
      }
    } catch (abstract_db::abstract_abort_exception &ex) {
      db->abort_txn(txn);
      ntxn_aborts++;
    }
  }

  static void
  TxnConsumeScanHint(bench_worker *w)
  {
    static_cast<queue_worker *>(w)->txn_consume_scanhint();
  }

  void
  txn_consume_noscan()
  {
    void *txn = db->new_txn(txn_flags);
    const bool direct_mem = db->index_supports_direct_mem_access();
    try {
      const string k = queue_key(id, ctr);
      char *v = 0;
      size_t vlen = 0;
      bool found = false;
      if (likely((found = tbl->get(txn, k.data(), k.size(), v, vlen)))) {
        if (!direct_mem) free(v);
        tbl->remove(txn, k.data(), k.size());
      }
      if (db->commit_txn(txn)) {
        if (likely(found)) ctr++;
        ntxn_commits++;
      } else {
        ntxn_aborts++;
      }
    } catch (abstract_db::abstract_abort_exception &ex) {
      db->abort_txn(txn);
      ntxn_aborts++;
    }
  }

  static void
  TxnConsumeNoScan(bench_worker *w)
  {
    static_cast<queue_worker *>(w)->txn_consume_noscan();
  }

  virtual workload_desc_vec
  get_workload() const
  {
    workload_desc_vec w;
    if (consumer)
      w.push_back(workload_desc("Consume", 1.0, TxnConsume));
      //w.push_back(workload_desc("ConsumeScanHint", 1.0, TxnConsumeScanHint));
      //w.push_back(workload_desc("ConsumeNoScan", 1.0, TxnConsumeNoScan));
    else
      w.push_back(workload_desc("Produce", 1.0, TxnProduce));
    return w;
  }

private:
  abstract_ordered_index *tbl;
  uint64_t id;
  bool consumer;
  uint64_t ctr;
};

class queue_table_loader : public bench_loader {
public:
  queue_table_loader(unsigned long seed,
                     abstract_db *db,
                     const map<string, abstract_ordered_index *> &open_tables)
    : bench_loader(seed, db, open_tables)
  {}

protected:
  virtual void
  load()
  {
    abstract_ordered_index *tbl = open_tables.at("table");
    try {
      // load
      const size_t batchsize = (db->txn_max_batch_size() == -1) ?
        10000 : db->txn_max_batch_size();
      ALWAYS_ASSERT(batchsize > 0);
      const size_t nbatches = nkeys / batchsize;
      for (size_t id = 0; id < nthreads / 2; id++) {
        if (nbatches == 0) {
          void *txn = db->new_txn(txn_flags);
          for (size_t j = 0; j < nkeys; j++) {
            const string k = queue_key(id, j);
            const string &v = queue_values;
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
              const string k = queue_key(id, j);
              const string &v = queue_values;
              tbl->insert(txn, k.data(), k.size(), v.data(), v.size());
            }
            if (verbose)
              cerr << "batch " << (i + 1) << "/" << nbatches << " done" << endl;
            ALWAYS_ASSERT(db->commit_txn(txn));
          }
        }
      }
    } catch (abstract_db::abstract_abort_exception &ex) {
      // shouldn't abort on loading!
      ALWAYS_ASSERT(false);
    }
    if (verbose)
      cerr << "[INFO] finished loading table" << endl;
  }
};

class queue_bench_runner : public bench_runner {
public:
  queue_bench_runner(abstract_db *db, bool write_only)
    : bench_runner(db), write_only(write_only)
  {
    open_tables["table"] = db->open_index("table");
  }

protected:
  virtual vector<bench_loader *>
  make_loaders()
  {
    vector<bench_loader *> ret;
    ret.push_back(new queue_table_loader(0, db, open_tables));
    return ret;
  }

  virtual vector<bench_worker *>
  make_workers()
  {
    fast_random r(8544290);
    vector<bench_worker *> ret;
    if (write_only) {
      for (size_t i = 0; i < nthreads; i++)
        ret.push_back(
          new queue_worker(
            r.next(), db, open_tables,
            &barrier_a, &barrier_b, i, false));
    } else {
      ALWAYS_ASSERT(nthreads >= 2);
      if (verbose && (nthreads % 2))
        cerr << "queue_bench_runner: odd number of workers given" << endl;
      for (size_t i = 0; i < nthreads / 2; i++) {
        ret.push_back(
          new queue_worker(
            r.next(), db, open_tables,
            &barrier_a, &barrier_b, i, true));
        ret.push_back(
          new queue_worker(
            r.next(), db, open_tables,
            &barrier_a, &barrier_b, i, false));
      }
    }
    return ret;
  }

private:
  bool write_only;
};

void
queue_do_test(abstract_db *db)
{
  nkeys = size_t(scale_factor * 1000.0);
  ALWAYS_ASSERT(nkeys > 0);
  queue_bench_runner r(db, true);
  r.run();
}
