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

#include "../record/encoder.h"
#include "bench.h"

using namespace std;
using namespace util;

static size_t nkeys;

#define ENCSTRESS_REC_KEY_FIELDS(x, y) \
  x(int32_t,k0)
#define ENCSTRESS_REC_VALUE_FIELDS(x, y) \
  x(int32_t,f0) \
  y(int32_t,f1) \
  y(int32_t,f2) \
  y(int32_t,f3) \
  y(int32_t,f4) \
  y(int32_t,f5) \
  y(int32_t,f6) \
  y(int32_t,f7)
DO_STRUCT(encstress_rec, ENCSTRESS_REC_KEY_FIELDS, ENCSTRESS_REC_VALUE_FIELDS)

class encstress_worker : public bench_worker {
public:
  encstress_worker(
      unsigned int worker_id,
      unsigned long seed, abstract_db *db,
      const map<string, abstract_ordered_index *> &open_tables,
      spin_barrier *barrier_a, spin_barrier *barrier_b)
    : bench_worker(worker_id, false, seed, db,
                   open_tables, barrier_a, barrier_b),
      tbl(open_tables.at("table"))
  {
  }

  txn_result
  txn_read()
  {
    void *txn = db->new_txn(txn_flags, arena, txn_buf());
    const string k = u64_varkey(r.next() % nkeys).str();
    try {
      string v;
      ALWAYS_ASSERT(tbl->get(txn, k, v));
      if (likely(db->commit_txn(txn)))
        return txn_result(true, 0);
    } catch (abstract_db::abstract_abort_exception &ex) {
      db->abort_txn(txn);
    }
    return txn_result(false, 0);
  }

  static txn_result
  TxnRead(bench_worker *w)
  {
    return static_cast<encstress_worker *>(w)->txn_read();
  }

  virtual workload_desc_vec
  get_workload() const
  {
    workload_desc_vec w;
    w.push_back(workload_desc("Read", 1.0, TxnRead));
    return w;
  }

private:
  abstract_ordered_index *tbl;
};

class encstress_loader : public bench_loader {
public:
  encstress_loader(unsigned long seed,
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
      if (nbatches == 0) {
        void *txn = db->new_txn(txn_flags, arena, txn_buf());
        for (size_t j = 0; j < nkeys; j++) {
          const encstress_rec::key key(j);
          encstress_rec::value rec;
          rec.f0 = 1; rec.f1 = 1; rec.f2 = 1; rec.f3 = 1;
          rec.f4 = 1; rec.f5 = 1; rec.f6 = 1; rec.f7 = 1;
          string buf;
          tbl->insert(txn, Encode(key), Encode(buf, rec));
        }
        if (verbose)
          cerr << "batch 1/1 done" << endl;
        ALWAYS_ASSERT(db->commit_txn(txn));
      } else {
        for (size_t i = 0; i < nbatches; i++) {
          size_t keyend = (i == nbatches - 1) ? nkeys : (i + 1) * batchsize;
          void *txn = db->new_txn(txn_flags, arena, txn_buf());
          for (size_t j = i * batchsize; j < keyend; j++) {
            const encstress_rec::key key(j);
            encstress_rec::value rec;
            rec.f0 = 1; rec.f1 = 1; rec.f2 = 1; rec.f3 = 1;
            rec.f4 = 1; rec.f5 = 1; rec.f6 = 1; rec.f7 = 1;
            string buf;
            tbl->insert(txn, Encode(key), Encode(buf, rec));
          }
          if (verbose)
            cerr << "batch " << (i + 1) << "/" << nbatches << " done" << endl;
          ALWAYS_ASSERT(db->commit_txn(txn));
        }
      }
    } catch (abstract_db::abstract_abort_exception &ex) {
      // shouldn't abort on loading!
      ALWAYS_ASSERT(false);
    }
    if (verbose)
      cerr << "[INFO] finished loading USERTABLE" << endl;
  }
};

class encstress_bench_runner : public bench_runner {
public:
  encstress_bench_runner(abstract_db *db)
    : bench_runner(db)
  {
    open_tables["table"] = db->open_index("table", sizeof(encstress_rec));
  }

protected:
  virtual vector<bench_loader *>
  make_loaders()
  {
    vector<bench_loader *> ret;
    ret.push_back(new encstress_loader(0, db, open_tables));
    return ret;
  }

  virtual vector<bench_worker *>
  make_workers()
  {
    fast_random r(8544290);
    vector<bench_worker *> ret;
    for (size_t i = 0; i < nthreads; i++)
      ret.push_back(
        new encstress_worker(
          i, r.next(), db, open_tables,
          &barrier_a, &barrier_b));
    return ret;
  }
};

void
encstress_do_test(abstract_db *db, int argc, char **argv)
{
  nkeys = size_t(scale_factor * 1000.0);
  ALWAYS_ASSERT(nkeys > 0);
  encstress_bench_runner r(db);
  r.run();
}
