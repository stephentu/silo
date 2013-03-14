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
static const size_t YCSBRecordSize = 1000;

class ycsb_worker : public bench_worker {
public:
  ycsb_worker(unsigned long seed, abstract_db *db,
              const map<string, abstract_ordered_index *> &open_tables,
              spin_barrier *barrier_a, spin_barrier *barrier_b)
    : bench_worker(seed, db, open_tables, barrier_a, barrier_b),
      tbl(open_tables.at("USERTABLE")),
      obj_put_strs_n(0),
      computation_n(0)
  {
  }

  ssize_t
  txn_read()
  {
    void * const txn = db->new_txn(txn_flags, txn_buf());
    try {
      ALWAYS_ASSERT(tbl->get(txn, u64_varkey(r.next() % nkeys).str(obj_key0), obj_v));
      computation_n += obj_v.size();
      if (db->commit_txn(txn))
        ntxn_commits++;
    } catch (abstract_db::abstract_abort_exception &ex) {
      db->abort_txn(txn);
      ntxn_aborts++;
    }
    return 0;
  }

  static ssize_t
  TxnRead(bench_worker *w)
  {
    return static_cast<ycsb_worker *>(w)->txn_read();
  }

  ssize_t
  txn_write()
  {
    void * const txn = db->new_txn(txn_flags, txn_buf());
    try {
      tbl->put(txn, u64_varkey(r.next() % nkeys).str(str()), str().assign(YCSBRecordSize, 'b'));
      if (db->commit_txn(txn))
        ntxn_commits++;
    } catch (abstract_db::abstract_abort_exception &ex) {
      db->abort_txn(txn);
      ntxn_aborts++;
    }
    return 0; // key already exists
  }

  static ssize_t
  TxnWrite(bench_worker *w)
  {
    return static_cast<ycsb_worker *>(w)->txn_write();
  }

  ssize_t
  txn_rmw()
  {
    void * const txn = db->new_txn(txn_flags, txn_buf());
    try {
      ALWAYS_ASSERT(tbl->get(txn, u64_varkey(r.next() % nkeys).str(obj_key0), obj_v));
      computation_n += obj_v.size();
      tbl->put(txn, u64_varkey(r.next() % nkeys).str(str()), str().assign(YCSBRecordSize, 'c'));
      if (db->commit_txn(txn))
        ntxn_commits++;
    } catch (abstract_db::abstract_abort_exception &ex) {
      db->abort_txn(txn);
      ntxn_aborts++;
    }
    return 0; // key already exists
  }

  static ssize_t
  TxnRmw(bench_worker *w)
  {
    return static_cast<ycsb_worker *>(w)->txn_rmw();
  }

  class worker_scan_callback : public abstract_ordered_index::scan_callback {
  public:
    worker_scan_callback() : n(0) {}
    virtual bool
    invoke(const char *key, size_t key_len,
           const char *value, size_t value_len)
    {
      n += value_len;
      return true;
    }
    size_t n;
  };

  ssize_t
  txn_scan()
  {
    void * const txn = db->new_txn(txn_flags, txn_buf());
    const size_t kstart = r.next() % nkeys;
    const string &kbegin = u64_varkey(kstart).str(obj_key0);
    const string &kend = u64_varkey(kstart + 100).str(obj_key1);
    worker_scan_callback c;
    try {
      tbl->scan(txn, kbegin, &kend, c);
      computation_n += c.n;
      if (db->commit_txn(txn))
        ntxn_commits++;
    } catch (abstract_db::abstract_abort_exception &ex) {
      db->abort_txn(txn);
      ntxn_aborts++;
    }
    return 0;
  }

  static ssize_t
  TxnScan(bench_worker *w)
  {
    return static_cast<ycsb_worker *>(w)->txn_scan();
  }

  virtual workload_desc_vec
  get_workload() const
  {
    workload_desc_vec w;
    //w.push_back(workload_desc("Read", 0.95, TxnRead));
    //w.push_back(workload_desc("ReadModifyWrite", 0.04, TxnRmw));
    //w.push_back(workload_desc("Write", 0.01, TxnWrite));

    w.push_back(workload_desc("Read", 1.0, TxnRead));

    // YCSB workload "A" - 50/50 read/write
    //w.push_back(workload_desc("Read", 0.5, TxnRead));
    //w.push_back(workload_desc("Write", 0.5, TxnWrite));
    return w;
  }

protected:

  inline ALWAYS_INLINE string &
  str() {
    return obj_put_strs[obj_put_strs_n++ % ARRAY_NELEMS(obj_put_strs)];
  }

private:
  abstract_ordered_index *tbl;

  string obj_key0;
  string obj_key1;
  string obj_v;

  string obj_put_strs[32];
  unsigned obj_put_strs_n;

  uint64_t computation_n;
};

class ycsb_usertable_loader : public bench_loader {
public:
  ycsb_usertable_loader(unsigned long seed,
                        abstract_db *db,
                        const map<string, abstract_ordered_index *> &open_tables)
    : bench_loader(seed, db, open_tables)
  {}

protected:
  virtual void
  load()
  {
    abstract_ordered_index *tbl = open_tables.at("USERTABLE");
    try {
      // load
      const size_t batchsize = (db->txn_max_batch_size() == -1) ?
        10000 : db->txn_max_batch_size();
      ALWAYS_ASSERT(batchsize > 0);
      const size_t nbatches = nkeys / batchsize;
      if (nbatches == 0) {
        void *txn = db->new_txn(txn_flags, txn_buf());
        for (size_t j = 0; j < nkeys; j++) {
          string k = u64_varkey(j).str();
          string v(YCSBRecordSize, 'a');
          tbl->insert(txn, move(k), move(v));
        }
        if (verbose)
          cerr << "batch 1/1 done" << endl;
        ALWAYS_ASSERT(db->commit_txn(txn));
      } else {
        for (size_t i = 0; i < nbatches; i++) {
          const size_t keyend = (i == nbatches - 1) ? nkeys : (i + 1) * batchsize;
          void *txn = db->new_txn(txn_flags, txn_buf());
          for (size_t j = i * batchsize; j < keyend; j++) {
            string k = u64_varkey(j).str();
            string v(YCSBRecordSize, 'a');
            tbl->insert(txn, move(k), move(v));
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

class ycsb_parallel_usertable_loader : public bench_loader {
public:
  ycsb_parallel_usertable_loader(unsigned long seed,
                                 abstract_db *db,
                                 const map<string, abstract_ordered_index *> &open_tables,
                                 uint64_t keystart,
                                 uint64_t keyend)
    : bench_loader(seed, db, open_tables),
      keystart(keystart), keyend(keyend)
  {
    INVARIANT(keyend > keystart);
  }

protected:
  virtual void
  load()
  {
    abstract_ordered_index *tbl = open_tables.at("USERTABLE");
    const size_t batchsize = (db->txn_max_batch_size() == -1) ?
      10000 : db->txn_max_batch_size();
    ALWAYS_ASSERT(batchsize > 0);
    const size_t nkeys = keyend - keystart;
    const size_t nbatches = nkeys < batchsize ? 1 : (nkeys / batchsize);
    for (size_t batchid = 0; batchid < nbatches;) {
      void * const txn = db->new_txn(txn_flags, txn_buf());
      try {
        for (size_t i = batchid * batchsize + keystart;
             i < min((batchid + 1) * batchsize + keystart, keyend); i++) {
          const string k = u64_varkey(i).str();
          const string v(YCSBRecordSize, 'a');
          tbl->insert(txn, move(k), move(v));
        }
        if (db->commit_txn(txn))
          batchid++;
        else
          db->abort_txn(txn);
      } catch (abstract_db::abstract_abort_exception &ex) {
        db->abort_txn(txn);
      }
    }
    if (verbose)
      cerr << "[INFO] finished loading USERTABLE range [kstart="
           << keystart << ", kend=" << keyend << ")" << endl;
  }

private:
  uint64_t keystart;
  uint64_t keyend;

};


class ycsb_bench_runner : public bench_runner {
public:
  ycsb_bench_runner(abstract_db *db)
    : bench_runner(db)
  {
    open_tables["USERTABLE"] = db->open_index("USERTABLE", YCSBRecordSize);
  }

protected:
  virtual vector<bench_loader *>
  make_loaders()
  {
    vector<bench_loader *> ret;
    if (enable_parallel_loading && nkeys >= nthreads) {
      const size_t nkeysperthd = nkeys / nthreads;
      for (size_t i = 0; i < nthreads; i++)
        ret.push_back(new ycsb_parallel_usertable_loader(0, db, open_tables, i * nkeysperthd, min((i + 1) * nkeysperthd, nkeys)));
    } else {
      ret.push_back(new ycsb_usertable_loader(0, db, open_tables));
    }
    return ret;
  }

  virtual vector<bench_worker *>
  make_workers()
  {
    fast_random r(8544290);
    vector<bench_worker *> ret;
    for (size_t i = 0; i < nthreads; i++)
      ret.push_back(
        new ycsb_worker(
          r.next(), db, open_tables,
          &barrier_a, &barrier_b));
    return ret;
  }
};

void
ycsb_do_test(abstract_db *db)
{
  nkeys = size_t(scale_factor * 1000.0);
  ALWAYS_ASSERT(nkeys > 0);
  ycsb_bench_runner r(db);
  r.run();
}
