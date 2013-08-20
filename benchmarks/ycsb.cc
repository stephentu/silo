#include <iostream>
#include <sstream>
#include <vector>
#include <utility>
#include <string>

#include <stdlib.h>
#include <unistd.h>
#include <getopt.h>
#include <numa.h>

#include "../macros.h"
#include "../varkey.h"
#include "../thread.h"
#include "../util.h"
#include "../spinbarrier.h"
#include "../core.h"

#include "bench.h"

using namespace std;
using namespace util;

static size_t nkeys;
static const size_t YCSBRecordSize = 100;

// [R, W, RMW, Scan]
// we're missing remove for now
// the default is a modification of YCSB "A" we made (80/20 R/W)
static unsigned g_txn_workload_mix[] = { 80, 20, 0, 0 };

class ycsb_worker : public bench_worker {
public:
  ycsb_worker(unsigned int worker_id,
              unsigned long seed, abstract_db *db,
              const map<string, abstract_ordered_index *> &open_tables,
              spin_barrier *barrier_a, spin_barrier *barrier_b)
    : bench_worker(worker_id, true, seed, db,
                   open_tables, barrier_a, barrier_b),
      tbl(open_tables.at("USERTABLE")),
      computation_n(0)
  {
  }

  txn_result
  txn_read()
  {
    void * const txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_KV_GET_PUT);
    scoped_str_arena s_arena(arena);
    try {
      ALWAYS_ASSERT(tbl->get(txn, u64_varkey(r.next() % nkeys).str(obj_key0), obj_v));
      computation_n += obj_v.size();
      measure_txn_counters(txn, "txn_read");
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
    return static_cast<ycsb_worker *>(w)->txn_read();
  }

  txn_result
  txn_write()
  {
    void * const txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_KV_GET_PUT);
    scoped_str_arena s_arena(arena);
    try {
      tbl->put(txn, u64_varkey(r.next() % nkeys).str(str()), str().assign(YCSBRecordSize, 'b'));
      measure_txn_counters(txn, "txn_write");
      if (likely(db->commit_txn(txn)))
        return txn_result(true, 0);
    } catch (abstract_db::abstract_abort_exception &ex) {
      db->abort_txn(txn);
    }
    return txn_result(false, 0);
  }

  static txn_result
  TxnWrite(bench_worker *w)
  {
    return static_cast<ycsb_worker *>(w)->txn_write();
  }

  txn_result
  txn_rmw()
  {
    void * const txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_KV_RMW);
    scoped_str_arena s_arena(arena);
    try {
      const uint64_t key = r.next() % nkeys;
      ALWAYS_ASSERT(tbl->get(txn, u64_varkey(key).str(obj_key0), obj_v));
      computation_n += obj_v.size();
      tbl->put(txn, obj_key0, str().assign(YCSBRecordSize, 'c'));
      measure_txn_counters(txn, "txn_rmw");
      if (likely(db->commit_txn(txn)))
        return txn_result(true, 0);
    } catch (abstract_db::abstract_abort_exception &ex) {
      db->abort_txn(txn);
    }
    return txn_result(false, 0);
  }

  static txn_result
  TxnRmw(bench_worker *w)
  {
    return static_cast<ycsb_worker *>(w)->txn_rmw();
  }

  class worker_scan_callback : public abstract_ordered_index::scan_callback {
  public:
    worker_scan_callback() : n(0) {}
    virtual bool
    invoke(const string &key, const string &value)
    {
      n += value.size();
      return true;
    }
    size_t n;
  };

  txn_result
  txn_scan()
  {
    void * const txn = db->new_txn(txn_flags, arena, txn_buf(), abstract_db::HINT_KV_SCAN);
    scoped_str_arena s_arena(arena);
    const size_t kstart = r.next() % nkeys;
    const string &kbegin = u64_varkey(kstart).str(obj_key0);
    const string &kend = u64_varkey(kstart + 100).str(obj_key1);
    worker_scan_callback c;
    try {
      tbl->scan(txn, kbegin, &kend, c);
      computation_n += c.n;
      measure_txn_counters(txn, "txn_scan");
      if (likely(db->commit_txn(txn)))
        return txn_result(true, 0);
    } catch (abstract_db::abstract_abort_exception &ex) {
      db->abort_txn(txn);
    }
    return txn_result(false, 0);
  }

  static txn_result
  TxnScan(bench_worker *w)
  {
    return static_cast<ycsb_worker *>(w)->txn_scan();
  }

  virtual workload_desc_vec
  get_workload() const
  {
    //w.push_back(workload_desc("Read", 0.95, TxnRead));
    //w.push_back(workload_desc("ReadModifyWrite", 0.04, TxnRmw));
    //w.push_back(workload_desc("Write", 0.01, TxnWrite));

    //w.push_back(workload_desc("Read", 1.0, TxnRead));
    //w.push_back(workload_desc("Write", 1.0, TxnWrite));

    // YCSB workload "A" - 50/50 read/write
    //w.push_back(workload_desc("Read", 0.5, TxnRead));
    //w.push_back(workload_desc("Write", 0.5, TxnWrite));

    // YCSB workload custom - 80/20 read/write
    //w.push_back(workload_desc("Read",  0.8, TxnRead));
    //w.push_back(workload_desc("Write", 0.2, TxnWrite));

    workload_desc_vec w;
    unsigned m = 0;
    for (size_t i = 0; i < ARRAY_NELEMS(g_txn_workload_mix); i++)
      m += g_txn_workload_mix[i];
    ALWAYS_ASSERT(m == 100);
    if (g_txn_workload_mix[0])
      w.push_back(workload_desc("Read",  double(g_txn_workload_mix[0])/100.0, TxnRead));
    if (g_txn_workload_mix[1])
      w.push_back(workload_desc("Write",  double(g_txn_workload_mix[1])/100.0, TxnWrite));
    if (g_txn_workload_mix[2])
      w.push_back(workload_desc("ReadModifyWrite",  double(g_txn_workload_mix[2])/100.0, TxnRmw));
    if (g_txn_workload_mix[3])
      w.push_back(workload_desc("Scan",  double(g_txn_workload_mix[3])/100.0, TxnScan));
    return w;
  }

protected:

  virtual void
  on_run_setup() OVERRIDE
  {
    if (!pin_cpus)
      return;
    rcu::s_instance.pin_current_thread(worker_id % MaxCpuForPinning());
  }

  inline ALWAYS_INLINE string &
  str() {
    return *arena.next();
  }

private:
  abstract_ordered_index *tbl;

  string obj_key0;
  string obj_key1;
  string obj_v;

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
        scoped_str_arena s_arena(arena);
        void *txn = db->new_txn(txn_flags, arena, txn_buf());
        for (size_t j = 0; j < nkeys; j++) {
          string k = u64_varkey(j).str();
          string v(YCSBRecordSize, 'a');
          tbl->insert(txn, k, v);
        }
        ALWAYS_ASSERT(db->commit_txn(txn));
        if (verbose)
          cerr << "batch 1/1 done" << endl;
      } else {
        for (size_t i = 0; i < nbatches; i++) {
          scoped_str_arena s_arena(arena);
          const size_t keyend = (i == nbatches - 1) ? nkeys : (i + 1) * batchsize;
          void *txn = db->new_txn(txn_flags, arena, txn_buf());
          for (size_t j = i * batchsize; j < keyend; j++) {
            string k = u64_varkey(j).str();
            string v(YCSBRecordSize, 'a');
            tbl->insert(txn, k, v);
          }
          ALWAYS_ASSERT(db->commit_txn(txn));
          if (verbose && !((i + 1) % 1000))
            cerr << "batch " << (i + 1) << "/" << nbatches << " done" << endl;
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
                                 unsigned int id,
                                 uint64_t keystart,
                                 uint64_t keyend)
    : bench_loader(seed, db, open_tables),
      id(id), keystart(keystart), keyend(keyend)
  {
    INVARIANT(keyend > keystart);
  }

protected:
  virtual void
  load()
  {
    if (pin_cpus) {
      rcu::s_instance.pin_current_thread(id % MaxCpuForPinning());
      rcu::s_instance.fault_region();
      ALWAYS_ASSERT(!numa_run_on_node(-1)); // XXX: HACK
      ALWAYS_ASSERT(!sched_yield());
    }

    abstract_ordered_index *tbl = open_tables.at("USERTABLE");
    const size_t batchsize = (db->txn_max_batch_size() == -1) ?
      10000 : db->txn_max_batch_size();
    ALWAYS_ASSERT(batchsize > 0);
    const size_t nkeys = keyend - keystart;
    ALWAYS_ASSERT(nkeys > 0);
    const size_t nbatches = nkeys < batchsize ? 1 : (nkeys / batchsize);
    for (size_t batchid = 0; batchid < nbatches;) {
      scoped_str_arena s_arena(arena);
      void * const txn = db->new_txn(txn_flags, arena, txn_buf());
      try {
        const size_t rend = (batchid + 1 == nbatches) ?
          keyend : keystart + ((batchid + 1) * batchsize);
        for (size_t i = batchid * batchsize + keystart; i < rend; i++) {
          ALWAYS_ASSERT(i >= keystart && i < keyend);
          const string k = u64_varkey(i).str();
          const string v(YCSBRecordSize, 'a');
          tbl->insert(txn, k, v);
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
           << keystart << ", kend=" << keyend << ") - nkeys: " << nkeys << endl;
  }

private:
  unsigned int id;
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
    const unsigned long ncpus = coreid::num_cpus_online();
    if (enable_parallel_loading && nkeys >= ncpus) {
      if (verbose)
        cerr << "[INFO] parallel loading with ncpus=" << ncpus << endl;
      const size_t nkeysperthd = nkeys / ncpus;
      for (size_t i = 0; i < ncpus; i++)
        ret.push_back(new ycsb_parallel_usertable_loader(0, db, open_tables, i, i * nkeysperthd, min((i + 1) * nkeysperthd, nkeys)));
    } else {
      ret.push_back(new ycsb_usertable_loader(0, db, open_tables));
    }
    return ret;
  }

  virtual vector<bench_worker *>
  make_workers()
  {
    const unsigned alignment = coreid::num_cpus_online();
    const int blockstart =
      coreid::allocate_contiguous_aligned_block(nthreads, alignment);
    ALWAYS_ASSERT(blockstart >= 0);
    ALWAYS_ASSERT((blockstart % alignment) == 0);
    fast_random r(8544290);
    vector<bench_worker *> ret;
    for (size_t i = 0; i < nthreads; i++)
      ret.push_back(
        new ycsb_worker(
          blockstart + i, r.next(), db, open_tables,
          &barrier_a, &barrier_b));
    return ret;
  }
};

void
ycsb_do_test(abstract_db *db, int argc, char **argv)
{
  nkeys = size_t(scale_factor * 1000.0);
  ALWAYS_ASSERT(nkeys > 0);

  // parse options
  optind = 1;
  while (1) {
    static struct option long_options[] = {
      {"workload-mix" , required_argument , 0 , 'w'},
      {0, 0, 0, 0}
    };
    int option_index = 0;
    int c = getopt_long(argc, argv, "w:", long_options, &option_index);
    if (c == -1)
      break;
    switch (c) {
    case 0:
      if (long_options[option_index].flag != 0)
        break;
      abort();
      break;

    case 'w':
      {
        const vector<string> toks = split(optarg, ',');
        ALWAYS_ASSERT(toks.size() == ARRAY_NELEMS(g_txn_workload_mix));
        unsigned s = 0;
        for (size_t i = 0; i < toks.size(); i++) {
          unsigned p = strtoul(toks[i].c_str(), nullptr, 10);
          ALWAYS_ASSERT(p >= 0 && p <= 100);
          s += p;
          g_txn_workload_mix[i] = p;
        }
        ALWAYS_ASSERT(s == 100);
      }
      break;

    case '?':
      /* getopt_long already printed an error message. */
      exit(1);

    default:
      abort();
    }
  }

  if (verbose) {
    cerr << "ycsb settings:" << endl;
    cerr << "  workload_mix: "
         << format_list(g_txn_workload_mix, g_txn_workload_mix + ARRAY_NELEMS(g_txn_workload_mix))
         << endl;
  }

  ycsb_bench_runner r(db);
  r.run();
}
