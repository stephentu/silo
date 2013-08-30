/**
 * An implementation of TPC-C based off of:
 * https://github.com/oltpbenchmark/oltpbench/tree/master/src/com/oltpbenchmark/benchmarks/tpcc
 */

#include <sys/time.h>
#include <string>
#include <ctype.h>
#include <stdlib.h>
#include <malloc.h>

#include <stdlib.h>
#include <unistd.h>
#include <getopt.h>

#include <set>
#include <vector>

#include "../txn.h"
#include "../macros.h"
#include "../scopedperf.hh"
#include "../spinlock.h"

#include "bench.h"
#include "tpcc.h"

#include "ndb_database.h"
#include "kvdb_database.h"

using namespace std;
using namespace util;

static inline ALWAYS_INLINE size_t
NumWarehouses()
{
  return (size_t) scale_factor;
}

// config constants

static constexpr inline ALWAYS_INLINE size_t
NumItems()
{
  return 100000;
}

static constexpr inline ALWAYS_INLINE size_t
NumDistrictsPerWarehouse()
{
  return 10;
}

static constexpr inline ALWAYS_INLINE size_t
NumCustomersPerDistrict()
{
  return 3000;
}

// T must implement lock()/unlock(). Both must *not* throw exceptions
template <typename T>
class scoped_multilock {
public:
  inline scoped_multilock()
    : did_lock(false)
  {
  }

  inline ~scoped_multilock()
  {
    if (did_lock)
      for (auto &t : locks)
        t->unlock();
  }

  inline void
  enq(T &t)
  {
    ALWAYS_ASSERT(!did_lock);
    locks.emplace_back(&t);
  }

  inline void
  multilock()
  {
    ALWAYS_ASSERT(!did_lock);
    if (locks.size() > 1)
      sort(locks.begin(), locks.end());
#ifdef CHECK_INVARIANTS
    if (set<T *>(locks.begin(), locks.end()).size() != locks.size()) {
      for (auto &t : locks)
        cerr << "lock: " << hexify(t) << endl;
      INVARIANT(false && "duplicate locks found");
    }
#endif
    for (auto &t : locks)
      t->lock();
    did_lock = true;
  }

private:
  bool did_lock;
  typename util::vec<T *, 64>::type locks;
};

// like a lock_guard, but has the option of not acquiring
template <typename T>
class scoped_lock_guard {
public:
  inline scoped_lock_guard(T &l)
    : l(&l)
  {
    this->l->lock();
  }

  inline scoped_lock_guard(T *l)
    : l(l)
  {
    if (this->l)
      this->l->lock();
  }

  inline ~scoped_lock_guard()
  {
    if (l)
      l->unlock();
  }

private:
  T *l;
};

// configuration flags
static int g_disable_xpartition_txn = 0;
static int g_disable_read_only_scans = 0;
static int g_enable_partition_locks = 0;
static int g_enable_separate_tree_per_partition = 0;
static int g_new_order_remote_item_pct = 1;
static int g_new_order_fast_id_gen = 0;
static int g_uniform_item_dist = 0;
static unsigned g_txn_workload_mix[] = { 45, 43, 4, 4, 4 }; // default TPC-C workload mix

static aligned_padded_elem<spinlock> *g_partition_locks = nullptr;
static aligned_padded_elem<atomic<uint64_t>> *g_district_ids = nullptr;

// maps a wid => partition id
static inline ALWAYS_INLINE unsigned int
PartitionId(unsigned int wid)
{
  INVARIANT(wid >= 1 && wid <= NumWarehouses());
  wid -= 1; // 0-idx
  if (NumWarehouses() <= nthreads)
    // more workers than partitions, so its easy
    return wid;
  const unsigned nwhse_per_partition = NumWarehouses() / nthreads;
  const unsigned partid = wid / nwhse_per_partition;
  if (partid >= nthreads)
    return nthreads - 1;
  return partid;
}

static inline ALWAYS_INLINE spinlock &
LockForPartition(unsigned int wid)
{
  INVARIANT(g_enable_partition_locks);
  return g_partition_locks[PartitionId(wid)].elem;
}

static inline atomic<uint64_t> &
NewOrderIdHolder(unsigned warehouse, unsigned district)
{
  INVARIANT(warehouse >= 1 && warehouse <= NumWarehouses());
  INVARIANT(district >= 1 && district <= NumDistrictsPerWarehouse());
  const unsigned idx =
    (warehouse - 1) * NumDistrictsPerWarehouse() + (district - 1);
  return g_district_ids[idx].elem;
}

static inline uint64_t
FastNewOrderIdGen(unsigned warehouse, unsigned district)
{
  return NewOrderIdHolder(warehouse, district).fetch_add(1, memory_order_acq_rel);
}

struct checker {
  // these sanity checks are just a few simple checks to make sure
  // the data is not entirely corrupted

  static inline ALWAYS_INLINE void
  SanityCheckCustomer(const customer::key *k, const customer::value *v)
  {
    INVARIANT(k->c_w_id >= 1 && static_cast<size_t>(k->c_w_id) <= NumWarehouses());
    INVARIANT(k->c_d_id >= 1 && static_cast<size_t>(k->c_d_id) <= NumDistrictsPerWarehouse());
    INVARIANT(k->c_id >= 1 && static_cast<size_t>(k->c_id) <= NumCustomersPerDistrict());
#ifdef DISABLE_FIELD_SELECTION
    INVARIANT(v->c_credit == "BC" || v->c_credit == "GC");
    INVARIANT(v->c_middle == "OE");
#endif
  }

  static inline ALWAYS_INLINE void
  SanityCheckWarehouse(const warehouse::key *k, const warehouse::value *v)
  {
    INVARIANT(k->w_id >= 1 && static_cast<size_t>(k->w_id) <= NumWarehouses());
#ifdef DISABLE_FIELD_SELECTION
    INVARIANT(v->w_state.size() == 2);
    INVARIANT(v->w_zip == "123456789");
#endif
  }

  static inline ALWAYS_INLINE void
  SanityCheckDistrict(const district::key *k, const district::value *v)
  {
    INVARIANT(k->d_w_id >= 1 && static_cast<size_t>(k->d_w_id) <= NumWarehouses());
    INVARIANT(k->d_id >= 1 && static_cast<size_t>(k->d_id) <= NumDistrictsPerWarehouse());
#ifdef DISABLE_FIELD_SELECTION
    INVARIANT(v->d_next_o_id >= 3001);
    INVARIANT(v->d_state.size() == 2);
    INVARIANT(v->d_zip == "123456789");
#endif
  }

  static inline ALWAYS_INLINE void
  SanityCheckItem(const item::key *k, const item::value *v)
  {
    INVARIANT(k->i_id >= 1 && static_cast<size_t>(k->i_id) <= NumItems());
#ifdef DISABLE_FIELD_SELECTION
    INVARIANT(v->i_price >= 1.0 && v->i_price <= 100.0);
#endif
  }

  static inline ALWAYS_INLINE void
  SanityCheckStock(const stock::key *k, const stock::value *v)
  {
    INVARIANT(k->s_w_id >= 1 && static_cast<size_t>(k->s_w_id) <= NumWarehouses());
    INVARIANT(k->s_i_id >= 1 && static_cast<size_t>(k->s_i_id) <= NumItems());
  }

  static inline ALWAYS_INLINE void
  SanityCheckNewOrder(const new_order::key *k, const new_order::value *v)
  {
    INVARIANT(k->no_w_id >= 1 && static_cast<size_t>(k->no_w_id) <= NumWarehouses());
    INVARIANT(k->no_d_id >= 1 && static_cast<size_t>(k->no_d_id) <= NumDistrictsPerWarehouse());
  }

  static inline ALWAYS_INLINE void
  SanityCheckOOrder(const oorder::key *k, const oorder::value *v)
  {
    INVARIANT(k->o_w_id >= 1 && static_cast<size_t>(k->o_w_id) <= NumWarehouses());
    INVARIANT(k->o_d_id >= 1 && static_cast<size_t>(k->o_d_id) <= NumDistrictsPerWarehouse());
#ifdef DISABLE_FIELD_SELECTION
    INVARIANT(v->o_c_id >= 1 && static_cast<size_t>(v->o_c_id) <= NumCustomersPerDistrict());
    INVARIANT(v->o_carrier_id >= 0 && static_cast<size_t>(v->o_carrier_id) <= NumDistrictsPerWarehouse());
    INVARIANT(v->o_ol_cnt >= 5 && v->o_ol_cnt <= 15);
#endif
  }

  static inline ALWAYS_INLINE void
  SanityCheckOrderLine(const order_line::key *k, const order_line::value *v)
  {
    INVARIANT(k->ol_w_id >= 1 && static_cast<size_t>(k->ol_w_id) <= NumWarehouses());
    INVARIANT(k->ol_d_id >= 1 && static_cast<size_t>(k->ol_d_id) <= NumDistrictsPerWarehouse());
    INVARIANT(k->ol_number >= 1 && k->ol_number <= 15);
#ifdef DISABLE_FIELD_SELECTION
    INVARIANT(v->ol_i_id >= 1 && static_cast<size_t>(v->ol_i_id) <= NumItems());
#endif
  }
};

static string NameTokens[] =
  {
    string("BAR"),
    string("OUGHT"),
    string("ABLE"),
    string("PRI"),
    string("PRES"),
    string("ESE"),
    string("ANTI"),
    string("CALLY"),
    string("ATION"),
    string("EING"),
  };

class tpcc_worker_mixin {
public:

  // only TPCC loaders need to call this- workers are automatically
  // pinned by their worker id (which corresponds to warehouse id
  // in TPCC)
  //
  // pins the *calling* thread
  static void
  PinToWarehouseId(unsigned int wid)
  {
    const unsigned int partid = PartitionId(wid);
    ALWAYS_ASSERT(partid < nthreads);
    const unsigned int pinid  = partid;
    if (verbose)
      cerr << "PinToWarehouseId(): coreid=" << coreid::core_id()
           << " pinned to whse=" << wid << " (partid=" << partid << ")"
           << endl;
    rcu::s_instance.pin_current_thread(pinid);
    rcu::s_instance.fault_region();
  }

  static inline uint32_t
  GetCurrentTimeMillis()
  {
    //struct timeval tv;
    //ALWAYS_ASSERT(gettimeofday(&tv, 0) == 0);
    //return tv.tv_sec * 1000;

    // XXX(stephentu): implement a scalable GetCurrentTimeMillis()
    // for now, we just give each core an increasing number

    static __thread uint32_t tl_hack = 0;
    return tl_hack++;
  }

  // utils for generating random #s and strings

  static inline ALWAYS_INLINE int
  CheckBetweenInclusive(int v, int lower, int upper)
  {
    INVARIANT(v >= lower);
    INVARIANT(v <= upper);
    return v;
  }

  static inline ALWAYS_INLINE int
  RandomNumber(fast_random &r, int min, int max)
  {
    return CheckBetweenInclusive((int) (r.next_uniform() * (max - min + 1) + min), min, max);
  }

  static inline ALWAYS_INLINE int
  NonUniformRandom(fast_random &r, int A, int C, int min, int max)
  {
    return (((RandomNumber(r, 0, A) | RandomNumber(r, min, max)) + C) % (max - min + 1)) + min;
  }

  static inline ALWAYS_INLINE int
  GetItemId(fast_random &r)
  {
    return CheckBetweenInclusive(
        g_uniform_item_dist ?
          RandomNumber(r, 1, NumItems()) :
          NonUniformRandom(r, 8191, 7911, 1, NumItems()),
        1, NumItems());
  }

  static inline ALWAYS_INLINE int
  GetCustomerId(fast_random &r)
  {
    return CheckBetweenInclusive(NonUniformRandom(r, 1023, 259, 1, NumCustomersPerDistrict()), 1, NumCustomersPerDistrict());
  }

  // pick a number between [start, end)
  static inline ALWAYS_INLINE unsigned
  PickWarehouseId(fast_random &r, unsigned start, unsigned end)
  {
    INVARIANT(start < end);
    const unsigned diff = end - start;
    if (diff == 1)
      return start;
    return (r.next() % diff) + start;
  }
  // all tokens are at most 5 chars long
  static const size_t CustomerLastNameMaxSize = 5 * 3;

  static inline size_t
  GetCustomerLastName(uint8_t *buf, fast_random &r, int num)
  {
    const string &s0 = NameTokens[num / 100];
    const string &s1 = NameTokens[(num / 10) % 10];
    const string &s2 = NameTokens[num % 10];
    uint8_t *const begin = buf;
    const size_t s0_sz = s0.size();
    const size_t s1_sz = s1.size();
    const size_t s2_sz = s2.size();
    NDB_MEMCPY(buf, s0.data(), s0_sz); buf += s0_sz;
    NDB_MEMCPY(buf, s1.data(), s1_sz); buf += s1_sz;
    NDB_MEMCPY(buf, s2.data(), s2_sz); buf += s2_sz;
    return buf - begin;
  }

  static inline ALWAYS_INLINE size_t
  GetCustomerLastName(char *buf, fast_random &r, int num)
  {
    return GetCustomerLastName((uint8_t *) buf, r, num);
  }

  static inline string
  GetCustomerLastName(fast_random &r, int num)
  {
    string ret;
    ret.resize(CustomerLastNameMaxSize);
    ret.resize(GetCustomerLastName((uint8_t *) &ret[0], r, num));
    return ret;
  }

  static inline ALWAYS_INLINE string
  GetNonUniformCustomerLastNameLoad(fast_random &r)
  {
    return GetCustomerLastName(r, NonUniformRandom(r, 255, 157, 0, 999));
  }

  static inline ALWAYS_INLINE size_t
  GetNonUniformCustomerLastNameRun(uint8_t *buf, fast_random &r)
  {
    return GetCustomerLastName(buf, r, NonUniformRandom(r, 255, 223, 0, 999));
  }

  static inline ALWAYS_INLINE size_t
  GetNonUniformCustomerLastNameRun(char *buf, fast_random &r)
  {
    return GetNonUniformCustomerLastNameRun((uint8_t *) buf, r);
  }

  static inline ALWAYS_INLINE string
  GetNonUniformCustomerLastNameRun(fast_random &r)
  {
    return GetCustomerLastName(r, NonUniformRandom(r, 255, 223, 0, 999));
  }

  // following oltpbench, we really generate strings of len - 1...
  static inline string
  RandomStr(fast_random &r, uint len)
  {
    // this is a property of the oltpbench implementation...
    if (!len)
      return "";

    uint i = 0;
    string buf(len - 1, 0);
    while (i < (len - 1)) {
      const char c = (char) r.next_char();
      // XXX(stephentu): oltpbench uses java's Character.isLetter(), which
      // is a less restrictive filter than isalnum()
      if (!isalnum(c))
        continue;
      buf[i++] = c;
    }
    return buf;
  }

  // RandomNStr() actually produces a string of length len
  static inline string
  RandomNStr(fast_random &r, uint len)
  {
    const char base = '0';
    string buf(len, 0);
    for (uint i = 0; i < len; i++)
      buf[i] = (char)(base + (r.next() % 10));
    return buf;
  }

};

STATIC_COUNTER_DECL(scopedperf::tsc_ctr, tpcc_txn, tpcc_txn_cg)

template <typename Database, bool AllowReadOnlyScans>
class tpcc_worker : public bench_worker, public tpcc_worker_mixin {
public:
  tpcc_worker(unsigned int worker_id,
              unsigned long seed, Database *db,
              const tpcc_tables<Database> &tables,
              spin_barrier *barrier_a, spin_barrier *barrier_b,
              uint warehouse_id_start, uint warehouse_id_end)
    : bench_worker(worker_id, true, seed, db,
                   barrier_a, barrier_b),
      tpcc_worker_mixin(),
      tables(tables),
      warehouse_id_start(warehouse_id_start),
      warehouse_id_end(warehouse_id_end)
  {
    ALWAYS_ASSERT(g_disable_read_only_scans == !AllowReadOnlyScans);
    INVARIANT(warehouse_id_start >= 1);
    INVARIANT(warehouse_id_start <= NumWarehouses());
    INVARIANT(warehouse_id_end > warehouse_id_start);
    INVARIANT(warehouse_id_end <= (NumWarehouses() + 1));
    NDB_MEMSET(&last_no_o_ids[0], 0, sizeof(last_no_o_ids));
    if (verbose) {
      cerr << "tpcc: worker id " << worker_id
        << " => warehouses [" << warehouse_id_start
        << ", " << warehouse_id_end << ")"
        << endl;
    }
  }

  // XXX(stephentu): tune this
  static const size_t NMaxCustomerIdxScanElems = 512;

  txn_result txn_new_order();

  static txn_result
  TxnNewOrder(bench_worker *w)
  {
    ANON_REGION("TxnNewOrder:", &tpcc_txn_cg);
    return static_cast<tpcc_worker *>(w)->txn_new_order();
  }

  txn_result txn_delivery();

  static txn_result
  TxnDelivery(bench_worker *w)
  {
    ANON_REGION("TxnDelivery:", &tpcc_txn_cg);
    return static_cast<tpcc_worker *>(w)->txn_delivery();
  }

  txn_result txn_payment();

  static txn_result
  TxnPayment(bench_worker *w)
  {
    ANON_REGION("TxnPayment:", &tpcc_txn_cg);
    return static_cast<tpcc_worker *>(w)->txn_payment();
  }

  txn_result txn_order_status();

  static txn_result
  TxnOrderStatus(bench_worker *w)
  {
    ANON_REGION("TxnOrderStatus:", &tpcc_txn_cg);
    return static_cast<tpcc_worker *>(w)->txn_order_status();
  }

  txn_result txn_stock_level();

  static txn_result
  TxnStockLevel(bench_worker *w)
  {
    ANON_REGION("TxnStockLevel:", &tpcc_txn_cg);
    return static_cast<tpcc_worker *>(w)->txn_stock_level();
  }

  virtual workload_desc_vec
  get_workload() const OVERRIDE
  {
    workload_desc_vec w;
    // numbers from sigmod.csail.mit.edu:
    //w.push_back(workload_desc("NewOrder", 1.0, TxnNewOrder)); // ~10k ops/sec
    //w.push_back(workload_desc("Payment", 1.0, TxnPayment)); // ~32k ops/sec
    //w.push_back(workload_desc("Delivery", 1.0, TxnDelivery)); // ~104k ops/sec
    //w.push_back(workload_desc("OrderStatus", 1.0, TxnOrderStatus)); // ~33k ops/sec
    //w.push_back(workload_desc("StockLevel", 1.0, TxnStockLevel)); // ~2k ops/sec
    unsigned m = 0;
    for (size_t i = 0; i < ARRAY_NELEMS(g_txn_workload_mix); i++)
      m += g_txn_workload_mix[i];
    ALWAYS_ASSERT(m == 100);
    if (g_txn_workload_mix[0])
      w.push_back(workload_desc("NewOrder", double(g_txn_workload_mix[0])/100.0, TxnNewOrder));
    if (g_txn_workload_mix[1])
      w.push_back(workload_desc("Payment", double(g_txn_workload_mix[1])/100.0, TxnPayment));
    if (g_txn_workload_mix[2])
      w.push_back(workload_desc("Delivery", double(g_txn_workload_mix[2])/100.0, TxnDelivery));
    if (g_txn_workload_mix[3])
      w.push_back(workload_desc("OrderStatus", double(g_txn_workload_mix[3])/100.0, TxnOrderStatus));
    if (g_txn_workload_mix[4])
      w.push_back(workload_desc("StockLevel", double(g_txn_workload_mix[4])/100.0, TxnStockLevel));
    return w;
  }

protected:

  virtual void
  on_run_setup() OVERRIDE
  {
    if (!pin_cpus)
      return;
    const size_t a = worker_id % coreid::num_cpus_online();
    const size_t b = a % nthreads;
    rcu::s_instance.pin_current_thread(b);
    rcu::s_instance.fault_region();
  }

private:
  tpcc_tables<Database> tables;
  const uint warehouse_id_start;
  const uint warehouse_id_end;
  int32_t last_no_o_ids[10]; // XXX(stephentu): hack
};

template <typename Database>
class tpcc_warehouse_loader : public typed_bench_loader<Database>,
                              public tpcc_worker_mixin {
public:
  tpcc_warehouse_loader(unsigned long seed,
                        Database *db,
                        const tpcc_tables<Database> &tables)
    : typed_bench_loader<Database>(seed, db),
      tpcc_worker_mixin(),
      tables(tables)
  {}

protected:
  virtual void
  load()
  {
    uint64_t warehouse_total_sz = 0, n_warehouses = 0;
    try {
      vector<warehouse::value> warehouses;
      {
        scoped_str_arena s_arena(this->arena);
        typename Database::template
          TransactionType<abstract_db::HINT_DEFAULT>::type txn(txn_flags, this->arena);
        for (uint i = 1; i <= NumWarehouses(); i++) {
          const warehouse::key k(i);

          const string w_name = RandomStr(this->r, RandomNumber(this->r, 6, 10));
          const string w_street_1 = RandomStr(this->r, RandomNumber(this->r, 10, 20));
          const string w_street_2 = RandomStr(this->r, RandomNumber(this->r, 10, 20));
          const string w_city = RandomStr(this->r, RandomNumber(this->r, 10, 20));
          const string w_state = RandomStr(this->r, 3);
          const string w_zip = "123456789";

          warehouse::value v;
          v.w_ytd = 300000;
          v.w_tax = (float) RandomNumber(this->r, 0, 2000) / 10000.0;
          v.w_name.assign(w_name);
          v.w_street_1.assign(w_street_1);
          v.w_street_2.assign(w_street_2);
          v.w_city.assign(w_city);
          v.w_state.assign(w_state);
          v.w_zip.assign(w_zip);

          checker::SanityCheckWarehouse(&k, &v);
          const size_t sz = Size(v);
          warehouse_total_sz += sz;
          n_warehouses++;
          tables.tbl_warehouse(i)->insert(txn, k, v);
          warehouses.push_back(v);
        }
        ALWAYS_ASSERT(txn.commit());
      }
      {
        scoped_str_arena s_arena(this->arena);
        typename Database::template
          TransactionType<abstract_db::HINT_DEFAULT>::type txn(txn_flags, this->arena);
        for (uint i = 1; i <= NumWarehouses(); i++) {
          const warehouse::key k(i);
          warehouse::value v;
          ALWAYS_ASSERT(tables.tbl_warehouse(i)->search(txn, k, v));
          ALWAYS_ASSERT(warehouses[i - 1] == v);
          checker::SanityCheckWarehouse(&k, &v);
        }
        ALWAYS_ASSERT(txn.commit());
      }
    } catch (typename Database::abort_exception_type &e) {
      // shouldn't abort on loading!
      ALWAYS_ASSERT(false);
    }
    if (verbose) {
      cerr << "[INFO] finished loading warehouse" << endl;
      cerr << "[INFO]   * average warehouse record length: "
           << (double(warehouse_total_sz)/double(n_warehouses)) << " bytes" << endl;
    }
  }

  tpcc_tables<Database> tables;
};

template <typename Database>
class tpcc_item_loader : public typed_bench_loader<Database>,
                         public tpcc_worker_mixin {
public:
  tpcc_item_loader(unsigned long seed,
                   Database *db,
                   const tpcc_tables<Database> &tables)
    : typed_bench_loader<Database>(seed, db),
      tpcc_worker_mixin(),
      tables(tables)
  {}

protected:
  virtual void
  load()
  {
    const ssize_t bsize = this->typed_db()->txn_max_batch_size();
    auto txn = this->typed_db()->template new_txn<abstract_db::HINT_DEFAULT>(txn_flags, this->arena);
    uint64_t total_sz = 0;
    try {
      for (uint i = 1; i <= NumItems(); i++) {
        // items don't "belong" to a certain warehouse, so no pinning
        const item::key k(i);

        item::value v;
        const string i_name = RandomStr(this->r, RandomNumber(this->r, 14, 24));
        v.i_name.assign(i_name);
        v.i_price = (float) RandomNumber(this->r, 100, 10000) / 100.0;
        const int len = RandomNumber(this->r, 26, 50);
        if (RandomNumber(this->r, 1, 100) > 10) {
          const string i_data = RandomStr(this->r, len);
          v.i_data.assign(i_data);
        } else {
          const int startOriginal = RandomNumber(this->r, 2, (len - 8));
          const string i_data = RandomStr(this->r, startOriginal + 1) + "ORIGINAL" + RandomStr(this->r, len - startOriginal - 7);
          v.i_data.assign(i_data);
        }
        v.i_im_id = RandomNumber(this->r, 1, 10000);

        checker::SanityCheckItem(&k, &v);
        const size_t sz = Size(v);
        total_sz += sz;
        // XXX: replicate items table across all NUMA nodes
        tables.tbl_item(1)->insert(*txn, k, v); // this table is shared, so any partition is OK

        if (bsize != -1 && !(i % bsize)) {
          ALWAYS_ASSERT(txn->commit());
          txn = this->typed_db()->template new_txn<abstract_db::HINT_DEFAULT>(txn_flags, this->arena);
          this->arena.reset();
        }
      }
      ALWAYS_ASSERT(txn->commit());
    } catch (typename Database::abort_exception_type &e) {
      // shouldn't abort on loading!
      ALWAYS_ASSERT(false);
    }
    if (verbose) {
      cerr << "[INFO] finished loading item" << endl;
      cerr << "[INFO]   * average item record length: "
           << (double(total_sz)/double(NumItems())) << " bytes" << endl;
    }
  }

  tpcc_tables<Database> tables;
};

template <typename Database>
class tpcc_stock_loader : public typed_bench_loader<Database>,
                          public tpcc_worker_mixin {
public:
  tpcc_stock_loader(unsigned long seed,
                    Database *db,
                    const tpcc_tables<Database> &tables,
                    ssize_t warehouse_id)
    : typed_bench_loader<Database>(seed, db),
      tpcc_worker_mixin(),
      tables(tables),
      warehouse_id(warehouse_id)
  {
    ALWAYS_ASSERT(warehouse_id == -1 ||
                  (warehouse_id >= 1 &&
                   static_cast<size_t>(warehouse_id) <= NumWarehouses()));
  }

protected:
  virtual void
  load()
  {
    uint64_t stock_total_sz = 0, n_stocks = 0;
    const uint w_start = (warehouse_id == -1) ?
      1 : static_cast<uint>(warehouse_id);
    const uint w_end   = (warehouse_id == -1) ?
      NumWarehouses() : static_cast<uint>(warehouse_id);

    for (uint w = w_start; w <= w_end; w++) {
      const size_t batchsize =
        (this->typed_db()->txn_max_batch_size() == -1) ?
          NumItems() : this->typed_db()->txn_max_batch_size();
      const size_t nbatches = (batchsize > NumItems()) ? 1 : (NumItems() / batchsize);

      if (pin_cpus)
        PinToWarehouseId(w);

      for (uint b = 0; b < nbatches;) {
        scoped_str_arena s_arena(this->arena);
        auto txn = this->typed_db()->template new_txn<abstract_db::HINT_DEFAULT>(txn_flags, this->arena);
        try {
          const size_t iend = std::min((b + 1) * batchsize + 1, NumItems());
          for (uint i = (b * batchsize + 1); i <= iend; i++) {
            const stock::key k(w, i);
            const stock_data::key k_data(w, i);

            stock::value v;
            v.s_quantity = RandomNumber(this->r, 10, 100);
            v.s_ytd = 0;
            v.s_order_cnt = 0;
            v.s_remote_cnt = 0;

            stock_data::value v_data;
            const int len = RandomNumber(this->r, 26, 50);
            if (RandomNumber(this->r, 1, 100) > 10) {
              const string s_data = RandomStr(this->r, len);
              v_data.s_data.assign(s_data);
            } else {
              const int startOriginal = RandomNumber(this->r, 2, (len - 8));
              const string s_data = RandomStr(this->r, startOriginal + 1) +
                "ORIGINAL" + RandomStr(this->r, len - startOriginal - 7);
              v_data.s_data.assign(s_data);
            }
            v_data.s_dist_01.assign(RandomStr(this->r, 24));
            v_data.s_dist_02.assign(RandomStr(this->r, 24));
            v_data.s_dist_03.assign(RandomStr(this->r, 24));
            v_data.s_dist_04.assign(RandomStr(this->r, 24));
            v_data.s_dist_05.assign(RandomStr(this->r, 24));
            v_data.s_dist_06.assign(RandomStr(this->r, 24));
            v_data.s_dist_07.assign(RandomStr(this->r, 24));
            v_data.s_dist_08.assign(RandomStr(this->r, 24));
            v_data.s_dist_09.assign(RandomStr(this->r, 24));
            v_data.s_dist_10.assign(RandomStr(this->r, 24));

            checker::SanityCheckStock(&k, &v);
            const size_t sz = Size(v);
            stock_total_sz += sz;
            n_stocks++;
            tables.tbl_stock(w)->insert(*txn, k, v);
            tables.tbl_stock_data(w)->insert(*txn, k_data, v_data);
          }
          if (txn->commit()) {
            b++;
          } else {
            if (verbose)
              cerr << "[WARNING] stock loader loading abort" << endl;
          }
        } catch (typename Database::abort_exception_type &e) {
          txn->abort();
          ALWAYS_ASSERT(warehouse_id != -1);
          if (verbose)
            cerr << "[WARNING] stock loader loading abort" << endl;
        }
      }
    }

    if (verbose) {
      if (warehouse_id == -1) {
        cerr << "[INFO] finished loading stock" << endl;
        cerr << "[INFO]   * average stock record length: "
             << (double(stock_total_sz)/double(n_stocks)) << " bytes" << endl;
      } else {
        cerr << "[INFO] finished loading stock (w=" << warehouse_id << ")" << endl;
      }
    }
  }

private:
  tpcc_tables<Database> tables;
  ssize_t warehouse_id;
};

template <typename Database>
class tpcc_district_loader : public typed_bench_loader<Database>,
                             public tpcc_worker_mixin {
public:
  tpcc_district_loader(unsigned long seed,
                       Database *db,
                       const tpcc_tables<Database> &tables)
    : typed_bench_loader<Database>(seed, db),
      tpcc_worker_mixin(),
      tables(tables)
  {}

protected:
  virtual void
  load()
  {
    const ssize_t bsize = this->typed_db()->txn_max_batch_size();
    auto txn = this->typed_db()->template new_txn<abstract_db::HINT_DEFAULT>(txn_flags, this->arena);
    uint64_t district_total_sz = 0, n_districts = 0;
    try {
      uint cnt = 0;
      for (uint w = 1; w <= NumWarehouses(); w++) {
        if (pin_cpus)
          PinToWarehouseId(w);
        for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++, cnt++) {
          const district::key k(w, d);

          district::value v;
          v.d_ytd = 30000;
          v.d_tax = (float) (RandomNumber(this->r, 0, 2000) / 10000.0);
          v.d_next_o_id = 3001;
          v.d_name.assign(RandomStr(this->r, RandomNumber(this->r, 6, 10)));
          v.d_street_1.assign(RandomStr(this->r, RandomNumber(this->r, 10, 20)));
          v.d_street_2.assign(RandomStr(this->r, RandomNumber(this->r, 10, 20)));
          v.d_city.assign(RandomStr(this->r, RandomNumber(this->r, 10, 20)));
          v.d_state.assign(RandomStr(this->r, 3));
          v.d_zip.assign("123456789");

          checker::SanityCheckDistrict(&k, &v);
          const size_t sz = Size(v);
          district_total_sz += sz;
          n_districts++;
          tables.tbl_district(w)->insert(*txn, k, v);

          if (bsize != -1 && !((cnt + 1) % bsize)) {
            ALWAYS_ASSERT(txn->commit());
            txn = this->typed_db()->template new_txn<abstract_db::HINT_DEFAULT>(txn_flags, this->arena);
            this->arena.reset();
          }
        }
      }
      ALWAYS_ASSERT(txn->commit());
    } catch (typename Database::abort_exception_type &e) {
      // shouldn't abort on loading!
      ALWAYS_ASSERT(false);
    }
    if (verbose) {
      cerr << "[INFO] finished loading district" << endl;
      cerr << "[INFO]   * average district record length: "
           << (double(district_total_sz)/double(n_districts)) << " bytes" << endl;
    }
  }

  tpcc_tables<Database> tables;
};

template <typename Database>
class tpcc_customer_loader : public typed_bench_loader<Database>,
                             public tpcc_worker_mixin {
public:
  tpcc_customer_loader(unsigned long seed,
                       Database *db,
                       const tpcc_tables<Database> &tables,
                       ssize_t warehouse_id)
    : typed_bench_loader<Database>(seed, db),
      tpcc_worker_mixin(),
      tables(tables),
      warehouse_id(warehouse_id)
  {
    ALWAYS_ASSERT(warehouse_id == -1 ||
                  (warehouse_id >= 1 &&
                   static_cast<size_t>(warehouse_id) <= NumWarehouses()));
  }

protected:
  virtual void
  load()
  {
    const uint w_start = (warehouse_id == -1) ?
      1 : static_cast<uint>(warehouse_id);
    const uint w_end   = (warehouse_id == -1) ?
      NumWarehouses() : static_cast<uint>(warehouse_id);
    const size_t batchsize =
      (this->typed_db()->txn_max_batch_size() == -1) ?
        NumCustomersPerDistrict() : this->typed_db()->txn_max_batch_size();
    const size_t nbatches =
      (batchsize > NumCustomersPerDistrict()) ?
        1 : (NumCustomersPerDistrict() / batchsize);
    cerr << "num batches: " << nbatches << endl;

    uint64_t total_sz = 0;

    for (uint w = w_start; w <= w_end; w++) {
      if (pin_cpus)
        PinToWarehouseId(w);
      for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
        for (uint batch = 0; batch < nbatches;) {
          scoped_str_arena s_arena(this->arena);
          typename Database::template TransactionType<abstract_db::HINT_DEFAULT>::type txn(txn_flags, this->arena);
          const size_t cstart = batch * batchsize;
          const size_t cend = std::min((batch + 1) * batchsize, NumCustomersPerDistrict());
          try {
            for (uint cidx0 = cstart; cidx0 < cend; cidx0++) {
              const uint c = cidx0 + 1;
              const customer::key k(w, d, c);

              customer::value v;
              v.c_discount = (float) (RandomNumber(this->r, 1, 5000) / 10000.0);
              if (RandomNumber(this->r, 1, 100) <= 10)
                v.c_credit.assign("BC");
              else
                v.c_credit.assign("GC");

              if (c <= 1000)
                v.c_last.assign(GetCustomerLastName(this->r, c - 1));
              else
                v.c_last.assign(GetNonUniformCustomerLastNameLoad(this->r));

              v.c_first.assign(RandomStr(this->r, RandomNumber(this->r, 8, 16)));
              v.c_credit_lim = 50000;

              v.c_balance = -10;
              v.c_ytd_payment = 10;
              v.c_payment_cnt = 1;
              v.c_delivery_cnt = 0;

              v.c_street_1.assign(RandomStr(this->r, RandomNumber(this->r, 10, 20)));
              v.c_street_2.assign(RandomStr(this->r, RandomNumber(this->r, 10, 20)));
              v.c_city.assign(RandomStr(this->r, RandomNumber(this->r, 10, 20)));
              v.c_state.assign(RandomStr(this->r, 3));
              v.c_zip.assign(RandomNStr(this->r, 4) + "11111");
              v.c_phone.assign(RandomNStr(this->r, 16));
              v.c_since = GetCurrentTimeMillis();
              v.c_middle.assign("OE");
              v.c_data.assign(RandomStr(this->r, RandomNumber(this->r, 300, 500)));

              checker::SanityCheckCustomer(&k, &v);
              const size_t sz = Size(v);
              total_sz += sz;
              tables.tbl_customer(w)->insert(txn, k, v);

              // customer name index
              const customer_name_idx::key k_idx(k.c_w_id, k.c_d_id, v.c_last.str(true), v.c_first.str(true));
              const customer_name_idx::value v_idx(k.c_id);

              // index structure is:
              // (c_w_id, c_d_id, c_last, c_first) -> (c_id)

              tables.tbl_customer_name_idx(w)->insert(txn, k_idx, v_idx);

              history::key k_hist;
              k_hist.h_c_id = c;
              k_hist.h_c_d_id = d;
              k_hist.h_c_w_id = w;
              k_hist.h_d_id = d;
              k_hist.h_w_id = w;
              k_hist.h_date = GetCurrentTimeMillis();

              history::value v_hist;
              v_hist.h_amount = 10;
              v_hist.h_data.assign(RandomStr(this->r, RandomNumber(this->r, 10, 24)));

              tables.tbl_history(w)->insert(txn, k_hist, v_hist);
            }
            if (txn.commit()) {
              batch++;
            } else {
              txn.abort();
              if (verbose)
                cerr << "[WARNING] customer loader loading abort" << endl;
            }
          } catch (typename Database::abort_exception_type &e) {
            txn.abort();
            if (verbose)
              cerr << "[WARNING] customer loader loading abort" << endl;
          }
        }
      }
    }

    if (verbose) {
      if (warehouse_id == -1) {
        cerr << "[INFO] finished loading customer" << endl;
        cerr << "[INFO]   * average customer record length: "
             << (double(total_sz)/double(NumWarehouses()*NumDistrictsPerWarehouse()*NumCustomersPerDistrict()))
             << " bytes " << endl;
      } else {
        cerr << "[INFO] finished loading customer (w=" << warehouse_id << ")" << endl;
      }
    }
  }

private:
  tpcc_tables<Database> tables;
  ssize_t warehouse_id;
};

template <typename Database>
class tpcc_order_loader : public typed_bench_loader<Database>,
                          public tpcc_worker_mixin {
public:
  tpcc_order_loader(unsigned long seed,
                    Database *db,
                    const tpcc_tables<Database> &tables,
                    ssize_t warehouse_id)
    : typed_bench_loader<Database>(seed, db),
      tpcc_worker_mixin(),
      tables(tables),
      warehouse_id(warehouse_id)
  {
    ALWAYS_ASSERT(warehouse_id == -1 ||
                  (warehouse_id >= 1 &&
                   static_cast<size_t>(warehouse_id) <= NumWarehouses()));
  }

protected:
  virtual void
  load()
  {
    uint64_t order_line_total_sz = 0, n_order_lines = 0;
    uint64_t oorder_total_sz = 0, n_oorders = 0;
    uint64_t new_order_total_sz = 0, n_new_orders = 0;

    const uint w_start = (warehouse_id == -1) ?
      1 : static_cast<uint>(warehouse_id);
    const uint w_end   = (warehouse_id == -1) ?
      NumWarehouses() : static_cast<uint>(warehouse_id);

    for (uint w = w_start; w <= w_end; w++) {
      if (pin_cpus)
        PinToWarehouseId(w);
      for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
        set<uint> c_ids_s;
        vector<uint> c_ids;
        while (c_ids.size() != NumCustomersPerDistrict()) {
          const auto x = (this->r.next() % NumCustomersPerDistrict()) + 1;
          if (c_ids_s.count(x))
            continue;
          c_ids_s.insert(x);
          c_ids.emplace_back(x);
        }
        for (uint c = 1; c <= NumCustomersPerDistrict();) {
          scoped_str_arena s_arena(this->arena);
          typename Database::template TransactionType<abstract_db::HINT_DEFAULT>::type txn(txn_flags, this->arena);
          try {
            const oorder::key k_oo(w, d, c);

            oorder::value v_oo;
            v_oo.o_c_id = c_ids[c - 1];
            if (k_oo.o_id < 2101)
              v_oo.o_carrier_id = RandomNumber(this->r, 1, 10);
            else
              v_oo.o_carrier_id = 0;
            v_oo.o_ol_cnt = RandomNumber(this->r, 5, 15);
            v_oo.o_all_local = 1;
            v_oo.o_entry_d = GetCurrentTimeMillis();

            checker::SanityCheckOOrder(&k_oo, &v_oo);
            const size_t sz = Size(v_oo);
            oorder_total_sz += sz;
            n_oorders++;
            tables.tbl_oorder(w)->insert(txn, k_oo, v_oo);

            const oorder_c_id_idx::key k_oo_idx(k_oo.o_w_id, k_oo.o_d_id, v_oo.o_c_id, k_oo.o_id);
            const oorder_c_id_idx::value v_oo_idx(0);

            tables.tbl_oorder_c_id_idx(w)->insert(txn, k_oo_idx, v_oo_idx);

            if (c >= 2101) {
              const new_order::key k_no(w, d, c);
              const new_order::value v_no;

              checker::SanityCheckNewOrder(&k_no, &v_no);
              const size_t sz = Size(v_no);
              new_order_total_sz += sz;
              n_new_orders++;
              tables.tbl_new_order(w)->insert(txn, k_no, v_no);
            }

            for (uint l = 1; l <= uint(v_oo.o_ol_cnt); l++) {
              const order_line::key k_ol(w, d, c, l);

              order_line::value v_ol;
              v_ol.ol_i_id = RandomNumber(this->r, 1, 100000);
              if (k_ol.ol_o_id < 2101) {
                v_ol.ol_delivery_d = v_oo.o_entry_d;
                v_ol.ol_amount = 0;
              } else {
                v_ol.ol_delivery_d = 0;
                // random within [0.01 .. 9,999.99]
                v_ol.ol_amount = (float) (RandomNumber(this->r, 1, 999999) / 100.0);
              }

              v_ol.ol_supply_w_id = k_ol.ol_w_id;
              v_ol.ol_quantity = 5;
              // v_ol.ol_dist_info comes from stock_data(ol_supply_w_id, ol_o_id)
              //v_ol.ol_dist_info = RandomStr(this->r, 24);

              checker::SanityCheckOrderLine(&k_ol, &v_ol);
              const size_t sz = Size(v_ol);
              order_line_total_sz += sz;
              n_order_lines++;
              tables.tbl_order_line(w)->insert(txn, k_ol, v_ol);
            }
            if (txn.commit()) {
              c++;
            } else {
              txn.abort();
              ALWAYS_ASSERT(warehouse_id != -1);
              if (verbose)
                cerr << "[WARNING] order loader loading abort" << endl;
            }
          } catch (typename Database::abort_exception_type &e) {
            txn.abort();
            ALWAYS_ASSERT(warehouse_id != -1);
            if (verbose)
              cerr << "[WARNING] order loader loading abort" << endl;
          }
        }
      }
    }

    if (verbose) {
      if (warehouse_id == -1) {
        cerr << "[INFO] finished loading order" << endl;
        cerr << "[INFO]   * average order_line record length: "
             << (double(order_line_total_sz)/double(n_order_lines)) << " bytes" << endl;
        cerr << "[INFO]   * average oorder record length: "
             << (double(oorder_total_sz)/double(n_oorders)) << " bytes" << endl;
        cerr << "[INFO]   * average new_order record length: "
             << (double(new_order_total_sz)/double(n_new_orders)) << " bytes" << endl;
      } else {
        cerr << "[INFO] finished loading order (w=" << warehouse_id << ")" << endl;
      }
    }
  }

private:
  tpcc_tables<Database> tables;
  ssize_t warehouse_id;
};

static event_counter evt_tpcc_cross_partition_new_order_txns("tpcc_cross_partition_new_order_txns");
static event_counter evt_tpcc_cross_partition_payment_txns("tpcc_cross_partition_payment_txns");

template <typename Database, bool AllowReadOnlyScans>
typename tpcc_worker<Database, AllowReadOnlyScans>::txn_result
tpcc_worker<Database, AllowReadOnlyScans>::txn_new_order()
{
  const uint warehouse_id = PickWarehouseId(this->r, warehouse_id_start, warehouse_id_end);
  const uint districtID = RandomNumber(this->r, 1, 10);
  const uint customerID = GetCustomerId(r);
  const uint numItems = RandomNumber(this->r, 5, 15);
  uint itemIDs[15], supplierWarehouseIDs[15], orderQuantities[15];
  bool allLocal = true;
  for (uint i = 0; i < numItems; i++) {
    itemIDs[i] = GetItemId(r);
    if (likely(g_disable_xpartition_txn ||
               NumWarehouses() == 1 ||
               RandomNumber(this->r, 1, 100) > g_new_order_remote_item_pct)) {
      supplierWarehouseIDs[i] = warehouse_id;
    } else {
      do {
       supplierWarehouseIDs[i] = RandomNumber(this->r, 1, NumWarehouses());
      } while (supplierWarehouseIDs[i] == warehouse_id);
      allLocal = false;
    }
    orderQuantities[i] = RandomNumber(this->r, 1, 10);
  }
  INVARIANT(!g_disable_xpartition_txn || allLocal);
  if (!allLocal)
    ++evt_tpcc_cross_partition_new_order_txns;

  // XXX(stephentu): implement rollback
  //
  // worst case txn profile:
  //   1 customer get
  //   1 warehouse get
  //   1 district get
  //   1 new_order insert
  //   1 district put
  //   1 oorder insert
  //   1 oorder_cid_idx insert
  //   15 times:
  //      1 item get
  //      1 stock get
  //      1 stock put
  //      1 order_line insert
  //
  // output from txn counters:
  //   max_absent_range_set_size : 0
  //   max_absent_set_size : 0
  //   max_node_scan_size : 0
  //   max_read_set_size : 15
  //   max_write_set_size : 15
  //   num_txn_contexts : 9
  typename Database::template TransactionType<abstract_db::HINT_TPCC_NEW_ORDER>::type txn(txn_flags, this->arena);
  scoped_str_arena s_arena(this->arena);
  scoped_multilock<spinlock> mlock;
  if (g_enable_partition_locks) {
    if (allLocal) {
      mlock.enq(LockForPartition(warehouse_id));
    } else {
      small_unordered_map<unsigned int, bool, 64> lockset;
      mlock.enq(LockForPartition(warehouse_id));
      lockset[PartitionId(warehouse_id)] = 1;
      for (uint i = 0; i < numItems; i++) {
        if (lockset.find(PartitionId(supplierWarehouseIDs[i])) == lockset.end()) {
          mlock.enq(LockForPartition(supplierWarehouseIDs[i]));
          lockset[PartitionId(supplierWarehouseIDs[i])] = 1;
        }
      }
    }
    mlock.multilock();
  }
  try {
    ssize_t ret = 0;
    const customer::key k_c(warehouse_id, districtID, customerID);
    customer::value v_c;
    ALWAYS_ASSERT(
        tables.tbl_customer(warehouse_id)->search(txn, k_c, v_c,
          GUARDED_FIELDS(
            customer::value::c_discount_field,
            customer::value::c_last_field,
            customer::value::c_credit_field)));
    checker::SanityCheckCustomer(&k_c, &v_c);

    const warehouse::key k_w(warehouse_id);
    warehouse::value v_w;
    ALWAYS_ASSERT(
        tables.tbl_warehouse(warehouse_id)->search(txn, k_w, v_w,
          GUARDED_FIELDS(warehouse::value::w_tax_field)));
    checker::SanityCheckWarehouse(&k_w, &v_w);

    const district::key k_d(warehouse_id, districtID);
    district::value v_d;
    ALWAYS_ASSERT(
        tables.tbl_district(warehouse_id)->search(txn, k_d, v_d,
          GUARDED_FIELDS(
            district::value::d_next_o_id_field,
            district::value::d_tax_field)));
    checker::SanityCheckDistrict(&k_d, &v_d);

    const uint64_t my_next_o_id = g_new_order_fast_id_gen ?
        FastNewOrderIdGen(warehouse_id, districtID) : v_d.d_next_o_id;

    const new_order::key k_no(warehouse_id, districtID, my_next_o_id);
    const new_order::value v_no;
    const size_t new_order_sz = Size(v_no);
    tables.tbl_new_order(warehouse_id)->insert(txn, k_no, v_no);
    ret += new_order_sz;

    if (!g_new_order_fast_id_gen) {
      v_d.d_next_o_id++;
      tables.tbl_district(warehouse_id)->put(txn, k_d, v_d,
          GUARDED_FIELDS(district::value::d_next_o_id_field));
    }

    const oorder::key k_oo(warehouse_id, districtID, k_no.no_o_id);
    oorder::value v_oo;
    v_oo.o_c_id = int32_t(customerID);
    v_oo.o_carrier_id = 0; // seems to be ignored
    v_oo.o_ol_cnt = int8_t(numItems);
    v_oo.o_all_local = allLocal;
    v_oo.o_entry_d = GetCurrentTimeMillis();

    const size_t oorder_sz = Size(v_oo);
    tables.tbl_oorder(warehouse_id)->insert(txn, k_oo, v_oo);
    ret += oorder_sz;

    const oorder_c_id_idx::key k_oo_idx(warehouse_id, districtID, customerID, k_no.no_o_id);
    const oorder_c_id_idx::value v_oo_idx(0);

    tables.tbl_oorder_c_id_idx(warehouse_id)->insert(txn, k_oo_idx, v_oo_idx);

    for (uint ol_number = 1; ol_number <= numItems; ol_number++) {
      const uint ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
      const uint ol_i_id = itemIDs[ol_number - 1];
      const uint ol_quantity = orderQuantities[ol_number - 1];

      const item::key k_i(ol_i_id);
      item::value v_i;
      ALWAYS_ASSERT(tables.tbl_item(1)->search(txn, k_i, v_i,
            GUARDED_FIELDS(
              item::value::i_price_field,
              item::value::i_name_field,
              item::value::i_data_field)));
      checker::SanityCheckItem(&k_i, &v_i);

      const stock::key k_s(ol_supply_w_id, ol_i_id);
      stock::value v_s;
      ALWAYS_ASSERT(tables.tbl_stock(ol_supply_w_id)->search(txn, k_s, v_s));
      checker::SanityCheckStock(&k_s, &v_s);

      stock::value v_s_new(v_s);
      if (v_s_new.s_quantity - ol_quantity >= 10)
        v_s_new.s_quantity -= ol_quantity;
      else
        v_s_new.s_quantity += -int32_t(ol_quantity) + 91;
      v_s_new.s_ytd += ol_quantity;
      v_s_new.s_remote_cnt += (ol_supply_w_id == warehouse_id) ? 0 : 1;

      tables.tbl_stock(ol_supply_w_id)->put(txn, k_s, v_s_new);

      const order_line::key k_ol(warehouse_id, districtID, k_no.no_o_id, ol_number);
      order_line::value v_ol;
      v_ol.ol_i_id = int32_t(ol_i_id);
      v_ol.ol_delivery_d = 0; // not delivered yet
      v_ol.ol_amount = float(ol_quantity) * v_i.i_price;
      v_ol.ol_supply_w_id = int32_t(ol_supply_w_id);
      v_ol.ol_quantity = int8_t(ol_quantity);

      const size_t order_line_sz = Size(v_ol);
      tables.tbl_order_line(warehouse_id)->insert(txn, k_ol, v_ol);
      ret += order_line_sz;
    }

    //measure_txn_counters(txn, "txn_new_order");
    if (likely(txn.commit()))
      return txn_result(true, ret);
  } catch (typename Database::abort_exception_type &e) {
    txn.abort();
  }
  return txn_result(false, 0);
}

template <typename Database>
class new_order_scan_callback : public
  Database::template IndexType<schema<new_order>>::type::search_range_callback {
public:
  new_order_scan_callback() : invoked(false) {}
  virtual bool
  invoke(const new_order::key &key, const new_order::value &value) OVERRIDE
  {
    INVARIANT(!invoked);
    k_no = key;
    invoked = true;
#ifdef CHECK_INVARIANTS
    checker::SanityCheckNewOrder(&key, &value);
#endif
    return false;
  }
  inline const new_order::key &
  get_key() const
  {
    return k_no;
  }
  inline bool was_invoked() const { return invoked; }
private:
  new_order::key k_no;
  bool invoked;
};

STATIC_COUNTER_DECL(scopedperf::tod_ctr, delivery_probe0_tod, delivery_probe0_cg)

template <typename Database, bool AllowReadOnlyScans>
typename tpcc_worker<Database, AllowReadOnlyScans>::txn_result
tpcc_worker<Database, AllowReadOnlyScans>::txn_delivery()
{
  const uint warehouse_id = PickWarehouseId(this->r, warehouse_id_start, warehouse_id_end);
  const uint o_carrier_id = RandomNumber(this->r, 1, NumDistrictsPerWarehouse());
  const uint32_t ts = GetCurrentTimeMillis();

  // worst case txn profile:
  //   10 times:
  //     1 new_order scan node
  //     1 oorder get
  //     2 order_line scan nodes
  //     15 order_line puts
  //     1 new_order remove
  //     1 oorder put
  //     1 customer get
  //     1 customer put
  //
  // output from counters:
  //   max_absent_range_set_size : 0
  //   max_absent_set_size : 0
  //   max_node_scan_size : 21
  //   max_read_set_size : 133
  //   max_write_set_size : 133
  //   num_txn_contexts : 4
  typename Database::template TransactionType<abstract_db::HINT_TPCC_DELIVERY>::type txn(txn_flags, this->arena);
  scoped_str_arena s_arena(this->arena);
  scoped_lock_guard<spinlock> slock(
      g_enable_partition_locks ? &LockForPartition(warehouse_id) : nullptr);
  try {
    ssize_t ret = 0;
    for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
      const new_order::key k_no_0(warehouse_id, d, last_no_o_ids[d - 1]);
      const new_order::key k_no_1(warehouse_id, d, numeric_limits<int32_t>::max());
      new_order_scan_callback<Database> new_order_c;
      {
        ANON_REGION("DeliverNewOrderScan:", &delivery_probe0_cg);
        tables.tbl_new_order(warehouse_id)->search_range_call(
            txn, k_no_0, &k_no_1, new_order_c);
      }

      const new_order::key &k_no = new_order_c.get_key();
      if (unlikely(!new_order_c.was_invoked()))
          continue;
      last_no_o_ids[d - 1] = k_no.no_o_id + 1; // XXX: update last seen

      const oorder::key k_oo(warehouse_id, d, k_no.no_o_id);
      oorder::value v_oo;
      if (unlikely(!tables.tbl_oorder(warehouse_id)->search(txn, k_oo, v_oo,
              GUARDED_FIELDS(
                oorder::value::o_c_id_field,
                oorder::value::o_carrier_id_field)))) {
        // even if we read the new order entry, there's no guarantee
        // we will read the oorder entry: in this case the txn will abort,
        // but we're simply bailing out early
        txn.abort();
        return txn_result(false, 0);
      }
      checker::SanityCheckOOrder(&k_oo, &v_oo);

      // never more than 15 order_lines per order
      static_limit_callback<typename Database::template IndexType<schema<order_line>>::type, 15, false> c;
      const order_line::key k_oo_0(warehouse_id, d, k_no.no_o_id, 0);
      const order_line::key k_oo_1(warehouse_id, d, k_no.no_o_id, numeric_limits<int32_t>::max());

      // XXX(stephentu): mutable scans would help here
      tables.tbl_order_line(warehouse_id)->search_range_call(
          txn, k_oo_0, &k_oo_1, c, false,
          GUARDED_FIELDS(
            order_line::value::ol_amount_field,
            order_line::value::ol_delivery_d_field));
      float sum = 0.0;
      for (size_t i = 0; i < c.size(); i++) {
#if defined(CHECK_INVARIANTS) && defined(DISABLE_FIELD_SELECTION)
        checker::SanityCheckOrderLine(&c.key(i), &c.value(i));
#endif
        sum += c.value(i).ol_amount;
        c.value(i).ol_delivery_d = ts;
        tables.tbl_order_line(warehouse_id)->put(txn, c.key(i), c.value(i),
            GUARDED_FIELDS(order_line::value::ol_delivery_d_field));
      }

      // delete new order
      tables.tbl_new_order(warehouse_id)->remove(txn, k_no);
      ret -= 0 /*new_order_c.get_value_size()*/;

      // update oorder
      v_oo.o_carrier_id = o_carrier_id;
      tables.tbl_oorder(warehouse_id)->put(txn, k_oo, v_oo,
          GUARDED_FIELDS(oorder::value::o_carrier_id_field));

      const uint c_id = v_oo.o_c_id;
      const float ol_total = sum;

      // update customer
      const customer::key k_c(warehouse_id, d, c_id);
      customer::value v_c;
      ALWAYS_ASSERT(tables.tbl_customer(warehouse_id)->search(txn, k_c, v_c,
            GUARDED_FIELDS(customer::value::c_balance_field)));
      v_c.c_balance += ol_total;
      tables.tbl_customer(warehouse_id)->put(txn, k_c, v_c,
            GUARDED_FIELDS(customer::value::c_balance_field));
    }
    //measure_txn_counters(txn, "txn_delivery");
    if (likely(txn.commit()))
      return txn_result(true, ret);
  } catch (typename Database::abort_exception_type &e) {
    txn.abort();
  }
  return txn_result(false, 0);
}

static event_avg_counter evt_avg_cust_name_idx_scan_size("avg_cust_name_idx_scan_size");

template <typename Database, bool AllowReadOnlyScans>
typename tpcc_worker<Database, AllowReadOnlyScans>::txn_result
tpcc_worker<Database, AllowReadOnlyScans>::txn_payment()
{
  const uint warehouse_id = PickWarehouseId(this->r, warehouse_id_start, warehouse_id_end);
  const uint districtID = RandomNumber(this->r, 1, NumDistrictsPerWarehouse());
  uint customerDistrictID, customerWarehouseID;
  if (likely(g_disable_xpartition_txn ||
             NumWarehouses() == 1 ||
             RandomNumber(this->r, 1, 100) <= 85)) {
    customerDistrictID = districtID;
    customerWarehouseID = warehouse_id;
  } else {
    customerDistrictID = RandomNumber(this->r, 1, NumDistrictsPerWarehouse());
    do {
      customerWarehouseID = RandomNumber(this->r, 1, NumWarehouses());
    } while (customerWarehouseID == warehouse_id);
  }
  const float paymentAmount = (float) (RandomNumber(this->r, 100, 500000) / 100.0);
  const uint32_t ts = GetCurrentTimeMillis();
  INVARIANT(!g_disable_xpartition_txn || customerWarehouseID == warehouse_id);

  // output from txn counters:
  //   max_absent_range_set_size : 0
  //   max_absent_set_size : 0
  //   max_node_scan_size : 10
  //   max_read_set_size : 71
  //   max_write_set_size : 1
  //   num_txn_contexts : 5
  typename Database::template TransactionType<abstract_db::HINT_TPCC_PAYMENT>::type txn(txn_flags, this->arena);
  scoped_str_arena s_arena(this->arena);
  scoped_multilock<spinlock> mlock;
  if (g_enable_partition_locks) {
    mlock.enq(LockForPartition(warehouse_id));
    if (PartitionId(customerWarehouseID) != PartitionId(warehouse_id))
      mlock.enq(LockForPartition(customerWarehouseID));
    mlock.multilock();
  }
  if (customerWarehouseID != warehouse_id)
    ++evt_tpcc_cross_partition_payment_txns;
  try {
    ssize_t ret = 0;

    const warehouse::key k_w(warehouse_id);
    warehouse::value v_w;
    ALWAYS_ASSERT(
        tables.tbl_warehouse(warehouse_id)->search(txn, k_w, v_w, GUARDED_FIELDS(
            warehouse::value::w_ytd_field,
            warehouse::value::w_street_1_field,
            warehouse::value::w_street_1_field,
            warehouse::value::w_city_field,
            warehouse::value::w_state_field,
            warehouse::value::w_zip_field,
            warehouse::value::w_name_field)));
    checker::SanityCheckWarehouse(&k_w, &v_w);

    v_w.w_ytd += paymentAmount;
    tables.tbl_warehouse(warehouse_id)->put(txn, k_w, v_w, GUARDED_FIELDS(warehouse::value::w_ytd_field));

    const district::key k_d(warehouse_id, districtID);
    district::value v_d;
    ALWAYS_ASSERT(tables.tbl_district(warehouse_id)->search(txn, k_d, v_d, GUARDED_FIELDS(
            district::value::d_ytd_field,
            district::value::d_street_1_field,
            district::value::d_street_1_field,
            district::value::d_city_field,
            district::value::d_state_field,
            district::value::d_zip_field,
            district::value::d_name_field)));
    checker::SanityCheckDistrict(&k_d, &v_d);

    v_d.d_ytd += paymentAmount;
    tables.tbl_district(warehouse_id)->put(txn, k_d, v_d, GUARDED_FIELDS(district::value::d_ytd_field));

    customer::key k_c;
    customer::value v_c;
    if (RandomNumber(this->r, 1, 100) <= 60) {
      // cust by name
      uint8_t lastname_buf[CustomerLastNameMaxSize + 1];
      static_assert(sizeof(lastname_buf) == 16, "XX");
      NDB_MEMSET(lastname_buf, 0, sizeof(lastname_buf));
      GetNonUniformCustomerLastNameRun(lastname_buf, r);

      static const string zeros(16, 0);
      static const string ones(16, 255);

      customer_name_idx::key k_c_idx_0;
      k_c_idx_0.c_w_id = customerWarehouseID;
      k_c_idx_0.c_d_id = customerDistrictID;
      k_c_idx_0.c_last.assign((const char *) lastname_buf, 16);
      k_c_idx_0.c_first.assign(zeros);

      customer_name_idx::key k_c_idx_1;
      k_c_idx_1.c_w_id = customerWarehouseID;
      k_c_idx_1.c_d_id = customerDistrictID;
      k_c_idx_1.c_last.assign((const char *) lastname_buf, 16);
      k_c_idx_1.c_first.assign(ones);

      // probably a safe bet for now
      bytes_static_limit_callback<
        typename Database::template IndexType<schema<customer_name_idx>>::type,
        NMaxCustomerIdxScanElems, true> c(s_arena.get());
      tables.tbl_customer_name_idx(customerWarehouseID)->bytes_search_range_call(
        txn, k_c_idx_0, &k_c_idx_1, c);
      ALWAYS_ASSERT(c.size() > 0);
      INVARIANT(c.size() < NMaxCustomerIdxScanElems); // we should detect this
      int index = c.size() / 2;
      if (c.size() % 2 == 0)
        index--;
      evt_avg_cust_name_idx_scan_size.offer(c.size());

      customer_name_idx::value v_c_idx_temp;
      const customer_name_idx::value *v_c_idx = Decode(c.value(index), v_c_idx_temp);

      k_c.c_w_id = customerWarehouseID;
      k_c.c_d_id = customerDistrictID;
      k_c.c_id = v_c_idx->c_id;
      ALWAYS_ASSERT(tables.tbl_customer(customerWarehouseID)->search(txn, k_c, v_c));
    } else {
      // cust by ID
      const uint customerID = GetCustomerId(r);
      k_c.c_w_id = customerWarehouseID;
      k_c.c_d_id = customerDistrictID;
      k_c.c_id = customerID;
      ALWAYS_ASSERT(tables.tbl_customer(customerWarehouseID)->search(txn, k_c, v_c));
    }
    checker::SanityCheckCustomer(&k_c, &v_c);

    v_c.c_balance -= paymentAmount;
    v_c.c_ytd_payment += paymentAmount;
    v_c.c_payment_cnt++;
    if (strncmp(v_c.c_credit.data(), "BC", 2) == 0) {
      char buf[501];
      int n = snprintf(buf, sizeof(buf), "%d %d %d %d %d %f | %s",
                       k_c.c_id,
                       k_c.c_d_id,
                       k_c.c_w_id,
                       districtID,
                       warehouse_id,
                       paymentAmount,
                       v_c.c_data.c_str());
      v_c.c_data.resize_junk(
          min(static_cast<size_t>(n), v_c.c_data.max_size()));
      NDB_MEMCPY((void *) v_c.c_data.data(), &buf[0], v_c.c_data.size());
    }

    tables.tbl_customer(customerWarehouseID)->put(txn, k_c, v_c);

    const history::key k_h(k_c.c_d_id, k_c.c_w_id, k_c.c_id, districtID, warehouse_id, ts);
    history::value v_h;
    v_h.h_amount = paymentAmount;
    v_h.h_data.resize_junk(v_h.h_data.max_size());
    int n = snprintf((char *) v_h.h_data.data(), v_h.h_data.max_size() + 1,
                     "%.10s    %.10s",
                     v_w.w_name.c_str(),
                     v_d.d_name.c_str());
    v_h.h_data.resize_junk(min(static_cast<size_t>(n), v_h.h_data.max_size()));

    const size_t history_sz = Size(v_h);
    tables.tbl_history(warehouse_id)->insert(txn, k_h, v_h);
    ret += history_sz;

    //measure_txn_counters(txn, "txn_payment");
    if (likely(txn.commit()))
      return txn_result(true, ret);
  } catch (typename Database::abort_exception_type &e) {
    txn.abort();
  }
  return txn_result(false, 0);
}

template <typename Database>
class order_line_nop_callback : public
  Database::template IndexType<schema<order_line>>::type::bytes_search_range_callback {

public:
  order_line_nop_callback() : n(0) {}
  virtual bool invoke(
      const string &key,
      const string &value)
  {
    INVARIANT(key.size() == sizeof(order_line::key));
    order_line::value v_ol_temp;
    const order_line::value *v_ol UNUSED = Decode(value, v_ol_temp);
#ifdef CHECK_INVARIANTS
    order_line::key k_ol_temp;
    const order_line::key *k_ol = Decode(key, k_ol_temp);
    checker::SanityCheckOrderLine(k_ol, v_ol);
#endif
    ++n;
    return true;
  }
  size_t n;
};

STATIC_COUNTER_DECL(scopedperf::tod_ctr, order_status_probe0_tod, order_status_probe0_cg)

template <typename Database, bool AllowReadOnlyScans>
typename tpcc_worker<Database, AllowReadOnlyScans>::txn_result
tpcc_worker<Database, AllowReadOnlyScans>::txn_order_status()
{
  const uint warehouse_id = PickWarehouseId(this->r, warehouse_id_start, warehouse_id_end);
  const uint districtID = RandomNumber(this->r, 1, NumDistrictsPerWarehouse());

  // output from txn counters:
  //   max_absent_range_set_size : 0
  //   max_absent_set_size : 0
  //   max_node_scan_size : 13
  //   max_read_set_size : 81
  //   max_write_set_size : 0
  //   num_txn_contexts : 4
  const uint64_t read_only_mask =
    !AllowReadOnlyScans ? 0 : transaction_base::TXN_FLAG_READ_ONLY;
  typename Database::template TransactionType<
    AllowReadOnlyScans ?
      abstract_db::HINT_TPCC_ORDER_STATUS_READ_ONLY :
      abstract_db::HINT_TPCC_ORDER_STATUS>::type txn(
          txn_flags | read_only_mask, this->arena);
  scoped_str_arena s_arena(this->arena);
  // NB: since txn_order_status() is a RO txn, we assume that
  // locking is un-necessary (since we can just read from some old snapshot)
  try {

    customer::key k_c;
    customer::value v_c;
    if (RandomNumber(this->r, 1, 100) <= 60) {
      // cust by name
      uint8_t lastname_buf[CustomerLastNameMaxSize + 1];
      static_assert(sizeof(lastname_buf) == 16, "xx");
      NDB_MEMSET(lastname_buf, 0, sizeof(lastname_buf));
      GetNonUniformCustomerLastNameRun(lastname_buf, r);

      static const string zeros(16, 0);
      static const string ones(16, 255);

      customer_name_idx::key k_c_idx_0;
      k_c_idx_0.c_w_id = warehouse_id;
      k_c_idx_0.c_d_id = districtID;
      k_c_idx_0.c_last.assign((const char *) lastname_buf, 16);
      k_c_idx_0.c_first.assign(zeros);

      customer_name_idx::key k_c_idx_1;
      k_c_idx_1.c_w_id = warehouse_id;
      k_c_idx_1.c_d_id = districtID;
      k_c_idx_1.c_last.assign((const char *) lastname_buf, 16);
      k_c_idx_1.c_first.assign(ones);

      // NMaxCustomerIdxScanElems is probably a safe bet for now
      bytes_static_limit_callback<
        typename Database::template IndexType<schema<customer_name_idx>>::type,
        NMaxCustomerIdxScanElems, true> c(s_arena.get());
      tables.tbl_customer_name_idx(warehouse_id)->bytes_search_range_call(
          txn, k_c_idx_0, &k_c_idx_1, c);
      ALWAYS_ASSERT(c.size() > 0);
      INVARIANT(c.size() < NMaxCustomerIdxScanElems); // we should detect this
      int index = c.size() / 2;
      if (c.size() % 2 == 0)
        index--;
      evt_avg_cust_name_idx_scan_size.offer(c.size());

      customer_name_idx::value v_c_idx_temp;
      const customer_name_idx::value *v_c_idx = Decode(c.value(index), v_c_idx_temp);

      k_c.c_w_id = warehouse_id;
      k_c.c_d_id = districtID;
      k_c.c_id = v_c_idx->c_id;
      ALWAYS_ASSERT(tables.tbl_customer(warehouse_id)->search(txn, k_c, v_c));

    } else {
      // cust by ID
      const uint customerID = GetCustomerId(r);
      k_c.c_w_id = warehouse_id;
      k_c.c_d_id = districtID;
      k_c.c_id = customerID;
      ALWAYS_ASSERT(tables.tbl_customer(warehouse_id)->search(txn, k_c, v_c));
    }
    checker::SanityCheckCustomer(&k_c, &v_c);

    // XXX(stephentu): HACK- we bound the # of elems returned by this scan to
    // 15- this is because we don't have reverse scans. In an ideal system, a
    // reverse scan would only need to read 1 btree node. We could simulate a
    // lookup by only reading the first element- but then we would *always*
    // read the first order by any customer.  To make this more interesting, we
    // randomly select which elem to pick within the 1st or 2nd btree nodes.
    // This is obviously a deviation from TPC-C, but it shouldn't make that
    // much of a difference in terms of performance numbers (in fact we are
    // making it worse for us)
    latest_key_callback<
      typename Database::template IndexType<schema<oorder_c_id_idx>>::type> c_oorder(
          *this->arena.next(), (r.next() % 15) + 1);
    const oorder_c_id_idx::key k_oo_idx_0(warehouse_id, districtID, k_c.c_id, 0);
    const oorder_c_id_idx::key k_oo_idx_1(warehouse_id, districtID, k_c.c_id, numeric_limits<int32_t>::max());
    {
      ANON_REGION("OrderStatusOOrderScan:", &order_status_probe0_cg);
      tables.tbl_oorder_c_id_idx(warehouse_id)->bytes_search_range_call(
          txn, k_oo_idx_0, &k_oo_idx_1, c_oorder);
    }
    ALWAYS_ASSERT(c_oorder.size());

    oorder_c_id_idx::key k_oo_idx_temp;
    const oorder_c_id_idx::key *k_oo_idx = Decode(c_oorder.kstr(), k_oo_idx_temp);
    const uint o_id = k_oo_idx->o_o_id;

    // XXX: fix
    // XXX(stephentu): what's wrong w/ it?
    order_line_nop_callback<Database> c_order_line;
    const order_line::key k_ol_0(warehouse_id, districtID, o_id, 0);
    const order_line::key k_ol_1(warehouse_id, districtID, o_id, numeric_limits<int32_t>::max());
    tables.tbl_order_line(warehouse_id)->bytes_search_range_call(
        txn, k_ol_0, &k_ol_1, c_order_line);
    ALWAYS_ASSERT(c_order_line.n >= 5 && c_order_line.n <= 15);

    //measure_txn_counters(txn, "txn_order_status");
    if (likely(txn.commit()))
      return txn_result(true, 0);
  } catch (typename Database::abort_exception_type &e) {
    txn.abort();
  }
  return txn_result(false, 0);
}

template <typename Database>
class order_line_scan_callback : public
  Database::template IndexType<schema<order_line>>::type::bytes_search_range_callback {
public:
  order_line_scan_callback() : n(0) {}
  virtual bool invoke(
      const string &key,
      const string &value)
  {
    INVARIANT(key.size() == sizeof(order_line::key));
    order_line::value v_ol;

#ifdef DISABLE_FIELD_SELECTION
    const uint64_t mask = numeric_limits<uint64_t>::max();
#else
    const uint64_t mask = compute_fields_mask(0);
#endif

    typed_txn_btree_<schema<order_line>>::do_record_read(
        (const uint8_t *) value.data(), value.size(), mask, &v_ol);

#if defined(CHECK_INVARIANTS) && defined(DISABLE_FIELD_SELECTION)
    order_line::key k_ol_temp;
    const order_line::key *k_ol = Decode(key, k_ol_temp);
    checker::SanityCheckOrderLine(k_ol, &v_ol);
#endif

    s_i_ids[v_ol.ol_i_id] = 1;
    n++;
    return true;
  }
  size_t n;
  small_unordered_map<uint, bool, 512> s_i_ids;
};

STATIC_COUNTER_DECL(scopedperf::tod_ctr, stock_level_probe0_tod, stock_level_probe0_cg)
STATIC_COUNTER_DECL(scopedperf::tod_ctr, stock_level_probe1_tod, stock_level_probe1_cg)
STATIC_COUNTER_DECL(scopedperf::tod_ctr, stock_level_probe2_tod, stock_level_probe2_cg)

static event_avg_counter evt_avg_stock_level_loop_join_lookups("stock_level_loop_join_lookups");

template <typename Database, bool AllowReadOnlyScans>
typename tpcc_worker<Database, AllowReadOnlyScans>::txn_result
tpcc_worker<Database, AllowReadOnlyScans>::txn_stock_level()
{
  const uint warehouse_id = PickWarehouseId(this->r, warehouse_id_start, warehouse_id_end);
  const uint threshold = RandomNumber(this->r, 10, 20);
  const uint districtID = RandomNumber(this->r, 1, NumDistrictsPerWarehouse());

  // output from txn counters:
  //   max_absent_range_set_size : 0
  //   max_absent_set_size : 0
  //   max_node_scan_size : 19
  //   max_read_set_size : 241
  //   max_write_set_size : 0
  //   n_node_scan_large_instances : 1
  //   n_read_set_large_instances : 2
  //   num_txn_contexts : 3
  const uint64_t read_only_mask =
    !AllowReadOnlyScans ? 0 : transaction_base::TXN_FLAG_READ_ONLY;
  typename Database::template TransactionType<
    AllowReadOnlyScans ?
      abstract_db::HINT_TPCC_STOCK_LEVEL_READ_ONLY :
      abstract_db::HINT_TPCC_STOCK_LEVEL>::type txn(
          txn_flags | read_only_mask, this->arena);
  scoped_str_arena s_arena(this->arena);
  // NB: since txn_stock_level() is a RO txn, we assume that
  // locking is un-necessary (since we can just read from some old snapshot)
  try {
    const district::key k_d(warehouse_id, districtID);
    district::value v_d;
    ALWAYS_ASSERT(tables.tbl_district(warehouse_id)->search(txn, k_d, v_d));
    checker::SanityCheckDistrict(&k_d, &v_d);
    const uint64_t cur_next_o_id = g_new_order_fast_id_gen ?
      NewOrderIdHolder(warehouse_id, districtID).load(memory_order_acquire) :
      v_d.d_next_o_id;

    // manual joins are fun!
    order_line_scan_callback<Database> c;
    const int32_t lower = cur_next_o_id >= 20 ? (cur_next_o_id - 20) : 0;
    const order_line::key k_ol_0(warehouse_id, districtID, lower, 0);
    const order_line::key k_ol_1(warehouse_id, districtID, cur_next_o_id, 0);
    {
      // mask must be kept in sync w/ order_line_scan_callback
#ifdef DISABLE_FIELD_SELECTION
      const size_t nfields = order_line::value::NFIELDS;
#else
      const size_t nfields = 1;
#endif
      ANON_REGION("StockLevelOrderLineScan:", &stock_level_probe0_cg);
      tables.tbl_order_line(warehouse_id)->bytes_search_range_call(
          txn, k_ol_0, &k_ol_1, c, nfields);
    }
    {
      small_unordered_map<uint, bool, 512> s_i_ids_distinct;
      for (auto &p : c.s_i_ids) {
        ANON_REGION("StockLevelLoopJoinIter:", &stock_level_probe1_cg);
        const stock::key k_s(warehouse_id, p.first);
        stock::value v_s;
        INVARIANT(p.first >= 1 && p.first <= NumItems());
        {
          ANON_REGION("StockLevelLoopJoinGet:", &stock_level_probe2_cg);
          ALWAYS_ASSERT(tables.tbl_stock(warehouse_id)->search(txn, k_s, v_s, GUARDED_FIELDS(stock::value::s_quantity_field)));
        }
        if (v_s.s_quantity < int(threshold))
          s_i_ids_distinct[p.first] = 1;
      }
      evt_avg_stock_level_loop_join_lookups.offer(c.s_i_ids.size());
      // NB(stephentu): s_i_ids_distinct.size() is the computed result of this txn
    }
    //measure_txn_counters(txn, "txn_stock_level");
    if (likely(txn.commit()))
      return txn_result(true, 0);
  } catch (typename Database::abort_exception_type &e) {
    txn.abort();
  }
  return txn_result(false, 0);
}

template <typename T>
static vector<T>
unique_filter(const vector<T> &v)
{
  set<T> seen;
  vector<T> ret;
  for (auto &e : v)
    if (!seen.count(e)) {
      ret.emplace_back(e);
      seen.insert(e);
    }
  return ret;
}

template <typename Database, bool AllowReadOnlyScans>
class tpcc_bench_runner : public typed_bench_runner<Database> {
private:

  static bool
  IsTableReadOnly(const char *name)
  {
    return strcmp("item", name) == 0;
  }

  static bool
  IsTableAppendOnly(const char *name)
  {
    return strcmp("history", name) == 0 ||
           strcmp("oorder_c_id_idx", name) == 0;
  }

  template <typename Schema>
  static vector<shared_ptr<typename Database::template IndexType<Schema>::type>>
  OpenTablesForTablespace(Database *db, const char *name, size_t expected_size)
  {
    const bool is_read_only = IsTableReadOnly(name);
    const bool is_append_only = IsTableAppendOnly(name);
    const string s_name(name);
    vector<typename Database::template IndexType<Schema>::ptr_type> ret(NumWarehouses());
    if (g_enable_separate_tree_per_partition && !is_read_only) {
      if (NumWarehouses() <= nthreads) {
        for (size_t i = 0; i < NumWarehouses(); i++)
          ret[i] = db->template open_index<Schema>(s_name + "_" + to_string(i), expected_size, is_append_only);
      } else {
        const unsigned nwhse_per_partition = NumWarehouses() / nthreads;
        for (size_t partid = 0; partid < nthreads; partid++) {
          const unsigned wstart = partid * nwhse_per_partition;
          const unsigned wend   = (partid + 1 == nthreads) ?
            NumWarehouses() : (partid + 1) * nwhse_per_partition;
          auto idx =
            db->template open_index<Schema>(s_name + "_" + to_string(partid), expected_size, is_append_only);
          for (size_t i = wstart; i < wend; i++)
            ret[i] = idx;
        }
      }
    } else {
      auto idx = db->template open_index<Schema>(s_name, expected_size, is_append_only);
      for (size_t i = 0; i < NumWarehouses(); i++)
        ret[i] = idx;
    }
    return ret;
  }

public:
  tpcc_bench_runner(Database *db)
    : typed_bench_runner<Database>(db)
  {

#define OPEN_TABLESPACE_X(x) \
    do { \
      tables.tbl_ ## x ## _vec = OpenTablesForTablespace<schema<x>>(db, #x, sizeof(x::value)); \
      auto v = unique_filter(tables.tbl_ ## x ## _vec); \
      for (size_t i = 0; i < v.size(); i++) \
        this->open_tables[string(#x) + "_" + to_string(i)] = v[i]; \
    } while (0);

    TPCC_TABLE_LIST(OPEN_TABLESPACE_X);

#undef OPEN_TABLESPACE_X

    if (g_enable_partition_locks) {
      static_assert(sizeof(aligned_padded_elem<spinlock>) == CACHELINE_SIZE, "xx");
      void * const px = memalign(CACHELINE_SIZE, sizeof(aligned_padded_elem<spinlock>) * nthreads);
      ALWAYS_ASSERT(px);
      ALWAYS_ASSERT(reinterpret_cast<uintptr_t>(px) % CACHELINE_SIZE == 0);
      g_partition_locks = reinterpret_cast<aligned_padded_elem<spinlock> *>(px);
      for (size_t i = 0; i < nthreads; i++) {
        new (&g_partition_locks[i]) aligned_padded_elem<spinlock>();
        ALWAYS_ASSERT(!g_partition_locks[i].elem.is_locked());
      }
    }

    if (g_new_order_fast_id_gen) {
      void * const px =
        memalign(
            CACHELINE_SIZE,
            sizeof(aligned_padded_elem<atomic<uint64_t>>) *
              NumWarehouses() * NumDistrictsPerWarehouse());
      g_district_ids = reinterpret_cast<aligned_padded_elem<atomic<uint64_t>> *>(px);
      for (size_t i = 0; i < NumWarehouses() * NumDistrictsPerWarehouse(); i++)
        new (&g_district_ids[i]) atomic<uint64_t>(3001);
    }
  }

protected:
  virtual vector<unique_ptr<bench_loader>>
  make_loaders()
  {
    vector<unique_ptr<bench_loader>> ret;
    ret.emplace_back(new tpcc_warehouse_loader<Database>(9324, this->typed_db(), tables));
    ret.emplace_back(new tpcc_item_loader<Database>(235443, this->typed_db(), tables));
    if (enable_parallel_loading) {
      fast_random r(89785943);
      for (uint i = 1; i <= NumWarehouses(); i++)
        ret.emplace_back(new tpcc_stock_loader<Database>(r.next(), this->typed_db(), tables, i));
    } else {
      ret.emplace_back(new tpcc_stock_loader<Database>(89785943, this->typed_db(), tables, -1));
    }
    ret.emplace_back(new tpcc_district_loader<Database>(129856349, this->typed_db(), tables));
    if (enable_parallel_loading) {
      fast_random r(923587856425);
      for (uint i = 1; i <= NumWarehouses(); i++)
        ret.emplace_back(new tpcc_customer_loader<Database>(r.next(), this->typed_db(), tables, i));
    } else {
      ret.emplace_back(new tpcc_customer_loader<Database>(923587856425, this->typed_db(), tables, -1));
    }
    if (enable_parallel_loading) {
      fast_random r(2343352);
      for (uint i = 1; i <= NumWarehouses(); i++)
        ret.emplace_back(new tpcc_order_loader<Database>(r.next(), this->typed_db(), tables, i));
    } else {
      ret.emplace_back(new tpcc_order_loader<Database>(2343352, this->typed_db(), tables, -1));
    }
    return ret;
  }

private:
  template <bool RO>
  vector<unique_ptr<bench_worker>>
  make_workers_impl()
  {
    const unsigned alignment = coreid::num_cpus_online();
    const int blockstart =
      coreid::allocate_contiguous_aligned_block(nthreads, alignment);
    ALWAYS_ASSERT(blockstart >= 0);
    ALWAYS_ASSERT((blockstart % alignment) == 0);
    fast_random r(23984543);
    vector<unique_ptr<bench_worker>> ret;
    if (NumWarehouses() <= nthreads) {
      for (size_t i = 0; i < nthreads; i++)
        ret.emplace_back(
          new tpcc_worker<Database, RO>(
            blockstart + i,
            r.next(), this->typed_db(), tables,
            &this->barrier_a, &this->barrier_b,
            (i % NumWarehouses()) + 1, (i % NumWarehouses()) + 2));
    } else {
      const unsigned nwhse_per_partition = NumWarehouses() / nthreads;
      for (size_t i = 0; i < nthreads; i++) {
        const unsigned wstart = i * nwhse_per_partition;
        const unsigned wend   = (i + 1 == nthreads) ?
          NumWarehouses() : (i + 1) * nwhse_per_partition;
        ret.emplace_back(
          new tpcc_worker<Database, RO>(
            blockstart + i,
            r.next(), this->typed_db(), tables,
            &this->barrier_a, &this->barrier_b, wstart+1, wend+2));
      }
    }
    return ret;
  }

protected:
  virtual vector<unique_ptr<bench_worker>>
  make_workers()
  {
    return g_disable_read_only_scans ? make_workers_impl<false>() : make_workers_impl<true>();
  }

private:
  tpcc_tables<Database> tables;
};

template <typename Database>
static unique_ptr<bench_runner>
MakeBenchRunner(Database *db)
{
  return unique_ptr<bench_runner>(
    g_disable_read_only_scans ?
      static_cast<bench_runner *>(new tpcc_bench_runner<Database, false>(db)) :
      static_cast<bench_runner *>(new tpcc_bench_runner<Database, true>(db)));
}

void
tpcc_do_test(const string &dbtype,
             const persistconfig &cfg,
             int argc, char **argv)
{
  // parse options
  optind = 1;
  bool did_spec_remote_pct = false;
  while (1) {
    static struct option long_options[] =
    {
      {"disable-cross-partition-transactions" , no_argument       , &g_disable_xpartition_txn             , 1}   ,
      {"disable-read-only-snapshots"          , no_argument       , &g_disable_read_only_scans            , 1}   ,
      {"enable-partition-locks"               , no_argument       , &g_enable_partition_locks             , 1}   ,
      {"enable-separate-tree-per-partition"   , no_argument       , &g_enable_separate_tree_per_partition , 1}   ,
      {"new-order-remote-item-pct"            , required_argument , 0                                     , 'r'} ,
      {"new-order-fast-id-gen"                , no_argument       , &g_new_order_fast_id_gen              , 1}   ,
      {"uniform-item-dist"                    , no_argument       , &g_uniform_item_dist                  , 1}   ,
      {"workload-mix"                         , required_argument , 0                                     , 'w'} ,
      {0, 0, 0, 0}
    };
    int option_index = 0;
    int c = getopt_long(argc, argv, "r:", long_options, &option_index);
    if (c == -1)
      break;
    switch (c) {
    case 0:
      if (long_options[option_index].flag != 0)
        break;
      abort();
      break;

    case 'r':
      g_new_order_remote_item_pct = strtoul(optarg, NULL, 10);
      ALWAYS_ASSERT(g_new_order_remote_item_pct >= 0 && g_new_order_remote_item_pct <= 100);
      did_spec_remote_pct = true;
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

  if (did_spec_remote_pct && g_disable_xpartition_txn) {
    cerr << "WARNING: --new-order-remote-item-pct given with --disable-cross-partition-transactions" << endl;
    cerr << "  --new-order-remote-item-pct will have no effect" << endl;
  }

  if (verbose) {
    cerr << "tpcc settings:" << endl;
    cerr << "  cross_partition_transactions : " << !g_disable_xpartition_txn << endl;
    cerr << "  read_only_snapshots          : " << !g_disable_read_only_scans << endl;
    cerr << "  partition_locks              : " << g_enable_partition_locks << endl;
    cerr << "  separate_tree_per_partition  : " << g_enable_separate_tree_per_partition << endl;
    cerr << "  new_order_remote_item_pct    : " << g_new_order_remote_item_pct << endl;
    cerr << "  new_order_fast_id_gen        : " << g_new_order_fast_id_gen << endl;
    cerr << "  uniform_item_dist            : " << g_uniform_item_dist << endl;
    cerr << "  workload_mix                 : " <<
      format_list(g_txn_workload_mix,
                  g_txn_workload_mix + ARRAY_NELEMS(g_txn_workload_mix)) << endl;
  }

  unique_ptr<abstract_db> db;
  unique_ptr<bench_runner> r;

  if (dbtype == "ndb-proto2") {
    if (!cfg.logfiles_.empty()) {
      vector<vector<unsigned>> assignments_used;
      txn_logger::Init(
          nthreads, cfg.logfiles_, cfg.assignments_, &assignments_used,
          !cfg.nofsync_,
          cfg.do_compress_,
          cfg.fake_writes_);
      if (verbose) {
        cerr << "[logging subsystem]" << endl;
        cerr << "  assignments: " << assignments_used  << endl;
        cerr << "  call fsync : " << !cfg.nofsync_     << endl;
        cerr << "  compression: " << cfg.do_compress_  << endl;
        cerr << "  fake_writes: " << cfg.fake_writes_  << endl;
      }
    }
#ifdef PROTO2_CAN_DISABLE_GC
    if (!cfg.disable_gc_)
      transaction_proto2_static::InitGC();
#endif
#ifdef PROTO2_CAN_DISABLE_SNAPSHOTS
    if (cfg.disable_snapshots_)
      transaction_proto2_static::DisableSnapshots();
#endif
    typedef ndb_database<transaction_proto2> Database;
    Database *raw = new Database;
    db.reset(raw);
    r = MakeBenchRunner(raw);
  } else if (dbtype == "kvdb-st") {
    typedef kvdb_database<false> Database;
    Database *raw = new Database;
    db.reset(raw);
    r = MakeBenchRunner(raw);
  } else
    ALWAYS_ASSERT(false);

  r->run();
}
