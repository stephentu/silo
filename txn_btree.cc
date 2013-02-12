#include <unistd.h>

#include "txn_btree.h"
#include "thread.h"
#include "util.h"
#include "macros.h"

using namespace std;
using namespace util;

bool
txn_btree::search(transaction &t, const key_type &k, value_type &v)
{
  t.ensure_active();
  transaction::txn_context &ctx = t.ctx_map[this];

  // priority is
  // 1) write set
  // 2) local read set
  // 3) range set
  // 4) query underlying tree
  //
  // note (1)-(3) are served by transaction::local_search()

  string sk(k.str());
  if (ctx.local_search_str(sk, v))
    return (bool) v;

  btree::value_type underlying_v;
  if (!underlying_btree.search(k, underlying_v)) {
    // all records exist in the system at MIN_TID with no value
    transaction::read_record_t *read_rec = &ctx.read_set[sk];
    read_rec->t = transaction::MIN_TID;
    read_rec->r = NULL;
    read_rec->ln = NULL;
    return false;
  } else {
    transaction::logical_node *ln = (transaction::logical_node *) underlying_v;
    INVARIANT(ln);
    prefetch_node(ln);
    transaction::tid_t start_t = 0;
    transaction::record_t r = 0;

    pair<bool, transaction::tid_t> snapshot_tid_t = t.consistent_snapshot_tid();
    transaction::tid_t snapshot_tid = snapshot_tid_t.first ? snapshot_tid_t.second : transaction::MAX_TID;

    //if (unlikely(!ln->stable_read(snapshot_tid, start_t, r))) {
    //  cerr << "snapshot_tid: "  << snapshot_tid << endl;
    //  t.abort();
    //  throw transaction_abort_exception();
    //}

    //if (unlikely(!t.can_read_tid(start_t))) {
    //  cerr << "snapshot_tid: " << snapshot_tid << ", start_tid: "  << start_t << endl;
    //  t.abort();
    //  throw transaction_abort_exception();
    //}

    if (unlikely(!ln->stable_read(snapshot_tid, start_t, r) || !t.can_read_tid(start_t))) {
      t.abort();
      throw transaction_abort_exception();
    }

    transaction::read_record_t *read_rec = &ctx.read_set[sk];
    read_rec->t = start_t;
    read_rec->r = r;
    read_rec->ln = ln;
    v = read_rec->r;
    return read_rec->r;
  }
}

void
txn_btree::txn_search_range_callback::on_resp_node(
    const btree::node_opaque_t *n, uint64_t version)
{
  VERBOSE(cerr << "on_resp_node(): <node=0x" << hexify(intptr_t(n))
               << ", version=" << version << ">" << endl);
  VERBOSE(cerr << "  " << btree::NodeStringify(n) << endl);
  if (t->get_flags() & transaction::TXN_FLAG_LOW_LEVEL_SCAN) {
    transaction::node_scan_map::iterator it = ctx->node_scan.find(n);
    if (it == ctx->node_scan.end()) {
      ctx->node_scan[n] = version;
    } else {
      if (unlikely(it->second != version)) {
        t->abort();
        throw transaction_abort_exception();
      }
    }
  }
}

bool
txn_btree::txn_search_range_callback::invoke(
    const key_type &k, value_type v,
    const btree::node_opaque_t *n, uint64_t version)
{
  t->ensure_active();
  VERBOSE(cerr << "search range k: " << k << " from <node=0x" << hexify(intptr_t(n))
               << ", version=" << version << ">" << endl);
  string sk(k.str());
  if (!(t->get_flags() & transaction::TXN_FLAG_LOW_LEVEL_SCAN)) {
    transaction::key_range_t r =
      invoked ? transaction::key_range_t(next_key(prev_key), sk) :
                transaction::key_range_t(lower, sk);
    VERBOSE(cerr << "  range: " << r << endl);
    if (!r.is_empty_range())
      ctx->add_absent_range(r);
  }
  prev_key = sk;
  invoked = true;
  value_type local_v = 0;
  bool local_read = ctx->local_search_str(sk, local_v);
  bool ret = true; // true means keep going, false means stop
  if (local_read && local_v)
    ret = caller_callback->invoke(k, local_v);
  transaction::read_set_map::const_iterator it =
    ctx->read_set.find(sk);
  if (it == ctx->read_set.end()) {
    transaction::logical_node *ln = (transaction::logical_node *) v;
    INVARIANT(ln);
    prefetch_node(ln);
    transaction::tid_t start_t = 0;
    transaction::record_t r = 0;
    pair<bool, transaction::tid_t> snapshot_tid_t = t->consistent_snapshot_tid();
    transaction::tid_t snapshot_tid = snapshot_tid_t.first ? snapshot_tid_t.second : transaction::MAX_TID;
    if (unlikely(!ln->stable_read(snapshot_tid, start_t, r) || !t->can_read_tid(start_t))) {
      t->abort();
      throw transaction_abort_exception();
    }
    transaction::read_record_t *read_rec = &ctx->read_set[sk];
    read_rec->t = start_t;
    read_rec->r = r;
    read_rec->ln = ln;
    VERBOSE(cerr << "read <t=" << start_t << ", r=" << size_t(r)
                 << "> (local_read=" << local_read << ")" << endl);
    if (!local_read && r)
      ret = caller_callback->invoke(k, r);
  }
  if (!ret)
    caller_stopped = true;
  return ret;
}

bool
txn_btree::absent_range_validation_callback::invoke(const key_type &k, value_type v)
{
  transaction::logical_node *ln = (transaction::logical_node *) v;
  INVARIANT(ln);
  VERBOSE(cerr << "absent_range_validation_callback: key " << k
               << " found logical_node 0x" << hexify(ln) << endl);

  bool did_write = ctx->write_set.find(k.str()) != ctx->write_set.end();
  // NB(stephentu): I don't think it matters here whether or not we use
  // snapshot_tid or MIN_TID, since this record did not exist @ snapshot_tid,
  // so any new entries must be > snapshot_tid, and therefore using MIN_TID or
  // snapshot_tid gives equivalent results.
  failed_flag = did_write ?
    !ln->is_snapshot_consistent(transaction::MIN_TID, commit_tid) :
    !ln->stable_is_snapshot_consistent(transaction::MIN_TID, commit_tid);
  if (failed_flag)
    VERBOSE(cerr << "absent_range_validation_callback: key " << k
                 << " found logical_node 0x" << hexify(ln) << endl);
  return !failed_flag;
}

void
txn_btree::search_range_call(transaction &t,
                             const key_type &lower,
                             const key_type *upper,
                             search_range_callback &callback)
{
  t.ensure_active();
  transaction::txn_context &ctx = t.ctx_map[this];

  if (upper)
    VERBOSE(cerr << "txn_btree(0x" << hexify(intptr_t(this)) << ")::search_range_call [" << lower
                 << ", " << *upper << ")" << endl);
  else
    VERBOSE(cerr << "txn_btree(0x" << hexify(intptr_t(this)) << ")::search_range_call [" << lower
                 << ", +inf)" << endl);

  // many cases to consider:
  // 1) for each logical_node returned from the scan, we need to
  //    record it in our local read set. there are several cases:
  //    A) if the logical_node corresponds to a key we have written, then
  //       we emit the version from the local write set
  //    B) if the logical_node corresponds to a key we have previous read,
  //       then we emit the previous version
  // 2) for each logical_node node *not* returned from the scan, we need
  //    to record its absense. we optimize this by recording the absense
  //    of contiguous ranges
  if (unlikely(upper && *upper <= lower))
    return;

  txn_search_range_callback c(&t, &ctx, lower, upper, &callback);
  underlying_btree.search_range_call(lower, upper, c);
  if (c.caller_stopped)
    return;
  if (!(t.get_flags() & transaction::TXN_FLAG_LOW_LEVEL_SCAN)) {
    if (upper)
      ctx.add_absent_range(
          transaction::key_range_t(
            c.invoked ? next_key(c.prev_key) : lower.str(), *upper));
    else
      ctx.add_absent_range(
          transaction::key_range_t(
            c.invoked ? next_key(c.prev_key) : lower.str()));
  }
}

void
txn_btree::insert_impl(transaction &t, const key_type &k, value_type v)
{
  t.ensure_active();
  transaction::txn_context &ctx = t.ctx_map[this];
  if (unlikely(t.get_flags() & transaction::TXN_FLAG_READ_ONLY)) {
    t.abort();
    throw transaction_abort_exception();
  }
  ctx.write_set[k.str()] = v;
}

struct test_callback_ctr {
  test_callback_ctr(size_t *ctr) : ctr(ctr) {}
  inline bool
  operator()(const txn_btree::key_type &k, txn_btree::value_type v) const
  {
    (*ctr)++;
    return true;
  }
  size_t *const ctr;
};

// all combinations of txn flags to test
static uint64_t TxnFlags[] = { 0, transaction::TXN_FLAG_LOW_LEVEL_SCAN };

static void
always_assert_cond_in_txn(
    const transaction &t, bool cond,
    const char *condstr, const char *func,
    const char *filename, int lineno)
{
  if (likely(cond))
    return;
  static pthread_mutex_t g_report_lock = PTHREAD_MUTEX_INITIALIZER;
  ALWAYS_ASSERT(pthread_mutex_lock(&g_report_lock) == 0);
  cerr << func << " (" << filename << ":" << lineno << ") - Condition `"
       << condstr << "' failed!" << endl;
  t.dump_debug_info();
  sleep(1); // XXX(stephentu): give time for debug dump to reach console
            // why doesn't flushing solve this?
  abort();
  ALWAYS_ASSERT(pthread_mutex_unlock(&g_report_lock) == 0);
}

#define ALWAYS_ASSERT_COND_IN_TXN(t, cond) \
  always_assert_cond_in_txn(t, cond, #cond, __PRETTY_FUNCTION__, __FILE__, __LINE__)

template <typename TxnType>
static void
test1()
{
  for (size_t txn_flags_idx = 0;
       txn_flags_idx < ARRAY_NELEMS(TxnFlags);
       txn_flags_idx++) {
    uint64_t txn_flags = TxnFlags[txn_flags_idx];

    VERBOSE(cerr << "Testing with flags=0x" << hexify(txn_flags) << endl);

    txn_btree btr;

    struct rec { uint64_t v; };
    rec recs[10];
    for (size_t i = 0; i < ARRAY_NELEMS(recs); i++)
      recs[i].v = 0;

    {
      TxnType t(txn_flags);
      txn_btree::value_type v;
      ALWAYS_ASSERT_COND_IN_TXN(t, !btr.search(t, u64_varkey(0), v));
      btr.insert(t, u64_varkey(0), (txn_btree::value_type) &recs[0]);
      ALWAYS_ASSERT_COND_IN_TXN(t, btr.search(t, u64_varkey(0), v));
      ALWAYS_ASSERT_COND_IN_TXN(t, v == (txn_btree::value_type) &recs[0]);
      t.commit(true);
      VERBOSE(cerr << "------" << endl);
    }

    {
      TxnType t0(txn_flags), t1(txn_flags);
      txn_btree::value_type v0, v1;

      ALWAYS_ASSERT_COND_IN_TXN(t0, btr.search(t0, u64_varkey(0), v0));
      ALWAYS_ASSERT_COND_IN_TXN(t0, v0 == (txn_btree::value_type) &recs[0]);

      btr.insert(t0, u64_varkey(0), (txn_btree::value_type) &recs[1]);

      ALWAYS_ASSERT_COND_IN_TXN(t1, btr.search(t1, u64_varkey(0), v1));
      ALWAYS_ASSERT_COND_IN_TXN(t1, v1 == (txn_btree::value_type) &recs[0]);

      t0.commit(true);
      try {
        t1.commit(true);
        // if we have a consistent snapshot, then this txn should not abort
        ALWAYS_ASSERT_COND_IN_TXN(t1, t1.consistent_snapshot_tid().first);
      } catch (transaction_abort_exception &e) {
        // if we dont have a snapshot, then we expect an abort
        ALWAYS_ASSERT_COND_IN_TXN(t1, !t1.consistent_snapshot_tid().first);
      }
      VERBOSE(cerr << "------" << endl);
    }

    {
      // racy insert
      TxnType t0(txn_flags), t1(txn_flags);
      txn_btree::value_type v0, v1;

      ALWAYS_ASSERT_COND_IN_TXN(t0, btr.search(t0, u64_varkey(0), v0));
      ALWAYS_ASSERT_COND_IN_TXN(t0, v0 == (txn_btree::value_type) &recs[1]);
      btr.insert(t0, u64_varkey(0), (txn_btree::value_type) &recs[2]);

      ALWAYS_ASSERT_COND_IN_TXN(t1, btr.search(t1, u64_varkey(0), v1));
      ALWAYS_ASSERT_COND_IN_TXN(t1, v1 == (txn_btree::value_type) &recs[1]);
      btr.insert(t1, u64_varkey(0), (txn_btree::value_type) &recs[3]);

      t0.commit(true); // succeeds

      try {
        t1.commit(true); // fails
        ALWAYS_ASSERT_COND_IN_TXN(t1, false);
      } catch (transaction_abort_exception &e) {

      }
      VERBOSE(cerr << "------" << endl);
    }

    {
      // racy scan
      TxnType t0(txn_flags), t1(txn_flags);

      u64_varkey vend(5);
      size_t ctr = 0;
      btr.search_range(t0, u64_varkey(1), &vend, test_callback_ctr(&ctr));
      ALWAYS_ASSERT_COND_IN_TXN(t0, ctr == 0);

      btr.insert(t1, u64_varkey(2), (txn_btree::value_type) &recs[4]);
      t1.commit(true);

      btr.insert(t0, u64_varkey(0), (txn_btree::value_type) &recs[0]);
      try {
        t0.commit(true); // fails
        ALWAYS_ASSERT_COND_IN_TXN(t0, false);
      } catch (transaction_abort_exception &e) {

      }
      VERBOSE(cerr << "------" << endl);
    }

    {
      TxnType t(txn_flags);
      u64_varkey vend(20);
      size_t ctr = 0;
      btr.search_range(t, u64_varkey(10), &vend, test_callback_ctr(&ctr));
      ALWAYS_ASSERT_COND_IN_TXN(t, ctr == 0);
      btr.insert(t, u64_varkey(15), (txn_btree::value_type) &recs[5]);
      t.commit(true);
      VERBOSE(cerr << "------" << endl);
    }
  }
}

template <typename TxnType>
static void
test2()
{
  for (size_t txn_flags_idx = 0;
       txn_flags_idx < ARRAY_NELEMS(TxnFlags);
       txn_flags_idx++) {
    uint64_t txn_flags = TxnFlags[txn_flags_idx];
    txn_btree btr;
    for (size_t i = 0; i < 100; i++) {
      TxnType t(txn_flags);
      btr.insert(t, u64_varkey(i), (txn_btree::value_type) 123);
      t.commit(true);
    }
  }
}

template <typename TxnType>
static void
test_multi_btree()
{
  for (size_t txn_flags_idx = 0;
       txn_flags_idx < ARRAY_NELEMS(TxnFlags);
       txn_flags_idx++) {
    uint64_t txn_flags = TxnFlags[txn_flags_idx];
    txn_btree btr0, btr1;
    for (size_t i = 0; i < 100; i++) {
      TxnType t(txn_flags);
      btr0.insert(t, u64_varkey(i), (txn_btree::value_type) 123);
      btr1.insert(t, u64_varkey(i), (txn_btree::value_type) 123);
      t.commit(true);
    }

    for (size_t i = 0; i < 100; i++) {
      TxnType t(txn_flags);
      txn_btree::value_type v0 = 0, v1 = 0;
      bool ret0 = btr0.search(t, u64_varkey(i), v0);
      bool ret1 = btr1.search(t, u64_varkey(i), v1);
      t.commit(true);
      ALWAYS_ASSERT_COND_IN_TXN(t, ret0);
      ALWAYS_ASSERT_COND_IN_TXN(t, ret1);
      ALWAYS_ASSERT_COND_IN_TXN(t, v0 == (txn_btree::value_type) 123);
      ALWAYS_ASSERT_COND_IN_TXN(t, v1 == (txn_btree::value_type) 123);
    }
  }
}

template <typename TxnType>
static void
test_read_only_snapshot()
{
  for (size_t txn_flags_idx = 0;
       txn_flags_idx < ARRAY_NELEMS(TxnFlags);
       txn_flags_idx++) {
    uint64_t txn_flags = TxnFlags[txn_flags_idx];
    txn_btree btr;

    struct rec { uint64_t v; };
    rec recs[2];
    for (size_t i = 0; i < ARRAY_NELEMS(recs); i++)
      recs[i].v = 0;

    {
      TxnType t(txn_flags);
      btr.insert(t, u64_varkey(0), (txn_btree::value_type) &recs[0]);
      t.commit(true);
    }

    // XXX(stephentu): HACK! we need to wait for the GC to
    // compute a new consistent snapshot version that includes this
    // latest update
    txn_epoch_sync<TxnType>::sync();

    {
      TxnType t0(txn_flags), t1(txn_flags | transaction::TXN_FLAG_READ_ONLY);
      txn_btree::value_type v0, v1;
      ALWAYS_ASSERT_COND_IN_TXN(t0, btr.search(t0, u64_varkey(0), v0));
      ALWAYS_ASSERT_COND_IN_TXN(t0, v0 == (txn_btree::value_type) &recs[0]);

      btr.insert(t0, u64_varkey(0), (txn_btree::value_type) &recs[1]);

      ALWAYS_ASSERT_COND_IN_TXN(t1, btr.search(t1, u64_varkey(0), v1));
      ALWAYS_ASSERT_COND_IN_TXN(t1, v1 == (txn_btree::value_type) &recs[0]);

      t0.commit(true);
      t1.commit(true);
    }
  }
}


namespace test_long_keys_ns {

static inline string
make_long_key(int32_t a, int32_t b, int32_t c, int32_t d) {
  char buf[4 * sizeof(int32_t)];
  int32_t *p = (int32_t *) &buf[0];
  *p++ = a;
  *p++ = b;
  *p++ = c;
  *p++ = d;
  return string(&buf[0], ARRAY_NELEMS(buf));
}

class counting_scan_callback : public txn_btree::search_range_callback {
public:
  counting_scan_callback() : ctr(0) {}

  virtual bool
  invoke(const txn_btree::key_type &k, txn_btree::value_type v)
  {
    ctr++;
    return true;
  }
  size_t ctr;
};

}

template <typename TxnType>
static void
test_long_keys()
{
  using namespace test_long_keys_ns;
  const size_t N = 10;
  for (size_t txn_flags_idx = 0;
       txn_flags_idx < ARRAY_NELEMS(TxnFlags);
       txn_flags_idx++) {
    uint64_t txn_flags = TxnFlags[txn_flags_idx];
    txn_btree btr;

    {
      TxnType t(txn_flags);
      struct { uint64_t v; } r;
      for (size_t a = 0; a < N; a++)
        for (size_t b = 0; b < N; b++)
          for (size_t c = 0; c < N; c++)
            for (size_t d = 0; d < N; d++) {
              string k = make_long_key(a, b, c, d);
              btr.insert(t, varkey(k), (txn_btree::value_type) &r);
            }
      t.commit(true);
    }

    {
      TxnType t(txn_flags);
      string lowkey_s = make_long_key(1, 2, 3, 0);
      string highkey_s = make_long_key(1, 2, 3, N);
      varkey highkey(highkey_s);
      counting_scan_callback c;
      btr.search_range_call(t, varkey(lowkey_s), &highkey, c);
      t.commit(true);
      ALWAYS_ASSERT_COND_IN_TXN(t, c.ctr == N);
    }
  }
}

template <typename TxnType>
static void
test_long_keys2()
{
  using namespace test_long_keys_ns;
  for (size_t txn_flags_idx = 0;
       txn_flags_idx < ARRAY_NELEMS(TxnFlags);
       txn_flags_idx++) {
    uint64_t txn_flags = TxnFlags[txn_flags_idx];

    const uint8_t lowkey_cstr[] = {
      0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x45, 0x49, 0x4E, 0x47,
      0x41, 0x54, 0x49, 0x4F, 0x4E, 0x45, 0x49, 0x4E, 0x47, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00,
    };
    const string lowkey_s((const char *) &lowkey_cstr[0], ARRAY_NELEMS(lowkey_cstr));
    const uint8_t highkey_cstr[] = {
      0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x45, 0x49, 0x4E, 0x47,
      0x41, 0x54, 0x49, 0x4F, 0x4E, 0x45, 0x49, 0x4E, 0x47, 0x00, 0x00, 0x00,
      0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
      0xFF, 0xFF, 0xFF, 0xFF
    };
    const string highkey_s((const char *) &highkey_cstr[0], ARRAY_NELEMS(highkey_cstr));

    struct { uint64_t v; } r;
    txn_btree btr;
    {
      TxnType t(txn_flags);
      btr.insert(t, varkey(lowkey_s), (txn_btree::value_type) &r);
      t.commit(true);
    }

    {
      TxnType t(txn_flags);
      txn_btree::value_type v = 0;
      ALWAYS_ASSERT_COND_IN_TXN(t, btr.search(t, varkey(lowkey_s), v));
      t.commit(true);
      ALWAYS_ASSERT_COND_IN_TXN(t, v == (txn_btree::value_type) &r);
    }

    {
      TxnType t(txn_flags);
      counting_scan_callback c;
      varkey highkey(highkey_s);
      btr.search_range_call(t, varkey(lowkey_s), &highkey, c);
      t.commit(true);
      ALWAYS_ASSERT_COND_IN_TXN(t, c.ctr == 1);
    }
  }
}

class txn_btree_worker : public ndb_thread {
public:
  txn_btree_worker(txn_btree &btr, uint64_t txn_flags)
    : btr(&btr), txn_flags(txn_flags) {}
protected:
  txn_btree *const btr;
  const uint64_t txn_flags;
};

namespace mp_test1_ns {
  // read-modify-write test (counters)

  struct record {
    record() : v(0) {}
    uint64_t v;
  };

  const size_t niters = 1000;

  template <typename TxnType>
  class worker : public txn_btree_worker {
  public:
    worker(txn_btree &btr, uint64_t txn_flags)
      : txn_btree_worker(btr, txn_flags) {}
    ~worker()
    {
      for (vector<record *>::iterator it = recs.begin();
           it != recs.end(); ++it)
        delete *it;
    }
    virtual void run()
    {
      for (size_t i = 0; i < niters; i++) {
      retry:
        TxnType t(txn_flags);
        record *rec = new record;
        recs.push_back(rec);
        try {
          txn_btree::value_type v = 0;
          if (!btr->search(t, u64_varkey(0), v)) {
            rec->v = 1;
          } else {
            *rec = *((record *)v);
            rec->v++;
          }
          btr->insert(t, u64_varkey(0), (txn_btree::value_type) rec);
          t.commit(true);
        } catch (transaction_abort_exception &e) {
          goto retry;
        }
      }
    }
  private:
    vector<record *> recs;
  };

}

template <typename TxnType>
static void
mp_test1()
{
  using namespace mp_test1_ns;

  for (size_t txn_flags_idx = 0;
       txn_flags_idx < ARRAY_NELEMS(TxnFlags);
       txn_flags_idx++) {
    uint64_t txn_flags = TxnFlags[txn_flags_idx];
    txn_btree btr;

    worker<TxnType> w0(btr, txn_flags);
    worker<TxnType> w1(btr, txn_flags);

    w0.start(); w1.start();
    w0.join(); w1.join();

    TxnType t(txn_flags);
    txn_btree::value_type v = 0;
    ALWAYS_ASSERT_COND_IN_TXN(t, btr.search(t, u64_varkey(0), v));
    ALWAYS_ASSERT_COND_IN_TXN(t,  ((record *) v)->v == (niters * 2) );
    t.commit(true);
  }
}

namespace mp_test2_ns {

  static const uint64_t ctr_key = 0;
  static const uint64_t range_begin = 100;
  static const uint64_t range_end = 200;

  static volatile bool running = true;

  template <typename TxnType>
  class mutate_worker : public txn_btree_worker {
  public:
    mutate_worker(txn_btree &btr, uint64_t flags)
      : txn_btree_worker(btr, flags), naborts(0) {}
    virtual void run()
    {
      while (running) {
        for (size_t i = range_begin; running && i < range_end; i++) {
        retry:
          TxnType t(txn_flags);
          try {
            txn_btree::value_type v = 0, v_ctr = 0;
            ALWAYS_ASSERT_COND_IN_TXN(t, btr->search(t, u64_varkey(ctr_key), v_ctr));
            ALWAYS_ASSERT_COND_IN_TXN(t, size_t(v_ctr) > 1);
            if (btr->search(t, u64_varkey(i), v)) {
              ALWAYS_ASSERT_COND_IN_TXN(t, v == (txn_btree::value_type) i);
              btr->remove(t, u64_varkey(i));
              v_ctr = (txn_btree::value_type)(size_t(v_ctr) - 1);
            } else {
              btr->insert(t, u64_varkey(i), (txn_btree::value_type) i);
              v_ctr = (txn_btree::value_type)(size_t(v_ctr) + 1);
            }
            btr->insert(t, u64_varkey(ctr_key), v_ctr);
            t.commit(true);
          } catch (transaction_abort_exception &e) {
            naborts++;
            goto retry;
          }
        }
      }
    }
    size_t naborts;
  };

  template <typename TxnType>
  class reader_worker : public txn_btree_worker,
                        public txn_btree::search_range_callback {
  public:
    reader_worker(txn_btree &btr, uint64_t flags)
      : txn_btree_worker(btr, flags), validations(0), naborts(0), ctr(0) {}
    virtual bool invoke(const txn_btree::key_type &k, txn_btree::value_type v)
    {
      ctr++;
      return true;
    }
    virtual void run()
    {
      while (running) {
        try {
          TxnType t(txn_flags);
          txn_btree::value_type v_ctr = 0;
          ALWAYS_ASSERT_COND_IN_TXN(t, btr->search(t, u64_varkey(ctr_key), v_ctr));
          ctr = 0;
          u64_varkey kend(range_end);
          btr->search_range_call(t, u64_varkey(range_begin), &kend, *this);
          t.commit(true);
          ALWAYS_ASSERT_COND_IN_TXN(t, ctr == size_t(v_ctr));
          validations++;
        } catch (transaction_abort_exception &e) {
          naborts++;
        }
      }
    }
    size_t validations;
    size_t naborts;
  private:
    size_t ctr;
  };
}

template <typename TxnType>
static void
mp_test2()
{
  using namespace mp_test2_ns;
  for (size_t txn_flags_idx = 0;
       txn_flags_idx < ARRAY_NELEMS(TxnFlags);
       txn_flags_idx++) {
    uint64_t txn_flags = TxnFlags[txn_flags_idx];
    txn_btree btr;
    {
      TxnType t(txn_flags);
      size_t n = 0;
      for (size_t i = range_begin; i < range_end; i++)
        if ((i % 2) == 0) {
          btr.insert(t, u64_varkey(i), (txn_btree::value_type) i);
          n++;
        }
      btr.insert(t, u64_varkey(ctr_key), (txn_btree::value_type) n);
      t.commit(true);
    }

    // XXX(stephentu): HACK! we need to wait for the GC to
    // compute a new consistent snapshot version that includes this
    // latest update
    txn_epoch_sync<TxnType>::sync();

    mutate_worker<TxnType> w0(btr, txn_flags);
    reader_worker<TxnType> w1(btr, txn_flags);
    reader_worker<TxnType> w2(btr, txn_flags | transaction::TXN_FLAG_READ_ONLY);

    running = true;
    w0.start();
    w1.start();
    w2.start();
    sleep(10);
    running = false;
    w0.join();
    w1.join();
    w2.join();

    cerr << "mutate naborts: " << w0.naborts << endl;
    cerr << "reader naborts: " << w1.naborts << endl;
    cerr << "reader validations: " << w1.validations << endl;
    cerr << "read-only reader naborts: " << w2.naborts << endl;
    cerr << "read-only reader validations: " << w2.validations << endl;

  }
}

namespace mp_test3_ns {

  static const size_t amount_per_person = 100;
  static const size_t naccounts = 100;
  static const size_t niters = 1000000;
  struct record { uint64_t v; };

  template <typename TxnType>
  class transfer_worker : public txn_btree_worker {
  public:
    transfer_worker(txn_btree &btr, uint64_t flags, unsigned long seed)
      : txn_btree_worker(btr, flags), seed(seed) {}
    ~transfer_worker()
    {
      for (vector<record *>::iterator it = recs.begin();
           it != recs.end(); ++it)
        delete *it;
    }
    virtual void run()
    {
      fast_random r(seed);
      for (size_t i = 0; i < niters; i++) {
        record *arec_new = new record;
        record *brec_new = new record;
        recs.push_back(arec_new);
        recs.push_back(brec_new);
      retry:
        try {
          TxnType t(txn_flags);
          uint64_t a = r.next() % naccounts;
          uint64_t b = r.next() % naccounts;
          while (unlikely(a == b))
            b = r.next() % naccounts;
          txn_btree::value_type arecv, brecv;
          ALWAYS_ASSERT_COND_IN_TXN(t, btr->search(t, u64_varkey(a), arecv));
          ALWAYS_ASSERT_COND_IN_TXN(t, btr->search(t, u64_varkey(b), brecv));
          record *arec = (record *) arecv;
          record *brec = (record *) brecv;
          if (arec->v == 0) {
            t.abort();
          } else {
            uint64_t xfer = (arec->v > 1) ? (r.next() % (arec->v - 1) + 1) : 1;
            arec_new->v = arec->v - xfer;
            brec_new->v = brec->v + xfer;
            btr->insert(t, u64_varkey(a), (txn_btree::value_type) arec_new);
            btr->insert(t, u64_varkey(b), (txn_btree::value_type) brec_new);
            t.commit(true);
          }
        } catch (transaction_abort_exception &e) {
          goto retry;
        }
      }
    }
  private:
    const unsigned long seed;
    vector<record *> recs;
  };

  template <typename TxnType>
  class invariant_worker_scan : public txn_btree_worker,
                                public txn_btree::search_range_callback {
  public:
    invariant_worker_scan(txn_btree &btr, uint64_t flags)
      : txn_btree_worker(btr, flags), running(true),
        validations(0), naborts(0), sum(0) {}
    virtual void run()
    {
      while (running) {
        try {
          TxnType t(txn_flags);
          sum = 0;
          btr->search_range_call(t, u64_varkey(0), NULL, *this);
          t.commit(true);
          ALWAYS_ASSERT_COND_IN_TXN(t, sum == (naccounts * amount_per_person));
          validations++;
        } catch (transaction_abort_exception &e) {
          naborts++;
        }
      }
    }
    virtual bool invoke(const txn_btree::key_type &k, txn_btree::value_type v)
    {
      sum += ((record *) v)->v;
      return true;
    }
    volatile bool running;
    size_t validations;
    size_t naborts;
    uint64_t sum;
  };

  template <typename TxnType>
  class invariant_worker_1by1 : public txn_btree_worker {
  public:
    invariant_worker_1by1(txn_btree &btr, uint64_t flags)
      : txn_btree_worker(btr, flags), running(true),
        validations(0), naborts(0) {}
    virtual void run()
    {
      while (running) {
        try {
          TxnType t(txn_flags);
          uint64_t sum = 0;
          for (uint64_t i = 0; i < naccounts; i++) {
            txn_btree::value_type v = 0;
            ALWAYS_ASSERT_COND_IN_TXN(t, btr->search(t, u64_varkey(i), v));
            sum += ((record *) v)->v;
          }
          t.commit(true);
          if (sum != (naccounts * amount_per_person)) {
            cerr << "sum: " << sum << endl;
            cerr << "naccounts * amount_per_person: " << (naccounts * amount_per_person) << endl;
          }
          ALWAYS_ASSERT_COND_IN_TXN(t, sum == (naccounts * amount_per_person));
          validations++;
        } catch (transaction_abort_exception &e) {
          naborts++;
        }
      }
    }
    volatile bool running;
    size_t validations;
    size_t naborts;
  };

}

template <typename TxnType>
static void
mp_test3()
{
  using namespace mp_test3_ns;

  for (size_t txn_flags_idx = 0;
       txn_flags_idx < ARRAY_NELEMS(TxnFlags);
       txn_flags_idx++) {
    uint64_t txn_flags = TxnFlags[txn_flags_idx];

    vector<record *> recs;
    txn_btree btr;
    {
      TxnType t(txn_flags);
      for (uint64_t i = 0; i < naccounts; i++) {
        record *r = new record;
        recs.push_back(r);
        r->v = amount_per_person;
        btr.insert(t, u64_varkey(i), (txn_btree::value_type) r);
      }
      t.commit(true);
    }

    txn_epoch_sync<TxnType>::sync();

    transfer_worker<TxnType> w0(btr, txn_flags, 342),
                             w1(btr, txn_flags, 93852),
                             w2(btr, txn_flags, 23085),
                             w3(btr, txn_flags, 859438989);
    invariant_worker_scan<TxnType> w4(btr, txn_flags);
    invariant_worker_1by1<TxnType> w5(btr, txn_flags);
    invariant_worker_scan<TxnType> w6(btr, txn_flags | transaction::TXN_FLAG_READ_ONLY);
    invariant_worker_1by1<TxnType> w7(btr, txn_flags | transaction::TXN_FLAG_READ_ONLY);

    w0.start(); w1.start(); w2.start(); w3.start(); w4.start(); w5.start(); w6.start(); w7.start();
    w0.join(); w1.join(); w2.join(); w3.join();
    w4.running = false; w5.running = false; w6.running = false; w7.running = false;
    __sync_synchronize();
    w4.join(); w5.join(); w6.join(); w7.join();

    cerr << "scan validations: " << w4.validations << ", scan aborts: " << w4.naborts << endl;
    cerr << "1by1 validations: " << w5.validations << ", 1by1 aborts: " << w5.naborts << endl;
    cerr << "scan-readonly validations: " << w6.validations << ", scan-readonly aborts: " << w6.naborts << endl;
    cerr << "1by1-readonly validations: " << w7.validations << ", 1by1-readonly aborts: " << w7.naborts << endl;

    for (vector<record *>::iterator it = recs.begin();
        it != recs.end(); ++it)
      delete *it;
  }
}

namespace read_only_perf_ns {
  const size_t nkeys = 140000000; // 140M
  //const size_t nkeys = 100000; // 100K

  unsigned long seeds[] = {
    9576455804445224191ULL,
    3303315688255411629ULL,
    3116364238170296072ULL,
    641702699332002535ULL,
    17755947590284612420ULL,
    13349066465957081273ULL,
    16389054441777092823ULL,
    2687412585397891607ULL,
    16665670053534306255ULL,
    5166823197462453937ULL,
    1252059952779729626ULL,
    17962022827457676982ULL,
    940911318964853784ULL,
    479878990529143738ULL,
    250864516707124695ULL,
    8507722621803716653ULL,
  };

  volatile bool running = false;

  template <typename TxnType>
  class worker : public txn_btree_worker {
  public:
    worker(unsigned int seed, txn_btree &btr, uint64_t txn_flags)
      : txn_btree_worker(btr, txn_flags), n(0), seed(seed) {}
    virtual void run()
    {
      fast_random r(seed);
      while (running) {
        uint64_t k = r.next() % nkeys;
      retry:
        try {
          TxnType t(txn_flags);
          btree::value_type v = 0;
          bool found = btr->search(t, u64_varkey(k), v);
          t.commit(true);
          ALWAYS_ASSERT_COND_IN_TXN(t, found);
          ALWAYS_ASSERT_COND_IN_TXN(t, v == (btree::value_type) (k + 1));
        } catch (transaction_abort_exception &e) {
          goto retry;
        }
        n++;
      }
    }
    uint64_t n;
  private:
    unsigned int seed;
  };
}

template <typename TxnType>
static void
read_only_perf()
{
  using namespace read_only_perf_ns;
  for (size_t txn_flags_idx = 0;
       txn_flags_idx < ARRAY_NELEMS(TxnFlags);
       txn_flags_idx++) {
    uint64_t txn_flags = TxnFlags[txn_flags_idx];

    txn_btree btr;

    {
      const size_t nkeyspertxn = 100000;
      for (size_t i = 0; i < nkeys / nkeyspertxn; i++) {
        TxnType t;
        size_t end = (i == (nkeys / nkeyspertxn - 1)) ? nkeys : ((i + 1) * nkeyspertxn);
        for (size_t j = i * nkeyspertxn; j < end; j++)
          btr.insert(t, u64_varkey(j), (btree::value_type) (j + 1));
        t.commit(true);
        cerr << "batch " << i << " completed" << endl;
      }
      cerr << "btree loaded, test starting" << endl;
    }

    vector<worker<TxnType> *> workers;
    for (size_t i = 0; i < ARRAY_NELEMS(seeds); i++)
      workers.push_back(new worker<TxnType>(seeds[i], btr, txn_flags));

    running = true;
    timer t;
    COMPILER_MEMORY_FENCE;
    for (size_t i = 0; i < ARRAY_NELEMS(seeds); i++)
      workers[i]->start();
    sleep(30);
    COMPILER_MEMORY_FENCE;
    running = false;
    COMPILER_MEMORY_FENCE;
    uint64_t total_n = 0;
    for (size_t i = 0; i < ARRAY_NELEMS(seeds); i++) {
      workers[i]->join();
      total_n += workers[i]->n;
      delete workers[i];
    }

    double agg_throughput = double(total_n) / (double(t.lap()) / 1000000.0);
    double avg_per_core_throughput = agg_throughput / double(ARRAY_NELEMS(seeds));

    cerr << "agg_read_throughput: " << agg_throughput << " gets/sec" << endl;
    cerr << "avg_per_core_read_throughput: " << avg_per_core_throughput << " gets/sec/core" << endl;
  }
}

void
txn_btree::Test()
{
  cerr << "Test proto1" << endl;
  test1<transaction_proto1>();
  test2<transaction_proto1>();
  test_multi_btree<transaction_proto1>();
  test_read_only_snapshot<transaction_proto1>();
  test_long_keys<transaction_proto1>();
  test_long_keys2<transaction_proto1>();
  mp_test1<transaction_proto1>();
  mp_test2<transaction_proto1>();
  mp_test3<transaction_proto1>();

  cerr << "Test proto2" << endl;
  test1<transaction_proto2>();
  test2<transaction_proto2>();
  test_multi_btree<transaction_proto2>();
  test_read_only_snapshot<transaction_proto2>();
  test_long_keys<transaction_proto2>();
  test_long_keys2<transaction_proto2>();
  mp_test1<transaction_proto2>();
  mp_test2<transaction_proto2>();
  mp_test3<transaction_proto2>();

  //read_only_perf<transaction_proto1>();
  //read_only_perf<transaction_proto2>();
}
