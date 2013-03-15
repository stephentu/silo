#include <unistd.h>
#include <limits>

#include "txn.h"
#include "txn_proto1_impl.h"
#include "txn_proto2_impl.h"
#include "txn_btree.h"
#include "txn_btree_impl.h"
#include "thread.h"
#include "util.h"
#include "macros.h"
#include "tuple.h"

#include "scopedperf.hh"

using namespace std;
using namespace util;

struct test_callback_ctr {
  test_callback_ctr(size_t *ctr) : ctr(ctr) {}
  inline bool
  operator()(const string &k, const uint8_t *v, size_t sz) const
  {
    (*ctr)++;
    return true;
  }
  size_t *const ctr;
};

// all combinations of txn flags to test
static uint64_t TxnFlags[] = { 0, transaction_base::TXN_FLAG_LOW_LEVEL_SCAN };

template <typename P>
static void
always_assert_cond_in_txn(
    const P &t, bool cond,
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

template <typename P>
static inline void
AssertSuccessfulCommit(P &t)
{
  ALWAYS_ASSERT_COND_IN_TXN(t, t.commit(false));
}

template <typename P>
static inline void
AssertFailedCommit(P &t)
{
  ALWAYS_ASSERT_COND_IN_TXN(t, !t.commit(false));
}

template <typename T>
inline void
AssertByteEquality(const T &t, const uint8_t * v, size_t sz)
{
  ALWAYS_ASSERT(sizeof(T) == sz);
  bool success = memcmp(&t, v, sz) == 0;
  if (!success) {
    cerr << "expecting: " << hexify(string((const char *) &t, sizeof(T))) << endl;
    cerr << "got      : " << hexify(string((const char *) v, sz)) << endl;
    ALWAYS_ASSERT(false);
  }
}

template <typename T>
inline void
AssertByteEquality(const T &t, const string &v)
{
  AssertByteEquality(t, (const uint8_t *) v.data(), v.size());
}

struct rec {
  rec() : v() {}
  rec(uint64_t v) : v(v) {}
  uint64_t v;
};

static inline ostream &
operator<<(ostream &o, const rec &r)
{
  o << "[rec=" << r.v << "]";
  return o;
}

template <template <typename> class TxnType, typename Traits>
static void
test1()
{
  for (size_t txn_flags_idx = 0;
       txn_flags_idx < ARRAY_NELEMS(TxnFlags);
       txn_flags_idx++) {
    const uint64_t txn_flags = TxnFlags[txn_flags_idx];

    VERBOSE(cerr << "Testing with flags=0x" << hexify(txn_flags) << endl);

    txn_btree<TxnType> btr(sizeof(rec));

    {
      TxnType<Traits> t(txn_flags);
      string v;
      ALWAYS_ASSERT_COND_IN_TXN(t, !btr.search(t, u64_varkey(0), v));
      btr.insert_object(t, u64_varkey(0), rec(0));
      ALWAYS_ASSERT_COND_IN_TXN(t, btr.search(t, u64_varkey(0), v));
      ALWAYS_ASSERT_COND_IN_TXN(t, !v.empty());
      AssertByteEquality(rec(0), v);
      AssertSuccessfulCommit(t);
      VERBOSE(cerr << "------" << endl);
    }

    {
      TxnType<Traits> t0(txn_flags), t1(txn_flags);
      string v0, v1;

      ALWAYS_ASSERT_COND_IN_TXN(t0, btr.search(t0, u64_varkey(0), v0));
      ALWAYS_ASSERT_COND_IN_TXN(t0, !v0.empty());
      AssertByteEquality(rec(0), v0);

      btr.insert_object(t0, u64_varkey(0), rec(1));
      ALWAYS_ASSERT_COND_IN_TXN(t1, btr.search(t1, u64_varkey(0), v1));
      ALWAYS_ASSERT_COND_IN_TXN(t1, !v1.empty());

      if (t1.consistent_snapshot_tid().first)
        // we don't read-uncommitted for consistent snapshot
        AssertByteEquality(rec(0), v1);

      AssertSuccessfulCommit(t0);
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
      TxnType<Traits> t0(txn_flags), t1(txn_flags);
      string v0, v1;

      ALWAYS_ASSERT_COND_IN_TXN(t0, btr.search(t0, u64_varkey(0), v0));
      ALWAYS_ASSERT_COND_IN_TXN(t0, !v0.empty());
      AssertByteEquality(rec(1), v0);

      btr.insert_object(t0, u64_varkey(0), rec(2));
      ALWAYS_ASSERT_COND_IN_TXN(t1, btr.search(t1, u64_varkey(0), v1));
      ALWAYS_ASSERT_COND_IN_TXN(t1, !v1.empty());
      // our API allows v1 to be a dirty read though (t1 will abort)

      btr.insert_object(t1, u64_varkey(0), rec(3));

      AssertSuccessfulCommit(t0);
      AssertFailedCommit(t1);
      VERBOSE(cerr << "------" << endl);
    }

    {
      // racy scan
      TxnType<Traits> t0(txn_flags), t1(txn_flags);

      const u64_varkey vend(5);
      size_t ctr = 0;
      btr.search_range(t0, u64_varkey(1), &vend, test_callback_ctr(&ctr));
      ALWAYS_ASSERT_COND_IN_TXN(t0, ctr == 0);

      btr.insert_object(t1, u64_varkey(2), rec(4));
      AssertSuccessfulCommit(t1);

      btr.insert_object(t0, u64_varkey(0), rec(0));
      AssertFailedCommit(t0);
      VERBOSE(cerr << "------" << endl);
    }

    {
      TxnType<Traits> t(txn_flags);
      const u64_varkey vend(20);
      size_t ctr = 0;
      btr.search_range(t, u64_varkey(10), &vend, test_callback_ctr(&ctr));
      ALWAYS_ASSERT_COND_IN_TXN(t, ctr == 0);
      btr.insert_object(t, u64_varkey(15), rec(5));
      AssertSuccessfulCommit(t);
      VERBOSE(cerr << "------" << endl);
    }

    txn_epoch_sync<TxnType>::sync();
    txn_epoch_sync<TxnType>::finish();
  }
}

struct bufrec { char buf[256]; };

template <template <typename> class TxnType, typename Traits>
static void
test2()
{
  for (size_t txn_flags_idx = 0;
       txn_flags_idx < ARRAY_NELEMS(TxnFlags);
       txn_flags_idx++) {
    const uint64_t txn_flags = TxnFlags[txn_flags_idx];
    txn_btree<TxnType> btr(sizeof(bufrec));
    bufrec r;
    NDB_MEMSET(r.buf, 'a', ARRAY_NELEMS(r.buf));
    for (size_t i = 0; i < 100; i++) {
      TxnType<Traits> t(txn_flags);
      btr.insert_object(t, u64_varkey(i), r);
      AssertSuccessfulCommit(t);
    }
    for (size_t i = 0; i < 100; i++) {
      TxnType<Traits> t(txn_flags);
      string v;
      ALWAYS_ASSERT_COND_IN_TXN(t, btr.search(t, u64_varkey(i), v));
      AssertByteEquality(r, v);
      AssertSuccessfulCommit(t);
    }
    txn_epoch_sync<TxnType>::sync();
    txn_epoch_sync<TxnType>::finish();
  }
}

template <template <typename> class TxnType, typename Traits>
static void
test_absent_key_race()
{
  for (size_t txn_flags_idx = 0;
       txn_flags_idx < ARRAY_NELEMS(TxnFlags);
       txn_flags_idx++) {
    const uint64_t txn_flags = TxnFlags[txn_flags_idx];
    txn_btree<TxnType> btr;

    {
      TxnType<Traits> t0(txn_flags), t1(txn_flags);
      string v0, v1;
      ALWAYS_ASSERT_COND_IN_TXN(t0, !btr.search(t0, u64_varkey(0), v0));
      ALWAYS_ASSERT_COND_IN_TXN(t1, !btr.search(t1, u64_varkey(0), v1));

      // t0 does a write
      btr.insert_object(t0, u64_varkey(0), rec(1));

      // t1 does a write
      btr.insert_object(t1, u64_varkey(0), rec(1));

      // t0 should win, t1 should abort
      AssertSuccessfulCommit(t0);
      AssertFailedCommit(t1);
    }

    txn_epoch_sync<TxnType>::sync();
    txn_epoch_sync<TxnType>::finish();
  }
}

template <template <typename> class TxnType, typename Traits>
static void
test_inc_value_size()
{
  for (size_t txn_flags_idx = 0;
       txn_flags_idx < ARRAY_NELEMS(TxnFlags);
       txn_flags_idx++) {
    const uint64_t txn_flags = TxnFlags[txn_flags_idx];
    txn_btree<TxnType> btr;
    const size_t upper = numeric_limits<dbtuple::node_size_type>::max();
    for (size_t i = 1; i < upper; i++) {
      const string v(i, 'a');
      TxnType<Traits> t(txn_flags);
      btr.insert(t, u64_varkey(0), (const uint8_t *) v.data(), v.size());
      AssertSuccessfulCommit(t);
    }
    txn_epoch_sync<TxnType>::sync();
    txn_epoch_sync<TxnType>::finish();
  }
}

template <template <typename> class TxnType, typename Traits>
static void
test_multi_btree()
{
  for (size_t txn_flags_idx = 0;
       txn_flags_idx < ARRAY_NELEMS(TxnFlags);
       txn_flags_idx++) {
    const uint64_t txn_flags = TxnFlags[txn_flags_idx];
    txn_btree<TxnType> btr0, btr1;
    for (size_t i = 0; i < 100; i++) {
      TxnType<Traits> t(txn_flags);
      btr0.insert_object(t, u64_varkey(i), rec(123));
      btr1.insert_object(t, u64_varkey(i), rec(123));
      AssertSuccessfulCommit(t);
    }

    for (size_t i = 0; i < 100; i++) {
      TxnType<Traits> t(txn_flags);
      string v0, v1;
      bool ret0 = btr0.search(t, u64_varkey(i), v0);
      bool ret1 = btr1.search(t, u64_varkey(i), v1);
      AssertSuccessfulCommit(t);
      ALWAYS_ASSERT_COND_IN_TXN(t, ret0);
      ALWAYS_ASSERT_COND_IN_TXN(t, !v0.empty());
      ALWAYS_ASSERT_COND_IN_TXN(t, ret1);
      ALWAYS_ASSERT_COND_IN_TXN(t, !v1.empty());
      AssertByteEquality(rec(123), v0);
      AssertByteEquality(rec(123), v1);
    }

    txn_epoch_sync<TxnType>::sync();
    txn_epoch_sync<TxnType>::finish();
  }
}

template <template <typename> class TxnType, typename Traits>
static void
test_read_only_snapshot()
{
  for (size_t txn_flags_idx = 0;
       txn_flags_idx < ARRAY_NELEMS(TxnFlags);
       txn_flags_idx++) {
    const uint64_t txn_flags = TxnFlags[txn_flags_idx];
    txn_btree<TxnType> btr;

    {
      TxnType<Traits> t(txn_flags);
      btr.insert_object(t, u64_varkey(0), rec(0));
      AssertSuccessfulCommit(t);
    }

    // XXX(stephentu): HACK! we need to wait for the GC to
    // compute a new consistent snapshot version that includes this
    // latest update
    txn_epoch_sync<TxnType>::sync();

    {
      TxnType<Traits> t0(txn_flags), t1(txn_flags | transaction_base::TXN_FLAG_READ_ONLY);
      string v0, v1;
      ALWAYS_ASSERT_COND_IN_TXN(t0, btr.search(t0, u64_varkey(0), v0));
      ALWAYS_ASSERT_COND_IN_TXN(t0, !v0.empty());
      AssertByteEquality(rec(0), v0);

      btr.insert_object(t0, u64_varkey(0), rec(1));

      ALWAYS_ASSERT_COND_IN_TXN(t1, btr.search(t1, u64_varkey(0), v1));
      ALWAYS_ASSERT_COND_IN_TXN(t1, !v1.empty());
      AssertByteEquality(rec(0), v1);

      AssertSuccessfulCommit(t0);
      AssertSuccessfulCommit(t1);
    }

    txn_epoch_sync<TxnType>::sync();
    txn_epoch_sync<TxnType>::finish();
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

template <template <typename> class Protocol>
class counting_scan_callback : public txn_btree<Protocol>::search_range_callback {
public:
  counting_scan_callback(uint64_t expect) : ctr(0), expect(expect) {}

  virtual bool
  invoke(const string &k, const uint8_t * v, size_t sz)
  {
    AssertByteEquality(rec(expect), v, sz);
    ctr++;
    return true;
  }
  size_t ctr;
  uint64_t expect;
};

}

template <template <typename> class TxnType, typename Traits>
static void
test_long_keys()
{
  using namespace test_long_keys_ns;
  const size_t N = 10;
  for (size_t txn_flags_idx = 0;
       txn_flags_idx < ARRAY_NELEMS(TxnFlags);
       txn_flags_idx++) {
    const uint64_t txn_flags = TxnFlags[txn_flags_idx];
    txn_btree<TxnType> btr;

    {
      TxnType<Traits> t(txn_flags);
      for (size_t a = 0; a < N; a++)
        for (size_t b = 0; b < N; b++)
          for (size_t c = 0; c < N; c++)
            for (size_t d = 0; d < N; d++) {
              const string k = make_long_key(a, b, c, d);
              btr.insert_object(t, varkey(k), rec(1));
            }
      AssertSuccessfulCommit(t);
    }

    {
      TxnType<Traits> t(txn_flags);
      const string lowkey_s = make_long_key(1, 2, 3, 0);
      const string highkey_s = make_long_key(1, 2, 3, N);
      const varkey highkey(highkey_s);
      counting_scan_callback<TxnType> c(1);
      btr.search_range_call(t, varkey(lowkey_s), &highkey, c);
      AssertSuccessfulCommit(t);
      if (c.ctr != N)
        cerr << "c.ctr: " << c.ctr << ", N: " << N << endl;
      ALWAYS_ASSERT_COND_IN_TXN(t, c.ctr == N);
    }

    txn_epoch_sync<TxnType>::sync();
    txn_epoch_sync<TxnType>::finish();
  }
}

template <template <typename> class TxnType, typename Traits>
static void
test_long_keys2()
{
  using namespace test_long_keys_ns;
  for (size_t txn_flags_idx = 0;
       txn_flags_idx < ARRAY_NELEMS(TxnFlags);
       txn_flags_idx++) {
    const uint64_t txn_flags = TxnFlags[txn_flags_idx];

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

    txn_btree<TxnType> btr;
    {
      TxnType<Traits> t(txn_flags);
      btr.insert_object(t, varkey(lowkey_s), rec(12345));
      AssertSuccessfulCommit(t);
    }

    {
      TxnType<Traits> t(txn_flags);
      string v;
      ALWAYS_ASSERT_COND_IN_TXN(t, btr.search(t, varkey(lowkey_s), v));
      AssertByteEquality(rec(12345), v);
      AssertSuccessfulCommit(t);
    }

    {
      TxnType<Traits> t(txn_flags);
      counting_scan_callback<TxnType> c(12345);
      const varkey highkey(highkey_s);
      btr.search_range_call(t, varkey(lowkey_s), &highkey, c);
      AssertSuccessfulCommit(t);
      ALWAYS_ASSERT_COND_IN_TXN(t, c.ctr == 1);
    }

    txn_epoch_sync<TxnType>::sync();
    txn_epoch_sync<TxnType>::finish();
  }
}

template <template <typename> class Protocol>
class txn_btree_worker : public ndb_thread {
public:
  txn_btree_worker(txn_btree<Protocol> &btr, uint64_t txn_flags)
    : btr(&btr), txn_flags(txn_flags) {}
  inline uint64_t get_txn_flags() const { return txn_flags; }
protected:
  txn_btree<Protocol> *const btr;
  const uint64_t txn_flags;
};

namespace mp_test1_ns {
  // read-modify-write test (counters)

  const size_t niters = 1000;

  template <template <typename> class TxnType, typename Traits>
  class worker : public txn_btree_worker<TxnType> {
  public:
    worker(txn_btree<TxnType> &btr, uint64_t txn_flags)
      : txn_btree_worker<TxnType>(btr, txn_flags) {}
    ~worker() {}
    virtual void run()
    {
      for (size_t i = 0; i < niters; i++) {
      retry:
        TxnType<Traits> t(this->txn_flags);
        try {
          rec r;
          string v;
          if (!this->btr->search(t, u64_varkey(0), v)) {
            r.v = 1;
          } else {
            r = *((const rec *) v.data());
            r.v++;
          }
          this->btr->insert_object(t, u64_varkey(0), r);
          t.commit(true);
        } catch (transaction_abort_exception &e) {
          goto retry;
        }
      }
    }
  };
}

template <template <typename> class TxnType, typename Traits>
static void
mp_test1()
{
  using namespace mp_test1_ns;

  for (size_t txn_flags_idx = 0;
       txn_flags_idx < ARRAY_NELEMS(TxnFlags);
       txn_flags_idx++) {
    const uint64_t txn_flags = TxnFlags[txn_flags_idx];
    txn_btree<TxnType> btr;

    worker<TxnType, Traits> w0(btr, txn_flags);
    worker<TxnType, Traits> w1(btr, txn_flags);
    worker<TxnType, Traits> w2(btr, txn_flags);
    worker<TxnType, Traits> w3(btr, txn_flags);

    w0.start(); w1.start(); w2.start(); w3.start();
    w0.join(); w1.join(); w2.join(); w3.join();

    {
      TxnType<Traits> t(txn_flags);
      string v;
      ALWAYS_ASSERT_COND_IN_TXN(t, btr.search(t, u64_varkey(0), v));
      ALWAYS_ASSERT_COND_IN_TXN(t, !v.empty());
      const uint64_t rv = ((const rec *) v.data())->v;
      if (rv != (niters * 4))
        cerr << "v: " << rv << ", expected: " << (niters * 4) << endl;
      ALWAYS_ASSERT_COND_IN_TXN(t, rv == (niters * 4));
      AssertSuccessfulCommit(t);
    }

    txn_epoch_sync<TxnType>::sync();
    txn_epoch_sync<TxnType>::finish();
  }
}

namespace mp_test2_ns {

  static const uint64_t ctr_key = 0;
  static const uint64_t range_begin = 100;
  static const uint64_t range_end = 200;

  static volatile bool running = true;

  // expects the values to be monotonically increasing (as records)
  template <template <typename> class Protocol>
  class counting_scan_callback : public txn_btree<Protocol>::search_range_callback {
  public:
    counting_scan_callback() : ctr(0), has_last(false), last(0) {}
    virtual bool
    invoke(const string &k, const uint8_t * v, size_t sz)
    {
      ALWAYS_ASSERT(sz == sizeof(rec));
      rec *r = (rec *) v;
      VERBOSE(cerr << "counting_scan_callback: " << hexify(k) << " => " << r->v << endl);
      if (!has_last) {
        last = r->v;
        has_last = true;
      } else {
        ALWAYS_ASSERT(r->v > last);
        last = r->v;
      }
      ctr++;
      return true;
    }
    size_t ctr;
    bool has_last;
    uint64_t last;
  };

  template <template <typename> class TxnType, typename Traits>
  class mutate_worker : public txn_btree_worker<TxnType> {
  public:
    mutate_worker(txn_btree<TxnType> &btr, uint64_t flags)
      : txn_btree_worker<TxnType>(btr, flags), naborts(0) {}
    virtual void run()
    {
      while (running) {
        for (size_t i = range_begin; running && i < range_end; i++) {
        retry:
          TxnType<Traits> t(this->txn_flags);
          try {
            rec ctr_rec;
            string v, v_ctr;
            ALWAYS_ASSERT_COND_IN_TXN(t, this->btr->search(t, u64_varkey(ctr_key), v_ctr));
            ALWAYS_ASSERT_COND_IN_TXN(t, v_ctr.size() == sizeof(rec));
            ALWAYS_ASSERT_COND_IN_TXN(t, ((const rec *) v_ctr.data())->v > 1);
            if (this->btr->search(t, u64_varkey(i), v)) {
              AssertByteEquality(rec(i), v);
              this->btr->remove(t, u64_varkey(i));
              ctr_rec.v = ((const rec *) v_ctr.data())->v - 1;
            } else {
              this->btr->insert_object(t, u64_varkey(i), rec(i));
              ctr_rec.v = ((const rec *) v_ctr.data())->v + 1;
            }
            this->btr->insert_object(t, u64_varkey(ctr_key), ctr_rec);
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

  template <template <typename> class TxnType, typename Traits>
  class reader_worker : public txn_btree_worker<TxnType> {
  public:
    reader_worker(txn_btree<TxnType> &btr, uint64_t flags)
      : txn_btree_worker<TxnType>(btr, flags), validations(0), naborts(0) {}
    virtual void run()
    {
      while (running) {
        try {
          TxnType<Traits> t(this->txn_flags);
          string v_ctr;
          ALWAYS_ASSERT_COND_IN_TXN(t, this->btr->search(t, u64_varkey(ctr_key), v_ctr));
          ALWAYS_ASSERT_COND_IN_TXN(t, v_ctr.size() == sizeof(rec));
          counting_scan_callback<TxnType> c;
          const u64_varkey kend(range_end);
          this->btr->search_range_call(t, u64_varkey(range_begin), &kend, c);
          t.commit(true);
          if (c.ctr != ((const rec *) v_ctr.data())->v) {
            cerr << "ctr: " << c.ctr << ", v_ctr: " << ((const rec *) v_ctr.data())->v << endl;
            cerr << "validations: " << validations << endl;
          }
          ALWAYS_ASSERT_COND_IN_TXN(t, c.ctr == ((const rec *) v_ctr.data())->v);
          validations++;
          VERBOSE(cerr << "successful validation" << endl);
        } catch (transaction_abort_exception &e) {
          naborts++;
        }
      }
    }
    size_t validations;
    size_t naborts;
  };
}

template <template <typename> class TxnType, typename Traits>
static void
mp_test2()
{
  using namespace mp_test2_ns;
  for (size_t txn_flags_idx = 0;
       txn_flags_idx < ARRAY_NELEMS(TxnFlags);
       txn_flags_idx++) {
    const uint64_t txn_flags = TxnFlags[txn_flags_idx];
    txn_btree<TxnType> btr;
    {
      TxnType<Traits> t(txn_flags);
      size_t n = 0;
      for (size_t i = range_begin; i < range_end; i++)
        if ((i % 2) == 0) {
          btr.insert_object(t, u64_varkey(i), rec(i));
          n++;
        }
      btr.insert_object(t, u64_varkey(ctr_key), rec(n));
      AssertSuccessfulCommit(t);
    }

    // XXX(stephentu): HACK! we need to wait for the GC to
    // compute a new consistent snapshot version that includes this
    // latest update
    txn_epoch_sync<TxnType>::sync();

    {
      // make sure the first validation passes
      TxnType<Traits> t(txn_flags | transaction_base::TXN_FLAG_READ_ONLY);
      string v_ctr;
      ALWAYS_ASSERT_COND_IN_TXN(t, btr.search(t, u64_varkey(ctr_key), v_ctr));
      ALWAYS_ASSERT_COND_IN_TXN(t, v_ctr.size() == sizeof(rec));
      counting_scan_callback<TxnType> c;
      const u64_varkey kend(range_end);
      btr.search_range_call(t, u64_varkey(range_begin), &kend, c);
      AssertSuccessfulCommit(t);
      if (c.ctr != ((const rec *) v_ctr.data())->v) {
        cerr << "ctr: " << c.ctr << ", v_ctr: " << ((const rec *) v_ctr.data())->v << endl;
      }
      ALWAYS_ASSERT_COND_IN_TXN(t, c.ctr == ((const rec *) v_ctr.data())->v);
      VERBOSE(cerr << "initial read only scan passed" << endl);
    }

    mutate_worker<TxnType, Traits> w0(btr, txn_flags);
    reader_worker<TxnType, Traits> w1(btr, txn_flags);
    reader_worker<TxnType, Traits> w2(btr, txn_flags | transaction_base::TXN_FLAG_READ_ONLY);

    running = true;
    __sync_synchronize();
    w0.start();
    w1.start();
    w2.start();
    sleep(10);
    running = false;
    __sync_synchronize();
    w0.join();
    w1.join();
    w2.join();

    cerr << "mutate naborts: " << w0.naborts << endl;
    cerr << "reader naborts: " << w1.naborts << endl;
    cerr << "reader validations: " << w1.validations << endl;
    cerr << "read-only reader naborts: " << w2.naborts << endl;
    cerr << "read-only reader validations: " << w2.validations << endl;

    txn_epoch_sync<TxnType>::sync();
    txn_epoch_sync<TxnType>::finish();
  }
}

namespace mp_test3_ns {

  static const size_t amount_per_person = 100;
  static const size_t naccounts = 100;
  static const size_t niters = 1000000;

  template <template <typename> class TxnType, typename Traits>
  class transfer_worker : public txn_btree_worker<TxnType> {
  public:
    transfer_worker(txn_btree<TxnType> &btr, uint64_t flags, unsigned long seed)
      : txn_btree_worker<TxnType>(btr, flags), seed(seed) {}
    virtual void run()
    {
      fast_random r(seed);
      for (size_t i = 0; i < niters; i++) {
      retry:
        try {
          TxnType<Traits> t(this->txn_flags);
          uint64_t a = r.next() % naccounts;
          uint64_t b = r.next() % naccounts;
          while (unlikely(a == b))
            b = r.next() % naccounts;
          string arecv, brecv;
          ALWAYS_ASSERT_COND_IN_TXN(t, this->btr->search(t, u64_varkey(a), arecv));
          ALWAYS_ASSERT_COND_IN_TXN(t, this->btr->search(t, u64_varkey(b), brecv));
          const rec *arec = (const rec *) arecv.data();
          const rec *brec = (const rec *) brecv.data();
          if (arec->v == 0) {
            t.abort();
          } else {
            const uint64_t xfer = (arec->v > 1) ? (r.next() % (arec->v - 1) + 1) : 1;
            this->btr->insert_object(t, u64_varkey(a), rec(arec->v - xfer));
            this->btr->insert_object(t, u64_varkey(b), rec(brec->v + xfer));
            t.commit(true);
          }
        } catch (transaction_abort_exception &e) {
          goto retry;
        }
      }
    }
  private:
    const unsigned long seed;
  };

  template <template <typename> class TxnType, typename Traits>
  class invariant_worker_scan : public txn_btree_worker<TxnType>,
                                public txn_btree<TxnType>::search_range_callback {
  public:
    invariant_worker_scan(txn_btree<TxnType> &btr, uint64_t flags)
      : txn_btree_worker<TxnType>(btr, flags), running(true),
        validations(0), naborts(0), sum(0) {}
    virtual void run()
    {
      while (running) {
        try {
          TxnType<Traits> t(this->txn_flags);
          sum = 0;
          this->btr->search_range_call(t, u64_varkey(0), NULL, *this);
          t.commit(true);
          ALWAYS_ASSERT_COND_IN_TXN(t, sum == (naccounts * amount_per_person));
          validations++;
        } catch (transaction_abort_exception &e) {
          naborts++;
        }
      }
    }
    virtual bool invoke(const string &k, const uint8_t * v, size_t)
    {
      sum += ((rec *) v)->v;
      return true;
    }
    volatile bool running;
    size_t validations;
    size_t naborts;
    uint64_t sum;
  };

  template <template <typename> class TxnType, typename Traits>
  class invariant_worker_1by1 : public txn_btree_worker<TxnType> {
  public:
    invariant_worker_1by1(txn_btree<TxnType> &btr, uint64_t flags)
      : txn_btree_worker<TxnType>(btr, flags), running(true),
        validations(0), naborts(0) {}
    virtual void run()
    {
      while (running) {
        try {
          TxnType<Traits> t(this->txn_flags);
          uint64_t sum = 0;
          for (uint64_t i = 0; i < naccounts; i++) {
            string v;
            ALWAYS_ASSERT_COND_IN_TXN(t, this->btr->search(t, u64_varkey(i), v));
            sum += ((const rec *) v.data())->v;
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

template <template <typename> class TxnType, typename Traits>
static void
mp_test3()
{
  using namespace mp_test3_ns;

  for (size_t txn_flags_idx = 0;
       txn_flags_idx < ARRAY_NELEMS(TxnFlags);
       txn_flags_idx++) {
    const uint64_t txn_flags = TxnFlags[txn_flags_idx];

    txn_btree<TxnType> btr;
    {
      TxnType<Traits> t(txn_flags);
      for (uint64_t i = 0; i < naccounts; i++)
        btr.insert_object(t, u64_varkey(i), rec(amount_per_person));
      AssertSuccessfulCommit(t);
    }

    txn_epoch_sync<TxnType>::sync();

    transfer_worker<TxnType, Traits> w0(btr, txn_flags, 342),
                             w1(btr, txn_flags, 93852),
                             w2(btr, txn_flags, 23085),
                             w3(btr, txn_flags, 859438989);
    invariant_worker_scan<TxnType, Traits> w4(btr, txn_flags);
    invariant_worker_1by1<TxnType, Traits> w5(btr, txn_flags);
    invariant_worker_scan<TxnType, Traits> w6(btr, txn_flags | transaction_base::TXN_FLAG_READ_ONLY);
    invariant_worker_1by1<TxnType, Traits> w7(btr, txn_flags | transaction_base::TXN_FLAG_READ_ONLY);

    w0.start(); w1.start(); w2.start(); w3.start(); w4.start(); w5.start(); w6.start(); w7.start();
    w0.join(); w1.join(); w2.join(); w3.join();
    w4.running = false; w5.running = false; w6.running = false; w7.running = false;
    __sync_synchronize();
    w4.join(); w5.join(); w6.join(); w7.join();

    cerr << "scan validations: " << w4.validations << ", scan aborts: " << w4.naborts << endl;
    cerr << "1by1 validations: " << w5.validations << ", 1by1 aborts: " << w5.naborts << endl;
    cerr << "scan-readonly validations: " << w6.validations << ", scan-readonly aborts: " << w6.naborts << endl;
    cerr << "1by1-readonly validations: " << w7.validations << ", 1by1-readonly aborts: " << w7.naborts << endl;

    txn_epoch_sync<TxnType>::sync();
    txn_epoch_sync<TxnType>::finish();
  }
}

namespace mp_test_simple_write_skew_ns {
  static const size_t NDoctors = 16;

  volatile bool running = true;

  template <template <typename> class TxnType, typename Traits>
  class get_worker : public txn_btree_worker<TxnType> {
  public:
    get_worker(unsigned int d, txn_btree<TxnType> &btr, uint64_t txn_flags)
      : txn_btree_worker<TxnType>(btr, txn_flags), n(0), d(d) {}
    virtual void run()
    {
      while (running) {
        try {
          TxnType<Traits> t(this->txn_flags);
          if ((n % 2) == 0) {
            // try to take this doctor off call
            unsigned int ctr = 0;
            for (unsigned int i = 0; i < NDoctors && ctr < 2; i++) {
              string v;
              ALWAYS_ASSERT_COND_IN_TXN(t, this->btr->search(t, u64_varkey(i), v));
              INVARIANT(v.size() == sizeof(rec));
              const rec *r = (const rec *) v.data();
              if (r->v)
                ctr++;
            }
            if (ctr == 2)
              this->btr->insert_object(t, u64_varkey(d), rec(0));
            t.commit(true);
            ALWAYS_ASSERT_COND_IN_TXN(t, ctr >= 1);
          } else {
            // place this doctor on call
            this->btr->insert_object(t, u64_varkey(d), rec(1));
            t.commit(true);
          }
          n++;
        } catch (transaction_abort_exception &e) {
          // no-op
        }
      }
    }
    uint64_t n;
  private:
    unsigned int d;
  };

  template <template <typename> class TxnType, typename Traits>
  class scan_worker : public txn_btree_worker<TxnType>,
                      public txn_btree<TxnType>::search_range_callback {
  public:
    scan_worker(unsigned int d, txn_btree<TxnType> &btr, uint64_t txn_flags)
      : txn_btree_worker<TxnType>(btr, txn_flags), n(0), d(d), ctr(0) {}
    virtual void run()
    {
      while (running) {
        try {
          TxnType<Traits> t(this->txn_flags);
          if ((n % 2) == 0) {
            ctr = 0;
            this->btr->search_range_call(t, u64_varkey(0), NULL, *this);
            if (ctr == 2)
              this->btr->insert_object(t, u64_varkey(d), rec(0));
            t.commit(true);
            ALWAYS_ASSERT_COND_IN_TXN(t, ctr >= 1);
          } else {
            this->btr->insert_object(t, u64_varkey(d), rec(1));
            t.commit(true);
          }
          n++;
        } catch (transaction_abort_exception &e) {
          // no-op
        }
      }
    }
    virtual bool invoke(const string &k, const uint8_t * v, size_t sz)
    {
      INVARIANT(sz == sizeof(rec));
      const rec *r = (const rec *) v;
      if (r->v)
        ctr++;
      return ctr < 2;
    }
    uint64_t n;
  private:
    unsigned int d;
    unsigned int ctr;
  };
}

template <template <typename> class TxnType, typename Traits>
static void
mp_test_simple_write_skew()
{
  using namespace mp_test_simple_write_skew_ns;

  for (size_t txn_flags_idx = 0;
       txn_flags_idx < ARRAY_NELEMS(TxnFlags);
       txn_flags_idx++) {
    const uint64_t txn_flags = TxnFlags[txn_flags_idx];

    txn_btree<TxnType> btr;
    {
      TxnType<Traits> t(txn_flags);
      _static_assert(NDoctors >= 2);
      for (uint64_t i = 0; i < NDoctors; i++)
        btr.insert_object(t, u64_varkey(i), rec(i < 2 ? 1 : 0));
      AssertSuccessfulCommit(t);
    }

    txn_epoch_sync<TxnType>::sync();

    running = true;
    __sync_synchronize();
    vector<txn_btree_worker<TxnType> *> workers;
    for (size_t i = 0; i < NDoctors / 2; i++)
      workers.push_back(new get_worker<TxnType, Traits>(i, btr, txn_flags));
    for (size_t i = NDoctors / 2; i < NDoctors; i++)
      workers.push_back(new scan_worker<TxnType, Traits>(i, btr, txn_flags));
    for (size_t i = 0; i < NDoctors; i++)
      workers[i]->start();
    sleep(10);
    running = false;
    __sync_synchronize();


    size_t n_get_succ = 0, n_scan_succ = 0;
    for (size_t i = 0; i < NDoctors; i++) {
      workers[i]->join();
      if (get_worker<TxnType, Traits> *w = dynamic_cast<get_worker<TxnType, Traits> *>(workers[i]))
        n_get_succ += w->n;
      else if (scan_worker<TxnType, Traits> *w = dynamic_cast<scan_worker<TxnType, Traits> *>(workers[i]))
        n_scan_succ += w->n;
      else
        ALWAYS_ASSERT(false);
      delete workers[i];
    }
    workers.clear();

    cerr << "get_worker  txns: " << n_get_succ << endl;
    cerr << "scan_worker txns: " << n_scan_succ << endl;

    txn_epoch_sync<TxnType>::sync();
    txn_epoch_sync<TxnType>::finish();
  }
}

namespace mp_test_batch_processing_ns {

  volatile bool running = true;

  static inline string
  MakeKey(uint32_t batch_id, uint32_t receipt_id)
  {
    const big_endian_trfm<int32_t> t;
    string buf(8, 0);
    uint32_t *p = (uint32_t *) &buf[0];
    *p++ = t(batch_id);
    *p++ = t(receipt_id);
    return buf;
  }

  template <template <typename> class TxnType, typename Traits>
  class report_worker : public ndb_thread,
                        public txn_btree<TxnType>::search_range_callback {
  public:
    report_worker(txn_btree<TxnType> &ctrl, txn_btree<TxnType> &receipts, uint64_t txn_flags)
      : ctrl(&ctrl), receipts(&receipts), txn_flags(txn_flags), n(0), m(0), sum(0) {}
    virtual void run()
    {
      while (running) {
        try {
          TxnType<Traits> t(this->txn_flags);
          string v;
          ALWAYS_ASSERT_COND_IN_TXN(t, ctrl->search(t, u64_varkey(0), v));
          ALWAYS_ASSERT_COND_IN_TXN(t, v.size() == sizeof(rec));
          const rec * const r = (const rec *) v.data();
          const uint32_t prev_bid = r->v - 1; // prev batch
          sum = 0;
          const string endkey = MakeKey(prev_bid, numeric_limits<uint32_t>::max());
          receipts->search_range_call(t, MakeKey(prev_bid, 0), &endkey, *this);
          t.commit(true);
          map<uint32_t, uint32_t>::iterator it = reports.find(prev_bid);
          if (it != reports.end()) {
            ALWAYS_ASSERT_COND_IN_TXN(t, sum == it->second);
            m++;
          } else {
            reports[prev_bid] = sum;
          }
          n++;
        } catch (transaction_abort_exception &e) {
          // no-op
        }
      }
    }
    virtual bool invoke(const string &k, const uint8_t * v, size_t sz)
    {
      INVARIANT(sz == sizeof(rec));
      const rec * const r = (const rec *) v;
      sum += r->v;
      return true;
    }
  private:
    txn_btree<TxnType> *ctrl;
    txn_btree<TxnType> *receipts;
    uint64_t txn_flags;

  public:
    uint64_t n;
    uint64_t m;

  private:
    map<uint32_t, uint32_t> reports;
    unsigned int sum;
  };

  template <template <typename> class TxnType, typename Traits>
  class new_receipt_worker : public ndb_thread {
  public:
    new_receipt_worker(txn_btree<TxnType> &ctrl, txn_btree<TxnType> &receipts, uint64_t txn_flags)
      : ctrl(&ctrl), receipts(&receipts), txn_flags(txn_flags),
        n(0), last_bid(0), last_rid(0) {}
    virtual void run()
    {
      while (running) {
        try {
          TxnType<Traits> t(this->txn_flags);
          string v;
          ALWAYS_ASSERT_COND_IN_TXN(t, ctrl->search(t, u64_varkey(0), v));
          ALWAYS_ASSERT_COND_IN_TXN(t, v.size() == sizeof(rec));
          const rec * const r = (const rec *) v.data();
          const uint32_t cur_bid = r->v;
          const uint32_t cur_rid = (cur_bid != last_bid) ? 0 : last_rid + 1;
          const string rkey = MakeKey(cur_bid, cur_rid);
          receipts->insert_object(t, rkey, rec(1));
          t.commit(true);
          last_bid = cur_bid;
          last_rid = cur_rid;
          n++;
        } catch (transaction_abort_exception &e) {
          // no-op
        }
      }
    }

  private:
    txn_btree<TxnType> *ctrl;
    txn_btree<TxnType> *receipts;
    uint64_t txn_flags;

  public:
    uint64_t n;

  private:
    uint32_t last_bid;
    uint32_t last_rid;
  };

  template <template <typename> class TxnType, typename Traits>
  class incr_worker : public ndb_thread {
  public:
    incr_worker(txn_btree<TxnType> &ctrl, txn_btree<TxnType> &receipts, uint64_t txn_flags)
      : ctrl(&ctrl), receipts(&receipts), txn_flags(txn_flags), n(0) {}
    virtual void run()
    {
      struct timespec t;
      NDB_MEMSET(&t, 0, sizeof(t));
      t.tv_nsec = 1000; // 1 us
      while (running) {
        try {
          TxnType<Traits> t(this->txn_flags);
          string v;
          ALWAYS_ASSERT_COND_IN_TXN(t, ctrl->search(t, u64_varkey(0), v));
          ALWAYS_ASSERT_COND_IN_TXN(t, v.size() == sizeof(rec));
          const rec * const r = (const rec *) v.data();
          ctrl->insert_object(t, u64_varkey(0), rec(r->v + 1));
          t.commit(true);
          n++;
        } catch (transaction_abort_exception &e) {
          // no-op
        }
        nanosleep(&t, NULL);
      }
    }
  private:
    txn_btree<TxnType> *ctrl;
    txn_btree<TxnType> *receipts;
    uint64_t txn_flags;

  public:
    uint64_t n;
  };
}

template <template <typename> class TxnType, typename Traits>
static void
mp_test_batch_processing()
{
  using namespace mp_test_batch_processing_ns;

  for (size_t txn_flags_idx = 0;
       txn_flags_idx < ARRAY_NELEMS(TxnFlags);
       txn_flags_idx++) {
    const uint64_t txn_flags = TxnFlags[txn_flags_idx];

    txn_btree<TxnType> ctrl;
    txn_btree<TxnType> receipts;
    {
      TxnType<Traits> t(txn_flags);
      ctrl.insert_object(t, u64_varkey(0), rec(1));
      AssertSuccessfulCommit(t);
    }

    txn_epoch_sync<TxnType>::sync();

    report_worker<TxnType, Traits> w0(ctrl, receipts, txn_flags);
    new_receipt_worker<TxnType, Traits> w1(ctrl, receipts, txn_flags);
    incr_worker<TxnType, Traits> w2(ctrl, receipts, txn_flags);
    running = true;
    __sync_synchronize();
    w0.start(); w1.start(); w2.start();
    sleep(10);
    running = false;
    __sync_synchronize();
    w0.join(); w1.join(); w2.join();


    cerr << "report_worker      txns       : " << w0.n << endl;
    cerr << "report_worker      validations: " << w0.m << endl;
    cerr << "new_receipt_worker txns       : " << w1.n << endl;
    cerr << "incr_worker        txns       : " << w2.n << endl;

    txn_epoch_sync<TxnType>::sync();
    txn_epoch_sync<TxnType>::finish();
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

  template <template <typename> class TxnType, typename Traits>
  class worker : public txn_btree_worker<TxnType> {
  public:
    worker(unsigned int seed, txn_btree<TxnType> &btr, uint64_t txn_flags)
      : txn_btree_worker<TxnType>(btr, txn_flags), n(0), seed(seed) {}
    virtual void run()
    {
      fast_random r(seed);
      while (running) {
        const uint64_t k = r.next() % nkeys;
      retry:
        try {
          TxnType<Traits> t(this->txn_flags);
          string v;
          bool found = this->btr->search(t, u64_varkey(k), v);
          t.commit(true);
          ALWAYS_ASSERT_COND_IN_TXN(t, found);
          AssertByteEquality(rec(k + 1), v);
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

template <template <typename> class TxnType, typename Traits>
static void
read_only_perf()
{
  using namespace read_only_perf_ns;
  for (size_t txn_flags_idx = 0;
       txn_flags_idx < ARRAY_NELEMS(TxnFlags);
       txn_flags_idx++) {
    const uint64_t txn_flags = TxnFlags[txn_flags_idx];

    txn_btree<TxnType> btr;

    {
      const size_t nkeyspertxn = 100000;
      for (size_t i = 0; i < nkeys / nkeyspertxn; i++) {
        TxnType<Traits> t;
        const size_t end = (i == (nkeys / nkeyspertxn - 1)) ? nkeys : ((i + 1) * nkeyspertxn);
        for (size_t j = i * nkeyspertxn; j < end; j++)
          btr.insert_object(t, u64_varkey(j), rec(j + 1));
        AssertSuccessfulCommit(t);
        cerr << "batch " << i << " completed" << endl;
      }
      cerr << "btree loaded, test starting" << endl;
    }

    vector<worker<TxnType, Traits> *> workers;
    for (size_t i = 0; i < ARRAY_NELEMS(seeds); i++)
      workers.push_back(new worker<TxnType, Traits>(seeds[i], btr, txn_flags));

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

    txn_epoch_sync<TxnType>::sync();
    txn_epoch_sync<TxnType>::finish();
  }
}

void txn_btree_test()
{
  // XXX(stephentu): proto1 is bit-rotting for now
  //cerr << "Test proto1" << endl;
  //test1<transaction_proto1>();
  //test2<transaction_proto1>();
  //test_absent_key_race<transaction_proto1>();
  //test_inc_value_size<transaction_proto1>();
  //test_multi_btree<transaction_proto1>();
  //test_read_only_snapshot<transaction_proto1>();
  //test_long_keys<transaction_proto1>();
  //test_long_keys2<transaction_proto1>();
  //mp_test1<transaction_proto1>();
  //mp_test2<transaction_proto1>();
  //mp_test3<transaction_proto1>();
  //mp_test_simple_write_skew<transaction_proto1>();
  //mp_test_batch_processing<transaction_proto1>();

  cerr << "Test proto2" << endl;
  test1<transaction_proto2, default_transaction_traits>();
  test2<transaction_proto2, default_transaction_traits>();
  test_absent_key_race<transaction_proto2, default_transaction_traits>();
  test_inc_value_size<transaction_proto2, default_transaction_traits>();
  test_multi_btree<transaction_proto2, default_transaction_traits>();
  test_read_only_snapshot<transaction_proto2, default_transaction_traits>();
  test_long_keys<transaction_proto2, default_transaction_traits>();
  test_long_keys2<transaction_proto2, default_transaction_traits>();
  mp_test1<transaction_proto2, default_transaction_traits>();
  mp_test2<transaction_proto2, default_transaction_traits>();
  mp_test3<transaction_proto2, default_transaction_traits>();
  mp_test_simple_write_skew<transaction_proto2, default_transaction_traits>();
  mp_test_batch_processing<transaction_proto2, default_transaction_traits>();

  //read_only_perf<transaction_proto1>();
  //read_only_perf<transaction_proto2>();
}
