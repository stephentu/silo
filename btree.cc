#include <unistd.h>

#include <iostream>
#include <map>
#include <set>
#include <stack>
#include <vector>
#include <sstream>
#include <atomic>
#include <memory>

#include "core.h"
#include "btree.h"
#include "btree_impl.h"
#include "thread.h"
#include "txn.h"
#include "util.h"
#include "scopedperf.hh"

#if defined(NDB_MASSTREE)
#include "masstree_btree.h"
struct testing_concurrent_btree_traits : public masstree_params {
  static const bool RcuRespCaller = false;
};
typedef mbtree<testing_concurrent_btree_traits> testing_concurrent_btree;
#define HAVE_REVERSE_RANGE_SCANS
#else
struct testing_concurrent_btree_traits : public concurrent_btree_traits {
  static const bool RcuRespCaller = false;
};
typedef btree<testing_concurrent_btree_traits> testing_concurrent_btree;
#endif

using namespace std;
using namespace util;

class scoped_rate_timer {
private:
  util::timer t;
  string region;
  size_t n;

public:
  scoped_rate_timer(const string &region, size_t n) : region(region), n(n)
  {}

  ~scoped_rate_timer()
  {
    double x = t.lap() / 1000.0; // ms
    double rate = double(n) / (x / 1000.0);
    cerr << "timed region `" << region << "' took " << x
              << " ms (" << rate << " events/sec)" << endl;
  }
};

class btree_worker : public ndb_thread {
public:
  btree_worker(testing_concurrent_btree *btr) : btr(btr)  {}
  btree_worker(testing_concurrent_btree &btr) : btr(&btr) {}
protected:
  testing_concurrent_btree *const btr;
};

static void
test1()
{
  testing_concurrent_btree btr;
  btr.invariant_checker();

  // fill up root leaf node
  for (size_t i = 0; i < testing_concurrent_btree::NKeysPerNode; i++) {
    btr.insert(u64_varkey(i), (typename testing_concurrent_btree::value_type) i);
    btr.invariant_checker();

    typename testing_concurrent_btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(u64_varkey(i), v));
    ALWAYS_ASSERT(v == (typename testing_concurrent_btree::value_type) i);
  }
  ALWAYS_ASSERT(btr.size() == testing_concurrent_btree::NKeysPerNode);

  // induce a split
  btr.insert(u64_varkey(testing_concurrent_btree::NKeysPerNode), (typename testing_concurrent_btree::value_type) (testing_concurrent_btree::NKeysPerNode));
  btr.invariant_checker();
  ALWAYS_ASSERT(btr.size() == testing_concurrent_btree::NKeysPerNode + 1);

  // now make sure we can find everything post split
  for (size_t i = 0; i < testing_concurrent_btree::NKeysPerNode + 1; i++) {
    typename testing_concurrent_btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(u64_varkey(i), v));
    ALWAYS_ASSERT(v == (typename testing_concurrent_btree::value_type) i);
  }

  // now fill up the new root node
  const size_t n = (testing_concurrent_btree::NKeysPerNode + testing_concurrent_btree::NKeysPerNode * (testing_concurrent_btree::NMinKeysPerNode));
  for (size_t i = testing_concurrent_btree::NKeysPerNode + 1; i < n; i++) {
    btr.insert(u64_varkey(i), (typename testing_concurrent_btree::value_type) i);
    btr.invariant_checker();

    typename testing_concurrent_btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(u64_varkey(i), v));
    ALWAYS_ASSERT(v == (typename testing_concurrent_btree::value_type) i);
  }
  ALWAYS_ASSERT(btr.size() == n);

  // cause the root node to split
  btr.insert(u64_varkey(n), (typename testing_concurrent_btree::value_type) n);
  btr.invariant_checker();
  ALWAYS_ASSERT(btr.size() == n + 1);

  // once again make sure we can find everything
  for (size_t i = 0; i < n + 1; i++) {
    typename testing_concurrent_btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(u64_varkey(i), v));
    ALWAYS_ASSERT(v == (typename testing_concurrent_btree::value_type) i);
  }
}

static void
test2()
{
  testing_concurrent_btree btr;
  const size_t n = 1000;
  for (size_t i = 0; i < n; i += 2) {
    btr.insert(u64_varkey(i), (typename testing_concurrent_btree::value_type) i);
    btr.invariant_checker();

    typename testing_concurrent_btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(u64_varkey(i), v));
    ALWAYS_ASSERT(v == (typename testing_concurrent_btree::value_type) i);
  }

  for (size_t i = 1; i < n; i += 2) {
    btr.insert(u64_varkey(i), (typename testing_concurrent_btree::value_type) i);
    btr.invariant_checker();

    typename testing_concurrent_btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(u64_varkey(i), v));
    ALWAYS_ASSERT(v == (typename testing_concurrent_btree::value_type) i);
  }

  ALWAYS_ASSERT(btr.size() == n);
}

static void
test3()
{
  testing_concurrent_btree btr;

  for (size_t i = 0; i < testing_concurrent_btree::NKeysPerNode * 2; i++) {
    btr.insert(u64_varkey(i), (typename testing_concurrent_btree::value_type) i);
    btr.invariant_checker();

    typename testing_concurrent_btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(u64_varkey(i), v));
    ALWAYS_ASSERT(v == (typename testing_concurrent_btree::value_type) i);
  }
  ALWAYS_ASSERT(btr.size() == testing_concurrent_btree::NKeysPerNode * 2);

  for (size_t i = 0; i < testing_concurrent_btree::NKeysPerNode * 2; i++) {
    btr.remove(u64_varkey(i));
    btr.invariant_checker();

    typename testing_concurrent_btree::value_type v = 0;
    ALWAYS_ASSERT(!btr.search(u64_varkey(i), v));
  }
  ALWAYS_ASSERT(btr.size() == 0);

  for (size_t i = 0; i < testing_concurrent_btree::NKeysPerNode * 2; i++) {
    btr.insert(u64_varkey(i), (typename testing_concurrent_btree::value_type) i);
    btr.invariant_checker();

    typename testing_concurrent_btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(u64_varkey(i), v));
    ALWAYS_ASSERT(v == (typename testing_concurrent_btree::value_type) i);
  }
  ALWAYS_ASSERT(btr.size() == testing_concurrent_btree::NKeysPerNode * 2);

  for (ssize_t i = testing_concurrent_btree::NKeysPerNode * 2 - 1; i >= 0; i--) {
    btr.remove(u64_varkey(i));
    btr.invariant_checker();

    typename testing_concurrent_btree::value_type v = 0;
    ALWAYS_ASSERT(!btr.search(u64_varkey(i), v));
  }
  ALWAYS_ASSERT(btr.size() == 0);

  for (size_t i = 0; i < testing_concurrent_btree::NKeysPerNode * 2; i++) {
    btr.insert(u64_varkey(i), (typename testing_concurrent_btree::value_type) i);
    btr.invariant_checker();

    typename testing_concurrent_btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(u64_varkey(i), v));
    ALWAYS_ASSERT(v == (typename testing_concurrent_btree::value_type) i);
  }
  ALWAYS_ASSERT(btr.size() == testing_concurrent_btree::NKeysPerNode * 2);

  for (ssize_t i = testing_concurrent_btree::NKeysPerNode; i >= 0; i--) {
    btr.remove(u64_varkey(i));
    btr.invariant_checker();

    typename testing_concurrent_btree::value_type v = 0;
    ALWAYS_ASSERT(!btr.search(u64_varkey(i), v));
  }

  for (size_t i = testing_concurrent_btree::NKeysPerNode + 1; i < testing_concurrent_btree::NKeysPerNode * 2; i++) {
    btr.remove(u64_varkey(i));
    btr.invariant_checker();

    typename testing_concurrent_btree::value_type v = 0;
    ALWAYS_ASSERT(!btr.search(u64_varkey(i), v));
  }
  ALWAYS_ASSERT(btr.size() == 0);
}

static void
test4()
{
  testing_concurrent_btree btr;
  const size_t nkeys = 10000;
  for (size_t i = 0; i < nkeys; i++) {
    btr.insert(u64_varkey(i), (typename testing_concurrent_btree::value_type) i);
    btr.invariant_checker();
    typename testing_concurrent_btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(u64_varkey(i), v));
    ALWAYS_ASSERT(v == (typename testing_concurrent_btree::value_type) i);
  }
  ALWAYS_ASSERT(btr.size() == nkeys);

  srand(12345);

  for (size_t i = 0; i < nkeys; i++) {
    size_t k = rand() % nkeys;
    btr.remove(u64_varkey(k));
    btr.invariant_checker();
    typename testing_concurrent_btree::value_type v = 0;
    ALWAYS_ASSERT(!btr.search(u64_varkey(k), v));
  }

  for (size_t i = 0; i < nkeys; i++) {
    btr.remove(u64_varkey(i));
    btr.invariant_checker();
    typename testing_concurrent_btree::value_type v = 0;
    ALWAYS_ASSERT(!btr.search(u64_varkey(i), v));
  }
  ALWAYS_ASSERT(btr.size() == 0);
}

static void
test5()
{
  // insert in random order, delete in random order
  testing_concurrent_btree btr;

  unsigned int seeds[] = {
    54321, 2013883780, 3028985725, 3058602342, 256561598, 2895653051
  };

  for (size_t iter = 0; iter < ARRAY_NELEMS(seeds); iter++) {
    srand(seeds[iter]);
    const size_t nkeys = 20000;
    set<size_t> s;
    for (size_t i = 0; i < nkeys; i++) {
      size_t k = rand() % nkeys;
      s.insert(k);
      btr.insert(u64_varkey(k), (typename testing_concurrent_btree::value_type) k);
      btr.invariant_checker();
      typename testing_concurrent_btree::value_type v = 0;
      ALWAYS_ASSERT(btr.search(u64_varkey(k), v));
      ALWAYS_ASSERT(v == (typename testing_concurrent_btree::value_type) k);
    }
    ALWAYS_ASSERT(btr.size() == s.size());

    for (size_t i = 0; i < nkeys * 2; i++) {
      size_t k = rand() % nkeys;
      btr.remove(u64_varkey(k));
      btr.invariant_checker();
      typename testing_concurrent_btree::value_type v = 0;
      ALWAYS_ASSERT(!btr.search(u64_varkey(k), v));
    }

    // clean it up
    for (size_t i = 0; i < nkeys; i++) {
      btr.remove(u64_varkey(i));
      btr.invariant_checker();
      typename testing_concurrent_btree::value_type v = 0;
      ALWAYS_ASSERT(!btr.search(u64_varkey(i), v));
    }

    ALWAYS_ASSERT(btr.size() == 0);
  }
}

namespace test6_ns {
  struct scan_callback {
    typedef vector<
      pair< std::string, // we want to make copies of keys
            typename testing_concurrent_btree::value_type > > kv_vec;
    scan_callback(kv_vec *data, bool reverse = false)
      : data(data), reverse_(reverse) {}
    inline bool
    operator()(const typename testing_concurrent_btree::string_type &k,
                     typename testing_concurrent_btree::value_type v) const
    {
      if (!data->empty()) {
        const bool geq =
          typename testing_concurrent_btree::string_type(data->back().first) >= k;
        const bool leq =
          typename testing_concurrent_btree::string_type(data->back().first) <= k;
        if ((!reverse_ && geq) || (reverse_ && leq)) {
          cerr << "data->size(): " << data->size() << endl;
          cerr << "prev: " << varkey(data->back().first) << endl;
          cerr << "cur : " << varkey(k) << endl;
          ALWAYS_ASSERT(false);
        }
      }
      data->push_back(make_pair(k, v));
      return true;
    }
    kv_vec *data;
    bool reverse_;
  };
}

static void
test6()
{
  testing_concurrent_btree btr;
  const size_t nkeys = 1000;
  for (size_t i = 0; i < nkeys; i++)
    btr.insert(u64_varkey(i), (typename testing_concurrent_btree::value_type) i);
  btr.invariant_checker();
  ALWAYS_ASSERT(btr.size() == nkeys);

  using namespace test6_ns;

  scan_callback::kv_vec data;
  scan_callback cb(&data);
  u64_varkey max_key(600);
  btr.search_range(u64_varkey(500), &max_key, cb);
  ALWAYS_ASSERT(data.size() == 100);
  for (size_t i = 0; i < 100; i++) {
    const varkey lhs(data[i].first), rhs(u64_varkey(500 + i));
    ALWAYS_ASSERT(lhs == rhs);
    ALWAYS_ASSERT(data[i].second == (typename testing_concurrent_btree::value_type) (500 + i));
  }

  data.clear();
  btr.search_range(u64_varkey(500), NULL, cb);
  ALWAYS_ASSERT(data.size() == 500);
  for (size_t i = 0; i < 500; i++) {
    ALWAYS_ASSERT(varkey(data[i].first) == u64_varkey(500 + i));
    ALWAYS_ASSERT(data[i].second == (typename testing_concurrent_btree::value_type) (500 + i));
  }

#ifdef HAVE_REVERSE_RANGE_SCANS
  data.clear();
  scan_callback cb_rev(&data, true);
  btr.rsearch_range(u64_varkey(499), NULL, cb_rev);
  ALWAYS_ASSERT(data.size() == 500);
  for (ssize_t i = 499; i >= 0; i--) {
    ALWAYS_ASSERT(varkey(data[499 - i].first) == u64_varkey(i));
    ALWAYS_ASSERT(data[499 - i].second == (typename testing_concurrent_btree::value_type) (i));
  }

  data.clear();
  u64_varkey min_key(499);
  btr.rsearch_range(u64_varkey(999), &min_key, cb_rev);
  ALWAYS_ASSERT(data.size() == 500);
  for (ssize_t i = 999; i >= 500; i--) {
    ALWAYS_ASSERT(varkey(data[999 - i].first) == u64_varkey(i));
    ALWAYS_ASSERT(data[999 - i].second == (typename testing_concurrent_btree::value_type) (i));
  }
#endif
}

static void
test7()
{
  testing_concurrent_btree btr;
  ALWAYS_ASSERT(!btr.remove(u64_varkey(0)));
  ALWAYS_ASSERT(btr.insert(u64_varkey(0), (typename testing_concurrent_btree::value_type) 0));
  ALWAYS_ASSERT(!btr.insert(u64_varkey(0), (typename testing_concurrent_btree::value_type) 1));
  typename testing_concurrent_btree::value_type v;
  ALWAYS_ASSERT(btr.search(u64_varkey(0), v));
  ALWAYS_ASSERT(v == (typename testing_concurrent_btree::value_type) 1);
  ALWAYS_ASSERT(!btr.insert_if_absent(u64_varkey(0), (typename testing_concurrent_btree::value_type) 2));
  ALWAYS_ASSERT(btr.search(u64_varkey(0), v));
  ALWAYS_ASSERT(v == (typename testing_concurrent_btree::value_type) 1);
  ALWAYS_ASSERT(btr.remove(u64_varkey(0)));
  ALWAYS_ASSERT(btr.insert_if_absent(u64_varkey(0), (typename testing_concurrent_btree::value_type) 2));
  ALWAYS_ASSERT(btr.search(u64_varkey(0), v));
  ALWAYS_ASSERT(v == (typename testing_concurrent_btree::value_type) 2);
}

static void
test_varlen_single_layer()
{
  testing_concurrent_btree btr;

  const char *k0 = "a";
  const char *k1 = "aa";
  const char *k2 = "aaa";
  const char *k3 = "aaaa";
  const char *k4 = "aaaaa";

  const char *keys[] = {k0, k1, k2, k3, k4};
  for (size_t i = 0; i < ARRAY_NELEMS(keys); i++) {
    ALWAYS_ASSERT(btr.insert(varkey(keys[i]), (typename testing_concurrent_btree::value_type) keys[i]));
    btr.invariant_checker();
  }

  ALWAYS_ASSERT(btr.size() == ARRAY_NELEMS(keys));
  for (size_t i = 0; i < ARRAY_NELEMS(keys); i++) {
    typename testing_concurrent_btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(varkey(keys[i]), v));
    ALWAYS_ASSERT(strcmp((const char *) v, keys[i]) == 0);
  }

  for (size_t i = 0; i < ARRAY_NELEMS(keys); i++) {
    ALWAYS_ASSERT(btr.remove(varkey(keys[i])));
    btr.invariant_checker();
  }
  ALWAYS_ASSERT(btr.size() == 0);
}

static void
test_varlen_multi_layer()
{
  testing_concurrent_btree btr;

  const char *k0 = "aaaaaaa";
  const char *k1 = "aaaaaaaa";
  const char *k2 = "aaaaaaaaa";
  const char *k3 = "aaaaaaaaaa";
  const char *k4 = "aaaaaaaaaaa";
  const char *k5 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
  const char *keys[] = {k0, k1, k2, k3, k4, k5};

  for (size_t i = 0; i < ARRAY_NELEMS(keys); i++) {
    ALWAYS_ASSERT(btr.insert(varkey(keys[i]), (typename testing_concurrent_btree::value_type) keys[i]));
    btr.invariant_checker();
  }

  ALWAYS_ASSERT(btr.size() == ARRAY_NELEMS(keys));
  for (size_t i = 0; i < ARRAY_NELEMS(keys); i++) {
    typename testing_concurrent_btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(varkey(keys[i]), v));
    ALWAYS_ASSERT(strcmp((const char *) v, keys[i]) == 0);
  }

  for (size_t i = 0; i < ARRAY_NELEMS(keys); i++) {
    ALWAYS_ASSERT(btr.remove(varkey(keys[i])));
    btr.invariant_checker();
  }
  ALWAYS_ASSERT(btr.size() == 0);
}

static void
test_two_layer()
{
  const char *k0 = "aaaaaaaaa";
  const char *k1 = "aaaaaaaaaa";

  testing_concurrent_btree btr;
  ALWAYS_ASSERT(btr.insert(varkey(k0), (typename testing_concurrent_btree::value_type) k0));
  ALWAYS_ASSERT(btr.insert(varkey(k1), (typename testing_concurrent_btree::value_type) k1));
  ALWAYS_ASSERT(btr.size() == 2);
}

static __attribute__((used)) void test_ensure_printable() {
    testing_concurrent_btree btr;
    btr.print();
}

class test_range_scan_helper : public testing_concurrent_btree::search_range_callback {
public:

  struct expect {
    expect() : tag(), expected_size() {}
    expect(size_t expected_size)
      : tag(0), expected_size(expected_size) {}
    expect(const set<string> &expected_keys)
      : tag(1), expected_keys(expected_keys) {}
    uint8_t tag;
    size_t expected_size;
    set<string> expected_keys;
  };

  enum ExpectType {
    EXPECT_EXACT,
    EXPECT_ATLEAST,
  };

  test_range_scan_helper(
    testing_concurrent_btree &btr,
    const testing_concurrent_btree::key_type &begin,
    const testing_concurrent_btree::key_type *end,
    bool reverse,
    const expect &expectation,
    ExpectType ex_type = EXPECT_EXACT)
    : btr(&btr),
      begin(begin),
      end(end ? new testing_concurrent_btree::key_type(*end) : NULL),
      reverse_(reverse),
      expectation(expectation),
      ex_type(ex_type)
  {
  }

  ~test_range_scan_helper()
  {
    if (end)
      delete end;
  }

  virtual bool
  invoke(const typename testing_concurrent_btree::string_type &k,
         typename testing_concurrent_btree::value_type v)
  {
    VERBOSE(cerr << "test_range_scan_helper::invoke(): received key(size="
                 << k.size() << "): " << hexify(k) << endl);
    if (!keys.empty()) {
      if (!reverse_)
        ALWAYS_ASSERT(typename testing_concurrent_btree::string_type(keys.back()) < k);
      else
        ALWAYS_ASSERT(typename testing_concurrent_btree::string_type(keys.back()) > k);
    }
    keys.push_back(k);
    return true;
  }

  void test()
  {
    keys.clear();
    if (!reverse_)
      btr->search_range_call(begin, end, *this);
    else
      btr->rsearch_range_call(begin, end, *this);
    if (expectation.tag == 0) {
      switch (ex_type) {
      case EXPECT_EXACT:
        ALWAYS_ASSERT(keys.size() == expectation.expected_size);
        break;
      case EXPECT_ATLEAST:
        ALWAYS_ASSERT(keys.size() >= expectation.expected_size);
        break;
      }
    } else {
      switch (ex_type) {
      case EXPECT_EXACT: {
        ALWAYS_ASSERT(keys.size() == expectation.expected_keys.size());
        vector<string> cmp;
        if (!reverse_)
          cmp.assign(expectation.expected_keys.begin(), expectation.expected_keys.end());
        else
          cmp.assign(expectation.expected_keys.rbegin(), expectation.expected_keys.rend());
        for (size_t i = 0; i < keys.size(); i++) {
          if (keys[i] != cmp[i]) {
            cerr << "A: " << hexify(keys[i]) << endl;
            cerr << "B: " << hexify(cmp[i]) << endl;
            ALWAYS_ASSERT(false);
          }
        }
        break;
      }
      case EXPECT_ATLEAST: {
        ALWAYS_ASSERT(keys.size() >= expectation.expected_keys.size());
        // every key in the expected set must be present
        set<string> keyset(keys.begin(), keys.end());
        for (auto it = expectation.expected_keys.begin();
             it != expectation.expected_keys.end(); ++it)
          ALWAYS_ASSERT(keyset.count(*it) == 1);
        break;
      }
      }
    }
  }

private:
  testing_concurrent_btree *const btr;
  testing_concurrent_btree::key_type begin;
  testing_concurrent_btree::key_type *end;
  bool reverse_;
  expect expectation;
  ExpectType ex_type;

  vector<string> keys;
};

static void
test_two_layer_range_scan()
{
  const char *keys[] = {
    "a",
    "aaaaaaaa",
    "aaaaaaaaa",
    "aaaaaaaaaa",
    "aaaaaaaaaaa",
    "b", "c", "d", "e", "f", "g", "h", "i", "j", "k",
    "l", "m", "n", "o", "p", "q", "r", "s",
  };

  testing_concurrent_btree btr;
  for (size_t i = 0; i < ARRAY_NELEMS(keys); i++) {
    ALWAYS_ASSERT(btr.insert(varkey(keys[i]), (typename testing_concurrent_btree::value_type) keys[i]));
    btr.invariant_checker();
  }

  test_range_scan_helper::expect ex(set<string>(keys, keys + ARRAY_NELEMS(keys)));
  test_range_scan_helper tester(btr, varkey(""), NULL, false, ex);
  tester.test();

#ifdef HAVE_REVERSE_RANGE_SCANS
  test_range_scan_helper tester_rev(btr, varkey("zzzzzzzzzzzzzzzzzzzzzz"), NULL, true, ex);
  tester_rev.test();
#endif
}

static void
test_multi_layer_scan()
{
  const uint8_t lokey_cstr[] = {
    0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x45, 0x49, 0x4E, 0x47,
    0x41, 0x54, 0x49, 0x4F, 0x4E, 0x45, 0x49, 0x4E, 0x47, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00
  };
  const uint8_t hikey_cstr[] = {
    0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x45, 0x49, 0x4E, 0x47,
    0x41, 0x54, 0x49, 0x4F, 0x4E, 0x45, 0x49, 0x4E, 0x47, 0x00, 0x00, 0x00,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF
  };
  const string lokey_s((const char *) &lokey_cstr[0], ARRAY_NELEMS(lokey_cstr));
  const string hikey_s((const char *) &hikey_cstr[0], ARRAY_NELEMS(hikey_cstr));

  string lokey_s_next(lokey_s);
  lokey_s_next.resize(lokey_s_next.size() + 1);

  const varkey hikey(hikey_s);

  testing_concurrent_btree btr;
  ALWAYS_ASSERT(btr.insert(varkey(lokey_s), (typename testing_concurrent_btree::value_type) 0x123));

  test_range_scan_helper::expect ex(0);
  test_range_scan_helper tester(btr, varkey(lokey_s_next), &hikey, false, ex);
  tester.test();

#ifdef HAVE_REVERSE_RANGE_SCANS
  const varkey lokey(lokey_s);
  test_range_scan_helper tester_rev(btr, varkey(hikey_s), &lokey, true, ex);
  tester_rev.test();
#endif
}

static void
test_null_keys()
{
  const uint8_t k0[] = {};
  const uint8_t k1[] = {'\0'};
  const uint8_t k2[] = {'\0', '\0'};
  const uint8_t k3[] = {'\0', '\0', '\0'};
  const uint8_t k4[] = {'\0', '\0', '\0', '\0'};
  const uint8_t k5[] = {'\0', '\0', '\0', '\0', '\0'};
  const uint8_t k6[] = {'\0', '\0', '\0', '\0', '\0', '\0'};
  const uint8_t k7[] = {'\0', '\0', '\0', '\0', '\0', '\0', '\0'};
  const uint8_t k8[] = {'\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0'};
  const uint8_t k9[] = {'\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0'};
  const uint8_t k10[] = {'\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0'};
  const uint8_t *keys[] = {k0, k1, k2, k3, k4, k5, k6, k7, k8, k9, k10};

  testing_concurrent_btree btr;

  for (size_t i = 0; i < ARRAY_NELEMS(keys); i++) {
    ALWAYS_ASSERT(btr.insert(varkey(keys[i], i), (typename testing_concurrent_btree::value_type) i));
    btr.invariant_checker();
  }

  for (size_t i = 0; i < ARRAY_NELEMS(keys); i++) {
    typename testing_concurrent_btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(varkey(keys[i], i), v));
    ALWAYS_ASSERT(v == (typename testing_concurrent_btree::value_type) i);
  }

  for (size_t i = 1; i <= 20; i++) {
    ALWAYS_ASSERT(btr.insert(u64_varkey(i), (typename testing_concurrent_btree::value_type) i));
    btr.invariant_checker();
  }

  for (size_t i = 0; i < ARRAY_NELEMS(keys); i++) {
    typename testing_concurrent_btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(varkey(keys[i], i), v));
    ALWAYS_ASSERT(v == (typename testing_concurrent_btree::value_type) i);
  }

  for (size_t i = 1; i <= 20; i++) {
    typename testing_concurrent_btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(u64_varkey(i), v));
    ALWAYS_ASSERT(v == (typename testing_concurrent_btree::value_type) i);
  }
}

static void
test_null_keys_2()
{
  const size_t nprefixes = 200;

  testing_concurrent_btree btr;

  fast_random r(9084398309893);

  set<string> prefixes;
  for (size_t i = 0; i < nprefixes; i++) {
  retry:
    const string k(r.next_string(r.next() % 30));
    if (prefixes.count(k) == 1)
      goto retry;
    prefixes.insert(k);
  }

  set<string> keys;
  for (auto &prefix : prefixes) {
    for (size_t i = 1; i <= 12; i++) {
      std::string x(prefix);
      x.resize(x.size() + i);
      keys.insert(x);
    }
  }

  size_t ctr = 1;
  for (auto it = keys.begin(); it != keys.end(); ++it, ++ctr) {
    ALWAYS_ASSERT(btr.insert(varkey(*it), (typename testing_concurrent_btree::value_type) it->data()));
    btr.invariant_checker();
    ALWAYS_ASSERT(btr.size() == ctr);
  }
  ALWAYS_ASSERT(btr.size() == keys.size());

  for (auto it = keys.begin(); it != keys.end(); ++it) {
    typename testing_concurrent_btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(varkey(*it), v));
    ALWAYS_ASSERT(v == (typename testing_concurrent_btree::value_type) it->data());
  }

  test_range_scan_helper::expect ex(keys);
  test_range_scan_helper tester(btr, varkey(*keys.begin()), NULL, false, ex);
  tester.test();

#ifdef HAVE_REVERSE_RANGE_SCANS
  test_range_scan_helper tester_rev(btr, varkey(*keys.rbegin()), NULL, true, ex);
  tester_rev.test();
#endif

  ctr = keys.size() - 1;
  for (auto it = keys.begin(); it != keys.end(); ++it, --ctr) {
    ALWAYS_ASSERT(btr.remove(varkey(*it)));
    btr.invariant_checker();
    ALWAYS_ASSERT(btr.size() == ctr);
  }
  ALWAYS_ASSERT(btr.size() == 0);
}

static inline string
maxkey(unsigned size)
{
  return string(size, 255);
}

static void
test_random_keys()
{
  testing_concurrent_btree btr;
  fast_random r(43698);

  const size_t nkeys = 10000;
  const unsigned int maxkeylen = 1000;

  set<string> keyset;
  vector<string> keys;
  keys.resize(nkeys);
  for (size_t i = 0; i < nkeys; i++) {
  retry:
    string k = r.next_readable_string(r.next() % (maxkeylen + 1));
    if (keyset.count(k) == 1)
      goto retry;
    keyset.insert(k);
    swap(keys[i], k);
    btr.insert(varkey(keys[i]), (typename testing_concurrent_btree::value_type) keys[i].data());
    btr.invariant_checker();
  }

  ALWAYS_ASSERT(btr.size() == keyset.size());

  for (size_t i = 0; i < nkeys; i++) {
    typename testing_concurrent_btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(varkey(keys[i]), v));
    ALWAYS_ASSERT(v == (typename testing_concurrent_btree::value_type) keys[i].data());
  }

  test_range_scan_helper::expect ex(keyset);
  test_range_scan_helper tester(btr, varkey(""), NULL, false, ex);
  tester.test();

#ifdef HAVE_REVERSE_RANGE_SCANS
  const string mkey = maxkey(maxkeylen);
  test_range_scan_helper tester_rev(btr, varkey(mkey), NULL, true, ex);
  tester_rev.test();
#endif

  for (size_t i = 0; i < nkeys; i++) {
    btr.remove(varkey(keys[i]));
    btr.invariant_checker();
  }
  ALWAYS_ASSERT(btr.size() == 0);
}

static void
test_insert_remove_mix()
{
  testing_concurrent_btree btr;
  fast_random r(38953623328597);

  // bootstrap with keys, then alternate insert/remove
  const size_t nkeys_start = 100000;

  vector<string> start_keys_v;
  set<string> start_keys;
  for (size_t i = 0; i < nkeys_start; i++) {
  retry:
    string k = r.next_readable_string(r.next() % 200);
    if (start_keys.count(k) == 1)
      goto retry;
    start_keys_v.push_back(k);
    start_keys.insert(k);
    ALWAYS_ASSERT(btr.insert(varkey(k), (typename testing_concurrent_btree::value_type) k.data()));
  }
  btr.invariant_checker();
  ALWAYS_ASSERT(btr.size() == start_keys.size());

  vector<string> insert_keys_v;
  set<string> insert_keys;
  for (size_t i = 0; i < nkeys_start; i++) {
  retry1:
    string k = r.next_readable_string(r.next() % 200);
    if (start_keys.count(k) == 1 || insert_keys.count(k) == 1)
      goto retry1;
    insert_keys_v.push_back(k);
    insert_keys.insert(k);
  }

  for (size_t i = 0; i < nkeys_start; i++) {
    ALWAYS_ASSERT(btr.remove(varkey(start_keys_v[i])));
    ALWAYS_ASSERT(btr.insert(varkey(insert_keys_v[i]), (typename testing_concurrent_btree::value_type) insert_keys_v[i].data()));
  }
  btr.invariant_checker();
  ALWAYS_ASSERT(btr.size() == insert_keys.size());
}

namespace mp_test1_ns {

  static const size_t nkeys = 20000;

  class ins0_worker : public btree_worker {
  public:
    ins0_worker(testing_concurrent_btree &btr) : btree_worker(btr) {}
    virtual void run()
    {
      for (size_t i = 0; i < nkeys / 2; i++)
        btr->insert(u64_varkey(i), (typename testing_concurrent_btree::value_type) i);
    }
  };

  class ins1_worker : public btree_worker {
  public:
    ins1_worker(testing_concurrent_btree &btr) : btree_worker(btr) {}
    virtual void run()
    {
      for (size_t i = nkeys / 2; i < nkeys; i++)
        btr->insert(u64_varkey(i), (typename testing_concurrent_btree::value_type) i);
    }
  };
}

static void
mp_test1()
{
  using namespace mp_test1_ns;

  // test a bunch of concurrent inserts
  testing_concurrent_btree btr;

  ins0_worker w0(btr);
  ins1_worker w1(btr);

  w0.start(); w1.start();
  w0.join(); w1.join();

  btr.invariant_checker();
  for (size_t i = 0; i < nkeys; i++) {
    typename testing_concurrent_btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(u64_varkey(i), v));
    ALWAYS_ASSERT(v == (typename testing_concurrent_btree::value_type) i);
  }
  ALWAYS_ASSERT(btr.size() == nkeys);
}

namespace mp_test2_ns {

  static const size_t nkeys = 20000;

  class rm0_worker : public btree_worker {
  public:
    rm0_worker(testing_concurrent_btree &btr) : btree_worker(btr) {}
    virtual void run()
    {
      for (size_t i = 0; i < nkeys / 2; i++)
        btr->remove(u64_varkey(i));
    }
  };

  class rm1_worker : public btree_worker {
  public:
    rm1_worker(testing_concurrent_btree &btr) : btree_worker(btr) {}
    virtual void run()
    {
      for (size_t i = nkeys / 2; i < nkeys; i++)
        btr->remove(u64_varkey(i));
    }
  };
}

static void
mp_test2()
{
  using namespace mp_test2_ns;

  // test a bunch of concurrent removes
  testing_concurrent_btree btr;

  for (size_t i = 0; i < nkeys; i++)
    btr.insert(u64_varkey(u64_varkey(i)), (typename testing_concurrent_btree::value_type) i);
  btr.invariant_checker();

  rm0_worker w0(btr);
  rm1_worker w1(btr);

  w0.start(); w1.start();
  w0.join(); w1.join();

  btr.invariant_checker();
  for (size_t i = 0; i < nkeys; i++) {
    typename testing_concurrent_btree::value_type v = 0;
    ALWAYS_ASSERT(!btr.search(u64_varkey(i), v));
  }
  ALWAYS_ASSERT(btr.size() == 0);
}

namespace mp_test3_ns {

  static const size_t nkeys = 20000;

  class rm0_worker : public btree_worker {
  public:
    rm0_worker(testing_concurrent_btree &btr) : btree_worker(btr) {}
    virtual void run()
    {
      // remove the even keys
      for (size_t i = 0; i < nkeys; i += 2)
        btr->remove(u64_varkey(i));
    }
  };

  class ins0_worker : public btree_worker {
  public:
    ins0_worker(testing_concurrent_btree &btr) : btree_worker(btr) {}
    virtual void run()
    {
      // insert the odd keys
      for (size_t i = 1; i < nkeys; i += 2)
        btr->insert(u64_varkey(i), (typename testing_concurrent_btree::value_type) i);
    }
  };
}

static void
mp_test3()
{
  using namespace mp_test3_ns;

  // test a bunch of concurrent inserts and removes
  testing_concurrent_btree btr;

  // insert the even keys
  for (size_t i = 0; i < nkeys; i += 2)
    btr.insert(u64_varkey(u64_varkey(i)), (typename testing_concurrent_btree::value_type) i);
  btr.invariant_checker();

  rm0_worker w0(btr);
  ins0_worker w1(btr);

  w0.start(); w1.start();
  w0.join(); w1.join();

  btr.invariant_checker();

  // should find no even keys
  for (size_t i = 0; i < nkeys; i += 2) {
    typename testing_concurrent_btree::value_type v = 0;
    ALWAYS_ASSERT(!btr.search(u64_varkey(i), v));
  }

  // should find all odd keys
  for (size_t i = 1; i < nkeys; i += 2) {
    typename testing_concurrent_btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(u64_varkey(i), v));
    ALWAYS_ASSERT(v == (typename testing_concurrent_btree::value_type) i);
  }

  ALWAYS_ASSERT(btr.size() == nkeys / 2);
}

namespace mp_test4_ns {

  static const size_t nkeys = 20000;

  class search0_worker : public btree_worker {
  public:
    search0_worker(testing_concurrent_btree &btr) : btree_worker(btr) {}
    virtual void run()
    {
      // search the even keys
      for (size_t i = 0; i < nkeys; i += 2) {
        typename testing_concurrent_btree::value_type v = 0;
        ALWAYS_ASSERT(btr->search(u64_varkey(i), v));
        ALWAYS_ASSERT(v == (typename testing_concurrent_btree::value_type) i);
      }
    }
  };

  class ins0_worker : public btree_worker {
  public:
    ins0_worker(testing_concurrent_btree &btr) : btree_worker(btr) {}
    virtual void run()
    {
      // insert the odd keys
      for (size_t i = 1; i < nkeys; i += 2)
        btr->insert(u64_varkey(i), (typename testing_concurrent_btree::value_type) i);
    }
  };

  class rm0_worker : public btree_worker {
  public:
    rm0_worker(testing_concurrent_btree &btr) : btree_worker(btr) {}
    virtual void run()
    {
      // remove and reinsert odd keys
      for (size_t i = 1; i < nkeys; i += 2) {
        btr->remove(u64_varkey(i));
        btr->insert(u64_varkey(i), (typename testing_concurrent_btree::value_type) i);
      }
    }
  };
}

static void
mp_test4()
{
  using namespace mp_test4_ns;

  // test a bunch of concurrent searches, inserts, and removes
  testing_concurrent_btree btr;

  // insert the even keys
  for (size_t i = 0; i < nkeys; i += 2)
    btr.insert(u64_varkey(u64_varkey(i)), (typename testing_concurrent_btree::value_type) i);
  btr.invariant_checker();

  search0_worker w0(btr);
  ins0_worker w1(btr);
  rm0_worker w2(btr);

  w0.start(); w1.start(); w2.start();
  w0.join(); w1.join(); w2.join();

  btr.invariant_checker();

  // should find all keys
  for (size_t i = 0; i < nkeys; i++) {
    typename testing_concurrent_btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(u64_varkey(i), v));
    ALWAYS_ASSERT(v == (typename testing_concurrent_btree::value_type) i);
  }

  ALWAYS_ASSERT(btr.size() == nkeys);
}

namespace mp_test_pinning_ns {
  static const size_t keys_per_thread = 1000;
  static const size_t nthreads = 4;
  static atomic<bool> running(true);
  class worker : public btree_worker {
  public:
    worker(unsigned int thread, testing_concurrent_btree &btr)
      : btree_worker(btr), thread(thread) {}
    virtual void
    run()
    {
      rcu::s_instance.pin_current_thread(thread % coreid::num_cpus_online());
      for (unsigned mode = 0; running.load(); mode++) {
        for (size_t i = thread * keys_per_thread;
             running.load() && i < (thread + 1) * keys_per_thread;
             i++) {
          if (mode % 2) {
            // remove
            btr->remove(u64_varkey(i));
          } else {
            // insert
            btr->insert(u64_varkey(i), (typename testing_concurrent_btree::value_type) i);
          }
        }
      }
    }
  private:
    unsigned int thread;
  };
}

static void
mp_test_pinning()
{
  using namespace mp_test_pinning_ns;
  testing_concurrent_btree btr;
  vector<unique_ptr<worker>> workers;
  for (size_t i = 0; i < nthreads; i++)
    workers.emplace_back(new worker(i, btr));
  for (auto &p : workers)
    p->start();
  sleep(5);
  running.store(false);
  for (auto &p : workers)
    p->join();
  btr.invariant_checker();
}

namespace mp_test_inserts_removes_ns {
  static const size_t keys_per_thread = 10000;
  static const size_t nthreads = 4;
  class worker : public btree_worker {
  public:
    worker(bool inserts, unsigned int thread,
           testing_concurrent_btree &btr)
      : btree_worker(btr), inserts(inserts), thread(thread) {}
    virtual void
    run()
    {
      for (size_t i = thread * keys_per_thread;
           i < (thread + 1) * keys_per_thread;
           i++) {
        if (inserts)
          // insert
          btr->insert(u64_varkey(i), (typename testing_concurrent_btree::value_type) i);
        else
          btr->remove(u64_varkey(i));
      }
    }
  private:
    bool inserts;
    unsigned int thread;
  };
}

static void
mp_test_inserts_removes()
{
  using namespace mp_test_inserts_removes_ns;
  for (size_t iter = 0; iter < 3; iter++) {
    testing_concurrent_btree btr;
    vector<unique_ptr<worker>> workers;

    for (size_t i = 0; i < nthreads; i++)
      workers.emplace_back(new worker(true, i, btr));
    for (auto &p : workers)
      p->start();
    for (auto &p : workers)
      p->join();
    btr.invariant_checker();
    workers.clear();

    for (size_t i = 0; i < nthreads; i++)
      workers.emplace_back(new worker(false, i, btr));
    for (auto &p : workers)
      p->start();
    for (auto &p : workers)
      p->join();
    btr.invariant_checker();
    workers.clear();

    for (size_t i = 0; i < nthreads; i++) {
      workers.emplace_back(new worker(true, i, btr));
      workers.emplace_back(new worker(false, i, btr));
    }
    for (auto &p : workers)
      p->start();
    for (auto &p : workers)
      p->join();
    btr.invariant_checker();
    workers.clear();
  }
}

namespace mp_test5_ns {

  static const size_t niters = 100000;
  static const size_t max_key = 45;

  typedef set<typename testing_concurrent_btree::key_slice> key_set;

  struct summary {
    key_set inserts;
    key_set removes;
  };

  class worker : public btree_worker {
  public:
    worker(unsigned int seed, testing_concurrent_btree &btr) : btree_worker(btr), seed(seed) {}
    virtual void run()
    {
      unsigned int s = seed;
      // 60% search, 30% insert, 10% remove
      for (size_t i = 0; i < niters; i++) {
        double choice = double(rand_r(&s)) / double(RAND_MAX);
        typename testing_concurrent_btree::key_slice k = rand_r(&s) % max_key;
        if (choice < 0.6) {
          typename testing_concurrent_btree::value_type v = 0;
          if (btr->search(u64_varkey(k), v))
            ALWAYS_ASSERT(v == (typename testing_concurrent_btree::value_type) k);
        } else if (choice < 0.9) {
          btr->insert(u64_varkey(k), (typename testing_concurrent_btree::value_type) k);
          sum.inserts.insert(k);
        } else {
          btr->remove(u64_varkey(k));
          sum.removes.insert(k);
        }
      }
    }
    summary sum;
  private:
    unsigned int seed;
  };
}

static void
mp_test5()
{
  using namespace mp_test5_ns;

  testing_concurrent_btree btr;

  worker w0(2145906155, btr);
  worker w1(409088773, btr);
  worker w2(4199288861, btr);
  worker w3(496889962, btr);

  w0.start(); w1.start(); w2.start(); w3.start();
  w0.join(); w1.join(); w2.join(); w3.join();

  summary *s0, *s1, *s2, *s3;
  s0 = (summary *) &w0.sum;
  s1 = (summary *) &w1.sum;
  s2 = (summary *) &w2.sum;
  s3 = (summary *) &w3.sum;

  key_set inserts;
  key_set removes;

  summary *sums[] = { s0, s1, s2, s3 };
  for (size_t i = 0; i < ARRAY_NELEMS(sums); i++) {
    inserts.insert(sums[i]->inserts.begin(), sums[i]->inserts.end());
   removes.insert(sums[i]->removes.begin(), sums[i]->removes.end());
  }

  cerr << "num_inserts: " << inserts.size() << endl;
  cerr << "num_removes: " << removes.size() << endl;

  for (key_set::iterator it = inserts.begin(); it != inserts.end(); ++it) {
    if (removes.count(*it) == 1)
      continue;
    typename testing_concurrent_btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(u64_varkey(*it), v));
    ALWAYS_ASSERT(v == (typename testing_concurrent_btree::value_type) *it);
  }

  btr.invariant_checker();
  cerr << "btr size: " << btr.size() << endl;
}

namespace mp_test6_ns {
  static const size_t nthreads = 16;
  static const size_t ninsertkeys_perthread = 100000;
  static const size_t nremovekeys_perthread = 100000;

  typedef vector<typename testing_concurrent_btree::key_slice> key_vec;

  class insert_worker : public btree_worker {
  public:
    insert_worker(const vector<typename testing_concurrent_btree::key_slice> &keys, testing_concurrent_btree &btr)
      : btree_worker(btr), keys(keys) {}
    virtual void run()
    {
      for (size_t i = 0; i < keys.size(); i++)
        btr->insert(u64_varkey(keys[i]), (typename testing_concurrent_btree::value_type) keys[i]);
    }
  private:
    vector<typename testing_concurrent_btree::key_slice> keys;
  };

  class remove_worker : public btree_worker {
  public:
    remove_worker(const vector<typename testing_concurrent_btree::key_slice> &keys, testing_concurrent_btree &btr)
      : btree_worker(btr), keys(keys) {}
    virtual void run()
    {
      for (size_t i = 0; i < keys.size(); i++)
        btr->remove(u64_varkey(keys[i]));
    }
  private:
    vector<typename testing_concurrent_btree::key_slice> keys;
  };
}

static void
mp_test6()
{
  using namespace mp_test6_ns;

  testing_concurrent_btree btr;
  vector<key_vec> inps;
  set<unsigned long> insert_keys, remove_keys;

  fast_random r(87643982);
  for (size_t i = 0; i < nthreads / 2; i++) {
    key_vec inp;
    for (size_t j = 0; j < ninsertkeys_perthread; j++) {
      unsigned long k = r.next();
      insert_keys.insert(k);
      inp.push_back(k);
    }
    inps.push_back(inp);
  }
  for (size_t i = nthreads / 2; i < nthreads; i++) {
    key_vec inp;
    for (size_t j = 0; j < nremovekeys_perthread;) {
      unsigned long k = r.next();
      if (insert_keys.count(k) == 1)
        continue;
      btr.insert(u64_varkey(k), (typename testing_concurrent_btree::value_type) k);
      remove_keys.insert(k);
      inp.push_back(k);
      j++;
    }
    inps.push_back(inp);
  }

  vector<btree_worker*> workers;
  for (size_t i = 0; i < nthreads / 2; i++)
    workers.push_back(new insert_worker(inps[i], btr));
  for (size_t i = nthreads / 2; i < nthreads; i++)
    workers.push_back(new remove_worker(inps[i], btr));
  for (size_t i = 0; i < nthreads; i++)
    workers[i]->start();
  for (size_t i = 0; i < nthreads; i++)
    workers[i]->join();

  btr.invariant_checker();

  ALWAYS_ASSERT(btr.size() == insert_keys.size());
  for (set<unsigned long>::iterator it = insert_keys.begin();
       it != insert_keys.end(); ++it) {
    typename testing_concurrent_btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(u64_varkey(*it), v));
    ALWAYS_ASSERT(v == (typename testing_concurrent_btree::value_type) *it);
  }
  for (set<unsigned long>::iterator it = remove_keys.begin();
       it != remove_keys.end(); ++it) {
    typename testing_concurrent_btree::value_type v = 0;
    ALWAYS_ASSERT(!btr.search(u64_varkey(*it), v));
  }

  for (size_t i = 0; i < nthreads; i++)
    delete workers[i];
}

namespace mp_test7_ns {
  static const size_t nkeys = 50;
  static volatile bool running = false;

  typedef vector<typename testing_concurrent_btree::key_slice> key_vec;

  struct scan_callback {
    typedef vector<
      pair< std::string, typename testing_concurrent_btree::value_type > > kv_vec;
    scan_callback(kv_vec *data) : data(data) {}
    inline bool
    operator()(const typename testing_concurrent_btree::string_type &k, typename testing_concurrent_btree::value_type v) const
    {
      //ALWAYS_ASSERT(data->empty() || data->back().first < k.str());
      std::string k_str(k);
      if (!data->empty() && data->back().first >= k_str) {
        cerr << "prev: <" << hexify(data->back().first) << ">" << endl;
        cerr << "cur : <" << hexify(k_str) << ">" << endl;
        ALWAYS_ASSERT(false);
      }
      data->push_back(make_pair(std::move(k_str), v));
      return true;
    }
    kv_vec *data;
  };

  class lookup_worker : public btree_worker {
  public:
    lookup_worker(unsigned long seed, const key_vec &keys, testing_concurrent_btree &btr)
      : btree_worker(btr), seed(seed), keys(keys)
    {}
    virtual void run()
    {
      fast_random r(seed);
      while (running) {
        uint64_t k = keys[r.next() % keys.size()];
        typename testing_concurrent_btree::value_type v = NULL;
        ALWAYS_ASSERT(btr->search(u64_varkey(k), v));
        ALWAYS_ASSERT(v == (typename testing_concurrent_btree::value_type) k);
      }
    }
    unsigned long seed;
    key_vec keys;
  };

  class scan_worker : public btree_worker {
  public:
    scan_worker(const key_vec &keys, testing_concurrent_btree &btr)
      : btree_worker(btr), keys(keys)
    {}
    virtual void run()
    {
      while (running) {
        scan_callback::kv_vec data;
        scan_callback cb(&data);
        btr->search_range(u64_varkey(nkeys / 2), NULL, cb);
        set<typename testing_concurrent_btree::string_type> scan_keys;
        std::string prev;
        for (size_t i = 0; i < data.size(); i++) {
          if (i != 0) {
            ALWAYS_ASSERT(data[i].first != prev);
            ALWAYS_ASSERT(data[i].first > prev);
          }
          scan_keys.insert(data[i].first);
          prev = data[i].first;
        }
        for (size_t i = 0; i < keys.size(); i++) {
          if (keys[i] < (nkeys / 2))
            continue;
          ALWAYS_ASSERT(scan_keys.count(u64_varkey(keys[i]).str()) == 1);
        }
      }
    }
    key_vec keys;
  };

  class mod_worker : public btree_worker {
  public:
    mod_worker(const key_vec &keys, testing_concurrent_btree &btr)
      : btree_worker(btr), keys(keys)
    {}
    virtual void run()
    {
      bool insert = true;
      for (size_t i = 0; running; i = (i + 1) % keys.size(), insert = !insert) {
        if (insert)
          btr->insert(u64_varkey(keys[i]), (typename testing_concurrent_btree::value_type) keys[i]);
        else
          btr->remove(u64_varkey(keys[i]));
      }
    }
    key_vec keys;
  };
}

static void
mp_test7()
{
  using namespace mp_test7_ns;
  fast_random r(904380439);
  key_vec lookup_keys;
  key_vec mod_keys;
  for (size_t i = 0; i < nkeys; i++) {
    if (r.next() % 2)
      mod_keys.push_back(i);
    else
      lookup_keys.push_back(i);
  }

  testing_concurrent_btree btr;
  for (size_t i = 0; i < lookup_keys.size(); i++)
    btr.insert(u64_varkey(lookup_keys[i]), (typename testing_concurrent_btree::value_type) lookup_keys[i]);
  btr.invariant_checker();

  lookup_worker w0(2398430, lookup_keys, btr);
  lookup_worker w1(8532, lookup_keys, btr);
  lookup_worker w2(23, lookup_keys, btr);
  lookup_worker w3(1328209843, lookup_keys, btr);
  scan_worker w4(lookup_keys, btr);
  scan_worker w5(lookup_keys, btr);
  mod_worker w6(mod_keys, btr);

  running = true;
  COMPILER_MEMORY_FENCE;
  w0.start(); w1.start(); w2.start(); w3.start(); w4.start(); w5.start(); w6.start();
  sleep(10);
  COMPILER_MEMORY_FENCE;
  running = false;
  COMPILER_MEMORY_FENCE;
  w0.join(); w1.join(); w2.join(); w3.join(); w4.join(); w5.join(); w6.join();
}

namespace mp_test8_ns {
  static const size_t nthreads = 16;
  static const size_t ninsertkeys_perthread = 100000;
  static const size_t nremovekeys_perthread = 100000;

  typedef vector<string> key_vec;

  class insert_worker : public btree_worker {
  public:
    insert_worker(const vector<string> &keys, testing_concurrent_btree &btr)
      : btree_worker(btr), keys(keys) {}
    virtual void run()
    {
      for (size_t i = 0; i < keys.size(); i++)
        ALWAYS_ASSERT(btr->insert(varkey(keys[i]), (typename testing_concurrent_btree::value_type) keys[i].data()));
    }
  private:
    vector<string> keys;
  };

  class remove_worker : public btree_worker {
  public:
    remove_worker(const vector<string> &keys, testing_concurrent_btree &btr)
      : btree_worker(btr), keys(keys) {}
    virtual void run()
    {
      for (size_t i = 0; i < keys.size(); i++)
        ALWAYS_ASSERT(btr->remove(varkey(keys[i])));
    }
  private:
    vector<string> keys;
  };
}

static void
mp_test8()
{
  using namespace mp_test8_ns;

  testing_concurrent_btree btr;
  vector<key_vec> inps;
  set<string> insert_keys, remove_keys;

  fast_random r(83287583);
  for (size_t i = 0; i < nthreads / 2; i++) {
    key_vec inp;
    for (size_t j = 0; j < ninsertkeys_perthread; j++) {
    retry:
      string k = r.next_string(r.next() % 200);
      if (insert_keys.count(k) == 1)
        goto retry;
      insert_keys.insert(k);
      inp.push_back(k);
    }
    inps.push_back(inp);
  }
  for (size_t i = nthreads / 2; i < nthreads; i++) {
    key_vec inp;
    for (size_t j = 0; j < nremovekeys_perthread;) {
      string k = r.next_string(r.next() % 200);
      if (insert_keys.count(k) == 1 || remove_keys.count(k) == 1)
        continue;
      ALWAYS_ASSERT(btr.insert(varkey(k), (typename testing_concurrent_btree::value_type) k.data()));
      remove_keys.insert(k);
      inp.push_back(k);
      j++;
    }
    inps.push_back(inp);
  }

  btr.invariant_checker();

  vector<btree_worker*> workers;
  for (size_t i = 0; i < nthreads / 2; i++)
    workers.push_back(new insert_worker(inps[i], btr));
  for (size_t i = nthreads / 2; i < nthreads; i++)
    workers.push_back(new remove_worker(inps[i], btr));
  for (size_t i = 0; i < nthreads; i++)
    workers[i]->start();
  for (size_t i = 0; i < nthreads; i++)
    workers[i]->join();

  btr.invariant_checker();

  ALWAYS_ASSERT(btr.size() == insert_keys.size());
  for (set<string>::iterator it = insert_keys.begin();
       it != insert_keys.end(); ++it) {
    typename testing_concurrent_btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(varkey(*it), v));
  }
  for (set<string>::iterator it = remove_keys.begin();
       it != remove_keys.end(); ++it) {
    typename testing_concurrent_btree::value_type v = 0;
    ALWAYS_ASSERT(!btr.search(varkey(*it), v));
  }

  for (size_t i = 0; i < nthreads; i++)
    delete workers[i];
}

namespace mp_test_long_keys_ns {
  static const size_t nthreads = 16;
  static const size_t ninsertkeys_perthread = 500000;
  static const size_t nremovekeys_perthread = 500000;

  typedef vector<string> key_vec;

  class insert_worker : public btree_worker {
  public:
    insert_worker(const vector<string> &keys, testing_concurrent_btree &btr)
      : btree_worker(btr), keys(keys) {}
    virtual void run()
    {
      for (size_t i = 0; i < keys.size(); i++)
        ALWAYS_ASSERT(btr->insert(varkey(keys[i]), (typename testing_concurrent_btree::value_type) keys[i].data()));
    }
  private:
    vector<string> keys;
  };

  class remove_worker : public btree_worker {
  public:
    remove_worker(const vector<string> &keys, testing_concurrent_btree &btr)
      : btree_worker(btr), keys(keys) {}
    virtual void run()
    {
      for (size_t i = 0; i < keys.size(); i++)
        ALWAYS_ASSERT(btr->remove(varkey(keys[i])));
    }
  private:
    vector<string> keys;
  };

  static volatile bool running = false;

  class scan_worker : public btree_worker {
  public:
    scan_worker(const set<string> &ex, testing_concurrent_btree &btr, bool reverse)
      : btree_worker(btr), ex(ex), reverse_(reverse) {}
    virtual void run()
    {
      const string mkey = maxkey(200+9);
      while (running) {
        if (!reverse_) {
          test_range_scan_helper tester(*btr, varkey(""), NULL, false,
              ex, test_range_scan_helper::EXPECT_ATLEAST);
          tester.test();
        } else {
          test_range_scan_helper tester(*btr, varkey(mkey), NULL, true,
              ex, test_range_scan_helper::EXPECT_ATLEAST);
          tester.test();
        }
      }
    }
  private:
    test_range_scan_helper::expect ex;
    bool reverse_;
  };
}

static void
mp_test_long_keys()
{
  // all keys at least 9-bytes long
  using namespace mp_test_long_keys_ns;

  testing_concurrent_btree btr;
  vector<key_vec> inps;
  set<string> existing_keys, insert_keys, remove_keys;

  fast_random r(189230589352);
  for (size_t i = 0; i < 10000; i++) {
  retry0:
    string k = r.next_string((r.next() % 200) + 9);
    if (existing_keys.count(k) == 1)
      goto retry0;
    existing_keys.insert(k);
    ALWAYS_ASSERT(btr.insert(varkey(k), (typename testing_concurrent_btree::value_type) k.data()));
  }
  ALWAYS_ASSERT(btr.size() == existing_keys.size());

  for (size_t i = 0; i < nthreads / 2; i++) {
    key_vec inp;
    for (size_t j = 0; j < ninsertkeys_perthread; j++) {
    retry:
      string k = r.next_string((r.next() % 200) + 9);
      if (insert_keys.count(k) == 1 || existing_keys.count(k) == 1)
        goto retry;
      insert_keys.insert(k);
      inp.push_back(k);
    }
    inps.push_back(inp);
  }

  for (size_t i = nthreads / 2; i < nthreads; i++) {
    key_vec inp;
    for (size_t j = 0; j < nremovekeys_perthread;) {
      string k = r.next_string((r.next() % 200) + 9);
      if (insert_keys.count(k) == 1 || existing_keys.count(k) == 1 || remove_keys.count(k) == 1)
        continue;
      ALWAYS_ASSERT(btr.insert(varkey(k), (typename testing_concurrent_btree::value_type) k.data()));
      remove_keys.insert(k);
      inp.push_back(k);
      j++;
    }
    inps.push_back(inp);
  }

  ALWAYS_ASSERT(btr.size() == (insert_keys.size() + existing_keys.size()));
  btr.invariant_checker();

  vector<btree_worker*> workers, running_workers;
  running = true;
  for (size_t i = 0; i < nthreads / 2; i++)
    workers.push_back(new insert_worker(inps[i], btr));
  for (size_t i = nthreads / 2; i < nthreads; i++)
    workers.push_back(new remove_worker(inps[i], btr));
  for (size_t i = 0; i < 4; i++)
    running_workers.push_back(new scan_worker(existing_keys, btr, (i % 2)));
  for (size_t i = 0; i < nthreads; i++)
    workers[i]->start();
  for (size_t i = 0; i < running_workers.size(); i++)
    running_workers[i]->start();
  for (size_t i = 0; i < nthreads; i++)
    workers[i]->join();
  running = false;
  for (size_t i = 0; i < running_workers.size(); i++)
    running_workers[i]->join();

  btr.invariant_checker();

  ALWAYS_ASSERT(btr.size() == (insert_keys.size() + existing_keys.size()));
  for (set<string>::iterator it = insert_keys.begin();
       it != insert_keys.end(); ++it) {
    typename testing_concurrent_btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(varkey(*it), v));
  }
  for (set<string>::iterator it = remove_keys.begin();
       it != remove_keys.end(); ++it) {
    typename testing_concurrent_btree::value_type v = 0;
    ALWAYS_ASSERT(!btr.search(varkey(*it), v));
  }

  for (size_t i = 0; i < nthreads; i++)
    delete workers[i];
  for (size_t i = 0; i < running_workers.size(); i++)
    delete running_workers[i];
}

static void perf_test() UNUSED;
static void
perf_test()
{
  const size_t nrecs = 10000000;
  const size_t nlookups = 10000000;

  {
    srand(9876);
    map<uint64_t, uint64_t> m;
    {
      scoped_rate_timer t("map insert", nrecs);
      for (size_t i = 0; i < nrecs; i++)
        m[i] = i;
    }
    {
      scoped_rate_timer t("map random lookups", nlookups);
      for (size_t i = 0; i < nlookups; i++) {
        //uint64_t key = rand() % nrecs;
        uint64_t key = i;
        map<uint64_t, uint64_t>::iterator it =
          m.find(key);
        ALWAYS_ASSERT(it != m.end());
      }
    }
  }

  {
    srand(9876);
    testing_concurrent_btree btr;
    {
      scoped_rate_timer t("btree insert", nrecs);
      for (size_t i = 0; i < nrecs; i++)
        btr.insert(u64_varkey(u64_varkey(i)), (typename testing_concurrent_btree::value_type) i);
    }
    {
      scoped_rate_timer t("btree random lookups", nlookups);
      for (size_t i = 0; i < nlookups; i++) {
        //uint64_t key = rand() % nrecs;
        uint64_t key = i;
        typename testing_concurrent_btree::value_type v = 0;
        ALWAYS_ASSERT(btr.search(u64_varkey(key), v));
      }
    }
  }
}

namespace read_only_perf_test_ns {
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

  class worker : public btree_worker {
  public:
    worker(unsigned int seed, testing_concurrent_btree &btr) : btree_worker(btr), n(0), seed(seed) {}
    virtual void run()
    {
      fast_random r(seed);
      while (running) {
        typename testing_concurrent_btree::key_slice k = r.next() % nkeys;
        typename testing_concurrent_btree::value_type v = 0;
        ALWAYS_ASSERT(btr->search(u64_varkey(k), v));
        ALWAYS_ASSERT(v == (typename testing_concurrent_btree::value_type) k);
        n++;
      }
    }
    uint64_t n;
  private:
    unsigned int seed;
  };
}

static void read_only_perf_test() UNUSED;
static void
read_only_perf_test()
{
  using namespace read_only_perf_test_ns;

  testing_concurrent_btree btr;

  for (size_t i = 0; i < nkeys; i++)
    btr.insert(u64_varkey(i), (typename testing_concurrent_btree::value_type) i);
  cerr << "btree loaded, test starting" << endl;

  vector<worker *> workers;
  for (size_t i = 0; i < ARRAY_NELEMS(seeds); i++)
    workers.push_back(new worker(seeds[i], btr));

  running = true;
  util::timer t;
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

namespace write_only_perf_test_ns {
  const size_t nkeys = 140000000; // 140M
  //const size_t nkeys = 100000; // 100K

  unsigned long seeds[] = {
    17188055221422272641ULL,
    915721317773011804ULL,
    11607688859420148202ULL,
    16566896965529356730ULL,
    3687473034241167633ULL,
    1168118474092824592ULL,
    912212972587845337ULL,
    890657129662032640ULL,
    7557640044845923769ULL,
    9490577770668659131ULL,
    14081403972130650060ULL,
    14956552848279294368ULL,
    8669268465391111275ULL,
    1904251150166743550ULL,
    4418832947790992405ULL,
    9558684485283258563ULL,
  };

  class worker : public btree_worker {
  public:
    worker(unsigned int seed, testing_concurrent_btree &btr) : btree_worker(btr), seed(seed) {}
    virtual void run()
    {
      fast_random r(seed);
      for (size_t i = 0; i < nkeys / ARRAY_NELEMS(seeds); i++) {
        typename testing_concurrent_btree::key_slice k = r.next() % nkeys;
        btr->insert(u64_varkey(k), (typename testing_concurrent_btree::value_type) k);
      }
    }
  private:
    unsigned int seed;
  };
}

static void write_only_perf_test() UNUSED;
static void
write_only_perf_test()
{
  using namespace write_only_perf_test_ns;

  testing_concurrent_btree btr;

  vector<worker *> workers;
  for (size_t i = 0; i < ARRAY_NELEMS(seeds); i++)
    workers.push_back(new worker(seeds[i], btr));

  util::timer t;
  for (size_t i = 0; i < ARRAY_NELEMS(seeds); i++)
    workers[i]->start();
  for (size_t i = 0; i < ARRAY_NELEMS(seeds); i++) {
    workers[i]->join();
    delete workers[i];
  }

  double agg_throughput = double(nkeys) / (double(t.lap()) / 1000000.0);
  double avg_per_core_throughput = agg_throughput / double(ARRAY_NELEMS(seeds));

  cerr << "agg_write_throughput: " << agg_throughput << " puts/sec" << endl;
  cerr << "avg_per_core_write_throughput: " << avg_per_core_throughput << " puts/sec/core" << endl;
}

void
TestConcurrentBtreeFast()
{
  test1();
  test2();
  test3();
  test4();
  test6();
  test7();
  test_varlen_single_layer();
  test_varlen_multi_layer();
  test_two_layer();
  test_two_layer_range_scan();
  test_multi_layer_scan();
  test_null_keys();
  test_null_keys_2();
  test_random_keys();
  test_insert_remove_mix();
  mp_test_pinning();
  mp_test_inserts_removes();
  cout << "testing_concurrent_btree::TestFast passed" << endl;
}

void
TestConcurrentBtreeSlow()
{
  test5();
  mp_test1();
  mp_test2();
  mp_test3();
  mp_test4();
  mp_test5();
  mp_test6();
  mp_test7();
  mp_test8();
  mp_test_long_keys();
  //perf_test();
  //read_only_perf_test();
  //write_only_perf_test();
  cout << "testing_concurrent_btree::TestSlow passed" << endl;
}
