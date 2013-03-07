#include <iostream>
#include <tr1/functional>
#include <tr1/unordered_map>

#include "thread.h"
#include "txn.h"
#include "btree.h"
#include "txn_btree.h"
#include "varint.h"
#include "xbuf.h"
#include "small_vector.h"
#include "small_unordered_map.h"

using namespace std;
using namespace tr1;
using namespace util;

void
XbufTest()
{
  xbuf s = "hello";
  ALWAYS_ASSERT(s.size() == 5);
  ALWAYS_ASSERT(strncmp(s.data(), "hello", 5) == 0);
  xbuf y = s;
  ALWAYS_ASSERT(s == y);
  vector<xbuf> v;
  for (size_t i = 0; i < 5; i++)
    v.push_back("hi");
  unordered_map<xbuf, int> m;
  m["foo"] = 10;
  m["bar"] = 20;
  ostringstream buf;
  buf << s;
  ALWAYS_ASSERT(buf.str().size() == s.size());

  xbuf a = "hello";
  xbuf b = "world";
  swap(a, b);
  ALWAYS_ASSERT(a == "world");
  ALWAYS_ASSERT(b == "hello");

  xbuf t0;
  t0.resize(10, 'a');
  ALWAYS_ASSERT(t0.size() == 10);
  ALWAYS_ASSERT(t0 == xbuf("aaaaaaaaaa"));
  t0.append("foobar", 6);
  ALWAYS_ASSERT(t0 == xbuf("aaaaaaaaaafoobar"));
  t0.resize(5);
  ALWAYS_ASSERT(t0 == xbuf("aaaaa"));

  cout << xbuf("xbuf test passed") << endl;
}

namespace small_vector_ns {

typedef small_vector<string, 4> vec_type;
typedef vector<string> stl_vec_type;

template <typename VecType>
static void
init_vec0(VecType &v)
{
  ALWAYS_ASSERT(v.empty());
  ALWAYS_ASSERT(v.size() == 0);

  v.push_back("a");
  ALWAYS_ASSERT(!v.empty());
  ALWAYS_ASSERT(v.size() == 1);
  ALWAYS_ASSERT(v.front() == "a");
  ALWAYS_ASSERT(v.back() == "a");

  v.push_back("b");
  ALWAYS_ASSERT(v.size() == 2);
  ALWAYS_ASSERT(v.front() == "a");
  ALWAYS_ASSERT(v.back() == "b");

  v.push_back("c");
  ALWAYS_ASSERT(v.size() == 3);
  ALWAYS_ASSERT(v.front() == "a");
  ALWAYS_ASSERT(v.back() == "c");

  v.push_back("d");
  ALWAYS_ASSERT(v.size() == 4);
  ALWAYS_ASSERT(v.front() == "a");
  ALWAYS_ASSERT(v.back() == "d");
}

template <typename VecType>
static void
init_vec1(VecType &v)
{
  ALWAYS_ASSERT(v.empty());
  ALWAYS_ASSERT(v.size() == 0);

  v.push_back("a");
  ALWAYS_ASSERT(!v.empty());
  ALWAYS_ASSERT(v.size() == 1);
  ALWAYS_ASSERT(v.front() == "a");
  ALWAYS_ASSERT(v.back() == "a");

  v.push_back("b");
  ALWAYS_ASSERT(v.size() == 2);
  ALWAYS_ASSERT(v.front() == "a");
  ALWAYS_ASSERT(v.back() == "b");

  v.push_back("c");
  ALWAYS_ASSERT(v.size() == 3);
  ALWAYS_ASSERT(v.front() == "a");
  ALWAYS_ASSERT(v.back() == "c");

  v.push_back("d");
  ALWAYS_ASSERT(v.size() == 4);
  ALWAYS_ASSERT(v.front() == "a");
  ALWAYS_ASSERT(v.back() == "d");

  v.push_back("e");
  ALWAYS_ASSERT(v.size() == 5);
  ALWAYS_ASSERT(v.front() == "a");
  ALWAYS_ASSERT(v.back() == "e");

  v.push_back("f");
  ALWAYS_ASSERT(v.size() == 6);
  ALWAYS_ASSERT(v.front() == "a");
  ALWAYS_ASSERT(v.back() == "f");
}

template <typename VecA, typename VecB>
static void
assert_vecs_equal(VecA &v, const VecB &stl_v)
{
  ALWAYS_ASSERT(v.size() == stl_v.size());
  VecB tmp(v.begin(), v.end());
  ALWAYS_ASSERT(tmp == stl_v);
  const VecA &cv = v;
  VecB tmp1(cv.begin(), cv.end());
  ALWAYS_ASSERT(tmp1 == stl_v);
}

struct foo {
  int a;
  int b;
  int c;
  int d;

  foo()
    : a(), b(), c(), d()
  {}

  foo(int a, int b, int c, int d)
    : a(a), b(b), c(c), d(d)
  {}
};

struct PComp {
  inline bool
  operator()(const pair<uint32_t, uint32_t> &a,
             const pair<uint32_t, uint32_t> &b) const
  {
    return a.first < b.first;
  }
};

void
Test()
{
  {
    vec_type v;
    stl_vec_type stl_v;
    init_vec0(v);
    init_vec0(stl_v);
    vec_type v_copy(v);
    vec_type v_assign;
    ALWAYS_ASSERT(v_assign.empty());
    v_assign = v;
    assert_vecs_equal(v, stl_v);
    assert_vecs_equal(v_copy, stl_v);
    assert_vecs_equal(v_assign, stl_v);
    v.clear();
    assert_vecs_equal(v, stl_vec_type());
  }

  {
    vec_type v;
    stl_vec_type stl_v;
    init_vec1(v);
    init_vec1(stl_v);
    vec_type v_copy(v);
    vec_type v_assign;
    ALWAYS_ASSERT(v_assign.empty());
    v_assign = v;
    assert_vecs_equal(v, stl_v);
    assert_vecs_equal(v_copy, stl_v);
    assert_vecs_equal(v_assign, stl_v);
    v.clear();
    assert_vecs_equal(v, stl_vec_type());
  }

  {
    for (int iter = 0; iter < 10; iter++) {
      small_vector<foo> v;
      for (int i = 0; i < 20; i++) {
        v.push_back(foo(i, i + 1, i + 2, i + 3));
        ALWAYS_ASSERT(v.back().a == i);
        ALWAYS_ASSERT(v.back().b == (i + 1));
        ALWAYS_ASSERT(v.back().c == (i + 2));
        ALWAYS_ASSERT(v.back().d == (i + 3));
        ALWAYS_ASSERT(v[i].a == i);
        ALWAYS_ASSERT(v[i].b == (i + 1));
        ALWAYS_ASSERT(v[i].c == (i + 2));
        ALWAYS_ASSERT(v[i].d == (i + 3));
      }
      for (int i = 0; i < 20; i++) {
        ALWAYS_ASSERT(v[i].a == i);
        ALWAYS_ASSERT(v[i].b == (i + 1));
        ALWAYS_ASSERT(v[i].c == (i + 2));
        ALWAYS_ASSERT(v[i].d == (i + 3));
      }
    }
  }

  {
    small_vector<int> v;
    v.push_back(10);
    v.push_back(2);
    v.push_back(5);
    v.push_back(7);
    v.push_back(3);
    v.push_back(100);
    sort(v.begin(), v.end());

    vector<int> stl_v;
    stl_v.push_back(10);
    stl_v.push_back(2);
    stl_v.push_back(5);
    stl_v.push_back(7);
    stl_v.push_back(3);
    stl_v.push_back(100);
    sort(stl_v.begin(), stl_v.end());

    assert_vecs_equal(v, stl_v);
  }

  {
    small_vector<int, 3> v;
    v.push_back(10);
    v.push_back(2);
    v.push_back(5);
    v.push_back(7);
    v.push_back(3);
    v.push_back(100);
    sort(v.begin(), v.end());

    vector<int> stl_v;
    stl_v.push_back(10);
    stl_v.push_back(2);
    stl_v.push_back(5);
    stl_v.push_back(7);
    stl_v.push_back(3);
    stl_v.push_back(100);
    sort(stl_v.begin(), stl_v.end());

    assert_vecs_equal(v, stl_v);
  }

  {
    fast_random r(29395);
    small_vector< pair<uint32_t, uint32_t> > v;
    vector< pair<uint32_t, uint32_t> > stl_v;
    for (size_t i = 0; i < 48; i++) {
      uint32_t x = r.next();
      v.push_back(make_pair(x, x + 1));
      stl_v.push_back(make_pair(x, x + 1));
    }
    sort(v.begin(), v.end(), PComp());
    sort(stl_v.begin(), stl_v.end(), PComp());
    assert_vecs_equal(v, stl_v);
    for (size_t i = 0; i < 48; i++)
      ALWAYS_ASSERT(v[i].first + 1 == v[i].second);
  }

  cout << "vec test passed" << endl;
}

}

namespace small_map_ns {

typedef small_unordered_map<string, string, 4> map_type;
typedef map<string, string> stl_map_type;

static void
assert_map_contains(map_type &m, const string &k, const string &v)
{
  ALWAYS_ASSERT(!m.empty());
  ALWAYS_ASSERT(m[k] == v);
  {
    map_type::iterator it = m.find(k);
    ALWAYS_ASSERT(it != m.end());
    ALWAYS_ASSERT(it->first == k);
    ALWAYS_ASSERT(it->second == v);
  }
  const map_type &const_m = m;
  {
    map_type::const_iterator it = const_m.find(k);
    ALWAYS_ASSERT(it != const_m.end());
    ALWAYS_ASSERT(it->first == k);
    ALWAYS_ASSERT(it->second == v);
  }
}

static void
assert_map_equals(map_type &m, const stl_map_type &stl_m)
{
  ALWAYS_ASSERT(m.size() == stl_m.size());

  // reg version prefix
  {
    stl_map_type test;
    for (map_type::iterator it = m.begin();
         it != m.end(); ++it) {
      ALWAYS_ASSERT(test.find(it->first) == test.end());
      test[it->first] = it->second;
    }
    ALWAYS_ASSERT(test == stl_m);
  }

  // reg version postfix
  {
    stl_map_type test;
    for (map_type::iterator it = m.begin();
         it != m.end(); it++) {
      ALWAYS_ASSERT(test.find(it->first) == test.end());
      test[it->first] = it->second;
    }
    ALWAYS_ASSERT(test == stl_m);
  }

  // const version prefix
  {
    const map_type &const_m = m;
    stl_map_type test;
    for (map_type::const_iterator it = const_m.begin();
         it != const_m.end(); ++it) {
      ALWAYS_ASSERT(test.find(it->first) == test.end());
      test[it->first] = it->second;
    }
    ALWAYS_ASSERT(test == stl_m);
  }

  // const version postfix
  {
    const map_type &const_m = m;
    stl_map_type test;
    for (map_type::const_iterator it = const_m.begin();
         it != const_m.end(); it++) {
      ALWAYS_ASSERT(test.find(it->first) == test.end());
      test[it->first] = it->second;
    }
    ALWAYS_ASSERT(test == stl_m);
  }
}

template <typename T>
static void
init_map(T& m)
{
  m["a"] = "1";
  m["b"] = "2";
  m["c"] = "3";
  m["d"] = "4";
}

template <typename T>
static void
init_map1(T& m)
{
  m["a"] = "1";
  m["b"] = "2";
  m["c"] = "3";
  m["d"] = "4";
  m["e"] = "5";
  m["f"] = "6";
}

void
Test()
{
  {
    map_type m, m_copy;
    stl_map_type stl_m;
    init_map(m);
    init_map(stl_m);
    m_copy = m;
    map_type m_construct(m);
    for (stl_map_type::iterator it = stl_m.begin();
         it != stl_m.end(); ++it) {
      assert_map_contains(m, it->first, it->second);
      assert_map_contains(m_copy, it->first, it->second);
    }
    assert_map_equals(m, stl_m);
    assert_map_equals(m_copy, stl_m);
    assert_map_equals(m_construct, stl_m);
  }

  {
    map_type m, m_copy;
    stl_map_type stl_m;
    init_map1(m);
    init_map1(stl_m);
    m_copy = m;
    map_type m_construct(m);
    for (stl_map_type::iterator it = stl_m.begin();
         it != stl_m.end(); ++it) {
      assert_map_contains(m, it->first, it->second);
      assert_map_contains(m_copy, it->first, it->second);
    }
    assert_map_equals(m, stl_m);
    assert_map_equals(m_copy, stl_m);
    assert_map_equals(m_construct, stl_m);
  }

  {
    map_type m;
    ALWAYS_ASSERT(m.empty());
    ALWAYS_ASSERT(m.size() == 0);
    m["a"] = "1";
    ALWAYS_ASSERT(!m.empty());
    ALWAYS_ASSERT(m.size() == 1);
    m["b"] = "2";
    ALWAYS_ASSERT(!m.empty());
    ALWAYS_ASSERT(m.size() == 2);
    m["b"] = "2";
    ALWAYS_ASSERT(!m.empty());
    ALWAYS_ASSERT(m.size() == 2);
    m["c"] = "3";
    ALWAYS_ASSERT(!m.empty());
    ALWAYS_ASSERT(m.size() == 3);
    m["d"] = "4";
    ALWAYS_ASSERT(!m.empty());
    ALWAYS_ASSERT(m.size() == 4);

    m.clear();
    ALWAYS_ASSERT(m.empty());
    ALWAYS_ASSERT(m.size() == 0);
    m["a"] = "1";
    m["b"] = "2";
    m["c"] = "3";
    m["d"] = "4";
    m["d"] = "4";
    m["d"] = "4";
    m["e"] = "5";
    ALWAYS_ASSERT(!m.empty());
    ALWAYS_ASSERT(m.size() == 5);
  }

  cout << "map test passed" << endl;
}
}

class main_thread : public ndb_thread {
public:
  main_thread(int argc, char **argv)
    : ndb_thread(false, string("main")),
      argc(argc), argv(argv), ret(0)
  {}

  virtual void
  run()
  {
#ifndef CHECK_INVARIANTS
    cerr << "WARNING: tests are running without invariant checking" << endl;
#endif
    //XbufTest();
    //varint::Test();
    //small_vector_ns::Test();
    //small_map_ns::Test();
    //transaction::Test();
    //btree::TestFast();
    //btree::TestSlow();
    txn_btree::Test();
    ret = 0;
  }

  inline int
  retval() const
  {
    return ret;
  }
private:
  const int argc;
  char **const argv;
  volatile int ret;
};

int
main(int argc, char **argv)
{
  main_thread t(argc, argv);
  t.start();
  t.join();
  return t.retval();
}
