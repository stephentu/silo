#include <iostream>
#include <tr1/functional>
#include <tr1/unordered_map>

#include "thread.h"
#include "txn.h"
#include "btree.h"
#include "txn_btree.h"
#include "varint.h"
#include "xbuf.h"

using namespace std;
using namespace tr1;

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
    XbufTest();
    varint::Test();
    //transaction::Test();
    btree::TestFast();
    btree::TestSlow();
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
