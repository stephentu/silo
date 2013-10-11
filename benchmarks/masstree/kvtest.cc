#include <iostream>
#include <vector>

#include <unistd.h>

#include "../../btree_choice.h"
#include "../../thread.h"
#include "../../spinbarrier.h"
#include "../../varkey.h"
#include "kvrandom.hh"

using namespace std;
using namespace util;

struct quick_istr {
private:
  char buf_[32];
  char *bbuf_;
public:
  inline quick_istr()
  {
    set(0);
  }

  inline quick_istr(unsigned long x, int minlen = 0)
  {
    set(x, minlen);
  }

  inline void
  set(unsigned long x, int minlen = 0)
  {
    bbuf_ = buf_ + sizeof(buf_) - 1;
    do {
      *--bbuf_ = (x % 10) + '0';
      x /= 10;
    } while (--minlen > 0 || x != 0);
  }

  inline const char *
  c_str()
  {
    buf_[sizeof(buf_) - 1] = 0;
    return bbuf_;
  }

  inline const char *
  data() const
  {
    return bbuf_;
  }

  inline size_t
  size() const
  {
    return (buf_ + sizeof(buf_) - 1) - bbuf_;
  }

  inline varkey
  key() const
  {
    return varkey((const uint8_t *) bbuf_, size());
  }
};

template <typename T>
class kvtest_worker : public ndb_thread {
public:
  kvtest_worker(btree &btr,
                spin_barrier &b,
                unsigned int id,
                const volatile bool *phases)
    : ndb_thread(false, string("kvtest-worker")),
      btr(&btr), b(&b), id(id), phases(phases) {}

  virtual void
  run()
  {
    b->count_down();
    b->wait_for();
    T()(*btr, id, phases);
  }

private:
  btree *btr;
  spin_barrier *b;
  unsigned int id;
  const volatile bool *phases;
};

template <typename T>
class kvtest_runner {
public:
  kvtest_runner(unsigned int nthreads)
    : nthreads(nthreads) {}

  void
  go()
  {
    btree btr;
    spin_barrier barrier(nthreads);
    volatile bool phases[T::NPhases] = {0};
    vector<kvtest_worker<T> *> workers;
    for (unsigned int i = 0; i < nthreads; i++)
      workers.push_back(new kvtest_worker<T>(btr, barrier, i, phases));
    for (unsigned int i = 0; i < nthreads; i++)
      workers[i]->start();
    for (unsigned int i = 0; i < T::NPhases; i++) {
      sleep(T::PhaseRuntimes[i]);
      phases[i] = 1;
      __sync_synchronize();
    }
    for (unsigned int i = 0; i < nthreads; i++) {
      workers[i]->join();
      delete workers[i];
    }
  }

public:
  unsigned int nthreads;
};

struct kvtest_rw1 {
  static const size_t NPhases = 2;
  static const unsigned long PhaseRuntimes[NPhases];

  typedef kvrandom_lcg_nr rand_type;

  void
  operator()(btree &btr, unsigned int id, const volatile bool *phases) const
  {
    const int seed = 31949 + id % 48;
    rand_type r(seed);
    timer t;
    unsigned n;
    for (n = 0; !phases[0]; ++n) {
      const int32_t x = (int32_t) r.next();
      const quick_istr key(x), value(x + 1);
      btree::value_type v = (btree::value_type) malloc(value.size());
      NDB_MEMCPY(v, value.data(), value.size());
      btr.insert(key.key(), v, 0, 0);
    }
    const double put_ms = t.lap_ms();
    int32_t *a = (int32_t *) malloc(sizeof(int32_t) * n);
    r.reset(seed);
    for (unsigned i = 0; i < n; i++)
      a[i] = (int32_t) r.next();
    for (unsigned i = 0; i < n; ++i)
      swap(a[i], a[r.next() % n]);

    t.lap();
    unsigned g;
    for (g = 0; g < n && !phases[1]; ++g) {
      const quick_istr key(a[g]), value(a[g] + 1);
      btree::value_type v = 0;
      ALWAYS_ASSERT(btr.search(key.key(), v));
      ALWAYS_ASSERT(memcmp(value.data(), v, value.size()) == 0);
    }
    const double get_ms = t.lap_ms();

    cout << "puts: " << n << ", rate: " << (double(n) / (put_ms / 1000.0)) << " ops/sec/core" << endl;
    cout << "gets: " << g << ", rate: " << (double(g) / (get_ms / 1000.0)) << " ops/sec/core" << endl;

    // XXX(stephentu): free btree values
  }
};

const unsigned long kvtest_rw1::PhaseRuntimes[NPhases] = { 10, 10 }; // seconds

int
main(int argc, char **argv)
{
  ALWAYS_ASSERT(argc == 2);
  int nthreads = atoi(argv[1]);
  ALWAYS_ASSERT(nthreads > 0);
  kvtest_runner<kvtest_rw1> r(nthreads);
  r.go();
  return 0;
}
