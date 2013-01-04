#ifndef _UTIL_H_
#define _UTIL_H_

#include <iostream>
#include <sstream>
#include <string>

#include <pthread.h>
#include <sys/time.h>

#include "macros.h"

namespace util {

// xor-shift:
// http://dmurphy747.wordpress.com/2011/03/23/xorshift-vs-random-performance-in-java/
class fast_random {
public:
  fast_random(unsigned long seed)
    : seed(seed == 0 ? 0xABCD1234 : seed)
  {}

  inline unsigned long
  next()
  {
    seed ^= (seed << 21);
    seed ^= (seed >> 35);
    seed ^= (seed << 4);
    return seed;
  }

private:
  unsigned long seed;
};

template <typename ForwardIterator>
std::string
format_list(ForwardIterator begin, ForwardIterator end)
{
  std::stringstream ss;
  ss << "[";
  bool first = true;
  while (begin != end) {
    if (!first)
      ss << ", ";
    first = false;
    ss << *begin++;
  }
  ss << "]";
  return ss.str();
}

class timer {
private:
  timer(const timer &t);

public:
  timer()
  {
    lap();
  }

  uint64_t
  lap()
  {
    uint64_t t0 = start;
    uint64_t t1 = cur_usec();
    start = t1;
    return t1 - t0;
  }

private:
  static uint64_t
  cur_usec()
  {
    struct timeval tv;
    gettimeofday(&tv, 0);
    return ((uint64_t)tv.tv_sec) * 1000000 + tv.tv_usec;
  }

  uint64_t start;
};

class scoped_timer {
private:
  timer t;
  std::string region;

public:
  scoped_timer(const std::string &region) : region(region)
  {}

  ~scoped_timer()
  {
    double x = t.lap() / 1000.0; // ms
    std::cerr << "timed region `" << region << "' took " << x << " ms" << std::endl;
  }
};

class scoped_spinlock {
public:
  inline scoped_spinlock(pthread_spinlock_t *l)
    : l(l)
  {
    ALWAYS_ASSERT(pthread_spin_lock(l) == 0);
  }
  inline ~scoped_spinlock()
  {
    ALWAYS_ASSERT(pthread_spin_unlock(l) == 0);
  }
private:
  pthread_spinlock_t *const l;
};

}

#endif /* _UTIL_H_ */
