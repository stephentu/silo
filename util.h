#ifndef _UTIL_H_
#define _UTIL_H_

#include <iostream>
#include <sstream>
#include <string>

#include <sys/time.h>

namespace util {


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

}

#endif /* _UTIL_H_ */
