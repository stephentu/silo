#ifndef _UTIL_H_
#define _UTIL_H_

#include <iostream>
#include <sstream>
#include <string>

#include <pthread.h>
#include <sys/time.h>

#include "macros.h"

namespace util {

template <typename T>
struct host_endian_trfm {
  inline T operator()(T t) const { return t; }
};

template <>
struct host_endian_trfm<uint16_t> {
  inline uint16_t operator()(uint16_t t) const { return be16toh(t); }
};

template <>
struct host_endian_trfm<int16_t> {
  inline int16_t operator()(int16_t t) const { return be16toh(t); }
};

template <>
struct host_endian_trfm<int32_t> {
  inline int32_t operator()(int32_t t) const { return be32toh(t); }
};

template <>
struct host_endian_trfm<uint32_t> {
  inline uint32_t operator()(uint32_t t) const { return be32toh(t); }
};

template <>
struct host_endian_trfm<int64_t> {
  inline int64_t operator()(int64_t t) const { return be64toh(t); }
};

template <>
struct host_endian_trfm<uint64_t> {
  inline uint64_t operator()(uint64_t t) const { return be64toh(t); }
};

template <typename T>
struct big_endian_trfm {
  inline T operator()(T t) const { return t; }
};

template <>
struct big_endian_trfm<uint16_t> {
  inline uint16_t operator()(uint16_t t) const { return htobe16(t); }
};

template <>
struct big_endian_trfm<int16_t> {
  inline int16_t operator()(int16_t t) const { return htobe16(t); }
};

template <>
struct big_endian_trfm<int32_t> {
  inline int32_t operator()(int32_t t) const { return htobe32(t); }
};

template <>
struct big_endian_trfm<uint32_t> {
  inline uint32_t operator()(uint32_t t) const { return htobe32(t); }
};

template <>
struct big_endian_trfm<int64_t> {
  inline int64_t operator()(int64_t t) const { return htobe64(t); }
};

template <>
struct big_endian_trfm<uint64_t> {
  inline uint64_t operator()(uint64_t t) const { return htobe64(t); }
};

template <typename T>
inline std::string
hexify(const T &t)
{
  std::ostringstream buf;
  buf << std::hex << t << std::endl;
  return buf.str();
}

template <>
inline std::string
hexify(const std::string &input)
{
  size_t len = input.length();
  const char *const lut = "0123456789ABCDEF";

  std::string output;
  output.reserve(2 * len);
  for (size_t i = 0; i < len; ++i) {
    const unsigned char c = (unsigned char) input[i];
    output.push_back(lut[c >> 4]);
    output.push_back(lut[c & 15]);
  }
  return output;
}

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

//  The following is taken from:
//  Boost noncopyable.hpp header file  --------------------------------------//

//  (C) Copyright Beman Dawes 1999-2003. Distributed under the Boost
//  Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/utility for documentation.


//  Private copy constructor and copy assignment ensure classes derived from
//  class noncopyable cannot be copied.

//  Contributed by Dave Abrahams

namespace noncopyable_  // protection from unintended ADL
{
  class noncopyable
  {
   protected:
      noncopyable() {}
      ~noncopyable() {}
   private:  // emphasize the following members are private
      noncopyable( const noncopyable& );
      const noncopyable& operator=( const noncopyable& );
  };
}

typedef noncopyable_::noncopyable noncopyable;

/**
 * unsafe_share_with has the semantics of dst = src, except
 * done cheaply (but safely) if possible.
 *
 * For instance, suppose T manages some underlying memory pointer, and T's dtor
 * frees this memory pointer.  If we guarantee that dst will outlive src, then
 * instead of allocating new memory for dst and copying the contents of memory
 * into dst, we can simply have dst and src share the memory pointer, and have
 * src not delete the underlying pointer.
 */
template <typename T>
inline void
unsafe_share_with(T &dst, T &src)
{
  dst.unsafe_share_with(src);
}

}

#endif /* _UTIL_H_ */
