#ifndef _UTIL_H_
#define _UTIL_H_

#include <iostream>
#include <sstream>
#include <string>
#include <limits>
#include <queue>
#include <utility>

#include <stdint.h>
#include <pthread.h>
#include <sys/time.h>

#include "macros.h"
#include "xbuf.h"
#include "small_vector.h"

namespace util {

// padded, aligned primitives
template <typename T>
struct aligned_padded_elem {
  aligned_padded_elem() : elem() {}
  T elem;
  // syntactic sugar- can treat like a pointer
  inline T & operator*() { return elem; }
  inline const T & operator*() const { return elem; }
  inline T * operator->() { return &elem; }
  inline const T * operator->() const { return &elem; }
  CACHE_PADOUT;
} CACHE_ALIGNED;

// some pre-defs
typedef aligned_padded_elem<uint8_t>  aligned_padded_u8;
typedef aligned_padded_elem<uint16_t> aligned_padded_u16;
typedef aligned_padded_elem<uint32_t> aligned_padded_u32;
typedef aligned_padded_elem<uint64_t> aligned_padded_u64;

template <typename T>
struct host_endian_trfm {
  inline ALWAYS_INLINE T operator()(const T &t) const { return t; }
};

template <>
struct host_endian_trfm<uint16_t> {
  inline ALWAYS_INLINE uint16_t operator()(uint16_t t) const { return be16toh(t); }
};

template <>
struct host_endian_trfm<int16_t> {
  inline ALWAYS_INLINE int16_t operator()(int16_t t) const { return be16toh(t); }
};

template <>
struct host_endian_trfm<int32_t> {
  inline ALWAYS_INLINE int32_t operator()(int32_t t) const { return be32toh(t); }
};

template <>
struct host_endian_trfm<uint32_t> {
  inline ALWAYS_INLINE uint32_t operator()(uint32_t t) const { return be32toh(t); }
};

template <>
struct host_endian_trfm<int64_t> {
  inline ALWAYS_INLINE int64_t operator()(int64_t t) const { return be64toh(t); }
};

template <>
struct host_endian_trfm<uint64_t> {
  inline ALWAYS_INLINE uint64_t operator()(uint64_t t) const { return be64toh(t); }
};

template <typename T>
struct big_endian_trfm {
  inline ALWAYS_INLINE T operator()(const T &t) const { return t; }
};

template <>
struct big_endian_trfm<uint16_t> {
  inline ALWAYS_INLINE uint16_t operator()(uint16_t t) const { return htobe16(t); }
};

template <>
struct big_endian_trfm<int16_t> {
  inline ALWAYS_INLINE int16_t operator()(int16_t t) const { return htobe16(t); }
};

template <>
struct big_endian_trfm<int32_t> {
  inline ALWAYS_INLINE int32_t operator()(int32_t t) const { return htobe32(t); }
};

template <>
struct big_endian_trfm<uint32_t> {
  inline ALWAYS_INLINE uint32_t operator()(uint32_t t) const { return htobe32(t); }
};

template <>
struct big_endian_trfm<int64_t> {
  inline ALWAYS_INLINE int64_t operator()(int64_t t) const { return htobe64(t); }
};

template <>
struct big_endian_trfm<uint64_t> {
  inline ALWAYS_INLINE uint64_t operator()(uint64_t t) const { return htobe64(t); }
};

template <typename T>
inline std::string
hexify(const T &t)
{
  std::ostringstream buf;
  buf << std::hex << t;
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

template <typename T, unsigned int lgbase>
struct mask_ {
  static const T value = ((T(1) << lgbase) - 1);
};

// rounding
template <typename T, unsigned int lgbase>
static constexpr inline ALWAYS_INLINE T
round_up(T t)
{
  return (t + mask_<T, lgbase>::value) & ~mask_<T, lgbase>::value;
}

template <typename T, unsigned int lgbase>
static constexpr inline ALWAYS_INLINE T
round_down(T t)
{
  return (t & ~mask_<T, lgbase>::value);
}

template <typename T, typename U>
static inline ALWAYS_INLINE T
iceil(T x, U y)
{
  U mod = x % y;
  return x + (mod ? y - mod : 0);
}

//// xor-shift:
//// http://dmurphy747.wordpress.com/2011/03/23/xorshift-vs-random-performance-in-java/
//class fast_random {
//public:
//  fast_random(unsigned long seed)
//    : seed(seed == 0 ? 0xABCD1234 : seed)
//  {}
//
//  inline unsigned long
//  next()
//  {
//    seed ^= (seed << 21);
//    seed ^= (seed >> 35);
//    seed ^= (seed << 4);
//    return seed;
//  }
//
//  /** [0.0, 1.0) */
//  inline double
//  next_uniform()
//  {
//    return double(next())/double(std::numeric_limits<unsigned long>::max());
//  }
//
//  inline char
//  next_char()
//  {
//    return next() % 256;
//  }
//
//  inline std::string
//  next_string(size_t len)
//  {
//    std::string s(len, 0);
//    for (size_t i = 0; i < len; i++)
//      s[i] = next_char();
//    return s;
//  }
//
//private:
//  unsigned long seed;
//};

// not thread-safe
//
// taken from java:
//   http://developer.classpath.org/doc/java/util/Random-source.html
class fast_random {
public:
  fast_random(unsigned long seed)
    : seed(0)
  {
    set_seed(seed);
  }

  inline unsigned long
  next()
  {
    return ((unsigned long) next(32) << 32) + next(32);
  }

  inline uint32_t
  next_u32()
  {
    return next(32);
  }

  /** [0.0, 1.0) */
  inline double
  next_uniform()
  {
    return (((unsigned long) next(26) << 27) + next(27)) / (double) (1L << 53);
  }

  inline char
  next_char()
  {
    return next(8) % 256;
  }

  inline std::string
  next_string(size_t len)
  {
    std::string s(len, 0);
    for (size_t i = 0; i < len; i++)
      s[i] = next_char();
    return s;
  }

private:
  inline void
  set_seed(unsigned long seed)
  {
    this->seed = (seed ^ 0x5DEECE66DL) & ((1L << 48) - 1);
  }

  inline unsigned long
  next(unsigned int bits)
  {
    seed = (seed * 0x5DEECE66DL + 0xBL) & ((1L << 48) - 1);
    return (unsigned long) (seed >> (48 - bits));
  }

  unsigned long seed;
};

template <typename ForwardIterator>
std::string
format_list(ForwardIterator begin, ForwardIterator end)
{
  std::ostringstream ss;
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

/**
 * Returns the lowest position p such that p0+p != p1+p.
 */
inline size_t
first_pos_diff(const char *p0, size_t sz0,
               const char *p1, size_t sz1)
{
  const char *p0end = p0 + sz0;
  const char *p1end = p1 + sz1;
  size_t n = 0;
  while (p0 != p0end &&
         p1 != p1end &&
         p0[n] == p1[n])
    n++;
  return n;
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

  inline double
  lap_ms()
  {
    return lap() / 1000.0;
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
  bool enabled;

public:
  scoped_timer(const std::string &region, bool enabled = true)
    : region(region), enabled(enabled)
  {}

  ~scoped_timer()
  {
    if (enabled) {
      const double x = t.lap() / 1000.0; // ms
      std::cerr << "timed region " << region << " took " << x << " ms" << std::endl;
    }
  }
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

inline std::string
next_key(const std::string &s)
{
  std::string s0(s);
  s0.resize(s.size() + 1);
  return s0;
}

inline xbuf
next_key(const xbuf &s)
{
  xbuf s0(s);
  s0.resize(s.size() + 1);
  return s0;
}

template <typename T, typename Container = std::vector<T> >
struct std_reverse_pq {
  typedef std::priority_queue<T, Container, std::greater<T> > type;
};

template <typename PairType, typename FirstComp>
struct std_pair_first_cmp {
  inline bool
  operator()(const PairType &lhs, const PairType &rhs) const
  {
    FirstComp c;
    return c(lhs.first, rhs.first);
  }
};

// deal with small container opt vectors correctly
template <typename T, size_t SmallSize = SMALL_SIZE_VEC>
struct vec {
#ifdef USE_SMALL_CONTAINER_OPT
  typedef small_vector<T, SmallSize> type;
#else
  typedef std::vector<T> type;
#endif
};

static inline std::vector<std::string>
split(const std::string &s, char delim)
{
	std::vector<std::string> elems;
  std::stringstream ss(s);
  std::string item;
  while (std::getline(ss, item, delim))
    elems.emplace_back(item);
  return elems;
}

} // namespace util

// pretty printer for std::pair<A, B>
template <typename A, typename B>
inline std::ostream &
operator<<(std::ostream &o, const std::pair<A, B> &p)
{
  o << "[" << p.first << ", " << p.second << "]";
  return o;
}

#endif /* _UTIL_H_ */
