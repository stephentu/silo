#ifndef _NDB_BENCH_INLINE_STR_H_
#define _NDB_BENCH_INLINE_STR_H_

#include <assert.h>
#include <stdint.h>
#include <string.h>

#include <string>

#include "../macros.h"

// equivalent to VARCHAR(N)

template <typename IntSizeType, unsigned int N>
class inline_str_base {
public:

  inline_str_base() : sz(0) {}

  inline_str_base(const char *s)
  {
    construct(s, strlen(s));
  }

  inline_str_base(const char *s, size_t n)
  {
    construct(s, n);
  }

  inline std::string
  str() const
  {
    return std::string(&buf[0], sz);
  }

  inline const char *
  data() const
  {
    return &buf[0];
  }

private:

  inline void
  construct(const char *s, size_t n)
  {
    assert(n <= N);
    memcpy(&buf[0], s, n);
    sz = n;
  }

  IntSizeType sz;
  char buf[N];
} PACKED;

template <unsigned int N>
class inline_str_8 : public inline_str_base<uint8_t, N> {} PACKED;

template <unsigned int N>
class inline_str_16 : public inline_str_base<uint16_t, N> {} PACKED;

#endif /* _NDB_BENCH_INLINE_STR_H_ */
