#ifndef _NDB_BENCH_INLINE_STR_H_
#define _NDB_BENCH_INLINE_STR_H_

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
    assign(s);
  }

  inline_str_base(const char *s, size_t n)
  {
    assign(s, n);
  }

  inline_str_base(const std::string &s)
  {
    assign(s);
  }

  inline std::string
  str(bool zeropad = false) const
  {
		if (zeropad) {
			INVARIANT(N >= sz);
			std::string r(N, 0);
			memcpy((char *) r.data(), &buf[0], sz);
			return r;
		} else {
			return std::string(&buf[0], sz);
		}
  }

  inline ALWAYS_INLINE const char *
  data() const
  {
    return &buf[0];
  }

  inline ALWAYS_INLINE size_t
  size() const
  {
    return sz;
  }

  inline ALWAYS_INLINE void
  assign(const char *s)
  {
    assign(s, strlen(s));
  }

  inline void
  assign(const char *s, size_t n)
  {
    INVARIANT(n <= N);
    memcpy(&buf[0], s, n);
    sz = n;
  }

  inline ALWAYS_INLINE void
  assign(const std::string &s)
  {
    assign(s.data(), s.size());
  }

private:
  IntSizeType sz;
  char buf[N];
} PACKED;

template <unsigned int N>
class inline_str_8 : public inline_str_base<uint8_t, N> {} PACKED;

template <unsigned int N>
class inline_str_16 : public inline_str_base<uint16_t, N> {} PACKED;

// equiavlent to CHAR(N)
template <unsigned int N>
class inline_str_fixed {
public:
  inline_str_fixed()
  {
    memset(&buf[0], 0, N);
  }

  inline_str_fixed(const char *s)
  {
    assign(s, strlen(s));
  }

  inline_str_fixed(const char *s, size_t n)
  {
    assign(s, n);
  }

  inline_str_fixed(const std::string &s)
  {
    assign(s.data(), s.size());
  }

  inline ALWAYS_INLINE std::string
  str() const
  {
    return std::string(&buf[0], N);
  }

  inline ALWAYS_INLINE const char *
  data() const
  {
    return &buf[0];
  }

  inline ALWAYS_INLINE size_t
  size() const
  {
    return N;
  }

  inline ALWAYS_INLINE void
  assign(const char *s)
  {
    assign(s, strlen(s));
  }

  inline void
  assign(const char *s, size_t n)
  {
    INVARIANT(n <= N);
    memcpy(&buf[0], s, n);
    if ((N - n) > 0) // to suppress compiler warning
      memset(&buf[n], ' ', N - n); // pad with spaces
  }

  inline ALWAYS_INLINE void
  assign(const std::string &s)
  {
    assign(s.data(), s.size());
  }

private:
  char buf[N];
} PACKED;

#endif /* _NDB_BENCH_INLINE_STR_H_ */
