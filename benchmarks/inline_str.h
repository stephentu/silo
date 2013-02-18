#ifndef _NDB_BENCH_INLINE_STR_H_
#define _NDB_BENCH_INLINE_STR_H_

#include <stdint.h>
#include <string.h>

#include <string>

#include "../macros.h"
#include "serializer.h"

// equivalent to VARCHAR(N)

template <typename IntSizeType, unsigned int N>
class inline_str_base {
  friend class serializer< inline_str_base<IntSizeType, N> >;
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

  inline bool
  operator==(const inline_str_base &other) const
  {
    return memcmp(buf, other.buf, sz) == 0;
  }

  inline bool
  operator!=(const inline_str_base &other) const
  {
    return !operator==(other);
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
  friend class serializer< inline_str_fixed<N> >;
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

  inline bool
  operator==(const inline_str_fixed &other) const
  {
    return memcmp(buf, other.buf, N) == 0;
  }

  inline bool
  operator!=(const inline_str_fixed &other) const
  {
    return !operator==(other);
  }

private:
  char buf[N];
} PACKED;

// serializer<T> specialization
template <typename IntSizeType, unsigned int N>
struct serializer< inline_str_base<IntSizeType, N> > {
  typedef inline_str_base<IntSizeType, N> obj_type;
  inline uint8_t *
  write(uint8_t *buf, const obj_type *obj) const
  {
    serializer<IntSizeType> s;
    buf = write(buf, &obj->sz);
    memcpy(buf, &obj->buf[0], obj->sz);
    return buf + obj->sz;
  }

  const uint8_t *
  read(const uint8_t *buf, obj_type *obj) const
  {
    serializer<IntSizeType> s;
    buf = read(buf, &obj->sz);
    memcpy(&obj->buf[0], buf, obj->sz);
    return buf + obj->sz;
  }

  size_t
  nbytes(const obj_type *obj) const
  {
    serializer<IntSizeType> s;
    return s.nbytes(&obj->sz) + obj->sz;
  }
};

#endif /* _NDB_BENCH_INLINE_STR_H_ */
