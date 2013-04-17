#ifndef _NDB_BENCH_INLINE_STR_H_
#define _NDB_BENCH_INLINE_STR_H_

#include <stdint.h>
#include <string.h>

#include <string>

#include "../macros.h"
#include "../record/serializer.h"

// equivalent to VARCHAR(N)

template <typename IntSizeType, unsigned int N>
class inline_str_base {
  // XXX: argh...
  template <typename T, bool DoCompress> friend class serializer;
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

  inline_str_base(const inline_str_base &that)
    : sz(that.sz)
  {
    NDB_MEMCPY(&buf[0], &that.buf[0], sz);
  }

  inline_str_base &
  operator=(const inline_str_base &that)
  {
    if (this == &that)
      return *this;
    sz = that.sz;
    NDB_MEMCPY(&buf[0], &that.buf[0], sz);
    return *this;
  }

  inline size_t
  max_size() const
  {
    return N;
  }

  inline const char *
  c_str() const
  {
    buf[sz] = 0;
    return &buf[0];
  }

  inline std::string
  str(bool zeropad = false) const
  {
		if (zeropad) {
			INVARIANT(N >= sz);
			std::string r(N, 0);
			NDB_MEMCPY((char *) r.data(), &buf[0], sz);
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
    NDB_MEMCPY(&buf[0], s, n);
    sz = n;
  }

  inline ALWAYS_INLINE void
  assign(const std::string &s)
  {
    assign(s.data(), s.size());
  }

  inline void
  resize(size_t n, char c = 0)
  {
    INVARIANT(n <= N);
    if (n > sz)
      NDB_MEMSET(&buf[sz], c, n - sz);
    sz = n;
  }

  inline void
  resize_junk(size_t n)
  {
    INVARIANT(n <= N);
    sz = n;
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
  mutable char buf[N + 1];
} PACKED;

template <unsigned int N>
class inline_str_8 : public inline_str_base<uint8_t, N> {} PACKED;

template <unsigned int N>
class inline_str_16 : public inline_str_base<uint16_t, N> {} PACKED;

// equiavlent to CHAR(N)
template <unsigned int N, char FillChar = ' '>
class inline_str_fixed {
  // XXX: argh...
  template <typename T, bool DoCompress> friend class serializer;
public:
  inline_str_fixed()
  {
    NDB_MEMSET(&buf[0], FillChar, N);
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

  inline_str_fixed(const inline_str_fixed &that)
  {
    NDB_MEMCPY(&buf[0], &that.buf[0], N);
  }

  inline_str_fixed &
  operator=(const inline_str_fixed &that)
  {
    if (this == &that)
      return *this;
    NDB_MEMCPY(&buf[0], &that.buf[0], N);
    return *this;
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
    NDB_MEMCPY(&buf[0], s, n);
    if ((N - n) > 0) // to suppress compiler warning
      NDB_MEMSET(&buf[n], FillChar, N - n); // pad with spaces
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
template <typename IntSizeType, unsigned int N, bool Compress>
struct serializer< inline_str_base<IntSizeType, N>, Compress > {
  typedef inline_str_base<IntSizeType, N> obj_type;
  inline uint8_t *
  write(uint8_t *buf, const obj_type &obj) const
  {
    serializer<IntSizeType, Compress> s;
    buf = write(buf, &obj.sz);
    NDB_MEMCPY(buf, &obj.buf[0], obj.sz);
    return buf + obj.sz;
  }

  const uint8_t *
  read(const uint8_t *buf, obj_type *obj) const
  {
    serializer<IntSizeType, Compress> s;
    buf = read(buf, &obj->sz);
    NDB_MEMCPY(&obj->buf[0], buf, obj->sz);
    return buf + obj->sz;
  }

  size_t
  nbytes(const obj_type *obj) const
  {
    serializer<IntSizeType, Compress> s;
    return s.nbytes(&obj->sz) + obj->sz;
  }

  static inline constexpr size_t
  max_nbytes()
  {
    return serializer<IntSizeType, Compress>::max_bytes() + N;
  }
};

#endif /* _NDB_BENCH_INLINE_STR_H_ */
