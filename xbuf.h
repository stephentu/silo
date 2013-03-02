#ifndef _XBUF_H_
#define _XBUF_H_

#include <algorithm> // for std::swap
#include <iostream>
#include <string>
#include <tr1/functional> // for std::tr1::hash

#include <stdint.h>
#include <string.h>

#include "macros.h"
#include "hash_bytes.h"

// a non-thread safe, limited drop-in replacement
// for std::string. doesn't do ref counting/copy on write.
class xbuf {
public:

  xbuf() : b(0), l(0), h(-1) {}
  xbuf(const char *p, size_t l)
    : b(AllocRaw(l)), l(l), h(-1)
  {
    NDB_MEMCPY(b, p, l);
  }
  xbuf(const char *p)
    : b(0), l(strlen(p)), h(-1)
  {
    b = AllocRaw(l);
    NDB_MEMCPY(b, p, l);
  }
  xbuf(const std::string &s)
    : b(AllocRaw(s.size())), l(s.size()), h(-1)
  {
    NDB_MEMCPY(b, s.data(), l);
  }
  xbuf(const xbuf &that)
    : b(0), l(0), h(-1)
  {
    assignFrom(that);
  }

  ~xbuf()
  {
    if (b)
      free(b);
  }

  inline xbuf &
  operator=(const xbuf &that)
  {
    assignFrom(that);
    return *this;
  }

  inline bool
  operator==(const xbuf &that) const
  {
    if (size() != that.size())
      return false;
    return memcmp(data(), that.data(), size()) == 0;
  }

  inline bool
  operator!=(const xbuf &that) const
  {
    return !operator==(that);
  }

  inline bool
  operator<(const xbuf &that) const
  {
    int r = memcmp(data(), that.data(), std::min(size(), that.size()));
    return r < 0 || (r == 0 && size() < that.size());
  }

  inline bool
  operator>=(const xbuf &that) const
  {
    return !operator<(that);
  }

  inline bool
  operator<=(const xbuf &that) const
  {
    int r = memcmp(data(), that.data(), std::min(size(), that.size()));
    return r < 0 || (r == 0 && size() <= that.size());
  }

  inline bool
  operator>(const xbuf &that) const
  {
    return !operator<=(that);
  }

  inline void
  swap(xbuf &other)
  {
    std::swap(b, other.b);
    std::swap(l, other.l);
    std::swap(h, other.h);
  }

  inline size_t
  size() const
  {
    return l;
  }

  inline bool
  empty() const
  {
    return size() == 0;
  }

  inline const char *
  data() const
  {
    return (const char *) b;
  }

  size_t
  hash() const
  {
    if (likely(h != -1))
      return h;
    // seed taken from bits/functional_hash.h
    const size_t seed = static_cast<size_t>(0xc70f6907UL);
    return (h = ndb::hash_bytes(b, l, seed));
  }

  const char &
  operator[](size_t pos) const
  {
    return reinterpret_cast<const char &>(b[pos]);
  }

  inline void
  assign(const char *p, size_t len)
  {
    assignFrom(p, len);
  }

private:
  static inline uint8_t *
  AllocRaw(size_t l)
  {
    uint8_t *p = (uint8_t *) malloc(l);
    ALWAYS_ASSERT(p);
    return p;
  }

  inline void
  assignFrom(const char *p, size_t len)
  {
    INVARIANT((const char *) b != p); // would be bad
    if (b)
      free(b);
    b = AllocRaw(len);
    NDB_MEMCPY(b, p, len);
    l = len;
    h = -1;
  }

  inline void
  assignFrom(const xbuf &that)
  {
    // stupid self assignment
    if (unlikely(this == &that))
      return;
    INVARIANT(b != that.b); // would be bad
    if (b)
      free(b);
    b = AllocRaw(that.l);
    NDB_MEMCPY(b, that.b, that.l);
    l = that.l;
    h = that.h;
  }

  uint8_t *b;
  size_t l;
  mutable ssize_t h;
};

namespace {
std::ostream &
operator<<(std::ostream &o, const xbuf &s)
{
  // XXX(stephentu): lazy
  o << std::string(s.data(), s.size());
  return o;
}
}

namespace std {
namespace tr1 {
template <>
struct hash<xbuf> {
  inline size_t
  operator()(const xbuf &s) const
  {
    return s.hash();
  }
};
}
}

#endif /* _XBUF_H_ */
