#ifndef _NDB_IMSTRING_H_
#define _NDB_IMSTRING_H_

#include <stdint.h>
#include <string.h>

#include <algorithm>
#include <string>

#include "rcu.h"

/**
 * Not-really-immutable string, for perf reasons. Also uses
 * RCU for GC
 */
class imstring {
public:
  imstring() : p(NULL), l(0) {}

  imstring(const uint8_t *src, size_t l)
    : p(new uint8_t[l]), l(l)
  {
    memcpy(p, src, l);
  }

  imstring(const std::string &s)
    : p(new uint8_t[s.size()]), l(s.size())
  {
    memcpy(p, s.data(), l);
  }

  imstring(const imstring &that)
  {
    replaceWith(that);
  }

  inline void
  swap(imstring &that)
  {
    std::swap(p, that.p);
    std::swap(l, that.l);
  }

  imstring &
  operator=(const imstring &that)
  {
    replaceWith(that);
    return *this;
  }

  ~imstring()
  {
    if (p)
      rcu::free_array(p);
  }

  inline const uint8_t *
  data() const
  {
    return p;
  }

  inline size_t
  size() const
  {
    return l;
  }

  inline void
  reset()
  {

  }

private:

  inline void
  replaceWith(const imstring &that)
  {
    if (p)
      rcu::free_array(p);
    p = new uint8_t[that.size()];
    l = that.size();
    memcpy(p, that.data(), l);
  }

  uint8_t *p;
  size_t l;
};

namespace std {
  template <>
  inline void
  swap(imstring &a, imstring &b)
  {
    a.swap(b);
  }
}

#endif /* _NDB_IMSTRING_H_ */
