#ifndef _NDB_IMSTRING_H_
#define _NDB_IMSTRING_H_

#include <stdint.h>
#include <string.h>

#include <algorithm>
#include <string>

#include "rcu.h"
#include "util.h"

/**
 * Not-really-immutable string, for perf reasons. Also can use
 * RCU for GC
 */
template <bool RCU>
class base_imstring : private util::noncopyable {

  template <bool R>
  friend class base_imstring;

public:
  inline base_imstring() : p(NULL), l(0), xfer(false) {}

  base_imstring(const uint8_t *src, size_t l)
    : p(new uint8_t[l]), l(l), xfer(false)
  {
    memcpy(p, src, l);
  }

  base_imstring(const std::string &s)
    : p(new uint8_t[s.size()]), l(s.size()), xfer(false)
  {
    memcpy(p, s.data(), l);
  }

  template <bool R>
  inline void
  swap(base_imstring<R> &that)
  {
    std::swap(p, that.p);
    std::swap(l, that.l);
    std::swap(xfer, that.xfer);
  }

  inline
  ~base_imstring()
  {
    release();
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
  unsafe_share_with(base_imstring &that)
  {
    release();
    p = that.p;
    l = that.l;
    xfer = that.xfer;
    that.xfer = true;
  }

protected:

  inline void
  release()
  {
    if (p && xfer) {
      if (RCU)
        rcu::free_array(p);
      else
        delete [] p;
    }
  }

  uint8_t *p;
  size_t l;
  bool xfer; // if xfer is true, no longer has ownership of p
};

class imstring : public base_imstring<false> {
public:
  imstring() : base_imstring<false>() {}

  imstring(const uint8_t *src, size_t l)
    : base_imstring<false>(src, l)
  {}

  imstring(const std::string &s)
    : base_imstring<false>(s)
  {}
};

class rcu_imstring : public base_imstring<true> {
public:
  rcu_imstring() : base_imstring<true>() {}

  rcu_imstring(const uint8_t *src, size_t l)
    : base_imstring<true>(src, l)
  {}

  rcu_imstring(const std::string &s)
    : base_imstring<true>(s)
  {}
};

#endif /* _NDB_IMSTRING_H_ */
