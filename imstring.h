#ifndef _NDB_IMSTRING_H_
#define _NDB_IMSTRING_H_

#include <stdint.h>
#include <string.h>

#include <algorithm>
#include <string>
#include <limits>

#include "macros.h"
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

  static inline ALWAYS_INLINE uint32_t
  CheckBounds(size_t l)
  {
    INVARIANT(l <= std::numeric_limits<uint32_t>::max());
    return l;
  }

public:
  inline base_imstring() : p(NULL), l(0) {}

  base_imstring(const uint8_t *src, size_t l)
    : p(new uint8_t[l]), l(CheckBounds(l))
  {
    memcpy(p, src, l);
  }

  base_imstring(const std::string &s)
    : p(new uint8_t[s.size()]), l(CheckBounds(s.size()))
  {
    memcpy(p, s.data(), l);
  }

  template <bool R>
  inline void
  swap(base_imstring<R> &that)
  {
    // std::swap() doesn't work for packed elems
    uint8_t * const temp_p = p;
    p = that.p;
    that.p = temp_p;
    uint32_t const temp_l = l;
    l = that.l;
    that.l = temp_l;
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

protected:

  inline void
  release()
  {
    if (likely(p)) {
      if (RCU)
        rcu::free_array(p);
      else
        delete [] p;
    }
  }

  uint8_t *p;
  uint32_t l;
} PACKED;

typedef base_imstring<false> imstring;
typedef base_imstring<true>  rcu_imstring;

#endif /* _NDB_IMSTRING_H_ */
