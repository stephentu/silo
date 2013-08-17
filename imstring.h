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
#include "counter.h"

/**
 * Not-really-immutable string, for perf reasons. Also can use
 * RCU for GC
 */
template <bool RCU>
class base_imstring {

  template <bool R>
  friend class base_imstring;

  // we can't really support keys > 65536, but most DBs impose
  // limits on keys
  typedef uint16_t internal_size_type;

  static inline ALWAYS_INLINE internal_size_type
  CheckBounds(size_t l)
  {
    INVARIANT(l <= std::numeric_limits<internal_size_type>::max());
    return l;
  }

  static event_counter g_evt_imstring_bytes_allocated;
  static event_counter g_evt_imstring_bytes_freed;
  static event_avg_counter g_evt_avg_imstring_len;

public:
  base_imstring() : p(NULL), l(0) {}

  base_imstring(const uint8_t *src, size_t l)
    : p(new uint8_t[l]), l(CheckBounds(l))
  {
    g_evt_imstring_bytes_allocated += l;
    g_evt_avg_imstring_len.offer(l);
    NDB_MEMCPY(p, src, l);
  }

  base_imstring(const std::string &s)
    : p(new uint8_t[s.size()]), l(CheckBounds(s.size()))
  {
    g_evt_imstring_bytes_allocated += l;
    g_evt_avg_imstring_len.offer(l);
    NDB_MEMCPY(p, s.data(), l);
  }

  base_imstring(const base_imstring &) = delete;
  base_imstring(base_imstring &&) = delete;
  base_imstring &operator=(const base_imstring &) = delete;

  template <bool R>
  inline void
  swap(base_imstring<R> &that)
  {
    // std::swap() doesn't work for packed elems
    uint8_t * const temp_p = p;
    p = that.p;
    that.p = temp_p;
    internal_size_type const temp_l = l;
    l = that.l;
    that.l = temp_l;
  }

  inline
  ~base_imstring()
  {
    release();
    g_evt_imstring_bytes_freed += l;
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

private:

  inline void
  release()
  {
    if (likely(p)) {
      if (RCU)
        rcu::s_instance.free_array(p);
      else
        delete [] p;
    }
  }

  uint8_t *p;
  internal_size_type l;
} PACKED;

template <bool RCU>
event_counter base_imstring<RCU>::g_evt_imstring_bytes_allocated("imstring_bytes_allocated");

template <bool RCU>
event_counter base_imstring<RCU>::g_evt_imstring_bytes_freed("imstring_bytes_freed");

template <bool RCU>
event_avg_counter base_imstring<RCU>::g_evt_avg_imstring_len("avg_imstring_len");

typedef base_imstring<false> imstring;
typedef base_imstring<true>  rcu_imstring;

#endif /* _NDB_IMSTRING_H_ */
