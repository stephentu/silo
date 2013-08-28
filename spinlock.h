#ifndef _SPINLOCK_H_
#define _SPINLOCK_H_

#include <stdint.h>

#include "amd64.h"
#include "macros.h"
#include "util.h"

class spinlock {
public:
  spinlock() : value(0) {}

  spinlock(const spinlock &) = delete;
  spinlock(spinlock &&) = delete;
  spinlock &operator=(const spinlock &) = delete;

  inline void
  lock()
  {
    // XXX: implement SPINLOCK_BACKOFF
    uint32_t v = value;
    while (v || !__sync_bool_compare_and_swap(&value, 0, 1)) {
      nop_pause();
      v = value;
    }
    COMPILER_MEMORY_FENCE;
  }

  inline bool
  try_lock()
  {
    return __sync_bool_compare_and_swap(&value, 0, 1);
  }

  inline void
  unlock()
  {
    INVARIANT(value);
    value = 0;
    COMPILER_MEMORY_FENCE;
  }

  // just for debugging
  inline bool
  is_locked() const
  {
    return value;
  }

private:
  volatile uint32_t value;
};

#endif /* _SPINLOCK_H_ */
