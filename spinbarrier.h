#ifndef _SPINBARRIER_H_
#define _SPINBARRIER_H_

#include "amd64.h"
#include "macros.h"
#include "util.h"

/**
 * Barrier implemented by spinning
 */

class spin_barrier {
public:
  spin_barrier(size_t n)
    : n(n)
  {
    ALWAYS_ASSERT(n > 0);
  }

  spin_barrier(const spin_barrier &) = delete;
  spin_barrier(spin_barrier &&) = delete;
  spin_barrier &operator=(const spin_barrier &) = delete;

  ~spin_barrier()
  {
    ALWAYS_ASSERT(n == 0);
  }

  void
  count_down()
  {
    // written like this (instead of using __sync_fetch_and_add())
    // so we can have assertions
    for (;;) {
      size_t copy = n;
      ALWAYS_ASSERT(copy > 0);
      if (__sync_bool_compare_and_swap(&n, copy, copy - 1))
        return;
    }
  }

  void
  wait_for()
  {
    while (n > 0)
      nop_pause();
  }

private:
  volatile size_t n;
};

#endif /* _SPINBARRIER_H_ */
