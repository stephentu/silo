#ifndef _AMD64_H_
#define _AMD64_H_

#include "macros.h"
#include <stdint.h>

inline ALWAYS_INLINE void
nop_pause()
{
  __asm volatile("pause" : :);
}

inline ALWAYS_INLINE uint64_t
rdtsc(void)
{
  uint32_t hi, lo;
  __asm volatile("rdtsc" : "=a"(lo), "=d"(hi));
  return ((uint64_t)lo)|(((uint64_t)hi)<<32);
}

#endif /* _AMD64_H_ */
