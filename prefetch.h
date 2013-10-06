#ifndef _PREFETCH_H_
#define _PREFETCH_H_

#include <algorithm>

#include "util.h"
#include "macros.h"

#if !MASSTREE_COMPILER_HH
// assumes cache-aligned
static inline ALWAYS_INLINE void
prefetch(const void *ptr)
{
  typedef struct { char x[CACHELINE_SIZE]; } cacheline_t;
  asm volatile("prefetcht0 %0" : : "m" (*(const cacheline_t *) ptr));
}
#define PREFETCH_DEFINED 1
#endif

// assumes cache-aligned
template <typename T>
static inline ALWAYS_INLINE void
prefetch_object(const T *ptr)
{
  for (unsigned i = CACHELINE_SIZE;
       i < std::min(static_cast<unsigned>(sizeof(*ptr)),
                    static_cast<unsigned>(4 * CACHELINE_SIZE));
       i += CACHELINE_SIZE)
    prefetch((const char *) ptr + i);
}

// prefetch an object resident in [ptr, ptr + n). doesn't assume cache aligned
static inline ALWAYS_INLINE void
prefetch_bytes(const void *p, size_t n)
{
  const char *ptr = (const char *) p;
  // round down to nearest cacheline, then prefetch
  const void * const pend =
    std::min(ptr + n,  ptr + 4 * CACHELINE_SIZE);
  ptr = (const char *) util::round_down<uintptr_t, LG_CACHELINE_SIZE>((uintptr_t) ptr);

  // manually unroll loop 3 times
  ptr += CACHELINE_SIZE;
  if (ptr < pend)
    prefetch(ptr);
  ptr += CACHELINE_SIZE;
  if (ptr < pend)
    prefetch(ptr);
  ptr += CACHELINE_SIZE;
  if (ptr < pend)
    prefetch(ptr);
}

#endif /* _PREFETCH_H_ */
