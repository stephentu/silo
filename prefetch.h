#ifndef _PREFETCH_H_
#define _PREFETCH_H_

#include <algorithm>

static inline ALWAYS_INLINE void
prefetch(const void *ptr)
{
#ifdef NODE_PREFETCH
  typedef struct { char x[CACHELINE_SIZE]; } cacheline_t;
  asm volatile("prefetcht0 %0" : : "m" (*(const cacheline_t *) ptr));
#endif
}

template <typename T>
static inline ALWAYS_INLINE void
prefetch_object(const T *ptr)
{
  for (unsigned i = CACHELINE_SIZE;
       i < std::min(static_cast<unsigned>(sizeof(*ptr)),
                    static_cast<unsigned>(4 * CACHELINE_SIZE));
       i += CACHELINE_SIZE)
    ::prefetch((const char *) ptr + i);
}

#endif /* _PREFETCH_H_ */
