#ifndef _MACROS_H_
#define _MACROS_H_

#include <assert.h>

#define PACKED_CACHE_ALIGNED __attribute__((packed, aligned(CACHELINE_SIZE)))
#define NEVER_INLINE  __attribute__((noinline))
#define ALWAYS_INLINE __attribute__((always_inline))
#define UNUSED __attribute__((unused))

#define likely(x)   __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

#define COMPILER_MEMORY_FENCE asm volatile("" ::: "memory")

#ifdef NDEBUG
  #define ALWAYS_ASSERT(expr) (likely(e) ? (void)0 : abort())
#else
  #define ALWAYS_ASSERT(expr) assert(expr)
#endif /* NDEBUG */

#define ARRAY_NELEMS(a) (sizeof(a)/sizeof((a)[0]))

#endif /* _MACROS_H_ */
