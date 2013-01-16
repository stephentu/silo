#ifndef _MACROS_H_
#define _MACROS_H_

#include <assert.h>

/** options */
#define NODE_PREFETCH
#define CHECK_INVARIANTS

#define CACHELINE_SIZE 64 // XXX: don't assume x86

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

#define VERBOSE(expr) ((void)0)
//#define VERBOSE(expr) (expr)

#ifdef CHECK_INVARIANTS
  #define INVARIANT(expr) ALWAYS_ASSERT(expr)
#else
  #define INVARIANT(expr) ((void)0)
#endif /* CHECK_INVARIANTS */

// XXX: would be nice if we checked these during single threaded execution
#define SINGLE_THREADED_INVARIANT(expr) ((void)0)

#ifdef NODE_PREFETCH
  #define prefetch_node(n) \
    do { \
      __builtin_prefetch(((uint8_t *)n)); \
      __builtin_prefetch(((uint8_t *)n) + 1 * CACHELINE_SIZE); \
      __builtin_prefetch(((uint8_t *)n) + 2 * CACHELINE_SIZE); \
      __builtin_prefetch(((uint8_t *)n) + 3 * CACHELINE_SIZE); \
    } while (0)
#else
  #define prefetch_node(n) ((void)0)
#endif /* NODE_PREFETCH */

#endif /* _MACROS_H_ */
