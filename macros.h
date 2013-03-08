#ifndef _MACROS_H_
#define _MACROS_H_

#include <assert.h>
#include <stdexcept>

/** options */
//#define NODE_PREFETCH
//#define DIE_ON_ABORT
//#define TRAP_LARGE_ALLOOCATIONS
#define USE_BUILTIN_MEMFUNCS
//#define CHECK_INVARIANTS
#define USE_SMALL_CONTAINER_OPT
#define BTREE_NODE_ALLOC_CACHE_ALIGNED
//#define TXN_BTREE_DUMP_PURGE_STATS
#define USE_VARINT_ENCODING

#define CACHELINE_SIZE 64 // XXX: don't assume x86

// global maximum on the number of unique threads allowed
// in the system
#define NMAXCOREBITS 10
#define NMAXCORES    (1 << NMAXCOREBITS)

// some helpers for cacheline alignment
#define CACHE_ALIGNED __attribute__((aligned(CACHELINE_SIZE)))
#define CACHE_PADOUT  char __padout[0] __attribute__((aligned(CACHELINE_SIZE)))
#define PACKED __attribute__((packed))

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

#define SMALL_SIZE_VEC 128
#define SMALL_SIZE_MAP 64

#define always_prefetch(n) \
  do { \
    __builtin_prefetch(((uint8_t *)n)); \
    __builtin_prefetch(((uint8_t *)n) + 1 * CACHELINE_SIZE); \
    __builtin_prefetch(((uint8_t *)n) + 2 * CACHELINE_SIZE); \
    __builtin_prefetch(((uint8_t *)n) + 3 * CACHELINE_SIZE); \
  } while (0)

#ifdef NODE_PREFETCH
  #define prefetch_node(n) always_prefetch(n)
#else
  #define prefetch_node(n) ((void)0)
#endif /* NODE_PREFETCH */

// throw exception after the assert(), so that GCC knows
// we'll never return
#define NDB_UNIMPLEMENTED(what) \
  do { \
    ALWAYS_ASSERT(false); \
    throw ::std::runtime_error(what); \
  } while (0)

#ifdef USE_BUILTIN_MEMFUNCS
#define NDB_MEMCPY __builtin_memcpy
#define NDB_MEMSET __builtin_memset
#else
#define NDB_MEMCPY memcpy
#define NDB_MEMSET memset
#endif

#endif /* _MACROS_H_ */
