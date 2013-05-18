#ifndef _NDB_ALLOCATOR_H_
#define _NDB_ALLOCATOR_H_

#include <cstdint>
#include <iterator>
#include <mutex>

#include "util.h"
#include "macros.h"
#include "spinlock.h"

class allocator {
public:

  // our allocator doesn't let allocations exceed maxpercore over a single core
  //
  // Initialize can be called many times- but only the first call has effect.
  //
  // w/o calling Initialize(), behavior for this class is undefined
  static void Initialize(size_t ncpus, size_t maxpercore);

  static void DumpStats();

  // returns an arena linked-list
  static void *
  AllocateArenas(size_t cpu, size_t sz);

  static void
  ReleaseArenas(void **arenas);

  static const size_t LgAllocAlignment = 4; // all allocations aligned to 2^4 = 16
  static const size_t AllocAlignment = 1 << LgAllocAlignment;
  static const size_t MAX_ARENAS = 32;

  static inline std::pair<size_t, size_t>
  ArenaSize(size_t sz)
  {
    const size_t allocsz = util::round_up<size_t, LgAllocAlignment>(sz);
    const size_t arena = allocsz / AllocAlignment - 1;
    return std::make_pair(allocsz, arena);
  }

  // slow, but only needs to be called on initialization
  static void
  FaultRegion(size_t cpu);

  // returns true if managed by this allocator, false otherwise
  static inline bool
  ManagesPointer(void *p)
  {
    return p >= g_memstart && p < g_memend;
  }

  // assumes p is managed by this allocator- returns the CPU from which this pointer
  // was allocated
  static inline size_t
  PointerToCpu(void *p)
  {
    INVARIANT(p >= g_memstart);
    INVARIANT(p < g_memend);
    const size_t ret =
      (reinterpret_cast<char *>(p) - reinterpret_cast<char *>(g_memstart)) / g_maxpercore;
    INVARIANT(ret < g_ncpus);
    return ret;
  }

  static size_t GetPageSize();
  static size_t GetHugepageSize();

private:

  // [g_memstart, g_memstart + ncpus * maxpercore) is the region of memory mmap()-ed
  static void *g_memstart;
  static void *g_memend; // g_memstart + ncpus * maxpercore
  static size_t g_ncpus;
  static size_t g_maxpercore;

  struct percore {
    percore()
      : region_begin(nullptr),
        region_end(nullptr),
        region_faulted(false)
    {
      NDB_MEMSET(arenas, 0, sizeof(arenas));
    }
    percore(const percore &) = delete;
    percore(percore &&) = delete;
    percore &operator=(const percore &) = delete;

    // set by Initialize()
    void *region_begin;
    void *region_end;

    bool region_faulted;

    spinlock lock;
    std::mutex fault_lock; // XXX: hacky
    void *arenas[MAX_ARENAS];
  };
  static util::aligned_padded_elem<percore> g_regions[NMAXCORES];
};

#endif /* _NDB_ALLOCATOR_H_ */
