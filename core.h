#ifndef _NDB_CORE_H_
#define _NDB_CORE_H_

#include "macros.h"
#include <sys/types.h>

class coreid {
public:
  static const size_t NMaxCores = NMAXCORES;

  static inline size_t
  core_id()
  {
    if (unlikely(tl_core_id == -1)) {
      // initialize per-core data structures
      tl_core_id = __sync_fetch_and_add(&g_core_count, 1);
      // did we exceed max cores?
      ALWAYS_ASSERT(size_t(tl_core_id) < NMaxCores);
    }
    return tl_core_id;
  }

  static inline size_t
  core_count()
  {
    return g_core_count;
  }

  // actual number of CPUs online for the system
  static size_t num_cpus_online();

private:
  // the core ID of this core: -1 if not set
  static __thread ssize_t tl_core_id;

  // contains a running count of all the cores
  static volatile size_t g_core_count CACHE_ALIGNED;
};

#endif /* _NDB_CORE_H_ */
