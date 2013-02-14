#ifndef _NDB_CORE_H_
#define _NDB_CORE_H_

#include "macros.h"
#include <sys/types.h>

class coreid {
public:
  static const size_t NMaxCores = NMAXCORES;
  static size_t core_id();
  static inline size_t
  core_count()
  {
    return g_core_count;
  }
private:
  // the core ID of this core: -1 if not set
  static __thread ssize_t tl_core_id;

  // contains a running count of all the cores
  static volatile size_t g_core_count CACHE_ALIGNED;
};

#endif /* _NDB_CORE_H_ */
