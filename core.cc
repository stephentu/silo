#include "core.h"
#include "unistd.h"

size_t
coreid::core_id()
{
  if (unlikely(tl_core_id == -1)) {
    // initialize per-core data structures
    tl_core_id = __sync_fetch_and_add(&g_core_count, 1);
    // did we exceed max cores?
    ALWAYS_ASSERT(size_t(tl_core_id) < NMaxCores);
  }
  return tl_core_id;
}

size_t
coreid::num_cpus_online()
{
  const long nprocs = sysconf(_SC_NPROCESSORS_ONLN);
  ALWAYS_ASSERT(nprocs >= 1);
  return nprocs;
}

__thread ssize_t coreid::tl_core_id = -1;
volatile size_t coreid::g_core_count = 0;
