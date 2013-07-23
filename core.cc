#include "core.h"
#include <unistd.h>

using namespace std;

size_t
coreid::num_cpus_online()
{
  const long nprocs = sysconf(_SC_NPROCESSORS_ONLN);
  ALWAYS_ASSERT(nprocs >= 1);
  return nprocs;
}

__thread ssize_t coreid::tl_core_id = -1;
atomic<size_t> coreid::g_core_count(0);
