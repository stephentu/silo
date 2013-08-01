#include <unistd.h>

#include "amd64.h"
#include "core.h"
#include "util.h"

using namespace std;
using namespace util;

int
coreid::allocate_contiguous_aligned_block(unsigned n, unsigned alignment)
{
retry:
  unsigned current = g_core_count.load(memory_order_acquire);
  const unsigned rounded = slow_round_up(current, alignment);
  const unsigned replace = rounded + n;
  if (unlikely(replace > NMaxCores))
    return -1;
  if (!g_core_count.compare_exchange_strong(current, replace, memory_order_acq_rel)) {
    nop_pause();
    goto retry;
  }
  return rounded;
}

unsigned
coreid::num_cpus_online()
{
  const long nprocs = sysconf(_SC_NPROCESSORS_ONLN);
  ALWAYS_ASSERT(nprocs >= 1);
  return nprocs;
}

__thread int coreid::tl_core_id = -1;
atomic<unsigned> coreid::g_core_count(0);
