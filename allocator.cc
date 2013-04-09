#include <sys/mman.h>
#include <unistd.h>

#include "allocator.h"
#include "spinlock.h"
#include "lockguard.h"

using namespace util;

// page+alloc routines taken from masstree

static size_t
get_hugepage_size()
{
  FILE *f = fopen("/proc/meminfo", "r");
  assert(f);
  char *linep = NULL;
  size_t n = 0;
  static const char *key = "Hugepagesize:";
  static const int keylen = strlen(key);
  size_t size = 0;
  while (getline(&linep, &n, f) > 0) {
    if (strstr(linep, key) != linep)
      continue;
    size = atol(linep + keylen) * 1024;
    break;
  }
  fclose(f);
  assert(size);
  return size;
}

static size_t
get_page_size()
{
  return sysconf(_SC_PAGESIZE);
}

static inline size_t
slow_round_up(size_t x, size_t q)
{
  const size_t r = x % q;
  if (!r)
    return x;
  return x + (q - r);
}

void
allocator::Initialize(size_t ncpus, size_t maxpercore)
{
  static spinlock s_lock;
  static bool s_init = false;
  if (likely(s_init))
    return;
  lock_guard<spinlock> l(s_lock);
  if (s_init)
    return;
  ALWAYS_ASSERT(!g_memstart);
  ALWAYS_ASSERT(!g_memend);
  ALWAYS_ASSERT(!g_ncpus);
  ALWAYS_ASSERT(!g_maxpercore);

  static const size_t hugepgsize = get_hugepage_size();

  // round maxpercore to the nearest hugepagesize
  maxpercore = slow_round_up(maxpercore, hugepgsize);

  g_ncpus = ncpus;
  g_maxpercore = maxpercore;

  // mmap() the entire region for now, but just as a marker
  // (this does not actually cause physical pages to be allocated)

  g_memstart = mmap(nullptr, g_ncpus * g_maxpercore,
      PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (g_memstart == MAP_FAILED) {
    perror("mmap");
    ALWAYS_ASSERT(false);
  }
  g_memend = reinterpret_cast<char *>(g_memstart) + (g_ncpus * g_maxpercore);

  // XXX: no reason this has to actually hold, but assume it does for now
  ALWAYS_ASSERT(!(reinterpret_cast<uintptr_t>(g_memstart) % get_page_size()));

  for (size_t i = 0; i < g_ncpus; i++) {
    g_regions[i]->region_begin = reinterpret_cast<char *>(g_memstart) + (i * g_maxpercore);
    g_regions[i]->region_end   = reinterpret_cast<char *>(g_memstart) + ((i + 1) * g_maxpercore);
  }

  s_init = true;
}

static void *
initialize_page(void *page, const size_t pagesize, const size_t unit)
{
  void *first = (void *)util::iceil((uintptr_t)page, (uintptr_t)unit);
  assert((uintptr_t)first + unit <= (uintptr_t)page + pagesize);
  void **p = (void **)first;
  void *next = (void *)((uintptr_t)p + unit);
  while ((uintptr_t)next + unit <= (uintptr_t)page + pagesize) {
    *p = next;
    p = (void **)next;
    next = (void *)((uintptr_t)next + unit);
  }
  *p = NULL;
  return first;
}

void *
allocator::AllocateArenas(size_t cpu, size_t arena)
{
  INVARIANT(cpu < g_ncpus);
  INVARIANT(arena < MAX_ARENAS);
  INVARIANT(g_memstart);
  INVARIANT(g_maxpercore);
  static const size_t pgsize = get_page_size();
  static const size_t hugepgsize = get_hugepage_size();

  // check w/o locking first
  percore &pc = g_regions[cpu].elem;
  if (pc.arenas[arena]) {
    lock_guard<spinlock> l(pc.lock);
    if (pc.arenas[arena]) {
      // claim
      void *ret = pc.arenas[arena];
      pc.arenas[arena] = nullptr;
      return ret;
    }
  }

  // out of memory?
  ALWAYS_ASSERT(pc.region_begin < pc.region_end);

  // do allocation in region
  void *mypx = nullptr;
  {
    lock_guard<spinlock> l(pc.lock);
    mypx = pc.region_begin;
    pc.region_begin = reinterpret_cast<char *>(pc.region_begin) + hugepgsize;
  }

  // out of memory?
  ALWAYS_ASSERT(mypx < pc.region_end);

  if (reinterpret_cast<uintptr_t>(mypx) % pgsize)
    ALWAYS_ASSERT(false);

  void *x = mmap(mypx, hugepgsize, PROT_READ | PROT_WRITE,
      MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB | MAP_FIXED, -1, 0);
  if (unlikely(x == MAP_FAILED)) {
    perror("mmap");
    ALWAYS_ASSERT(false);
  }

  return initialize_page(x, hugepgsize, (arena + 1) * AllocAlignment);
}

void *allocator::g_memstart = nullptr;
void *allocator::g_memend = nullptr;
size_t allocator::g_ncpus = 0;
size_t allocator::g_maxpercore = 0;
aligned_padded_elem<allocator::percore> allocator::g_regions[NMAXCORES];
