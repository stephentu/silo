#include <sys/mman.h>
#include <unistd.h>
#include <map>
#include <iostream>

#include "allocator.h"
#include "spinlock.h"
#include "lockguard.h"
#include "static_vector.h"

using namespace util;

// page+alloc routines taken from masstree

size_t
allocator::GetHugepageSizeImpl()
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

size_t
allocator::GetPageSizeImpl()
{
  return sysconf(_SC_PAGESIZE);
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

  static const size_t hugepgsize = GetHugepageSize();

  // round maxpercore to the nearest hugepagesize
  maxpercore = slow_round_up(maxpercore, hugepgsize);

  g_ncpus = ncpus;
  g_maxpercore = maxpercore;

  // mmap() the entire region for now, but just as a marker
  // (this does not actually cause physical pages to be allocated)
  // note: we allocate an extra hugepgsize so we can guarantee alignment
  // of g_memstart to a huge page boundary

  void * const x = mmap(nullptr, g_ncpus * g_maxpercore + hugepgsize,
      PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (x == MAP_FAILED) {
    perror("mmap");
    ALWAYS_ASSERT(false);
  }

  g_memstart = reinterpret_cast<void *>(iceil(uintptr_t(x), hugepgsize));
  g_memend = reinterpret_cast<char *>(g_memstart) + (g_ncpus * g_maxpercore);

  ALWAYS_ASSERT(!(reinterpret_cast<uintptr_t>(g_memstart) % hugepgsize));
  ALWAYS_ASSERT(reinterpret_cast<uintptr_t>(g_memend) <=
      (reinterpret_cast<uintptr_t>(x) + (g_ncpus * g_maxpercore + hugepgsize)));

  for (size_t i = 0; i < g_ncpus; i++) {
    g_regions[i]->region_begin =
      reinterpret_cast<char *>(g_memstart) + (i * g_maxpercore);
    g_regions[i]->region_end   =
      reinterpret_cast<char *>(g_memstart) + ((i + 1) * g_maxpercore);
  }

  s_init = true;
}

void
allocator::DumpStats()
{
  std::cerr << "[allocator] ncpus=" << g_ncpus << std::endl;
  for (size_t i = 0; i < g_ncpus; i++) {
    const bool f = g_regions[i]->region_faulted;
    const size_t remaining =
      intptr_t(g_regions[i]->region_end) -
      intptr_t(g_regions[i]->region_begin);
    std::cerr << "[allocator] cpu=" << i << " fully_faulted?=" << f
              << " remaining=" << remaining << " bytes" << std::endl;
  }
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
  static const size_t hugepgsize = GetHugepageSize();

  percore &pc = g_regions[cpu].elem;
  pc.lock.lock();
  if (likely(pc.arenas[arena])) {
    // claim
    void *ret = pc.arenas[arena];
    pc.arenas[arena] = nullptr;
    pc.lock.unlock();
    return ret;
  }

  void * const mypx = AllocateUnmanagedWithLock(pc, 1); // releases lock
  return initialize_page(mypx, hugepgsize, (arena + 1) * AllocAlignment);
}

void *
allocator::AllocateUnmanaged(size_t cpu, size_t nhugepgs)
{
  percore &pc = g_regions[cpu].elem;
  pc.lock.lock();
  return AllocateUnmanagedWithLock(pc, nhugepgs); // releases lock
}

void *
allocator::AllocateUnmanagedWithLock(percore &pc, size_t nhugepgs)
{
  static const size_t hugepgsize = GetHugepageSize();

  void * const mypx = pc.region_begin;

  // check alignment
  if (reinterpret_cast<uintptr_t>(mypx) % hugepgsize)
    ALWAYS_ASSERT(false);

  void * const mynewpx =
    reinterpret_cast<char *>(mypx) + nhugepgs * hugepgsize;

  ALWAYS_ASSERT(mynewpx <= pc.region_end); // out of memory otherwise

  const bool needs_mmap = !pc.region_faulted;
  pc.region_begin = mynewpx;
  pc.lock.unlock();

  if (needs_mmap) {
    void * const x = mmap(mypx, hugepgsize, PROT_READ | PROT_WRITE,
        MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED, -1, 0);
    if (unlikely(x == MAP_FAILED)) {
      perror("mmap");
      ALWAYS_ASSERT(false);
    }
    INVARIANT(x == mypx);
    //if (madvise(x, hugepgsize, MADV_HUGEPAGE | MADV_WILLNEED)) {
    if (madvise(x, hugepgsize, MADV_HUGEPAGE)) {
      perror("madvise");
      ALWAYS_ASSERT(false);
    }
  }

  return mypx;
}

void
allocator::ReleaseArenas(void **arenas)
{
  // cpu -> [(head, tail)]
  // XXX: use a small_map here?
  std::map<size_t, static_vector<std::pair<void *, void *>, MAX_ARENAS>> m;
  for (size_t arena = 0; arena < MAX_ARENAS; arena++) {
    void *p = arenas[arena];
    while (p) {
      void * const pnext = *reinterpret_cast<void **>(p);
      const size_t cpu = PointerToCpu(p);
      auto it = m.find(cpu);
      if (it == m.end()) {
        auto &v = m[cpu];
        v.resize(MAX_ARENAS);
        *reinterpret_cast<void **>(p) = nullptr;
        v[arena].first = v[arena].second = p;
      } else {
        auto &v = it->second;
        if (!v[arena].second) {
          *reinterpret_cast<void **>(p) = nullptr;
          v[arena].first = v[arena].second = p;
        } else {
          *reinterpret_cast<void **>(p) = v[arena].first;
          v[arena].first = p;
        }
      }
      p = pnext;
    }
  }
  for (auto &p : m) {
    INVARIANT(!p.second.empty());
    percore &pc = g_regions[p.first].elem;
    lock_guard<spinlock> l(pc.lock);
    for (size_t arena = 0; arena < MAX_ARENAS; arena++) {
      INVARIANT(bool(p.second[arena].first) == bool(p.second[arena].second));
      if (!p.second[arena].first)
        continue;
      *reinterpret_cast<void **>(p.second[arena].second) = pc.arenas[arena];
      pc.arenas[arena] = p.second[arena].first;
    }
  }
}

void
allocator::FaultRegion(size_t cpu)
{
  static const size_t hugepgsize = GetHugepageSize();
  //static const size_t pgsize = GetPageSize();
  INVARIANT(cpu < g_ncpus);
  percore &pc = g_regions[cpu].elem;
  if (pc.region_faulted)
    return;
  lock_guard<std::mutex> l1(pc.fault_lock);
  lock_guard<spinlock> l(pc.lock); // exclude other users of the allocator
  if (pc.region_faulted)
    return;
  // mmap the entire region + memset it for faulting
  if (reinterpret_cast<uintptr_t>(pc.region_begin) % hugepgsize)
    ALWAYS_ASSERT(false);
  const size_t sz =
    reinterpret_cast<uintptr_t>(pc.region_end) -
    reinterpret_cast<uintptr_t>(pc.region_begin);
  void * const x = mmap(pc.region_begin, sz, PROT_READ | PROT_WRITE,
      MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED, -1, 0);
  if (unlikely(x == MAP_FAILED)) {
    perror("mmap");
    ALWAYS_ASSERT(false);
  }
  INVARIANT(x == pc.region_begin);
  static const bool s_use_madv_willneed = getenv("USE_MADV_WILLNEED");
  const int advice =
    s_use_madv_willneed ? (MADV_HUGEPAGE | MADV_WILLNEED) : MADV_HUGEPAGE;
  if (madvise(x, sz, advice)) {
    perror("madvise");
    ALWAYS_ASSERT(false);
  }
  std::cerr << "cpu" << cpu << " starting faulting region ("
            << intptr_t(pc.region_end) - intptr_t(pc.region_begin) << " bytes)"
            << std::endl;
  for (char *px = (char *) pc.region_begin; px < (char *) pc.region_end; px += hugepgsize)
    *px = 0;
  std::cerr << "cpu" << cpu << " finished faulting region" << std::endl;
  pc.region_faulted = true;
}

void *allocator::g_memstart = nullptr;
void *allocator::g_memend = nullptr;
size_t allocator::g_ncpus = 0;
size_t allocator::g_maxpercore = 0;
aligned_padded_elem<allocator::percore> allocator::g_regions[NMAXCORES];
