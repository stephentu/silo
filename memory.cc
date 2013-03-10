#include "macros.h" // for TRAP_LARGE_ALLOOCATIONS

#ifdef TRAP_LARGE_ALLOOCATIONS

#include <new>
#include <iostream>
#include <stdlib.h>
#include <stdint.h>
#include <execinfo.h>
#include <unistd.h>

static void *
do_allocation(size_t sz, bool do_throw)
{
  if (unlikely(sz > (1 << 20))) { // allocations more than 1MB are suspect
    // print stacktrace:
    std::cerr << "Warning: Large memory allocation (" << sz << " bytes)" << std::endl;
    void *addrs[128];
    const size_t naddrs = backtrace(addrs, ARRAY_NELEMS(addrs));
    backtrace_symbols_fd(addrs, naddrs, STDERR_FILENO);
  }
  void *ret = malloc(sz);
  if (unlikely(!ret && do_throw))
    throw std::bad_alloc();
  return ret;
}

static inline void
do_deletion(void *p)
{
  free(p);
}

void*
operator new(size_t sz) throw (std::bad_alloc)
{
  return do_allocation(sz, true);
}

void*
operator new(size_t sz, const std::nothrow_t&) throw ()
{
  return do_allocation(sz, false);
}

void*
operator new[](size_t sz) throw (std::bad_alloc)
{
  return do_allocation(sz, true);
}

void*
operator new[](size_t sz, std::nothrow_t &) throw ()
{
  return do_allocation(sz, false);
}

void
operator delete(void *p) throw ()
{
  return do_deletion(p);
}

void
operator delete(void *p, const std::nothrow_t &) throw ()
{
  return do_deletion(p);
}

void
operator delete[](void *p) throw ()
{
  return do_deletion(p);
}

void
operator delete[](void *p, const std::nothrow_t &) throw ()
{
  return do_deletion(p);
}

#endif
