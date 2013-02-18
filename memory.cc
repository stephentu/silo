#include <stdlib.h>
#include <stdint.h>
#include <new>

#include "macros.h"

#ifdef TRAP_LARGE_ALLOOCATIONS

static void *
do_allocation(size_t sz, bool do_throw)
{
  if (unlikely(sz > (1 << 30)))
    // allow us to trap
    ALWAYS_ASSERT(false);
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

#endif /* CODEX_TAG_MEMORY */
