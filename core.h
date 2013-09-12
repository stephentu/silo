#pragma once

#include <atomic>
#include <sys/types.h>
#include "macros.h"
#include "util.h"

/**
 * XXX: CoreIDs are not recyclable for now, so NMAXCORES is really the number
 * of threads which can ever be spawned in the system
 */
class coreid {
public:
  static const unsigned NMaxCores = NMAXCORES;

  static inline unsigned
  core_id()
  {
    if (unlikely(tl_core_id == -1)) {
      // initialize per-core data structures
      tl_core_id = g_core_count.fetch_add(1, std::memory_order_acq_rel);
      // did we exceed max cores?
      ALWAYS_ASSERT(unsigned(tl_core_id) < NMaxCores);
    }
    return tl_core_id;
  }

  /**
   * Since our current allocation scheme does not allow for holes in the
   * allocation, this function is quite wasteful. Don't abuse.
   *
   * Returns -1 if it is impossible to do this w/o exceeding max allocations
   */
  static int
  allocate_contiguous_aligned_block(unsigned n, unsigned alignment);

  /**
   * WARNING: this function is scary, and exists solely as a hack
   *
   * You are allowed to set your own core id under several conditions
   * (the idea is that somebody else has allocated a block of core ids
   *  and is assigning one to you, under the promise of uniqueness):
   *
   * 1) You haven't already called core_id() yet (so you have no assignment)
   * 2) The number you are setting is < the current assignment counter (meaning
   *    it was previously assigned by someone)
   *
   * These are necessary but not sufficient conditions for uniqueness
   */
  static void
  set_core_id(unsigned cid)
  {
    ALWAYS_ASSERT(cid < NMaxCores);
    ALWAYS_ASSERT(cid < g_core_count.load(std::memory_order_acquire));
    ALWAYS_ASSERT(tl_core_id == -1);
    tl_core_id = cid; // sigh
  }

  // actual number of CPUs online for the system
  static unsigned num_cpus_online();

private:
  // the core ID of this core: -1 if not set
  static __thread int tl_core_id;

  // contains a running count of all the cores
  static std::atomic<unsigned> g_core_count CACHE_ALIGNED;
};

// requires T to have no-arg ctor
template <typename T, bool CallDtor = false, bool Pedantic = true>
class percore {
public:

  percore()
  {
    for (size_t i = 0; i < size(); i++) {
      using namespace util;
      new (&(elems()[i])) aligned_padded_elem<T, Pedantic>();
    }
  }

  ~percore()
  {
    if (!CallDtor)
      return;
    for (size_t i = 0; i < size(); i++) {
      using namespace util;
      elems()[i].~aligned_padded_elem<T, Pedantic>();
    }
  }

  inline T &
  operator[](unsigned i)
  {
    INVARIANT(i < NMAXCORES);
    return elems()[i].elem;
  }

  inline const T &
  operator[](unsigned i) const
  {
    INVARIANT(i < NMAXCORES);
    return elems()[i].elem;
  }

  inline T &
  my()
  {
    return (*this)[coreid::core_id()];
  }

  inline const T &
  my() const
  {
    return (*this)[coreid::core_id()];
  }

  // XXX: make an iterator

  inline size_t
  size() const
  {
    return NMAXCORES;
  }

protected:

  inline util::aligned_padded_elem<T, Pedantic> *
  elems()
  {
    return (util::aligned_padded_elem<T, Pedantic> *) &bytes_[0];
  }

  inline const util::aligned_padded_elem<T, Pedantic> *
  elems() const
  {
    return (const util::aligned_padded_elem<T, Pedantic> *) &bytes_[0];
  }

  char bytes_[sizeof(util::aligned_padded_elem<T, Pedantic>) * NMAXCORES];
};

namespace private_ {
  template <typename T>
  struct buf {
    char bytes_[sizeof(T)];
    inline T * cast() { return (T *) &bytes_[0]; }
    inline const T * cast() const { return (T *) &bytes_[0]; }
  };
}

template <typename T>
class percore_lazy : private percore<private_::buf<T>, false> {
  typedef private_::buf<T> buf_t;
public:

  percore_lazy()
  {
    NDB_MEMSET(&flags_[0], 0, sizeof(flags_));
  }

  template <class... Args>
  inline T &
  get(unsigned i, Args &&... args)
  {
    buf_t &b = this->elems()[i].elem;
    if (unlikely(!flags_[i])) {
      flags_[i] = true;
      T *px = new (&b.bytes_[0]) T(std::forward<Args>(args)...);
      return *px;
    }
    return *b.cast();
  }

  template <class... Args>
  inline T &
  my(Args &&... args)
  {
    return get(coreid::core_id(), std::forward<Args>(args)...);
  }

  inline T *
  view(unsigned i)
  {
    buf_t &b = this->elems()[i].elem;
    return flags_[i] ? b.cast() : nullptr;
  }

  inline const T *
  view(unsigned i) const
  {
    const buf_t &b = this->elems()[i].elem;
    return flags_[i] ? b.cast() : nullptr;
  }

  inline const T *
  myview() const
  {
    return view(coreid::core_id());
  }

private:
  bool flags_[NMAXCORES];
  CACHE_PADOUT;
};
