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
  static const size_t NMaxCores = NMAXCORES;

  static inline size_t
  core_id()
  {
    if (unlikely(tl_core_id == -1)) {
      // initialize per-core data structures
      tl_core_id = g_core_count.fetch_add(1, std::memory_order_acq_rel);
      // did we exceed max cores?
      ALWAYS_ASSERT(size_t(tl_core_id) < NMaxCores);
    }
    return tl_core_id;
  }

  // actual number of CPUs online for the system
  static size_t num_cpus_online();

private:
  // the core ID of this core: -1 if not set
  static __thread ssize_t tl_core_id;

  // contains a running count of all the cores
  static std::atomic<size_t> g_core_count CACHE_ALIGNED;
};

// requires T to have no-arg ctor
template <typename T>
class percore {
public:

  inline T &
  operator[](unsigned i)
  {
    INVARIANT(i < NMAXCORES);
    return elems_[i].elem;
  }

  inline const T &
  operator[](unsigned i) const
  {
    INVARIANT(i < NMAXCORES);
    return elems_[i].elem;
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
  util::aligned_padded_elem<T> elems_[NMAXCORES];
};

template <typename T>
class percore_lazy : public percore<T *> {
public:

  percore_lazy(std::function<void(T &)> init = [](T &) {})
    : init_(init) {}

  inline T &
  operator[](unsigned i)
  {
    T * px = this->elems_[i].elem;
    if (unlikely(!px)) {
      T &ret = *(this->elems_[i].elem = new T);
      init_(ret);
      return ret;
    }
    return *px;
  }

  inline T &
  my()
  {
    return (*this)[coreid::core_id()];
  }

  inline T *
  view(unsigned i)
  {
    return percore<T *>::operator[](i);
  }

  inline const T *
  view(unsigned i) const
  {
    return percore<T *>::operator[](i);
  }

  inline const T *
  myview() const
  {
    return percore<T *>::my();
  }

private:
  std::function<void(T &)> init_;
};
