#ifndef _LOCK_GUARD_H_
#define _LOCK_GUARD_H_

#include <utility>

#include "macros.h"

// this exists in C++11, but we have a richer constructor
template <typename BasicLockable>
class lock_guard {
public:

  template <class... Args>
  lock_guard(BasicLockable *l, Args &&... args)
    : l(l)
  {
    if (likely(l))
      l->lock(std::forward<Args>(args)...);
  }

  template <class... Args>
  lock_guard(BasicLockable &l, Args &&... args)
    : l(&l)
  {
    l.lock(std::forward<Args>(args)...);
  }

  ~lock_guard()
  {
    if (likely(l))
      l->unlock();
  }
private:
  BasicLockable *l;
};

#endif /* _LOCK_GUARD_H_ */
