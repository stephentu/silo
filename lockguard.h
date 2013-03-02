#ifndef _LOCK_GUARD_H_
#define _LOCK_GUARD_H_

#include "macros.h"

template <typename BasicLockable>
class lock_guard {
public:
  lock_guard(BasicLockable *l)
    : l(l)
  {
    if (likely(l))
      l->lock();
  }
  lock_guard(BasicLockable &l)
    : l(&l)
  {
    l.lock();
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
