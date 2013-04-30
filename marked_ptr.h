#ifndef _MARKED_PTR_H_
#define _MARKED_PTR_H_

#include <functional>
#include <iostream>

#include "macros.h"
#include "util.h"

template <typename T>
class marked_ptr {
public:

  // can take the bottom 3 bits of a ptr [ptrs must be 8-byte aligned]
  static const uintptr_t LowBitsMask = 0x7;

  constexpr inline marked_ptr() : px(0) {}

  template <typename U>
  inline marked_ptr(U *px) : px(reinterpret_cast<uintptr_t>(px))
  {
    // type-safety
    if (static_cast<T *>(px))
      ;
    INVARIANT(!(this->px & LowBitsMask));
  }

  // both operator= and copy-ctor PROPAGATE mark bits

  template <typename U>
  inline marked_ptr(const marked_ptr<U> &o)
    : px(o.px)
  {
    // type-safety
    if (static_cast<T *>((U *) nullptr))
      ;
  }

  template <typename U>
  inline marked_ptr &
  operator=(const marked_ptr<U> &o)
  {
    // type-safety
    if (static_cast<T *>((U *) nullptr))
      ;
    px = o.px;
    return *this;
  }

  inline
  operator bool() const
  {
    return get();
  }

  inline T *
  operator->() const
  {
    return get();
  }

  inline T &
  operator*() const
  {
    return *get();
  }

  inline T *
  get() const
  {
    return reinterpret_cast<T *>(px & ~LowBitsMask);
  }

  template <typename U>
  inline void
  reset(U *px)
  {
    INVARIANT(!(px & LowBitsMask));
    // type-safety
    if (static_cast<T *>(px))
      ;
    this->px = reinterpret_cast<uintptr_t>(px);
  }

  inline uint8_t
  get_flags() const
  {
    return px & LowBitsMask;
  }

  inline void
  set_flags(uint8_t flags)
  {
    INVARIANT(!(flags & ~LowBitsMask));
    px = (px & ~LowBitsMask) | flags;
  }

  inline void
  or_flags(uint8_t flags)
  {
    INVARIANT(!(flags & ~LowBitsMask));
    px |= flags;
  }

  template <typename U>
  inline bool
  operator==(const marked_ptr<U> &o) const
  {
    return get() == o.get();
  }

  template <typename U>
  inline bool
  operator!=(const marked_ptr<U> &o) const
  {
    return !operator==(o);
  }

  template <typename U>
  inline bool
  operator<(const marked_ptr<U> &o) const
  {
    return get() < o.get();
  }

  template <typename U>
  inline bool
  operator>=(const marked_ptr<U> &o) const
  {
    return !operator<(o);
  }

  template <typename U>
  inline bool
  operator>(const marked_ptr<U> &o) const
  {
    return get() > o.get();
  }

  template <typename U>
  inline bool
  operator<=(const marked_ptr<U> &o) const
  {
    return !operator>(o);
  }

private:
  uintptr_t px;
};

template <typename T>
inline std::ostream &
operator<<(std::ostream &o, const marked_ptr<T> &p)
{
  o << "[px=" << util::hexify(p.get()) << ", flags=0x" << util::hexify(p.get_flags()) << "]";
  return o;
}

namespace std {
  template <typename T>
  struct hash<marked_ptr<T>> {
    inline size_t
    operator()(marked_ptr<T> p) const
    {
      return hash<T *>()(p.get());
    }
  };
}

#endif /* _MARKED_PTR_H_ */
