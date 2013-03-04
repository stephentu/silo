#ifndef _SMALL_VECTOR_H_
#define _SMALL_VECTOR_H_

#include <vector>
#include "macros.h"

/**
 * References are not guaranteed to be stable across mutation
 *
 * Also does not have precise destructor semantics
 */
template <typename T, size_t SmallSize = 128>
class small_vector {
public:

  typedef T value_type;
  typedef T & reference;
  typedef const T & const_reference;
  typedef size_t size_type;

  small_vector() : n(0), large_elems(0) {}
  ~small_vector()
  {
    if (unlikely(large_elems))
      delete large_elems;
  }

  template <size_t OtherSmallSize>
  small_vector(const small_vector<T, OtherSmallSize> &that)
  {
    NDB_UNIMPLEMENTED("copy-constructor");
  }

  template <size_t OtherSmallSize>
  small_vector &
  operator=(const small_vector &)
  {
    NDB_UNIMPLEMENTED("copy-assignment");
  }

  inline size_t
  size() const
  {
    if (unlikely(large_elems))
      return large_elems->size();
    return n;
  }

  inline bool
  empty() const
  {
    return size() == 0;
  }

  inline reference
  front()
  {
    if (unlikely(large_elems))
      return large_elems->front();
    INVARIANT(n > 0);
    return small_elems[0];
  }

  inline const_reference
  front() const
  {
    return const_cast<small_vector>(this)->front();
  }

  inline reference
  back()
  {
    if (unlikely(large_elems))
      return large_elems->back();
    INVARIANT(n > 0);
    return small_elems[n - 1];
  }

  inline const_reference
  back() const
  {
    return const_cast<small_vector>(this)->back();
  }

  inline void
  pop_back()
  {
    if (unlikely(large_elems)) {
      large_elems->pop_back();
      return;
    }
    INVARIANT(n > 0);
    n--;
  }

  inline void
  push_back(const T &obj)
  {
    if (unlikely(large_elems)) {
      large_elems->push_back(obj);
      return;
    }
    if (unlikely(n == SmallSize)) {
      large_elems = new std::vector<T>(&small_elems[0], &small_elems[n]);
      large_elems->push_back(obj);
      return;
    }
    small_elems[n++] = obj;
  }

  inline reference
  operator[](int i)
  {
    if (unlikely(large_elems))
      return large_elems->operator[](i);
    small_elems[i];
  }

  inline const_reference
  operator[](int i) const
  {
    return const_cast<small_vector>(this)->operator[](i);
  }

private:
  size_t n;
  T small_elems[SmallSize];
  std::vector<T> *large_elems;
};

#endif /* _SMALL_VECTOR_H_ */
