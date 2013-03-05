#ifndef _SMALL_VECTOR_H_
#define _SMALL_VECTOR_H_

#include <vector>
#include "macros.h"

/**
 * References are not guaranteed to be stable across mutation
 *
 * XXX(stephentu): allow custom allocator
 */
template <typename T, size_t SmallSize = 128>
class small_vector {
  typedef std::vector<T> large_vector_type;
public:

  typedef T value_type;
  typedef T & reference;
  typedef const T & const_reference;
  typedef size_t size_type;

  small_vector() : n(0), large_elems(0) {}
  ~small_vector()
  {
    clear();
  }

  small_vector(const small_vector &that)
    : n(0), large_elems(0)
  {
    assignFrom(that);
  }

  small_vector &
  operator=(const small_vector &that)
  {
    assignFrom(that);
    return *this;
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
    INVARIANT(n <= SmallSize);
    return *ptr();
  }

  inline const_reference
  front() const
  {
    return const_cast<small_vector *>(this)->front();
  }

  inline reference
  back()
  {
    if (unlikely(large_elems))
      return large_elems->back();
    INVARIANT(n > 0);
    INVARIANT(n <= SmallSize);
    return ptr()[n - 1];
  }

  inline const_reference
  back() const
  {
    return const_cast<small_vector *>(this)->back();
  }

  inline void
  pop_back()
  {
    if (unlikely(large_elems)) {
      large_elems->pop_back();
      return;
    }
    INVARIANT(n > 0);
    ptr()[n - 1].~T();
    n--;
  }

  inline void
  push_back(const T &obj)
  {
    if (unlikely(large_elems)) {
      INVARIANT(!n);
      large_elems->push_back(obj);
      return;
    }
    if (unlikely(n == SmallSize)) {
      large_elems = new large_vector_type(ptr(), ptr() + n);
      large_elems->push_back(obj);
      n = 0;
      return;
    }
    INVARIANT(n < SmallSize);
    new (&(ptr()[n++])) T(obj);
  }

  inline reference
  operator[](int i)
  {
    if (unlikely(large_elems))
      return large_elems->operator[](i);
    return ptr()[n];
  }

  inline const_reference
  operator[](int i) const
  {
    return const_cast<small_vector *>(this)->operator[](i);
  }

  void
  clear()
  {
    if (unlikely(large_elems)) {
      INVARIANT(!n);
      delete large_elems;
      large_elems = NULL;
      return;
    }
    for (size_t i = 0; i < n; i++)
      ptr()[i].~T();
    n = 0;
  }

private:

  template <typename ObjType, typename LargeTypeIter>
  class iterator_ : public std::iterator<std::forward_iterator_tag, ObjType> {
    friend class small_vector;
  public:
    iterator_() : large(false), p(0) {}

    template <typename O, typename L>
    iterator_(const iterator_<O, L> &other)
      : large(other.large), p(other.p)
    {}

    ObjType &
    operator*() const
    {
      if (unlikely(large))
        return *large_it;
      return *p;
    }

    ObjType *
    operator->() const
    {
      if (unlikely(large))
        return &(*large_it);
      return p;
    }

    bool
    operator==(const iterator_ &o) const
    {
      if (unlikely(large && o.large))
        return large_it == o.large_it;
      if (!large && !o.large)
        return p == o.p;
      return false;
    }

    bool
    operator!=(const iterator_ &o) const
    {
      return !operator==(o);
    }

    iterator_ &
    operator++()
    {
      if (unlikely(large)) {
        ++large_it;
        return *this;
      }
      ++p;
      return *this;
    }

    iterator_
    operator++(int)
    {
      iterator_ cur = *this;
      ++(*this);
      return cur;
    }

  protected:
    iterator_(ObjType *p)
      : large(false), p(p) {}
    iterator_(LargeTypeIter large_it)
      : large(true), p(0), large_it(large_it) {}

  private:
    bool large;
    ObjType *p;
    LargeTypeIter large_it;
  };

public:
  typedef
    iterator_<
      T, typename large_vector_type::iterator>
    iterator;

  typedef
    iterator_<
      const T, typename large_vector_type::const_iterator>
    const_iterator;

  iterator
  begin()
  {
    if (unlikely(large_elems))
      return iterator(large_elems->begin());
    return iterator(ptr());
  }

  const_iterator
  begin() const
  {
    if (unlikely(large_elems))
      return const_iterator(large_elems->begin());
    return const_iterator(ptr());
  }

  iterator
  end()
  {
    if (unlikely(large_elems))
      return iterator(large_elems->end());
    return iterator(ptr() + n);
  }

  const_iterator
  end() const
  {
    if (unlikely(large_elems))
      return const_iterator(large_elems->end());
    return const_iterator(ptr() + n);
  }

private:
  void
  assignFrom(const small_vector &that)
  {
    if (unlikely(this == &that))
      return;
    clear();
    if (unlikely(that.large_elems)) {
      large_elems = new large_vector_type(*that.large_elems);
    } else {
      INVARIANT(that.n <= SmallSize);
      for (size_t i = 0; i < that.n; i++)
        new (&(ptr()[i])) T(that.ptr()[i]);
      n = that.n;
    }
  }

  inline T *
  ptr()
  {
    return reinterpret_cast<T *>(&small_elems_buf[0]);
  }

  inline const T *
  ptr() const
  {
    return reinterpret_cast<const T *>(&small_elems_buf[0]);
  }

  size_t n;
  char small_elems_buf[sizeof(T) * SmallSize];
  large_vector_type *large_elems;
};

#endif /* _SMALL_VECTOR_H_ */
