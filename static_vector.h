#ifndef _STATIC_VECTOR_H_
#define _STATIC_VECTOR_H_

#include <algorithm>
#include <type_traits>
#include "macros.h"
#include "ndb_type_traits.h"

template <typename T, size_t StaticSize = SMALL_SIZE_VEC>
class static_vector {

  static const bool is_trivially_destructible =
    private_::is_trivially_destructible<T>::value;

  // std::is_trivially_copyable not supported in g++-4.7
  static const bool is_trivially_copyable = std::is_scalar<T>::value;

public:

  typedef T value_type;
  typedef T & reference;
  typedef const T & const_reference;
  typedef size_t size_type;

  inline static_vector() : n(0) {}
  inline ~static_vector() { clear(); }

  inline static_vector(const static_vector &that)
    : n(0)
  {
    assignFrom(that);
  }

  // not efficient, don't use in performance critical parts
  static_vector(std::initializer_list<T> l)
    : n(0)
  {
    for (auto &p : l)
      push_back(p);
  }

  static_vector &
  operator=(const static_vector &that)
  {
    assignFrom(that);
    return *this;
  }

  inline size_t
  size() const
  {
    return n;
  }

  inline bool
  empty() const
  {
    return !n;
  }

  inline reference
  front()
  {
    INVARIANT(n > 0);
    INVARIANT(n <= StaticSize);
    return *ptr();
  }

  inline const_reference
  front() const
  {
    return const_cast<static_vector *>(this)->front();
  }

  inline reference
  back()
  {
    INVARIANT(n > 0);
    INVARIANT(n <= StaticSize);
    return ptr()[n - 1];
  }

  inline const_reference
  back() const
  {
    return const_cast<static_vector *>(this)->back();
  }

  inline void
  pop_back()
  {
    INVARIANT(n > 0);
    if (!is_trivially_destructible)
      ptr()[n - 1].~T();
    n--;
  }

  inline void
  push_back(const T &obj)
  {
    emplace_back(obj);
  }

  inline void
  push_back(T &&obj)
  {
    emplace_back(std::move(obj));
  }

  // C++11 goodness- a strange syntax this is

  template <class... Args>
  inline void
  emplace_back(Args &&... args)
  {
    INVARIANT(n < StaticSize);
    new (&(ptr()[n++])) T(std::forward<Args>(args)...);
  }

  inline reference
  operator[](int i)
  {
    return ptr()[i];
  }

  inline const_reference
  operator[](int i) const
  {
    return const_cast<static_vector *>(this)->operator[](i);
  }

  void
  clear()
  {
    if (!is_trivially_destructible)
      for (size_t i = 0; i < n; i++)
        ptr()[i].~T();
    n = 0;
  }

  inline void
  reserve(size_t n)
  {
  }

  inline void
  resize(size_t n, value_type val = value_type())
  {
    INVARIANT(n <= StaticSize);
    if (n > this->n) {
      // expand
      while (this->n < n)
        new (&ptr()[this->n++]) T(val);
    } else if (n < this->n) {
      // shrink
      while (this->n > n) {
        if (!is_trivially_destructible)
          ptr()[this->n - 1].~T();
        this->n--;
      }
    }
  }

  // non-standard API
  inline bool is_small_type() const { return true; }

  template <typename Compare = std::less<T>>
  inline void
  sort(Compare c = Compare())
  {
    std::sort(begin(), end(), c);
  }

private:

  template <typename ObjType>
  class iterator_ : public std::iterator<std::bidirectional_iterator_tag, ObjType> {
    friend class static_vector;
  public:
    inline iterator_() : p(0) {}

    template <typename O>
    inline iterator_(const iterator_<O> &other)
      : p(other.p)
    {}

    inline ObjType &
    operator*() const
    {
      return *p;
    }

    inline ObjType *
    operator->() const
    {
      return p;
    }

    inline bool
    operator==(const iterator_ &o) const
    {
      return p == o.p;
    }

    inline bool
    operator!=(const iterator_ &o) const
    {
      return !operator==(o);
    }

    inline bool
    operator<(const iterator_ &o) const
    {
      return p < o.p;
    }

    inline bool
    operator>=(const iterator_ &o) const
    {
      return !operator<(o);
    }

    inline bool
    operator>(const iterator_ &o) const
    {
      return p > o.p;
    }

    inline bool
    operator<=(const iterator_ &o) const
    {
      return !operator>(o);
    }

    inline iterator_ &
    operator+=(int n)
    {
      p += n;
      return *this;
    }

    inline iterator_ &
    operator-=(int n)
    {
      p -= n;
      return *this;
    }

    inline iterator_
    operator+(int n) const
    {
      iterator_ cpy = *this;
      return cpy += n;
    }

    inline iterator_
    operator-(int n) const
    {
      iterator_ cpy = *this;
      return cpy -= n;
    }

    inline intptr_t
    operator-(const iterator_ &o) const
    {
      return p - o.p;
    }

    inline iterator_ &
    operator++()
    {
      ++p;
      return *this;
    }

    inline iterator_
    operator++(int)
    {
      iterator_ cur = *this;
      ++(*this);
      return cur;
    }

    inline iterator_ &
    operator--()
    {
      --p;
      return *this;
    }

    inline iterator_
    operator--(int)
    {
      iterator_ cur = *this;
      --(*this);
      return cur;
    }

  protected:
    inline iterator_(ObjType *p) : p(p) {}

  private:
    ObjType *p;
  };

public:

  typedef iterator_<T> iterator;
  typedef iterator_<const T> const_iterator;

  typedef std::reverse_iterator<iterator> reverse_iterator;
  typedef std::reverse_iterator<const_iterator> const_reverse_iterator;

  inline iterator
  begin()
  {
    return iterator(ptr());
  }

  inline const_iterator
  begin() const
  {
    return const_iterator(ptr());
  }

  inline iterator
  end()
  {
    return iterator(endptr());
  }

  inline const_iterator
  end() const
  {
    return const_iterator(endptr());
  }

  inline reverse_iterator
  rbegin()
  {
    return reverse_iterator(end());
  }

  inline const_reverse_iterator
  rbegin() const
  {
    return const_reverse_iterator(end());
  }

  inline reverse_iterator
  rend()
  {
    return reverse_iterator(begin());
  }

  inline const_reverse_iterator
  rend() const
  {
    return const_reverse_iterator(begin());
  }

private:
  void
  assignFrom(const static_vector &that)
  {
    if (unlikely(this == &that))
      return;
    clear();
    INVARIANT(that.n <= StaticSize);
    if (is_trivially_copyable) {
      NDB_MEMCPY(ptr(), that.ptr(), that.n * sizeof(T));
    } else {
      for (size_t i = 0; i < that.n; i++)
        new (&(ptr()[i])) T(that.ptr()[i]);
    }
    n = that.n;
  }

  inline ALWAYS_INLINE T *
  ptr()
  {
    return reinterpret_cast<T *>(&elems_buf[0]);
  }

  inline ALWAYS_INLINE const T *
  ptr() const
  {
    return reinterpret_cast<const T *>(&elems_buf[0]);
  }

  inline ALWAYS_INLINE T *
  endptr()
  {
    return ptr() + n;
  }

  inline ALWAYS_INLINE const T *
  endptr() const
  {
    return ptr() + n;
  }

  size_t n;
  char elems_buf[sizeof(T) * StaticSize];
};

#endif /* _STATIC_VECTOR_H_ */
