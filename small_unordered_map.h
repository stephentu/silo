#ifndef _SMALL_UNORDERED_MAP_H_
#define _SMALL_UNORDERED_MAP_H_

#include <algorithm>
#include <iterator>
#include <stdint.h>
#include <unordered_map>
#include <tr1/unordered_map>

#include "macros.h"

namespace private_ {

  template <typename T>
  struct is_eq_expensive { static const bool value = true; };

  struct cheap_eq { static const bool value = false; };

  // equals is cheap for integer types
  template <> struct is_eq_expensive<bool>     : public cheap_eq {};
  template <> struct is_eq_expensive<uint8_t>  : public cheap_eq {};
  template <> struct is_eq_expensive<int8_t>   : public cheap_eq {};
  template <> struct is_eq_expensive<uint16_t> : public cheap_eq {};
  template <> struct is_eq_expensive<int16_t>  : public cheap_eq {};
  template <> struct is_eq_expensive<uint32_t> : public cheap_eq {};
  template <> struct is_eq_expensive<int32_t>  : public cheap_eq {};
  template <> struct is_eq_expensive<uint64_t> : public cheap_eq {};
  template <> struct is_eq_expensive<int64_t>  : public cheap_eq {};

}

/**
 * For under SmallSize, uses linear probing on a fixed size array. Otherwise,
 * delegates to a regular std::unordered_map
 *
 * XXX(stephentu): allow custom allocator
 */
template <typename Key,
          typename T,
          size_t SmallSize = SMALL_SIZE_MAP,
          typename Hash = std::hash<Key> >
class small_unordered_map {
public:
  typedef Key key_type;
  typedef T mapped_type;
  typedef std::pair<const key_type, mapped_type> value_type;
  typedef Hash hasher;
  typedef T & reference;
  typedef const T & const_reference;

private:
  typedef std::unordered_map<Key, T, Hash> large_table_type;
  typedef std::pair<key_type, mapped_type> bucket_value_type;

  typedef typename large_table_type::iterator large_table_iterator;
  typedef typename large_table_type::const_iterator large_table_const_iterator;

  struct bucket {
    inline bucket() : mapped(false) {}

    inline ALWAYS_INLINE bucket_value_type *
    ptr()
    {
      return reinterpret_cast<bucket_value_type *>(&buf[0]);
    }

    inline ALWAYS_INLINE const bucket_value_type *
    ptr() const
    {
      return reinterpret_cast<const bucket_value_type *>(&buf[0]);
    }

    inline ALWAYS_INLINE bucket_value_type &
    ref()
    {
      return *ptr();
    }

    inline ALWAYS_INLINE const bucket_value_type &
    ref() const
    {
      return *ptr();
    }

    template <class... Args>
    void
    construct(size_t hash, Args &&... args)
    {
      INVARIANT(!mapped);
      new (&ref()) bucket_value_type(std::forward<Args>(args)...);
      mapped = true;
      h = hash;
    }

    void
    destroy()
    {
      INVARIANT(mapped);
      ref().~bucket_value_type();
      mapped = false;
    }

    bool mapped;
    size_t h;
    char buf[sizeof(value_type)];
  };

  // iterators are not stable across mutation
  template <typename SmallIterType,
            typename LargeIterType,
            typename ValueType>
  class iterator_ : public std::iterator<std::forward_iterator_tag, ValueType> {
    friend class small_unordered_map;
  public:
    inline iterator_() : large(false), b(0), bend(0) {}

    template <typename S, typename L, typename V>
    inline iterator_(const iterator_<S, L, V> &other)
      : large(other.large), b(other.b), bend(other.bend)
    {}

    inline ValueType &
    operator*() const
    {
      if (unlikely(large))
        return *large_it;
      INVARIANT(b != bend);
      INVARIANT(b->mapped);
      return reinterpret_cast<ValueType &>(b->ref());
    }

    inline ValueType *
    operator->() const
    {
      if (unlikely(large))
        return &(*large_it);
      INVARIANT(b != bend);
      INVARIANT(b->mapped);
      return reinterpret_cast<ValueType *>(b->ptr());
    }

    inline bool
    operator==(const iterator_ &o) const
    {
      if (unlikely(large && o.large))
        return large_it == o.large_it;
      if (!large && !o.large)
        return b == o.b;
      return false;
    }

    inline bool
    operator!=(const iterator_ &o) const
    {
      return !operator==(o);
    }

    inline iterator_ &
    operator++()
    {
      if (unlikely(large)) {
        ++large_it;
        return *this;
      }
      INVARIANT(b < bend);
      do {
        b++;
      } while (b != bend && !b->mapped);
      return *this;
    }

    inline iterator_
    operator++(int)
    {
      iterator_ cur = *this;
      ++(*this);
      return cur;
    }

  protected:
    inline iterator_(SmallIterType *b, SmallIterType *bend)
      : large(false), b(b), bend(bend)
    {
      INVARIANT(b == bend || b->mapped);
    }
    inline iterator_(LargeIterType large_it)
      : large(true), large_it(large_it) {}

  private:
    bool large;
    SmallIterType *b;
    SmallIterType *bend;
    LargeIterType large_it;
  };

public:

  typedef
    iterator_<
      bucket,
      large_table_iterator,
      value_type>
    iterator;

  typedef
    iterator_<
      const bucket,
      large_table_const_iterator,
      const value_type>
    const_iterator;

  small_unordered_map()
    : n(0), large_elems(0)
  {
  }

  ~small_unordered_map()
  {
    clear();
  }

  small_unordered_map(const small_unordered_map &other)
    : n(other.n), large_elems()
  {
    if (unlikely(other.large_elems)) {
      large_elems = new large_table_type(*other.large_elems);
    } else {
      if (!n)
        return;
      for (size_t i = 0; i < SmallSize; i++) {
        bucket *const this_b = &small_elems[i];
        const bucket *const that_b = &other.small_elems[i];
        if (that_b->mapped)
          this_b->construct(that_b->h,
                            that_b->ref().first,
                            that_b->ref().second);
      }
    }
  }

  small_unordered_map &
  operator=(const small_unordered_map &other)
  {
    // self assignment
    if (unlikely(this == &other))
      return *this;
    clear();
    n = other.n;
    if (unlikely(other.large_elems)) {
      large_elems = new large_table_type(*other.large_elems);
    } else {
      if (!n)
        return *this;
      for (size_t i = 0; i < SmallSize; i++) {
        bucket *const this_b = &small_elems[i];
        const bucket *const that_b = &other.small_elems[i];
        if (that_b->mapped)
          this_b->construct(that_b->h,
                            that_b->ref().first,
                            that_b->ref().second);
      }
    }
    return *this;
  }

private:
  bucket *
  find_bucket(const key_type &k, size_t *hash_value)
  {
    INVARIANT(!large_elems);
    const size_t h = Hash()(k);
    if (hash_value)
      *hash_value = h;
    size_t i = h % SmallSize;
    size_t n = 0;
    while (n++ < SmallSize) {
      bucket &b = small_elems[i];
      const bool check_hash = private_::is_eq_expensive<key_type>::value;
      if (check_hash) {
        if ((b.mapped && b.h == h && b.ref().first == k) ||
            !b.mapped) {
          // found bucket
          return &b;
        }
      } else {
        if ((b.mapped && b.ref().first == k) ||
            !b.mapped) {
          // found bucket
          return &b;
        }
      }
      i = (i + 1) % SmallSize;
    }
    return 0;
  }

  inline ALWAYS_INLINE const bucket *
  find_bucket(const key_type &k, size_t *hash_value) const
  {
    return const_cast<small_unordered_map *>(this)->find_bucket(k, hash_value);
  }

public:

  // XXX(stephentu): can we avoid this code duplication?

  mapped_type &
  operator[](const key_type &k)
  {
    if (unlikely(large_elems))
      return large_elems->operator[](k);
    size_t h;
    bucket * const b = find_bucket(k, &h);
    if (likely(b)) {
      if (!b->mapped) {
        n++;
        b->construct(h, k, mapped_type());
      }
      return b->ref().second;
    }
    INVARIANT(n == SmallSize);
    // small_elems is full, so spill over to large_elems
    n = 0;
    large_elems = new large_table_type;
    for (size_t idx = 0; idx < SmallSize; idx++) {
      bucket &b = small_elems[idx];
      INVARIANT(b.mapped);
      large_elems->emplace(std::move(b.ref()));
      b.destroy();
    }
    return large_elems->operator[](k);
  }

  mapped_type &
  operator[](key_type &&k)
  {
    if (unlikely(large_elems))
      return large_elems->operator[](std::move(k));
    size_t h;
    bucket * const b = find_bucket(k, &h);
    if (likely(b)) {
      if (!b->mapped) {
        n++;
        b->construct(h, std::move(k), mapped_type());
      }
      return b->ref().second;
    }
    INVARIANT(n == SmallSize);
    // small_elems is full, so spill over to large_elems
    n = 0;
    large_elems = new large_table_type;
    for (size_t idx = 0; idx < SmallSize; idx++) {
      bucket &b = small_elems[idx];
      INVARIANT(b.mapped);
      large_elems->emplace(std::move(b.ref()));
      b.destroy();
    }
    return large_elems->operator[](std::move(k));
  }

  // C++11 goodness
  template <class... Args>
  std::pair<iterator, bool>
  emplace(Args &&... args)
  {
    if (unlikely(large_elems)) {
      std::pair<large_table_iterator, bool> ret =
        large_elems->emplace(std::forward<Args>(args)...);
      return std::make_pair(iterator(ret.first), ret.second);
    }
    bucket_value_type tpe(std::forward<Args>(args)...);
    size_t h;
    bucket * const b = find_bucket(tpe.first, &h);
    bucket * const bend = &small_elems[SmallSize];
    if (likely(b)) {
      if (!b->mapped) {
        n++;
        b->construct(h, std::move(tpe));
        return std::make_pair(iterator(b, bend), true);
      }
      return std::make_pair(iterator(b, bend), false);
    }
    INVARIANT(n == SmallSize);
    // small_elems is full, so spill over to large_elems
    n = 0;
    large_elems = new large_table_type;
    for (size_t idx = 0; idx < SmallSize; idx++) {
      bucket &b = small_elems[idx];
      INVARIANT(b.mapped);
      large_elems->emplace(std::move(b.ref()));
      b.destroy();
    }
    std::pair<large_table_iterator, bool> ret =
      large_elems->emplace(std::move(tpe));
    return std::make_pair(iterator(ret.first), ret.second);
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

  iterator
  begin()
  {
    if (unlikely(large_elems))
      return iterator(large_elems->begin());
    bucket *b = &small_elems[0];
    bucket *const bend = &small_elems[SmallSize];
    while (b != bend && !b->mapped)
      b++;
    return iterator(b, bend);
  }

  const_iterator
  begin() const
  {
    if (unlikely(large_elems))
      return const_iterator(large_elems->begin());
    const bucket *b = &small_elems[0];
    const bucket *const bend = &small_elems[SmallSize];
    while (b != bend && !b->mapped)
      b++;
    return const_iterator(b, bend);
  }

  inline iterator
  end()
  {
    if (unlikely(large_elems))
      return iterator(large_elems->end());
    return iterator(&small_elems[SmallSize], &small_elems[SmallSize]);
  }

  inline const_iterator
  end() const
  {
    if (unlikely(large_elems))
      return const_iterator(large_elems->end());
    return const_iterator(&small_elems[SmallSize], &small_elems[SmallSize]);
  }

  iterator
  find(const key_type &k)
  {
    if (unlikely(large_elems))
      return iterator(large_elems->find(k));
    bucket *b = find_bucket(k, 0);
    if (likely(b) && b->mapped)
      return iterator(b, &small_elems[SmallSize]);
    return end();
  }

  const_iterator
  find(const key_type &k) const
  {
    if (unlikely(large_elems))
      return const_iterator(large_elems->find(k));
    const bucket *b = find_bucket(k, 0);
    if (likely(b) && b->mapped)
      return const_iterator(b, &small_elems[SmallSize]);
    return end();
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
    for (size_t i = 0; i < SmallSize; i++)
      if (small_elems[i].mapped)
        small_elems[i].destroy();
    n = 0;
  }

private:

  size_t n;
  bucket small_elems[SmallSize];
  large_table_type *large_elems;
};

#endif /* _SMALL_UNORDERED_MAP_H_ */
