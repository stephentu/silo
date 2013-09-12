#ifndef _SMALL_UNORDERED_MAP_H_
#define _SMALL_UNORDERED_MAP_H_

#include <algorithm>
#include <iterator>
#include <stdint.h>
#include <unordered_map>
#include <type_traits>

#include "macros.h"
#include "counter.h"
#include "log2.hh"
#include "ndb_type_traits.h"

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

  static event_avg_counter evt_avg_max_unordered_map_chain_length CACHE_ALIGNED
    ("avg_max_unordered_map_chain_length");

  template <typename T>
  struct fast_func_param {
    typedef typename std::conditional<std::is_scalar<T>::value, T, const T &>::type type;
  };

  template <typename T>
  struct myhash {
    inline ALWAYS_INLINE size_t
    operator()(typename fast_func_param<T>::type t) const
    {
      return std::hash<T>()(t);
    }
  };

  template <typename Tp>
  struct myhash<Tp *> {
    inline ALWAYS_INLINE size_t
    operator()(Tp *t) const
    {
      // std::hash for ptrs is bad
      // tommyhash: http://tommyds.sourceforge.net/doc/tommyhash_8h_source.html
      size_t key = reinterpret_cast<size_t>(t) >> 3; // shift out 8-byte pointer alignment
#ifdef USE_TOMMY_HASH
      key = (~key) + (key << 21); // key = (key << 21) - key - 1;
      key = key ^ (key >> 24);
      key = (key + (key << 3)) + (key << 8); // key * 265
      key = key ^ (key >> 14);
      key = (key + (key << 2)) + (key << 4); // key * 21
      key = key ^ (key >> 28);
      key = key + (key << 31);
#else
      key = (~key) + (key << 21); // key = (key << 21) - key - 1;
      key = key ^ (key >> 24);
#endif
      return key;
    }
  };

  static inline constexpr size_t
  TableSize(size_t small_size)
  {
    return round_up_to_pow2_const(small_size);
  }
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
          typename Hash = private_::myhash<Key>>
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

  static const bool is_trivially_destructible =
    private_::is_trivially_destructible<bucket_value_type>::value;

  static const size_t TableSize = private_::TableSize(SmallSize);
  static_assert(SmallSize >= 1, "XXX");
  static_assert(TableSize >= 1, "XXX");

  struct bucket {
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
    inline ALWAYS_INLINE void
    construct(size_t hash, Args &&... args)
    {
      h = hash;
      new (&ref()) bucket_value_type(std::forward<Args>(args)...);
    }

    inline ALWAYS_INLINE void
    destroy()
    {
      if (!is_trivially_destructible)
        ref().~bucket_value_type();
    }

    struct bucket *bnext;
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
    inline iterator_() : large(false), b(0) {}

    template <typename S, typename L, typename V>
    inline iterator_(const iterator_<S, L, V> &other)
      : large(other.large), b(other.b)
    {}

    inline ValueType &
    operator*() const
    {
      if (unlikely(large))
        return *large_it;
      return reinterpret_cast<ValueType &>(b->ref());
    }

    inline ValueType *
    operator->() const
    {
      if (unlikely(large))
        return &(*large_it);
      return reinterpret_cast<ValueType *>(b->ptr());
    }

    inline bool
    operator==(const iterator_ &o) const
    {
      INVARIANT(large == o.large);
      if (likely(!large))
        return b == o.b;
      return large_it == o.large_it;
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
      b++;
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
    inline iterator_(SmallIterType *b)
      : large(false), b(b)
    {
    }
    inline iterator_(LargeIterType large_it)
      : large(true), large_it(large_it) {}

  private:
    bool large;
    SmallIterType *b;
    LargeIterType large_it;
  };

  static size_t
  chain_length(bucket *b)
  {
    size_t ret = 0;
    while (b) {
      ret++;
      b = b->bnext;
    }
    return ret;
  }

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
    NDB_MEMSET(&table[0], 0, sizeof(table));
  }

  ~small_unordered_map()
  {
    if (unlikely(large_elems)) {
      delete large_elems;
      return;
    }
    for (size_t i = 0; i < n; i++)
      small_elems[i].destroy();
  }

  small_unordered_map(const small_unordered_map &other)
    : n(0), large_elems(0)
  {
    NDB_MEMSET(&table[0], 0, sizeof(table));
    assignFrom(other);
  }

  small_unordered_map &
  operator=(const small_unordered_map &other)
  {
    // self assignment
    if (unlikely(this == &other))
      return *this;
    assignFrom(other);
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
    const size_t i = h % TableSize;
    bucket *b = table[i];
    while (b) {
      const bool check_hash = private_::is_eq_expensive<key_type>::value;
      if ((!check_hash || b->h == h) && b->ref().first == k)
        return b;
      b = b->bnext;
    }
    return 0;
  }

  inline ALWAYS_INLINE const bucket *
  find_bucket(const key_type &k, size_t *hash_value) const
  {
    return const_cast<small_unordered_map *>(this)->find_bucket(k, hash_value);
  }

public:

  // XXX(stephentu): template away this stuff

  mapped_type &
  operator[](const key_type &k)
  {
    if (unlikely(large_elems))
      return large_elems->operator[](k);
    size_t h;
    bucket *b = find_bucket(k, &h);
    if (b)
      return b->ref().second;
    if (unlikely(n == SmallSize)) {
      large_elems = new large_table_type;
      for (size_t n = 0; n < SmallSize; n++) {
        bucket &b = small_elems[n];
#if GCC_AT_LEAST_47
        large_elems->emplace(std::move(b.ref()));
#else
        large_elems->operator[](std::move(b.ref().first)) = std::move(b.ref().second);
#endif
        b.destroy();
      }
      n = 0;
      return large_elems->operator[](k);
    }
    INVARIANT(n < SmallSize);
    b = &small_elems[n++];
    b->construct(h, k, mapped_type());
    const size_t i = h % TableSize;
    b->bnext = table[i];
    table[i] = b;
    return b->ref().second;
  }

  mapped_type &
  operator[](key_type &&k)
  {
    if (unlikely(large_elems))
      return large_elems->operator[](std::move(k));
    size_t h;
    bucket *b = find_bucket(k, &h);
    if (b)
      return b->ref().second;
    if (unlikely(n == SmallSize)) {
      large_elems = new large_table_type;
      for (size_t n = 0; n < SmallSize; n++) {
        bucket &b = small_elems[n];
#if GCC_AT_LEAST_47
        large_elems->emplace(std::move(b.ref()));
#else
        large_elems->operator[](std::move(b.ref().first)) = std::move(b.ref().second);
#endif
        b.destroy();
      }
      n = 0;
      return large_elems->operator[](std::move(k));
    }
    INVARIANT(n < SmallSize);
    b = &small_elems[n++];
    b->construct(h, std::move(k), mapped_type());
    const size_t i = h % TableSize;
    b->bnext = table[i];
    table[i] = b;
    return b->ref().second;
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
    return iterator(&small_elems[0]);
  }

  const_iterator
  begin() const
  {
    if (unlikely(large_elems))
      return const_iterator(large_elems->begin());
    return const_iterator(&small_elems[0]);
  }

  inline iterator
  end()
  {
    if (unlikely(large_elems))
      return iterator(large_elems->end());
    return iterator(&small_elems[n]);
  }

  inline const_iterator
  end() const
  {
    if (unlikely(large_elems))
      return const_iterator(large_elems->end());
    return const_iterator(&small_elems[n]);
  }

  iterator
  find(const key_type &k)
  {
    if (unlikely(large_elems))
      return iterator(large_elems->find(k));
    bucket * const b = find_bucket(k, 0);
    if (b)
      return iterator(b);
    return end();
  }

  const_iterator
  find(const key_type &k) const
  {
    if (unlikely(large_elems))
      return const_iterator(large_elems->find(k));
    const bucket * const b = find_bucket(k, 0);
    if (b)
      return const_iterator(b);
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
    if (!n)
      return;
    NDB_MEMSET(&table[0], 0, sizeof(table));
    for (size_t i = 0; i < n; i++)
      small_elems[i].destroy();
    n = 0;
  }

public:
  // non-standard API
  inline bool is_small_type() const { return !large_elems; }

private:

  // doesn't check for self assignment
  inline void
  assignFrom(const small_unordered_map &that)
  {
    clear();
    if (that.large_elems) {
      INVARIANT(!that.n);
      INVARIANT(!n);
      large_elems = new large_table_type(*that.large_elems);
      return;
    }
    INVARIANT(!large_elems);
    for (size_t i = 0; i < that.n; i++) {
      bucket * const b = &small_elems[n++];
      const bucket * const that_b = &that.small_elems[i];
      b->construct(that_b->h, that_b->ref().first, that_b->ref().second);
      const size_t idx = b->h % TableSize;
      b->bnext = table[idx];
      table[idx] = b;
    }
  }

  size_t n;

  bucket small_elems[SmallSize];
  bucket *table[TableSize];

  large_table_type *large_elems;
};

#endif /* _SMALL_UNORDERED_MAP_H_ */
