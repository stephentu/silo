#ifndef _STATIC_UNORDERED_MAP_H_
#define _STATIC_UNORDERED_MAP_H_

#include "small_unordered_map.h"

/**
 * XXX(stephentu): allow custom allocator
 */
template <typename Key,
          typename T,
          size_t StaticSize = SMALL_SIZE_MAP,
          typename Hash = private_::myhash<Key>>
class static_unordered_map {
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

  static const bool is_trivially_destructible =
    private_::is_trivially_destructible<bucket_value_type>::value;

  static const size_t TableSize = private_::TableSize(StaticSize);
  static_assert(StaticSize >= 1, "XXX");
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
  template <typename BucketType, typename ValueType>
  class iterator_ : public std::iterator<std::forward_iterator_tag, ValueType> {
    friend class static_unordered_map;
  public:
    inline iterator_() : b(0) {}

    template <typename B, typename V>
    inline iterator_(const iterator_<B, V> &other)
      : b(other.b)
    {}

    inline ValueType &
    operator*() const
    {
      return reinterpret_cast<ValueType &>(b->ref());
    }

    inline ValueType *
    operator->() const
    {
      return reinterpret_cast<ValueType *>(b->ptr());
    }

    inline bool
    operator==(const iterator_ &o) const
    {
      return b == o.b;
    }

    inline bool
    operator!=(const iterator_ &o) const
    {
      return !operator==(o);
    }

    inline iterator_ &
    operator++()
    {
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
    inline iterator_(BucketType *b)
      : b(b)
    {
    }

  private:
    BucketType *b;
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

  typedef iterator_<bucket, value_type> iterator;
  typedef iterator_<const bucket, const value_type> const_iterator;

  static_unordered_map()
    : n(0)
  {
    NDB_MEMSET(&table[0], 0, sizeof(table));
  }

  ~static_unordered_map()
  {
    for (size_t i = 0; i < n; i++)
      elems[i].destroy();
#ifdef ENABLE_EVENT_COUNTERS
    size_t ml = 0;
    for (size_t i = 0; i < TableSize; i++) {
      const size_t l = chain_length(table[i]);
      ml = std::max(ml, l);

    }
    if (ml)
      private_::evt_avg_max_unordered_map_chain_length.offer(ml);
#endif
  }

  static_unordered_map(const static_unordered_map &other)
    : n(0)
  {
    NDB_MEMSET(&table[0], 0, sizeof(table));
    assignFrom(other);
  }

  static_unordered_map &
  operator=(const static_unordered_map &other)
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
    return const_cast<static_unordered_map *>(this)->find_bucket(k, hash_value);
  }

public:

  // XXX(stephentu): template away this stuff

  mapped_type &
  operator[](const key_type &k)
  {
    size_t h;
    bucket *b = find_bucket(k, &h);
    if (b)
      return b->ref().second;
    INVARIANT(n < StaticSize);
    b = &elems[n++];
    b->construct(h, k, mapped_type());
    const size_t i = h % TableSize;
    b->bnext = table[i];
    table[i] = b;
    return b->ref().second;
  }

  mapped_type &
  operator[](key_type &&k)
  {
    size_t h;
    bucket *b = find_bucket(k, &h);
    if (b)
      return b->ref().second;
    INVARIANT(n < StaticSize);
    b = &elems[n++];
    b->construct(h, std::move(k), mapped_type());
    const size_t i = h % TableSize;
    b->bnext = table[i];
    table[i] = b;
    return b->ref().second;
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

  iterator
  begin()
  {
    return iterator(&elems[0]);
  }

  const_iterator
  begin() const
  {
    return const_iterator(&elems[0]);
  }

  inline iterator
  end()
  {
    return iterator(&elems[n]);
  }

  inline const_iterator
  end() const
  {
    return const_iterator(&elems[n]);
  }

  iterator
  find(const key_type &k)
  {
    bucket * const b = find_bucket(k, 0);
    if (b)
      return iterator(b);
    return end();
  }

  const_iterator
  find(const key_type &k) const
  {
    const bucket * const b = find_bucket(k, 0);
    if (b)
      return const_iterator(b);
    return end();
  }

  void
  clear()
  {
    if (!n)
      return;
    NDB_MEMSET(&table[0], 0, sizeof(table));
    for (size_t i = 0; i < n; i++)
      elems[i].destroy();
    n = 0;
  }

public:
  // non-standard API
  inline bool is_small_type() const { return true; }

private:

  // doesn't check for self assignment
  inline void
  assignFrom(const static_unordered_map &that)
  {
    clear();
    for (size_t i = 0; i < that.n; i++) {
      bucket * const b = &elems[n++];
      const bucket * const that_b = &that.elems[i];
      b->construct(that_b->h, that_b->ref().first, that_b->ref().second);
      const size_t idx = b->h % TableSize;
      b->bnext = table[idx];
      table[idx] = b;
    }
  }

  size_t n;
  bucket elems[StaticSize];
  bucket *table[TableSize];
};

#endif /* _STATIC_UNORDERED_MAP_H_ */
