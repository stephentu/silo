#ifndef _NDB_TYPED_TXN_BTREE_H_
#define _NDB_TYPED_TXN_BTREE_H_

#include "base_txn_btree.h"
#include "record/cursor.h"

template <template <typename> class Transaction, typename Schema>
class typed_txn_btree : public base_txn_btree<Transaction> {
  typedef base_txn_btree<Transaction> super_type;
public:

  typedef typename super_type::string_type string_type;
  typedef typename super_type::size_type size_type;
  typedef typename super_type::default_string_allocator default_string_allocator;

  typedef typename Schema::base_type base_type;
  typedef typename Schema::key_type key_type;
  typedef typename Schema::value_type value_type;
  typedef typename Schema::value_descriptor_type value_descriptor_type;
  typedef typename Schema::key_encoder_type key_encoder_type;
  typedef typename Schema::value_encoder_type value_encoder_type;

private:

  static_assert(value_descriptor_type::nfields() <= 64, "xx");

  static const uint64_t AllFieldsMask = (1UL << value_descriptor_type::nfields()) - 1;

  class key_reader {
  public:
    constexpr key_reader(bool no_key_results) : no_key_results(no_key_results) {}
    inline const key_type &
    operator()(const std::string &s)
    {
      const key_encoder_type key_encoder;
      if (!no_key_results)
        key_encoder.read(s, &k);
      return k;
    }
  private:
    key_type k;
    bool no_key_results;
  };

  static inline bool
  do_record_read(const uint8_t *data, size_t sz, uint64_t fields_mask, value_type *v)
  {
    if ((fields_mask & AllFieldsMask) == AllFieldsMask) {
      // read the entire record
      const value_encoder_type value_encoder;
      return value_encoder.failsafe_read(data, sz, v);
    } else {
      // pick individual fields
      read_record_cursor<base_type> r(data, sz);
      for (uint64_t i = 0; i < value_descriptor_type::nfields(); i++) {
        if ((1UL << i) & fields_mask) {
          r.skip_to(i);
          if (unlikely(!r.read_current_and_advance(v)))
            return false;
        }
      }
      return true;
    }
  }

  class single_value_reader {
  public:
    constexpr single_value_reader(value_type &v, uint64_t fields_mask)
      : v(&v), fields_mask(fields_mask) {}

    inline bool
    operator()(const uint8_t *data, size_t sz)
    {
      return do_record_read(data, sz, fields_mask, v);
    }

    inline value_type &
    results()
    {
      return *v;
    }

    inline const value_type &
    results() const
    {
      return *v;
    }

  private:
    value_type *v;
    uint64_t fields_mask;
  };

  class value_reader {
  public:
    constexpr value_reader(uint64_t fields_mask) : fields_mask(fields_mask) {}

    inline bool
    operator()(const uint8_t *data, size_t sz)
    {
      return do_record_read(data, sz, fields_mask, &v);
    }

    inline value_type &
    results()
    {
      return v;
    }

    inline const value_type &
    results() const
    {
      return v;
    }

  private:
    value_type v;
    uint64_t fields_mask;
  };

  //typedef typename super_type::key_reader bytes_key_reader;
  //typedef typename super_type::single_value_reader bytes_single_value_reader;
  //typedef typename super_type::value_reader bytes_value_reader;

public:

  struct search_range_callback {
  public:
    virtual ~search_range_callback() {}
    virtual bool invoke(const key_type &k, const value_type &v) = 0;
  };

  struct bytes_search_range_callback {
  public:
    virtual ~bytes_search_range_callback() {}
    virtual bool invoke(const string_type &k, const string_type &v) = 0;
  };

  typed_txn_btree(size_type value_size_hint = 128,
                  bool mostly_append = false,
                  const std::string &name = "<unknown>")
    : super_type(value_size_hint, mostly_append, name)
  {}

  template <typename Traits, typename StringAllocator>
  inline bool search(
      Transaction<Traits> &t, const key_type &k, value_type &v,
      StringAllocator &sa, uint64_t fields_mask = AllFieldsMask);

  template <typename Traits, typename StringAllocator>
  inline void search_range_call(
      Transaction<Traits> &t, const key_type &lower, const key_type *upper,
      search_range_callback &callback,
      StringAllocator &sa, bool no_key_results = false, /* skip decoding of keys? */
      uint64_t fields_mask = AllFieldsMask);

  // a lower-level variant which does not bother to decode the key/values
  template <typename Traits, typename StringAllocator>
  inline void bytes_search_range_call(
      Transaction<Traits> &t, const key_type &lower, const key_type *upper,
      bytes_search_range_callback &callback,
      StringAllocator &sa,
      size_type value_fields_prefix = std::numeric_limits<size_type>::max());

  /* save_columns currently ignored */
  template <typename Traits, typename StringAllocator>
  inline void put(
      Transaction<Traits> &t, const key_type &k, const value_type &v,
      StringAllocator &sa, uint64_t fields_mask = AllFieldsMask);

  template <typename Traits, typename StringAllocator>
  inline void insert(
      Transaction<Traits> &t, const key_type &k, const value_type &v,
      StringAllocator &sa);

  template <typename Traits, typename StringAllocator>
  inline void remove(
      Transaction<Traits> &t, const key_type &k,
      StringAllocator &sa);

private:
  key_encoder_type key_encoder;
  value_encoder_type value_encoder;
};

template <template <typename> class Transaction, typename Schema>
template <typename Traits, typename StringAllocator>
bool
typed_txn_btree<Transaction, Schema>::search(
    Transaction<Traits> &t, const key_type &k, value_type &v,
    StringAllocator &sa, uint64_t fields_mask)
{
  string_type * const kbuf = sa();
  INVARIANT(kbuf);
  key_encoder.write(*kbuf, &k);
  single_value_reader vr(v, fields_mask);
  return this->do_search(t, varkey(*kbuf), vr);
}

template <template <typename> class Transaction, typename Schema>
template <typename Traits, typename StringAllocator>
void
typed_txn_btree<Transaction, Schema>::search_range_call(
    Transaction<Traits> &t, const key_type &lower, const key_type *upper,
    search_range_callback &callback,
    StringAllocator &sa,
    bool no_key_results, uint64_t fields_mask)
{
  string_type * const lowerbuf = sa();
  string_type * const upperbuf = upper ? sa() : nullptr;
  key_encoder.write(*lowerbuf, &lower);
  if (upperbuf)
    key_encoder.write(*upperbuf, upper);
  const varkey upperbufvk(upperbuf ? varkey(*upperbuf) : varkey());
  key_reader kr(no_key_results);
  value_reader vr(fields_mask);
  this->do_search_range_call(
      t, varkey(*lowerbuf), upperbuf ? &upperbufvk : nullptr, callback, kr, vr, sa);
}

template <template <typename> class Transaction, typename Schema>
template <typename Traits, typename StringAllocator>
void
typed_txn_btree<Transaction, Schema>::bytes_search_range_call(
    Transaction<Traits> &t, const key_type &lower, const key_type *upper,
    bytes_search_range_callback &callback,
    StringAllocator &sa,
    size_type value_fields_prefix)
{
  string_type * const lowerbuf = sa();
  string_type * const upperbuf = upper ? sa() : nullptr;
  key_encoder.write(*lowerbuf, &lower);
  if (upperbuf)
    key_encoder.write(*upperbuf, upper);
  const varkey upperbufvk(upperbuf ? varkey(*upperbuf) : varkey());
  const size_t max_bytes_read = value_encoder.encode_max_nbytes_prefix(value_fields_prefix);
  typename super_type::key_reader kr;
  typename super_type::template value_reader<StringAllocator> vr(sa, max_bytes_read);
  this->do_search_range_call(
      t, varkey(*lowerbuf), upperbuf ? &upperbufvk : nullptr, callback, kr, vr, sa);
}

template <template <typename> class Transaction, typename Schema>
template <typename Traits, typename StringAllocator>
void
typed_txn_btree<Transaction, Schema>::put(
    Transaction<Traits> &t, const key_type &k, const value_type &v,
    StringAllocator &sa, uint64_t save_columns)
{
  string_type * const kbuf = sa();
  string_type * const vbuf = sa();
  INVARIANT(kbuf);
  INVARIANT(vbuf);
  key_encoder.write(*kbuf, &k);
  value_encoder.write(*vbuf, &v);
  this->do_tree_put(t, *kbuf, *vbuf, false);
}

template <template <typename> class Transaction, typename Schema>
template <typename Traits, typename StringAllocator>
void
typed_txn_btree<Transaction, Schema>::insert(
    Transaction<Traits> &t, const key_type &k, const value_type &v,
    StringAllocator &sa)
{
  string_type * const kbuf = sa();
  string_type * const vbuf = sa();
  INVARIANT(kbuf);
  INVARIANT(vbuf);
  key_encoder.write(*kbuf, &k);
  value_encoder.write(*vbuf, &v);
  this->do_tree_put(t, *kbuf, *vbuf, true);
}

template <template <typename> class Transaction, typename Schema>
template <typename Traits, typename StringAllocator>
void
typed_txn_btree<Transaction, Schema>::remove(
    Transaction<Traits> &t, const key_type &k,
    StringAllocator &sa)
{
  static const std::string s_empty;
  string_type * const kbuf = sa();
  INVARIANT(kbuf);
  key_encoder.write(*kbuf, &k);
  this->do_tree_put(t, *kbuf, s_empty, false);
}

#endif /* _NDB_TYPED_TXN_BTREE_H_ */
