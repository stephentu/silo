#ifndef _NDB_TYPED_TXN_BTREE_H_
#define _NDB_TYPED_TXN_BTREE_H_

#include "base_txn_btree.h"

template <template <typename> class Transaction, typename Schema>
class typed_txn_btree : public base_txn_btree<Transaction> {
  typedef base_txn_btree<Transaction> super_type;
public:

  typedef typename super_type::string_type string_type;
  typedef typename super_type::size_type size_type;
  typedef typename super_type::default_string_allocator default_string_allocator;

  typedef typename Schema::key_type key_type;
  typedef typename Schema::value_type value_type;
  typedef typename Schema::key_encoder_type key_encoder_type;
  typedef typename Schema::value_encoder_type value_encoder_type;

  struct search_range_callback {
  public:
    virtual ~search_range_callback() {}
    virtual bool invoke(const key_type &k, const value_type &v) = 0;
  };

  typed_txn_btree(size_type value_size_hint = 128,
                  bool mostly_append = false,
                  const std::string &name = "<unknown>")
    : super_type(value_size_hint, mostly_append, name)
  {}

  template <typename Traits, typename StringAllocator>
  inline bool search(
      Transaction<Traits> &t, const key_type &k, value_type &v,
      StringAllocator &sa,
      size_type fetch_prefix = std::numeric_limits<size_type>::max());

  template <typename Traits, typename StringAllocator>
  inline void search_range_call(
      Transaction<Traits> &t, const key_type &lower, const key_type *upper,
      search_range_callback &callback, StringAllocator &sa,
      size_type fetch_prefix = std::numeric_limits<size_type>::max());

  /* save_columns currently ignored */
  template <typename Traits, typename StringAllocator>
  inline void put(
      Transaction<Traits> &t, const key_type &k, const value_type &v,
      StringAllocator &sa,
      size_type save_columns = std::numeric_limits<size_type>::max());

  template <typename Traits, typename StringAllocator>
  inline void insert(
      Transaction<Traits> &t, const key_type &k, const value_type &v,
      StringAllocator &sa);

  template <typename Traits, typename StringAllocator>
  inline void remove(
      Transaction<Traits> &t, const key_type &k,
      StringAllocator &sa);

private:

  struct wrapper : public super_type::search_range_callback {
    wrapper(search_range_callback &caller_callback)
      : caller_callback(&caller_callback) {}
    virtual bool
    invoke(const typename super_type::string_type &k,
           const typename super_type::string_type &v)
    {
      key_type key;
      value_type value;
      key_encoder.read(k, &key);
      value_encoder.read(v, &value);
      return caller_callback(key, value);
    }
  private:
    search_range_callback *const caller_callback;
    key_encoder_type key_encoder;
    value_encoder_type value_encoder;
  };

  key_encoder_type key_encoder;
  value_encoder_type value_encoder;
};

template <template <typename> class Transaction, typename Schema>
template <typename Traits, typename StringAllocator>
bool
typed_txn_btree<Transaction, Schema>::search(
    Transaction<Traits> &t, const key_type &k, value_type &v,
    StringAllocator &sa, size_type fetch_prefix)
{
  string_type *kbuf = sa();
  string_type *vbuf = sa();
  INVARIANT(kbuf);
  INVARIANT(vbuf);
  key_encoder.write(*kbuf, &k);
  // compute max_bytes_read
  const size_t max_bytes_read = key_encoder.encode_max_nbytes_prefix(fetch_prefix);
  if (!this->do_search(t, varkey(*kbuf), *vbuf, max_bytes_read))
    return false;
  value_encoder.read(vbuf->data(), &v);
  return true;
}

template <template <typename> class Transaction, typename Schema>
template <typename Traits, typename StringAllocator>
void
typed_txn_btree<Transaction, Schema>::search_range_call(
    Transaction<Traits> &t, const key_type &lower, const key_type *upper,
    search_range_callback &callback, StringAllocator &sa,
    size_type fetch_prefix)
{
  string_type *lowerbuf = sa();
  string_type *upperbuf = upper ? sa() : nullptr;
  key_encoder.write(*lowerbuf, &lower);
  if (upperbuf)
    key_encoder.write(*upperbuf, upper);
  wrapper cb(callback);
  this->do_search_range_call(t, *lowerbuf, upperbuf, cb, sa, fetch_prefix);
}

template <template <typename> class Transaction, typename Schema>
template <typename Traits, typename StringAllocator>
void
typed_txn_btree<Transaction, Schema>::put(
    Transaction<Traits> &t, const key_type &k, const value_type &v,
    StringAllocator &sa, size_type save_columns)
{
  string_type *kbuf = sa();
  string_type *vbuf = sa();
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
  string_type *kbuf = sa();
  string_type *vbuf = sa();
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
  string_type *kbuf = sa();
  INVARIANT(kbuf);
  key_encoder.write(*kbuf, &k);
  this->do_tree_put(t, *kbuf, s_empty, false);
}

#endif /* _NDB_TYPED_TXN_BTREE_H_ */
