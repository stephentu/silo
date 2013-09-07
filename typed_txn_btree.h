#ifndef _NDB_TYPED_TXN_BTREE_H_
#define _NDB_TYPED_TXN_BTREE_H_

#include "base_txn_btree.h"
#include "txn_btree.h"
#include "record/cursor.h"

template <typename Schema>
struct typed_txn_btree_ {

  typedef typename Schema::base_type base_type;
  typedef typename Schema::key_type key_type;
  typedef typename Schema::value_type value_type;
  typedef typename Schema::value_descriptor_type value_descriptor_type;
  typedef typename Schema::key_encoder_type key_encoder_type;
  typedef typename Schema::value_encoder_type value_encoder_type;

  static_assert(value_descriptor_type::nfields() <= 64, "xx");
  static const uint64_t AllFieldsMask = (1UL << value_descriptor_type::nfields()) - 1;

  static inline constexpr bool
  IsAllFields(uint64_t m)
  {
    return (m & AllFieldsMask) == AllFieldsMask;
  }

  class key_reader {
  public:
    constexpr key_reader(bool no_key_results) : no_key_results(no_key_results) {}
    inline const key_type &
    operator()(const std::string &s)
    {
      const typename Schema::key_encoder_type key_encoder;
      if (!no_key_results)
        key_encoder.read(s, &k);
      return k;
    }
#if NDB_MASSTREE
    inline const key_type &
    operator()(lcdf::Str s)
    {
      const typename Schema::key_encoder_type key_encoder;
      if (!no_key_results)
        key_encoder.read(s, &k);
      return k;
    }
#endif
  private:
    key_type k;
    bool no_key_results;
  };

  static inline bool
  do_record_read(const uint8_t *data, size_t sz, uint64_t fields_mask, value_type *v)
  {
    if (IsAllFields(fields_mask)) {
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
    typedef typename Schema::value_type value_type;

    constexpr single_value_reader(value_type &v, uint64_t fields_mask)
      : v(&v), fields_mask(fields_mask) {}

    template <typename StringAllocator>
    inline bool
    operator()(const uint8_t *data, size_t sz, StringAllocator &sa)
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

    template <typename StringAllocator>
    inline void
    dup(const value_type &vdup, StringAllocator &sa)
    {
      *v = vdup;
    }

  private:
    value_type *v;
    uint64_t fields_mask;
  };

  class value_reader {
  public:
    typedef typename Schema::value_type value_type;

    constexpr value_reader(uint64_t fields_mask) : fields_mask(fields_mask) {}

    template <typename StringAllocator>
    inline bool
    operator()(const uint8_t *data, size_t sz, StringAllocator &sa)
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

    template <typename StringAllocator>
    inline void
    dup(const value_type &vdup, StringAllocator &sa)
    {
      v = vdup;
    }

  private:
    value_type v;
    uint64_t fields_mask;
  };

  class key_writer {
  public:
    constexpr key_writer(const key_type *k) : k(k) {}

    template <typename StringAllocator>
    inline const std::string *
    fully_materialize(bool stable_input, StringAllocator &sa)
    {
      if (!k)
        return nullptr;
      std::string * const ret = sa();
      const key_encoder_type key_encoder;
      key_encoder.write(*ret, k);
      return ret;
    }
  private:
    const key_type *k;
  };

  static inline size_t
  compute_needed_standalone(
    const value_type *v, uint64_t fields,
    const uint8_t *buf, size_t sz)
  {
    if (fields == 0) {
      // delete
      INVARIANT(!v);
      return 0;
    }
    INVARIANT(v);
    if (sz == 0) {
      // new record (insert)
      INVARIANT(IsAllFields(fields));
      const value_encoder_type value_encoder;
      return value_encoder.nbytes(v);
    }

    ssize_t new_updates_sum = 0;
    for (uint64_t i = 0; i < value_descriptor_type::nfields(); i++) {
      if ((1UL << i) & fields) {
        const uint8_t * px = reinterpret_cast<const uint8_t *>(v) +
          value_descriptor_type::cstruct_offsetof(i);
        new_updates_sum += value_descriptor_type::nbytes_fn(i)(px);
      }
    }

    // XXX: should try to cache pointers discovered by read_record_cursor
    ssize_t old_updates_sum = 0;
    read_record_cursor<base_type> rc(buf, sz);
    for (uint64_t i = 0; i < value_descriptor_type::nfields(); i++) {
      if ((1UL << i) & fields) {
        rc.skip_to(i);
        const size_t sz = rc.read_current_raw_size_and_advance();
        INVARIANT(sz);
        old_updates_sum += sz;
      }
    }

    // XXX: see if approximate version works almost as well (approx version is
    // to assume that each field has the minimum possible size, which is
    // overly conservative but correct)

    const ssize_t ret = static_cast<ssize_t>(sz) - old_updates_sum + new_updates_sum;
    INVARIANT(ret > 0);
    return ret;
  }

  // how many bytes do we need to encode a delta record
  static inline size_t
  compute_needed_delta_standalone(
      const value_type *v, uint64_t fields)
  {
    size_t size_needed = 0;
    size_needed += sizeof(uint64_t);
    if (fields == 0) {
      // delete
      INVARIANT(!v);
      return size_needed;
    }
    INVARIANT(v);
    if (IsAllFields(fields)) {
      // new record (insert)
      const value_encoder_type value_encoder;
      size_needed += value_encoder.nbytes(v);
      return size_needed;
    }

    for (uint64_t i = 0; i < value_descriptor_type::nfields(); i++) {
      if ((1UL << i) & fields) {
        const uint8_t * px = reinterpret_cast<const uint8_t *>(v) +
          value_descriptor_type::cstruct_offsetof(i);
        size_needed += value_descriptor_type::nbytes_fn(i)(px);
      }
    }

    return size_needed;
  }

  static inline void
  do_write_standalone(
      const value_type *v, uint64_t fields,
      uint8_t *buf, size_t sz)
  {
    if (fields == 0) {
      // no-op for delete
      INVARIANT(!v);
      return;
    }
    if (IsAllFields(fields)) {
      // special case, just use the standard encoder (faster)
      // because it's straight-line w/ no branching
      const value_encoder_type value_encoder;
      value_encoder.write(buf, v);
      return;
    }
    write_record_cursor<base_type> wc(buf);
    for (uint64_t i = 0; i < value_descriptor_type::nfields(); i++) {
      if ((1UL << i) & fields) {
        wc.skip_to(i);
        wc.write_current_and_advance(v, nullptr);
      }
    }
  }

  static inline void
  do_delta_write_standalone(
      const value_type *v, uint64_t fields,
      uint8_t *buf, size_t sz)
  {
    serializer<uint64_t, false> s_uint64_t;

#ifdef CHECK_INVARIANTS
    const uint8_t * const orig_buf = buf;
#endif

    buf = s_uint64_t.write(buf, fields);
    if (fields == 0) {
      // no-op for delete
      INVARIANT(!v);
      return;
    }
    if (IsAllFields(fields)) {
      // special case, just use the standard encoder (faster)
      // because it's straight-line w/ no branching
      const value_encoder_type value_encoder;
      value_encoder.write(buf, v);
      return;
    }
    for (uint64_t i = 0; i < value_descriptor_type::nfields(); i++) {
      if ((1UL << i) & fields) {
        const uint8_t * px = reinterpret_cast<const uint8_t *>(v) +
          value_descriptor_type::cstruct_offsetof(i);
        buf = value_descriptor_type::write_fn(i)(buf, px);
      }
    }

    INVARIANT(buf - orig_buf == ptrdiff_t(sz));
  }

  template <uint64_t Fields>
  static inline size_t
  tuple_writer(dbtuple::TupleWriterMode mode, const void *v, uint8_t *p, size_t sz)
  {
    const value_type *vx = reinterpret_cast<const value_type *>(v);
    switch (mode) {
    case dbtuple::TUPLE_WRITER_NEEDS_OLD_VALUE:
      return 1;
    case dbtuple::TUPLE_WRITER_COMPUTE_NEEDED:
      return compute_needed_standalone(vx, Fields, p, sz);
    case dbtuple::TUPLE_WRITER_COMPUTE_DELTA_NEEDED:
      return compute_needed_delta_standalone(vx, Fields);
    case dbtuple::TUPLE_WRITER_DO_WRITE:
      do_write_standalone(vx, Fields, p, sz);
      return 0;
    case dbtuple::TUPLE_WRITER_DO_DELTA_WRITE:
      do_delta_write_standalone(vx, Fields, p, sz);
      return 0;
    }
    ALWAYS_ASSERT(false);
    return 0;
  }

  class value_writer {
  public:
    constexpr value_writer(const value_type *v, uint64_t fields)
      : v(v), fields(fields) {}

    // old version of record is stored at
    // [buf, buf+sz).
    //
    // compute the new required size for the update
    inline size_t
    compute_needed(const uint8_t *buf, size_t sz)
    {
      return compute_needed_standalone(v, fields, buf, sz);
    }

    template <typename StringAllocator>
    inline const std::string *
    fully_materialize(bool stable_input, StringAllocator &sa)
    {
      INVARIANT(IsAllFields(fields) || fields == 0);
      if (fields == 0) {
        // delete
        INVARIANT(!v);
        return nullptr;
      }
      std::string * const ret = sa();
      const value_encoder_type value_encoder;
      value_encoder.write(*ret, v);
      return ret;
    }

    // the old value lives in [buf, buf+sz), but [buf, buf+compute_needed())
    // is valid memory to write to
    inline void
    operator()(uint8_t *buf, size_t sz)
    {
      do_write_standalone(v, fields, buf, sz);
    }

  private:
    const value_type *v;
    uint64_t fields;
  };

  typedef key_type Key;
  typedef key_writer KeyWriter;
  typedef value_type Value;
  typedef value_writer ValueWriter;
  typedef uint64_t ValueInfo;

  //typedef key_reader KeyReader;
  //typedef single_value_reader SingleValueReader;
  //typedef value_reader ValueReader;

};

template <template <typename> class Transaction, typename Schema>
class typed_txn_btree : public base_txn_btree<Transaction, typed_txn_btree_<Schema>> {
  typedef base_txn_btree<Transaction, typed_txn_btree_<Schema>> super_type;
public:

  typedef typename super_type::string_type string_type;
  typedef typename super_type::size_type size_type;

  typedef typename Schema::base_type base_type;
  typedef typename Schema::key_type key_type;
  typedef typename Schema::value_type value_type;
  typedef typename Schema::value_descriptor_type value_descriptor_type;
  typedef typename Schema::key_encoder_type key_encoder_type;
  typedef typename Schema::value_encoder_type value_encoder_type;

private:

  typedef txn_btree_::key_reader bytes_key_reader;
  typedef txn_btree_::single_value_reader bytes_single_value_reader;
  typedef txn_btree_::value_reader bytes_value_reader;

  typedef
    typename typed_txn_btree_<Schema>::key_writer
    key_writer;

  typedef
    typename typed_txn_btree_<Schema>::key_reader
    key_reader;
  typedef
    typename typed_txn_btree_<Schema>::single_value_reader
    single_value_reader;
  typedef
    typename typed_txn_btree_<Schema>::value_reader
    value_reader;

  template <typename Traits>
  static constexpr inline bool
  IsSupportable()
  {
    return Traits::stable_input_memory ||
      (private_::is_trivially_copyable<key_type>::value &&
       private_::is_trivially_destructible<key_type>::value &&
       private_::is_trivially_copyable<value_type>::value &&
       private_::is_trivially_destructible<value_type>::value);
  }

public:

  static const uint64_t AllFieldsMask = typed_txn_btree_<Schema>::AllFieldsMask;
  typedef util::Fields<AllFieldsMask> AllFields;

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

  template <typename Traits, typename FieldsMask = AllFields>
  inline bool search(
      Transaction<Traits> &t, const key_type &k, value_type &v,
      FieldsMask fm = FieldsMask());

  template <typename Traits, typename FieldsMask = AllFields>
  inline void search_range_call(
      Transaction<Traits> &t, const key_type &lower, const key_type *upper,
      search_range_callback &callback,
      bool no_key_results = false /* skip decoding of keys? */,
      FieldsMask fm = FieldsMask());

  // a lower-level variant which does not bother to decode the key/values
  template <typename Traits>
  inline void bytes_search_range_call(
      Transaction<Traits> &t, const key_type &lower, const key_type *upper,
      bytes_search_range_callback &callback,
      size_type value_fields_prefix = std::numeric_limits<size_type>::max());

  template <typename Traits, typename FieldsMask = AllFields>
  inline void put(
      Transaction<Traits> &t, const key_type &k, const value_type &v,
      FieldsMask fm = FieldsMask());

  template <typename Traits>
  inline void insert(
      Transaction<Traits> &t, const key_type &k, const value_type &v);

  template <typename Traits>
  inline void remove(
      Transaction<Traits> &t, const key_type &k);

private:

  template <typename Traits>
  static inline const std::string *
  stablize(Transaction<Traits> &t, const key_type &k)
  {
    key_writer writer(&k);
    return writer.fully_materialize(
        Traits::stable_input_memory, t.string_allocator());
  }

  template <typename Traits>
  static inline const value_type *
  stablize(Transaction<Traits> &t, const value_type &v)
  {
    if (Traits::stable_input_memory)
      return &v;
    std::string * const px = t.string_allocator()();
    px->assign(reinterpret_cast<const char *>(&v), sizeof(v));
    return reinterpret_cast<const value_type *>(px->data());
  }

  key_encoder_type key_encoder;
  value_encoder_type value_encoder;
};

template <template <typename> class Transaction, typename Schema>
template <typename Traits, typename FieldsMask>
bool
typed_txn_btree<Transaction, Schema>::search(
    Transaction<Traits> &t, const key_type &k, value_type &v,
    FieldsMask fm)
{
  // XXX: template single_value_reader with mask
  single_value_reader vr(v, FieldsMask::value);
  return this->do_search(t, k, vr);
}

template <template <typename> class Transaction, typename Schema>
template <typename Traits, typename FieldsMask>
void
typed_txn_btree<Transaction, Schema>::search_range_call(
    Transaction<Traits> &t,
    const key_type &lower, const key_type *upper,
    search_range_callback &callback,
    bool no_key_results,
    FieldsMask fm)
{
  key_reader kr(no_key_results);
  value_reader vr(FieldsMask::value);
  this->do_search_range_call(t, lower, upper, callback, kr, vr);
}

template <template <typename> class Transaction, typename Schema>
template <typename Traits>
void
typed_txn_btree<Transaction, Schema>::bytes_search_range_call(
    Transaction<Traits> &t, const key_type &lower, const key_type *upper,
    bytes_search_range_callback &callback,
    size_type value_fields_prefix)
{
  const value_encoder_type value_encoder;
  const size_t max_bytes_read =
    value_encoder.encode_max_nbytes_prefix(value_fields_prefix);
  bytes_key_reader kr;
  bytes_value_reader vr(max_bytes_read);
  this->do_search_range_call(t, lower, upper, callback, kr, vr);
}

template <template <typename> class Transaction, typename Schema>
template <typename Traits, typename FieldsMask>
void
typed_txn_btree<Transaction, Schema>::put(
    Transaction<Traits> &t, const key_type &k, const value_type &v, FieldsMask fm)
{
  static_assert(IsSupportable<Traits>(), "xx");
  const dbtuple::tuple_writer_t tw =
    &typed_txn_btree_<Schema>::template tuple_writer<FieldsMask::value>;
  this->do_tree_put(t, stablize(t, k), stablize(t, v), tw, false);
}

template <template <typename> class Transaction, typename Schema>
template <typename Traits>
void
typed_txn_btree<Transaction, Schema>::insert(
    Transaction<Traits> &t, const key_type &k, const value_type &v)
{
  static_assert(IsSupportable<Traits>(), "xx");
  const dbtuple::tuple_writer_t tw =
    &typed_txn_btree_<Schema>::template tuple_writer<AllFieldsMask>;
  this->do_tree_put(t, stablize(t, k), stablize(t, v), tw, true);
}

template <template <typename> class Transaction, typename Schema>
template <typename Traits>
void
typed_txn_btree<Transaction, Schema>::remove(
    Transaction<Traits> &t, const key_type &k)
{
  static_assert(IsSupportable<Traits>(), "xx");
  const dbtuple::tuple_writer_t tw =
    &typed_txn_btree_<Schema>::template tuple_writer<0>;
  this->do_tree_put(t, stablize(t, k), nullptr, tw, false);
}

#endif /* _NDB_TYPED_TXN_BTREE_H_ */
