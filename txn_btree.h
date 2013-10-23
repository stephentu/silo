#ifndef _NDB_TXN_BTREE_H_
#define _NDB_TXN_BTREE_H_

#include "base_txn_btree.h"

// XXX: hacky
extern void txn_btree_test();

struct txn_btree_ {
  class key_reader {
  public:
    inline ALWAYS_INLINE const std::string &
    operator()(const std::string &s)
    {
      return s;
    }
#if NDB_MASSTREE
    inline ALWAYS_INLINE lcdf::Str operator()(lcdf::Str s) {
      return s;
    }
#endif
  };

  class key_writer {
  public:
    constexpr key_writer(const std::string *k)
      : k(k) {}

    template <typename StringAllocator>
    inline const std::string *
    fully_materialize(bool stable_input, StringAllocator &sa)
    {
      if (stable_input || !k)
        return k;
      std::string * const ret = sa();
      ret->assign(k->data(), k->size());
      return ret;
    }

  private:
    const std::string *k;
  };

  // does not bother to interpret the bytes from a record
  class single_value_reader {
  public:
    typedef std::string value_type;

    constexpr single_value_reader(std::string *px, size_t max_bytes_read)
      : px(px), max_bytes_read(max_bytes_read) {}

    template <typename StringAllocator>
    inline bool
    operator()(const uint8_t *data, size_t sz, StringAllocator &sa)
    {
      const size_t readsz = std::min(sz, max_bytes_read);
      px->assign((const char *) data, readsz);
      return true;
    }

    inline std::string &
    results()
    {
      return *px;
    }

    inline const std::string &
    results() const
    {
      return *px;
    }

    template <typename StringAllocator>
    inline void
    dup(const std::string &vdup, StringAllocator &sa)
    {
      *px = vdup;
    }

  private:
    std::string *px;
    size_t max_bytes_read;
  };

  class value_reader {
  public:
    typedef std::string value_type;

    constexpr value_reader(size_t max_bytes_read)
      : px(nullptr), max_bytes_read(max_bytes_read) {}

    template <typename StringAllocator>
    inline bool
    operator()(const uint8_t *data, size_t sz, StringAllocator &sa)
    {
      px = sa();
      const size_t readsz = std::min(sz, max_bytes_read);
      px->assign((const char *) data, readsz);
      return true;
    }

    inline std::string &
    results()
    {
      return *px;
    }

    inline const std::string &
    results() const
    {
      return *px;
    }

    template <typename StringAllocator>
    inline void
    dup(const std::string &vdup, StringAllocator &sa)
    {
      px = sa();
      *px = vdup;
    }

  private:
    std::string *px;
    size_t max_bytes_read;
  };

  class value_writer {
  public:
    constexpr value_writer(const std::string *v) : v(v) {}
    inline size_t
    compute_needed(const uint8_t *buf, size_t sz)
    {
      return v ? v->size() : 0;
    }
    template <typename StringAllocator>
    inline const std::string *
    fully_materialize(bool stable_input, StringAllocator &sa)
    {
      if (stable_input || !v)
        return v;
      std::string * const ret = sa();
      ret->assign(v->data(), v->size());
      return ret;
    }

    // input [buf, buf+sz) is old value
    inline void
    operator()(uint8_t *buf, size_t sz)
    {
      if (!v)
        return;
      NDB_MEMCPY(buf, v->data(), v->size());
    }
  private:
    const std::string *v;
  };

  static size_t
  tuple_writer(dbtuple::TupleWriterMode mode, const void *v, uint8_t *p, size_t sz)
  {
    const std::string * const vx = reinterpret_cast<const std::string *>(v);
    switch (mode) {
    case dbtuple::TUPLE_WRITER_NEEDS_OLD_VALUE:
      return 0;
    case dbtuple::TUPLE_WRITER_COMPUTE_NEEDED:
    case dbtuple::TUPLE_WRITER_COMPUTE_DELTA_NEEDED:
      return vx->size();
    case dbtuple::TUPLE_WRITER_DO_WRITE:
    case dbtuple::TUPLE_WRITER_DO_DELTA_WRITE:
      NDB_MEMCPY(p, vx->data(), vx->size());
      return 0;
    }
    ALWAYS_ASSERT(false);
    return 0;
  }

  typedef std::string Key;
  typedef key_reader KeyReader;
  typedef key_writer KeyWriter;
  typedef std::string Value;
  typedef single_value_reader SingleValueReader;
  typedef value_reader ValueReader;
  typedef value_writer ValueWriter;
};

/**
 * This class implements a serializable, multi-version b-tree
 *
 * It presents mostly same interface as the underlying concurrent b-tree,
 * but the interface is serializable. The main interface differences are,
 * insert() and put() do not return a boolean value to indicate whether or not
 * they caused the tree to mutate
 *
 * A txn_btree does not allow keys to map to NULL records, even though the
 * underlying concurrent btree does- this simplifies some of the book-keeping
 * Additionally, keys cannot map to zero length records.
 *
 * Note that the txn_btree *manages* the memory of both keys and values internally.
 * See the specific notes on search()/insert() about memory ownership
 */
template <template <typename> class Transaction>
class txn_btree : public base_txn_btree<Transaction, txn_btree_> {
  typedef base_txn_btree<Transaction, txn_btree_> super_type;
public:

  //template <typename Traits>
  //struct transaction {
  //  typedef Transaction<txn_btree_, Traits> type;
  //};

  //template <typename Traits>
  //  using transaction = Transaction<txn_btree_, Traits>;

  typedef typename super_type::string_type string_type;
  typedef typename super_type::keystring_type keystring_type;
  typedef typename super_type::size_type size_type;

  typedef txn_btree_::Key key_type;
  typedef txn_btree_::Value value_type;
  typedef txn_btree_::KeyReader key_reader_type;
  typedef txn_btree_::KeyWriter key_writer_type;
  typedef txn_btree_::SingleValueReader single_value_reader_type;
  typedef txn_btree_::ValueReader value_reader_type;
  typedef txn_btree_::ValueWriter value_writer_type;

  struct search_range_callback {
  public:
    virtual ~search_range_callback() {}
    virtual bool invoke(const keystring_type &k, const string_type &v) = 0;
  };

private:

  template <typename T>
  class type_callback_wrapper : public search_range_callback {
  public:
    constexpr type_callback_wrapper(T *callback)
      : callback(callback) {}
    virtual bool
    invoke(const keystring_type &k, const string_type &v)
    {
      return callback->operator()(k, v);
    }
  private:
    T *const callback;
  };

  static inline ALWAYS_INLINE string_type
  to_string_type(const varkey &k)
  {
    return string_type((const char *) k.data(), k.size());
  }

  template <typename Traits>
  static inline const std::string *
  stablize(Transaction<Traits> &t, const std::string &s)
  {
    if (Traits::stable_input_memory)
      return &s;
    std::string * const px = t.string_allocator()();
    *px = s;
    return px;
  }

  template <typename Traits>
  static inline const std::string *
  stablize(Transaction<Traits> &t, const uint8_t *p, size_t sz)
  {
    if (!sz)
      return nullptr;
    std::string * const px = t.string_allocator()();
    px->assign((const char *) p, sz);
    return px;
  }

  template <typename Traits>
  static inline const std::string *
  stablize(Transaction<Traits> &t, const varkey &k)
  {
    return stablize(t, k.data(), k.size());
  }

public:

  txn_btree(size_type value_size_hint = 128,
            bool mostly_append = false,
            const std::string &name = "<unknown>")
    : super_type(value_size_hint, mostly_append, name)
  {}

  template <typename Traits>
  inline bool
  search(Transaction<Traits> &t,
         const varkey &k,
         value_type &v,
         size_t max_bytes_read = string_type::npos)
  {
    return search(t, to_string_type(k), v, max_bytes_read);
  }

  // either returns false or v is set to not-empty with value
  // precondition: max_bytes_read > 0
  template <typename Traits>
  inline bool
  search(Transaction<Traits> &t,
         const key_type &k,
         value_type &v,
         size_type max_bytes_read = string_type::npos)
  {
    single_value_reader_type r(&v, max_bytes_read);
    return this->do_search(t, k, r);
  }

  template <typename Traits>
  inline void
  search_range_call(Transaction<Traits> &t,
                    const key_type &lower,
                    const key_type *upper,
                    search_range_callback &callback,
                    size_type max_bytes_read = string_type::npos)
  {
    key_reader_type kr;
    value_reader_type vr(max_bytes_read);
    this->do_search_range_call(t, lower, upper, callback, kr, vr);
  }

  template <typename Traits>
  inline void
  rsearch_range_call(Transaction<Traits> &t,
                     const key_type &upper,
                     const key_type *lower,
                     search_range_callback &callback,
                     size_type max_bytes_read = string_type::npos)
  {
    key_reader_type kr;
    value_reader_type vr(max_bytes_read);
    this->do_rsearch_range_call(t, upper, lower, callback, kr, vr);
  }

  template <typename Traits>
  inline void
  search_range_call(Transaction<Traits> &t,
                    const varkey &lower,
                    const varkey *upper,
                    search_range_callback &callback,
                    size_type max_bytes_read = string_type::npos)
  {
    key_type u;
    if (upper)
      u = to_string_type(*upper);
    search_range_call(t, to_string_type(lower),
        upper ? &u : nullptr, callback, max_bytes_read);
  }

  template <typename Traits>
  inline void
  rsearch_range_call(Transaction<Traits> &t,
                     const varkey &upper,
                     const varkey *lower,
                     search_range_callback &callback,
                     size_type max_bytes_read = string_type::npos)
  {
    key_type l;
    if (lower)
      l = to_string_type(*lower);
    rsearch_range_call(t, to_string_type(upper),
        lower ? &l : nullptr, callback, max_bytes_read);
  }

  template <typename Traits, typename T>
  inline void
  search_range(Transaction<Traits> &t,
               const key_type &lower,
               const key_type *upper,
               T &callback,
               size_type max_bytes_read = string_type::npos)
  {
    type_callback_wrapper<T> w(&callback);
    search_range_call(t, lower, upper, w, max_bytes_read);
  }

  template <typename Traits, typename T>
  inline void
  search_range(Transaction<Traits> &t,
               const varkey &lower,
               const varkey *upper,
               T &callback,
               size_type max_bytes_read = string_type::npos)
  {
    key_type u;
    if (upper)
      u = to_string_type(*upper);
    search_range(t, to_string_type(lower),
        upper ? &u : nullptr, callback, max_bytes_read);
  }

  template <typename Traits>
  inline void
  put(Transaction<Traits> &t, const key_type &k, const value_type &v)
  {
    INVARIANT(!v.empty());
    this->do_tree_put(
        t, stablize(t, k), stablize(t, v),
        txn_btree_::tuple_writer, false);
  }

  template <typename Traits>
  inline void
  put(Transaction<Traits> &t, const varkey &k, const value_type &v)
  {
    INVARIANT(!v.empty());
    this->do_tree_put(
        t, stablize(t, k), stablize(t, v),
        txn_btree_::tuple_writer, false);
  }

  template <typename Traits>
  inline void
  insert(Transaction<Traits> &t, const key_type &k, const value_type &v)
  {
    INVARIANT(!v.empty());
    this->do_tree_put(
        t, stablize(t, k), stablize(t, v),
        txn_btree_::tuple_writer, true);
  }

  // insert() methods below are for legacy use

  template <typename Traits>
  inline void
  insert(Transaction<Traits> &t, const key_type &k, const uint8_t *v, size_type sz)
  {
    INVARIANT(v);
    INVARIANT(sz);
    this->do_tree_put(
        t, stablize(t, k), stablize(t, v, sz),
        txn_btree_::tuple_writer, true);
  }

  template <typename Traits>
  inline void
  insert(Transaction<Traits> &t, const varkey &k, const uint8_t *v, size_type sz)
  {
    INVARIANT(v);
    INVARIANT(sz);
    this->do_tree_put(
        t, stablize(t, k), stablize(t, v, sz),
        txn_btree_::tuple_writer, true);
  }

  template <typename Traits, typename T>
  inline void
  insert_object(Transaction<Traits> &t, const varkey &k, const T &obj)
  {
    insert(t, k, (const uint8_t *) &obj, sizeof(obj));
  }

  template <typename Traits, typename T>
  inline void
  insert_object(Transaction<Traits> &t, const key_type &k, const T &obj)
  {
    insert(t, k, (const uint8_t *) &obj, sizeof(obj));
  }

  template <typename Traits>
  inline void
  remove(Transaction<Traits> &t, const key_type &k)
  {
    this->do_tree_put(t, stablize(t, k), nullptr, txn_btree_::tuple_writer, false);
  }

  template <typename Traits>
  inline void
  remove(Transaction<Traits> &t, const varkey &k)
  {
    this->do_tree_put(t, stablize(t, k), nullptr, txn_btree_::tuple_writer, false);
  }

  static void Test();

};

#endif /* _NDB_TXN_BTREE_H_ */
