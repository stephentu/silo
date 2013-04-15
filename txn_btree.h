#ifndef _NDB_TXN_BTREE_H_
#define _NDB_TXN_BTREE_H_

#include "base_txn_btree.h"

// XXX: hacky
extern void txn_btree_test();

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
class txn_btree : public base_txn_btree<Transaction> {
  typedef base_txn_btree<Transaction> super_type;
public:

  typedef typename super_type::key_type key_type;
  typedef typename super_type::string_type string_type;
  typedef typename super_type::value_type value_type;
  typedef typename super_type::size_type size_type;
  typedef typename super_type::default_string_allocator default_string_allocator;

  struct search_range_callback {
  public:
    virtual ~search_range_callback() {}
    virtual bool invoke(const string_type &k, const string_type &v) = 0;
  };

private:
  template <typename T>
  class type_callback_wrapper : public search_range_callback {
  public:
    type_callback_wrapper(T *callback) : callback(callback) {}
    virtual bool
    invoke(const string_type &k, const string_type &v)
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

public:

  txn_btree(size_type value_size_hint = 128,
            bool mostly_append = false,
            const std::string &name = "<unknown>")
    : super_type(value_size_hint, mostly_append, name)
  {}

  // either returns false or v is set to not-empty with value
  // precondition: max_bytes_read > 0
  template <typename Traits>
  inline bool
  search(Transaction<Traits> &t, const string_type &k, string_type &v,
         size_t max_bytes_read = string_type::npos)
  {
    return search(t, key_type(k), v, max_bytes_read);
  }

  template <typename Traits>
  inline bool
  search(Transaction<Traits> &t, const key_type &k, string_type &v,
         size_t max_bytes_read = string_type::npos);


  // StringAllocator needs to be CopyConstructable
  template <typename Traits,
            typename StringAllocator = default_string_allocator>
  inline void
  search_range_call(Transaction<Traits> &t,
                    const string_type &lower,
                    const string_type *upper,
                    search_range_callback &callback,
                    const StringAllocator &sa = StringAllocator())
  {
    key_type u;
    if (upper)
      u = key_type(*upper);
    search_range_call(t, key_type(lower), upper ? &u : nullptr, callback, sa);
  }

  template <typename Traits,
            typename StringAllocator = default_string_allocator>
  void
  search_range_call(Transaction<Traits> &t,
                    const key_type &lower,
                    const key_type *upper,
                    search_range_callback &callback,
                    const StringAllocator &sa = StringAllocator());

  template <typename Traits, typename T,
            typename StringAllocator = default_string_allocator>
  inline void
  search_range(
      Transaction<Traits> &t, const string_type &lower,
      const string_type *upper, T callback,
      const StringAllocator &sa = StringAllocator())
  {
    type_callback_wrapper<T> w(&callback);
    search_range_call(t, lower, upper, w, sa);
  }

  template <typename Traits, typename T,
            typename StringAllocator = default_string_allocator>
  inline void
  search_range(
      Transaction<Traits> &t, const key_type &lower,
      const key_type *upper, T callback,
      const StringAllocator &sa = StringAllocator())
  {
    type_callback_wrapper<T> w(&callback);
    search_range_call(t, lower, upper, w, sa);
  }

  template <typename Traits>
  inline void
  put(Transaction<Traits> &t, const string_type &k, const string_type &v)
  {
    INVARIANT(!v.empty());
    do_tree_put(t, k, v, false);
  }

  // XXX: other put variants

  template <typename Traits>
  inline void
  insert(Transaction<Traits> &t, const string_type &k, const string_type &v)
  {
    INVARIANT(!v.empty());
    do_tree_put(t, k, v, true);
  }

  // insert() methods below are for legacy use

  template <typename Traits,
            class = typename std::enable_if<!Traits::stable_input_memory>::type>
  inline void
  insert(Transaction<Traits> &t, const string_type &k, value_type v, size_type sz)
  {
    INVARIANT(v);
    INVARIANT(sz);
    do_tree_put(t, k, string_type((const char *) v, sz), true);
  }

  template <typename Traits,
            class = typename std::enable_if<!Traits::stable_input_memory>::type>
  inline void
  insert(Transaction<Traits> &t, const key_type &k, value_type v, size_type sz)
  {
    INVARIANT(v);
    INVARIANT(sz);
    do_tree_put(t, to_string_type(k), string_type((const char *) v, sz), true);
  }

  template <typename Traits, typename T>
  inline void
  insert_object(Transaction<Traits> &t, const key_type &k, const T &obj)
  {
    insert(t, k, (value_type) &obj, sizeof(obj));
  }

  template <typename Traits, typename T>
  inline void
  insert_object(Transaction<Traits> &t, const string_type &k, const T &obj)
  {
    insert(t, k, (value_type) &obj, sizeof(obj));
  }

  template <typename Traits>
  inline void
  remove(Transaction<Traits> &t, const string_type &k)
  {
    // use static empty string so stable_input_memory can take
    // address of the value
    static const std::string s_empty;
    do_tree_put(t, k, s_empty, false);
  }

  template <typename Traits,
            class = typename std::enable_if<!Traits::stable_input_memory>::type>
  inline void
  remove(Transaction<Traits> &t, const key_type &k)
  {
    do_tree_put(t, to_string_type(k), string_type(), false);
  }

  static void Test();

private:
  template <typename Traits, typename StringAllocator>
  struct txn_search_range_callback : public btree::low_level_search_range_callback {
    txn_search_range_callback(Transaction<Traits> *t,
                              search_range_callback *caller_callback,
                              const StringAllocator &sa)
      : t(t), caller_callback(caller_callback), sa(sa) {}
    virtual void on_resp_node(const btree::node_opaque_t *n, uint64_t version);
    virtual bool invoke(const btree::string_type &k, btree::value_type v,
                        const btree::node_opaque_t *n, uint64_t version);
    Transaction<Traits> *const t;
    search_range_callback *const caller_callback;
    StringAllocator sa;
    string_type temp_buf;
  };

  // remove() is just do_tree_put() with empty-string
  // expect_new indicates if we expect the record to not exist in the tree-
  // is just a hint that affects perf, not correctness
  template <typename Traits>
  void do_tree_put(Transaction<Traits> &t, const string_type &k,
                   const string_type &v, bool expect_new);
};

#endif /* _NDB_TXN_BTREE_H_ */
