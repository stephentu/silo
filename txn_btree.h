#ifndef _NDB_TXN_BTREE_H_
#define _NDB_TXN_BTREE_H_

#include "btree.h"
#include "txn.h"

#include <type_traits>

// XXX: hacky
extern void txn_btree_test();

// each Transaction implementation should specialize this for special
// behavior- the default implementation is just nops
template <template <typename> class Transaction>
struct txn_btree_handler {
  inline void on_construct(btree *underlying) {} // get a handle to the underying btree
  inline void on_destruct() {} // called at the beginning of the txn_btree's dtor
  static const bool has_background_task = false;
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
class txn_btree {

  // XXX: not ideal
  template <template <typename> class P, typename T>
    friend class transaction;

  // XXX: would like to declare friend wth all Transaction<T> classes, but
  // doesn't seem like an easy way to do that for template template parameters

public:
  typedef transaction_base::key_type key_type;
  typedef transaction_base::string_type string_type;
  typedef const uint8_t * value_type;
  typedef size_t size_type;

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
            bool mostly_append = false)
    : value_size_hint(value_size_hint),
      mostly_append(mostly_append),
      been_destructed(false)
  {
    handler.on_construct(&underlying_btree);
  }

  ~txn_btree()
  {
    if (!been_destructed)
      unsafe_purge(false);
  }

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

  struct default_string_allocator {
    inline ALWAYS_INLINE string_type *
    operator()()
    {
      return nullptr;
    }
    inline ALWAYS_INLINE void
    return_last(string_type *px)
    {
    }
  };

  // StringAllocator needs to be CopyConstructable
  template <typename Traits, typename StringAllocator = default_string_allocator>
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

  template <typename Traits, typename StringAllocator = default_string_allocator>
  void
  search_range_call(Transaction<Traits> &t,
                    const key_type &lower,
                    const key_type *upper,
                    search_range_callback &callback,
                    const StringAllocator &sa = StringAllocator());

  template <typename Traits, typename T, typename StringAllocator = default_string_allocator>
  inline void
  search_range(
      Transaction<Traits> &t, const string_type &lower,
      const string_type *upper, T callback,
      const StringAllocator &sa = StringAllocator())
  {
    type_callback_wrapper<T> w(&callback);
    search_range_call(t, lower, upper, w, sa);
  }

  template <typename Traits, typename T, typename StringAllocator = default_string_allocator>
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

  inline size_t
  size_estimate() const
  {
    return underlying_btree.size();
  }

  inline size_type
  get_value_size_hint() const
  {
    return value_size_hint;
  }

  inline void
  set_value_size_hint(size_type value_size_hint)
  {
    this->value_size_hint = value_size_hint;
  }

  inline bool
  is_mostly_append() const
  {
    return mostly_append;
  }

  inline void
  set_mostly_append(bool mostly_append)
  {
    this->mostly_append = mostly_append;
  }

  /**
   * only call when you are sure there are no concurrent modifications on the
   * tree. is neither threadsafe nor transactional
   *
   * Note that when you call unsafe_purge(), this txn_btree becomes
   * completely invalidated and un-usable. Any further operations
   * (other than calling the destructor) are undefined
   */
  void unsafe_purge(bool dump_stats = false);

  static void Test();

  // XXX: only exists because can't declare friend of template parameter
  // Transaction
  inline btree *
  get_underlying_btree()
  {
    return &underlying_btree;
  }

private:

  struct purge_tree_walker : public btree::tree_walk_callback {
    virtual void on_node_begin(const btree::node_opaque_t *n);
    virtual void on_node_success();
    virtual void on_node_failure();
#ifdef TXN_BTREE_DUMP_PURGE_STATS
    purge_tree_walker()
      : purge_stats_nodes(0),
        purge_stats_nosuffix_nodes(0) {}
    std::map<size_t, size_t> purge_stats_ln_record_size_counts; // just the record
    std::map<size_t, size_t> purge_stats_ln_alloc_size_counts; // includes overhead
    std::vector<uint16_t> purge_stats_nkeys_node;
    size_t purge_stats_nodes;
    size_t purge_stats_nosuffix_nodes;

    void
    dump_stats()
    {
      size_t v = 0;
      for (std::vector<uint16_t>::iterator it = purge_stats_nkeys_node.begin();
          it != purge_stats_nkeys_node.end(); ++it)
        v += *it;
      const double avg_nkeys_node = double(v)/double(purge_stats_nkeys_node.size());
      const double avg_fill_factor = avg_nkeys_node/double(btree::NKeysPerNode);
      std::cerr << "btree node stats" << std::endl;
      std::cerr << "    avg_nkeys_node: " << avg_nkeys_node << std::endl;
      std::cerr << "    avg_fill_factor: " << avg_fill_factor << std::endl;
      std::cerr << "    num_nodes: " << purge_stats_nodes << std::endl;
      std::cerr << "    num_nosuffix_nodes: " << purge_stats_nosuffix_nodes << std::endl;
      std::cerr << "record size stats (nbytes => count)" << std::endl;
      for (std::map<size_t, size_t>::iterator it = purge_stats_ln_record_size_counts.begin();
          it != purge_stats_ln_record_size_counts.end(); ++it)
        std::cerr << "    " << it->first << " => " << it->second << std::endl;
      std::cerr << "alloc size stats  (nbytes => count)" << std::endl;
      for (std::map<size_t, size_t>::iterator it = purge_stats_ln_alloc_size_counts.begin();
          it != purge_stats_ln_alloc_size_counts.end(); ++it)
        std::cerr << "    " << (it->first + sizeof(dbtuple)) << " => " << it->second << std::endl;

    }
#endif

  private:
    std::vector< std::pair<btree::value_type, bool> > spec_values;
  };

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

  btree underlying_btree;
  size_type value_size_hint;
  bool mostly_append;
  bool been_destructed;
  txn_btree_handler<Transaction> handler;
};

#endif /* _NDB_TXN_BTREE_H_ */
