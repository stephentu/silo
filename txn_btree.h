#ifndef _NDB_TXN_BTREE_H_
#define _NDB_TXN_BTREE_H_

#include "btree.h"
#include "txn.h"

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
template <typename Transaction>
class txn_btree {
  friend class transaction<Transaction>;
  friend Transaction; // C++11 allows this

public:
  typedef typename Transaction::key_type key_type;
  typedef typename Transaction::string_type string_type;
  typedef const uint8_t * value_type;
  typedef size_t size_type;

  struct search_range_callback {
  public:
    virtual ~search_range_callback() {}
    virtual bool invoke(const string_type &k, value_type v, size_type sz) = 0;
  };

private:
  template <typename T>
  class type_callback_wrapper : public search_range_callback {
  public:
    type_callback_wrapper(T *callback) : callback(callback) {}
    virtual bool
    invoke(const string_type &k, value_type v, size_type sz)
    {
      return callback->operator()(k, v, sz);
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
      mostly_append(mostly_append)
  {}

  ~txn_btree()
  {
    unsafe_purge(false);
  }

  // either returns false or v is set to not-empty with value
  // precondition: max_bytes_read > 0
  bool search(Transaction &t, const string_type &k, string_type &v,
              size_t max_bytes_read = string_type::npos);

  // memory:
  // k - assumed to point to valid memory, *not* managed by btree
  // v - if k is found, points to a region of (immutable) memory of size sz which
  //     is guaranteed to be valid memory as long as transaction t is in scope
  inline bool
  search(Transaction &t, const key_type &k, string_type &v,
         size_t max_bytes_read = string_type::npos)
  {
    return search(t, to_string_type(k), v, max_bytes_read);
  }

  void
  search_range_call(Transaction &t,
                    const string_type &lower,
                    const string_type *upper,
                    search_range_callback &callback);

  inline void
  search_range_call(Transaction &t,
                    const key_type &lower,
                    const key_type *upper,
                    search_range_callback &callback)
  {
    string_type s;
    if (upper)
      s = to_string_type(*upper);
    search_range_call(t, to_string_type(lower), upper ? &s : NULL, callback);
  }

  template <typename T>
  inline void
  search_range(
      Transaction &t, const string_type &lower,
      const string_type *upper, T callback)
  {
    type_callback_wrapper<T> w(&callback);
    search_range_call(t, lower, upper, w);
  }

  template <typename T>
  inline void
  search_range(
      Transaction &t, const key_type &lower,
      const key_type *upper, T callback)
  {
    type_callback_wrapper<T> w(&callback);
    search_range_call(t, lower, upper, w);
  }

  inline void
  insert(Transaction &t, const string_type &k, const string_type &v)
  {
    INVARIANT(!v.empty());
    insert_impl(t, k, v);
  }

  inline void
  insert(Transaction &t, string_type &&k, string_type &&v)
  {
    INVARIANT(!v.empty());
    insert_impl(t, std::move(k), std::move(v));
  }

  // insert() methods below are for legacy use

  inline void
  insert(Transaction &t, const string_type &k, value_type v, size_type sz)
  {
    INVARIANT(v);
    INVARIANT(sz);
    insert_impl(t, k, string_type((const char *) v, sz));
  }

  inline void
  insert(Transaction &t, const key_type &k, value_type v, size_type sz)
  {
    INVARIANT(v);
    INVARIANT(sz);
    insert_impl(t, to_string_type(k), string_type((const char *) v, sz));
  }

  template <typename T>
  inline void
  insert_object(Transaction &t, const key_type &k, const T &obj)
  {
    insert(t, k, (value_type) &obj, sizeof(obj));
  }

  template <typename T>
  inline void
  insert_object(Transaction &t, const string_type &k, const T &obj)
  {
    insert(t, k, (value_type) &obj, sizeof(obj));
  }

  inline void
  remove(Transaction &t, const string_type &k)
  {
    insert_impl(t, k, "");
  }

  inline void
  remove(Transaction &t, string_type &&k)
  {
    insert_impl(t, std::move(k), "");
  }

  inline void
  remove(Transaction &t, const key_type &k)
  {
    insert_impl(t, to_string_type(k), "");
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
   */
  void unsafe_purge(bool dump_stats = false);

  static void Test();

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
#endif
  private:
    std::vector< std::pair<btree::value_type, bool> > spec_values;
  };

  struct txn_search_range_callback : public btree::low_level_search_range_callback {
    txn_search_range_callback(Transaction *t,
                              typename Transaction::txn_context *ctx,
                              const key_type &lower,
                              search_range_callback *caller_callback)
      : t(t), ctx(ctx), lower(lower), prev_key(),
        invoked(false), caller_callback(caller_callback),
        caller_stopped(false) {}
    virtual void on_resp_node(const btree::node_opaque_t *n, uint64_t version);
    virtual bool invoke(const btree::string_type &k, btree::value_type v,
                        const btree::node_opaque_t *n, uint64_t version);
    Transaction *const t;
    typename Transaction::txn_context *const ctx;
    const key_type lower;
    string_type prev_key;
    bool invoked;
    search_range_callback *const caller_callback;
    bool caller_stopped;
  };

  struct absent_range_validation_callback : public btree::search_range_callback {
    absent_range_validation_callback(typename Transaction::txn_context *ctx,
                                     transaction_base::tid_t commit_tid)
      : ctx(ctx), commit_tid(commit_tid), failed_flag(false) {}
    inline bool failed() const { return failed_flag; }
    virtual bool
    invoke(const btree::string_type &k, btree::value_type v)
    {
      dbtuple *ln = (dbtuple *) v;
      INVARIANT(ln);
      VERBOSE(std::cerr << "absent_range_validation_callback: key " << util::hexify(k)
          << " found dbtuple 0x" << util::hexify(ln) << std::endl);
      const bool did_write = ctx->write_set.find(k) != ctx->write_set.end();
      failed_flag = did_write ? !ln->latest_value_is_nil() : !ln->stable_latest_value_is_nil();
      if (failed_flag)
        VERBOSE(std::cerr << "absent_range_validation_callback: key " << util::hexify(k)
            << " found dbtuple 0x" << util::hexify(ln) << std::endl);
      return !failed_flag;
    }
    typename Transaction::txn_context *const ctx;
    const transaction_base::tid_t commit_tid;
    bool failed_flag;
  };

  // remove() is just insert_impl() with NULL value
  void insert_impl(Transaction &t, const string_type &k, const string_type &v);
  void insert_impl(Transaction &t, string_type &&k, string_type &&v);

  btree underlying_btree;
  size_type value_size_hint;
  bool mostly_append;
};

#endif /* _NDB_TXN_BTREE_H_ */
