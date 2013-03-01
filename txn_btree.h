#ifndef _NDB_TXN_BTREE_H_
#define _NDB_TXN_BTREE_H_

#include "btree.h"
#include "txn.h"

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
class txn_btree {
  friend class transaction;
  friend class transaction_proto1;
  friend class transaction_proto2;
public:
  typedef btree::key_type key_type;
  typedef const uint8_t * value_type;
  typedef size_t size_type;

  struct search_range_callback {
  public:
    virtual ~search_range_callback() {}
    virtual bool invoke(const key_type &k, value_type v, size_type sz) = 0;
  };

private:
  template <typename T>
  class type_callback_wrapper : public search_range_callback {
  public:
    type_callback_wrapper(T *callback) : callback(callback) {}
    virtual bool
    invoke(const key_type &k, value_type v, size_type sz)
    {
      return callback->operator()(k, v, sz);
    }
  private:
    T *const callback;
  };

public:

  txn_btree(size_type value_size_hint = 128)
    : value_size_hint(value_size_hint)
  {}

  ~txn_btree()
  {
    unsafe_purge(false);
  }

  // memory:
  // k - assumed to point to valid memory, *not* managed by btree
  // v - if k is found, points to a region of (immutable) memory of size sz which
  //     is guaranteed to be valid memory as long as transaction t is in scope
  bool search(transaction &t, const key_type &k, value_type &v, size_type &sz);

  void
  search_range_call(transaction &t,
                    const key_type &lower,
                    const key_type *upper,
                    search_range_callback &callback);

  // T must be copy-constructable
  template <typename T>
  inline void
  search_range(transaction &t, const key_type &lower, const key_type *upper, T callback)
  {
    type_callback_wrapper<T> w(&callback);
    search_range_call(t, lower, upper, w);
  }

  // memory:
  // v - assumed to point to valid memory of length sz. the txn_btree does
  //     *not* take ownership of v- as soon as insert() returns it is ok for
  //     v to be invalidated
  inline void
  insert(transaction &t, const key_type &k, value_type v, size_type sz)
  {
    INVARIANT(v);
    INVARIANT(sz);
    insert_impl(t, k, v, sz);
  }

  template <typename T>
  inline void
  insert_object(transaction &t, const key_type &k, const T &obj)
  {
    insert(t, k, (value_type) &obj, sizeof(obj));
  }

  inline void
  remove(transaction &t, const key_type &k)
  {
    insert_impl(t, k, NULL, 0);
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
    std::map<size_t, size_t> purge_stats_ln_record_size_counts; // just the record
    std::map<size_t, size_t> purge_stats_ln_alloc_size_counts; // includes overhead
#endif
  private:
    std::vector<btree::value_type> spec_values;
  };

  struct txn_search_range_callback : public btree::low_level_search_range_callback {
    txn_search_range_callback(transaction *t,
                              transaction::txn_context *ctx,
                              const key_type &lower,
                              const key_type *upper,
                              search_range_callback *caller_callback)
      : t(t), ctx(ctx), lower(lower), upper(upper), prev_key(),
        invoked(false), caller_callback(caller_callback),
        caller_stopped(false) {}
    virtual void on_resp_node(const btree::node_opaque_t *n, uint64_t version);
    virtual bool invoke(const key_type &k, btree::value_type v,
                        const btree::node_opaque_t *n, uint64_t version);
    transaction *const t;
    transaction::txn_context *const ctx;
    const key_type lower;
    const key_type *const upper;
    std::string prev_key;
    bool invoked;
    search_range_callback *const caller_callback;
    bool caller_stopped;
  };

  struct absent_range_validation_callback : public btree::search_range_callback {
    absent_range_validation_callback(transaction::txn_context *ctx,
                                     transaction::tid_t commit_tid)
      : ctx(ctx), commit_tid(commit_tid), failed_flag(false) {}
    inline bool failed() const { return failed_flag; }
    virtual bool invoke(const key_type &k, btree::value_type v);
    transaction::txn_context *const ctx;
    const transaction::tid_t commit_tid;
    bool failed_flag;
  };

  // remove() is just insert_impl() with NULL value
  void insert_impl(transaction &t, const key_type &k, value_type v, size_type sz);

  btree underlying_btree;
  size_type value_size_hint;
};

#endif /* _NDB_TXN_BTREE_H_ */
