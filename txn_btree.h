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
 */
class txn_btree {
  friend class transaction;
  friend class transaction_proto1;
  friend class transaction_proto2;
public:
  typedef btree::key_type key_type;
  typedef btree::value_type value_type;
  typedef btree::search_range_callback search_range_callback;

  bool search(transaction &t, const key_type &k, value_type &v);

  void
  search_range_call(transaction &t,
                    const key_type &lower,
                    const key_type *upper,
                    search_range_callback &callback);

  template <typename T>
  inline void
  search_range(transaction &t, const key_type &lower, const key_type *upper, T callback)
  {
    btree::type_callback_wrapper<T> w(&callback);
    search_range_call(t, lower, upper, w);
  }

  inline void
  insert(transaction &t, const key_type &k, value_type v)
  {
    INVARIANT(v);
    insert_impl(t, k, v);
  }

  inline void
  remove(transaction &t, const key_type &k)
  {
    insert_impl(t, k, NULL);
  }

  static void Test();

  inline size_t
  size_estimate() const
  {
    return underlying_btree.size();
  }

private:

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
    virtual bool invoke(const key_type &k, value_type v,
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

  struct absent_range_validation_callback : public search_range_callback {
    absent_range_validation_callback(transaction::txn_context *ctx,
                                     transaction::tid_t commit_tid)
      : ctx(ctx), commit_tid(commit_tid), failed_flag(false) {}
    inline bool failed() const { return failed_flag; }
    virtual bool invoke(const key_type &k, value_type v);
    transaction::txn_context *const ctx;
    const transaction::tid_t commit_tid;
    bool failed_flag;
  };

  // remove() is just insert_impl() with NULL value
  void insert_impl(transaction &t, const key_type &k, value_type v);

  btree underlying_btree;
};

#endif /* _NDB_TXN_BTREE_H_ */
