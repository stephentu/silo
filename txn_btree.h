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
public:
  typedef btree::key_type key_type;
  typedef btree::value_type value_type;
  typedef btree::search_range_callback search_range_callback;

  bool search(transaction &t, key_type k, value_type &v);

  void
  search_range_call(transaction &t,
                    key_type lower,
                    key_type *upper,
                    search_range_callback &callback);

  template <typename T>
  inline void
  search_range(transaction &t, key_type lower, key_type *upper, T callback)
  {
    btree::type_callback_wrapper<T> w(&callback);
    search_range_call(t, lower, upper, w);
  }

  inline void
  insert(transaction &t, key_type k, value_type v)
  {
    assert(v);
    insert_impl(t, k, v);
  }

  inline void
  remove(transaction &t, key_type k)
  {
    insert_impl(t, k, NULL);
  }

  static void Test();

private:

  struct txn_search_range_callback : public search_range_callback {
    txn_search_range_callback(transaction *t,
                              key_type lower,
                              key_type *upper,
                              search_range_callback *caller_callback)
      : t(t), lower(lower), upper(upper), prev_key(),
        invoked(false), caller_callback(caller_callback),
        caller_stopped(false) {}
    virtual bool invoke(key_type k, value_type v);
    transaction *t;
    key_type lower;
    key_type *upper;
    key_type prev_key;
    bool invoked;
    search_range_callback *caller_callback;
    bool caller_stopped;
  };

  // remove() is just insert_impl() with NULL value
  void insert_impl(transaction  &t, key_type k, value_type v);

  btree underlying_btree;
};

#endif /* _NDB_TXN_BTREE_H_ */
