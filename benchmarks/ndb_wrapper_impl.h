#ifndef _NDB_WRAPPER_IMPL_H_
#define _NDB_WRAPPER_IMPL_H_

#include <stdint.h>
#include "ndb_wrapper.h"
#include "../counter.h"
#include "../rcu.h"
#include "../varkey.h"
#include "../macros.h"
#include "../util.h"
#include "../scopedperf.hh"
#include "../txn.h"
#include "../txn_proto1_impl.h"
#include "../txn_proto2_impl.h"

template <typename T>
void *
ndb_wrapper<T>::new_txn(uint64_t txn_flags, void *buf)
{
  return new (buf) T(txn_flags);
}

template <typename T>
bool
ndb_wrapper<T>::commit_txn(void *txn)
{
  T * const t = (T *) txn;
  const bool ret = t->commit(); // won't throw
  t->~T();
  return ret;
}

template <typename T>
void
ndb_wrapper<T>::abort_txn(void *txn)
{
  T * const t = (T *) txn;
  t->abort();
  t->~T();
}

template <typename T>
void
ndb_wrapper<T>::print_txn_debug(void *txn) const
{
  ((T *) txn)->dump_debug_info();
}

template <typename T>
abstract_ordered_index *
ndb_wrapper<T>::open_index(const std::string &name, size_t value_size_hint, bool mostly_append)
{
  return new ndb_ordered_index<T>(name, value_size_hint, mostly_append);
}

template <typename T>
void
ndb_wrapper<T>::close_index(abstract_ordered_index *idx)
{
  delete idx;
}

template <typename T>
ndb_ordered_index<T>::ndb_ordered_index(const std::string &name, size_t value_size_hint, bool mostly_append)
  : name(name), btr()
{
  btr.set_value_size_hint(value_size_hint);
  btr.set_mostly_append(mostly_append);
}

//STATIC_COUNTER_DECL(scopedperf::tsc_ctr, ndb_get_probe0_tsc, ndb_get_probe0_cg);

template <typename T>
bool
ndb_ordered_index<T>::get(
    void *txn,
    const std::string &key,
    std::string &value, size_t max_bytes_read)
{
  //ANON_REGION("ndb_ordered_index::get:", &ndb_get_probe0_cg);
  try {
    if (!btr.search(*((T *) txn), key, value, max_bytes_read))
      return false;
    INVARIANT(!value.empty());
    return true;
  } catch (transaction_abort_exception &ex) {
    throw abstract_db::abstract_abort_exception();
  }
}

static event_counter evt_rec_inserts("record_inserts");

template <typename T>
const char *
ndb_ordered_index<T>::put(
    void *txn,
    const std::string &key,
    const std::string &value)
{
  try {
    btr.insert(*((T *) txn), key, value);
  } catch (transaction_abort_exception &ex) {
    throw abstract_db::abstract_abort_exception();
  }
  ++evt_rec_inserts;
  // XXX(stephentu): we currently can't return a stable pointer because we
  // don't even know if the txn will commit (and even if it does, the value
  // could get overwritten).  So that means our secondary index performance
  // will suffer for now
  return 0;
}

template <typename T>
const char *
ndb_ordered_index<T>::put(
    void *txn,
    std::string &&key,
    std::string &&value)
{
  try {
    btr.insert(*((T *) txn), move(key), move(value));
  } catch (transaction_abort_exception &ex) {
    throw abstract_db::abstract_abort_exception();
  }
  ++evt_rec_inserts;
  // XXX(stephentu): we currently can't return a stable pointer because we
  // don't even know if the txn will commit (and even if it does, the value
  // could get overwritten).  So that means our secondary index performance
  // will suffer for now
  return 0;
}

template <typename T>
class ndb_wrapper_search_range_callback : public txn_btree<T>::search_range_callback {
public:
  ndb_wrapper_search_range_callback(abstract_ordered_index::scan_callback &upcall)
    : upcall(&upcall) {}

  virtual bool
  invoke(const typename txn_btree<T>::string_type &k,
         const typename txn_btree<T>::value_type v,
         const typename txn_btree<T>::size_type sz)
  {
    const char * const key = (const char *) k.data();
    const size_t keylen = k.size();

    const char * const value = (const char *) v;
    const size_t valuelen = sz;

    return upcall->invoke(key, keylen, value, valuelen);
  }

private:
  abstract_ordered_index::scan_callback *upcall;
};

template <typename T>
void
ndb_ordered_index<T>::scan(
    void *txn,
    const std::string &start_key,
    const std::string *end_key,
    scan_callback &callback)
{
  T &t = *((T *) txn);
  ndb_wrapper_search_range_callback<T> c(callback);
  try {
    btr.search_range_call(t, start_key, end_key, c);
  } catch (transaction_abort_exception &ex) {
    throw abstract_db::abstract_abort_exception();
  }
}

template <typename T>
void
ndb_ordered_index<T>::remove(void *txn, const std::string &key)
{
  T &t = *((T *) txn);
  try {
    btr.remove(t, key);
  } catch (transaction_abort_exception &ex) {
    throw abstract_db::abstract_abort_exception();
  }
}

template <typename T>
void
ndb_ordered_index<T>::remove(void *txn, std::string &&key)
{
  T &t = *((T *) txn);
  try {
    btr.remove(t, move(key));
  } catch (transaction_abort_exception &ex) {
    throw abstract_db::abstract_abort_exception();
  }
}

template <typename T>
size_t
ndb_ordered_index<T>::size() const
{
  return btr.size_estimate();
}

template <typename T>
void
ndb_ordered_index<T>::clear()
{
#ifdef TXN_BTREE_DUMP_PURGE_STATS
  cerr << "purging txn index: " << name << endl;
#endif
  return btr.unsafe_purge(true);
}

#endif /* _NDB_WRAPPER_IMPL_H_ */
