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
//#include "../txn_proto1_impl.h"
#include "../txn_proto2_impl.h"

//struct default_transaction_traits {
//  static const size_t read_set_expected_size = SMALL_SIZE_MAP;
//  static const size_t absent_set_expected_size = EXTRA_SMALL_SIZE_MAP;
//  static const size_t write_set_expected_size = SMALL_SIZE_MAP;
//  static const bool stable_input_memory = false;
//  static const bool hard_expected_sizes = false;
//  static const bool read_own_writes = true;
//};

// ycsb profiles

struct hint_kv_get_put_traits {
  static const size_t read_set_expected_size = 1;
  static const size_t write_set_expected_size = 1;
  static const size_t absent_set_expected_size = 1;
  static const bool stable_input_memory = true;
  static const bool hard_expected_sizes = true;
  static const bool read_own_writes = false;
};

struct hint_kv_rmw_traits : public hint_kv_get_put_traits {};

struct hint_kv_scan_traits {
  static const size_t read_set_expected_size = 100;
  static const size_t write_set_expected_size = 1;
  static const size_t absent_set_expected_size = read_set_expected_size / 7 + 1;
  static const bool stable_input_memory = true;
  static const bool hard_expected_sizes = false;
  static const bool read_own_writes = false;
};

// tpcc profiles

struct hint_read_only_traits {
  static const size_t read_set_expected_size = 1;
  static const size_t write_set_expected_size = 1;
  static const size_t absent_set_expected_size = 1;
  static const bool stable_input_memory = true;
  static const bool hard_expected_sizes = true;
  static const bool read_own_writes = false;
};

struct hint_tpcc_new_order_traits {
  static const size_t read_set_expected_size = 35;
  static const size_t write_set_expected_size = 35;
  static const size_t absent_set_expected_size = 1;
  static const bool stable_input_memory = true;
  static const bool hard_expected_sizes = true;
  static const bool read_own_writes = false;
};

struct hint_tpcc_payment_traits {
  static const size_t read_set_expected_size = 85;
  static const size_t write_set_expected_size = 10;
  static const size_t absent_set_expected_size = 15;
  static const bool stable_input_memory = true;
  static const bool hard_expected_sizes = false;
  static const bool read_own_writes = false;
};

struct hint_tpcc_delivery_traits {
  static const size_t read_set_expected_size = 175;
  static const size_t write_set_expected_size = 175;
  static const size_t absent_set_expected_size = 35;
  static const bool stable_input_memory = true;
  static const bool hard_expected_sizes = false;
  static const bool read_own_writes = false;
};

struct hint_tpcc_order_status_traits {
  static const size_t read_set_expected_size = 95;
  static const size_t write_set_expected_size = 1;
  static const size_t absent_set_expected_size = 25;
  static const bool stable_input_memory = true;
  static const bool hard_expected_sizes = false;
  static const bool read_own_writes = false;
};

struct hint_tpcc_order_status_read_only_traits : public hint_read_only_traits {};

struct hint_tpcc_stock_level_traits {
  static const size_t read_set_expected_size = 500;
  static const size_t write_set_expected_size = 1;
  static const size_t absent_set_expected_size = 25;
  static const bool stable_input_memory = true;
  static const bool hard_expected_sizes = false;
  static const bool read_own_writes = false;
};

struct hint_tpcc_stock_level_read_only_traits : public hint_read_only_traits {};

#define TXN_PROFILE_HINT_OP(x) \
  x(abstract_db::HINT_DEFAULT, default_transaction_traits) \
  x(abstract_db::HINT_KV_GET_PUT, hint_kv_get_put_traits) \
  x(abstract_db::HINT_KV_RMW, hint_kv_rmw_traits) \
  x(abstract_db::HINT_KV_SCAN, hint_kv_scan_traits) \
  x(abstract_db::HINT_TPCC_NEW_ORDER, hint_tpcc_new_order_traits) \
  x(abstract_db::HINT_TPCC_PAYMENT, hint_tpcc_payment_traits) \
  x(abstract_db::HINT_TPCC_DELIVERY, hint_tpcc_delivery_traits) \
  x(abstract_db::HINT_TPCC_ORDER_STATUS, hint_tpcc_order_status_traits) \
  x(abstract_db::HINT_TPCC_ORDER_STATUS_READ_ONLY, hint_tpcc_order_status_read_only_traits) \
  x(abstract_db::HINT_TPCC_STOCK_LEVEL, hint_tpcc_stock_level_traits) \
  x(abstract_db::HINT_TPCC_STOCK_LEVEL_READ_ONLY, hint_tpcc_stock_level_read_only_traits)

template <template <typename> class Transaction>
size_t
ndb_wrapper<Transaction>::sizeof_txn_object(uint64_t txn_flags) const
{
#define MY_OP_X(a, b) sizeof(Transaction< b >),
  const size_t xs[] = {
    TXN_PROFILE_HINT_OP(MY_OP_X)
  };
#undef MY_OP_X
  size_t xmax = 0;
  for (size_t i = 0; i < ARRAY_NELEMS(xs); i++)
    xmax = std::max(xmax, xs[i]);
  return xmax;
}

template <template <typename> class Transaction>
void *
ndb_wrapper<Transaction>::new_txn(uint64_t txn_flags, void *buf, TxnProfileHint hint)
{
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(buf);
  p->hint = hint;
#define MY_OP_X(a, b) \
  case a: \
    new (&p->buf[0]) Transaction< b >(txn_flags); \
    return p;
  switch (hint) {
    TXN_PROFILE_HINT_OP(MY_OP_X)
  default:
    ALWAYS_ASSERT(false);
  }
#undef MY_OP_X
  return 0;
}

template <typename T>
static inline ALWAYS_INLINE void
Destroy(T *t)
{
  PERF_DECL(static std::string probe1_name(std::string(__PRETTY_FUNCTION__) + std::string(":total:")));
  ANON_REGION(probe1_name.c_str(), &private_::ndb_dtor_probe0_cg);
  t->~T();
}

template <template <typename> class Transaction>
bool
ndb_wrapper<Transaction>::commit_txn(void *txn)
{
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(txn);
#define MY_OP_X(a, b) \
  case a: \
    { \
      auto t = cast< b >()(p); \
      const bool ret = t->commit(); \
      Destroy(t); \
      return ret; \
    }
  switch (p->hint) {
    TXN_PROFILE_HINT_OP(MY_OP_X)
  default:
    ALWAYS_ASSERT(false);
  }
#undef MY_OP_X
  return false;
}

template <template <typename> class Transaction>
void
ndb_wrapper<Transaction>::abort_txn(void *txn)
{
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(txn);
#define MY_OP_X(a, b) \
  case a: \
    { \
      auto t = cast< b >()(p); \
      t->abort(); \
      Destroy(t); \
      return; \
    }
  switch (p->hint) {
    TXN_PROFILE_HINT_OP(MY_OP_X)
  default:
    ALWAYS_ASSERT(false);
  }
#undef MY_OP_X
}

template <template <typename> class Transaction>
void
ndb_wrapper<Transaction>::print_txn_debug(void *txn) const
{
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(txn);
#define MY_OP_X(a, b) \
  case a: \
    { \
      auto t = cast< b >()(p); \
      t->dump_debug_info(); \
      return; \
    }
  switch (p->hint) {
    TXN_PROFILE_HINT_OP(MY_OP_X)
  default:
    ALWAYS_ASSERT(false);
  }
#undef MY_OP_X
}

template <template <typename> class Transaction>
std::map<std::string, uint64_t>
ndb_wrapper<Transaction>::get_txn_counters(void *txn) const
{
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(txn);
#define MY_OP_X(a, b) \
  case a: \
    { \
      auto t = cast< b >()(p); \
      return t->get_txn_counters(); \
    }
  switch (p->hint) {
    TXN_PROFILE_HINT_OP(MY_OP_X)
  default:
    ALWAYS_ASSERT(false);
  }
#undef MY_OP_X
  return std::map<std::string, uint64_t>();
}

template <template <typename> class Transaction>
abstract_ordered_index *
ndb_wrapper<Transaction>::open_index(const std::string &name, size_t value_size_hint, bool mostly_append)
{
  return new ndb_ordered_index<Transaction>(name, value_size_hint, mostly_append);
}

template <template <typename> class Transaction>
void
ndb_wrapper<Transaction>::close_index(abstract_ordered_index *idx)
{
  delete idx;
}

template <template <typename> class Transaction>
ndb_ordered_index<Transaction>::ndb_ordered_index(const std::string &name, size_t value_size_hint, bool mostly_append)
  : name(name), btr()
{
  btr.set_value_size_hint(value_size_hint);
  btr.set_mostly_append(mostly_append);
}

template <template <typename> class Transaction>
bool
ndb_ordered_index<Transaction>::get(
    void *txn,
    const std::string &key,
    std::string &value, size_t max_bytes_read)
{
  PERF_DECL(static std::string probe1_name(std::string(__PRETTY_FUNCTION__) + std::string(":total:")));
  ANON_REGION(probe1_name.c_str(), &private_::ndb_get_probe0_cg);
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(txn);
  try {
#define MY_OP_X(a, b) \
  case a: \
    { \
      auto t = cast< b >()(p); \
      if (!btr.search(*t, key, value, max_bytes_read)) \
        return false; \
      return true; \
    }
    switch (p->hint) {
      TXN_PROFILE_HINT_OP(MY_OP_X)
    default:
      ALWAYS_ASSERT(false);
    }
#undef MY_OP_X
    INVARIANT(!value.empty());
    return true;
  } catch (transaction_abort_exception &ex) {
    throw abstract_db::abstract_abort_exception();
  }
}

// XXX: find way to remove code duplication below using C++ templates!

template <template <typename> class Transaction>
const char *
ndb_ordered_index<Transaction>::put(
    void *txn,
    const std::string &key,
    const std::string &value)
{
  PERF_DECL(static std::string probe1_name(std::string(__PRETTY_FUNCTION__) + std::string(":total:")));
  ANON_REGION(probe1_name.c_str(), &private_::ndb_put_probe0_cg);
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(txn);
  try {
#define MY_OP_X(a, b) \
  case a: \
    { \
      auto t = cast< b >()(p); \
      btr.put(*t, key, value); \
      return 0; \
    }
    switch (p->hint) {
      TXN_PROFILE_HINT_OP(MY_OP_X)
    default:
      ALWAYS_ASSERT(false);
    }
#undef MY_OP_X
  } catch (transaction_abort_exception &ex) {
    throw abstract_db::abstract_abort_exception();
  }
  return 0;
}

template <template <typename> class Transaction>
const char *
ndb_ordered_index<Transaction>::put(
    void *txn,
    std::string &&key,
    std::string &&value)
{
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(txn);
  try {
#define MY_OP_X(a, b) \
  case a: \
    { \
      auto t = cast< b >()(p); \
      btr.put(*t, std::move(key), std::move(value)); \
      return 0; \
    }
    switch (p->hint) {
      TXN_PROFILE_HINT_OP(MY_OP_X)
    default:
      ALWAYS_ASSERT(false);
    }
#undef MY_OP_X
  } catch (transaction_abort_exception &ex) {
    throw abstract_db::abstract_abort_exception();
  }
  return 0;
}

template <template <typename> class Transaction>
const char *
ndb_ordered_index<Transaction>::insert(
    void *txn,
    const std::string &key,
    const std::string &value)
{
  PERF_DECL(static std::string probe1_name(std::string(__PRETTY_FUNCTION__) + std::string(":total:")));
  ANON_REGION(probe1_name.c_str(), &private_::ndb_insert_probe0_cg);
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(txn);
  try {
#define MY_OP_X(a, b) \
  case a: \
    { \
      auto t = cast< b >()(p); \
      btr.insert(*t, key, value); \
      return 0; \
    }
    switch (p->hint) {
      TXN_PROFILE_HINT_OP(MY_OP_X)
    default:
      ALWAYS_ASSERT(false);
    }
#undef MY_OP_X
  } catch (transaction_abort_exception &ex) {
    throw abstract_db::abstract_abort_exception();
  }
  return 0;
}

template <template <typename> class Transaction>
const char *
ndb_ordered_index<Transaction>::insert(
    void *txn,
    std::string &&key,
    std::string &&value)
{
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(txn);
  try {
#define MY_OP_X(a, b) \
  case a: \
    { \
      auto t = cast< b >()(p); \
      btr.insert(*t, std::move(key), std::move(value)); \
      return 0; \
    }
    switch (p->hint) {
      TXN_PROFILE_HINT_OP(MY_OP_X)
    default:
      ALWAYS_ASSERT(false);
    }
#undef MY_OP_X
  } catch (transaction_abort_exception &ex) {
    throw abstract_db::abstract_abort_exception();
  }
  return 0;
}

template <template <typename> class Transaction>
class ndb_wrapper_search_range_callback : public txn_btree<Transaction>::search_range_callback {
public:
  ndb_wrapper_search_range_callback(abstract_ordered_index::scan_callback &upcall)
    : upcall(&upcall) {}

  virtual bool
  invoke(const typename txn_btree<Transaction>::string_type &k,
         const typename txn_btree<Transaction>::string_type &v)
  {
    return upcall->invoke(k, v);
  }

private:
  abstract_ordered_index::scan_callback *upcall;
};

struct ndb_str_allocator {
public:
  ndb_str_allocator(str_arena *arena) : arena(arena) {}
  inline std::string *
  operator()()
  {
    return likely(arena) ? arena->next() : nullptr;
  }
  inline void
  return_last(std::string *px)
  {
    if (likely(arena))
      arena->return_last(px);
  }
private:
  str_arena *arena;
};

template <template <typename> class Transaction>
void
ndb_ordered_index<Transaction>::scan(
    void *txn,
    const std::string &start_key,
    const std::string *end_key,
    scan_callback &callback,
    str_arena *arena)
{
  PERF_DECL(static std::string probe1_name(std::string(__PRETTY_FUNCTION__) + std::string(":total:")));
  ANON_REGION(probe1_name.c_str(), &private_::ndb_scan_probe0_cg);
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(txn);
  ndb_wrapper_search_range_callback<Transaction> c(callback);
  try {
#define MY_OP_X(a, b) \
  case a: \
    { \
      auto t = cast< b >()(p); \
      btr.search_range_call(*t, start_key, end_key, c, ndb_str_allocator(arena)); \
      return; \
    }
    switch (p->hint) {
      TXN_PROFILE_HINT_OP(MY_OP_X)
    default:
      ALWAYS_ASSERT(false);
    }
#undef MY_OP_X
  } catch (transaction_abort_exception &ex) {
    throw abstract_db::abstract_abort_exception();
  }
}

template <template <typename> class Transaction>
void
ndb_ordered_index<Transaction>::remove(void *txn, const std::string &key)
{
  PERF_DECL(static std::string probe1_name(std::string(__PRETTY_FUNCTION__) + std::string(":total:")));
  ANON_REGION(probe1_name.c_str(), &private_::ndb_remove_probe0_cg);
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(txn);
  try {
#define MY_OP_X(a, b) \
  case a: \
    { \
      auto t = cast< b >()(p); \
      btr.remove(*t, key); \
      return; \
    }
    switch (p->hint) {
      TXN_PROFILE_HINT_OP(MY_OP_X)
    default:
      ALWAYS_ASSERT(false);
    }
#undef MY_OP_X
  } catch (transaction_abort_exception &ex) {
    throw abstract_db::abstract_abort_exception();
  }
}

template <template <typename> class Transaction>
void
ndb_ordered_index<Transaction>::remove(void *txn, std::string &&key)
{
  ndbtxn * const p = reinterpret_cast<ndbtxn *>(txn);
  try {
#define MY_OP_X(a, b) \
  case a: \
    { \
      auto t = cast< b >()(p); \
      btr.remove(*t, std::move(key)); \
      return; \
    }
    switch (p->hint) {
      TXN_PROFILE_HINT_OP(MY_OP_X)
    default:
      ALWAYS_ASSERT(false);
    }
#undef MY_OP_X
  } catch (transaction_abort_exception &ex) {
    throw abstract_db::abstract_abort_exception();
  }
}

template <template <typename> class Transaction>
size_t
ndb_ordered_index<Transaction>::size() const
{
  return btr.size_estimate();
}

template <template <typename> class Transaction>
void
ndb_ordered_index<Transaction>::clear()
{
#ifdef TXN_BTREE_DUMP_PURGE_STATS
  std::cerr << "purging txn index: " << name << std::endl;
#endif
  return btr.unsafe_purge(true);
}

#endif /* _NDB_WRAPPER_IMPL_H_ */
