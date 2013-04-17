#pragma once

#include <memory>

#include "abstract_db.h"
#include "../typed_txn_btree.h"
#include "../txn.h"
#include "../txn_impl.h"
#include "../txn_proto2_impl.h"

template <template <typename> class Transaction, typename Schema>
class ndb_index : public abstract_ordered_index,
                  public typed_txn_btree<Transaction, Schema> {
  typedef typed_txn_btree<Transaction, Schema> super_type;
public:

  ndb_index(size_t value_size_hint,
            bool mostly_append,
            const std::string &name)
    : super_type(value_size_hint, mostly_append, name),
      name(name)
  {}

  virtual size_t
  size() const OVERRIDE
  {
    return this->size_estimate();
  }

  virtual std::map<std::string, uint64_t>
  clear() OVERRIDE
  {
#ifdef TXN_BTREE_DUMP_PURGE_STATS
    std::cerr << "purging txn index: " << name << std::endl;
#endif
    return this->unsafe_purge(true);
  }

private:
  std::string name;
};

namespace private_ {

  template <enum abstract_db::TxnProfileHint> struct ndb_txn_type {};

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

#define SPECIALIZE_OP_HINTS_X(hint, traitstype) \
  template <> struct ndb_txn_type< hint > { \
    typedef traitstype type; \
  };

TXN_PROFILE_HINT_OP(SPECIALIZE_OP_HINTS_X)

#undef SPECIALIZE_OP_HINTS_X
}

template <template <typename> class Transaction>
class ndb_database : public abstract_db {
public:

  template <typename Schema>
  struct IndexType {
    typedef ndb_index<Transaction, Schema> type;
    typedef std::shared_ptr<type> ptr_type;
  };

  template <enum abstract_db::TxnProfileHint hint>
  struct TransactionType
  {
    typedef Transaction<typename private_::ndb_txn_type<hint>::type> type;
    typedef std::shared_ptr<type> ptr_type;
  };

  template <enum abstract_db::TxnProfileHint hint>
  inline typename TransactionType<hint>::ptr_type
  new_txn(uint64_t txn_flags) const
  {
    return std::make_shared<typename TransactionType<hint>::type>(txn_flags);
  }

  typedef transaction_abort_exception abort_exception_type;

  virtual void
  do_txn_epoch_sync() const OVERRIDE
  {
    txn_epoch_sync<Transaction>::sync();
  }

  virtual void
  do_txn_finish() const OVERRIDE
  {
    txn_epoch_sync<Transaction>::finish();
  }

  template <typename Schema>
  inline typename IndexType<Schema>::ptr_type
  open_index(const std::string &name,
             size_t value_size_hint,
             bool mostly_append)
  {
    return std::make_shared<typename IndexType<Schema>::type>(
        value_size_hint, mostly_append, name);
  }
};
