#ifndef _NDB_WRAPPER_H_
#define _NDB_WRAPPER_H_

#include "abstract_db.h"
#include "../txn_btree.h"
#include "../txn_btree_impl.h"

template <typename Transaction>
class ndb_wrapper : public abstract_db {
public:

  virtual void
  do_txn_epoch_sync() const
  {
    txn_epoch_sync<Transaction>::sync();
  }

  virtual void
  do_txn_finish() const
  {
    txn_epoch_sync<Transaction>::finish();
  }

  virtual size_t
  sizeof_txn_object(uint64_t txn_flags) const
  {
    return sizeof(Transaction);
  }

  virtual void *new_txn(uint64_t txn_flags, void *buf);
  virtual bool commit_txn(void *txn);
  virtual void abort_txn(void *txn);
  virtual void print_txn_debug(void *txn) const;

  virtual abstract_ordered_index *
  open_index(const std::string &name,
             size_t value_size_hint,
             bool mostly_append);

  virtual void
  close_index(abstract_ordered_index *idx);

};

template <typename Transaction>
class ndb_ordered_index : public abstract_ordered_index {
public:
  ndb_ordered_index(const std::string &name, size_t value_size_hint, bool mostly_append);
  virtual bool get(
      void *txn,
      const std::string &key,
      std::string &value, size_t max_bytes_read);
  virtual const char * put(
      void *txn,
      const std::string &key,
      const std::string &value);
  virtual const char * put(
      void *txn,
      std::string &&key,
      std::string &&value);
  virtual void scan(
      void *txn,
      const std::string &start_key,
      const std::string *end_key,
      scan_callback &callback);
  virtual void remove(
      void *txn,
      const std::string &key);
  virtual void remove(
      void *txn,
      std::string &&key);
  virtual size_t size() const;
  virtual void clear();
private:
  std::string name;
  txn_btree<Transaction> btr;
};

#endif /* _NDB_WRAPPER_H_ */
