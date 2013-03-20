#ifndef _KVDB_WRAPPER_H_
#define _KVDB_WRAPPER_H_

#include "abstract_db.h"
#include "../btree.h"
#include "../rcu.h"

class kvdb_wrapper : public abstract_db {
public:

  virtual void do_txn_epoch_sync() const { }

  virtual void do_txn_finish() const { }

  virtual size_t
  sizeof_txn_object(uint64_t txn_flags) const
  {
    return sizeof(scoped_rcu_region);
  }

  virtual void *
  new_txn(uint64_t txn_flags, void *buf, TxnProfileHint hint)
  {
    return new (buf) scoped_rcu_region;
  }

  virtual bool
  commit_txn(void *txn)
  {
    ((scoped_rcu_region *) txn)->~scoped_rcu_region();
    return true;
  }

  virtual void abort_txn(void *txn) { ALWAYS_ASSERT(false); } // txn should never abort
  virtual void print_txn_debug(void *txn) const { }

  virtual abstract_ordered_index *
  open_index(const std::string &name,
             size_t value_size_hint,
             bool mostly_append);

  virtual void
  close_index(abstract_ordered_index *idx)
  {
    delete idx;
  }
};

class kvdb_ordered_index : public abstract_ordered_index {
public:
  kvdb_ordered_index(const std::string &name)
    : name(name) {}
  virtual bool get(
      void *txn,
      const std::string &key,
      std::string &value, size_t max_bytes_read);
  virtual const char * put(
      void *txn,
      const std::string &key,
      const std::string &value);
  virtual const char *
  insert(void *txn,
         const std::string &key,
         const std::string &value);
  virtual void scan(
      void *txn,
      const std::string &start_key,
      const std::string *end_key,
      scan_callback &callback,
      str_arena *arena);
  virtual void remove(
      void *txn,
      const std::string &key);
  virtual size_t size() const;
  virtual void clear();
private:
  std::string name;
  btree btr;
};

#endif /* _KVDB_WRAPPER_H_ */
