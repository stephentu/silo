#ifndef _KVDB_WRAPPER_H_
#define _KVDB_WRAPPER_H_

#include "abstract_db.h"
#include "../btree_choice.h"
#include "../rcu.h"

template <bool UseConcurrencyControl>
class kvdb_wrapper : public abstract_db {
public:

  virtual ssize_t txn_max_batch_size() const OVERRIDE { return 100; }

  virtual void do_txn_epoch_sync() const { }

  virtual void do_txn_finish() const { }

  virtual size_t
  sizeof_txn_object(uint64_t txn_flags) const
  {
    return sizeof(scoped_rcu_region);
  }

  virtual void *
  new_txn(uint64_t txn_flags, str_arena &arena, void *buf, TxnProfileHint hint)
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

template <bool UseConcurrencyControl>
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
  virtual void rscan(
      void *txn,
      const std::string &start_key,
      const std::string *end_key,
      scan_callback &callback,
      str_arena *arena);
  virtual void remove(
      void *txn,
      const std::string &key);
  virtual size_t size() const;
  virtual std::map<std::string, uint64_t> clear();
private:
  std::string name;
  typedef
    typename std::conditional<
      UseConcurrencyControl,
      concurrent_btree,
      single_threaded_btree>::type
    my_btree;
  typedef typename my_btree::key_type key_type;
  my_btree btr;
};

#endif /* _KVDB_WRAPPER_H_ */
