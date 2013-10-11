#ifndef _BDB_WRAPPER_H_
#define _BDB_WRAPPER_H_

#include <string>
#include <db_cxx.h>

#include "abstract_db.h"
#include "../macros.h"

class bdb_wrapper : public abstract_db {
public:
  bdb_wrapper(const std::string &envdir,
              const std::string &dbfile);
  ~bdb_wrapper();

  /**
   * BDB has small txn sizes
   */
  virtual ssize_t txn_max_batch_size() const { return 1000; }

  virtual void *new_txn(
      uint64_t txn_flags,
      str_arena &arena,
      void *buf,
      TxnProfileHint hint);
  virtual bool commit_txn(void *txn);
  virtual void abort_txn(void *txn);

  virtual abstract_ordered_index *
  open_index(const std::string &name,
             size_t value_size_hint,
             bool mostly_append);

  virtual void
  close_index(abstract_ordered_index *idx);

private:
  DbEnv *env;
};

class bdb_ordered_index : public abstract_ordered_index {
public:

  // takes ownership of db
  bdb_ordered_index(Db *db) : db(db) {}
  ~bdb_ordered_index();

  virtual bool get(
      void *txn,
      const std::string &key,
      std::string &value,
      size_t max_bytes_read);

  virtual const char * put(
      void *txn,
      const std::string &key,
      const std::string &value);

  virtual void scan(
      void *txn,
      const std::string &key,
      const std::string *value,
      scan_callback &callback,
      str_arena *arena)
  {
    NDB_UNIMPLEMENTED("scan");
  }

  virtual void rscan(
      void *txn,
      const std::string &start_key,
      const std::string *end_key,
      scan_callback &callback,
      str_arena *arena)
  {
    NDB_UNIMPLEMENTED("rscan");
  }

  virtual size_t
  size() const
  {
    NDB_UNIMPLEMENTED("size");
  }

  virtual std::map<std::string, uint64_t>
  clear()
  {
    NDB_UNIMPLEMENTED("clear");
  }

private:
  Db *db;
};

#endif /* _BDB_WRAPPER_H_ */
