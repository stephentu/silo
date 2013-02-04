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

  virtual void *new_txn(uint64_t txn_flags);
  virtual bool commit_txn(void *txn);
  virtual void abort_txn(void *txn);

  virtual abstract_ordered_index *
  open_index(const std::string &name);

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
      const char *key, size_t keylen,
      char *&value, size_t &valuelen);

  virtual void put(
      void *txn,
      const char *key, size_t keylen,
      const char *value, size_t valuelen);

  virtual void scan(
      void *txn,
      const char *start_key, size_t start_len,
      const char *end_key, size_t end_len,
      bool has_end_key,
      scan_callback &callback)
  {
    NDB_UNIMPLEMENTED("scan");
  }
private:
  Db *db;
};

#endif /* _BDB_WRAPPER_H_ */
