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

  virtual void *new_txn();
  virtual bool commit_txn(void *txn);
  virtual void abort_txn(void *txn);

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
  DbEnv *env;
  Db *db;
};

#endif /* _BDB_WRAPPER_H_ */
