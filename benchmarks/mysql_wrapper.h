#ifndef _MYSQL_WRAPPER_H_
#define _MYSQL_WRAPPER_H_

#include <string>
#include <mysql/mysql.h>

#include "abstract_db.h"
#include "../macros.h"

class mysql_wrapper : public abstract_db {
public:
  mysql_wrapper(const std::string &dir, const std::string &db);
  ~mysql_wrapper();

  virtual void thread_init();
  virtual void thread_end();

  virtual void *new_txn(uint64_t txn_flags);
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

  virtual void insert(
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
  std::string db;
  MYSQL *new_connection(const std::string &db);
  static __thread MYSQL *tl_conn;
};

#endif /* _MYSQL_WRAPPER_H_ */
