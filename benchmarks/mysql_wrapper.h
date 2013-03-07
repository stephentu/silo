#ifndef _MYSQL_WRAPPER_H_
#define _MYSQL_WRAPPER_H_

#include <string>
#include <mysql/mysql.h>

#include "abstract_db.h"
#include "../macros.h"

class mysql_wrapper : public abstract_db {
  friend class mysql_ordered_index;
public:
  mysql_wrapper(const std::string &dir, const std::string &db);
  ~mysql_wrapper();

  virtual void thread_init();
  virtual void thread_end();

  virtual void *new_txn(uint64_t txn_flags);
  virtual bool commit_txn(void *txn);
  virtual void abort_txn(void *txn);

  virtual abstract_ordered_index *
  open_index(const std::string &name, size_t value_size_hint);

  virtual void
  close_index(abstract_ordered_index *idx);

private:
  std::string db;
  MYSQL *new_connection(const std::string &db);
  static __thread MYSQL *tl_conn;
};

class mysql_ordered_index : public abstract_ordered_index {
public:
  mysql_ordered_index(const std::string &name) : name(name) {}

  virtual bool get(
      void *txn,
      const char *key, size_t keylen,
      std::string &value, size_t max_bytes_read);

  virtual const char * put(
      void *txn,
      const char *key, size_t keylen,
      const char *value, size_t valuelen);

  virtual const char * insert(
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

  virtual size_t
  size() const
  {
    NDB_UNIMPLEMENTED("size");
  }

  virtual void
  clear()
  {
    NDB_UNIMPLEMENTED("clear");
  }

private:
  std::string name;
};

#endif /* _MYSQL_WRAPPER_H_ */
