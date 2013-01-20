#ifndef _NDB_WRAPPER_H_
#define _NDB_WRAPPER_H_

#include "abstract_db.h"
#include "../txn_btree.h"

class ndb_wrapper : public abstract_db {
public:

  enum Proto {
    PROTO_1,
    PROTO_2,
  }

  ndb_wrapper(Proto proto) : proto(proto) {}

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

private:
  Proto proto;
  txn_btree btr;
};

#endif /* _NDB_WRAPPER_H_ */
