#include <stdint.h>
#include "ndb_wrapper.h"
#include "../varkey.h"
#include "../macros.h"

void *
ndb_wrapper::new_txn()
{
  return new transaction;
}

bool
ndb_wrapper::commit_txn(void *txn)
{
  bool ret;
  try {
    ((transaction *) txn)->commit();
    ret = true;
  } catch (transaction_abort_exception &ex) {
    ret = false;
  }
  delete (transaction *) txn;
  return ret;
}

void
ndb_wrapper::abort_txn(void *txn)
{
  ((transaction *) txn)->abort();
  delete (transaction *) txn;
}

bool
ndb_wrapper::get(
    void *txn,
    const char *key, size_t keylen,
    char *&value, size_t &valuelen)
{
  try {
    txn_btree::value_type v = 0;
    bool ret = btr.search(*((transaction *) txn), varkey((const uint8_t *) key, keylen), v);
    if (!ret)
      return false;
    INVARIANT(v != NULL);
    size_t *sp = (size_t *) v;
    valuelen = *sp;
    value = (char *) malloc(valuelen);
    INVARIANT(value != NULL); // XXX: deal with this later
    memcpy(value, v + sizeof(size_t), valuelen);
    return true;
  } catch (transaction_abort_exception &ex) {
    throw abstract_abort_exception();
  }
}

void
ndb_wrapper::put(
    void *txn,
    const char *key, size_t keylen,
    const char *value, size_t valuelen)
{
  uint8_t *record = new uint8_t[sizeof(size_t) + valuelen];
  size_t *sp = (size_t *) record;
  *sp = valuelen;
  memcpy(record + sizeof(size_t), value, valuelen);
  try {
    btr.insert(*((transaction *) txn), varkey((const uint8_t *) key, keylen), record);
  } catch (transaction_abort_exception &ex) {
    delete [] record;
    throw abstract_abort_exception();
  }
}

static void
record_cleanup_callback(uint8_t *record)
{
  delete [] record;
}
NDB_TXN_REGISTER_CLEANUP_CALLBACK(record_cleanup_callback);
