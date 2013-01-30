#include <stdint.h>
#include "ndb_wrapper.h"
#include "../rcu.h"
#include "../varkey.h"
#include "../macros.h"

void *
ndb_wrapper::new_txn(uint64_t txn_flags)
{
  switch (proto) {
  case PROTO_1:
    return new transaction_proto1(txn_flags);
  case PROTO_2:
    return new transaction_proto2(txn_flags);
  default:
    ALWAYS_ASSERT(false);
    return NULL;
  }
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

class ndb_wrapper_search_range_callback : public txn_btree::search_range_callback {
public:
  ndb_wrapper_search_range_callback(ndb_wrapper::scan_callback &upcall)
    : upcall(&upcall) {}

  virtual bool
  invoke(const txn_btree::key_type &k, txn_btree::value_type v)
  {
    const char *key = (const char *) k.data();
    const size_t keylen = k.size();

    const size_t *sp = (const size_t *) v;
    const size_t valuelen = *sp;
    const char *value = (const char *) (sp + 1);

    return upcall->invoke(key, keylen, value, valuelen);
  }

private:
  ndb_wrapper::scan_callback *upcall;
};

void
ndb_wrapper::scan(
    void *txn,
    const char *start_key, size_t start_len,
    const char *end_key, size_t end_len,
    bool has_end_key,
    scan_callback &callback)
{
  transaction &t = *((transaction *) txn);

  txn_btree::key_type lower((const uint8_t *) start_key, start_len);
  txn_btree::key_type upper((const uint8_t *) end_key, end_len);

  ndb_wrapper_search_range_callback c(callback);

  if (has_end_key)
    btr.search_range_call(t, lower, &upper, c);
  else
    btr.search_range_call(t, lower, NULL, c);
}

static void
record_cleanup_callback(uint8_t *record)
{
  if (!record)
    return;
  scoped_rcu_region rcu;
  rcu::free_array(record);
}
NDB_TXN_REGISTER_CLEANUP_CALLBACK(record_cleanup_callback);
