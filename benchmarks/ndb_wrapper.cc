#include <stdint.h>
#include "ndb_wrapper.h"
#include "../counter.h"
#include "../rcu.h"
#include "../varkey.h"
#include "../macros.h"
#include "../util.h"
#include "../varint.h"

using namespace std;
using namespace util;

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
    ret = ((transaction *) txn)->commit();
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

void
ndb_wrapper::print_txn_debug(void *txn) const
{
  ((transaction *) txn)->dump_debug_info();
}

abstract_ordered_index *
ndb_wrapper::open_index(const string &name, size_t value_size_hint)
{
  return new ndb_ordered_index(value_size_hint);
}

void
ndb_wrapper::close_index(abstract_ordered_index *idx)
{
  delete idx;
}

static inline ALWAYS_INLINE size_t
size_encode_uint32(uint32_t value)
{
#ifdef USE_VARINT_ENCODING
  return size_uvint32(value);
#else
  return sizeof(uint32_t);
#endif
}

static inline ALWAYS_INLINE uint8_t *
write_uint32(uint8_t *buf, uint32_t value)
{
  uint32_t *p = (uint32_t *) buf;
  *p = value;
  return (uint8_t *) (p + 1);
}

static inline ALWAYS_INLINE uint8_t *
write_encode_uint32(uint8_t *buf, uint32_t value)
{
#ifdef USE_VARINT_ENCODING
  return write_uvint32(buf, value);
#else
  return write_uint32(buf, value);
#endif
}

static inline ALWAYS_INLINE const uint8_t *
read_uint32(const uint8_t *buf, uint32_t *value)
{
  const uint32_t *p = (const uint32_t *) buf;
  *value = *p;
  return (const uint8_t *) (p + 1);
}

static inline ALWAYS_INLINE const uint8_t *
read_encode_uint32(const uint8_t *buf, uint32_t *value)
{
#ifdef USE_VARINT_ENCODING
  return read_uvint32(buf, value);
#else
  return read_uint32(buf, value);
#endif
}

ndb_ordered_index::ndb_ordered_index(size_t value_size_hint)
  : btr()
{
  btr.set_value_size_hint(value_size_hint);
}

bool
ndb_ordered_index::get(
    void *txn,
    const char *key, size_t keylen,
    char *&value, size_t &valuelen)
{
  try {
    txn_btree::value_type v = 0;
    txn_btree::size_type sz = 0;
    if (!btr.search(*((transaction *) txn), varkey((const uint8_t *) key, keylen), v, sz))
      return false;
    INVARIANT(v != NULL);
    INVARIANT(sz > 0);
    value = (char *) v;
    valuelen = sz;
    return true;
  } catch (transaction_abort_exception &ex) {
    throw abstract_db::abstract_abort_exception();
  }
}

static event_counter evt_rec_creates("record_creates");
static event_counter evt_rec_bytes_alloc("record_bytes_alloc");

const char *
ndb_ordered_index::put(
    void *txn,
    const char *key, size_t keylen,
    const char *value, size_t valuelen)
{
  try {
    btr.insert(
        *((transaction *) txn),
        varkey((const uint8_t *) key, keylen),
        (const txn_btree::value_type) value, valuelen);
  } catch (transaction_abort_exception &ex) {
    throw abstract_db::abstract_abort_exception();
  }
  ++evt_rec_creates;
  // XXX(stephentu): we currently can't return a stable pointer because we
  // don't even know if the txn will commit (and even if it does, the value
  // could get overwritten).  So that means our secondary index performance
  // will suffer for now
  return 0;
}

class ndb_wrapper_search_range_callback : public txn_btree::search_range_callback {
public:
  ndb_wrapper_search_range_callback(ndb_ordered_index::scan_callback &upcall)
    : upcall(&upcall) {}

  virtual bool
  invoke(const txn_btree::key_type &k, const txn_btree::value_type v, txn_btree::size_type sz)
  {
    const char *key = (const char *) k.data();
    const size_t keylen = k.size();

    const char *value = (const char *) v;
    const size_t valuelen = sz;

    return upcall->invoke(key, keylen, value, valuelen);
  }

private:
  ndb_ordered_index::scan_callback *upcall;
};

void
ndb_ordered_index::scan(
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

  try {
    if (has_end_key)
      btr.search_range_call(t, lower, &upper, c);
    else
      btr.search_range_call(t, lower, NULL, c);
  } catch (transaction_abort_exception &ex) {
    throw abstract_db::abstract_abort_exception();
  }
}

void
ndb_ordered_index::remove(
    void *txn,
    const char *key, size_t keylen)
{
  transaction &t = *((transaction *) txn);
  try {
    btr.remove(t, varkey((const uint8_t *) key, keylen));
  } catch (transaction_abort_exception &ex) {
    throw abstract_db::abstract_abort_exception();
  }
}

size_t
ndb_ordered_index::size() const
{
  return btr.size_estimate();
}
