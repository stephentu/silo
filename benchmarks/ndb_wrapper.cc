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
ndb_wrapper::open_index(const string &name)
{
  return new ndb_ordered_index;
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

bool
ndb_ordered_index::get(
    void *txn,
    const char *key, size_t keylen,
    char *&value, size_t &valuelen)
{
  try {
    txn_btree::value_type v = 0;
    if (!btr.search(*((transaction *) txn), varkey((const uint8_t *) key, keylen), v))
      return false;
    INVARIANT(v != NULL);
    uint32_t valuelen0;
    v = (txn_btree::value_type) read_encode_uint32(v, &valuelen0);
    value = (char *) v;
    valuelen = valuelen0;
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
  ALWAYS_ASSERT(valuelen <= numeric_limits<uint32_t>::max());
  const size_t s = size_encode_uint32(valuelen);
  uint8_t *record = new uint8_t[s + valuelen];
  memcpy(write_encode_uint32(record, valuelen), value, valuelen);
  try {
    btr.insert(*((transaction *) txn), varkey((const uint8_t *) key, keylen), record);
  } catch (transaction_abort_exception &ex) {
    delete [] record;
    throw abstract_db::abstract_abort_exception();
  }
  ++evt_rec_creates;
  evt_rec_bytes_alloc += valuelen;
  return (const char *) record + s;
}

class ndb_wrapper_search_range_callback : public txn_btree::search_range_callback {
public:
  ndb_wrapper_search_range_callback(ndb_ordered_index::scan_callback &upcall)
    : upcall(&upcall) {}

  virtual bool
  invoke(const txn_btree::key_type &k, txn_btree::value_type v)
  {
    const char *key = (const char *) k.data();
    const size_t keylen = k.size();

    uint32_t valuelen = 0;
    const char *value = (const char *) read_encode_uint32(v, &valuelen);

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

static event_counter evt_rec_deletes("record_deletes");
static event_counter evt_rec_bytes_free("record_bytes_free");

static void
record_cleanup_callback(uint8_t *record, bool outstanding_refs)
{
  INVARIANT(!outstanding_refs || rcu::in_rcu_region());
  if (unlikely(!record))
    return;
  VERBOSE(cerr << "record_cleanup_callback(refs="
               << outstanding_refs << "): 0x"
               << hexify(intptr_t(record)) << endl);
  ++evt_rec_deletes;
  uint32_t valuelen = 0;
  read_encode_uint32(record, &valuelen);
  evt_rec_bytes_free += valuelen;
  if (outstanding_refs)
    rcu::free_array(record);
  else
    delete [] record;
}
NDB_TXN_REGISTER_CLEANUP_CALLBACK(record_cleanup_callback);
