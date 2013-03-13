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
ndb_wrapper::open_index(const string &name, size_t value_size_hint, bool mostly_append)
{
  return new ndb_ordered_index(name, value_size_hint, mostly_append);
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

ndb_ordered_index::ndb_ordered_index(const string &name, size_t value_size_hint, bool mostly_append)
  : name(name), btr()
{
  btr.set_value_size_hint(value_size_hint);
  btr.set_mostly_append(mostly_append);
}

bool
ndb_ordered_index::get(
    void *txn,
    const string &key,
    string &value, size_t max_bytes_read)
{
  try {
    if (!btr.search(*((transaction *) txn), key, value, max_bytes_read))
      return false;
    INVARIANT(!value.empty());
    return true;
  } catch (transaction_abort_exception &ex) {
    throw abstract_db::abstract_abort_exception();
  }
}

static event_counter evt_rec_inserts("record_inserts");

const char *
ndb_ordered_index::put(
    void *txn,
    const string &key,
    const string &value)
{
  try {
    btr.insert(*((transaction *) txn), key, value);
  } catch (transaction_abort_exception &ex) {
    throw abstract_db::abstract_abort_exception();
  }
  ++evt_rec_inserts;
  // XXX(stephentu): we currently can't return a stable pointer because we
  // don't even know if the txn will commit (and even if it does, the value
  // could get overwritten).  So that means our secondary index performance
  // will suffer for now
  return 0;
}

const char *
ndb_ordered_index::put(
    void *txn,
    string &&key,
    string &&value)
{
  try {
    btr.insert(*((transaction *) txn), move(key), move(value));
  } catch (transaction_abort_exception &ex) {
    throw abstract_db::abstract_abort_exception();
  }
  ++evt_rec_inserts;
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
  invoke(const txn_btree::string_type &k, const txn_btree::value_type v, txn_btree::size_type sz)
  {
    const char * const key = (const char *) k.data();
    const size_t keylen = k.size();

    const char * const value = (const char *) v;
    const size_t valuelen = sz;

    return upcall->invoke(key, keylen, value, valuelen);
  }

private:
  ndb_ordered_index::scan_callback *upcall;
};

void
ndb_ordered_index::scan(
    void *txn,
    const string &start_key,
    const string *end_key,
    scan_callback &callback)
{
  transaction &t = *((transaction *) txn);
  ndb_wrapper_search_range_callback c(callback);
  try {
    btr.search_range_call(t, start_key, end_key, c);
  } catch (transaction_abort_exception &ex) {
    throw abstract_db::abstract_abort_exception();
  }
}

void
ndb_ordered_index::remove(void *txn, const string &key)
{
  transaction &t = *((transaction *) txn);
  try {
    btr.remove(t, key);
  } catch (transaction_abort_exception &ex) {
    throw abstract_db::abstract_abort_exception();
  }
}

void
ndb_ordered_index::remove(void *txn, string &&key)
{
  transaction &t = *((transaction *) txn);
  try {
    btr.remove(t, move(key));
  } catch (transaction_abort_exception &ex) {
    throw abstract_db::abstract_abort_exception();
  }
}

size_t
ndb_ordered_index::size() const
{
  return btr.size_estimate();
}

void
ndb_ordered_index::clear()
{
#ifdef TXN_BTREE_DUMP_PURGE_STATS
  cerr << "purging txn index: " << name << endl;
#endif
  return btr.unsafe_purge(true);
}
