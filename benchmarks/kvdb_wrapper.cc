#include <vector>
#include <utility>

#include "kvdb_wrapper.h"
#include "../varint.h"
#include "../macros.h"
#include "../util.h"

using namespace std;
using namespace util;

abstract_ordered_index *
kvdb_wrapper::open_index(const string &name, size_t value_size_hint, bool mostly_append)
{
  return new kvdb_ordered_index;
}

//static inline ALWAYS_INLINE size_t
//size_encode_uint32(uint32_t value)
//{
//#ifdef USE_VARINT_ENCODING
//  return size_uvint32(value);
//#else
//  return sizeof(uint32_t);
//#endif
//}
//
//static inline ALWAYS_INLINE uint8_t *
//write_uint32(uint8_t *buf, uint32_t value)
//{
//  uint32_t *p = (uint32_t *) buf;
//  *p = value;
//  return (uint8_t *) (p + 1);
//}
//
//static inline ALWAYS_INLINE uint8_t *
//write_encode_uint32(uint8_t *buf, uint32_t value)
//{
//#ifdef USE_VARINT_ENCODING
//  return write_uvint32(buf, value);
//#else
//  return write_uint32(buf, value);
//#endif
//}
//
//static inline ALWAYS_INLINE const uint8_t *
//read_uint32(const uint8_t *buf, uint32_t *value)
//{
//  const uint32_t *p = (const uint32_t *) buf;
//  *value = *p;
//  return (const uint8_t *) (p + 1);
//}
//
//static inline ALWAYS_INLINE const uint8_t *
//read_encode_uint32(const uint8_t *buf, uint32_t *value)
//{
//#ifdef USE_VARINT_ENCODING
//  return read_uvint32(buf, value);
//#else
//  return read_uint32(buf, value);
//#endif
//}

struct kvdb_record {
  uint16_t size;
  char data[0];

  static struct kvdb_record *
  alloc(const string &s)
  {
    INVARIANT(s.size() <= numeric_limits<uint16_t>::max());
    struct kvdb_record *r = (struct kvdb_record *)
      malloc(sizeof(uint16_t) + s.size());
    INVARIANT(r);
    r->size = s.size();
    NDB_MEMCPY(&r->data[0], s.data(), s.size());
    return r;
  }

  static void
  release(struct kvdb_record *r)
  {
    if (unlikely(!r))
      return;
    rcu::free_with_fn(r, free);
  }

} PACKED;

bool
kvdb_ordered_index::get(
    void *txn,
    const string &key,
    string &value, size_t max_bytes_read)
{
  btree::value_type v = 0;
  if (btr.search(varkey(key), v)) {
    const struct kvdb_record * const r = (const struct kvdb_record *) v;
    const size_t sz = std::min(static_cast<size_t>(r->size), max_bytes_read);
    value.assign(&r->data[0], sz);
    return true;
  }
  return false;
}

const char *
kvdb_ordered_index::put(
    void *txn,
    const string &key,
    const string &value)
{
  btree::value_type old_v = 0;
  if (!btr.insert(varkey(key), (btree::value_type) kvdb_record::alloc(value), &old_v, 0)) {
    struct kvdb_record *r = (struct kvdb_record *) old_v;
    kvdb_record::release(r);
  }
  return 0;
}

const char *
kvdb_ordered_index::put(
    void *txn,
    string &&key,
    string &&value)
{
  return put(txn, static_cast<const string &>(key), static_cast<const string &>(value));
}

class kvdb_wrapper_search_range_callback : public btree::search_range_callback {
public:
  kvdb_wrapper_search_range_callback(kvdb_ordered_index::scan_callback &upcall)
    : upcall(&upcall) {}

  virtual bool
  invoke(const btree::string_type &k, btree::value_type v)
  {
    const char * const key = (const char *) k.data();
    const size_t keylen = k.size();

    const struct kvdb_record * const r = (const struct kvdb_record *) v;

    return upcall->invoke(key, keylen, &r->data[0], r->size);
  }

private:
  kvdb_ordered_index::scan_callback *upcall;
};

void
kvdb_ordered_index::scan(
    void *txn,
    const string &start_key,
    const string *end_key,
    scan_callback &callback)
{
  kvdb_wrapper_search_range_callback c(callback);
  const varkey end(end_key ? varkey(*end_key) : varkey());
  btr.search_range_call(varkey(start_key), end_key ? &end : 0, c);
}

void
kvdb_ordered_index::remove(void *txn, const string &key)
{
  btree::value_type v = 0;
  if (btr.remove(varkey(key), &v)) {
    struct kvdb_record * const r = (struct kvdb_record *) v;
    kvdb_record::release(r);
  }
}

void
kvdb_ordered_index::remove(void *txn, string &&key)
{
  remove(txn, static_cast<const string &>(key));
}

size_t
kvdb_ordered_index::size() const
{
  return btr.size();
}

struct purge_tree_walker : public btree::tree_walk_callback {
  virtual void
  on_node_begin(const btree::node_opaque_t *n)
  {
    INVARIANT(spec_values.empty());
    spec_values = btree::ExtractValues(n);
  }

  virtual void
  on_node_success()
  {
    for (size_t i = 0; i < spec_values.size(); i++) {
      struct kvdb_record * const r = (struct kvdb_record *) spec_values[i].first;
      free(r);
    }
    spec_values.clear();
  }

  virtual void
  on_node_failure()
  {
    spec_values.clear();
  }

private:
  vector< pair<btree::value_type, bool> > spec_values;
};

void
kvdb_ordered_index::clear()
{
  purge_tree_walker w;
  btr.tree_walk(w);
  btr.clear();
}
