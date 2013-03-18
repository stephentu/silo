#include <vector>
#include <limits>
#include <utility>

#include "kvdb_wrapper.h"
#include "../varint.h"
#include "../macros.h"
#include "../util.h"
#include "../amd64.h"
#include "../lockguard.h"
#include "../prefetch.h"
#include "../scopedperf.hh"
#include "../counter.h"

using namespace std;
using namespace util;

#define MUTABLE_RECORDS

abstract_ordered_index *
kvdb_wrapper::open_index(const string &name, size_t value_size_hint, bool mostly_append)
{
  return new kvdb_ordered_index;
}

#ifdef MUTABLE_RECORDS

static event_avg_counter evt_avg_kvdb_stable_version_spins("avg_kvdb_stable_version_spins");
static event_avg_counter evt_avg_kvdb_lock_acquire_spins("avg_kvdb_lock_acquire_spins");
static event_avg_counter evt_avg_kvdb_read_retries("avg_kvdb_read_retries");

struct kvdb_record {
  volatile uint32_t hdr;
  char data[0];

  // [ locked | size  | version ]
  // [  0..1  | 1..17 | 17..32  ]

  static const uint32_t HDR_LOCKED_MASK = 0x1;

  static const uint32_t HDR_SIZE_SHIFT = 1;
  static const uint32_t HDR_SIZE_MASK = numeric_limits<uint16_t>::max() << HDR_SIZE_SHIFT;

  static const uint32_t HDR_VERSION_SHIFT = 17;
  static const uint32_t HDR_VERSION_MASK = ((uint32_t)-1) << HDR_VERSION_SHIFT;

  kvdb_record(const string &s)
    : hdr(0)
  {
#ifdef CHECK_INVARIANTS
    lock();
    set_size(s.size());
    do_write(s);
    unlock();
#else
    set_size(s.size());
    do_write(s);
#endif
  }

  inline void
  prefetch() const
  {
#ifdef LOGICAL_NODE_PREFETCH
    prefetch_bytes(this, sizeof(*this) + size());
#endif
  }

  static inline bool
  IsLocked(uint32_t v)
  {
    return v & HDR_LOCKED_MASK;
  }

  inline bool
  is_locked() const
  {
    return IsLocked(hdr);
  }

  inline void
  lock()
  {
#ifdef ENABLE_EVENT_COUNTERS
    unsigned long nspins = 0;
#endif
    uint32_t v = hdr;
    while (IsLocked(v) ||
           !__sync_bool_compare_and_swap(&hdr, v, v | HDR_LOCKED_MASK)) {
      nop_pause();
      v = hdr;
#ifdef ENABLE_EVENT_COUNTERS
      ++nspins;
#endif
    }
    COMPILER_MEMORY_FENCE;
#ifdef ENABLE_EVENT_COUNTERS
    evt_avg_kvdb_lock_acquire_spins.offer(nspins);
#endif
  }

  inline void
  unlock()
  {
    uint32_t v = hdr;
    INVARIANT(IsLocked(v));
    const uint32_t n = Version(v);
    v &= ~HDR_VERSION_MASK;
    v |= (((n + 1) << HDR_VERSION_SHIFT) & HDR_VERSION_MASK);
    v &= ~HDR_LOCKED_MASK;
    INVARIANT(!IsLocked(v));
    COMPILER_MEMORY_FENCE;
    hdr = v;
  }

  static inline size_t
  Size(uint32_t v)
  {
    return (v & HDR_SIZE_MASK) >> HDR_SIZE_SHIFT;
  }

  inline size_t
  size() const
  {
    return Size(hdr);
  }

  inline void
  set_size(size_t s)
  {
    INVARIANT(s <= numeric_limits<uint16_t>::max());
    INVARIANT(is_locked());
    const uint16_t new_sz = static_cast<uint16_t>(s);
    hdr &= ~HDR_SIZE_MASK;
    hdr |= (new_sz << HDR_SIZE_SHIFT);
    INVARIANT(size() == s);
  }

  static inline uint32_t
  Version(uint32_t v)
  {
    return (v & HDR_VERSION_MASK) >> HDR_VERSION_SHIFT;
  }

  inline uint32_t
  stable_version() const
  {
    uint32_t v = hdr;
#ifdef ENABLE_EVENT_COUNTERS
    unsigned long nspins = 0;
#endif
    while (IsLocked(v)) {
      nop_pause();
      v = hdr;
      ++nspins;
    }
    COMPILER_MEMORY_FENCE;
#ifdef ENABLE_EVENT_COUNTERS
    evt_avg_kvdb_stable_version_spins.offer(nspins);
#endif
    return v;
  }

  inline bool
  check_version(uint32_t version) const
  {
    COMPILER_MEMORY_FENCE;
    return hdr == version;
  }

  inline void
  do_read(string &s, size_t max_bytes_read) const
  {
#ifdef ENABLE_EVENT_COUNTERS
    unsigned long nretries = 0;
#endif
  retry:
    const uint32_t v = stable_version();
    const size_t sz = min(Size(v), max_bytes_read);
    s.assign(&data[0], sz);
    if (unlikely(!check_version(v))) {
#ifdef ENABLE_EVENT_COUNTERS
      ++nretries;
#endif
      goto retry;
    }
#ifdef ENABLE_EVENT_COUNTERS
    evt_avg_kvdb_read_retries.offer(nretries);
#endif
  }

  inline bool
  do_write(const string &s)
  {
    INVARIANT(is_locked());
    const size_t max_r =
      round_up<size_t, /* lgbase*/ 4>(sizeof(*this) + size()) - sizeof(*this);
    if (unlikely(s.size() > max_r))
      return false;
    set_size(s.size());
    NDB_MEMCPY(&data[0], s.data(), s.size());
    return true;
  }

  static struct kvdb_record *
  alloc(const string &s)
  {
    const size_t sz = s.size();
    const size_t alloc_sz = round_up<size_t, 4>(sizeof(kvdb_record) + sz);
    void * const p = malloc(alloc_sz);
    INVARIANT(p);
    return new (p) kvdb_record(s);
  }

  static void
  release(struct kvdb_record *r)
  {
    if (unlikely(!r))
      return;
    rcu::free_with_fn(r, free);
  }

} PACKED;

#else

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

  inline void
  prefetch() const
  {
#ifdef LOGICAL_NODE_PREFETCH
    prefetch_bytes(this, sizeof(*this) + size);
#endif
  }

  inline void
  do_read(string &s, size_t max_bytes_read) const
  {
    const size_t sz = min(static_cast<size_t>(size), max_bytes_read);
    s.assign(&r->data[0], sz);
  }

} PACKED;

#endif

STATIC_COUNTER_DECL(scopedperf::tsc_ctr, kvdb_get_probe0_tsc, kvdb_get_probe0_cg);

bool
kvdb_ordered_index::get(
    void *txn,
    const string &key,
    string &value, size_t max_bytes_read)
{
  ANON_REGION("kvdb_ordered_index::get:", &kvdb_get_probe0_cg);
  btree::value_type v = 0;
  if (btr.search(varkey(key), v)) {
    const struct kvdb_record * const r = (const struct kvdb_record *) v;
    r->prefetch();
    r->do_read(value, max_bytes_read);
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
#ifdef MUTABLE_RECORDS
  btree::value_type v = 0, v_old = 0;
  if (btr.search(varkey(key), v)) {
    // easy
    struct kvdb_record * const r = (struct kvdb_record *) v;
    r->prefetch();
    lock_guard<kvdb_record> guard(*r);
    if (r->do_write(value))
      return 0;
    // replace
    struct kvdb_record * const rnew = kvdb_record::alloc(value);
    btr.insert(varkey(key), (btree::value_type) rnew, &v_old, 0);
    INVARIANT((btree::value_type) r == v_old);
    // rcu-free the old record
    kvdb_record::release(r);
    return 0;
  }
  struct kvdb_record * const rnew = kvdb_record::alloc(value);
  if (!btr.insert(varkey(key), (btree::value_type) rnew, &v_old, 0)) {
    struct kvdb_record * const r = (struct kvdb_record *) v_old;
    kvdb_record::release(r);
  }
  return 0;
#else
  btree::value_type old_v = 0;
  if (!btr.insert(varkey(key), (btree::value_type) kvdb_record::alloc(value), &old_v, 0)) {
    struct kvdb_record * const r = (struct kvdb_record *) old_v;
    kvdb_record::release(r);
  }
#endif
  return 0;
}

const char *
kvdb_ordered_index::insert(void *txn,
                           const std::string &key,
                           const std::string &value)
{
#ifdef MUTABLE_RECORDS
  struct kvdb_record * const rnew = kvdb_record::alloc(value);
  btree::value_type v_old = 0;
  if (!btr.insert(varkey(key), (btree::value_type) rnew, &v_old, 0)) {
    struct kvdb_record * const r = (struct kvdb_record *) v_old;
    kvdb_record::release(r);
  }
  return 0;
#else
  return put(txn, key, value);
#endif
}

class kvdb_wrapper_search_range_callback : public btree::search_range_callback {
public:
  kvdb_wrapper_search_range_callback(
      kvdb_ordered_index::scan_callback &upcall,
      str_arena *arena)
    : upcall(&upcall), arena(arena) {}

  virtual bool
  invoke(const btree::string_type &k, btree::value_type v)
  {
    const struct kvdb_record * const r = (const struct kvdb_record *) v;
    string * const s_px = likely(arena) ? arena->next() : nullptr;
    INVARIANT(s_px && s_px->empty());
    r->prefetch();
    r->do_read(*s_px, numeric_limits<size_t>::max());
    return upcall->invoke(k, *s_px);
  }

private:
  kvdb_ordered_index::scan_callback *upcall;
  str_arena *arena;
};

void
kvdb_ordered_index::scan(
    void *txn,
    const string &start_key,
    const string *end_key,
    scan_callback &callback,
    str_arena *arena)
{
  kvdb_wrapper_search_range_callback c(callback, arena);
  const varkey end(end_key ? varkey(*end_key) : varkey());
  btr.search_range_call(varkey(start_key), end_key ? &end : 0, c, arena->next());
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
