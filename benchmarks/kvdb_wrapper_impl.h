#ifndef _KVDB_WRAPPER_IMPL_H_
#define _KVDB_WRAPPER_IMPL_H_

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

namespace private_ {
  static event_avg_counter evt_avg_kvdb_stable_version_spins("avg_kvdb_stable_version_spins");
  static event_avg_counter evt_avg_kvdb_lock_acquire_spins("avg_kvdb_lock_acquire_spins");
  static event_avg_counter evt_avg_kvdb_read_retries("avg_kvdb_read_retries");

  STATIC_COUNTER_DECL(scopedperf::tsc_ctr, kvdb_get_probe0, kvdb_get_probe0_cg);
  STATIC_COUNTER_DECL(scopedperf::tsc_ctr, kvdb_get_probe1, kvdb_get_probe1_cg);
  STATIC_COUNTER_DECL(scopedperf::tsc_ctr, kvdb_put_probe0, kvdb_put_probe0_cg);
  STATIC_COUNTER_DECL(scopedperf::tsc_ctr, kvdb_insert_probe0, kvdb_insert_probe0_cg);
  STATIC_COUNTER_DECL(scopedperf::tsc_ctr, kvdb_scan_probe0, kvdb_scan_probe0_cg);
  STATIC_COUNTER_DECL(scopedperf::tsc_ctr, kvdb_remove_probe0, kvdb_remove_probe0_cg);
}

// defines single-threaded version
template <bool UseConcurrencyControl>
struct record_version {
  uint16_t sz;

  inline ALWAYS_INLINE bool
  is_locked() const
  {
    return false;
  }

  inline ALWAYS_INLINE void lock() {}

  inline ALWAYS_INLINE void unlock() {}

  static inline ALWAYS_INLINE size_t
  Size(uint32_t v)
  {
    return 0;
  }

  inline ALWAYS_INLINE size_t
  size() const
  {
    return sz;
  }

  inline ALWAYS_INLINE void
  set_size(size_t s)
  {
    INVARIANT(s <= std::numeric_limits<uint16_t>::max());
    sz = s;
  }

  inline ALWAYS_INLINE uint32_t
  stable_version() const
  {
    return 0;
  }

  inline ALWAYS_INLINE bool
  check_version(uint32_t version) const
  {
    return true;
  }
};

// concurrency control version
template <>
struct record_version<true> {
  // [ locked | size  | version ]
  // [  0..1  | 1..17 | 17..32  ]

  static const uint32_t HDR_LOCKED_MASK = 0x1;

  static const uint32_t HDR_SIZE_SHIFT = 1;
  static const uint32_t HDR_SIZE_MASK = std::numeric_limits<uint16_t>::max() << HDR_SIZE_SHIFT;

  static const uint32_t HDR_VERSION_SHIFT = 17;
  static const uint32_t HDR_VERSION_MASK = ((uint32_t)-1) << HDR_VERSION_SHIFT;

  record_version<true>() : hdr(0) {}

  volatile uint32_t hdr;

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
    private_::evt_avg_kvdb_lock_acquire_spins.offer(nspins);
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
    INVARIANT(s <= std::numeric_limits<uint16_t>::max());
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
#ifdef ENABLE_EVENT_COUNTERS
      ++nspins;
#endif
    }
    COMPILER_MEMORY_FENCE;
#ifdef ENABLE_EVENT_COUNTERS
    private_::evt_avg_kvdb_stable_version_spins.offer(nspins);
#endif
    return v;
  }

  inline bool
  check_version(uint32_t version) const
  {
    COMPILER_MEMORY_FENCE;
    return hdr == version;
  }
};

template <bool UseConcurrencyControl>
struct basic_kvdb_record : public record_version<UseConcurrencyControl> {
  typedef record_version<UseConcurrencyControl> super_type;
  uint16_t alloc_size;
  char data[0];

  basic_kvdb_record(uint16_t alloc_size, const std::string &s)
    : record_version<UseConcurrencyControl>(),
      alloc_size(alloc_size)
  {
#ifdef CHECK_INVARIANTS
    this->lock();
    this->set_size(s.size());
    do_write(s);
    this->unlock();
#else
    this->set_size(s.size());
    do_write(s);
#endif
  }

  inline void
  prefetch() const
  {
#ifdef PREFETCH
    prefetch_bytes(this, sizeof(*this) + size());
#endif
  }

  inline void
  do_read(std::string &s, size_t max_bytes_read) const
  {
    if (UseConcurrencyControl) {
#ifdef ENABLE_EVENT_COUNTERS
      unsigned long nretries = 0;
#endif
    retry:
      const uint32_t v = this->stable_version();
      const size_t sz = std::min(super_type::Size(v), max_bytes_read);
      s.assign(&data[0], sz);
      if (unlikely(!this->check_version(v))) {
#ifdef ENABLE_EVENT_COUNTERS
        ++nretries;
#endif
        goto retry;
      }
#ifdef ENABLE_EVENT_COUNTERS
      private_::evt_avg_kvdb_read_retries.offer(nretries);
#endif
    } else {
      const size_t sz = std::min(this->size(), max_bytes_read);
      s.assign(&data[0], sz);
    }
  }

  inline bool
  do_write(const std::string &s)
  {
    INVARIANT(!UseConcurrencyControl || this->is_locked());
    if (unlikely(s.size() > alloc_size))
      return false;
    this->set_size(s.size());
    NDB_MEMCPY(&data[0], s.data(), s.size());
    return true;
  }

  static basic_kvdb_record *
  alloc(const std::string &s)
  {
    const size_t sz = s.size();
    const size_t max_alloc_sz =
      std::numeric_limits<uint16_t>::max() + sizeof(basic_kvdb_record);
    const size_t alloc_sz =
      std::min(
          util::round_up<size_t, allocator::LgAllocAlignment>(sizeof(basic_kvdb_record) + sz),
          max_alloc_sz);
    char * const p = reinterpret_cast<char *>(rcu::s_instance.alloc(alloc_sz));
    INVARIANT(p);
    return new (p) basic_kvdb_record(alloc_sz - sizeof(basic_kvdb_record), s);
  }

private:
  static inline void
  deleter(void *r)
  {
    basic_kvdb_record * const px =
      reinterpret_cast<basic_kvdb_record *>(r);
    const size_t alloc_sz = px->alloc_size + sizeof(*px);
    px->~basic_kvdb_record();
    rcu::s_instance.dealloc(px, alloc_sz);
  }

public:
  static void
  release(basic_kvdb_record *r)
  {
    if (unlikely(!r))
      return;
    rcu::s_instance.free_with_fn(r, deleter);
  }

  static void
  release_no_rcu(basic_kvdb_record *r)
  {
    if (unlikely(!r))
      return;
    deleter(r);
  }

} PACKED;

template <bool UseConcurrencyControl>
bool
kvdb_ordered_index<UseConcurrencyControl>::get(
    void *txn,
    const std::string &key,
    std::string &value, size_t max_bytes_read)
{
  typedef basic_kvdb_record<UseConcurrencyControl> kvdb_record;
  ANON_REGION("kvdb_ordered_index::get:", &private_::kvdb_get_probe0_cg);
  typename my_btree::value_type v = 0;
  if (btr.search(varkey(key), v)) {
    ANON_REGION("kvdb_ordered_index::get:do_read:", &private_::kvdb_get_probe1_cg);
    const kvdb_record * const r = (const kvdb_record *) v;
    r->prefetch();
    r->do_read(value, max_bytes_read);
    return true;
  }
  return false;
}

template <bool UseConcurrencyControl>
const char *
kvdb_ordered_index<UseConcurrencyControl>::put(
    void *txn,
    const std::string &key,
    const std::string &value)
{
  typedef basic_kvdb_record<UseConcurrencyControl> kvdb_record;
  ANON_REGION("kvdb_ordered_index::put:", &private_::kvdb_put_probe0_cg);
  typename my_btree::value_type v = 0, v_old = 0;
  if (btr.search(varkey(key), v)) {
    // easy
    kvdb_record * const r = (kvdb_record *) v;
    r->prefetch();
    lock_guard<kvdb_record> guard(*r);
    if (r->do_write(value))
      return 0;
    // replace
    kvdb_record * const rnew = kvdb_record::alloc(value);
    btr.insert(varkey(key), (typename my_btree::value_type) rnew, &v_old, 0);
    INVARIANT((typename my_btree::value_type) r == v_old);
    // rcu-free the old record
    kvdb_record::release(r);
    return 0;
  }
  kvdb_record * const rnew = kvdb_record::alloc(value);
  if (!btr.insert(varkey(key), (typename my_btree::value_type) rnew, &v_old, 0)) {
    kvdb_record * const r = (kvdb_record *) v_old;
    kvdb_record::release(r);
  }
  return 0;
}

template <bool UseConcurrencyControl>
const char *
kvdb_ordered_index<UseConcurrencyControl>::insert(void *txn,
                           const std::string &key,
                           const std::string &value)
{
  typedef basic_kvdb_record<UseConcurrencyControl> kvdb_record;
  ANON_REGION("kvdb_ordered_index::insert:", &private_::kvdb_insert_probe0_cg);
  kvdb_record * const rnew = kvdb_record::alloc(value);
  typename my_btree::value_type v_old = 0;
  if (!btr.insert(varkey(key), (typename my_btree::value_type) rnew, &v_old, 0)) {
    kvdb_record * const r = (kvdb_record *) v_old;
    kvdb_record::release(r);
  }
  return 0;
}

template <typename Btree, bool UseConcurrencyControl>
class kvdb_wrapper_search_range_callback : public Btree::search_range_callback {
public:
  typedef basic_kvdb_record<UseConcurrencyControl> kvdb_record;
  kvdb_wrapper_search_range_callback(
      abstract_ordered_index::scan_callback &upcall,
      str_arena *arena)
    : upcall(&upcall), arena(arena) {}

  virtual bool
  invoke(const typename Btree::string_type &k, typename Btree::value_type v)
  {
    const kvdb_record * const r = (const kvdb_record *) v;
    std::string * const s_px = likely(arena) ? arena->next() : nullptr;
    INVARIANT(s_px && s_px->empty());
    r->prefetch();
    r->do_read(*s_px, std::numeric_limits<size_t>::max());
    return upcall->invoke(k.data(), k.length(), *s_px);
  }

private:
  abstract_ordered_index::scan_callback *upcall;
  str_arena *arena;
};

template <bool UseConcurrencyControl>
void
kvdb_ordered_index<UseConcurrencyControl>::scan(
    void *txn,
    const std::string &start_key,
    const std::string *end_key,
    scan_callback &callback,
    str_arena *arena)
{
  ANON_REGION("kvdb_ordered_index::scan:", &private_::kvdb_scan_probe0_cg);
  kvdb_wrapper_search_range_callback<my_btree, UseConcurrencyControl> c(callback, arena);
  key_type end(end_key ? key_type(*end_key) : key_type());
  btr.search_range_call(key_type(start_key), end_key ? &end : 0, c, arena->next());
}

template <bool UseConcurrencyControl>
void
kvdb_ordered_index<UseConcurrencyControl>::rscan(
    void *txn,
    const std::string &start_key,
    const std::string *end_key,
    scan_callback &callback,
    str_arena *arena)
{
  ANON_REGION("kvdb_ordered_index::rscan:", &private_::kvdb_scan_probe0_cg);
  kvdb_wrapper_search_range_callback<my_btree, UseConcurrencyControl> c(callback, arena);
  key_type end(end_key ? key_type(*end_key) : key_type());
  btr.rsearch_range_call(key_type(start_key), end_key ? &end : 0, c, arena->next());
}

template <bool UseConcurrencyControl>
void
kvdb_ordered_index<UseConcurrencyControl>::remove(void *txn, const std::string &key)
{
  typedef basic_kvdb_record<UseConcurrencyControl> kvdb_record;
  ANON_REGION("kvdb_ordered_index::remove:", &private_::kvdb_remove_probe0_cg);
  typename my_btree::value_type v = 0;
  if (btr.remove(varkey(key), &v)) {
    kvdb_record * const r = (kvdb_record *) v;
    kvdb_record::release(r);
  }
}

template <bool UseConcurrencyControl>
size_t
kvdb_ordered_index<UseConcurrencyControl>::size() const
{
  return btr.size();
}

template <typename Btree, bool UseConcurrencyControl>
struct purge_tree_walker : public Btree::tree_walk_callback {
  typedef basic_kvdb_record<UseConcurrencyControl> kvdb_record;

#ifdef TXN_BTREE_DUMP_PURGE_STATS
  purge_tree_walker()
    : purge_stats_nodes(0),
      purge_stats_nosuffix_nodes(0) {}
  std::map<size_t, size_t> purge_stats_record_size_counts; // just the record
  std::map<size_t, size_t> purge_stats_alloc_size_counts; // includes overhead
  std::vector<uint16_t> purge_stats_nkeys_node;
  size_t purge_stats_nodes;
  size_t purge_stats_nosuffix_nodes;

  void
  dump_stats()
  {
    size_t v = 0;
    for (std::vector<uint16_t>::iterator it = purge_stats_nkeys_node.begin();
        it != purge_stats_nkeys_node.end(); ++it)
      v += *it;
    const double avg_nkeys_node = double(v)/double(purge_stats_nkeys_node.size());
    const double avg_fill_factor = avg_nkeys_node/double(Btree::NKeysPerNode);
    std::cerr << "btree node stats" << std::endl;
    std::cerr << "    avg_nkeys_node: " << avg_nkeys_node << std::endl;
    std::cerr << "    avg_fill_factor: " << avg_fill_factor << std::endl;
    std::cerr << "    num_nodes: " << purge_stats_nodes << std::endl;
    std::cerr << "    num_nosuffix_nodes: " << purge_stats_nosuffix_nodes << std::endl;
    std::cerr << "record size stats (nbytes => count)" << std::endl;
    for (std::map<size_t, size_t>::iterator it = purge_stats_record_size_counts.begin();
        it != purge_stats_record_size_counts.end(); ++it)
      std::cerr << "    " << it->first << " => " << it->second << std::endl;
    std::cerr << "alloc size stats  (nbytes => count)" << std::endl;
    for (std::map<size_t, size_t>::iterator it = purge_stats_alloc_size_counts.begin();
        it != purge_stats_alloc_size_counts.end(); ++it)
      std::cerr << "    " << (it->first + sizeof(kvdb_record)) << " => " << it->second << std::endl;
  }
#endif

  virtual void
  on_node_begin(const typename Btree::node_opaque_t *n)
  {
    INVARIANT(spec_values.empty());
    spec_values = Btree::ExtractValues(n);
  }

  virtual void
  on_node_success()
  {
    for (size_t i = 0; i < spec_values.size(); i++) {
      kvdb_record * const r = (kvdb_record *) spec_values[i].first;
      purge_stats_record_size_counts[r->size()]++;
      purge_stats_alloc_size_counts[r->alloc_size]++;
      kvdb_record::release_no_rcu(r);
    }
#ifdef TXN_BTREE_DUMP_PURGE_STATS
    purge_stats_nkeys_node.push_back(spec_values.size());
    purge_stats_nodes++;
    for (size_t i = 0; i < spec_values.size(); i++)
      if (spec_values[i].second)
        goto done;
    purge_stats_nosuffix_nodes++;
done:
#endif
    spec_values.clear();
  }

  virtual void
  on_node_failure()
  {
    spec_values.clear();
  }

private:
  std::vector<std::pair<typename Btree::value_type, bool>> spec_values;
};

template <bool UseConcurrencyControl>
std::map<std::string, uint64_t>
kvdb_ordered_index<UseConcurrencyControl>::clear()
{

  purge_tree_walker<my_btree, UseConcurrencyControl> w;
  scoped_rcu_region guard;
  btr.tree_walk(w);
  btr.clear();
#ifdef TXN_BTREE_DUMP_PURGE_STATS
  std::cerr << "purging kvdb index: " << name << std::endl;
  w.dump_stats();
#endif
  return std::map<std::string, uint64_t>();
}

template <bool UseConcurrencyControl>
abstract_ordered_index *
kvdb_wrapper<UseConcurrencyControl>::open_index(
    const std::string &name, size_t value_size_hint, bool mostly_append)
{
  return new kvdb_ordered_index<UseConcurrencyControl>(name);
}

#endif /* _KVDB_WRAPPER_IMPL_H_ */
