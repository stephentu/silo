#ifndef _KVDB_WRAPPER_IMPL_H_
#define _KVDB_WRAPPER_IMPL_H_

#include <vector>
#include <limits>
#include <utility>

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
    NDB_MEMCPY(&data[0], s.data(), s.size());
    this->set_size(s.size());
  }

  // just allocate, and set size to 0
  basic_kvdb_record(uint16_t alloc_size)
    : record_version<UseConcurrencyControl>(),
      alloc_size(alloc_size)
  {
    this->set_size(0);
  }

  inline void
  prefetch() const
  {
#ifdef TUPLE_PREFETCH
    prefetch_bytes(this, sizeof(*this) + this->size());
#endif
  }

private:
  template <typename Reader, typename StringAllocator>
  inline bool
  do_guarded_read(Reader &reader, StringAllocator &sa) const
  {
    const size_t read_sz = this->size();
    INVARIANT(read_sz);
    return reader((uint8_t *) &this->data[0], read_sz, sa);
  }

public:

  template <typename Reader, typename StringAllocator>
  inline void
  do_read(Reader &reader, StringAllocator &sa) const
  {
    if (UseConcurrencyControl) {
#ifdef ENABLE_EVENT_COUNTERS
      unsigned long nretries = 0;
#endif
    retry:
      const uint32_t v = this->stable_version();
      if (unlikely(!do_guarded_read(reader, sa) ||
                   !this->check_version(v))) {
#ifdef ENABLE_EVENT_COUNTERS
        ++nretries;
#endif
        goto retry;
      }
#ifdef ENABLE_EVENT_COUNTERS
      private_::evt_avg_kvdb_read_retries.offer(nretries);
#endif
    } else {
      const bool ret = do_guarded_read(reader, sa);
      if (!ret)
        INVARIANT(false);
    }
  }

  template <typename Writer>
  inline bool
  do_write(Writer &writer)
  {
    INVARIANT(!UseConcurrencyControl || this->is_locked());
    const size_t new_sz = writer.compute_needed((const uint8_t *) &this->data[0], this->size());
    if (unlikely(new_sz > alloc_size))
      return false;
    writer((uint8_t *) &this->data[0], this->size());
    this->set_size(new_sz);
    return true;
  }

  static basic_kvdb_record *
  alloc(size_t alloc_sz)
  {
    INVARIANT(alloc_sz <= std::numeric_limits<uint16_t>::max());
    const size_t max_alloc_sz =
      std::numeric_limits<uint16_t>::max() + sizeof(basic_kvdb_record);
    const size_t actual_alloc_sz =
      std::min(
          util::round_up<size_t, allocator::LgAllocAlignment>(sizeof(basic_kvdb_record) + alloc_sz),
          max_alloc_sz);
    char * const p = reinterpret_cast<char *>(rcu::s_instance.alloc(actual_alloc_sz));
    INVARIANT(p);
    return new (p) basic_kvdb_record(actual_alloc_sz - sizeof(basic_kvdb_record));
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

template <typename Btree, bool UseConcurrencyControl>
struct purge_tree_walker : public Btree::tree_walk_callback {
  typedef basic_kvdb_record<UseConcurrencyControl> kvdb_record;

#ifdef TXN_BTREE_DUMP_PURGE_STATS
  purge_tree_walker()
    : purge_stats_nodes(0),
      purge_stats_nosuffix_nodes(0) {}
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

class kvdb_txn {
public:
  inline kvdb_txn(uint64_t, str_arena &a) : a(&a) {}
  inline str_arena & string_allocator() { return *a; }

  inline bool
  commit()
  {
    return true;
  }

  inline void
  abort()
  {
    // should never abort
    ALWAYS_ASSERT(false);
  }

private:
  str_arena *a;
  scoped_rcu_region region;
};

template <typename Schema, bool UseConcurrencyControl>
class kvdb_index : public abstract_ordered_index {
public:

  typedef typename Schema::base_type base_type;
  typedef typename Schema::key_type key_type;
  typedef typename Schema::value_type value_type;
  typedef typename Schema::value_descriptor_type value_descriptor_type;
  typedef typename Schema::key_encoder_type key_encoder_type;
  typedef typename Schema::value_encoder_type value_encoder_type;

  static const uint64_t AllFieldsMask = typed_txn_btree_<Schema>::AllFieldsMask;
  typedef util::Fields<AllFieldsMask> AllFields;

  struct search_range_callback {
  public:
    virtual ~search_range_callback() {}
    virtual bool invoke(const key_type &k, const value_type &v) = 0;
  };

  struct bytes_search_range_callback {
  public:
    virtual ~bytes_search_range_callback() {}
    virtual bool invoke(const std::string &k, const std::string &v) = 0;
  };

private:
  // leverage the definitions for txn_btree and typed_txn_btree

  typedef txn_btree_::key_reader bytes_key_reader;
  typedef txn_btree_::single_value_reader bytes_single_value_reader;
  typedef txn_btree_::value_reader bytes_value_reader;

  typedef
    typename typed_txn_btree_<Schema>::key_writer
    key_writer;
  typedef
    typename typed_txn_btree_<Schema>::key_reader
    key_reader;

  typedef
    typename typed_txn_btree_<Schema>::value_writer
    value_writer;
  typedef
    typename typed_txn_btree_<Schema>::single_value_reader
    single_value_reader;
  typedef
    typename typed_txn_btree_<Schema>::value_reader
    value_reader;

  typedef basic_kvdb_record<UseConcurrencyControl> kvdb_record;

  template <typename Btree, typename Callback, typename KeyReader, typename ValueReader>
  class kvdb_wrapper_search_range_callback : public Btree::search_range_callback {
  public:

    kvdb_wrapper_search_range_callback(
        Callback &upcall,
        KeyReader &kr,
        ValueReader &vr,
        str_arena &arena)
      : upcall(&upcall), kr(&kr),
        vr(&vr), arena(&arena) {}

    virtual bool
    invoke(const typename Btree::string_type &k, typename Btree::value_type v)
    {
      const kvdb_record * const r =
        reinterpret_cast<const kvdb_record *>(v);
      r->prefetch();
      r->do_read(*vr, *arena);
      return upcall->invoke((*kr)(k), vr->results());
    }

  private:
    Callback *upcall;
    KeyReader *kr;
    ValueReader *vr;
    str_arena *arena;
  };

public:

  kvdb_index(size_t value_size_hint,
            bool mostly_append,
            const std::string &name)
    : name(name)
  {}

  // virtual interface

  virtual size_t
  size() const OVERRIDE
  {
    return btr.size();
  }

  virtual std::map<std::string, uint64_t>
  clear() OVERRIDE
  {
    purge_tree_walker<my_btree, UseConcurrencyControl> w;
    btr.tree_walk(w);
    btr.clear();
#ifdef TXN_BTREE_DUMP_PURGE_STATS
    std::cerr << "purging kvdb index: " << name << std::endl;
    w.dump_stats();
#endif
    return std::map<std::string, uint64_t>();
  }

  // templated interface

  template <typename FieldsMask = AllFields>
  inline bool search(
      kvdb_txn &t, const key_type &k, value_type &v,
      FieldsMask fm = FieldsMask());

  template <typename FieldsMask = AllFields>
  inline void search_range_call(
      kvdb_txn &t, const key_type &lower, const key_type *upper,
      search_range_callback &callback,
      bool no_key_results = false /* skip decoding of keys? */,
      FieldsMask fm = FieldsMask());

  // a lower-level variant which does not bother to decode the key/values
  inline void bytes_search_range_call(
      kvdb_txn &t, const key_type &lower, const key_type *upper,
      bytes_search_range_callback &callback,
      size_t value_fields_prefix = std::numeric_limits<size_t>::max());

  template <typename FieldsMask = AllFields>
  inline void put(
      kvdb_txn &t, const key_type &k, const value_type &v,
      FieldsMask fm = FieldsMask());

  inline void insert(
      kvdb_txn &t, const key_type &k, const value_type &v);

  inline void remove(
      kvdb_txn &t, const key_type &k);

private:

  template <typename Callback, typename KeyReader, typename ValueReader>
  inline void do_search_range_call(
      kvdb_txn &t, const key_type &lower, const key_type *upper,
      Callback &callback, KeyReader &kr, ValueReader &vr);

  std::string name;
  typedef
    typename std::conditional<
      UseConcurrencyControl,
      concurrent_btree,
      single_threaded_btree>::type
    my_btree;
   my_btree btr;
};

template <bool UseConcurrencyControl>
class kvdb_database : public abstract_db {
public:

  template <typename Schema>
  struct IndexType {
    typedef kvdb_index<Schema, UseConcurrencyControl> type;
    typedef std::shared_ptr<type> ptr_type;
  };

  template <enum abstract_db::TxnProfileHint hint>
  struct TransactionType
  {
    typedef kvdb_txn type;
    typedef std::shared_ptr<type> ptr_type;
  };

  template <enum abstract_db::TxnProfileHint hint>
  inline typename TransactionType<hint>::ptr_type
  new_txn(uint64_t txn_flags, str_arena &arena) const
  {
    return std::make_shared<typename TransactionType<hint>::type>(txn_flags, arena);
  }

  typedef transaction_abort_exception abort_exception_type;

  virtual void
  do_txn_epoch_sync() const OVERRIDE
  {
  }

  virtual void
  do_txn_finish() const OVERRIDE
  {
  }

  template <typename Schema>
  inline typename IndexType<Schema>::ptr_type
  open_index(const std::string &name,
             size_t value_size_hint,
             bool mostly_append)
  {
    return std::make_shared<typename IndexType<Schema>::type>(
        value_size_hint, mostly_append, name);
  }
};

template <typename Schema, bool UseConcurrencyControl>
template <typename FieldsMask>
bool
kvdb_index<Schema, UseConcurrencyControl>::search(
    kvdb_txn &t, const key_type &k, value_type &v,
    FieldsMask fm)
{
  key_writer kw(&k);
  const std::string * const keypx =
    kw.fully_materialize(false, t.string_allocator());

  typedef basic_kvdb_record<UseConcurrencyControl> kvdb_record;
  ANON_REGION("kvdb_ordered_index::get:", &private_::kvdb_get_probe0_cg);
  typename my_btree::value_type p = 0;
  if (btr.search(varkey(*keypx), p)) {
    ANON_REGION("kvdb_ordered_index::get:do_read:", &private_::kvdb_get_probe1_cg);
    const kvdb_record * const r = reinterpret_cast<const kvdb_record *>(p);
    r->prefetch();
    single_value_reader vr(v, FieldsMask::value);
    r->do_read(vr, t.string_allocator());
    return true;
  }
  return false;
}

template <typename Schema, bool UseConcurrencyControl>
template <typename FieldsMask>
void
kvdb_index<Schema, UseConcurrencyControl>::search_range_call(
    kvdb_txn &t, const key_type &lower, const key_type *upper,
    search_range_callback &callback,
    bool no_key_results,
    FieldsMask fm)
{
  key_reader kr(no_key_results);
  value_reader vr(FieldsMask::value);

  do_search_range_call(t, lower, upper, callback, kr, vr);
}

template <typename Schema, bool UseConcurrencyControl>
void
kvdb_index<Schema, UseConcurrencyControl>::bytes_search_range_call(
    kvdb_txn &t, const key_type &lower, const key_type *upper,
    bytes_search_range_callback &callback,
    size_t value_fields_prefix)
{
  const value_encoder_type value_encoder;
  const size_t max_bytes_read =
    value_encoder.encode_max_nbytes_prefix(value_fields_prefix);
  bytes_key_reader kr;
  bytes_value_reader vr(max_bytes_read);

  do_search_range_call(t, lower, upper, callback, kr, vr);
}

template <typename Schema, bool UseConcurrencyControl>
template <typename FieldsMask>
void
kvdb_index<Schema, UseConcurrencyControl>::put(
    kvdb_txn &t, const key_type &key, const value_type &value,
    FieldsMask fm)
{
  key_writer kw(&key);
  const std::string * const keypx =
    kw.fully_materialize(false, t.string_allocator());
  if (UseConcurrencyControl)
    // XXX: currently unsupported- need to ensure locked values
    // are the canonical versions pointed to by the tree
    ALWAYS_ASSERT(false);
  value_writer vw(&value, FieldsMask::value);
  typename my_btree::value_type v = 0, v_old = 0;
  if (btr.search(varkey(*keypx), v)) {
    kvdb_record * const r = reinterpret_cast<kvdb_record *>(v);
    r->prefetch();
    lock_guard<kvdb_record> guard(*r);
    if (r->do_write(vw))
      return;
    // replace - slow-path
    kvdb_record * const rnew =
      kvdb_record::alloc(*vw.fully_materialize(false, t.string_allocator()));
    btr.insert(varkey(*keypx), (typename my_btree::value_type) rnew, &v_old, 0);
    INVARIANT((typename my_btree::value_type) r == v_old);
    // rcu-free the old record
    kvdb_record::release(r);
    return;
  }

  // also slow-path
  kvdb_record * const rnew =
    kvdb_record::alloc(*vw.fully_materialize(false, t.string_allocator()));
  if (!btr.insert(varkey(*keypx), (typename my_btree::value_type) rnew, &v_old, 0)) {
    kvdb_record * const r = (kvdb_record *) v_old;
    kvdb_record::release(r);
  }
  return;
}

template <typename Schema, bool UseConcurrencyControl>
void
kvdb_index<Schema, UseConcurrencyControl>::insert(
    kvdb_txn &t, const key_type &k, const value_type &v)
{
  key_writer kw(&k);
  const std::string * const keypx =
    kw.fully_materialize(false, t.string_allocator());
  if (UseConcurrencyControl)
    // XXX: currently unsupported- see above
    ALWAYS_ASSERT(false);
  value_writer vw(&v, AllFieldsMask);
  const size_t sz = vw.compute_needed(nullptr, 0);
  kvdb_record * const rec = kvdb_record::alloc(sz);
  vw((uint8_t *) &rec->data[0], 0);
  rec->set_size(sz);
  if (likely(btr.insert_if_absent(varkey(*keypx), (typename my_btree::value_type) rec, nullptr)))
    return;
  kvdb_record::release_no_rcu(rec);
  put(t, k, v);
}

template <typename Schema, bool UseConcurrencyControl>
void
kvdb_index<Schema, UseConcurrencyControl>::remove(
    kvdb_txn &t, const key_type &k)
{
  key_writer kw(&k);
  const std::string * const keypx =
    kw.fully_materialize(false, t.string_allocator());
  ANON_REGION("kvdb_ordered_index::remove:", &private_::kvdb_remove_probe0_cg);
  if (UseConcurrencyControl)
    // XXX: currently unsupported- see above
    ALWAYS_ASSERT(false);
  typename my_btree::value_type v = 0;
  if (likely(btr.remove(varkey(*keypx), &v))) {
    kvdb_record * const r = reinterpret_cast<kvdb_record *>(v);
    kvdb_record::release(r);
  }
}

template <typename Schema, bool UseConcurrencyControl>
template <typename Callback, typename KeyReader, typename ValueReader>
void
kvdb_index<Schema, UseConcurrencyControl>::do_search_range_call(
      kvdb_txn &t, const key_type &lower, const key_type *upper,
      Callback &callback, KeyReader &kr, ValueReader &vr)
{
  key_writer lower_key_writer(&lower);
  key_writer upper_key_writer(upper);
  const std::string * const lower_str =
    lower_key_writer.fully_materialize(false, t.string_allocator());
  const std::string * const upper_str =
    upper_key_writer.fully_materialize(false, t.string_allocator());

  kvdb_wrapper_search_range_callback<
    my_btree,
    Callback,
    KeyReader,
    ValueReader> c(callback, kr, vr, t.string_allocator());

  varkey uppervk;
  if (upper_str)
    uppervk = varkey(*upper_str);
  btr.search_range_call(varkey(*lower_str), upper_str ? &uppervk : nullptr, c, t.string_allocator()());
}

#endif /* _KVDB_WRAPPER_IMPL_H_ */
