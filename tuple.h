#ifndef _NDB_TUPLE_H_
#define _NDB_TUPLE_H_

#include <vector>
#include <string>
#include <utility>
#include <limits>
#include <unordered_map>

#include "amd64.h"
#include "core.h"
#include "counter.h"
#include "macros.h"
#include "varkey.h"
#include "util.h"
#include "static_assert.h"
#include "rcu.h"
#include "thread.h"
#include "spinlock.h"
#include "small_unordered_map.h"
#include "prefetch.h"

template <template <typename> class Protocol, typename Traits>
  class transaction; // forward decl

/**
 * A dbtuple is the type of value which we stick
 * into underlying (non-transactional) data structures- it
 * also contains the memory of the value
 */
struct dbtuple : private util::noncopyable {
  friend std::ostream &
  operator<<(std::ostream &o, const dbtuple &tuple);

public:
  // trying to save space by putting constraints
  // on node maximums
  typedef uint32_t version_t;
  typedef uint16_t node_size_type;
  typedef uint64_t tid_t;
  typedef uint8_t * record_type;
  typedef const uint8_t * const_record_type;
  typedef size_t size_type;
  typedef std::string string_type;

  static const tid_t MIN_TID = 0;
  static const tid_t MAX_TID = (tid_t) -1;

private:
  static const version_t HDR_LOCKED_MASK = 0x1;

  static const version_t HDR_DELETING_SHIFT = 1;
  static const version_t HDR_DELETING_MASK = 0x1 << HDR_DELETING_SHIFT;

  static const version_t HDR_WRITE_INTENT_SHIFT = 2;
  static const version_t HDR_WRITE_INTENT_MASK = 0x1 << HDR_WRITE_INTENT_SHIFT;

  static const version_t HDR_MODIFYING_SHIFT = 3;
  static const version_t HDR_MODIFYING_MASK = 0x1 << HDR_MODIFYING_SHIFT;

  static const version_t HDR_LATEST_SHIFT = 4;
  static const version_t HDR_LATEST_MASK = 0x1 << HDR_LATEST_SHIFT;

  static const version_t HDR_VERSION_SHIFT = 5;
  static const version_t HDR_VERSION_MASK = ((version_t)-1) << HDR_VERSION_SHIFT;

public:

  // NB(stephentu): ABA problem happens after some multiple of
  // 2^(NBits(version_t)-6) concurrent modifications- somewhat low probability
  // event, so we let it happen
  //
  // <-- low bits
  // [ locked | deleting | write_intent | modifying | latest | version ]
  // [  0..1  |   1..2   |    2..3      |   3..4    |  4..5  |  5..32  ]
  volatile version_t hdr;

  // uninterpreted TID
  tid_t version;

  // small sizes on purpose
  node_size_type size; // actual size of record (0 implies absent record)
  node_size_type alloc_size; // max size record allowed. is the space
                             // available for the record buf

  dbtuple *next;

  // must be last field
  uint8_t value_start[0];

private:
  // private ctor/dtor b/c we do some special memory stuff
  // ctors start node off as latest node

  static inline ALWAYS_INLINE node_size_type
  CheckBounds(size_type s)
  {
    INVARIANT(s <= std::numeric_limits<node_size_type>::max());
    return s;
  }

  // creates a record with a tentative value at MAX_TID
  dbtuple(const_record_type r,
          size_type size, size_type alloc_size)
    : hdr(HDR_LATEST_MASK),
      version(MAX_TID),
      size(CheckBounds(size)),
      alloc_size(CheckBounds(alloc_size)),
      next(nullptr)
  {
    INVARIANT(((char *)this) + sizeof(*this) == (char *) &value_start[0]);
    INVARIANT(is_latest());
    NDB_MEMCPY(&value_start[0], r, size);
    ++g_evt_dbtuple_creates;
    g_evt_dbtuple_bytes_allocated += alloc_size + sizeof(dbtuple);
  }

  // creates a record with a non-empty, non tentative value
  dbtuple(tid_t version, const_record_type r,
          size_type size, size_type alloc_size,
          struct dbtuple *next, bool set_latest)
    : hdr(set_latest ? HDR_LATEST_MASK : 0),
      version(version),
      size(CheckBounds(size)),
      alloc_size(CheckBounds(alloc_size)),
      next(next)
  {
    INVARIANT(size <= alloc_size);
    INVARIANT(set_latest == is_latest());
    NDB_MEMCPY(&value_start[0], r, size);
    ++g_evt_dbtuple_creates;
    g_evt_dbtuple_bytes_allocated += alloc_size + sizeof(dbtuple);
  }

  friend class rcu;
  ~dbtuple();

  static event_avg_counter g_evt_avg_dbtuple_stable_version_spins;
  static event_avg_counter g_evt_avg_dbtuple_lock_acquire_spins;
  static event_avg_counter g_evt_avg_dbtuple_read_retries;

public:

  inline void
  prefetch() const
  {
#ifdef TUPLE_PREFETCH
    prefetch_bytes(this, sizeof(*this) + alloc_size);
#endif
  }

  // gc_chain() schedules this instance, and all instances
  // reachable from this instance for deletion via RCU.
  void gc_chain();

  size_t
  chain_length() const
  {
    size_t ret = 0;
    const dbtuple * cur = this;
    while (cur) {
      ret++;
      cur = cur->get_next();
    }
    return ret;
  }

  inline bool
  is_locked() const
  {
    return IsLocked(hdr);
  }

  static inline bool
  IsLocked(version_t v)
  {
    return v & HDR_LOCKED_MASK;
  }

  inline version_t
  lock(bool write_intent)
  {
#ifdef ENABLE_EVENT_COUNTERS
    unsigned long nspins = 0;
#endif
    version_t v = hdr;
    const version_t lockmask = write_intent ?
      (HDR_LOCKED_MASK | HDR_WRITE_INTENT_MASK) :
      (HDR_LOCKED_MASK);
    while (IsLocked(v) ||
           !__sync_bool_compare_and_swap(&hdr, v, v | lockmask)) {
      nop_pause();
      v = hdr;
#ifdef ENABLE_EVENT_COUNTERS
      ++nspins;
#endif
    }
    COMPILER_MEMORY_FENCE;
    INVARIANT(IsLocked(hdr));
    INVARIANT(!write_intent || IsWriteIntent(hdr));
    INVARIANT(!IsModifying(hdr));
#ifdef ENABLE_EVENT_COUNTERS
    g_evt_avg_dbtuple_lock_acquire_spins.offer(nspins);
#endif
    return hdr;
  }

  inline void
  unlock()
  {
    version_t v = hdr;
    bool newv = false;
    INVARIANT(IsLocked(v));
    if (IsModifying(v) || IsWriteIntent(v)) {
      newv = true;
      const version_t n = Version(v);
      v &= ~HDR_VERSION_MASK;
      v |= (((n + 1) << HDR_VERSION_SHIFT) & HDR_VERSION_MASK);
    }
    // clear locked + modifying bits
    v &= ~(HDR_LOCKED_MASK | HDR_MODIFYING_MASK | HDR_WRITE_INTENT_MASK);
    if (newv) {
      INVARIANT(!reader_check_version(v));
      INVARIANT(!writer_check_version(v));
    }
    INVARIANT(!IsLocked(v));
    INVARIANT(!IsModifying(v));
    INVARIANT(!IsWriteIntent(v));
    COMPILER_MEMORY_FENCE;
    hdr = v;
  }

  inline bool
  is_deleting() const
  {
    return IsDeleting(hdr);
  }

  static inline bool
  IsDeleting(version_t v)
  {
    return v & HDR_DELETING_MASK;
  }

  inline void
  mark_deleting()
  {
    // the lock on the latest version guards non-latest versions
    INVARIANT(!is_latest() || is_locked());
    INVARIANT(!is_deleting());
    hdr |= HDR_DELETING_MASK;
  }

  inline bool
  is_modifying() const
  {
    return IsModifying(hdr);
  }

  inline void
  mark_modifying()
  {
    version_t v = hdr;
    INVARIANT(IsLocked(v));
    //INVARIANT(!IsModifying(v)); // mark_modifying() must be re-entrant
    v |= HDR_MODIFYING_MASK;
    COMPILER_MEMORY_FENCE; // XXX: is this fence necessary?
    hdr = v;
    COMPILER_MEMORY_FENCE;
  }

  static inline bool
  IsModifying(version_t v)
  {
    return v & HDR_MODIFYING_MASK;
  }

  inline bool
  is_write_intent() const
  {
    return IsWriteIntent(hdr);
  }

  static inline bool
  IsWriteIntent(version_t v)
  {
    return v & HDR_WRITE_INTENT_MASK;
  }

  inline bool
  is_latest() const
  {
    return IsLatest(hdr);
  }

  static inline bool
  IsLatest(version_t v)
  {
    return v & HDR_LATEST_MASK;
  }

  inline void
  clear_latest()
  {
    INVARIANT(is_locked());
    INVARIANT(is_latest());
    hdr &= ~HDR_LATEST_MASK;
  }

  static inline version_t
  Version(version_t v)
  {
    return (v & HDR_VERSION_MASK) >> HDR_VERSION_SHIFT;
  }

  inline version_t
  reader_stable_version(bool allow_write_intent) const
  {
    version_t v = hdr;
#ifdef ENABLE_EVENT_COUNTERS
    unsigned long nspins = 0;
#endif
    while (IsModifying(v) ||
           (!allow_write_intent && IsWriteIntent(v))) {
      nop_pause();
      v = hdr;
#ifdef ENABLE_EVENT_COUNTERS
      ++nspins;
#endif
    }
    COMPILER_MEMORY_FENCE;
#ifdef ENABLE_EVENT_COUNTERS
    g_evt_avg_dbtuple_stable_version_spins.offer(nspins);
#endif
    return v;
  }

  /**
   * returns true if succeeded, false otherwise
   */
  inline bool
  try_writer_stable_version(version_t &v, unsigned int spins) const
  {
    v = hdr;
    while (IsWriteIntent(v) && spins--) {
      INVARIANT(IsLocked(v));
      nop_pause();
      v = hdr;
    }
    const bool ret = !IsWriteIntent(v);
    COMPILER_MEMORY_FENCE;
    INVARIANT(ret || IsLocked(v));
    INVARIANT(!ret || !IsModifying(v));
    return ret;
  }

  inline version_t
  unstable_version() const
  {
    return hdr;
  }

  inline bool
  reader_check_version(version_t version) const
  {
    COMPILER_MEMORY_FENCE;
    // are the versions the same, modulo the
    // {locked, deleting, write_intent, latest} bits?
    const version_t MODULO_BITS =
      (HDR_LOCKED_MASK | HDR_DELETING_MASK |
       HDR_WRITE_INTENT_MASK | HDR_LATEST_MASK);
    return (hdr & ~MODULO_BITS) == (version & ~MODULO_BITS);
  }

  inline bool
  writer_check_version(version_t version) const
  {
    COMPILER_MEMORY_FENCE;
    return hdr == version;
  }

  inline ALWAYS_INLINE struct dbtuple *
  get_next()
  {
    return next;
  }

  inline const struct dbtuple *
  get_next() const
  {
    return next;
  }

  inline ALWAYS_INLINE void
  set_next(struct dbtuple *next)
  {
    this->next = next;
  }

  inline void
  clear_next()
  {
    this->next = nullptr;
  }

  inline ALWAYS_INLINE char *
  get_value_start()
  {
    return (char *) &value_start[0];
  }

  inline ALWAYS_INLINE const char *
  get_value_start() const
  {
    return (const char *) &value_start[0];
  }

private:

  inline bool
  is_not_behind(tid_t t) const
  {
    return version <= t;
  }

#ifdef ENABLE_EVENT_COUNTERS
  struct scoped_recorder {
    scoped_recorder(unsigned long &n) : n(&n) {}
    ~scoped_recorder()
    {
      g_evt_avg_dbtuple_read_retries.offer(*n);
    }
  private:
    unsigned long *n;
  };
#endif

  // written to be non-recursive
  static bool
  record_at_chain(const dbtuple *starting,
                  tid_t t, tid_t &start_t, string_type &r,
                  size_t max_len, bool allow_write_intent)
  {
#ifdef ENABLE_EVENT_COUNTERS
    unsigned long nretries = 0;
    scoped_recorder rec(nretries);
#endif
    const dbtuple *current = starting;
  loop:
    INVARIANT(current->version != MAX_TID);
    const version_t v = current->reader_stable_version(allow_write_intent);
    const struct dbtuple *p;
    const bool found = current->is_not_behind(t);
    if (found) {
      start_t = current->version;
      const size_t read_sz = std::min(static_cast<size_t>(current->size), max_len);
      r.assign(current->get_value_start(), read_sz);
      if (unlikely(!current->reader_check_version(v))) {
#ifdef ENABLE_EVENT_COUNTERS
        ++nretries;
#endif
        goto loop;
      }
      return true;
    } else {
      p = current->get_next();
    }
    if (unlikely(!current->reader_check_version(v))) {
#ifdef ENABLE_EVENT_COUNTERS
      ++nretries;
#endif
      goto loop;
    }
    if (p) {
      current = p;
      goto loop;
    }
    // see note in record_at()
    start_t = MIN_TID;
    r.clear();
    return true;
  }

  // we force one level of inlining, but don't force record_at_chain()
  // to be inlined
  inline ALWAYS_INLINE bool
  record_at(tid_t t, tid_t &start_t, string_type &r,
            size_t max_len, bool allow_write_intent) const
  {
#ifdef ENABLE_EVENT_COUNTERS
    unsigned long nretries = 0;
    scoped_recorder rec(nretries);
#endif
    if (unlikely(version == MAX_TID)) {
      // XXX(stephentu): HACK! we use MAX_TID to indicate a tentative
      // "insert"- the actual latest value is empty.
      //
      // since our system is screwed anyways if we ever reach MAX_TID, this
      // is OK for now, but a real solution should exist at some point
      start_t = MIN_TID;
      r.clear();
      return true;
    }
  retry:
    const version_t v = reader_stable_version(allow_write_intent);
    const struct dbtuple *p;
    const bool found = is_not_behind(t);
    if (found) {
      if (unlikely(!IsLatest(v)))
        return false;
      start_t = version;
      const size_t read_sz = std::min(static_cast<size_t>(size), max_len);
      r.assign(get_value_start(), read_sz);
      if (unlikely(!reader_check_version(v))) {
#ifdef ENABLE_EVENT_COUNTERS
        ++nretries;
#endif
        goto retry;
      }
      return true;
    } else {
      p = get_next();
    }
    if (unlikely(!reader_check_version(v))) {
#ifdef ENABLE_EVENT_COUNTERS
      ++nretries;
#endif
      goto retry;
    }
    if (p)
      return record_at_chain(p, t, start_t, r, max_len, allow_write_intent);
    // NB(stephentu): if we reach the end of a chain then we assume that
    // the record exists as a deleted record.
    //
    // This is safe because we have been very careful to not garbage collect
    // elements along the chain until it is guaranteed that the record
    // is superceded by later record in any consistent read. Therefore,
    // if we reach the end of the chain, then it *must* be the case that
    // the record does not actually exist.
    //
    // Note that MIN_TID is the *wrong* tid to use here given wrap-around- we
    // really should be setting this value to the tid which represents the
    // oldest TID possible in the system. But we currently don't implement
    // wrap around
    start_t = MIN_TID;
    r.clear();
    return true;
  }

  static event_counter g_evt_dbtuple_creates;
  static event_counter g_evt_dbtuple_logical_deletes;
  static event_counter g_evt_dbtuple_physical_deletes;
  static event_counter g_evt_dbtuple_bytes_allocated;
  static event_counter g_evt_dbtuple_bytes_freed;
  static event_counter g_evt_dbtuple_spills;
  static event_counter g_evt_dbtuple_inplace_buf_insufficient;
  static event_counter g_evt_dbtuple_inplace_buf_insufficient_on_spill;
  static event_avg_counter g_evt_avg_record_spill_len;

public:

  /**
   * Read the record at tid t. Returns true if such a record exists, false
   * otherwise (ie the record was GC-ed, or other reasons). On a successful
   * read, the value @ start_t will be stored in r
   *
   * NB(stephentu): calling stable_read() while holding the lock
   * is an error- this will cause deadlock
   */
  inline ALWAYS_INLINE bool
  stable_read(tid_t t, tid_t &start_t, string_type &r,
              bool allow_write_intent,
              size_t max_len = string_type::npos) const
  {
    INVARIANT(max_len > 0); // otherwise something will probably break
    return record_at(t, start_t, r, max_len, allow_write_intent);
  }

  inline bool
  is_latest_version(tid_t t) const
  {
    return is_latest() && is_not_behind(t);
  }

  bool
  stable_is_latest_version(tid_t t) const
  {
    version_t v = 0;
    if (!try_writer_stable_version(v, 16))
      return false;
    // now v is a stable version
    INVARIANT(!IsWriteIntent(v));
    INVARIANT(!IsModifying(v));
    const bool ret = IsLatest(v) && is_not_behind(t);
    // only check_version() if the answer would be true- otherwise,
    // no point in doing a version check
    if (ret && writer_check_version(v))
      return true;
    else
      // no point in retrying, since we know it will fail (since we had a
      // version change)
      return false;
  }

  inline bool
  latest_value_is_nil() const
  {
    return is_latest() && size == 0;
  }

  inline bool
  stable_latest_value_is_nil() const
  {
    version_t v = 0;
    if (!try_writer_stable_version(v, 16))
      return false;
    INVARIANT(!IsWriteIntent(v));
    INVARIANT(!IsModifying(v));
    const bool ret = IsLatest(v) && size == 0;
    if (ret && writer_check_version(v))
      return true;
    else
      return false;
  }

  typedef std::pair<bool, dbtuple *> write_record_ret;

  /**
   * Always writes the record in the latest (newest) version slot,
   * not asserting whether or not inserting r @ t would violate the
   * sorted order invariant
   *
   * XXX: document return value
   */
  template <typename Transaction>
  write_record_ret
  write_record_at(const Transaction *txn, tid_t t, const_record_type r, size_type sz)
  {
    INVARIANT(is_locked());
    INVARIANT(is_latest());
    INVARIANT(is_write_intent());
    INVARIANT(!is_deleting());

    if (!sz)
      ++g_evt_dbtuple_logical_deletes;

    // try to overwrite this record
    if (likely(txn->can_overwrite_record_tid(version, t))) {
      // see if we have enough space

      if (likely(sz <= alloc_size)) {
        // directly update in place
        mark_modifying();
        version = t;
        size = sz;
        NDB_MEMCPY(get_value_start(), r, sz);
        return write_record_ret(false, NULL);
      }

      // keep in the chain (it's wasteful, but not incorrect)
      // so that cleanup is easier
      dbtuple * const rep = alloc(t, r, sz, this, true);
      INVARIANT(rep->is_latest());
      clear_latest();
      ++g_evt_dbtuple_inplace_buf_insufficient;
      return write_record_ret(false, rep);
    }

    // need to spill
    ++g_evt_dbtuple_spills;
    g_evt_avg_record_spill_len.offer(size);

    char * const vstart = get_value_start();

    if (sz <= alloc_size) {
      dbtuple * const spill = alloc(version, (const_record_type) vstart, size, get_next(), false);
      INVARIANT(!spill->is_latest());
      mark_modifying();
      set_next(spill);
      version = t;
      size = sz;
      NDB_MEMCPY(vstart, r, sz);
      return write_record_ret(true, NULL);
    }

    dbtuple * const rep = alloc(t, r, sz, this, true);
    INVARIANT(rep->is_latest());
    clear_latest();
    ++g_evt_dbtuple_inplace_buf_insufficient_on_spill;
    return write_record_ret(true, rep);
  }

  // NB: we round up allocation sizes because jemalloc will do this
  // internally anyways, so we might as well grab more usable space (really
  // just internal vs external fragmentation)

  static inline dbtuple *
  alloc_first(const_record_type value, size_type sz)
  {
    INVARIANT(sz <= std::numeric_limits<node_size_type>::max());
    const size_t max_alloc_sz =
      std::numeric_limits<node_size_type>::max() + sizeof(dbtuple);
    const size_t alloc_sz =
      std::min(
          util::round_up<size_t, /* lgbase*/ 4>(sizeof(dbtuple) + sz),
          max_alloc_sz);
    char *p = (char *) malloc(alloc_sz);
    INVARIANT(p);
    INVARIANT((alloc_sz - sizeof(dbtuple)) >= sz);
    return new (p) dbtuple(
        value, sz, alloc_sz - sizeof(dbtuple));
  }

  static inline dbtuple *
  alloc(tid_t version, const_record_type value, size_type sz, struct dbtuple *next, bool set_latest)
  {
    INVARIANT(sz <= std::numeric_limits<node_size_type>::max());
    const size_t max_alloc_sz =
      std::numeric_limits<node_size_type>::max() + sizeof(dbtuple);
    const size_t alloc_sz =
      std::min(
          util::round_up<size_t, /* lgbase*/ 4>(sizeof(dbtuple) + sz),
          max_alloc_sz);
    char *p = (char *) malloc(alloc_sz);
    INVARIANT(p);
    return new (p) dbtuple(
        version, value, sz,
        alloc_sz - sizeof(dbtuple), next, set_latest);
  }

  static void
  deleter(void *p)
  {
    dbtuple * const n = (dbtuple *) p;
    INVARIANT(n->is_deleting());
    INVARIANT(!n->is_locked());
    INVARIANT(!n->is_modifying());
    n->~dbtuple();
    free(n);
  }

  static inline void
  release(dbtuple *n)
  {
    if (unlikely(!n))
      return;
    n->mark_deleting();
    rcu::free_with_fn(n, deleter);
  }

  static inline void
  release_no_rcu(dbtuple *n)
  {
    if (unlikely(!n))
      return;
#ifdef CHECK_INVARIANTS
    n->lock(false);
    n->mark_deleting();
    n->unlock();
#endif
    n->~dbtuple();
    free(n);
  }

  static std::string
  VersionInfoStr(version_t v);

}
PACKED
;

#endif /* _NDB_TUPLE_H_ */
