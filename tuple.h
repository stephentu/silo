#ifndef _NDB_TUPLE_H_
#define _NDB_TUPLE_H_

#include <atomic>
#include <vector>
#include <string>
#include <utility>
#include <limits>
#include <unordered_map>
#include <ostream>
#include <thread>

#include "amd64.h"
#include "core.h"
#include "counter.h"
#include "macros.h"
#include "varkey.h"
#include "util.h"
#include "rcu.h"
#include "thread.h"
#include "spinlock.h"
#include "small_unordered_map.h"
#include "prefetch.h"
#include "ownership_checker.h"

// debugging tool
//#define TUPLE_LOCK_OWNERSHIP_CHECKING

template <template <typename> class Protocol, typename Traits>
  class transaction; // forward decl

// XXX: hack
extern std::string (*g_proto_version_str)(uint64_t v);

/**
 * A dbtuple is the type of value which we stick
 * into underlying (non-transactional) data structures- it
 * also contains the memory of the value
 */
struct dbtuple {
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

  // lock ownership helpers- works by recording all tuple
  // locks obtained in each transaction, and then when the txn
  // finishes, calling AssertAllTupleLocksReleased(), which makes
  // sure the current thread is no longer the owner of any locks
  // acquired during the txn
#ifdef TUPLE_LOCK_OWNERSHIP_CHECKING
  static inline void
  TupleLockRegionBegin()
  {
    ownership_checker<dbtuple, dbtuple>::NodeLockRegionBegin();
  }

  // is used to signal the end of a tuple lock region
  static inline void
  AssertAllTupleLocksReleased()
  {
    ownership_checker<dbtuple, dbtuple>::AssertAllNodeLocksReleased();
  }
private:
  static inline void
  AddTupleToLockRegion(const dbtuple *n)
  {
    ownership_checker<dbtuple, dbtuple>::AddNodeToLockRegion(n);
  }
#endif

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

#ifdef TUPLE_MAGIC
  class magic_failed_exception: public std::exception {};
  static const uint8_t TUPLE_MAGIC = 0x29U;
  uint8_t magic;
  inline ALWAYS_INLINE void CheckMagic() const
  {
    if (unlikely(magic != TUPLE_MAGIC)) {
      print(1);
      // so we can catch it later and print out useful debugging output
      throw magic_failed_exception();
    }
  }
#else
  inline ALWAYS_INLINE void CheckMagic() const {}
#endif

  // NB(stephentu): ABA problem happens after some multiple of
  // 2^(NBits(version_t)-6) concurrent modifications- somewhat low probability
  // event, so we let it happen
  //
  // <-- low bits
  // [ locked | deleting | write_intent | modifying | latest | version ]
  // [  0..1  |   1..2   |    2..3      |   3..4    |  4..5  |  5..32  ]
  volatile version_t hdr;

#ifdef TUPLE_LOCK_OWNERSHIP_CHECKING
  std::thread::id lock_owner;
#endif

  // uninterpreted TID
  tid_t version;

  // small sizes on purpose
  node_size_type size; // actual size of record
                       // (only meaningful is the deleting bit is not set)
  node_size_type alloc_size; // max size record allowed. is the space
                             // available for the record buf

  dbtuple *next; // be very careful about traversing this pointer,
                 // GC is capable of reaping it at certain (well defined)
                 // points, and will not bother to set it to null

#ifdef TUPLE_CHECK_KEY
  // for debugging
  std::string key;
  void *tree;
#endif

#ifdef CHECK_INVARIANTS
  // for debugging
  std::atomic<uint64_t> opaque;
#endif

  // must be last field
  uint8_t value_start[0];

  void print(std::ostream &o, unsigned len) const;

private:
  // private ctor/dtor b/c we do some special memory stuff
  // ctors start node off as latest node

  static inline ALWAYS_INLINE node_size_type
  CheckBounds(size_type s)
  {
    INVARIANT(s <= std::numeric_limits<node_size_type>::max());
    return s;
  }

  dbtuple(const dbtuple &) = delete;
  dbtuple(dbtuple &&) = delete;
  dbtuple &operator=(const dbtuple &) = delete;

  // creates a (new) record with a tentative value at MAX_TID
  dbtuple(size_type size, size_type alloc_size, bool acquire_lock)
    :
#ifdef TUPLE_MAGIC
      magic(TUPLE_MAGIC),
#endif
      hdr(HDR_LATEST_MASK |
          (acquire_lock ? (HDR_LOCKED_MASK | HDR_WRITE_INTENT_MASK) : 0) |
          (!size ? HDR_DELETING_MASK : 0))
#ifdef TUPLE_LOCK_OWNERSHIP_CHECKING
      , lock_owner()
#endif
      , version(MAX_TID)
      , size(CheckBounds(size))
      , alloc_size(CheckBounds(alloc_size))
      , next(nullptr)
#ifdef TUPLE_CHECK_KEY
      , key()
      , tree(nullptr)
#endif
#ifdef CHECK_INVARIANTS
      , opaque(0)
#endif
  {
    INVARIANT(((char *)this) + sizeof(*this) == (char *) &value_start[0]);
    INVARIANT(is_latest());
    INVARIANT(size || is_deleting());
    ++g_evt_dbtuple_creates;
    g_evt_dbtuple_bytes_allocated += alloc_size + sizeof(dbtuple);
#ifdef TUPLE_LOCK_OWNERSHIP_CHECKING
    if (acquire_lock) {
      lock_owner = std::this_thread::get_id();
      AddTupleToLockRegion(this);
      INVARIANT(is_lock_owner());
    }
#endif
    COMPILER_MEMORY_FENCE; // for acquire_lock
  }

  // creates a record at version derived from base
  // (inheriting its value).
  dbtuple(tid_t version,
          struct dbtuple *base,
          size_type alloc_size,
          bool set_latest)
    :
#ifdef TUPLE_MAGIC
      magic(TUPLE_MAGIC),
#endif
      hdr(set_latest ? HDR_LATEST_MASK : 0)
#ifdef TUPLE_LOCK_OWNERSHIP_CHECKING
      , lock_owner()
#endif
      , version(version)
      , size(base->size)
      , alloc_size(CheckBounds(alloc_size))
      , next(base->next)
#ifdef TUPLE_CHECK_KEY
      , key()
      , tree(nullptr)
#endif
#ifdef CHECK_INVARIANTS
      , opaque(0)
#endif
  {
    INVARIANT(size <= alloc_size);
    INVARIANT(set_latest == is_latest());
    if (base->is_deleting())
      mark_deleting();
    NDB_MEMCPY(&value_start[0], base->get_value_start(), size);
    ++g_evt_dbtuple_creates;
    g_evt_dbtuple_bytes_allocated += alloc_size + sizeof(dbtuple);
  }

  // creates a spill record, copying in the *old* value if necessary, but
  // setting the size to the *new* value
  dbtuple(tid_t version,
          const_record_type r,
          size_type old_size,
          size_type new_size,
          size_type alloc_size,
          struct dbtuple *next,
          bool set_latest,
          bool needs_old_value)
    :
#ifdef TUPLE_MAGIC
      magic(TUPLE_MAGIC),
#endif
      hdr((set_latest ? HDR_LATEST_MASK : 0) | (!new_size ? HDR_DELETING_MASK : 0))
#ifdef TUPLE_LOCK_OWNERSHIP_CHECKING
      , lock_owner()
#endif
      , version(version)
      , size(CheckBounds(new_size))
      , alloc_size(CheckBounds(alloc_size))
      , next(next)
#ifdef TUPLE_CHECK_KEY
      , key()
      , tree(nullptr)
#endif
#ifdef CHECK_INVARIANTS
      , opaque(0)
#endif
  {
    INVARIANT(!needs_old_value || old_size <= alloc_size);
    INVARIANT(new_size <= alloc_size);
    INVARIANT(set_latest == is_latest());
    INVARIANT(new_size || is_deleting());
    if (needs_old_value)
      NDB_MEMCPY(&value_start[0], r, old_size);
    ++g_evt_dbtuple_creates;
    g_evt_dbtuple_bytes_allocated += alloc_size + sizeof(dbtuple);
  }

  friend class rcu;
  ~dbtuple();

  static event_avg_counter g_evt_avg_dbtuple_stable_version_spins;
  static event_avg_counter g_evt_avg_dbtuple_lock_acquire_spins;
  static event_avg_counter g_evt_avg_dbtuple_read_retries;

public:

  enum ReadStatus {
    READ_FAILED,
    READ_EMPTY,
    READ_RECORD,
  };

  inline void
  prefetch() const
  {
#ifdef TUPLE_PREFETCH
    prefetch_bytes(this, sizeof(*this) + alloc_size);
#endif
  }

  // gcs *this* instance, ignoring the chain
  void gc_this();

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

#ifdef TUPLE_LOCK_OWNERSHIP_CHECKING
  inline bool
  is_lock_owner() const
  {
    return std::this_thread::get_id() == lock_owner;
  }
#else
  inline bool
  is_lock_owner() const
  {
    return true;
  }
#endif

  inline version_t
  lock(bool write_intent)
  {
    // XXX: implement SPINLOCK_BACKOFF
    CheckMagic();
#ifdef ENABLE_EVENT_COUNTERS
    unsigned nspins = 0;
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
#ifdef TUPLE_LOCK_OWNERSHIP_CHECKING
    lock_owner = std::this_thread::get_id();
    AddTupleToLockRegion(this);
    INVARIANT(is_lock_owner());
#endif
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
    CheckMagic();
    version_t v = hdr;
    bool newv = false;
    INVARIANT(IsLocked(v));
    INVARIANT(is_lock_owner());
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
#ifdef TUPLE_LOCK_OWNERSHIP_CHECKING
    lock_owner = std::thread::id();
    INVARIANT(!is_lock_owner());
#endif
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
    CheckMagic();
    // the lock on the latest version guards non-latest versions
    INVARIANT(!is_latest() || is_locked());
    INVARIANT(!is_latest() || is_lock_owner());
    INVARIANT(!is_deleting());
    hdr |= HDR_DELETING_MASK;
  }

  inline void
  clear_deleting()
  {
    CheckMagic();
    INVARIANT(is_locked());
    INVARIANT(is_lock_owner());
    INVARIANT(is_deleting());
    hdr &= ~HDR_DELETING_MASK;
  }

  inline bool
  is_modifying() const
  {
    return IsModifying(hdr);
  }

  inline void
  mark_modifying()
  {
    CheckMagic();
    version_t v = hdr;
    INVARIANT(IsLocked(v));
    INVARIANT(is_lock_owner());
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
    CheckMagic();
    INVARIANT(is_locked());
    INVARIANT(is_lock_owner());
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
    // {locked, write_intent, latest} bits?
    const version_t MODULO_BITS =
      (HDR_LOCKED_MASK | HDR_WRITE_INTENT_MASK | HDR_LATEST_MASK);
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
    CheckMagic();
    this->next = next;
  }

  inline void
  clear_next()
  {
    CheckMagic();
    this->next = nullptr;
  }

  inline ALWAYS_INLINE uint8_t *
  get_value_start()
  {
    CheckMagic();
    return &value_start[0];
  }

  inline ALWAYS_INLINE const uint8_t *
  get_value_start() const
  {
    return &value_start[0];
  }

  // worst name ever...
  inline bool
  is_not_behind(tid_t t) const
  {
    return version <= t;
  }

private:

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
  template <typename Reader, typename StringAllocator>
  static ReadStatus
  record_at_chain(
      const dbtuple *starting, tid_t t, tid_t &start_t,
      Reader &reader, StringAllocator &sa, bool allow_write_intent)
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
      const size_t read_sz = IsDeleting(v) ? 0 : current->size;
      if (unlikely(read_sz && !reader(current->get_value_start(), read_sz, sa)))
        goto retry;
      if (unlikely(!current->reader_check_version(v)))
        goto retry;
      return read_sz ? READ_RECORD : READ_EMPTY;
    } else {
      p = current->get_next();
    }
    if (unlikely(!current->reader_check_version(v)))
      goto retry;
    if (p) {
      current = p;
      goto loop;
    }
    // see note in record_at()
    start_t = MIN_TID;
    return READ_EMPTY;
  retry:
#ifdef ENABLE_EVENT_COUNTERS
    ++nretries;
#endif
    goto loop;
  }

  // we force one level of inlining, but don't force record_at_chain()
  // to be inlined
  template <typename Reader, typename StringAllocator>
  inline ALWAYS_INLINE ReadStatus
  record_at(
      tid_t t, tid_t &start_t,
      Reader &reader, StringAllocator &sa, bool allow_write_intent) const
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
      return READ_EMPTY;
    }
  loop:
    const version_t v = reader_stable_version(allow_write_intent);
    const struct dbtuple *p;
    const bool found = is_not_behind(t);
    if (found) {
      //if (unlikely(!IsLatest(v)))
      //  return READ_FAILED;
      start_t = version;
      const size_t read_sz = IsDeleting(v) ? 0 : size;
      if (unlikely(read_sz && !reader(get_value_start(), read_sz, sa)))
        goto retry;
      if (unlikely(!reader_check_version(v)))
        goto retry;
      return read_sz ? READ_RECORD : READ_EMPTY;
    } else {
      p = get_next();
    }
    if (unlikely(!reader_check_version(v)))
      goto retry;
    if (p)
      return record_at_chain(p, t, start_t, reader, sa, allow_write_intent);
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
    return READ_EMPTY;
  retry:
#ifdef ENABLE_EVENT_COUNTERS
    ++nretries;
#endif
    goto loop;
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
  template <typename Reader, typename StringAllocator>
  inline ALWAYS_INLINE ReadStatus
  stable_read(
      tid_t t, tid_t &start_t,
      Reader &reader, StringAllocator &sa,
      bool allow_write_intent) const
  {
    return record_at(t, start_t, reader, sa, allow_write_intent);
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

  struct write_record_ret {
    write_record_ret() : head_(), rest_(), forced_spill_() {}
    write_record_ret(dbtuple *head, dbtuple* rest, bool forced_spill)
      : head_(head), rest_(rest), forced_spill_(forced_spill)
    {
      INVARIANT(head);
      INVARIANT(head != rest);
      INVARIANT(!forced_spill || rest);
    }
    dbtuple *head_;
    dbtuple *rest_;
    bool forced_spill_;
  };

  // XXX: kind of hacky, but we do this to avoid virtual
  // functions / passing multiple function pointers around
  enum TupleWriterMode {
    TUPLE_WRITER_NEEDS_OLD_VALUE, // all three args ignored
    TUPLE_WRITER_COMPUTE_NEEDED,
    TUPLE_WRITER_COMPUTE_DELTA_NEEDED, // last two args ignored
    TUPLE_WRITER_DO_WRITE,
    TUPLE_WRITER_DO_DELTA_WRITE,
  };
  typedef size_t (*tuple_writer_t)(TupleWriterMode, const void *, uint8_t *, size_t);

  /**
   * Always writes the record in the latest (newest) version slot,
   * not asserting whether or not inserting r @ t would violate the
   * sorted order invariant
   *
   * ret.first  = latest tuple after the write (guaranteed to not be nullptr)
   * ret.second = old version of tuple, iff no overwrite (can be nullptr)
   *
   * Note: if this != ret.first, then we need a tree replacement
   */
  template <typename Transaction>
  write_record_ret
  write_record_at(const Transaction *txn, tid_t t,
                  const void *v, tuple_writer_t writer)
  {
#ifndef DISABLE_OVERWRITE_IN_PLACE
    CheckMagic();
    INVARIANT(is_locked());
    INVARIANT(is_lock_owner());
    INVARIANT(is_latest());
    INVARIANT(is_write_intent());

    const size_t new_sz =
      v ? writer(TUPLE_WRITER_COMPUTE_NEEDED, v, get_value_start(), size) : 0;
    INVARIANT(!v || new_sz);
    INVARIANT(is_deleting() || size);
    const size_t old_sz = is_deleting() ? 0 : size;

    if (!new_sz)
      ++g_evt_dbtuple_logical_deletes;

    // try to overwrite this record
    if (likely(txn->can_overwrite_record_tid(version, t) && old_sz)) {
      INVARIANT(!is_deleting());
      // see if we have enough space
      if (likely(new_sz <= alloc_size)) {
        // directly update in place
        mark_modifying();
        if (v)
          writer(TUPLE_WRITER_DO_WRITE, v, get_value_start(), old_sz);
        version = t;
        size = new_sz;
        if (!new_sz)
          mark_deleting();
        return write_record_ret(this, nullptr, false);
      }

      //std::cerr
      //  << "existing: " << g_proto_version_str(version) << std::endl
      //  << "new     : " << g_proto_version_str(t)       << std::endl
      //  << "alloc_size : " << alloc_size                << std::endl
      //  << "new_sz     : " << new_sz                    << std::endl;

      // keep this tuple in the chain (it's wasteful, but not incorrect)
      // so that cleanup is easier
      //
      // XXX(stephentu): alloc_spill() should acquire the lock on
      // the returned tuple in the ctor, as an optimization

      const bool needs_old_value =
        writer(TUPLE_WRITER_NEEDS_OLD_VALUE, nullptr, nullptr, 0);
      INVARIANT(new_sz);
      INVARIANT(v);
      dbtuple * const rep =
        alloc_spill(t, get_value_start(), old_sz, new_sz,
                    this, true, needs_old_value);
      writer(TUPLE_WRITER_DO_WRITE, v, rep->get_value_start(), old_sz);
      INVARIANT(rep->is_latest());
      INVARIANT(rep->size == new_sz);
      clear_latest();
      ++g_evt_dbtuple_inplace_buf_insufficient;

      // [did not spill because of epochs, need to replace this with rep]
      return write_record_ret(rep, this, false);
    }

    //std::cerr
    //  << "existing: " << g_proto_version_str(version) << std::endl
    //  << "new     : " << g_proto_version_str(t)       << std::endl
    //  << "alloc_size : " << alloc_size                << std::endl
    //  << "new_sz     : " << new_sz                    << std::endl;

    // need to spill
    ++g_evt_dbtuple_spills;
    g_evt_avg_record_spill_len.offer(size);

    if (new_sz <= alloc_size && old_sz) {
      INVARIANT(!is_deleting());
      dbtuple * const spill = alloc(version, this, false);
      INVARIANT(!spill->is_latest());
      mark_modifying();
      set_next(spill);
      if (v)
        writer(TUPLE_WRITER_DO_WRITE, v, get_value_start(), size);
      version = t;
      size = new_sz;
      if (!new_sz)
        mark_deleting();
      return write_record_ret(this, spill, true);
    }

    const bool needs_old_value =
      writer(TUPLE_WRITER_NEEDS_OLD_VALUE, nullptr, nullptr, 0);
    dbtuple * const rep =
      alloc_spill(t, get_value_start(), old_sz, new_sz,
                  this, true, needs_old_value);
    if (v)
      writer(TUPLE_WRITER_DO_WRITE, v, rep->get_value_start(), size);
    INVARIANT(rep->is_latest());
    INVARIANT(rep->size == new_sz);
    INVARIANT(new_sz || rep->is_deleting()); // set by alloc_spill()
    clear_latest();
    ++g_evt_dbtuple_inplace_buf_insufficient_on_spill;
    return write_record_ret(rep, this, true);
#else
    CheckMagic();
    INVARIANT(is_locked());
    INVARIANT(is_lock_owner());
    INVARIANT(is_latest());
    INVARIANT(is_write_intent());

    const size_t new_sz =
      v ? writer(TUPLE_WRITER_COMPUTE_NEEDED, v, get_value_start(), size) : 0;
    INVARIANT(!v || new_sz);
    INVARIANT(is_deleting() || size);
    const size_t old_sz = is_deleting() ? 0 : size;

    if (!new_sz)
      ++g_evt_dbtuple_logical_deletes;

    const bool needs_old_value =
      writer(TUPLE_WRITER_NEEDS_OLD_VALUE, nullptr, nullptr, 0);
    dbtuple * const rep =
      alloc_spill(t, get_value_start(), old_sz, new_sz,
                  this, true, needs_old_value);
    if (v)
      writer(TUPLE_WRITER_DO_WRITE, v, rep->get_value_start(), size);
    INVARIANT(rep->is_latest());
    INVARIANT(rep->size == new_sz);
    INVARIANT(new_sz || rep->is_deleting()); // set by alloc_spill()
    clear_latest();
    ++g_evt_dbtuple_inplace_buf_insufficient_on_spill;
    return write_record_ret(rep, this, true);
#endif
  }

  // NB: we round up allocation sizes because jemalloc will do this
  // internally anyways, so we might as well grab more usable space (really
  // just internal vs external fragmentation)

  static inline dbtuple *
  alloc_first(size_type sz, bool acquire_lock)
  {
    INVARIANT(sz <= std::numeric_limits<node_size_type>::max());
    const size_t max_alloc_sz =
      std::numeric_limits<node_size_type>::max() + sizeof(dbtuple);
    const size_t alloc_sz =
      std::min(
          util::round_up<size_t, allocator::LgAllocAlignment>(sizeof(dbtuple) + sz),
          max_alloc_sz);
    char *p = reinterpret_cast<char *>(rcu::s_instance.alloc(alloc_sz));
    INVARIANT(p);
    INVARIANT((alloc_sz - sizeof(dbtuple)) >= sz);
    return new (p) dbtuple(
        sz, alloc_sz - sizeof(dbtuple), acquire_lock);
  }

  static inline dbtuple *
  alloc(tid_t version, struct dbtuple *base, bool set_latest)
  {
    const size_t max_alloc_sz =
      std::numeric_limits<node_size_type>::max() + sizeof(dbtuple);
    const size_t alloc_sz =
      std::min(
          util::round_up<size_t, allocator::LgAllocAlignment>(sizeof(dbtuple) + base->size),
          max_alloc_sz);
    char *p = reinterpret_cast<char *>(rcu::s_instance.alloc(alloc_sz));
    INVARIANT(p);
    return new (p) dbtuple(
        version, base, alloc_sz - sizeof(dbtuple), set_latest);
  }

  static inline dbtuple *
  alloc_spill(tid_t version, const_record_type value, size_type oldsz,
              size_type newsz, struct dbtuple *next, bool set_latest,
              bool copy_old_value)
  {
    INVARIANT(oldsz <= std::numeric_limits<node_size_type>::max());
    INVARIANT(newsz <= std::numeric_limits<node_size_type>::max());

    const size_t needed_sz =
      copy_old_value ? std::max(newsz, oldsz) : newsz;
    const size_t max_alloc_sz =
      std::numeric_limits<node_size_type>::max() + sizeof(dbtuple);
    const size_t alloc_sz =
      std::min(
          util::round_up<size_t, allocator::LgAllocAlignment>(sizeof(dbtuple) + needed_sz),
          max_alloc_sz);
    char *p = reinterpret_cast<char *>(rcu::s_instance.alloc(alloc_sz));
    INVARIANT(p);
    return new (p) dbtuple(
        version, value, oldsz, newsz,
        alloc_sz - sizeof(dbtuple), next, set_latest, copy_old_value);
  }


private:
  static inline void
  destruct_and_free(dbtuple *n)
  {
    const size_t alloc_sz = n->alloc_size + sizeof(*n);
    n->~dbtuple();
    rcu::s_instance.dealloc(n, alloc_sz);
  }

public:
  static void
  deleter(void *p)
  {
    dbtuple * const n = (dbtuple *) p;
    INVARIANT(!n->is_latest());
    INVARIANT(!n->is_locked());
    INVARIANT(!n->is_modifying());
    destruct_and_free(n);
  }

  static inline void
  release(dbtuple *n)
  {
    if (unlikely(!n))
      return;
    INVARIANT(n->is_locked());
    INVARIANT(!n->is_latest());
    rcu::s_instance.free_with_fn(n, deleter);
  }

  static inline void
  release_no_rcu(dbtuple *n)
  {
    if (unlikely(!n))
      return;
    INVARIANT(!n->is_latest());
    destruct_and_free(n);
  }

  static std::string
  VersionInfoStr(version_t v);

}
#if !defined(TUPLE_CHECK_KEY) && \
    !defined(CHECK_INVARIANTS) && \
    !defined(TUPLE_LOCK_OWNERSHIP_CHECKING)
PACKED
#endif
;

#endif /* _NDB_TUPLE_H_ */
