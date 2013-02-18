#ifndef _NDB_TXN_H_
#define _NDB_TXN_H_

#include <malloc.h>
#include <stdint.h>
#include <sys/types.h>
#include <pthread.h>

#include <map>
#include <iostream>
#include <vector>
#include <string>
#include <utility>
#include <stdexcept>

#include <tr1/unordered_map>

#include "amd64.h"
#include "btree.h"
#include "core.h"
#include "counter.h"
#include "macros.h"
#include "varkey.h"
#include "util.h"
#include "static_assert.h"
#include "rcu.h"
#include "thread.h"

// forward decl
class txn_btree;

class transaction_unusable_exception {};
class transaction_read_only_exception {};

class transaction : private util::noncopyable {
protected:
  friend class txn_btree;
  friend class txn_context;
  class key_range_t;
  friend std::ostream &operator<<(std::ostream &, const key_range_t &);

public:

  typedef uint64_t tid_t;
  typedef uint8_t* record_t;

  // TXN_EMBRYO - the transaction object has been allocated but has not
  // done any operations yet
  enum txn_state { TXN_EMBRYO, TXN_ACTIVE, TXN_COMMITED, TXN_ABRT, };

  enum {
    // use the low-level scan protocol for checking scan consistency,
    // instead of keeping track of absent ranges
    TXN_FLAG_LOW_LEVEL_SCAN = 0x1,

    // true to mark a read-only transaction- if a txn marked read-only
    // does a write, a transaction_read_only_exception is thrown and the
    // txn is aborted
    TXN_FLAG_READ_ONLY = 0x2,

    // XXX: more flags in the future, things like consistency levels
  };

#define ABORT_REASONS(x) \
    x(ABORT_REASON_USER) \
    x(ABORT_REASON_UNSTABLE_READ) \
    x(ABORT_REASON_FUTURE_TID_READ) \
    x(ABORT_REASON_NODE_SCAN_WRITE_VERSION_CHANGED) \
    x(ABORT_REASON_NODE_SCAN_READ_VERSION_CHANGED) \
    x(ABORT_REASON_WRITE_NODE_INTERFERENCE) \
    x(ABORT_REASON_READ_NODE_INTEREFERENCE) \
    x(ABORT_REASON_READ_ABSENCE_INTEREFERENCE) \

  enum abort_reason {
#define ENUM_X(x) x,
    ABORT_REASONS(ENUM_X)
#undef ENUM_X
  };

  static const char *
  AbortReasonStr(abort_reason reason)
  {
    switch (reason) {
#define CASE_X(x) case x: return #x;
    ABORT_REASONS(CASE_X)
#undef CASE_X
    default:
      break;
    }
    ALWAYS_ASSERT(false);
    return 0;
  }

#define EVENT_COUNTER_DEF_X(x) \
  static event_counter g_ ## x ## _ctr;
  ABORT_REASONS(EVENT_COUNTER_DEF_X)
#undef EVENT_COUNTER_DEF_X

  static event_counter *
  AbortReasonCounter(abort_reason reason)
  {
    switch (reason) {
#define EVENT_COUNTER_CASE_X(x) case x: return &g_ ## x ## _ctr;
    ABORT_REASONS(EVENT_COUNTER_CASE_X)
#undef EVENT_COUNTER_CASE_X
    default:
      break;
    }
    ALWAYS_ASSERT(false);
    return 0;
  }

  static event_counter g_evt_read_logical_deleted_node_search;
  static event_counter g_evt_read_logical_deleted_node_scan;

  static const tid_t MIN_TID = 0;
  static const tid_t MAX_TID = (tid_t) -1;

  typedef varkey key_type;

  // declared here so logical_node::write_record_at() can see it.
  virtual bool
  can_overwrite_record_tid(tid_t prev, tid_t cur) const
  {
    return false;
  }

  /**
   * Just a singly linked list for now
   */
  struct logical_node_spillblock : private util::noncopyable {

    tid_t version;
    record_t value; // has ownership over the value pointed to:
                    // gc_chain() is responsible for free-ing the
                    // value (via the completion callbacks)

    struct logical_node_spillblock *spillblock_next;

    logical_node_spillblock(
        tid_t version, record_t value,
        struct logical_node_spillblock *spillblock_next);
    ~logical_node_spillblock();

    /**
     * Read the record at tid t. Returns true if such a record exists,
     * false otherwise (ie the record was GC-ed)
     *
     * The cool thing about record_at() for spillblocks is we don't ever have
     * to check version numbers- the contents are never overwritten
     */
    inline bool
    record_at(tid_t t, tid_t &start_t, record_t &r) const
    {
      const struct logical_node_spillblock *cur = this;
      while (cur) {
        if (likely(cur->version <= t)) {
          start_t = cur->version;
          r = cur->value;
          return true;
        }
        cur = cur->spillblock_next;
      }
      return false;
    }

    inline bool
    is_snapshot_consistent(tid_t snapshot_tid, tid_t commit_tid, tid_t oldest_tid) const
    {
      const struct logical_node_spillblock *cur = this;
      while (cur) {
        if (version <= snapshot_tid)
          return oldest_tid > commit_tid;
        oldest_tid = version;
        cur = cur->spillblock_next;
      }
      return false;
    }

    /**
     * Put this spillblock and all the spillblocks later in the chain up for GC
     */
    void gc_chain(bool do_rcu);

  } PACKED;

  /**
   * A logical_node is the type of value which we stick
   * into underlying (non-transactional) data structures
   */
  struct logical_node : private util::noncopyable {
  public:
    typedef uint64_t version_t;

  private:
    static const version_t HDR_LOCKED_MASK = 0x1;

    static const version_t HDR_DELETING_SHIFT = 1;
    static const version_t HDR_DELETING_MASK = 0x1 << HDR_DELETING_SHIFT;

    static const version_t HDR_ENQUEUED_SHIFT = 2;
    static const version_t HDR_ENQUEUED_MASK = 0x1 << HDR_ENQUEUED_SHIFT;

    static const version_t HDR_VERSION_SHIFT = 3;
    static const version_t HDR_VERSION_MASK = ((version_t)-1) << HDR_VERSION_SHIFT;

  public:

    // NB(stephentu): ABA problem happens after 2^61 concurrent modifications-
    // *very* low probability event, so we let it happen
    //
    // [ locked | deleted | enqueued | version ]
    // [  0..1  |  1..2   |   2..3   |  3..64  ]
    volatile version_t hdr;

    // constraints:
    //   * enqueued => !deleted
    //   * deleted  => !enqueued

    tid_t version;
    record_t value;

    // spill in units of NVersions
    struct logical_node_spillblock *spillblock_head;

    logical_node()
      : hdr(0), version(MIN_TID), value(0), spillblock_head(0)
    {
      // each logical node starts with one "deleted" entry at MIN_TID
    }

    logical_node(tid_t version, record_t value)
      : hdr(0), version(version), value(value), spillblock_head(0)
    {
    }

    ~logical_node();

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

    inline void
    lock()
    {
      version_t v = hdr;
      while (IsLocked(v) ||
             !__sync_bool_compare_and_swap(&hdr, v, v | HDR_LOCKED_MASK)) {
        nop_pause();
        v = hdr;
      }
      COMPILER_MEMORY_FENCE;
    }

    inline void
    unlock()
    {
      version_t v = hdr;
      INVARIANT(IsLocked(v));
      const version_t n = Version(v);
      v &= ~HDR_VERSION_MASK;
      v |= (((n + 1) << HDR_VERSION_SHIFT) & HDR_VERSION_MASK);
      v &= ~HDR_LOCKED_MASK;
      INVARIANT(!IsLocked(v));
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
      INVARIANT(is_locked());
      INVARIANT(!is_enqueued());
      INVARIANT(!is_deleting());
      hdr |= HDR_DELETING_MASK;
    }

    inline bool
    is_enqueued() const
    {
      return IsEnqueued(hdr);
    }

    static inline bool
    IsEnqueued(version_t v)
    {
      return v & HDR_ENQUEUED_MASK;
    }

    inline void
    set_enqueued(bool enqueued)
    {
      INVARIANT(is_locked());
      INVARIANT(!is_deleting());
      if (enqueued)
        hdr |= HDR_ENQUEUED_MASK;
      else
        hdr &= ~HDR_ENQUEUED_MASK;
    }

    static inline version_t
    Version(version_t v)
    {
      return (v & HDR_VERSION_MASK) >> HDR_VERSION_SHIFT;
    }

    inline version_t
    stable_version() const
    {
      version_t v = hdr;
      while (IsLocked(v)) {
        nop_pause();
        v = hdr;
      }
      COMPILER_MEMORY_FENCE;
      return v;
    }

    /**
     * returns true if succeeded, false otherwise
     */
    inline bool
    try_stable_version(version_t &v, unsigned int spins) const
    {
      v = hdr;
      while (IsLocked(v) && spins) {
        nop_pause();
        v = hdr;
        spins--;
      }
      COMPILER_MEMORY_FENCE;
      return !IsLocked(v);
    }

    inline version_t
    unstable_version() const
    {
      return hdr;
    }

    inline bool
    check_version(version_t version) const
    {
      COMPILER_MEMORY_FENCE;
      return hdr == version;
    }

    /**
     * Read the record at tid t. Returns true if such a record exists,
     * false otherwise (ie the record was GC-ed). Note that
     * record_at()'s return values must be validated using versions.
     *
     * NB(stephentu): calling record_at() while holding the lock
     * is an error- this will cause deadlock
     */
    inline bool
    record_at(tid_t t, tid_t &start_t, record_t &r) const
    {
    retry:
      const version_t v = stable_version();
      // because we expect t's to be relatively recent, instead of
      // doing binary search, we simply do a linear scan from the
      // end- most of the time we should find a match on the first try
      const struct logical_node_spillblock *p = spillblock_head;
      const bool found = is_latest_version(t);
      if (found) {
        start_t = version;
        r = value;
      }
      if (unlikely(!check_version(v)))
        goto retry;
      return found ? true : (p ? p->record_at(t, start_t, r) : false);
    }

    inline bool
    stable_read(tid_t t, tid_t &start_t, record_t &r) const
    {
      return record_at(t, start_t, r);
    }

    inline bool
    is_latest_version(tid_t t) const
    {
      return version <= t;
    }

    inline bool
    stable_is_latest_version(tid_t t) const
    {
      version_t v = 0;
      if (!try_stable_version(v, 16))
        return false;
      // now v is a stable version
      const bool ret = is_latest_version(t);
      // only check_version() if the answer would be true- otherwise,
      // no point in doing a version check
      if (ret && check_version(v))
        return true;
      else
        // no point in retrying, since we know it will fail (since we had a
        // version change)
        return false;
    }

    inline tid_t
    latest_version() const
    {
      return version;
    }

    inline record_t
    latest_value() const
    {
      return value;
    }

    /**
     * Is the valid read at snapshot_tid still consistent at commit_tid
     */
    template <bool check>
    inline bool
    is_snapshot_consistent_impl(tid_t snapshot_tid, tid_t commit_tid) const
    {
    retry:
      const version_t v = check ? stable_version() : unstable_version();
      const struct logical_node_spillblock *p = spillblock_head;
      if (version <= snapshot_tid) {
        if (unlikely(check && !check_version(v)))
          goto retry;
        return true;
      }
      if (unlikely(check && !check_version(v)))
        goto retry;
      return p ? p->is_snapshot_consistent(snapshot_tid, commit_tid, version) : false;
    }

    inline bool
    is_snapshot_consistent(tid_t snapshot_tid, tid_t commit_tid) const
    {
      return is_snapshot_consistent_impl<false>(snapshot_tid, commit_tid);
    }

    inline bool
    stable_is_snapshot_consistent(tid_t snapshot_tid, tid_t commit_tid) const
    {
      return is_snapshot_consistent_impl<true>(snapshot_tid, commit_tid);
    }

    /**
     * Always writes the record in the latest (newest) version slot,
     * not asserting whether or not inserting r @ t would violate the
     * sorted order invariant
     *
     * Returns true if the write induces a spill
     */
    inline bool
    write_record_at(const transaction *txn, tid_t t, record_t r, record_t *removed)
    {
      INVARIANT(is_locked());

      if (removed)
        *removed = 0;

      // easy case
      if (txn->can_overwrite_record_tid(version, t)) {
        version = t;
        if (removed)
          *removed = value;
        value = r;
        return false;
      }

      // need to spill
      struct logical_node_spillblock *sb =
        new logical_node_spillblock(version, value, spillblock_head);
      spillblock_head = sb;
      COMPILER_MEMORY_FENCE;
      version = t;
      value = r;
      return true;
    }

    static inline logical_node *
    alloc(tid_t version, record_t value)
    {
      return new logical_node(version, value);
    }

    static void
    deleter(void *p)
    {
      logical_node *n = (logical_node *) p;
      INVARIANT(n->is_deleting());
      INVARIANT(!n->is_locked());
      delete n;
    }

    static inline void
    release(logical_node *n)
    {
      if (unlikely(!n))
        return;
      n->mark_deleting();
      rcu::free_with_fn(n, deleter);
    }

    static inline void
    release_no_rcu(logical_node *n)
    {
      if (unlikely(!n))
        return;
#ifdef CHECK_INVARIANTS
      n->lock();
      n->mark_deleting();
      n->unlock();
#endif
      delete n;
    }

    static std::string
    VersionInfoStr(version_t v);

  } PACKED;

  friend std::ostream &
  operator<<(std::ostream &o, const logical_node &ln);

  transaction(uint64_t flags);
  virtual ~transaction();

  // returns TRUE on successful commit, FALSE on abort
  // if doThrow, signals success by returning true, and
  // failure by throwing an abort exception
  bool commit(bool doThrow = false);

  // abort() always succeeds
  inline void
  abort()
  {
    abort_impl(ABORT_REASON_USER);
  }

  inline uint64_t
  get_flags() const
  {
    return flags;
  }

  virtual void dump_debug_info() const;

  /**
   * If outstanding_refs, then there *could* still be outstanding
   * refs (but then we would be in a RCU region)
   */
  typedef void (*callback_t)(record_t, bool outstanding_refs);

  /** not thread-safe */
  static bool register_cleanup_callback(callback_t callback);

  static void Test();

#ifdef DIE_ON_ABORT
  void
  abort_trap(abort_reason reason) const
  {
    AbortReasonCounter(reason)->inc();
    this->reason = reason; // for dump_debug_info() to see
    dump_debug_info();
    ::abort();
  }
#else
  inline ALWAYS_INLINE void
  abort_trap(abort_reason reason) const
  {
    AbortReasonCounter(reason)->inc();
  }
#endif

  /**
   * XXX: document
   */
  virtual std::pair<bool, tid_t> consistent_snapshot_tid() const = 0;

  virtual tid_t null_entry_tid() const = 0;

protected:

  void abort_impl(abort_reason r);

  /**
   * create a new, unique TID for a txn. at the point which gen_commit_tid(),
   * it still has not been decided whether or not this txn will commit
   * successfully
   */
  virtual tid_t gen_commit_tid(
      const std::vector<logical_node *> &write_nodes) = 0;

  virtual bool can_read_tid(tid_t t) const { return true; }

  // For GC handlers- note that on_logical_node_spill() is called
  // with the lock on ln held, to simplify GC code
  //
  // Is also called within an RCU read region
  virtual void on_logical_node_spill(logical_node *ln) = 0;

  // Called when the latest value written to ln is an empty
  // (delete) marker. The protocol can then decide how to schedule
  // the logical node for actual deletion
  virtual void on_logical_delete(
      txn_btree *btr, const std::string &key, logical_node *ln) = 0;

  // if gen_commit_tid() is called, then on_tid_finish() will be called
  // with the commit tid. before on_tid_finish() is called, state is updated
  // with the resolution (commited, aborted) of this txn
  virtual void on_tid_finish(tid_t commit_tid) = 0;

  /**
   * throws transaction_unusable_exception if already resolved (commited/aborted)
   */
  inline void
  ensure_active()
  {
    if (state == TXN_EMBRYO)
      state = TXN_ACTIVE;
    else if (unlikely(state != TXN_ACTIVE))
      throw transaction_unusable_exception();
  }

  static std::vector<callback_t> &completion_callbacks();

  struct read_record_t {
    tid_t t;
    record_t r;
    logical_node *ln;
  };

  friend std::ostream &
  operator<<(std::ostream &o, const transaction::read_record_t &rr);

  //typedef std::map<std::string, read_record_t> read_set_map;
  //typedef std::map<std::string, record_t> write_set_map;
  //typedef std::vector<key_range_t> absent_range_vec;
  //typedef std::map<const btree::node_opaque_t *, uint64_t> node_scan_map;

  typedef std::tr1::unordered_map<std::string, read_record_t> read_set_map;
  typedef std::tr1::unordered_map<std::string, record_t> write_set_map;
  typedef std::vector<key_range_t> absent_range_vec;
  typedef std::tr1::unordered_map<const btree::node_opaque_t *, uint64_t> node_scan_map;

  struct txn_context {
    read_set_map read_set;
    write_set_map write_set;
    absent_range_vec absent_range_set; // ranges do not overlap
    node_scan_map node_scan; // we scanned these nodes at verison v

    bool local_search_str(const std::string &k, record_t &v) const;

    inline bool
    local_search(const key_type &k, record_t &v) const
    {
      // XXX: we have to make an un-necessary copy of the key each time we search
      // the write/read set- we need to find a way to avoid this
      return local_search_str(k.str(), v);
    }

    bool key_in_absent_set(const key_type &k) const;

    void add_absent_range(const key_range_t &range);
  };

  void clear();

  // [a, b)
  struct key_range_t {
    key_range_t() : a(), has_b(true), b() {}

    key_range_t(const key_type &a) : a(a.str()), has_b(false), b() {}
    key_range_t(const key_type &a, const key_type &b)
      : a(a.str()), has_b(true), b(b.str())
    { }
    key_range_t(const key_type &a, const std::string &b)
      : a(a.str()), has_b(true), b(b)
    { }
    key_range_t(const key_type &a, bool has_b, const key_type &b)
      : a(a.str()), has_b(has_b), b(b.str())
    { }
    key_range_t(const key_type &a, bool has_b, const std::string &b)
      : a(a.str()), has_b(has_b), b(b)
    { }

    key_range_t(const std::string &a) : a(a), has_b(false), b() {}
    key_range_t(const std::string &a, const key_type &b)
      : a(a), has_b(true), b(b.str())
    { }
    key_range_t(const std::string &a, bool has_b, const key_type &b)
      : a(a), has_b(has_b), b(b.str())
    { }

    key_range_t(const std::string &a, const std::string &b)
      : a(a), has_b(true), b(b)
    { }
    key_range_t(const std::string &a, bool has_b, const std::string &b)
      : a(a), has_b(has_b), b(b)
    { }

    std::string a;
    bool has_b; // false indicates infinity, true indicates use b
    std::string b; // has meaning only if !has_b

    inline bool
    is_empty_range() const
    {
      return has_b && a >= b;
    }

    inline bool
    contains(const key_range_t &that) const
    {
      if (a > that.a)
        return false;
      if (!has_b)
        return true;
      if (!that.has_b)
        return false;
      return b >= that.b;
    }

    inline bool
    key_in_range(const key_type &k) const
    {
      return varkey(a) <= k && (!has_b || k < varkey(b));
    }
  };

  // NOTE: with this comparator, upper_bound() will return a pointer to the first
  // range which has upper bound greater than k (if one exists)- it does not
  // guarantee that the range returned has a lower bound <= k
  struct key_range_search_less_cmp {
    inline bool
    operator()(const key_type &k, const key_range_t &range) const
    {
      return !range.has_b || k < varkey(range.b);
    }
  };

#ifdef CHECK_INVARIANTS
  static void AssertValidRangeSet(const std::vector<key_range_t> &range_set);
#else
  static inline ALWAYS_INLINE void
  AssertValidRangeSet(const std::vector<key_range_t> &range_set)
  { }
#endif /* CHECK_INVARIANTS */

  static std::string PrintRangeSet(
      const std::vector<key_range_t> &range_set);

  txn_state state;
  abort_reason reason;
  const uint64_t flags;
  std::map<txn_btree *, txn_context> ctx_map;
};

class transaction_abort_exception : public std::exception {
public:
  transaction_abort_exception(transaction::abort_reason r)
    : r(r) {}
  inline transaction::abort_reason
  get_reason() const
  {
    return r;
  }
  virtual const char *
  what() const throw()
  {
    return transaction::AbortReasonStr(r);
  }
private:
  transaction::abort_reason r;
};

#define NDB_TXN_REGISTER_CLEANUP_CALLBACK(fn) \
  static bool _txn_cleanup_callback_register_ ## __LINE__ UNUSED = \
    ::transaction::register_cleanup_callback(fn);

// protocol 1 - global consistent TIDs
class transaction_proto1 : public transaction {
public:
  transaction_proto1(uint64_t flags = 0);
  virtual std::pair<bool, tid_t> consistent_snapshot_tid() const;
  virtual tid_t null_entry_tid() const;
  virtual void dump_debug_info() const;

protected:
  static const size_t NMaxChainLength = 10; // XXX(stephentu): tune me?

  virtual tid_t gen_commit_tid(
      const std::vector<logical_node *> &write_nodes);
  virtual void on_logical_node_spill(logical_node *ln);
  virtual void on_logical_delete(
      txn_btree *btr, const std::string &key, logical_node *ln);
  virtual void on_tid_finish(tid_t commit_tid);

private:
  static tid_t incr_and_get_global_tid();

  const tid_t snapshot_tid;

  volatile static tid_t global_tid CACHE_ALIGNED;
  volatile static tid_t last_consistent_global_tid CACHE_ALIGNED;
};

// protocol 2 - no global consistent TIDs
class transaction_proto2 : public transaction {

  // in this protocol, the version number is:
  //
  // [ core  | number |  epoch ]
  // [ 0..10 | 10..27 | 27..64 ]

public:
  transaction_proto2(uint64_t flags = 0);
  ~transaction_proto2();

  virtual std::pair<bool, tid_t> consistent_snapshot_tid() const;

  virtual tid_t null_entry_tid() const;

  virtual void dump_debug_info() const;

  static inline ALWAYS_INLINE
  uint64_t CoreId(uint64_t v)
  {
    return v & CoreMask;
  }

  static inline ALWAYS_INLINE
  uint64_t NumId(uint64_t v)
  {
    return (v & NumIdMask) >> NumIdShift;
  }

  static inline ALWAYS_INLINE
  uint64_t EpochId(uint64_t v)
  {
    return (v & EpochMask) >> EpochShift;
  }

  // XXX(stephentu): HACK
  static void
  wait_an_epoch()
  {
    ALWAYS_ASSERT(!tl_nest_level);
    const uint64_t e = g_last_consistent_epoch;
    while (g_last_consistent_epoch == e)
      nop_pause();
    COMPILER_MEMORY_FENCE;
  }

  // XXX(stephentu): HACK
  static void wait_for_empty_work_queue();

  virtual bool
  can_overwrite_record_tid(tid_t prev, tid_t cur) const
  {
    INVARIANT(prev < cur);
    INVARIANT(EpochId(cur) >= g_last_consistent_epoch);
    return EpochId(prev) == EpochId(cur);
  }

protected:
  virtual tid_t gen_commit_tid(
      const std::vector<logical_node *> &write_nodes);

  // can only read elements in this epoch or previous epochs
  virtual bool
  can_read_tid(tid_t t) const
  {
    return EpochId(t) <= current_epoch;
  }

  virtual void on_logical_node_spill(logical_node *ln);
  virtual void on_logical_delete(
      txn_btree *btr, const std::string &key, logical_node *ln);
  virtual void on_tid_finish(tid_t commit_tid) {}

private:
  // the global epoch this txn is running in (this # is read when it starts)
  uint64_t current_epoch;
  uint64_t last_consistent_tid;

  static size_t core_id();

  /**
   * If true is returned, reschedule the job to run after epoch.
   * Otherwise, the task is finished
   */
  typedef bool (*work_callback_t)(void *, uint64_t &epoch);

  // work will get called after epoch finishes
  void enqueue_work_after_current_epoch(uint64_t epoch, work_callback_t work, void *p);

  static bool
  try_delete_logical_node(void *p, uint64_t &epoch);

  // XXX(stephentu): need to implement core ID recycling
  static const size_t CoreBits = NMAXCOREBITS; // allow 2^CoreShift distinct threads
  static const size_t NMaxCores = NMAXCORES;

  static const uint64_t CoreMask = (NMaxCores - 1);

  static const uint64_t NumIdShift = CoreBits;
  static const uint64_t NumIdMask = ((((uint64_t)1) << 27) - 1) << NumIdShift;

  static const uint64_t EpochShift = NumIdShift + 27;
  static const uint64_t EpochMask = ((uint64_t)-1) << EpochShift;

  static inline ALWAYS_INLINE
  uint64_t MakeTid(uint64_t core_id, uint64_t num_id, uint64_t epoch_id)
  {
    // some sanity checking
    _static_assert((CoreMask | NumIdMask | EpochMask) == ((uint64_t)-1));
    _static_assert((CoreMask & NumIdMask) == 0);
    _static_assert((NumIdMask & EpochMask) == 0);
    return (core_id) | (num_id << NumIdShift) | (epoch_id << EpochShift);
  }

  // XXX(stephentu): we re-implement another epoch-based scheme- we should
  // reconcile this with the RCU-subsystem, by implementing an epoch based
  // thread manager, which both the RCU GC and this machinery can build on top
  // of

  static bool InitEpochScheme();
  static bool _init_epoch_scheme_flag;

  class epoch_loop : public ndb_thread {
    friend class transaction_proto2;
  public:
    epoch_loop()
      : ndb_thread(true, std::string("epochloop")), is_wq_empty(true) {}
    virtual void run();
  private:
    volatile bool is_wq_empty;
  };

  static epoch_loop g_epoch_loop;

  /**
   * Get a (possibly stale) consistent TID
   */
  static uint64_t GetConsistentTid();

  // allows a single core to run multiple transactions at the same time
  // XXX(stephentu): should we allow this? this seems potentially troubling
  static __thread unsigned int tl_nest_level;

  static __thread uint64_t tl_last_commit_tid;

  // XXX(stephentu): think about if the vars below really need to be volatile

  // contains the current epoch number, is either == g_last_consistent_epoch or
  // == g_last_consistent_epoch + 1
  static volatile uint64_t g_current_epoch CACHE_ALIGNED;

  // contains the epoch # to take a consistent snapshot at the beginning of
  // (this means g_last_consistent_epoch - 1 is the last epoch fully completed)
  static volatile uint64_t g_last_consistent_epoch CACHE_ALIGNED;


  // for synchronizing with the epoch incrementor loop
  static volatile util::aligned_padded_elem<pthread_spinlock_t>
    g_epoch_spinlocks[NMaxCores] CACHE_ALIGNED;

  struct work_record_t {
    work_record_t() {}
    work_record_t(uint64_t epoch, work_callback_t work, void *p)
      : epoch(epoch), work(work), p(p)
    {}
    // for work_pq
    inline bool
    operator>(const work_record_t &that) const
    {
      return epoch > that.epoch;
    }
    uint64_t epoch;
    work_callback_t work;
    void *p;
  };

  typedef std::vector<work_record_t> work_q;
  typedef util::std_reverse_pq<work_record_t>::type work_pq;

  // for passing work to the epoch loop
  static volatile util::aligned_padded_elem<work_q*>
    g_work_queues[NMaxCores] CACHE_ALIGNED;
};

// XXX(stephentu): stupid hacks
template <typename TxnType>
struct txn_epoch_sync {
  // block until the next epoch
  static inline void sync() {}
  // finish any async jobs
  static inline void finish() {}
};

template <>
struct txn_epoch_sync<transaction_proto2> {
  static inline void sync() { transaction_proto2::wait_an_epoch(); }
  static inline void finish() { transaction_proto2::wait_for_empty_work_queue(); }
};

#endif /* _NDB_TXN_H_ */
