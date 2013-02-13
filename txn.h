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

#include <tr1/unordered_map>

#include "btree.h"
#include "macros.h"
#include "varkey.h"
#include "util.h"
#include "static_assert.h"
#include "rcu.h"
#include "thread.h"

class transaction_abort_exception {};
class transaction_unusable_exception {};
class transaction_read_only_exception {};
class txn_btree;

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

  static const tid_t MIN_TID = 0;
  static const tid_t MAX_TID = (tid_t) -1;

  typedef varkey key_type;

  // declared here so logical_node::write_record_at() can see it.
  virtual bool
  can_overwrite_record_tid(tid_t prev, tid_t cur) const
  {
    return false;
  }

  struct logical_node_spillblock : public util::noncopyable {

    static const size_t NSpills = 5; // makes each spillblock about 4 cachelines
    static const size_t NSpillSize = 3;
    static const size_t NElems = NSpills * NSpillSize;

    volatile uint8_t nspills; // how many spills so far, the # only increases
    volatile uint64_t gc_opaque_value; // an opaque 64-bit value for GC, initialized to 0

    tid_t versions[NElems];
    record_t values[NElems]; // has ownership over the values pointed to:
                             // gc_chain() is responsible for free-ing the
                             // values (via the completion callbacks)

    struct logical_node_spillblock *spillblock_next;

    logical_node_spillblock(
        struct logical_node_spillblock *spillblock_next)
      : nspills(0), gc_opaque_value(0),
        spillblock_next(spillblock_next)
    {}

    inline size_t
    size() const
    {
      return nspills * NSpillSize;
    }

    inline uint8_t
    version() const
    {
      COMPILER_MEMORY_FENCE;
      return nspills;
    }

    inline bool
    check_version(uint8_t version) const
    {
      COMPILER_MEMORY_FENCE;
      return nspills == version;
    }

    inline bool
    is_full() const
    {
      INVARIANT(nspills <= NSpills);
      return nspills == NSpills;
    }

    inline void
    spill_into(tid_t *intake_versions, record_t *intake_values)
    {
      INVARIANT(!is_full());
      memcpy((char *) &versions[nspills * NSpillSize],
             (const char *) intake_versions,
             NSpillSize * sizeof(tid_t));
      memcpy((char *) &values[nspills * NSpillSize],
             (const char *) intake_values,
             NSpillSize * sizeof(record_t));
      COMPILER_MEMORY_FENCE;
      nspills++;
    }

    /**
     * Read the record at tid t. Returns true if such a record exists,
     * false otherwise (ie the record was GC-ed). Note that
     * record_at()'s return values must be validated using versions.
     *
     * The cool thing about record_at() for spillblocks is we don't ever have
     * to check version numbers- the contents are never overwritten
     */
    inline bool
    record_at(tid_t t, tid_t &start_t, record_t &r) const
    {
      const struct logical_node_spillblock *cur = this;
      while (cur) {
        const size_t n = cur->size();
        INVARIANT(n > 0 && n <= NElems);
        for (ssize_t i = n - 1; i >= 0; i--)
          if (cur->versions[i] <= t) {
            start_t = cur->versions[i];
            r = cur->values[i];
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
        const size_t n = cur->size();
        INVARIANT(n > 0 && n <= NElems);
        if (versions[n - 1] <= snapshot_tid)
          return oldest_tid > commit_tid;
        for (ssize_t i = n - 2; i >= 0; i--)
          if (versions[i] <= snapshot_tid) {
            INVARIANT(versions[i + 1] != commit_tid);
            return versions[i + 1] > commit_tid;
          }
        oldest_tid = versions[0];
        cur = cur->spillblock_next;
      }
      return false;
    }

    /**
     * Put this spillblock and all the spillblocks later in the chain up for GC
     *
     * **must be called within an RCU region **
     */
    void gc_chain();

  }; // XXX(stephentu): do we care about cache alignment for spill blocks?

  /**
   * A logical_node is the type of value which we stick
   * into underlying (non-transactional) data structures
   *
   * A logical node is one cache line wide
   */
  struct logical_node : public util::noncopyable {
  private:
    static const uint64_t HDR_LOCKED_MASK = 0x1;

    static const uint64_t HDR_DELETING_SHIFT = 1;
    static const uint64_t HDR_DELETING_MASK = 0x1 << HDR_DELETING_SHIFT;

    static const uint64_t HDR_SIZE_SHIFT = 2;
    static const uint64_t HDR_SIZE_MASK = 0xf << HDR_SIZE_SHIFT;

    static const uint64_t HDR_VERSION_SHIFT = 6;
    static const uint64_t HDR_VERSION_MASK = ((uint64_t)-1) << HDR_VERSION_SHIFT;

  public:

    static const size_t NVersions = logical_node_spillblock::NSpillSize;

    // [ locked | deleted | num_versions | version ]
    // [  0..1  |  1..2   |     2..6     |  6..64  ]
    volatile uint64_t hdr;

    // in each logical_node, the latest verison/value is stored in
    // versions[size() - 1] and values[size() - 1]. each
    // node can store up to 15 values
    tid_t versions[NVersions];
    record_t values[NVersions];

    // spill in units of NVersions
    struct logical_node_spillblock *spillblock_head;

    logical_node()
      : hdr(0), spillblock_head(0)
    {
      // each logical node starts with one "deleted" entry at MIN_TID
      set_size(1);
      versions[0] = MIN_TID;
      values[0] = NULL;
    }

    ~logical_node();

    inline bool
    is_locked() const
    {
      return IsLocked(hdr);
    }

    static inline bool
    IsLocked(uint64_t v)
    {
      return v & HDR_LOCKED_MASK;
    }

    inline void
    lock()
    {
      uint64_t v = hdr;
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
      uint64_t v = hdr;
      INVARIANT(IsLocked(v));
      uint64_t n = Version(v);
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
    IsDeleting(uint64_t v)
    {
      return v & HDR_DELETING_MASK;
    }

    inline void
    mark_deleting()
    {
      INVARIANT(is_locked());
      INVARIANT(!is_deleting());
      hdr |= HDR_DELETING_MASK;
    }

    inline size_t
    size() const
    {
      return Size(hdr);
    }

    static inline size_t
    Size(uint64_t v)
    {
      return (v & HDR_SIZE_MASK) >> HDR_SIZE_SHIFT;
    }

    inline void
    set_size(size_t n)
    {
      INVARIANT(n > 0);
      INVARIANT(n <= NVersions);
      hdr &= ~HDR_SIZE_MASK;
      hdr |= (n << HDR_SIZE_SHIFT);
    }

    static inline uint64_t
    Version(uint64_t v)
    {
      return (v & HDR_VERSION_MASK) >> HDR_VERSION_SHIFT;
    }

    inline uint64_t
    stable_version() const
    {
      uint64_t v = hdr;
      while (IsLocked(v)) {
        nop_pause();
        v = hdr;
      }
      COMPILER_MEMORY_FENCE;
      return v;
    }

    inline uint64_t
    unstable_version() const
    {
      return hdr;
    }

    inline bool
    check_version(uint64_t version) const
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
      uint64_t v = stable_version();
      // because we expect t's to be relatively recent, instead of
      // doing binary search, we simply do a linear scan from the
      // end- most of the time we should find a match on the first try
      size_t n = size();
      const struct logical_node_spillblock *p = spillblock_head;
      INVARIANT(n > 0 && n <= NVersions);
      bool found = false;
      for (ssize_t i = n - 1; i >= 0; i--)
        if (versions[i] <= t) {
          start_t = versions[i];
          r = values[i];
          found = true;
          break;
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
      return latest_version() <= t;
    }

    inline bool
    stable_is_latest_version(tid_t t) const
    {
      const uint64_t v = unstable_version();
      if (unlikely(IsLocked(v)))
        return false;
      COMPILER_MEMORY_FENCE;
      // now v is a stable version
      bool ret = is_latest_version(t);
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
      size_t n = size();
      INVARIANT(n > 0 && n <= NVersions);
      return versions[n - 1];
    }

    inline record_t
    latest_value() const
    {
      size_t n = size();
      INVARIANT(n > 0 && n <= NVersions);
      return values[n - 1];
    }

    /**
     * Is the valid read at snapshot_tid still consistent at commit_tid
     */
    template <bool check>
    inline bool
    is_snapshot_consistent_impl(tid_t snapshot_tid, tid_t commit_tid) const
    {
    retry:
      const uint64_t v = check ? stable_version() : unstable_version();

      const size_t n = size();
      INVARIANT(n > 0 && n <= NVersions);

      // fast path
      if (versions[n - 1] <= snapshot_tid) {
        if (unlikely(check && !check_version(v)))
          goto retry;
        return true;
      }

      // slow path
      ssize_t i = n - 2; /* we already checked @ (n-1) */
      bool ret = false;
      const struct logical_node_spillblock *p = spillblock_head;
      tid_t oldest_version = versions[0];
      for (; i >= 0; i--)
        if (versions[i] <= snapshot_tid) {
          // see if theres any conflict between the version we read, and
          // the next modification. there is no conflict (conservatively)
          // if the next modification happens *after* our commit tid
          INVARIANT(versions[i + 1] != commit_tid);
          ret = versions[i + 1] > commit_tid;
          break;
        }
      if (unlikely(check && !check_version(v)))
        goto retry;
      if (i == -1)
        return p ? p->is_snapshot_consistent(snapshot_tid, commit_tid, oldest_version) : false;
      else
        return ret;
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
      const size_t n = size();
      INVARIANT(n > 0 && n <= NVersions);
      INVARIANT(versions[n - 1] < t);

      if (removed)
        *removed = 0;

      // easy case
      if (txn->can_overwrite_record_tid(versions[n - 1], t)) {
        versions[n - 1] = t;
        if (removed)
          *removed = values[n - 1];
        values[n - 1] = r;
        return false;
      }

      if (n == NVersions) {
        // need to spill
        struct logical_node_spillblock *sb = spillblock_head;
        if (!spillblock_head || spillblock_head->is_full())
          sb = new logical_node_spillblock(spillblock_head);
        sb->spill_into(&versions[0], &values[0]);
        if (sb != spillblock_head)
          spillblock_head = sb;
        versions[0] = t;
        values[0] = r;
        set_size(1);

        return true;
      } else {
        versions[n] = t;
        values[n] = r;
        set_size(n + 1);

        return false;
      }
    }

    static inline logical_node *
    alloc()
    {
      void *p = memalign(CACHELINE_SIZE, sizeof(logical_node));
      assert(p);
      return new (p) logical_node;
    }

    static void
    deleter(void *p)
    {
      logical_node *n = (logical_node *) p;
      INVARIANT(n->is_deleting());
      INVARIANT(!n->is_locked());
      n->~logical_node();
      free(n);
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
      n->~logical_node();
      free(n);
    }

    static std::string
    VersionInfoStr(uint64_t v);

    static void
    try_delete(void *p);

  } PACKED_CACHE_ALIGNED;

  friend std::ostream &
  operator<<(std::ostream &o, const logical_node &ln);

  transaction(uint64_t flags);
  virtual ~transaction();

  // returns TRUE on successful commit, FALSE on abort
  // if doThrow, signals success by returning true, and
  // failure by throwing an abort exception
  bool commit(bool doThrow = false);

  // abort() always succeeds
  void abort();

  inline uint64_t
  get_flags() const
  {
    return flags;
  }

  virtual void dump_debug_info() const;

  typedef void (*callback_t)(record_t);
  /** not thread-safe */
  static bool register_cleanup_callback(callback_t callback);

  static void Test();

#ifdef DIE_ON_ABORT
  void
  abort_trap() const
  {
    dump_debug_info();
    ::abort();
  }
#else
  inline ALWAYS_INLINE void
  abort_trap() const
  {
  }
#endif

  /**
   * XXX: document
   */
  virtual std::pair<bool, tid_t> consistent_snapshot_tid() const = 0;

protected:

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
  const uint64_t flags;
  std::map<txn_btree *, txn_context> ctx_map;
};

#define NDB_TXN_REGISTER_CLEANUP_CALLBACK(fn) \
  static bool _txn_cleanup_callback_register_ ## __LINE__ UNUSED = \
    ::transaction::register_cleanup_callback(fn);

// protocol 1 - global consistent TIDs
class transaction_proto1 : public transaction {
public:
  transaction_proto1(uint64_t flags = 0);
  virtual std::pair<bool, tid_t> consistent_snapshot_tid() const;
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
  static void wait_an_epoch()
  {
    const uint64_t e = g_last_consistent_epoch;
    while (g_last_consistent_epoch == e)
      ;
    COMPILER_MEMORY_FENCE;
  }

  virtual bool
  can_overwrite_record_tid(tid_t prev, tid_t cur) const
  {
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

  typedef void (*work_callback_t)(void *);

  // work will get called after current epoch finishes
  void enqueue_work_after_current_epoch(work_callback_t work, void *p);

  // XXX(stephentu): need to implement core ID recycling
  static const size_t CoreBits = 10; // allow 2^CoreShift distinct threads
  static const size_t NMaxCores = (1 << CoreBits);

  static const uint64_t CoreMask = ((1 << CoreBits) - 1);

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
  public:
    epoch_loop() : ndb_thread(true) {}
    virtual void run();
  };

  /**
   * Get a (possibly stale) consistent TID
   */
  static uint64_t GetConsistentTid();

  // the core ID of this core: -1 if not set
  static __thread ssize_t tl_core_id;

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

  // contains a running count of all the cores
  static volatile size_t g_core_count CACHE_ALIGNED;

  // for synchronizing with the epoch incrementor loop
  static volatile util::aligned_padded_elem<pthread_spinlock_t>
    g_epoch_spinlocks[NMaxCores] CACHE_ALIGNED;

  //typedef std::pair<uint64_t, work_callback_t> work_record_t;
  //typedef std::priority_queue<
  //  work_record_t,
  //  std::vector<work_record_t>,
  //  util::std_pair_first_cmp<
  //    work_record_t,
  //    greater<work_record_t::first_type>
  //  >
  //> work_pq;

  typedef std::pair<work_callback_t, void *> work_record_t;
  typedef std::vector<work_record_t> work_pq;

  // for passing work to the epoch loop
  static volatile util::aligned_padded_elem<work_pq*>
    g_work_queues[NMaxCores] CACHE_ALIGNED;
};

// XXX(stephentu): stupid hacks
template <typename TxnType>
struct txn_epoch_sync {
  static inline void sync() {}
};

template <>
struct txn_epoch_sync<transaction_proto2> {
  static inline void sync() { transaction_proto2::wait_an_epoch(); }
};

#endif /* _NDB_TXN_H_ */
