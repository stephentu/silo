#ifndef _NDB_TXN_H_
#define _NDB_TXN_H_

#include <malloc.h>
#include <stdint.h>
#include <sys/types.h>

#include <map>
#include <iostream>
#include <vector>
#include <string>

#include "btree.h"
#include "macros.h"
#include "varkey.h"
#include "util.h"

class transaction_abort_exception {};
class transaction_unusable_exception {};
class txn_btree;

class transaction : public util::noncopyable {
protected:
  friend class txn_btree;
  friend class txn_context;
  class key_range_t;
  friend std::ostream &operator<<(std::ostream &, const key_range_t &);

public:

  typedef uint64_t tid_t;
  typedef uint8_t* record_t;

  enum txn_state { TXN_ACTIVE, TXN_COMMITED, TXN_ABRT, };

  enum {
    // use the low-level scan protocol for checking scan consistency,
    // instead of keeping track of absent ranges
    TXN_FLAG_LOW_LEVEL_SCAN = 0x1,

    // XXX: more flags in the future, things like consistency levels
  };

  static const tid_t MIN_TID = 0;
  static const tid_t MAX_TID = (tid_t) -1;

  typedef varkey key_type;

  /**
   * A logical_node is the type of value which we stick
   * into underlying (non-transactional) data structures
   *
   * We try to size a logical node to be about 1 cache line wide
   */
  struct logical_node {
  private:
    static const uint64_t HDR_LOCKED_MASK = 0x1;

    static const uint64_t HDR_SIZE_SHIFT = 1;
    static const uint64_t HDR_SIZE_MASK = 0xf << HDR_SIZE_SHIFT;

    static const uint64_t HDR_VERSION_SHIFT = 5;
    static const uint64_t HDR_VERSION_MASK = ((uint64_t)-1) << HDR_VERSION_SHIFT;

  public:

    static const size_t NVersions = 3;

    // [ locked | num_versions | version ]
    // [  0..1  |     1..5     |  5..64  ]
    volatile uint64_t hdr;

    // in each logical_node, the latest verison/value is stored in
    // versions[size() - 1] and values[size() - 1]. each
    // node can store up to 15 values
    tid_t versions[NVersions];
    record_t values[NVersions];

    logical_node()
      : hdr(0)
    {
      // each logical node starts with one "deleted" entry at MIN_TID
      set_size(1);
      versions[0] = MIN_TID;
      values[0] = NULL;
    }

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
             !__sync_bool_compare_and_swap(&hdr, v, v | HDR_LOCKED_MASK))
        v = hdr;
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
      while (IsLocked(v))
        v = hdr;
      COMPILER_MEMORY_FENCE;
      return v;
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
     */
    inline bool
    record_at(tid_t t, tid_t &start_t, record_t &r) const
    {
      // because we expect t's to be relatively recent, instead of
      // doing binary search, we simply do a linear scan from the
      // end- most of the time we should find a match on the first try
      size_t n = size();
      INVARIANT(n > 0 && n <= NVersions);
      for (ssize_t i = n - 1; i >= 0; i--)
        if (versions[i] <= t) {
          start_t = versions[i];
          r = values[i];
          return true;
        }
      return false;
    }

    inline bool
    stable_read(tid_t t, tid_t &start_t, record_t &r) const
    {
      while (true) {
        uint64_t v = stable_version();
        if (unlikely(!record_at(t, start_t, r)))
          // the record at this tid was gc-ed
          return false;
        if (likely(check_version(v)))
          break;
      }
      return true;
    }

    inline bool
    is_latest_version(tid_t t) const
    {
      return latest_version() <= t;
    }

    inline bool
    stable_is_latest_version(tid_t t) const
    {
      while (true) {
        uint64_t v = stable_version();
        bool ret = is_latest_version(t);
        if (likely(check_version(v)))
          return ret;
      }
    }

    inline tid_t
    latest_version() const
    {
      size_t n = size();
      INVARIANT(n > 0 && n <= NVersions);
      return versions[n - 1];
    }

    /**
     * Is the valid read at snapshot_tid still consistent at commit_tid
     */
    inline bool
    is_snapshot_consistent(tid_t snapshot_tid, tid_t commit_tid) const
    {
      size_t n = size();
      INVARIANT(n > 0 && n <= NVersions);

      // fast path
      if (likely(versions[n - 1] <= snapshot_tid))
        return true;

      // slow path
      for (ssize_t i = n - 2 /* we already checked @ (n-1) */; i >= 0; i--)
        if (versions[i] <= snapshot_tid) {
          // see if theres any conflict between the version we read, and
          // the next modification. there is no conflict (conservatively)
          // if the next modification happens *after* our commit tid
          INVARIANT(versions[i + 1] != commit_tid);
          return versions[i + 1] > commit_tid;
        }

      return false;
    }

    inline bool
    stable_is_snapshot_consistent(tid_t snapshot_tid, tid_t commit_tid) const
    {
      while (true) {
        uint64_t v = stable_version();
        bool ret = is_snapshot_consistent(snapshot_tid, commit_tid);
        if (likely(check_version(v)))
          return ret;
      }
    }

    inline void
    write_record_at(tid_t t, record_t r, record_t *removed)
    {
      INVARIANT(is_locked());
      size_t n = size();
      INVARIANT(n > 0 && n <= NVersions);
      INVARIANT(versions[n - 1] < t);
      if (n == NVersions) {
        // drop oldest version
        if (removed)
          *removed = values[0];
        for (size_t i = 0; i < NVersions - 1; i++) {
          versions[i] = versions[i + 1];
          values[i] = values[i + 1];
        }
        versions[NVersions - 1] = t;
        values[NVersions - 1] = r;
      } else {
        versions[n] = t;
        values[n] = r;
        set_size(n + 1);
      }
    }

    static inline logical_node *
    alloc()
    {
      void *p = memalign(CACHELINE_SIZE, sizeof(logical_node));
      assert(p);
      return new (p) logical_node;
    }

    static inline void
    release(logical_node *n)
    {
      // XXX: currently not RCU GC-ed (since we only free when we cannot put
      // the logical_node into the shared data structure)
      if (unlikely(!n))
        return;
      n->~logical_node();
      free(n);
    }

    static std::string
    VersionInfoStr(uint64_t v);

  } PACKED_CACHE_ALIGNED;

  transaction(uint64_t flags);
  virtual ~transaction();

  void commit();

  // abort() always succeeds
  void abort();

  inline uint64_t get_flags() const
  {
    return flags;
  }

  typedef void (*callback_t)(record_t);
  /** not thread-safe */
  static bool register_cleanup_callback(callback_t callback);

  static void Test();

  /**
   * XXX: document
   */
  virtual std::pair<bool, tid_t> consistent_snapshot_tid() const = 0;

protected:

  /**
   * XXX: document
   */
  virtual tid_t gen_commit_tid(const std::map<logical_node *, record_t> &write_nodes) = 0;

  /**
   * throws transaction_unusable_exception if already resolved (commited/aborted)
   */
  inline void
  ensure_active()
  {
    if (unlikely(state != TXN_ACTIVE))
      throw transaction_unusable_exception();
  }

  static std::vector<callback_t> &completion_callbacks();

  struct read_record_t {
    tid_t t;
    record_t r;
    logical_node *ln;
  };

  // XXX: use hash tables for the read/write set
  typedef std::map<std::string, read_record_t> read_set_map;
  typedef std::map<std::string, record_t> write_set_map;
  typedef std::vector<key_range_t> absent_range_vec;
  typedef std::map<const btree::node_opaque_t *, uint64_t> node_scan_map;

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

protected:
  virtual tid_t gen_commit_tid(const std::map<logical_node *, record_t> &write_nodes);

private:
  static tid_t current_global_tid(); // tid of the last commit
  static tid_t incr_and_get_global_tid();

  const tid_t snapshot_tid;
  volatile static tid_t global_tid;
};

// protocol 2 - no global consistent TIDs
class transaction_proto2 : public transaction {
public:
  transaction_proto2(uint64_t flags = 0) : transaction(flags) {}
  virtual std::pair<bool, tid_t> consistent_snapshot_tid() const;

protected:
  virtual tid_t gen_commit_tid(const std::map<logical_node *, record_t> &write_nodes);

private:
  static size_t core_id();

  // XXX: need to implement core ID recycling
  static const size_t CoreBits = 10; // allow 2^CoreShift distinct threads
  static __thread tid_t tl_last_commit_tid;
  static __thread ssize_t tl_core_id; // -1 if not set
  static volatile size_t g_core_count;
};

#endif /* _NDB_TXN_H_ */
