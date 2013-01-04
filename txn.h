#ifndef _NDB_TXN_H_
#define _NDB_TXN_H_

#include <malloc.h>
#include <stdint.h>
#include <sys/types.h>

#include <map>
#include <iostream>
#include <vector>
#include <string>

#include "macros.h"

class transaction_abort_exception {};
class txn_btree;

class transaction {
  friend class txn_btree;
  class key_range_t;
  friend std::ostream &operator<<(std::ostream &, const key_range_t &);
public:

  typedef uint64_t tid_t;
  typedef uint8_t* record_t;

  static const tid_t MIN_TID = 0;

  typedef uint64_t key_type;

  /**
   * A logical_node is the type of value which we stick
   * into underlying (non-transactional) data structures
   *
   * We try to size a logical node to be about 4 cache lines wide
   */
  struct logical_node {
  private:
    static const uint64_t HDR_LOCKED_MASK = 0x1;

    static const uint64_t HDR_SIZE_SHIFT = 1;
    static const uint64_t HDR_SIZE_MASK = 0xf << HDR_SIZE_SHIFT;

    static const uint64_t HDR_VERSION_SHIFT = 5;
    static const uint64_t HDR_VERSION_MASK = ((uint64_t)-1) << HDR_VERSION_SHIFT;

  public:

    static const size_t NVersions = 15;

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
      assert(IsLocked(v));
      uint64_t n = Version(v);
      v &= ~HDR_VERSION_MASK;
      v |= (((n + 1) << HDR_VERSION_SHIFT) & HDR_VERSION_MASK);
      v &= ~HDR_LOCKED_MASK;
      assert(!IsLocked(v));
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
      assert(n <= NVersions);
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
      assert(n > 0 && n <= NVersions);
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
      size_t n = size();
      assert(n > 0 && n <= NVersions);
      return versions[n - 1] <= t;
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

    inline void
    write_record_at(tid_t t, record_t r)
    {
      assert(is_locked());
      size_t n = size();
      assert(n > 0 && n <= NVersions);
      assert(versions[n - 1] < t);
      if (n == NVersions) {
        // drop oldest version
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
      if (unlikely(!n))
        return;
      n->~logical_node();
      free(n);
    }

    static std::string
    VersionInfoStr(uint64_t v);

  } PACKED_CACHE_ALIGNED;

  transaction();
  ~transaction();

  void commit();

  // abort() always succeeds
  void abort();

  static tid_t current_global_tid(); // tid of the last commit
  static tid_t incr_and_get_global_tid();

  static void Test();

private:

  void clear();

  bool local_search(key_type k, record_t &v) const;

  struct read_record_t {
    tid_t t;
    record_t r;
    logical_node *ln;
  };

  // [a, b)
  struct key_range_t {
    key_range_t() : a(), has_b(true), b() {}
    key_range_t(key_type a) : a(a), has_b(false) {}
    key_range_t(key_type a, key_type b)
      : a(a), has_b(true), b(b)
    {
    }
    key_range_t(key_type a, bool has_b, key_type b)
      : a(a), has_b(has_b), b(b)
    {
    }

    key_type a;
    bool has_b; // false indicates infinity, true indicates use b
    key_type b; // has meaning only if !has_b

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
    key_in_range(key_type k) const
    {
      return a <= k && (!has_b || k < b);
    }
  };

  // NOTE: with this comparator, upper_bound() will return a pointer to the first
  // range which has upper bound greater than k (if one exists)- it does not
  // guarantee that the range returned has a lower bound <= k
  struct key_range_search_less_cmp {
    inline bool
    operator()(key_type k, const key_range_t &range) const
    {
      return !range.has_b || k < range.b;
    }
  };

  bool key_in_absent_set(key_type k) const;

  void add_absent_range(const key_range_t &range);

  static void AssertValidRangeSet(
      const std::vector<key_range_t> &range_set);

  static std::string PrintRangeSet(
      const std::vector<key_range_t> &range_set);

  const tid_t snapshot_tid;
  bool resolved;
  txn_btree *btree;
  std::map<key_type, read_record_t> read_set;
  std::map<key_type, record_t> write_set;
  std::vector<key_range_t> absent_range_set; // ranges do not overlap

  volatile static tid_t global_tid;

};

#endif /* _NDB_TXN_H_ */
