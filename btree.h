#pragma once

#include <assert.h>
#include <malloc.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <iostream>
#include <string>
#include <vector>
#include <utility>
#include <atomic>
#include <thread>

#include "log2.hh"
#include "ndb_type_traits.h"
#include "varkey.h"
#include "counter.h"
#include "macros.h"
#include "prefetch.h"
#include "amd64.h"
#include "rcu.h"
#include "util.h"
#include "small_vector.h"
#include "ownership_checker.h"

namespace private_ {
  template <typename T, typename P> struct u64manip;
  template <typename P>
  struct u64manip<uint64_t, P> {
    static inline uint64_t Load(uint64_t t) { return t; }
    static inline void Store(uint64_t &t, uint64_t v) { t = v; }
    static inline uint64_t
    Lock(uint64_t &t)
    {
#ifdef CHECK_INVARIANTS
      INVARIANT(!P::IsLocked(t));
      t |= P::HDR_LOCKED_MASK;
      return t;
#endif
      return 0;
    }
    static inline uint64_t
    LockWithSpinCount(uint64_t &t, unsigned &spins)
    {
      const uint64_t ret = Lock(t);
      spins = 0;
      return ret;
    }
    static inline void
    Unlock(uint64_t &t)
    {
#ifdef CHECK_INVARIANTS
      INVARIANT(P::IsLocked(t));
      t &= ~(P::HDR_LOCKED_MASK | P::HDR_MODIFYING_MASK);
#endif
    }
    static inline uint64_t
    StableVersion(uint64_t t)
    {
      INVARIANT(!P::IsModifying(t));
      return t;
    }
    static inline bool
    CheckVersion(uint64_t t, uint64_t stablev)
    {
      INVARIANT(!P::IsModifying(stablev));
      INVARIANT((t & ~P::HDR_LOCKED_MASK) == (stablev & ~P::HDR_LOCKED_MASK));
      return true;
    }
  };
  template <typename P>
  struct u64manip<std::atomic<uint64_t>, P> {
    static inline uint64_t
    Load(const std::atomic<uint64_t> &t)
    {
      return t.load(std::memory_order_acquire);
    }
    static inline void
    Store(std::atomic<uint64_t> &t, uint64_t v)
    {
      t.store(v, std::memory_order_release);
    }
    static inline uint64_t
    Lock(std::atomic<uint64_t> &t)
    {
#ifdef SPINLOCK_BACKOFF
      uint64_t backoff_shift = 0;
#endif
      uint64_t v = Load(t);
      while ((v & P::HDR_LOCKED_MASK) ||
             !t.compare_exchange_strong(v, v | P::HDR_LOCKED_MASK)) {
#ifdef SPINLOCK_BACKOFF
        if (backoff_shift < 63)
          backoff_shift++;
        uint64_t spins = (1UL << backoff_shift) * BACKOFF_SPINS_FACTOR;
        while (spins) {
          nop_pause();
          spins--;
        }
#else
        nop_pause();
#endif
        v = Load(t);
      }
      COMPILER_MEMORY_FENCE;
      return v;
    }
    static inline uint64_t
    LockWithSpinCount(std::atomic<uint64_t> &t, unsigned &spins)
    {
#ifdef SPINLOCK_BACKOFF
      uint64_t backoff_shift = 0;
#endif
      spins = 0;
      uint64_t v = Load(t);
      while ((v & P::HDR_LOCKED_MASK) ||
             !t.compare_exchange_strong(v, v | P::HDR_LOCKED_MASK)) {
#ifdef SPINLOCK_BACKOFF
        if (backoff_shift < 63)
          backoff_shift++;
        uint64_t backoff_spins = (1UL << backoff_shift) * BACKOFF_SPINS_FACTOR;
        while (backoff_spins) {
          nop_pause();
          backoff_spins--;
        }
#else
        nop_pause();
#endif
        v = Load(t);
        spins++;
      }
      COMPILER_MEMORY_FENCE;
      return v;
    }
    static inline void
    Unlock(std::atomic<uint64_t> &v)
    {
      INVARIANT(P::IsLocked(v));
      const uint64_t oldh = Load(v);
      uint64_t h = oldh;
      bool newv = false;
      if ((h & P::HDR_MODIFYING_MASK) ||
          (h & P::HDR_DELETING_MASK)) {
        newv = true;
        const uint64_t n = (h & P::HDR_VERSION_MASK) >> P::HDR_VERSION_SHIFT;
        h &= ~P::HDR_VERSION_MASK;
        h |= (((n + 1) << P::HDR_VERSION_SHIFT) & P::HDR_VERSION_MASK);
      }
      // clear locked + modifying bits
      h &= ~(P::HDR_LOCKED_MASK | P::HDR_MODIFYING_MASK);
      if (newv)
        INVARIANT(!CheckVersion(oldh, h));
      INVARIANT(!(h & P::HDR_LOCKED_MASK));
      INVARIANT(!(h & P::HDR_MODIFYING_MASK));
      COMPILER_MEMORY_FENCE;
      Store(v, h);
    }
    static inline uint64_t
    StableVersion(const std::atomic<uint64_t> &t)
    {
      uint64_t v = Load(t);
      while ((v & P::HDR_MODIFYING_MASK)) {
        nop_pause();
        v = Load(t);
      }
      COMPILER_MEMORY_FENCE;
      return v;
    }
    static inline bool
    CheckVersion(uint64_t t, uint64_t stablev)
    {
      INVARIANT(!(stablev & P::HDR_MODIFYING_MASK));
      COMPILER_MEMORY_FENCE;
      return (t & ~P::HDR_LOCKED_MASK) ==
             (stablev & ~P::HDR_LOCKED_MASK);
    }
  };
}

/**
 * manipulates a btree version
 *
 * hdr bits: layout is (actual bytes depend on the NKeysPerNode parameter)
 *
 * <-- low bits
 * [type | key_slots_used | locked | is_root | modifying | deleting | version ]
 * [0:1  | 1:5            | 5:6    | 6:7     | 7:8       | 8:9      | 9:64    ]
 *
 * bit invariants:
 *   1) modifying => locked
 *   2) deleting  => locked
 *
 * WARNING: the correctness of our concurrency scheme relies on being able
 * to do a memory reads/writes from/to hdr atomically. x86 architectures
 * guarantee that aligned writes are atomic (see intel spec)
 */
template <typename VersionType, unsigned NKeysPerNode>
class btree_version_manip {

  typedef
    typename private_::typeutil<VersionType>::func_param_type
    LoadVersionType;

  typedef
    private_::u64manip<
      VersionType,
      btree_version_manip<VersionType, NKeysPerNode>>
    U64Manip;

  static inline constexpr uint64_t
  LowMask(uint64_t nbits)
  {
    return (1UL << nbits) - 1UL;
  }

public:

  static const uint64_t HDR_TYPE_BITS = 1;
  static const uint64_t HDR_TYPE_MASK = 0x1;

  static const uint64_t HDR_KEY_SLOTS_SHIFT = HDR_TYPE_BITS;
  static const uint64_t HDR_KEY_SLOTS_BITS = ceil_log2_const(NKeysPerNode);
  static const uint64_t HDR_KEY_SLOTS_MASK = LowMask(HDR_KEY_SLOTS_BITS) << HDR_KEY_SLOTS_SHIFT;

  static const uint64_t HDR_LOCKED_SHIFT = HDR_KEY_SLOTS_SHIFT + HDR_KEY_SLOTS_BITS;
  static const uint64_t HDR_LOCKED_BITS = 1;
  static const uint64_t HDR_LOCKED_MASK = LowMask(HDR_LOCKED_BITS) << HDR_LOCKED_SHIFT;

  static const uint64_t HDR_IS_ROOT_SHIFT = HDR_LOCKED_SHIFT + HDR_LOCKED_BITS;
  static const uint64_t HDR_IS_ROOT_BITS = 1;
  static const uint64_t HDR_IS_ROOT_MASK = LowMask(HDR_IS_ROOT_BITS) << HDR_IS_ROOT_SHIFT;

  static const uint64_t HDR_MODIFYING_SHIFT = HDR_IS_ROOT_SHIFT + HDR_IS_ROOT_BITS;
  static const uint64_t HDR_MODIFYING_BITS = 1;
  static const uint64_t HDR_MODIFYING_MASK = LowMask(HDR_MODIFYING_BITS) << HDR_MODIFYING_SHIFT;

  static const uint64_t HDR_DELETING_SHIFT = HDR_MODIFYING_SHIFT + HDR_MODIFYING_BITS;
  static const uint64_t HDR_DELETING_BITS = 1;
  static const uint64_t HDR_DELETING_MASK = LowMask(HDR_DELETING_BITS) << HDR_DELETING_SHIFT;

  static const uint64_t HDR_VERSION_SHIFT = HDR_DELETING_SHIFT + HDR_DELETING_BITS;
  static const uint64_t HDR_VERSION_MASK = ((uint64_t)-1) << HDR_VERSION_SHIFT;

  // sanity checks
  static_assert(NKeysPerNode >= 1, "XX");

  static_assert(std::numeric_limits<uint64_t>::max() == (
      HDR_TYPE_MASK |
      HDR_KEY_SLOTS_MASK |
      HDR_LOCKED_MASK |
      HDR_IS_ROOT_MASK |
      HDR_MODIFYING_MASK |
      HDR_DELETING_MASK |
      HDR_VERSION_MASK
        ), "XX");

  static_assert( !(HDR_TYPE_MASK & HDR_KEY_SLOTS_MASK) , "XX");
  static_assert( !(HDR_KEY_SLOTS_MASK & HDR_LOCKED_MASK) , "XX");
  static_assert( !(HDR_LOCKED_MASK & HDR_IS_ROOT_MASK) , "XX");
  static_assert( !(HDR_IS_ROOT_MASK & HDR_MODIFYING_MASK) , "XX");
  static_assert( !(HDR_MODIFYING_MASK & HDR_DELETING_MASK) , "XX");
  static_assert( !(HDR_DELETING_MASK & HDR_VERSION_MASK) , "XX");

  // low level ops

  static inline uint64_t
  Load(LoadVersionType v)
  {
    return U64Manip::Load(v);
  }

  static inline void
  Store(VersionType &t, uint64_t v)
  {
    U64Manip::Store(t, v);
  }

  // accessors

  static inline bool
  IsLeafNode(LoadVersionType v)
  {
    return (Load(v) & HDR_TYPE_MASK) == 0;
  }

  static inline bool
  IsInternalNode(LoadVersionType v)
  {
    return !IsLeafNode(v);
  }

  static inline size_t
  KeySlotsUsed(LoadVersionType v)
  {
    return (Load(v) & HDR_KEY_SLOTS_MASK) >> HDR_KEY_SLOTS_SHIFT;
  }

  static inline bool
  IsLocked(LoadVersionType v)
  {
    return (Load(v) & HDR_LOCKED_MASK);
  }

  static inline bool
  IsRoot(LoadVersionType v)
  {
    return (Load(v) & HDR_IS_ROOT_MASK);
  }

  static inline bool
  IsModifying(LoadVersionType v)
  {
    return (Load(v) & HDR_MODIFYING_MASK);
  }

  static inline bool
  IsDeleting(LoadVersionType v)
  {
    return (Load(v) & HDR_DELETING_MASK);
  }

  static inline uint64_t
  Version(LoadVersionType v)
  {
    return (Load(v) & HDR_VERSION_MASK) >> HDR_VERSION_SHIFT;
  }

  static std::string
  VersionInfoStr(LoadVersionType v)
  {
    std::ostringstream buf;
    buf << "[";

    if (IsLeafNode(v))
      buf << "LEAF";
    else
      buf << "INT";
    buf << " | ";

    buf << KeySlotsUsed(v) << " | ";

    if (IsLocked(v))
      buf << "LOCKED";
    else
      buf << "-";
    buf << " | ";

    if (IsRoot(v))
      buf << "ROOT";
    else
      buf << "-";
    buf << " | ";

    if (IsModifying(v))
      buf << "MOD";
    else
      buf << "-";
    buf << " | ";

    if (IsDeleting(v))
      buf << "DEL";
    else
      buf << "-";
    buf << " | ";

    buf << Version(v);

    buf << "]";
    return buf.str();
  }

  // mutators

  static inline void
  SetKeySlotsUsed(VersionType &v, size_t n)
  {
    INVARIANT(n <= NKeysPerNode);
    INVARIANT(IsModifying(v));
    uint64_t h = Load(v);
    h &= ~HDR_KEY_SLOTS_MASK;
    h |= (n << HDR_KEY_SLOTS_SHIFT);
    Store(v, h);
  }

  static inline void
  IncKeySlotsUsed(VersionType &v)
  {
    SetKeySlotsUsed(v, KeySlotsUsed(v) + 1);
  }

  static inline void
  DecKeySlotsUsed(VersionType &v)
  {
    INVARIANT(KeySlotsUsed(v) > 0);
    SetKeySlotsUsed(v, KeySlotsUsed(v) - 1);
  }

  static inline void
  SetRoot(VersionType &v)
  {
    INVARIANT(IsLocked(v));
    INVARIANT(!IsRoot(v));
    uint64_t h = Load(v);
    h |= HDR_IS_ROOT_MASK;
    Store(v, h);
  }

  static inline void
  ClearRoot(VersionType &v)
  {
    INVARIANT(IsLocked(v));
    INVARIANT(IsRoot(v));
    uint64_t h = Load(v);
    h &= ~HDR_IS_ROOT_MASK;
    Store(v, h);
  }

  static inline void
  MarkModifying(VersionType &v)
  {
    INVARIANT(IsLocked(v));
    INVARIANT(!IsModifying(v));
    uint64_t h = Load(v);
    h |= HDR_MODIFYING_MASK;
    Store(v, h);
  }

  static inline void
  MarkDeleting(VersionType &v)
  {
    INVARIANT(IsLocked(v));
    INVARIANT(!IsDeleting(v));
    uint64_t h = Load(v);
    h |= HDR_DELETING_MASK;
    Store(v, h);
  }

  // concurrency control

  static inline uint64_t
  StableVersion(LoadVersionType v)
  {
    return U64Manip::StableVersion(v);
  }

  static inline uint64_t
  UnstableVersion(LoadVersionType v)
  {
    return Load(v);
  }

  static inline bool
  CheckVersion(LoadVersionType v, uint64_t stablev)
  {
    return U64Manip::CheckVersion(Load(v), stablev);
  }

  static inline uint64_t
  Lock(VersionType &v)
  {
    return U64Manip::Lock(v);
  }

  static inline uint64_t
  LockWithSpinCount(VersionType &v, unsigned &spins)
  {
    return U64Manip::LockWithSpinCount(v, spins);
  }

  static inline void
  Unlock(VersionType &v)
  {
    U64Manip::Unlock(v);
  }
};

struct base_btree_config {
  static const unsigned int NKeysPerNode = 15;
  static const bool RcuRespCaller = true;
};

struct concurrent_btree_traits : public base_btree_config {
  typedef std::atomic<uint64_t> VersionType;
};

struct single_threaded_btree_traits : public base_btree_config {
  typedef uint64_t VersionType;
};

/**
 * A concurrent, variable key length b+-tree, optimized for read heavy
 * workloads.
 *
 * This b+-tree maps uninterpreted binary strings (key_type) of arbitrary
 * length to a single pointer (value_type). Binary string values are copied
 * into the b+-tree, so the caller does not have to worry about preserving
 * memory for key values.
 *
 * This b+-tree does not manage the memory pointed to by value_type. The
 * pointer is treated completely opaquely.
 *
 * So far, this b+-tree has only been tested on 64-bit intel x86 processors.
 * It's correctness, as it is implemented (not conceptually), requires the
 * semantics of total store order (TSO) for correctness. To fix this, we would
 * change compiler fences into actual memory fences, at the very least.
 */
template <typename P>
class btree {
  template <template <typename> class, typename>
    friend class base_txn_btree;
public:
  typedef varkey key_type;
  typedef std::string string_type;
  typedef uint64_t key_slice;
  typedef uint8_t* value_type;
  typedef typename std::conditional<!P::RcuRespCaller,
      scoped_rcu_region,
      disabled_rcu_region>::type rcu_region;

  // public to assist in testing
  static const unsigned int NKeysPerNode    = P::NKeysPerNode;
  static const unsigned int NMinKeysPerNode = P::NKeysPerNode / 2;

private:

  typedef std::pair<ssize_t, size_t> key_search_ret;

  typedef
    btree_version_manip<typename P::VersionType, NKeysPerNode>
    VersionManip;
  typedef
    btree_version_manip<uint64_t, NKeysPerNode>
    RawVersionManip;

  struct node {

    typename P::VersionType hdr_;

#ifdef BTREE_LOCK_OWNERSHIP_CHECKING
    std::thread::id lock_owner_;
#endif /* BTREE_LOCK_OWNERSHIP_CHECKING */

    /**
     * Keys are assumed to be stored in contiguous sorted order, so that all
     * the used slots are grouped together. That is, elems in positions
     * [0, key_slots_used) are valid, and elems in positions
     * [key_slots_used, NKeysPerNode) are empty
     */
    key_slice keys_[NKeysPerNode];

    node() :
      hdr_()
#ifdef BTREE_LOCK_OWNERSHIP_CHECKING
      , lock_owner_()
#endif
    {}
    ~node()
    {
      INVARIANT(!is_locked());
      INVARIANT(is_deleting());
    }

    inline bool
    is_leaf_node() const
    {
      return VersionManip::IsLeafNode(hdr_);
    }

    inline bool
    is_internal_node() const
    {
      return !is_leaf_node();
    }

    inline size_t
    key_slots_used() const
    {
      return VersionManip::KeySlotsUsed(hdr_);
    }

    inline void
    set_key_slots_used(size_t n)
    {
      VersionManip::SetKeySlotsUsed(hdr_, n);
    }

    inline void
    inc_key_slots_used()
    {
      VersionManip::IncKeySlotsUsed(hdr_);
    }

    inline void
    dec_key_slots_used()
    {
      VersionManip::DecKeySlotsUsed(hdr_);
    }

    inline bool
    is_locked() const
    {
      return VersionManip::IsLocked(hdr_);
    }

#ifdef BTREE_LOCK_OWNERSHIP_CHECKING
    inline bool
    is_lock_owner() const
    {
      return std::this_thread::get_id() == lock_owner_;
    }
#else
    inline bool
    is_lock_owner() const
    {
      return true;
    }
#endif /* BTREE_LOCK_OWNERSHIP_CHECKING */

    inline uint64_t
    lock()
    {
#ifdef ENABLE_EVENT_COUNTERS
      static event_avg_counter
        evt_avg_btree_leaf_node_lock_acquire_spins(
            util::cxx_typename<btree<P>>::value() +
            std::string("_avg_btree_leaf_node_lock_acquire_spins"));
      static event_avg_counter
        evt_avg_btree_internal_node_lock_acquire_spins(
            util::cxx_typename<btree<P>>::value() +
            std::string("_avg_btree_internal_node_lock_acquire_spins"));
      unsigned spins;
      const uint64_t ret = VersionManip::LockWithSpinCount(hdr_, spins);
      if (is_leaf_node())
        evt_avg_btree_leaf_node_lock_acquire_spins.offer(spins);
      else
        evt_avg_btree_internal_node_lock_acquire_spins.offer(spins);
#else
      const uint64_t ret = VersionManip::Lock(hdr_);
#endif
#ifdef BTREE_LOCK_OWNERSHIP_CHECKING
      lock_owner_ = std::this_thread::get_id();
      AddNodeToLockRegion(this);
      INVARIANT(is_lock_owner());
#endif
      return ret;
    }

    inline void
    unlock()
    {
#ifdef BTREE_LOCK_OWNERSHIP_CHECKING
      lock_owner_ = std::thread::id();
      INVARIANT(!is_lock_owner());
#endif
      VersionManip::Unlock(hdr_);
    }

    inline bool
    is_root() const
    {
      return VersionManip::IsRoot(hdr_);
    }

    inline void
    set_root()
    {
      VersionManip::SetRoot(hdr_);
    }

    inline void
    clear_root()
    {
      VersionManip::ClearRoot(hdr_);
    }

    inline bool
    is_modifying() const
    {
      return VersionManip::IsModifying(hdr_);
    }

    inline void
    mark_modifying()
    {
      VersionManip::MarkModifying(hdr_);
    }

    inline bool
    is_deleting() const
    {
      return VersionManip::IsDeleting(hdr_);
    }

    inline void
    mark_deleting()
    {
      VersionManip::MarkDeleting(hdr_);
    }

    inline uint64_t
    unstable_version() const
    {
      return VersionManip::UnstableVersion(hdr_);
    }

    /**
     * spin until we get a version which is not modifying (but can be locked)
     */
    inline uint64_t
    stable_version() const
    {
      return VersionManip::StableVersion(hdr_);
    }

    inline bool
    check_version(uint64_t version) const
    {
      return VersionManip::CheckVersion(hdr_, version);
    }

    inline std::string
    version_info_str() const
    {
      return VersionManip::VersionInfoStr(hdr_);
    }

    void base_invariant_unique_keys_check() const;

    // [min_key, max_key)
    void
    base_invariant_checker(const key_slice *min_key,
                           const key_slice *max_key,
                           bool is_root) const;

    /** manually simulated virtual function (so we don't make node virtual) */
    void
    invariant_checker(const key_slice *min_key,
                      const key_slice *max_key,
                      const node *left_sibling,
                      const node *right_sibling,
                      bool is_root) const;

    /** another manually simulated virtual function */
    inline void prefetch() const;
  };

  struct leaf_node : public node {
    union value_or_node_ptr {
      value_type v_;
      node *n_;
    };

    key_slice min_key_; // really is min_key's key slice
    value_or_node_ptr values_[NKeysPerNode];

    // format is:
    // [ slice_length | type | unused ]
    // [    0:4       |  4:5 |  5:8   ]
    uint8_t lengths_[NKeysPerNode];

    leaf_node *prev_;
    leaf_node *next_;

    // starts out empty- once set, doesn't get freed until dtor (even if all
    // keys w/ suffixes get removed)
    imstring *suffixes_;

    inline ALWAYS_INLINE varkey
    suffix(size_t i) const
    {
      return suffixes_ ? varkey(suffixes_[i]) : varkey();
    }

    //static event_counter g_evt_suffixes_array_created;

    inline void
    alloc_suffixes()
    {
      INVARIANT(this->is_modifying());
      INVARIANT(!suffixes_);
      suffixes_ = new imstring[NKeysPerNode];
      //++g_evt_suffixes_array_created;
    }

    inline void
    ensure_suffixes()
    {
      INVARIANT(this->is_modifying());
      if (!suffixes_)
        alloc_suffixes();
    }

    leaf_node();
    ~leaf_node();

    static const uint64_t LEN_LEN_MASK = 0xf;

    static const uint64_t LEN_TYPE_SHIFT = 4;
    static const uint64_t LEN_TYPE_MASK = 0x1 << LEN_TYPE_SHIFT;

    inline void
    prefetch() const
    {
#ifdef BTREE_NODE_PREFETCH
      prefetch_object(this);
#endif
    }

    inline size_t
    keyslice_length(size_t n) const
    {
      INVARIANT(n < NKeysPerNode);
      return lengths_[n] & LEN_LEN_MASK;
    }

    inline void
    keyslice_set_length(size_t n, size_t len, bool layer)
    {
      INVARIANT(n < NKeysPerNode);
      INVARIANT(this->is_modifying());
      INVARIANT(len <= 9);
      INVARIANT(!layer || len == 9);
      lengths_[n] = (len | (layer ? LEN_TYPE_MASK : 0));
    }

    inline bool
    value_is_layer(size_t n) const
    {
      INVARIANT(n < NKeysPerNode);
      return lengths_[n] & LEN_TYPE_MASK;
    }

    inline void
    value_set_layer(size_t n)
    {
      INVARIANT(n < NKeysPerNode);
      INVARIANT(this->is_modifying());
      INVARIANT(keyslice_length(n) == 9);
      INVARIANT(!value_is_layer(n));
      lengths_[n] |= LEN_TYPE_MASK;
    }

    /**
     * keys[key_search(k).first] == k if key_search(k).first != -1
     * key does not exist otherwise. considers key length also
     */
    inline key_search_ret
    key_search(key_slice k, size_t len) const
    {
      size_t n = this->key_slots_used();
      ssize_t lower = 0;
      ssize_t upper = n;
      while (lower < upper) {
        ssize_t i = (lower + upper) / 2;
        key_slice k0 = this->keys_[i];
        size_t len0 = this->keyslice_length(i);
        if (k0 < k || (k0 == k && len0 < len))
          lower = i + 1;
        else if (k0 == k && len0 == len)
          return key_search_ret(i, n);
        else
          upper = i;
      }
      return key_search_ret(-1, n);
    }

    /**
     * tightest lower bound key, -1 if no such key exists. operates only
     * on key slices (internal nodes have unique key slices)
     */
    inline key_search_ret
    key_lower_bound_search(key_slice k, size_t len) const
    {
      ssize_t ret = -1;
      size_t n = this->key_slots_used();
      ssize_t lower = 0;
      ssize_t upper = n;
      while (lower < upper) {
        ssize_t i = (lower + upper) / 2;
        key_slice k0 = this->keys_[i];
        size_t len0 = this->keyslice_length(i);
        if (k0 < k || (k0 == k && len0 < len)) {
          ret = i;
          lower = i + 1;
        } else if (k0 == k && len0 == len) {
          return key_search_ret(i, n);
        } else {
          upper = i;
        }
      }
      return key_search_ret(ret, n);
    }

    void
    invariant_checker_impl(const key_slice *min_key,
                           const key_slice *max_key,
                           const node *left_sibling,
                           const node *right_sibling,
                           bool is_root) const;

    static inline leaf_node*
    alloc()
    {
      void * const p = rcu::s_instance.alloc(LeafNodeAllocSize);
      INVARIANT(p);
      return new (p) leaf_node;
    }

    static void
    deleter(void *p)
    {
      leaf_node *n = (leaf_node *) p;
      INVARIANT(n->is_deleting());
      INVARIANT(!n->is_locked());
      n->~leaf_node();
      rcu::s_instance.dealloc(p, LeafNodeAllocSize);
    }

    static inline void
    release(leaf_node *n)
    {
      if (unlikely(!n))
        return;
      n->mark_deleting();
      rcu::s_instance.free_with_fn(n, deleter);
    }

  };

  struct internal_node : public node {
    /**
     * child at position child_idx is responsible for keys
     * [keys[child_idx - 1], keys[child_idx])
     *
     * in the case where child_idx == 0 or child_idx == this->key_slots_used(), then
     * the responsiblity value of the min/max key, respectively, is determined
     * by the parent
     */
    node *children_[NKeysPerNode + 1];

    internal_node();
    ~internal_node();

    inline void
    prefetch() const
    {
#ifdef BTREE_NODE_PREFETCH
      prefetch_object(this);
#endif
    }

    /**
     * keys[key_search(k).first] == k if key_search(k).first != -1
     * key does not exist otherwise. operates ony on key slices
     * (internal nodes have unique key slices)
     */
    inline key_search_ret
    key_search(key_slice k) const
    {
      size_t n = this->key_slots_used();
      ssize_t lower = 0;
      ssize_t upper = n;
      while (lower < upper) {
        ssize_t i = (lower + upper) / 2;
        key_slice k0 = this->keys_[i];
        if (k0 == k)
          return key_search_ret(i, n);
        else if (k0 > k)
          upper = i;
        else
          lower = i + 1;
      }
      return key_search_ret(-1, n);
    }

    /**
     * tightest lower bound key, -1 if no such key exists. operates only
     * on key slices (internal nodes have unique key slices)
     */
    inline key_search_ret
    key_lower_bound_search(key_slice k) const
    {
      ssize_t ret = -1;
      size_t n = this->key_slots_used();
      ssize_t lower = 0;
      ssize_t upper = n;
      while (lower < upper) {
        ssize_t i = (lower + upper) / 2;
        key_slice k0 = this->keys_[i];
        if (k0 == k)
          return key_search_ret(i, n);
        else if (k0 > k)
          upper = i;
        else {
          ret = i;
          lower = i + 1;
        }
      }
      return key_search_ret(ret, n);
    }

    void
    invariant_checker_impl(const key_slice *min_key,
                           const key_slice *max_key,
                           const node *left_sibling,
                           const node *right_sibling,
                           bool is_root) const;

    // XXX: alloc(), deleter(), and release() are copied from leaf_node-
    // we should templatize them to avoid code duplication

    static inline internal_node*
    alloc()
    {
      void * const p = rcu::s_instance.alloc(InternalNodeAllocSize);
      INVARIANT(p);
      return new (p) internal_node;
    }

    static void
    deleter(void *p)
    {
      internal_node *n = (internal_node *) p;
      INVARIANT(n->is_deleting());
      INVARIANT(!n->is_locked());
      n->~internal_node();
      rcu::s_instance.dealloc(p, InternalNodeAllocSize);
    }

    static inline void
    release(internal_node *n)
    {
      if (unlikely(!n))
        return;
      n->mark_deleting();
      rcu::s_instance.free_with_fn(n, deleter);
    }

  } PACKED;

#ifdef BTREE_LOCK_OWNERSHIP_CHECKING
public:
  static inline void
  NodeLockRegionBegin()
  {
    ownership_checker<btree<P>, typename btree<P>::node>::NodeLockRegionBegin();
  }
  static inline void
  AssertAllNodeLocksReleased()
  {
    ownership_checker<btree<P>, typename btree<P>::node>::AssertAllNodeLocksReleased();
  }
private:
  static inline void
  AddNodeToLockRegion(const node *n)
  {
    ownership_checker<btree<P>, typename btree<P>::node>::AddNodeToLockRegion(n);
  }
#endif

#ifdef BTREE_NODE_ALLOC_CACHE_ALIGNED
  static const size_t LeafNodeAllocSize = util::round_up<size_t, LG_CACHELINE_SIZE>(sizeof(leaf_node));
  static const size_t InternalNodeAllocSize = util::round_up<size_t, LG_CACHELINE_SIZE>(sizeof(internal_node));
#else
  static const size_t LeafNodeAllocSize = sizeof(leaf_node);
  static const size_t InternalNodeAllocSize = sizeof(internal_node);
#endif

  static inline leaf_node*
  AsLeaf(node *n)
  {
    INVARIANT(!n || n->is_leaf_node());
    return static_cast<leaf_node *>(n);
  }

  static inline const leaf_node*
  AsLeaf(const node *n)
  {
    return AsLeaf(const_cast<node *>(n));
  }

  static inline internal_node*
  AsInternal(node *n)
  {
    INVARIANT(!n || n->is_internal_node());
    return static_cast<internal_node *>(n);
  }

  static inline const internal_node*
  AsInternal(const node *n)
  {
    return AsInternal(const_cast<node *>(n));
  }

  static inline leaf_node *
  AsLeafCheck(node *n, uint64_t v)
  {
    return likely(n) && RawVersionManip::IsLeafNode(v) ?
      static_cast<leaf_node *>(n) : NULL;
  }

  static inline leaf_node*
  AsLeafCheck(node *n)
  {
    return likely(n) && n->is_leaf_node() ?
      static_cast<leaf_node *>(n) : NULL;
  }

  static inline const leaf_node*
  AsLeafCheck(const node *n, uint64_t v)
  {
    return AsLeafCheck(const_cast<node *>(n), v);
  }

  static inline const leaf_node*
  AsLeafCheck(const node *n)
  {
    return AsLeafCheck(const_cast<node *>(n));
  }

  static inline internal_node*
  AsInternalCheck(node *n)
  {
    return likely(n) && n->is_internal_node() ? static_cast<internal_node *>(n) : NULL;
  }

  static inline const internal_node*
  AsInternalCheck(const node *n)
  {
    return AsInternalCheck(const_cast<node *>(n));
  }

  static inline void
  UnlockNodes(typename util::vec<node *>::type &locked_nodes)
  {
    for (auto it = locked_nodes.begin(); it != locked_nodes.end(); ++it)
      (*it)->unlock();
    locked_nodes.clear();
  }

  template <typename T>
  static inline T
  UnlockAndReturn(typename util::vec<node *>::type &locked_nodes, T t)
  {
    UnlockNodes(locked_nodes);
    return t;
  }

  static bool
  CheckVersion(uint64_t a, uint64_t b)
  {
    return VersionManip::CheckVersion(a, b);
  }

  /**
   * Is not thread safe, and does not use RCU to free memory
   *
   * Should only be called when there are no outstanding operations on
   * any nodes reachable from n
   */
  static void recursive_delete(node *n);

  node *volatile root_;

public:

  // XXX(stephentu): trying out a very opaque node API for now
  typedef struct node node_opaque_t;
  typedef std::pair< const node_opaque_t *, uint64_t > versioned_node_t;
  struct insert_info_t {
    const node_opaque_t* node;
    uint64_t old_version;
    uint64_t new_version;
  };

  btree() : root_(leaf_node::alloc())
  {
    static_assert(
        NKeysPerNode > (sizeof(key_slice) + 2), "XX"); // so we can always do a split
    static_assert(
        NKeysPerNode <=
        (VersionManip::HDR_KEY_SLOTS_MASK >> VersionManip::HDR_KEY_SLOTS_SHIFT), "XX");

#ifdef CHECK_INVARIANTS
    root_->lock();
    root_->set_root();
    root_->unlock();
#else
    root_->set_root();
#endif /* CHECK_INVARIANTS */
  }

  ~btree()
  {
    // NOTE: it is assumed on deletion time there are no
    // outstanding requests to the btree, so deletion proceeds
    // in a non-threadsafe manner
    recursive_delete(root_);
    root_ = NULL;
  }

  /**
   * NOT THREAD SAFE
   */
  inline void
  clear()
  {
    recursive_delete(root_);
    root_ = leaf_node::alloc();
#ifdef CHECK_INVARIANTS
    root_->lock();
    root_->set_root();
    root_->unlock();
#else
    root_->set_root();
#endif /* CHECK_INVARIANTS */
  }

  /** Note: invariant checking is not thread safe */
  inline void
  invariant_checker() const
  {
    root_->invariant_checker(NULL, NULL, NULL, NULL, true);
  }

          /** NOTE: the public interface assumes that the caller has taken care
           * of setting up RCU */

  inline bool
  search(const key_type &k, value_type &v,
         versioned_node_t *search_info = nullptr) const
  {
    rcu_region guard;
    typename util::vec<leaf_node *>::type ns;
    return search_impl(k, v, ns, search_info);
  }

  /**
   * The low level callback interface is as follows:
   *
   * Consider a scan in the range [a, b):
   *   1) on_resp_node() is called at least once per node which
   *      has a responibility range that overlaps with the scan range
   *   2) invoke() is called per <k, v>-pair such that k is in [a, b)
   *
   * The order of calling on_resp_node() and invoke() is up to the implementation.
   */
  class low_level_search_range_callback {
  public:
    virtual ~low_level_search_range_callback() {}

    /**
     * This node lies within the search range (at version v)
     */
    virtual void on_resp_node(const node_opaque_t *n, uint64_t version) = 0;

    /**
     * This key/value pair was read from node n @ version
     */
    virtual bool invoke(const string_type &k, value_type v,
                        const node_opaque_t *n, uint64_t version) = 0;
  };

  /**
   * A higher level interface if you don't care about node and version numbers
   */
  class search_range_callback : public low_level_search_range_callback {
  public:
    virtual void
    on_resp_node(const node_opaque_t *n, uint64_t version)
    {
    }

    virtual bool
    invoke(const string_type &k, value_type v,
           const node_opaque_t *n, uint64_t version)
    {
      return invoke(k, v);
    }

    virtual bool invoke(const string_type &k, value_type v) = 0;
  };

private:
  template <typename T>
  class type_callback_wrapper : public search_range_callback {
  public:
    type_callback_wrapper(T *callback) : callback_(callback) {}
    virtual bool
    invoke(const string_type &k, value_type v)
    {
      return callback_->operator()(k, v);
    }
  private:
    T *const callback_;
  };

  struct leaf_kvinfo {
    key_slice key_; // in host endian
    key_slice key_big_endian_;
    typename leaf_node::value_or_node_ptr vn_;
    bool layer_;
    size_t length_;
    varkey suffix_;
    leaf_kvinfo() {} // for STL
    leaf_kvinfo(key_slice key,
                typename leaf_node::value_or_node_ptr vn,
                bool layer,
                size_t length,
                const varkey &suffix)
      : key_(key), key_big_endian_(util::big_endian_trfm<key_slice>()(key)),
        vn_(vn), layer_(layer), length_(length), suffix_(suffix)
    {}

    inline const char *
    keyslice() const
    {
      return (const char *) &key_big_endian_;
    }
  };

  bool search_range_at_layer(leaf_node *leaf,
                             string_type &prefix,
                             const key_type &lower,
                             bool inc_lower,
                             const key_type *upper,
                             low_level_search_range_callback &callback) const;

public:

  /**
   * For all keys in [lower, *upper), invoke callback in ascending order.
   * If upper is NULL, then there is no upper bound
   *

   * This function by default provides a weakly consistent view of the b-tree. For
   * instance, consider the following tree, where n = 3 is the max number of
   * keys in a node:
   *
   *              [D|G]
   *             /  |  \
   *            /   |   \
   *           /    |    \
   *          /     |     \
   *   [A|B|C]<->[D|E|F]<->[G|H|I]
   *
   * Suppose we want to scan [A, inf), so we traverse to the leftmost leaf node
   * and start a left-to-right walk. Suppose we have emitted keys A, B, and C,
   * and we are now just about to scan the middle leaf node.  Now suppose
   * another thread concurrently does delete(A), followed by a delete(H).  Now
   * the scaning thread resumes and emits keys D, E, F, G, and I, omitting H
   * because H was deleted. This is an inconsistent view of the b-tree, since
   * the scanning thread has observed the deletion of H but did not observe the
   * deletion of A, but we know that delete(A) happens before delete(H).
   *
   * The weakly consistent guarantee provided is the following: all keys
   * which, at the time of invocation, are known to exist in the btree
   * will be discovered on a scan (provided the key falls within the scan's range),
   * and provided there are no concurrent modifications/removals of that key
   *
   * Note that scans within a single node are consistent
   *
   * XXX: add other modes which provide better consistency:
   * A) locking mode
   * B) optimistic validation mode
   *
   * the last string parameter is an optional string buffer to use:
   * if null, a stack allocated string will be used. if not null, must
   * ensure:
   *   A) buf->empty() at the beginning
   *   B) no concurrent mutation of string
   * note that string contents upon return are arbitrary
   */
  void
  search_range_call(const key_type &lower,
                    const key_type *upper,
                    low_level_search_range_callback &callback,
                    string_type *buf = nullptr) const;

  // (lower, upper]
  void
  rsearch_range_call(const key_type &upper,
                     const key_type *lower,
                     low_level_search_range_callback &callback,
                     std::string *buf = nullptr) const
  {
    NDB_UNIMPLEMENTED("rsearch_range_call");
  }

  /**
   * Callback is expected to implement bool operator()(key_slice k, value_type v),
   * where the callback returns true if it wants to keep going, false otherwise
   *
   */
  template <typename T>
  inline void
  search_range(const key_type &lower,
               const key_type *upper,
               T &callback,
               string_type *buf = nullptr) const
  {
    type_callback_wrapper<T> w(&callback);
    search_range_call(lower, upper, w, buf);
  }

  template <typename F>
  inline void
  rsearch_range(const key_type &upper,
                const key_type *lower,
                F& callback,
                std::string *buf = nullptr) const
  {
    NDB_UNIMPLEMENTED("rsearch_range");
  }

  /**
   * returns true if key k did not already exist, false otherwise
   * If k exists with a different mapping, still returns false
   *
   * If false and old_v is not NULL, then the overwritten value of v
   * is written into old_v
   */
  inline bool
  insert(const key_type &k, value_type v,
         value_type *old_v = NULL,
         insert_info_t *insert_info = NULL)
  {
    rcu_region guard;
    return insert_stable_location((node **) &root_, k, v, false, old_v, insert_info);
  }

  /**
   * Only puts k=>v if k does not exist in map. returns true
   * if k inserted, false otherwise (k exists already)
   */
  inline bool
  insert_if_absent(const key_type &k, value_type v,
                   insert_info_t *insert_info = NULL)
  {
    rcu_region guard;
    return insert_stable_location((node **) &root_, k, v, true, NULL, insert_info);
  }

  /**
   * return true if a value was removed, false otherwise.
   *
   * if true and old_v is not NULL, then the removed value of v
   * is written into old_v
   */
  inline bool
  remove(const key_type &k, value_type *old_v = NULL)
  {
    rcu_region guard;
    return remove_stable_location((node **) &root_, k, old_v);
  }

private:
  bool
  insert_stable_location(node **root_location, const key_type &k, value_type v,
                         bool only_if_absent, value_type *old_v,
                         insert_info_t *insert_info);

  bool
  remove_stable_location(node **root_location, const key_type &k, value_type *old_v);

public:

  /**
   * The tree walk API is a bit strange, due to the optimistic nature of the
   * btree.
   *
   * The way it works is that, on_node_begin() is first called. In
   * on_node_begin(), a callback function should read (but not modify) the
   * values it is interested in, and save them.
   *
   * Then, either one of on_node_success() or on_node_failure() is called. If
   * on_node_success() is called, then the previous values read in
   * on_node_begin() are indeed valid.  If on_node_failure() is called, then
   * the previous values are not valid and should be discarded.
   */
  class tree_walk_callback {
  public:
    virtual ~tree_walk_callback() {}
    virtual void on_node_begin(const node_opaque_t *n) = 0;
    virtual void on_node_success() = 0;
    virtual void on_node_failure() = 0;
  };

  void tree_walk(tree_walk_callback &callback) const;

private:
  class size_walk_callback : public tree_walk_callback {
  public:
    size_walk_callback() : spec_size_(0), size_(0) {}
    virtual void on_node_begin(const node_opaque_t *n);
    virtual void on_node_success();
    virtual void on_node_failure();
    inline size_t get_size() const { return size_; }
  private:
    size_t spec_size_;
    size_t size_;
  };

public:
  /**
   * Is thread-safe, but not really designed to perform well with concurrent
   * modifications. also the value returned is not consistent given concurrent
   * modifications
   */
  inline size_t
  size() const
  {
    size_walk_callback c;
    tree_walk(c);
    return c.get_size();
  }

  static inline uint64_t
  ExtractVersionNumber(const node_opaque_t *n)
  {
    // XXX(stephentu): I think we must use stable_version() for
    // correctness, but I am not 100% sure. It's definitely correct to use it,
    // but maybe we can get away with unstable_version()?
    return RawVersionManip::Version(n->stable_version());
  }

  // [value, has_suffix]
  static std::vector< std::pair<value_type, bool> >
  ExtractValues(const node_opaque_t *n);

  void print() {
  }

  /**
   * Not well defined if n is being concurrently modified, just for debugging
   */
  static std::string
  NodeStringify(const node_opaque_t *n);

  static inline size_t
  InternalNodeSize()
  {
    return sizeof(internal_node);
  }

  static inline size_t
  LeafNodeSize()
  {
    return sizeof(leaf_node);
  }

private:

  /**
   * Move the array slice from [p, n) to the right by k position, occupying [p + k, n + k),
   * leaving the values of array[p] to array[p + k -1] undefined. Has no effect if p >= n
   *
   * Note: Assumes that array[n] is valid memory.
   */
  template <typename T>
  static inline ALWAYS_INLINE void
  sift_right(T *array, size_t p, size_t n, size_t k = 1)
  {
    if (k == 0)
      return;
    for (size_t i = n + k - 1; i > p + k - 1; i--)
      array[i] = array[i - k];
  }

  // use swap() to do moves, for efficiency- avoid this
  // variant when using primitive arrays
  template <typename T>
  static inline ALWAYS_INLINE void
  sift_swap_right(T *array, size_t p, size_t n, size_t k = 1)
  {
    if (k == 0)
      return;
    for (size_t i = n + k - 1; i > p + k - 1; i--)
      array[i].swap(array[i - k]);
  }

  /**
   * Move the array slice from [p + k, n) to the left by k positions, occupying [p, n - k),
   * overwriting array[p]..array[p+k-1] Has no effect if p + k >= n
   */
  template <typename T>
  static inline ALWAYS_INLINE void
  sift_left(T *array, size_t p, size_t n, size_t k = 1)
  {
    if (unlikely(p + k >= n))
      return;
    for (size_t i = p; i < n - k; i++)
      array[i] = array[i + k];
  }

  template <typename T>
  static inline ALWAYS_INLINE void
  sift_swap_left(T *array, size_t p, size_t n, size_t k = 1)
  {
    if (unlikely(p + k >= n))
      return;
    for (size_t i = p; i < n - k; i++)
      array[i].swap(array[i + k]);
  }

  /**
   * Copy [p, n) from source into dest. Has no effect if p >= n
   */
  template <typename T>
  static inline ALWAYS_INLINE void
  copy_into(T *dest, T *source, size_t p, size_t n)
  {
    for (size_t i = p; i < n; i++)
      *dest++ = source[i];
  }

  template <typename T>
  static inline ALWAYS_INLINE void
  swap_with(T *dest, T *source, size_t p, size_t n)
  {
    for (size_t i = p; i < n; i++)
      (dest++)->swap(source[i]);
  }

  leaf_node *leftmost_descend_layer(node *n) const;

  /**
   * Assumes RCU region scope is held
   */
  bool search_impl(const key_type &k, value_type &v,
                   typename util::vec<leaf_node *>::type &leaf_nodes,
                   versioned_node_t *search_info = nullptr) const;

  static leaf_node *
  FindRespLeafNode(
      leaf_node *leaf, uint64_t kslice, uint64_t &version);

  /**
   * traverses the lower leaf levels for a leaf node resp for kslice such that
   * version is stable and not deleting. resp info is given via idxmatch +
   * idxlowerbound
   *
   * if idxmatch != -1, then ignore idxlowerbound
   *
   * note to actually use the info, you still need to validate it (the info is
   * tentative as of the version)
   */
  static leaf_node *
  FindRespLeafLowerBound(
      leaf_node *leaf, uint64_t kslice,
      size_t kslicelen, uint64_t &version,
      size_t &n, ssize_t &idxmatch, ssize_t &idxlowerbound);

  static leaf_node *
  FindRespLeafExact(
      leaf_node *leaf, uint64_t kslice,
      size_t kslicelen, uint64_t &version,
      size_t &n, ssize_t &idxmatch);

  typedef std::pair<node *, uint64_t> insert_parent_entry;

  enum insert_status {
    I_NONE_NOMOD, // no nodes split nor modified
    I_NONE_MOD, // no nodes split, but modified
    I_RETRY,
    I_SPLIT, // node(s) split
  };

  /**
   * insert k=>v into node n. if this insert into n causes it to split into two
   * nodes, return the new node (upper half of keys). in this case, min_key is set to the
   * smallest key that the new node is responsible for. otherwise return null, in which
   * case min_key's value is not defined.
   *
   * NOTE: our implementation of insert0() is not as efficient as possible, in favor
   * of code clarity
   */
  insert_status
  insert0(node *n,
          const key_type &k,
          value_type v,
          bool only_if_absent,
          value_type *old_v,
          insert_info_t *insert_info,
          key_slice &min_key,
          node *&new_node,
          typename util::vec<insert_parent_entry>::type &parents,
          typename util::vec<node *>::type &locked_nodes);

  enum remove_status {
    R_NONE_NOMOD,
    R_NONE_MOD,
    R_RETRY,
    R_STOLE_FROM_LEFT,
    R_STOLE_FROM_RIGHT,
    R_MERGE_WITH_LEFT,
    R_MERGE_WITH_RIGHT,
    R_REPLACE_NODE,
  };

  inline ALWAYS_INLINE void
  remove_pos_from_leaf_node(leaf_node *leaf, size_t pos, size_t n)
  {
    INVARIANT(leaf->key_slots_used() == n);
    INVARIANT(pos < n);
    if (leaf->value_is_layer(pos)) {
#ifdef CHECK_INVARIANTS
      leaf->values_[pos].n_->lock();
#endif
      leaf->values_[pos].n_->mark_deleting();
      INVARIANT(leaf->values_[pos].n_->is_leaf_node());
      INVARIANT(leaf->values_[pos].n_->key_slots_used() == 0);
      leaf_node::release((leaf_node *) leaf->values_[pos].n_);
#ifdef CHECK_INVARIANTS
      leaf->values_[pos].n_->unlock();
#endif
    }
    sift_left(leaf->keys_, pos, n);
    sift_left(leaf->values_, pos, n);
    sift_left(leaf->lengths_, pos, n);
    if (leaf->suffixes_)
      sift_swap_left(leaf->suffixes_, pos, n);
    leaf->dec_key_slots_used();
  }

  inline ALWAYS_INLINE void
  remove_pos_from_internal_node(
      internal_node *internal, size_t key_pos, size_t child_pos, size_t n)
  {
    INVARIANT(internal->key_slots_used() == n);
    INVARIANT(key_pos < n);
    INVARIANT(child_pos < n + 1);
    sift_left(internal->keys_, key_pos, n);
    sift_left(internal->children_, child_pos, n + 1);
    internal->dec_key_slots_used();
  }

  struct remove_parent_entry {
    // non-const members for STL
    node *parent_;
    node *parent_left_sibling_;
    node *parent_right_sibling_;
    uint64_t parent_version_;

    // default ctor for STL
    remove_parent_entry()
      : parent_(NULL), parent_left_sibling_(NULL),
        parent_right_sibling_(NULL), parent_version_(0)
    {}

    remove_parent_entry(node *parent,
                        node *parent_left_sibling,
                        node *parent_right_sibling,
                        uint64_t parent_version)
      : parent_(parent), parent_left_sibling_(parent_left_sibling),
        parent_right_sibling_(parent_right_sibling),
        parent_version_(parent_version)
    {}
  };

  remove_status
  remove0(node *np,
          key_slice *min_key,
          key_slice *max_key,
          const key_type &k,
          value_type *old_v,
          node *left_node,
          node *right_node,
          key_slice &new_key,
          node *&replace_node,
          typename util::vec<remove_parent_entry>::type &parents,
          typename util::vec<node *>::type &locked_nodes);
};

template <typename P>
inline void
btree<P>::node::prefetch() const
{
  if (is_leaf_node())
    AsLeaf(this)->prefetch();
  else
    AsInternal(this)->prefetch();
}

extern void TestConcurrentBtreeFast();
extern void TestConcurrentBtreeSlow();

#if !NDB_MASSTREE
typedef btree<concurrent_btree_traits> concurrent_btree;
typedef btree<single_threaded_btree_traits> single_threaded_btree;
#endif
