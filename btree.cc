#include <assert.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <malloc.h>
#include <stdio.h>
#include <pthread.h>

#include <map>
#include <set>
#include <cstddef>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <vector>
#include <utility>

#include "rcu.h"
#include "static_assert.h"
#include "macros.h"
#include "util.h"
#include "thread.h"

/** options */
#define NODE_PREFETCH
#define CHECK_INVARIANTS
//#define LOCK_OWNERSHIP_CHECKING
//#define USE_MEMMOVE
//#define USE_MEMCPY

/** macro helpers */
#define CACHELINE_SIZE 64

#ifdef CHECK_INVARIANTS
  #define INVARIANT(expr) ALWAYS_ASSERT(expr)
#else
  #define INVARIANT(expr) ((void)0)
#endif /* CHECK_INVARIANTS */

// XXX: would be nice if we checked these during single threaded execution
#define SINGLE_THREADED_INVARIANT(expr) ((void)0)

#ifdef NODE_PREFETCH
  #define prefetch_node(n) \
    do { \
      __builtin_prefetch((uint8_t *)n); \
      __builtin_prefetch(((uint8_t *)n) + CACHELINE_SIZE); \
      __builtin_prefetch(((uint8_t *)n) + 2 * CACHELINE_SIZE); \
      __builtin_prefetch(((uint8_t *)n) + 3 * CACHELINE_SIZE); \
    } while (0)
#else
  #define prefetch_node(n) ((void)0)
#endif /* NODE_PREFETCH */

/**
 * This btree maps keys of type key_type -> value_type, where key_type is
 * uint64_t and value_type is a pointer to un-interpreted byte string
 */
class btree : public rcu_enabled {
public:
  typedef uint64_t key_type;
  typedef uint8_t* value_type;

  // public to assist in testing
  static const unsigned int NKeysPerNode = 14;
  static const unsigned int NMinKeysPerNode = NKeysPerNode / 2;

private:

  static const unsigned int NodeSize = 256; /* 4 x86 cache lines */

  static const uint64_t HDR_TYPE_MASK = 0x1;

  // 0xf = (1 << ceil(log2(NKeysPerNode))) - 1
  static const uint64_t HDR_KEY_SLOTS_SHIFT = 1;
  static const uint64_t HDR_KEY_SLOTS_MASK = 0xf << HDR_KEY_SLOTS_SHIFT;

  static const uint64_t HDR_LOCKED_SHIFT = 5;
  static const uint64_t HDR_LOCKED_MASK = 0x1 << HDR_LOCKED_SHIFT;

  static const uint64_t HDR_IS_ROOT_SHIFT = 6;
  static const uint64_t HDR_IS_ROOT_MASK = 0x1 << HDR_IS_ROOT_SHIFT;

  static const uint64_t HDR_MODIFYING_SHIFT = 7;
  static const uint64_t HDR_MODIFYING_MASK = 0x1 << HDR_MODIFYING_SHIFT;

  static const uint64_t HDR_DELETING_SHIFT = 8;
  static const uint64_t HDR_DELETING_MASK = 0x1 << HDR_DELETING_SHIFT;

  static const uint64_t HDR_VERSION_SHIFT = 9;
  static const uint64_t HDR_VERSION_MASK = ((uint64_t)-1) << HDR_VERSION_SHIFT;

  typedef std::pair<ssize_t, size_t> key_search_ret;

  struct node {

    /**
     * hdr bits: layout is:
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
     *
     * XXX: there's some GCC syntax to make these bit fields easier to define-
     * we should use it
     */
    volatile uint64_t hdr;

#ifdef LOCK_OWNERSHIP_CHECKING
    pthread_t lock_owner;
#endif /* LOCK_OWNERSHIP_CHECKING */

    /**
     * Keys are assumed to be stored in contiguous sorted order, so that all
     * the used slots are grouped together. That is, elems in positions
     * [0, key_slots_used) are valid, and elems in positions
     * [key_slots_used, NKeysPerNode) are empty
     */
    key_type keys[NKeysPerNode];

    inline bool
    is_leaf_node() const
    {
      return IsLeafNode(hdr);
    }

    static inline bool
    IsLeafNode(uint64_t v)
    {
      return (v & HDR_TYPE_MASK) == 0;
    }

    inline bool
    is_internal_node() const
    {
      return !is_leaf_node();
    }

    inline size_t
    key_slots_used() const
    {
      return KeySlotsUsed(hdr);
    }

    static inline size_t
    KeySlotsUsed(uint64_t v)
    {
      return (v & HDR_KEY_SLOTS_MASK) >> HDR_KEY_SLOTS_SHIFT;
    }

    inline void
    set_key_slots_used(size_t n)
    {
      INVARIANT(n <= NKeysPerNode);
      INVARIANT(is_modifying());
      hdr &= ~HDR_KEY_SLOTS_MASK;
      hdr |= (n << HDR_KEY_SLOTS_SHIFT);
    }

    inline void
    inc_key_slots_used()
    {
      INVARIANT(is_modifying());
      INVARIANT(key_slots_used() < NKeysPerNode);
      set_key_slots_used(key_slots_used() + 1);
    }

    inline void
    dec_key_slots_used()
    {
      INVARIANT(is_modifying());
      INVARIANT(key_slots_used() > 0);
      set_key_slots_used(key_slots_used() - 1);
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

#ifdef LOCK_OWNERSHIP_CHECKING
    inline bool
    is_lock_owner() const
    {
      return pthread_equal(pthread_self(), lock_owner);
    }
#else
    inline bool
    is_lock_owner() const
    {
      return true;
    }
#endif /* LOCK_OWNERSHIP_CHECKING */

    inline void
    lock()
    {
      uint64_t v = hdr;
      while (IsLocked(v) || !__sync_bool_compare_and_swap(&hdr, v, v | HDR_LOCKED_MASK))
        v = hdr;
#ifdef LOCK_OWNERSHIP_CHECKING
      lock_owner = pthread_self();
#endif
      COMPILER_MEMORY_FENCE;
    }

    inline void
    unlock()
    {
      uint64_t v = hdr;
      bool newv = false;
      INVARIANT(IsLocked(v));
      INVARIANT(is_lock_owner());
      if (IsModifying(v) || IsDeleting(v)) {
        newv = true;
        uint64_t n = Version(v);
        v &= ~HDR_VERSION_MASK;
        v |= (((n + 1) << HDR_VERSION_SHIFT) & HDR_VERSION_MASK);
      }
      // clear locked + modifying bits
      v &= ~(HDR_LOCKED_MASK | HDR_MODIFYING_MASK);
      if (newv) INVARIANT(!check_version(v));
      INVARIANT(!IsLocked(v));
      INVARIANT(!IsModifying(v));
      COMPILER_MEMORY_FENCE;
      hdr = v;
    }

    inline bool
    is_root() const
    {
      return IsRoot(hdr);
    }

    static inline bool
    IsRoot(uint64_t v)
    {
      return v & HDR_IS_ROOT_MASK;
    }

    inline void
    set_root()
    {
      INVARIANT(is_locked());
      INVARIANT(is_lock_owner());
      INVARIANT(!is_root());
      hdr |= HDR_IS_ROOT_MASK;
    }

    inline void
    clear_root()
    {
      INVARIANT(is_locked());
      INVARIANT(is_lock_owner());
      INVARIANT(is_root());
      hdr &= ~HDR_IS_ROOT_MASK;
    }

    inline bool
    is_modifying() const
    {
      return IsModifying(hdr);
    }

    inline void
    mark_modifying()
    {
      uint64_t v = hdr;
      INVARIANT(IsLocked(v));
      INVARIANT(is_lock_owner());
      INVARIANT(!IsModifying(v));
      v |= HDR_MODIFYING_MASK;
      COMPILER_MEMORY_FENCE;
      hdr = v;
      COMPILER_MEMORY_FENCE;
    }

    static inline bool
    IsModifying(uint64_t v)
    {
      return v & HDR_MODIFYING_MASK;
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
      INVARIANT(is_lock_owner());
      INVARIANT(!is_deleting());
      hdr |= HDR_DELETING_MASK;
    }

    inline uint64_t
    unstable_version() const
    {
      return hdr;
    }

    static inline uint64_t
    Version(uint64_t v)
    {
      return (v & HDR_VERSION_MASK) >> HDR_VERSION_SHIFT;
    }

    /**
     * spin until we get a version which is not modifying (but can be locked)
     */
    inline uint64_t
    stable_version() const
    {
      uint64_t v = hdr;
      while (is_modifying())
        v = hdr;
      COMPILER_MEMORY_FENCE;
      return v;
    }

    inline bool
    check_version(uint64_t version) const
    {
      COMPILER_MEMORY_FENCE;
      // are the version the same, modulo the locked bit?
      return (hdr & ~HDR_LOCKED_MASK) == (version & ~HDR_LOCKED_MASK);
    }

    static std::string
    VersionInfoStr(uint64_t v)
    {
      std::stringstream buf;
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

    /**
     * keys[key_search(k).first] == k if key_search(k).first != -1
     * key does not exist otherwise
     */
    inline key_search_ret
    key_search(key_type k) const
    {
      size_t n = key_slots_used();
      ssize_t lower = 0;
      ssize_t upper = n;
      while (lower < upper) {
        ssize_t i = (lower + upper) / 2;
        key_type k0 = keys[i];
        if (k0 == k)
          return key_search_ret(i, n);
        else if (k0 > k)
          upper = i;
        else
          lower = i + 1;
      }
      return key_search_ret(-1, n);
    }

    NEVER_INLINE std::string
    key_list_string() const
    {
      return util::format_list(keys, keys + key_slots_used());
    }

    /**
     * tightest lower bound key, -1 if no such key exists
     */
    inline key_search_ret
    key_lower_bound_search(key_type k) const
    {
      ssize_t ret = -1;
      size_t n = key_slots_used();
      ssize_t lower = 0;
      ssize_t upper = n;
      while (lower < upper) {
        ssize_t i = (lower + upper) / 2;
        key_type k0 = keys[i];
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

    // [min_key, max_key)
    void
    base_invariant_checker(
        const key_type *min_key,
        const key_type *max_key,
        bool is_root) const
    {
      ALWAYS_ASSERT(!is_locked());
      ALWAYS_ASSERT(!is_modifying());
      ALWAYS_ASSERT(this->is_root() == is_root);
      size_t n = key_slots_used();
      ALWAYS_ASSERT(n <= NKeysPerNode);
      if (is_root) {
        if (is_internal_node())
          ALWAYS_ASSERT(n >= 1);
      } else {
        ALWAYS_ASSERT(n >= NMinKeysPerNode);
      }
      if (n == 0)
        return;
      for (size_t i = 1; i < n; i++) {
        ALWAYS_ASSERT(!min_key || keys[i] >= *min_key);
        ALWAYS_ASSERT(!max_key || keys[i] < *max_key);
      }
      key_type prev = keys[0];
      for (size_t i = 1; i < n; i++) {
        ALWAYS_ASSERT(keys[i] > prev);
        prev = keys[i];
      }
    }

    /** manually simulated virtual function */
    void
    invariant_checker(
        const key_type *min_key,
        const key_type *max_key,
        const node *left_sibling,
        const node *right_sibling,
        bool is_root) const;
  };

  struct leaf_node : public node {
    key_type min_key;
    value_type values[NKeysPerNode];
    leaf_node *prev;
    leaf_node *next;
    leaf_node() : min_key(0), prev(NULL), next(NULL)
    {
      hdr = 0;
    }

    void
    invariant_checker_impl(
        const key_type *min_key,
        const key_type *max_key,
        const node *left_sibling,
        const node *right_sibling,
        bool is_root) const
    {
      base_invariant_checker(min_key, max_key, is_root);
      ALWAYS_ASSERT(!min_key || *min_key == this->min_key);
      if (min_key || is_root)
        return;
      ALWAYS_ASSERT(key_slots_used() > 0);
      bool first = true;
      key_type k = keys[0];
      const leaf_node *cur = this;
      while (cur) {
        size_t n = cur->key_slots_used();
        for (size_t i = (first ? 1 : 0); i < n; i++) {
          ALWAYS_ASSERT(cur->keys[i] > k);
          k = cur->keys[i];
        }
        first = false;
        cur = cur->next;
      }
    }

    static inline leaf_node*
    alloc()
    {
      void *p = memalign(CACHELINE_SIZE, sizeof(leaf_node));
      if (!p)
        return NULL;
      return new (p) leaf_node;
    }

    static void
    deleter(void *p)
    {
      leaf_node *n = (leaf_node *) p;
      INVARIANT(n->is_deleting());
      INVARIANT(!n->is_locked());
      n->~leaf_node();
      free(n);
    }

    static inline void
    release(leaf_node *n)
    {
      if (unlikely(!n))
        return;
      n->mark_deleting();
      rcu::free(n, deleter);
    }

  } PACKED_CACHE_ALIGNED;

  struct internal_node : public node {
    /**
     * child at position child_idx is responsible for keys
     * [keys[child_idx - 1], keys[child_idx])
     *
     * in the case where child_idx == 0 or child_idx == key_slots_used(), then
     * the responsiblity value of the min/max key, respectively, is determined
     * by the parent
     */
    node *children[NKeysPerNode + 1];
    internal_node()
    {
      hdr = 1;
    }

    void
    invariant_checker_impl(
        const key_type *min_key,
        const key_type *max_key,
        const node *left_sibling,
        const node *right_sibling,
        bool is_root) const
    {
      base_invariant_checker(min_key, max_key, is_root);
      size_t n = key_slots_used();
      for (size_t i = 0; i <= n; i++) {
        ALWAYS_ASSERT(children[i] != NULL);
        if (i == 0) {
          const node *left_child_sibling = NULL;
          if (left_sibling)
            left_child_sibling = AsInternal(left_sibling)->children[left_sibling->key_slots_used()];
          children[0]->invariant_checker(min_key, &keys[0], left_child_sibling, children[i + 1], false);
        } else if (i == n) {
          const node *right_child_sibling = NULL;
          if (right_sibling)
            right_child_sibling = AsInternal(right_sibling)->children[0];
          children[n]->invariant_checker(&keys[n - 1], max_key, children[i - 1], right_child_sibling, false);
        } else {
          children[i]->invariant_checker(&keys[i - 1], &keys[i], children[i - 1], children[i + 1], false);
        }
      }
      if (!n || children[0]->is_internal_node())
        return;
      for (size_t i = 0; i <= n; i++) {
        const node *left_child_sibling = NULL;
        const node *right_child_sibling = NULL;
        if (left_sibling)
          left_child_sibling = AsInternal(left_sibling)->children[left_sibling->key_slots_used()];
        if (right_sibling)
          right_child_sibling = AsInternal(right_sibling)->children[0];
        const leaf_node *child_prev = (i == 0) ? AsLeaf(left_child_sibling) : AsLeaf(children[i - 1]);
        const leaf_node *child_next = (i == n) ? AsLeaf(right_child_sibling) : AsLeaf(children[i + 1]);
        ALWAYS_ASSERT(AsLeaf(children[i])->prev == child_prev);
        ALWAYS_ASSERT(AsLeaf(children[i])->next == child_next);
      }
    }

    // XXX: alloc(), deleter(), and release() are copied from leaf_node-
    // we should templatize them to avoid code duplication

    static inline internal_node*
    alloc()
    {
      void *p = memalign(CACHELINE_SIZE, sizeof(internal_node));
      if (unlikely(!p))
        return NULL;
      return new (p) internal_node;
    }

    static void
    deleter(void *p)
    {
      internal_node *n = (internal_node *) p;
      INVARIANT(n->is_deleting());
      INVARIANT(!n->is_locked());
      n->~internal_node();
      free(n);
    }

    static inline void
    release(internal_node *n)
    {
      if (unlikely(!n))
        return;
      n->mark_deleting();
      rcu::free(n, deleter);
    }

  } PACKED_CACHE_ALIGNED;

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

  static inline leaf_node*
  AsLeafCheck(node *n)
  {
    return likely(n) && n->is_leaf_node() ? static_cast<leaf_node *>(n) : NULL;
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

  node *volatile root;

  static inline void
  UnlockNodes(const std::vector<node *> &locked_nodes)
  {
    for (std::vector<node *>::const_iterator it = locked_nodes.begin();
         it != locked_nodes.end(); ++it)
      (*it)->unlock();
  }

  template <typename T>
  static inline T
  UnlockAndReturn(const std::vector<node *> &locked_nodes, T t)
  {
    UnlockNodes(locked_nodes);
    return t;
  }

  static void recursive_delete(node *n)
  {
    if (leaf_node *leaf = AsLeafCheck(n)) {
#ifdef CHECK_INVARIANTS
      leaf->lock();
      leaf->mark_deleting();
      leaf->unlock();
#endif
      leaf_node::deleter(leaf);
    } else {
      internal_node *internal = AsInternal(n);
      size_t n = internal->key_slots_used();
      for (size_t i = 0; i < n + 1; i++)
        recursive_delete(internal->children[i]);
#ifdef CHECK_INVARIANTS
      internal->lock();
      internal->mark_deleting();
      internal->unlock();
#endif
      internal_node::deleter(internal);
    }
  }

public:

  btree() : root(leaf_node::alloc())
  {

#ifndef LOCK_OWNERSHIP_CHECKING
    _static_assert(sizeof(leaf_node) <= NodeSize);
    _static_assert(sizeof(internal_node) <= NodeSize);
#endif /* LOCK_OWNERSHIP_CHECKING */

    _static_assert(sizeof(leaf_node) % 64 == 0);
    _static_assert(sizeof(internal_node) % 64 == 0);

#ifdef CHECK_INVARIANTS
    root->lock();
    root->set_root();
    root->unlock();
#else
    root->set_root();
#endif /* CHECK_INVARIANTS */
  }

  ~btree()
  {
    // NOTE: it is assumed on deletion time there are no
    // outstanding requests to the btree, so deletion proceeds
    // in a non-threadsafe manner
    recursive_delete(root);
    root = NULL;
  }

  /** Note: invariant checking is not thread safe */
  void
  invariant_checker() const
  {
    root->invariant_checker(NULL, NULL, NULL, NULL, true);
  }

private:

  bool
  search_impl(key_type k, value_type &v, leaf_node *&n) const
  {
  retry:
    node *cur = root;
    while (true) {
      prefetch_node(cur);
    process:
      uint64_t version = cur->stable_version();
      if (node::IsDeleting(version))
        goto retry;
      if (leaf_node *leaf = AsLeafCheck(cur)) {
        key_search_ret kret = leaf->key_search(k);
        ssize_t ret = kret.first;
        if (ret != -1) {
          // found
          v = leaf->values[ret];
          if (unlikely(!leaf->check_version(version)))
            goto process;
          n = leaf;
          return true;
        }

        // leaf might have lost responsibility for key k during the descend. we
        // need to check this and adjust accordingly
        if (unlikely(k < leaf->min_key)) {
          // try to go left
          leaf_node *left_sibling = leaf->prev;
          if (unlikely(!leaf->check_version(version)))
            goto process;
          if (likely(left_sibling)) {
            cur = left_sibling;
            continue;
          } else {
            // XXX: this case shouldn't be possible...
            goto retry;
          }
        } else {
          // try to go right
          leaf_node *right_sibling = leaf->next;
          if (unlikely(!leaf->check_version(version)))
            goto process;
          prefetch_node(right_sibling);
          if (unlikely(!right_sibling)) {
            n = leaf;
            return false;
          }
          uint64_t right_version = right_sibling->stable_version();
          key_type right_min_key = right_sibling->min_key;
          if (unlikely(!right_sibling->check_version(right_version)))
            goto process;
          if (unlikely(k >= right_min_key)) {
            cur = right_sibling;
            continue;
          }
        }

        n = leaf;
        return false;
      } else {
        internal_node *internal = AsInternal(cur);
        key_search_ret kret = internal->key_lower_bound_search(k);
        ssize_t ret = kret.first;
        size_t n = kret.second;
        if (ret != -1)
          cur = internal->children[ret + 1];
        else
          cur = internal->children[0];
        if (unlikely(!internal->check_version(version)))
          goto process;
        INVARIANT(n);
      }
    }
  }

public:

  inline bool
  search(key_type k, value_type &v) const
  {
    leaf_node *n;
    scoped_rcu_region rcu_region;
    return search_impl(k, v, n);
  }

  /**
   * For all keys in [lower, *upper), invoke callback in ascending order.
   * If upper is NULL, then there is no upper bound
   *
   * Callback is expected to implement bool operator()(key_type k, value_type v),
   * where the callback returns true if it wants to keep going, false otherwise
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
   */
  template <typename T>
  void
  search_range(key_type lower, key_type *upper, T callback)
  {
    if (unlikely(upper && *upper <= lower))
      return;
    leaf_node *n = NULL;
    value_type v = 0;
    scoped_rcu_region rcu_region;
    search_impl(lower, v, n);
    INVARIANT(n != NULL);
    key_type next_key = lower;
    key_type last_key = 0;
    bool emitted_last_key = false;
    while (!upper || next_key < *upper) {
      prefetch_node(n);
      std::vector< std::pair<key_type, value_type> > buf;

      uint64_t version = n->stable_version();
      key_type leaf_min_key = n->min_key;
      if (leaf_min_key > next_key) {
        // go left
        leaf_node *left_sibling = n->prev;
        if (unlikely(!n->check_version(version)))
          // try this node again
          continue;
        // try from left_sibling
        n = left_sibling;
        INVARIANT(n);
        continue;
      }

      for (size_t i = 0; i < n->key_slots_used(); i++)
        if (n->keys[i] >= lower && (!upper || n->keys[i] < *upper))
          buf.push_back(std::make_pair(n->keys[i], n->values[i]));

      leaf_node *right_sibling = n->next;
      key_type leaf_max_key = right_sibling ? right_sibling->min_key : 0;
      if (unlikely(!n->check_version(version)))
        continue;

      for (size_t i = 0; i < buf.size(); i++) {
        if (emitted_last_key && buf[i].first <= last_key)
          continue;
        if (!callback(buf[i].first, buf[i].second))
          return;
        last_key = buf[i].first;
        emitted_last_key = true;
      }

      if (!right_sibling)
        // we're done
        return;

      next_key = leaf_max_key;
      n = right_sibling;
    }
  }

  void
  insert(key_type k, value_type v)
  {
  retry:
    key_type mk;
    node *ret;
    std::vector<insert_parent_entry> parents;
    std::vector<node *> locked_nodes;
    scoped_rcu_region rcu_region;
    node *local_root = root;
    insert_status status = insert0(local_root, k, v, mk, ret, parents, locked_nodes);
    switch (status) {
    case I_NONE:
      break;
    case I_RETRY:
      goto retry;
    case I_SPLIT:
      INVARIANT(ret);
      INVARIANT(ret->key_slots_used() > 0);
      INVARIANT(local_root->is_modifying());
      INVARIANT(local_root->is_lock_owner());
      INVARIANT(local_root->is_root());
      INVARIANT(local_root == root);
      internal_node *new_root = internal_node::alloc();
#ifdef CHECK_INVARIANTS
      new_root->lock();
      new_root->mark_modifying();
      locked_nodes.push_back(new_root);
#endif /* CHECK_INVARIANTS */
      new_root->children[0] = local_root;
      new_root->children[1] = ret;
      new_root->keys[0] = mk;
      new_root->set_key_slots_used(1);
      new_root->set_root();
      local_root->clear_root();
      COMPILER_MEMORY_FENCE;
      root = new_root;
      // locks are still held here
      UnlockNodes(locked_nodes);
      break;
    }
  }

  void
  remove(key_type k)
  {
  retry:
    key_type new_key;
    node *replace_node;
    std::vector<remove_parent_entry> parents;
    std::vector<node *> locked_nodes;
    scoped_rcu_region rcu_region;
    node *local_root = root;
    remove_status status = remove0(local_root,
                                   NULL, /* min_key */
                                   NULL, /* max_key */
                                   k,
                                   NULL, /* left_node */
                                   NULL, /* right_node */
                                   new_key,
                                   replace_node,
                                   parents,
                                   locked_nodes);
    switch (status) {
    case R_NONE:
      break;
    case R_RETRY:
      goto retry;
    case R_REPLACE_NODE:
      INVARIANT(local_root->is_deleting());
      INVARIANT(local_root->is_lock_owner());
      INVARIANT(local_root->is_root());
      INVARIANT(local_root == root);
      replace_node->set_root();
      local_root->clear_root();
      COMPILER_MEMORY_FENCE;
      root = replace_node;
      // locks are still held here
      UnlockNodes(locked_nodes);
      break;
    default:
      ALWAYS_ASSERT(false);
      break;
    }
  }

  size_t
  size() const
  {
  retry:
    node *cur = root;
    size_t count = 0;
    while (cur) {
      prefetch_node(cur);
    process:
      uint64_t version = cur->stable_version();
      if (node::IsDeleting(version))
        goto retry;
      if (leaf_node *leaf = AsLeafCheck(cur)) {
        size_t n = leaf->key_slots_used();
        leaf_node *next = leaf->next;
        if (unlikely(!leaf->check_version(version)))
          goto process;
        count += n;
        cur = next;
      } else {
        internal_node *internal = AsInternal(cur);
        cur = internal->children[0];
        if (unlikely(!internal->check_version(version)))
          goto retry;
      }
    }
    return count;
  }

private:

  /**
   * Move the array slice from [p, n) to the right by 1 position, occupying [p + 1, n + 1),
   * leaving the value of array[p] undefined. Has no effect if p >= n
   *
   * Note: Assumes that array[n] is valid memory.
   */
  template <typename T>
  static inline ALWAYS_INLINE void
  sift_right(T *array, size_t p, size_t n)
  {
#ifdef USE_MEMMOVE
    if (unlikely(p >= n))
      return;
    memmove(&array[p + 1], &array[p], (n - p) * sizeof(T));
#else
    for (size_t i = n; i > p; i--)
      array[i] = array[i - 1];
#endif /* USE_MEMMOVE */
  }

  /**
   * Move the array slice from [p + 1, n) to the left by 1 position, occupying [p, n - 1),
   * overwriting array[p] and leaving the value of array[n - 1] undefined. Has no effect if p + 1 >= n
   */
  template <typename T>
  static inline ALWAYS_INLINE void
  sift_left(T *array, size_t p, size_t n)
  {
    if (unlikely(p + 1 >= n))
      return;
#ifdef USE_MEMMOVE
    memmove(&array[p], &array[p + 1], (n - 1 - p) * sizeof(T));
#else
    for (size_t i = p; i < n - 1; i++)
      array[i] = array[i + 1];
#endif /* USE_MEMMOVE */
  }

  /**
   * Copy [p, n) from source into dest. Has no effect if p >= n
   */
  template <typename T>
  static inline ALWAYS_INLINE void
  copy_into(T *dest, T *source, size_t p, size_t n)
  {
#ifdef USE_MEMCPY
    if (unlikely(p >= n))
      return;
    memcpy(dest, &source[p], (n - p) * sizeof(T));
#else
    for (size_t i = p; i < n; i++)
      *dest++ = source[i];
#endif /* USE_MEMCPY */
  }

  typedef std::pair<node *, uint64_t> insert_parent_entry;

  enum insert_status {
    I_NONE,
    I_RETRY,
    I_SPLIT,
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
          key_type k,
          value_type v,
          key_type &min_key,
          node *&new_node,
          std::vector<insert_parent_entry> &parents,
          std::vector<node *> &locked_nodes)
  {
    prefetch_node(n);
    if (leaf_node *leaf = AsLeafCheck(n)) {
      // locked nodes are acquired bottom to top
      INVARIANT(locked_nodes.empty());

      leaf->lock();
      locked_nodes.push_back(leaf);

      // now we need to ensure that this leaf node still has
      // responsibility for k, before we proceed
      if (unlikely(leaf->is_deleting()))
        return UnlockAndReturn(locked_nodes, I_RETRY);
      if (unlikely(k < leaf->min_key))
        return UnlockAndReturn(locked_nodes, I_RETRY);
      if (likely(leaf->next)) {
        uint64_t right_version = leaf->next->stable_version();
        key_type right_min_key = leaf->next->min_key;
        if (unlikely(!leaf->next->check_version(right_version)))
          return UnlockAndReturn(locked_nodes, I_RETRY);
        if (unlikely(k >= right_min_key))
          return UnlockAndReturn(locked_nodes, I_RETRY);
      }

      // we know now that leaf is responsible for k, so we can proceed

      key_search_ret kret = leaf->key_lower_bound_search(k);
      ssize_t ret = kret.first;
      if (ret != -1 && leaf->keys[ret] == k) {
        // easy case- we don't modify the node itself
        leaf->values[ret] = v;
        return UnlockAndReturn(locked_nodes, I_NONE);
      }

      size_t n = kret.second;
      // ret + 1 is the slot we want the new key to go into, in the leaf node
      if (n < NKeysPerNode) {
        // also easy case- we only need to make local modifications
        leaf->mark_modifying();

        sift_right(leaf->keys, ret + 1, n);
        leaf->keys[ret + 1] = k;
        sift_right(leaf->values, ret + 1, n);
        leaf->values[ret + 1] = v;
        leaf->inc_key_slots_used();

        return UnlockAndReturn(locked_nodes, I_NONE);
      } else {
        INVARIANT(n == NKeysPerNode);

        // we need to split the current node, potentially causing a bunch of
        // splits to happen in ancestors. to make this safe w/o
        // using a very complicated locking protocol, we will first acquire all
        // locks on nodes which will be modified, in left-to-right,
        // bottom-to-top order

        if (parents.empty()) {
          if (unlikely(!leaf->is_root()))
            return UnlockAndReturn(locked_nodes, I_RETRY);
          INVARIANT(leaf == root);
        } else {
          for (std::vector<insert_parent_entry>::reverse_iterator rit = parents.rbegin();
               rit != parents.rend(); ++rit) {
            // lock the parent
            node *p = rit->first;
            p->lock();
            locked_nodes.push_back(p);
            if (unlikely(!p->check_version(rit->second)))
              // in traversing down the tree, an ancestor of this node was
              // modified- to be safe, we start over
              return UnlockAndReturn(locked_nodes, I_RETRY);
            if ((rit + 1) == parents.rend()) {
              // did the root change?
              if (unlikely(!p->is_root()))
                return UnlockAndReturn(locked_nodes, I_RETRY);
              INVARIANT(p == root);
            }

            // since the child needs a split, see if we have room in the parent-
            // if we don't have room, we'll also need to split the parent, in which
            // case we must grab its parent's lock
            INVARIANT(p->is_internal_node());
            size_t parent_n = p->key_slots_used();
            INVARIANT(parent_n > 0 && parent_n <= NKeysPerNode);
            if (parent_n < NKeysPerNode)
              // can stop locking up now, since this node won't split
              break;
          }
        }

        // at this point, we have locked all nodes which will be split/modified
        // modulo the new nodes to be created
        leaf->mark_modifying();

        leaf_node *new_leaf = leaf_node::alloc();
        prefetch_node(new_leaf);

#ifdef CHECK_INVARIANTS
        new_leaf->lock();
        new_leaf->mark_modifying();
        locked_nodes.push_back(new_leaf);
#endif /* CHECK_INVARIANTS */

        if (ret + 1 >= NMinKeysPerNode) {
          // put new key in new leaf
          size_t pos = ret + 1 - NMinKeysPerNode;

          copy_into(&new_leaf->keys[0], leaf->keys, NMinKeysPerNode, ret + 1);
          new_leaf->keys[pos] = k;
          copy_into(&new_leaf->keys[pos + 1], leaf->keys, ret + 1, NKeysPerNode);

          copy_into(&new_leaf->values[0], leaf->values, NMinKeysPerNode, ret + 1);
          new_leaf->values[pos] = v;
          copy_into(&new_leaf->values[pos + 1], leaf->values, ret + 1, NKeysPerNode);

          leaf->set_key_slots_used(NMinKeysPerNode);
          new_leaf->set_key_slots_used(NKeysPerNode - NMinKeysPerNode + 1);
        } else {
          // put new key in original leaf
          copy_into(&new_leaf->keys[0], leaf->keys, NMinKeysPerNode, NKeysPerNode);
          copy_into(&new_leaf->values[0], leaf->values, NMinKeysPerNode, NKeysPerNode);

          sift_right(leaf->keys, ret + 1, NMinKeysPerNode);
          leaf->keys[ret + 1] = k;
          sift_right(leaf->values, ret + 1, NMinKeysPerNode);
          leaf->values[ret + 1] = v;

          leaf->set_key_slots_used(NMinKeysPerNode + 1);
          new_leaf->set_key_slots_used(NKeysPerNode - NMinKeysPerNode);
        }

        // pointer adjustment
        new_leaf->prev = leaf;
        new_leaf->next = leaf->next;
        if (leaf->next)
          leaf->next->prev = new_leaf;
        leaf->next = new_leaf;

        min_key = new_leaf->keys[0];
        new_leaf->min_key = min_key;
        new_node = new_leaf;
        return I_SPLIT;
      }
    } else {
      internal_node *internal = AsInternal(n);
      uint64_t version = internal->stable_version();
      if (unlikely(node::IsDeleting(version)))
        return UnlockAndReturn(locked_nodes, I_RETRY);
      key_search_ret kret = internal->key_lower_bound_search(k);
      ssize_t ret = kret.first;
      size_t n = kret.second;
      size_t child_idx = (ret == -1) ? 0 : ret + 1;
      node *child_ptr = internal->children[child_idx];
      if (unlikely(!internal->check_version(version)))
        return UnlockAndReturn(locked_nodes, I_RETRY);
      parents.push_back(insert_parent_entry(internal, version));
      key_type mk = 0;
      node *new_child = NULL;
      insert_status status = insert0(child_ptr, k, v, mk, new_child, parents, locked_nodes);
      if (status != I_SPLIT)
        return status;
      INVARIANT(new_child);
      INVARIANT(internal->is_locked()); // previous call to insert0() must lock internal node for insertion
      INVARIANT(internal->is_lock_owner());
      INVARIANT(internal->check_version(version));
      INVARIANT(new_child->key_slots_used() > 0);
      INVARIANT(n > 0);
      internal->mark_modifying();
      if (n < NKeysPerNode) {
        sift_right(internal->keys, child_idx, n);
        internal->keys[child_idx] = mk;
        sift_right(internal->children, child_idx + 1, n + 1);
        internal->children[child_idx + 1] = new_child;
        internal->inc_key_slots_used();
        return UnlockAndReturn(locked_nodes, I_NONE);
      } else {
        INVARIANT(n == NKeysPerNode);
        INVARIANT(ret == internal->key_lower_bound_search(mk).first);

        internal_node *new_internal = internal_node::alloc();
        prefetch_node(new_internal);
#ifdef CHECK_INVARIANTS
        new_internal->lock();
        new_internal->mark_modifying();
        locked_nodes.push_back(new_internal);
#endif /* CHECK_INVARIANTS */

        // there are three cases post-split:
        // (1) mk goes in the original node
        // (2) mk is the key we push up
        // (3) mk goes in the new node

        const ssize_t split_point = NMinKeysPerNode - 1;
        if (ret < split_point) {
          // case (1)
          min_key = internal->keys[split_point];

          copy_into(&new_internal->keys[0], internal->keys, NMinKeysPerNode, NKeysPerNode);
          copy_into(&new_internal->children[0], internal->children, NMinKeysPerNode, NKeysPerNode + 1);
          new_internal->set_key_slots_used(NKeysPerNode - NMinKeysPerNode);

          sift_right(internal->keys, child_idx, NMinKeysPerNode - 1);
          internal->keys[child_idx] = mk;
          sift_right(internal->children, child_idx + 1, NMinKeysPerNode);
          internal->children[child_idx + 1] = new_child;
          internal->set_key_slots_used(NMinKeysPerNode);

        } else if (ret == split_point) {
          // case (2)
          min_key = mk;

          copy_into(&new_internal->keys[0], internal->keys, NMinKeysPerNode, NKeysPerNode);
          copy_into(&new_internal->children[1], internal->children, NMinKeysPerNode + 1, NKeysPerNode + 1);
          new_internal->children[0] = new_child;
          new_internal->set_key_slots_used(NKeysPerNode - NMinKeysPerNode);
          internal->set_key_slots_used(NMinKeysPerNode);

        } else {
          // case (3)
          min_key = internal->keys[NMinKeysPerNode];

          size_t pos = child_idx - NMinKeysPerNode - 1;

          copy_into(&new_internal->keys[0], internal->keys, NMinKeysPerNode + 1, child_idx);
          new_internal->keys[pos] = mk;
          copy_into(&new_internal->keys[pos + 1], internal->keys, child_idx, NKeysPerNode);

          copy_into(&new_internal->children[0], internal->children, NMinKeysPerNode + 1, child_idx + 1);
          new_internal->children[pos + 1] = new_child;
          copy_into(&new_internal->children[pos + 2], internal->children, child_idx + 1, NKeysPerNode + 1);

          new_internal->set_key_slots_used(NKeysPerNode - NMinKeysPerNode);
          internal->set_key_slots_used(NMinKeysPerNode);
        }

        new_node = new_internal;
        return I_SPLIT;
      }
    }
  }

  enum remove_status {
    R_NONE,
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
    sift_left(leaf->keys, pos, n);
    sift_left(leaf->values, pos, n);
    leaf->dec_key_slots_used();
  }

  inline ALWAYS_INLINE void
  remove_pos_from_internal_node(
      internal_node *internal, size_t key_pos, size_t child_pos, size_t n)
  {
    INVARIANT(internal->key_slots_used() == n);
    INVARIANT(key_pos < n);
    INVARIANT(child_pos < n + 1);
    sift_left(internal->keys, key_pos, n);
    sift_left(internal->children, child_pos, n + 1);
    internal->dec_key_slots_used();
  }

  struct remove_parent_entry {
    // non-const members for STL
    node *parent;
    node *parent_left_sibling;
    node *parent_right_sibling;
    uint64_t parent_version;

    // default ctor for STL
    remove_parent_entry()
      : parent(NULL), parent_left_sibling(NULL),
        parent_right_sibling(NULL), parent_version(0)
    {}

    remove_parent_entry(node *parent,
                        node *parent_left_sibling,
                        node *parent_right_sibling,
                        uint64_t parent_version)
      : parent(parent), parent_left_sibling(parent_left_sibling),
        parent_right_sibling(parent_right_sibling), parent_version(parent_version)
    {}
  };

  /**
   * remove is very tricky to get right!
   */
  remove_status
  remove0(node *np,
          key_type *min_key,
          key_type *max_key,
          key_type k,
          node *left_node,
          node *right_node,
          key_type &new_key,
          btree::node *&replace_node,
          std::vector<remove_parent_entry> &parents,
          std::vector<node *> &locked_nodes)
  {
    prefetch_node(np);
    if (leaf_node *leaf = AsLeafCheck(np)) {

      INVARIANT(locked_nodes.empty());
      leaf->lock();
      locked_nodes.push_back(leaf);

      SINGLE_THREADED_INVARIANT(!left_node || (leaf->prev == left_node && AsLeaf(left_node)->next == leaf));
      SINGLE_THREADED_INVARIANT(!right_node || (leaf->next == right_node && AsLeaf(right_node)->prev == leaf));

      // now we need to ensure that this leaf node still has
      // responsibility for k, before we proceed - note this check
      // is duplicated from insert0()
      if (unlikely(leaf->is_deleting()))
        return UnlockAndReturn(locked_nodes, R_RETRY);
      if (unlikely(k < leaf->min_key))
        return UnlockAndReturn(locked_nodes, R_RETRY);
      if (likely(leaf->next)) {
        uint64_t right_version = leaf->next->stable_version();
        key_type right_min_key = leaf->next->min_key;
        if (unlikely(!leaf->next->check_version(right_version)))
          return UnlockAndReturn(locked_nodes, R_RETRY);
        if (unlikely(k >= right_min_key))
          return UnlockAndReturn(locked_nodes, R_RETRY);
      }

      // we know now that leaf is responsible for k, so we can proceed

      key_search_ret kret = leaf->key_search(k);
      ssize_t ret = kret.first;
      if (ret == -1)
        return UnlockAndReturn(locked_nodes, R_NONE);
      size_t n = kret.second;
      if (n > NMinKeysPerNode) {
        leaf->mark_modifying();
        remove_pos_from_leaf_node(leaf, ret, n);
        return UnlockAndReturn(locked_nodes, R_NONE);
      } else {

        uint64_t leaf_version = leaf->unstable_version();

        leaf_node *left_sibling = AsLeaf(left_node);
        leaf_node *right_sibling = AsLeaf(right_node);

        // NOTE: remember that our locking discipline is left-to-right,
        // bottom-to-top. Here, we must acquire all locks on nodes being
        // modified in the tree. How we choose to handle removes is in the
        // following preference:
        //   1) steal from right node
        //   2) merge with right node
        //   3) steal from left node
        //   4) merge with left node
        //
        // Btree invariants guarantee that at least one of the options above is
        // available (except for the root node). We pick the right node first,
        // because our locking discipline allows us to directly lock the right
        // node.  If (1) and (2) cannot be satisfied, then we must first
        // *unlock* the current node, lock the left node, relock the current
        // node, and check nothing changed in between.

        if (right_sibling) {
          right_sibling->lock();
          locked_nodes.push_back(right_sibling);
        } else if (left_sibling) {
          leaf->unlock();
          left_sibling->lock();
          locked_nodes.push_back(left_sibling);
          leaf->lock();
          if (unlikely(!leaf->check_version(leaf_version)))
            return UnlockAndReturn(locked_nodes, R_RETRY);
        } else {
          INVARIANT(parents.empty());
          if (unlikely(!leaf->is_root()))
            return UnlockAndReturn(locked_nodes, R_RETRY);
          INVARIANT(leaf == root);
        }

        for (std::vector<remove_parent_entry>::reverse_iterator rit = parents.rbegin();
             rit != parents.rend(); ++rit) {
          node *p = rit->parent;
          node *l = rit->parent_left_sibling;
          node *r = rit->parent_right_sibling;
          uint64_t p_version = rit->parent_version;
          p->lock();
          locked_nodes.push_back(p);
          if (unlikely(!p->check_version(p_version)))
            return UnlockAndReturn(locked_nodes, R_RETRY);
          size_t p_n = p->key_slots_used();
          if (p_n > NMinKeysPerNode)
            break;
          if (r) {
            r->lock();
            locked_nodes.push_back(r);
          } else if (l) {
            p->unlock();
            l->lock();
            locked_nodes.push_back(l);
            p->lock();
            if (unlikely(!p->check_version(p_version)))
              return UnlockAndReturn(locked_nodes, R_RETRY);
          } else {
            if (unlikely(!p->is_root()))
              return UnlockAndReturn(locked_nodes, R_RETRY);
            INVARIANT(p == root);
          }
        }

        leaf->mark_modifying();

        if (right_sibling) {
          right_sibling->mark_modifying();
          size_t right_n = right_sibling->key_slots_used();
          if (right_n > NMinKeysPerNode) {
            // steal first from right
            INVARIANT(right_sibling->keys[0] > leaf->keys[n - 1]);
            sift_left(leaf->keys, ret, n);
            leaf->keys[n - 1] = right_sibling->keys[0];
            sift_left(leaf->values, ret, n);
            leaf->values[n - 1] = right_sibling->values[0];
            sift_left(right_sibling->keys, 0, right_n);
            sift_left(right_sibling->values, 0, right_n);
            right_sibling->dec_key_slots_used();
            new_key = right_sibling->keys[0];
            right_sibling->min_key = new_key;
            return R_STOLE_FROM_RIGHT;
          } else {
            // merge right sibling into this node
            INVARIANT(right_sibling->keys[0] > leaf->keys[n - 1]);
            INVARIANT((right_n + (n - 1)) <= NKeysPerNode);

            sift_left(leaf->keys, ret, n);
            copy_into(&leaf->keys[n - 1], right_sibling->keys, 0, right_n);

            sift_left(leaf->values, ret, n);
            copy_into(&leaf->values[n - 1], right_sibling->values, 0, right_n);

            leaf->set_key_slots_used(right_n + (n - 1));
            leaf->next = right_sibling->next;
            if (right_sibling->next)
              right_sibling->next->prev = leaf;

            // leaf->next->prev won't change because we hold lock for both leaf
            // and right_sibling
            INVARIANT(!leaf->next || leaf->next->prev == leaf);

            // leaf->prev->next might change, however, since the left node could be
            // splitting (and we might hold a pointer to the left-split of the left node,
            // before it gets updated)
            SINGLE_THREADED_INVARIANT(!leaf->prev || leaf->prev->next == leaf);

            leaf_node::release(right_sibling);
            return R_MERGE_WITH_RIGHT;
          }
        }

        if (left_sibling) {
          left_sibling->mark_modifying();
          size_t left_n = left_sibling->key_slots_used();
          if (left_n > NMinKeysPerNode) {
            // steal last from left
            INVARIANT(left_sibling->keys[left_n - 1] < leaf->keys[0]);
            sift_right(leaf->keys, 0, ret);
            leaf->keys[0] = left_sibling->keys[left_n - 1];
            sift_right(leaf->values, 0, ret);
            leaf->values[0] = left_sibling->values[left_n - 1];
            left_sibling->dec_key_slots_used();
            new_key = leaf->keys[0];
            leaf->min_key = new_key;
            return R_STOLE_FROM_LEFT;
          } else {
            // merge this node into left sibling
            INVARIANT(left_sibling->keys[left_n - 1] < leaf->keys[0]);
            INVARIANT((left_n + (n - 1)) <= NKeysPerNode);

            copy_into(&left_sibling->keys[left_n], leaf->keys, 0, ret);
            copy_into(&left_sibling->keys[left_n + ret], leaf->keys, ret + 1, n);

            copy_into(&left_sibling->values[left_n], leaf->values, 0, ret);
            copy_into(&left_sibling->values[left_n + ret], leaf->values, ret + 1, n);

            left_sibling->set_key_slots_used(left_n + (n - 1));
            left_sibling->next = leaf->next;
            if (leaf->next)
              leaf->next->prev = left_sibling;

            // see comments in right_sibling case above, for why one of them is INVARIANT and
            // the other is SINGLE_THREADED_INVARIANT
            INVARIANT(!left_sibling->next || left_sibling->next->prev == left_sibling);
            SINGLE_THREADED_INVARIANT(!left_sibling->prev || left_sibling->prev->next == left_sibling);

            leaf_node::release(leaf);
            return R_MERGE_WITH_LEFT;
          }
        }

        // root node, so we are ok
        INVARIANT(leaf == root);
        INVARIANT(leaf->is_root());
        remove_pos_from_leaf_node(leaf, ret, n);
        return UnlockAndReturn(locked_nodes, R_NONE);
      }
    } else {
      internal_node *internal = AsInternal(np);
      uint64_t version = internal->stable_version();
      if (unlikely(node::IsDeleting(version)))
        return UnlockAndReturn(locked_nodes, R_RETRY);
      key_search_ret kret = internal->key_lower_bound_search(k);
      ssize_t ret = kret.first;
      size_t n = kret.second;
      size_t child_idx = (ret == -1) ? 0 : ret + 1;
      node *child_ptr = internal->children[child_idx];
      key_type *child_min_key = child_idx == 0 ? NULL : &internal->keys[child_idx - 1];
      key_type *child_max_key = child_idx == n ? NULL : &internal->keys[child_idx];
      node *child_left_sibling = child_idx == 0 ? NULL : internal->children[child_idx - 1];
      node *child_right_sibling = child_idx == n ? NULL : internal->children[child_idx + 1];
      if (unlikely(!internal->check_version(version)))
        return UnlockAndReturn(locked_nodes, R_RETRY);
      parents.push_back(remove_parent_entry(internal, left_node, right_node, version));
      INVARIANT(n > 0);
      key_type nk;
      node *rn;
      remove_status status = remove0(child_ptr,
                                     child_min_key,
                                     child_max_key,
                                     k,
                                     child_left_sibling,
                                     child_right_sibling,
                                     nk,
                                     rn,
                                     parents,
                                     locked_nodes);
      switch (status) {
      case R_NONE:
      case R_RETRY:
        return status;

      case R_STOLE_FROM_LEFT:
        INVARIANT(internal->is_locked());
        INVARIANT(internal->is_lock_owner());
        internal->keys[child_idx - 1] = nk;
        return UnlockAndReturn(locked_nodes, R_NONE);

      case R_STOLE_FROM_RIGHT:
        INVARIANT(internal->is_locked());
        INVARIANT(internal->is_lock_owner());
        internal->keys[child_idx] = nk;
        return UnlockAndReturn(locked_nodes, R_NONE);

      case R_MERGE_WITH_LEFT:
      case R_MERGE_WITH_RIGHT:
      {
        internal->mark_modifying();

        size_t del_key_idx, del_child_idx;
        if (status == R_MERGE_WITH_LEFT) {
          // need to delete key at position (child_idx - 1), and child at
          // position (child_idx)
          del_key_idx = child_idx - 1;
          del_child_idx = child_idx;
        } else {
          // need to delete key at position (child_idx), and child at
          // posiiton (child_idx + 1)
          del_key_idx = child_idx;
          del_child_idx = child_idx + 1;
        }

        if (n > NMinKeysPerNode) {
          remove_pos_from_internal_node(internal, del_key_idx, del_child_idx, n);
          return UnlockAndReturn(locked_nodes, R_NONE);
        }

        internal_node *left_sibling = AsInternal(left_node);
        internal_node *right_sibling = AsInternal(right_node);

        // WARNING: if you change the order of events here, then you must also
        // change the locking protocol (see the comment in the leaf node case)

        if (right_sibling) {
          right_sibling->mark_modifying();
          size_t right_n = right_sibling->key_slots_used();
          INVARIANT(max_key);
          INVARIANT(right_sibling->keys[0] > internal->keys[n - 1]);
          INVARIANT(*max_key > internal->keys[n - 1]);
          if (right_n > NMinKeysPerNode) {
            // steal from right
            sift_left(internal->keys, del_key_idx, n);
            internal->keys[n - 1] = *max_key;

            sift_left(internal->children, del_child_idx, n + 1);
            internal->children[n] = right_sibling->children[0];

            new_key = right_sibling->keys[0];

            sift_left(right_sibling->keys, 0, right_n);
            sift_left(right_sibling->children, 0, right_n + 1);
            right_sibling->dec_key_slots_used();

            return R_STOLE_FROM_RIGHT;
          } else {
            // merge with right
            INVARIANT(max_key);

            sift_left(internal->keys, del_key_idx, n);
            internal->keys[n - 1] = *max_key;
            copy_into(&internal->keys[n], right_sibling->keys, 0, right_n);

            sift_left(internal->children, del_child_idx, n + 1);
            copy_into(&internal->children[n], right_sibling->children, 0, right_n + 1);

            internal->set_key_slots_used(n + right_n);
            internal_node::release(right_sibling);
            return R_MERGE_WITH_RIGHT;
          }
        }

        if (left_sibling) {
          left_sibling->mark_modifying();
          size_t left_n = left_sibling->key_slots_used();
          INVARIANT(min_key);
          INVARIANT(left_sibling->keys[left_n - 1] < internal->keys[0]);
          INVARIANT(left_sibling->keys[left_n - 1] < *min_key);
          INVARIANT(*min_key < internal->keys[0]);
          if (left_n > NMinKeysPerNode) {
            // steal from left
            sift_right(internal->keys, 0, del_key_idx);
            internal->keys[0] = *min_key;

            sift_right(internal->children, 0, del_child_idx);
            internal->children[0] = left_sibling->children[left_n];

            new_key = left_sibling->keys[left_n - 1];
            left_sibling->dec_key_slots_used();

            return R_STOLE_FROM_LEFT;
          } else {
            // merge into left sibling
            INVARIANT(min_key);

            size_t left_key_j = left_n;
            size_t left_child_j = left_n + 1;

            left_sibling->keys[left_key_j++] = *min_key;

            copy_into(&left_sibling->keys[left_key_j], internal->keys, 0, del_key_idx);
            left_key_j += del_key_idx;
            copy_into(&left_sibling->keys[left_key_j], internal->keys, del_key_idx + 1, n);

            copy_into(&left_sibling->children[left_child_j], internal->children, 0, del_child_idx);
            left_child_j += del_child_idx;
            copy_into(&left_sibling->children[left_child_j], internal->children, del_child_idx + 1, n + 1);

            left_sibling->set_key_slots_used(n + left_n);
            internal_node::release(internal);
            return R_MERGE_WITH_LEFT;
          }
        }

        INVARIANT(internal == root);
        INVARIANT(internal->is_root());
        remove_pos_from_internal_node(internal, del_key_idx, del_child_idx, n);
        INVARIANT(internal->key_slots_used() + 1 == n);
        if ((n - 1) == 0) {
          replace_node = internal->children[0];
          internal_node::release(internal);
          return R_REPLACE_NODE;
        }

        return UnlockAndReturn(locked_nodes, R_NONE);
      }

      default:
        ALWAYS_ASSERT(false);
        return UnlockAndReturn(locked_nodes, R_NONE);
      }
    }
  }

};

void
btree::node::invariant_checker(
    const key_type *min_key,
    const key_type *max_key,
    const node *left_sibling,
    const node *right_sibling,
    bool is_root) const
{
  is_leaf_node() ?
    AsLeaf(this)->invariant_checker_impl(min_key, max_key, left_sibling, right_sibling, is_root) :
    AsInternal(this)->invariant_checker_impl(min_key, max_key, left_sibling, right_sibling, is_root) ;
}

/** end of btree impl - the rest is a bunch of testing code */

// xor-shift:
// http://dmurphy747.wordpress.com/2011/03/23/xorshift-vs-random-performance-in-java/
class fast_random {
public:
  fast_random(unsigned long seed)
    : seed(seed == 0 ? 0xABCD1234 : seed)
  {}

  inline unsigned long
  next()
  {
    seed ^= (seed << 21);
    seed ^= (seed >> 35);
    seed ^= (seed << 4);
    return seed;
  }

private:
  unsigned long seed;
};

class scoped_rate_timer {
private:
  util::timer t;
  std::string region;
  size_t n;

public:
  scoped_rate_timer(const std::string &region, size_t n) : region(region), n(n)
  {}

  ~scoped_rate_timer()
  {
    double x = t.lap() / 1000.0; // ms
    double rate = double(n) / (x / 1000.0);
    std::cerr << "timed region `" << region << "' took " << x
              << " ms (" << rate << " events/sec)" << std::endl;
  }
};

#define WORKER(name) \
  static void * \
  name ## _worker(void *p) \
  { \
    btree *btr = (btree *) p; \
    name(btr); \
    return NULL; \
  }

#define WORKER_RET(name) \
  static void * \
  name ## _worker(void *p) \
  { \
    btree *btr = (btree *) p; \
    void *ret = name(btr); \
    pthread_exit(ret); \
    return NULL; \
  }

class btree_worker : public ndb_thread {
public:
  btree_worker(btree *btr) : btr(btr)  {}
  btree_worker(btree &btr) : btr(&btr) {}
protected:
  btree *const btr;
};

static void
test1()
{
  btree btr;
  btr.invariant_checker();

  // fill up root leaf node
  for (size_t i = 0; i < btree::NKeysPerNode; i++) {
    btr.insert(i, (btree::value_type) i);
    btr.invariant_checker();

    btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(i, v));
    ALWAYS_ASSERT(v == (btree::value_type) i);
  }
  ALWAYS_ASSERT(btr.size() == btree::NKeysPerNode);

  // induce a split
  btr.insert(btree::NKeysPerNode, (btree::value_type) (btree::NKeysPerNode));
  btr.invariant_checker();
  ALWAYS_ASSERT(btr.size() == btree::NKeysPerNode + 1);

  // now make sure we can find everything post split
  for (size_t i = 0; i < btree::NKeysPerNode + 1; i++) {
    btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(i, v));
    ALWAYS_ASSERT(v == (btree::value_type) i);
  }

  // now fill up the new root node
  const size_t n = (btree::NKeysPerNode + btree::NKeysPerNode * (btree::NMinKeysPerNode));
  for (size_t i = btree::NKeysPerNode + 1; i < n; i++) {
    btr.insert(i, (btree::value_type) i);
    btr.invariant_checker();

    btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(i, v));
    ALWAYS_ASSERT(v == (btree::value_type) i);
  }
  ALWAYS_ASSERT(btr.size() == n);

  // cause the root node to split
  btr.insert(n, (btree::value_type) n);
  btr.invariant_checker();
  ALWAYS_ASSERT(btr.size() == n + 1);

  // once again make sure we can find everything
  for (size_t i = 0; i < n + 1; i++) {
    btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(i, v));
    ALWAYS_ASSERT(v == (btree::value_type) i);
  }
}

static void
test2()
{
  btree btr;
  const size_t n = 1000;
  for (size_t i = 0; i < n; i += 2) {
    btr.insert(i, (btree::value_type) i);
    btr.invariant_checker();

    btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(i, v));
    ALWAYS_ASSERT(v == (btree::value_type) i);
  }

  for (size_t i = 1; i < n; i += 2) {
    btr.insert(i, (btree::value_type) i);
    btr.invariant_checker();

    btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(i, v));
    ALWAYS_ASSERT(v == (btree::value_type) i);
  }

  ALWAYS_ASSERT(btr.size() == n);
}

static void
test3()
{
  btree btr;

  for (size_t i = 0; i < btree::NKeysPerNode * 2; i++) {
    btr.insert(i, (btree::value_type) i);
    btr.invariant_checker();

    btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(i, v));
    ALWAYS_ASSERT(v == (btree::value_type) i);
  }
  ALWAYS_ASSERT(btr.size() == btree::NKeysPerNode * 2);

  for (size_t i = 0; i < btree::NKeysPerNode * 2; i++) {
    btr.remove(i);
    btr.invariant_checker();

    btree::value_type v = 0;
    ALWAYS_ASSERT(!btr.search(i, v));
  }
  ALWAYS_ASSERT(btr.size() == 0);

  for (size_t i = 0; i < btree::NKeysPerNode * 2; i++) {
    btr.insert(i, (btree::value_type) i);
    btr.invariant_checker();

    btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(i, v));
    ALWAYS_ASSERT(v == (btree::value_type) i);
  }
  ALWAYS_ASSERT(btr.size() == btree::NKeysPerNode * 2);

  for (ssize_t i = btree::NKeysPerNode * 2 - 1; i >= 0; i--) {
    btr.remove(i);
    btr.invariant_checker();

    btree::value_type v = 0;
    ALWAYS_ASSERT(!btr.search(i, v));
  }
  ALWAYS_ASSERT(btr.size() == 0);

  for (size_t i = 0; i < btree::NKeysPerNode * 2; i++) {
    btr.insert(i, (btree::value_type) i);
    btr.invariant_checker();

    btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(i, v));
    ALWAYS_ASSERT(v == (btree::value_type) i);
  }
  ALWAYS_ASSERT(btr.size() == btree::NKeysPerNode * 2);

  for (ssize_t i = btree::NKeysPerNode; i >= 0; i--) {
    btr.remove(i);
    btr.invariant_checker();

    btree::value_type v = 0;
    ALWAYS_ASSERT(!btr.search(i, v));
  }

  for (size_t i = btree::NKeysPerNode + 1; i < btree::NKeysPerNode * 2; i++) {
    btr.remove(i);
    btr.invariant_checker();

    btree::value_type v = 0;
    ALWAYS_ASSERT(!btr.search(i, v));
  }
  ALWAYS_ASSERT(btr.size() == 0);
}

static void
test4()
{
  btree btr;
  const size_t nkeys = 10000;
  for (size_t i = 0; i < nkeys; i++) {
    btr.insert(i, (btree::value_type) i);
    btr.invariant_checker();
    btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(i, v));
    ALWAYS_ASSERT(v == (btree::value_type) i);
  }
  ALWAYS_ASSERT(btr.size() == nkeys);

  srand(12345);

  for (size_t i = 0; i < nkeys; i++) {
    size_t k = rand() % nkeys;
    btr.remove(k);
    btr.invariant_checker();
    btree::value_type v = 0;
    ALWAYS_ASSERT(!btr.search(k, v));
  }

  for (size_t i = 0; i < nkeys; i++) {
    btr.remove(i);
    btr.invariant_checker();
    btree::value_type v = 0;
    ALWAYS_ASSERT(!btr.search(i, v));
  }
  ALWAYS_ASSERT(btr.size() == 0);
}

static void
test5()
{
  // insert in random order, delete in random order
  btree btr;

  unsigned int seeds[] = {
    54321, 2013883780, 3028985725, 3058602342, 256561598, 2895653051
  };

  for (size_t iter = 0; iter < ARRAY_NELEMS(seeds); iter++) {
    srand(seeds[iter]);
    const size_t nkeys = 20000;
    std::set<size_t> s;
    for (size_t i = 0; i < nkeys; i++) {
      size_t k = rand() % nkeys;
      s.insert(k);
      btr.insert(k, (btree::value_type) k);
      btr.invariant_checker();
      btree::value_type v = 0;
      ALWAYS_ASSERT(btr.search(k, v));
      ALWAYS_ASSERT(v == (btree::value_type) k);
    }
    ALWAYS_ASSERT(btr.size() == s.size());

    for (size_t i = 0; i < nkeys * 2; i++) {
      size_t k = rand() % nkeys;
      btr.remove(k);
      btr.invariant_checker();
      btree::value_type v = 0;
      ALWAYS_ASSERT(!btr.search(k, v));
    }

    // clean it up
    for (size_t i = 0; i < nkeys; i++) {
      btr.remove(i);
      btr.invariant_checker();
      btree::value_type v = 0;
      ALWAYS_ASSERT(!btr.search(i, v));
    }

    ALWAYS_ASSERT(btr.size() == 0);
  }
}

namespace test6_ns {
  struct scan_callback {
    typedef std::vector<
      std::pair< btree::key_type, btree::value_type > > kv_vec;
    scan_callback(kv_vec *data) : data(data) {}
    inline bool
    operator()(btree::key_type k, btree::value_type v) const
    {
      data->push_back(std::make_pair(k, v));
      return true;
    }
    kv_vec *data;
  };
}

static void
test6()
{
  btree btr;
  const size_t nkeys = 1000;
  for (size_t i = 0; i < nkeys; i++)
    btr.insert(i, (btree::value_type) i);
  btr.invariant_checker();
  ALWAYS_ASSERT(btr.size() == nkeys);

  using namespace test6_ns;

  scan_callback::kv_vec data;
  btree::key_type max_key = 600;
  btr.search_range(500, &max_key, scan_callback(&data));
  ALWAYS_ASSERT(data.size() == 100);
  for (size_t i = 0; i < 100; i++) {
    ALWAYS_ASSERT(data[i].first == 500 + i);
    ALWAYS_ASSERT(data[i].second == (btree::value_type) (500 + i));
  }

  data.clear();
  btr.search_range(500, NULL, scan_callback(&data));
  ALWAYS_ASSERT(data.size() == 500);
  for (size_t i = 0; i < 500; i++) {
    ALWAYS_ASSERT(data[i].first == 500 + i);
    ALWAYS_ASSERT(data[i].second == (btree::value_type) (500 + i));
  }
}

namespace mp_test1_ns {

  static const size_t nkeys = 20000;

  class ins0_worker : public btree_worker {
  public:
    ins0_worker(btree &btr) : btree_worker(btr) {}
    virtual void run()
    {
      for (size_t i = 0; i < nkeys / 2; i++)
        btr->insert(i, (btree::value_type) i);
    }
  };

  class ins1_worker : public btree_worker {
  public:
    ins1_worker(btree &btr) : btree_worker(btr) {}
    virtual void run()
    {
      for (size_t i = nkeys / 2; i < nkeys; i++)
        btr->insert(i, (btree::value_type) i);
    }
  };
}

static void
mp_test1()
{
  using namespace mp_test1_ns;

  // test a bunch of concurrent inserts
  btree btr;

  ins0_worker w0(btr);
  ins1_worker w1(btr);

  w0.start(); w1.start();
  w0.join(); w1.join();

  btr.invariant_checker();
  for (size_t i = 0; i < nkeys; i++) {
    btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(i, v));
    ALWAYS_ASSERT(v == (btree::value_type) i);
  }
  ALWAYS_ASSERT(btr.size() == nkeys);
}

namespace mp_test2_ns {

  static const size_t nkeys = 20000;

  class rm0_worker : public btree_worker {
  public:
    rm0_worker(btree &btr) : btree_worker(btr) {}
    virtual void run()
    {
      for (size_t i = 0; i < nkeys / 2; i++)
        btr->remove(i);
    }
  };

  class rm1_worker : public btree_worker {
  public:
    rm1_worker(btree &btr) : btree_worker(btr) {}
    virtual void run()
    {
      for (size_t i = nkeys / 2; i < nkeys; i++)
        btr->remove(i);
    }
  };
}

static void
mp_test2()
{
  using namespace mp_test2_ns;

  // test a bunch of concurrent removes
  btree btr;

  for (size_t i = 0; i < nkeys; i++)
    btr.insert(i, (btree::value_type) i);
  btr.invariant_checker();

  rm0_worker w0(btr);
  rm1_worker w1(btr);

  w0.start(); w1.start();
  w0.join(); w1.join();

  btr.invariant_checker();
  for (size_t i = 0; i < nkeys; i++) {
    btree::value_type v = 0;
    ALWAYS_ASSERT(!btr.search(i, v));
  }
  ALWAYS_ASSERT(btr.size() == 0);
}

namespace mp_test3_ns {

  static const size_t nkeys = 20000;

  class rm0_worker : public btree_worker {
  public:
    rm0_worker(btree &btr) : btree_worker(btr) {}
    virtual void run()
    {
      // remove the even keys
      for (size_t i = 0; i < nkeys; i += 2)
        btr->remove(i);
    }
  };

  class ins0_worker : public btree_worker {
  public:
    ins0_worker(btree &btr) : btree_worker(btr) {}
    virtual void run()
    {
      // insert the odd keys
      for (size_t i = 1; i < nkeys; i += 2)
        btr->insert(i, (btree::value_type) i);
    }
  };
}

static void
mp_test3()
{
  using namespace mp_test3_ns;

  // test a bunch of concurrent inserts and removes
  btree btr;

  // insert the even keys
  for (size_t i = 0; i < nkeys; i += 2)
    btr.insert(i, (btree::value_type) i);
  btr.invariant_checker();

  rm0_worker w0(btr);
  ins0_worker w1(btr);

  w0.start(); w1.start();
  w0.join(); w1.join();

  btr.invariant_checker();

  // should find no even keys
  for (size_t i = 0; i < nkeys; i += 2) {
    btree::value_type v = 0;
    ALWAYS_ASSERT(!btr.search(i, v));
  }

  // should find all odd keys
  for (size_t i = 1; i < nkeys; i += 2) {
    btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(i, v));
    ALWAYS_ASSERT(v == (btree::value_type) i);
  }

  ALWAYS_ASSERT(btr.size() == nkeys / 2);
}

namespace mp_test4_ns {

  static const size_t nkeys = 20000;

  class search0_worker : public btree_worker {
  public:
    search0_worker(btree &btr) : btree_worker(btr) {}
    virtual void run()
    {
      // search the even keys
      for (size_t i = 0; i < nkeys; i += 2) {
        btree::value_type v = 0;
        ALWAYS_ASSERT(btr->search(i, v));
        ALWAYS_ASSERT(v == (btree::value_type) i);
      }
    }
  };

  class ins0_worker : public btree_worker {
  public:
    ins0_worker(btree &btr) : btree_worker(btr) {}
    virtual void run()
    {
      // insert the odd keys
      for (size_t i = 1; i < nkeys; i += 2)
        btr->insert(i, (btree::value_type) i);
    }
  };

  class rm0_worker : public btree_worker {
  public:
    rm0_worker(btree &btr) : btree_worker(btr) {}
    virtual void run()
    {
      // remove and reinsert odd keys
      for (size_t i = 1; i < nkeys; i += 2) {
        btr->remove(i);
        btr->insert(i, (btree::value_type) i);
      }
    }
  };
}

static void
mp_test4()
{
  using namespace mp_test4_ns;

  // test a bunch of concurrent searches, inserts, and removes
  btree btr;

  // insert the even keys
  for (size_t i = 0; i < nkeys; i += 2)
    btr.insert(i, (btree::value_type) i);
  btr.invariant_checker();

  search0_worker w0(btr);
  ins0_worker w1(btr);
  rm0_worker w2(btr);

  w0.start(); w1.start(); w2.start();
  w0.join(); w1.join(); w2.join();

  btr.invariant_checker();

  // should find all keys
  for (size_t i = 0; i < nkeys; i++) {
    btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(i, v));
    ALWAYS_ASSERT(v == (btree::value_type) i);
  }

  ALWAYS_ASSERT(btr.size() == nkeys);
}

namespace mp_test5_ns {

  static const size_t niters = 100000;
  static const btree::key_type max_key = 45;

  typedef std::set<btree::key_type> key_set;

  struct summary {
    key_set inserts;
    key_set removes;
  };

  class worker : public btree_worker {
  public:
    worker(unsigned int seed, btree &btr) : btree_worker(btr), seed(seed) {}
    virtual void run()
    {
      unsigned int s = seed;
      // 60% search, 30% insert, 10% remove
      for (size_t i = 0; i < niters; i++) {
        double choice = double(rand_r(&s)) / double(RAND_MAX);
        btree::key_type k = rand_r(&s) % max_key;
        if (choice < 0.6) {
          btree::value_type v = 0;
          if (btr->search(k, v))
            ALWAYS_ASSERT(v == (btree::value_type) k);
        } else if (choice < 0.9) {
          btr->insert(k, (btree::value_type) k);
          sum.inserts.insert(k);
        } else {
          btr->remove(k);
          sum.removes.insert(k);
        }
      }
    }
    summary sum;
  private:
    unsigned int seed;
  };
}

static void
mp_test5()
{
  using namespace mp_test5_ns;

  btree btr;

  worker w0(2145906155, btr);
  worker w1(409088773, btr);
  worker w2(4199288861, btr);
  worker w3(496889962, btr);

  w0.start(); w1.start(); w2.start(); w3.start();
  w0.join(); w1.join(); w2.join(); w3.join();

  summary *s0, *s1, *s2, *s3;
  s0 = (summary *) &w0.sum;
  s1 = (summary *) &w1.sum;
  s2 = (summary *) &w2.sum;
  s3 = (summary *) &w3.sum;

  key_set inserts;
  key_set removes;

  summary *sums[] = { s0, s1, s2, s3 };
  for (size_t i = 0; i < ARRAY_NELEMS(sums); i++) {
    inserts.insert(sums[i]->inserts.begin(), sums[i]->inserts.end());
    removes.insert(sums[i]->removes.begin(), sums[i]->removes.end());
  }

  std::cerr << "num_inserts: " << inserts.size() << std::endl;
  std::cerr << "num_removes: " << removes.size() << std::endl;

  for (key_set::iterator it = inserts.begin(); it != inserts.end(); ++it) {
    if (removes.count(*it) == 1)
      continue;
    btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(*it, v));
    ALWAYS_ASSERT(v == (btree::value_type) *it);
  }

  btr.invariant_checker();
  std::cerr << "btr size: " << btr.size() << std::endl;
}

namespace mp_test6_ns {
  static const size_t nthreads = 16;
  static const size_t ninsertkeys_perthread = 100000;
  static const size_t nremovekeys_perthread = 100000;

  typedef std::vector<btree::key_type> key_vec;

  class insert_worker : public btree_worker {
  public:
    insert_worker(const std::vector<btree::key_type> &keys, btree &btr)
      : btree_worker(btr), keys(keys) {}
    virtual void run()
    {
      for (size_t i = 0; i < keys.size(); i++)
        btr->insert(keys[i], (btree::value_type) keys[i]);
    }
  private:
    std::vector<btree::key_type> keys;
  };

  class remove_worker : public btree_worker {
  public:
    remove_worker(const std::vector<btree::key_type> &keys, btree &btr)
      : btree_worker(btr), keys(keys) {}
    virtual void run()
    {
      for (size_t i = 0; i < keys.size(); i++)
        btr->remove(keys[i]);
    }
  private:
    std::vector<btree::key_type> keys;
  };
}

static void
mp_test6()
{
  using namespace mp_test6_ns;

  btree btr;
  std::vector<key_vec> inps;
  std::set<unsigned long> insert_keys, remove_keys;

  fast_random r(87643982);
  for (size_t i = 0; i < nthreads / 2; i++) {
    key_vec inp;
    for (size_t j = 0; j < ninsertkeys_perthread; j++) {
      unsigned long k = r.next();
      insert_keys.insert(k);
      inp.push_back(k);
    }
    inps.push_back(inp);
  }
  for (size_t i = nthreads / 2; i < nthreads; i++) {
    key_vec inp;
    for (size_t j = 0; j < nremovekeys_perthread;) {
      unsigned long k = r.next();
      if (insert_keys.count(k) == 1)
        continue;
      btr.insert(k, (btree::value_type) k);
      remove_keys.insert(k);
      inp.push_back(k);
      j++;
    }
    inps.push_back(inp);
  }

  std::vector<btree_worker*> workers;
  for (size_t i = 0; i < nthreads / 2; i++)
    workers.push_back(new insert_worker(inps[i], btr));
  for (size_t i = nthreads / 2; i < nthreads; i++)
    workers.push_back(new remove_worker(inps[i], btr));
  for (size_t i = 0; i < nthreads; i++)
    workers[i]->start();
  for (size_t i = 0; i < nthreads; i++)
    workers[i]->join();

  btr.invariant_checker();

  ALWAYS_ASSERT(btr.size() == insert_keys.size());
  for (std::set<unsigned long>::iterator it = insert_keys.begin();
       it != insert_keys.end(); ++it) {
    btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(*it, v));
    ALWAYS_ASSERT(v == (btree::value_type) *it);
  }
  for (std::set<unsigned long>::iterator it = remove_keys.begin();
       it != remove_keys.end(); ++it) {
    btree::value_type v = 0;
    ALWAYS_ASSERT(!btr.search(*it, v));
  }

  for (size_t i = 0; i < nthreads; i++)
    delete workers[i];
}

namespace mp_test7_ns {
  static const size_t nkeys = 50;
  static volatile bool running = false;

  typedef std::vector<btree::key_type> key_vec;

  struct scan_callback {
    typedef std::vector<
      std::pair< btree::key_type, btree::value_type > > kv_vec;
    scan_callback(kv_vec *data) : data(data) {}
    inline bool
    operator()(btree::key_type k, btree::value_type v) const
    {
      data->push_back(std::make_pair(k, v));
      return true;
    }
    kv_vec *data;
  };

  class lookup_worker : public btree_worker {
  public:
    lookup_worker(unsigned long seed, const key_vec &keys, btree &btr)
      : btree_worker(btr), seed(seed), keys(keys)
    {}
    virtual void run()
    {
      fast_random r(seed);
      while (running) {
        btree::key_type k = keys[r.next() % keys.size()];
        btree::value_type v = NULL;
        ALWAYS_ASSERT(btr->search(k, v));
        ALWAYS_ASSERT(v == (btree::value_type) k);
      }
    }
    unsigned long seed;
    key_vec keys;
  };

  class scan_worker : public btree_worker {
  public:
    scan_worker(const key_vec &keys, btree &btr)
      : btree_worker(btr), keys(keys)
    {}
    virtual void run()
    {
      while (running) {
        scan_callback::kv_vec data;
        btr->search_range(nkeys / 2, NULL, scan_callback(&data));
        std::set<btree::key_type> scan_keys;
        btree::key_type prev = 0;
        for (size_t i = 0; i < data.size(); i++) {
          if (i != 0)
            ALWAYS_ASSERT(data[i].first > prev);
          scan_keys.insert(data[i].first);
          prev = data[i].first;
        }
        for (size_t i = 0; i < keys.size(); i++) {
          if (keys[i] < (nkeys / 2))
            continue;
          ALWAYS_ASSERT(scan_keys.count(keys[i]) == 1);
        }
      }
    }
    key_vec keys;
  };

  class mod_worker : public btree_worker {
  public:
    mod_worker(const key_vec &keys, btree &btr)
      : btree_worker(btr), keys(keys)
    {}
    virtual void run()
    {
      bool insert = true;
      for (size_t i = 0; running; i = (i + 1) % keys.size(), insert = !insert){
        if (insert)
          btr->insert(keys[i], (btree::value_type) keys[i]);
        else
          btr->remove(keys[i]);
      }
    }
    key_vec keys;
  };
}

static void
mp_test7()
{
  using namespace mp_test7_ns;
  fast_random r(904380439);
  key_vec lookup_keys;
  key_vec mod_keys;
  for (size_t i = 0; i < nkeys; i++) {
    if (r.next() % 2)
      mod_keys.push_back(i);
    else
      lookup_keys.push_back(i);
  }

  btree btr;
  for (size_t i = 0; i < lookup_keys.size(); i++)
    btr.insert(lookup_keys[i], (btree::value_type) lookup_keys[i]);

  lookup_worker w0(2398430, lookup_keys, btr);
  lookup_worker w1(8532, lookup_keys, btr);
  lookup_worker w2(23, lookup_keys, btr);
  lookup_worker w3(1328209843, lookup_keys, btr);
  scan_worker w4(lookup_keys, btr);
  scan_worker w5(lookup_keys, btr);
  mod_worker w6(mod_keys, btr);

  running = true;
  COMPILER_MEMORY_FENCE;
  w0.start(); w1.start(); w2.start(); w3.start(); w4.start(); w5.start(); w6.start();
  sleep(10);
  COMPILER_MEMORY_FENCE;
  running = false;
  COMPILER_MEMORY_FENCE;
  w0.join(); w1.join(); w2.join(); w3.join(); w4.join(); w5.join(); w6.join();
}

static void
perf_test()
{
  const size_t nrecs = 10000000;
  const size_t nlookups = 10000000;

  {
    srand(9876);
    std::map<uint64_t, uint64_t> m;
    {
      scoped_rate_timer t("std::map insert", nrecs);
      for (size_t i = 0; i < nrecs; i++)
        m[i] = i;
    }
    {
      scoped_rate_timer t("std::map random lookups", nlookups);
      for (size_t i = 0; i < nlookups; i++) {
        //uint64_t key = rand() % nrecs;
        uint64_t key = i;
        std::map<uint64_t, uint64_t>::iterator it =
          m.find(key);
        ALWAYS_ASSERT(it != m.end());
      }
    }
  }

  {
    srand(9876);
    btree btr;
    {
      scoped_rate_timer t("btree insert", nrecs);
      for (size_t i = 0; i < nrecs; i++)
        btr.insert(i, (btree::value_type) i);
    }
    {
      scoped_rate_timer t("btree random lookups", nlookups);
      for (size_t i = 0; i < nlookups; i++) {
        //uint64_t key = rand() % nrecs;
        uint64_t key = i;
        btree::value_type v = 0;
        ALWAYS_ASSERT(btr.search(key, v));
      }
    }
  }
}

namespace read_only_perf_test_ns {
  const size_t nkeys = 140000000; // 140M
  //const size_t nkeys = 100000; // 100K

  unsigned long seeds[] = {
    9576455804445224191ULL,
    3303315688255411629ULL,
    3116364238170296072ULL,
    641702699332002535ULL,
    17755947590284612420ULL,
    13349066465957081273ULL,
    16389054441777092823ULL,
    2687412585397891607ULL,
    16665670053534306255ULL,
    5166823197462453937ULL,
    1252059952779729626ULL,
    17962022827457676982ULL,
    940911318964853784ULL,
    479878990529143738ULL,
    250864516707124695ULL,
    8507722621803716653ULL,
  };

  volatile bool running = false;

  class worker : public btree_worker {
  public:
    worker(unsigned int seed, btree &btr) : btree_worker(btr), n(0), seed(seed) {}
    virtual void run()
    {
      fast_random r(seed);
      while (running) {
        btree::key_type k = r.next() % nkeys;
        btree::value_type v = 0;
        ALWAYS_ASSERT(btr->search(k, v));
        ALWAYS_ASSERT(v == (btree::value_type) k);
        n++;
      }
    }
    uint64_t n;
  private:
    unsigned int seed;
  };
}

static void
read_only_perf_test()
{
  using namespace read_only_perf_test_ns;

  btree btr;

  for (size_t i = 0; i < nkeys; i++)
    btr.insert(i, (btree::value_type) i);
  std::cerr << "btree loaded, test starting" << std::endl;

  std::vector<worker *> workers;
  for (size_t i = 0; i < ARRAY_NELEMS(seeds); i++)
    workers.push_back(new worker(seeds[i], btr));

  running = true;
  util::timer t;
  COMPILER_MEMORY_FENCE;
  for (size_t i = 0; i < ARRAY_NELEMS(seeds); i++)
    workers[i]->start();
  sleep(30);
  COMPILER_MEMORY_FENCE;
  running = false;
  COMPILER_MEMORY_FENCE;
  uint64_t total_n = 0;
  for (size_t i = 0; i < ARRAY_NELEMS(seeds); i++) {
    workers[i]->join();
    total_n += workers[i]->n;
    delete workers[i];
  }

  double agg_throughput = double(total_n) / (double(t.lap()) / 1000000.0);
  double avg_per_core_throughput = agg_throughput / double(ARRAY_NELEMS(seeds));

  std::cerr << "agg_read_throughput: " << agg_throughput << " gets/sec" << std::endl;
  std::cerr << "avg_per_core_read_throughput: " << avg_per_core_throughput << " gets/sec/core" << std::endl;
}

namespace write_only_perf_test_ns {
  const size_t nkeys = 140000000; // 140M
  //const size_t nkeys = 100000; // 100K

  unsigned long seeds[] = {
    17188055221422272641ULL,
    915721317773011804ULL,
    11607688859420148202ULL,
    16566896965529356730ULL,
    3687473034241167633ULL,
    1168118474092824592ULL,
    912212972587845337ULL,
    890657129662032640ULL,
    7557640044845923769ULL,
    9490577770668659131ULL,
    14081403972130650060ULL,
    14956552848279294368ULL,
    8669268465391111275ULL,
    1904251150166743550ULL,
    4418832947790992405ULL,
    9558684485283258563ULL,
  };

  class worker : public btree_worker {
  public:
    worker(unsigned int seed, btree &btr) : btree_worker(btr), seed(seed) {}
    virtual void run()
    {
      fast_random r(seed);
      for (size_t i = 0; i < nkeys / ARRAY_NELEMS(seeds); i++) {
        btree::key_type k = r.next() % nkeys;
        btr->insert(k, (btree::value_type) k);
      }
    }
  private:
    unsigned int seed;
  };
}

static void
write_only_perf_test()
{
  using namespace write_only_perf_test_ns;

  btree btr;

  std::vector<worker *> workers;
  for (size_t i = 0; i < ARRAY_NELEMS(seeds); i++)
    workers.push_back(new worker(seeds[i], btr));

  util::timer t;
  for (size_t i = 0; i < ARRAY_NELEMS(seeds); i++)
    workers[i]->start();
  for (size_t i = 0; i < ARRAY_NELEMS(seeds); i++) {
    workers[i]->join();
    delete workers[i];
  }

  double agg_throughput = double(nkeys) / (double(t.lap()) / 1000000.0);
  double avg_per_core_throughput = agg_throughput / double(ARRAY_NELEMS(seeds));

  std::cerr << "agg_write_throughput: " << agg_throughput << " puts/sec" << std::endl;
  std::cerr << "avg_per_core_write_throughput: " << avg_per_core_throughput << " puts/sec/core" << std::endl;
}

class main_thread : public ndb_thread {
public:
  main_thread(int argc, char **argv)
    : argc(argc), argv(argv), ret(0)
  {}

  virtual void
  run()
  {
    test1();
    test2();
    test3();
    test4();
    test5();
    test6();
    mp_test1();
    mp_test2();
    mp_test3();
    mp_test4();
    mp_test5();
    mp_test6();
    mp_test7();
    //perf_test();
    //read_only_perf_test();
    //write_only_perf_test();

    ret = 0;
  }

  inline int
  retval() const
  {
    return ret;
  }
private:
  int argc;
  char **argv;
  volatile int ret;
};

int
main(int argc, char **argv)
{
  main_thread t(argc, argv);
  t.start();
  t.join();
  return t.retval();
}
