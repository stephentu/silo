#ifndef _NDB_BTREE_H_
#define _NDB_BTREE_H_

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

#include "varkey.h"
#include "macros.h"
#include "amd64.h"
#include "rcu.h"
#include "static_assert.h"
#include "util.h"

/** options */

//#define LOCK_OWNERSHIP_CHECKING
#define BTREE_NODE_ALLOC_CACHE_ALIGNED

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
class btree : public rcu_enabled {
  friend class txn_btree;
public:
  typedef varkey key_type;
  typedef uint64_t key_slice;
  typedef uint8_t* value_type;

  // public to assist in testing
  // WARNING: if you want to increase NKeysPerNode beyond 15, must also
  // increase the # of size bits in the header
  static const unsigned int NKeysPerNode = 14;
  static const unsigned int NMinKeysPerNode = NKeysPerNode / 2;

private:

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
    key_slice keys[NKeysPerNode];

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
      while (IsLocked(v) || !__sync_bool_compare_and_swap(&hdr, v, v | HDR_LOCKED_MASK)) {
        nop_pause();
        v = hdr;
      }
#ifdef LOCK_OWNERSHIP_CHECKING
      lock_owner = pthread_self();
#endif
      COMPILER_MEMORY_FENCE;
      //std::cerr << "0x" << util::hexify(this) << ": lock acquired: "
      //          << VersionInfoStr(v | HDR_LOCKED_MASK) << std::endl;
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
      //std::cerr << "0x" << util::hexify(this) << ":lock released: "
      //          << VersionInfoStr(v) << std::endl;
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
      while (is_modifying()) {
        nop_pause();
        v = hdr;
      }
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

    static std::string VersionInfoStr(uint64_t v);

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
  };

  struct leaf_node : public node {
    union value_or_node_ptr {
      value_type v;
      node *n;
    };

    key_slice min_key; // really is min_key's key slice
    value_or_node_ptr values[NKeysPerNode];

    // format is:
    // [ slice_length | type | unused ]
    // [    0:4       |  4:5 |  5:8   ]
    uint8_t lengths[NKeysPerNode];

    leaf_node *prev;
    leaf_node *next;

    // starts out empty- once set, doesn't get freed until dtor (even if all
    // keys w/ suffixes get removed)
    imstring *suffixes;

    inline ALWAYS_INLINE varkey
    suffix(size_t i) const
    {
      return suffixes ? varkey(suffixes[i]) : varkey();
    }

    inline void
    alloc_suffixes()
    {
      INVARIANT(is_modifying());
      INVARIANT(!suffixes);
      suffixes = new imstring[NKeysPerNode];
    }

    inline void
    ensure_suffixes()
    {
      INVARIANT(is_modifying());
      if (!suffixes)
        alloc_suffixes();
    }

    leaf_node();
    ~leaf_node();

    static const uint64_t LEN_LEN_MASK = 0xf;

    static const uint64_t LEN_TYPE_SHIFT = 4;
    static const uint64_t LEN_TYPE_MASK = 0x1 << LEN_TYPE_SHIFT;

    inline size_t
    keyslice_length(size_t n) const
    {
      INVARIANT(n < NKeysPerNode);
      return lengths[n] & LEN_LEN_MASK;
    }

    inline void
    keyslice_set_length(size_t n, size_t len, bool layer)
    {
      INVARIANT(n < NKeysPerNode);
      INVARIANT(is_modifying());
      INVARIANT(len <= 9);
      INVARIANT(!layer || len == 9);
      lengths[n] = (len | (layer ? LEN_TYPE_MASK : 0));
    }

    inline bool
    value_is_layer(size_t n) const
    {
      INVARIANT(n < NKeysPerNode);
      return lengths[n] & LEN_TYPE_MASK;
    }

    inline void
    value_set_layer(size_t n)
    {
      INVARIANT(n < NKeysPerNode);
      INVARIANT(is_modifying());
      INVARIANT(keyslice_length(n) == 9);
      INVARIANT(!value_is_layer(n));
      lengths[n] |= LEN_TYPE_MASK;
    }

    /**
     * keys[key_search(k).first] == k if key_search(k).first != -1
     * key does not exist otherwise. considers key length also
     */
    inline key_search_ret
    key_search(key_slice k, size_t len) const
    {
      size_t n = key_slots_used();
      ssize_t lower = 0;
      ssize_t upper = n;
      while (lower < upper) {
        ssize_t i = (lower + upper) / 2;
        key_slice k0 = keys[i];
        size_t len0 = keyslice_length(i);
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
      size_t n = key_slots_used();
      ssize_t lower = 0;
      ssize_t upper = n;
      while (lower < upper) {
        ssize_t i = (lower + upper) / 2;
        key_slice k0 = keys[i];
        size_t len0 = keyslice_length(i);
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
#ifdef BTREE_NODE_ALLOC_CACHE_ALIGNED
      void *p = memalign(CACHELINE_SIZE, sizeof(leaf_node));
#else
      void *p = malloc(sizeof(leaf_node));
#endif
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
      free(n);
    }

    static inline void
    release(leaf_node *n)
    {
      if (unlikely(!n))
        return;
      n->mark_deleting();
      rcu::free_with_fn(n, deleter);
    }

  } PACKED;

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

    internal_node();
    ~internal_node();

    /**
     * keys[key_search(k).first] == k if key_search(k).first != -1
     * key does not exist otherwise. operates ony on key slices
     * (internal nodes have unique key slices)
     */
    inline key_search_ret
    key_search(key_slice k) const
    {
      size_t n = key_slots_used();
      ssize_t lower = 0;
      ssize_t upper = n;
      while (lower < upper) {
        ssize_t i = (lower + upper) / 2;
        key_slice k0 = keys[i];
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
      size_t n = key_slots_used();
      ssize_t lower = 0;
      ssize_t upper = n;
      while (lower < upper) {
        ssize_t i = (lower + upper) / 2;
        key_slice k0 = keys[i];
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
#ifdef BTREE_NODE_ALLOC_CACHE_ALIGNED
      void *p = memalign(CACHELINE_SIZE, sizeof(internal_node));
#else
      void *p = malloc(sizeof(internal_node));
#endif
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
      free(n);
    }

    static inline void
    release(internal_node *n)
    {
      if (unlikely(!n))
        return;
      n->mark_deleting();
      rcu::free_with_fn(n, deleter);
    }

  } PACKED;

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

  /**
   * Is not thread safe, and does not use RCU to free memory
   *
   * Should only be called when there are no outstanding operations on
   * any nodes reachable from n
   */
  static void recursive_delete(node *n);

  node *volatile root;

public:

  // XXX(stephentu): trying out a very opaque node API for now
  typedef struct node node_opaque_t;

  btree() : root(leaf_node::alloc())
  {
    _static_assert(NKeysPerNode > (sizeof(key_slice) + 2)); // so we can always do a split
    _static_assert(NKeysPerNode <= (HDR_KEY_SLOTS_MASK >> HDR_KEY_SLOTS_SHIFT));

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

  /**
   * NOT THREAD SAFE
   */
  inline void
  clear()
  {
    recursive_delete(root);
    root = leaf_node::alloc();
#ifdef CHECK_INVARIANTS
    root->lock();
    root->set_root();
    root->unlock();
#else
    root->set_root();
#endif /* CHECK_INVARIANTS */
  }

  /** Note: invariant checking is not thread safe */
  inline void
  invariant_checker() const
  {
    root->invariant_checker(NULL, NULL, NULL, NULL, true);
  }

  inline bool
  search(const key_type &k, value_type &v) const
  {
    std::vector<leaf_node *> ns;
    scoped_rcu_region rcu_region;
    return search_impl(k, v, ns);
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
    virtual bool invoke(const key_type &k, value_type v,
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
    invoke(const key_type &k, value_type v,
           const node_opaque_t *n, uint64_t version)
    {
      return invoke(k, v);
    }

    virtual bool invoke(const key_type &k, value_type v) = 0;
  };

private:
  template <typename T>
  class type_callback_wrapper : public search_range_callback {
  public:
    type_callback_wrapper(T *callback) : callback(callback) {}
    virtual bool
    invoke(const key_type &k, value_type v)
    {
      return callback->operator()(k, v);
    }
  private:
    T *const callback;
  };

  struct leaf_kvinfo;

  bool search_range_at_layer(leaf_node *leaf,
                             const std::string &prefix,
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
   */
  void
  search_range_call(const key_type &lower,
                    const key_type *upper,
                    low_level_search_range_callback &callback) const;

  /**
   * Callback is expected to implement bool operator()(key_slice k, value_type v),
   * where the callback returns true if it wants to keep going, false otherwise
   *
   */
  template <typename T>
  inline void
  search_range(const key_type &lower, const key_type *upper, T callback) const
  {
    type_callback_wrapper<T> w(&callback);
    search_range_call(lower, upper, w);
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
         std::pair< const node_opaque_t *, uint64_t > *insert_info = NULL)
  {
    // XXX: not sure if this cast is safe
    return insert_impl((node **) &root, k, v, false, old_v, insert_info);
  }

  /**
   * Only puts k=>v if k does not exist in map. returns true
   * if k inserted, false otherwise (k exists already)
   */
  inline bool
  insert_if_absent(const key_type &k, value_type v,
                   std::pair< const node_opaque_t *, uint64_t > *insert_info = NULL)
  {
    return insert_impl((node **) &root, k, v, true, NULL, insert_info);
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
    return remove_impl((node **) &root, k, old_v);
  }

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
    size_walk_callback() : spec_size(0), size(0) {}
    virtual void on_node_begin(const node_opaque_t *n);
    virtual void on_node_success();
    virtual void on_node_failure();
    inline size_t get_size() const { return size; }
  private:
    size_t spec_size;
    size_t size;
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
    return node::Version(n->stable_version());
  }

  // [value, has_suffix]
  static std::vector< std::pair<value_type, bool> >
  ExtractValues(const node_opaque_t *n);

  /**
   * Not well defined if n is being concurrently modified, just for debugging
   */
  static std::string
  NodeStringify(const node_opaque_t *n);

  static void TestFast();
  static void TestSlow();

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
  bool search_impl(const key_type &k, value_type &v, std::vector<leaf_node *> &leaf_nodes) const;

  bool insert_impl(node **root_location, const key_type &k, value_type v, bool only_if_absent,
                   value_type *old_v, std::pair< const node_opaque_t *, uint64_t > *insert_info);

  bool remove_impl(node **root_location, const key_type &k, value_type *old_v);

  typedef std::pair<node *, uint64_t> insert_parent_entry;

  enum insert_status {
    I_NONE_NOMOD,
    I_NONE_MOD,
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
          const key_type &k,
          value_type v,
          bool only_if_absent,
          value_type *old_v,
          std::pair< const node_opaque_t *, uint64_t > *insert_info,
          key_slice &min_key,
          node *&new_node,
          std::vector<insert_parent_entry> &parents,
          std::vector<node *> &locked_nodes);

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
      leaf->values[pos].n->lock();
#endif
      leaf->values[pos].n->mark_deleting();
      INVARIANT(leaf->values[pos].n->is_leaf_node());
      INVARIANT(leaf->values[pos].n->key_slots_used() == 0);
      leaf_node::release((leaf_node *) leaf->values[pos].n);
#ifdef CHECK_INVARIANTS
      leaf->values[pos].n->unlock();
#endif
    }
    sift_left(leaf->keys, pos, n);
    sift_left(leaf->values, pos, n);
    sift_left(leaf->lengths, pos, n);
    if (leaf->suffixes)
      sift_swap_left(leaf->suffixes, pos, n);
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
          std::vector<remove_parent_entry> &parents,
          std::vector<node *> &locked_nodes);
};

#endif /* _NDB_BTREE_H_ */
