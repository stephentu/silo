#ifndef _NDB_BTREE_H_
#define _NDB_BTREE_H_

#include <assert.h>
#include <malloc.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <string>
#include <vector>
#include <utility>

#include "macros.h"
#include "rcu.h"
#include "static_assert.h"

/** options */
#define NODE_PREFETCH
#define CHECK_INVARIANTS
//#define LOCK_OWNERSHIP_CHECKING
//#define USE_MEMMOVE
//#define USE_MEMCPY

/** macro helpers */

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
 * This btree maps fixed-size keys of type key_type -> value_type, where
 * key_type is uint64_t and value_type is a pointer to un-interpreted byte
 * string
 *
 * This btree does not manage the memory pointed to by value_type
 */
class btree : public rcu_enabled {
  friend class txn_btree;
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

    static std::string VersionInfoStr(uint64_t v);

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
    base_invariant_checker(const key_type *min_key,
                           const key_type *max_key,
                           bool is_root) const;

    /** manually simulated virtual function (so we don't make node virtual) */
    void
    invariant_checker(const key_type *min_key,
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

    leaf_node()
      : min_key(0), prev(NULL), next(NULL)
    {
      hdr = 0;
    }

    void
    invariant_checker_impl(const key_type *min_key,
                           const key_type *max_key,
                           const node *left_sibling,
                           const node *right_sibling,
                           bool is_root) const;

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
    invariant_checker_impl(const key_type *min_key,
                           const key_type *max_key,
                           const node *left_sibling,
                           const node *right_sibling,
                           bool is_root) const;

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

  static void recursive_delete(node *n);

  node *volatile root;

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
  inline void
  invariant_checker() const
  {
    root->invariant_checker(NULL, NULL, NULL, NULL, true);
  }

  inline bool
  search(key_type k, value_type &v) const
  {
    leaf_node *n;
    scoped_rcu_region rcu_region;
    return search_impl(k, v, n);
  }


  class search_range_callback {
  public:
    virtual ~search_range_callback() {}
    virtual bool invoke(key_type k, value_type v) = 0;
  };

private:
  template <typename T>
  class type_callback_wrapper : public search_range_callback {
  public:
    type_callback_wrapper(T *callback) : callback(callback) {}
    virtual bool
    invoke(key_type k, value_type v)
    {
      return callback->operator()(k, v);
    }
  private:
    T *callback;
  };

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
  search_range_call(key_type lower, const key_type *upper, search_range_callback &callback) const;


  /**
   * Callback is expected to implement bool operator()(key_type k, value_type v),
   * where the callback returns true if it wants to keep going, false otherwise
   *
   */
  template <typename T>
  inline void
  search_range(key_type lower, const key_type *upper, T callback) const
  {
    type_callback_wrapper<T> w(&callback);
    search_range_call(lower, upper, w);
  }

  /**
   * returns true if key k did not already exist, false otherwise
   * If k exists with a different mapping, still returns false
   */
  inline bool
  insert(key_type k, value_type v)
  {
    return insert_impl(k, v, false);
  }

  /**
   * Only puts k=>v if k does not exist in map. returns true
   * if k inserted, false otherwise (k exists already)
   */
  inline bool
  insert_if_absent(key_type k, value_type v)
  {
    return insert_impl(k, v, true);
  }

  bool remove(key_type k);

  size_t size() const;

  static void Test();

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

  bool search_impl(key_type k, value_type &v, leaf_node *&n) const;

  bool insert_impl(key_type k, value_type v, bool only_if_absent);

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
          key_type k,
          value_type v,
          bool only_if_absent,
          key_type &min_key,
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
          std::vector<node *> &locked_nodes);
};

#endif /* _NDB_BTREE_H_ */
