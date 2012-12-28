#include <assert.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <malloc.h>
#include <stdio.h>

#include <map>
#include <cstddef>
#include <cstdlib>
#include <iostream>
#include <vector>
#include <utility>
#include <stdexcept>

#include "static_assert.h"
#include "util.h"

#define CACHELINE_SIZE 64
#define PACKED_CACHE_ALIGNED __attribute__((packed, aligned(CACHELINE_SIZE)))
#define NEVER_INLINE  __attribute__((noinline))
#define ALWAYS_INLINE __attribute__((always_inline))

#define likely(x)   __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

#define COMPILER_MEMORY_FENCE asm volatile("" ::: "memory")

#ifdef NDEBUG
  #define ALWAYS_ASSERT(expr) (likely(e) ? (void)0 : abort())
#else
  #define ALWAYS_ASSERT(expr) assert(expr)
#endif /* NDEBUG */

#define CHECK_INVARIANTS

#ifdef CHECK_INVARIANTS
  #define INVARIANT(expr) ALWAYS_ASSERT(expr)
#else
  #define INVARIANT(expr) ((void)0)
#endif /* CHECK_INVARIANTS */

//#define USE_MEMMOVE
//#define USE_MEMCPY

#define NODE_PREFETCH

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
class btree {
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

  static const uint64_t HDR_MODIFYING_SHIFT = 6;
  static const uint64_t HDR_MODIFYING_MASK = 0x1 << HDR_MODIFYING_SHIFT;

  static const uint64_t HDR_VERSION_SHIFT = 7;
  static const uint64_t HDR_VERSION_MASK = ((uint64_t)-1) << HDR_VERSION_SHIFT;

  class retry_exception {};

  typedef std::pair<ssize_t, size_t> key_search_ret;

  struct node {

    /**
     * hdr bits: layout is:
     *
     * <-- low bits
     * [type | key_slots_used | locked | modifying | version ]
     * [0:1  | 1:5            | 5:6    | 6:7       | 7:64    ]
     *
     * bit invariants:
     *   1) modifying => locked
     *
     * WARNING: the correctness of our concurrency scheme relies on being able
     * to do a memory reads/writes from/to hdr atomically. x86 architectures
     * guarantee that aligned writes are atomic (see intel spec)
     *
     */
    volatile uint64_t hdr;

    /**
     * Keys are assumed to be stored in contiguous sorted order, so that all
     * the used slots are grouped together. That is, elems in positions [0,
     * key_slots_used) are valid, and elems in positions [key_slots_used,
     * NKeysPerNode) are empty
     */
    key_type keys[NKeysPerNode];

    inline bool
    is_leaf_node() const
    {
      return (hdr & HDR_TYPE_MASK) == 0;
    }

    inline bool
    is_internal_node() const
    {
      return !is_leaf_node();
    }

    inline size_t
    key_slots_used() const
    {
      return (hdr & HDR_KEY_SLOTS_MASK) >> HDR_KEY_SLOTS_SHIFT;
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
      return (v & HDR_LOCKED_MASK) >> HDR_LOCKED_SHIFT;
    }

    inline void
    lock()
    {
      uint64_t v = hdr;
      while (IsLocked(v) || !__sync_bool_compare_and_swap(&hdr, v, v | HDR_LOCKED_MASK))
        v = hdr;
      COMPILER_MEMORY_FENCE;
    }

    inline void
    unlock()
    {
      uint64_t v = hdr;
      uint64_t n = Version(v);
      bool mod = false;
      INVARIANT(IsLocked(v));
      if (IsModifying(v)) {
        mod = true;
        v &= ~HDR_VERSION_MASK;
        v |= (((n + 1) << HDR_VERSION_SHIFT) & HDR_VERSION_MASK);
      }
      // clear locked + modifying bits
      v &= ~(HDR_LOCKED_MASK | HDR_MODIFYING_MASK);
      INVARIANT(!IsLocked(v));
      INVARIANT(!IsModifying(v));
      INVARIANT(mod || Version(v) == n);
      COMPILER_MEMORY_FENCE;
      hdr = v;
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
      INVARIANT(!IsModifying(v));
      v |= HDR_MODIFYING_MASK;
      COMPILER_MEMORY_FENCE;
      hdr = v;
      COMPILER_MEMORY_FENCE;
    }

    static inline bool
    IsModifying(uint64_t v)
    {
      return (v & HDR_MODIFYING_MASK) >> HDR_MODIFYING_SHIFT;
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
    value_type values[NKeysPerNode];
    leaf_node *prev;
    leaf_node *next;
    leaf_node() : prev(NULL), next(NULL)
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

    static inline void
    release(leaf_node *n)
    {
      if (!n)
        return;
      // XXX: cannot free yet, need to hook into RCU scheme
      //n->~leaf_node();
      //free(n);
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

    static inline internal_node*
    alloc()
    {
      void *p = memalign(CACHELINE_SIZE, sizeof(internal_node));
      if (unlikely(!p))
        return NULL;
      return new (p) internal_node;
    }

    static inline void
    release(internal_node *n)
    {
      if (unlikely(!n))
        return;
      // XXX: cannot free yet, need to hook into RCU scheme
      //n->~internal_node();
      //free(n);
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

  node *root;

public:

  btree() : root(leaf_node::alloc())
  {
    _static_assert(sizeof(leaf_node) <= NodeSize);
    _static_assert(sizeof(leaf_node) % 64 == 0);
    _static_assert(sizeof(internal_node) <= NodeSize);
    _static_assert(sizeof(internal_node) % 64 == 0);
  }

  ~btree()
  {
    // XXX: cleanup

  }

  void
  invariant_checker() const
  {
    root->invariant_checker(NULL, NULL, NULL, NULL, true);
  }

  bool
  search(key_type k, value_type &v) const
  {
  retry:
    node *cur = root;
    while (cur) {
      prefetch_node(cur);
      uint64_t version = cur->stable_version();
      if (leaf_node *leaf = AsLeafCheck(cur)) {
        key_search_ret kret = leaf->key_search(k);
        ssize_t ret = kret.first;
        if (ret != -1)
          v = leaf->values[ret];
        if (!leaf->check_version(version))
          goto retry;
        return ret != -1;
      } else {
        internal_node *internal = AsInternal(cur);
        key_search_ret kret = internal->key_lower_bound_search(k);
        ssize_t ret = kret.first;
        size_t n = kret.second;
        if (ret != -1)
          cur = internal->children[ret + 1];
        else
          cur = n ? internal->children[0] : NULL;
        if (!internal->check_version(version))
          goto retry;
      }
    }
    return false;
  }

  void
  insert(key_type k, value_type v)
  {
  retry:
    key_type mk;
    std::vector<insert_parent_entry> parents;
    std::vector<node *> locked_nodes;
    try {
      node *ret = insert0(root, k, v, mk, parents, locked_nodes);
      if (ret) {
        INVARIANT(ret->key_slots_used() > 0);
        INVARIANT(root->is_modifying());
        internal_node *new_root = internal_node::alloc();
#ifdef CHECK_INVARIANTS
        new_root->lock();
        new_root->mark_modifying();
        locked_nodes.push_back(new_root);
#endif /* CHECK_INVARIANTS */
        new_root->children[0] = root;
        new_root->children[1] = ret;
        new_root->keys[0] = mk;
        new_root->set_key_slots_used(1);
        root = new_root;
      }
      UnlockNodes(locked_nodes);
    } catch (const retry_exception &e) {
      UnlockNodes(locked_nodes);
      goto retry;
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
    try {
      remove_status status = remove0(root,
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
      case NONE:
        break;
      case REPLACE_NODE:
        root = replace_node;
        break;
      default:
        ALWAYS_ASSERT(false);
        break;
      }
      UnlockNodes(locked_nodes);
    } catch (const retry_exception &e) {
      UnlockNodes(locked_nodes);
      goto retry;
    }
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

  static inline void
  UnlockNodes(const std::vector<node *> &locked_nodes)
  {
    for (std::vector<node *>::const_iterator it = locked_nodes.begin();
         it != locked_nodes.end(); ++it)
      (*it)->unlock();
  }

  /**
   * insert k=>v into node n. if this insert into n causes it to split into two
   * nodes, return the new node (upper half of keys). in this case, min_key is set to the
   * smallest key that the new node is responsible for. otherwise return null, in which
   * case min_key's value is not defined.
   *
   * NOTE: our implementation of insert0() is not as efficient as possible, in favor
   * of code clarity
   */
  node *
  insert0(node *n,
          key_type k,
          value_type v,
          key_type &min_key,
          std::vector<insert_parent_entry> &parents,
          std::vector<node *> &locked_nodes)
  {
    prefetch_node(n);
    if (leaf_node *leaf = AsLeafCheck(n)) {

      // locked nodes are acquired bottom to top
      INVARIANT(locked_nodes.empty());

      leaf->lock();
      locked_nodes.push_back(leaf);

      key_search_ret kret = leaf->key_lower_bound_search(k);
      ssize_t ret = kret.first;
      if (ret != -1 && leaf->keys[ret] == k) {
        // easy case- we don't modify the node itself
        leaf->values[ret] = v;
        return NULL;
      }

      size_t n = kret.second;
      // ret + 1 is the slot we want the new key to go into, in the leaf node
      if (n < NKeysPerNode) {
        // also easy case- we ony need to make local modifications
        leaf->mark_modifying();

        sift_right(leaf->keys, ret + 1, n);
        leaf->keys[ret + 1] = k;
        sift_right(leaf->values, ret + 1, n);
        leaf->values[ret + 1] = v;
        leaf->inc_key_slots_used();

        return NULL;
      } else {
        INVARIANT(n == NKeysPerNode);

        // we need to split the current node, potentially causing a bunch of
        // splits to happen in ancestors. to make this safe w/o
        // using a very complicated locking protocol, we will first acquire all
        // locks on nodes which will be modified, in left-to-right,
        // bottom-to-top order

        for (std::vector<insert_parent_entry>::reverse_iterator rit = parents.rbegin();
             rit != parents.rend(); ++rit) {
          // lock the parent
          node *p = rit->first;
          p->lock();
          locked_nodes.push_back(p);
          if (!p->check_version(rit->second))
            // in traversing down the tree, an ancestor of this node was
            // modified- to be safe, we start over
            throw retry_exception();

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
        return new_leaf;
      }
    } else {
      internal_node *internal = AsInternal(n);
      uint64_t version = internal->stable_version();
      key_search_ret kret = internal->key_lower_bound_search(k);
      ssize_t ret = kret.first;
      size_t n = kret.second;
      size_t child_idx = (ret == -1) ? 0 : ret + 1;
      node *child_ptr = internal->children[child_idx];
      if (!internal->check_version(version))
        throw retry_exception();
      parents.push_back(insert_parent_entry(internal, version));
      key_type mk = 0;
      node *new_child = insert0(child_ptr, k, v, mk, parents, locked_nodes);
      if (!new_child)
        return NULL;
      INVARIANT(internal->is_locked()); // previous call to insert0() must lock internal node for insertion
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
        return NULL;
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

        return new_internal;
      }
    }
  }

  enum remove_status {
    NONE,
    STOLE_FROM_LEFT,
    STOLE_FROM_RIGHT,
    MERGE_WITH_LEFT,
    MERGE_WITH_RIGHT,
    REPLACE_NODE,
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

      INVARIANT(!left_node || (leaf->prev == left_node && AsLeaf(left_node)->next == leaf));
      INVARIANT(!right_node || (leaf->next == right_node && AsLeaf(right_node)->prev == leaf));

      key_search_ret kret = leaf->key_search(k);
      ssize_t ret = kret.first;
      if (ret == -1)
        return NONE;
      size_t n = kret.second;
      if (n > NMinKeysPerNode) {
        leaf->mark_modifying();
        remove_pos_from_leaf_node(leaf, ret, n);
        return NONE;
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
          if (!leaf->check_version(leaf_version))
            throw retry_exception();
        } else {
          INVARIANT(leaf == root);
          INVARIANT(parents.empty());
        }

        for (std::vector<remove_parent_entry>::reverse_iterator rit = parents.rbegin();
             rit != parents.rend(); ++rit) {
          node *p = rit->parent;
          node *l = rit->parent_left_sibling;
          node *r = rit->parent_right_sibling;
          uint64_t p_version = rit->parent_version;
          p->lock();
          locked_nodes.push_back(p);
          if (!p->check_version(p_version))
            throw retry_exception();
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
            if (!p->check_version(p_version))
              throw retry_exception();
          } else {
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
            return STOLE_FROM_RIGHT;
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

            INVARIANT(!leaf->next || leaf->next->prev == leaf);
            INVARIANT(!leaf->prev || leaf->prev->next == leaf);

            leaf_node::release(right_sibling);
            return MERGE_WITH_RIGHT;
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
            return STOLE_FROM_LEFT;
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

            INVARIANT(!left_sibling->next || left_sibling->next->prev == left_sibling);
            INVARIANT(!left_sibling->prev || left_sibling->prev->next == left_sibling);

            leaf_node::release(leaf);
            return MERGE_WITH_LEFT;
          }
        }

        // root node, so we are ok
        INVARIANT(leaf == root);
        remove_pos_from_leaf_node(leaf, ret, n);
        return NONE;
      }
    } else {
      internal_node *internal = AsInternal(np);
      uint64_t version = internal->stable_version();
      key_search_ret kret = internal->key_lower_bound_search(k);
      ssize_t ret = kret.first;
      size_t n = kret.second;
      size_t child_idx = (ret == -1) ? 0 : ret + 1;
      node *child_ptr = internal->children[child_idx];
      key_type *child_min_key = child_idx == 0 ? NULL : &internal->keys[child_idx - 1];
      key_type *child_max_key = child_idx == n ? NULL : &internal->keys[child_idx];
      node *child_left_sibling = child_idx == 0 ? NULL : internal->children[child_idx - 1];
      node *child_right_sibling = child_idx == n ? NULL : internal->children[child_idx + 1];
      if (!internal->check_version(version))
        throw retry_exception();
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
      case NONE:
        return NONE;

      case STOLE_FROM_LEFT:
        INVARIANT(internal->is_locked());
        internal->keys[child_idx - 1] = nk;
        return NONE;

      case STOLE_FROM_RIGHT:
        INVARIANT(internal->is_locked());
        internal->keys[child_idx] = nk;
        return NONE;

      case MERGE_WITH_LEFT:
      case MERGE_WITH_RIGHT:
      {
        internal->mark_modifying();

        size_t del_key_idx, del_child_idx;
        if (status == MERGE_WITH_LEFT) {
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
          return NONE;
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

            return STOLE_FROM_RIGHT;
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
            return MERGE_WITH_RIGHT;
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

            return STOLE_FROM_LEFT;
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
            return MERGE_WITH_LEFT;
          }
        }

        INVARIANT(internal == root);
        remove_pos_from_internal_node(internal, del_key_idx, del_child_idx, n);
        INVARIANT(internal->key_slots_used() + 1 == n);
        if ((n - 1) == 0) {
          replace_node = internal->children[0];
          internal_node::release(internal);
          return REPLACE_NODE;
        }

        return NONE;
      }

      default:
        ALWAYS_ASSERT(false);
        return NONE;
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

  // induce a split
  btr.insert(btree::NKeysPerNode, (btree::value_type) (btree::NKeysPerNode));
  btr.invariant_checker();

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

  // cause the root node to split
  btr.insert(n, (btree::value_type) n);
  btr.invariant_checker();

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
  const size_t n = 0;
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

  for (size_t i = 0; i < btree::NKeysPerNode * 2; i++) {
    btr.remove(i);
    btr.invariant_checker();

    btree::value_type v = 0;
    ALWAYS_ASSERT(!btr.search(i, v));
  }

  for (size_t i = 0; i < btree::NKeysPerNode * 2; i++) {
    btr.insert(i, (btree::value_type) i);
    btr.invariant_checker();

    btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(i, v));
    ALWAYS_ASSERT(v == (btree::value_type) i);
  }

  for (ssize_t i = btree::NKeysPerNode * 2 - 1; i >= 0; i--) {
    btr.remove(i);
    btr.invariant_checker();

    btree::value_type v = 0;
    ALWAYS_ASSERT(!btr.search(i, v));
  }

  for (size_t i = 0; i < btree::NKeysPerNode * 2; i++) {
    btr.insert(i, (btree::value_type) i);
    btr.invariant_checker();

    btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(i, v));
    ALWAYS_ASSERT(v == (btree::value_type) i);
  }

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
}

static void
test4()
{
  btree btr;
  for (size_t i = 0; i < 10000; i++) {
    btr.insert(i, (btree::value_type) i);
    btr.invariant_checker();
    btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(i, v));
    ALWAYS_ASSERT(v == (btree::value_type) i);
  }

  srand(12345);

  for (size_t i = 0; i < 10000; i++) {
    size_t k = rand() % 10000;
    btr.remove(k);
    btr.invariant_checker();
    btree::value_type v = 0;
    ALWAYS_ASSERT(!btr.search(k, v));
  }

  for (size_t i = 0; i < 10000; i++) {
    btr.remove(i);
    btr.invariant_checker();
    btree::value_type v = 0;
    ALWAYS_ASSERT(!btr.search(i, v));
  }
}

static void
test5()
{
  // insert in random order, delete in random order
  btree btr;

  unsigned int seeds[] = {
    54321, 2013883780, 3028985725, 3058602342, 256561598, 2895653051
  };

#define ARRAY_NELEMS(a) (sizeof(a)/sizeof(a[0]))
  for (size_t iter = 0; iter < ARRAY_NELEMS(seeds); iter++) {
    srand(seeds[iter]);
    const size_t nkeys = 20000;
    for (size_t i = 0; i < nkeys; i++) {
      size_t k = rand() % nkeys;
      btr.insert(k, (btree::value_type) k);
      btr.invariant_checker();
      btree::value_type v = 0;
      ALWAYS_ASSERT(btr.search(k, v));
      ALWAYS_ASSERT(v == (btree::value_type) k);
    }

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
  }
}

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

int
main(void)
{
  test1();
  test2();
  test3();
  test4();
  test5();
  //perf_test();
  return 0;
}
