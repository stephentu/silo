
#include <assert.h>
#include <stdint.h>
#include <unistd.h>
#include <malloc.h>
#include <stdio.h>

#include <map>
#include <cstddef>
#include <cstdlib>
#include <iostream>

#include "static_assert.h"
#include "util.h"

#define CACHELINE_SIZE 64
#define PACKED_CACHE_ALIGNED __attribute__((packed, aligned(CACHELINE_SIZE)))
#define NEVER_INLINE __attribute__((noinline))

#define likely(x)   __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

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

private:

  static const unsigned int NodeSize = 256; /* 4 x86 cache lines */

  static const uint64_t HDR_TYPE_MASK = 0x1;

  // 0xf = (1 << ceil(log2(NKeysPerNode))) - 1
  static const uint64_t HDR_KEY_SLOTS_MASK = (0xf) << 1;

  struct node {
    /**
     * hdr bits: layout is:
     *
     * <-- low bits
     * [type | key_slots | unused]
     * [0:1  | 1:5       | 5:64  ]
     */
    uint64_t hdr;

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
      return (hdr & HDR_KEY_SLOTS_MASK) >> 1;
    }

    inline void
    set_key_slots_used(size_t n)
    {
      INVARIANT(n <= NKeysPerNode);
      hdr &= ~HDR_KEY_SLOTS_MASK;
      hdr |= (n << 1);
    }

    inline void
    inc_key_slots_used()
    {
      INVARIANT(key_slots_used() < NKeysPerNode);
      set_key_slots_used(key_slots_used() + 1);
    }

    inline void
    dec_key_slots_used()
    {
      INVARIANT(key_slots_used() > 0);
      set_key_slots_used(key_slots_used() - 1);
    }

    /**
     * keys[key_search(k)] == k if key_search(k) != -1
     * key does not exist otherwise
     */
    inline ssize_t
    key_search(key_type k) const
    {
      ssize_t lower = 0;
      ssize_t upper = key_slots_used();
      while (lower < upper) {
        ssize_t i = (lower + upper) / 2;
        key_type k0 = keys[i];
        if (k0 == k)
          return i;
        else if (k0 > k)
          upper = i;
        else
          lower = i + 1;
      }
      return -1;
    }

    NEVER_INLINE std::string
    key_list_string() const
    {
      return util::format_list(keys, keys + key_slots_used());
    }

    /**
     * tightest lower bound key, -1 if no such key exists
     */
    inline ssize_t
    key_lower_bound_search(key_type k) const
    {
      ssize_t ret = key_lower_bound_search0(k);
      //std::cout << "key_lower_bound_search(" << k << "):" << std::endl
      //          << "  keys: " << util::format_list(keys, keys + key_slots_used()) << std::endl
      //          << "  ret: " << ret << std::endl
      //          ;
      return ret;
    }

    inline ssize_t
    key_lower_bound_search0(key_type k) const
    {
      ssize_t ret = -1;
      ssize_t lower = 0;
      ssize_t upper = key_slots_used();
      while (lower < upper) {
        ssize_t i = (lower + upper) / 2;
        key_type k0 = keys[i];
        if (k0 == k)
          return i;
        else if (k0 > k)
          upper = i;
        else {
          ret = i;
          lower = i + 1;
        }
      }
      return ret;
    }

    // [min_key, max_key)
    void
    base_invariant_checker(
        const key_type *min_key,
        const key_type *max_key,
        bool is_root) const
    {
      size_t n = key_slots_used();
      ALWAYS_ASSERT(n <= NKeysPerNode);
      if (is_root) {
        if (is_internal_node())
          ALWAYS_ASSERT(n >= 1);
      } else {
        ALWAYS_ASSERT(n >= NKeysPerNode / 2);
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
      n->~leaf_node();
      free(n);
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
      n->~internal_node();
      free(n);
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
    node *cur = root;
    while (cur) {
      if (leaf_node *leaf = AsLeafCheck(cur)) {
        ssize_t ret = leaf->key_search(k);
        if (ret != -1) {
          v = leaf->values[ret];
          return true;
        } else {
          return false;
        }
      } else {
        internal_node *internal = AsInternal(cur);
        ssize_t ret = internal->key_lower_bound_search(k);
        if (ret != -1)
          cur = internal->children[ret + 1];
        else
          cur = internal->key_slots_used() ? internal->children[0] : NULL;
      }
    }
    return false;
  }

  void
  insert(key_type k, value_type v)
  {
    key_type mk;
    node *ret = insert0(root, k, insert_value(v), mk);
    if (ret) {
      INVARIANT(ret->key_slots_used() > 0);
      internal_node *new_root = internal_node::alloc();
      new_root->children[0] = root;
      new_root->children[1] = ret;
      new_root->keys[0] = mk;
      new_root->set_key_slots_used(1);
      root = new_root;
    }
  }

  void
  remove(key_type k)
  {
    key_type new_key;
    node *replace_node;
    remove_status status = remove0(
        root,
        NULL, /* min_key */
        NULL, /* max_key */
        k,
        NULL, /* left_node */
        NULL, /* right_node */
        new_key,
        replace_node);
    switch (status) {
    case NONE:
      return;
    case REPLACE_NODE:
      root = replace_node;
      return;
    default:
      ALWAYS_ASSERT(false);
    }
  }

private:

  struct insert_value {
    uint8_t type;
    union {
      value_type value;
      node *ptr;
    } v;
    insert_value(value_type value)
    {
      type = 0;
      v.value = value;
    }
    insert_value(node *ptr)
    {
      type = 1;
      v.ptr = ptr;
    }
    inline bool
    is_value() const
    {
      return type == 0;
    }
    inline bool
    is_ptr() const
    {
      return !is_value();
    }
    inline value_type
    as_value() const
    {
      INVARIANT(type == 0);
      return v.value;
    }
    inline node *
    as_ptr() const
    {
      INVARIANT(type == 1);
      return v.ptr;
    }
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
  node *
  insert0(node *n, key_type k, const insert_value &v, key_type &min_key)
  {
    if (leaf_node *leaf = AsLeafCheck(n)) {
      ssize_t ret = leaf->key_search(k);
      if (ret != -1) {
        leaf->values[ret] = v.as_value();
        return NULL;
      }
      if (leaf->key_slots_used() < NKeysPerNode) {
        ret = leaf->key_lower_bound_search(k);
        // ret + 1 is the slot we want the new key to go into
        for (size_t i = leaf->key_slots_used(); i > size_t(ret + 1); i--) {
          leaf->keys[i] = leaf->keys[i - 1];
          leaf->values[i] = leaf->values[i - 1];
        }
        leaf->keys[ret + 1] = k;
        leaf->values[ret + 1] = v.as_value();
        leaf->inc_key_slots_used();
        return NULL;
      } else {
        leaf_node *new_leaf = leaf_node::alloc();
        for (size_t i = NKeysPerNode / 2, j = 0; i < NKeysPerNode; i++, j++) {
          new_leaf->keys[j] = leaf->keys[i];
          new_leaf->values[j] = leaf->values[i];
        }
        new_leaf->set_key_slots_used(NKeysPerNode - (NKeysPerNode / 2));
        new_leaf->prev = leaf;
        new_leaf->next = leaf->next;
        if (leaf->next)
          leaf->next->prev = new_leaf;

        leaf->next = new_leaf;
        leaf->set_key_slots_used(NKeysPerNode / 2);

        if (k >= new_leaf->keys[0]) {
          key_type mk;
          node *ret = insert0(new_leaf, k, v, mk);
          if (ret)
            INVARIANT(false);
        } else {
          key_type mk;
          node *ret = insert0(leaf, k, v, mk);
          if (ret)
            INVARIANT(false);
        }

        min_key = new_leaf->keys[0];
        return new_leaf;
      }
    } else {
      internal_node *internal = AsInternal(n);
      INVARIANT(internal->key_slots_used() > 0);
      ssize_t ret = internal->key_lower_bound_search(k);
      size_t child_idx = (ret == -1) ? 0 : ret + 1;
      key_type mk = 0;
      node *new_child =
        v.is_value() ?
          insert0(internal->children[child_idx], k, v, mk) :
          v.as_ptr();
      if (!new_child)
        return NULL;
      INVARIANT(new_child->key_slots_used() > 0);
      if (internal->key_slots_used() < NKeysPerNode) {
        for (size_t i = internal->key_slots_used(); i > child_idx; i--)
          internal->keys[i] = internal->keys[i - 1];
        for (size_t i = internal->key_slots_used() + 1; i > child_idx + 1; i--)
          internal->children[i] = internal->children[i - 1];
        internal->keys[child_idx] = v.is_value() ? mk : k;
        internal->children[child_idx + 1] = new_child;
        internal->inc_key_slots_used();
        return NULL;
      } else {
        internal_node *new_internal = internal_node::alloc();

        // find where we *would* put the new key (mk) if we could
        ssize_t ret = internal->key_lower_bound_search(mk);

        // there are three cases post-split:
        // (1) mk goes in the original node
        // (2) mk is the key we push up
        // (3) mk goes in the new node

        const ssize_t split_point = NKeysPerNode / 2 - 1;
        if (ret < split_point) {
          // case (1)
          min_key = internal->keys[split_point];

          // copy keys at positions [NKeysPerNode/2, NKeysPerNode) over to
          // the new node starting at position 0
          for (size_t i = NKeysPerNode / 2, j = 0; i < NKeysPerNode; i++, j++)
            new_internal->keys[j] = internal->keys[i];

          // copy children at positions [NKeysPerNode/2, NKeysPerNode + 1)
          // over to the new node starting at position 0
          for (size_t i = NKeysPerNode / 2, j = 0; i < NKeysPerNode + 1; i++, j++)
            new_internal->children[j] = internal->children[i];

          new_internal->set_key_slots_used(NKeysPerNode - (NKeysPerNode / 2));
          internal->set_key_slots_used(NKeysPerNode / 2 - 1);

          key_type mk0;
          node *ret0 = insert0(internal, mk, insert_value(new_child), mk0);
          if (ret0)
            INVARIANT(false);
          INVARIANT(internal->key_slots_used() == (NKeysPerNode / 2));
        } else if (ret == split_point) {
          // case (2)
          min_key = mk;

          // copy keys at positions [NKeysPerNode/2, NKeysPerNode) over to
          // the new node starting at position 0
          for (size_t i = NKeysPerNode / 2, j = 0; i < NKeysPerNode; i++, j++)
            new_internal->keys[j] = internal->keys[i];

          // copy children at positions [NKeysPerNode/2 + 1, NKeysPerNode + 1)
          // over to the new node starting at position 1
          for (size_t i = NKeysPerNode / 2 + 1, j = 1; i < NKeysPerNode + 1; i++, j++)
            new_internal->children[j] = internal->children[i];

          new_internal->children[0] = new_child;

          new_internal->set_key_slots_used(NKeysPerNode - (NKeysPerNode / 2));
          internal->set_key_slots_used(NKeysPerNode / 2);

        } else {
          // case (3)
          min_key = internal->keys[NKeysPerNode / 2];

          // copy keys at positions [NKeysPerNode/2 + 1, NKeysPerNode) over to
          // the new node starting at position 0
          for (size_t i = NKeysPerNode / 2 + 1, j = 0; i < NKeysPerNode; i++, j++)
            new_internal->keys[j] = internal->keys[i];

          // copy children at positions [NKeysPerNode/2 + 1, NKeysPerNode + 1)
          // over to the new node starting at position 0
          for (size_t i = NKeysPerNode / 2 + 1, j = 0; i < NKeysPerNode + 1; i++, j++)
            new_internal->children[j] = internal->children[i];

          new_internal->set_key_slots_used(NKeysPerNode - (NKeysPerNode / 2) - 1);
          internal->set_key_slots_used(NKeysPerNode / 2);

          key_type mk0;
          node *ret0 = insert0(new_internal, mk, insert_value(new_child), mk0);
          if (ret0)
            INVARIANT(false);
          INVARIANT(new_internal->key_slots_used() == (NKeysPerNode - (NKeysPerNode / 2)));
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

  inline void
  remove_pos_from_leaf_node(leaf_node *leaf, size_t pos, size_t n)
  {
    INVARIANT(leaf->key_slots_used() == n);
    INVARIANT(pos < n);
    for (size_t i = pos; i < n - 1; i++) {
      leaf->keys[i] = leaf->keys[i + 1];
      leaf->values[i] = leaf->values[i + 1];
    }
    leaf->dec_key_slots_used();
  }

  inline void
  remove_pos_from_internal_node(
      internal_node *internal, size_t key_pos, size_t child_pos, size_t n)
  {
    INVARIANT(internal->key_slots_used() == n);
    INVARIANT(key_pos < n);
    INVARIANT(child_pos < n + 1);
    for (size_t i = key_pos; i < n - 1; i++)
      internal->keys[i] = internal->keys[i + 1];
    for (size_t i = child_pos; i < n; i++)
      internal->children[i] = internal->children[i + 1];
    internal->dec_key_slots_used();
  }

  /**
   * remove is very tricky to get right!
   *
   * XXX: g++ complains if we don't say btree::node here, for some reason...
   */
  remove_status
  remove0(
      btree::node *node,
      key_type *min_key,
      key_type *max_key,

      key_type k,

      btree::node *left_node,
      btree::node *right_node,

      key_type &new_key,
      btree::node *&replace_node)
  {
    if (leaf_node *leaf = AsLeafCheck(node)) {
      INVARIANT(!left_node || (leaf->prev == left_node && AsLeaf(left_node)->next == leaf));
      INVARIANT(!right_node || (leaf->next == right_node && AsLeaf(right_node)->prev == leaf));
      ssize_t ret = leaf->key_search(k);
      if (ret == -1)
        return NONE;
      size_t n = leaf->key_slots_used();
      if (n > NKeysPerNode / 2) {
        remove_pos_from_leaf_node(leaf, ret, n);
        return NONE;
      } else {
        leaf_node *left_sibling = AsLeaf(left_node);
        leaf_node *right_sibling = AsLeaf(right_node);
        if (left_sibling) {
          size_t left_n = left_sibling->key_slots_used();
          if (left_n > NKeysPerNode / 2) {
            // steal last from left
            INVARIANT(left_sibling->keys[left_n - 1] < leaf->keys[0]);
            for (size_t i = ret; i > 0; i--) {
              leaf->keys[i] = leaf->keys[i - 1];
              leaf->values[i] = leaf->values[i - 1];
            }
            leaf->keys[0] = left_sibling->keys[left_n - 1];
            leaf->values[0] = left_sibling->values[left_n - 1];
            left_sibling->dec_key_slots_used();
            new_key = leaf->keys[0];
            return STOLE_FROM_LEFT;
          }
        }

        if (right_sibling) {
          size_t right_n = right_sibling->key_slots_used();
          if (right_n > NKeysPerNode / 2) {
            // steal first from right
            INVARIANT(right_sibling->keys[0] > leaf->keys[n - 1]);
            for (size_t i = ret; i < n - 1; i++) {
              leaf->keys[i] = leaf->keys[i + 1];
              leaf->values[i] = leaf->values[i + 1];
            }
            leaf->keys[n - 1] = right_sibling->keys[0];
            leaf->values[n - 1] = right_sibling->values[0];
            for (size_t i = 0; i < right_n; i++) {
              right_sibling->keys[i] = right_sibling->keys[i + 1];
              right_sibling->values[i] = right_sibling->values[i + 1];
            }
            right_sibling->dec_key_slots_used();
            new_key = right_sibling->keys[0];
            return STOLE_FROM_RIGHT;
          }
        }

        if (left_sibling) {
          // merge this node into left sibling
          size_t left_n = left_sibling->key_slots_used();
          INVARIANT(left_sibling->keys[left_n - 1] < leaf->keys[0]);
          INVARIANT((left_n + (n - 1)) <= NKeysPerNode);

          size_t j = left_n;
          for (size_t i = 0; i < size_t(ret); i++, j++) {
            left_sibling->keys[j] = leaf->keys[i];
            left_sibling->values[j] = leaf->values[i];
          }
          for (size_t i = ret + 1; i < n; i++, j++) {
            left_sibling->keys[j] = leaf->keys[i];
            left_sibling->values[j] = leaf->values[i];
          }

          left_sibling->set_key_slots_used(left_n + (n - 1));
          left_sibling->next = leaf->next;
          if (leaf->next)
            leaf->next->prev = left_sibling;

          INVARIANT(!left_sibling->next || left_sibling->next->prev == left_sibling);
          INVARIANT(!left_sibling->prev || left_sibling->prev->next == left_sibling);

          leaf_node::release(leaf);
          return MERGE_WITH_LEFT;
        }

        if (right_sibling) {
          // merge right sibling into this node
          size_t right_n = right_sibling->key_slots_used();
          INVARIANT(right_sibling->keys[0] > leaf->keys[n - 1]);
          INVARIANT((right_n + (n - 1)) <= NKeysPerNode);

          for (size_t i = ret; i < n - 1; i++) {
            leaf->keys[i] = leaf->keys[i + 1];
            leaf->values[i] = leaf->values[i + 1];
          }
          for (size_t i = 0, j = n - 1; i < right_n; i++, j++) {
            leaf->keys[j] = right_sibling->keys[i];
            leaf->values[j] = right_sibling->values[i];
          }
          leaf->set_key_slots_used(right_n + (n - 1));
          leaf->next = right_sibling->next;
          if (right_sibling->next)
            right_sibling->next->prev = leaf;

          INVARIANT(!leaf->next || leaf->next->prev == leaf);
          INVARIANT(!leaf->prev || leaf->prev->next == leaf);

          leaf_node::release(right_sibling);
          return MERGE_WITH_RIGHT;
        }

        // root node, so we are ok
        INVARIANT(leaf == root);
        remove_pos_from_leaf_node(leaf, ret, n);
        return NONE;
      }
    } else {
      internal_node *internal = AsInternal(node);
      size_t n = internal->key_slots_used();
      INVARIANT(n);
      ssize_t ret = internal->key_lower_bound_search(k);
      size_t child_idx = (ret == -1) ? 0 : ret + 1;
      key_type nk;
      btree::node *rn;
      remove_status status = remove0(
          internal->children[child_idx],
          child_idx == 0 ? NULL : &internal->keys[child_idx - 1],
          child_idx == n ? NULL : &internal->keys[child_idx],
          k,
          child_idx == 0 ? NULL : internal->children[child_idx - 1],
          child_idx == n ? NULL : internal->children[child_idx + 1],
          nk, rn);

      switch (status) {
      case NONE:
        return NONE;

      case STOLE_FROM_LEFT:
        internal->keys[child_idx - 1] = nk;
        return NONE;

      case STOLE_FROM_RIGHT:
        internal->keys[child_idx] = nk;
        return NONE;

      case MERGE_WITH_LEFT:
      case MERGE_WITH_RIGHT:
      {

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

        if (n > NKeysPerNode / 2) {
          remove_pos_from_internal_node(internal, del_key_idx, del_child_idx, n);
          return NONE;
        }

        internal_node *left_sibling = AsInternal(left_node);
        internal_node *right_sibling = AsInternal(right_node);
        size_t left_n = 0;
        size_t right_n = 0;

        if (left_sibling) {
          left_n = left_sibling->key_slots_used();
          INVARIANT(min_key);
          INVARIANT(left_sibling->keys[left_n - 1] < internal->keys[0]);
          INVARIANT(left_sibling->keys[left_n - 1] < *min_key);
          INVARIANT(*min_key < internal->keys[0]);
          if (left_n > NKeysPerNode / 2) {
            // sift keys to right
            for (size_t i = del_key_idx; i > 0; i--)
              internal->keys[i] = internal->keys[i - 1];
            // sift children to right
            for (size_t i = del_child_idx; i > 0; i--)
              internal->children[i] = internal->children[i - 1];

            internal->keys[0] = *min_key;
            internal->children[0] = left_sibling->children[left_n];
            new_key = left_sibling->keys[left_n - 1];
            left_sibling->dec_key_slots_used();

            return STOLE_FROM_LEFT;
          }
        }

        if (right_sibling) {
          INVARIANT(max_key);
          right_n = right_sibling->key_slots_used();
          INVARIANT(right_sibling->keys[0] > internal->keys[n - 1]);
          INVARIANT(*max_key > internal->keys[n - 1]);
          if (right_n > NKeysPerNode / 2) {
            // sift keys to left
            for (size_t i = del_key_idx; i < n - 1; i++)
              internal->keys[i] = internal->keys[i + 1];
            // sift children to left
            for (size_t i = del_child_idx; i < n; i++)
              internal->children[i] = internal->children[i + 1];

            internal->keys[n - 1] = *max_key;
            internal->children[n] = right_sibling->children[0];
            new_key = right_sibling->keys[0];

            // sift keys to left
            for (size_t i = 0; i < right_n - 1; i++)
              right_sibling->keys[i] = right_sibling->keys[i + 1];
            // sift children to left
            for (size_t i = 0; i < right_n ; i++)
              right_sibling->children[i] = right_sibling->children[i + 1];
            right_sibling->dec_key_slots_used();

            return STOLE_FROM_RIGHT;
          }
        }

        if (left_sibling) {
          // merge into left sibling
          INVARIANT(min_key);

          size_t left_key_j = left_n;
          size_t left_child_j = left_n + 1;

          left_sibling->keys[left_key_j++] = *min_key;
          for (size_t i = 0; i < del_key_idx; i++, left_key_j++)
            left_sibling->keys[left_key_j] = internal->keys[i];
          for (size_t i = del_key_idx + 1; i < n; i++, left_key_j++)
            left_sibling->keys[left_key_j] = internal->keys[i];
          for (size_t i = 0; i < del_child_idx; i++, left_child_j++)
            left_sibling->children[left_child_j] = internal->children[i];
          for (size_t i = del_child_idx + 1; i < n + 1; i++, left_child_j++)
            left_sibling->children[left_child_j] = internal->children[i];

          INVARIANT(left_key_j == n + left_n);
          left_sibling->set_key_slots_used(left_key_j);
          internal_node::release(internal);
          return MERGE_WITH_LEFT;
        }

        if (right_sibling) {
          // merge with right
          INVARIANT(max_key);

          // sift keys left
          for (size_t i = del_key_idx; i < n - 1; i++)
            internal->keys[i] = internal->keys[i + 1];
          // sift children left
          for (size_t i = del_child_idx; i < n; i++)
            internal->children[i] = internal->children[i + 1];

          internal->keys[n - 1] = *max_key;
          for (size_t i = 0, j = n; i < right_n; i++, j++)
            internal->keys[j] = right_sibling->keys[i];
          for (size_t i = 0, j = n; i < right_n + 1; i++, j++)
            internal->children[j] = right_sibling->children[i];

          internal->set_key_slots_used(n + right_n);
          internal_node::release(right_sibling);
          return MERGE_WITH_RIGHT;
        }

        INVARIANT(internal == root);
        remove_pos_from_internal_node(internal, del_key_idx, del_child_idx, n);
        if (internal->key_slots_used() == 0) {
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

    btree::value_type v;
    ALWAYS_ASSERT(btr.search(i, v));
    ALWAYS_ASSERT(v == (btree::value_type) i);
  }

  // induce a split
  btr.insert(btree::NKeysPerNode, (btree::value_type) (btree::NKeysPerNode));
  btr.invariant_checker();

  // now make sure we can find everything post split
  for (size_t i = 0; i < btree::NKeysPerNode + 1; i++) {
    btree::value_type v;
    ALWAYS_ASSERT(btr.search(i, v));
    ALWAYS_ASSERT(v == (btree::value_type) i);
  }

  // now fill up the new root node
  const size_t n = (btree::NKeysPerNode + btree::NKeysPerNode * (btree::NKeysPerNode / 2));
  for (size_t i = btree::NKeysPerNode + 1; i < n; i++) {
    btr.insert(i, (btree::value_type) i);
    btr.invariant_checker();

    btree::value_type v;
    ALWAYS_ASSERT(btr.search(i, v));
    ALWAYS_ASSERT(v == (btree::value_type) i);
  }

  // cause the root node to split
  btr.insert(n, (btree::value_type) n);
  btr.invariant_checker();

  // once again make sure we can find everything
  for (size_t i = 0; i < n + 1; i++) {
    btree::value_type v;
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

    btree::value_type v;
    ALWAYS_ASSERT(btr.search(i, v));
    ALWAYS_ASSERT(v == (btree::value_type) i);
  }

  for (size_t i = 1; i < n; i += 2) {
    btr.insert(i, (btree::value_type) i);
    btr.invariant_checker();

    btree::value_type v;
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

    btree::value_type v;
    ALWAYS_ASSERT(btr.search(i, v));
    ALWAYS_ASSERT(v == (btree::value_type) i);
  }

  for (size_t i = 0; i < btree::NKeysPerNode * 2; i++) {
    btr.remove(i);
    btr.invariant_checker();

    btree::value_type v;
    ALWAYS_ASSERT(!btr.search(i, v));
  }

  for (size_t i = 0; i < btree::NKeysPerNode * 2; i++) {
    btr.insert(i, (btree::value_type) i);
    btr.invariant_checker();

    btree::value_type v;
    ALWAYS_ASSERT(btr.search(i, v));
    ALWAYS_ASSERT(v == (btree::value_type) i);
  }

  for (ssize_t i = btree::NKeysPerNode * 2 - 1; i >= 0; i--) {
    btr.remove(i);
    btr.invariant_checker();

    btree::value_type v;
    ALWAYS_ASSERT(!btr.search(i, v));
  }

  for (size_t i = 0; i < btree::NKeysPerNode * 2; i++) {
    btr.insert(i, (btree::value_type) i);
    btr.invariant_checker();

    btree::value_type v;
    ALWAYS_ASSERT(btr.search(i, v));
    ALWAYS_ASSERT(v == (btree::value_type) i);
  }

  for (ssize_t i = btree::NKeysPerNode; i >= 0; i--) {
    btr.remove(i);
    btr.invariant_checker();

    btree::value_type v;
    ALWAYS_ASSERT(!btr.search(i, v));
  }

  for (size_t i = btree::NKeysPerNode + 1; i < btree::NKeysPerNode * 2; i++) {
    btr.remove(i);
    btr.invariant_checker();

    btree::value_type v;
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
    btree::value_type v;
    ALWAYS_ASSERT(btr.search(i, v));
    ALWAYS_ASSERT(v == (btree::value_type) i);
  }

  srand(12345);

  for (size_t i = 0; i < 10000; i++) {
    size_t k = rand() % 10000;
    btr.remove(k);
    btr.invariant_checker();
    btree::value_type v;
    ALWAYS_ASSERT(!btr.search(k, v));
  }

  for (size_t i = 0; i < 10000; i++) {
    btr.remove(i);
    btr.invariant_checker();
    btree::value_type v;
    ALWAYS_ASSERT(!btr.search(i, v));
  }
}

static void
test5()
{
  // insert in random order, delete in random order
  btree btr;

  srand(54321);

  for (size_t i = 0; i < 10000; i++) {
    size_t k = rand() % 10000;
    btr.insert(k, (btree::value_type) k);
    btr.invariant_checker();
    btree::value_type v;
    ALWAYS_ASSERT(btr.search(k, v));
    ALWAYS_ASSERT(v == (btree::value_type) k);
  }

  for (size_t i = 0; i < 10000; i++) {
    size_t k = rand() % 10000;
    btr.remove(k);
    btr.invariant_checker();
    btree::value_type v;
    ALWAYS_ASSERT(!btr.search(k, v));
  }

  for (size_t i = 0; i < 10000; i++) {
    btr.remove(i);
    btr.invariant_checker();
    btree::value_type v;
    ALWAYS_ASSERT(!btr.search(i, v));
  }
}

static void
perf_test()
{
  const size_t nrecs = 10000000;

  {
    std::map<uint64_t, uint64_t> m;
    {
      util::scoped_timer t("std::map insert");
      for (size_t i = 0; i < nrecs; i++)
        m[i] = i;
    }
  }

  {
    btree btr;
    {
      util::scoped_timer t("btree insert");
      for (size_t i = 0; i < nrecs; i++)
        btr.insert(i, (btree::value_type) i);
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
