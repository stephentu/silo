
#include <assert.h>
#include <stdint.h>
#include <unistd.h>

#include <cstddef>

#include "static_assert.h"

#define PACKED_CACHE_ALIGNED __attribute__((packed, aligned(64)))

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
      assert(n <= NKeysPerNode);
      hdr &= ~HDR_KEY_SLOTS_MASK;
      hdr |= (n << 1);
    }

    inline void
    inc_key_slots_used()
    {
      assert(key_slots_used() < NKeysPerNode);
      set_key_slots_used(key_slots_used() + 1);
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

    /**
     * tightest lower bound key, -1 if no such key exists
     */
    inline ssize_t
    key_lower_bound_search(key_type k) const
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
      assert(n <= NKeysPerNode);
      if (is_root) {
        if (is_internal_node())
          assert(n >= 2);
      } else {
        assert(n >= NKeysPerNode / 2);
      }
      if (n == 0)
        return;
      for (size_t i = 1; i < n; i++) {
        if (min_key && keys[i] < *min_key)
          assert(false);
        if (max_key && keys[i] >= *max_key)
          assert(false);
      }
      key_type prev = keys[0];
      for (size_t i = 1; i < n; i++) {
        if (keys[i] <= prev)
          assert(false);
        prev = keys[i];
      }
    }

    /** manually simulated virtual function */
    void
    invariant_checker(
        const key_type *min_key,
        const key_type *max_key,
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
        bool is_root) const
    {
      base_invariant_checker(min_key, max_key, is_root);
    }

  } PACKED_CACHE_ALIGNED;

  struct internal_node : public node {
    node *children[NKeysPerNode + 1];
    internal_node()
    {
      hdr = 1;
    }

    void
    invariant_checker_impl(
        const key_type *min_key,
        const key_type *max_key,
        bool is_root) const
    {
      base_invariant_checker(min_key, max_key, is_root);
      size_t n = key_slots_used();
      for (size_t i = 0; i <= n; i++) {
        assert(children[i]);
        if (i == 0) {
          children[0]->invariant_checker(min_key, &keys[0], false);
        } else if (i == n) {
          children[n]->invariant_checker(&keys[n], max_key, false);
        } else {
          children[i]->invariant_checker(&keys[i - 1], &keys[i], false);
        }
      }
    }

  } PACKED_CACHE_ALIGNED;

  static inline leaf_node*
  AsLeaf(node *n)
  {
    assert(n->is_leaf_node());
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
    assert(n->is_internal_node());
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
    return n->is_leaf_node() ? static_cast<leaf_node *>(n) : NULL;
  }

  static inline const leaf_node*
  AsLeafCheck(const node *n)
  {
    return AsLeafCheck(const_cast<node *>(n));
  }

  static inline internal_node*
  AsInternalCheck(node *n)
  {
    return n->is_internal_node() ? static_cast<internal_node *>(n) : NULL;
  }

  static inline const internal_node*
  AsInternalCheck(const node *n)
  {
    return AsInternalCheck(const_cast<node *>(n));
  }

  node *root;

public:

  btree() : root(new leaf_node)
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
    root->invariant_checker(NULL, NULL, true);
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
      assert(ret->key_slots_used());
      internal_node *new_root = new internal_node;
      new_root->children[0] = root;
      new_root->children[1] = ret;
      new_root->keys[0] = mk;
      new_root->set_key_slots_used(1);
      root = new_root;
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
      assert(type == 0);
      return v.value;
    }
    inline node *
    as_ptr() const
    {
      assert(type == 1);
      return v.ptr;
    }
  };

  /**
   * insert k=>v into node n. if this insert into n causes it to split into two
   * nodes, return the new node (upper half of keys). in this case, min_key is set to the
   * smallest key that the new node is responsible for. otherwise return null, in which
   * case min_key's value is not defined
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
        for (size_t i = leaf->key_slots_used(); i > size_t(ret) + 1; i--) {
          leaf->keys[i] = leaf->keys[i - 1];
          leaf->values[i] = leaf->values[i - 1];
        }
        leaf->keys[ret + 1] = k;
        leaf->values[ret + 1] = v.as_value();
        leaf->inc_key_slots_used();
        return NULL;
      } else {
        leaf_node *new_leaf = new leaf_node;
        for (size_t i = NKeysPerNode / 2, j = 0; i < NKeysPerNode; i++, j++) {
          new_leaf->keys[j] = leaf->keys[i];
          new_leaf->values[j] = leaf->values[i];
        }
        new_leaf->set_key_slots_used(NKeysPerNode - (NKeysPerNode / 2));
        new_leaf->prev = leaf;
        new_leaf->next = leaf->next;

        leaf->next = new_leaf;
        leaf->set_key_slots_used(NKeysPerNode / 2);

        if (k >= new_leaf->keys[0]) {
          key_type mk;
          node *ret = insert0(new_leaf, k, v, mk);
          if (ret)
            assert(false);
        } else {
          key_type mk;
          node *ret = insert0(n, k, v, mk);
          if (ret)
            assert(false);
        }

        min_key = new_leaf->keys[0];
        return new_leaf;
      }
    } else {
      internal_node *internal = AsInternal(n);
      assert(internal->key_slots_used());
      ssize_t ret = internal->key_lower_bound_search(k);
      size_t child_idx = (ret == -1) ? 0 : ret;
      key_type mk;
      node *new_child =
        v.is_value() ?
          insert0(internal->children[child_idx], k, v, mk) :
          v.as_ptr();
      if (!new_child)
        return NULL;
      assert(new_child->key_slots_used());
      if (internal->key_slots_used() < NKeysPerNode) {
        for (size_t i = internal->key_slots_used(); i > (child_idx + 1) + 1; i--)
          internal->keys[i] = internal->keys[i - 1];
        for (size_t i = internal->key_slots_used() + 1; i > child_idx + 1; i--)
          internal->children[i] = internal->children[i - 1];
        internal->keys[child_idx] = mk;
        internal->children[child_idx + 1] = new_child;
        internal->inc_key_slots_used();
        return NULL;
      } else {
        internal_node *new_internal = new internal_node;
        min_key = internal->keys[NKeysPerNode / 2];
        for (size_t i = NKeysPerNode / 2 + 1, j = 0; i < NKeysPerNode; i++, j++)
          new_internal->keys[j] = internal->keys[i];
        for (size_t i = NKeysPerNode / 2 + 1, j = 0; i < NKeysPerNode + 1; i++, j++)
          new_internal->children[j] = internal->children[i];
        new_internal->set_key_slots_used(NKeysPerNode - (NKeysPerNode / 2));
        internal->set_key_slots_used(NKeysPerNode / 2);
        if (mk >= min_key) {
          key_type mk0;
          node *ret = insert0(new_internal, k, insert_value(new_child), mk0);
          if (ret)
            assert(false);
        } else {
          key_type mk0;
          node *ret = insert0(internal, k, insert_value(new_child), mk0);
          if (ret)
            assert(false);
        }
        return new_internal;
      }
    }
  }
};

void
btree::node::invariant_checker(
    const key_type *min_key,
    const key_type *max_key,
    bool is_root) const
{
  is_leaf_node() ?
    AsLeaf(this)->invariant_checker_impl(min_key, max_key, is_root) :
    AsInternal(this)->invariant_checker_impl(min_key, max_key, is_root) ;
}


int
main(void)
{
  btree btr;
  btr.invariant_checker();

  for (size_t i = 0; i < btree::NKeysPerNode; i++) {
    btr.insert(i, (btree::value_type) i);
    btr.invariant_checker();

    btree::value_type v;
    assert(btr.search(i, v));
    assert(v == (btree::value_type) i);
  }

  return 0;
}
