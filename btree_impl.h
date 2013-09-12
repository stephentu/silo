#pragma once

#include <unistd.h>

#include <iostream>
#include <map>
#include <set>
#include <stack>
#include <vector>
#include <sstream>
#include <atomic>
#include <memory>

#include "core.h"
#include "btree.h"
#include "thread.h"
#include "txn.h"
#include "util.h"
#include "scopedperf.hh"

template <typename P>
void
btree<P>::node::base_invariant_unique_keys_check() const
{
  size_t n = this->key_slots_used();
  if (n == 0)
    return;
  if (is_leaf_node()) {
    const leaf_node *leaf = AsLeaf(this);
    typedef std::pair<key_slice, size_t> leaf_key;
    leaf_key prev;
    prev.first = keys_[0];
    prev.second = leaf->keyslice_length(0);
    ALWAYS_ASSERT(prev.second <= 9);
    ALWAYS_ASSERT(!leaf->value_is_layer(0) || prev.second == 9);
    if (!leaf->value_is_layer(0) && prev.second == 9) {
      ALWAYS_ASSERT(leaf->suffixes_);
      ALWAYS_ASSERT(leaf->suffixes_[0].size() >= 1);
    }
    for (size_t i = 1; i < n; i++) {
      leaf_key cur_key;
      cur_key.first = keys_[i];
      cur_key.second = leaf->keyslice_length(i);
      ALWAYS_ASSERT(cur_key.second <= 9);
      ALWAYS_ASSERT(!leaf->value_is_layer(i) || cur_key.second == 9);
      if (!leaf->value_is_layer(i) && cur_key.second == 9) {
        ALWAYS_ASSERT(leaf->suffixes_);
        ALWAYS_ASSERT(leaf->suffixes_[i].size() >= 1);
      }
      ALWAYS_ASSERT(cur_key > prev);
      prev = cur_key;
    }
  } else {
    key_slice prev = keys_[0];
    for (size_t i = 1; i < n; i++) {
      ALWAYS_ASSERT(keys_[i] > prev);
      prev = keys_[i];
    }
  }
}

template <typename P>
void
btree<P>::node::base_invariant_checker(const key_slice *min_key,
                                       const key_slice *max_key,
                                       bool is_root) const
{
  ALWAYS_ASSERT(!is_locked());
  ALWAYS_ASSERT(!is_modifying());
  ALWAYS_ASSERT(this->is_root() == is_root);
  size_t n = this->key_slots_used();
  ALWAYS_ASSERT(n <= NKeysPerNode);
  if (is_root) {
    if (is_internal_node())
      ALWAYS_ASSERT(n >= 1);
  } else {
    if (is_internal_node())
      ALWAYS_ASSERT(n >= NMinKeysPerNode);
    else
      // key-slices constrain splits
      ALWAYS_ASSERT(n >= 1);
  }
  for (size_t i = 0; i < n; i++) {
    ALWAYS_ASSERT(!min_key || keys_[i] >= *min_key);
    ALWAYS_ASSERT(!max_key || keys_[i] < *max_key);
  }
  base_invariant_unique_keys_check();
}

template <typename P>
void
btree<P>::node::invariant_checker(const key_slice *min_key,
                                  const key_slice *max_key,
                                  const node *left_sibling,
                                  const node *right_sibling,
                                  bool is_root) const
{
  is_leaf_node() ?
    AsLeaf(this)->invariant_checker_impl(min_key, max_key, left_sibling, right_sibling, is_root) :
    AsInternal(this)->invariant_checker_impl(min_key, max_key, left_sibling, right_sibling, is_root) ;
}

//static event_counter evt_btree_leaf_node_creates("btree_leaf_node_creates");
//static event_counter evt_btree_leaf_node_deletes("btree_leaf_node_deletes");

//event_counter btree<P>::leaf_node::g_evt_suffixes_array_created("btree_leaf_node_suffix_array_creates");

template <typename P>
btree<P>::leaf_node::leaf_node()
  : node(), min_key_(0), prev_(NULL), next_(NULL), suffixes_(NULL)
{
  //++evt_btree_leaf_node_creates;
}

template <typename P>
btree<P>::leaf_node::~leaf_node()
{
  if (suffixes_)
    delete [] suffixes_;
  //suffixes_ = NULL;
  //++evt_btree_leaf_node_deletes;
}

template <typename P>
void
btree<P>::leaf_node::invariant_checker_impl(const key_slice *min_key,
                                            const key_slice *max_key,
                                            const node *left_sibling,
                                            const node *right_sibling,
                                            bool is_root) const
{
  this->base_invariant_checker(min_key, max_key, is_root);
  ALWAYS_ASSERT(!min_key || *min_key == this->min_key_);
  ALWAYS_ASSERT(!is_root || min_key == NULL);
  ALWAYS_ASSERT(!is_root || max_key == NULL);
  ALWAYS_ASSERT(is_root || this->key_slots_used() > 0);
  size_t n = this->key_slots_used();
  for (size_t i = 0; i < n; i++)
    if (this->value_is_layer(i))
      this->values_[i].n_->invariant_checker(NULL, NULL, NULL, NULL, true);
}

//static event_counter evt_btree_internal_node_creates("btree_internal_node_creates");
//static event_counter evt_btree_internal_node_deletes("btree_internal_node_deletes");

template <typename P>
btree<P>::internal_node::internal_node()
{
  VersionManip::Store(this->hdr_, 1);
  //++evt_btree_internal_node_creates;
}

template <typename P>
btree<P>::internal_node::~internal_node()
{
  //++evt_btree_internal_node_deletes;
}

template <typename P>
void
btree<P>::internal_node::invariant_checker_impl(const key_slice *min_key,
                                                const key_slice *max_key,
                                                const node *left_sibling,
                                                const node *right_sibling,
                                                bool is_root) const
{
  this->base_invariant_checker(min_key, max_key, is_root);
  size_t n = this->key_slots_used();
  for (size_t i = 0; i <= n; i++) {
    ALWAYS_ASSERT(this->children_[i] != NULL);
    if (i == 0) {
      const node *left_child_sibling = NULL;
      if (left_sibling)
        left_child_sibling = AsInternal(left_sibling)->children_[left_sibling->key_slots_used()];
      this->children_[0]->invariant_checker(min_key, &this->keys_[0], left_child_sibling, this->children_[i + 1], false);
    } else if (i == n) {
      const node *right_child_sibling = NULL;
      if (right_sibling)
        right_child_sibling = AsInternal(right_sibling)->children_[0];
      this->children_[n]->invariant_checker(&this->keys_[n - 1], max_key, this->children_[i - 1], right_child_sibling, false);
    } else {
      this->children_[i]->invariant_checker(&this->keys_[i - 1], &this->keys_[i], this->children_[i - 1], this->children_[i + 1], false);
    }
  }
  if (!n || this->children_[0]->is_internal_node())
    return;
  for (size_t i = 0; i <= n; i++) {
    const node *left_child_sibling = NULL;
    const node *right_child_sibling = NULL;
    if (left_sibling)
      left_child_sibling = AsInternal(left_sibling)->children_[left_sibling->key_slots_used()];
    if (right_sibling)
      right_child_sibling = AsInternal(right_sibling)->children_[0];
    const leaf_node *child_prev = (i == 0) ? AsLeaf(left_child_sibling) : AsLeaf(this->children_[i - 1]);
    const leaf_node *child_next = (i == n) ? AsLeaf(right_child_sibling) : AsLeaf(this->children_[i + 1]);
    ALWAYS_ASSERT(AsLeaf(this->children_[i])->prev_ == child_prev);
    ALWAYS_ASSERT(AsLeaf(this->children_[i])->next_ == child_next);
  }
}

template <typename P>
void
btree<P>::recursive_delete(node *n)
{
  if (leaf_node *leaf = AsLeafCheck(n)) {
#ifdef CHECK_INVARIANTS
    leaf->lock();
    leaf->mark_deleting();
    leaf->unlock();
#endif
    size_t n = leaf->key_slots_used();
    for (size_t i = 0; i < n; i++)
      if (leaf->value_is_layer(i))
        recursive_delete(leaf->values_[i].n_);
    leaf_node::deleter(leaf);
  } else {
    internal_node *internal = AsInternal(n);
    size_t n = internal->key_slots_used();
    for (size_t i = 0; i < n + 1; i++)
      recursive_delete(internal->children_[i]);
#ifdef CHECK_INVARIANTS
    internal->lock();
    internal->mark_deleting();
    internal->unlock();
#endif
    internal_node::deleter(internal);
  }
}

//STATIC_COUNTER_DECL(scopedperf::tsc_ctr, btree_search_impl_tsc, btree_search_impl_perf_cg);

template <typename P>
bool
btree<P>::search_impl(const key_type &k, value_type &v,
                      typename util::vec<leaf_node *>::type &leaf_nodes,
                      versioned_node_t *search_info) const
{
  INVARIANT(rcu::s_instance.in_rcu_region());
  //ANON_REGION("btree<P>::search_impl:", &btree_search_impl_perf_cg);
  INVARIANT(leaf_nodes.empty());

retry:
  node *cur;
  key_type kcur;
  uint64_t kslice;
  size_t kslicelen;
  if (likely(leaf_nodes.empty())) {
    kcur = k;
    cur = root_;
    kslice = k.slice();
    kslicelen = std::min(k.size(), size_t(9));
  } else {
    kcur = k.shift_many(leaf_nodes.size() - 1);
    cur = leaf_nodes.back();
    kslice = kcur.slice();
    kslicelen = std::min(kcur.size(), size_t(9));
    leaf_nodes.pop_back();
  }

  while (true) {
    // each iteration of this while loop tries to descend
    // down node "cur", looking for key kcur

process:
    uint64_t version = cur->stable_version();
    if (unlikely(RawVersionManip::IsDeleting(version)))
      // XXX: maybe we can only retry at the parent of this node, not the
      // root node of the b-tree *layer*
      goto retry;
    if (leaf_node *leaf = AsLeafCheck(cur, version)) {
      leaf->prefetch();
      if (search_info) {
        search_info->first = leaf;
        search_info->second = RawVersionManip::Version(version);
      }
      key_search_ret kret = leaf->key_search(kslice, kslicelen);
      ssize_t ret = kret.first;
      if (ret != -1) {
        // found
        typename leaf_node::value_or_node_ptr vn = leaf->values_[ret];
        const bool is_layer = leaf->value_is_layer(ret);
        INVARIANT(!is_layer || kslicelen == 9);
        varkey suffix(leaf->suffix(ret));
        if (unlikely(!leaf->check_version(version)))
          goto process;
        leaf_nodes.push_back(leaf);

        if (!is_layer) {
          // check suffixes
          if (kslicelen == 9 && suffix != kcur.shift())
            return false;
          v = vn.v_;
          return true;
        }

        // search the next layer
        cur = vn.n_;
        kcur = kcur.shift();
        kslice = kcur.slice();
        kslicelen = std::min(kcur.size(), size_t(9));
        continue;
      }

      // leaf might have lost responsibility for key k during the descend. we
      // need to check this and adjust accordingly
      if (unlikely(kslice < leaf->min_key_)) {
        // try to go left
        leaf_node *left_sibling = leaf->prev_;
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
        leaf_node *right_sibling = leaf->next_;
        if (unlikely(!leaf->check_version(version)))
          goto process;
        if (unlikely(!right_sibling)) {
          leaf_nodes.push_back(leaf);
          return false;
        }
        right_sibling->prefetch();
        uint64_t right_version = right_sibling->stable_version();
        key_slice right_min_key = right_sibling->min_key_;
        if (unlikely(!right_sibling->check_version(right_version)))
          goto process;
        if (unlikely(kslice >= right_min_key)) {
          cur = right_sibling;
          continue;
        }
      }

      leaf_nodes.push_back(leaf);
      return false;
    } else {
      internal_node *internal = AsInternal(cur);
      internal->prefetch();
      key_search_ret kret = internal->key_lower_bound_search(kslice);
      ssize_t ret = kret.first;
      if (ret != -1)
        cur = internal->children_[ret + 1];
      else
        cur = internal->children_[0];
      if (unlikely(!internal->check_version(version)))
        goto process;
      INVARIANT(kret.second);
    }
  }
}

template <typename S>
class string_restore {
public:
  inline string_restore(S &s, size_t n)
    : s_(&s), n_(n) {}
  inline ~string_restore()
  {
    s_->resize(n_);
  }
private:
  S *s_;
  size_t n_;
};

// recursively read the range from the layer down
//
// all args relative to prefix.
//
// prefix is non-const, meaning that this function might modify the contents.
// it is guaranteed, however, to restore prefix to the state it was before the
// invocation
//
// returns true if we keep going
template <typename P>
bool
btree<P>::search_range_at_layer(
    leaf_node *leaf,
    string_type &prefix,
    const key_type &lower,
    bool inc_lower,
    const key_type *upper,
    low_level_search_range_callback &callback) const
{
  VERBOSE(std::cerr << "search_range_at_layer: prefix.size()=" << prefix.size() << std::endl);

  key_slice last_keyslice = 0;
  size_t last_keyslice_len = 0;
  bool emitted_last_keyslice = false;
  if (!inc_lower) {
    last_keyslice = lower.slice();
    last_keyslice_len = std::min(lower.size(), size_t(9));
    emitted_last_keyslice = true;
  }

  key_slice lower_slice = lower.slice();
  key_slice next_key = lower_slice;
  const size_t prefix_size = prefix.size();
  // NB: DON'T CALL RESERVE()
  //prefix.reserve(prefix_size + 8); // allow for next layer
  const uint64_t upper_slice = upper ? upper->slice() : 0;
  string_restore<string_type> restorer(prefix, prefix_size);
  while (!upper || next_key <= upper_slice) {
    leaf->prefetch();

    typename util::vec<leaf_kvinfo>::type buf;
    const uint64_t version = leaf->stable_version();
    key_slice leaf_min_key = leaf->min_key_;
    if (leaf_min_key > next_key) {
      // go left
      leaf_node *left_sibling = leaf->prev_;
      if (unlikely(!leaf->check_version(version)))
        // try this node again
        continue;
      // try from left_sibling
      leaf = left_sibling;
      INVARIANT(leaf);
      continue;
    }

    // grab all keys in [lower_slice, upper_slice]. we'll do boundary condition
    // checking later (outside of the critical section)
    for (size_t i = 0; i < leaf->key_slots_used(); i++) {
      // XXX(stephentu): this 1st filter is overly conservative and we can do
      // better
      if ((leaf->keys_[i] > lower_slice ||
           (leaf->keys_[i] == lower_slice &&
            leaf->keyslice_length(i) >= std::min(lower.size(), size_t(9)))) &&
          (!upper || leaf->keys_[i] <= upper_slice))
        buf.emplace_back(
            leaf->keys_[i],
            leaf->values_[i],
            leaf->value_is_layer(i),
            leaf->keyslice_length(i),
            leaf->suffix(i));
    }

    leaf_node *const right_sibling = leaf->next_;
    key_slice leaf_max_key = right_sibling ? right_sibling->min_key_ : 0;

    if (unlikely(!leaf->check_version(version)))
      continue;

    callback.on_resp_node(leaf, RawVersionManip::Version(version));

    for (size_t i = 0; i < buf.size(); i++) {
      // check to see if we already omitted a key <= buf[i]: if so, don't omit it
      if (emitted_last_keyslice &&
          ((buf[i].key_ < last_keyslice) ||
           (buf[i].key_ == last_keyslice && buf[i].length_ <= last_keyslice_len)))
        continue;
      const size_t ncpy = std::min(buf[i].length_, size_t(8));
      // XXX: prefix.end() calls _M_leak()
      //prefix.replace(prefix.begin() + prefix_size, prefix.end(), buf[i].keyslice(), ncpy);
      prefix.replace(prefix_size, string_type::npos, buf[i].keyslice(), ncpy);
      if (buf[i].layer_) {
        // recurse into layer
        leaf_node *const next_layer = leftmost_descend_layer(buf[i].vn_.n_);
        varkey zerokey;
        if (emitted_last_keyslice && last_keyslice == buf[i].key_)
          // NB(stephentu): this is implied by the filter above
          INVARIANT(last_keyslice_len <= 8);
        if (!search_range_at_layer(next_layer, prefix, zerokey, false, NULL, callback))
          return false;
      } else {
        // check if we are before the start
        if (buf[i].key_ == lower_slice) {
          if (buf[i].length_ <= 8) {
            if (buf[i].length_ < lower.size())
              // skip
              continue;
          } else {
            INVARIANT(buf[i].length_ == 9);
            if (lower.size() > 8 && buf[i].suffix_ < lower.shift())
              // skip
              continue;
          }
        }

        // check if we are after the end
        if (upper && buf[i].key_ == upper_slice) {
          if (buf[i].length_ == 9) {
            if (upper->size() <= 8)
              break;
            if (buf[i].suffix_ >= upper->shift())
              break;
          } else if (buf[i].length_ >= upper->size()) {
            break;
          }
        }
        if (buf[i].length_ == 9)
          prefix.append((const char *) buf[i].suffix_.data(), buf[i].suffix_.size());
        // we give the actual version # minus all the other bits, b/c they are not
        // important here and make comparison easier at higher layers
        if (!callback.invoke(prefix, buf[i].vn_.v_, leaf, RawVersionManip::Version(version)))
          return false;
      }
      last_keyslice = buf[i].key_;
      last_keyslice_len = buf[i].length_;
      emitted_last_keyslice = true;
    }

    if (!right_sibling)
      // we're done
      return true;

    next_key = leaf_max_key;
    leaf = right_sibling;
  }

  return true;
}

template <typename P>
void
btree<P>::search_range_call(const key_type &lower,
                            const key_type *upper,
                            low_level_search_range_callback &callback,
                            string_type *buf) const
{
  rcu_region guard;
  INVARIANT(rcu::s_instance.in_rcu_region());
  if (unlikely(upper && *upper <= lower))
    return;
  typename util::vec<leaf_node *>::type leaf_nodes;
  value_type v = 0;
  search_impl(lower, v, leaf_nodes);
  INVARIANT(!leaf_nodes.empty());
  bool first = true;
  string_type prefix_tmp, *prefix_px;
  if (buf)
    prefix_px = buf;
  else
    prefix_px = &prefix_tmp;
  string_type &prefix(*prefix_px);
  INVARIANT(prefix.empty());
  prefix.assign((const char *) lower.data(), 8 * (leaf_nodes.size() - 1));
  while (!leaf_nodes.empty()) {
    leaf_node *cur = leaf_nodes.back();
    leaf_nodes.pop_back();
    key_type layer_upper;
    bool layer_has_upper = false;
    if (upper && upper->size() >= (8 * leaf_nodes.size())) {
      layer_upper = upper->shift_many(leaf_nodes.size());
      layer_has_upper = true;
    }
#ifdef CHECK_INVARIANTS
    string_type prefix_before(prefix);
#endif
    if (!search_range_at_layer(
          cur, prefix, lower.shift_many(leaf_nodes.size()),
          first, layer_has_upper ? &layer_upper : NULL, callback))
      return;
#ifdef CHECK_INVARIANTS
    INVARIANT(prefix == prefix_before);
#endif
    first = false;
    if (!leaf_nodes.empty()) {
      INVARIANT(prefix.size() >= 8);
      prefix.resize(prefix.size() - 8);
    }
  }
}

template <typename P>
bool
btree<P>::remove_stable_location(node **root_location, const key_type &k, value_type *old_v)
{
  INVARIANT(rcu::s_instance.in_rcu_region());
retry:
  key_slice new_key;
  node *replace_node = NULL;
  typename util::vec<remove_parent_entry>::type parents;
  typename util::vec<node *>::type locked_nodes;
  node *local_root = *root_location;
  remove_status status = remove0(local_root,
      NULL, /* min_key */
      NULL, /* max_key */
      k,
      old_v,
      NULL, /* left_node */
      NULL, /* right_node */
      new_key,
      replace_node,
      parents,
      locked_nodes);
  switch (status) {
  case R_NONE_NOMOD:
    return false;
  case R_NONE_MOD:
    return true;
  case R_RETRY:
    goto retry;
  case R_REPLACE_NODE:
    INVARIANT(local_root->is_deleting());
    INVARIANT(local_root->is_lock_owner());
    INVARIANT(local_root->is_root());
    INVARIANT(local_root == *root_location);
    replace_node->set_root();
    local_root->clear_root();
    COMPILER_MEMORY_FENCE;
    *root_location = replace_node;
    // locks are still held here
    return UnlockAndReturn(locked_nodes, true);
  default:
    ALWAYS_ASSERT(false);
    return false;
  }
  ALWAYS_ASSERT(false);
  return false;
}

template <typename P>
typename btree<P>::leaf_node *
btree<P>::leftmost_descend_layer(node *n) const
{
  node *cur = n;
  while (true) {
    if (leaf_node *leaf = AsLeafCheck(cur))
      return leaf;
    internal_node *internal = AsInternal(cur);
    uint64_t version = cur->stable_version();
    node *child = internal->children_[0];
    if (unlikely(!internal->check_version(version)))
      continue;
    cur = child;
  }
}

template <typename P>
void
btree<P>::tree_walk(tree_walk_callback &callback) const
{
  rcu_region guard;
  INVARIANT(rcu::s_instance.in_rcu_region());
  std::vector<node *> q;
  // XXX: not sure if cast is safe
  q.push_back((node *) root_);
  while (!q.empty()) {
    node *cur = q.back();
    q.pop_back();
    cur->prefetch();
    leaf_node *leaf = leftmost_descend_layer(cur);
    INVARIANT(leaf);
    while (leaf) {
      leaf->prefetch();
    process:
      const uint64_t version = leaf->stable_version();
      const size_t n = leaf->key_slots_used();
      std::vector<node *> layers;
      for (size_t i = 0; i < n; i++)
        if (leaf->value_is_layer(i))
          layers.push_back(leaf->values_[i].n_);
      leaf_node *next = leaf->next_;
      callback.on_node_begin(leaf);
      if (unlikely(!leaf->check_version(version))) {
        callback.on_node_failure();
        goto process;
      }
      callback.on_node_success();
      leaf = next;
      q.insert(q.end(), layers.begin(), layers.end());
    }
  }
}

template <typename P>
void
btree<P>::size_walk_callback::on_node_begin(const node_opaque_t *n)
{
  INVARIANT(n->is_leaf_node());
  INVARIANT(spec_size_ == 0);
  const leaf_node *leaf = (const leaf_node *) n;
  const size_t sz = leaf->key_slots_used();
  for (size_t i = 0; i < sz; i++)
    if (!leaf->value_is_layer(i))
      spec_size_++;
}

template <typename P>
void
btree<P>::size_walk_callback::on_node_success()
{
  size_ += spec_size_;
  spec_size_ = 0;
}

template <typename P>
void
btree<P>::size_walk_callback::on_node_failure()
{
  spec_size_ = 0;
}

template <typename P>
typename btree<P>::leaf_node *
btree<P>::FindRespLeafNode(
      leaf_node *leaf,
      uint64_t kslice,
      uint64_t &version)
{
retry:
  version = leaf->stable_version();
  if (unlikely(leaf->is_deleting())) {
    leaf_node *left = leaf->prev_;
    if (left) {
      leaf = left;
      goto retry;
    }
    leaf_node *right = leaf->next_;
    if (right) {
      leaf = right;
      goto retry;
    }
    // XXX(stephentu): not sure if we can *really* depend on this,
    // need to convince ourselves why this is not possible!
    ALWAYS_ASSERT(false);
  }
  if (unlikely(kslice < leaf->min_key_)) {
    // we need to go left
    leaf_node *left = leaf->prev_;
    if (left)
      leaf = left;
    goto retry;
  }
  leaf_node *right = leaf->next_;

  // NB(stephentu): the only way for right->min_key_ to decrease, is for the
  // current leaf node to split. therefore, it is un-necessary to ensure stable
  // version on the next node (we only need to ensure it on the current node)
  if (likely(right) && unlikely(kslice >= right->min_key_)) {
    leaf = right;
    goto retry;
  }

  //if (likely(right)) {
  //  const uint64_t right_version = right->stable_version();
  //  const uint64_t right_min_key = right->min_key_;
  //  if (unlikely(!right->check_version(right_version)))
  //    goto retry;
  //  if (unlikely(kslice >= right_min_key)) {
  //    leaf = right;
  //    goto retry;
  //  }
  //}

  return leaf;
}

template <typename P>
typename btree<P>::leaf_node *
btree<P>::FindRespLeafLowerBound(
      leaf_node *leaf,
      uint64_t kslice,
      size_t kslicelen,
      uint64_t &version,
      size_t &n,
      ssize_t &idxmatch,
      ssize_t &idxlowerbound)
{
  leaf = FindRespLeafNode(leaf, kslice, version);

  // use 0 for slice length, so we can a pointer <= all elements
  // with the same slice
  const key_search_ret kret = leaf->key_lower_bound_search(kslice, 0);
  const ssize_t ret = kret.first;
  n = kret.second;

  // count the number of values already here with the same kslice.
  idxmatch = -1;
  idxlowerbound = ret;
  for (size_t i = (ret == -1 ? 0 : ret); i < n; i++) {
    if (leaf->keys_[i] < kslice) {
      continue;
    } else if (leaf->keys_[i] == kslice) {
      const size_t kslicelen0 = leaf->keyslice_length(i);
      if (kslicelen0 <= kslicelen) {
        // invariant doesn't hold, b/c values can be changing
        // concurrently (leaf is not assumed to be locked)
        //INVARIANT(idxmatch == -1);
        idxlowerbound = i;
        if (kslicelen0 == kslicelen)
          idxmatch = i;
      }
    } else {
      break;
    }
  }

  return leaf;
}

template <typename P>
typename btree<P>::leaf_node *
btree<P>::FindRespLeafExact(
      leaf_node *leaf,
      uint64_t kslice,
      size_t kslicelen,
      uint64_t &version,
      size_t &n,
      ssize_t &idxmatch)
{
  leaf = FindRespLeafNode(leaf, kslice, version);
  key_search_ret kret = leaf->key_search(kslice, kslicelen);
  idxmatch = kret.first;
  n = kret.second;
  return leaf;
}

template <typename P>
typename btree<P>::insert_status
btree<P>::insert0(node *np,
                  const key_type &k,
                  value_type v,
                  bool only_if_absent,
                  value_type *old_v,
                  insert_info_t *insert_info,
                  key_slice &min_key,
                  node *&new_node,
                  typename util::vec<insert_parent_entry>::type &parents,
                  typename util::vec<node *>::type &locked_nodes)
{
  uint64_t kslice = k.slice();
  size_t kslicelen = std::min(k.size(), size_t(9));

  np->prefetch();
  if (leaf_node *leaf = AsLeafCheck(np)) {
    // locked nodes are acquired bottom to top
    INVARIANT(locked_nodes.empty());

retry_cur_leaf:
    uint64_t version;
    size_t n;
    ssize_t lenmatch, lenlowerbound;
    leaf_node *resp_leaf = FindRespLeafLowerBound(
        leaf, kslice, kslicelen, version, n, lenmatch, lenlowerbound);

    // len match case
    if (lenmatch != -1) {
      // exact match case
      if (kslicelen <= 8 ||
          (!resp_leaf->value_is_layer(lenmatch) &&
           resp_leaf->suffix(lenmatch) == k.shift())) {
        const uint64_t locked_version = resp_leaf->lock();
        if (unlikely(!btree::CheckVersion(version, locked_version))) {
          resp_leaf->unlock();
          goto retry_cur_leaf;
        }
        locked_nodes.push_back(resp_leaf);
        // easy case- we don't modify the node itself
        if (old_v)
          *old_v = resp_leaf->values_[lenmatch].v_;
        if (!only_if_absent)
          resp_leaf->values_[lenmatch].v_ = v;
        if (insert_info)
          insert_info->node = 0;
        return UnlockAndReturn(locked_nodes, I_NONE_NOMOD);
      }
      INVARIANT(kslicelen == 9);
      if (resp_leaf->value_is_layer(lenmatch)) {
        node *subroot = resp_leaf->values_[lenmatch].n_;
        INVARIANT(subroot);
        if (unlikely(!resp_leaf->check_version(version)))
          goto retry_cur_leaf;
        key_slice mk;
        node *ret;
        typename util::vec<insert_parent_entry>::type subparents;
        typename util::vec<node *>::type sub_locked_nodes;
        const insert_status status =
          insert0(subroot, k.shift(), v, only_if_absent, old_v, insert_info,
              mk, ret, subparents, sub_locked_nodes);

        switch (status) {
        case I_NONE_NOMOD:
        case I_NONE_MOD:
        case I_RETRY:
          INVARIANT(sub_locked_nodes.empty());
          return status;

        case I_SPLIT:
          // the subroot split, so we need to find the leaf again, lock the
          // node, and create a new internal node

          INVARIANT(ret);
          INVARIANT(ret->key_slots_used() > 0);

          for (;;) {
            resp_leaf = FindRespLeafLowerBound(
                resp_leaf, kslice, kslicelen, version, n, lenmatch, lenlowerbound);
            const uint64_t locked_version = resp_leaf->lock();
            if (likely(btree::CheckVersion(version, locked_version))) {
              locked_nodes.push_back(resp_leaf);
              break;
            }
            resp_leaf->unlock();
          }

          INVARIANT(lenmatch != -1);
          INVARIANT(resp_leaf->value_is_layer(lenmatch));
          subroot = resp_leaf->values_[lenmatch].n_;
          INVARIANT(subroot->is_modifying());
          INVARIANT(subroot->is_lock_owner());
          INVARIANT(subroot->is_root());

          internal_node *new_root = internal_node::alloc();
#ifdef CHECK_INVARIANTS
          new_root->lock();
          new_root->mark_modifying();
          locked_nodes.push_back(new_root);
#endif /* CHECK_INVARIANTS */
          new_root->children_[0] = subroot;
          new_root->children_[1] = ret;
          new_root->keys_[0] = mk;
          new_root->set_key_slots_used(1);
          new_root->set_root();
          subroot->clear_root();
          resp_leaf->values_[lenmatch].n_ = new_root;

          // locks are still held here
          UnlockNodes(sub_locked_nodes);
          return UnlockAndReturn(locked_nodes, I_NONE_MOD);
        }
        ALWAYS_ASSERT(false);

      } else {
        const uint64_t locked_version = resp_leaf->lock();
        if (unlikely(!btree::CheckVersion(version, locked_version))) {
          resp_leaf->unlock();
          goto retry_cur_leaf;
        }
        locked_nodes.push_back(resp_leaf);

        INVARIANT(resp_leaf->suffixes_); // b/c lenmatch != -1 and this is not a layer
        // need to create a new btree layer, and add both existing key and
        // new key to it

        // XXX: need to mark modifying because we cannot change both the
        // value and the type atomically
        resp_leaf->mark_modifying();

        leaf_node *new_root = leaf_node::alloc();
#ifdef CHECK_INVARIANTS
        new_root->lock();
        new_root->mark_modifying();
#endif /* CHECK_INVARIANTS */
        new_root->set_root();
        varkey old_slice(resp_leaf->suffix(lenmatch));
        new_root->keys_[0] = old_slice.slice();
        new_root->values_[0] = resp_leaf->values_[lenmatch];
        new_root->keyslice_set_length(0, std::min(old_slice.size(), size_t(9)), false);
        new_root->inc_key_slots_used();
        if (new_root->keyslice_length(0) == 9) {
          new_root->alloc_suffixes();
          rcu_imstring i(old_slice.data() + 8, old_slice.size() - 8);
          new_root->suffixes_[0].swap(i);
        }
        resp_leaf->values_[lenmatch].n_ = new_root;
        {
          rcu_imstring i;
          resp_leaf->suffixes_[lenmatch].swap(i);
        }
        resp_leaf->value_set_layer(lenmatch);
#ifdef CHECK_INVARIANTS
        new_root->unlock();
#endif /* CHECK_INVARIANTS */

        key_slice mk;
        node *ret;
        typename util::vec<insert_parent_entry>::type subparents;
        typename util::vec<node *>::type sub_locked_nodes;
        const insert_status status =
          insert0(new_root, k.shift(), v, only_if_absent, old_v, insert_info,
              mk, ret, subparents, sub_locked_nodes);
        if (status != I_NONE_MOD)
          INVARIANT(false);
        INVARIANT(sub_locked_nodes.empty());
        return UnlockAndReturn(locked_nodes, I_NONE_MOD);
      }
    }

    // lenlowerbound + 1 is the slot (0-based index) we want the new key to go
    // into, in the leaf node
    if (n < NKeysPerNode) {
      const uint64_t locked_version = resp_leaf->lock();
      if (unlikely(!btree::CheckVersion(version, locked_version))) {
        resp_leaf->unlock();
        goto retry_cur_leaf;
      }
      locked_nodes.push_back(resp_leaf);

      // also easy case- we only need to make local modifications
      resp_leaf->mark_modifying();

      sift_right(resp_leaf->keys_, lenlowerbound + 1, n);
      resp_leaf->keys_[lenlowerbound + 1] = kslice;
      sift_right(resp_leaf->values_, lenlowerbound + 1, n);
      resp_leaf->values_[lenlowerbound + 1].v_ = v;
      sift_right(resp_leaf->lengths_, lenlowerbound + 1, n);
      resp_leaf->keyslice_set_length(lenlowerbound + 1, kslicelen, false);
      if (resp_leaf->suffixes_)
        sift_swap_right(resp_leaf->suffixes_, lenlowerbound + 1, n);
      if (kslicelen == 9) {
        resp_leaf->ensure_suffixes();
        rcu_imstring i(k.data() + 8, k.size() - 8);
        resp_leaf->suffixes_[lenlowerbound + 1].swap(i);
      } else if (resp_leaf->suffixes_) {
        rcu_imstring i;
        resp_leaf->suffixes_[lenlowerbound + 1].swap(i);
      }
      resp_leaf->inc_key_slots_used();

//#ifdef CHECK_INVARIANTS
//      resp_leaf->base_invariant_unique_keys_check();
//#endif
      if (insert_info) {
        insert_info->node = resp_leaf;
        insert_info->old_version = RawVersionManip::Version(resp_leaf->unstable_version()); // we hold lock on leaf
        insert_info->new_version = insert_info->old_version + 1;
      }
      return UnlockAndReturn(locked_nodes, I_NONE_MOD);
    } else {
      INVARIANT(n == NKeysPerNode);

      if (unlikely(resp_leaf != leaf))
        // sigh, we really do need parent points- if resp_leaf != leaf, then
        // all the parent points we saved on the way down are no longer valid
        return UnlockAndReturn(locked_nodes, I_RETRY);
      const uint64_t locked_version = resp_leaf->lock();
      if (unlikely(!btree::CheckVersion(version, locked_version))) {
        resp_leaf->unlock();
        goto retry_cur_leaf;
      }
      locked_nodes.push_back(resp_leaf);

      // we need to split the current node, potentially causing a bunch of
      // splits to happen in ancestors. to make this safe w/o
      // using a very complicated locking protocol, we will first acquire all
      // locks on nodes which will be modified, in left-to-right,
      // bottom-to-top order

      if (parents.empty()) {
        if (unlikely(!resp_leaf->is_root()))
          return UnlockAndReturn(locked_nodes, I_RETRY);
        //INVARIANT(resp_leaf == root);
      } else {
        for (auto rit = parents.rbegin(); rit != parents.rend(); ++rit) {
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
            //INVARIANT(p == root);
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
      resp_leaf->mark_modifying();

      leaf_node *new_leaf = leaf_node::alloc();
      new_leaf->prefetch();

#ifdef CHECK_INVARIANTS
      new_leaf->lock();
      new_leaf->mark_modifying();
      locked_nodes.push_back(new_leaf);
#endif /* CHECK_INVARIANTS */

      if (!resp_leaf->next_ && resp_leaf->keys_[n - 1] < kslice) {
        // sequential insert optimization- in this case, we don't bother
        // splitting the node. instead, keep the current leaf node full, and
        // insert the new key into the new leaf node (violating the btree invariant)
        //
        // this optimization is commonly implemented, including in masstree and
        // berkeley db- w/o this optimization, sequential inserts leave the all
        // nodes half full

        new_leaf->keys_[0] = kslice;
        new_leaf->values_[0].v_ = v;
        new_leaf->keyslice_set_length(0, kslicelen, false);
        if (kslicelen == 9) {
          new_leaf->alloc_suffixes();
          rcu_imstring i(k.data() + 8, k.size() - 8);
          new_leaf->suffixes_[0].swap(i);
        }
        new_leaf->set_key_slots_used(1);

      } else {
        // regular case

        // compute how many keys the smaller node would have if we put the
        // new key in the left (old) or right (new) side of the split.
        // then choose the split which maximizes the mininum.
        //
        // XXX: do this in a more elegant way

        // a split point S is a number such that indices [0, s) go into the left
        // partition, and indices [s, N) go into the right partition
        size_t left_split_point, right_split_point;

        left_split_point = NKeysPerNode / 2;
        for (ssize_t i = left_split_point - 1; i >= 0; i--) {
          if (likely(resp_leaf->keys_[i] != resp_leaf->keys_[left_split_point]))
            break;
          left_split_point--;
        }
        INVARIANT(left_split_point <= NKeysPerNode);
        INVARIANT(left_split_point == 0 || resp_leaf->keys_[left_split_point - 1] != resp_leaf->keys_[left_split_point]);

        right_split_point = NKeysPerNode / 2;
        for (ssize_t i = right_split_point - 1; i >= 0 && i < ssize_t(NKeysPerNode) - 1; i++) {
          if (likely(resp_leaf->keys_[i] != resp_leaf->keys_[right_split_point]))
            break;
          right_split_point++;
        }
        INVARIANT(right_split_point <= NKeysPerNode);
        INVARIANT(right_split_point == 0 || resp_leaf->keys_[right_split_point - 1] != resp_leaf->keys_[right_split_point]);

        size_t split_point;
        if (std::min(left_split_point, NKeysPerNode - left_split_point) <
            std::min(right_split_point, NKeysPerNode - right_split_point))
          split_point = right_split_point;
        else
          split_point = left_split_point;

        if (split_point <= size_t(lenlowerbound + 1) && resp_leaf->keys_[split_point - 1] != kslice) {
          // put new key in new leaf (right)
          size_t pos = lenlowerbound + 1 - split_point;

          copy_into(&new_leaf->keys_[0], resp_leaf->keys_, split_point, lenlowerbound + 1);
          new_leaf->keys_[pos] = kslice;
          copy_into(&new_leaf->keys_[pos + 1], resp_leaf->keys_, lenlowerbound + 1, NKeysPerNode);

          copy_into(&new_leaf->values_[0], resp_leaf->values_, split_point, lenlowerbound + 1);
          new_leaf->values_[pos].v_ = v;
          copy_into(&new_leaf->values_[pos + 1], resp_leaf->values_, lenlowerbound + 1, NKeysPerNode);

          copy_into(&new_leaf->lengths_[0], resp_leaf->lengths_, split_point, lenlowerbound + 1);
          new_leaf->keyslice_set_length(pos, kslicelen, false);
          copy_into(&new_leaf->lengths_[pos + 1], resp_leaf->lengths_, lenlowerbound + 1, NKeysPerNode);

          if (resp_leaf->suffixes_) {
            new_leaf->ensure_suffixes();
            swap_with(&new_leaf->suffixes_[0], resp_leaf->suffixes_, split_point, lenlowerbound + 1);
          }
          if (kslicelen == 9) {
            new_leaf->ensure_suffixes();
            rcu_imstring i(k.data() + 8, k.size() - 8);
            new_leaf->suffixes_[pos].swap(i);
          } else if (new_leaf->suffixes_) {
            rcu_imstring i;
            new_leaf->suffixes_[pos].swap(i);
          }
          if (resp_leaf->suffixes_) {
            new_leaf->ensure_suffixes();
            swap_with(&new_leaf->suffixes_[pos + 1], resp_leaf->suffixes_, lenlowerbound + 1, NKeysPerNode);
          }

          resp_leaf->set_key_slots_used(split_point);
          new_leaf->set_key_slots_used(NKeysPerNode - split_point + 1);

#ifdef CHECK_INVARIANTS
          resp_leaf->base_invariant_unique_keys_check();
          new_leaf->base_invariant_unique_keys_check();
          INVARIANT(resp_leaf->keys_[split_point - 1] < new_leaf->keys_[0]);
#endif /* CHECK_INVARIANTS */

        } else {
          // XXX: not really sure if this invariant is true, but we rely
          // on it for now
          INVARIANT(size_t(lenlowerbound + 1) <= split_point);

          // put new key in original leaf
          copy_into(&new_leaf->keys_[0], resp_leaf->keys_, split_point, NKeysPerNode);
          copy_into(&new_leaf->values_[0], resp_leaf->values_, split_point, NKeysPerNode);
          copy_into(&new_leaf->lengths_[0], resp_leaf->lengths_, split_point, NKeysPerNode);
          if (resp_leaf->suffixes_) {
            new_leaf->ensure_suffixes();
            swap_with(&new_leaf->suffixes_[0], resp_leaf->suffixes_, split_point, NKeysPerNode);
          }

          sift_right(resp_leaf->keys_, lenlowerbound + 1, split_point);
          resp_leaf->keys_[lenlowerbound + 1] = kslice;
          sift_right(resp_leaf->values_, lenlowerbound + 1, split_point);
          resp_leaf->values_[lenlowerbound + 1].v_ = v;
          sift_right(resp_leaf->lengths_, lenlowerbound + 1, split_point);
          resp_leaf->keyslice_set_length(lenlowerbound + 1, kslicelen, false);
          if (resp_leaf->suffixes_)
            sift_swap_right(resp_leaf->suffixes_, lenlowerbound + 1, split_point);
          if (kslicelen == 9) {
            resp_leaf->ensure_suffixes();
            rcu_imstring i(k.data() + 8, k.size() - 8);
            resp_leaf->suffixes_[lenlowerbound + 1].swap(i);
          } else if (resp_leaf->suffixes_) {
            rcu_imstring i;
            resp_leaf->suffixes_[lenlowerbound + 1].swap(i);
          }

          resp_leaf->set_key_slots_used(split_point + 1);
          new_leaf->set_key_slots_used(NKeysPerNode - split_point);

#ifdef CHECK_INVARIANTS
          resp_leaf->base_invariant_unique_keys_check();
          new_leaf->base_invariant_unique_keys_check();
          INVARIANT(resp_leaf->keys_[split_point] < new_leaf->keys_[0]);
#endif /* CHECK_INVARIANTS */
        }
      }

      // pointer adjustment
      new_leaf->prev_ = resp_leaf;
      new_leaf->next_ = resp_leaf->next_;
      if (resp_leaf->next_)
        resp_leaf->next_->prev_ = new_leaf;
      resp_leaf->next_ = new_leaf;

      min_key = new_leaf->keys_[0];
      new_leaf->min_key_ = min_key;
      new_node = new_leaf;

      if (insert_info) {
        insert_info->node = resp_leaf;
        insert_info->old_version = RawVersionManip::Version(resp_leaf->unstable_version()); // we hold lock on leaf
        insert_info->new_version = insert_info->old_version + 1;
      }

      return I_SPLIT;
    }
  } else {
    internal_node *internal = AsInternal(np);
    uint64_t version = internal->stable_version();
    if (unlikely(RawVersionManip::IsDeleting(version)))
      return UnlockAndReturn(locked_nodes, I_RETRY);
    key_search_ret kret = internal->key_lower_bound_search(kslice);
    ssize_t ret = kret.first;
    size_t n = kret.second;
    size_t child_idx = (ret == -1) ? 0 : ret + 1;
    node *child_ptr = internal->children_[child_idx];
    if (unlikely(!internal->check_version(version)))
      return UnlockAndReturn(locked_nodes, I_RETRY);
    parents.push_back(insert_parent_entry(internal, version));
    key_slice mk = 0;
    node *new_child = NULL;
    insert_status status =
      insert0(child_ptr, k, v, only_if_absent, old_v, insert_info,
              mk, new_child, parents, locked_nodes);
    if (status != I_SPLIT) {
      INVARIANT(locked_nodes.empty());
      return status;
    }
    INVARIANT(new_child);
    INVARIANT(internal->is_locked()); // previous call to insert0() must lock internal node for insertion
    INVARIANT(internal->is_lock_owner());
    INVARIANT(internal->check_version(version));
    INVARIANT(new_child->key_slots_used() > 0);
    INVARIANT(n > 0);
    internal->mark_modifying();
    if (n < NKeysPerNode) {
      sift_right(internal->keys_, child_idx, n);
      internal->keys_[child_idx] = mk;
      sift_right(internal->children_, child_idx + 1, n + 1);
      internal->children_[child_idx + 1] = new_child;
      internal->inc_key_slots_used();
      return UnlockAndReturn(locked_nodes, I_NONE_MOD);
    } else {
      INVARIANT(n == NKeysPerNode);
      INVARIANT(ret == internal->key_lower_bound_search(mk).first);

      internal_node *new_internal = internal_node::alloc();
      new_internal->prefetch();
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
        min_key = internal->keys_[split_point];

        copy_into(&new_internal->keys_[0], internal->keys_, NMinKeysPerNode, NKeysPerNode);
        copy_into(&new_internal->children_[0], internal->children_, NMinKeysPerNode, NKeysPerNode + 1);
        new_internal->set_key_slots_used(NKeysPerNode - NMinKeysPerNode);

        sift_right(internal->keys_, child_idx, NMinKeysPerNode - 1);
        internal->keys_[child_idx] = mk;
        sift_right(internal->children_, child_idx + 1, NMinKeysPerNode);
        internal->children_[child_idx + 1] = new_child;
        internal->set_key_slots_used(NMinKeysPerNode);

      } else if (ret == split_point) {
        // case (2)
        min_key = mk;

        copy_into(&new_internal->keys_[0], internal->keys_, NMinKeysPerNode, NKeysPerNode);
        copy_into(&new_internal->children_[1], internal->children_, NMinKeysPerNode + 1, NKeysPerNode + 1);
        new_internal->children_[0] = new_child;
        new_internal->set_key_slots_used(NKeysPerNode - NMinKeysPerNode);
        internal->set_key_slots_used(NMinKeysPerNode);

      } else {
        // case (3)
        min_key = internal->keys_[NMinKeysPerNode];

        size_t pos = child_idx - NMinKeysPerNode - 1;

        copy_into(&new_internal->keys_[0], internal->keys_, NMinKeysPerNode + 1, child_idx);
        new_internal->keys_[pos] = mk;
        copy_into(&new_internal->keys_[pos + 1], internal->keys_, child_idx, NKeysPerNode);

        copy_into(&new_internal->children_[0], internal->children_, NMinKeysPerNode + 1, child_idx + 1);
        new_internal->children_[pos + 1] = new_child;
        copy_into(&new_internal->children_[pos + 2], internal->children_, child_idx + 1, NKeysPerNode + 1);

        new_internal->set_key_slots_used(NKeysPerNode - NMinKeysPerNode);
        internal->set_key_slots_used(NMinKeysPerNode);
      }

      INVARIANT(internal->keys_[internal->key_slots_used() - 1] < new_internal->keys_[0]);
      new_node = new_internal;
      return I_SPLIT;
    }
  }
}

template <typename P>
bool
btree<P>::insert_stable_location(
    node **root_location, const key_type &k, value_type v,
    bool only_if_absent, value_type *old_v,
    insert_info_t *insert_info)
{
  INVARIANT(rcu::s_instance.in_rcu_region());
retry:
  key_slice mk;
  node *ret;
  typename util::vec<insert_parent_entry>::type parents;
  typename util::vec<node *>::type locked_nodes;
  node *local_root = *root_location;
  const insert_status status =
    insert0(local_root, k, v, only_if_absent, old_v, insert_info,
            mk, ret, parents, locked_nodes);
  INVARIANT(status == I_SPLIT || locked_nodes.empty());
  switch (status) {
  case I_NONE_NOMOD:
    return false;
  case I_NONE_MOD:
    return true;
  case I_RETRY:
    goto retry;
  case I_SPLIT:
    INVARIANT(ret);
    INVARIANT(ret->key_slots_used() > 0);
    INVARIANT(local_root->is_modifying());
    INVARIANT(local_root->is_lock_owner());
    INVARIANT(local_root->is_root());
    INVARIANT(local_root == *root_location);
    internal_node *new_root = internal_node::alloc();
#ifdef CHECK_INVARIANTS
    new_root->lock();
    new_root->mark_modifying();
    locked_nodes.push_back(new_root);
#endif /* CHECK_INVARIANTS */
    new_root->children_[0] = local_root;
    new_root->children_[1] = ret;
    new_root->keys_[0] = mk;
    new_root->set_key_slots_used(1);
    new_root->set_root();
    local_root->clear_root();
    COMPILER_MEMORY_FENCE;
    *root_location = new_root;
    // locks are still held here
    return UnlockAndReturn(locked_nodes, true);
  }
  ALWAYS_ASSERT(false);
  return false;
}

/**
 * remove is very tricky to get right!
 *
 * XXX: optimize remove so it holds less locks
 */
template <typename P>
typename btree<P>::remove_status
btree<P>::remove0(node *np,
                  key_slice *min_key,
                  key_slice *max_key,
                  const key_type &k,
                  value_type *old_v,
                  node *left_node,
                  node *right_node,
                  key_slice &new_key,
                  node *&replace_node,
                  typename util::vec<remove_parent_entry>::type &parents,
                  typename util::vec<node *>::type &locked_nodes)
{
  uint64_t kslice = k.slice();
  size_t kslicelen = std::min(k.size(), size_t(9));

  np->prefetch();
  if (leaf_node *leaf = AsLeafCheck(np)) {
    INVARIANT(locked_nodes.empty());

    SINGLE_THREADED_INVARIANT(!left_node || (leaf->prev_ == left_node && AsLeaf(left_node)->next == leaf));
    SINGLE_THREADED_INVARIANT(!right_node || (leaf->next_ == right_node && AsLeaf(right_node)->prev == leaf));

retry_cur_leaf:
    uint64_t version;
    size_t n;
    ssize_t ret;
    leaf_node *resp_leaf = FindRespLeafExact(
        leaf, kslice, kslicelen, version, n, ret);

    if (ret == -1) {
      if (unlikely(!resp_leaf->check_version(version)))
        goto retry_cur_leaf;
      return UnlockAndReturn(locked_nodes, R_NONE_NOMOD);
    }
    if (kslicelen == 9) {
      if (resp_leaf->value_is_layer(ret)) {
        node *subroot = resp_leaf->values_[ret].n_;
        INVARIANT(subroot);
        if (unlikely(!resp_leaf->check_version(version)))
          goto retry_cur_leaf;

        key_slice new_key;
        node *replace_node = NULL;
        typename util::vec<remove_parent_entry>::type sub_parents;
        typename util::vec<node *>::type sub_locked_nodes;
        remove_status status = remove0(subroot,
            NULL, /* min_key */
            NULL, /* max_key */
            k.shift(),
            old_v,
            NULL, /* left_node */
            NULL, /* right_node */
            new_key,
            replace_node,
            sub_parents,
            sub_locked_nodes);
        switch (status) {
        case R_NONE_NOMOD:
        case R_NONE_MOD:
        case R_RETRY:
          INVARIANT(sub_locked_nodes.empty());
          return status;

        case R_REPLACE_NODE:
          INVARIANT(replace_node);
          for (;;) {
            resp_leaf = FindRespLeafExact(
                resp_leaf, kslice, kslicelen, version, n, ret);
            const uint64_t locked_version = resp_leaf->lock();
            if (likely(btree::CheckVersion(version, locked_version))) {
              locked_nodes.push_back(resp_leaf);
              break;
            }
            resp_leaf->unlock();
          }

          INVARIANT(subroot->is_deleting());
          INVARIANT(subroot->is_lock_owner());
          INVARIANT(subroot->is_root());
          replace_node->set_root();
          subroot->clear_root();
          resp_leaf->values_[ret].n_ = replace_node;

          // XXX: need to re-merge back when layer size becomes 1, but skip this
          // for now

          // locks are still held here
          UnlockNodes(sub_locked_nodes);
          return UnlockAndReturn(locked_nodes, R_NONE_MOD);

        default:
          break;
        }
        ALWAYS_ASSERT(false);

      } else {
        // suffix check
        if (resp_leaf->suffix(ret) != k.shift()) {
          if (unlikely(!resp_leaf->check_version(version)))
            goto retry_cur_leaf;
          return UnlockAndReturn(locked_nodes, R_NONE_NOMOD);
        }
      }
    }

    //INVARIANT(!resp_leaf->value_is_layer(ret));
    if (n > NMinKeysPerNode) {
      const uint64_t locked_version = resp_leaf->lock();
      if (unlikely(!btree::CheckVersion(version, locked_version))) {
        resp_leaf->unlock();
        goto retry_cur_leaf;
      }
      locked_nodes.push_back(resp_leaf);
      if (old_v)
        *old_v = resp_leaf->values_[ret].v_;
      resp_leaf->mark_modifying();
      remove_pos_from_leaf_node(resp_leaf, ret, n);
      return UnlockAndReturn(locked_nodes, R_NONE_MOD);
    } else {

      if (unlikely(resp_leaf != leaf))
        return UnlockAndReturn(locked_nodes, R_RETRY);
      const uint64_t locked_version = leaf->lock();
      if (unlikely(!btree::CheckVersion(version, locked_version))) {
        leaf->unlock();
        goto retry_cur_leaf;
      }
      locked_nodes.push_back(leaf);
      if (old_v)
        *old_v = leaf->values_[ret].v_;

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
        //INVARIANT(leaf == root);
      }

      for (typename util::vec<remove_parent_entry>::type::reverse_iterator rit = parents.rbegin();
           rit != parents.rend(); ++rit) {
        node *p = rit->parent_;
        node *l = rit->parent_left_sibling_;
        node *r = rit->parent_right_sibling_;
        uint64_t p_version = rit->parent_version_;
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
          //INVARIANT(p == root);
        }
      }

      leaf->mark_modifying();

      if (right_sibling) {
        right_sibling->mark_modifying();
        size_t right_n = right_sibling->key_slots_used();
        if (right_n > NMinKeysPerNode) {
          // steal first contiguous key slices from right
          INVARIANT(right_sibling->keys_[0] > leaf->keys_[n - 1]);

          // indices [0, steal_point) will be taken from the right
          size_t steal_point = 1;
          for (size_t i = 0; i < right_n - 1; i++, steal_point++)
            if (likely(right_sibling->keys_[i] != right_sibling->keys_[steal_point]))
              break;

          INVARIANT(steal_point <= sizeof(key_slice) + 2);
          INVARIANT(steal_point <= right_n);

          // to steal, we need to ensure:
          // 1) we have enough room to steal
          // 2) the right sibling will not be empty after the steal
          if ((n - 1 + steal_point) <= NKeysPerNode && steal_point < right_n) {
            sift_left(leaf->keys_, ret, n);
            copy_into(&leaf->keys_[n - 1], right_sibling->keys_, 0, steal_point);
            sift_left(leaf->values_, ret, n);
            copy_into(&leaf->values_[n - 1], right_sibling->values_, 0, steal_point);
            sift_left(leaf->lengths_, ret, n);
            copy_into(&leaf->lengths_[n - 1], right_sibling->lengths_, 0, steal_point);
            if (leaf->suffixes_)
              sift_swap_left(leaf->suffixes_, ret, n);
            if (right_sibling->suffixes_) {
              leaf->ensure_suffixes();
              swap_with(&leaf->suffixes_[n - 1], right_sibling->suffixes_, 0, steal_point);
            }

            sift_left(right_sibling->keys_, 0, right_n, steal_point);
            sift_left(right_sibling->values_, 0, right_n, steal_point);
            sift_left(right_sibling->lengths_, 0, right_n, steal_point);
            if (right_sibling->suffixes_)
              sift_swap_left(right_sibling->suffixes_, 0, right_n, steal_point);
            leaf->set_key_slots_used(n - 1 + steal_point);
            right_sibling->set_key_slots_used(right_n - steal_point);
            new_key = right_sibling->keys_[0];
            right_sibling->min_key_ = new_key;

#ifdef CHECK_INVARIANTS
            leaf->base_invariant_unique_keys_check();
            right_sibling->base_invariant_unique_keys_check();
            INVARIANT(leaf->keys_[n - 1 + steal_point - 1] < new_key);
#endif /* CHECK_INVARIANTS */

            return R_STOLE_FROM_RIGHT;
          } else {
            // can't steal, so try merging- but only merge if we have room,
            // otherwise just allow this node to have less elements
            if ((n - 1 + right_n) > NKeysPerNode) {
              INVARIANT(n > 1); // if we can't steal or merge, we must have
                                // enough elements to just remove one w/o going empty
              remove_pos_from_leaf_node(leaf, ret, n);
              return UnlockAndReturn(locked_nodes, R_NONE_MOD);
            }
          }
        }

        // merge right sibling into this node
        INVARIANT(right_sibling->keys_[0] > leaf->keys_[n - 1]);
        INVARIANT((right_n + (n - 1)) <= NKeysPerNode);

        sift_left(leaf->keys_, ret, n);
        copy_into(&leaf->keys_[n - 1], right_sibling->keys_, 0, right_n);

        sift_left(leaf->values_, ret, n);
        copy_into(&leaf->values_[n - 1], right_sibling->values_, 0, right_n);

        sift_left(leaf->lengths_, ret, n);
        copy_into(&leaf->lengths_[n - 1], right_sibling->lengths_, 0, right_n);

        if (leaf->suffixes_)
          sift_swap_left(leaf->suffixes_, ret, n);

        if (right_sibling->suffixes_) {
          leaf->ensure_suffixes();
          swap_with(&leaf->suffixes_[n - 1], right_sibling->suffixes_, 0, right_n);
        }

        leaf->set_key_slots_used(right_n + (n - 1));
        leaf->next_ = right_sibling->next_;
        if (right_sibling->next_)
          right_sibling->next_->prev_ = leaf;

        // leaf->next_->prev won't change because we hold lock for both leaf
        // and right_sibling
        INVARIANT(!leaf->next_ || leaf->next_->prev_ == leaf);

        // leaf->prev_->next might change, however, since the left node could be
        // splitting (and we might hold a pointer to the left-split of the left node,
        // before it gets updated)
        SINGLE_THREADED_INVARIANT(!leaf->prev_ || leaf->prev_->next_ == leaf);

//#ifdef CHECK_INVARIANTS
//        leaf->base_invariant_unique_keys_check();
//#endif
        leaf_node::release(right_sibling);
        return R_MERGE_WITH_RIGHT;
      }

      if (left_sibling) {
        left_sibling->mark_modifying();
        size_t left_n = left_sibling->key_slots_used();
        if (left_n > NMinKeysPerNode) {
          // try to steal from left
          INVARIANT(left_sibling->keys_[left_n - 1] < leaf->keys_[0]);

          // indices [steal_point, left_n) will be taken from the left
          size_t steal_point = left_n - 1;
          for (ssize_t i = steal_point - 1; i >= 0; i--, steal_point--)
            if (likely(left_sibling->keys_[i] != left_sibling->keys_[steal_point]))
              break;

          size_t nstolen = left_n - steal_point;
          INVARIANT(nstolen <= sizeof(key_slice) + 2);
          INVARIANT(steal_point < left_n);

          if ((n - 1 + nstolen) <= NKeysPerNode && steal_point > 0) {
            sift_right(leaf->keys_, ret + 1, n, nstolen - 1);
            sift_right(leaf->keys_, 0, ret, nstolen);
            copy_into(&leaf->keys_[0], &left_sibling->keys_[0], left_n - nstolen, left_n);

            sift_right(leaf->values_, ret + 1, n, nstolen - 1);
            sift_right(leaf->values_, 0, ret, nstolen);
            copy_into(&leaf->values_[0], &left_sibling->values_[0], left_n - nstolen, left_n);

            sift_right(leaf->lengths_, ret + 1, n, nstolen - 1);
            sift_right(leaf->lengths_, 0, ret, nstolen);
            copy_into(&leaf->lengths_[0], &left_sibling->lengths_[0], left_n - nstolen, left_n);

            if (leaf->suffixes_) {
              sift_swap_right(leaf->suffixes_, ret + 1, n, nstolen - 1);
              sift_swap_right(leaf->suffixes_, 0, ret, nstolen);
            }
            if (left_sibling->suffixes_) {
              leaf->ensure_suffixes();
              swap_with(&leaf->suffixes_[0], &left_sibling->suffixes_[0], left_n - nstolen, left_n);
            }

            left_sibling->set_key_slots_used(left_n - nstolen);
            leaf->set_key_slots_used(n - 1 + nstolen);
            new_key = leaf->keys_[0];
            leaf->min_key_ = new_key;

#ifdef CHECK_INVARIANTS
            leaf->base_invariant_unique_keys_check();
            left_sibling->base_invariant_unique_keys_check();
            INVARIANT(left_sibling->keys_[left_n - nstolen - 1] < new_key);
#endif /* CHECK_INVARIANTS */

            return R_STOLE_FROM_LEFT;
          } else {
            if ((left_n + (n - 1)) > NKeysPerNode) {
              INVARIANT(n > 1);
              remove_pos_from_leaf_node(leaf, ret, n);
              return UnlockAndReturn(locked_nodes, R_NONE_MOD);
            }
          }
        }

        // merge this node into left sibling
        INVARIANT(left_sibling->keys_[left_n - 1] < leaf->keys_[0]);
        INVARIANT((left_n + (n - 1)) <= NKeysPerNode);

        copy_into(&left_sibling->keys_[left_n], leaf->keys_, 0, ret);
        copy_into(&left_sibling->keys_[left_n + ret], leaf->keys_, ret + 1, n);

        copy_into(&left_sibling->values_[left_n], leaf->values_, 0, ret);
        copy_into(&left_sibling->values_[left_n + ret], leaf->values_, ret + 1, n);

        copy_into(&left_sibling->lengths_[left_n], leaf->lengths_, 0, ret);
        copy_into(&left_sibling->lengths_[left_n + ret], leaf->lengths_, ret + 1, n);

        if (leaf->suffixes_) {
          left_sibling->ensure_suffixes();
          swap_with(&left_sibling->suffixes_[left_n], leaf->suffixes_, 0, ret);
          swap_with(&left_sibling->suffixes_[left_n + ret], leaf->suffixes_, ret + 1, n);
        }

        left_sibling->set_key_slots_used(left_n + (n - 1));
        left_sibling->next_ = leaf->next_;
        if (leaf->next_)
          leaf->next_->prev_ = left_sibling;

        // see comments in right_sibling case above, for why one of them is INVARIANT and
        // the other is SINGLE_THREADED_INVARIANT
        INVARIANT(!left_sibling->next_ || left_sibling->next_->prev_ == left_sibling);
        SINGLE_THREADED_INVARIANT(
            !left_sibling->prev_ ||
            left_sibling->prev_->next_ == left_sibling);

        //left_sibling->base_invariant_unique_keys_check();
        leaf_node::release(leaf);
        return R_MERGE_WITH_LEFT;
      }

      // root node, so we are ok
      //INVARIANT(leaf == root);
      INVARIANT(leaf->is_root());
      remove_pos_from_leaf_node(leaf, ret, n);
      return UnlockAndReturn(locked_nodes, R_NONE_MOD);
    }
  } else {
    internal_node *internal = AsInternal(np);
    uint64_t version = internal->stable_version();
    if (unlikely(RawVersionManip::IsDeleting(version)))
      return UnlockAndReturn(locked_nodes, R_RETRY);
    key_search_ret kret = internal->key_lower_bound_search(kslice);
    ssize_t ret = kret.first;
    size_t n = kret.second;
    size_t child_idx = (ret == -1) ? 0 : ret + 1;
    node *child_ptr = internal->children_[child_idx];
    key_slice *child_min_key = child_idx == 0 ? NULL : &internal->keys_[child_idx - 1];
    key_slice *child_max_key = child_idx == n ? NULL : &internal->keys_[child_idx];
    node *child_left_sibling = child_idx == 0 ? NULL : internal->children_[child_idx - 1];
    node *child_right_sibling = child_idx == n ? NULL : internal->children_[child_idx + 1];
    if (unlikely(!internal->check_version(version)))
      return UnlockAndReturn(locked_nodes, R_RETRY);
    parents.push_back(remove_parent_entry(internal, left_node, right_node, version));
    INVARIANT(n > 0);
    key_slice nk;
    node *rn;
    remove_status status = remove0(child_ptr,
        child_min_key,
        child_max_key,
        k,
        old_v,
        child_left_sibling,
        child_right_sibling,
        nk,
        rn,
        parents,
        locked_nodes);
    switch (status) {
      case R_NONE_NOMOD:
      case R_NONE_MOD:
      case R_RETRY:
        return status;

      case R_STOLE_FROM_LEFT:
        INVARIANT(internal->is_locked());
        INVARIANT(internal->is_lock_owner());
        internal->keys_[child_idx - 1] = nk;
        return UnlockAndReturn(locked_nodes, R_NONE_MOD);

      case R_STOLE_FROM_RIGHT:
        INVARIANT(internal->is_locked());
        INVARIANT(internal->is_lock_owner());
        internal->keys_[child_idx] = nk;
        return UnlockAndReturn(locked_nodes, R_NONE_MOD);

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
            return UnlockAndReturn(locked_nodes, R_NONE_MOD);
          }

          internal_node *left_sibling = AsInternal(left_node);
          internal_node *right_sibling = AsInternal(right_node);

          // WARNING: if you change the order of events here, then you must also
          // change the locking protocol (see the comment in the leaf node case)

          if (right_sibling) {
            right_sibling->mark_modifying();
            size_t right_n = right_sibling->key_slots_used();
            INVARIANT(max_key);
            INVARIANT(right_sibling->keys_[0] > internal->keys_[n - 1]);
            INVARIANT(*max_key > internal->keys_[n - 1]);
            if (right_n > NMinKeysPerNode) {
              // steal from right
              sift_left(internal->keys_, del_key_idx, n);
              internal->keys_[n - 1] = *max_key;

              sift_left(internal->children_, del_child_idx, n + 1);
              internal->children_[n] = right_sibling->children_[0];

              new_key = right_sibling->keys_[0];

              sift_left(right_sibling->keys_, 0, right_n);
              sift_left(right_sibling->children_, 0, right_n + 1);
              right_sibling->dec_key_slots_used();

              return R_STOLE_FROM_RIGHT;
            } else {
              // merge with right
              INVARIANT(max_key);

              sift_left(internal->keys_, del_key_idx, n);
              internal->keys_[n - 1] = *max_key;
              copy_into(&internal->keys_[n], right_sibling->keys_, 0, right_n);

              sift_left(internal->children_, del_child_idx, n + 1);
              copy_into(&internal->children_[n], right_sibling->children_, 0, right_n + 1);

              internal->set_key_slots_used(n + right_n);
              internal_node::release(right_sibling);
              return R_MERGE_WITH_RIGHT;
            }
          }

          if (left_sibling) {
            left_sibling->mark_modifying();
            size_t left_n = left_sibling->key_slots_used();
            INVARIANT(min_key);
            INVARIANT(left_sibling->keys_[left_n - 1] < internal->keys_[0]);
            INVARIANT(left_sibling->keys_[left_n - 1] < *min_key);
            INVARIANT(*min_key < internal->keys_[0]);
            if (left_n > NMinKeysPerNode) {
              // steal from left
              sift_right(internal->keys_, 0, del_key_idx);
              internal->keys_[0] = *min_key;

              sift_right(internal->children_, 0, del_child_idx);
              internal->children_[0] = left_sibling->children_[left_n];

              new_key = left_sibling->keys_[left_n - 1];
              left_sibling->dec_key_slots_used();

              return R_STOLE_FROM_LEFT;
            } else {
              // merge into left sibling
              INVARIANT(min_key);

              size_t left_key_j = left_n;
              size_t left_child_j = left_n + 1;

              left_sibling->keys_[left_key_j++] = *min_key;

              copy_into(&left_sibling->keys_[left_key_j], internal->keys_, 0, del_key_idx);
              left_key_j += del_key_idx;
              copy_into(&left_sibling->keys_[left_key_j], internal->keys_, del_key_idx + 1, n);

              copy_into(&left_sibling->children_[left_child_j], internal->children_, 0, del_child_idx);
              left_child_j += del_child_idx;
              copy_into(&left_sibling->children_[left_child_j], internal->children_, del_child_idx + 1, n + 1);

              left_sibling->set_key_slots_used(n + left_n);
              internal_node::release(internal);
              return R_MERGE_WITH_LEFT;
            }
          }

          //INVARIANT(internal == root);
          INVARIANT(internal->is_root());
          remove_pos_from_internal_node(internal, del_key_idx, del_child_idx, n);
          INVARIANT(internal->key_slots_used() + 1 == n);
          if ((n - 1) == 0) {
            replace_node = internal->children_[0];
            internal_node::release(internal);
            return R_REPLACE_NODE;
          }

          return UnlockAndReturn(locked_nodes, R_NONE_MOD);
        }

      default:
        ALWAYS_ASSERT(false);
        return UnlockAndReturn(locked_nodes, R_NONE_NOMOD);
    }
  }
}

template <typename P>
std::string
btree<P>::NodeStringify(const node_opaque_t *n)
{
  std::vector<std::string> keys;
  for (size_t i = 0; i < n->key_slots_used(); i++)
    keys.push_back(std::string("0x") + util::hexify(n->keys_[i]));

  std::ostringstream b;
  b << "node[v=" << n->version_info_str()
    << ", keys=" << util::format_list(keys.begin(), keys.end());

  if (n->is_leaf_node()) {
    const leaf_node *leaf = AsLeaf(n);
    std::vector<std::string> lengths;
    for (size_t i = 0; i < leaf->key_slots_used(); i++) {
      std::ostringstream inf;
      inf << "<l=" << leaf->keyslice_length(i) << ",is_layer=" << leaf->value_is_layer(i) << ">";
      lengths.push_back(inf.str());
    }
    b << ", lengths=" << util::format_list(lengths.begin(), lengths.end());
  } else {
    //const internal_node *internal = AsInternal(n);
    // nothing for now
  }

  b << "]";
  return b.str();
}

template <typename P>
std::vector<std::pair<typename btree<P>::value_type, bool>>
btree<P>::ExtractValues(const node_opaque_t *n)
{
  std::vector< std::pair<value_type, bool> > ret;
  if (!n->is_leaf_node())
    return ret;
  const leaf_node *leaf = (const leaf_node *) n;
  const size_t sz = leaf->key_slots_used();
  for (size_t i = 0; i < sz; i++)
    if (!leaf->value_is_layer(i))
      ret.emplace_back(leaf->values_[i].v_, leaf->keyslice_length(i) > 8);
  return ret;
}
