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

using namespace std;
using namespace util;

string
btree::node::VersionInfoStr(uint64_t v)
{
  ostringstream buf;
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

void
btree::node::base_invariant_unique_keys_check() const
{
  size_t n = key_slots_used();
  if (n == 0)
    return;
  if (is_leaf_node()) {
    const leaf_node *leaf = AsLeaf(this);
    typedef pair<key_slice, size_t> leaf_key;
    leaf_key prev;
    prev.first = keys[0];
    prev.second = leaf->keyslice_length(0);
    ALWAYS_ASSERT(prev.second <= 9);
    ALWAYS_ASSERT(!leaf->value_is_layer(0) || prev.second == 9);
    if (!leaf->value_is_layer(0) && prev.second == 9) {
      ALWAYS_ASSERT(leaf->suffixes);
      ALWAYS_ASSERT(leaf->suffixes[0].size() >= 1);
    }
    for (size_t i = 1; i < n; i++) {
      leaf_key cur_key;
      cur_key.first = keys[i];
      cur_key.second = leaf->keyslice_length(i);
      ALWAYS_ASSERT(cur_key.second <= 9);
      ALWAYS_ASSERT(!leaf->value_is_layer(i) || cur_key.second == 9);
      if (!leaf->value_is_layer(i) && cur_key.second == 9) {
        ALWAYS_ASSERT(leaf->suffixes);
        ALWAYS_ASSERT(leaf->suffixes[i].size() >= 1);
      }
      ALWAYS_ASSERT(cur_key > prev);
      prev = cur_key;
    }
  } else {
    key_slice prev = keys[0];
    for (size_t i = 1; i < n; i++) {
      ALWAYS_ASSERT(keys[i] > prev);
      prev = keys[i];
    }
  }
}

void
btree::node::base_invariant_checker(const key_slice *min_key,
                                    const key_slice *max_key,
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
    if (is_internal_node())
      ALWAYS_ASSERT(n >= NMinKeysPerNode);
    else
      // key-slices constrain splits
      ALWAYS_ASSERT(n >= 1);
  }
  for (size_t i = 0; i < n; i++) {
    ALWAYS_ASSERT(!min_key || keys[i] >= *min_key);
    ALWAYS_ASSERT(!max_key || keys[i] < *max_key);
  }
  base_invariant_unique_keys_check();
}

void
btree::node::invariant_checker(const key_slice *min_key,
                               const key_slice *max_key,
                               const node *left_sibling,
                               const node *right_sibling,
                               bool is_root) const
{
  is_leaf_node() ?
    AsLeaf(this)->invariant_checker_impl(min_key, max_key, left_sibling, right_sibling, is_root) :
    AsInternal(this)->invariant_checker_impl(min_key, max_key, left_sibling, right_sibling, is_root) ;
}

static event_counter evt_btree_leaf_node_creates("btree_leaf_node_creates");
static event_counter evt_btree_leaf_node_deletes("btree_leaf_node_deletes");

event_counter btree::leaf_node::g_evt_suffixes_array_created("btree_leaf_node_suffix_array_creates");

btree::leaf_node::leaf_node()
  : min_key(0), prev(NULL), next(NULL), suffixes(NULL)
{
  hdr = 0;
  ++evt_btree_leaf_node_creates;
}

btree::leaf_node::~leaf_node()
{
  if (suffixes)
    delete [] suffixes;
  suffixes = NULL;
  ++evt_btree_leaf_node_deletes;
}

void
btree::leaf_node::invariant_checker_impl(const key_slice *min_key,
                                         const key_slice *max_key,
                                         const node *left_sibling,
                                         const node *right_sibling,
                                         bool is_root) const
{
  base_invariant_checker(min_key, max_key, is_root);
  ALWAYS_ASSERT(!min_key || *min_key == this->min_key);
  ALWAYS_ASSERT(!is_root || min_key == NULL);
  ALWAYS_ASSERT(!is_root || max_key == NULL);
  ALWAYS_ASSERT(is_root || key_slots_used() > 0);
  size_t n = key_slots_used();
  for (size_t i = 0; i < n; i++)
    if (value_is_layer(i))
      values[i].n->invariant_checker(NULL, NULL, NULL, NULL, true);
}

static event_counter evt_btree_internal_node_creates("btree_internal_node_creates");
static event_counter evt_btree_internal_node_deletes("btree_internal_node_deletes");

btree::internal_node::internal_node()
{
  hdr = 1;
  ++evt_btree_internal_node_creates;
}

btree::internal_node::~internal_node()
{
  ++evt_btree_internal_node_deletes;
}

void
btree::internal_node::invariant_checker_impl(const key_slice *min_key,
                                             const key_slice *max_key,
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

void
btree::recursive_delete(node *n)
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
        recursive_delete(leaf->values[i].n);
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

STATIC_COUNTER_DECL(scopedperf::tsc_ctr, btree_search_impl_tsc, btree_search_impl_perf_cg);

bool
btree::search_impl(const key_type &k, value_type &v,
                   typename vec<leaf_node *>::type &leaf_nodes,
                   versioned_node_t *search_info) const
{
  ANON_REGION("btree::search_impl:", &btree_search_impl_perf_cg);
  INVARIANT(leaf_nodes.empty());

retry:
  node *cur;
  key_type kcur;
  uint64_t kslice;
  size_t kslicelen;
  if (likely(leaf_nodes.empty())) {
    kcur = k;
    cur = root;
    kslice = k.slice();
    kslicelen = min(k.size(), size_t(9));
  } else {
    kcur = k.shift_many(leaf_nodes.size() - 1);
    cur = leaf_nodes.back();
    kslice = kcur.slice();
    kslicelen = min(kcur.size(), size_t(9));
    leaf_nodes.pop_back();
  }

  while (true) {
    // each iteration of this while loop tries to descend
    // down node "cur", looking for key kcur

process:
    uint64_t version = cur->stable_version();
    if (unlikely(node::IsDeleting(version)))
      // XXX: maybe we can only retry at the parent of this node, not the
      // root node of the b-tree *layer*
      goto retry;
    if (leaf_node *leaf = AsLeafCheck(cur, version)) {
      leaf->prefetch();
      if (search_info) {
        search_info->first = leaf;
        search_info->second = node::Version(version);
      }
      key_search_ret kret = leaf->key_search(kslice, kslicelen);
      ssize_t ret = kret.first;
      if (ret != -1) {
        // found
        leaf_node::value_or_node_ptr vn = leaf->values[ret];
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
          v = vn.v;
          return true;
        }

        // search the next layer
        cur = vn.n;
        kcur = kcur.shift();
        kslice = kcur.slice();
        kslicelen = min(kcur.size(), size_t(9));
        continue;
      }

      // leaf might have lost responsibility for key k during the descend. we
      // need to check this and adjust accordingly
      if (unlikely(kslice < leaf->min_key)) {
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
        if (unlikely(!right_sibling)) {
          leaf_nodes.push_back(leaf);
          return false;
        }
        right_sibling->prefetch();
        uint64_t right_version = right_sibling->stable_version();
        key_slice right_min_key = right_sibling->min_key;
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
        cur = internal->children[ret + 1];
      else
        cur = internal->children[0];
      if (unlikely(!internal->check_version(version)))
        goto process;
      INVARIANT(kret.second);
    }
  }
}

struct btree::leaf_kvinfo {
  key_slice key; // in host endian
  key_slice key_big_endian;
  leaf_node::value_or_node_ptr vn;
  bool layer;
  size_t length;
  varkey suffix;
  leaf_kvinfo() {} // for STL
  leaf_kvinfo(btree::key_slice key,
              leaf_node::value_or_node_ptr vn,
              bool layer,
              size_t length,
              const varkey &suffix)
    : key(key), key_big_endian(big_endian_trfm<key_slice>()(key)),
      vn(vn), layer(layer), length(length), suffix(suffix)
  {}

  inline const char *
  keyslice() const
  {
    return (const char *) &key_big_endian;
  }
};

class string_restore {
public:
  inline string_restore(btree::string_type &s, size_t n)
    : s(&s), n(n) {}
  inline ~string_restore()
  {
    s->resize(n);
  }
private:
  btree::string_type *s;
  size_t n;
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
bool
btree::search_range_at_layer(
    leaf_node *leaf,
    string_type &prefix,
    const key_type &lower,
    bool inc_lower,
    const key_type *upper,
    low_level_search_range_callback &callback) const
{
  VERBOSE(cerr << "search_range_at_layer: prefix.size()=" << prefix.size() << endl);

  key_slice last_keyslice = 0;
  size_t last_keyslice_len = 0;
  bool emitted_last_keyslice = false;
  if (!inc_lower) {
    last_keyslice = lower.slice();
    last_keyslice_len = min(lower.size(), size_t(9));
    emitted_last_keyslice = true;
  }

  key_slice lower_slice = lower.slice();
  key_slice next_key = lower_slice;
  const size_t prefix_size = prefix.size();
  // NB: DON'T CALL RESERVE()
  //prefix.reserve(prefix_size + 8); // allow for next layer
  const uint64_t upper_slice = upper ? upper->slice() : 0;
  string_restore restorer(prefix, prefix_size);
  while (!upper || next_key <= upper_slice) {
    leaf->prefetch();

    typename vec<leaf_kvinfo>::type buf;
    const uint64_t version = leaf->stable_version();
    key_slice leaf_min_key = leaf->min_key;
    if (leaf_min_key > next_key) {
      // go left
      leaf_node *left_sibling = leaf->prev;
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
      if ((leaf->keys[i] > lower_slice ||
           (leaf->keys[i] == lower_slice &&
            leaf->keyslice_length(i) >= min(lower.size(), size_t(9)))) &&
          (!upper || leaf->keys[i] <= upper_slice))
        buf.emplace_back(
            leaf->keys[i],
            leaf->values[i],
            leaf->value_is_layer(i),
            leaf->keyslice_length(i),
            leaf->suffix(i));
    }

    leaf_node *const right_sibling = leaf->next;
    key_slice leaf_max_key = right_sibling ? right_sibling->min_key : 0;

    if (unlikely(!leaf->check_version(version)))
      continue;

    callback.on_resp_node(leaf, node::Version(version));

    for (size_t i = 0; i < buf.size(); i++) {
      // check to see if we already omitted a key <= buf[i]: if so, don't omit it
      if (emitted_last_keyslice &&
          ((buf[i].key < last_keyslice) ||
           (buf[i].key == last_keyslice && buf[i].length <= last_keyslice_len)))
        continue;
      const size_t ncpy = min(buf[i].length, size_t(8));
      // XXX: prefix.end() calls _M_leak()
      //prefix.replace(prefix.begin() + prefix_size, prefix.end(), buf[i].keyslice(), ncpy);
      prefix.replace(prefix_size, string_type::npos, buf[i].keyslice(), ncpy);
      if (buf[i].layer) {
        // recurse into layer
        leaf_node *const next_layer = leftmost_descend_layer(buf[i].vn.n);
        varkey zerokey;
        if (emitted_last_keyslice && last_keyslice == buf[i].key)
          // NB(stephentu): this is implied by the filter above
          INVARIANT(last_keyslice_len <= 8);
        if (!search_range_at_layer(next_layer, prefix, zerokey, false, NULL, callback))
          return false;
      } else {
        // check if we are before the start
        if (buf[i].key == lower_slice) {
          if (buf[i].length <= 8) {
            if (buf[i].length < lower.size())
              // skip
              continue;
          } else {
            INVARIANT(buf[i].length == 9);
            if (lower.size() > 8 && buf[i].suffix < lower.shift())
              // skip
              continue;
          }
        }

        // check if we are after the end
        if (upper && buf[i].key == upper_slice) {
          if (buf[i].length == 9) {
            if (upper->size() <= 8)
              break;
            if (buf[i].suffix >= upper->shift())
              break;
          } else if (buf[i].length >= upper->size()) {
            break;
          }
        }
        if (buf[i].length == 9)
          prefix.append((const char *) buf[i].suffix.data(), buf[i].suffix.size());
        // we give the actual version # minus all the other bits, b/c they are not
        // important here and make comparison easier at higher layers
        if (!callback.invoke(prefix, buf[i].vn.v, leaf, node::Version(version)))
          return false;
      }
      last_keyslice = buf[i].key;
      last_keyslice_len = buf[i].length;
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

void
btree::search_range_call(const key_type &lower,
                         const key_type *upper,
                         low_level_search_range_callback &callback,
                         string_type *buf) const
{
  if (unlikely(upper && *upper <= lower))
    return;
  typename vec<leaf_node *>::type leaf_nodes;
  value_type v = 0;
  scoped_rcu_region rcu_region;
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

// XXX: requires that root_location is a stable memory location
bool
btree::insert_impl(node **root_location, const key_type &k, value_type v,
                   bool only_if_absent, value_type *old_v,
                   versioned_node_t *insert_info)
{
retry:
  key_slice mk;
  node *ret;
  typename vec<insert_parent_entry>::type parents;
  typename vec<node *>::type locked_nodes;
  scoped_rcu_region rcu_region;
  node *local_root = *root_location;
  insert_status status =
    insert0(local_root, k, v, only_if_absent, old_v, insert_info,
            mk, ret, parents, locked_nodes);
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
    new_root->children[0] = local_root;
    new_root->children[1] = ret;
    new_root->keys[0] = mk;
    new_root->set_key_slots_used(1);
    new_root->set_root();
    local_root->clear_root();
    COMPILER_MEMORY_FENCE;
    *root_location = new_root;
    // locks are still held here
    UnlockNodes(locked_nodes);
    return true;
  }
  ALWAYS_ASSERT(false);
  return false;
}

// XXX: like insert(), requires a stable memory location
bool
btree::remove_impl(node **root_location, const key_type &k, value_type *old_v)
{
retry:
  key_slice new_key;
  node *replace_node = NULL;
  typename vec<remove_parent_entry>::type parents;
  typename vec<node *>::type locked_nodes;
  scoped_rcu_region rcu_region;
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
    UnlockNodes(locked_nodes);
    return true;
  default:
    ALWAYS_ASSERT(false);
    return false;
  }
  ALWAYS_ASSERT(false);
  return false;
}

btree::leaf_node *
btree::leftmost_descend_layer(node *n) const
{
  node *cur = n;
  while (true) {
    if (leaf_node *leaf = AsLeafCheck(cur))
      return leaf;
    internal_node *internal = AsInternal(cur);
    uint64_t version = cur->stable_version();
    node *child = internal->children[0];
    if (unlikely(!internal->check_version(version)))
      continue;
    cur = child;
  }
}

void
btree::tree_walk(tree_walk_callback &callback) const
{
  // XXX(stephentu): try to release RCU region every once in a while
  scoped_rcu_region rcu_region;
  vector<node *> q;
  // XXX: not sure if cast is safe
  q.push_back((node *) root);
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
      vector<node *> layers;
      for (size_t i = 0; i < n; i++)
        if (leaf->value_is_layer(i))
          layers.push_back(leaf->values[i].n);
      leaf_node *next = leaf->next;
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

void
btree::size_walk_callback::on_node_begin(const node_opaque_t *n)
{
  INVARIANT(n->is_leaf_node());
  INVARIANT(spec_size == 0);
  const leaf_node *leaf = (const leaf_node *) n;
  const size_t sz = leaf->key_slots_used();
  for (size_t i = 0; i < sz; i++)
    if (!leaf->value_is_layer(i))
      spec_size++;
}

void
btree::size_walk_callback::on_node_success()
{
  size += spec_size;
  spec_size = 0;
}

void
btree::size_walk_callback::on_node_failure()
{
  spec_size = 0;
}

btree::insert_status
btree::insert0(node *np,
               const key_type &k,
               value_type v,
               bool only_if_absent,
               value_type *old_v,
               versioned_node_t *insert_info,
               key_slice &min_key,
               node *&new_node,
               typename vec<insert_parent_entry>::type &parents,
               typename vec<node *>::type &locked_nodes)
{
  uint64_t kslice = k.slice();
  size_t kslicelen = min(k.size(), size_t(9));

  np->prefetch();
  if (leaf_node *leaf = AsLeafCheck(np)) {
    // locked nodes are acquired bottom to top
    INVARIANT(locked_nodes.empty());

    leaf->lock();
    locked_nodes.push_back(leaf);

    // now we need to ensure that this leaf node still has
    // responsibility for k, before we proceed
    if (unlikely(leaf->is_deleting()))
      return UnlockAndReturn(locked_nodes, I_RETRY);
    if (unlikely(kslice < leaf->min_key))
      return UnlockAndReturn(locked_nodes, I_RETRY);
    if (likely(leaf->next)) {
      uint64_t right_version = leaf->next->stable_version();
      key_slice right_min_key = leaf->next->min_key;
      if (unlikely(!leaf->next->check_version(right_version)))
        return UnlockAndReturn(locked_nodes, I_RETRY);
      if (unlikely(kslice >= right_min_key))
        return UnlockAndReturn(locked_nodes, I_RETRY);
    }

    // we know now that leaf is responsible for kslice, so we can proceed

    // use 0 for slice length, so we can a pointer <= all elements
    // with the same slice
    key_search_ret kret = leaf->key_lower_bound_search(kslice, 0);
    ssize_t ret = kret.first;
    size_t n = kret.second;

    // count the number of values already here with the same kslice.
    size_t nslice = 0;
    ssize_t lenmatch = -1;
    ssize_t lenlowerbound = ret;
    for (size_t i = (ret == -1 ? 0 : ret); i < n; i++) {
      if (leaf->keys[i] < kslice) {
        continue;
      } else if (leaf->keys[i] == kslice) {
        nslice++;
        size_t kslicelen0 = leaf->keyslice_length(i);
        if (kslicelen0 <= kslicelen) {
          INVARIANT(lenmatch == -1);
          lenlowerbound = i;
          if (kslicelen0 == kslicelen)
            lenmatch = i;
        }
      } else {
        break;
      }
    }

    // len match case
    if (lenmatch != -1) {
      // exact match case
      if (kslicelen <= 8 ||
          (!leaf->value_is_layer(lenmatch) &&
           leaf->suffix(lenmatch) == k.shift())) {
        // easy case- we don't modify the node itself
        if (old_v)
          *old_v = leaf->values[lenmatch].v;
        if (!only_if_absent)
          leaf->values[lenmatch].v = v;
        if (insert_info)
          insert_info->first = 0;
        return UnlockAndReturn(locked_nodes, I_NONE_NOMOD);
      }
      INVARIANT(kslicelen == 9);
      if (leaf->value_is_layer(lenmatch)) {
        // need to insert in next level btree (using insert_impl())
        node **root_location = &leaf->values[lenmatch].n;
        bool ret = insert_impl(root_location, k.shift(), v, only_if_absent, old_v, insert_info);
        return UnlockAndReturn(locked_nodes, ret ? I_NONE_MOD : I_NONE_NOMOD);
      } else {
        INVARIANT(leaf->suffixes); // b/c lenmatch != -1 and this is not a layer
        // need to create a new btree layer, and add both existing key and
        // new key to it

        // XXX: need to mark modifying because we cannot change both the
        // value and the type atomically
        leaf->mark_modifying();

        leaf_node *new_root = leaf_node::alloc();
#ifdef CHECK_INVARIANTS
        new_root->lock();
        new_root->mark_modifying();
#endif /* CHECK_INVARIANTS */
        new_root->set_root();
        varkey old_slice(leaf->suffix(lenmatch));
        new_root->keys[0] = old_slice.slice();
        new_root->values[0] = leaf->values[lenmatch];
        new_root->keyslice_set_length(0, min(old_slice.size(), size_t(9)), false);
        new_root->inc_key_slots_used();
        if (new_root->keyslice_length(0) == 9) {
          new_root->alloc_suffixes();
          rcu_imstring i(old_slice.data() + 8, old_slice.size() - 8);
          new_root->suffixes[0].swap(i);
        }
        leaf->values[lenmatch].n = new_root;
        {
          rcu_imstring i;
          leaf->suffixes[lenmatch].swap(i);
        }
        leaf->value_set_layer(lenmatch);
        node **root_location = &leaf->values[lenmatch].n;
#ifdef CHECK_INVARIANTS
        new_root->unlock();
#endif /* CHECK_INVARIANTS */
        bool ret = insert_impl(root_location, k.shift(), v, only_if_absent, old_v, insert_info);
        if (!ret)
          INVARIANT(false);
        return UnlockAndReturn(locked_nodes, I_NONE_MOD);
      }
    }

    // lenlowerbound + 1 is the slot (0-based index) we want the new key to go
    // into, in the leaf node
    if (n < NKeysPerNode) {
      // also easy case- we only need to make local modifications
      leaf->mark_modifying();

      sift_right(leaf->keys, lenlowerbound + 1, n);
      leaf->keys[lenlowerbound + 1] = kslice;
      sift_right(leaf->values, lenlowerbound + 1, n);
      leaf->values[lenlowerbound + 1].v = v;
      sift_right(leaf->lengths, lenlowerbound + 1, n);
      leaf->keyslice_set_length(lenlowerbound + 1, kslicelen, false);
      if (leaf->suffixes)
        sift_swap_right(leaf->suffixes, lenlowerbound + 1, n);
      if (kslicelen == 9) {
        leaf->ensure_suffixes();
        rcu_imstring i(k.data() + 8, k.size() - 8);
        leaf->suffixes[lenlowerbound + 1].swap(i);
      } else if (leaf->suffixes) {
        rcu_imstring i;
        leaf->suffixes[lenlowerbound + 1].swap(i);
      }
      leaf->inc_key_slots_used();

//#ifdef CHECK_INVARIANTS
//      leaf->base_invariant_unique_keys_check();
//#endif
      if (insert_info) {
        insert_info->first = leaf;
        insert_info->second = node::Version(leaf->unstable_version()); // we hold lock on leaf
      }
      return UnlockAndReturn(locked_nodes, I_NONE_MOD);
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
        //INVARIANT(leaf == root);
      } else {
        for (typename vec<insert_parent_entry>::type::reverse_iterator rit = parents.rbegin();
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
      leaf->mark_modifying();

      leaf_node *new_leaf = leaf_node::alloc();
      new_leaf->prefetch();

#ifdef CHECK_INVARIANTS
      new_leaf->lock();
      new_leaf->mark_modifying();
      locked_nodes.push_back(new_leaf);
#endif /* CHECK_INVARIANTS */

      if (!leaf->next && leaf->keys[n - 1] < kslice) {
        // sequential insert optimization- in this case, we don't bother
        // splitting the node. instead, keep the current leaf node full, and
        // insert the new key into the new leaf node (violating the btree invariant)
        //
        // this optimization is commonly implemented, including in masstree and
        // berkeley db- w/o this optimization, sequential inserts leave the all
        // nodes half full

        new_leaf->keys[0] = kslice;
        new_leaf->values[0].v = v;
        new_leaf->keyslice_set_length(0, kslicelen, false);
        if (kslicelen == 9) {
          new_leaf->alloc_suffixes();
          rcu_imstring i(k.data() + 8, k.size() - 8);
          new_leaf->suffixes[0].swap(i);
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
          if (likely(leaf->keys[i] != leaf->keys[left_split_point]))
            break;
          left_split_point--;
        }
        INVARIANT(left_split_point <= NKeysPerNode);
        INVARIANT(left_split_point == 0 || leaf->keys[left_split_point - 1] != leaf->keys[left_split_point]);

        right_split_point = NKeysPerNode / 2;
        for (ssize_t i = right_split_point - 1; i >= 0 && i < ssize_t(NKeysPerNode) - 1; i++) {
          if (likely(leaf->keys[i] != leaf->keys[right_split_point]))
            break;
          right_split_point++;
        }
        INVARIANT(right_split_point <= NKeysPerNode);
        INVARIANT(right_split_point == 0 || leaf->keys[right_split_point - 1] != leaf->keys[right_split_point]);

        size_t split_point;
        if (min(left_split_point, NKeysPerNode - left_split_point) <
            min(right_split_point, NKeysPerNode - right_split_point))
          split_point = right_split_point;
        else
          split_point = left_split_point;

        if (split_point <= size_t(lenlowerbound + 1) && leaf->keys[split_point - 1] != kslice) {
          // put new key in new leaf (right)
          size_t pos = lenlowerbound + 1 - split_point;

          copy_into(&new_leaf->keys[0], leaf->keys, split_point, lenlowerbound + 1);
          new_leaf->keys[pos] = kslice;
          copy_into(&new_leaf->keys[pos + 1], leaf->keys, lenlowerbound + 1, NKeysPerNode);

          copy_into(&new_leaf->values[0], leaf->values, split_point, lenlowerbound + 1);
          new_leaf->values[pos].v = v;
          copy_into(&new_leaf->values[pos + 1], leaf->values, lenlowerbound + 1, NKeysPerNode);

          copy_into(&new_leaf->lengths[0], leaf->lengths, split_point, lenlowerbound + 1);
          new_leaf->keyslice_set_length(pos, kslicelen, false);
          copy_into(&new_leaf->lengths[pos + 1], leaf->lengths, lenlowerbound + 1, NKeysPerNode);

          if (leaf->suffixes) {
            new_leaf->ensure_suffixes();
            swap_with(&new_leaf->suffixes[0], leaf->suffixes, split_point, lenlowerbound + 1);
          }
          if (kslicelen == 9) {
            new_leaf->ensure_suffixes();
            rcu_imstring i(k.data() + 8, k.size() - 8);
            new_leaf->suffixes[pos].swap(i);
          } else if (new_leaf->suffixes) {
            rcu_imstring i;
            new_leaf->suffixes[pos].swap(i);
          }
          if (leaf->suffixes) {
            new_leaf->ensure_suffixes();
            swap_with(&new_leaf->suffixes[pos + 1], leaf->suffixes, lenlowerbound + 1, NKeysPerNode);
          }

          leaf->set_key_slots_used(split_point);
          new_leaf->set_key_slots_used(NKeysPerNode - split_point + 1);

#ifdef CHECK_INVARIANTS
          leaf->base_invariant_unique_keys_check();
          new_leaf->base_invariant_unique_keys_check();
          INVARIANT(leaf->keys[split_point - 1] < new_leaf->keys[0]);
#endif /* CHECK_INVARIANTS */

        } else {
          // XXX: not really sure if this invariant is true, but we rely
          // on it for now
          INVARIANT(size_t(lenlowerbound + 1) <= split_point);

          // put new key in original leaf
          copy_into(&new_leaf->keys[0], leaf->keys, split_point, NKeysPerNode);
          copy_into(&new_leaf->values[0], leaf->values, split_point, NKeysPerNode);
          copy_into(&new_leaf->lengths[0], leaf->lengths, split_point, NKeysPerNode);
          if (leaf->suffixes) {
            new_leaf->ensure_suffixes();
            swap_with(&new_leaf->suffixes[0], leaf->suffixes, split_point, NKeysPerNode);
          }

          sift_right(leaf->keys, lenlowerbound + 1, split_point);
          leaf->keys[lenlowerbound + 1] = kslice;
          sift_right(leaf->values, lenlowerbound + 1, split_point);
          leaf->values[lenlowerbound + 1].v = v;
          sift_right(leaf->lengths, lenlowerbound + 1, split_point);
          leaf->keyslice_set_length(lenlowerbound + 1, kslicelen, false);
          if (leaf->suffixes)
            sift_swap_right(leaf->suffixes, lenlowerbound + 1, split_point);
          if (kslicelen == 9) {
            leaf->ensure_suffixes();
            rcu_imstring i(k.data() + 8, k.size() - 8);
            leaf->suffixes[lenlowerbound + 1].swap(i);
          } else if (leaf->suffixes) {
            rcu_imstring i;
            leaf->suffixes[lenlowerbound + 1].swap(i);
          }

          leaf->set_key_slots_used(split_point + 1);
          new_leaf->set_key_slots_used(NKeysPerNode - split_point);

#ifdef CHECK_INVARIANTS
          leaf->base_invariant_unique_keys_check();
          new_leaf->base_invariant_unique_keys_check();
          INVARIANT(leaf->keys[split_point] < new_leaf->keys[0]);
#endif /* CHECK_INVARIANTS */
        }
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

      if (insert_info) {
        insert_info->first = leaf;
        insert_info->second = node::Version(leaf->unstable_version()); // we hold lock on leaf
      }

      return I_SPLIT;
    }
  } else {
    internal_node *internal = AsInternal(np);
    uint64_t version = internal->stable_version();
    if (unlikely(node::IsDeleting(version)))
      return UnlockAndReturn(locked_nodes, I_RETRY);
    key_search_ret kret = internal->key_lower_bound_search(kslice);
    ssize_t ret = kret.first;
    size_t n = kret.second;
    size_t child_idx = (ret == -1) ? 0 : ret + 1;
    node *child_ptr = internal->children[child_idx];
    if (unlikely(!internal->check_version(version)))
      return UnlockAndReturn(locked_nodes, I_RETRY);
    parents.push_back(insert_parent_entry(internal, version));
    key_slice mk = 0;
    node *new_child = NULL;
    insert_status status =
      insert0(child_ptr, k, v, only_if_absent, old_v, insert_info,
              mk, new_child, parents, locked_nodes);
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

/**
 * remove is very tricky to get right!
 */
btree::remove_status
btree::remove0(node *np,
               key_slice *min_key,
               key_slice *max_key,
               const key_type &k,
               value_type *old_v,
               node *left_node,
               node *right_node,
               key_slice &new_key,
               node *&replace_node,
               typename vec<remove_parent_entry>::type &parents,
               typename vec<node *>::type &locked_nodes)
{
  uint64_t kslice = k.slice();
  size_t kslicelen = min(k.size(), size_t(9));

  np->prefetch();
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
    if (unlikely(kslice < leaf->min_key))
      return UnlockAndReturn(locked_nodes, R_RETRY);
    if (likely(leaf->next)) {
      uint64_t right_version = leaf->next->stable_version();
      key_slice right_min_key = leaf->next->min_key;
      if (unlikely(!leaf->next->check_version(right_version)))
        return UnlockAndReturn(locked_nodes, R_RETRY);
      if (unlikely(kslice >= right_min_key))
        return UnlockAndReturn(locked_nodes, R_RETRY);
    }

    // we know now that leaf is responsible for k, so we can proceed

    key_search_ret kret = leaf->key_search(kslice, kslicelen);
    ssize_t ret = kret.first;
    if (ret == -1)
      return UnlockAndReturn(locked_nodes, R_NONE_NOMOD);
    if (kslicelen == 9) {
      if (leaf->value_is_layer(ret)) {
        node **root_location = &leaf->values[ret].n;
        bool ret = remove_impl(root_location, k.shift(), old_v);
        node *layer_n = *root_location;
        bool layer_leaf = layer_n->is_leaf_node();
        // XXX: could also re-merge back when layer size becomes 1, but
        // no need for now
        if (!layer_leaf || layer_n)
          return UnlockAndReturn(locked_nodes, ret ? R_NONE_MOD : R_NONE_NOMOD);
      } else {
        // suffix check
        if (leaf->suffix(ret) != k.shift())
          return UnlockAndReturn(locked_nodes, R_NONE_NOMOD);
      }
    }

    INVARIANT(!leaf->value_is_layer(ret));
    if (old_v)
      *old_v = leaf->values[ret].v;
    size_t n = kret.second;
    if (n > NMinKeysPerNode) {
      leaf->mark_modifying();
      remove_pos_from_leaf_node(leaf, ret, n);
      return UnlockAndReturn(locked_nodes, R_NONE_MOD);
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
        //INVARIANT(leaf == root);
      }

      for (typename vec<remove_parent_entry>::type::reverse_iterator rit = parents.rbegin();
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
          //INVARIANT(p == root);
        }
      }

      leaf->mark_modifying();

      if (right_sibling) {
        right_sibling->mark_modifying();
        size_t right_n = right_sibling->key_slots_used();
        if (right_n > NMinKeysPerNode) {
          // steal first contiguous key slices from right
          INVARIANT(right_sibling->keys[0] > leaf->keys[n - 1]);

          // indices [0, steal_point) will be taken from the right
          size_t steal_point = 1;
          for (size_t i = 0; i < right_n - 1; i++, steal_point++)
            if (likely(right_sibling->keys[i] != right_sibling->keys[steal_point]))
              break;

          INVARIANT(steal_point <= sizeof(key_slice) + 2);
          INVARIANT(steal_point <= right_n);

          // to steal, we need to ensure:
          // 1) we have enough room to steal
          // 2) the right sibling will not be empty after the steal
          if ((n - 1 + steal_point) <= NKeysPerNode && steal_point < right_n) {
            sift_left(leaf->keys, ret, n);
            copy_into(&leaf->keys[n - 1], right_sibling->keys, 0, steal_point);
            sift_left(leaf->values, ret, n);
            copy_into(&leaf->values[n - 1], right_sibling->values, 0, steal_point);
            sift_left(leaf->lengths, ret, n);
            copy_into(&leaf->lengths[n - 1], right_sibling->lengths, 0, steal_point);
            if (leaf->suffixes)
              sift_swap_left(leaf->suffixes, ret, n);
            if (right_sibling->suffixes) {
              leaf->ensure_suffixes();
              swap_with(&leaf->suffixes[n - 1], right_sibling->suffixes, 0, steal_point);
            }

            sift_left(right_sibling->keys, 0, right_n, steal_point);
            sift_left(right_sibling->values, 0, right_n, steal_point);
            sift_left(right_sibling->lengths, 0, right_n, steal_point);
            if (right_sibling->suffixes)
              sift_swap_left(right_sibling->suffixes, 0, right_n, steal_point);
            leaf->set_key_slots_used(n - 1 + steal_point);
            right_sibling->set_key_slots_used(right_n - steal_point);
            new_key = right_sibling->keys[0];
            right_sibling->min_key = new_key;

#ifdef CHECK_INVARIANTS
            leaf->base_invariant_unique_keys_check();
            right_sibling->base_invariant_unique_keys_check();
            INVARIANT(leaf->keys[n - 1 + steal_point - 1] < new_key);
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
        INVARIANT(right_sibling->keys[0] > leaf->keys[n - 1]);
        INVARIANT((right_n + (n - 1)) <= NKeysPerNode);

        sift_left(leaf->keys, ret, n);
        copy_into(&leaf->keys[n - 1], right_sibling->keys, 0, right_n);

        sift_left(leaf->values, ret, n);
        copy_into(&leaf->values[n - 1], right_sibling->values, 0, right_n);

        sift_left(leaf->lengths, ret, n);
        copy_into(&leaf->lengths[n - 1], right_sibling->lengths, 0, right_n);

        if (leaf->suffixes)
          sift_swap_left(leaf->suffixes, ret, n);

        if (right_sibling->suffixes) {
          leaf->ensure_suffixes();
          swap_with(&leaf->suffixes[n - 1], right_sibling->suffixes, 0, right_n);
        }

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
          INVARIANT(left_sibling->keys[left_n - 1] < leaf->keys[0]);

          // indices [steal_point, left_n) will be taken from the left
          size_t steal_point = left_n - 1;
          for (ssize_t i = steal_point - 1; i >= 0; i--, steal_point--)
            if (likely(left_sibling->keys[i] != left_sibling->keys[steal_point]))
              break;

          size_t nstolen = left_n - steal_point;
          INVARIANT(nstolen <= sizeof(key_slice) + 2);
          INVARIANT(steal_point < left_n);

          if ((n - 1 + nstolen) <= NKeysPerNode && steal_point > 0) {
            sift_right(leaf->keys, ret + 1, n, nstolen - 1);
            sift_right(leaf->keys, 0, ret, nstolen);
            copy_into(&leaf->keys[0], &left_sibling->keys[0], left_n - nstolen, left_n);

            sift_right(leaf->values, ret + 1, n, nstolen - 1);
            sift_right(leaf->values, 0, ret, nstolen);
            copy_into(&leaf->values[0], &left_sibling->values[0], left_n - nstolen, left_n);

            sift_right(leaf->lengths, ret + 1, n, nstolen - 1);
            sift_right(leaf->lengths, 0, ret, nstolen);
            copy_into(&leaf->lengths[0], &left_sibling->lengths[0], left_n - nstolen, left_n);

            if (leaf->suffixes) {
              sift_swap_right(leaf->suffixes, ret + 1, n, nstolen - 1);
              sift_swap_right(leaf->suffixes, 0, ret, nstolen);
            }
            if (left_sibling->suffixes) {
              leaf->ensure_suffixes();
              swap_with(&leaf->suffixes[0], &left_sibling->suffixes[0], left_n - nstolen, left_n);
            }

            left_sibling->set_key_slots_used(left_n - nstolen);
            leaf->set_key_slots_used(n - 1 + nstolen);
            new_key = leaf->keys[0];
            leaf->min_key = new_key;

#ifdef CHECK_INVARIANTS
            leaf->base_invariant_unique_keys_check();
            left_sibling->base_invariant_unique_keys_check();
            INVARIANT(left_sibling->keys[left_n - nstolen - 1] < new_key);
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
        INVARIANT(left_sibling->keys[left_n - 1] < leaf->keys[0]);
        INVARIANT((left_n + (n - 1)) <= NKeysPerNode);

        copy_into(&left_sibling->keys[left_n], leaf->keys, 0, ret);
        copy_into(&left_sibling->keys[left_n + ret], leaf->keys, ret + 1, n);

        copy_into(&left_sibling->values[left_n], leaf->values, 0, ret);
        copy_into(&left_sibling->values[left_n + ret], leaf->values, ret + 1, n);

        copy_into(&left_sibling->lengths[left_n], leaf->lengths, 0, ret);
        copy_into(&left_sibling->lengths[left_n + ret], leaf->lengths, ret + 1, n);

        if (leaf->suffixes) {
          left_sibling->ensure_suffixes();
          swap_with(&left_sibling->suffixes[left_n], leaf->suffixes, 0, ret);
          swap_with(&left_sibling->suffixes[left_n + ret], leaf->suffixes, ret + 1, n);
        }

        left_sibling->set_key_slots_used(left_n + (n - 1));
        left_sibling->next = leaf->next;
        if (leaf->next)
          leaf->next->prev = left_sibling;

        // see comments in right_sibling case above, for why one of them is INVARIANT and
        // the other is SINGLE_THREADED_INVARIANT
        INVARIANT(!left_sibling->next || left_sibling->next->prev == left_sibling);
        SINGLE_THREADED_INVARIANT(!left_sibling->prev || left_sibling->prev->next == left_sibling);

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
    if (unlikely(node::IsDeleting(version)))
      return UnlockAndReturn(locked_nodes, R_RETRY);
    key_search_ret kret = internal->key_lower_bound_search(kslice);
    ssize_t ret = kret.first;
    size_t n = kret.second;
    size_t child_idx = (ret == -1) ? 0 : ret + 1;
    node *child_ptr = internal->children[child_idx];
    key_slice *child_min_key = child_idx == 0 ? NULL : &internal->keys[child_idx - 1];
    key_slice *child_max_key = child_idx == n ? NULL : &internal->keys[child_idx];
    node *child_left_sibling = child_idx == 0 ? NULL : internal->children[child_idx - 1];
    node *child_right_sibling = child_idx == n ? NULL : internal->children[child_idx + 1];
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
        internal->keys[child_idx - 1] = nk;
        return UnlockAndReturn(locked_nodes, R_NONE_MOD);

      case R_STOLE_FROM_RIGHT:
        INVARIANT(internal->is_locked());
        INVARIANT(internal->is_lock_owner());
        internal->keys[child_idx] = nk;
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

          //INVARIANT(internal == root);
          INVARIANT(internal->is_root());
          remove_pos_from_internal_node(internal, del_key_idx, del_child_idx, n);
          INVARIANT(internal->key_slots_used() + 1 == n);
          if ((n - 1) == 0) {
            replace_node = internal->children[0];
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

/** end of btree impl - the rest is a bunch of testing code */

class scoped_rate_timer {
private:
  util::timer t;
  string region;
  size_t n;

public:
  scoped_rate_timer(const string &region, size_t n) : region(region), n(n)
  {}

  ~scoped_rate_timer()
  {
    double x = t.lap() / 1000.0; // ms
    double rate = double(n) / (x / 1000.0);
    cerr << "timed region `" << region << "' took " << x
              << " ms (" << rate << " events/sec)" << endl;
  }
};

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
    btr.insert(u64_varkey(i), (btree::value_type) i);
    btr.invariant_checker();

    btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(u64_varkey(i), v));
    ALWAYS_ASSERT(v == (btree::value_type) i);
  }
  ALWAYS_ASSERT(btr.size() == btree::NKeysPerNode);

  // induce a split
  btr.insert(u64_varkey(btree::NKeysPerNode), (btree::value_type) (btree::NKeysPerNode));
  btr.invariant_checker();
  ALWAYS_ASSERT(btr.size() == btree::NKeysPerNode + 1);

  // now make sure we can find everything post split
  for (size_t i = 0; i < btree::NKeysPerNode + 1; i++) {
    btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(u64_varkey(i), v));
    ALWAYS_ASSERT(v == (btree::value_type) i);
  }

  // now fill up the new root node
  const size_t n = (btree::NKeysPerNode + btree::NKeysPerNode * (btree::NMinKeysPerNode));
  for (size_t i = btree::NKeysPerNode + 1; i < n; i++) {
    btr.insert(u64_varkey(i), (btree::value_type) i);
    btr.invariant_checker();

    btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(u64_varkey(i), v));
    ALWAYS_ASSERT(v == (btree::value_type) i);
  }
  ALWAYS_ASSERT(btr.size() == n);

  // cause the root node to split
  btr.insert(u64_varkey(n), (btree::value_type) n);
  btr.invariant_checker();
  ALWAYS_ASSERT(btr.size() == n + 1);

  // once again make sure we can find everything
  for (size_t i = 0; i < n + 1; i++) {
    btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(u64_varkey(i), v));
    ALWAYS_ASSERT(v == (btree::value_type) i);
  }
}

static void
test2()
{
  btree btr;
  const size_t n = 1000;
  for (size_t i = 0; i < n; i += 2) {
    btr.insert(u64_varkey(i), (btree::value_type) i);
    btr.invariant_checker();

    btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(u64_varkey(i), v));
    ALWAYS_ASSERT(v == (btree::value_type) i);
  }

  for (size_t i = 1; i < n; i += 2) {
    btr.insert(u64_varkey(i), (btree::value_type) i);
    btr.invariant_checker();

    btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(u64_varkey(i), v));
    ALWAYS_ASSERT(v == (btree::value_type) i);
  }

  ALWAYS_ASSERT(btr.size() == n);
}

static void
test3()
{
  btree btr;

  for (size_t i = 0; i < btree::NKeysPerNode * 2; i++) {
    btr.insert(u64_varkey(i), (btree::value_type) i);
    btr.invariant_checker();

    btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(u64_varkey(i), v));
    ALWAYS_ASSERT(v == (btree::value_type) i);
  }
  ALWAYS_ASSERT(btr.size() == btree::NKeysPerNode * 2);

  for (size_t i = 0; i < btree::NKeysPerNode * 2; i++) {
    btr.remove(u64_varkey(i));
    btr.invariant_checker();

    btree::value_type v = 0;
    ALWAYS_ASSERT(!btr.search(u64_varkey(i), v));
  }
  ALWAYS_ASSERT(btr.size() == 0);

  for (size_t i = 0; i < btree::NKeysPerNode * 2; i++) {
    btr.insert(u64_varkey(i), (btree::value_type) i);
    btr.invariant_checker();

    btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(u64_varkey(i), v));
    ALWAYS_ASSERT(v == (btree::value_type) i);
  }
  ALWAYS_ASSERT(btr.size() == btree::NKeysPerNode * 2);

  for (ssize_t i = btree::NKeysPerNode * 2 - 1; i >= 0; i--) {
    btr.remove(u64_varkey(i));
    btr.invariant_checker();

    btree::value_type v = 0;
    ALWAYS_ASSERT(!btr.search(u64_varkey(i), v));
  }
  ALWAYS_ASSERT(btr.size() == 0);

  for (size_t i = 0; i < btree::NKeysPerNode * 2; i++) {
    btr.insert(u64_varkey(i), (btree::value_type) i);
    btr.invariant_checker();

    btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(u64_varkey(i), v));
    ALWAYS_ASSERT(v == (btree::value_type) i);
  }
  ALWAYS_ASSERT(btr.size() == btree::NKeysPerNode * 2);

  for (ssize_t i = btree::NKeysPerNode; i >= 0; i--) {
    btr.remove(u64_varkey(i));
    btr.invariant_checker();

    btree::value_type v = 0;
    ALWAYS_ASSERT(!btr.search(u64_varkey(i), v));
  }

  for (size_t i = btree::NKeysPerNode + 1; i < btree::NKeysPerNode * 2; i++) {
    btr.remove(u64_varkey(i));
    btr.invariant_checker();

    btree::value_type v = 0;
    ALWAYS_ASSERT(!btr.search(u64_varkey(i), v));
  }
  ALWAYS_ASSERT(btr.size() == 0);
}

static void
test4()
{
  btree btr;
  const size_t nkeys = 10000;
  for (size_t i = 0; i < nkeys; i++) {
    btr.insert(u64_varkey(i), (btree::value_type) i);
    btr.invariant_checker();
    btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(u64_varkey(i), v));
    ALWAYS_ASSERT(v == (btree::value_type) i);
  }
  ALWAYS_ASSERT(btr.size() == nkeys);

  srand(12345);

  for (size_t i = 0; i < nkeys; i++) {
    size_t k = rand() % nkeys;
    btr.remove(u64_varkey(k));
    btr.invariant_checker();
    btree::value_type v = 0;
    ALWAYS_ASSERT(!btr.search(u64_varkey(k), v));
  }

  for (size_t i = 0; i < nkeys; i++) {
    btr.remove(u64_varkey(i));
    btr.invariant_checker();
    btree::value_type v = 0;
    ALWAYS_ASSERT(!btr.search(u64_varkey(i), v));
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
    set<size_t> s;
    for (size_t i = 0; i < nkeys; i++) {
      size_t k = rand() % nkeys;
      s.insert(k);
      btr.insert(u64_varkey(k), (btree::value_type) k);
      btr.invariant_checker();
      btree::value_type v = 0;
      ALWAYS_ASSERT(btr.search(u64_varkey(k), v));
      ALWAYS_ASSERT(v == (btree::value_type) k);
    }
    ALWAYS_ASSERT(btr.size() == s.size());

    for (size_t i = 0; i < nkeys * 2; i++) {
      size_t k = rand() % nkeys;
      btr.remove(u64_varkey(k));
      btr.invariant_checker();
      btree::value_type v = 0;
      ALWAYS_ASSERT(!btr.search(u64_varkey(k), v));
    }

    // clean it up
    for (size_t i = 0; i < nkeys; i++) {
      btr.remove(u64_varkey(i));
      btr.invariant_checker();
      btree::value_type v = 0;
      ALWAYS_ASSERT(!btr.search(u64_varkey(i), v));
    }

    ALWAYS_ASSERT(btr.size() == 0);
  }
}

namespace test6_ns {
  struct scan_callback {
    typedef vector<
      pair< btree::string_type, btree::value_type > > kv_vec;
    scan_callback(kv_vec *data) : data(data) {}
    inline bool
    operator()(const btree::string_type &k, btree::value_type v) const
    {
      data->push_back(make_pair(k, v));
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
    btr.insert(u64_varkey(i), (btree::value_type) i);
  btr.invariant_checker();
  ALWAYS_ASSERT(btr.size() == nkeys);

  using namespace test6_ns;

  scan_callback::kv_vec data;
  u64_varkey max_key(600);
  btr.search_range(u64_varkey(500), &max_key, scan_callback(&data));
  ALWAYS_ASSERT(data.size() == 100);
  for (size_t i = 0; i < 100; i++) {
    ALWAYS_ASSERT(varkey(data[i].first) == u64_varkey(500 + i));
    ALWAYS_ASSERT(data[i].second == (btree::value_type) (500 + i));
  }

  data.clear();
  btr.search_range(u64_varkey(500), NULL, scan_callback(&data));
  ALWAYS_ASSERT(data.size() == 500);
  for (size_t i = 0; i < 500; i++) {
    ALWAYS_ASSERT(varkey(data[i].first) == u64_varkey(500 + i));
    ALWAYS_ASSERT(data[i].second == (btree::value_type) (500 + i));
  }
}

static void
test7()
{
  btree btr;
  ALWAYS_ASSERT(!btr.remove(u64_varkey(0)));
  ALWAYS_ASSERT(btr.insert(u64_varkey(0), (btree::value_type) 0));
  ALWAYS_ASSERT(!btr.insert(u64_varkey(0), (btree::value_type) 1));
  btree::value_type v;
  ALWAYS_ASSERT(btr.search(u64_varkey(0), v));
  ALWAYS_ASSERT(v == (btree::value_type) 1);
  ALWAYS_ASSERT(!btr.insert_if_absent(u64_varkey(0), (btree::value_type) 2));
  ALWAYS_ASSERT(btr.search(u64_varkey(0), v));
  ALWAYS_ASSERT(v == (btree::value_type) 1);
  ALWAYS_ASSERT(btr.remove(u64_varkey(0)));
  ALWAYS_ASSERT(btr.insert_if_absent(u64_varkey(0), (btree::value_type) 2));
  ALWAYS_ASSERT(btr.search(u64_varkey(0), v));
  ALWAYS_ASSERT(v == (btree::value_type) 2);
}

static void
test_varlen_single_layer()
{
  btree btr;

  const char *k0 = "a";
  const char *k1 = "aa";
  const char *k2 = "aaa";
  const char *k3 = "aaaa";
  const char *k4 = "aaaaa";

  const char *keys[] = {k0, k1, k2, k3, k4};
  for (size_t i = 0; i < ARRAY_NELEMS(keys); i++) {
    ALWAYS_ASSERT(btr.insert(varkey(keys[i]), (btree::value_type) keys[i]));
    btr.invariant_checker();
  }

  ALWAYS_ASSERT(btr.size() == ARRAY_NELEMS(keys));
  for (size_t i = 0; i < ARRAY_NELEMS(keys); i++) {
    btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(varkey(keys[i]), v));
    ALWAYS_ASSERT(strcmp((const char *) v, keys[i]) == 0);
  }

  for (size_t i = 0; i < ARRAY_NELEMS(keys); i++) {
    ALWAYS_ASSERT(btr.remove(varkey(keys[i])));
    btr.invariant_checker();
  }
  ALWAYS_ASSERT(btr.size() == 0);
}

static void
test_varlen_multi_layer()
{
  btree btr;

  const char *k0 = "aaaaaaa";
  const char *k1 = "aaaaaaaa";
  const char *k2 = "aaaaaaaaa";
  const char *k3 = "aaaaaaaaaa";
  const char *k4 = "aaaaaaaaaaa";
  const char *k5 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
  const char *keys[] = {k0, k1, k2, k3, k4, k5};

  for (size_t i = 0; i < ARRAY_NELEMS(keys); i++) {
    ALWAYS_ASSERT(btr.insert(varkey(keys[i]), (btree::value_type) keys[i]));
    btr.invariant_checker();
  }

  ALWAYS_ASSERT(btr.size() == ARRAY_NELEMS(keys));
  for (size_t i = 0; i < ARRAY_NELEMS(keys); i++) {
    btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(varkey(keys[i]), v));
    ALWAYS_ASSERT(strcmp((const char *) v, keys[i]) == 0);
  }

  for (size_t i = 0; i < ARRAY_NELEMS(keys); i++) {
    ALWAYS_ASSERT(btr.remove(varkey(keys[i])));
    btr.invariant_checker();
  }
  ALWAYS_ASSERT(btr.size() == 0);
}

static void
test_two_layer()
{
  const char *k0 = "aaaaaaaaa";
  const char *k1 = "aaaaaaaaaa";

  btree btr;
  ALWAYS_ASSERT(btr.insert(varkey(k0), (btree::value_type) k0));
  ALWAYS_ASSERT(btr.insert(varkey(k1), (btree::value_type) k1));
  ALWAYS_ASSERT(btr.size() == 2);
}

class test_range_scan_helper : public btree::search_range_callback {
public:

  struct expect {
    expect() : tag(), expected_size() {}
    expect(size_t expected_size)
      : tag(0), expected_size(expected_size) {}
    expect(const set<btree::string_type> &expected_keys)
      : tag(1), expected_keys(expected_keys) {}
    uint8_t tag;
    size_t expected_size;
    set<btree::string_type> expected_keys;
  };

  enum ExpectType {
    EXPECT_EXACT,
    EXPECT_ATLEAST,
  };

  test_range_scan_helper(
    btree &btr,
    const btree::key_type &begin,
    const btree::key_type *end,
    const expect &expectation,
    ExpectType ex_type = EXPECT_EXACT)
    : btr(&btr), begin(begin), end(end ? new btree::key_type(*end) : NULL),
      expectation(expectation), ex_type(ex_type)
  {
  }

  ~test_range_scan_helper()
  {
    if (end)
      delete end;
  }

  virtual bool
  invoke(const btree::string_type &k, btree::value_type v)
  {
    VERBOSE(cerr << "test_range_scan_helper::invoke(): received key(size="
                 << k.size() << "): " << hexify(k) << endl);
    if (!keys.empty())
      ALWAYS_ASSERT(keys.back() < k);
    keys.push_back(k);
    return true;
  }

  void test()
  {
    keys.clear();
    btr->search_range_call(begin, end, *this);
    if (expectation.tag == 0) {
      switch (ex_type) {
      case EXPECT_EXACT:
        ALWAYS_ASSERT(keys.size() == expectation.expected_size);
        break;
      case EXPECT_ATLEAST:
        ALWAYS_ASSERT(keys.size() >= expectation.expected_size);
        break;
      }
    } else {
      switch (ex_type) {
      case EXPECT_EXACT: {
        ALWAYS_ASSERT(keys.size() == expectation.expected_keys.size());
        vector<btree::string_type> cmp(
            expectation.expected_keys.begin(), expectation.expected_keys.end());
        for (size_t i = 0; i < keys.size(); i++) {
          if (keys[i] != cmp[i]) {
            cerr << "A: " << hexify(keys[i]) << endl;
            cerr << "B: " << hexify(cmp[i]) << endl;
            ALWAYS_ASSERT(false);
          }
        }
        break;
      }
      case EXPECT_ATLEAST: {
        ALWAYS_ASSERT(keys.size() >= expectation.expected_keys.size());
        // every key in the expected set must be present
        set<btree::string_type> keyset(keys.begin(), keys.end());
        for (set<btree::string_type>::iterator it = expectation.expected_keys.begin();
             it != expectation.expected_keys.end(); ++it)
          ALWAYS_ASSERT(keyset.count(*it) == 1);
        break;
      }
      }
    }
  }

private:
  btree *const btr;
  btree::key_type begin;
  btree::key_type *end;
  expect expectation;
  ExpectType ex_type;

  vector<btree::string_type> keys;
};

static void
test_two_layer_range_scan()
{
  const char *keys[] = {
    "a",
    "aaaaaaaa",
    "aaaaaaaaa",
    "aaaaaaaaaa",
    "aaaaaaaaaaa",
    "b", "c", "d", "e", "f", "g", "h", "i", "j", "k",
    "l", "m", "n", "o", "p", "q", "r", "s",
  };

  btree btr;
  for (size_t i = 0; i < ARRAY_NELEMS(keys); i++) {
    ALWAYS_ASSERT(btr.insert(varkey(keys[i]), (btree::value_type) keys[i]));
    btr.invariant_checker();
  }

  test_range_scan_helper::expect ex(set<btree::string_type>(keys, keys + ARRAY_NELEMS(keys)));
  test_range_scan_helper tester(btr, varkey(""), NULL, ex);
  tester.test();
}

static void
test_multi_layer_scan()
{
  const uint8_t lokey_cstr[] = {
    0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x45, 0x49, 0x4E, 0x47,
    0x41, 0x54, 0x49, 0x4F, 0x4E, 0x45, 0x49, 0x4E, 0x47, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00
  };
  const uint8_t hikey_cstr[] = {
    0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x45, 0x49, 0x4E, 0x47,
    0x41, 0x54, 0x49, 0x4F, 0x4E, 0x45, 0x49, 0x4E, 0x47, 0x00, 0x00, 0x00,
    0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
    0xFF, 0xFF, 0xFF, 0xFF
  };
  const string lokey_s((const char *) &lokey_cstr[0], ARRAY_NELEMS(lokey_cstr));
  const string hikey_s((const char *) &hikey_cstr[0], ARRAY_NELEMS(hikey_cstr));

  string lokey_s_next(lokey_s);
  lokey_s_next.resize(lokey_s_next.size() + 1);

  const varkey hikey(hikey_s);

  btree btr;
  ALWAYS_ASSERT(btr.insert(varkey(lokey_s), (btree::value_type) 0x123));

  test_range_scan_helper::expect ex(0);
  test_range_scan_helper tester(btr, varkey(lokey_s_next), &hikey, ex);
  tester.test();
}

static void
test_null_keys()
{
  const uint8_t k0[] = {};
  const uint8_t k1[] = {'\0'};
  const uint8_t k2[] = {'\0', '\0'};
  const uint8_t k3[] = {'\0', '\0', '\0'};
  const uint8_t k4[] = {'\0', '\0', '\0', '\0'};
  const uint8_t k5[] = {'\0', '\0', '\0', '\0', '\0'};
  const uint8_t k6[] = {'\0', '\0', '\0', '\0', '\0', '\0'};
  const uint8_t k7[] = {'\0', '\0', '\0', '\0', '\0', '\0', '\0'};
  const uint8_t k8[] = {'\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0'};
  const uint8_t k9[] = {'\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0'};
  const uint8_t k10[] = {'\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0', '\0'};
  const uint8_t *keys[] = {k0, k1, k2, k3, k4, k5, k6, k7, k8, k9, k10};

  btree btr;

  for (size_t i = 0; i < ARRAY_NELEMS(keys); i++) {
    ALWAYS_ASSERT(btr.insert(varkey(keys[i], i), (btree::value_type) i));
    btr.invariant_checker();
  }

  for (size_t i = 0; i < ARRAY_NELEMS(keys); i++) {
    btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(varkey(keys[i], i), v));
    ALWAYS_ASSERT(v == (btree::value_type) i);
  }

  for (size_t i = 1; i <= 20; i++) {
    ALWAYS_ASSERT(btr.insert(u64_varkey(i), (btree::value_type) i));
    btr.invariant_checker();
  }

  for (size_t i = 0; i < ARRAY_NELEMS(keys); i++) {
    btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(varkey(keys[i], i), v));
    ALWAYS_ASSERT(v == (btree::value_type) i);
  }

  for (size_t i = 1; i <= 20; i++) {
    btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(u64_varkey(i), v));
    ALWAYS_ASSERT(v == (btree::value_type) i);
  }
}

static void
test_null_keys_2()
{
  const size_t nprefixes = 200;

  btree btr;

  fast_random r(9084398309893);

  set<btree::string_type> prefixes;
  for (size_t i = 0; i < nprefixes; i++) {
  retry:
    const btree::string_type k = r.next_string(r.next() % 30);
    if (prefixes.count(k) == 1)
      goto retry;
    prefixes.insert(k);
  }

  set<btree::string_type> keys;
  for (set<btree::string_type>::iterator it = prefixes.begin();
       it != prefixes.end(); ++it) {
    const btree::string_type k = *it;
    for (size_t i = 1; i <= 12; i++) {
      btree::string_type x = k;
      x.resize(x.size() + i);
      keys.insert(x);
    }
  }

  size_t ctr = 1;
  for (set<btree::string_type>::iterator it = keys.begin();
       it != keys.end(); ++it, ++ctr) {
    ALWAYS_ASSERT(btr.insert(varkey(*it), (btree::value_type) it->data()));
    btr.invariant_checker();
    ALWAYS_ASSERT(btr.size() == ctr);
  }
  ALWAYS_ASSERT(btr.size() == keys.size());

  for (set<btree::string_type>::iterator it = keys.begin();
       it != keys.end(); ++it) {
    btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(varkey(*it), v));
    ALWAYS_ASSERT(v == (btree::value_type) it->data());
  }

  test_range_scan_helper::expect ex(keys);
  test_range_scan_helper tester(btr, varkey(*keys.begin()), NULL, ex);
  tester.test();

  ctr = keys.size() - 1;
  for (set<btree::string_type>::iterator it = keys.begin();
       it != keys.end(); ++it, --ctr) {
    ALWAYS_ASSERT(btr.remove(varkey(*it)));
    btr.invariant_checker();
    ALWAYS_ASSERT(btr.size() == ctr);
  }
  ALWAYS_ASSERT(btr.size() == 0);
}

static void
test_random_keys()
{
  btree btr;
  fast_random r(43698);

  const size_t nkeys = 10000;
  const unsigned int maxkeylen = 1000;

  set<btree::string_type> keyset;
  vector<btree::string_type> keys;
  keys.resize(nkeys);
  for (size_t i = 0; i < nkeys; i++) {
  retry:
    btree::string_type k = r.next_string(r.next() % (maxkeylen + 1));
    if (keyset.count(k) == 1)
      goto retry;
    keyset.insert(k);
    swap(keys[i], k);
    btr.insert(varkey(keys[i]), (btree::value_type) keys[i].data());
    btr.invariant_checker();
  }

  ALWAYS_ASSERT(btr.size() == keyset.size());

  for (size_t i = 0; i < nkeys; i++) {
    btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(varkey(keys[i]), v));
    ALWAYS_ASSERT(v == (btree::value_type) keys[i].data());
  }

  test_range_scan_helper::expect ex(keyset);
  test_range_scan_helper tester(btr, varkey(""), NULL, ex);
  tester.test();

  for (size_t i = 0; i < nkeys; i++) {
    btr.remove(varkey(keys[i]));
    btr.invariant_checker();
  }
  ALWAYS_ASSERT(btr.size() == 0);
}

static void
test_insert_remove_mix()
{
  btree btr;
  fast_random r(38953623328597);

  // bootstrap with keys, then alternate insert/remove
  const size_t nkeys_start = 100000;

  vector<string> start_keys_v;
  set<string> start_keys;
  for (size_t i = 0; i < nkeys_start; i++) {
  retry:
    string k = r.next_string(r.next() % 200);
    if (start_keys.count(k) == 1)
      goto retry;
    start_keys_v.push_back(k);
    start_keys.insert(k);
    ALWAYS_ASSERT(btr.insert(varkey(k), (btree::value_type) k.data()));
  }
  btr.invariant_checker();
  ALWAYS_ASSERT(btr.size() == start_keys.size());

  vector<string> insert_keys_v;
  set<string> insert_keys;
  for (size_t i = 0; i < nkeys_start; i++) {
  retry1:
    string k = r.next_string(r.next() % 200);
    if (start_keys.count(k) == 1 || insert_keys.count(k) == 1)
      goto retry1;
    insert_keys_v.push_back(k);
    insert_keys.insert(k);
  }

  for (size_t i = 0; i < nkeys_start; i++) {
    ALWAYS_ASSERT(btr.remove(varkey(start_keys_v[i])));
    ALWAYS_ASSERT(btr.insert(varkey(insert_keys_v[i]), (btree::value_type) insert_keys_v[i].data()));
  }
  btr.invariant_checker();
  ALWAYS_ASSERT(btr.size() == insert_keys.size());
}

namespace mp_test1_ns {

  static const size_t nkeys = 20000;

  class ins0_worker : public btree_worker {
  public:
    ins0_worker(btree &btr) : btree_worker(btr) {}
    virtual void run()
    {
      for (size_t i = 0; i < nkeys / 2; i++)
        btr->insert(u64_varkey(i), (btree::value_type) i);
    }
  };

  class ins1_worker : public btree_worker {
  public:
    ins1_worker(btree &btr) : btree_worker(btr) {}
    virtual void run()
    {
      for (size_t i = nkeys / 2; i < nkeys; i++)
        btr->insert(u64_varkey(i), (btree::value_type) i);
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
    ALWAYS_ASSERT(btr.search(u64_varkey(i), v));
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
        btr->remove(u64_varkey(i));
    }
  };

  class rm1_worker : public btree_worker {
  public:
    rm1_worker(btree &btr) : btree_worker(btr) {}
    virtual void run()
    {
      for (size_t i = nkeys / 2; i < nkeys; i++)
        btr->remove(u64_varkey(i));
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
    btr.insert(u64_varkey(u64_varkey(i)), (btree::value_type) i);
  btr.invariant_checker();

  rm0_worker w0(btr);
  rm1_worker w1(btr);

  w0.start(); w1.start();
  w0.join(); w1.join();

  btr.invariant_checker();
  for (size_t i = 0; i < nkeys; i++) {
    btree::value_type v = 0;
    ALWAYS_ASSERT(!btr.search(u64_varkey(i), v));
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
        btr->remove(u64_varkey(i));
    }
  };

  class ins0_worker : public btree_worker {
  public:
    ins0_worker(btree &btr) : btree_worker(btr) {}
    virtual void run()
    {
      // insert the odd keys
      for (size_t i = 1; i < nkeys; i += 2)
        btr->insert(u64_varkey(i), (btree::value_type) i);
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
    btr.insert(u64_varkey(u64_varkey(i)), (btree::value_type) i);
  btr.invariant_checker();

  rm0_worker w0(btr);
  ins0_worker w1(btr);

  w0.start(); w1.start();
  w0.join(); w1.join();

  btr.invariant_checker();

  // should find no even keys
  for (size_t i = 0; i < nkeys; i += 2) {
    btree::value_type v = 0;
    ALWAYS_ASSERT(!btr.search(u64_varkey(i), v));
  }

  // should find all odd keys
  for (size_t i = 1; i < nkeys; i += 2) {
    btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(u64_varkey(i), v));
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
        ALWAYS_ASSERT(btr->search(u64_varkey(i), v));
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
        btr->insert(u64_varkey(i), (btree::value_type) i);
    }
  };

  class rm0_worker : public btree_worker {
  public:
    rm0_worker(btree &btr) : btree_worker(btr) {}
    virtual void run()
    {
      // remove and reinsert odd keys
      for (size_t i = 1; i < nkeys; i += 2) {
        btr->remove(u64_varkey(i));
        btr->insert(u64_varkey(i), (btree::value_type) i);
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
    btr.insert(u64_varkey(u64_varkey(i)), (btree::value_type) i);
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
    ALWAYS_ASSERT(btr.search(u64_varkey(i), v));
    ALWAYS_ASSERT(v == (btree::value_type) i);
  }

  ALWAYS_ASSERT(btr.size() == nkeys);
}

namespace mp_test_pinning_ns {
  static const size_t keys_per_thread = 1000;
  static const size_t nthreads = 4;
  static atomic<bool> running(true);
  class worker : public btree_worker {
  public:
    worker(unsigned int thread, btree &btr) : btree_worker(btr), thread(thread) {}
    virtual void
    run()
    {
      rcu::pin_current_thread(thread % coreid::num_cpus_online());
      for (unsigned mode = 0; running.load(); mode++) {
        for (size_t i = thread * keys_per_thread;
             running.load() && i < (thread + 1) * keys_per_thread;
             i++) {
          if (mode % 2) {
            // remove
            btr->remove(u64_varkey(i));
          } else {
            // insert
            btr->insert(u64_varkey(i), (btree::value_type) i);
          }
        }
      }
    }
  private:
    unsigned int thread;
  };
}

static void
mp_test_pinning()
{
  using namespace mp_test_pinning_ns;
  btree btr;
  vector<shared_ptr<worker>> workers;
  for (size_t i = 0; i < nthreads; i++)
    workers.emplace_back(new worker(i, btr));
  for (auto &p : workers)
    p->start();
  sleep(5);
  running.store(false);
  for (auto &p : workers)
    p->join();
  btr.invariant_checker();
}

namespace mp_test5_ns {

  static const size_t niters = 100000;
  static const size_t max_key = 45;

  typedef set<btree::key_slice> key_set;

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
        btree::key_slice k = rand_r(&s) % max_key;
        if (choice < 0.6) {
          btree::value_type v = 0;
          if (btr->search(u64_varkey(k), v))
            ALWAYS_ASSERT(v == (btree::value_type) k);
        } else if (choice < 0.9) {
          btr->insert(u64_varkey(k), (btree::value_type) k);
          sum.inserts.insert(k);
        } else {
          btr->remove(u64_varkey(k));
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

  cerr << "num_inserts: " << inserts.size() << endl;
  cerr << "num_removes: " << removes.size() << endl;

  for (key_set::iterator it = inserts.begin(); it != inserts.end(); ++it) {
    if (removes.count(*it) == 1)
      continue;
    btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(u64_varkey(*it), v));
    ALWAYS_ASSERT(v == (btree::value_type) *it);
  }

  btr.invariant_checker();
  cerr << "btr size: " << btr.size() << endl;
}

namespace mp_test6_ns {
  static const size_t nthreads = 16;
  static const size_t ninsertkeys_perthread = 100000;
  static const size_t nremovekeys_perthread = 100000;

  typedef vector<btree::key_slice> key_vec;

  class insert_worker : public btree_worker {
  public:
    insert_worker(const vector<btree::key_slice> &keys, btree &btr)
      : btree_worker(btr), keys(keys) {}
    virtual void run()
    {
      for (size_t i = 0; i < keys.size(); i++)
        btr->insert(u64_varkey(keys[i]), (btree::value_type) keys[i]);
    }
  private:
    vector<btree::key_slice> keys;
  };

  class remove_worker : public btree_worker {
  public:
    remove_worker(const vector<btree::key_slice> &keys, btree &btr)
      : btree_worker(btr), keys(keys) {}
    virtual void run()
    {
      for (size_t i = 0; i < keys.size(); i++)
        btr->remove(u64_varkey(keys[i]));
    }
  private:
    vector<btree::key_slice> keys;
  };
}

static void
mp_test6()
{
  using namespace mp_test6_ns;

  btree btr;
  vector<key_vec> inps;
  set<unsigned long> insert_keys, remove_keys;

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
      btr.insert(u64_varkey(k), (btree::value_type) k);
      remove_keys.insert(k);
      inp.push_back(k);
      j++;
    }
    inps.push_back(inp);
  }

  vector<btree_worker*> workers;
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
  for (set<unsigned long>::iterator it = insert_keys.begin();
       it != insert_keys.end(); ++it) {
    btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(u64_varkey(*it), v));
    ALWAYS_ASSERT(v == (btree::value_type) *it);
  }
  for (set<unsigned long>::iterator it = remove_keys.begin();
       it != remove_keys.end(); ++it) {
    btree::value_type v = 0;
    ALWAYS_ASSERT(!btr.search(u64_varkey(*it), v));
  }

  for (size_t i = 0; i < nthreads; i++)
    delete workers[i];
}

namespace mp_test7_ns {
  static const size_t nkeys = 50;
  static volatile bool running = false;

  typedef vector<btree::key_slice> key_vec;

  struct scan_callback {
    typedef vector<
      pair< btree::string_type, btree::value_type > > kv_vec;
    scan_callback(kv_vec *data) : data(data) {}
    inline bool
    operator()(const btree::string_type &k, btree::value_type v) const
    {
      //ALWAYS_ASSERT(data->empty() || data->back().first < k.str());
      if (!data->empty() && data->back().first >= k) {
        cerr << "prev: " << hexify(data->back().first) << endl;
        cerr << "cur : " << hexify(k) << endl;
        ALWAYS_ASSERT(false);
      }
      data->push_back(make_pair(k, v));
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
        uint64_t k = keys[r.next() % keys.size()];
        btree::value_type v = NULL;
        ALWAYS_ASSERT(btr->search(u64_varkey(k), v));
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
        btr->search_range(u64_varkey(nkeys / 2), NULL, scan_callback(&data));
        set<btree::string_type> scan_keys;
        btree::string_type prev;
        for (size_t i = 0; i < data.size(); i++) {
          if (i != 0) {
            ALWAYS_ASSERT(data[i].first != prev);
            ALWAYS_ASSERT(data[i].first > prev);
          }
          scan_keys.insert(data[i].first);
          prev = data[i].first;
        }
        for (size_t i = 0; i < keys.size(); i++) {
          if (keys[i] < (nkeys / 2))
            continue;
          ALWAYS_ASSERT(scan_keys.count(u64_varkey(keys[i]).str()) == 1);
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
      for (size_t i = 0; running; i = (i + 1) % keys.size(), insert = !insert) {
        if (insert)
          btr->insert(u64_varkey(keys[i]), (btree::value_type) keys[i]);
        else
          btr->remove(u64_varkey(keys[i]));
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
    btr.insert(u64_varkey(lookup_keys[i]), (btree::value_type) lookup_keys[i]);
  btr.invariant_checker();

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

namespace mp_test8_ns {
  static const size_t nthreads = 16;
  static const size_t ninsertkeys_perthread = 100000;
  static const size_t nremovekeys_perthread = 100000;

  typedef vector<string> key_vec;

  class insert_worker : public btree_worker {
  public:
    insert_worker(const vector<string> &keys, btree &btr)
      : btree_worker(btr), keys(keys) {}
    virtual void run()
    {
      for (size_t i = 0; i < keys.size(); i++)
        ALWAYS_ASSERT(btr->insert(varkey(keys[i]), (btree::value_type) keys[i].data()));
    }
  private:
    vector<string> keys;
  };

  class remove_worker : public btree_worker {
  public:
    remove_worker(const vector<string> &keys, btree &btr)
      : btree_worker(btr), keys(keys) {}
    virtual void run()
    {
      for (size_t i = 0; i < keys.size(); i++)
        ALWAYS_ASSERT(btr->remove(varkey(keys[i])));
    }
  private:
    vector<string> keys;
  };
}

static void
mp_test8()
{
  using namespace mp_test8_ns;

  btree btr;
  vector<key_vec> inps;
  set<string> insert_keys, remove_keys;

  fast_random r(83287583);
  for (size_t i = 0; i < nthreads / 2; i++) {
    key_vec inp;
    for (size_t j = 0; j < ninsertkeys_perthread; j++) {
    retry:
      string k = r.next_string(r.next() % 200);
      if (insert_keys.count(k) == 1)
        goto retry;
      insert_keys.insert(k);
      inp.push_back(k);
    }
    inps.push_back(inp);
  }
  for (size_t i = nthreads / 2; i < nthreads; i++) {
    key_vec inp;
    for (size_t j = 0; j < nremovekeys_perthread;) {
      string k = r.next_string(r.next() % 200);
      if (insert_keys.count(k) == 1 || remove_keys.count(k) == 1)
        continue;
      ALWAYS_ASSERT(btr.insert(varkey(k), (btree::value_type) k.data()));
      remove_keys.insert(k);
      inp.push_back(k);
      j++;
    }
    inps.push_back(inp);
  }

  btr.invariant_checker();

  vector<btree_worker*> workers;
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
  for (set<string>::iterator it = insert_keys.begin();
       it != insert_keys.end(); ++it) {
    btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(varkey(*it), v));
  }
  for (set<string>::iterator it = remove_keys.begin();
       it != remove_keys.end(); ++it) {
    btree::value_type v = 0;
    ALWAYS_ASSERT(!btr.search(varkey(*it), v));
  }

  for (size_t i = 0; i < nthreads; i++)
    delete workers[i];
}

namespace mp_test_long_keys_ns {
  static const size_t nthreads = 16;
  static const size_t ninsertkeys_perthread = 500000;
  static const size_t nremovekeys_perthread = 500000;

  typedef vector<btree::string_type> key_vec;

  class insert_worker : public btree_worker {
  public:
    insert_worker(const vector<btree::string_type> &keys, btree &btr)
      : btree_worker(btr), keys(keys) {}
    virtual void run()
    {
      for (size_t i = 0; i < keys.size(); i++)
        ALWAYS_ASSERT(btr->insert(varkey(keys[i]), (btree::value_type) keys[i].data()));
    }
  private:
    vector<btree::string_type> keys;
  };

  class remove_worker : public btree_worker {
  public:
    remove_worker(const vector<btree::string_type> &keys, btree &btr)
      : btree_worker(btr), keys(keys) {}
    virtual void run()
    {
      for (size_t i = 0; i < keys.size(); i++)
        ALWAYS_ASSERT(btr->remove(varkey(keys[i])));
    }
  private:
    vector<btree::string_type> keys;
  };

  static volatile bool running = false;

  class scan_worker : public btree_worker {
  public:
    scan_worker(const set<btree::string_type> &ex, btree &btr)
      : btree_worker(btr), ex(ex) {}
    virtual void run()
    {
      while (running) {
        test_range_scan_helper tester(*btr, varkey(""), NULL, ex, test_range_scan_helper::EXPECT_ATLEAST);
        tester.test();
      }
    }
  private:
    test_range_scan_helper::expect ex;
  };
}

static void
mp_test_long_keys()
{
  // all keys at least 9-bytes long
  using namespace mp_test_long_keys_ns;

  btree btr;
  vector<key_vec> inps;
  set<btree::string_type> existing_keys, insert_keys, remove_keys;

  fast_random r(189230589352);
  for (size_t i = 0; i < 10000; i++) {
  retry0:
    btree::string_type k = r.next_string((r.next() % 200) + 9);
    if (existing_keys.count(k) == 1)
      goto retry0;
    existing_keys.insert(k);
    ALWAYS_ASSERT(btr.insert(varkey(k), (btree::value_type) k.data()));
  }
  ALWAYS_ASSERT(btr.size() == existing_keys.size());

  for (size_t i = 0; i < nthreads / 2; i++) {
    key_vec inp;
    for (size_t j = 0; j < ninsertkeys_perthread; j++) {
    retry:
      btree::string_type k = r.next_string((r.next() % 200) + 9);
      if (insert_keys.count(k) == 1 || existing_keys.count(k) == 1)
        goto retry;
      insert_keys.insert(k);
      inp.push_back(k);
    }
    inps.push_back(inp);
  }

  for (size_t i = nthreads / 2; i < nthreads; i++) {
    key_vec inp;
    for (size_t j = 0; j < nremovekeys_perthread;) {
      btree::string_type k = r.next_string((r.next() % 200) + 9);
      if (insert_keys.count(k) == 1 || existing_keys.count(k) == 1 || remove_keys.count(k) == 1)
        continue;
      ALWAYS_ASSERT(btr.insert(varkey(k), (btree::value_type) k.data()));
      remove_keys.insert(k);
      inp.push_back(k);
      j++;
    }
    inps.push_back(inp);
  }

  ALWAYS_ASSERT(btr.size() == (insert_keys.size() + existing_keys.size()));
  btr.invariant_checker();

  vector<btree_worker*> workers, running_workers;
  running = true;
  for (size_t i = 0; i < nthreads / 2; i++)
    workers.push_back(new insert_worker(inps[i], btr));
  for (size_t i = nthreads / 2; i < nthreads; i++)
    workers.push_back(new remove_worker(inps[i], btr));
  for (size_t i = 0; i < 4; i++)
    running_workers.push_back(new scan_worker(existing_keys, btr));
  for (size_t i = 0; i < nthreads; i++)
    workers[i]->start();
  for (size_t i = 0; i < running_workers.size(); i++)
    running_workers[i]->start();
  for (size_t i = 0; i < nthreads; i++)
    workers[i]->join();
  running = false;
  for (size_t i = 0; i < running_workers.size(); i++)
    running_workers[i]->join();

  btr.invariant_checker();

  ALWAYS_ASSERT(btr.size() == (insert_keys.size() + existing_keys.size()));
  for (set<btree::string_type>::iterator it = insert_keys.begin();
       it != insert_keys.end(); ++it) {
    btree::value_type v = 0;
    ALWAYS_ASSERT(btr.search(varkey(*it), v));
  }
  for (set<btree::string_type>::iterator it = remove_keys.begin();
       it != remove_keys.end(); ++it) {
    btree::value_type v = 0;
    ALWAYS_ASSERT(!btr.search(varkey(*it), v));
  }

  for (size_t i = 0; i < nthreads; i++)
    delete workers[i];
  for (size_t i = 0; i < running_workers.size(); i++)
    delete running_workers[i];
}

static void perf_test() UNUSED;
static void
perf_test()
{
  const size_t nrecs = 10000000;
  const size_t nlookups = 10000000;

  {
    srand(9876);
    map<uint64_t, uint64_t> m;
    {
      scoped_rate_timer t("map insert", nrecs);
      for (size_t i = 0; i < nrecs; i++)
        m[i] = i;
    }
    {
      scoped_rate_timer t("map random lookups", nlookups);
      for (size_t i = 0; i < nlookups; i++) {
        //uint64_t key = rand() % nrecs;
        uint64_t key = i;
        map<uint64_t, uint64_t>::iterator it =
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
        btr.insert(u64_varkey(u64_varkey(i)), (btree::value_type) i);
    }
    {
      scoped_rate_timer t("btree random lookups", nlookups);
      for (size_t i = 0; i < nlookups; i++) {
        //uint64_t key = rand() % nrecs;
        uint64_t key = i;
        btree::value_type v = 0;
        ALWAYS_ASSERT(btr.search(u64_varkey(key), v));
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
        btree::key_slice k = r.next() % nkeys;
        btree::value_type v = 0;
        ALWAYS_ASSERT(btr->search(u64_varkey(k), v));
        ALWAYS_ASSERT(v == (btree::value_type) k);
        n++;
      }
    }
    uint64_t n;
  private:
    unsigned int seed;
  };
}

static void read_only_perf_test() UNUSED;
static void
read_only_perf_test()
{
  using namespace read_only_perf_test_ns;

  btree btr;

  for (size_t i = 0; i < nkeys; i++)
    btr.insert(u64_varkey(i), (btree::value_type) i);
  cerr << "btree loaded, test starting" << endl;

  vector<worker *> workers;
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

  cerr << "agg_read_throughput: " << agg_throughput << " gets/sec" << endl;
  cerr << "avg_per_core_read_throughput: " << avg_per_core_throughput << " gets/sec/core" << endl;
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
        btree::key_slice k = r.next() % nkeys;
        btr->insert(u64_varkey(k), (btree::value_type) k);
      }
    }
  private:
    unsigned int seed;
  };
}

static void write_only_perf_test() UNUSED;
static void
write_only_perf_test()
{
  using namespace write_only_perf_test_ns;

  btree btr;

  vector<worker *> workers;
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

  cerr << "agg_write_throughput: " << agg_throughput << " puts/sec" << endl;
  cerr << "avg_per_core_write_throughput: " << avg_per_core_throughput << " puts/sec/core" << endl;
}

void
btree::TestFast()
{
  test1();
  test2();
  test3();
  test4();
  test6();
  test7();
  test_varlen_single_layer();
  test_varlen_multi_layer();
  test_two_layer();
  test_two_layer_range_scan();
  test_multi_layer_scan();
  test_null_keys();
  test_null_keys_2();
  test_random_keys();
  test_insert_remove_mix();
  mp_test_pinning();
  cout << "btree::TestFast passed" << endl;
}

void
btree::TestSlow()
{
  test5();
  mp_test1();
  mp_test2();
  mp_test3();
  mp_test4();
  mp_test5();
  mp_test6();
  mp_test7();
  mp_test8();
  mp_test_long_keys();
  //perf_test();
  //read_only_perf_test();
  //write_only_perf_test();
  cout << "btree::TestSlow passed" << endl;
}

string
btree::NodeStringify(const node_opaque_t *n)
{
  vector<string> keys;
  for (size_t i = 0; i < n->key_slots_used(); i++)
    keys.push_back(string("0x") + hexify(n->keys[i]));

  ostringstream b;
  b << "node[v=" << node::VersionInfoStr(n->unstable_version())
    << ", keys=" << format_list(keys.begin(), keys.end())
    ;

  if (n->is_leaf_node()) {
    const leaf_node *leaf = AsLeaf(n);
    vector<string> lengths;
    for (size_t i = 0; i < leaf->key_slots_used(); i++) {
      ostringstream inf;
      inf << "<l=" << leaf->keyslice_length(i) << ",is_layer=" << leaf->value_is_layer(i) << ">";
      lengths.push_back(inf.str());
    }
    b << ", lengths=" << format_list(lengths.begin(), lengths.end());
  } else {
    //const internal_node *internal = AsInternal(n);
    // nothing for now
  }

  b << "]";
  return b.str();
}

vector< pair<btree::value_type, bool> >
btree::ExtractValues(const node_opaque_t *n)
{
  vector< pair<value_type, bool> > ret;
  if (!n->is_leaf_node())
    return ret;
  const leaf_node *leaf = (const leaf_node *) n;
  const size_t sz = leaf->key_slots_used();
  for (size_t i = 0; i < sz; i++)
    if (!leaf->value_is_layer(i))
      ret.push_back(make_pair(leaf->values[i].v, leaf->keyslice_length(i) > 8));
  return ret;
}
