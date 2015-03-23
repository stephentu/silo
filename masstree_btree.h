// -*- c-basic-offset: 2 -*-
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

#include "masstree/masstree_scan.hh"
#include "masstree/masstree_insert.hh"
#include "masstree/masstree_remove.hh"
#include "masstree/masstree_print.hh"
#include "masstree/timestamp.hh"
#include "masstree/mtcounters.hh"
#include "masstree/circular_int.hh"

class simple_threadinfo {
 public:
    simple_threadinfo()
        : ts_(0) { // XXX?
    }
    class rcu_callback {
    public:
      virtual void operator()(simple_threadinfo& ti) = 0;
    };

 private:
    static inline void rcu_callback_function(void* p) {
      simple_threadinfo ti;
      static_cast<rcu_callback*>(p)->operator()(ti);
    }

 public:
    // XXX Correct node timstamps are needed for recovery, but for no other
    // reason.
    kvtimestamp_t operation_timestamp() const {
      return 0;
    }
    kvtimestamp_t update_timestamp() const {
	return ts_;
    }
    kvtimestamp_t update_timestamp(kvtimestamp_t x) const {
	if (circular_int<kvtimestamp_t>::less_equal(ts_, x))
	    // x might be a marker timestamp; ensure result is not
	    ts_ = (x | 1) + 1;
	return ts_;
    }
    kvtimestamp_t update_timestamp(kvtimestamp_t x, kvtimestamp_t y) const {
	if (circular_int<kvtimestamp_t>::less(x, y))
	    x = y;
	if (circular_int<kvtimestamp_t>::less_equal(ts_, x))
	    // x might be a marker timestamp; ensure result is not
	    ts_ = (x | 1) + 1;
	return ts_;
    }
    void increment_timestamp() {
	ts_ += 2;
    }
    void advance_timestamp(kvtimestamp_t x) {
	if (circular_int<kvtimestamp_t>::less(ts_, x))
	    ts_ = x;
    }

    // event counters
    void mark(threadcounter) {
    }
    void mark(threadcounter, int64_t) {
    }
    bool has_counter(threadcounter) const {
        return false;
    }
    uint64_t counter(threadcounter ci) const {
	return 0;
    }

    /** @brief Return a function object that calls mark(ci); relax_fence().
     *
     * This function object can be used to count the number of relax_fence()s
     * executed. */
    relax_fence_function accounting_relax_fence(threadcounter) {
	return relax_fence_function();
    }

    class accounting_relax_fence_function {
    public:
      template <typename V>
      void operator()(V) {
        relax_fence();
      }
    };
    /** @brief Return a function object that calls mark(ci); relax_fence().
     *
     * This function object can be used to count the number of relax_fence()s
     * executed. */
    accounting_relax_fence_function stable_fence() {
	return accounting_relax_fence_function();
    }

    relax_fence_function lock_fence(threadcounter) {
	return relax_fence_function();
    }

    // memory allocation
    void* allocate(size_t sz, memtag) {
        return rcu::s_instance.alloc(sz);
    }
    void deallocate(void* p, size_t sz, memtag) {
	// in C++ allocators, 'p' must be nonnull
        rcu::s_instance.dealloc(p, sz);
    }
    void deallocate_rcu(void *p, size_t sz, memtag) {
	assert(p);
        rcu::s_instance.dealloc_rcu(p, sz);
    }

    void* pool_allocate(size_t sz, memtag) {
	int nl = (sz + CACHE_LINE_SIZE - 1) / CACHE_LINE_SIZE;
        return rcu::s_instance.alloc(nl * CACHE_LINE_SIZE);
    }
    void pool_deallocate(void* p, size_t sz, memtag) {
	int nl = (sz + CACHE_LINE_SIZE - 1) / CACHE_LINE_SIZE;
        rcu::s_instance.dealloc(p, nl * CACHE_LINE_SIZE);
    }
    void pool_deallocate_rcu(void* p, size_t sz, memtag) {
	assert(p);
	int nl = (sz + CACHE_LINE_SIZE - 1) / CACHE_LINE_SIZE;
        rcu::s_instance.dealloc_rcu(p, nl * CACHE_LINE_SIZE);
    }

    // RCU
    void rcu_register(rcu_callback *cb) {
      scoped_rcu_base<false> guard;
      rcu::s_instance.free_with_fn(cb, rcu_callback_function);
    }

  private:
    mutable kvtimestamp_t ts_;
};

struct masstree_params : public Masstree::nodeparams<> {
  typedef uint8_t* value_type;
  typedef Masstree::value_print<value_type> value_print_type;
  typedef simple_threadinfo threadinfo_type;
  enum { RcuRespCaller = true };
};

struct masstree_single_threaded_params : public masstree_params {
  static constexpr bool concurrent = false;
};

template <typename P>
class mbtree {
 public:
  typedef Masstree::node_base<P> node_base_type;
  typedef Masstree::internode<P> internode_type;
  typedef Masstree::leaf<P> leaf_type;
  typedef Masstree::leaf<P> node_type;
  typedef typename node_base_type::nodeversion_type nodeversion_type;

  typedef varkey key_type;
  typedef lcdf::Str string_type;
  typedef uint64_t key_slice;
  typedef typename P::value_type value_type;
  typedef typename P::threadinfo_type threadinfo;
  typedef typename std::conditional<!P::RcuRespCaller,
      scoped_rcu_region,
      disabled_rcu_region>::type rcu_region;

  // public to assist in testing
  static const unsigned int NKeysPerNode    = P::leaf_width;
  static const unsigned int NMinKeysPerNode = P::leaf_width / 2;

  // XXX(stephentu): trying out a very opaque node API for now
  typedef node_type node_opaque_t;
  typedef std::pair< const node_opaque_t *, uint64_t > versioned_node_t;
  struct insert_info_t {
    const node_opaque_t* node;
    uint64_t old_version;
    uint64_t new_version;
  };

  void invariant_checker() {} // stub for now

#ifdef BTREE_LOCK_OWNERSHIP_CHECKING
public:
  static inline void
  NodeLockRegionBegin()
  {
    // XXX: implement me
    ALWAYS_ASSERT(false);
    //ownership_checker<mbtree<P>, node_base_type>::NodeLockRegionBegin();
  }
  static inline void
  AssertAllNodeLocksReleased()
  {
    // XXX: implement me
    ALWAYS_ASSERT(false);
    //ownership_checker<mbtree<P>, node_base_type>::AssertAllNodeLocksReleased();
  }
private:
  static inline void
  AddNodeToLockRegion(const node_base_type *n)
  {
    // XXX: implement me
    ALWAYS_ASSERT(false);
    //ownership_checker<mbtree<P>, node_base_type>::AddNodeToLockRegion(n);
  }
public:
#endif

  mbtree() {
    threadinfo ti;
    table_.initialize(ti);
  }

  ~mbtree() {
    rcu_region guard;
    threadinfo ti;
    table_.destroy(ti);
  }

  /**
   * NOT THREAD SAFE
   */
  inline void clear() {
    rcu_region guard;
    threadinfo ti;
    table_.destroy(ti);
    table_.initialize(ti);
  }

  /** Note: invariant checking is not thread safe */
  inline void invariant_checker() const {
  }

          /** NOTE: the public interface assumes that the caller has taken care
           * of setting up RCU */

  inline bool search(const key_type &k, value_type &v,
                     versioned_node_t *search_info = nullptr) const;

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
                    std::string *buf = nullptr) const;

  // (lower, upper]
  void
  rsearch_range_call(const key_type &upper,
                     const key_type *lower,
                     low_level_search_range_callback &callback,
                     std::string *buf = nullptr) const;

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

  /**
   * [lower, *upper)
   *
   * Callback is expected to implement bool operator()(key_slice k, value_type v),
   * where the callback returns true if it wants to keep going, false otherwise
   */
  template <typename F>
  inline void
  search_range(const key_type &lower,
               const key_type *upper,
               F& callback,
               std::string *buf = nullptr) const;

  /**
   * (*lower, upper]
   *
   * Callback is expected to implement bool operator()(key_slice k, value_type v),
   * where the callback returns true if it wants to keep going, false otherwise
   */
  template <typename F>
  inline void
  rsearch_range(const key_type &upper,
                const key_type *lower,
                F& callback,
                std::string *buf = nullptr) const;

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
         insert_info_t *insert_info = NULL);

  /**
   * Only puts k=>v if k does not exist in map. returns true
   * if k inserted, false otherwise (k exists already)
   */
  inline bool
  insert_if_absent(const key_type &k, value_type v,
                   insert_info_t *insert_info = NULL);

  /**
   * return true if a value was removed, false otherwise.
   *
   * if true and old_v is not NULL, then the removed value of v
   * is written into old_v
   */
  inline bool
  remove(const key_type &k, value_type *old_v = NULL);

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

  /**
   * Is thread-safe, but not really designed to perform well with concurrent
   * modifications. also the value returned is not consistent given concurrent
   * modifications
   */
  inline size_t size() const;

  static inline uint64_t
  ExtractVersionNumber(const node_opaque_t *n) {
    // XXX(stephentu): I think we must use stable_version() for
    // correctness, but I am not 100% sure. It's definitely correct to use it,
    // but maybe we can get away with unstable_version()?
    return n->full_version_value();
  }

  // [value, has_suffix]
  static std::vector< std::pair<value_type, bool> >
  ExtractValues(const node_opaque_t *n);

  /**
   * Not well defined if n is being concurrently modified, just for debugging
   */
  static std::string
  NodeStringify(const node_opaque_t *n);

  void print();

  static inline size_t InternalNodeSize() {
    return sizeof(internode_type);
  }

  static inline size_t LeafNodeSize() {
    return sizeof(leaf_type);
  }

 private:
  Masstree::basic_table<P> table_;

  static leaf_type* leftmost_descend_layer(node_base_type* n);
  class size_walk_callback;
  template <bool Reverse> class search_range_scanner_base;
  template <bool Reverse> class low_level_search_range_scanner;
  template <typename F> class low_level_search_range_callback_wrapper;
};

template <typename P>
typename mbtree<P>::leaf_type *
mbtree<P>::leftmost_descend_layer(node_base_type *n)
{
  node_base_type *cur = n;
  while (true) {
    if (cur->isleaf())
      return static_cast<leaf_type*>(cur);
    internode_type *in = static_cast<internode_type*>(cur);
    nodeversion_type version = cur->stable();
    node_base_type *child = in->child_[0];
    if (unlikely(in->has_changed(version)))
      continue;
    cur = child;
  }
}

template <typename P>
void mbtree<P>::tree_walk(tree_walk_callback &callback) const {
  rcu_region guard;
  INVARIANT(rcu::s_instance.in_rcu_region());
  std::vector<node_base_type *> q, layers;
  q.push_back(table_.root());
  while (!q.empty()) {
    node_base_type *cur = q.back();
    q.pop_back();
    prefetch(cur);
    leaf_type *leaf = leftmost_descend_layer(cur);
    INVARIANT(leaf);
    while (leaf) {
      leaf->prefetch();
    process:
      auto version = leaf->stable();
      auto perm = leaf->permutation();
      for (int i = 0; i != perm.size(); ++i)
        if (leaf->is_layer(perm[i]))
          layers.push_back(leaf->lv_[perm[i]].layer());
      leaf_type *next = leaf->safe_next();
      callback.on_node_begin(leaf);
      if (unlikely(leaf->has_changed(version))) {
        callback.on_node_failure();
        layers.clear();
        goto process;
      }
      callback.on_node_success();
      leaf = next;
      if (!layers.empty()) {
        q.insert(q.end(), layers.begin(), layers.end());
        layers.clear();
      }
    }
  }
}

template <typename P>
class mbtree<P>::size_walk_callback : public tree_walk_callback {
 public:
  size_walk_callback()
    : size_(0) {
  }
  virtual void on_node_begin(const node_opaque_t *n);
  virtual void on_node_success();
  virtual void on_node_failure();
  size_t size_;
  int node_size_;
};

template <typename P>
void
mbtree<P>::size_walk_callback::on_node_begin(const node_opaque_t *n)
{
  auto perm = n->permutation();
  node_size_ = 0;
  for (int i = 0; i != perm.size(); ++i)
    if (!n->is_layer(perm[i]))
      ++node_size_;
}

template <typename P>
void
mbtree<P>::size_walk_callback::on_node_success()
{
  size_ += node_size_;
}

template <typename P>
void
mbtree<P>::size_walk_callback::on_node_failure()
{
}

template <typename P>
inline size_t mbtree<P>::size() const
{
  size_walk_callback c;
  tree_walk(c);
  return c.size_;
}

template <typename P>
inline bool mbtree<P>::search(const key_type &k, value_type &v,
                              versioned_node_t *search_info) const
{
  rcu_region guard;
  threadinfo ti;
  Masstree::unlocked_tcursor<P> lp(table_, k.data(), k.length());
  bool found = lp.find_unlocked(ti);
  if (found)
    v = lp.value();
  if (search_info)
    *search_info = versioned_node_t(lp.node(), lp.full_version_value());
  return found;
}

template <typename P>
inline bool mbtree<P>::insert(const key_type &k, value_type v,
                              value_type *old_v,
                              insert_info_t *insert_info)
{
  rcu_region guard;
  threadinfo ti;
  Masstree::tcursor<P> lp(table_, k.data(), k.length());
  bool found = lp.find_insert(ti);
  if (!found)
    ti.advance_timestamp(lp.node_timestamp());
  if (found && old_v)
    *old_v = lp.value();
  lp.value() = v;
  if (insert_info) {
    insert_info->node = lp.node();
    insert_info->old_version = lp.previous_full_version_value();
    insert_info->new_version = lp.next_full_version_value(1);
  }
  lp.finish(1, ti);
  return !found;
}

template <typename P>
inline bool mbtree<P>::insert_if_absent(const key_type &k, value_type v,
                                        insert_info_t *insert_info)
{
  rcu_region guard;
  threadinfo ti;
  Masstree::tcursor<P> lp(table_, k.data(), k.length());
  bool found = lp.find_insert(ti);
  if (!found) {
    ti.advance_timestamp(lp.node_timestamp());
    lp.value() = v;
    if (insert_info) {
      insert_info->node = lp.node();
      insert_info->old_version = lp.previous_full_version_value();
      insert_info->new_version = lp.next_full_version_value(1);
    }
  }
  lp.finish(!found, ti);
  return !found;
}

/**
 * return true if a value was removed, false otherwise.
 *
 * if true and old_v is not NULL, then the removed value of v
 * is written into old_v
 */
template <typename P>
inline bool mbtree<P>::remove(const key_type &k, value_type *old_v)
{
  rcu_region guard;
  threadinfo ti;
  Masstree::tcursor<P> lp(table_, k.data(), k.length());
  bool found = lp.find_locked(ti);
  if (found && old_v)
    *old_v = lp.value();
  lp.finish(found ? -1 : 0, ti);
  return found;
}

template <typename P>
template <bool Reverse>
class mbtree<P>::search_range_scanner_base {
 public:
  search_range_scanner_base(const key_type* boundary)
    : boundary_(boundary), boundary_compar_(false) {
  }
  void check(const Masstree::scanstackelt<P>& iter,
             const Masstree::key<uint64_t>& key) {
    int min = std::min(boundary_->length(), key.prefix_length());
    int cmp = memcmp(boundary_->data(), key.full_string().data(), min);
    if (!Reverse) {
      if (cmp < 0 || (cmp == 0 && boundary_->length() <= key.prefix_length()))
        boundary_compar_ = true;
      else if (cmp == 0) {
        uint64_t last_ikey = iter.node()->ikey0_[iter.permutation()[iter.permutation().size() - 1]];
        boundary_compar_ = boundary_->slice_at(key.prefix_length()) <= last_ikey;
      }
    } else {
      if (cmp >= 0)
        boundary_compar_ = true;
    }
  }
 protected:
  const key_type* boundary_;
  bool boundary_compar_;
};

template <typename P>
template <bool Reverse>
class mbtree<P>::low_level_search_range_scanner
  : public search_range_scanner_base<Reverse> {
 public:
  low_level_search_range_scanner(const key_type* boundary,
                                 low_level_search_range_callback& callback)
    : search_range_scanner_base<Reverse>(boundary), callback_(callback) {
  }
  void visit_leaf(const Masstree::scanstackelt<P>& iter,
                  const Masstree::key<uint64_t>& key, threadinfo&) {
    this->n_ = iter.node();
    this->v_ = iter.full_version_value();
    callback_.on_resp_node(this->n_, this->v_);
    if (this->boundary_)
      this->check(iter, key);
  }
  bool visit_value(const Masstree::key<uint64_t>& key,
                   value_type value, threadinfo&) {
    if (this->boundary_compar_) {
      lcdf::Str bs(this->boundary_->data(), this->boundary_->size());
      if ((!Reverse && bs <= key.full_string()) ||
          ( Reverse && bs >= key.full_string()))
        return false;
    }
    return callback_.invoke(key.full_string(), value, this->n_, this->v_);
  }
 private:
  Masstree::leaf<P>* n_;
  uint64_t v_;
  low_level_search_range_callback& callback_;
};

template <typename P>
template <typename F>
class mbtree<P>::low_level_search_range_callback_wrapper :
  public mbtree<P>::low_level_search_range_callback {
public:
  low_level_search_range_callback_wrapper(F& callback) : callback_(callback) {}

  void on_resp_node(const node_opaque_t *n, uint64_t version) OVERRIDE {}

  bool
  invoke(const string_type &k, value_type v,
         const node_opaque_t *n, uint64_t version) OVERRIDE
  {
    return callback_(k, v);
  }

 private:
  F& callback_;
};

template <typename P>
inline void mbtree<P>::search_range_call(const key_type &lower,
                                         const key_type *upper,
                                         low_level_search_range_callback &callback,
                                         std::string*) const {
  low_level_search_range_scanner<false> scanner(upper, callback);
  threadinfo ti;
  table_.scan(lcdf::Str(lower.data(), lower.length()), true, scanner, ti);
}

template <typename P>
inline void mbtree<P>::rsearch_range_call(const key_type &upper,
                                          const key_type *lower,
                                          low_level_search_range_callback &callback,
                                          std::string*) const {
  low_level_search_range_scanner<true> scanner(lower, callback);
  threadinfo ti;
  table_.rscan(lcdf::Str(upper.data(), upper.length()), true, scanner, ti);
}

template <typename P> template <typename F>
inline void mbtree<P>::search_range(const key_type &lower,
                                    const key_type *upper,
                                    F& callback,
                                    std::string*) const {
  low_level_search_range_callback_wrapper<F> wrapper(callback);
  low_level_search_range_scanner<false> scanner(upper, wrapper);
  threadinfo ti;
  table_.scan(lcdf::Str(lower.data(), lower.length()), true, scanner, ti);
}

template <typename P> template <typename F>
inline void mbtree<P>::rsearch_range(const key_type &upper,
                                     const key_type *lower,
                                     F& callback,
                                     std::string*) const {
  low_level_search_range_callback_wrapper<F> wrapper(callback);
  low_level_search_range_scanner<true> scanner(lower, wrapper);
  threadinfo ti;
  table_.rscan(lcdf::Str(upper.data(), upper.length()), true, scanner, ti);
}

template <typename P>
std::string mbtree<P>::NodeStringify(const node_opaque_t *n)
{
  std::ostringstream b;
  b << "node[v=" << n->version_value() << "]";
  return b.str();
}

template <typename P>
std::vector<std::pair<typename mbtree<P>::value_type, bool>>
mbtree<P>::ExtractValues(const node_opaque_t *n)
{
  std::vector< std::pair<value_type, bool> > ret;
  auto perm = n->permutation();
  for (int i = 0; i != perm.size(); ++i) {
    int keylenx = n->keylenx_[perm[i]];
    if (!n->keylenx_is_layer(keylenx))
      ret.emplace_back(n->lv_[perm[i]].value(), n->keylenx_has_ksuf(keylenx));
  }
  return ret;
}

template <typename P>
void mbtree<P>::print() {
  table_.print();
}

typedef mbtree<masstree_params> concurrent_btree;
typedef mbtree<masstree_single_threaded_params> single_threaded_btree;
