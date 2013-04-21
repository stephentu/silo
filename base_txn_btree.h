#ifndef _NDB_BASE_TXN_BTREE_H_
#define _NDB_BASE_TXN_BTREE_H_

#include "btree.h"
#include "txn.h"
#include "lockguard.h"
#include "util.h"

#include <string>
#include <map>
#include <type_traits>
#include <memory>

// each Transaction implementation should specialize this for special
// behavior- the default implementation is just nops
template <template <typename> class Transaction>
struct base_txn_btree_handler {
  inline void on_construct(const std::string &name, btree *underlying) {} // get a handle to the underying btree
  inline void on_destruct() {} // called at the beginning of the txn_btree's dtor
  static const bool has_background_task = false;
};

template <template <typename> class Transaction>
class base_txn_btree {

  // XXX: not ideal
  template <template <typename> class P, typename T>
    friend class transaction;

  // XXX: would like to declare friend wth all Transaction<T> classes, but
  // doesn't seem like an easy way to do that for template template parameters

  // KeyReader Interface
  //
  // KeyReader is a simple transformation from (const std::string &) => T.
  // The input is guaranteed to be stable, so it has a simple interface:
  //
  //   T operator()(const std::string &)
  //
  // The KeyReader is expect to preserve the following property: After a call
  // to operator(), but before the next, the returned value is guaranteed to be
  // valid and remain stable.

  // ValueReader Interface
  //
  // ValueReader is a more complex transformation from (const uint8_t *, size_t) => T.
  // The input is not guaranteed to be stable, so it has a more complex interface:
  //
  //   bool operator()(const uint8_t *, size_t)
  //
  // This interface returns false if there was not enough buffer space to
  // finish the read, true otherwise.  Note that this interface returning true
  // does NOT mean that a read was stable, but it just means there were enough
  // bytes in the buffer to perform the tentative read. Note that ValueReader
  // also exposes a means to fetch results:
  //
  //   T results()
  //
  // The ValueReader is expected to preserve the following property: After a
  // call to operator(), if it returns true, then the value returned from
  // results() should remain valid and stable until the next call to
  // operator().

public:
  typedef transaction_base::key_type key_type;
  typedef transaction_base::string_type string_type;
  typedef const uint8_t * value_type;
  typedef size_t size_type;

  typedef util::default_string_allocator default_string_allocator;

  base_txn_btree(size_type value_size_hint = 128,
            bool mostly_append = false,
            const std::string &name = "<unknown>")
    : value_size_hint(value_size_hint),
      name(name),
      been_destructed(false)
  {
    handler.on_construct(name, &underlying_btree);
  }

  ~base_txn_btree()
  {
    if (!been_destructed)
      unsafe_purge(false);
  }

  inline size_t
  size_estimate() const
  {
    return underlying_btree.size();
  }

  inline size_type
  get_value_size_hint() const
  {
    return value_size_hint;
  }

  inline void
  set_value_size_hint(size_type value_size_hint)
  {
    this->value_size_hint = value_size_hint;
  }

  /**
   * only call when you are sure there are no concurrent modifications on the
   * tree. is neither threadsafe nor transactional
   *
   * Note that when you call unsafe_purge(), this txn_btree becomes
   * completely invalidated and un-usable. Any further operations
   * (other than calling the destructor) are undefined
   */
  std::map<std::string, uint64_t> unsafe_purge(bool dump_stats = false);

  // XXX: only exists because can't declare friend of template parameter
  // Transaction
  inline btree *
  get_underlying_btree()
  {
    return &underlying_btree;
  }

private:

  struct purge_tree_walker : public btree::tree_walk_callback {
    virtual void on_node_begin(const btree::node_opaque_t *n);
    virtual void on_node_success();
    virtual void on_node_failure();
#ifdef TXN_BTREE_DUMP_PURGE_STATS
    purge_tree_walker()
      : purge_stats_tuple_logically_removed_no_mark(0),
        purge_stats_tuple_logically_removed_with_mark(0),
        purge_stats_nodes(0),
        purge_stats_nosuffix_nodes(0) {}
    std::map<size_t, size_t> purge_stats_ln_record_size_counts; // just the record
    std::map<size_t, size_t> purge_stats_ln_alloc_size_counts; // includes overhead
    std::map<size_t, size_t> purge_stats_tuple_chain_counts;
    size_t purge_stats_tuple_logically_removed_no_mark;
    size_t purge_stats_tuple_logically_removed_with_mark;
    std::vector<uint16_t> purge_stats_nkeys_node;
    size_t purge_stats_nodes;
    size_t purge_stats_nosuffix_nodes;

    std::map<std::string, uint64_t>
    dump_stats()
    {
    std::map<std::string, uint64_t> ret;
      size_t v = 0;
      for (std::vector<uint16_t>::iterator it = purge_stats_nkeys_node.begin();
          it != purge_stats_nkeys_node.end(); ++it)
        v += *it;
      const double avg_nkeys_node = double(v)/double(purge_stats_nkeys_node.size());
      const double avg_fill_factor = avg_nkeys_node/double(btree::NKeysPerNode);
      std::cerr << "btree node stats" << std::endl;
      std::cerr << "    avg_nkeys_node: " << avg_nkeys_node << std::endl;
      std::cerr << "    avg_fill_factor: " << avg_fill_factor << std::endl;
      std::cerr << "    num_nodes: " << purge_stats_nodes << std::endl;
      std::cerr << "    num_nosuffix_nodes: " << purge_stats_nosuffix_nodes << std::endl;
      std::cerr << "record size stats (nbytes => count)" << std::endl;
      for (std::map<size_t, size_t>::iterator it = purge_stats_ln_record_size_counts.begin();
          it != purge_stats_ln_record_size_counts.end(); ++it)
        std::cerr << "    " << it->first << " => " << it->second << std::endl;
      std::cerr << "alloc size stats  (nbytes => count)" << std::endl;
      for (std::map<size_t, size_t>::iterator it = purge_stats_ln_alloc_size_counts.begin();
          it != purge_stats_ln_alloc_size_counts.end(); ++it)
        std::cerr << "    " << (it->first + sizeof(dbtuple)) << " => " << it->second << std::endl;
      std::cerr << "chain stats  (length => count)" << std::endl;
      for (std::map<size_t, size_t>::iterator it = purge_stats_tuple_chain_counts.begin();
          it != purge_stats_tuple_chain_counts.end(); ++it) {
        std::cerr << "    " << it->first << " => " << it->second << std::endl;
        ret["chain_" + std::to_string(it->first)] += it->second;
      }
      std::cerr << "deleted recored stats" << std::endl;
      std::cerr << "    logically_removed (total): " << (purge_stats_tuple_logically_removed_no_mark + purge_stats_tuple_logically_removed_with_mark) << std::endl;
      std::cerr << "    logically_removed_no_mark: " << purge_stats_tuple_logically_removed_no_mark << std::endl;
      std::cerr << "    logically_removed_with_mark: " << purge_stats_tuple_logically_removed_with_mark << std::endl;
      return ret;
    }
#endif

  private:
    std::vector< std::pair<btree::value_type, bool> > spec_values;
  };

protected:

  // readers are placed here so they can be shared amongst
  // derived implementations

  class key_reader {
  public:
    inline ALWAYS_INLINE const std::string &
    operator()(const std::string &s)
    {
      return s;
    }
  };

  class single_value_reader {
  public:
    constexpr single_value_reader(
        std::string &s, size_t max_bytes_read)
      : s(&s), max_bytes_read(max_bytes_read) {}

    inline bool
    operator()(const uint8_t *data, size_t sz)
    {
      const size_t readsz = std::min(sz, max_bytes_read);
      s->assign((const char *) data, readsz);
      return true;
    }

    inline std::string &
    results()
    {
      return *s;
    }

    inline const std::string &
    results() const
    {
      return *s;
    }

  private:
    std::string *s;
    size_t max_bytes_read;
  };

  // does not bother to interpret the bytes from a record
  template <typename StringAllocator>
  class value_reader {
  public:
    constexpr value_reader(StringAllocator &sa, size_t max_bytes_read)
      : sa(&sa), px(nullptr), max_bytes_read(max_bytes_read) {}

    inline bool
    operator()(const uint8_t *data, size_t sz)
    {
      const size_t readsz = std::min(sz, max_bytes_read);
      px = (*sa)();
      px->assign((const char *) data, readsz);
      return true;
    }

    inline std::string &
    results()
    {
      INVARIANT(px);
      return *px;
    }

    inline const std::string &
    results() const
    {
      INVARIANT(px);
      return *px;
    }

  private:
    StringAllocator *sa;
    std::string *px;
    size_t max_bytes_read;
  };

  template <typename Traits,
            typename Callback,
            typename KeyReader,
            typename ValueReader,
            typename StringAllocator>
  struct txn_search_range_callback : public btree::low_level_search_range_callback {
    constexpr txn_search_range_callback(
          Transaction<Traits> *t,
          Callback *caller_callback,
          KeyReader *key_reader,
          ValueReader *value_reader,
          StringAllocator *sa)
      : t(t), caller_callback(caller_callback),
        key_reader(key_reader), value_reader(value_reader), sa(sa) {}

    virtual void on_resp_node(const btree::node_opaque_t *n, uint64_t version);
    virtual bool invoke(const btree::string_type &k, btree::value_type v,
                        const btree::node_opaque_t *n, uint64_t version);

  private:
    Transaction<Traits> *const t;
    Callback *const caller_callback;
    KeyReader *const key_reader;
    ValueReader *const value_reader;
    StringAllocator *const sa;
  };

  template <typename Traits, typename ValueReader>
  inline bool
  do_search(Transaction<Traits> &t, const key_type &k, ValueReader &value_reader);

  template <typename Traits,
            typename Callback,
            typename KeyReader,
            typename ValueReader,
            typename StringAllocator>
  inline void
  do_search_range_call(Transaction<Traits> &t,
                       const key_type &lower,
                       const key_type *upper,
                       Callback &callback,
                       KeyReader &key_reader,
                       ValueReader &value_reader,
                       StringAllocator &sa);

  // remove() is just do_tree_put() with empty-string
  // expect_new indicates if we expect the record to not exist in the tree-
  // is just a hint that affects perf, not correctness
  template <typename Traits>
  void do_tree_put(Transaction<Traits> &t, const string_type &k,
                   const string_type &v, bool expect_new);


  btree underlying_btree;
  size_type value_size_hint;
  std::string name;
  bool been_destructed;
  base_txn_btree_handler<Transaction> handler;
};

namespace private_ {
  STATIC_COUNTER_DECL(scopedperf::tsc_ctr, txn_btree_search_probe0, txn_btree_search_probe0_cg)
  STATIC_COUNTER_DECL(scopedperf::tsc_ctr, txn_btree_search_probe1, txn_btree_search_probe1_cg)
}

template <template <typename> class Transaction>
template <typename Traits, typename ValueReader>
bool
base_txn_btree<Transaction>::do_search(
    Transaction<Traits> &t, const key_type &k, ValueReader &value_reader)
{
  t.ensure_active();

  // search the underlying btree to map k=>(btree_node|tuple)
  btree::value_type underlying_v;
  btree::versioned_node_t search_info;
  const bool found = this->underlying_btree.search(k, underlying_v, &search_info);
  if (found) {
    const dbtuple * const tuple = reinterpret_cast<const dbtuple *>(underlying_v);
    return t.do_tuple_read(tuple, value_reader);
  } else {
    // not found, add to absent_set
    t.do_node_read(search_info.first, search_info.second);
    return false;
  }
}

template <template <typename> class Transaction>
std::map<std::string, uint64_t>
base_txn_btree<Transaction>::unsafe_purge(bool dump_stats)
{
  ALWAYS_ASSERT(!been_destructed);
  been_destructed = true;
  handler.on_destruct(); // stop background tasks
  purge_tree_walker w;
  underlying_btree.tree_walk(w);
  underlying_btree.clear();
#ifdef TXN_BTREE_DUMP_PURGE_STATS
  if (!dump_stats)
    return std::map<std::string, uint64_t>();
  return w.dump_stats();
#else
  return std::map<std::string, uint64_t>();
#endif
}

template <template <typename> class Transaction>
void
base_txn_btree<Transaction>::purge_tree_walker::on_node_begin(const btree::node_opaque_t *n)
{
  INVARIANT(spec_values.empty());
  spec_values = btree::ExtractValues(n);
}

template <template <typename> class Transaction>
void
base_txn_btree<Transaction>::purge_tree_walker::on_node_success()
{
  for (size_t i = 0; i < spec_values.size(); i++) {
    dbtuple *ln =
      (dbtuple *) spec_values[i].first;
    INVARIANT(ln);
#ifdef TXN_BTREE_DUMP_PURGE_STATS
    // XXX(stephentu): should we also walk the chain?
    purge_stats_ln_record_size_counts[ln->size]++;
    purge_stats_ln_alloc_size_counts[ln->alloc_size]++;
    purge_stats_tuple_chain_counts[ln->chain_length()]++;
    if (!ln->size && !ln->is_deleting())
      purge_stats_tuple_logically_removed_no_mark++;
    if (!ln->size && ln->is_deleting())
      purge_stats_tuple_logically_removed_with_mark++;
#endif
    if (base_txn_btree_handler<Transaction>::has_background_task) {
#ifdef CHECK_INVARIANTS
      lock_guard<dbtuple> l(ln, false);
#endif
      dbtuple::release(ln);
    } else {
      dbtuple::release_no_rcu(ln);
    }
  }
#ifdef TXN_BTREE_DUMP_PURGE_STATS
  purge_stats_nkeys_node.push_back(spec_values.size());
  purge_stats_nodes++;
  for (size_t i = 0; i < spec_values.size(); i++)
    if (spec_values[i].second)
      goto done;
  purge_stats_nosuffix_nodes++;
done:
#endif
  spec_values.clear();
}

template <template <typename> class Transaction>
void
base_txn_btree<Transaction>::purge_tree_walker::on_node_failure()
{
  spec_values.clear();
}

template <template <typename> class Transaction>
template <typename Traits>
void
base_txn_btree<Transaction>::do_tree_put(
    Transaction<Traits> &t, const string_type &k,
    const string_type &v, bool expect_new)
{
  t.ensure_active();
  if (unlikely(t.is_read_only())) {
    const transaction_base::abort_reason r = transaction_base::ABORT_REASON_USER;
    t.abort_impl(r);
    throw transaction_abort_exception(r);
  }
  dbtuple *px = nullptr;
  bool insert = false;
retry:
  if (expect_new) {
    auto ret = t.try_insert_new_tuple(this->underlying_btree, k, v);
    INVARIANT(!ret.second || ret.first);
    if (unlikely(ret.second)) {
      const transaction_base::abort_reason r = transaction_base::ABORT_REASON_WRITE_NODE_INTERFERENCE;
      t.abort_impl(r);
      throw transaction_abort_exception(r);
    }
    px = ret.first;
    if (px)
      insert = true;
  }
  if (!px) {
    // do regular search
    btree::value_type bv = 0;
    if (!this->underlying_btree.search(varkey(k), bv)) {
      expect_new = true;
      goto retry;
    }
    px = reinterpret_cast<dbtuple *>(bv);
  }
  INVARIANT(px);
  if (!insert) {
    // add to write set normally, as non-insert
    t.write_set.emplace_back(px, k, v, &this->underlying_btree, false);
  } else {
    // should already exist in write set as insert
    // (because of try_insert_new_tuple())

    // too expensive to be a practical check
    //INVARIANT(t.find_write_set(px) != t.write_set.end());
    //INVARIANT(t.find_write_set(px)->is_insert());
  }
}

template <template <typename> class Transaction>
template <typename Traits,
          typename Callback,
          typename KeyReader,
          typename ValueReader,
          typename StringAllocator>
void
base_txn_btree<Transaction>
  ::txn_search_range_callback<Traits, Callback, KeyReader, ValueReader, StringAllocator>
  ::on_resp_node(
    const btree::node_opaque_t *n, uint64_t version)
{
  VERBOSE(std::cerr << "on_resp_node(): <node=0x" << util::hexify(intptr_t(n))
               << ", version=" << version << ">" << std::endl);
  VERBOSE(std::cerr << "  " << btree::NodeStringify(n) << std::endl);
  t->do_node_read(n, version);
}

template <template <typename> class Transaction>
template <typename Traits,
          typename Callback,
          typename KeyReader,
          typename ValueReader,
          typename StringAllocator>
bool
base_txn_btree<Transaction>
  ::txn_search_range_callback<Traits, Callback, KeyReader, ValueReader, StringAllocator>
  ::invoke(
    const btree::string_type &k, btree::value_type v,
    const btree::node_opaque_t *n, uint64_t version)
{
  t->ensure_active();
  VERBOSE(std::cerr << "search range k: " << util::hexify(k) << " from <node=0x" << util::hexify(n)
                    << ", version=" << version << ">" << std::endl
                    << "  " << *((dbtuple *) v) << std::endl);
  const dbtuple * const tuple = reinterpret_cast<const dbtuple *>(v);
  if (t->do_tuple_read(tuple, *value_reader))
    return caller_callback->invoke((*key_reader)(k), value_reader->results());
  return true;
}

template <template <typename> class Transaction>
template <typename Traits,
          typename Callback,
          typename KeyReader,
          typename ValueReader,
          typename StringAllocator>
void
base_txn_btree<Transaction>::do_search_range_call(
    Transaction<Traits> &t,
    const key_type &lower,
    const key_type *upper,
    Callback &callback,
    KeyReader &key_reader,
    ValueReader &value_reader,
    StringAllocator &sa)
{
  t.ensure_active();
  if (upper)
    VERBOSE(std::cerr << "txn_btree(0x" << util::hexify(intptr_t(this))
                 << ")::search_range_call [" << util::hexify(lower)
                 << ", " << util::hexify(*upper) << ")" << std::endl);
  else
    VERBOSE(std::cerr << "txn_btree(0x" << util::hexify(intptr_t(this))
                 << ")::search_range_call [" << util::hexify(lower)
                 << ", +inf)" << std::endl);
  if (unlikely(upper && *upper <= lower))
    return;
  txn_search_range_callback<Traits, Callback, KeyReader, ValueReader, StringAllocator> c(
			&t, &callback, &key_reader, &value_reader, &sa);
  this->underlying_btree.search_range_call(lower, upper, c, sa());
}

#endif /* _NDB_BASE_TXN_BTREE_H_ */
