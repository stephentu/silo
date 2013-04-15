#ifndef _NDB_BASE_TXN_BTREE_H_
#define _NDB_BASE_TXN_BTREE_H_

#include "btree.h"
#include "txn.h"

#include <string>
#include <map>
#include <type_traits>

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

public:
  typedef transaction_base::key_type key_type;
  typedef transaction_base::string_type string_type;
  typedef const uint8_t * value_type;
  typedef size_t size_type;

  struct default_string_allocator {
    inline ALWAYS_INLINE string_type *
    operator()()
    {
      return nullptr;
    }
    inline ALWAYS_INLINE void
    return_last(string_type *px)
    {
    }
  };

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

  btree underlying_btree;
  size_type value_size_hint;
  std::string name;
  bool been_destructed;
  base_txn_btree_handler<Transaction> handler;
};

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

#endif /* _NDB_BASE_TXN_BTREE_H_ */
