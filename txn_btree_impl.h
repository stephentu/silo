#ifndef _NDB_TXN_BTREE_IMPL_H_
#define _NDB_TXN_BTREE_IMPL_H_

#include <iostream>
#include <vector>
#include <map>

#include "txn_btree.h"
#include "lockguard.h"

namespace private_ {
  STATIC_COUNTER_DECL(scopedperf::tsc_ctr, txn_btree_search_probe0, txn_btree_search_probe0_cg)
  STATIC_COUNTER_DECL(scopedperf::tsc_ctr, txn_btree_search_probe1, txn_btree_search_probe1_cg)
}

template <template <typename> class Transaction>
template <typename Traits>
bool
txn_btree<Transaction>::search(
    Transaction<Traits> &t, const key_type &k,
    string_type &v, size_t max_bytes_read)
{
  t.ensure_active();

  // search the underlying btree to map k=>(btree_node|tuple)
  btree::value_type underlying_v;
  btree::versioned_node_t search_info;
  const bool found = underlying_btree.search(k, underlying_v, &search_info);
  if (found) {
    const dbtuple * const tuple = reinterpret_cast<const dbtuple *>(underlying_v);
    return t.do_tuple_read(tuple, v, max_bytes_read);
  } else {
    // not found, add to absent_set
    t.do_node_read(search_info.first, search_info.second);
    return false;
  }
}

template <template <typename> class Transaction>
template <typename Traits, typename StringAllocator>
void
txn_btree<Transaction>::txn_search_range_callback<Traits, StringAllocator>::on_resp_node(
    const btree::node_opaque_t *n, uint64_t version)
{
  VERBOSE(std::cerr << "on_resp_node(): <node=0x" << util::hexify(intptr_t(n))
               << ", version=" << version << ">" << std::endl);
  VERBOSE(std::cerr << "  " << btree::NodeStringify(n) << std::endl);
  t->do_node_read(n, version);
}

template <template <typename> class Transaction>
template <typename Traits, typename StringAllocator>
bool
txn_btree<Transaction>::txn_search_range_callback<Traits, StringAllocator>::invoke(
    const btree::string_type &k, btree::value_type v,
    const btree::node_opaque_t *n, uint64_t version)
{
  t->ensure_active();
  VERBOSE(std::cerr << "search range k: " << util::hexify(k) << " from <node=0x" << util::hexify(n)
                    << ", version=" << version << ">" << std::endl
                    << "  " << *((dbtuple *) v) << std::endl);

  string_type *r_px = sa();
  string_type *r_px_orig = r_px;
  if (!r_px) {
    temp_buf.clear();
    r_px = &temp_buf;
  }
  string_type &r(*r_px);
  INVARIANT(r.empty());
  const dbtuple * const tuple = reinterpret_cast<const dbtuple *>(v);
  if (t->do_tuple_read(tuple, r))
    return caller_callback->invoke(k, r);
  sa.return_last(r_px_orig);
  return true;
}

template <template <typename> class Transaction>
template <typename Traits, typename StringAllocator>
void
txn_btree<Transaction>::search_range_call(
    Transaction<Traits> &t,
    const key_type &lower,
    const key_type *upper,
    search_range_callback &callback,
    const StringAllocator &sa)
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
  txn_search_range_callback<Traits, StringAllocator> c(&t, &callback, sa);
  underlying_btree.search_range_call(lower, upper, c, c.sa());
}

template <template <typename> class Transaction>
template <typename Traits>
void
txn_btree<Transaction>::do_tree_put(
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
    auto ret = t.try_insert_new_tuple(underlying_btree, k, v);
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
    if (!underlying_btree.search(varkey(k), bv)) {
      expect_new = true;
      goto retry;
    }
    px = reinterpret_cast<dbtuple *>(bv);
  }
  INVARIANT(px);
  if (!insert) {
    // add to write set normally, as non-insert
    t.write_set.emplace_back(px, k, v, &underlying_btree, false);
  } else {
    // should already exist in write set as insert
    // (because of try_insert_new_tuple())

    // too expensive to be a practical check
    //INVARIANT(t.find_write_set(px) != t.write_set.end());
    //INVARIANT(t.find_write_set(px)->is_insert());
  }
}

template <template <typename> class Transaction>
std::map<std::string, uint64_t>
txn_btree<Transaction>::unsafe_purge(bool dump_stats)
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
txn_btree<Transaction>::purge_tree_walker::on_node_begin(const btree::node_opaque_t *n)
{
  INVARIANT(spec_values.empty());
  spec_values = btree::ExtractValues(n);
}

template <template <typename> class Transaction>
void
txn_btree<Transaction>::purge_tree_walker::on_node_success()
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
    if (txn_btree_handler<Transaction>::has_background_task) {
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
txn_btree<Transaction>::purge_tree_walker::on_node_failure()
{
  spec_values.clear();
}

#endif /* _NDB_TXN_BTREE_IMPL_H_ */
