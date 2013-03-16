#ifndef _NDB_TXN_IMPL_H_
#define _NDB_TXN_IMPL_H_

#include "txn.h"
#include "lockguard.h"

// base definitions

template <template <typename> class Protocol, typename Traits>
inline transaction<Protocol, Traits>::transaction(uint64_t flags)
  : transaction_base(flags)
{
  // XXX(stephentu): VERY large RCU region
  rcu::region_begin();
}

template <template <typename> class Protocol, typename Traits>
inline transaction<Protocol, Traits>::~transaction()
{
  // transaction shouldn't fall out of scope w/o resolution
  // resolution means TXN_EMBRYO, TXN_COMMITED, and TXN_ABRT
  INVARIANT(state != TXN_ACTIVE);
  rcu::region_end();
}

template <template <typename> class Protocol, typename Traits>
inline void
transaction<Protocol, Traits>::clear()
{
  // don't clear for debugging purposes
  //ctx_map.clear();
}

template <template <typename> class Protocol, typename Traits>
inline void
transaction<Protocol, Traits>::abort_impl(abort_reason reason)
{
  abort_trap(reason);
  switch (state) {
  case TXN_EMBRYO:
  case TXN_ACTIVE:
    break;
  case TXN_ABRT:
    return;
  case TXN_COMMITED:
    throw transaction_unusable_exception();
  }
  state = TXN_ABRT;
  this->reason = reason;
  clear();
}

namespace {
  inline const char *
  transaction_state_to_cstr(transaction_base::txn_state state)
  {
    switch (state) {
    case transaction_base::TXN_EMBRYO: return "TXN_EMBRYO";
    case transaction_base::TXN_ACTIVE: return "TXN_ACTIVE";
    case transaction_base::TXN_ABRT: return "TXN_ABRT";
    case transaction_base::TXN_COMMITED: return "TXN_COMMITED";
    }
    ALWAYS_ASSERT(false);
    return 0;
  }

  inline std::string
  transaction_flags_to_str(uint64_t flags)
  {
    bool first = true;
    std::ostringstream oss;
    if (flags & transaction_base::TXN_FLAG_LOW_LEVEL_SCAN) {
      oss << "TXN_FLAG_LOW_LEVEL_SCAN";
      first = false;
    }
    if (flags & transaction_base::TXN_FLAG_READ_ONLY) {
      if (first)
        oss << "TXN_FLAG_READ_ONLY";
      else
        oss << " | TXN_FLAG_READ_ONLY";
      first = false;
    }
    return oss.str();
  }
}

template <template <typename> class Protocol, typename Traits>
void
transaction<Protocol, Traits>::dump_debug_info() const
{
  std::cerr << "Transaction (obj=" << util::hexify(this) << ") -- state "
       << transaction_state_to_cstr(state) << std::endl;
  std::cerr << "  Abort Reason: " << AbortReasonStr(reason) << std::endl;
  std::cerr << "  Flags: " << transaction_flags_to_str(flags) << std::endl;
  std::cerr << "  Read/Write sets:" << std::endl;
  for (typename ctx_map_type::const_iterator it = ctx_map.begin();
       it != ctx_map.end(); ++it) {
    std::cerr << "    Btree @ " << util::hexify(it->first) << ":" << std::endl;

    std::cerr << "      === Read Set ===" << std::endl;
    // read-set
    for (typename read_set_map::const_iterator rs_it = it->second.read_set.begin();
         rs_it != it->second.read_set.end(); ++rs_it)
      std::cerr << "      Node " << util::hexify(rs_it->first) << " @ " << rs_it->second << std::endl;

    std::cerr << "      === Absent Set ===" << std::endl;
    // absent-set
    for (typename absent_set_map::const_iterator as_it = it->second.absent_set.begin();
         as_it != it->second.absent_set.end(); ++as_it)
      std::cerr << "      Key 0x" << util::hexify(as_it->first) << " : locked=" << as_it->second << std::endl;

    std::cerr << "      === Write Set ===" << std::endl;
    // write-set
    for (typename write_set_map::const_iterator ws_it = it->second.write_set.begin();
         ws_it != it->second.write_set.end(); ++ws_it)
      if (!ws_it->second.empty())
        std::cerr << "      Key 0x" << util::hexify(ws_it->first) << " @ " << util::hexify(ws_it->second) << std::endl;
      else
        std::cerr << "      Key 0x" << util::hexify(ws_it->first) << " : remove" << std::endl;

    // XXX: node set + absent ranges
    std::cerr << "      === Absent Ranges ===" << std::endl;
    for (typename absent_range_vec::const_iterator ar_it = it->second.absent_range_set.begin();
         ar_it != it->second.absent_range_set.end(); ++ar_it)
      std::cerr << "      " << *ar_it << std::endl;
  }
}

template <template <typename> class Protocol, typename Traits>
std::map<std::string, uint64_t>
transaction<Protocol, Traits>::get_txn_counters() const
{
  std::map<std::string, uint64_t> ret;
  // num_txn_contexts:
  ret["num_txn_contexts"] = ctx_map.size();
  for (typename ctx_map_type::const_iterator it = ctx_map.begin();
       it != ctx_map.end(); ++it) {
    // max_read_set_size
    ret["max_read_set_size"] = std::max(ret["max_read_set_size"], it->second.read_set.size());
    if (!it->second.read_set.is_small_type())
      ret["n_read_set_large_instances"]++;

    // max_absent_set_size
    ret["max_absent_set_size"] = std::max(ret["max_absent_set_size"], it->second.absent_set.size());
    if (!it->second.absent_set.is_small_type())
      ret["n_absent_set_large_instances"]++;

    // max_write_set_size
    ret["max_write_set_size"] = std::max(ret["max_write_set_size"], it->second.write_set.size());
    if (!it->second.write_set.is_small_type())
      ret["n_write_set_large_instances"]++;

    // max_absent_range_set_size
    ret["max_absent_range_set_size"] = std::max(ret["max_absent_range_set_size"], it->second.absent_range_set.size());

    // max_node_scan_size
    ret["max_node_scan_size"] = std::max(ret["max_node_scan_size"], it->second.node_scan.size());
    if (!it->second.node_scan.is_small_type())
      ret["n_node_scan_large_instances"]++;
  }
  return ret;
}

template <template <typename> class Protocol, typename Traits>
bool
transaction<Protocol, Traits>::commit(bool doThrow)
{
  // XXX(stephentu): specific abort counters, to see which
  // case we are aborting the most on (could integrate this with
  // abort_trap())
  //ANON_REGION("transaction::commit:", &txn_commit_probe0_cg);

  switch (state) {
  case TXN_EMBRYO:
  case TXN_ACTIVE:
    break;
  case TXN_COMMITED:
    return true;
  case TXN_ABRT:
    if (doThrow)
      throw transaction_abort_exception(reason);
    return false;
  }

  // fetch dbtuples for insert
  typename util::vec<dbtuple_pair>::type dbtuples;
  const std::pair<bool, tid_t> snapshot_tid_t = cast()->consistent_snapshot_tid();
  std::pair<bool, tid_t> commit_tid(false, 0);

  {
    typename ctx_map_type::iterator outer_it = ctx_map.begin();
    typename ctx_map_type::iterator outer_it_end = ctx_map.end();
    for (; outer_it != outer_it_end; ++outer_it) {
      INVARIANT(!(get_flags() & TXN_FLAG_READ_ONLY) || outer_it->second.write_set.empty());
      if (outer_it->second.write_set.empty())
        continue;
      typename write_set_map::iterator it = outer_it->second.write_set.begin();
      typename write_set_map::iterator it_end = outer_it->second.write_set.end();
      for (; it != it_end; ++it) {
      retry:
        btree::value_type v = 0;
        if (outer_it->first->underlying_btree.search(varkey(it->first), v)) {
          VERBOSE(cerr << "key " << util::hexify(it->first) << " : dbtuple 0x" << util::hexify(intptr_t(v)) << endl);
          dbtuples.emplace_back(
              (dbtuple *) v,
              dbtuple_info(outer_it->first, it->first, false, it->second));
          // mark that we hold lock in read set
          typename read_set_map::iterator read_it =
            outer_it->second.read_set.find((const dbtuple *) v);
          if (read_it != outer_it->second.read_set.end()) {
            INVARIANT(!read_it->second.holds_lock);
            read_it->second.holds_lock = true;
          }
          // mark that we hold lock in absent set
          typename absent_set_map::iterator absent_it =
            outer_it->second.absent_set.find(it->first);
          if (absent_it != outer_it->second.absent_set.end()) {
            INVARIANT(!absent_it->second);
            absent_it->second = true;
          }
        } else {
          dbtuple *ln = dbtuple::alloc_first(
              !outer_it->first->is_mostly_append(), it->second.size());
          // XXX: underlying btree api should return the existing value if
          // insert fails- this would allow us to avoid having to do another search
          std::pair<const btree::node_opaque_t *, uint64_t> insert_info;
          if (!outer_it->first->underlying_btree.insert_if_absent(
                varkey(it->first), (btree::value_type) ln, &insert_info)) {
            dbtuple::release_no_rcu(ln);
            goto retry;
          }
          VERBOSE(cerr << "key " << util::hexify(it->first) << " : dbtuple 0x" << util::hexify(intptr_t(ln)) << endl);
          if (get_flags() & TXN_FLAG_LOW_LEVEL_SCAN) {
            // update node #s
            INVARIANT(insert_info.first);
            typename node_scan_map::iterator nit = outer_it->second.node_scan.find(insert_info.first);
            if (nit != outer_it->second.node_scan.end()) {
              if (unlikely(nit->second != insert_info.second)) {
                abort_trap((reason = ABORT_REASON_WRITE_NODE_INTERFERENCE));
                goto do_abort;
              }
              VERBOSE(cerr << "bump node=" << util::hexify(nit->first) << " from v=" << (nit->second)
                           << " -> v=" << (nit->second + 1) << endl);
              // otherwise, bump the version by 1
              nit->second++; // XXX(stephentu): this doesn't properly handle wrap-around
                             // but we're probably F-ed on a wrap around anyways for now
              SINGLE_THREADED_INVARIANT(btree::ExtractVersionNumber(nit->first) == nit->second);
            }
          }
          dbtuples.emplace_back(
              ln, dbtuple_info(outer_it->first, it->first, false, it->second));
          // mark that we hold lock in read set
          typename read_set_map::iterator read_it =
            outer_it->second.read_set.find(ln);
          if (read_it != outer_it->second.read_set.end()) {
            INVARIANT(!read_it->second.holds_lock);
            read_it->second.holds_lock = true;
          }
          // mark that we hold lock in absent set
          typename absent_set_map::iterator absent_it =
            outer_it->second.absent_set.find(it->first);
          if (absent_it != outer_it->second.absent_set.end()) {
            INVARIANT(!absent_it->second);
            absent_it->second = true;
          }
        }
      }
    }
  }

  if (!snapshot_tid_t.first || !dbtuples.empty()) {
    // we don't have consistent tids, or not a read-only txn

    if (!dbtuples.empty()) {
      // lock the logical nodes in sort order
      std::sort(dbtuples.begin(), dbtuples.end(), LNodeComp());
      typename util::vec<dbtuple_pair>::type::iterator it = dbtuples.begin();
      typename util::vec<dbtuple_pair>::type::iterator it_end = dbtuples.end();
      for (; it != it_end; ++it) {
        VERBOSE(cerr << "locking node 0x" << util::hexify(intptr_t(it->first)) << endl);
        const dbtuple::version_t v = it->first->lock();
        it->second.locked = true; // we locked the node
        if (unlikely(dbtuple::IsDeleting(v) ||
                     !dbtuple::IsLatest(v) ||
                     !cast()->can_read_tid(it->first->version))) {
          // XXX(stephentu): overly conservative (with the can_read_tid() check)
          abort_trap((reason = ABORT_REASON_WRITE_NODE_INTERFERENCE));
          goto do_abort;
        }
      }
      commit_tid.first = true;
      commit_tid.second = cast()->gen_commit_tid(dbtuples);
      VERBOSE(cerr << "commit tid: " << g_proto_version_str(commit_tid.second) << endl);
    } else {
      VERBOSE(cerr << "commit tid: <read-only>" << endl);
    }

    // do read validation
    {
      typename ctx_map_type::iterator outer_it = ctx_map.begin();
      typename ctx_map_type::iterator outer_it_end = ctx_map.end();
      for (; outer_it != outer_it_end; ++outer_it) {

        // check the nodes we actually read are still the latest version
        if (!outer_it->second.read_set.empty()) {
          typename read_set_map::iterator it = outer_it->second.read_set.begin();
          typename read_set_map::iterator it_end = outer_it->second.read_set.end();
          for (; it != it_end; ++it) {
            const dbtuple * const ln = it->first;
            VERBOSE(cerr << "validating key " << util::hexify(it->first) << " @ dbtuple 0x"
                         << util::hexify(intptr_t(ln)) << " at snapshot_tid " << snapshot_tid_t.second << endl);

            if (likely(it->second.holds_lock ?
                  ln->is_latest_version(it->second.t) :
                  ln->stable_is_latest_version(it->second.t)))
              continue;

            VERBOSE(cerr << "validating key " << util::hexify(it->first) << " @ dbtuple 0x"
                         << util::hexify(intptr_t(ln)) << " at snapshot_tid " << snapshot_tid_t.second << " FAILED" << endl
                         << "  txn read version: " << g_proto_version_str(it->second.t) << endl
                         << "  ln=" << *ln << endl);

            abort_trap((reason = ABORT_REASON_READ_NODE_INTEREFERENCE));
            goto do_abort;
          }
        }

        // check the nodes we read as absent are actually absent
        if (!outer_it->second.absent_set.empty()) {
          VERBOSE(cerr << "absent_set.size(): " << outer_it->second.absent_set.size() << endl);
          typename absent_set_map::iterator it = outer_it->second.absent_set.begin();
          typename absent_set_map::iterator it_end = outer_it->second.absent_set.end();
          for (; it != it_end; ++it) {
            btree::value_type v = 0;
            if (likely(!outer_it->first->underlying_btree.search(varkey(it->first), v))) {
              // done
              VERBOSE(cerr << "absent key " << util::hexify(it->first) << " was not found in btree" << endl);
              continue;
            }
            const dbtuple * const ln = (const dbtuple *) v;
            if (it->second ? ln->latest_value_is_nil() :
                             ln->stable_latest_value_is_nil()) {
              // NB(stephentu): this seems like an optimization,
              // but its actually necessary- otherwise a newly inserted
              // key which we read first would always get aborted
              VERBOSE(cerr << "absent key " << util::hexify(it->first) << " @ dbtuple "
                           << util::hexify(ln) << " has latest value nil" << endl);
              continue;
            }
            abort_trap((reason = ABORT_REASON_READ_ABSENCE_INTEREFERENCE));
            goto do_abort;
          }
        }

        // check the nodes we scanned are still the same
        if (likely(get_flags() & TXN_FLAG_LOW_LEVEL_SCAN)) {
          // do it the fast way
          INVARIANT(outer_it->second.absent_range_set.empty());
          if (!outer_it->second.node_scan.empty()) {
            typename node_scan_map::iterator it = outer_it->second.node_scan.begin();
            typename node_scan_map::iterator it_end = outer_it->second.node_scan.end();
            for (; it != it_end; ++it) {
              const uint64_t v = btree::ExtractVersionNumber(it->first);
              if (unlikely(v != it->second)) {
                VERBOSE(cerr << "expected node " << util::hexify(it->first) << " at v="
                             << it->second << ", got v=" << v << endl);
                abort_trap((reason = ABORT_REASON_NODE_SCAN_READ_VERSION_CHANGED));
                goto do_abort;
              }
            }
          }
        } else {
          // do it the slow way
          INVARIANT(outer_it->second.node_scan.empty());
          for (absent_range_vec::iterator it = outer_it->second.absent_range_set.begin();
               it != outer_it->second.absent_range_set.end(); ++it) {
            VERBOSE(cerr << "checking absent range: " << *it << endl);
            typename txn_btree<Protocol>::template absent_range_validation_callback<Traits> c(
                &outer_it->second, commit_tid.second);
            varkey upper(it->b);
            outer_it->first->underlying_btree.search_range_call(varkey(it->a), it->has_b ? &upper : NULL, c);
            if (unlikely(c.failed())) {
              abort_trap((reason = ABORT_REASON_WRITE_NODE_INTERFERENCE));
              goto do_abort;
            }
          }
        }

      }
    }

    // commit actual records
    if (!dbtuples.empty()) {
      typename util::vec<dbtuple_pair>::type::iterator it = dbtuples.begin();
      typename util::vec<dbtuple_pair>::type::iterator it_end = dbtuples.end();
      for (; it != it_end; ++it) {
        INVARIANT(it->second.locked);
        VERBOSE(cerr << "writing dbtuple 0x" << util::hexify(intptr_t(it->first))
                     << " at commit_tid " << commit_tid.second << endl);
        it->first->prefetch();
        const dbtuple::write_record_ret ret = it->first->write_record_at(
            cast(), commit_tid.second,
            (const record_type) it->second.r.data(), it->second.r.size());
        lock_guard<dbtuple> guard(ret.second);
        if (unlikely(ret.second)) {
          // need to unlink it->first from underlying btree, replacing
          // with ret.second (atomically)
          btree::value_type old_v = 0;
          if (it->second.btr->underlying_btree.insert(
                varkey(it->second.key), (btree::value_type) ret.second, &old_v, NULL))
            // should already exist in tree
            INVARIANT(false);
          INVARIANT(old_v == (btree::value_type) it->first);
          ++evt_dbtuple_latest_replacement;
        }
        dbtuple * const latest = ret.second ? ret.second : it->first;
        if (unlikely(ret.first))
          // spill happened: signal for GC
          cast()->on_dbtuple_spill(it->second.btr, it->second.key, latest);
        if (it->second.r.empty())
          // logical delete happened: schedule physical deletion
          cast()->on_logical_delete(it->second.btr, it->second.key, latest);
        it->first->unlock();
      }
    }
  }

  state = TXN_COMMITED;
  if (commit_tid.first)
    cast()->on_tid_finish(commit_tid.second);
  clear();
  return true;

do_abort:
  // XXX: these values are possibly un-initialized
  if (snapshot_tid_t.first)
    VERBOSE(cerr << "aborting txn @ snapshot_tid " << snapshot_tid_t.second << endl);
  else
    VERBOSE(cerr << "aborting txn" << endl);
  for (typename util::vec<dbtuple_pair>::type::iterator it = dbtuples.begin();
       it != dbtuples.end(); ++it)
    if (it->second.locked)
      it->first->unlock();
  state = TXN_ABRT;
  if (commit_tid.first)
    cast()->on_tid_finish(commit_tid.second);
  clear();
  if (doThrow)
    throw transaction_abort_exception(reason);
  return false;
}

template <template <typename> class Protocol, typename Traits>
bool
transaction<Protocol, Traits>::txn_context::local_search_str(
    const transaction &t, const string_type &k, string_type &v) const
{
  ++evt_local_search_lookups;

  // XXX(stephentu): we should merge the write_set and the absent_set, so we
  // can only need to do a hash table lookup once

  if (!write_set.empty()) {
    typename transaction<Protocol, Traits>::write_set_map::const_iterator it =
      write_set.find(k);
    if (it != write_set.end()) {
      VERBOSE(cerr << "local_search_str: key " << util::hexify(k) << " found in write set"  << endl);
      VERBOSE(cerr << "  value: " << util::hexify(it->second) << endl);
      v = it->second;
      ++evt_local_search_write_set_hits;
      return true;
    }
  }

  if (!absent_set.empty()) {
    typename transaction<Protocol, Traits>::absent_set_map::const_iterator it =
      absent_set.find(k);
    if (it != absent_set.end()) {
      VERBOSE(cerr << "local_search_str: key " << util::hexify(k) << " found in absent set"  << endl);
      v.clear();
      ++evt_local_search_absent_set_hits;
      return true;
    }
  }

  if (!(t.get_flags() & TXN_FLAG_LOW_LEVEL_SCAN) &&
      key_in_absent_set(varkey(k))) {
    VERBOSE(cerr << "local_search_str: key " << util::hexify(k) << " found in absent set" << endl);
    v.clear();
    return true;
  }

  VERBOSE(cerr << "local_search_str: key " << util::hexify(k) << " not found locally" << endl);
  return false;
}

template <template <typename> class Protocol, typename Traits>
bool
transaction<Protocol, Traits>::txn_context::key_in_absent_set(const key_type &k) const
{
  std::vector<key_range_t>::const_iterator it =
    std::upper_bound(absent_range_set.begin(), absent_range_set.end(), k,
                     key_range_search_less_cmp());
  if (it == absent_range_set.end())
    return false;
  return it->key_in_range(k);
}

template <template <typename> class Protocol, typename Traits>
void
transaction<Protocol, Traits>::txn_context::add_absent_range(const key_range_t &range)
{
  // add range, possibly merging overlapping ranges
  if (range.is_empty_range())
    return;

  std::vector<key_range_t>::iterator it =
    std::upper_bound(absent_range_set.begin(), absent_range_set.end(), varkey(range.a),
                     key_range_search_less_cmp());

  if (it == absent_range_set.end()) {
    if (!absent_range_set.empty() && absent_range_set.back().b == range.a) {
      INVARIANT(absent_range_set.back().has_b);
      absent_range_set.back().has_b = range.has_b;
      absent_range_set.back().b = range.b;
    } else {
      absent_range_set.push_back(range);
    }
    return;
  }

  if (it->contains(range))
    return;

  std::vector<key_range_t> new_absent_range_set;

  // look to the left of it, and see if we need to merge with the
  // left
  bool merge_left = (it != absent_range_set.begin()) &&
    (it - 1)->b == range.a;
  new_absent_range_set.insert(
      new_absent_range_set.end(),
      absent_range_set.begin(),
      merge_left ? (it - 1) : it);
  string_type left_key = merge_left ? (it - 1)->a : min(it->a, range.a);

  if (range.has_b) {
    if (!it->has_b || it->b >= range.b) {
      // no need to look right, since it's upper bound subsumes
      if (range.b < it->a) {
        new_absent_range_set.push_back(key_range_t(left_key, range.b));
        new_absent_range_set.insert(
            new_absent_range_set.end(),
            it,
            absent_range_set.end());
      } else {
        new_absent_range_set.push_back(key_range_t(left_key, it->has_b, it->b));
        new_absent_range_set.insert(
            new_absent_range_set.end(),
            it + 1,
            absent_range_set.end());
      }
    } else {
      std::vector<key_range_t>::iterator it1;
      for (it1 = it + 1; it1 != absent_range_set.end(); ++it1)
        if (it1->a >= range.b || !it1->has_b || it1->b >= range.b)
          break;
      if (it1 == absent_range_set.end()) {
        new_absent_range_set.push_back(key_range_t(left_key, range.b));
      } else if (it1->a <= range.b) {
        new_absent_range_set.push_back(key_range_t(left_key, it1->has_b, it1->b));
        new_absent_range_set.insert(
            new_absent_range_set.end(),
            it1 + 1,
            absent_range_set.end());
      } else {
        // it1->a > range.b
        new_absent_range_set.push_back(key_range_t(left_key, range.b));
        new_absent_range_set.insert(
            new_absent_range_set.end(),
            it1,
            absent_range_set.end());
      }
    }
  } else {
    new_absent_range_set.push_back(key_range_t(left_key));
  }

  key_range_t::AssertValidRangeSet(new_absent_range_set);
  std::swap(absent_range_set, new_absent_range_set);
}

#endif /* _NDB_TXN_IMPL_H_ */
