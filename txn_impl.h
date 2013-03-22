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
  //read_set.clear();
  //write_set.clear();
  //absent_set.clear();
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

  // on abort, we need to go over all insert nodes and
  // release the locks
  typename write_set_map::iterator it     = write_set.begin();
  typename write_set_map::iterator it_end = write_set.end();
  for (; it != it_end; ++it) {
    dbtuple * const tuple = it->first;
    if (it->second.is_insert()) {
      INVARIANT(tuple->version == dbtuple::MAX_TID);
      INVARIANT(tuple->is_locked());
      // clear tuple, and let background reaper clean up
      tuple->version = dbtuple::MIN_TID;
      tuple->size = 0;
      tuple->unlock();
    }
  }

  clear();
}

template <template <typename> class Protocol, typename Traits>
std::pair< dbtuple *, bool >
transaction<Protocol, Traits>::try_insert_new_tuple(
    btree &btr,
    const std::string &key,
    const std::string &value)
{
  // perf: ~900 tsc/alloc on istc11.csail.mit.edu
  dbtuple * const tuple = dbtuple::alloc_first(
      (dbtuple::const_record_type) value.data(), value.size());
  INVARIANT(read_set.find(tuple) == read_set.end());
  INVARIANT(tuple->is_latest());
  INVARIANT(tuple->version == dbtuple::MAX_TID);
  tuple->lock(true);
  // XXX: underlying btree api should return the existing value if
  // insert fails- this would allow us to avoid having to do another search
  std::pair<const btree::node_opaque_t *, uint64_t> insert_info;
  if (unlikely(!btr.insert_if_absent(varkey(key), (btree::value_type) tuple, &insert_info))) {
    VERBOSE(std::cerr << "insert_if_absent failed for key: " << util::hexify(key) << std::endl);
    tuple->unlock();
    dbtuple::release_no_rcu(tuple);
    ++transaction_base::g_evt_dbtuple_write_insert_failed;
    return std::pair< dbtuple *, bool >(nullptr, false);
  }
  VERBOSE(std::cerr << "insert_if_absent suceeded for key: " << util::hexify(key) << std::endl
                    << "  new dbtuple is " << util::hexify(tuple) << std::endl);
  // update write_set
  INVARIANT(read_set.find(tuple) == read_set.end());
  INVARIANT(write_set.find(tuple) == write_set.end());
  write_set[tuple] = write_record_t(key, value, &btr, true);

  // update node #s
  INVARIANT(insert_info.first);
  auto it = absent_set.find(insert_info.first);
  if (it != absent_set.end()) {
    if (unlikely(it->second.version != insert_info.second)) {
      abort_trap((reason = ABORT_REASON_WRITE_NODE_INTERFERENCE));
      return std::make_pair(tuple, true);
    }
    VERBOSE(std::cerr << "bump node=" << util::hexify(it->first) << " from v=" << (it->second.version)
                      << " -> v=" << (it->second.version + 1) << std::endl);
    // otherwise, bump the version by 1
    it->second.version++; // XXX(stephentu): this doesn't properly handle wrap-around
    // but we're probably F-ed on a wrap around anyways for now
    SINGLE_THREADED_INVARIANT(btree::ExtractVersionNumber(it->first) == it->second);
  }
  return std::make_pair(tuple, false);
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

  std::cerr << "      === Read Set ===" << std::endl;
  // read-set
  for (typename read_set_map::const_iterator rs_it = read_set.begin();
       rs_it != read_set.end(); ++rs_it)
    std::cerr << "      Node " << util::hexify(rs_it->first) << " @ " << rs_it->second << std::endl;

  std::cerr << "      === Absent Set ===" << std::endl;
  // absent-set
  for (typename absent_set_map::const_iterator as_it = absent_set.begin();
       as_it != absent_set.end(); ++as_it)
    std::cerr << "      B-tree Node " << util::hexify(as_it->first) << " : " << as_it->second << std::endl;

  std::cerr << "      === Write Set ===" << std::endl;
  // write-set
  for (typename write_set_map::const_iterator ws_it = write_set.begin();
       ws_it != write_set.end(); ++ws_it)
    if (!ws_it->second.get_value().empty())
      std::cerr << "      Node " << util::hexify(ws_it->first) << " @ " << util::hexify(ws_it->second.get_value()) << std::endl;
    else
      std::cerr << "      Node " << util::hexify(ws_it->first) << " : remove" << std::endl;

}

template <template <typename> class Protocol, typename Traits>
std::map<std::string, uint64_t>
transaction<Protocol, Traits>::get_txn_counters() const
{
  std::map<std::string, uint64_t> ret;

  // max_read_set_size
  ret["read_set_size"] = read_set.size();;
  ret["read_set_is_large?"] = !read_set.is_small_type();

  // max_absent_set_size
  ret["absent_set_size"] = absent_set.size();
  ret["absent_set_is_large?"] = !absent_set.is_small_type();

  // max_write_set_size
  ret["write_set_size"] = write_set.size();
  ret["write_set_is_large?"] = !write_set.is_small_type();

  return ret;
}

template <template <typename> class Protocol, typename Traits>
bool
transaction<Protocol, Traits>::commit(bool doThrow)
{
  PERF_DECL(
      static std::string probe0_name(
        std::string(__PRETTY_FUNCTION__) + std::string(":total:")));
  ANON_REGION(probe0_name.c_str(), &transaction_base::g_txn_commit_probe0_cg);

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

  dbtuple_write_info_vec write_dbtuples;
  const std::pair<bool, tid_t> snapshot_tid_t = cast()->consistent_snapshot_tid();
  std::pair<bool, tid_t> commit_tid(false, 0);

  // copy write tuples to vector for sorting
  if (!write_set.empty()) {
    PERF_DECL(
        static std::string probe1_name(
          std::string(__PRETTY_FUNCTION__) + std::string(":lock_write_nodes:")));
    ANON_REGION(probe1_name.c_str(), &transaction_base::g_txn_commit_probe1_cg);
    INVARIANT(!(get_flags() & TXN_FLAG_READ_ONLY));
    typename write_set_map::iterator it     = write_set.begin();
    typename write_set_map::iterator it_end = write_set.end();
    for (; it != it_end; ++it) {
      INVARIANT(!it->second.is_insert() || it->first->is_locked());
      write_dbtuples.emplace_back(it->first, it->second.is_insert());
    }
  }

  if (!snapshot_tid_t.first || !write_dbtuples.empty()) {
    // we don't have consistent tids, or not a read-only txn

    // lock write nodes
    if (!write_dbtuples.empty()) {
      PERF_DECL(
          static std::string probe2_name(
            std::string(__PRETTY_FUNCTION__) + std::string(":lock_write_nodes:")));
      ANON_REGION(probe2_name.c_str(), &transaction_base::g_txn_commit_probe2_cg);
      // lock the logical nodes in sort order
      {
        PERF_DECL(
            static std::string probe6_name(
              std::string(__PRETTY_FUNCTION__) + std::string(":sort_write_nodes:")));
        ANON_REGION(probe6_name.c_str(), &transaction_base::g_txn_commit_probe6_cg);
        write_dbtuples.sort(); // in-place
      }
      typename dbtuple_write_info_vec::iterator it     = write_dbtuples.begin();
      typename dbtuple_write_info_vec::iterator it_end = write_dbtuples.end();
      for (; it != it_end; ++it) {
        if (it->is_insert())
          continue;
        VERBOSE(std::cerr << "locking node " << util::hexify(it->tuple) << std::endl);
        const dbtuple::version_t v = it->tuple->lock(true); // lock for write
        INVARIANT(dbtuple::IsLatest(v) == it->tuple->is_latest());
        it->mark_locked();
        if (unlikely(dbtuple::IsDeleting(v) ||
                     !dbtuple::IsLatest(v) ||
                     !cast()->can_read_tid(it->tuple->version))) {
          // XXX(stephentu): overly conservative (with the can_read_tid() check)
          abort_trap((reason = ABORT_REASON_WRITE_NODE_INTERFERENCE));
          goto do_abort;
        }
      }
      commit_tid.first = true;
      PERF_DECL(
          static std::string probe5_name(
            std::string(__PRETTY_FUNCTION__) + std::string(":gen_commit_tid:")));
      ANON_REGION(probe5_name.c_str(), &transaction_base::g_txn_commit_probe5_cg);
      commit_tid.second = cast()->gen_commit_tid(write_dbtuples);
      VERBOSE(cerr << "commit tid: " << g_proto_version_str(commit_tid.second) << endl);
    } else {
      VERBOSE(cerr << "commit tid: <read-only>" << endl);
    }

    // do read validation
    {
      PERF_DECL(
          static std::string probe3_name(
            std::string(__PRETTY_FUNCTION__) + std::string(":read_validation:")));
      ANON_REGION(probe3_name.c_str(), &transaction_base::g_txn_commit_probe3_cg);

      // check the nodes we actually read are still the latest version
      if (!read_set.empty()) {
        typename read_set_map::iterator it     = read_set.begin();
        typename read_set_map::iterator it_end = read_set.end();
        for (; it != it_end; ++it) {
          VERBOSE(std::cerr << "validating dbtuple " << util::hexify(it->first)
                            << " at snapshot_tid "
                            << g_proto_version_str(snapshot_tid_t.second)
                            << std::endl);

          if (likely(it->second.write_set ?
                it->first->is_latest_version(it->second.t) :
                it->first->stable_is_latest_version(it->second.t)))
            continue;

          VERBOSE(std::cerr << "validating dbtuple " << util::hexify(it->first) << " at snapshot_tid "
                            << g_proto_version_str(snapshot_tid_t.second) << " FAILED" << std::endl
                            << "  txn read version: " << g_proto_version_str(it->second.t) << std::endl
                            << "  tuple=" << **it->first << std::endl);

          abort_trap((reason = ABORT_REASON_READ_NODE_INTEREFERENCE));
          goto do_abort;
        }
      }

      // check btree versions have not changed
      if (!absent_set.empty()) {
        typename absent_set_map::iterator it     = absent_set.begin();
        typename absent_set_map::iterator it_end = absent_set.end();
        for (; it != it_end; ++it) {
          const uint64_t v = btree::ExtractVersionNumber(it->first);
          if (unlikely(v != it->second.version)) {
            VERBOSE(std::cerr << "expected node " << util::hexify(it->first) << " at v="
                              << it->second.verison << ", got v=" << v << std::endl);
            abort_trap((reason = ABORT_REASON_NODE_SCAN_READ_VERSION_CHANGED));
            goto do_abort;
          }
        }
      }
    }

    // commit actual records
    if (!write_dbtuples.empty()) {
      PERF_DECL(
          static std::string probe4_name(
            std::string(__PRETTY_FUNCTION__) + std::string(":write_records:")));
      ANON_REGION(probe4_name.c_str(), &transaction_base::g_txn_commit_probe4_cg);
      typename write_set_map::iterator it     = write_set.begin();
      typename write_set_map::iterator it_end = write_set.end();
      for (; it != it_end; ++it) {
        dbtuple * const tuple = it->first;
        INVARIANT(tuple->is_locked());
        VERBOSE(std::cerr << "writing dbtuple " << util::hexify(tuple)
                          << " at commit_tid " << g_proto_version_str(commit_tid.second)
                          << std::endl);
        write_record_t &wr = it->second;
        INVARIANT(!wr.needs_overwrite() || wr.is_insert());
        if (wr.is_insert()) {
          INVARIANT(tuple->version == dbtuple::MAX_TID);
          tuple->version = commit_tid.second; // allows write_record_ret() to succeed
                                              // w/o creating a new chain
          if (!wr.needs_overwrite()) {
            INVARIANT(tuple->size == wr.get_value().size());
            INVARIANT(memcmp(tuple->get_value_start(), wr.get_value().data(), tuple->size) == 0);
          }
        }
        if (!wr.is_insert() || wr.needs_overwrite()) {
          tuple->prefetch();
          const dbtuple::write_record_ret ret = tuple->write_record_at(
              cast(), commit_tid.second,
              (const record_type) wr.get_value().data(), wr.get_value().size());
          lock_guard<dbtuple> guard(ret.second, true);
          if (unlikely(ret.second)) {
            // need to unlink tuple from underlying btree, replacing
            // with ret.second (atomically)
            btree::value_type old_v = 0;
            if (wr.get_btree()->insert(
                  varkey(wr.get_key()), (btree::value_type) ret.second, &old_v, NULL))
              // should already exist in tree
              INVARIANT(false);
            INVARIANT(old_v == (btree::value_type) tuple);
            // we don't RCU free this, because it is now part of the chain
            // (the cleaners will take care of this)
            ++evt_dbtuple_latest_replacement;
          }
          dbtuple * const latest = ret.second ? ret.second : tuple;
          if (unlikely(ret.first))
            // spill happened: signal for GC
            cast()->on_dbtuple_spill(latest);
          if (wr.get_value().empty())
            // logical delete happened: schedule physical deletion
            cast()->on_logical_delete(latest);
        }
        tuple->unlock();
        VERBOSE(std::cerr << "dbtuple " << util::hexify(tuple) << " is_locked? " << tuple->is_locked() << std::endl);
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

  for (typename dbtuple_write_info_vec::iterator it = write_dbtuples.begin();
       it != write_dbtuples.end(); ++it) {
    if (it->is_locked()) {
      if (it->is_insert()) {
        INVARIANT(it->tuple->version == dbtuple::MAX_TID);
        // clear tuple, and let background reaper clean up
        it->tuple->version = dbtuple::MIN_TID;
        it->tuple->size = 0;
      }
      // XXX: potential optimization: on unlock() for abort, we don't
      // technically need to change the version number
      it->tuple->unlock();
    } else {
      INVARIANT(!it->is_insert());
    }
  }

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
transaction<Protocol, Traits>::do_tuple_read(
    const dbtuple *tuple,
    string_type &v,
    size_t max_bytes_read)
{
  INVARIANT(tuple);
  INVARIANT(max_bytes_read > 0);
  ++evt_local_search_lookups;

  const std::pair<bool, transaction_base::tid_t> snapshot_tid_t =
    cast()->consistent_snapshot_tid();
  const transaction_base::tid_t snapshot_tid = snapshot_tid_t.first ?
    snapshot_tid_t.second : static_cast<transaction_base::tid_t>(dbtuple::MAX_TID);
  const bool is_read_only_txn = get_flags() & transaction_base::TXN_FLAG_READ_ONLY;
  transaction_base::tid_t start_t = 0;

  if (!write_set.empty()) {
    auto write_set_it = write_set.find(const_cast<dbtuple *>(tuple));
    if (unlikely(write_set_it != write_set.end())) {
      v.assign(write_set_it->second.get_value().data(),
               std::min(write_set_it->second.get_value().size(), max_bytes_read));
      return !v.empty();
    }
  }

  transaction_base::read_record_t &read_rec = read_set[tuple];
  INVARIANT(!read_rec.write_set);
  // do the actual tuple read
  {
    PERF_DECL(static std::string probe0_name(std::string(__PRETTY_FUNCTION__) + std::string(":do_read:")));
    ANON_REGION(probe0_name.c_str(), &private_::txn_btree_search_probe0_cg);
    tuple->prefetch();
    if (unlikely(!tuple->stable_read(snapshot_tid, start_t, v, is_read_only_txn, max_bytes_read))) {
      const transaction_base::abort_reason r = transaction_base::ABORT_REASON_UNSTABLE_READ;
      abort_impl(r);
      throw transaction_abort_exception(r);
    }
  }
  if (unlikely(!cast()->can_read_tid(start_t))) {
    const transaction_base::abort_reason r = transaction_base::ABORT_REASON_FUTURE_TID_READ;
    abort_impl(r);
    throw transaction_abort_exception(r);
  }
  const bool v_empty = v.empty();
  if (v_empty)
    ++transaction_base::g_evt_read_logical_deleted_node_search;
  PERF_DECL(static std::string probe1_name(std::string(__PRETTY_FUNCTION__) + std::string(":readset:")));
  ANON_REGION(probe1_name.c_str(), &private_::txn_btree_search_probe1_cg);
  if (!read_rec.t) {
    // XXX(stephentu): this doesn't work if we allow wrap around
    read_rec.t = start_t;
  } else if (unlikely(read_rec.t != start_t)) {
    const transaction_base::abort_reason r =
      transaction_base::ABORT_REASON_READ_NODE_INTEREFERENCE;
    abort_impl(r);
    throw transaction_abort_exception(r);
  }
  return !v_empty;
}

template <template <typename> class Protocol, typename Traits>
void
transaction<Protocol, Traits>::do_node_read(
    const btree::node_opaque_t *n, uint64_t v)
{
  INVARIANT(n);
  auto it = absent_set.find(n);
  if (it == absent_set.end()) {
    absent_set[n] = v;
  } else if (it->second.version != v) {
    const transaction_base::abort_reason r =
      transaction_base::ABORT_REASON_NODE_SCAN_READ_VERSION_CHANGED;
    abort_impl(r);
    throw transaction_abort_exception(r);
  }
}

#endif /* _NDB_TXN_IMPL_H_ */
