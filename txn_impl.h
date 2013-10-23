#ifndef _NDB_TXN_IMPL_H_
#define _NDB_TXN_IMPL_H_

#include "txn.h"
#include "lockguard.h"

// base definitions

template <template <typename> class Protocol, typename Traits>
transaction<Protocol, Traits>::transaction(uint64_t flags, string_allocator_type &sa)
  : transaction_base(flags), sa(&sa)
{
  INVARIANT(rcu::s_instance.in_rcu_region());
#ifdef BTREE_LOCK_OWNERSHIP_CHECKING
  concurrent_btree::NodeLockRegionBegin();
#endif
}

template <template <typename> class Protocol, typename Traits>
transaction<Protocol, Traits>::~transaction()
{
  // transaction shouldn't fall out of scope w/o resolution
  // resolution means TXN_EMBRYO, TXN_COMMITED, and TXN_ABRT
  INVARIANT(state != TXN_ACTIVE);
  INVARIANT(rcu::s_instance.in_rcu_region());
  const unsigned cur_depth = rcu_guard_->sync()->depth();
  rcu_guard_.destroy();
  if (cur_depth == 1) {
    INVARIANT(!rcu::s_instance.in_rcu_region());
    cast()->on_post_rcu_region_completion();
  }
#ifdef BTREE_LOCK_OWNERSHIP_CHECKING
  concurrent_btree::AssertAllNodeLocksReleased();
#endif
}

template <template <typename> class Protocol, typename Traits>
void
transaction<Protocol, Traits>::clear()
{
  // it's actually *more* efficient to not call clear explicitly on the
  // read/write/absent sets, and let the destructors do the clearing- this is
  // because the destructors can take shortcuts since it knows the obj doesn't
  // have to end in a valid state
}

template <template <typename> class Protocol, typename Traits>
void
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
    dbtuple * const tuple = it->get_tuple();
    if (it->is_insert()) {
      INVARIANT(tuple->is_locked());
      this->cleanup_inserted_tuple_marker(tuple, it->get_key(), it->get_btree());
      tuple->unlock();
    }
  }

  clear();
}

template <template <typename> class Protocol, typename Traits>
void
transaction<Protocol, Traits>::cleanup_inserted_tuple_marker(
    dbtuple *marker, const std::string &key, concurrent_btree *btr)
{
  // XXX: this code should really live in txn_proto2_impl.h
  INVARIANT(marker->version == dbtuple::MAX_TID);
  INVARIANT(marker->is_locked());
  INVARIANT(marker->is_lock_owner());
  typename concurrent_btree::value_type removed = 0;
  const bool did_remove = btr->remove(varkey(key), &removed);
  if (unlikely(!did_remove)) {
#ifdef CHECK_INVARIANTS
    std::cerr << " *** could not remove key: " << util::hexify(key)  << std::endl;
#ifdef TUPLE_CHECK_KEY
    std::cerr << " *** original key        : " << util::hexify(marker->key) << std::endl;
#endif
#endif
    ALWAYS_ASSERT(false);
  }
  INVARIANT(removed == (typename concurrent_btree::value_type) marker);
  INVARIANT(marker->is_latest());
  marker->clear_latest();
  dbtuple::release(marker); // rcu free
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
    std::cerr << *rs_it << std::endl;

  std::cerr << "      === Write Set ===" << std::endl;
  // write-set
  for (typename write_set_map::const_iterator ws_it = write_set.begin();
       ws_it != write_set.end(); ++ws_it)
    std::cerr << *ws_it << std::endl;

  std::cerr << "      === Absent Set ===" << std::endl;
  // absent-set
  for (typename absent_set_map::const_iterator as_it = absent_set.begin();
       as_it != absent_set.end(); ++as_it)
    std::cerr << "      B-tree Node " << util::hexify(as_it->first)
              << " : " << as_it->second << std::endl;

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
transaction<Protocol, Traits>::handle_last_tuple_in_group(
    dbtuple_write_info &last,
    bool did_group_insert)
{
  if (did_group_insert) {
    // don't need to lock
    if (!last.is_insert())
      // we inserted the last run, and then we did 1+ more overwrites
      // to it, so we do NOT need to lock the node (again), but we DO
      // need to apply the latest write
      last.entry->set_do_write();
  } else {
    dbtuple *tuple = last.get_tuple();
    if (unlikely(tuple->version == dbtuple::MAX_TID)) {
      // if we race to put/insert w/ another txn which has inserted a new
      // record, we *must* abort b/c the other txn could try to put/insert
      // into a new record which we hold the lock on, so we must abort
      //
      // other ideas:
      // we could *not* abort if this txn did not insert any new records.
      // we could also release our insert locks and try to acquire them
      // again in sorted order
      return false; // signal abort
    }
    const dbtuple::version_t v = tuple->lock(true); // lock for write
    INVARIANT(dbtuple::IsLatest(v) == tuple->is_latest());
    last.mark_locked();
    if (unlikely(!dbtuple::IsLatest(v) ||
                 !cast()->can_read_tid(tuple->version))) {
      // XXX(stephentu): overly conservative (with the can_read_tid() check)
      return false; // signal abort
    }
    last.entry->set_do_write();
  }
  return true;
}

template <template <typename> class Protocol, typename Traits>
bool
transaction<Protocol, Traits>::commit(bool doThrow)
{
#ifdef TUPLE_MAGIC
  try {
#endif

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
  std::pair<bool, tid_t> commit_tid(false, 0);

  // copy write tuples to vector for sorting
  if (!write_set.empty()) {
    PERF_DECL(
        static std::string probe1_name(
          std::string(__PRETTY_FUNCTION__) + std::string(":lock_write_nodes:")));
    ANON_REGION(probe1_name.c_str(), &transaction_base::g_txn_commit_probe1_cg);
    INVARIANT(!is_snapshot());
    typename write_set_map::iterator it     = write_set.begin();
    typename write_set_map::iterator it_end = write_set.end();
    for (size_t pos = 0; it != it_end; ++it, ++pos) {
      INVARIANT(!it->is_insert() || it->get_tuple()->is_locked());
      write_dbtuples.emplace_back(it->get_tuple(), &(*it), it->is_insert(), pos);
    }
  }

  // read_only txns require consistent snapshots
  INVARIANT(!is_snapshot() || read_set.empty());
  INVARIANT(!is_snapshot() || write_set.empty());
  INVARIANT(!is_snapshot() || absent_set.empty());
  if (!is_snapshot()) {
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
      dbtuple_write_info *last_px = nullptr;
      bool inserted_last_run = false;
      for (; it != it_end; last_px = &(*it), ++it) {
        if (likely(last_px && last_px->tuple != it->tuple)) {
          // on boundary
          if (unlikely(!handle_last_tuple_in_group(*last_px, inserted_last_run))) {
            abort_trap((reason = ABORT_REASON_WRITE_NODE_INTERFERENCE));
            goto do_abort;
          }
          inserted_last_run = false;
        }
        if (it->is_insert()) {
          INVARIANT(!last_px || last_px->tuple != it->tuple);
          INVARIANT(it->is_locked());
          INVARIANT(it->get_tuple()->is_locked());
          INVARIANT(it->get_tuple()->is_lock_owner());
          it->entry->set_do_write(); // all inserts are marked do-write
          inserted_last_run = true;
        } else {
          INVARIANT(!it->is_locked());
        }
      }
      if (likely(last_px) &&
          unlikely(!handle_last_tuple_in_group(*last_px, inserted_last_run))) {
        abort_trap((reason = ABORT_REASON_WRITE_NODE_INTERFERENCE));
        goto do_abort;
      }
      commit_tid.first = true;
      PERF_DECL(
          static std::string probe5_name(
            std::string(__PRETTY_FUNCTION__) + std::string(":gen_commit_tid:")));
      ANON_REGION(probe5_name.c_str(), &transaction_base::g_txn_commit_probe5_cg);
      commit_tid.second = cast()->gen_commit_tid(write_dbtuples);
      VERBOSE(std::cerr << "commit tid: " << g_proto_version_str(commit_tid.second) << std::endl);
    } else {
      VERBOSE(std::cerr << "commit tid: <read-only>" << std::endl);
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
          VERBOSE(std::cerr << "validating dbtuple " << util::hexify(it->get_tuple())
                            << " at snapshot_tid "
                            << g_proto_version_str(cast()->snapshot_tid())
                            << std::endl);
          const bool found = sorted_dbtuples_contains(
              write_dbtuples, it->get_tuple());
          if (likely(found ?
                it->get_tuple()->is_latest_version(it->get_tid()) :
                it->get_tuple()->stable_is_latest_version(it->get_tid())))
            continue;

          VERBOSE(std::cerr << "validating dbtuple " << util::hexify(it->get_tuple()) << " at snapshot_tid "
                            << g_proto_version_str(cast()->snapshot_tid()) << " FAILED" << std::endl
                            << "  txn read version: " << g_proto_version_str(it->get_tid()) << std::endl
                            << "  tuple=" << *it->get_tuple() << std::endl);

          //std::cerr << "failed tuple: " << *it->get_tuple() << std::endl;

          abort_trap((reason = ABORT_REASON_READ_NODE_INTEREFERENCE));
          goto do_abort;
        }
      }

      // check btree versions have not changed
      if (!absent_set.empty()) {
        typename absent_set_map::iterator it     = absent_set.begin();
        typename absent_set_map::iterator it_end = absent_set.end();
        for (; it != it_end; ++it) {
          const uint64_t v = concurrent_btree::ExtractVersionNumber(it->first);
          if (unlikely(v != it->second.version)) {
            VERBOSE(std::cerr << "expected node " << util::hexify(it->first) << " at v="
                              << it->second.version << ", got v=" << v << std::endl);
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
        if (unlikely(!it->do_write()))
          continue;
        dbtuple * const tuple = it->get_tuple();
        INVARIANT(tuple->is_locked());
        VERBOSE(std::cerr << "writing dbtuple " << util::hexify(tuple)
                          << " at commit_tid " << g_proto_version_str(commit_tid.second)
                          << std::endl);
        if (it->is_insert()) {
          INVARIANT(tuple->version == dbtuple::MAX_TID);
          tuple->version = commit_tid.second; // allows write_record_ret() to succeed
                                              // w/o creating a new chain
        } else {
          tuple->prefetch();
          const dbtuple::write_record_ret ret =
            tuple->write_record_at(
                cast(), commit_tid.second,
                it->get_value(), it->get_writer());
          bool unlock_head = false;
          if (unlikely(ret.head_ != tuple)) {
            // tuple was replaced by ret.head_
            INVARIANT(ret.rest_ == tuple);
            // XXX: write_record_at() should acquire this lock
            ret.head_->lock(true);
            unlock_head = true;
            // need to unlink tuple from underlying btree, replacing
            // with ret.rest_ (atomically)
            typename concurrent_btree::value_type old_v = 0;
            if (it->get_btree()->insert(
                  varkey(it->get_key()), (typename concurrent_btree::value_type) ret.head_, &old_v, NULL))
              // should already exist in tree
              INVARIANT(false);
            INVARIANT(old_v == (typename concurrent_btree::value_type) tuple);
            // we don't RCU free this, because it is now part of the chain
            // (the cleaners will take care of this)
            ++evt_dbtuple_latest_replacement;
          }
          if (unlikely(ret.rest_))
            // spill happened: schedule GC task
            cast()->on_dbtuple_spill(ret.head_, ret.rest_);
          if (!it->get_value())
            // logical delete happened: schedule GC task
            cast()->on_logical_delete(ret.head_, it->get_key(), it->get_btree());
          if (unlikely(unlock_head))
            ret.head_->unlock();
        }
        VERBOSE(std::cerr << "dbtuple " << util::hexify(tuple) << " is_locked? " << tuple->is_locked() << std::endl);
      }
      // unlock
      // NB: we can no longer un-lock after doing the writes above
      for (typename dbtuple_write_info_vec::iterator it = write_dbtuples.begin();
           it != write_dbtuples.end(); ++it) {
        if (it->is_locked())
          it->tuple->unlock();
        else
          INVARIANT(!it->is_insert());
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
  if (this->is_snapshot())
    VERBOSE(std::cerr << "aborting txn @ snapshot_tid " << cast()->snapshot_tid() << std::endl);
  else
    VERBOSE(std::cerr << "aborting txn" << std::endl);

  for (typename dbtuple_write_info_vec::iterator it = write_dbtuples.begin();
       it != write_dbtuples.end(); ++it) {
    if (it->is_locked()) {
      if (it->is_insert()) {
        INVARIANT(it->entry);
        this->cleanup_inserted_tuple_marker(
            it->tuple.get(), it->entry->get_key(), it->entry->get_btree());
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

#ifdef TUPLE_MAGIC
  } catch (dbtuple::magic_failed_exception &) {
    dump_debug_info();
    ALWAYS_ASSERT(false);
  }
#endif
}

template <template <typename> class Protocol, typename Traits>
std::pair< dbtuple *, bool >
transaction<Protocol, Traits>::try_insert_new_tuple(
    concurrent_btree &btr,
    const std::string *key,
    const void *value,
    dbtuple::tuple_writer_t writer)
{
  INVARIANT(key);
  const size_t sz =
    value ? writer(dbtuple::TUPLE_WRITER_COMPUTE_NEEDED,
      value, nullptr, 0) : 0;

  // perf: ~900 tsc/alloc on istc11.csail.mit.edu
  dbtuple * const tuple = dbtuple::alloc_first(sz, true);
  if (value)
    writer(dbtuple::TUPLE_WRITER_DO_WRITE,
        value, tuple->get_value_start(), 0);
  INVARIANT(find_read_set(tuple) == read_set.end());
  INVARIANT(tuple->is_latest());
  INVARIANT(tuple->version == dbtuple::MAX_TID);
  INVARIANT(tuple->is_locked());
  INVARIANT(tuple->is_write_intent());
#ifdef TUPLE_CHECK_KEY
  tuple->key.assign(key->data(), key->size());
  tuple->tree = (void *) &btr;
#endif

  // XXX: underlying btree api should return the existing value if insert
  // fails- this would allow us to avoid having to do another search
  typename concurrent_btree::insert_info_t insert_info;
  if (unlikely(!btr.insert_if_absent(
          varkey(*key), (typename concurrent_btree::value_type) tuple, &insert_info))) {
    VERBOSE(std::cerr << "insert_if_absent failed for key: " << util::hexify(key) << std::endl);
    tuple->clear_latest();
    tuple->unlock();
    dbtuple::release_no_rcu(tuple);
    ++transaction_base::g_evt_dbtuple_write_insert_failed;
    return std::pair< dbtuple *, bool >(nullptr, false);
  }
  VERBOSE(std::cerr << "insert_if_absent suceeded for key: " << util::hexify(key) << std::endl
                    << "  new dbtuple is " << util::hexify(tuple) << std::endl);
  // update write_set
  // too expensive to be practical
  // INVARIANT(find_write_set(tuple) == write_set.end());
  write_set.emplace_back(tuple, key, value, writer, &btr, true);

  // update node #s
  INVARIANT(insert_info.node);
  if (!absent_set.empty()) {
    auto it = absent_set.find(insert_info.node);
    if (it != absent_set.end()) {
      if (unlikely(it->second.version != insert_info.old_version)) {
        abort_trap((reason = ABORT_REASON_WRITE_NODE_INTERFERENCE));
        return std::make_pair(tuple, true);
      }
      VERBOSE(std::cerr << "bump node=" << util::hexify(it->first) << " from v=" << insert_info.old_version
                        << " -> v=" << insert_info.new_version << std::endl);
      // otherwise, bump the version
      it->second.version = insert_info.new_version;
      SINGLE_THREADED_INVARIANT(concurrent_btree::ExtractVersionNumber(it->first) == it->second);
    }
  }
  return std::make_pair(tuple, false);
}

template <template <typename> class Protocol, typename Traits>
template <typename ValueReader>
bool
transaction<Protocol, Traits>::do_tuple_read(
    const dbtuple *tuple, ValueReader &value_reader)
{
  INVARIANT(tuple);
  ++evt_local_search_lookups;

  const bool is_snapshot_txn = is_snapshot();
  const transaction_base::tid_t snapshot_tid = is_snapshot_txn ?
    cast()->snapshot_tid() : static_cast<transaction_base::tid_t>(dbtuple::MAX_TID);
  transaction_base::tid_t start_t = 0;

  if (Traits::read_own_writes) {
    // this is why read_own_writes is not performant, because we have
    // to do linear scan
    auto write_set_it = find_write_set(const_cast<dbtuple *>(tuple));
    if (unlikely(write_set_it != write_set.end())) {
      ++evt_local_search_write_set_hits;
      if (!write_set_it->get_value())
        return false;
      const typename ValueReader::value_type * const px =
        reinterpret_cast<const typename ValueReader::value_type *>(
            write_set_it->get_value());
      value_reader.dup(*px, this->string_allocator());
      return true;
    }
  }

  // do the actual tuple read
  dbtuple::ReadStatus stat;
  {
    PERF_DECL(static std::string probe0_name(std::string(__PRETTY_FUNCTION__) + std::string(":do_read:")));
    ANON_REGION(probe0_name.c_str(), &private_::txn_btree_search_probe0_cg);
    tuple->prefetch();
    stat = tuple->stable_read(snapshot_tid, start_t, value_reader, this->string_allocator(), is_snapshot_txn);
    if (unlikely(stat == dbtuple::READ_FAILED)) {
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
  INVARIANT(stat == dbtuple::READ_EMPTY ||
            stat == dbtuple::READ_RECORD);
  const bool v_empty = (stat == dbtuple::READ_EMPTY);
  if (v_empty)
    ++transaction_base::g_evt_read_logical_deleted_node_search;
  if (!is_snapshot_txn)
    // read-only txns do not need read-set tracking
    // (b/c we know the values are consistent)
    read_set.emplace_back(tuple, start_t);
  return !v_empty;
}

template <template <typename> class Protocol, typename Traits>
void
transaction<Protocol, Traits>::do_node_read(
    const typename concurrent_btree::node_opaque_t *n, uint64_t v)
{
  INVARIANT(n);
  if (is_snapshot())
    return;
  auto it = absent_set.find(n);
  if (it == absent_set.end()) {
    absent_set[n].version = v;
  } else if (it->second.version != v) {
    const transaction_base::abort_reason r =
      transaction_base::ABORT_REASON_NODE_SCAN_READ_VERSION_CHANGED;
    abort_impl(r);
    throw transaction_abort_exception(r);
  }
}

#endif /* _NDB_TXN_IMPL_H_ */
