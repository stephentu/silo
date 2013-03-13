#include "macros.h"
#include "amd64.h"
#include "txn.h"
#include "txn_btree.h"
#include "lockguard.h"

#include <algorithm>
#include <iostream>
#include <sstream>
#include <vector>
#include <utility>

using namespace std;
using namespace util;

event_counter transaction::logical_node::g_evt_logical_node_creates("logical_node_creates");
event_counter transaction::logical_node::g_evt_logical_node_logical_deletes("logical_node_logical_deletes");
event_counter transaction::logical_node::g_evt_logical_node_physical_deletes("logical_node_physical_deletes");
event_counter transaction::logical_node::g_evt_logical_node_bytes_allocated("logical_node_bytes_allocated");
event_counter transaction::logical_node::g_evt_logical_node_bytes_freed("logical_node_bytes_freed");
event_counter transaction::logical_node::g_evt_logical_node_spills("logical_node_spills");

event_avg_counter transaction::logical_node::g_evt_avg_record_spill_len("avg_record_spill_len");

transaction::logical_node::~logical_node()
{
  INVARIANT(is_deleting());
  INVARIANT(!is_enqueued());
  INVARIANT(!is_locked());

  VERBOSE(cerr << "logical_node: " << hexify(intptr_t(this)) << " is being deleted" << endl);

  // free reachable nodes:
  // don't do this recursively, to avoid overflowing
  // stack w/ really long chains
  struct logical_node *cur = get_next();
  while (cur) {
    struct logical_node *tmp = cur->get_next();
    INVARIANT(!cur->is_enqueued());
    cur->clear_next(); // so cur's dtor doesn't attempt to double free
    release_no_rcu(cur); // just a wrapper for ~logical_node() + free()
    cur = tmp;
  }

  // stats-keeping
  ++g_evt_logical_node_physical_deletes;
  g_evt_logical_node_bytes_freed += (alloc_size + sizeof(logical_node));
}

void
transaction::logical_node::gc_chain()
{
  INVARIANT(rcu::in_rcu_region());
  INVARIANT(!is_latest());
  INVARIANT(!is_enqueued());
  release(this); // ~logical_node() takes care of all reachable ptrs
}

string
transaction::logical_node::VersionInfoStr(version_t v)
{
  ostringstream buf;
  buf << "[";
  buf << (IsLocked(v) ? "LOCKED" : "-") << " | ";
  buf << (IsBigType(v) ? "BIG" : "SMALL") << " | ";
  buf << (IsDeleting(v) ? "DEL" : "-") << " | ";
  buf << (IsEnqueued(v) ? "ENQ" : "-") << " | ";
  buf << (IsLatest(v) ? "LATEST" : "-") << " | ";
  buf << Version(v);
  buf << "]";
  return buf.str();
}

static string
proto1_version_str(uint64_t v) UNUSED;
static string
proto1_version_str(uint64_t v)
{
  ostringstream b;
  b << v;
  return b.str();
}

static string
proto2_version_str(uint64_t v) UNUSED;
static string
proto2_version_str(uint64_t v)
{
  ostringstream b;
  b << "[core=" << transaction_proto2::CoreId(v) << " | n="
    << transaction_proto2::NumId(v) << " | epoch=" << transaction_proto2::EpochId(v) << "]";
  return b.str();
}

// XXX(stephentu): hacky!
static string (*g_proto_version_str)(uint64_t v) = proto2_version_str;

static vector<string>
format_tid_list(const vector<transaction::tid_t> &tids)
{
  vector<string> s;
  for (vector<transaction::tid_t>::const_iterator it = tids.begin();
       it != tids.end(); ++it)
    s.push_back(g_proto_version_str(*it));
  return s;
}

inline ostream &
operator<<(ostream &o, const transaction::logical_node &ln)
{
  vector<transaction::tid_t> tids;
  vector<transaction::size_type> recs;
  tids.push_back(ln.version);
  recs.push_back(ln.size);
  vector<string> tids_s = format_tid_list(tids);
  const bool has_spill = ln.get_next();
  o << "[v=" << transaction::logical_node::VersionInfoStr(ln.unstable_version()) <<
    ", tids=" << format_list(tids_s.rbegin(), tids_s.rend()) <<
    ", sizes=" << format_list(recs.rbegin(), recs.rend()) <<
    ", has_spill=" <<  has_spill << "]";
  o << endl;
  const struct transaction::logical_node *p = ln.get_next();
  for (; p; p = p->get_next()) {
    vector<transaction::tid_t> itids;
    vector<transaction::size_type> irecs;
    itids.push_back(p->version);
    irecs.push_back(p->size);
    vector<string> itids_s = format_tid_list(itids);
    o << "[tids=" << format_list(itids_s.rbegin(), itids_s.rend())
      << ", sizes=" << format_list(irecs.rbegin(), irecs.rend())
      << "]" << endl;
  }
  return o;
}

transaction::transaction(uint64_t flags)
  : state(TXN_EMBRYO), flags(flags)
{
  // XXX(stephentu): VERY large RCU region
  rcu::region_begin();
}

transaction::~transaction()
{
  // transaction shouldn't fall out of scope w/o resolution
  // resolution means TXN_EMBRYO, TXN_COMMITED, and TXN_ABRT
  INVARIANT(state != TXN_ACTIVE);
  rcu::region_end();
}

static event_counter evt_logical_node_latest_replacement("logical_node_latest_replacement");

bool
transaction::commit(bool doThrow)
{
  // XXX(stephentu): specific abort counters, to see which
  // case we are aborting the most on (could integrate this with
  // abort_trap())

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

  // fetch logical_nodes for insert
  typename vec<lnode_pair>::type logical_nodes;
  const pair<bool, tid_t> snapshot_tid_t = consistent_snapshot_tid();
  pair<bool, tid_t> commit_tid(false, 0);

  for (ctx_map_type::iterator outer_it = ctx_map.begin();
       outer_it != ctx_map.end(); ++outer_it) {
    INVARIANT(!(get_flags() & TXN_FLAG_READ_ONLY) || outer_it->second.write_set.empty());
    for (write_set_map::iterator it = outer_it->second.write_set.begin();
         it != outer_it->second.write_set.end(); ++it) {
    retry:
      btree::value_type v = 0;
      if (outer_it->first->underlying_btree.search(varkey(it->first), v)) {
        VERBOSE(cerr << "key " << hexify(it->first) << " : logical_node 0x" << hexify(intptr_t(v)) << endl);
        logical_nodes.emplace_back(
            (logical_node *) v,
            lnode_info(outer_it->first, it->first, false, it->second));
        // mark that we hold lock in read set
        read_set_map::iterator read_it =
          outer_it->second.read_set.find((const logical_node *) v);
        if (read_it != outer_it->second.read_set.end()) {
          INVARIANT(!read_it->second.holds_lock);
          read_it->second.holds_lock = true;
        }
        // mark that we hold lock in absent set
        absent_set_map::iterator absent_it =
          outer_it->second.absent_set.find(it->first);
        if (absent_it != outer_it->second.absent_set.end()) {
          INVARIANT(!absent_it->second);
          absent_it->second = true;
        }
      } else {
        logical_node *ln = logical_node::alloc_first(
            !outer_it->first->is_mostly_append(), it->second.size());
        // XXX: underlying btree api should return the existing value if
        // insert fails- this would allow us to avoid having to do another search
        pair<const btree::node_opaque_t *, uint64_t> insert_info;
        if (!outer_it->first->underlying_btree.insert_if_absent(
              varkey(it->first), (btree::value_type) ln, &insert_info)) {
          logical_node::release_no_rcu(ln);
          goto retry;
        }
        VERBOSE(cerr << "key " << hexify(it->first) << " : logical_node 0x" << hexify(intptr_t(ln)) << endl);
        if (get_flags() & TXN_FLAG_LOW_LEVEL_SCAN) {
          // update node #s
          INVARIANT(insert_info.first);
          node_scan_map::iterator nit = outer_it->second.node_scan.find(insert_info.first);
          if (nit != outer_it->second.node_scan.end()) {
            if (unlikely(nit->second != insert_info.second)) {
              abort_trap((reason = ABORT_REASON_WRITE_NODE_INTERFERENCE));
              goto do_abort;
            }
            VERBOSE(cerr << "bump node=" << hexify(nit->first) << " from v=" << (nit->second)
                         << " -> v=" << (nit->second + 1) << endl);
            // otherwise, bump the version by 1
            nit->second++; // XXX(stephentu): this doesn't properly handle wrap-around
                           // but we're probably F-ed on a wrap around anyways for now
            SINGLE_THREADED_INVARIANT(btree::ExtractVersionNumber(nit->first) == nit->second);
          }
        }
        logical_nodes.emplace_back(
            ln, lnode_info(outer_it->first, it->first, false, it->second));
        // mark that we hold lock in read set
        read_set_map::iterator read_it =
          outer_it->second.read_set.find(ln);
        if (read_it != outer_it->second.read_set.end()) {
          INVARIANT(!read_it->second.holds_lock);
          read_it->second.holds_lock = true;
        }
        // mark that we hold lock in absent set
        absent_set_map::iterator absent_it =
          outer_it->second.absent_set.find(it->first);
        if (absent_it != outer_it->second.absent_set.end()) {
          INVARIANT(!absent_it->second);
          absent_it->second = true;
        }
      }
    }
  }

  if (!snapshot_tid_t.first || !logical_nodes.empty()) {
    // we don't have consistent tids, or not a read-only txn

    // lock the logical nodes in sort order
    sort(logical_nodes.begin(), logical_nodes.end(), LNodeComp());
    for (typename vec<lnode_pair>::type::iterator it = logical_nodes.begin();
         it != logical_nodes.end(); ++it) {
      VERBOSE(cerr << "locking node 0x" << hexify(intptr_t(it->first)) << endl);
      it->first->lock();
      it->second.locked = true; // we locked the node
      if (unlikely(it->first->is_deleting() ||
                   !it->first->is_latest() ||
                   !can_read_tid(it->first->version))) {
        // XXX(stephentu): overly conservative (with the can_read_tid() check)
        abort_trap((reason = ABORT_REASON_WRITE_NODE_INTERFERENCE));
        goto do_abort;
      }
    }

    // acquire commit tid (if not read-only txn)
    if (!logical_nodes.empty()) {
      commit_tid.first = true;
      commit_tid.second = gen_commit_tid(logical_nodes);
    }

    if (logical_nodes.empty())
      VERBOSE(cerr << "commit tid: <read-only>" << endl);
    else
      VERBOSE(cerr << "commit tid: " << g_proto_version_str(commit_tid.second) << endl);

    // do read validation
    for (ctx_map_type::iterator outer_it = ctx_map.begin();
         outer_it != ctx_map.end(); ++outer_it) {
      // check the nodes we actually read are still the latest version
      for (read_set_map::iterator it = outer_it->second.read_set.begin();
           it != outer_it->second.read_set.end(); ++it) {
        const transaction::logical_node *ln = it->first;
        VERBOSE(cerr << "validating key " << hexify(it->first) << " @ logical_node 0x"
                     << hexify(intptr_t(ln)) << " at snapshot_tid " << snapshot_tid_t.second << endl);

        if (likely(it->second.holds_lock ?
              ln->is_latest_version(it->second.t) :
              ln->stable_is_latest_version(it->second.t)))
          continue;

        VERBOSE(cerr << "validating key " << hexify(it->first) << " @ logical_node 0x"
                     << hexify(intptr_t(ln)) << " at snapshot_tid " << snapshot_tid_t.second << " FAILED" << endl
                     << "  txn read version: " << g_proto_version_str(it->second.t) << endl
                     << "  ln=" << *ln << endl);

        abort_trap((reason = ABORT_REASON_READ_NODE_INTEREFERENCE));
        goto do_abort;
      }

      // check the nodes we read as absent are actually absent
      VERBOSE(cerr << "absent_set.size(): " << outer_it->second.absent_set.size() << endl);
      for (absent_set_map::iterator it = outer_it->second.absent_set.begin();
           it != outer_it->second.absent_set.end(); ++it) {
        btree::value_type v = 0;
        if (!outer_it->first->underlying_btree.search(varkey(it->first), v)) {
          // done
          VERBOSE(cerr << "absent key " << hexify(it->first) << " was not found in btree" << endl);
          continue;
        }
        const transaction::logical_node *ln = (const transaction::logical_node *) v;
        if (it->second ? ln->latest_value_is_nil() :
                         ln->stable_latest_value_is_nil()) {
          // NB(stephentu): this seems like an optimization,
          // but its actually necessary- otherwise a newly inserted
          // key which we read first would always get aborted
          VERBOSE(cerr << "absent key " << hexify(it->first) << " @ logical_node "
                       << hexify(ln) << " has latest value nil" << endl);
          continue;
        }
        abort_trap((reason = ABORT_REASON_READ_ABSENCE_INTEREFERENCE));
        goto do_abort;
      }

      // check the nodes we scanned are still the same
      if (get_flags() & TXN_FLAG_LOW_LEVEL_SCAN) {
        // do it the fast way
        INVARIANT(outer_it->second.absent_range_set.empty());
        for (node_scan_map::iterator it = outer_it->second.node_scan.begin();
             it != outer_it->second.node_scan.end(); ++it) {
          const uint64_t v = btree::ExtractVersionNumber(it->first);
          if (unlikely(v != it->second)) {
            VERBOSE(cerr << "expected node " << hexify(it->first) << " at v="
                         << it->second << ", got v=" << v << endl);
            abort_trap((reason = ABORT_REASON_NODE_SCAN_READ_VERSION_CHANGED));
            goto do_abort;
          }
        }
      } else {
        // do it the slow way
        INVARIANT(outer_it->second.node_scan.empty());
        for (absent_range_vec::iterator it = outer_it->second.absent_range_set.begin();
             it != outer_it->second.absent_range_set.end(); ++it) {
          VERBOSE(cerr << "checking absent range: " << *it << endl);
          txn_btree::absent_range_validation_callback c(&outer_it->second, commit_tid.second);
          varkey upper(it->b);
          outer_it->first->underlying_btree.search_range_call(varkey(it->a), it->has_b ? &upper : NULL, c);
          if (unlikely(c.failed())) {
            abort_trap((reason = ABORT_REASON_WRITE_NODE_INTERFERENCE));
            goto do_abort;
          }
        }
      }
    }

    // commit actual records
    for (typename vec<lnode_pair>::type::iterator it = logical_nodes.begin();
         it != logical_nodes.end(); ++it) {
      INVARIANT(it->second.locked);
      VERBOSE(cerr << "writing logical_node 0x" << hexify(intptr_t(it->first))
                   << " at commit_tid " << commit_tid.second << endl);
      it->first->prefetch();
      const logical_node::write_record_ret ret = it->first->write_record_at(
          this, commit_tid.second,
          (const record_type) it->second.r.data(), it->second.r.size());
      lock_guard<logical_node> guard(ret.second);
      if (unlikely(ret.second)) {
        // need to unlink it->first from underlying btree, replacing
        // with ret.second (atomically)
        btree::value_type old_v = 0;
        if (it->second.btr->underlying_btree.insert(
              varkey(it->second.key), (btree::value_type) ret.second, &old_v, NULL))
          // should already exist in tree
          INVARIANT(false);
        INVARIANT(old_v == (btree::value_type) it->first);
        ++evt_logical_node_latest_replacement;
      }
      logical_node *latest = ret.second ? ret.second : it->first;
      if (unlikely(ret.first))
        // spill happened: signal for GC
        on_logical_node_spill(it->second.btr, it->second.key, latest);
      if (it->second.r.empty())
        // logical delete happened: schedule physical deletion
        on_logical_delete(it->second.btr, it->second.key, latest);
      it->first->unlock();
    }
  }

  state = TXN_COMMITED;
  if (commit_tid.first)
    on_tid_finish(commit_tid.second);
  clear();
  return true;

do_abort:
  // XXX: these values are possibly un-initialized
  if (snapshot_tid_t.first)
    VERBOSE(cerr << "aborting txn @ snapshot_tid " << snapshot_tid_t.second << endl);
  else
    VERBOSE(cerr << "aborting txn" << endl);
  for (typename vec<lnode_pair>::type::iterator it = logical_nodes.begin();
       it != logical_nodes.end(); ++it)
    if (it->second.locked)
      it->first->unlock();
  state = TXN_ABRT;
  if (commit_tid.first)
    on_tid_finish(commit_tid.second);
  clear();
  if (doThrow)
    throw transaction_abort_exception(reason);
  return false;
}

void
transaction::clear()
{
  // don't clear for debugging purposes
  //ctx_map.clear();
}

void
transaction::abort_impl(abort_reason reason)
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

static inline const char *
transaction_state_to_cstr(transaction::txn_state state)
{
  switch (state) {
  case transaction::TXN_EMBRYO: return "TXN_EMBRYO";
  case transaction::TXN_ACTIVE: return "TXN_ACTIVE";
  case transaction::TXN_ABRT: return "TXN_ABRT";
  case transaction::TXN_COMMITED: return "TXN_COMMITED";
  }
  ALWAYS_ASSERT(false);
  return 0;
}

static inline string
transaction_flags_to_str(uint64_t flags)
{
  bool first = true;
  ostringstream oss;
  if (flags & transaction::TXN_FLAG_LOW_LEVEL_SCAN) {
    oss << "TXN_FLAG_LOW_LEVEL_SCAN";
    first = false;
  }
  if (flags & transaction::TXN_FLAG_READ_ONLY) {
    if (first)
      oss << "TXN_FLAG_READ_ONLY";
    else
      oss << " | TXN_FLAG_READ_ONLY";
    first = false;
  }
  return oss.str();
}

inline ostream &
operator<<(ostream &o, const transaction::read_record_t &r)
{
  o << "[tid_read=" << g_proto_version_str(r.t)
    << ", locked=" << r.holds_lock << "]";
  return o;
}

void
transaction::dump_debug_info() const
{
  cerr << "Transaction (obj=" << hexify(this) << ") -- state "
       << transaction_state_to_cstr(state) << endl;
  cerr << "  Abort Reason: " << AbortReasonStr(reason) << endl;
  cerr << "  Flags: " << transaction_flags_to_str(flags) << endl;
  cerr << "  Read/Write sets:" << endl;
  for (ctx_map_type::const_iterator it = ctx_map.begin();
       it != ctx_map.end(); ++it) {
    cerr << "    Btree @ " << hexify(it->first) << ":" << endl;

    cerr << "      === Read Set ===" << endl;
    // read-set
    for (read_set_map::const_iterator rs_it = it->second.read_set.begin();
         rs_it != it->second.read_set.end(); ++rs_it)
      cerr << "      Node " << hexify(rs_it->first) << " @ " << rs_it->second << endl;

    cerr << "      === Absent Set ===" << endl;
    // absent-set
    for (absent_set_map::const_iterator as_it = it->second.absent_set.begin();
         as_it != it->second.absent_set.end(); ++as_it)
      cerr << "      Key 0x" << hexify(as_it->first) << " : locked=" << as_it->second << endl;

    cerr << "      === Write Set ===" << endl;
    // write-set
    for (write_set_map::const_iterator ws_it = it->second.write_set.begin();
         ws_it != it->second.write_set.end(); ++ws_it)
      if (!ws_it->second.empty())
        cerr << "      Key 0x" << hexify(ws_it->first) << " @ " << hexify(ws_it->second) << endl;
      else
        cerr << "      Key 0x" << hexify(ws_it->first) << " : remove" << endl;

    // XXX: node set + absent ranges
    cerr << "      === Absent Ranges ===" << endl;
    for (absent_range_vec::const_iterator ar_it = it->second.absent_range_set.begin();
         ar_it != it->second.absent_range_set.end(); ++ar_it)
      cerr << "      " << *ar_it << endl;
  }
}

ostream &
operator<<(ostream &o, const transaction::key_range_t &range)
{
  o << "[" << hexify(range.a) << ", ";
  if (range.has_b)
    o << hexify(range.b);
  else
    o << "+inf";
  o << ")";
  return o;
}

#define EVENT_COUNTER_IMPL_X(x) \
  event_counter transaction::g_ ## x ## _ctr(#x);
ABORT_REASONS(EVENT_COUNTER_IMPL_X)
#undef EVENT_COUNTER_IMPL_X

event_counter transaction::g_evt_read_logical_deleted_node_search(
    "read_logical_deleted_node_search");
event_counter transaction::g_evt_read_logical_deleted_node_scan(
    "read_logical_deleted_node_scan");

#ifdef CHECK_INVARIANTS
void
transaction::AssertValidRangeSet(const vector<key_range_t> &range_set)
{
  if (range_set.empty())
    return;
  key_range_t last = range_set.front();
  INVARIANT(!last.is_empty_range());
  for (vector<key_range_t>::const_iterator it = range_set.begin() + 1;
       it != range_set.end(); ++it) {
    INVARIANT(!it->is_empty_range());
    INVARIANT(last.has_b);
    INVARIANT(last.b < it->a);
  }
}
#endif /* CHECK_INVARIANTS */

string
transaction::PrintRangeSet(const vector<key_range_t> &range_set)
{
  ostringstream buf;
  buf << "<";
  bool first = true;
  for (vector<key_range_t>::const_iterator it = range_set.begin();
       it != range_set.end(); ++it, first = false) {
    if (!first)
      buf << ", ";
    buf << *it;
  }
  buf << ">";
  return buf.str();
}

void
transaction::Test()
{
  txn_context t;

  t.add_absent_range(key_range_t(u64_varkey(10), u64_varkey(20)));
  cout << PrintRangeSet(t.absent_range_set) << endl;
  t.add_absent_range(key_range_t(u64_varkey(20), u64_varkey(30)));
  cout << PrintRangeSet(t.absent_range_set) << endl;
  t.add_absent_range(key_range_t(u64_varkey(50), u64_varkey(60)));
  cout << PrintRangeSet(t.absent_range_set) << endl;
  t.add_absent_range(key_range_t(u64_varkey(31), u64_varkey(40)));
  cout << PrintRangeSet(t.absent_range_set) << endl;
  t.add_absent_range(key_range_t(u64_varkey(49), u64_varkey(50)));
  cout << PrintRangeSet(t.absent_range_set) << endl;
  t.add_absent_range(key_range_t(u64_varkey(47), u64_varkey(50)));
  cout << PrintRangeSet(t.absent_range_set) << endl;
  t.add_absent_range(key_range_t(u64_varkey(39), u64_varkey(50)));
  cout << PrintRangeSet(t.absent_range_set) << endl;
  t.add_absent_range(key_range_t(u64_varkey(100), u64_varkey(200)));
  cout << PrintRangeSet(t.absent_range_set) << endl;
  t.add_absent_range(key_range_t(u64_varkey(300), u64_varkey(400)));
  cout << PrintRangeSet(t.absent_range_set) << endl;
  t.add_absent_range(key_range_t(u64_varkey(50), u64_varkey(212)));
  cout << PrintRangeSet(t.absent_range_set) << endl;
}

static event_counter evt_local_search_lookups("local_search_lookups");
static event_counter evt_local_search_write_set_hits("local_search_write_set_hits");
static event_counter evt_local_search_absent_set_hits("local_search_absent_set_hits");

bool
transaction::txn_context::local_search_str(
    const transaction &t, const string_type &k, string_type &v) const
{
  ++evt_local_search_lookups;

  // XXX(stephentu): we should merge the write_set and the absent_set, so we
  // can only need to do a hash table lookup once

  if (!write_set.empty()) {
    transaction::write_set_map::const_iterator it = write_set.find(k);
    if (it != write_set.end()) {
      VERBOSE(cerr << "local_search_str: key " << hexify(k) << " found in write set"  << endl);
      VERBOSE(cerr << "  value: " << hexify(it->second) << endl);
      v = it->second;
      ++evt_local_search_write_set_hits;
      return true;
    }
  }

  if (!absent_set.empty()) {
    transaction::absent_set_map::const_iterator it = absent_set.find(k);
    if (it != absent_set.end()) {
      VERBOSE(cerr << "local_search_str: key " << hexify(k) << " found in absent set"  << endl);
      v.clear();
      ++evt_local_search_absent_set_hits;
      return true;
    }
  }

  if (!(t.get_flags() & TXN_FLAG_LOW_LEVEL_SCAN) &&
      key_in_absent_set(varkey(k))) {
    VERBOSE(cerr << "local_search_str: key " << hexify(k) << " found in absent set" << endl);
    v.clear();
    return true;
  }

  VERBOSE(cerr << "local_search_str: key " << hexify(k) << " not found locally" << endl);
  return false;
}

bool
transaction::txn_context::key_in_absent_set(const key_type &k) const
{
  vector<key_range_t>::const_iterator it =
    upper_bound(absent_range_set.begin(), absent_range_set.end(), k,
                key_range_search_less_cmp());
  if (it == absent_range_set.end())
    return false;
  return it->key_in_range(k);
}

void
transaction::txn_context::add_absent_range(const key_range_t &range)
{
  // add range, possibly merging overlapping ranges
  if (range.is_empty_range())
    return;

  vector<key_range_t>::iterator it =
    upper_bound(absent_range_set.begin(), absent_range_set.end(), varkey(range.a),
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

  vector<key_range_t> new_absent_range_set;

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
      vector<key_range_t>::iterator it1;
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

  AssertValidRangeSet(new_absent_range_set);
  swap(absent_range_set, new_absent_range_set);
}

transaction_proto1::transaction_proto1(uint64_t flags)
  : transaction(flags), snapshot_tid(last_consistent_global_tid)
{
}

pair<bool, transaction::tid_t>
transaction_proto1::consistent_snapshot_tid() const
{
  return make_pair(true, snapshot_tid);
}

transaction::tid_t
transaction_proto1::null_entry_tid() const
{
  return 0;
}

void
transaction_proto1::dump_debug_info() const
{
  transaction::dump_debug_info();
  cerr << "  snapshot_tid: " << snapshot_tid << endl;
  cerr << "  global_tid: " << global_tid << endl;
}

transaction::tid_t
transaction_proto1::gen_commit_tid(const typename vec<lnode_pair>::type &write_nodes)
{
  return incr_and_get_global_tid();
}

// XXX(stephentu): proto1 is unmaintained for now, will
// need to fix later

void
transaction_proto1::on_logical_node_spill(
    txn_btree *btr, const string_type &key, logical_node *ln)
{
  NDB_UNIMPLEMENTED(__PRETTY_FUNCTION__);
  //INVARIANT(ln->is_locked());
  //INVARIANT(rcu::in_rcu_region());
  //struct logical_node *p = ln->next, **pp = &ln->next;
  //for (size_t i = 0; p && i < NMaxChainLength; i++) {
  //  pp = &p->next;
  //  p = p->next;
  //}
  //if (p) {
  //  *pp = 0;
  //  p->gc_chain();
  //}
}

void
transaction_proto1::on_logical_delete(
    txn_btree *btr, const string_type &key, logical_node *ln)
{
  NDB_UNIMPLEMENTED(__PRETTY_FUNCTION__);
  //INVARIANT(ln->is_locked());
  //INVARIANT(ln->is_latest());
  //INVARIANT(!ln->size);
  //INVARIANT(!ln->is_deleting());
  ////btree::value_type removed = 0;
  ////ALWAYS_ASSERT(btr->underlying_btree.remove(varkey(key), &removed));
  ////ALWAYS_ASSERT(removed == (btree::value_type) ln);
  ////logical_node::release(ln);

  //// XXX(stephentu): cannot do the above, b/c some consistent
  //// reads might break. we need to enqueue this work to run
  //// after no consistent reads are running at the latest version
  //// of this node
}

void
transaction_proto1::on_tid_finish(tid_t commit_tid)
{
  INVARIANT(state == TXN_COMMITED || state == TXN_ABRT);
  // XXX(stephentu): handle wrap around
  INVARIANT(commit_tid > last_consistent_global_tid);
  while (!__sync_bool_compare_and_swap(
         &last_consistent_global_tid, commit_tid - 1, commit_tid))
    nop_pause();
}

transaction::tid_t
transaction_proto1::incr_and_get_global_tid()
{
  return __sync_add_and_fetch(&global_tid, 1);
}

volatile transaction::tid_t transaction_proto1::global_tid = MIN_TID;
volatile transaction::tid_t transaction_proto1::last_consistent_global_tid = 0;

transaction_proto2::transaction_proto2(uint64_t flags)
  : transaction(flags), current_epoch(0), last_consistent_tid(0)
{
  const size_t my_core_id = coreid::core_id();
  VERBOSE(cerr << "new transaction_proto2 (core=" << my_core_id
               << ", nest=" << tl_nest_level << ")" << endl);
  if (tl_nest_level++ == 0)
    g_epoch_spinlocks[my_core_id].elem.lock();
  current_epoch = g_current_epoch;
  if (get_flags() & TXN_FLAG_READ_ONLY)
    last_consistent_tid = MakeTid(0, 0, g_consistent_epoch);
}

transaction_proto2::~transaction_proto2()
{
  const size_t my_core_id = coreid::core_id();
  VERBOSE(cerr << "destroy transaction_proto2 (core=" << my_core_id
               << ", nest=" << tl_nest_level << ")" << endl);
  ALWAYS_ASSERT(tl_nest_level > 0);
  if (!--tl_nest_level) {
    g_epoch_spinlocks[my_core_id].elem.unlock();
    // XXX(stephentu): tune this
    if (tl_last_cleanup_epoch != current_epoch) {
      process_local_cleanup_nodes();
      tl_last_cleanup_epoch = current_epoch;
    }
  }
}

pair<bool, transaction::tid_t>
transaction_proto2::consistent_snapshot_tid() const
{
  if (get_flags() & TXN_FLAG_READ_ONLY)
    return make_pair(true, last_consistent_tid);
  else
    return make_pair(false, 0);
}

transaction::tid_t
transaction_proto2::null_entry_tid() const
{
  return MakeTid(0, 0, current_epoch);
}

void
transaction_proto2::dump_debug_info() const
{
  transaction::dump_debug_info();
  cerr << "  current_epoch: " << current_epoch << endl;
  cerr << "  last_consistent_tid: " << g_proto_version_str(last_consistent_tid) << endl;
}

transaction::tid_t
transaction_proto2::gen_commit_tid(const typename vec<lnode_pair>::type &write_nodes)
{
  const size_t my_core_id = coreid::core_id();
  const tid_t l_last_commit_tid = tl_last_commit_tid;
  INVARIANT(l_last_commit_tid == MIN_TID || CoreId(l_last_commit_tid) == my_core_id);

  // XXX(stephentu): wrap-around messes this up
  INVARIANT(EpochId(l_last_commit_tid) <= current_epoch);

  tid_t ret = l_last_commit_tid;

  // XXX(stephentu): I believe this is correct, but not 100% sure
  //const size_t my_core_id = 0;
  //tid_t ret = 0;

  for (ctx_map_type::const_iterator outer_it = ctx_map.begin();
       outer_it != ctx_map.end(); ++outer_it)
    for (read_set_map::const_iterator it = outer_it->second.read_set.begin();
         it != outer_it->second.read_set.end(); ++it) {
      // NB: we don't allow ourselves to do reads in future epochs
      INVARIANT(EpochId(it->second.t) <= current_epoch);
      if (it->second.t > ret)
        ret = it->second.t;
    }
  for (typename vec<lnode_pair>::type::const_iterator it = write_nodes.begin();
       it != write_nodes.end(); ++it) {
    INVARIANT(it->first->is_locked());
    INVARIANT(it->first->is_latest());
    const tid_t t = it->first->version;
    // XXX(stephentu): we are overly conservative for now- technically this
    // abort isn't necessary (we really should just write the value in the correct
    // position)
    INVARIANT(EpochId(t) <= current_epoch);
    if (t > ret)
      ret = t;
  }
  ret = MakeTid(my_core_id, NumId(ret) + 1, current_epoch);

  // XXX(stephentu): document why we need this memory fence
  __sync_synchronize();

  // XXX(stephentu): this txn hasn't actually been commited yet,
  // and could potentially be aborted - but it's ok to increase this #, since
  // subsequent txns on this core will read this # anyways
  return (tl_last_commit_tid = ret);
}

void
transaction_proto2::on_logical_node_spill(
    txn_btree *btr, const string_type &key, logical_node *ln)
{
  INVARIANT(ln->is_locked());
  INVARIANT(ln->is_latest());
  INVARIANT(rcu::in_rcu_region());

  do_logical_node_chain_cleanup(ln);

  if (!ln->size)
    // let the on_delete handler take care of this
    return;
  if (ln->is_enqueued())
    // already being taken care of by another queue
    return;
  ln->set_enqueued(true, logical_node::QUEUE_TYPE_LOCAL);
  local_cleanup_nodes().emplace_back(
    logical_node_context(btr, key, ln),
    try_logical_node_cleanup);
}

void
transaction_proto2::on_logical_delete(
    txn_btree *btr, const string_type &key, logical_node *ln)
{
  on_logical_delete_impl(btr, key, ln);
}

void
transaction_proto2::on_logical_delete_impl(
    txn_btree *btr, const string_type &key, logical_node *ln)
{
  INVARIANT(ln->is_locked());
  INVARIANT(ln->is_latest());
  INVARIANT(!ln->size);
  INVARIANT(!ln->is_deleting());
  if (ln->is_enqueued())
    return;
  ln->set_enqueued(true, logical_node::QUEUE_TYPE_LOCAL);
  INVARIANT(ln->is_enqueued());
  VERBOSE(cerr << "on_logical_delete: enq ln=0x" << hexify(intptr_t(ln))
               << " at current_epoch=" << current_epoch
               << ", latest_version_epoch=" << EpochId(ln->version) << endl
               << "  ln=" << *ln << endl);
  local_cleanup_nodes().emplace_back(
    logical_node_context(btr, key, ln),
    try_logical_node_cleanup);
}

void
transaction_proto2::enqueue_work_after_epoch(
    uint64_t epoch, work_callback_t work, void *p)
{
  const size_t id = coreid::core_id();
  // XXX(stephentu): optimize by running work when we know epoch is over
  g_work_queues[id].elem->emplace_back(epoch, work, p);
}

static event_counter evt_local_cleanup_reschedules("local_cleanup_reschedules");
static event_counter evt_local_chain_cleanups("local_chain_cleanups");
static event_counter evt_try_delete_revivals("try_delete_revivals");
static event_counter evt_try_delete_reschedules("try_delete_reschedules");
static event_counter evt_try_delete_unlinks("try_delete_unlinks");

static inline bool
chain_contains_enqueued(const transaction::logical_node *p)
{
  const transaction::logical_node *cur = p;
  while (cur) {
    if (cur->is_enqueued())
      return true;
    cur = cur->get_next();
  }
  return false;
}

void
transaction_proto2::do_logical_node_chain_cleanup(logical_node *ln)
{
  // try to clean up the chain
  INVARIANT(ln->is_latest());
  struct logical_node *p = ln, *pprev = 0;
  const bool has_chain = ln->get_next();
  bool do_break = false;
  while (p) {
    INVARIANT(p == ln || !p->is_latest());
    if (do_break)
      break;
    do_break = false;
    if (EpochId(p->version) <= g_reads_finished_epoch)
      do_break = true;
    pprev = p;
    p = p->get_next();
  }
  if (p) {
    INVARIANT(pprev);
    // can only GC a continous chain of not-enqueued.
    logical_node *last_enq = NULL, *cur = p;
    while (cur) {
      if (cur->is_enqueued())
        last_enq = cur;
      cur = cur->get_next();
    }
    p = last_enq ? last_enq->get_next() : p;
  }
  if (p) {
    INVARIANT(p != ln);
    INVARIANT(pprev);
    INVARIANT(!p->is_latest());
    INVARIANT(!chain_contains_enqueued(p));
    pprev->set_next(NULL);
    p->gc_chain();
  }
  if (has_chain && !ln->get_next())
    ++evt_local_chain_cleanups;
}

bool
transaction_proto2::try_logical_node_cleanup(const logical_node_context &ctx)
{
  bool ret = false;
  lock_guard<logical_node> lock(ctx.ln);
  INVARIANT(rcu::in_rcu_region());
  INVARIANT(ctx.ln->is_enqueued());
  INVARIANT(!ctx.ln->is_deleting());

  ctx.ln->set_enqueued(false, logical_node::QUEUE_TYPE_LOCAL);
  if (!ctx.ln->is_latest())
    // was replaced, so let the newer handlers do the work
    return false;

  do_logical_node_chain_cleanup(ctx.ln);

  if (!ctx.ln->size) {
    // latest version is a deleted entry, so try to delete
    // from the tree
    const uint64_t v = EpochId(ctx.ln->version);
    if (g_reads_finished_epoch < v || chain_contains_enqueued(ctx.ln)) {
      ret = true;
    } else {
      btree::value_type removed = 0;
      bool did_remove = ctx.btr->underlying_btree.remove(varkey(ctx.key), &removed);
      if (!did_remove) INVARIANT(false);
      INVARIANT(removed == (btree::value_type) ctx.ln);
      logical_node::release(ctx.ln);
      ++evt_try_delete_unlinks;
    }
  } else {
    ret = ctx.ln->get_next();
  }
  if (ret) {
    ctx.ln->set_enqueued(true, logical_node::QUEUE_TYPE_LOCAL);
    ++evt_local_cleanup_reschedules;
  }
  return ret;
}

bool
transaction_proto2::try_chain_cleanup(const logical_node_context &ctx)
{
  NDB_UNIMPLEMENTED(__PRETTY_FUNCTION__);
  //const uint64_t last_consistent_epoch = g_consistent_epoch;
  //bool ret = false;
  //ctx.ln->lock();
  //INVARIANT(ctx.ln->is_enqueued());
  //INVARIANT(ctx.ln->is_latest());
  //INVARIANT(!ctx.ln->is_deleting());
  //// find the first value n w/ EpochId < last_consistent_epoch.
  //// call gc_chain() on n->next
  //struct logical_node *p = ctx.ln, **pp = 0;
  //const bool has_chain = ctx.ln->next;
  //bool do_break = false;
  //while (p) {
  //  if (do_break)
  //    break;
  //  // XXX(stephentu): do we need g_reads_finished_epoch instead?
  //  if (EpochId(p->version) < last_consistent_epoch)
  //    do_break = true;
  //  pp = &p->next;
  //  p = p->next;
  //}
  //if (p) {
  //  INVARIANT(p != ctx.ln);
  //  INVARIANT(pp);
  //  *pp = 0;
  //  p->gc_chain();
  //}
  //if (has_chain && !ctx.ln->next) {
  //  ++evt_local_chain_cleanups;
  //}
  //if (ctx.ln->next) {
  //  // keep enqueued so we can clean up at a later time
  //  ret = true;
  //} else {
  //  ctx.ln->set_enqueued(false, logical_node::QUEUE_TYPE_GC); // we're done
  //}
  //// XXX(stephentu): I can't figure out why doing the following causes all
  //// sorts of race conditions (seems like the same node gets on the delete
  //// list twice)
  ////if (!ctx.ln->size)
  ////  // schedule for deletion
  ////  on_logical_delete_impl(ctx.btr, ctx.key, ctx.ln);
  //ctx.ln->unlock();
  //return ret;
}

#ifdef LOGICAL_NODE_QUEUE_TRACKING
static ostream &
operator<<(ostream &o, const transaction::logical_node::op_hist_rec &h)
{
  o << "[enq=" << h.enqueued << ", type=" << h.type << ", tid=" << h.tid << "]";
  return o;
}
#endif

bool
transaction_proto2::try_delete_logical_node(const logical_node_context &info)
{
  NDB_UNIMPLEMENTED(__PRETTY_FUNCTION__);
//  INVARIANT(info.btr);
//  INVARIANT(info.ln);
//  INVARIANT(!info.key.empty());
//  btree::value_type removed = 0;
//  uint64_t v = 0;
//  scoped_rcu_region rcu_region;
//  info.ln->lock();
//#ifdef LOGICAL_NODE_QUEUE_TRACKING
//  if (!info.ln->is_enqueued()) {
//    cerr << "try_delete_logical_node: ln=0x" << hexify(intptr_t(info.ln)) << " is NOT enqueued" << endl;
//    cerr << "  last_queue_type: " << info.ln->last_queue_type << endl;
//    cerr << "  op_hist: " << format_list(info.ln->op_hist.begin(), info.ln->op_hist.end()) << endl;
//  }
//#endif
//  INVARIANT(info.ln->is_enqueued());
//  INVARIANT(!info.ln->is_deleting());
//  //cerr << "try_delete_logical_node: setting ln=0x" << hexify(intptr_t(info.ln)) << " to NOT enqueued" << endl;
//  info.ln->set_enqueued(false, logical_node::QUEUE_TYPE_DELETE); // we are processing this record
//  if (!info.ln->is_latest() || info.ln->size) {
//    // somebody added a record again, so we don't want to delete it
//    ++evt_try_delete_revivals;
//    goto unlock_and_free;
//  }
//  VERBOSE(cerr << "logical_node: 0x" << hexify(intptr_t(info.ln)) << " is being unlinked" << endl
//               << "  g_consistent_epoch=" << g_consistent_epoch << endl
//               << "  ln=" << *info.ln << endl);
//  v = EpochId(info.ln->version);
//  if (g_reads_finished_epoch < v) {
//    // need to reschedule to run when epoch=v ends
//    VERBOSE(cerr << "  rerunning at end of epoch=" << v << endl);
//    info.ln->set_enqueued(true, logical_node::QUEUE_TYPE_DELETE); // re-queue it up
//    info.ln->unlock();
//    // don't free, b/c we need to run again
//    //epoch = v;
//    ++evt_try_delete_reschedules;
//    return true;
//  }
//  ALWAYS_ASSERT(info.btr->underlying_btree.remove(varkey(info.key), &removed));
//  ALWAYS_ASSERT(removed == (btree::value_type) info.ln);
//  logical_node::release(info.ln);
//  ++evt_try_delete_unlinks;
//unlock_and_free:
//  info.ln->unlock();
//  return false;
}

static event_avg_counter evt_avg_local_cleanup_queue_len("avg_local_cleanup_queue_len");

void
transaction_proto2::process_local_cleanup_nodes()
{
  if (unlikely(!tl_cleanup_nodes))
    return;
  INVARIANT(tl_cleanup_nodes_buf);
  INVARIANT(tl_cleanup_nodes_buf->empty());
  evt_avg_local_cleanup_queue_len.offer(tl_cleanup_nodes->size());
  for (node_cleanup_queue::iterator it = tl_cleanup_nodes->begin();
       it != tl_cleanup_nodes->end(); ++it) {
    scoped_rcu_region rcu_region;
    // XXX(stephentu): try-catch block
    if (it->second(it->first))
      // keep around
      tl_cleanup_nodes_buf->emplace_back(move(*it));
  }
  swap(*tl_cleanup_nodes, *tl_cleanup_nodes_buf);
  tl_cleanup_nodes_buf->clear();
}

void
transaction_proto2::wait_for_empty_work_queue()
{
  ALWAYS_ASSERT(!tl_nest_level);
  while (!g_epoch_loop.is_wq_empty)
    nop_pause();
}

bool
transaction_proto2::InitEpochScheme()
{
  for (size_t i = 0; i < NMaxCores; i++)
    g_work_queues[i].elem = new work_q;
  g_epoch_loop.start();
  return true;
}

transaction_proto2::epoch_loop transaction_proto2::g_epoch_loop;
bool transaction_proto2::_init_epoch_scheme_flag = InitEpochScheme();

static const uint64_t txn_epoch_us = 50 * 1000; /* 50 ms */
//static const uint64_t txn_epoch_ns = txn_epoch_us * 1000;

static event_avg_counter evt_avg_epoch_thread_queue_len("avg_epoch_thread_queue_len");
static event_avg_counter evt_avg_epoch_work_queue_len("avg_epoch_work_queue_len");

void
transaction_proto2::epoch_loop::run()
{
  // runs as daemon thread
  struct timespec t;
  NDB_MEMSET(&t, 0, sizeof(t));
  work_pq pq;
  timer loop_timer;
  for (;;) {

    const uint64_t last_loop_usec = loop_timer.lap();
    const uint64_t delay_time_usec = txn_epoch_us;
    if (last_loop_usec < delay_time_usec) {
      t.tv_nsec = (delay_time_usec - last_loop_usec) * 1000;
      nanosleep(&t, NULL);
    }

    // bump epoch number
    // NB(stephentu): no need to do this as an atomic operation because we are
    // the only writer!
    const uint64_t l_current_epoch = g_current_epoch++;

    // XXX(stephentu): document why we need this memory fence
    __sync_synchronize();

    //static uint64_t pq_size_prev = 0;
    //static timer pq_timer;
    //static int _x = 0;
    //const uint64_t pq_size_cur = pq.size();
    //const double pq_loop_time = pq_timer.lap() / 1000000.0; // sec
    //if (((++_x) % 10) == 0) {
    //  const double pq_growth_rate = double(pq_size_cur - pq_size_prev)/pq_loop_time;
    //  cerr << "pq_growth_rate: " << pq_growth_rate << " elems/sec" << endl;
    //  cerr << "pq_current_size: " << pq.size() << " elems" << endl;
    //  cerr << "last_loop_time: " << double(last_loop_usec)/1000.0 << endl;
    //}
    //pq_size_prev = pq_size_cur;

    // wait for each core to finish epoch (g_current_epoch - 1)
    const size_t l_core_count = coreid::core_count();
    for (size_t i = 0; i < l_core_count; i++) {
      lock_guard<spinlock> l(g_epoch_spinlocks[i].elem);
      work_q &core_q = *g_work_queues[i].elem;
      for (work_q::iterator it = core_q.begin(); it != core_q.end(); ++it) {
        if (pq.empty())
          is_wq_empty = false;
        pq.push(*it);
      }
      evt_avg_epoch_thread_queue_len.offer(core_q.size());
      core_q.clear();
    }

    COMPILER_MEMORY_FENCE;

    // sync point 1: l_current_epoch = (g_current_epoch - 1) is now finished.
    // at this point, all threads will be operating at epoch=g_current_epoch,
    // which means all values < g_current_epoch are consistent with each other
    g_consistent_epoch++;
    __sync_synchronize(); // XXX(stephentu): same reason as above

    // XXX(stephentu): I would really like to avoid having to loop over
    // all the threads again, but I don't know how else to ensure all the
    // threads will finish any oustanding consistent reads at
    // g_consistent_epoch - 1
    for (size_t i = 0; i < l_core_count; i++) {
      lock_guard<spinlock> l(g_epoch_spinlocks[i].elem);
    }

    // sync point 2: all consistent reads will be operating at
    // g_consistent_epoch = g_current_epoch, which means they will be
    // reading changes up to and including (g_current_epoch - 1)
    g_reads_finished_epoch++;

    VERBOSE(cerr << "epoch_loop: running work <= (l_current_epoch=" << l_current_epoch << ")" << endl);
    evt_avg_epoch_work_queue_len.offer(pq.size());
    while (!pq.empty()) {
      const work_record_t &work = pq.top();
      if (work.epoch > l_current_epoch)
        break;
      try {
        uint64_t e = 0;
        bool resched = work.work(work.p, e);
        if (resched)
          pq.push(work_record_t(e, work.work, work.p));
      } catch (...) {
        cerr << "epoch_loop: uncaught exception from enqueued work fn" << endl;
      }
      pq.pop();
    }

    if (pq.empty())
      is_wq_empty = true;

    COMPILER_MEMORY_FENCE; // XXX(stephentu) do we need?
  }
}

void
transaction_proto2::purge_local_work_queue()
{
  if (!tl_cleanup_nodes)
    return;
  // lock and dequeue all the nodes
  // XXX(stephentu): maybe we should run another iteration of
  // process_local_cleanup_nodes()
  INVARIANT(tl_cleanup_nodes_buf);
  for (node_cleanup_queue::iterator it = tl_cleanup_nodes->begin();
       it != tl_cleanup_nodes->end(); ++it) {
    it->first.ln->lock();
    ALWAYS_ASSERT(it->first.ln->is_enqueued());
    it->first.ln->set_enqueued(false, logical_node::QUEUE_TYPE_GC);
    it->first.ln->unlock();
  }
  tl_cleanup_nodes->clear();
  delete tl_cleanup_nodes;
  delete tl_cleanup_nodes_buf;
  tl_cleanup_nodes = tl_cleanup_nodes_buf = NULL;
}

void
transaction_proto2::completion_callback(ndb_thread *p)
{
  purge_local_work_queue();
}
NDB_THREAD_REGISTER_COMPLETION_CALLBACK(transaction_proto2::completion_callback)

__thread unsigned int transaction_proto2::tl_nest_level = 0;
__thread uint64_t transaction_proto2::tl_last_commit_tid = MIN_TID;
__thread uint64_t transaction_proto2::tl_last_cleanup_epoch = MIN_TID;
__thread transaction_proto2::node_cleanup_queue *transaction_proto2::tl_cleanup_nodes = NULL;
__thread transaction_proto2::node_cleanup_queue *transaction_proto2::tl_cleanup_nodes_buf = NULL;

// start epoch at 1, to avoid some boundary conditions
volatile uint64_t transaction_proto2::g_current_epoch = 1;
volatile uint64_t transaction_proto2::g_consistent_epoch = 1;
volatile uint64_t transaction_proto2::g_reads_finished_epoch = 0;

aligned_padded_elem<spinlock> transaction_proto2::g_epoch_spinlocks[NMaxCores];
volatile aligned_padded_elem<transaction_proto2::work_q*> transaction_proto2::g_work_queues[NMaxCores];
