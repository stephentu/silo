#include "macros.h"
#include "amd64.h"
#include "txn.h"
#include "txn_btree.h"

#include <algorithm>
#include <iostream>
#include <sstream>
#include <vector>
#include <utility>

using namespace std;
using namespace util;

transaction::logical_node::~logical_node()
{
  INVARIANT(is_deleting());
  INVARIANT(!is_enqueued());
  INVARIANT(!is_locked());

  VERBOSE(cerr << "logical_node: " << hexify(intptr_t(this)) << " is being deleted" << endl);

  // gc the chain
  if (spillblock_head)
    spillblock_head->gc_chain(false);

  // invoke callbacks on the values
  const vector<callback_t> &callbacks = completion_callbacks();
  const size_t n = size();
  for (size_t i = 0; i < n; i++)
    for (vector<callback_t>::const_iterator inner_it = callbacks.begin();
        inner_it != callbacks.end(); ++inner_it)
      (*inner_it)(values[i], false);
}

void
transaction::logical_node_spillblock::gc_chain(bool do_rcu)
{
  INVARIANT(!do_rcu || rcu::in_rcu_region());
  struct logical_node_spillblock *cur = this;
  const vector<callback_t> &callbacks = completion_callbacks();
  while (cur) {
    // free all the records
    const size_t n = cur->size();
    for (size_t i = 0; i < n; i++)
      for (vector<callback_t>::const_iterator inner_it = callbacks.begin();
           inner_it != callbacks.end(); ++inner_it)
        (*inner_it)(cur->values[i], do_rcu);
    struct logical_node_spillblock *next = cur->spillblock_next;
    if (do_rcu)
      rcu::free(cur);
    else
      delete cur;
    cur = next;
  }
}

string
transaction::logical_node::VersionInfoStr(uint64_t v)
{
  ostringstream buf;
  buf << "[";
  buf << (IsLocked(v) ? "LOCKED" : "-") << " | ";
  buf << (IsDeleting(v) ? "DEL" : "-") << " | ";
  buf << (IsEnqueued(v) ? "ENQ" : "-") << " | ";
  buf << Size(v) << " | ";
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

static vector<string>
format_rec_list(const vector<transaction::record_t> &recs)
{
  vector<string> s;
  for (vector<transaction::record_t>::const_iterator it = recs.begin();
       it != recs.end(); ++it)
    s.push_back(hexify(intptr_t(*it)));
  return s;
}

inline ostream &
operator<<(ostream &o, const transaction::logical_node &ln)
{
  vector<transaction::tid_t> tids;
  vector<transaction::record_t> recs;
  const size_t n = ln.size();
  for (size_t i = 0 ; i < n; i++) {
    tids.push_back(ln.versions[i]);
    recs.push_back(ln.values[i]);
  }
  vector<string> tids_s = format_tid_list(tids);
  vector<string> recs_s = format_rec_list(recs);
  bool has_spill = ln.spillblock_head;
  o << "[v=" << transaction::logical_node::VersionInfoStr(ln.unstable_version()) <<
    ", tids=" << format_list(tids_s.rbegin(), tids_s.rend()) <<
    ", recs=" << format_list(recs_s.rbegin(), recs_s.rend()) <<
    ", has_spill=" <<  has_spill << "]";
  o << endl;
  const struct transaction::logical_node_spillblock *p = ln.spillblock_head;
  for (; p; p = p->spillblock_next) {
    const size_t in = p->size();
    vector<transaction::tid_t> itids;
    vector<transaction::record_t> irecs;
    for (size_t j = 0; j < in; j++) {
      itids.push_back(p->versions[j]);
      irecs.push_back(p->values[j]);
    }
    vector<string> itids_s = format_tid_list(itids);
    vector<string> irecs_s = format_rec_list(irecs);
    o << "[tids=" << format_list(itids_s.rbegin(), itids_s.rend())
      << ", recs=" << format_list(irecs_s.rbegin(), irecs_s.rend())
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

struct lnode_info {
  lnode_info() {}
  lnode_info(txn_btree *btr,
             const string &key,
             bool locked,
             transaction::record_t r)
    : btr(btr),
      key(key),
      locked(locked),
      r(r)
  {}
  txn_btree *btr;
  string key;
  bool locked;
  transaction::record_t r;
};

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
  map<logical_node *, lnode_info> logical_nodes;
  const pair<bool, tid_t> snapshot_tid_t = consistent_snapshot_tid();
  pair<bool, tid_t> commit_tid(false, 0);

  for (map<txn_btree *, txn_context>::iterator outer_it = ctx_map.begin();
       outer_it != ctx_map.end(); ++outer_it) {
    INVARIANT(!(get_flags() & TXN_FLAG_READ_ONLY) || outer_it->second.write_set.empty());
    for (write_set_map::iterator it = outer_it->second.write_set.begin();
        it != outer_it->second.write_set.end(); ++it) {
    retry:
      btree::value_type v = 0;
      if (outer_it->first->underlying_btree.search(varkey(it->first), v)) {
        VERBOSE(cerr << "key " << hexify(it->first) << " : logical_node 0x" << hexify(intptr_t(v)) << endl);
        logical_nodes[(logical_node *) v] = lnode_info(outer_it->first, it->first, false, it->second);
      } else {
        logical_node *ln = logical_node::alloc();
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
        logical_nodes[ln] = lnode_info(outer_it->first, it->first, false, it->second);
      }
    }
  }

  if (!snapshot_tid_t.first || !logical_nodes.empty()) {
    // we don't have consistent tids, or not a read-only txn

    // lock the logical nodes in sort order
    vector<logical_node *> lnodes;
    lnodes.reserve(logical_nodes.size());
    for (map<logical_node *, lnode_info>::iterator it = logical_nodes.begin();
         it != logical_nodes.end(); ++it) {
      VERBOSE(cerr << "locking node 0x" << hexify(intptr_t(it->first)) << endl);
      it->first->lock();
      it->second.locked = true; // we locked the node
      if (unlikely(it->first->is_deleting() || !can_read_tid(it->first->latest_version()))) {
        // XXX(stephentu): overly conservative (with the can_read_tid() check)
        abort_trap((reason = ABORT_REASON_WRITE_NODE_INTERFERENCE));
        goto do_abort;
      }
      lnodes.push_back(it->first);
    }

    // acquire commit tid (if not read-only txn)
    if (!logical_nodes.empty()) {
      commit_tid.first = true;
      commit_tid.second = gen_commit_tid(lnodes);
    }

    if (logical_nodes.empty())
      VERBOSE(cerr << "commit tid: <read-only>" << endl);
    else
      VERBOSE(cerr << "commit tid: " << commit_tid.second << endl);

    // do read validation
    for (map<txn_btree *, txn_context>::iterator outer_it = ctx_map.begin();
         outer_it != ctx_map.end(); ++outer_it) {
      for (read_set_map::iterator it = outer_it->second.read_set.begin();
           it != outer_it->second.read_set.end(); ++it) {
        bool did_write = outer_it->second.write_set.find(it->first) != outer_it->second.write_set.end();
        transaction::logical_node *ln = NULL;
        if (likely(it->second.ln)) {
          ln = it->second.ln;
        } else {
          btree::value_type v = 0;
          if (outer_it->first->underlying_btree.search(varkey(it->first), v))
            ln = (transaction::logical_node *) v;
        }

        if (unlikely(!ln)) {
          INVARIANT(!it->second.ln); // otherwise ln == it->second.ln
          INVARIANT(!did_write); // otherwise it would be there in the tree
          INVARIANT(!it->second.r); // b/c ln did not exist when we read it
          // trivially validated
          continue;
        } else if (unlikely(!it->second.ln)) {
          // have in tree now but we didnt read it initially
          if (ln->is_deleting() || !ln->latest_value())
            // NB(stephentu): optimization: the logical value is the same
            // as when we read it (not existing)
            continue;
          abort_trap((reason = ABORT_REASON_READ_NODE_INTEREFERENCE));
          goto do_abort;
        }

        VERBOSE(cerr << "validating key " << hexify(it->first) << " @ logical_node 0x"
                     << hexify(intptr_t(ln)) << " at snapshot_tid " << snapshot_tid_t.second << endl);

        // XXX(stephentu): this optimization seems broken for now, and don't quite know why,
        // so we disable it for the time being (it doesn't help proto2)
        //if (snapshot_tid_t.first) {
        //  // An optimization:
        //  // if the latest version is > commit_tid, and the next latest version is <
        //  // snapshot_tid, then it is also ok because the latest version will be ordered
        //  // after this txn, and we know its read set does not depend on our write set
        //  // (since it has a txn id higher than we do)
        //  if (likely(did_write ?
        //        ln->is_snapshot_consistent(snapshot_tid_t.second, commit_tid.second) :
        //        ln->stable_is_snapshot_consistent(snapshot_tid_t.second, commit_tid.second)))
        //    continue;
        //} else {

          if (likely(did_write ?
                ln->is_latest_version(it->second.t) :
                ln->stable_is_latest_version(it->second.t)))
            continue;

        //}

        abort_trap((reason = ABORT_REASON_READ_NODE_INTEREFERENCE));
        goto do_abort;
      }

      if (get_flags() & TXN_FLAG_LOW_LEVEL_SCAN) {
        INVARIANT(outer_it->second.absent_range_set.empty());
        for (node_scan_map::iterator it = outer_it->second.node_scan.begin();
             it != outer_it->second.node_scan.end(); ++it) {
          uint64_t v = btree::ExtractVersionNumber(it->first);
          if (unlikely(v != it->second)) {
            VERBOSE(cerr << "expected node " << hexify(it->first) << " at v="
                         << it->second << ", got v=" << v << endl);
            abort_trap((reason = ABORT_REASON_READ_NODE_INTEREFERENCE));
            goto do_abort;
          }
        }
      } else {
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
    vector<record_t> removed_vec;
    for (map<logical_node *, lnode_info>::iterator it = logical_nodes.begin();
         it != logical_nodes.end(); ++it) {
      INVARIANT(it->second.locked);
      VERBOSE(cerr << "writing logical_node 0x" << hexify(intptr_t(it->first))
                   << " at commit_tid " << commit_tid.second << endl);
      record_t removed = 0;
      if (it->first->write_record_at(this, commit_tid.second, it->second.r, &removed))
        // signal for GC
        on_logical_node_spill(it->first);
      if (!it->second.r)
        // schedule deletion
        on_logical_delete(it->second.btr, it->second.key, it->first);
      it->first->unlock();
      if (removed)
        removed_vec.push_back(removed);
    }

    const vector<callback_t> &callbacks = completion_callbacks();
    for (vector<record_t>::iterator it = removed_vec.begin();
         it != removed_vec.end(); ++it)
      for (vector<callback_t>::const_iterator inner_it = callbacks.begin();
           inner_it != callbacks.end(); ++inner_it)
        (*inner_it)(*it, true);
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
  for (map<logical_node *, lnode_info>::iterator it = logical_nodes.begin();
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

const char *
transaction::AbortReasonStr(abort_reason reason)
{
	switch (reason) {
	case ABORT_REASON_USER:
		return "ABORT_REASON_USER";
	case ABORT_REASON_UNSTABLE_READ:
		return "ABORT_REASON_UNSTABLE_READ";
	case ABORT_REASON_NODE_SCAN_WRITE_VERSION_CHANGED:
		return "ABORT_REASON_NODE_SCAN_WRITE_VERSION_CHANGED";
	case ABORT_REASON_NODE_SCAN_READ_VERSION_CHANGED:
		return "ABORT_REASON_NODE_SCAN_READ_VERSION_CHANGED";
	case ABORT_REASON_WRITE_NODE_INTERFERENCE:
		return "ABORT_REASON_WRITE_NODE_INTERFERENCE";
	case ABORT_REASON_READ_NODE_INTEREFERENCE:
		return "ABORT_REASON_READ_NODE_INTEREFERENCE";
	default:
		break;
	}
  ALWAYS_ASSERT(false);
  return 0;
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
operator<<(ostream &o, const transaction::read_record_t &rr)
{
  o << "[tid=" << g_proto_version_str(rr.t)
    << ", record=0x" << hexify(intptr_t(rr.r))
    << ", ln_ptr=0x" << hexify(intptr_t(rr.ln))
    << ", ln=" << *rr.ln << "]";
  return o;
}

void
transaction::dump_debug_info(abort_reason reason) const
{
  cerr << "Transaction (obj=0x" << hexify(this) << ") -- state "
       << transaction_state_to_cstr(state) << endl;
  cerr << "  Abort Reason: " << AbortReasonStr(reason) << endl;
  cerr << "  Flags: " << transaction_flags_to_str(flags) << endl;
  cerr << "  Read/Write sets:" << endl;
  for (map<txn_btree *, txn_context>::const_iterator it = ctx_map.begin();
       it != ctx_map.end(); ++it) {
    cerr << "    Btree @ " << hexify(it->first) << ":" << endl;

    cerr << "      === Read Set ===" << endl;
    // read-set
    for (read_set_map::const_iterator rs_it = it->second.read_set.begin();
         rs_it != it->second.read_set.end(); ++rs_it)
      cerr << "      Key 0x" << hexify(rs_it->first) << " @ " << rs_it->second << endl;

    cerr << "      === Write Set ===" << endl;
    // write-set
    for (write_set_map::const_iterator ws_it = it->second.write_set.begin();
         ws_it != it->second.write_set.end(); ++ws_it)
      cerr << "      Key 0x" << hexify(ws_it->first) << " @ " << hexify(ws_it->second) << endl;

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

bool
transaction::txn_context::local_search_str(const string &k, record_t &v) const
{
  {
    transaction::write_set_map::const_iterator it = write_set.find(k);
    if (it != write_set.end()) {
      v = it->second;
      return true;
    }
  }

  {
    transaction::read_set_map::const_iterator it = read_set.find(k);
    if (it != read_set.end()) {
      v = it->second.r;
      return true;
    }
  }

  if (key_in_absent_set(varkey(k))) {
    v = NULL;
    return true;
  }

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
  string left_key = merge_left ? (it - 1)->a : min(it->a, range.a);

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

bool
transaction::register_cleanup_callback(callback_t callback)
{
  completion_callbacks().push_back(callback);
  return true;
}

vector<transaction::callback_t> &
transaction::completion_callbacks()
{
  static vector<callback_t> *callbacks = NULL;
  if (!callbacks)
    callbacks = new vector<callback_t>;
  return *callbacks;
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

void
transaction_proto1::dump_debug_info(abort_reason reason) const
{
  transaction::dump_debug_info(reason);
  cerr << "  snapshot_tid: " << snapshot_tid << endl;
  cerr << "  global_tid: " << global_tid << endl;
}

transaction::tid_t
transaction_proto1::gen_commit_tid(const vector<logical_node *> &write_nodes)
{
  return incr_and_get_global_tid();
}

void
transaction_proto1::on_logical_node_spill(logical_node *ln)
{
  INVARIANT(ln->is_locked());
  INVARIANT(rcu::in_rcu_region());
  struct logical_node_spillblock *p = ln->spillblock_head, **pp = &ln->spillblock_head;
  for (size_t i = 0; p && i < NMaxChainLength; i++) {
    pp = &p->spillblock_next;
    p = p->spillblock_next;
  }
  if (p) {
    *pp = 0;
    p->gc_chain(true);
  }
}

void
transaction_proto1::on_logical_delete(
    txn_btree *btr, const string &key, logical_node *ln)
{
  INVARIANT(ln->is_locked());
  INVARIANT(!ln->latest_value());
  btree::value_type removed = 0;
  ALWAYS_ASSERT(btr->underlying_btree.remove(varkey(key), &removed));
  ALWAYS_ASSERT(removed == (btree::value_type) ln);
  logical_node::release(ln);
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
  size_t my_core_id = core_id();
  VERBOSE(cerr << "new transaction_proto2 (core=" << my_core_id
               << ", nest=" << tl_nest_level << ")" << endl);
  if (tl_nest_level++ == 0)
    ALWAYS_ASSERT(pthread_spin_lock(&g_epoch_spinlocks[my_core_id].elem) == 0);
  current_epoch = g_current_epoch;
  if (get_flags() & TXN_FLAG_READ_ONLY)
    last_consistent_tid = MakeTid(0, 0, g_last_consistent_epoch);
}

transaction_proto2::~transaction_proto2()
{
  size_t my_core_id = core_id();
  VERBOSE(cerr << "destroy transaction_proto2 (core=" << my_core_id
               << ", nest=" << tl_nest_level << ")" << endl);
  ALWAYS_ASSERT(tl_nest_level > 0);
  if (!--tl_nest_level)
    ALWAYS_ASSERT(pthread_spin_unlock(&g_epoch_spinlocks[my_core_id].elem) == 0);
}

pair<bool, transaction::tid_t>
transaction_proto2::consistent_snapshot_tid() const
{
  if (get_flags() & TXN_FLAG_READ_ONLY)
    return make_pair(true, last_consistent_tid);
  else
    return make_pair(false, 0);
}

void
transaction_proto2::dump_debug_info(abort_reason reason) const
{
  transaction::dump_debug_info(reason);
  cerr << "  current_epoch: " << current_epoch << endl;
  cerr << "  last_consistent_tid: " << g_proto_version_str(last_consistent_tid) << endl;
}

transaction::tid_t
transaction_proto2::gen_commit_tid(const vector<logical_node *> &write_nodes)
{
  size_t my_core_id = core_id();
  tid_t l_last_commit_tid = tl_last_commit_tid;
  INVARIANT(l_last_commit_tid == MIN_TID || CoreId(l_last_commit_tid) == my_core_id);

  // XXX(stephentu): wrap-around messes this up
  INVARIANT(EpochId(l_last_commit_tid) <= current_epoch);

  tid_t ret = l_last_commit_tid;
  for (map<txn_btree *, txn_context>::const_iterator outer_it = ctx_map.begin();
       outer_it != ctx_map.end(); ++outer_it)
    for (read_set_map::const_iterator it = outer_it->second.read_set.begin();
         it != outer_it->second.read_set.end(); ++it) {
      // NB: we don't allow ourselves to do reads in future epochs
      INVARIANT(EpochId(it->second.t) <= current_epoch);
      if (it->second.t > ret)
        ret = it->second.t;
    }
  for (vector<logical_node *>::const_iterator it = write_nodes.begin();
       it != write_nodes.end(); ++it) {
    INVARIANT((*it)->is_locked());
    tid_t t = (*it)->latest_version();
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
transaction_proto2::on_logical_node_spill(logical_node *ln)
{
  INVARIANT(ln->is_locked());
  INVARIANT(rcu::in_rcu_region());

  // XXX(stephentu): punt on wrap-around for now
  if (current_epoch < 2 || !ln->spillblock_head)
    return;
  const uint64_t gc_epoch = current_epoch - 2;

  struct logical_node_spillblock *p = ln->spillblock_head, **pp = &ln->spillblock_head;

  // we store the number of the last GC in the 1st spillblock's opaque counter
  if (p->gc_opaque_value >= gc_epoch)
    // don't need to GC anymore
    return;

  p->gc_opaque_value = gc_epoch;

  // chase the pointers until we find a value which has epoch < gc_epoch. Then we gc the
  // entire chain
  bool do_gc = false;
  while (p) {
    INVARIANT(p->size());
    if (do_gc) {
      // victim found
      *pp = 0;
      p->gc_chain(true);
      break;
    }
    if (p->is_full() /* don't GC non-full blocks */ &&
        EpochId(p->versions[logical_node_spillblock::NElems - 1]) < gc_epoch)
      // NB(stephentu): gc the NEXT spillblock- this guarantees us that we'll
      // still be able to perform a consistent read at the gc_epoch
      do_gc = true;
    pp = &p->spillblock_next;
    p = p->spillblock_next;
  }
}

struct try_delete_info {
  try_delete_info(txn_btree *btr, const string &key, transaction::logical_node *ln)
    : btr(btr), key(key), ln(ln) {}
  txn_btree *btr;
  string key;
  transaction::logical_node *ln;
};

void
transaction_proto2::on_logical_delete(
    txn_btree *btr, const string &key, logical_node *ln)
{
  INVARIANT(ln->is_locked());
  INVARIANT(!ln->latest_value());
  INVARIANT(!ln->is_deleting());
  if (ln->is_enqueued())
    return;
  ln->set_enqueued(true);
  struct try_delete_info *info = new try_delete_info(btr, key, ln);
  VERBOSE(cerr << "on_logical_delete: enq ln=0x" << hexify(intptr_t(info->ln))
               << " at current_epoch=" << current_epoch
               << ", latest_version_epoch=" << EpochId(ln->latest_version()) << endl
               << "  ln=" << *info->ln << endl);
  enqueue_work_after_current_epoch(
      EpochId(ln->latest_version()),
      try_delete_logical_node,
      (void *) info);
}

size_t
transaction_proto2::core_id()
{
  if (unlikely(tl_core_id == -1)) {
    // initialize per-core data structures
    tl_core_id = __sync_fetch_and_add(&g_core_count, 1);
    // did we exceed max cores?
    ALWAYS_ASSERT(tl_core_id < (1 << CoreBits));
  }
  return tl_core_id;
}

void
transaction_proto2::enqueue_work_after_current_epoch(
    uint64_t epoch, work_callback_t work, void *p)
{
  const size_t id = core_id();
  g_work_queues[id].elem->push_back(work_record_t(epoch, work, p));
}

bool
transaction_proto2::try_delete_logical_node(void *p, uint64_t &epoch)
{
  try_delete_info *info = (try_delete_info *) p;
  btree::value_type removed = 0;
  uint64_t v = 0;
  scoped_rcu_region rcu_region;
  info->ln->lock();
  INVARIANT(info->ln->is_enqueued());
  INVARIANT(!info->ln->is_deleting());
  info->ln->set_enqueued(false); // we are processing this record
  if (info->ln->latest_value())
    // somebody added a record again, so we don't want to delete it
    goto unlock_and_free;
  VERBOSE(cerr << "logical_node: 0x" << hexify(intptr_t(info->ln)) << " is being unlinked" << endl
               << "  g_last_consistent_epoch=" << g_last_consistent_epoch << endl
               << "  ln=" << *info->ln << endl);
  v = EpochId(info->ln->latest_version());
  if (g_last_consistent_epoch <= v) {
    // need to reschedule to run when epoch=v ends
    VERBOSE(cerr << "  rerunning at end of epoch=" << v << endl);
    info->ln->set_enqueued(true); // re-queue it up
    info->ln->unlock();
    // don't free, b/c we need to run again
    epoch = v;
    return true;
  }
  ALWAYS_ASSERT(info->btr->underlying_btree.remove(varkey(info->key), &removed));
  ALWAYS_ASSERT(removed == (btree::value_type) info->ln);
  logical_node::release(info->ln);
unlock_and_free:
  info->ln->unlock();
  delete info;
  return false;
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
  for (size_t i = 0; i < NMaxCores; i++) {
    ALWAYS_ASSERT(pthread_spin_init(&g_epoch_spinlocks[i].elem, PTHREAD_PROCESS_PRIVATE) == 0);
    g_work_queues[i].elem = new work_q;
  }
  g_epoch_loop.start();
  return true;
}

bool transaction_proto2::_init_epoch_scheme_flag = InitEpochScheme();

void
transaction_proto2::epoch_loop::run()
{
  work_pq pq;
  for (;;) {
    // XXX(stephentu): better solution for epoch intervals?
    // this interval time defines the staleness of our consistent
    // snapshots
    struct timespec t;
    memset(&t, 0, sizeof(t));

    // XXX(stephentu): time how much time we spent
    // executing work, and subtract that time from here
    t.tv_nsec = 100 * 1000000; // 100 ms

    nanosleep(&t, NULL);

    // bump epoch number
    // NB(stephentu): no need to do this as an atomic operation because we are
    // the only writer!
    const uint64_t l_current_epoch = g_current_epoch++;

    // XXX(stephentu): document why we need this memory fence
    __sync_synchronize();

    // wait for each core to finish epoch (g_current_epoch - 1)
    const size_t l_core_count = g_core_count;
    for (size_t i = 0; i < l_core_count; i++) {
      scoped_spinlock l(&g_epoch_spinlocks[i].elem);
      work_q &core_q = *g_work_queues[i].elem;
      for (work_q::iterator it = core_q.begin(); it != core_q.end(); ++it) {
        if (pq.empty())
          is_wq_empty = false;
        pq.push(*it);
      }
      core_q.clear();
    }

    COMPILER_MEMORY_FENCE;
    g_last_consistent_epoch = g_current_epoch;
    __sync_synchronize(); // XXX(stephentu): do we need this?

    // l_current_epoch = (g_current_epoch - 1) is now finished. at this point:
    // A) all threads will be operating at epoch=g_current_epoch
    // B) all consistent reads will be operating at g_last_consistent_epoch =
    //    g_current_epoch, which means they will be reading changes up to
    //    and including (g_current_epoch - 1)
    VERBOSE(cerr << "epoch_loop: running work <= (l_current_epoch=" << l_current_epoch << ")" << endl);
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

transaction_proto2::epoch_loop transaction_proto2::g_epoch_loop;

__thread ssize_t transaction_proto2::tl_core_id = -1;
__thread unsigned int transaction_proto2::tl_nest_level = 0;
__thread uint64_t transaction_proto2::tl_last_commit_tid = MIN_TID;

volatile uint64_t transaction_proto2::g_current_epoch = 0;
volatile uint64_t transaction_proto2::g_last_consistent_epoch = 0;
volatile transaction::tid_t transaction_proto2::g_core_count = 0;
volatile aligned_padded_elem<pthread_spinlock_t> transaction_proto2::g_epoch_spinlocks[NMaxCores];
volatile aligned_padded_elem<transaction_proto2::work_q*> transaction_proto2::g_work_queues[NMaxCores];
