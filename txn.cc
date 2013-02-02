#include "macros.h"
#include "txn.h"
#include "txn_btree.h"

#include <algorithm>
#include <iostream>
#include <sstream>
#include <vector>
#include <utility>

using namespace std;
using namespace util;

void
transaction::logical_node_spillblock::gc_chain()
{
  INVARIANT(rcu::in_rcu_region());
  struct logical_node_spillblock *cur = this;
  while (cur) {
    // free all the records
    vector<callback_t> &callbacks = completion_callbacks();
    const size_t n = cur->size();
    for (size_t i = 0; i < n; i++)
      for (vector<callback_t>::iterator inner_it = callbacks.begin();
           inner_it != callbacks.end(); ++inner_it)
        (*inner_it)(cur->values[i]);
    struct logical_node_spillblock *next = cur->spillblock_next;
    rcu::free(cur);
    cur = next;
  }
}

string
transaction::logical_node::VersionInfoStr(uint64_t v)
{
  stringstream buf;
  buf << "[";
  if (IsLocked(v))
    buf << "LOCKED";
  else
    buf << "-";
  buf << " | ";
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
static string (*g_proto_version_str)(uint64_t v) = proto1_version_str;

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
  const size_t n = ln.size();
  for (size_t i = 0 ; i < n; i++)
    tids.push_back(ln.versions[i]);
  vector<string> tids_s = format_tid_list(tids);
  bool has_spill = ln.spillblock_head;
  o << "[v=" << transaction::logical_node::VersionInfoStr(ln.unstable_version()) << ", tids="
    << format_list(tids_s.rbegin(), tids_s.rend()) << ", has_spill="
    <<  has_spill << "]";
  o << endl;
  const struct transaction::logical_node_spillblock *p = ln.spillblock_head;
  for (; p; p = p->spillblock_next) {
    const size_t in = p->size();
    vector<transaction::tid_t> itids;
    for (size_t j = 0; j < in; j++)
      itids.push_back(p->versions[j]);
    vector<string> itids_s = format_tid_list(itids);
    o << format_list(itids_s.rbegin(), itids_s.rend()) << endl;
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

void
transaction::commit()
{
  switch (state) {
  case TXN_EMBRYO:
  case TXN_ACTIVE:
    break;
  case TXN_COMMITED:
    return;
  case TXN_ABRT:
    throw transaction_abort_exception();
  }

  // fetch logical_nodes for insert
  map<logical_node *, pair<bool, record_t> > logical_nodes;
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
        logical_nodes[(logical_node *) v] = make_pair(false, it->second);
      } else {
        logical_node *ln = logical_node::alloc();
        // XXX: underlying btree api should return the existing value if
        // insert fails- this would allow us to avoid having to do another search
        pair<const btree::node_opaque_t *, uint64_t> insert_info;
        if (!outer_it->first->underlying_btree.insert_if_absent(
              varkey(it->first), (btree::value_type) ln, &insert_info)) {
          logical_node::release(ln);
          goto retry;
        }
        VERBOSE(cerr << "key " << hexify(it->first) << " : logical_node 0x" << hexify(intptr_t(ln)) << endl);
        if (get_flags() & TXN_FLAG_LOW_LEVEL_SCAN) {
          // update node #s
          INVARIANT(insert_info.first);
          node_scan_map::iterator nit = outer_it->second.node_scan.find(insert_info.first);
          if (nit != outer_it->second.node_scan.end()) {
            if (unlikely(nit->second != insert_info.second))
              goto do_abort;
            VERBOSE(cerr << "bump node=" << hexify(nit->first) << " from v=" << (nit->second)
                         << " -> v=" << (nit->second + 1) << endl);
            // otherwise, bump the version by 1
            nit->second++; // XXX(stephentu): this doesn't properly handle wrap-around
                           // but we're probably F-ed on a wrap around anyways for now
            SINGLE_THREADED_INVARIANT(btree::ExtractVersionNumber(nit->first) == nit->second);
          }
        }
        logical_nodes[ln] = make_pair(false, it->second);
      }
    }
  }

  if (!snapshot_tid_t.first || !logical_nodes.empty()) {
    // we don't have consistent tids, or not a read-only txn

    // lock the logical nodes in sort order
    for (map<logical_node *, pair<bool, record_t> >::iterator it = logical_nodes.begin();
         it != logical_nodes.end(); ++it) {
      VERBOSE(cerr << "locking node 0x" << hexify(intptr_t(it->first)) << endl);
      it->first->lock();
      it->second.first = true; // we locked the node
      if (!can_read_tid(it->first->latest_version()))
        // XXX(stephentu): overly conservative
        goto do_abort;
    }

    // acquire commit tid (if not read-only txn)
    if (!logical_nodes.empty()) {
      commit_tid.first = true;
      commit_tid.second = gen_commit_tid(logical_nodes);
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
        if (unlikely(!ln))
          continue;
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
          if (unlikely(c.failed()))
            goto do_abort;
        }
      }
    }

    // commit actual records
    vector<record_t> removed_vec;
    for (map<logical_node *, pair<bool, record_t> >::iterator it = logical_nodes.begin();
         it != logical_nodes.end(); ++it) {
      INVARIANT(it->second.first);
      VERBOSE(cerr << "writing logical_node 0x" << hexify(intptr_t(it->first))
                   << " at commit_tid " << commit_tid.second << endl);
      record_t removed = 0;
      if (it->first->write_record_at(this, commit_tid.second, it->second.second, &removed))
        // signal for GC
        on_logical_node_spill(it->first);
      it->first->unlock();
      if (removed)
        removed_vec.push_back(removed);
    }

    vector<callback_t> &callbacks = completion_callbacks();
    for (vector<record_t>::iterator it = removed_vec.begin();
         it != removed_vec.end(); ++it)
      for (vector<callback_t>::iterator inner_it = callbacks.begin();
           inner_it != callbacks.end(); ++inner_it)
        (*inner_it)(*it);
  }

  state = TXN_COMMITED;
  if (commit_tid.first)
    on_tid_finish(commit_tid.second);
  clear();
  return;

do_abort:
  // XXX: these values are possibly un-initialized
  if (snapshot_tid_t.first)
    VERBOSE(cerr << "aborting txn @ snapshot_tid " << snapshot_tid_t.second << endl);
  else
    VERBOSE(cerr << "aborting txn" << endl);
  for (map<logical_node *, pair<bool, record_t> >::iterator it = logical_nodes.begin();
       it != logical_nodes.end(); ++it)
    if (it->second.first)
      it->first->unlock();
  state = TXN_ABRT;
  if (commit_tid.first)
    on_tid_finish(commit_tid.second);
  clear();
  throw transaction_abort_exception();
}

void
transaction::clear()
{
  // don't clear for debugging purposes
  //ctx_map.clear();
}

void
transaction::abort()
{
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
  o << "[tid=" << g_proto_version_str(rr.t) << ", record=0x"
    << hexify(intptr_t(rr.r))
    << ", ln=" << *rr.ln << "]";
  return o;
}

void
transaction::dump_debug_info() const
{
  cerr << "Transaction (obj=0x" << hexify(this) << ") -- state "
       << transaction_state_to_cstr(state) << endl;
  cerr << "  Flags: " << transaction_flags_to_str(flags) << endl;
  cerr << "  Read/Write sets:" << endl;
  for (map<txn_btree *, txn_context>::const_iterator it = ctx_map.begin();
       it != ctx_map.end(); ++it) {
    cerr << "    Btree @ " << hexify(it->first) << ":" << endl;

    // read-set
    for (read_set_map::const_iterator rs_it = it->second.read_set.begin();
         rs_it != it->second.read_set.end(); ++rs_it)
      cerr << "      Key 0x" << hexify(rs_it->first) << " @ " << rs_it->second << endl;

    // write-set
    for (write_set_map::const_iterator ws_it = it->second.write_set.begin();
         ws_it != it->second.write_set.end(); ++ws_it)
      cerr << "      Key 0x" << hexify(ws_it->first) << " @ " << hexify(ws_it->second) << endl;

    // XXX: node set + absent ranges
  }
}

inline ostream &
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
transaction_proto1::dump_debug_info() const
{
  transaction::dump_debug_info();
  cerr << "  snapshot_tid: " << snapshot_tid << endl;
  cerr << "  global_tid: " << global_tid << endl;
}

transaction::tid_t
transaction_proto1::gen_commit_tid(const map<logical_node *, pair<bool, record_t> > &write_nodes)
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
    p->gc_chain();
  }
}

void
transaction_proto1::on_tid_finish(tid_t commit_tid)
{
  INVARIANT(state == TXN_COMMITED || state == TXN_ABRT);
  // XXX(stephentu): handle wrap around
  INVARIANT(commit_tid > last_consistent_global_tid);
  while (!__sync_bool_compare_and_swap(
         &last_consistent_global_tid, commit_tid - 1, commit_tid))
    ;
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
transaction_proto2::dump_debug_info() const
{
  transaction::dump_debug_info();
  cerr << "  current_epoch: " << current_epoch << endl;
  cerr << "  last_consistent_tid: " << g_proto_version_str(last_consistent_tid) << endl;
}

transaction::tid_t
transaction_proto2::gen_commit_tid(const map<logical_node *, pair<bool, record_t> > &write_nodes)
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
  for (map<logical_node *, pair<bool, record_t> >::const_iterator it = write_nodes.begin();
       it != write_nodes.end(); ++it) {
    INVARIANT(it->first->is_locked());
    INVARIANT(it->second.first);
    tid_t t = it->first->latest_version();
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
  while (p) {
    INVARIANT(p->size());
    // don't GC non-full blocks
    if (p->is_full() &&
        EpochId(p->versions[logical_node_spillblock::NElems - 1]) < gc_epoch) {
      *pp = 0;
      p->gc_chain();
      break;
    }
    pp = &p->spillblock_next;
    p = p->spillblock_next;
  }
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

bool
transaction_proto2::InitEpochScheme()
{
  for (size_t i = 0; i < NMaxCores; i++)
    ALWAYS_ASSERT(pthread_spin_init(&g_epoch_spinlocks[i].elem, PTHREAD_PROCESS_PRIVATE) == 0);

  // NB(stephentu): we don't use ndb_thread here, because the EpochLoop
  // does not participate in any transactions.
  pthread_t p;
  pthread_attr_t attr;
  ALWAYS_ASSERT(pthread_attr_init(&attr) == 0);
  ALWAYS_ASSERT(pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED) == 0);
  ALWAYS_ASSERT(pthread_create(&p, &attr, EpochLoop, NULL) == 0);
  ALWAYS_ASSERT(pthread_attr_destroy(&attr) == 0);
  return true;
}

bool transaction_proto2::_init_epoch_scheme_flag = InitEpochScheme();

void *
transaction_proto2::EpochLoop(void *p)
{
  for (;;) {
    // XXX(stephentu): better solution for epoch intervals?
    // this interval time defines the staleness of our consistent
    // snapshots
    struct timespec t;
    memset(&t, 0, sizeof(t));
    t.tv_sec = 2;
    nanosleep(&t, NULL);

    // bump epoch number
    // NB(stephentu): no need to do this as an atomic operation because we are
    // the only writer!
    g_current_epoch++;

    // XXX(stephentu): document why we need this memory fence
    __sync_synchronize();

    // wait for each core to finish epoch (g_current_epoch - 1)
    const size_t l_core_count = g_core_count;
    for (size_t i = 0; i < l_core_count; i++) {
      scoped_spinlock l(&g_epoch_spinlocks[i].elem);
    }

    COMPILER_MEMORY_FENCE;
    g_last_consistent_epoch = g_current_epoch;
  }
  return 0;
}

__thread ssize_t transaction_proto2::tl_core_id = -1;
__thread unsigned int transaction_proto2::tl_nest_level = 0;
__thread uint64_t transaction_proto2::tl_last_commit_tid = MIN_TID;

volatile uint64_t transaction_proto2::g_current_epoch = 0;
volatile uint64_t transaction_proto2::g_last_consistent_epoch = 0;
volatile transaction::tid_t transaction_proto2::g_core_count = 0;
volatile aligned_padded_elem<pthread_spinlock_t> transaction_proto2::g_epoch_spinlocks[NMaxCores];
