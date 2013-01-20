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

transaction::transaction()
  : state(TXN_ACTIVE)
{
}

transaction::~transaction()
{
  // transaction shouldn't fall out of scope w/o resolution
  INVARIANT(state != TXN_ACTIVE);
}

void
transaction::commit()
{
  switch (state) {
  case TXN_ACTIVE:
    break;
  case TXN_COMMITED:
    return;
  case TXN_ABRT:
    throw transaction_abort_exception();
  }

  // fetch logical_nodes for insert
  map<logical_node *, record_t> logical_nodes;
  for (map<txn_btree *, txn_context>::iterator outer_it = ctx_map.begin();
       outer_it != ctx_map.end(); ++outer_it) {
    for (write_set_map::iterator it = outer_it->second.write_set.begin();
        it != outer_it->second.write_set.end(); ++it) {
    retry:
      btree::value_type v = 0;
      if (outer_it->first->underlying_btree.search(varkey(it->first), v)) {
        VERBOSE(cout << "key " << hexify(it->first) << " : logical_node 0x" << hexify(v) << endl);
        logical_nodes[(logical_node *) v] = it->second;
      } else {
        logical_node *ln = logical_node::alloc();
        // XXX: underlying btree api should return the existing value if
        // insert fails- this would allow us to avoid having to do another search
        if (!outer_it->first->underlying_btree.insert_if_absent(varkey(it->first), (btree::value_type) ln)) {
          logical_node::release(ln);
          goto retry;
        }
        VERBOSE(cout << "key " << hexify(it->first) << " : logical_node 0x" << hexify(ln) << endl);
        logical_nodes[ln] = it->second;
      }
    }
  }

  pair<bool, tid_t> snapshot_tid_t = consistent_snapshot_tid();
  if (!snapshot_tid_t.first || !logical_nodes.empty()) {
    // we don't have consistent tids, or not a read-only txn

    // lock the logical nodes in sort order
    for (map<logical_node *, record_t>::iterator it = logical_nodes.begin();
         it != logical_nodes.end(); ++it) {
      VERBOSE(cout << "locking node 0x" << hexify(it->first) << endl);
      it->first->lock();
    }

    // acquire commit tid (if not read-only txn)
    tid_t commit_tid = logical_nodes.empty() ? 0 : gen_commit_tid(logical_nodes);

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
        VERBOSE(cout << "validating key " << hexify(it->first) << " @ logical_node 0x"
                     << hexify(ln) << " at snapshot_tid " << snapshot_tid << endl);

        if (snapshot_tid_t.first) {
          // An optimization:
          // if the latest version is > commit_tid, and the next latest version is <
          // snapshot_tid, then it is also ok because the latest version will be ordered
          // after this txn, and we know its read set does not depend on our write set
          // (since it has a txn id higher than we do)
          if (likely(did_write ?
                ln->is_snapshot_consistent(snapshot_tid_t.second, commit_tid) :
                ln->stable_is_snapshot_consistent(snapshot_tid_t.second, commit_tid)))
            continue;
        } else {
          if (likely(did_write ?
                ln->is_latest_version(it->second.t) :
                ln->stable_is_latest_version(it->second.t)))
            continue;
        }
        goto do_abort;
      }

      for (absent_range_vec::iterator it = outer_it->second.absent_range_set.begin();
           it != outer_it->second.absent_range_set.end(); ++it) {
        VERBOSE(cout << "checking absent range: " << *it << endl);
        txn_btree::absent_range_validation_callback c(&outer_it->second, commit_tid);
        varkey upper(it->b);
        outer_it->first->underlying_btree.search_range_call(varkey(it->a), it->has_b ? &upper : NULL, c);
        if (unlikely(c.failed()))
          goto do_abort;
      }
    }

    // commit actual records
    vector<record_t> removed_vec;
    for (map<logical_node *, record_t>::iterator it = logical_nodes.begin();
         it != logical_nodes.end(); ++it) {
      VERBOSE(cout << "writing logical_node 0x" << hexify(it->first)
                   << " at commit_tid " << commit_tid << endl);
      record_t removed = 0;
      it->first->write_record_at(commit_tid, it->second, &removed);
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
  clear();
  return;

do_abort:
  VERBOSE(cout << "aborting txn @ snapshot_tid " << snapshot_tid << endl);
  for (map<logical_node *, record_t>::iterator it = logical_nodes.begin();
       it != logical_nodes.end(); ++it)
    it->first->unlock();
  state = TXN_ABRT;
  clear();
  throw transaction_abort_exception();
}

void
transaction::clear()
{
  ctx_map.clear();
}

void
transaction::abort()
{
  switch (state) {
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

transaction_proto1::transaction_proto1()
  : transaction(), snapshot_tid(current_global_tid())
{
}

pair<bool, transaction::tid_t>
transaction_proto1::consistent_snapshot_tid() const
{
  return make_pair(true, snapshot_tid);
}

transaction::tid_t
transaction_proto1::gen_commit_tid(const map<logical_node *, record_t> &write_nodes) const
{
  return incr_and_get_global_tid();
}

transaction::tid_t
transaction_proto1::current_global_tid()
{
  return global_tid;
}

transaction::tid_t
transaction_proto1::incr_and_get_global_tid()
{
  return __sync_add_and_fetch(&global_tid, 1);
}

volatile transaction::tid_t transaction_proto1::global_tid = MIN_TID;

pair<bool, transaction::tid_t>
transaction_proto2::consistent_snapshot_tid() const
{
  return make_pair(false, 0);
}

transaction::tid_t
transaction_proto2::gen_commit_tid(const map<logical_node *, record_t> &write_nodes) const
{
  size_t my_core_id = core_id();
  INVARIANT(tl_last_commit_tid == MIN_TID ||
            ((tl_last_commit_tid & (CoreBits - 1)) == my_core_id));
  tid_t ret = tl_last_commit_tid;
  for (map<txn_btree *, txn_context>::const_iterator outer_it = ctx_map.begin();
       outer_it != ctx_map.end(); ++outer_it)
    for (read_set_map::const_iterator it = outer_it->second.read_set.begin();
         it != outer_it->second.read_set.end(); ++it)
      if (it->second.t > ret)
        ret = it->second.t;
  for (map<logical_node *, record_t>::const_iterator it = write_nodes.begin();
       it != write_nodes.end(); ++it) {
    INVARIANT(it->first->is_locked());
    tid_t t = it->first->latest_version();
    if (t > ret)
      ret = t;
  }
  ret &= (CoreBits - 1);
  ret += (1 << CoreBits);
  ret |= my_core_id;
  return ret;
}

size_t
transaction_proto2::core_id()
{
  if (unlikely(tl_core_id == -1))
    tl_core_id = __sync_fetch_and_add(&g_core_count, 1);
  ALWAYS_ASSERT(tl_core_id < (1 << CoreBits));
  return tl_core_id;
}

__thread transaction::tid_t transaction_proto2::tl_last_commit_tid = MIN_TID;
__thread ssize_t transaction_proto2::tl_core_id = -1;
volatile transaction::tid_t transaction_proto2::g_core_count = 0;
