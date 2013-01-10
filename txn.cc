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
  : snapshot_tid(current_global_tid()),
    resolved(false),
    btree(NULL)
{
}

transaction::~transaction()
{
  // transaction shouldn't fall out of scope w/o resolution
  INVARIANT(resolved);
}

void
transaction::commit()
{
  INVARIANT(!resolved);

  // fetch logical_nodes for insert
  map<logical_node *, record_t> logical_nodes;
  for (map<string, record_t>::iterator it = write_set.begin();
       it != write_set.end(); ++it) {
  retry:
    btree::value_type v = 0;
    if (btree->underlying_btree.search(varkey(it->first), v)) {
      VERBOSE(cout << "key " << hexify(it->first) << " : logical_node 0x" << hexify(v) << endl);
      prefetch_node(v);
      logical_nodes[(logical_node *) v] = it->second;
    } else {
      logical_node *ln = logical_node::alloc();
      // XXX: underlying btree api should return the existing value if
      // insert fails- this would allow us to avoid having to do another search
      if (!btree->underlying_btree.insert_if_absent(varkey(it->first), (btree::value_type) ln)) {
        logical_node::release(ln);
        goto retry;
      }
      VERBOSE(cout << "key " << hexify(it->first) << " : logical_node 0x" << hexify(ln) << endl);
      prefetch_node(ln);
      logical_nodes[ln] = it->second;
    }
  }

  if (!logical_nodes.empty()) {
    // not a read-only txn

    // lock the logical nodes in sort order
    for (map<logical_node *, record_t>::iterator it = logical_nodes.begin();
         it != logical_nodes.end(); ++it) {
      VERBOSE(cout << "locking node 0x" << hexify(it->first) << endl);
      it->first->lock();
    }

    // acquire commit tid
    tid_t commit_tid = incr_and_get_global_tid();

    // do read validation
    for (map<string, read_record_t>::iterator it = read_set.begin();
         it != read_set.end(); ++it) {
      bool did_write = write_set.find(it->first) != write_set.end();
      transaction::logical_node *ln = NULL;
      if (likely(it->second.ln)) {
        ln = it->second.ln;
      } else {
        btree::value_type v = 0;
        if (btree->underlying_btree.search(varkey(it->first), v))
          ln = (transaction::logical_node *) v;
      }
      if (unlikely(!ln))
        continue;
      VERBOSE(cout << "validating key " << hexify(it->first) << " @ logical_node 0x"
                   << hexify(ln) << " at snapshot_tid " << snapshot_tid << endl);

      // An optimization:
      // if the latest version is > commit_tid, and the next latest version is <
      // snapshot_tid, then it is also ok because the latest version will be ordered
      // after this txn, and we know its read set does not depend on our write set
      // (since it has a txn id higher than we do)
      if (likely(did_write ?
            ln->is_snapshot_consistent(snapshot_tid, commit_tid) :
            ln->stable_is_snapshot_consistent(snapshot_tid, commit_tid)))
        continue;
      goto do_abort;
    }

    for (vector<key_range_t>::iterator it = absent_range_set.begin();
         it != absent_range_set.end(); ++it) {
      VERBOSE(cout << "checking absent range: " << *it << endl);
      txn_btree::absent_range_validation_callback c(this, commit_tid);
      varkey upper(it->b);
      btree->underlying_btree.search_range_call(varkey(it->a), it->has_b ? &upper : NULL, c);
      if (unlikely(c.failed()))
        goto do_abort;
    }

    // commit actual records
    for (map<logical_node *, record_t>::iterator it = logical_nodes.begin();
         it != logical_nodes.end(); ++it) {
      VERBOSE(cout << "writing logical_node 0x" << hexify(it->first)
                   << " at commit_tid " << commit_tid << endl);
      it->first->write_record_at(commit_tid, it->second);
      it->first->unlock();
    }
  }

  resolved = true;
  clear();
  return;

do_abort:
  VERBOSE(cout << "aborting txn @ snapshot_tid " << snapshot_tid << endl);
  for (map<logical_node *, record_t>::iterator it = logical_nodes.begin();
       it != logical_nodes.end(); ++it)
    it->first->unlock();
  resolved = true;
  clear();
  throw transaction_abort_exception();
}

void
transaction::clear()
{
  btree = NULL;
  read_set.clear();
  write_set.clear();
  absent_range_set.clear();
}

void
transaction::abort()
{
  INVARIANT(!resolved);
  resolved = true;
  clear();
}

transaction::tid_t
transaction::current_global_tid()
{
  return global_tid;
}

transaction::tid_t
transaction::incr_and_get_global_tid()
{
  return __sync_add_and_fetch(&global_tid, 1);
}

bool
transaction::key_in_absent_set(const key_type &k) const
{
  vector<key_range_t>::const_iterator it =
    upper_bound(absent_range_set.begin(), absent_range_set.end(), k,
                key_range_search_less_cmp());
  if (it == absent_range_set.end())
    return false;
  return it->key_in_range(k);
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

void
transaction::add_absent_range(const key_range_t &range)
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
  transaction t;
  t.resolved = true;

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
transaction::local_search_str(const string &k, record_t &v) const
{
  {
    map<string, transaction::record_t>::const_iterator it =
      write_set.find(k);
    if (it != write_set.end()) {
      v = it->second;
      return true;
    }
  }

  {
    map<string, transaction::read_record_t>::const_iterator it =
      read_set.find(k);
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

volatile transaction::tid_t transaction::global_tid = MIN_TID;
