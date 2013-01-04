#include "macros.h"
#include "txn.h"
#include "txn_btree.h"

#include <algorithm>
#include <iostream>
#include <sstream>
#include <vector>
#include <utility>

using namespace std;

transaction::transaction()
  : snapshot_tid(current_global_tid()),
    resolved(false),
    btree(NULL)
{
}

transaction::~transaction()
{
  // transaction shouldn't fall out of scope w/o resolution
  assert(resolved);
}

void
transaction::commit()
{
  assert(!resolved);

  // fetch logical_nodes for insert
  map<logical_node *, record_t> logical_nodes;
  for (map<key_type, record_t>::iterator it = write_set.begin();
       it != write_set.end(); ++it) {
  retry:
    btree::value_type v = 0;
    if (btree->underlying_btree.search(it->first, v)) {
      logical_nodes[(logical_node *) v] = it->second;
    } else {
      logical_node *ln = logical_node::alloc();
      if (!btree->underlying_btree.insert_if_absent(it->first, (btree::value_type) ln)) {
        logical_node::release(ln);
        goto retry;
      }
      logical_nodes[ln] = it->second;
    }
  }

  // lock the logical nodes in sort order
  for (map<logical_node *, record_t>::iterator it = logical_nodes.begin();
       it != logical_nodes.end(); ++it)
    it->first->lock();

  // acquire commit tid
  tid_t commit_tid = get_and_incr_global_tid();

  // do read validation
  for (map<key_type, read_record_t>::iterator it = read_set.begin();
       it != read_set.end(); ++it) {
    bool did_write = write_set.find(it->first) != write_set.end();
    transaction::logical_node *ln = NULL;
    if (it->second.ln) {
      ln = it->second.ln;
    } else {
      btree::value_type v = 0;
      if (btree->underlying_btree.search(it->first, v))
        ln = (transaction::logical_node *) v;
    }
    if (!ln)
      continue;
    if ((did_write ? ln->is_latest_version(snapshot_tid) : ln->stable_is_latest_version(snapshot_tid)))
      continue;
    goto do_abort;
  }

  // commit actual records
  for (map<logical_node *, record_t>::iterator it = logical_nodes.begin();
       it != logical_nodes.end(); ++it) {
    it->first->write_record_at(commit_tid, it->second);
    it->first->unlock();
  }

  resolved = true;
  clear();
  return;

do_abort:
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
  assert(!resolved);
  resolved = true;
  clear();
}

transaction::tid_t
transaction::current_global_tid()
{
  return global_tid;
}

transaction::tid_t
transaction::get_and_incr_global_tid()
{
  return __sync_fetch_and_add(&global_tid, 1);
}

bool
transaction::key_in_absent_set(key_type k) const
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
  o << "[" << range.a << ", ";
  if (range.has_b)
    o << range.b;
  else
    o << "infty";
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
    upper_bound(absent_range_set.begin(), absent_range_set.end(), range.a,
                key_range_search_less_cmp());

  if (it == absent_range_set.end()) {
    if (!absent_range_set.empty() && absent_range_set.back().b == range.a) {
      assert(absent_range_set.back().has_b);
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
  key_type left_key = merge_left ? (it - 1)->a : min(it->a, range.a);

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

void
transaction::AssertValidRangeSet(const vector<key_range_t> &range_set)
{
  if (range_set.empty())
    return;
  key_range_t last = range_set.front();
  assert(!last.is_empty_range());
  for (vector<key_range_t>::const_iterator it = range_set.begin() + 1;
       it != range_set.end(); ++it) {
    assert(!it->is_empty_range());
    assert(last.has_b);
    assert(last.b < it->a);
  }
}

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

  t.add_absent_range(key_range_t(10, 20));
  cout << PrintRangeSet(t.absent_range_set) << endl;
  t.add_absent_range(key_range_t(20, 30));
  cout << PrintRangeSet(t.absent_range_set) << endl;
  t.add_absent_range(key_range_t(50, 60));
  cout << PrintRangeSet(t.absent_range_set) << endl;
  t.add_absent_range(key_range_t(31, 40));
  cout << PrintRangeSet(t.absent_range_set) << endl;
  t.add_absent_range(key_range_t(49, 50));
  cout << PrintRangeSet(t.absent_range_set) << endl;
  t.add_absent_range(key_range_t(47, 50));
  cout << PrintRangeSet(t.absent_range_set) << endl;
  t.add_absent_range(key_range_t(39, 50));
  cout << PrintRangeSet(t.absent_range_set) << endl;
  t.add_absent_range(key_range_t(100, 200));
  cout << PrintRangeSet(t.absent_range_set) << endl;
  t.add_absent_range(key_range_t(300, 400));
  cout << PrintRangeSet(t.absent_range_set) << endl;
  t.add_absent_range(key_range_t(50, 212));
  cout << PrintRangeSet(t.absent_range_set) << endl;

}

bool
transaction::local_search(key_type k, record_t &v) const
{
  {
    map<key_type, transaction::record_t>::const_iterator it =
      write_set.find(k);
    if (it != write_set.end()) {
      v = it->second;
      return true;
    }
  }

  {
    map<key_type, transaction::read_record_t>::const_iterator it =
      read_set.find(k);
    if (it != read_set.end()) {
      v = it->second.r;
      return true;
    }
  }

  if (key_in_absent_set(k)) {
    v = NULL;
    return true;
  }

  return false;
}

volatile transaction::tid_t transaction::global_tid = 1;
