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

transaction::transaction(uint64_t flags)
  : state(TXN_ACTIVE), flags(flags)
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
  pair<bool, tid_t> snapshot_tid_t; // declared here so we don't cross initialization

  for (map<txn_btree *, txn_context>::iterator outer_it = ctx_map.begin();
       outer_it != ctx_map.end(); ++outer_it) {
    INVARIANT(!(get_flags() & TXN_FLAG_READ_ONLY) || outer_it->second.write_set.empty());
    for (write_set_map::iterator it = outer_it->second.write_set.begin();
        it != outer_it->second.write_set.end(); ++it) {
    retry:
      btree::value_type v = 0;
      if (outer_it->first->underlying_btree.search(varkey(it->first), v)) {
        VERBOSE(cerr << "key " << hexify(it->first) << " : logical_node 0x" << hexify(intptr_t(v)) << endl);
        logical_nodes[(logical_node *) v] = it->second;
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
        logical_nodes[ln] = it->second;
      }
    }
  }

  snapshot_tid_t = consistent_snapshot_tid();
  if (!snapshot_tid_t.first || !logical_nodes.empty()) {
    // we don't have consistent tids, or not a read-only txn

    // lock the logical nodes in sort order
    for (map<logical_node *, record_t>::iterator it = logical_nodes.begin();
         it != logical_nodes.end(); ++it) {
      VERBOSE(cerr << "locking node 0x" << hexify(intptr_t(it->first)) << endl);
      it->first->lock();
    }

    // acquire commit tid (if not read-only txn)
    tid_t commit_tid = logical_nodes.empty() ? 0 : gen_commit_tid(logical_nodes);

    if (logical_nodes.empty())
      VERBOSE(cerr << "commit tid: <read-only>" << endl);
    else
      VERBOSE(cerr << "commit tid: " << commit_tid << endl);

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
          txn_btree::absent_range_validation_callback c(&outer_it->second, commit_tid);
          varkey upper(it->b);
          outer_it->first->underlying_btree.search_range_call(varkey(it->a), it->has_b ? &upper : NULL, c);
          if (unlikely(c.failed()))
            goto do_abort;
        }
      }
    }

    // commit actual records
    vector<record_t> removed_vec;
    for (map<logical_node *, record_t>::iterator it = logical_nodes.begin();
         it != logical_nodes.end(); ++it) {
      VERBOSE(cerr << "writing logical_node 0x" << hexify(intptr_t(it->first))
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
  // XXX: these values are possibly un-initialized
  if (snapshot_tid_t.first)
    VERBOSE(cerr << "aborting txn @ snapshot_tid " << snapshot_tid_t.second << endl);
  else
    VERBOSE(cerr << "aborting txn" << endl);
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

transaction_proto1::transaction_proto1(uint64_t flags)
  : transaction(flags), snapshot_tid(current_global_tid())
{
}

pair<bool, transaction::tid_t>
transaction_proto1::consistent_snapshot_tid() const
{
  return make_pair(true, snapshot_tid);
}

transaction::tid_t
transaction_proto1::gen_commit_tid(const map<logical_node *, record_t> &write_nodes)
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

transaction_proto2::transaction_proto2(uint64_t flags)
  : transaction(flags), current_epoch(0)
{
  size_t my_core_id = core_id();
  VERBOSE(cerr << "new transaction_proto2 (core=" << my_core_id
               << ", nest=" << tl_nest_level << ")" << endl);
  if (tl_nest_level++ == 0)
    ALWAYS_ASSERT(pthread_spin_lock(&g_epoch_spinlocks[my_core_id].elem) == 0);
  current_epoch = g_current_epoch;
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
    return make_pair(true, g_last_consistent_tid);
  else
    return make_pair(false, 0);
}

transaction::tid_t
transaction_proto2::gen_commit_tid(const map<logical_node *, record_t> &write_nodes)
{
  size_t my_core_id = core_id();
  tid_t l_last_commit_tid = g_last_commit_tids[my_core_id].elem;
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
  for (map<logical_node *, record_t>::const_iterator it = write_nodes.begin();
       it != write_nodes.end(); ++it) {
    INVARIANT(it->first->is_locked());
    tid_t t = it->first->latest_version();
    // NB: see above
    INVARIANT(EpochId(it->second.t) <= current_epoch);
    if (t > ret)
      ret = t;
  }
  ret = MakeTid(my_core_id, NumId(ret) + 1, current_epoch);

  // XXX(stephentu): document why we need this memory fence
  __sync_synchronize();

  return (g_last_commit_tids[my_core_id].elem = ret);
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
  // NB(stephentu): we don't use ndb_thread here, because the EpochLoop
  // does not participate in any transactions.
  for (size_t i = 0; i < NMaxCores; i++) {
    g_last_commit_tids[i].elem = MIN_TID;
    ALWAYS_ASSERT(pthread_spin_init(&g_epoch_spinlocks[i].elem, PTHREAD_PROCESS_PRIVATE) == 0);
  }
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

    // consistent TID = max_{all cores}(last tid in epoch of core)
    uint64_t ret = 0;
    size_t l_core_count = g_core_count;
    for (size_t i = 0; i < l_core_count; i++) {
      scoped_spinlock l(&g_epoch_spinlocks[i].elem);
      if (g_last_commit_tids[i].elem > ret)
        ret = g_last_commit_tids[i].elem;
    }

    g_last_consistent_tid = ret;
  }
  return 0;
}

__thread ssize_t transaction_proto2::tl_core_id = -1;
__thread unsigned int transaction_proto2::tl_nest_level = 0;

// NB(stephentu): we start the current epoch at t=1, to guarantee that
// zero modifications are made in epoch at t=0 (which is the epoch the DB
// started in). This allows us to *immediately* take a consistent snapshot
// of the DB (it will be of an empty DB).
volatile uint64_t transaction_proto2::g_current_epoch = 1;
volatile transaction::tid_t transaction_proto2::g_core_count = 0;
volatile aligned_padded_u64 transaction_proto2::g_last_commit_tids[NMaxCores];
volatile uint64_t transaction_proto2::g_last_consistent_tid = 0;
volatile aligned_padded_elem<pthread_spinlock_t> transaction_proto2::g_epoch_spinlocks[NMaxCores];
