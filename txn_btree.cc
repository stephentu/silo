#include "txn_btree.h"

using namespace std;

bool
txn_btree::search(transaction &t, key_type k, value_type &v)
{
  assert(!t.btree || t.btree == this);
  t.btree = this;

  // priority is
  // 1) write set
  // 2) local read set
  // 3) range set
  // 4) query underlying tree
  //
  // note (1)-(3) are served by transaction::local_search()

  if (t.local_search(k, v))
    return (bool) v;

  btree::value_type underlying_v;
  if (!underlying_btree.search(k, underlying_v)) {
    // all records exist in the system at MIN_TID with no value
    transaction::read_record_t *read_rec = &t.read_set[k];
    read_rec->t = transaction::MIN_TID;
    read_rec->r = NULL;
    read_rec->ln = NULL;
    return false;
  } else {
    transaction::logical_node *ln = (transaction::logical_node *) underlying_v;
    assert(ln);
    transaction::tid_t start_t;
    transaction::record_t r;
    if (unlikely(!ln->stable_read(t.snapshot_tid, start_t, r)))
      throw transaction_abort_exception();
    transaction::read_record_t *read_rec = &t.read_set[k];
    read_rec->t = start_t;
    read_rec->r = r;
    read_rec->ln = ln;
    v = read_rec->r;
    return read_rec->r;
  }
}

bool
txn_btree::txn_search_range_callback::invoke(key_type k, value_type v)
{
  transaction::key_range_t r(invoked ? prev_key : lower);
  if (!r.is_empty_range())
    t->add_absent_range(r);
  prev_key = k;
  invoked = true;
  value_type local_v = 0;
  bool local_read = t->local_search(k, local_v);
  bool ret = true;
  if (local_read && local_v)
    ret = caller_callback->invoke(k, local_v);
  map<key_type, transaction::read_record_t>::const_iterator it =
    t->read_set.find(k);
  if (it == t->read_set.end()) {
    transaction::logical_node *ln = (transaction::logical_node *) v;
    assert(ln);
    transaction::tid_t start_t;
    transaction::record_t r;
    if (unlikely(!ln->stable_read(t->snapshot_tid, start_t, r)))
      throw transaction_abort_exception();
    transaction::read_record_t *read_rec = &t->read_set[k];
    read_rec->t = start_t;
    read_rec->r = r;
    read_rec->ln = ln;
    if (!local_read && r)
      ret = caller_callback->invoke(k, r);
  }
  if (ret)
    caller_stopped = true;
  return ret;
}

void
txn_btree::search_range_call(transaction &t,
                             key_type lower,
                             key_type *upper,
                             search_range_callback &callback)
{
  assert(!t.btree || t.btree == this);
  t.btree = this;

  // many cases to consider:
  // 1) for each logical_node returned from the scan, we need to
  //    record it in our local read set. there are several cases:
  //    A) if the logical_node corresponds to a key we have written, then
  //       we emit the version from the local write set
  //    B) if the logical_node corresponds to a key we have previous read,
  //       then we emit the previous version
  // 2) for each logical_node node *not* returned from the scan, we need
  //    to record its absense. we optimize this by recording the absense
  //    of contiguous ranges
  if (upper && *upper <= lower)
    return;
  txn_search_range_callback c(&t, lower, upper, &callback);
  underlying_btree.search_range_call(lower, upper, c);
  if (c.caller_stopped)
    return;
  if (c.invoked && c.prev_key == (key_type)-1)
    return;
  if (upper)
    t.add_absent_range(transaction::key_range_t(c.invoked ? (c.prev_key + 1): lower, *upper));
  else
    t.add_absent_range(transaction::key_range_t(c.invoked ? c.prev_key : lower));
}

void
txn_btree::insert_impl(transaction &t, key_type k, value_type v)
{
  assert(!t.btree || t.btree == this);
  t.btree = this;
  t.write_set[k] = v;
}
