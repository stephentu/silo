#include "tuple.h"
#include "txn.h"

using namespace std;
using namespace util;

event_counter dbtuple::g_evt_dbtuple_creates("dbtuple_creates");
event_counter dbtuple::g_evt_dbtuple_logical_deletes("dbtuple_logical_deletes");
event_counter dbtuple::g_evt_dbtuple_physical_deletes("dbtuple_physical_deletes");
event_counter dbtuple::g_evt_dbtuple_bytes_allocated("dbtuple_bytes_allocated");
event_counter dbtuple::g_evt_dbtuple_bytes_freed("dbtuple_bytes_freed");
event_counter dbtuple::g_evt_dbtuple_spills("dbtuple_spills");

event_avg_counter dbtuple::g_evt_avg_record_spill_len("avg_record_spill_len");

dbtuple::~dbtuple()
{
  INVARIANT(is_deleting());
  INVARIANT(!is_enqueued());
  INVARIANT(!is_locked());

  VERBOSE(cerr << "dbtuple: " << hexify(intptr_t(this)) << " is being deleted" << endl);

  // free reachable nodes:
  // don't do this recursively, to avoid overflowing
  // stack w/ really long chains
  struct dbtuple *cur = get_next();
  while (cur) {
    struct dbtuple *tmp = cur->get_next();
    INVARIANT(!cur->is_enqueued());
    cur->clear_next(); // so cur's dtor doesn't attempt to double free
    release_no_rcu(cur); // just a wrapper for ~dbtuple() + free()
    cur = tmp;
  }

  // stats-keeping
  ++g_evt_dbtuple_physical_deletes;
  g_evt_dbtuple_bytes_freed += (alloc_size + sizeof(dbtuple));
}

void
dbtuple::gc_chain()
{
  INVARIANT(rcu::in_rcu_region());
  INVARIANT(!is_latest());
  INVARIANT(!is_enqueued());
  release(this); // ~dbtuple() takes care of all reachable ptrs
}

dbtuple::write_record_ret
dbtuple::write_record_at(const transaction *txn, tid_t t, const_record_type r, size_type sz)
{
  INVARIANT(is_locked());
  INVARIANT(is_latest());

  const version_t v = unstable_version();

  if (!sz)
    ++g_evt_dbtuple_logical_deletes;

  // try to overwrite this record
  if (likely(txn->can_overwrite_record_tid(version, t))) {
    // see if we have enough space

    if (likely(sz <= alloc_size)) {
      // directly update in place
      version = t;
      size = sz;
      NDB_MEMCPY(get_value_start(v), r, sz);
      return write_record_ret(false, NULL);
    }

    // keep in the chain (it's wasteful, but not incorrect)
    // so that cleanup is easier
    dbtuple * const rep = alloc(t, r, sz, this, true);
    INVARIANT(rep->is_latest());
    set_latest(false);
    return write_record_ret(false, rep);
  }

  // need to spill
  ++g_evt_dbtuple_spills;
  g_evt_avg_record_spill_len.offer(size);

  char * const vstart = get_value_start(v);

  if (IsBigType(v) && sz <= alloc_size) {
    dbtuple * const spill = alloc(version, (const_record_type) vstart, size, d->big.next, false);
    INVARIANT(!spill->is_latest());
    set_next(spill);
    version = t;
    size = sz;
    NDB_MEMCPY(vstart, r, sz);
    return write_record_ret(true, NULL);
  }

  dbtuple * const rep = alloc(t, r, sz, this, true);
  INVARIANT(rep->is_latest());
  set_latest(false);
  return write_record_ret(true, rep);

}

string
dbtuple::VersionInfoStr(version_t v)
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
operator<<(ostream &o, const dbtuple &ln)
{
  vector<transaction::tid_t> tids;
  vector<transaction::size_type> recs;
  tids.push_back(ln.version);
  recs.push_back(ln.size);
  vector<string> tids_s = format_tid_list(tids);
  const bool has_spill = ln.get_next();
  o << "[v=" << dbtuple::VersionInfoStr(ln.unstable_version()) <<
    ", tids=" << format_list(tids_s.rbegin(), tids_s.rend()) <<
    ", sizes=" << format_list(recs.rbegin(), recs.rend()) <<
    ", has_spill=" <<  has_spill << "]";
  o << endl;
  const struct dbtuple *p = ln.get_next();
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
