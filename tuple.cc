#include "tuple.h"
#include "txn.h"

using namespace std;
using namespace util;

event_avg_counter dbtuple::g_evt_avg_dbtuple_stable_version_spins
  ("avg_dbtuple_stable_version_spins");
event_avg_counter dbtuple::g_evt_avg_dbtuple_lock_acquire_spins
  ("avg_dbtuple_lock_acquire_spins");
event_avg_counter dbtuple::g_evt_avg_dbtuple_read_retries
  ("avg_dbtuple_read_retries");

event_counter dbtuple::g_evt_dbtuple_creates("dbtuple_creates");
event_counter dbtuple::g_evt_dbtuple_logical_deletes("dbtuple_logical_deletes");
event_counter dbtuple::g_evt_dbtuple_physical_deletes("dbtuple_physical_deletes");
event_counter dbtuple::g_evt_dbtuple_bytes_allocated("dbtuple_bytes_allocated");
event_counter dbtuple::g_evt_dbtuple_bytes_freed("dbtuple_bytes_freed");
event_counter dbtuple::g_evt_dbtuple_spills("dbtuple_spills");
event_counter dbtuple::g_evt_dbtuple_inplace_buf_insufficient("dbtuple_inplace_buf_insufficient");
event_counter dbtuple::g_evt_dbtuple_inplace_buf_insufficient_on_spill("dbtuple_inplace_buf_insufficient_on_spill");

event_avg_counter dbtuple::g_evt_avg_record_spill_len("avg_record_spill_len");

dbtuple::~dbtuple()
{
  INVARIANT(!is_locked());
  INVARIANT(is_deleting());
  INVARIANT(!is_write_intent());
  INVARIANT(!is_modifying());

  VERBOSE(cerr << "dbtuple: " << hexify(intptr_t(this)) << " is being deleted" << endl);

  // free reachable nodes:
  // don't do this recursively, to avoid overflowing
  // stack w/ really long chains
  struct dbtuple *cur = get_next();
  while (cur) {
    struct dbtuple *tmp = cur->get_next();
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
  release(this); // ~dbtuple() takes care of all reachable ptrs
}

string
dbtuple::VersionInfoStr(version_t v)
{
  ostringstream buf;
  buf << "[";
  buf << (IsLocked(v) ? "LOCKED" : "-") << " | ";
  buf << (IsDeleting(v) ? "DEL" : "-") << " | ";
  buf << (IsWriteIntent(v) ? "WR" : "-") << " | ";
  buf << (IsModifying(v) ? "MOD" : "-") << " | ";
  buf << (IsLatest(v) ? "LATEST" : "-") << " | ";
  buf << Version(v);
  buf << "]";
  return buf.str();
}

static vector<string>
format_tid_list(const vector<transaction_base::tid_t> &tids)
{
  vector<string> s;
  for (vector<transaction_base::tid_t>::const_iterator it = tids.begin();
       it != tids.end(); ++it)
    s.push_back(g_proto_version_str(*it));
  return s;
}

ostream &
operator<<(ostream &o, const dbtuple &ln)
{
  vector<transaction_base::tid_t> tids;
  vector<transaction_base::size_type> recs;
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
    vector<transaction_base::tid_t> itids;
    vector<transaction_base::size_type> irecs;
    itids.push_back(p->version);
    irecs.push_back(p->size);
    vector<string> itids_s = format_tid_list(itids);
    o << "[tids=" << format_list(itids_s.rbegin(), itids_s.rend())
      << ", sizes=" << format_list(irecs.rbegin(), irecs.rend())
      << "]" << endl;
  }
  return o;
}
