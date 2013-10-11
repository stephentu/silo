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
static event_avg_counter evt_avg_dbtuple_chain_length("avg_dbtuple_chain_len");

dbtuple::~dbtuple()
{
  CheckMagic();
  INVARIANT(!is_locked());
  INVARIANT(!is_latest());
  INVARIANT(!is_write_intent());
  INVARIANT(!is_modifying());

  VERBOSE(cerr << "dbtuple: " << hexify(intptr_t(this)) << " is being deleted" << endl);

  // only free this instance

  // stats-keeping
  ++g_evt_dbtuple_physical_deletes;
  g_evt_dbtuple_bytes_freed += (alloc_size + sizeof(dbtuple));

}

void
dbtuple::gc_this()
{
  INVARIANT(rcu::s_instance.in_rcu_region());
  INVARIANT(!is_latest());
  release(this);
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

static ostream &
format_tuple(ostream &o, const dbtuple &t)
{
  string truncated_contents(
      (const char *) &t.value_start[0], min(static_cast<size_t>(t.size), 16UL));
  o << &t << " [tid=" << g_proto_version_str(t.version)
    << ", size=" << t.size
    << ", contents=0x" << hexify(truncated_contents) << (t.size > 16 ? "..." : "")
    << ", next=" << t.next << "]";
  return o;
}

void
dbtuple::print(ostream &o, unsigned len) const
{
  o << "dbtuple:" << endl
    << "  hdr=" << VersionInfoStr(unstable_version())
#ifdef TUPLE_CHECK_KEY
    << endl << "  key=" << hexify(key)
    << endl << "  tree=" << tree
#endif
    << endl;

  size_t n = 0;
  for (const dbtuple *p = this;
       p && n < len;
       p = p->get_next(), ++n) {
    o << "  ";
    format_tuple(o, *p);
    o << endl;
  }
}

ostream &
operator<<(ostream &o, const dbtuple &t)
{
  t.print(o, 1);
  return o;
}
