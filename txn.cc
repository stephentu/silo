#include "macros.h"
#include "amd64.h"
#include "txn.h"
#include "txn_proto2_impl.h"
#include "txn_btree.h"
#include "lockguard.h"
#include "scopedperf.hh"

#include <algorithm>
#include <iostream>
#include <sstream>
#include <vector>
#include <utility>

using namespace std;
using namespace util;

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
  b << "[core=" << transaction_proto2_static::CoreId(v) << " | n="
    << transaction_proto2_static::NumId(v) << " | epoch="
    << transaction_proto2_static::EpochId(v) << "]";
  return b.str();
}

// XXX(stephentu): hacky!
string (*g_proto_version_str)(uint64_t v) = proto2_version_str;

CLASS_STATIC_COUNTER_IMPL(transaction_base, scopedperf::tsc_ctr, g_txn_commit_probe0, g_txn_commit_probe0_cg);
CLASS_STATIC_COUNTER_IMPL(transaction_base, scopedperf::tsc_ctr, g_txn_commit_probe1, g_txn_commit_probe1_cg);
CLASS_STATIC_COUNTER_IMPL(transaction_base, scopedperf::tsc_ctr, g_txn_commit_probe2, g_txn_commit_probe2_cg);
CLASS_STATIC_COUNTER_IMPL(transaction_base, scopedperf::tsc_ctr, g_txn_commit_probe3, g_txn_commit_probe3_cg);
CLASS_STATIC_COUNTER_IMPL(transaction_base, scopedperf::tsc_ctr, g_txn_commit_probe4, g_txn_commit_probe4_cg);
CLASS_STATIC_COUNTER_IMPL(transaction_base, scopedperf::tsc_ctr, g_txn_commit_probe5, g_txn_commit_probe5_cg);
CLASS_STATIC_COUNTER_IMPL(transaction_base, scopedperf::tsc_ctr, g_txn_commit_probe6, g_txn_commit_probe6_cg);

#define EVENT_COUNTER_IMPL_X(x) \
  event_counter transaction_base::g_ ## x ## _ctr(#x);
ABORT_REASONS(EVENT_COUNTER_IMPL_X)
#undef EVENT_COUNTER_IMPL_X

event_counter transaction_base::g_evt_read_logical_deleted_node_search
    ("read_logical_deleted_node_search");
event_counter transaction_base::g_evt_read_logical_deleted_node_scan
    ("read_logical_deleted_node_scan");
event_counter transaction_base::g_evt_dbtuple_write_search_failed
    ("dbtuple_write_search_failed");
event_counter transaction_base::g_evt_dbtuple_write_insert_failed
    ("dbtuple_write_insert_failed");

event_counter transaction_base::evt_local_search_lookups("local_search_lookups");
event_counter transaction_base::evt_local_search_write_set_hits("local_search_write_set_hits");
event_counter transaction_base::evt_dbtuple_latest_replacement("dbtuple_latest_replacement");
