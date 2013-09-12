#include "counter.h"
#include "util.h"
#include "lockguard.h"

using namespace std;
using namespace util;
using namespace private_;

map<string, event_ctx *> &
event_ctx::event_counters()
{
  static map<string, event_ctx *> s_counters;
  return s_counters;
}

spinlock &
event_ctx::event_counters_lock()
{
  static spinlock s_lock;
  return s_lock;
}

void
event_ctx::stat(counter_data &d)
{
  for (size_t i = 0; i < coreid::NMaxCores; i++)
    d.count_ += counts_[i];
  if (avg_tag_) {
    d.type_ = counter_data::TYPE_AGG;
    uint64_t m = 0;
    for (size_t i = 0; i < coreid::NMaxCores; i++) {
      m = max(m, static_cast<event_ctx_avg *>(this)->highs_[i]);
    }
    uint64_t s = 0;
    for (size_t i = 0; i < coreid::NMaxCores; i++)
      s += static_cast<event_ctx_avg *>(this)->sums_[i];
    d.sum_ = s;
    d.max_ = m;
  }
}

map<string, counter_data>
event_counter::get_all_counters()
{
  map<string, counter_data> ret;
  const map<string, event_ctx *> &evts = event_ctx::event_counters();
  spinlock &l = event_ctx::event_counters_lock();
  lock_guard<spinlock> sl(l);
  for (auto &p : evts) {
    counter_data d;
    p.second->stat(d);
    if (d.type_ == counter_data::TYPE_AGG)
      ret[p.first].type_ = counter_data::TYPE_AGG;
    ret[p.first] += d;
  }
  return ret;
}

void
event_counter::reset_all_counters()
{
  const map<string, event_ctx *> &evts = event_ctx::event_counters();
  spinlock &l = event_ctx::event_counters_lock();
  lock_guard<spinlock> sl(l);
  for (auto &p : evts)
    for (size_t i = 0; i < coreid::NMaxCores; i++) {
      p.second->counts_[i] = 0;
      if (p.second->avg_tag_) {
        static_cast<event_ctx_avg *>(p.second)->sums_[i] = 0;
        static_cast<event_ctx_avg *>(p.second)->highs_[i] = 0;
      }
    }
}

bool
event_counter::stat(const string &name, counter_data &d)
{
  const map<string, event_ctx *> &evts = event_ctx::event_counters();
  spinlock &l = event_ctx::event_counters_lock();
  event_ctx *ctx = nullptr;
  {
    lock_guard<spinlock> sl(l);
    auto it = evts.find(name);
    if (it != evts.end())
      ctx = it->second;
  }
  if (!ctx)
    return false;
  ctx->stat(d);
  return true;
}

#ifdef ENABLE_EVENT_COUNTERS
event_counter::event_counter(const string &name)
  : ctx_(name, false)
{
  spinlock &l = event_ctx::event_counters_lock();
  map<string, event_ctx *> &evts = event_ctx::event_counters();
  lock_guard<spinlock> sl(l);
  evts[name] = ctx_.obj();
}

event_avg_counter::event_avg_counter(const string &name)
  : ctx_(name)
{
  spinlock &l = event_ctx::event_counters_lock();
  map<string, event_ctx *> &evts = event_ctx::event_counters();
  lock_guard<spinlock> sl(l);
  evts[name] = ctx_.obj();
}
#else
event_counter::event_counter(const string &name)
{
}

event_avg_counter::event_avg_counter(const string &name)
{
}
#endif
