#include "counter.h"
#include "util.h"
#include "lockguard.h"

using namespace std;
using namespace util;
using namespace private_;

vector<event_ctx *> &
event_ctx::event_counters()
{
  static vector<event_ctx *> s_counters;
  return s_counters;
}

spinlock &
event_ctx::event_counters_lock()
{
  static spinlock s_lock;
  return s_lock;
}

map<string, event_counter::counter_data>
event_counter::get_all_counters()
{
  map<string, counter_data> ret;
  const vector<event_ctx *> &evts = event_ctx::event_counters();
  spinlock &l = event_ctx::event_counters_lock();
  lock_guard<spinlock> sl(l);
  for (vector<event_ctx *>::const_iterator it = evts.begin();
       it != evts.end(); ++it) {
    counter_data d;
    for (size_t i = 0; i < coreid::NMaxCores; i++)
      d.count += (*it)->tl_counts[i].elem;
    if ((*it)->avg_tag) {
      d.type = event_counter::counter_data::TYPE_AGG;
      uint64_t m = 0;
      for (size_t i = 0; i < coreid::NMaxCores; i++) {
        m = max(m, static_cast<uint64_t>(static_cast<event_ctx_avg *>(*it)->tl_highs[i].elem));
      }
      uint64_t s = 0;
      for (size_t i = 0; i < coreid::NMaxCores; i++)
        s += static_cast<event_ctx_avg *>(*it)->tl_sums[i].elem;
      d.sum = s;
      d.max = m;
      ret[(*it)->name].type = event_counter::counter_data::TYPE_AGG;
    }
    ret[(*it)->name] += d;
  }
  return ret;
}

void
event_counter::reset_all_counters()
{
  const vector<event_ctx *> &evts = event_ctx::event_counters();
  spinlock &l = event_ctx::event_counters_lock();
  lock_guard<spinlock> sl(l);
  for (vector<event_ctx *>::const_iterator it = evts.begin();
       it != evts.end(); ++it)
    for (size_t i = 0; i < coreid::NMaxCores; i++) {
      (*it)->tl_counts[i].elem = 0;
      if ((*it)->avg_tag) {
        static_cast<event_ctx_avg *>(*it)->tl_sums[i].elem = 0;
        static_cast<event_ctx_avg *>(*it)->tl_highs[i].elem = 0;
      }
    }
}

#ifdef ENABLE_EVENT_COUNTERS
event_counter::event_counter(const string &name)
  : ctx(new event_ctx(name, false))
{
  spinlock &l = event_ctx::event_counters_lock();
  vector<event_ctx *> &evts = event_ctx::event_counters();
  lock_guard<spinlock> sl(l);
  evts.push_back(ctx);
}

event_avg_counter::event_avg_counter(const string &name)
  : ctx(new event_ctx_avg(name))
{
  spinlock &l = event_ctx::event_counters_lock();
  vector<event_ctx *> &evts = event_ctx::event_counters();
  lock_guard<spinlock> sl(l);
  evts.push_back(ctx);
}
#else
event_counter::event_counter(const string &name)
{
}

event_avg_counter::event_avg_counter(const string &name)
{
}
#endif
