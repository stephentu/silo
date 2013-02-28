#include "counter.h"
#include "util.h"

using namespace std;
using namespace util;
using namespace private_;

vector<event_ctx *> &
event_ctx::event_counters()
{
  static vector<event_ctx *> s_counters;
  return s_counters;
}

pthread_spinlock_t &
event_ctx::event_counters_lock()
{
  static pthread_spinlock_t *volatile l = NULL;
  if (!l) {
    pthread_spinlock_t *sl = new pthread_spinlock_t;
    ALWAYS_ASSERT(pthread_spin_init(sl, PTHREAD_PROCESS_PRIVATE) == 0);
    if (!__sync_bool_compare_and_swap(&l, NULL, sl)) {
      ALWAYS_ASSERT(pthread_spin_destroy(sl) == 0);
      delete sl;
    }
  }
  INVARIANT(l);
  return *l;
}

map<string, double>
event_counter::get_all_counters()
{
  map<string, double> ret;
  const vector<event_ctx *> &evts = event_ctx::event_counters();
  pthread_spinlock_t &l = event_ctx::event_counters_lock();
  scoped_spinlock sl(&l);
  for (vector<event_ctx *>::const_iterator it = evts.begin();
       it != evts.end(); ++it) {
    uint64_t c = 0;
    for (size_t i = 0; i < coreid::NMaxCores; i++)
      c += (*it)->tl_counts[i].elem;
    double v = 0.0;
    if ((*it)->avg_tag) {
      uint64_t s = 0;
      for (size_t i = 0; i < coreid::NMaxCores; i++)
        s += static_cast<event_ctx_avg *>(*it)->tl_invokes[i].elem;
      v = double(c)/double(s);
    } else {
      v = double(c);
    }
    ret[(*it)->name] += v;
  }
  return ret;
}

void
event_counter::reset_all_counters()
{
  const vector<event_ctx *> &evts = event_ctx::event_counters();
  pthread_spinlock_t &l = event_ctx::event_counters_lock();
  scoped_spinlock sl(&l);
  for (vector<event_ctx *>::const_iterator it = evts.begin();
       it != evts.end(); ++it)
    for (size_t i = 0; i < coreid::NMaxCores; i++) {
      (*it)->tl_counts[i].elem = 0;
      if ((*it)->avg_tag)
        static_cast<event_ctx_avg *>(*it)->tl_invokes[i].elem = 0;
    }
}

event_counter::event_counter(const string &name)
  : ctx(new event_ctx(name, false))
{
  pthread_spinlock_t &l = event_ctx::event_counters_lock();
  vector<event_ctx *> &evts = event_ctx::event_counters();
  scoped_spinlock sl(&l);
  evts.push_back(ctx);
}

event_avg_counter::event_avg_counter(const string &name)
  : ctx(new event_ctx_avg(name))
{
  pthread_spinlock_t &l = event_ctx::event_counters_lock();
  vector<event_ctx *> &evts = event_ctx::event_counters();
  scoped_spinlock sl(&l);
  evts.push_back(ctx);
}
