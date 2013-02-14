#include "counter.h"
#include "util.h"

using namespace std;
using namespace util;

vector<event_counter::event_ctx *> &
event_counter::event_counters()
{
  static vector<event_ctx *> s_counters;
  return s_counters;
}

pthread_spinlock_t &
event_counter::event_counters_lock()
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

map<string, uint64_t>
event_counter::get_all_counters()
{
  map<string, uint64_t> ret;
  const vector<event_ctx *> &evts = event_counters();
  pthread_spinlock_t &l = event_counters_lock();
  scoped_spinlock sl(&l);
  for (vector<event_ctx *>::const_iterator it = evts.begin();
       it != evts.end(); ++it) {
    uint64_t c = 0;
    for (size_t i = 0; i < coreid::NMaxCores; i++)
      c += (*it)->tl_counts[i].elem;
    ret[(*it)->name] += c;
  }
  return ret;
}

event_counter::event_counter(const string &name)
  : ctx(new event_ctx(name))
{
  pthread_spinlock_t &l = event_counters_lock();
  vector<event_ctx *> &evts = event_counters();
  scoped_spinlock sl(&l);
  evts.push_back(ctx);
}
