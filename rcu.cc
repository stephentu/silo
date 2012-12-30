#include <unistd.h>

#include "rcu.h"
#include "macros.h"
#include "util.h"
#include "thread.h"

using namespace std;
using namespace util;

volatile rcu::epoch_t rcu::global_epoch = 0;
volatile bool rcu::gc_thread_started = false;
pthread_t rcu::gc_thread_p;
pthread_spinlock_t *rcu::rcu_mutex = rcu_mutex_init_and_get();
map<pthread_t, rcu::sync *> rcu::sync_map;
__thread rcu::sync *rcu::tl_sync = NULL;
__thread bool rcu::tl_in_crit_section = false;

rcu::sync::sync(epoch_t local_epoch)
  : local_epoch(local_epoch)
{
  ALWAYS_ASSERT(pthread_spin_init(&local_critical_mutex, PTHREAD_PROCESS_PRIVATE) == 0);
}

rcu::sync::~sync()
{
  ALWAYS_ASSERT(pthread_spin_destroy(&local_critical_mutex) == 0);
}

pthread_spinlock_t *
rcu::rcu_mutex_init_and_get()
{
  static bool called = false;
  static pthread_spinlock_t l;
  ALWAYS_ASSERT(!called);
  called = true;
  ALWAYS_ASSERT(pthread_spin_init(&l, PTHREAD_PROCESS_PRIVATE) == 0);
  return &l;
}

void
rcu::register_sync(pthread_t p, sync *s)
{
  scoped_spinlock l(rcu_mutex);
  map<pthread_t, sync *>::iterator it = sync_map.find(p);
  ALWAYS_ASSERT(it == sync_map.end());
  sync_map[p] = s;
}

void
rcu::unregister_sync(pthread_t p)
{
  scoped_spinlock l(rcu_mutex);
  map<pthread_t, sync *>::iterator it = sync_map.find(p);
  if (it == sync_map.end())
    return;
  sync_map.erase(it);
}

void
rcu::enable()
{
  if (gc_thread_started)
    return;
  {
    scoped_spinlock l(rcu_mutex);
    if (gc_thread_started)
      return;
    gc_thread_started = true;
  }
  // start gc thread as daemon thread
  pthread_attr_t attr;
  ALWAYS_ASSERT(pthread_attr_init(&attr) == 0);
  ALWAYS_ASSERT(pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED) == 0);
  ALWAYS_ASSERT(pthread_create(&gc_thread_p, &attr, gc_thread_loop, NULL) == 0);
  ALWAYS_ASSERT(pthread_attr_destroy(&attr) == 0);
}

void
rcu::region_begin()
{
  if (!tl_sync) {
    enable();
    tl_sync = new sync(global_epoch);
    register_sync(pthread_self(), tl_sync);
  }
  assert(!tl_in_crit_section);
  assert(gc_thread_started);
  tl_sync->local_epoch = global_epoch;
  ALWAYS_ASSERT(pthread_spin_lock(&tl_sync->local_critical_mutex) == 0);
  tl_in_crit_section = true;
}

void
rcu::free(void *p, deleter_t fn)
{
  assert(tl_sync);
  assert(tl_in_crit_section);
  assert(gc_thread_started);
  tl_sync->local_queues[tl_sync->local_epoch % 2].push_back(delete_entry(p, fn));
}

void
rcu::region_end()
{
  assert(tl_sync);
  assert(tl_in_crit_section);
  assert(gc_thread_started);
  ALWAYS_ASSERT(pthread_spin_unlock(&tl_sync->local_critical_mutex) == 0);
  tl_in_crit_section = false;
}

void *
rcu::gc_thread_loop(void *p)
{
  // runs as daemon thread
  while (true) {
    // increment global epoch
    global_epoch += 1;
    COMPILER_MEMORY_FENCE;

    vector<delete_entry> elems;

    // now wait for each thread to finish any outstanding critical sections
    // from the previous epoch
    {
      scoped_spinlock l(rcu_mutex);
      for (map<pthread_t, sync *>::iterator it = sync_map.begin();
           it != sync_map.end(); ++it) {
        sync *s = it->second;
        epoch_t local_epoch = s->local_epoch;
        if (local_epoch == global_epoch)
          continue;
        assert(local_epoch == (global_epoch - 1));
        {
          scoped_spinlock l0(&s->local_critical_mutex);
        }
        // now the next time the thread enters a critical section, it
        // *must* get the new global_epoch, so we can now claim its
        // deleted pointers from global_epoch - 1
        elems.insert(elems.end(),
                     s->local_queues[local_epoch % 2].begin(),
                     s->local_queues[local_epoch % 2].end());
        s->local_queues[local_epoch % 2].clear();
      }
    }

    for (vector<delete_entry>::iterator it = elems.begin();
         it != elems.end(); ++it)
      it->second(it->first);

    // XXX: better solution for GC intervals?
    sleep(2);
  }
  return NULL;
}

static void rcu_completion_callback(ndb_thread *t)
{
  rcu::unregister_sync(t->pthread_id());
}
NDB_THREAD_REGISTER_COMPLETION_CALLBACK(rcu_completion_callback)
