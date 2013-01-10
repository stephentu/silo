#include <unistd.h>
#include <time.h>
#include <string.h>
#include <iostream>

#include "rcu.h"
#include "macros.h"
#include "util.h"
#include "thread.h"

using namespace std;
using namespace util;

volatile rcu::epoch_t rcu::global_epoch = 0;
vector<rcu::delete_entry> rcu::global_queues[2];

volatile bool rcu::gc_thread_started = false;
pthread_t rcu::gc_thread_p;

map<pthread_t, rcu::sync *> rcu::sync_map;

__thread rcu::sync *rcu::tl_sync = NULL;
__thread unsigned int rcu::tl_crit_section_depth = 0;

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
rcu::rcu_mutex()
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
  return l;
}

void
rcu::register_sync(pthread_t p, sync *s)
{
  scoped_spinlock l(rcu_mutex());
  map<pthread_t, sync *>::iterator it = sync_map.find(p);
  ALWAYS_ASSERT(it == sync_map.end());
  sync_map[p] = s;
}

rcu::sync *
rcu::unregister_sync(pthread_t p)
{
  scoped_spinlock l(rcu_mutex());
  map<pthread_t, sync *>::iterator it = sync_map.find(p);
  if (it == sync_map.end())
    return NULL;
  sync *s = it->second;
  epoch_t local_epoch = s->local_epoch;
  INVARIANT(local_epoch == global_epoch || local_epoch == (global_epoch - 1));
  if (local_epoch == global_epoch) {
    // move both local_queue entries
    global_queues[0].insert(
        global_queues[0].end(),
        s->local_queues[0].begin(),
        s->local_queues[0].end());
    global_queues[1].insert(
        global_queues[1].end(),
        s->local_queues[1].begin(),
        s->local_queues[1].end());
  } else {
    // only move local_queue[s->local_epoch % 2] entries

    // should have no entries in the global epoch, since it isn't there yet
    INVARIANT(s->local_queues[(local_epoch + 1) % 2].empty());
    global_queues[local_epoch % 2].insert(
        global_queues[local_epoch % 2].end(),
        s->local_queues[local_epoch % 2].begin(),
        s->local_queues[local_epoch % 2].end());
  }
  s->local_queues[0].clear();
  s->local_queues[1].clear();
  sync_map.erase(it);
  return s;
}

void
rcu::enable()
{
  if (gc_thread_started)
    return;
  {
    scoped_spinlock l(rcu_mutex());
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
  if (unlikely(!tl_sync)) {
    INVARIANT(!tl_crit_section_depth);
    enable();
    tl_sync = new sync(global_epoch);
    register_sync(pthread_self(), tl_sync);
  }
  INVARIANT(gc_thread_started);
  if (likely(!tl_crit_section_depth++)) {
    tl_sync->local_epoch = global_epoch;
    ALWAYS_ASSERT(pthread_spin_lock(&tl_sync->local_critical_mutex) == 0);
  }
}

void
rcu::free_with_fn(void *p, deleter_t fn)
{
  INVARIANT(tl_sync);
  INVARIANT(tl_crit_section_depth);
  INVARIANT(gc_thread_started);
  tl_sync->local_queues[tl_sync->local_epoch % 2].push_back(delete_entry(p, fn));
}

void
rcu::region_end()
{
  INVARIANT(tl_sync);
  INVARIANT(tl_crit_section_depth);
  INVARIANT(gc_thread_started);
  if (likely(!--tl_crit_section_depth))
    ALWAYS_ASSERT(pthread_spin_unlock(&tl_sync->local_critical_mutex) == 0);
}

void *
rcu::gc_thread_loop(void *p)
{
  // runs as daemon thread
  while (true) {
    // increment global epoch
    epoch_t cleaning_epoch = global_epoch++;
    COMPILER_MEMORY_FENCE;

    vector<delete_entry> elems;

    // now wait for each thread to finish any outstanding critical sections
    // from the previous epoch, and advance it forward to the global epoch
    {
      scoped_spinlock l(rcu_mutex());
      for (map<pthread_t, sync *>::iterator it = sync_map.begin();
           it != sync_map.end(); ++it) {
        sync *s = it->second;
        epoch_t local_epoch = s->local_epoch;
        if (local_epoch != global_epoch) {
          INVARIANT(local_epoch == cleaning_epoch);
          scoped_spinlock l0(&s->local_critical_mutex);
          s->local_epoch = global_epoch;
        }
        // now the next time the thread enters a critical section, it
        // *must* get the new global_epoch, so we can now claim its
        // deleted pointers from global_epoch - 1
        elems.insert(elems.end(),
                     s->local_queues[cleaning_epoch % 2].begin(),
                     s->local_queues[cleaning_epoch % 2].end());
        s->local_queues[cleaning_epoch % 2].clear();
      }

      // pull the ones from the global queue
      elems.insert(
          elems.end(),
          global_queues[cleaning_epoch % 2].begin(),
          global_queues[cleaning_epoch % 2].end());
      global_queues[cleaning_epoch % 2].clear();
    }

    for (vector<delete_entry>::iterator it = elems.begin();
         it != elems.end(); ++it)
      it->second(it->first);

    //cerr << "deleted " << elems.size() << " elements" << endl;

    // XXX: better solution for GC intervals?
    struct timespec t;
    memset(&t, 0, sizeof(t));
    t.tv_sec = 2;
    nanosleep(&t, NULL);
  }
  return NULL;
}

static void rcu_completion_callback(ndb_thread *t)
{
  rcu::sync *s = rcu::unregister_sync(t->pthread_id());
  if (s)
    delete s;
}
NDB_THREAD_REGISTER_COMPLETION_CALLBACK(rcu_completion_callback)
