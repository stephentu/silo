#include <unistd.h>
#include <time.h>
#include <string.h>
#include <iostream>

#include "rcu.h"
#include "macros.h"
#include "util.h"
#include "thread.h"
#include "counter.h"
#include "lockguard.h"

using namespace std;
using namespace util;

volatile rcu::epoch_t rcu::global_epoch = 0;
rcu::delete_queue rcu::global_queues[2];

volatile bool rcu::gc_thread_started = false;
pthread_t rcu::gc_thread_p;

map<pthread_t, rcu::sync *> rcu::sync_map;

__thread rcu::sync *rcu::tl_sync = NULL;
__thread unsigned int rcu::tl_crit_section_depth = 0;

spinlock &
rcu::rcu_mutex()
{
  static spinlock s_lock;
  return s_lock;
}

void
rcu::register_sync(pthread_t p, sync *s)
{
  lock_guard<spinlock> l(rcu_mutex());
  map<pthread_t, sync *>::iterator it = sync_map.find(p);
  ALWAYS_ASSERT(it == sync_map.end());
  sync_map[p] = s;
}

rcu::sync *
rcu::unregister_sync(pthread_t p)
{
  lock_guard<spinlock> l(rcu_mutex());
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
    lock_guard<spinlock> l(rcu_mutex());
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
    tl_sync->local_critical_mutex.lock();
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
    tl_sync->local_critical_mutex.unlock();
}

bool
rcu::in_rcu_region()
{
  return tl_crit_section_depth;
}

static event_counter evt_rcu_deletes("rcu_deletes");
static const uint64_t rcu_epoch_us = 50 * 1000; /* 50 ms */
static const uint64_t rcu_epoch_ns = rcu_epoch_us * 1000;

static event_avg_counter evt_avg_gc_reaper_queue_len("avg_gc_reaper_queue_len");

class gc_reaper_thread : public ndb_thread {
public:
  gc_reaper_thread()
    : ndb_thread(true, "rcu-reaper")
  {
    queue.reserve(32000);
  }
  virtual void
  run()
  {
    struct timespec t;
    NDB_MEMSET(&t, 0, sizeof(t));
    t.tv_nsec = rcu_epoch_ns;
    rcu::delete_queue stack_queue;
    stack_queue.reserve(rcu::SyncDeleteQueueBufSize);
    for (;;) {
      // see if any elems to process
      {
        lock_guard<spinlock> l(lock);
        stack_queue.swap(queue);
      }
      evt_avg_gc_reaper_queue_len.offer(stack_queue.size());
      if (stack_queue.empty())
        nanosleep(&t, NULL);
      for (rcu::delete_queue::iterator it = stack_queue.begin();
           it != stack_queue.end(); ++it) {
        try {
          it->second(it->first);
        } catch (...) {
          cerr << "rcu-reaper: uncaught exception in free routine" << endl;
        }
      }
      evt_rcu_deletes += stack_queue.size();
      stack_queue.clear();
    }
  }
  spinlock lock;
  rcu::delete_queue queue;
};

static event_avg_counter evt_avg_rcu_delete_queue_len("avg_rcu_delete_queue_len");

void *
rcu::gc_thread_loop(void *p)
{
  // runs as daemon thread
  struct timespec t;
  NDB_MEMSET(&t, 0, sizeof(t));
  timer loop_timer;
  static gc_reaper_thread reaper_loops[NGCReapers];
  for (unsigned int i = 0; i < NGCReapers; i++)
    reaper_loops[i].start();
  unsigned int rr = 0;
  for (;;) {

    const uint64_t last_loop_usec = loop_timer.lap();
    const uint64_t delay_time_usec = rcu_epoch_us;
    if (last_loop_usec < delay_time_usec) {
      t.tv_nsec = (delay_time_usec - last_loop_usec) * 1000;
      nanosleep(&t, NULL);
    }

    // increment global epoch
    const epoch_t cleaning_epoch = global_epoch++;

    __sync_synchronize();

    // now wait for each thread to finish any outstanding critical sections
    // from the previous epoch, and advance it forward to the global epoch
    {
      lock_guard<spinlock> l(rcu_mutex()); // prevents new threads from joining

      // force all threads to advance to new epoch
      for (map<pthread_t, sync *>::iterator it = sync_map.begin();
           it != sync_map.end(); ++it) {
        sync * const s = it->second;
        const epoch_t local_epoch = s->local_epoch;
        if (local_epoch != global_epoch) {
          INVARIANT(local_epoch == cleaning_epoch);
          lock_guard<spinlock> l0(s->local_critical_mutex);
          s->local_epoch = global_epoch;
        }
      }

      // claim deleted pointers
      for (map<pthread_t, sync *>::iterator it = sync_map.begin();
           it != sync_map.end(); ++it) {
        sync * const s = it->second;
        INVARIANT(s->local_epoch == global_epoch);

        // so we can now claim its deleted pointers from global_epoch - 1
        delete_queue &local_queue = s->local_queues[cleaning_epoch % 2];
        evt_avg_rcu_delete_queue_len.offer(local_queue.size());

        gc_reaper_thread &reaper_loop = reaper_loops[rr++ % NGCReapers];
        lock_guard<spinlock> l0(reaper_loop.lock);
        if (reaper_loop.queue.empty()) {
          reaper_loop.queue.swap(local_queue);
          INVARIANT(local_queue.empty());
        } else {
          //reaper_loop.queue.reserve(reaper_loop.queue.size() + local_queue.size());
          reaper_loop.queue.insert(
              reaper_loop.queue.end(),
              local_queue.begin(),
              local_queue.end());
          local_queue.clear();
        }
      }

      // pull the ones from the global queue
      delete_queue &global_queue = global_queues[cleaning_epoch % 2];
      if (unlikely(!global_queue.empty())) {
        gc_reaper_thread &reaper_loop = reaper_loops[rr++ % NGCReapers];
        lock_guard<spinlock> l0(reaper_loop.lock);
        if (reaper_loop.queue.empty()) {
          reaper_loop.queue.swap(global_queue);
          INVARIANT(global_queue.empty());
        } else {
          //reaper_loop.queue.reserve(reaper_loop.queue.size() + global_queue.size());
          reaper_loop.queue.insert(
              reaper_loop.queue.end(),
              global_queue.begin(),
              global_queue.end());
          global_queue.clear();
        }
      }
    }
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
