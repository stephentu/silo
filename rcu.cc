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

// avoid some corner cases in the beginning
volatile rcu::epoch_t rcu::global_epoch = 1;
volatile rcu::epoch_t rcu::cleaning_epoch = 0;
rcu::delete_queue rcu::global_queues[2];

volatile bool rcu::gc_thread_started = false;
pthread_t rcu::gc_thread_p;

map<pthread_t, rcu::sync *> rcu::sync_map;

__thread rcu::sync *rcu::tl_sync = NULL;
__thread unsigned int rcu::tl_crit_section_depth = 0;

static event_counter evt_rcu_deletes("rcu_deletes");
static event_counter evt_rcu_frees("rcu_frees");
static event_counter evt_rcu_local_reaps("rcu_local_reaps");

static event_avg_counter evt_avg_gc_reaper_queue_len("avg_gc_reaper_queue_len");
static event_avg_counter evt_avg_rcu_delete_queue_len("avg_rcu_delete_queue_len");
static event_avg_counter evt_avg_rcu_local_delete_queue_len("avg_rcu_local_delete_queue_len");

spinlock &
rcu::rcu_mutex()
{
  static spinlock s_lock;
  return s_lock;
}

rcu::sync *
rcu::register_sync(pthread_t p)
{
  lock_guard<spinlock> l(rcu_mutex());
  map<pthread_t, sync *>::iterator it = sync_map.find(p);
  ALWAYS_ASSERT(it == sync_map.end());
  return (sync_map[p] = new sync(global_epoch));
}

rcu::sync *
rcu::unregister_sync(pthread_t p)
{
  lock_guard<spinlock> l(rcu_mutex());
  map<pthread_t, sync *>::iterator it = sync_map.find(p);
  if (it == sync_map.end())
    return NULL;
  sync * const s = it->second;
  const epoch_t local_epoch = s->local_epoch;
  const epoch_t local_cleaning_epoch = s->local_epoch - 1;
  INVARIANT(global_epoch == local_epoch || global_epoch == (local_epoch + 1));
  INVARIANT(local_cleaning_epoch == cleaning_epoch); // b/c we are out of RCU loop
  INVARIANT(EnableThreadLocalCleanup ||
            global_queues[local_cleaning_epoch % 2].empty());
  if (EnableThreadLocalCleanup) {
    global_queues[local_cleaning_epoch % 2].insert(
        global_queues[local_cleaning_epoch % 2].end(),
        s->local_queues[local_cleaning_epoch % 2].begin(),
        s->local_queues[local_cleaning_epoch % 2].end());
  }
  global_queues[local_epoch % 2].insert(
      global_queues[local_epoch % 2].end(),
      s->local_queues[local_epoch % 2].begin(),
      s->local_queues[local_epoch % 2].end());
  s->local_queues[0].clear();
  s->local_queues[1].clear();
  sync_map.erase(it);
  return s;
}

void
rcu::enable()
{
  if (likely(gc_thread_started))
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
    tl_sync = register_sync(pthread_self());
  }
  INVARIANT(tl_sync);
  INVARIANT(gc_thread_started);
  if (!tl_crit_section_depth++) {
    tl_sync->local_critical_mutex.lock();
    tl_sync->local_epoch = global_epoch;
  }
}

void
rcu::free_with_fn(void *p, deleter_t fn)
{
  INVARIANT(tl_sync);
  INVARIANT(tl_crit_section_depth);
  INVARIANT(gc_thread_started);
  tl_sync->local_queues[tl_sync->local_epoch % 2].push_back(delete_entry(p, fn));
  ++evt_rcu_frees;
}

void
rcu::region_end()
{
  INVARIANT(tl_sync);
  INVARIANT(tl_crit_section_depth);
  INVARIANT(gc_thread_started);
  if (!--tl_crit_section_depth) {
    const epoch_t my_global_epoch = global_epoch;
    INVARIANT(tl_sync->local_epoch == my_global_epoch ||
              tl_sync->local_epoch == (my_global_epoch - 1));
    if (EnableThreadLocalCleanup) {
      const epoch_t local_cleaning_epoch  = tl_sync->local_epoch - 1;
      const epoch_t global_cleaning_epoch = cleaning_epoch;
      if (!(local_cleaning_epoch == global_cleaning_epoch ||
            local_cleaning_epoch == global_cleaning_epoch + 1)) {
        cerr << "my_global_epoch      : " << my_global_epoch << endl;
        cerr << "local_cleaning_epoch : " << local_cleaning_epoch << endl;
        cerr << "global_cleaning_epoch: " << global_cleaning_epoch << endl;
        cerr << "cur_global_epoch     : " << global_epoch << endl;
        INVARIANT(false);
      }
      INVARIANT(tl_sync->scratch_queue.empty());
      if (local_cleaning_epoch == global_cleaning_epoch &&
          !tl_sync->local_queues[local_cleaning_epoch % 2].empty()) {
        // reap locally, outside the critical section
        swap(tl_sync->local_queues[local_cleaning_epoch % 2],
             tl_sync->scratch_queue);
        ++evt_rcu_local_reaps;
      }
    }
    tl_sync->local_critical_mutex.unlock();
    delete_queue &q = tl_sync->scratch_queue;
    if (EnableThreadLocalCleanup && !q.empty()) {
      for (rcu::delete_queue::iterator it = q.begin(); it != q.end(); ++it) {
        try {
          it->second(it->first);
        } catch (...) {
          cerr << "rcu::region_end: uncaught exception in free routine" << endl;
        }
      }
      evt_rcu_deletes += q.size();
      evt_avg_rcu_local_delete_queue_len.offer(q.size());
      q.clear();
    }
  }
}

bool
rcu::in_rcu_region()
{
  return tl_crit_section_depth;
}


static const uint64_t rcu_epoch_us = 50 * 1000; /* 50 ms */
static const uint64_t rcu_epoch_ns = rcu_epoch_us * 1000;

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

  void
  reap_and_clear(rcu::delete_queue &local_queue)
  {
    lock_guard<spinlock> l0(lock);
    if (queue.empty()) {
      queue.swap(local_queue);
    } else {
      queue.insert(
          queue.end(),
          local_queue.begin(),
          local_queue.end());
      local_queue.clear();
    }
    INVARIANT(local_queue.empty());
  }

  spinlock lock;
  rcu::delete_queue queue;
};



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
    INVARIANT(cleaning_epoch + 1 == global_epoch);
    COMPILER_MEMORY_FENCE;
    const epoch_t new_cleaning_epoch = global_epoch++;
    INVARIANT(cleaning_epoch + 1 == new_cleaning_epoch);
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
          INVARIANT(local_epoch == new_cleaning_epoch);
          lock_guard<spinlock> l0(s->local_critical_mutex);
          if (s->local_epoch == global_epoch)
            // has moved on, so we cannot safely reap
            // like we do below
            continue;
          INVARIANT(s->local_epoch == new_cleaning_epoch);
          if (EnableThreadLocalCleanup) {
            // if we haven't completely finished thread-local
            // cleanup, then we move the pointers into the
            // gc reapers
            delete_queue &local_queue = s->local_queues[global_epoch % 2];
            if (!local_queue.empty())
              reaper_loops[rr++ % NGCReapers].reap_and_clear(local_queue);
          }
          INVARIANT(s->local_queues[global_epoch % 2].empty());
          s->local_epoch = global_epoch;
        }
      }

      COMPILER_MEMORY_FENCE;
      cleaning_epoch = new_cleaning_epoch;
      __sync_synchronize();

      if (!EnableThreadLocalCleanup) {
        // claim deleted pointers
        for (map<pthread_t, sync *>::iterator it = sync_map.begin();
             it != sync_map.end(); ++it) {
          sync * const s = it->second;
          INVARIANT(s->local_epoch == global_epoch);

          // so we can now claim its deleted pointers from global_epoch - 1
          delete_queue &local_queue = s->local_queues[cleaning_epoch % 2];
          evt_avg_rcu_delete_queue_len.offer(local_queue.size());
          reaper_loops[rr++ % NGCReapers].reap_and_clear(local_queue);
          INVARIANT(local_queue.empty());
        }
      }

      // pull the ones from the global queue
      if (EnableThreadLocalCleanup) {
        delete_queue &q = global_queues[global_epoch % 2];
        if (!q.empty())
          reaper_loops[rr++ % NGCReapers].reap_and_clear(q);
      }
      INVARIANT(global_queues[global_epoch % 2].empty());
      delete_queue &global_queue = global_queues[cleaning_epoch % 2];
      reaper_loops[rr++ % NGCReapers].reap_and_clear(global_queue);
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
