#include <unistd.h>
#include <time.h>
#include <string.h>
#include <numa.h>
#include <sched.h>
#include <iostream>

#include "rcu.h"
#include "macros.h"
#include "util.h"
#include "thread.h"
#include "counter.h"
#include "lockguard.h"

using namespace std;
using namespace util;

event_counter rcu::evt_px_group_creates("px_group_creates");
event_counter rcu::evt_px_group_deletes("px_group_deletes");

// avoid some corner cases in the beginning
volatile rcu::epoch_t rcu::global_epoch = 1;
volatile rcu::epoch_t rcu::cleaning_epoch = 0;
rcu::px_queue rcu::global_queues[2];

volatile bool rcu::gc_thread_started = false;
pthread_t rcu::gc_thread_p;

map<pthread_t, rcu::sync *> rcu::sync_map;

__thread rcu::sync *rcu::tl_sync = NULL;
__thread unsigned int rcu::tl_crit_section_depth = 0;

static event_counter evt_rcu_deletes("rcu_deletes");
static event_counter evt_rcu_frees("rcu_frees");
static event_counter evt_rcu_local_reaps("rcu_local_reaps");
static event_counter evt_rcu_incomplete_local_reaps("rcu_incomplete_local_reaps");
static event_counter evt_rcu_loop_reaps("rcu_loop_reaps");
static event_counter evt_rcu_global_queue_reaps("rcu_global_queue_reaps");

static event_avg_counter evt_avg_gc_reaper_queue_len("avg_gc_reaper_queue_len");
static event_avg_counter evt_avg_rcu_delete_queue_len("avg_rcu_delete_queue_len");
static event_avg_counter evt_avg_rcu_local_delete_queue_len("avg_rcu_local_delete_queue_len");
static event_avg_counter evt_avg_rcu_sync_try_release("avg_rcu_sync_try_release");

spinlock &
rcu::rcu_mutex()
{
  static spinlock s_lock;
  return s_lock;
}

rcu::sync *
rcu::register_sync(pthread_t p)
{
  ::lock_guard<spinlock> l(rcu_mutex());
  map<pthread_t, sync *>::iterator it = sync_map.find(p);
  ALWAYS_ASSERT(it == sync_map.end());
  return (sync_map[p] = new sync(global_epoch));
}

rcu::sync *
rcu::unregister_sync(pthread_t p)
{
  ::lock_guard<spinlock> l(rcu_mutex());
  map<pthread_t, sync *>::iterator it = sync_map.find(p);
  if (it == sync_map.end())
    return NULL;
  sync * const s = it->second;

#ifdef CHECK_INVARIANTS
  const epoch_t local_epoch = s->local_epoch;
  //const epoch_t local_cleaning_epoch = s->local_epoch - 1;
  const epoch_t my_cleaning_epoch = cleaning_epoch;
  const epoch_t my_global_epoch = global_epoch;
  INVARIANT(my_global_epoch == local_epoch ||
            my_global_epoch == (local_epoch + 1));
  INVARIANT(my_global_epoch == (my_cleaning_epoch + 1) ||
            my_global_epoch == (my_cleaning_epoch + 2));
  INVARIANT(my_cleaning_epoch != local_epoch);
  //if (local_cleaning_epoch != my_cleaning_epoch) {
  //  cerr << "my_cleaning_epoch: " << my_cleaning_epoch << endl;
  //  cerr << "local_epoch: " << local_epoch << endl;
  //  cerr << "local_cleaning_epoch: " << local_cleaning_epoch << endl;
  //  cerr << "my_global_epoch: " << my_global_epoch << endl;
  //}
  INVARIANT(cleaning_epoch == my_cleaning_epoch);
  global_queues[0].sanity_check();
  global_queues[1].sanity_check();
#endif

  // xfer all px_groups to global queue
  global_queues[0].accept_from(s->local_queues[0]);
  global_queues[1].accept_from(s->local_queues[1]);
  sync_map.erase(it);
  INVARIANT(cleaning_epoch == my_cleaning_epoch); // shouldn't change b/c we hold rcu_mutex()
  return s;
}

void *
rcu::sync::alloc(size_t sz)
{
  if (pin_cpu == -1)
    // fallback to regular allocator
    return malloc(sz);
  auto sizes = ::allocator::ArenaSize(sz);
  auto arena = sizes.second;
  if (arena >= ::allocator::MAX_ARENAS)
    // fallback to regular allocator
    return malloc(sz);
  ensure_arena(arena);
  void *p = arenas[arena];
  ALWAYS_ASSERT(p);
  arenas[arena] = *reinterpret_cast<void **>(p);
  return p;
}

void
rcu::sync::dealloc(void *p, size_t sz)
{
  if (!::allocator::ManagesPointer(p)) {
    ::free(p);
    return;
  }
  auto sizes = ::allocator::ArenaSize(sz);
  auto arena = sizes.second;
  ALWAYS_ASSERT(arena < ::allocator::MAX_ARENAS);
  *reinterpret_cast<void **>(p) = arenas[arena];
  arenas[arena] = p;
  deallocs[arena]++;
}

void
rcu::sync::try_release()
{
  // XXX: tune
  static const size_t threshold = 10000;
  // only release if there are > threshold segments to release (over all arenas)
  size_t acc = 0;
  for (size_t i = 0; i < ::allocator::MAX_ARENAS; i++)
    acc += deallocs[i];
  if (acc > threshold) {
    do_release();
    evt_avg_rcu_sync_try_release.offer(acc);
  }
}

void
rcu::sync::do_release()
{
  ::allocator::ReleaseArenas(arenas);
  NDB_MEMSET(arenas, 0, sizeof(arenas));
  NDB_MEMSET(deallocs, 0, sizeof(deallocs));
}

void
rcu::enable_slowpath()
{
  {
    ::lock_guard<spinlock> l(rcu_mutex());
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
  sync * const s = mysync();
  INVARIANT(s);
  INVARIANT(gc_thread_started);
  if (!tl_crit_section_depth++) {
    s->local_critical_mutex.lock();
    s->local_epoch = global_epoch;
    INVARIANT(s->local_epoch != cleaning_epoch);
  }
}

void
rcu::free_with_fn(void *p, deleter_t fn)
{
  INVARIANT(tl_sync);
  INVARIANT(tl_crit_section_depth);
  INVARIANT(gc_thread_started);
  tl_sync->local_queues[tl_sync->local_epoch % 2].enqueue(p, fn);
  ++evt_rcu_frees;
#ifdef CHECK_INVARIANTS
  const epoch_t my_global_epoch = global_epoch;
  const epoch_t global_cleaning_epoch = cleaning_epoch;
  const epoch_t local_cleaning_epoch  = tl_sync->local_epoch - 1;
  if (!(local_cleaning_epoch == global_cleaning_epoch ||
        local_cleaning_epoch == (global_cleaning_epoch + 1))) {
    cerr << "my_global_epoch      : " << my_global_epoch << endl;
    cerr << "local_cleaning_epoch : " << local_cleaning_epoch << endl;
    cerr << "global_cleaning_epoch: " << global_cleaning_epoch << endl;
    cerr << "cur_global_epoch     : " << global_epoch << endl;
    INVARIANT(false);
  }
  tl_sync->local_queues[tl_sync->local_epoch % 2].sanity_check();
#endif
}

void
rcu::region_end(bool do_tl_cleanup)
{
  INVARIANT(tl_sync);
  INVARIANT(tl_crit_section_depth);
  INVARIANT(gc_thread_started);
  if (!--tl_crit_section_depth) {
#ifdef CHECK_INVARIANTS
    const epoch_t my_global_epoch = global_epoch;
    INVARIANT(tl_sync->local_epoch == my_global_epoch ||
              tl_sync->local_epoch == (my_global_epoch - 1));
#endif
    if (do_tl_cleanup) {
      const epoch_t local_cleaning_epoch  = tl_sync->local_epoch - 1;
      const epoch_t global_cleaning_epoch = cleaning_epoch;
#ifdef CHECK_INVARIANTS
      INVARIANT(tl_sync->local_epoch != global_cleaning_epoch);
      if (!(local_cleaning_epoch == global_cleaning_epoch ||
            local_cleaning_epoch == (global_cleaning_epoch + 1))) {
        cerr << "my_global_epoch      : " << my_global_epoch << endl;
        cerr << "local_cleaning_epoch : " << local_cleaning_epoch << endl;
        cerr << "global_cleaning_epoch: " << global_cleaning_epoch << endl;
        cerr << "cur_global_epoch     : " << global_epoch << endl;
        INVARIANT(false);
      }
#endif
      INVARIANT(tl_sync->scratch_queue.empty());
      tl_sync->scratch_queue.sanity_check();
      if (local_cleaning_epoch == global_cleaning_epoch) {
        // reap locally, outside the critical section
        px_queue &q = tl_sync->local_queues[local_cleaning_epoch % 2];
        if (!q.empty()) {
          tl_sync->scratch_queue.accept_from(q);
          tl_sync->scratch_queue.transfer_freelist(q);
          ++evt_rcu_local_reaps;
        }
      }
    }
    tl_sync->local_critical_mutex.unlock();
    px_queue &q = tl_sync->scratch_queue;
    if (!q.empty()) {
      size_t n = 0;
      for (px_queue::iterator it = q.begin(); it != q.end(); ++it, ++n) {
        try {
          it->second(it->first);
        } catch (...) {
          cerr << "rcu::region_end: uncaught exception in free routine" << endl;
        }
      }
      q.clear();
      evt_rcu_deletes += n;
      evt_avg_rcu_local_delete_queue_len.offer(n);
    }
  }
}

bool
rcu::in_rcu_region()
{
  return tl_crit_section_depth;
}

rcu::sync *
rcu::mysync()
{
  if (unlikely(!tl_sync)) {
    INVARIANT(!tl_crit_section_depth);
    enable();
    tl_sync = register_sync(pthread_self());
  }
  return tl_sync;
}

void
rcu::pin_current_thread(size_t cpu)
{
  sync * const s = mysync();
  s->set_pin_cpu(cpu);
  auto node = numa_node_of_cpu(cpu);
  // pin to node
  ALWAYS_ASSERT(!numa_run_on_node(node));
  // is numa_run_on_node() guaranteed to take effect immediately?
  ALWAYS_ASSERT(!sched_yield());
  // release current thread-local cache back to allocator
  s->do_release();
}

void
rcu::fault_region()
{
  sync * const s = mysync();
  if (s->get_pin_cpu() == -1)
    return;
  ::allocator::FaultRegion(s->get_pin_cpu());
}

static const uint64_t rcu_epoch_us = rcu::EpochTimeUsec;
static const uint64_t rcu_epoch_ns = rcu::EpochTimeNsec;

class gc_reaper_thread : public ndb_thread {
public:
  gc_reaper_thread()
    : ndb_thread(true, "rcu-reaper")
  {
  }

  virtual void
  run()
  {
    struct timespec t;
    t.tv_sec  = rcu_epoch_ns / ONE_SECOND_NS;
    t.tv_nsec = rcu_epoch_ns % ONE_SECOND_NS;
    rcu::px_queue stack_queue;
    for (;;) {
      // see if any elems to process
      {
        ::lock_guard<spinlock> l(lock);
        stack_queue.swap(queue);
      }
      if (stack_queue.empty()) {
        nanosleep(&t, NULL);
        continue;
      }
      stack_queue.sanity_check();
      size_t n = 0;
      for (rcu::px_queue::iterator it = stack_queue.begin();
           it != stack_queue.end(); ++it, ++n) {
        try {
          it->second(it->first);
        } catch (...) {
          cerr << "rcu-reaper: uncaught exception in free routine" << endl;
        }
      }
      evt_avg_gc_reaper_queue_len.offer(n);
      evt_rcu_deletes += n;
      stack_queue.clear();
      rcu::try_release();
    }
  }

  void
  reap(rcu::px_queue &local_queue)
  {
    if (local_queue.empty())
      return;
    ::lock_guard<spinlock> l0(lock);
    const size_t xfer = queue.accept_from(local_queue);
    evt_avg_rcu_delete_queue_len.offer(xfer);
    queue.transfer_freelist(local_queue, xfer); // push the memory back to the thread
  }

  spinlock lock;
  rcu::px_queue queue;
};

void *
rcu::gc_thread_loop(void *p)
{
  // runs as daemon thread
  struct timespec t;
  timer loop_timer;
  // ptrs so we don't have to deal w/ static destructors
  static gc_reaper_thread *reaper_loops[NGCReapers];
  for (unsigned int i = 0; i < NGCReapers; i++) {
    reaper_loops[i] = new gc_reaper_thread;
    reaper_loops[i]->start();
  }
  unsigned int rr = 0;
  for (;;) {

    const uint64_t last_loop_usec = loop_timer.lap();
    const uint64_t delay_time_usec = rcu_epoch_us;
    if (last_loop_usec < delay_time_usec) {
      const uint64_t sleep_ns = (delay_time_usec - last_loop_usec) * 1000;
      t.tv_sec  = sleep_ns / ONE_SECOND_NS;
      t.tv_nsec = sleep_ns % ONE_SECOND_NS;
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
      ::lock_guard<spinlock> l(rcu_mutex()); // prevents new threads from joining

      // force all threads to advance to new epoch
      for (map<pthread_t, sync *>::iterator it = sync_map.begin();
           it != sync_map.end(); ++it) {
        sync * const s = it->second;
        const epoch_t local_epoch = s->local_epoch;
#ifdef CHECK_INVARIANTS
        INVARIANT(local_epoch == global_epoch ||
                  local_epoch == new_cleaning_epoch);
#endif
        if (local_epoch != global_epoch) {
          ::lock_guard<spinlock> l0(s->local_critical_mutex);
          if (s->local_epoch == global_epoch)
            continue;
          // reap
          px_queue &q = s->local_queues[global_epoch % 2];
          if (!q.empty()) {
            reaper_loops[rr++ % NGCReapers]->reap(q);
            ++evt_rcu_incomplete_local_reaps;
          }
          s->local_epoch = global_epoch;
        }
        INVARIANT(s->local_epoch == global_epoch);
      }

      COMPILER_MEMORY_FENCE;
      cleaning_epoch = new_cleaning_epoch;
      __sync_synchronize();

      // reap the new cleaning epoch
      for (map<pthread_t, sync *>::iterator it = sync_map.begin();
           it != sync_map.end(); ++it) {
        sync * const s = it->second;
        INVARIANT(s->local_epoch == global_epoch);
        INVARIANT(new_cleaning_epoch != s->local_epoch);

        ::lock_guard<spinlock> l0(s->local_critical_mutex);
        // need a lock here because a thread-local cleanup could
        // be happening concurrently
        px_queue &q = s->local_queues[new_cleaning_epoch % 2];
        if (!q.empty()) {
          reaper_loops[rr++ % NGCReapers]->reap(q);
          ++evt_rcu_loop_reaps;
        }
      }

      // pull the ones from the global queue
      rcu::px_queue &q = global_queues[new_cleaning_epoch % 2];
      if (!q.empty()) {
        evt_rcu_global_queue_reaps += q.get_ngroups();
        reaper_loops[rr++ % NGCReapers]->reap(q);
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
