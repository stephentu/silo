#include <unistd.h>
#include <time.h>
#include <string.h>
#include <numa.h>
#include <sched.h>
#include <iostream>
#include <thread>

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
rcu rcu::s_instance;

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

void *
rcu::sync::alloc(size_t sz)
{
  if (pin_cpu_ == -1)
    // fallback to regular allocator
    return malloc(sz);
  auto sizes = ::allocator::ArenaSize(sz);
  auto arena = sizes.second;
  if (arena >= ::allocator::MAX_ARENAS)
    // fallback to regular allocator
    return malloc(sz);
  ensure_arena(arena);
  void *p = arenas_[arena];
  ALWAYS_ASSERT(p);
  arenas_[arena] = *reinterpret_cast<void **>(p);
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
  *reinterpret_cast<void **>(p) = arenas_[arena];
  arenas_[arena] = p;
  deallocs_[arena]++;
}

void
rcu::sync::try_release()
{
  // XXX: tune
  static const size_t threshold = 10000;
  // only release if there are > threshold segments to release (over all arenas)
  size_t acc = 0;
  for (size_t i = 0; i < ::allocator::MAX_ARENAS; i++)
    acc += deallocs_[i];
  if (acc > threshold) {
    do_release();
    evt_avg_rcu_sync_try_release.offer(acc);
  }
}

void
rcu::sync::do_release()
{
  ::allocator::ReleaseArenas(arenas_);
  NDB_MEMSET(arenas_, 0, sizeof(arenas_));
  NDB_MEMSET(deallocs_, 0, sizeof(deallocs_));
}

void
rcu::free_with_fn(void *p, deleter_t fn)
{
  uint64_t rcu_tick;
  const bool in_region = in_rcu_region(rcu_tick);
  if (!in_region)
    INVARIANT(false);
  // already locked by the scoped region
  sync &s = mysync();
  INVARIANT(s.local_queue_mutexes_[rcu_tick % 3].is_locked());
  s.local_queues_[rcu_tick % 3].enqueue(p, fn, rcu_tick);
  ++evt_rcu_frees;
}

void
rcu::sync::threadpurge()
{
  INVARIANT(!impl_->in_rcu_region());
  const uint64_t clean_tick_exclusive = impl_->cleaning_rcu_tick_exclusive();
  if (!clean_tick_exclusive)
    return;
  const uint64_t clean_tick = clean_tick_exclusive - 1;
  INVARIANT(scratch_queue_.empty());
  {
    ::lock_guard<spinlock> l(local_queue_mutexes_[clean_tick % 3]);
    scratch_queue_.empty_accept_from(local_queues_[clean_tick % 3], clean_tick);
    scratch_queue_.transfer_freelist(local_queues_[clean_tick % 3]);
  }
  px_queue &q = scratch_queue_;
  if (q.empty())
    return;
  size_t n = 0;
  for (auto it = q.begin(); it != q.end(); ++it, ++n) {
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

void
rcu::pin_current_thread(size_t cpu)
{
  sync &s = mysync();
  s.set_pin_cpu_(cpu);
  auto node = numa_node_of_cpu(cpu);
  // pin to node
  ALWAYS_ASSERT(!numa_run_on_node(node));
  // is numa_run_on_node() guaranteed to take effect immediately?
  ALWAYS_ASSERT(!sched_yield());
  // release current thread-local cache back to allocator
  s.do_release();
}

void
rcu::fault_region()
{
  sync &s = mysync();
  if (s.get_pin_cpu_() == -1)
    return;
  ::allocator::FaultRegion(s.get_pin_cpu_());
}

void
rcu::gcloop(unsigned id)
{
  // runs as daemon
  px_queue scratch_queue;
  timer loop_timer;
  struct timespec t;
  for (;;) {
    const uint64_t last_loop_usec = loop_timer.lap();
    const uint64_t delay_time_usec = EpochTimeUsec;
    if (last_loop_usec < delay_time_usec) {
      const uint64_t sleep_ns = (delay_time_usec - last_loop_usec) * 1000;
      t.tv_sec  = sleep_ns / ONE_SECOND_NS;
      t.tv_nsec = sleep_ns % ONE_SECOND_NS;
      nanosleep(&t, nullptr);
    }

    // compute cleaner epoch
    const uint64_t clean_tick_exclusive = cleaning_rcu_tick_exclusive();
    if (!clean_tick_exclusive)
      continue;
    const uint64_t clean_tick = clean_tick_exclusive - 1;

    // RR assignment of gcloops to cores
    for (size_t i = id; i < NMAXCORES; i += NGCReapers) {
      sync *s = syncs_.view(i);
      if (!s)
        continue;
      {
        ::lock_guard<spinlock> l(s->local_queue_mutexes_[clean_tick % 3]);
        scratch_queue.empty_accept_from(s->local_queues_[clean_tick % 3], clean_tick);
        scratch_queue.transfer_freelist(s->local_queues_[clean_tick % 3]);
      }
      px_queue &q = scratch_queue;
      if (q.empty())
        continue;
      size_t n = 0;
      for (auto it = q.begin(); it != q.end(); ++it, ++n) {
        try {
          it->second(it->first);
        } catch (...) {
          cerr << "rcu-reaper: uncaught exception in free routine" << endl;
        }
      }
      q.clear();
      evt_rcu_deletes += n;
      evt_avg_gc_reaper_queue_len.offer(n);
    }

    // try to release memory from allocator slabs back
    try_release();
  }
}

rcu::rcu()
  : syncs_([this](sync &s) { s.impl_ = this; })
{
  // background the collector threads
  for (size_t i = 0; i < NGCReapers; i++)
    thread(&rcu::gcloop, this, i).detach();
}

//class gc_reaper_thread : public ndb_thread {
//public:
//  gc_reaper_thread()
//    : ndb_thread(true, "rcu-reaper")
//  {
//  }
//
//  virtual void
//  run()
//  {
//    struct timespec t;
//    t.tv_sec  = rcu_epoch_ns / ONE_SECOND_NS;
//    t.tv_nsec = rcu_epoch_ns % ONE_SECOND_NS;
//    rcu::px_queue stack_queue;
//    for (;;) {
//      // see if any elems to process
//      {
//        ::lock_guard<spinlock> l(lock);
//        stack_queue.swap(queue);
//      }
//      if (stack_queue.empty()) {
//        nanosleep(&t, NULL);
//        continue;
//      }
//      stack_queue.sanity_check();
//      size_t n = 0;
//      for (rcu::px_queue::iterator it = stack_queue.begin();
//           it != stack_queue.end(); ++it, ++n) {
//        try {
//          it->second(it->first);
//        } catch (...) {
//          cerr << "rcu-reaper: uncaught exception in free routine" << endl;
//        }
//      }
//      evt_avg_gc_reaper_queue_len.offer(n);
//      evt_rcu_deletes += n;
//      stack_queue.clear();
//      rcu::try_release();
//    }
//  }
//
//  void
//  reap(rcu::px_queue &local_queue)
//  {
//    if (local_queue.empty())
//      return;
//    ::lock_guard<spinlock> l0(lock);
//    const size_t xfer = queue.accept_from(local_queue);
//    evt_avg_rcu_delete_queue_len.offer(xfer);
//    queue.transfer_freelist(local_queue, xfer); // push the memory back to the thread
//  }
//
//  spinlock lock;
//  rcu::px_queue queue;
//};
//
//void *
//rcu::gc_thread_loop(void *p)
//{
//  // runs as daemon thread
//  struct timespec t;
//  timer loop_timer;
//  // ptrs so we don't have to deal w/ static destructors
//  static gc_reaper_thread *reaper_loops[NGCReapers];
//  for (unsigned int i = 0; i < NGCReapers; i++) {
//    reaper_loops[i] = new gc_reaper_thread;
//    reaper_loops[i]->start();
//  }
//  unsigned int rr = 0;
//  for (;;) {
//
//    const uint64_t last_loop_usec = loop_timer.lap();
//    const uint64_t delay_time_usec = rcu_epoch_us;
//    if (last_loop_usec < delay_time_usec) {
//      const uint64_t sleep_ns = (delay_time_usec - last_loop_usec) * 1000;
//      t.tv_sec  = sleep_ns / ONE_SECOND_NS;
//      t.tv_nsec = sleep_ns % ONE_SECOND_NS;
//      nanosleep(&t, NULL);
//    }
//
//    // increment global epoch
//    INVARIANT(cleaning_epoch + 1 == global_epoch);
//    COMPILER_MEMORY_FENCE;
//    const epoch_t new_cleaning_epoch = global_epoch++;
//    INVARIANT(cleaning_epoch + 1 == new_cleaning_epoch);
//    __sync_synchronize();
//
//    // now wait for each thread to finish any outstanding critical sections
//    // from the previous epoch, and advance it forward to the global epoch
//    {
//      ::lock_guard<spinlock> l(rcu_mutex()); // prevents new threads from joining
//
//      // force all threads to advance to new epoch
//      for (map<pthread_t, sync *>::iterator it = sync_map.begin();
//           it != sync_map.end(); ++it) {
//        sync * const s = it->second;
//        const epoch_t local_epoch = s->local_epoch;
//#ifdef CHECK_INVARIANTS
//        INVARIANT(local_epoch == global_epoch ||
//                  local_epoch == new_cleaning_epoch);
//#endif
//        if (local_epoch != global_epoch) {
//          ::lock_guard<spinlock> l0(s->local_critical_mutex);
//          if (s->local_epoch == global_epoch)
//            continue;
//          // reap
//          px_queue &q = s->local_queues_[global_epoch % 2];
//          if (!q.empty()) {
//            reaper_loops[rr++ % NGCReapers]->reap(q);
//            ++evt_rcu_incomplete_local_reaps;
//          }
//          s->local_epoch = global_epoch;
//        }
//        INVARIANT(s->local_epoch == global_epoch);
//      }
//
//      COMPILER_MEMORY_FENCE;
//      cleaning_epoch = new_cleaning_epoch;
//      __sync_synchronize();
//
//      // reap the new cleaning epoch
//      for (map<pthread_t, sync *>::iterator it = sync_map.begin();
//           it != sync_map.end(); ++it) {
//        sync * const s = it->second;
//        INVARIANT(s->local_epoch == global_epoch);
//        INVARIANT(new_cleaning_epoch != s->local_epoch);
//
//        ::lock_guard<spinlock> l0(s->local_critical_mutex);
//        // need a lock here because a thread-local cleanup could
//        // be happening concurrently
//        px_queue &q = s->local_queues_[new_cleaning_epoch % 2];
//        if (!q.empty()) {
//          reaper_loops[rr++ % NGCReapers]->reap(q);
//          ++evt_rcu_loop_reaps;
//        }
//      }
//
//      // pull the ones from the global queue
//      rcu::px_queue &q = global_queues[new_cleaning_epoch % 2];
//      if (!q.empty()) {
//        evt_rcu_global_queue_reaps += q.get_ngroups();
//        reaper_loops[rr++ % NGCReapers]->reap(q);
//      }
//    }
//  }
//  return NULL;
//}
//
//static void rcu_completion_callback(ndb_thread *t)
//{
//  rcu::sync *s = rcu::unregister_sync(t->pthread_id());
//  if (s)
//    delete s;
//}
//NDB_THREAD_REGISTER_COMPLETION_CALLBACK(rcu_completion_callback)
