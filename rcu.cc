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

rcu rcu::s_instance;

static event_counter evt_rcu_deletes("rcu_deletes");
static event_counter evt_rcu_frees("rcu_frees");
static event_counter evt_rcu_local_reaps("rcu_local_reaps");
static event_counter evt_rcu_incomplete_local_reaps("rcu_incomplete_local_reaps");
static event_counter evt_rcu_loop_reaps("rcu_loop_reaps");

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
  uint64_t rcu_tick = 0;
  const bool in_region = in_rcu_region(rcu_tick);
  if (!in_region)
    INVARIANT(false);
  // already locked by the scoped region
  sync &s = mysync();
  INVARIANT(s.local_queue_mutexes_[rcu_tick % 3].is_locked());
  s.local_queues_[rcu_tick % 3].enqueue(delete_entry(p, fn), rcu_tick);
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
  s.set_pin_cpu(cpu);
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
  if (s.get_pin_cpu() == -1)
    return;
  ::allocator::FaultRegion(s.get_pin_cpu());
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
