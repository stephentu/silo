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
static event_counter *evt_allocator_arena_allocations[::allocator::MAX_ARENAS] = {nullptr};
static event_counter *evt_allocator_arena_deallocations[::allocator::MAX_ARENAS] = {nullptr};
static event_counter evt_allocator_large_allocation("allocator_large_allocation");

static event_avg_counter evt_avg_gc_reaper_queue_len("avg_gc_reaper_queue_len");
static event_avg_counter evt_avg_rcu_delete_queue_len("avg_rcu_delete_queue_len");
static event_avg_counter evt_avg_rcu_local_delete_queue_len("avg_rcu_local_delete_queue_len");
static event_avg_counter evt_avg_rcu_sync_try_release("avg_rcu_sync_try_release");
static event_avg_counter evt_avg_time_inbetween_rcu_epochs_usec(
    "avg_time_inbetween_rcu_epochs_usec");
static event_avg_counter evt_avg_time_inbetween_allocator_releases_usec(
    "avg_time_inbetween_allocator_releases_usec");

#ifdef MEMCHECK_MAGIC
static void
report_error_and_die(
    const void *p, size_t alloc_size, const char *msg,
    const string &prefix="", unsigned recurse=3, bool first=true)
{
  // print the entire allocation block, for debugging reference
  static_assert(::allocator::AllocAlignment % 8 == 0, "xx");
  const void *pnext = *((const void **) p);
  cerr << prefix << "Address " << p << " error found! (next ptr " << pnext << ")" << endl;
  if (pnext) {
    const ::allocator::pgmetadata *pmd = ::allocator::PointerToPgMetadata(pnext);
    if (!pmd) {
      cerr << prefix << "Error: could not get pgmetadata for next ptr" << endl;
      cerr << prefix << "Allocator managed next ptr? " << ::allocator::ManagesPointer(pnext) << endl;
    } else {
      cerr << prefix << "Next ptr allocation size: " << pmd->unit_ << endl;
      if (((uintptr_t)pnext % pmd->unit_) == 0) {
        if (recurse)
          report_error_and_die(
              pnext, pmd->unit_, "", prefix + "    ", recurse - 1, false);
        else
          cerr << prefix << "recursion stopped" << endl;
      } else {
        cerr << prefix << "Next ptr not properly aligned" << endl;
        if (recurse)
          report_error_and_die(
              (const void *) slow_round_down((uintptr_t)pnext, (uintptr_t)pmd->unit_),
              pmd->unit_, "", prefix + "    ", recurse - 1, false);
        else
          cerr << prefix << "recursion stopped" << endl;
      }
    }
  }
  cerr << prefix << "Msg: " << msg << endl;
  cerr << prefix << "Allocation size: " << alloc_size << endl;
  cerr << prefix << "Ptr aligned properly? " << (((uintptr_t)p % alloc_size) == 0) << endl;
  for (const char *buf = (const char *) p;
      buf < (const char *) p + alloc_size;
      buf += 8) {
    cerr << prefix << hexify_buf(buf, 8) << endl;
  }
  if (first)
    ALWAYS_ASSERT(false);
}

static void
check_pointer_or_die(void *p, size_t alloc_size)
{
  ALWAYS_ASSERT(p);
  if (unlikely(((uintptr_t)p % alloc_size) != 0))
    report_error_and_die(p, alloc_size, "pointer not properly aligned");
  for (size_t off = sizeof(void **); off < alloc_size; off++)
    if (unlikely(
         (unsigned char) *((const char *) p + off) !=
         (unsigned char) MEMCHECK_MAGIC ) )
      report_error_and_die(p, alloc_size, "memory magic not found");
  void *pnext = *((void **) p);
  if (unlikely(((uintptr_t)pnext % alloc_size) != 0))
    report_error_and_die(p, alloc_size, "next pointer not properly aligned");
}
#endif

void *
rcu::sync::alloc(size_t sz)
{
  if (pin_cpu_ == -1)
    // fallback to regular allocator
    return malloc(sz);
  auto sizes = ::allocator::ArenaSize(sz);
  auto arena = sizes.second;
  if (arena >= ::allocator::MAX_ARENAS) {
    // fallback to regular allocator
    ++evt_allocator_large_allocation;
    return malloc(sz);
  }
  ensure_arena(arena);
  void *p = arenas_[arena];
  INVARIANT(p);
#ifdef MEMCHECK_MAGIC
  const size_t alloc_size = (arena + 1) * ::allocator::AllocAlignment;
  check_pointer_or_die(p, alloc_size);
#endif
  arenas_[arena] = *reinterpret_cast<void **>(p);
  evt_allocator_arena_allocations[arena]->inc();
  return p;
}

void *
rcu::sync::alloc_static(size_t sz)
{
  if (pin_cpu_ == -1)
    return malloc(sz);
  // round up to hugepagesize
  static const size_t hugepgsize = ::allocator::GetHugepageSize();
  sz = slow_round_up(sz, hugepgsize);
  INVARIANT((sz % hugepgsize) == 0);
  return ::allocator::AllocateUnmanaged(pin_cpu_, sz / hugepgsize);
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
#ifdef MEMCHECK_MAGIC
  const size_t alloc_size = (arena + 1) * ::allocator::AllocAlignment;
  ALWAYS_ASSERT( ((uintptr_t)p % alloc_size) == 0 );
  NDB_MEMSET(
      (char *) p + sizeof(void **),
      MEMCHECK_MAGIC, alloc_size - sizeof(void **));
  ALWAYS_ASSERT(*((void **) p) == arenas_[arena]);
  check_pointer_or_die(p, alloc_size);
#endif
  arenas_[arena] = p;
  evt_allocator_arena_deallocations[arena]->inc();
  deallocs_[arena]++;
}

bool
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
    return true;
  }
  return false;
}

void
rcu::sync::do_release()
{
#ifdef MEMCHECK_MAGIC
  for (size_t i = 0; i < ::allocator::MAX_ARENAS; i++) {
    const size_t alloc_size = (i + 1) * ::allocator::AllocAlignment;
    void *p = arenas_[i];
    while (p) {
      check_pointer_or_die(p, alloc_size);
      p = *((void **) p);
    }
  }
#endif
  ::allocator::ReleaseArenas(&arenas_[0]);
  NDB_MEMSET(&arenas_[0], 0, sizeof(arenas_));
  NDB_MEMSET(&deallocs_[0], 0, sizeof(deallocs_));
}

void
rcu::sync::do_cleanup()
{
  // compute cleaner epoch
  const uint64_t clean_tick_exclusive = impl_->cleaning_rcu_tick_exclusive();
  if (!clean_tick_exclusive)
    return;
  const uint64_t clean_tick = clean_tick_exclusive - 1;

  INVARIANT(last_reaped_epoch_ <= clean_tick);
  INVARIANT(scratch_.empty());
  if (last_reaped_epoch_ == clean_tick)
    return;

#ifdef ENABLE_EVENT_COUNTERS
  const uint64_t now = timer::cur_usec();
  if (last_reaped_timestamp_us_ > 0) {
    const uint64_t diff = now - last_reaped_timestamp_us_;
    evt_avg_time_inbetween_rcu_epochs_usec.offer(diff);
  }
  last_reaped_timestamp_us_ = now;
#endif
  last_reaped_epoch_ = clean_tick;

  scratch_.empty_accept_from(queue_, clean_tick);
  scratch_.transfer_freelist(queue_);
  rcu::px_queue &q = scratch_;
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

  // try to release memory from allocator slabs back
  if (impl_->try_release()) {
#ifdef ENABLE_EVENT_COUNTERS
    const uint64_t now = timer::cur_usec();
    if (last_release_timestamp_us_ > 0) {
      const uint64_t diff = now - last_release_timestamp_us_;
      evt_avg_time_inbetween_allocator_releases_usec.offer(diff);
    }
    last_release_timestamp_us_ = now;
#endif
  }
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
  s.queue_.enqueue(delete_entry(p, fn), rcu_tick);
  ++evt_rcu_frees;
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

rcu::rcu()
  : syncs_([this](sync &s) { s.impl_ = this; })
{
  // XXX: these should really be instance members of RCU
  // we are assuming only one rcu object is ever created
  for (size_t i = 0; i < ::allocator::MAX_ARENAS; i++) {
    evt_allocator_arena_allocations[i] =
      new event_counter("allocator_arena" + to_string(i) + "_allocation");
    evt_allocator_arena_deallocations[i] =
      new event_counter("allocator_arena" + to_string(i) + "_deallocation");
  }
}

void
rcu::Test()
{

}
