#ifndef _RCU_H_
#define _RCU_H_

#include <stdint.h>
#include <pthread.h>

#include <map>
#include <vector>
#include <list>
#include <utility>

#include "allocator.h"
#include "counter.h"
#include "spinlock.h"
#include "util.h"
#include "ticker.h"
#include "pxqueue.h"

class rcu {
  friend class scoped_rcu_region;
public:
  typedef uint64_t epoch_t;

  typedef void (*deleter_t)(void *);
  typedef std::pair<void *, deleter_t> delete_entry;
  typedef basic_px_queue<delete_entry, 4096> px_queue;

  template <typename T>
  static inline void
  deleter(void *p)
  {
    delete (T *) p;
  }

  template <typename T>
  static inline void
  deleter_array(void *p)
  {
    delete [] (T *) p;
  }

  // XXX(stephentu): tune?
  static const size_t NGCReapers = 4;

#ifdef CHECK_INVARIANTS
  static const uint64_t EpochTimeMultiplier = 1; /* 10 ms */
#else
  static const uint64_t EpochTimeMultiplier = 100; /* 1 sec */
#endif

  static_assert(EpochTimeMultiplier >= 1, "XX");

  // legacy helpers
  static const uint64_t EpochTimeUsec = ticker::tick_us * EpochTimeMultiplier;
  static const uint64_t EpochTimeNsec = EpochTimeUsec * 1000;

  static const size_t NQueueGroups = 32;

  // all RCU threads interact w/ the RCU subsystem via
  // a sync struct
  //
  // this is also serving as a memory allocator for the time being
  struct sync {
    friend class rcu;
  public:
    spinlock local_queue_mutexes_[3]; // must hold mutex[i] to manipulate
                                      // queue[i]
    px_queue local_queues_[3];
    px_queue scratch_queue_; // no need to lock b/c completely local
    unsigned depth_; // 0 indicates no rcu region

  private:
    rcu *impl_;

    // local memory allocator
    ssize_t pin_cpu_;
    void *arenas_[allocator::MAX_ARENAS];
    size_t deallocs_[allocator::MAX_ARENAS]; // keeps track of the number of
                                            // un-released deallocations

  public:
    sync()
      : depth_(0), impl_(nullptr), pin_cpu_(-1)
    {
      local_queues_[0].alloc_freelist(NQueueGroups);
      local_queues_[1].alloc_freelist(NQueueGroups);
      local_queues_[2].alloc_freelist(NQueueGroups);
      NDB_MEMSET(arenas_, 0, sizeof(arenas_));
      NDB_MEMSET(deallocs_, 0, sizeof(deallocs_));
    }

    inline void
    set_pin_cpu_(size_t cpu)
    {
      pin_cpu_ = cpu;
    }

    inline ssize_t
    get_pin_cpu() const
    {
      return pin_cpu_;
    }

    // allocate a block of memory of size sz. caller needs to remember
    // the size of the allocation when calling free
    void *alloc(size_t sz);

    void dealloc(void *p, size_t sz);

    // try to release local arenas back to the allocator based on
    // some simple thresholding heuristics- should only be called
    // by background cleaners
    void try_release();

    // fill q with free-able pointers
    // q must be empty
    //
    // must be called OUTSIDE of RCU region
    //
    // note these pointers will be removed from delete queues, transfering
    // responsibility for calling free to the caller
    void threadpurge();

    inline unsigned depth() const { return depth_; }

  private:

    void do_release();

    inline void
    ensure_arena(size_t arena)
    {
      if (likely(arenas_[arena]))
        return;
      INVARIANT(pin_cpu_ >= 0);
      arenas_[arena] = allocator::AllocateArenas(pin_cpu_, arena);
    }
  };

  // thin forwarders
  inline void *
  alloc(size_t sz)
  {
    return mysync().alloc(sz);
  }

  // this releases memory back to the allocator subsystem
  // this should NOT be used to free objects!
  inline void
  dealloc(void *p, size_t sz)
  {
    return mysync().dealloc(p, sz);
  }

  inline void
  try_release()
  {
    return mysync().try_release();
  }

  inline void
  threadpurge()
  {
    mysync().threadpurge();
  }

  void free_with_fn(void *p, deleter_t fn);

  template <typename T>
  inline void
  free(T *p)
  {
    free_with_fn(p, deleter<T>);
  }

  template <typename T>
  inline void
  free_array(T *p)
  {
    free_with_fn(p, deleter_array<T>);
  }

  // the tick is in units of rcu ticks
  inline bool
  in_rcu_region(uint64_t &rcu_tick) const
  {
    const sync *s = syncs_.myview();
    if (!s)
      return false;
    const bool is_guarded = ticker::s_instance.is_locally_guarded(rcu_tick);
    const bool has_depth = s->depth();
    if (has_depth && !is_guarded)
      INVARIANT(false);
    rcu_tick = to_rcu_ticks(rcu_tick);
    return has_depth;
  }

  inline bool
  in_rcu_region() const
  {
    uint64_t rcu_tick;
    return in_rcu_region(rcu_tick);
  }

  // all threads have moved at least to the cleaning tick, so any pointers <
  // the cleaning tick can be safely purged
  inline uint64_t
  cleaning_rcu_tick_exclusive() const
  {
    return to_rcu_ticks(ticker::s_instance.global_last_tick_exclusive());
  }

  // pin the current thread to CPU.
  //
  // this CPU number corresponds to the ones exposed by
  // sched.h. note that we currently pin to the numa node
  // associated with the cpu. memory allocation, however, is
  // CPU-specific
  void pin_current_thread(size_t cpu);

  void fault_region();

  rcu(); // initer

  static rcu s_instance; // system wide instance

private:

  static inline uint64_t constexpr
  to_rcu_ticks(uint64_t ticks)
  {
    return ticks / EpochTimeMultiplier;
  }

  inline sync &mysync() { return syncs_.my(); }

  void gcloop(unsigned i); // main reaper loop

  percore_lazy<sync> syncs_;
};

class scoped_rcu_region {
public:

  // movable, but not copy-constructable
  scoped_rcu_region(scoped_rcu_region &&) = default;
  scoped_rcu_region(const scoped_rcu_region &) = delete;
  scoped_rcu_region &operator=(const scoped_rcu_region &) = delete;

  scoped_rcu_region()
    : guard_(ticker::s_instance),
      sync_(&rcu::s_instance.mysync())
  {
    if (!sync_->depth_++)
      sync_->local_queue_mutexes_[rcu::to_rcu_ticks(guard_.tick()) % 3].lock();
  }

  ~scoped_rcu_region()
  {
    INVARIANT(sync_->depth_);
    if (!--sync_->depth_)
      sync_->local_queue_mutexes_[rcu::to_rcu_ticks(guard_.tick()) % 3].unlock();
  }

  inline ticker::guard &
  guard()
  {
    return guard_;
  }

private:
  ticker::guard guard_;
  rcu::sync *sync_;
};

#endif /* _RCU_H_ */
