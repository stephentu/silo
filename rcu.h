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
  template <bool> friend class scoped_rcu_base;
public:
  class sync;
  typedef uint64_t epoch_t;

  typedef void (*deleter_t)(void *);
  struct delete_entry {
      void* ptr;
      intptr_t action;

      inline delete_entry(void* ptr, size_t sz)
          : ptr(ptr), action(-sz) {
          INVARIANT(action < 0);
      }
      inline delete_entry(void* ptr, deleter_t fn)
          : ptr(ptr), action(reinterpret_cast<uintptr_t>(fn)) {
          INVARIANT(action > 0);
      }
      void run(rcu::sync& s) {
          if (action < 0)
              s.dealloc(ptr, -action);
          else
              (*reinterpret_cast<deleter_t>(action))(ptr);
      }
      bool operator==(const delete_entry& x) const {
          return ptr == x.ptr && action == x.action;
      }
      bool operator!=(const delete_entry& x) const {
          return !(*this == x);
      }
      bool operator<(const delete_entry& x) const {
          return ptr < x.ptr || (ptr == x.ptr && action < x.action);
      }
  };
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

#ifdef CHECK_INVARIANTS
  static const uint64_t EpochTimeMultiplier = 10; /* 10 * 1 ms */
#else
  static const uint64_t EpochTimeMultiplier = 25; /* 25 * 40 ms */
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
  class sync {
    friend class rcu;
    template <bool> friend class scoped_rcu_base;
  public:
    px_queue queue_;
    px_queue scratch_;
    unsigned depth_; // 0 indicates no rcu region
    unsigned last_reaped_epoch_;
#ifdef ENABLE_EVENT_COUNTERS
    uint64_t last_reaped_timestamp_us_;
    uint64_t last_release_timestamp_us_;
#endif

  private:
    rcu *impl_;

    // local memory allocator
    ssize_t pin_cpu_;
    void *arenas_[allocator::MAX_ARENAS];
    size_t deallocs_[allocator::MAX_ARENAS]; // keeps track of the number of
                                             // un-released deallocations

  public:

    sync(rcu *impl)
      : depth_(0)
      , last_reaped_epoch_(0)
#ifdef ENABLE_EVENT_COUNTERS
      , last_reaped_timestamp_us_(0)
      , last_release_timestamp_us_(0)
#endif
      , impl_(impl)
      , pin_cpu_(-1)
    {
      ALWAYS_ASSERT(((uintptr_t)this % CACHELINE_SIZE) == 0);
      queue_.alloc_freelist(NQueueGroups);
      scratch_.alloc_freelist(NQueueGroups);
      NDB_MEMSET(&arenas_[0], 0, sizeof(arenas_));
      NDB_MEMSET(&deallocs_[0], 0, sizeof(deallocs_));
    }

    inline void
    set_pin_cpu(size_t cpu)
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

    // allocates a block of memory of size sz, with the intention of never
    // free-ing it. is meant for reasonably large allocations (order of pages)
    void *alloc_static(size_t sz);

    void dealloc(void *p, size_t sz);
    void dealloc_rcu(void *p, size_t sz);

    // try to release local arenas back to the allocator based on some simple
    // thresholding heuristics-- is relative expensive operation.  returns true
    // if a release was actually performed, false otherwise
    bool try_release();

    void do_cleanup();

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

  inline void *
  alloc_static(size_t sz)
  {
    return mysync().alloc_static(sz);
  }

  // this releases memory back to the allocator subsystem
  // this should NOT be used to free objects!
  inline void
  dealloc(void *p, size_t sz)
  {
    return mysync().dealloc(p, sz);
  }

  void dealloc_rcu(void *p, size_t sz);

  inline bool
  try_release()
  {
    return mysync().try_release();
  }

  inline void
  do_cleanup()
  {
    mysync().do_cleanup();
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
    if (unlikely(!s))
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

  static rcu s_instance CACHE_ALIGNED; // system wide instance

  static void Test();

private:

  rcu(); // private ctor to enforce singleton

  static inline uint64_t constexpr
  to_rcu_ticks(uint64_t ticks)
  {
    return ticks / EpochTimeMultiplier;
  }

  inline sync &mysync() { return syncs_.my(this); }

  percore_lazy<sync> syncs_;
};

template <bool DoCleanup>
class scoped_rcu_base {
public:

  // movable, but not copy-constructable
  scoped_rcu_base(scoped_rcu_base &&) = default;
  scoped_rcu_base(const scoped_rcu_base &) = delete;
  scoped_rcu_base &operator=(const scoped_rcu_base &) = delete;

  scoped_rcu_base()
    : sync_(&rcu::s_instance.mysync()),
      guard_(ticker::s_instance)
  {
    sync_->depth_++;
  }

  ~scoped_rcu_base()
  {
    INVARIANT(sync_->depth_);
    const unsigned new_depth = --sync_->depth_;
    guard_.destroy();
    if (new_depth || !DoCleanup)
      return;
    // out of RCU region now, check if we need to run cleaner
    sync_->do_cleanup();
  }

  inline ticker::guard *
  guard()
  {
    return guard_.obj();
  }

  inline rcu::sync *
  sync()
  {
    return sync_;
  }

private:
  rcu::sync *sync_;
  unmanaged<ticker::guard> guard_;
};

typedef scoped_rcu_base<true> scoped_rcu_region;

class disabled_rcu_region {};

#endif /* _RCU_H_ */
