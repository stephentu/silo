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

class rcu {
  friend class scoped_rcu_region;
public:
  typedef uint64_t epoch_t;

  typedef void (*deleter_t)(void *);
  typedef std::pair<void *, deleter_t> delete_entry;

  // forward decl
  template <size_t N>
    struct basic_px_queue;

  static event_counter evt_px_group_creates;
  static event_counter evt_px_group_deletes;

  // templated so we can test on smaller sizes
  template <size_t N>
  struct basic_px_group {
    basic_px_group()
      : next_(nullptr), rcu_tick_(0)
    {
      ++evt_px_group_creates;
    }
    ~basic_px_group()
    {
      ++evt_px_group_deletes;
    }

    // no copying/moving
    basic_px_group(const basic_px_group &) = delete;
    basic_px_group &operator=(const basic_px_group &) = delete;
    basic_px_group(basic_px_group &&) = delete;

    static const size_t GroupSize = N;
    friend class basic_px_queue<N>;

  private:
    basic_px_group *next_;
    typename util::vec<delete_entry, GroupSize>::type pxs_;
    uint64_t rcu_tick_; // all elements in pxs_ are from this tick,
                        // this number is only meaningful if pxs_ is
                        // not empty
  };

  // not thread safe- should guard with lock for concurrent manipulation
  template <size_t N>
  struct basic_px_queue {
    basic_px_queue()
      : head_(nullptr), tail_(nullptr),
        freelist_head_(nullptr), freelist_tail_(nullptr),
        ngroups_(0) {}

    typedef basic_px_group<N> px_group;

    basic_px_queue(basic_px_queue &&) = default;
    basic_px_queue(const basic_px_queue &) = delete;
    basic_px_queue &operator=(const basic_px_queue &) = delete;

    ~basic_px_queue()
    {
      reap_chain(head_);
      reap_chain(freelist_head_);
    }

    void
    swap(basic_px_queue &other)
    {
      std::swap(head_, other.head_);
      std::swap(tail_, other.tail_);
      std::swap(freelist_head_, other.freelist_head_);
      std::swap(freelist_tail_, other.freelist_tail_);
      std::swap(ngroups_, other.ngroups_);
    }

    template <typename PtrType, typename ObjType>
    class iterator_ : public std::iterator<std::forward_iterator_tag, ObjType> {
    public:
      inline iterator_() : px(nullptr), i() {}
      inline iterator_(PtrType *px) : px(px), i() {}

      // allow iterator to assign to const_iterator
      template <typename P, typename O>
      inline iterator_(const iterator_<P, O> &o) : px(o.px), i(o.i) {}

      inline ObjType &
      operator*() const
      {
        return px->pxs_[i];
      }

      inline ObjType *
      operator->() const
      {
        return &px->pxs_[i];
      }

      inline bool
      operator==(const iterator_ &o) const
      {
        return px == o.px && i == o.i;
      }

      inline bool
      operator!=(const iterator_ &o) const
      {
        return !operator==(o);
      }

      inline iterator_ &
      operator++()
      {
        ++i;
        if (i == px->pxs_.size()) {
          px = px->next_;
          i = 0;
        }
        return *this;
      }

      inline iterator_
      operator++(int)
      {
        iterator_ cur = *this;
        ++(*this);
        return cur;
      }

    private:
      PtrType *px;
      size_t i;
    };

    typedef iterator_<px_group, delete_entry> iterator;
    typedef iterator_<const px_group, const delete_entry> const_iterator;

    inline iterator begin() { return iterator(head_); }
    inline const_iterator begin() const { return iterator(head_); }

    inline iterator end() { return iterator(nullptr); }
    inline const_iterator end() const { return iterator(nullptr); }

    // enqueue px to be deleted with fn, where px was deleted in epoch rcu_tick
    // assumption: rcu_ticks can only go up!
    void
    enqueue(void *px, deleter_t fn, uint64_t rcu_tick)
    {
      INVARIANT(bool(head_) == bool(tail_));
      INVARIANT(bool(head_) == bool(ngroups_));
      INVARIANT(!tail_ || tail_->pxs_.size() <= px_group::GroupSize);
      INVARIANT(!tail_ || tail_->rcu_tick_ <= rcu_tick);
      px_group *g;
      if (unlikely(!tail_ ||
                   tail_->pxs_.size() == px_group::GroupSize ||
                   tail_->rcu_tick_ != rcu_tick)) {
        ensure_freelist();
        // pop off freelist
        g = freelist_head_;
        freelist_head_ = g->next_;
        if (g == freelist_tail_)
          freelist_tail_ = nullptr;
        g->next_ = nullptr;
        g->pxs_.clear();
        g->rcu_tick_ = rcu_tick;
        ngroups_++;

        // adjust ptrs
        if (!head_) {
          head_ = tail_ = g;
        } else {
          tail_->next_ = g;
          tail_ = g;
        }
      } else {
        g = tail_;
      }
      INVARIANT(g->pxs_.size() < px_group::GroupSize);
      INVARIANT(g->rcu_tick_ == rcu_tick);
      INVARIANT(!g->next_);
      INVARIANT(tail_ == g);
      g->pxs_.emplace_back(px, fn);
      sanity_check();
    }

    void
    ensure_freelist()
    {
      INVARIANT(bool(freelist_head_) == bool(freelist_tail_));
      if (likely(freelist_head_))
        return;
      const size_t nalloc = 16;
      alloc_freelist(nalloc);
    }

    inline bool
    empty() const
    {
      return !head_;
    }

#ifdef CHECK_INVARIANTS
    void
    sanity_check() const
    {
      INVARIANT(bool(head_) == bool(tail_));
      INVARIANT(!tail_ || ngroups_);
      INVARIANT(!tail_ || !tail_->next_);
      INVARIANT(bool(freelist_head_) == bool(freelist_tail_));
      INVARIANT(!freelist_tail_ || !freelist_tail_->next_);
      px_group *p = head_, *pprev = nullptr;
      size_t n = 0;
      uint64_t prev_tick = 0;
      while (p) {
        INVARIANT(p->pxs_.size());
        INVARIANT(p->pxs_.size() <= px_group::GroupSize);
        INVARIANT(prev_tick <= p->rcu_tick_);
        prev_tick = p->rcu_tick_;
        pprev = p;
        p = p->next_;
        n++;
      }
      INVARIANT(n == ngroups_);
      INVARIANT(!pprev || tail_ == pprev);
      p = freelist_head_;
      pprev = nullptr;
      while (p) {
        pprev = p;
        p = p->next_;
      }
      INVARIANT(!pprev || freelist_tail_ == pprev);
    }
#else
    inline ALWAYS_INLINE void sanity_check() const {}
#endif

    // assumes this instance is EMPTY, accept from source
    // all entries <= e
    inline void
    empty_accept_from(basic_px_queue &source, uint64_t rcu_tick)
    {
      ALWAYS_ASSERT(empty());
      INVARIANT(this != &source);
      INVARIANT(!tail_);
      px_group *p = source.head_, *pnext;
      while (p && p->rcu_tick_ <= rcu_tick) {
        pnext = p->next_;
        p->next_ = nullptr;
        if (!head_) {
          head_ = tail_ = p;
        } else {
          tail_->next_ = p;
          tail_ = p;
        }
        ngroups_++;
        source.ngroups_--;
        source.head_ = p = pnext;
        if (!source.head_)
          source.tail_ = nullptr;
      }
      sanity_check();
      source.sanity_check();
    }

    // transfer *this* elements freelist to dest
    void
    transfer_freelist(basic_px_queue &dest, ssize_t n = -1)
    {
      if (!freelist_head_)
        return;
      if (n < 0) {
        freelist_tail_->next_ = dest.freelist_head_;
        if (!dest.freelist_tail_)
          dest.freelist_tail_ = freelist_tail_;
        dest.freelist_head_ = freelist_head_;
        freelist_head_ = freelist_tail_ = nullptr;
      } else {
        px_group *p = freelist_head_;
        size_t c = 0;
        while (p && c++ < static_cast<size_t>(n)) {
          px_group *tmp = p->next_;
          p->next_ = dest.freelist_head_;
          dest.freelist_head_ = p;
          if (!dest.freelist_tail_)
            dest.freelist_tail_ = p;
          if (p == freelist_tail_)
            freelist_tail_ = nullptr;
          p = tmp;
          freelist_head_ = p;
        }
      }
      sanity_check();
    }

    void
    clear()
    {
      if (!head_)
        return;
      tail_->next_ = freelist_head_;
      if (!freelist_tail_)
        freelist_tail_ = tail_;
      freelist_head_ = head_;
      head_ = tail_ = nullptr;
      ngroups_ = 0;
    }

    // adds n new groups to the freelist
    void
    alloc_freelist(size_t n)
    {
      for (size_t i = 0; i < n; i++) {
        px_group *p = new px_group;
        if (!freelist_tail_)
          freelist_tail_ = p;
        p->next_ = freelist_head_;
        freelist_head_ = p;
      }
    }

    inline size_t get_ngroups() const { return ngroups_; }

  private:
    void
    reap_chain(px_group *px)
    {
      while (px) {
        px_group *tmp = px->next_;
        delete px;
        px = tmp;
      }
    }

    px_group *head_;
    px_group *tail_;
    px_group *freelist_head_;
    px_group *freelist_tail_;
    size_t ngroups_;
  };

  typedef basic_px_queue<4096> px_queue;

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
    get_pin_cpu_() const
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
