#ifndef _RCU_H_
#define _RCU_H_

#include <stdint.h>
#include <pthread.h>

#include <map>
#include <vector>
#include <list>
#include <utility>

#include "counter.h"
#include "spinlock.h"
#include "util.h"

class rcu {
  friend class gc_reaper_thread;
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
    inline basic_px_group()
    {
      ++evt_px_group_creates;
    }
    inline ~basic_px_group()
    {
      ++evt_px_group_deletes;
    }
    basic_px_group(const basic_px_group &) = delete;
    basic_px_group &operator=(const basic_px_group &) = delete;
    static const size_t GroupSize = N;
    friend class basic_px_queue<N>;
  private:
    basic_px_group *next;
    typename util::vec<delete_entry, GroupSize>::type pxs;
  };

  template <size_t N>
  struct basic_px_queue {
    basic_px_queue()
      : head(nullptr), tail(nullptr),
        freelist_head(nullptr), freelist_tail(nullptr),
        ngroups(0) {}

    typedef basic_px_group<N> px_group;

    basic_px_queue(basic_px_queue &&) = default;
    basic_px_queue(const basic_px_queue &) = delete;
    basic_px_queue &operator=(const basic_px_queue &) = delete;

    ~basic_px_queue()
    {
      reap_chain(head);
      reap_chain(freelist_head);
    }

    void
    swap(basic_px_queue &other)
    {
      std::swap(head, other.head);
      std::swap(tail, other.tail);
      std::swap(freelist_head, other.freelist_head);
      std::swap(freelist_tail, other.freelist_tail);
      std::swap(ngroups, other.ngroups);
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
        return px->pxs[i];
      }

      inline ObjType *
      operator->() const
      {
        return &px->pxs[i];
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
        if (i == px->pxs.size()) {
          px = px->next;
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

    inline iterator begin() { return iterator(head); }
    inline const_iterator begin() const { return iterator(head); }

    inline iterator end() { return iterator(nullptr); }
    inline const_iterator end() const { return iterator(nullptr); }

    // precondition: cannot enqueue an epoch number less than the latest
    inline void
    enqueue(void *px, deleter_t fn)
    {
      INVARIANT(bool(head) == bool(tail));
      INVARIANT(bool(head) == bool(ngroups));
      INVARIANT(!tail || tail->pxs.size() <= px_group::GroupSize);
      px_group *g;
      if (unlikely(!tail || tail->pxs.size() == px_group::GroupSize)) {
        ensure_freelist();
        // pop off freelist
        g = freelist_head;
        freelist_head = g->next;
        if (g == freelist_tail)
          freelist_tail = nullptr;
        g->next = nullptr;
        g->pxs.clear();
        ngroups++;

        // adjust ptrs
        if (!head) {
          head = tail = g;
        } else {
          tail->next = g;
          tail = g;
        }
      } else {
        g = tail;
      }
      INVARIANT(g->pxs.size() < px_group::GroupSize);
      INVARIANT(!g->next);
      INVARIANT(tail == g);
      g->pxs.emplace_back(px, fn);
      sanity_check();
    }

    void
    ensure_freelist()
    {
      INVARIANT(bool(freelist_head) == bool(freelist_tail));
      if (likely(freelist_head))
        return;
      const size_t nalloc = 16;
      alloc_freelist(nalloc);
    }

    inline bool
    empty() const
    {
      return !head;
    }

#ifdef CHECK_INVARIANTS
    void
    sanity_check() const
    {
      INVARIANT(bool(head) == bool(tail));
      INVARIANT(!tail || ngroups);
      INVARIANT(!tail || !tail->next);
      INVARIANT(bool(freelist_head) == bool(freelist_tail));
      INVARIANT(!freelist_tail || !freelist_tail->next);
      px_group *p = head, *pprev = nullptr;
      size_t n = 0;
      while (p) {
        INVARIANT(p->pxs.size());
        INVARIANT(p->pxs.size() <= px_group::GroupSize);
        pprev = p;
        p = p->next;
        n++;
      }
      INVARIANT(n == ngroups);
      INVARIANT(!pprev || tail == pprev);
      p = freelist_head;
      pprev = nullptr;
      while (p) {
        pprev = p;
        p = p->next;
      }
      INVARIANT(!pprev || freelist_tail == pprev);
    }
#else
    inline ALWAYS_INLINE void sanity_check() const {}
#endif

    // returns the number of groups xfered
    inline size_t
    accept_from(basic_px_queue &source)
    {
      INVARIANT(this != &source);
      if (!source.head)
        return 0;
      if (!tail) {
        std::swap(head, source.head);
        std::swap(tail, source.tail);
        std::swap(ngroups, source.ngroups);
        return ngroups;
      }
      INVARIANT(!tail->next);
      tail->next = source.head;
      tail = source.tail;
      const size_t ret = source.ngroups;
      ngroups += ret;
      source.head = source.tail = nullptr;
      source.ngroups = 0;
      sanity_check();
      source.sanity_check();
      return ret;
    }

    // transfer *this* elements freelist to dest
    void
    transfer_freelist(basic_px_queue &dest, ssize_t n = -1)
    {
      if (!freelist_head)
        return;
      if (n < 0) {
        freelist_tail->next = dest.freelist_head;
        if (!dest.freelist_tail)
          dest.freelist_tail = freelist_tail;
        dest.freelist_head = freelist_head;
        freelist_head = freelist_tail = nullptr;
      } else {
        px_group *p = freelist_head;
        size_t c = 0;
        while (p && c++ < static_cast<size_t>(n)) {
          px_group *tmp = p->next;
          p->next = dest.freelist_head;
          dest.freelist_head = p;
          if (!dest.freelist_tail)
            dest.freelist_tail = p;
          if (p == freelist_tail)
            freelist_tail = nullptr;
          p = tmp;
          freelist_head = p;
        }
      }
      sanity_check();
    }

    void
    clear()
    {
      if (!head)
        return;
      tail->next = freelist_head;
      if (!freelist_tail)
        freelist_tail = tail;
      freelist_head = head;
      head = tail = nullptr;
      ngroups = 0;
    }

    // adds n new groups to the freelist
    void
    alloc_freelist(size_t n)
    {
      for (size_t i = 0; i < n; i++) {
        px_group *p = new px_group;
        if (!freelist_tail)
          freelist_tail = p;
        p->next = freelist_head;
        freelist_head = p;
      }
    }

    inline size_t get_ngroups() const { return ngroups; }

  private:
    void
    reap_chain(px_group *px)
    {
      while (px) {
        px_group *tmp = px->next;
        delete px;
        px = tmp;
      }
    }

    px_group *head;
    px_group *tail;
    px_group *freelist_head;
    px_group *freelist_tail;
    size_t ngroups;
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
  static const uint64_t EpochTimeUsec = 10 * 1000; /* 10 ms */
  static const uint64_t EpochTimeNsec = EpochTimeUsec * 1000;
  static const size_t NQueueGroups = 32;

  // all RCU threads interact w/ the RCU subsystem via
  // a sync struct
  struct sync {
    volatile epoch_t local_epoch CACHE_ALIGNED;
    spinlock local_critical_mutex;
    px_queue local_queues[2]; // XXX: cache align?
    px_queue scratch_queue;
    sync(epoch_t local_epoch)
      : local_epoch(local_epoch)
    {
      local_queues[0].alloc_freelist(NQueueGroups);
      local_queues[1].alloc_freelist(NQueueGroups);
    }
  };

  /**
   * precondition: p must not already be registered - caller is
   * responsible for managing memory of s
   */
  static sync *register_sync(pthread_t p);

  static sync *unregister_sync(pthread_t p);

  static inline ALWAYS_INLINE void
  enable()
  {
    if (likely(gc_thread_started))
      return;
    enable_slowpath();
  }

  static void region_begin();

  static void free_with_fn(void *p, deleter_t fn);

  template <typename T>
  static inline void
  free(T *p)
  {
    free_with_fn(p, deleter<T>);
  }

  template <typename T>
  static inline void
  free_array(T *p)
  {
    free_with_fn(p, deleter_array<T>);
  }

  static void region_end(bool do_tl_cleanup = false);

  static bool in_rcu_region();

private:

  static void enable_slowpath();

  static void *gc_thread_loop(void *p);
  static spinlock &rcu_mutex();

  static volatile epoch_t global_epoch CACHE_ALIGNED;
  static volatile epoch_t cleaning_epoch;

  static px_queue global_queues[2]; // XXX: cache align?

  static volatile bool gc_thread_started CACHE_ALIGNED;
  static pthread_t gc_thread_p;

  static std::map<pthread_t, sync *> sync_map; // protected by rcu_mutex

  static __thread sync *tl_sync;
  static __thread unsigned int tl_crit_section_depth;
};

/**
 * Use by data structures which use RCU
 *
 * XXX(stephentu): we are doing a terrible job of annotating all the
 * data structures which use RCU now
 */
class rcu_enabled {
public:
  inline rcu_enabled()
  {
    rcu::enable();
  }
};

class scoped_rcu_region {
public:

  // movable, but not copy-constructable
  scoped_rcu_region(scoped_rcu_region &&) = default;
  scoped_rcu_region(const scoped_rcu_region &) = delete;
  scoped_rcu_region &operator=(const scoped_rcu_region &) = delete;

  inline scoped_rcu_region(bool do_tl_cleanup = true)
    : do_tl_cleanup(do_tl_cleanup)
  {
    rcu::region_begin();
  }

  inline ~scoped_rcu_region()
  {
    rcu::region_end(do_tl_cleanup);
  }

private:
  bool do_tl_cleanup;
};

#endif /* _RCU_H_ */
