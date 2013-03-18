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
    epoch_t epoch;
    basic_px_group *next;
    typename util::vec<delete_entry, GroupSize>::type pxs;
  };

  template <size_t N>
  struct basic_px_queue {
    basic_px_queue()
      : head(nullptr), tail(nullptr),
        freelist_head(nullptr), freelist_tail(nullptr) {}

    typedef basic_px_group<N> px_group;

    basic_px_queue(basic_px_queue &&) = default; // for swap
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
    enqueue(epoch_t epoch, void *px, deleter_t fn)
    {
      INVARIANT(!tail || tail->epoch <= epoch);
      px_group *g;
      if (unlikely(!tail || tail->epoch != epoch ||
                   tail->pxs.size() == px_group::GroupSize)) {

        INVARIANT(bool(head) == bool(tail));
        // XXX: breaks with wrap around
        INVARIANT(!tail || tail->epoch <= epoch);
        INVARIANT(!tail || tail->pxs.size() <= px_group::GroupSize);
        ensure_freelist();

        // pop off freelist
        g = freelist_head;
        freelist_head = g->next;
        if (g == freelist_tail)
          freelist_tail = nullptr;
        g->epoch = epoch;
        g->next = nullptr;
        g->pxs.clear();

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

      INVARIANT(g->epoch == epoch);
      INVARIANT(g->pxs.size() < px_group::GroupSize);
      INVARIANT(!g->next);
      g->pxs.emplace_back(px, fn);
    }

    void
    ensure_freelist()
    {
      INVARIANT(bool(freelist_head) == bool(freelist_tail));
      if (likely(freelist_head))
        return;
      const size_t nalloc = 16;
      for (size_t i = 0; i < nalloc; i++) {
        px_group *p = new px_group;
        if (!freelist_tail)
          freelist_tail = p;
        p->next = freelist_head;
        freelist_head = p;
      }
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
      INVARIANT(!tail || !tail->next);
      INVARIANT(bool(freelist_head) == bool(freelist_tail));
      INVARIANT(!freelist_tail || !freelist_tail->next);
      px_group *p = head, *pprev = nullptr;
      epoch_t e = 0;
      while (p) {
        INVARIANT(p->pxs.size());
        INVARIANT(p->pxs.size() <= px_group::GroupSize);
        INVARIANT(p->epoch >= e);
        e = p->epoch;
        pprev = p;
        p = p->next;
      }
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

    // returns true if all epochs on list are > e, false otherwise
    bool
    all_epochs_past(epoch_t e) const
    {
      return !head || head->epoch > e;
    }

    // true if all epochs on list are < e, false otherwise
    bool
    all_epochs_before(epoch_t e) const
    {
      return !tail || tail->epoch < e;
    }

    // transfer all px_groups *from* the input source to this instance
    // which are <= the input epoch e
    size_t
    accept_from(basic_px_queue &source, epoch_t e)
    {
      size_t ret = 0;
      px_group *dest_px = head, **dest_ppx = &head;
      px_group *source_px = source.head, **source_ppx = &source.head;
      while (source_px && source_px->epoch <= e) {
        while (dest_px && dest_px->epoch <= source_px->epoch) {
          dest_ppx = &dest_px->next;
          dest_px = dest_px->next;
        }
        if (source_px == source.tail)
          source.tail = nullptr;
        if (!dest_px)
          tail = source_px;
        *dest_ppx = source_px;
        dest_ppx = &source_px->next;
        *source_ppx = source_px->next;
        source_px->next = dest_px;
        source_px = *source_ppx;
        ret++;
      }
      sanity_check();
      source.sanity_check();
      INVARIANT(source.all_epochs_past(e));
      return ret;
    }

    inline void
    accept_from(basic_px_queue &source)
    {
      // various fast paths
      if (!source.head)
        return;
      if (!tail) {
        std::swap(head, source.head);
        std::swap(tail, source.tail);
        return;
      }
      if (tail->epoch <= source.head->epoch) {
        tail->next = source.head;
        tail = source.tail;
        source.head = source.tail = nullptr;
        return;
      }
      accept_from(source, std::numeric_limits<epoch_t>::max());
    }

    // transfer *this* elements freelist to dest
    void
    transfer_freelist(basic_px_queue &dest)
    {
      if (!freelist_head)
        return;
      freelist_tail->next = dest.freelist_head;
      if (!dest.freelist_tail)
        dest.freelist_tail = freelist_tail;
      dest.freelist_head = freelist_head;
      freelist_head = freelist_tail = nullptr;
    }

    // dequeue all elements <= e
    void
    dequeue_all(epoch_t e)
    {
      px_group *px = head, **ppx = &head;
      while (px && px->epoch <= e) {
        if (px == tail)
          tail = nullptr;
        *ppx = px->next;
        px->next = freelist_head;
        if (!freelist_tail)
          freelist_tail = px;
        freelist_head = px;
        px = *ppx;
      }
      INVARIANT(all_epochs_past(e));
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
    }

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
  static const bool EnableThreadLocalCleanup = false;
  static const uint64_t EpochTimeUsec = 10 * 1000; /* 10 ms */
  static const uint64_t EpochTimeNsec = EpochTimeUsec * 1000;

  // all RCU threads interact w/ the RCU subsystem via
  // a sync struct
  struct sync {
    volatile epoch_t local_epoch CACHE_ALIGNED;
    spinlock local_critical_mutex;
    px_queue local_queue;
    px_queue scratch_queue;
    sync(epoch_t local_epoch)
      : local_epoch(local_epoch)
    {
    }
  };

  /**
   * precondition: p must not already be registered - caller is
   * responsible for managing memory of s
   */
  static sync *register_sync(pthread_t p);

  static sync *unregister_sync(pthread_t p);

  static void enable();

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

  static void region_end();

  static bool in_rcu_region();

private:
  static void *gc_thread_loop(void *p);
  static spinlock &rcu_mutex();

  static volatile epoch_t global_epoch CACHE_ALIGNED;
  static volatile epoch_t cleaning_epoch;

  static px_queue global_queue;

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
  inline scoped_rcu_region()
  {
    rcu::region_begin();
  }

  inline ~scoped_rcu_region()
  {
    rcu::region_end();
  }
};

#endif /* _RCU_H_ */
