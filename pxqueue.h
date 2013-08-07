#pragma once

#include <iterator>
#include <string>

#include "counter.h"
#include "macros.h"
#include "util.h"

// abstract queue for RCU-like queues

// forward decl
template <typename T, size_t N> struct basic_px_queue;

template <typename T, size_t N>
struct basic_px_group {
  basic_px_group()
    : next_(nullptr), rcu_tick_(0)
  {
    static event_counter evt_px_group_creates(
        util::cxx_typename<T>::value() + std::string("_px_group_creates"));
    ++evt_px_group_creates;
  }
  ~basic_px_group()
  {
    static event_counter evt_px_group_deletes(
        util::cxx_typename<T>::value() + std::string("_px_group_deletes"));
    ++evt_px_group_deletes;
  }

  // no copying/moving
  basic_px_group(const basic_px_group &) = delete;
  basic_px_group &operator=(const basic_px_group &) = delete;
  basic_px_group(basic_px_group &&) = delete;

  static const size_t GroupSize = N;
  friend class basic_px_queue<T, N>;

private:
  basic_px_group *next_;
  typename util::vec<T, GroupSize>::type pxs_;
  uint64_t rcu_tick_; // all elements in pxs_ are from this tick,
                      // this number is only meaningful if pxs_ is
                      // not empty
};

// not thread safe- should guard with lock for concurrent manipulation
template <typename T, size_t N>
struct basic_px_queue {
  basic_px_queue()
    : head_(nullptr), tail_(nullptr),
      freelist_head_(nullptr), freelist_tail_(nullptr),
      ngroups_(0) {}

  typedef basic_px_group<T, N> px_group;

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
    inline iterator_() : px_(nullptr), i_() {}
    inline iterator_(PtrType *px) : px_(px), i_() {}

    // allow iterator to assign to const_iterator
    template <typename P, typename O>
    inline iterator_(const iterator_<P, O> &o) : px_(o.px_), i_(o.i_) {}

    inline ObjType &
    operator*() const
    {
      return px_->pxs_[i_];
    }

    inline ObjType *
    operator->() const
    {
      return &px_->pxs_[i_];
    }

    inline uint64_t
    tick() const
    {
      return px_->rcu_tick_;
    }

    inline bool
    operator==(const iterator_ &o) const
    {
      return px_ == o.px_ && i_ == o.i_;
    }

    inline bool
    operator!=(const iterator_ &o) const
    {
      return !operator==(o);
    }

    inline iterator_ &
    operator++()
    {
      ++i_;
      if (i_ == px_->pxs_.size()) {
        px_ = px_->next_;
        i_ = 0;
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
    PtrType *px_;
    size_t i_;
  };

  typedef iterator_<px_group, T> iterator;
  typedef iterator_<const px_group, const T> const_iterator;

  inline iterator begin() { return iterator(head_); }
  inline const_iterator begin() const { return iterator(head_); }

  inline iterator end() { return iterator(nullptr); }
  inline const_iterator end() const { return iterator(nullptr); }

  // enqueue t in epoch rcu_tick
  // assumption: rcu_ticks can only go up!
  void
  enqueue(const T &t, uint64_t rcu_tick)
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
    g->pxs_.emplace_back(t);
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

  inline bool
  get_latest_epoch(uint64_t &e) const
  {
    if (!tail_)
      return false;
    e = tail_->rcu_tick_;
    return true;
  }

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
