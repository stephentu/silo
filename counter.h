#ifndef _COUNTER_H_
#define _COUNTER_H_

// system event counters, for

#include <algorithm> // for std::max
#include <vector>
#include <map>
#include <string>
#include <stdint.h>

#include "macros.h"
#include "core.h"
#include "util.h"
#include "spinlock.h"

struct counter_data {
  enum Type { TYPE_COUNT, TYPE_AGG };

  counter_data()
    : type_(TYPE_COUNT), count_(0), sum_(0), max_(0) {}

  Type type_;
  uint64_t count_;
  uint64_t sum_;
  uint64_t max_;

  inline counter_data &
  operator+=(const counter_data &that)
  {
    count_ += that.count_;
    sum_   += that.sum_;
    max_    = std::max(max_, that.max_);
    return *this;
  }

  inline double
  avg() const
  {
    INVARIANT(type_ == TYPE_AGG);
    return double(sum_)/double(count_);
  }
};

namespace private_ {

  // these objects are *never* supposed to be destructed
  // (this is a purposeful memory leak)
  struct event_ctx {

    static std::map<std::string, event_ctx *> &event_counters();
    static spinlock &event_counters_lock();

    // tag to avoid making event_ctx virtual
    event_ctx(const std::string &name, bool avg_tag)
      : name_(name), avg_tag_(avg_tag)
    {}

    ~event_ctx()
    {
      ALWAYS_ASSERT(false);
    }

    event_ctx(const event_ctx &) = delete;
    event_ctx &operator=(const event_ctx &) = delete;
    event_ctx(event_ctx &&) = delete;

    void stat(counter_data &d);

    const std::string name_;
    const bool avg_tag_;

    // per-thread counts
    percore<uint64_t, false, false> counts_;
  };

  // more expensive
  struct event_ctx_avg : public event_ctx {
    event_ctx_avg(const std::string &name) : event_ctx(name, true) {}
    percore<uint64_t, false, false> sums_;
    percore<uint64_t, false, false> highs_;
  };
}

class event_counter {
public:
  event_counter(const std::string &name);

  event_counter(const event_counter &) = delete;
  event_counter &operator=(const event_counter &) = delete;
  event_counter(event_counter &&) = delete;

  inline ALWAYS_INLINE void
  inc(uint64_t i = 1)
  {
#ifdef ENABLE_EVENT_COUNTERS
    ctx_->counts_.my() += i;
#endif
  }

  inline ALWAYS_INLINE event_counter &
  operator++()
  {
    inc();
    return *this;
  }

  inline ALWAYS_INLINE event_counter &
  operator+=(uint64_t i)
  {
    inc(i);
    return *this;
  }

  // WARNING: an expensive operation!
  static std::map<std::string, counter_data> get_all_counters();
  // WARNING: an expensive operation!
  static void reset_all_counters();
  // WARNING: an expensive operation!
  static bool
  stat(const std::string &name, counter_data &d);

private:
#ifdef ENABLE_EVENT_COUNTERS
  unmanaged<private_::event_ctx> ctx_;
#endif
};

class event_avg_counter {
public:
  event_avg_counter(const std::string &name);

  event_avg_counter(const event_avg_counter &) = delete;
  event_avg_counter &operator=(const event_avg_counter &) = delete;
  event_avg_counter(event_avg_counter &&) = delete;

  inline ALWAYS_INLINE void
  offer(uint64_t value)
  {
#ifdef ENABLE_EVENT_COUNTERS
    ctx_->counts_.my()++;
    ctx_->sums_.my() += value;
    ctx_->highs_.my() = std::max(ctx_->highs_.my(), value);
#endif
  }

private:
#ifdef ENABLE_EVENT_COUNTERS
  unmanaged<private_::event_ctx_avg> ctx_;
#endif
};

inline std::ostream &
operator<<(std::ostream &o, const counter_data &d)
{
  if (d.type_ == counter_data::TYPE_COUNT)
    o << "count=" << d.count_;
  else
    o << "count=" << d.count_ << ", max=" << d.max_ << ", avg=" << d.avg();
  return o;
}

#endif /* _COUNTER_H_ */
