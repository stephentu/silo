#ifndef _COUNTER_H_
#define _COUNTER_H_

// system event counters, for

#include <vector>
#include <map>
#include <string>
#include <stdint.h>

#include "macros.h"
#include "core.h"
#include "util.h"

class event_counter : private util::noncopyable {
private:

  // these objects are *never* supposed to be destructed
  // (this is a purposeful memory leak)
  struct event_ctx : private util::noncopyable {
    event_ctx(const std::string &name)
      : name(name)
    {}

    ~event_ctx()
    {
      ALWAYS_ASSERT(false);
    }

    const std::string name;

    //// global count
    //volatile util::aligned_padded_u64 global_count;

    // per-thread counts
    volatile util::aligned_padded_u64 tl_counts[coreid::NMaxCores];
  };

  static std::vector<event_ctx *> &event_counters();
  static pthread_spinlock_t &event_counters_lock();

public:
  event_counter(const std::string &name);

  inline event_counter &
  operator++()
  {
    const size_t id = coreid::core_id();
    ctx->tl_counts[id].elem++;
    return *this;
  }

  inline event_counter &
  operator+=(uint64_t i)
  {
    const size_t id = coreid::core_id();
    ctx->tl_counts[id].elem += i;
    return *this;
  }

  // WARNING: an expensive operation!
  static std::map<std::string, uint64_t> get_all_counters();

private:
  event_ctx *const ctx;
};

#endif /* _COUNTER_H_ */
