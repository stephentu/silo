#ifndef _STR_ARENA_H_
#define _STR_ARENA_H_

#include <string>
#include "../small_vector.h"

// XXX: str arena hardcoded now to handle at most 1024 strings
class str_arena {
public:

  str_arena() : n(0) {}

  // non-copyable/non-movable for the time being
  str_arena(str_arena &&) = delete;
  str_arena(const str_arena &) = delete;
  str_arena &operator=(const str_arena &) = delete;


  inline void
  reset()
  {
    n = 0;
  }

  std::string *
  next()
  {
    if (n < 1024) {
      if (strs.size() == n)
        strs.emplace_back();
      ALWAYS_ASSERT(strs.size() > n);
      return &strs[n++];
    }
    return nullptr;
  }

private:
  small_vector<std::string, 1024> strs;
  size_t n;
};

class scoped_str_arena {
public:
  scoped_str_arena(str_arena *arena)
    : arena(arena)
  {
  }

  scoped_str_arena(str_arena &arena)
    : arena(&arena)
  {
  }

  scoped_str_arena(scoped_str_arena &&) = default;

  // non-copyable
  scoped_str_arena(const scoped_str_arena &) = delete;
  scoped_str_arena &operator=(const scoped_str_arena &) = delete;


  ~scoped_str_arena()
  {
    if (arena)
      arena->reset();
  }

  inline ALWAYS_INLINE str_arena *
  get()
  {
    return arena;
  }

private:
  str_arena *arena;
};

#endif /* _STR_ARENA_H_ */
