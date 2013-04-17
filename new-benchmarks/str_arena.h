#ifndef _STR_ARENA_H_
#define _STR_ARENA_H_

#include <string>
#include "../small_vector.h"

// XXX: str arena hardcoded now to handle at most 1024 strings
class str_arena {
public:

  static const size_t PreAllocBufSize = 256;
  static const size_t NStrs = 1024;

  str_arena()
    : n(0)
  {
    for (size_t i = 0; i < NStrs; i++)
      strs[i].reserve(PreAllocBufSize);
  }

  // non-copyable/non-movable for the time being
  str_arena(str_arena &&) = delete;
  str_arena(const str_arena &) = delete;
  str_arena &operator=(const str_arena &) = delete;

  inline void
  reset()
  {
    n = 0;
  }

  // next() is guaranteed to return an empty string
  std::string *
  next()
  {
    if (n < NStrs) {
      std::string * const px = &strs[n++];
      px->clear();
      INVARIANT(manages(px));
      return px;
    }
    ALWAYS_ASSERT(false); // for now, to catch inefficiencies
    return nullptr;
  }

  inline std::string *
  operator()()
  {
    return next();
  }

  void
  return_last(std::string *px)
  {
    INVARIANT(n > 0);
    INVARIANT(&strs[n - 1] == px);
    --n;
  }

  bool
  manages(const std::string *px) const
  {
    if (px < &strs[0])
      return false;
    if (px >= &strs[NStrs])
      return false;
    return 0 == ((reinterpret_cast<const char *>(px) -
                  reinterpret_cast<const char *>(&strs[0])) % sizeof(std::string));
  }

private:
  std::string strs[NStrs];
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

// wraps a ptr and forwards
class proxy_str_arena {
public:
  proxy_str_arena(str_arena *arena) : arena(arena) {}
  proxy_str_arena(str_arena &arena) : arena(&arena) {}

  inline void
  reset()
  {
    arena->reset();
  }

  std::string *
  next()
  {
    return arena->next();
  }

  inline std::string *
  operator()()
  {
    return next();
  }

  void
  return_last(std::string *px)
  {
    arena->return_last(px);
  }

private:
  str_arena *arena;
};

#endif /* _STR_ARENA_H_ */
