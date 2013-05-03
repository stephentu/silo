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
    if (likely(n < NStrs)) {
      std::string * const px = &strs[n++];
      px->clear();
      INVARIANT(manages(px));
      return px;
    }
    // only loaders need this- and this allows us to use a unified
    // str_arena for loaders/workers
    overflow.emplace_back(new std::string);
    ++n;
    return overflow.back();
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
    --n;
  }

  bool
  manages(const std::string *px) const
  {
    return manages_local(px) || manages_overflow(px);
  }

private:

  bool
  manages_local(const std::string *px) const
  {
    if (px < &strs[0])
      return false;
    if (px >= &strs[NStrs])
      return false;
    return 0 == ((reinterpret_cast<const char *>(px) -
                  reinterpret_cast<const char *>(&strs[0])) % sizeof(std::string));
  }

  bool
  manages_overflow(const std::string *px) const
  {
    return std::find(overflow.begin(), overflow.end(), px) != overflow.end();
  }

private:
  std::string strs[NStrs];
  std::vector<std::string *> overflow;
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
