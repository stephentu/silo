#ifndef _KEY_RANGE_H_
#define _KEY_RANGE_H_

#include <string>
#include <vector>
#include <iostream>

#include "macros.h"
#include "varkey.h"
#include "util.h"

// [a, b)
struct key_range_t {
  key_range_t() : a(), has_b(true), b() {}

  key_range_t(const varkey &a) : a(a.str()), has_b(false), b() {}
  key_range_t(const varkey &a, const varkey &b)
    : a(a.str()), has_b(true), b(b.str())
  { }
  key_range_t(const varkey &a, const std::string &b)
    : a(a.str()), has_b(true), b(b)
  { }
  key_range_t(const varkey &a, bool has_b, const varkey &b)
    : a(a.str()), has_b(has_b), b(b.str())
  { }
  key_range_t(const varkey &a, bool has_b, const std::string &b)
    : a(a.str()), has_b(has_b), b(b)
  { }

  key_range_t(const std::string &a) : a(a), has_b(false), b() {}
  key_range_t(const std::string &a, const varkey &b)
    : a(a), has_b(true), b(b.str())
  { }
  key_range_t(const std::string &a, bool has_b, const varkey &b)
    : a(a), has_b(has_b), b(b.str())
  { }

  key_range_t(const std::string &a, const std::string &b)
    : a(a), has_b(true), b(b)
  { }
  key_range_t(const std::string &a, bool has_b, const std::string &b)
    : a(a), has_b(has_b), b(b)
  { }

  std::string a;
  bool has_b; // false indicates infinity, true indicates use b
  std::string b; // has meaning only if !has_b

  inline bool
  is_empty_range() const
  {
    return has_b && a >= b;
  }

  inline bool
  contains(const key_range_t &that) const
  {
    if (a > that.a)
      return false;
    if (!has_b)
      return true;
    if (!that.has_b)
      return false;
    return b >= that.b;
  }

  inline bool
  key_in_range(const varkey &k) const
  {
    return varkey(a) <= k && (!has_b || k < varkey(b));
  }

#ifdef CHECK_INVARIANTS
  static void AssertValidRangeSet(const std::vector<key_range_t> &range_set);
#else
  static inline ALWAYS_INLINE void
  AssertValidRangeSet(const std::vector<key_range_t> &range_set)
  { }
#endif /* CHECK_INVARIANTS */

  static std::string PrintRangeSet(
      const std::vector<key_range_t> &range_set);
};

// NOTE: with this comparator, upper_bound() will return a pointer to the first
// range which has upper bound greater than k (if one exists)- it does not
// guarantee that the range returned has a lower bound <= k
struct key_range_search_less_cmp {
  inline bool
  operator()(const varkey &k, const key_range_t &range) const
  {
    return !range.has_b || k < varkey(range.b);
  }
};

static inline std::ostream &
operator<<(std::ostream &o, const key_range_t &range)
{
  o << "[" << util::hexify(range.a) << ", ";
  if (range.has_b)
    o << util::hexify(range.b);
  else
    o << "+inf";
  o << ")";
  return o;
}

#endif /* _KEY_RANGE_H_ */
