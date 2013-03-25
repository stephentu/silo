// taken from xv6

#pragma once

#include <cstddef>

// Return ceil(log2(x)).
static inline std::size_t
ceil_log2(std::size_t x)
{
  auto bits = sizeof(long long) * 8 - __builtin_clzll(x);
  if (x == (std::size_t)1 << (bits - 1))
    return bits - 1;
  return bits;
}

// Return ceil(log2(x)).  This is slow, but can be evaluated in a
// constexpr context.  'exact' is used internally and should not be
// provided by the caller.
static inline constexpr std::size_t
ceil_log2_const(std::size_t x, bool exact = true)
{
  return (x == 0) ? (1/x)
    : (x == 1) ? (exact ? 0 : 1)
    : 1 + ceil_log2_const(x >> 1, ((x & 1) == 1) ? false : exact);
}

// Round up to the nearest power of 2
static inline std::size_t
round_up_to_pow2(std::size_t x)
{
  auto bits = sizeof(long long) * 8 - __builtin_clzll(x);
  if (x == (std::size_t)1 << (bits - 1))
    return x;
  return (std::size_t)1 << bits;
}

static inline constexpr std::size_t
round_up_to_pow2_const(std::size_t x)
{
  return (std::size_t)1 << ceil_log2_const(x);
}
