#pragma once

#include <cstdint>
#include "../macros.h"
#include "../counter.h"
#include "../util.h"

// cursors can only move forward, or reset completely
template <typename T>
struct read_record_cursor {
public:
  typedef typename T::value value_type;
  typedef typename T::value_descriptor value_descriptor_type;

  // [px, px+nbytes) is assumed to be valid memory
  read_record_cursor(const uint8_t *px, size_t nbytes)
    : px_begin(px), nbytes(nbytes), px_cur(px), n(0) {}

  // returns true on success, false on fail
  inline bool
  skip_to(size_t i)
  {
    INVARIANT(i >= n);
    INVARIANT(i < value_descriptor_type::nfields());
    while (n < i) {
      const size_t sz = value_descriptor_type::failsafe_skip_fn(n++)(px_cur, nbytes, nullptr);
      if (unlikely(!sz))
        return false;
      px_cur += sz;
      nbytes -= sz;
    }
    return true;
  }

  inline void
  reset()
  {
    INVARIANT(px_cur >= px_begin);
    nbytes += (px_cur - px_begin);
    px_cur = px_begin;
    n = 0;
  }

  inline size_t
  field() const
  {
    return n;
  }

  // returns 0 on failure
  inline size_t
  read_current_and_advance(value_type *v)
  {
    INVARIANT(n < value_descriptor_type::nfields());
    uint8_t * const buf = reinterpret_cast<uint8_t *>(v) +
      value_descriptor_type::cstruct_offsetof(n);
    const uint8_t * const px_skip =
      value_descriptor_type::failsafe_read_fn(n++)(px_cur, nbytes, buf);
    if (unlikely(!px_skip))
      return 0;
    const size_t rawsz = px_skip - px_cur;
    INVARIANT(rawsz <= value_descriptor_type::max_nbytes(n - 1));
    nbytes -= rawsz;
    px_cur = px_skip;
    return rawsz;
  }

  // returns 0 on failure
  inline size_t
  read_current_raw_size_and_advance()
  {
    INVARIANT(n < value_descriptor_type::nfields());
    const size_t rawsz =
      value_descriptor_type::failsafe_skip_fn(n++)(px_cur, nbytes, nullptr);
    if (unlikely(!nbytes))
      return 0;
    INVARIANT(rawsz <= value_descriptor_type::max_nbytes(n - 1));
    nbytes -= rawsz;
    px_cur += rawsz;
    return rawsz;
  }

private:
  const uint8_t *px_begin;

  // the 3 fields below are kept in sync:
  // [px_cur, px_cur+nbytes) is valid memory
  // px_cur points to the start of the n-th field
  size_t nbytes;
  const uint8_t *px_cur;
  size_t n; // current field position in cursor
};


template <typename T>
struct write_record_cursor {
public:
  typedef typename T::value value_type;
  typedef typename T::value_descriptor value_descriptor_type;

  write_record_cursor(uint8_t *px)
    : px_begin(px), px_cur(px), px_end(nullptr), n(0)
  {
    INVARIANT(px);
  }

  inline void
  skip_to(size_t i)
  {
    INVARIANT(i >= n);
    INVARIANT(i < value_descriptor_type::nfields());
    while (n < i)
      px_cur += value_descriptor_type::skip_fn(n++)(px_cur, nullptr);
  }

  inline void
  reset()
  {
    px_cur = px_begin;
    n = 0;
  }

  inline size_t
  field() const
  {
    return n;
  }

  inline void
  write_current_and_advance(const value_type *v, uint8_t *old_v = nullptr)
  {
    static event_counter evt_write_memmove(
        util::cxx_typename<T>::value() + std::string("_write_memmove"));
    const uint8_t * const buf = reinterpret_cast<const uint8_t *>(v) +
      value_descriptor_type::cstruct_offsetof(n);
    const size_t newsz = value_descriptor_type::nbytes_fn(n)(buf);
    INVARIANT(newsz <= value_descriptor_type::max_nbytes(n));
    uint8_t stack_buf[value_descriptor_type::max_nbytes(n)];
    uint8_t * const old_buf = old_v ? old_v : &stack_buf[0];
    const size_t oldsz = value_descriptor_type::skip_fn(n)(px_cur, old_buf);
    INVARIANT(oldsz <= value_descriptor_type::max_nbytes(n));
    if (unlikely(oldsz != newsz)) {
      ++evt_write_memmove;
      compute_end();
      memmove(px_cur + newsz, px_cur + oldsz, px_end - px_cur - oldsz);
      if (oldsz > newsz)
        // shrink
        px_end -= (oldsz - newsz);
      else
        // grow
        px_end += (newsz - oldsz);
    }
    px_cur = value_descriptor_type::write_fn(n++)(px_cur, buf);
  }

private:

  inline void
  compute_end()
  {
    if (px_end)
      return;
    uint8_t *px = px_cur;
    size_t i = n;
    while (i < value_descriptor_type::nfields())
      px += value_descriptor_type::skip_fn(i++)(px, nullptr);
    px_end = px;
  }

  uint8_t *px_begin;
  uint8_t *px_cur;
  uint8_t *px_end;
  size_t n; // current field position in cursor
};
