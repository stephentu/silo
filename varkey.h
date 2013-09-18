#ifndef _NDB_VARKEY_H_
#define _NDB_VARKEY_H_

#include <endian.h>
#include <stdint.h>
#include <string.h>

#include <iostream>
#include <string>
#include <type_traits>
#include <limits>

#include "imstring.h"
#include "macros.h"
#include "util.h"

#if NDB_MASSTREE
#include "prefetch.h"
#include "masstree/config.h"
#include "masstree/string_slice.hh"
#endif

class varkey {
  friend std::ostream &operator<<(std::ostream &o, const varkey &k);
public:
  inline varkey() : p(NULL), l(0) {}
  inline varkey(const varkey &that) = default;
  inline varkey(varkey &&that) = default;
  inline varkey &operator=(const varkey &that) = default;

  inline varkey(const uint8_t *p, size_t l)
    : p(p), l(l)
  {
  }

  explicit inline varkey(const std::string &s)
    : p((const uint8_t *) s.data()), l(s.size())
  {
  }

  explicit inline varkey(const char *s)
    : p((const uint8_t *) s), l(strlen(s))
  {
  }

  explicit inline varkey(const imstring &s)
    : p(s.data()), l(s.size())
  {
  }

  inline bool
  operator==(const varkey &that) const
  {
    if (size() != that.size())
      return false;
    return memcmp(data(), that.data(), size()) == 0;
  }

  inline bool
  operator!=(const varkey &that) const
  {
    return !operator==(that);
  }

  inline bool
  operator<(const varkey &that) const
  {
    int r = memcmp(data(), that.data(), std::min(size(), that.size()));
    return r < 0 || (r == 0 && size() < that.size());
  }

  inline bool
  operator>=(const varkey &that) const
  {
    return !operator<(that);
  }

  inline bool
  operator<=(const varkey &that) const
  {
    int r = memcmp(data(), that.data(), std::min(size(), that.size()));
    return r < 0 || (r == 0 && size() <= that.size());
  }

  inline bool
  operator>(const varkey &that) const
  {
    return !operator<=(that);
  }

  inline uint64_t
  slice() const
  {
    uint64_t ret = 0;
    uint8_t *rp = (uint8_t *) &ret;
    for (size_t i = 0; i < std::min(l, size_t(8)); i++)
      rp[i] = p[i];
    return util::host_endian_trfm<uint64_t>()(ret);
  }

#if NDB_MASSTREE
  inline uint64_t slice_at(int pos) const {
    return string_slice<uint64_t>::make_comparable((const char*) p + pos, std::min(int(l - pos), 8));
  }
#endif

  inline varkey
  shift() const
  {
    INVARIANT(l >= 8);
    return varkey(p + 8, l - 8);
  }

  inline varkey
  shift_many(size_t n) const
  {
    INVARIANT(l >= 8 * n);
    return varkey(p + 8 * n, l - 8 * n);
  }

  inline size_t
  size() const
  {
    return l;
  }

  inline int length() const {
    return l;
  }

  inline const uint8_t *
  data() const
  {
    return p;
  }

  inline
  std::string str() const
  {
    return std::string((const char *) p, l);
  }

  inline std::string &
  str(std::string &buf) const
  {
    buf.assign((const char *) p, l);
    return buf;
  }

#if NDB_MASSTREE
  inline operator lcdf::Str() const {
    return lcdf::Str(p, l);
  }
#endif

private:
  const uint8_t *p;
  size_t l;
};

inline std::ostream &
operator<<(std::ostream &o, const varkey &k)
{
  o << util::hexify(k.str());
  return o;
}

template <bool is_signed, typename T>
struct signed_aware_trfm {};

template <typename T>
struct signed_aware_trfm<false, T> {
  inline ALWAYS_INLINE T operator()(T t) const { return t; }
};

template <typename T>
struct signed_aware_trfm<true, T> {
  typedef T signed_type;
  typedef
    typename std::enable_if<std::is_signed<T>::value, typename std::make_unsigned<T>::type>::type
    unsigned_type;
  inline ALWAYS_INLINE unsigned_type
  operator()(signed_type s) const
  {
    const unsigned_type offset = -std::numeric_limits<signed_type>::min();
    const unsigned_type converted = static_cast<unsigned_type>(s) + offset;
    return converted;
  }
};

template <typename T,
          typename EncodingTrfm = signed_aware_trfm<std::is_signed<T>::value, T>,
          typename ByteTrfm = util::big_endian_trfm<T> >
class obj_varkey : public varkey {
public:

  typedef
    typename std::enable_if<std::is_integral<T>::value, T>::type
    integral_type;

  inline obj_varkey() : varkey(), obj() {}

  inline obj_varkey(integral_type t)
    : varkey((const uint8_t *) &obj, sizeof(integral_type)),
      obj(ByteTrfm()(EncodingTrfm()(t)))
  {
  }

private:
  integral_type obj;
};

typedef obj_varkey<uint8_t>  u8_varkey;
typedef obj_varkey<int8_t>   s8_varkey;
typedef obj_varkey<uint16_t> u16_varkey;
typedef obj_varkey<int16_t>  s16_varkey;
typedef obj_varkey<uint32_t> u32_varkey;
typedef obj_varkey<int32_t>  s32_varkey;
typedef obj_varkey<uint64_t> u64_varkey;
typedef obj_varkey<int64_t>  s64_varkey;

#endif /* _NDB_VARKEY_H_ */
