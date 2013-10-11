#ifndef _NDB_BENCH_ENCODER_H_
#define _NDB_BENCH_ENCODER_H_

#include <string>
#include <stdint.h>
#include "serializer.h"
#include "../util.h"
#include "../ndb_type_traits.h"

#if NDB_MASSTREE
#include "../masstree/str.hh"
#endif

// the C preprocessor is absolutely wonderful...

template <typename T> struct encoder {};

template <typename T>
static inline std::string
Encode(const T &t)
{
  const encoder<T> enc;
  return enc.write(&t);
}

template <typename T>
static inline const char *
Encode(uint8_t *buf, const T &t)
{
  const encoder<T> enc;
  return (const char *) enc.write(buf, &t);
}

template <typename T>
static inline std::string &
Encode(std::string &buf, const T &t)
{
  const encoder<T> enc;
  return enc.write(buf, &t);
}

template <typename T>
static inline const T *
Decode(const std::string &buf, T &obj)
{
  const encoder<T> enc;
  return enc.read(buf.data(), &obj);
}

template <typename T>
static inline const T *
Decode(const char *buf, T &obj)
{
  const encoder<T> enc;
  return enc.read(buf, &obj);
}

template <typename T>
static inline const T *
PrefixDecode(const std::string &buf, T &obj, size_t prefix)
{
  const encoder<T> enc;
  return enc.prefix_read(buf.data(), &obj, prefix);
}

template <typename T>
static inline const T *
PrefixDecode(const char *buf, T &obj, size_t prefix)
{
  const encoder<T> enc;
  return enc.read(buf, &obj, prefix);
}

template <typename T>
static inline size_t
Size(const T &t)
{
  const encoder<T> enc;
  return enc.nbytes(&t);
}

#define IDENT_TRANSFORM(tpe, expr) (expr)
#define HOST_TO_BIG_TRANSFORM(tpe, expr) (util::host_endian_trfm< tpe >()(expr))
#define BIG_TO_HOST_TRANSFORM(tpe, expr) (util::big_endian_trfm< tpe >()(expr))

#define STRUCT_LAYOUT_X(tpe, name) \
  tpe name;

#define STRUCT_EQ_X(tpe, name) \
  if (this->name != other.name) \
    return false;

#define STRUCT_PARAM_FIRST_X(tpe, name) \
  typename private_::typeutil< tpe >::func_param_type name

#define STRUCT_PARAM_REST_X(tpe, name) \
  , typename private_::typeutil< tpe >::func_param_type name

#define STRUCT_INITLIST_FIRST_X(tpe, name) \
  name(name)

#define STRUCT_INITLIST_REST_X(tpe, name) \
  , name(name)

#define STRUCT_PRINTER_FIRST_X(tpe, name) \
  #name << "=" << obj.name

#define STRUCT_PRINTER_REST_X(tpe, name) \
  << ", " << #name << "=" << obj.name

#define STRUCT_FIELDPOS_X(tpe, name) \
  name ## _field,

#define SERIALIZE_WRITE_FIELD(tpe, name, compress, trfm) \
  do { \
    buf = serializer< tpe, compress >::write(buf, trfm(tpe, obj->name)); \
  } while (0);

#define SERIALIZE_READ_FIELD(tpe, name, compress, trfm) \
  do { \
    buf = serializer< tpe, compress >::read(buf, &obj->name); \
    obj->name = trfm(tpe, obj->name); \
  } while (0);

#define SERIALIZE_PREFIX_READ_FIELD(tpe, name, compress, trfm) \
  do { \
    buf = serializer< tpe, compress >::read(buf, &obj->name); \
    obj->name = trfm(tpe, obj->name); \
    if (++i >= prefix) \
      return; \
  } while (0);

#define SERIALIZE_FAILSAFE_READ_FIELD(tpe, name, compress, trfm) \
  do { \
    const uint8_t * const p = \
      serializer< tpe, compress >::failsafe_read(buf, nbytes, &obj->name); \
    if (unlikely(!p)) \
      return false; \
    nbytes -= (p - buf); \
    buf = p; \
    obj->name = trfm(tpe, obj->name); \
  } while (0);

#define SERIALIZE_NBYTES_FIELD(tpe, name, compress) \
  do { \
    size += serializer< tpe, compress >::nbytes(&obj->name); \
  } while (0);

#define SERIALIZE_MAX_NBYTES_KEY_FIELD_X(tpe, name) \
  serializer< tpe, false >::max_nbytes()
#define SERIALIZE_MAX_NBYTES_KEY_FIELD_Y(tpe, name) \
  + serializer< tpe, false >::max_nbytes()

#define SERIALIZE_MAX_NBYTES_VALUE_FIELD_X(tpe, name) \
  serializer< tpe, true >::max_nbytes()
#define SERIALIZE_MAX_NBYTES_VALUE_FIELD_Y(tpe, name) \
  + serializer< tpe, true >::max_nbytes()

#define SERIALIZE_MAX_NBYTES_PREFIX_KEY_FIELD_X(tpe, name) \
  do { \
    ret += serializer< tpe, false >::max_nbytes(); \
    if (++i >= nfields) \
      return ret; \
  } while (0);

#define SERIALIZE_MAX_NBYTES_PREFIX_VALUE_FIELD_X(tpe, name) \
  do { \
    ret += serializer< tpe, true >::max_nbytes(); \
    if (++i >= nfields) \
      return ret; \
  } while (0);

#define SERIALIZE_WRITE_KEY_FIELD_X(tpe, name) \
  SERIALIZE_WRITE_FIELD(tpe, name, false, HOST_TO_BIG_TRANSFORM)
#define SERIALIZE_WRITE_VALUE_FIELD_X(tpe, name) \
  SERIALIZE_WRITE_FIELD(tpe, name, true, IDENT_TRANSFORM)

#define SERIALIZE_READ_KEY_FIELD_X(tpe, name) \
  SERIALIZE_READ_FIELD(tpe, name, false, BIG_TO_HOST_TRANSFORM)
#define SERIALIZE_READ_VALUE_FIELD_X(tpe, name) \
  SERIALIZE_READ_FIELD(tpe, name, true, IDENT_TRANSFORM)

#define SERIALIZE_PREFIX_READ_KEY_FIELD_X(tpe, name) \
  SERIALIZE_PREFIX_READ_FIELD(tpe, name, false, BIG_TO_HOST_TRANSFORM)
#define SERIALIZE_PREFIX_READ_VALUE_FIELD_X(tpe, name) \
  SERIALIZE_PREFIX_READ_FIELD(tpe, name, true, IDENT_TRANSFORM)

#define SERIALIZE_FAILSAFE_READ_KEY_FIELD_X(tpe, name) \
  SERIALIZE_FAILSAFE_READ_FIELD(tpe, name, false, BIG_TO_HOST_TRANSFORM)
#define SERIALIZE_FAILSAFE_READ_VALUE_FIELD_X(tpe, name) \
  SERIALIZE_FAILSAFE_READ_FIELD(tpe, name, true, IDENT_TRANSFORM)

#define SERIALIZE_NBYTES_KEY_FIELD_X(tpe, name) \
  SERIALIZE_NBYTES_FIELD(tpe, name, false)
#define SERIALIZE_NBYTES_VALUE_FIELD_X(tpe, name) \
  SERIALIZE_NBYTES_FIELD(tpe, name, true)

#define DESCRIPTOR_VALUE_WRITE_FN_X(tpe, name) \
  &generic_serializer< serializer< tpe, true > >::write,
#define DESCRIPTOR_VALUE_READ_FN_X(tpe, name) \
  &generic_serializer< serializer< tpe, true > >::read,
#define DESCRIPTOR_VALUE_FAILSAFE_READ_FN_X(tpe, name) \
  &generic_serializer< serializer< tpe, true > >::failsafe_read,
#define DESCRIPTOR_VALUE_NBYTES_FN_X(tpe, name) \
  &generic_serializer< serializer< tpe, true > >::nbytes,
#define DESCRIPTOR_VALUE_SKIP_FN_X(tpe, name) \
  &generic_serializer< serializer< tpe, true > >::skip,
#define DESCRIPTOR_VALUE_FAILSAFE_SKIP_FN_X(tpe, name) \
  &generic_serializer< serializer< tpe, true > >::failsafe_skip,
#define DESCRIPTOR_VALUE_MAX_NBYTES_X(tpe, name) \
  serializer< tpe, true >::max_nbytes(),
#define DESCRIPTOR_VALUE_OFFSETOF_X(tpe, name) \
  offsetof(value, name),
#define DESCRIPTOR_VALUE_SIZEOF_X(tpe, name) \
  sizeof(tpe),

// semantics:

// Write the encoded version of obj into buf, returning a const pointer
// to buf. Assumes that [buf, buf + nbytes(obj)) is valid memory
//
// const uint8_t *
// write(uint8_t *buf, const T *obj)

// Write the encoded version of obj into buf, returning a const reference
// to buf. Makes no assumptions about buf
//
// std::string &
// write(std::string &buf, const T *obj)

// Read a serialized, encoded version from buf into obj, returning
// a const pointer to obj. Assumes that buf points to a valid encoding
//
// const T *
// read(const uint8_t *buf, T *obj)

// Returns the number of bytes required to encode this specific instance
// of obj.
//
// size_t
// nbytes(const T *obj)

// implements encoded versions of the above functions
#define DO_STRUCT_ENCODE_REST(name) \
  inline ALWAYS_INLINE const uint8_t * \
  write(uint8_t *buf, const struct name *obj) const \
  { \
    encode_write(buf, obj); \
    return buf; \
  } \
  inline ALWAYS_INLINE const struct name * \
  read(const uint8_t *buf, struct name *obj) const \
  { \
    encode_read(buf, obj); \
    return obj; \
  } \
  inline ALWAYS_INLINE const struct name * \
  prefix_read(const uint8_t *buf, struct name *obj, size_t prefix) const \
  { \
    encode_prefix_read(buf, obj, prefix); \
    return obj; \
  } \
  inline ALWAYS_INLINE const struct name * \
  failsafe_read(const uint8_t *buf, size_t nbytes, struct name *obj) const \
  { \
    if (unlikely(!encode_failsafe_read(buf, nbytes, obj))) \
      return nullptr; \
    return obj; \
  } \
  inline ALWAYS_INLINE size_t \
  nbytes(const struct name *obj) const \
  { \
    return encode_nbytes(obj); \
  }

// implements direct pass-through version of the above functions
#define DO_STRUCT_PASS_THROUGH_REST(name) \
  inline ALWAYS_INLINE const uint8_t * \
  write(uint8_t *buf, const struct name *obj) const \
  { \
    *((struct name *) buf) = *obj; \
    return buf; \
  } \
  inline ALWAYS_INLINE const struct name * \
  read(const uint8_t *buf, struct name *obj) const \
  { \
    *obj = *((const struct name *) buf); \
    return obj; \
  } \
  inline ALWAYS_INLINE const struct name * \
  prefix_read(const uint8_t *buf, struct name *obj, size_t prefix) const \
  { \
    *obj = *((const struct name *) buf); \
    return obj; \
  } \
  inline ALWAYS_INLINE const struct name * \
  failsafe_read(const uint8_t *buf, size_t nbytes, struct name *obj) const \
  { \
    if (unlikely(nbytes < sizeof(*obj))) \
      return nullptr; \
    *obj = *((const struct name *) buf); \
    return obj; \
  } \
  inline ALWAYS_INLINE size_t \
  nbytes(const struct name *obj) const \
  { \
    return sizeof(*obj); \
  }

#if NDB_MASSTREE
#define DO_STRUCT_MASSTREE(name) \
  inline ALWAYS_INLINE const struct name * \
  read(lcdf::Str buf, struct name *obj) const       \
  { \
    return read((const uint8_t *) buf.data(), obj); \
  }
#else
#define DO_STRUCT_MASSTREE(name)
#endif

#define DO_STRUCT_COMMON(name) \
  inline std::string & \
  write(std::string &buf, const struct name *obj) const \
  { \
    buf.clear(); \
    buf.resize(nbytes(obj)); \
    write((uint8_t *) buf.data(), obj); \
    return buf; \
  } \
  inline std::string \
  write(const struct name *obj) const \
  { \
    std::string ret; \
    write(ret, obj); \
    return ret; \
  } \
  inline ALWAYS_INLINE const struct name * \
  read(const std::string &buf, struct name *obj) const \
  { \
    return read((const uint8_t *) buf.data(), obj); \
  } \
  DO_STRUCT_MASSTREE(name) \
  inline ALWAYS_INLINE const struct name * \
  read(const char *buf, struct name *obj) const \
  { \
    return read((const uint8_t *) buf, obj); \
  } \
  inline ALWAYS_INLINE const struct name * \
  prefix_read(const std::string &buf, struct name *obj, size_t prefix) const \
  { \
    return prefix_read((const uint8_t *) buf.data(), obj, prefix); \
  } \
  inline ALWAYS_INLINE const struct name * \
  prefix_read(const char *buf, struct name *obj, size_t prefix) const \
  { \
    return prefix_read((const uint8_t *) buf, obj, prefix); \
  }

#ifdef USE_VARINT_ENCODING
#define DO_STRUCT_REST_VALUE(name) DO_STRUCT_ENCODE_REST(name)
#else
#define DO_STRUCT_REST_VALUE(name) DO_STRUCT_PASS_THROUGH_REST(name)
#endif

#define APPLY_X_AND_Y(x, y) x(y, y)

// the main macro
#define DO_STRUCT(name, keyfields, valuefields) \
  struct name { \
  struct key { \
    inline key() {} \
    inline key(keyfields(STRUCT_PARAM_FIRST_X, STRUCT_PARAM_REST_X)) : \
      keyfields(STRUCT_INITLIST_FIRST_X, STRUCT_INITLIST_REST_X) {} \
    APPLY_X_AND_Y(keyfields, STRUCT_LAYOUT_X) \
    inline bool \
    operator==(const struct key &other) const \
    { \
      APPLY_X_AND_Y(keyfields, STRUCT_EQ_X) \
      return true; \
    } \
    inline bool \
    operator!=(const struct key &other) const \
    { \
      return !operator==(other); \
    } \
    enum { \
      APPLY_X_AND_Y(keyfields, STRUCT_FIELDPOS_X) \
      NFIELDS \
    }; \
  } PACKED; \
  struct value { \
    inline value() {} \
    inline value(valuefields(STRUCT_PARAM_FIRST_X, STRUCT_PARAM_REST_X)) : \
      valuefields(STRUCT_INITLIST_FIRST_X, STRUCT_INITLIST_REST_X) {} \
    APPLY_X_AND_Y(valuefields, STRUCT_LAYOUT_X) \
    inline bool \
    operator==(const struct value &other) const \
    { \
      APPLY_X_AND_Y(valuefields, STRUCT_EQ_X) \
      return true; \
    } \
    inline bool \
    operator!=(const struct value &other) const \
    { \
      return !operator==(other); \
    } \
    enum { \
      APPLY_X_AND_Y(valuefields, STRUCT_FIELDPOS_X) \
      NFIELDS \
    }; \
  } PACKED; \
  struct value_descriptor { \
    static inline generic_write_fn \
    write_fn(size_t i) \
    { \
      static generic_write_fn write_fns[] = { \
        APPLY_X_AND_Y(valuefields, DESCRIPTOR_VALUE_WRITE_FN_X) \
      }; \
      return write_fns[i]; \
    } \
    static inline generic_read_fn \
    read_fn(size_t i) \
    { \
      static generic_read_fn read_fns[] = { \
        APPLY_X_AND_Y(valuefields, DESCRIPTOR_VALUE_READ_FN_X) \
      }; \
      return read_fns[i]; \
    } \
    static inline generic_failsafe_read_fn \
    failsafe_read_fn(size_t i) \
    { \
      static generic_failsafe_read_fn failsafe_read_fns[] = { \
        APPLY_X_AND_Y(valuefields, DESCRIPTOR_VALUE_FAILSAFE_READ_FN_X) \
      }; \
      return failsafe_read_fns[i]; \
    } \
    static inline generic_nbytes_fn \
    nbytes_fn(size_t i) \
    { \
      static generic_nbytes_fn nbytes_fns[] = { \
        APPLY_X_AND_Y(valuefields, DESCRIPTOR_VALUE_NBYTES_FN_X) \
      }; \
      return nbytes_fns[i]; \
    } \
    static inline generic_skip_fn \
    skip_fn(size_t i) \
    { \
      static generic_skip_fn skip_fns[] = { \
        APPLY_X_AND_Y(valuefields, DESCRIPTOR_VALUE_SKIP_FN_X) \
      }; \
      return skip_fns[i]; \
    } \
    static inline generic_failsafe_skip_fn \
    failsafe_skip_fn(size_t i) \
    { \
      static generic_failsafe_skip_fn failsafe_skip_fns[] = { \
        APPLY_X_AND_Y(valuefields, DESCRIPTOR_VALUE_FAILSAFE_SKIP_FN_X) \
      }; \
      return failsafe_skip_fns[i]; \
    } \
    static inline constexpr size_t \
    nfields() \
    { \
      return static_cast<size_t>(value::NFIELDS); \
    } \
    static inline size_t \
    max_nbytes(size_t i) \
    { \
      static size_t maxn[] = { \
        APPLY_X_AND_Y(valuefields, DESCRIPTOR_VALUE_MAX_NBYTES_X) \
      }; \
      return maxn[i]; \
    } \
    static inline size_t \
    cstruct_offsetof(size_t i) \
    { \
      static size_t offsets[] = { \
        APPLY_X_AND_Y(valuefields, DESCRIPTOR_VALUE_OFFSETOF_X) \
      }; \
      return offsets[i]; \
    } \
    static inline size_t \
    cstruct_sizeof(size_t i) \
    { \
      static size_t sizeofs[] = { \
        APPLY_X_AND_Y(valuefields, DESCRIPTOR_VALUE_SIZEOF_X) \
      }; \
      return sizeofs[i]; \
    } \
  }; \
  }; \
  inline std::ostream & \
  operator<<(std::ostream &o, const name::key &obj) \
  { \
    o << "{" << keyfields(STRUCT_PRINTER_FIRST_X, STRUCT_PRINTER_REST_X) << "}"; \
    return o; \
  } \
  inline std::ostream & \
  operator<<(std::ostream &o, const name::value &obj) \
  { \
    o << "{" << valuefields(STRUCT_PRINTER_FIRST_X, STRUCT_PRINTER_REST_X) << "}"; \
    return o; \
  } \
  namespace private_ { \
  template <> \
  struct is_trivially_destructible< name::key > { \
    static const bool value = true; \
  }; \
  } \
  template <> \
  struct encoder< name::key > { \
  inline void \
  encode_write(uint8_t *buf, const struct name::key *obj) const \
  { \
    APPLY_X_AND_Y(keyfields, SERIALIZE_WRITE_KEY_FIELD_X) \
  } \
  inline void \
  encode_read(const uint8_t *buf, struct name::key *obj) const \
  { \
    APPLY_X_AND_Y(keyfields, SERIALIZE_READ_KEY_FIELD_X) \
  } \
  inline void \
  encode_prefix_read(const uint8_t *buf, struct name::key *obj, size_t prefix) const \
  { \
    size_t i = 0; \
    APPLY_X_AND_Y(keyfields, SERIALIZE_PREFIX_READ_KEY_FIELD_X) \
  } \
  inline bool \
  encode_failsafe_read(const uint8_t *buf, size_t nbytes, struct name::key *obj) const \
  { \
    APPLY_X_AND_Y(keyfields, SERIALIZE_FAILSAFE_READ_KEY_FIELD_X) \
    return true; \
  } \
  inline ALWAYS_INLINE size_t \
  encode_nbytes(const struct name::key *obj) const \
  { \
    return sizeof(*obj); \
  } \
  static inline constexpr size_t \
  encode_max_nbytes() \
  { \
    return keyfields(SERIALIZE_MAX_NBYTES_KEY_FIELD_X, \
                     SERIALIZE_MAX_NBYTES_KEY_FIELD_Y); \
  } \
  inline ALWAYS_INLINE size_t \
  encode_max_nbytes_prefix(size_t nfields) const \
  { \
    size_t ret = 0; \
    size_t i = 0; \
    if (likely(nfields >= name::key::NFIELDS)) \
      return std::numeric_limits<size_t>::max(); \
    APPLY_X_AND_Y(keyfields, SERIALIZE_MAX_NBYTES_PREFIX_KEY_FIELD_X) \
    return ret; \
  } \
  DO_STRUCT_COMMON(name::key) \
  DO_STRUCT_ENCODE_REST(name::key) \
  }; \
  namespace private_ { \
  template <> \
  struct is_trivially_destructible< name::value > { \
    static const bool value = true; \
  }; \
  } \
  template <> \
  struct encoder< name::value > { \
  inline void \
  encode_write(uint8_t *buf, const struct name::value *obj) const \
  { \
    APPLY_X_AND_Y(valuefields, SERIALIZE_WRITE_VALUE_FIELD_X) \
  } \
  inline void \
  encode_read(const uint8_t *buf, struct name::value *obj) const \
  { \
    APPLY_X_AND_Y(valuefields, SERIALIZE_READ_VALUE_FIELD_X) \
  } \
  inline void \
  encode_prefix_read(const uint8_t *buf, struct name::value *obj, size_t prefix) const \
  { \
    size_t i = 0; \
    APPLY_X_AND_Y(valuefields, SERIALIZE_PREFIX_READ_VALUE_FIELD_X) \
  } \
  inline bool \
  encode_failsafe_read(const uint8_t *buf, size_t nbytes, struct name::value *obj) const \
  { \
    APPLY_X_AND_Y(valuefields, SERIALIZE_FAILSAFE_READ_VALUE_FIELD_X) \
    return true; \
  } \
  inline size_t \
  encode_nbytes(const struct name::value *obj) const \
  { \
    size_t size = 0; \
    APPLY_X_AND_Y(valuefields, SERIALIZE_NBYTES_VALUE_FIELD_X) \
    return size; \
  } \
  static inline constexpr size_t \
  encode_max_nbytes() \
  { \
    return valuefields(SERIALIZE_MAX_NBYTES_VALUE_FIELD_X, \
                       SERIALIZE_MAX_NBYTES_VALUE_FIELD_Y); \
  } \
  inline ALWAYS_INLINE size_t \
  encode_max_nbytes_prefix(size_t nfields) const \
  { \
    size_t ret = 0; \
    size_t i = 0; \
    if (likely(nfields >= name::value::NFIELDS)) \
      return std::numeric_limits<size_t>::max(); \
    APPLY_X_AND_Y(valuefields, SERIALIZE_MAX_NBYTES_PREFIX_VALUE_FIELD_X) \
    return ret; \
  } \
  DO_STRUCT_COMMON(name::value) \
  DO_STRUCT_REST_VALUE(name::value) \
  };

template <typename T>
struct schema {
  typedef T base_type;
  typedef typename T::key key_type;
  typedef typename T::value value_type;
  typedef typename T::value_descriptor value_descriptor_type;
  typedef encoder<key_type> key_encoder_type;
  typedef encoder<value_type> value_encoder_type;
};

#endif /* _NDB_BENCH_ENCODER_H_ */
