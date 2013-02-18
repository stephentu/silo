#ifndef _NDB_BENCH_ENCODER_H_
#define _NDB_BENCH_ENCODER_H_

#include <stdint.h>
#include "serializer.h"

// Very hacky- uses macros to generate encoders

template <typename T> struct encoder {};

// abuse the C preprocessor due to laziness
// we really should auto-generate this code

#define STRUCT_LAYOUT_X(tpe, name) \
  tpe name;

#define STRUCT_EQ_X(tpe, name) \
  if (this->name != other.name) \
    return false;

#define SERIALIZE_WRITE_FIELD_X(tpe, name) \
  do { \
    serializer< tpe > s; \
    buf = s.write(buf, (const tpe *) field); \
    field += sizeof(tpe); \
  } while (0);

#define SERIALIZE_READ_FIELD_X(tpe, name) \
  do { \
    serializer< tpe > s; \
    buf = s.read(buf, (tpe *) field); \
    field += sizeof(tpe); \
  } while (0);

#define SERIALIZE_NBYTES_FIELD_X(tpe, name) \
  do { \
    serializer< tpe > s; \
    size += s.nbytes((const tpe *) field); \
    field += sizeof(tpe); \
  } while (0);

#ifdef USE_VARINT_ENCODING
#define DO_STRUCT_REST(name) \
  inline const uint8_t * \
  write(uint8_t *temp_buf, const struct name *obj) const \
  { \
    encode_write(temp_buf, obj); \
    return temp_buf; \
  } \
  inline const struct name * \
  read(const uint8_t *buf, struct name *temp_obj) const \
  { \
    encode_read(buf, temp_obj); \
    return temp_obj; \
  } \
  inline size_t \
  nbytes(const struct name *obj) const \
  { \
    return encode_nbytes(obj); \
  }
#else
#define DO_STRUCT_REST(name) \
  inline const uint8_t * \
  write(uint8_t *temp_buf, const struct name *obj) const \
  { \
    return (const uint8_t *) obj; \
  } \
  inline const struct name * \
  read(const uint8_t *buf, struct name *temp_obj) const \
  { \
    return (const struct name *) buf; \
  } \
  inline size_t \
  nbytes(const struct name *obj) const \
  { \
    return sizeof(*obj); \
  }
#endif

// the main macro
#define DO_STRUCT(name, fields) \
  struct name { \
    fields(STRUCT_LAYOUT_X) \
    inline bool \
    operator==(const struct name &other) const \
    { \
      fields(STRUCT_EQ_X) \
      return true; \
    } \
    inline bool \
    operator!=(const struct name &other) const \
    { \
      return !operator==(other); \
    } \
  } PACKED; \
  template <> \
  struct encoder< name > { \
  private: \
  uint8_t * \
  encode_write(uint8_t *buf, const struct name *obj) const \
  { \
    const uint8_t *field = (const uint8_t *) obj; \
    fields(SERIALIZE_WRITE_FIELD_X) \
    return buf; \
  } \
  const uint8_t * \
  encode_read(const uint8_t *buf, struct name *obj) const \
  { \
    uint8_t *field = (uint8_t *) obj; \
    fields(SERIALIZE_READ_FIELD_X) \
    return buf; \
  } \
  size_t \
  encode_nbytes(const struct name *obj) const \
  { \
    size_t size = 0; \
    const uint8_t *field = (const uint8_t *) obj; \
    fields(SERIALIZE_NBYTES_FIELD_X) \
    return size; \
  } \
  public: \
  DO_STRUCT_REST(name) \
  };

#endif /* _NDB_BENCH_ENCODER_H_ */
