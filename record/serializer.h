#ifndef _NDB_BENCH_SERIALIZER_H_
#define _NDB_BENCH_SERIALIZER_H_

#include <stdint.h>
#include "../macros.h"
#include "../varint.h"

typedef uint8_t *(*generic_write_fn)(uint8_t *, const uint8_t *);
typedef const uint8_t *(*generic_read_fn)(const uint8_t *, uint8_t *);
typedef const uint8_t *(*generic_failsafe_read_fn)(const uint8_t *, size_t, uint8_t *);
typedef size_t (*generic_nbytes_fn)(const uint8_t *);
typedef size_t (*generic_skip_fn)(const uint8_t *, uint8_t *);
typedef size_t (*generic_failsafe_skip_fn)(const uint8_t *, size_t, uint8_t *);

// wraps a real serializer, exposing generic functions
template <typename Serializer>
struct generic_serializer {
  typedef typename Serializer::obj_type obj_type;

  static inline uint8_t *
  write(uint8_t *buf, const uint8_t *obj)
  {
    return Serializer::write(buf, *reinterpret_cast<const obj_type *>(obj));
  }

  static inline const uint8_t *
  read(const uint8_t *buf, uint8_t *obj)
  {
    return Serializer::read(buf, reinterpret_cast<obj_type *>(obj));
  }

  // returns nullptr on failure
  static inline const uint8_t *
  failsafe_read(const uint8_t *buf, size_t nbytes, uint8_t *obj)
  {
    return Serializer::failsafe_read(
        buf, nbytes, reinterpret_cast<obj_type *>(obj));
  }

  static inline size_t
  nbytes(const uint8_t *obj)
  {
    return Serializer::nbytes(reinterpret_cast<const obj_type *>(obj));
  }

  static inline size_t
  skip(const uint8_t *stream, uint8_t *rawv)
  {
    return Serializer::skip(stream, rawv);
  }

  // returns 0 on failure
  static inline size_t
  failsafe_skip(const uint8_t *stream, size_t nbytes, uint8_t *rawv)
  {
    return Serializer::failsafe_skip(stream, nbytes, rawv);
  }

  static inline constexpr size_t
  max_nbytes()
  {
    return Serializer::max_bytes();
  }
};

template <typename T, bool Compress>
struct serializer {
  typedef T obj_type;

  static inline uint8_t *
  write(uint8_t *buf, const T &obj)
  {
    T *p = (T *) buf;
    *p = obj;
    return (uint8_t *) (p + 1);
  }

  static inline const uint8_t *
  read(const uint8_t *buf, T *obj)
  {
    const T *p = (const T *) buf;
    *obj = *p;
    return (const uint8_t *) (p + 1);
  }

  static inline const uint8_t *
  failsafe_read(const uint8_t *buf, size_t nbytes, T *obj)
  {
    if (unlikely(nbytes < sizeof(T)))
      return nullptr;
    return read(buf, obj);
  }

  static inline size_t
  nbytes(const T *obj)
  {
    return sizeof(*obj);
  }

  static inline size_t
  skip(const uint8_t *stream, uint8_t *rawv)
  {
    if (rawv)
      NDB_MEMCPY(rawv, stream, sizeof(T));
    return sizeof(T);
  }

  static inline size_t
  failsafe_skip(const uint8_t *stream, size_t nbytes, uint8_t *rawv)
  {
    if (unlikely(nbytes < sizeof(T)))
      return 0;
    if (rawv)
      NDB_MEMCPY(rawv, stream, sizeof(T));
    return sizeof(T);
  }

  static inline constexpr size_t
  max_nbytes()
  {
    return sizeof(T);
  }
};

// serializer<T, True> specializations
template <>
struct serializer<uint32_t, true> {
  typedef uint32_t obj_type;

  static inline uint8_t *
  write(uint8_t *buf, uint32_t obj)
  {
    return write_uvint32(buf, obj);
  }

  static inline const uint8_t *
  read(const uint8_t *buf, uint32_t *obj)
  {
    return read_uvint32(buf, obj);
  }

  static inline const uint8_t *
  failsafe_read(const uint8_t *buf, size_t nbytes, uint32_t *obj)
  {
    return failsafe_read_uvint32(buf, nbytes, obj);
  }

  static inline size_t
  nbytes(const uint32_t *obj)
  {
    return size_uvint32(*obj);
  }

  static inline size_t
  skip(const uint8_t *stream, uint8_t *rawv)
  {
    return skip_uvint32(stream, rawv);
  }

  static inline size_t
  failsafe_skip(const uint8_t *stream, size_t nbytes, uint8_t *rawv)
  {
    return failsafe_skip_uvint32(stream, nbytes, rawv);
  }

  static inline constexpr size_t
  max_nbytes()
  {
    return 5;
  }
};

template <>
struct serializer<int32_t, true> {
  typedef int32_t obj_type;

  static inline uint8_t *
  write(uint8_t *buf, int32_t obj)
  {
    const uint32_t v = encode(obj);
    return serializer<uint32_t, true>::write(buf, v);
  }

  static inline const uint8_t *
  read(const uint8_t *buf, int32_t *obj)
  {
    uint32_t v;
    buf = serializer<uint32_t, true>::read(buf, &v);
    *obj = decode(v);
    return buf;
  }

  static inline const uint8_t *
  failsafe_read(const uint8_t *buf, size_t nbytes, int32_t *obj)
  {
    uint32_t v;
    buf = serializer<uint32_t, true>::failsafe_read(buf, nbytes, &v);
    if (unlikely(!buf))
      return 0;
    *obj = decode(v);
    return buf;
  }

  static inline size_t
  nbytes(const int32_t *obj)
  {
    const uint32_t v = encode(*obj);
    return serializer<uint32_t, true>::nbytes(&v);
  }

  static inline size_t
  skip(const uint8_t *stream, uint8_t *rawv)
  {
    return skip_uvint32(stream, rawv);
  }

  static inline size_t
  failsafe_skip(const uint8_t *stream, size_t nbytes, uint8_t *rawv)
  {
    return failsafe_skip_uvint32(stream, nbytes, rawv);
  }

  static inline constexpr size_t
  max_nbytes()
  {
    return 5;
  }

private:
  // zig-zag encoding from:
  // http://code.google.com/p/protobuf/source/browse/trunk/src/google/protobuf/wire_format_lite.h

  static inline ALWAYS_INLINE constexpr uint32_t
  encode(int32_t value)
  {
    return (value << 1) ^ (value >> 31);
  }

  static inline ALWAYS_INLINE constexpr int32_t
  decode(uint32_t value)
  {
    return (value >> 1) ^ -static_cast<int32_t>(value & 1);
  }
};

#endif /* _NDB_BENCH_SERIALIZER_H_ */
