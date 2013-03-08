#ifndef _NDB_BENCH_SERIALIZER_H_
#define _NDB_BENCH_SERIALIZER_H_

#include <stdint.h>
#include "../macros.h"
#include "../varint.h"

template <typename T, bool Compress>
struct serializer {
  inline uint8_t *
  write(uint8_t *buf, const T &obj) const
  {
    T *p = (T *) buf;
    *p = obj;
    return (uint8_t *) (p + 1);
  }

  inline const uint8_t *
  read(const uint8_t *buf, T *obj) const
  {
    const T *p = (const T *) buf;
    *obj = *p;
    return (const uint8_t *) (p + 1);
  }

  inline size_t
  nbytes(const T *obj) const
  {
    return sizeof(*obj);
  }

  inline size_t
  max_nbytes() const
  {
    return sizeof(T);
  }
};

// serializer<T, True> specializations
template <>
struct serializer<uint32_t, true> {
  inline uint8_t *
  write(uint8_t *buf, uint32_t obj) const
  {
    return write_uvint32(buf, obj);
  }

  const uint8_t *
  read(const uint8_t *buf, uint32_t *obj) const
  {
    return read_uvint32(buf, obj);
  }

  size_t
  nbytes(const uint32_t *obj) const
  {
    return size_uvint32(*obj);
  }

  size_t
  max_nbytes() const
  {
    return 5;
  }
};

template <>
struct serializer<int32_t, true> {
  inline uint8_t *
  write(uint8_t *buf, int32_t obj) const
  {
    serializer<uint32_t, true> s;
    const uint32_t v = encode(obj);
    return s.write(buf, v);
  }

  const uint8_t *
  read(const uint8_t *buf, int32_t *obj) const
  {
    serializer<uint32_t, true> s;
    uint32_t v;
    buf = s.read(buf, &v);
    *obj = decode(v);
    return buf;
  }

  size_t
  nbytes(const int32_t *obj) const
  {
    serializer<uint32_t, true> s;
    const uint32_t v = encode(*obj);
    return s.nbytes(&v);
  }

  size_t
  max_nbytes() const
  {
    return 5;
  }

private:
  // zig-zag encoding from:
  // http://code.google.com/p/protobuf/source/browse/trunk/src/google/protobuf/wire_format_lite.h

  static inline ALWAYS_INLINE uint32_t
  encode(int32_t value)
  {
    return (value << 1) ^ (value >> 31);
  }

  static inline ALWAYS_INLINE int32_t
  decode(uint32_t value)
  {
    return (value >> 1) ^ -static_cast<int32_t>(value & 1);
  }
};

#endif /* _NDB_BENCH_SERIALIZER_H_ */
