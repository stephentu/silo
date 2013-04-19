#ifndef _VARINT_H_
#define _VARINT_H_

#include <stdint.h>
#include "macros.h"

// read unsigned varint32 from buffer. assumes the buffer will have enough size
inline const uint8_t *
read_uvint32_slow(const uint8_t *buf, uint32_t *value)
{
  const uint8_t *p;
  uint32_t b, result;

  p = buf;

  b = *p++; result  = (b & 0x7F)      ; if (likely(b < 0x80)) goto done;
  b = *p++; result |= (b & 0x7F) <<  7; if (likely(b < 0x80)) goto done;
  b = *p++; result |= (b & 0x7F) << 14; if (likely(b < 0x80)) goto done;
  b = *p++; result |= (b & 0x7F) << 21; if (likely(b < 0x80)) goto done;
  b = *p++; result |=  b         << 28; if (likely(b < 0x80)) goto done;

  ALWAYS_ASSERT(false); // should not reach here (improper encoding)

done:
  *value = result;
  return p;
}

/**
 * Read a uvint32 from buf into value, returning the
 * next position in buf after the value has been read.
 *
 * Assumes buf points to a well encoded varint
 */
inline ALWAYS_INLINE const uint8_t *
read_uvint32(const uint8_t *buf, uint32_t *value)
{
  if (likely(*buf < 0x80)) {
    *value = *buf;
    return buf + 1;
  }
  return read_uvint32_slow(buf, value);
}

inline const uint8_t *
failsafe_read_uvint32_slow(
    const uint8_t *buf, size_t nbytes, uint32_t *value)
{
  const uint8_t *p;
  uint32_t b, result;

  p = buf;

  if (unlikely(!nbytes--)) return nullptr;
  b = *p++; result  = (b & 0x7F)      ; if (likely(b < 0x80)) goto done;
  if (unlikely(!nbytes--)) return nullptr;
  b = *p++; result |= (b & 0x7F) <<  7; if (likely(b < 0x80)) goto done;
  if (unlikely(!nbytes--)) return nullptr;
  b = *p++; result |= (b & 0x7F) << 14; if (likely(b < 0x80)) goto done;
  if (unlikely(!nbytes--)) return nullptr;
  b = *p++; result |= (b & 0x7F) << 21; if (likely(b < 0x80)) goto done;
  if (unlikely(!nbytes--)) return nullptr;
  b = *p++; result |=  b         << 28; if (likely(b < 0x80)) goto done;

done:
  *value = result;
  return p;
}

inline ALWAYS_INLINE const uint8_t *
failsafe_read_uvint32(
    const uint8_t *stream, size_t nbytes, uint32_t *value)
{
  if (unlikely(!nbytes))
    return nullptr;
  const uint8_t ch = *stream;
  if (likely(ch < 0x80)) {
    *value = ch;
    return stream + 1;
  }
  return failsafe_read_uvint32_slow(stream, nbytes, value);
}

inline ALWAYS_INLINE size_t
skip_uvint32(const uint8_t *stream, uint8_t *rawv)
{
  if (rawv) {
    if (likely((rawv[0] = stream[0]) < 0x80)) return 1;
    if (likely((rawv[1] = stream[1]) < 0x80)) return 2;
    if (likely((rawv[2] = stream[2]) < 0x80)) return 3;
    if (likely((rawv[3] = stream[3]) < 0x80)) return 4;
    if (likely((rawv[4] = stream[4]) < 0x80)) return 5;
  } else {
    if (likely(stream[0] < 0x80)) return 1;
    if (likely(stream[1] < 0x80)) return 2;
    if (likely(stream[2] < 0x80)) return 3;
    if (likely(stream[3] < 0x80)) return 4;
    if (likely(stream[4] < 0x80)) return 5;
  }
  ALWAYS_ASSERT(false);
  return 0;
}

inline ALWAYS_INLINE size_t
failsafe_skip_uvint32(const uint8_t *stream, size_t nbytes, uint8_t *rawv)
{
  if (rawv) {
    if (unlikely(!nbytes--)) return 0;
    if (likely((rawv[0] = stream[0]) < 0x80)) return 1;
    if (unlikely(!nbytes--)) return 0;
    if (likely((rawv[1] = stream[1]) < 0x80)) return 2;
    if (unlikely(!nbytes--)) return 0;
    if (likely((rawv[2] = stream[2]) < 0x80)) return 3;
    if (unlikely(!nbytes--)) return 0;
    if (likely((rawv[3] = stream[3]) < 0x80)) return 4;
    if (unlikely(!nbytes--)) return 0;
    if (likely((rawv[4] = stream[4]) < 0x80)) return 5;
  } else {
    if (unlikely(!nbytes--)) return 0;
    if (likely(stream[0] < 0x80)) return 1;
    if (unlikely(!nbytes--)) return 0;
    if (likely(stream[1] < 0x80)) return 2;
    if (unlikely(!nbytes--)) return 0;
    if (likely(stream[2] < 0x80)) return 3;
    if (unlikely(!nbytes--)) return 0;
    if (likely(stream[3] < 0x80)) return 4;
    if (unlikely(!nbytes--)) return 0;
    if (likely(stream[4] < 0x80)) return 5;
  }
  ALWAYS_ASSERT(false);
  return 0;
}

/**
 * write uint32_t as unsigned varint32 to buffer. assumes the buffer will have
 * enough size. returns the position in buf after the value has been written
 */
inline uint8_t *
write_uvint32(uint8_t *buf, uint32_t value)
{
  while (value > 0x7F) {
    *buf++ = (((uint8_t) value) & 0x7F) | 0x80;
    value >>= 7;
  }
  *buf++ = ((uint8_t) value) & 0x7F;
  return buf;
}

inline size_t
size_uvint32(uint32_t value)
{
  if (likely(value <= 0x7F))                                               return 1;
  if (likely(value <= ((0x7F << 7) | 0x7F)))                               return 2;
  if (likely(value <= ((0x7F << 14) | (0x7F << 7) | 0x7F)))                return 3;
  if (likely(value <= ((0x7F << 21) | (0x7F << 14) | (0x7F << 7) | 0x7F))) return 4;
  return 5;
}

class varint {
public:
  static void Test();
};

#endif /* _VARINT_H_ */
