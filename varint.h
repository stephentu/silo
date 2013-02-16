#ifndef _VARINT_H_
#define _VARINT_H_

#include <stdint.h>
#include "macros.h"

extern const uint8_t *
read_uvint32_slow(const uint8_t *buf, uint32_t *value) NEVER_INLINE;

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
  } else {
    return read_uvint32_slow(buf, value);
  }
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
