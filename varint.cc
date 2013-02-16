#include <iostream>

#include "varint.h"
#include "macros.h"
#include "util.h"

using namespace std;
using namespace util;

// read unsigned varint32 from buffer. assumes the buffer will have enough size
const uint8_t *
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

static void
do_test(uint32_t v)
{
  uint8_t buf[5];
  uint8_t *p = &buf[0];
  p = write_uvint32(p, v);
  ALWAYS_ASSERT(size_t(p - &buf[0]) == size_uvint32(v));

  const uint8_t *p0 = &buf[0];
  uint32_t v0 = 0;
  p0 = read_uvint32(p0, &v0);
  ALWAYS_ASSERT(v == v0);
  ALWAYS_ASSERT(p == p0);
}

void
varint::Test()
{
  fast_random r(2043859);
  for (int i = 0; i < 1000; i++)
    do_test(r.next_u32());
  cerr << "varint tests passed" << endl;
}
