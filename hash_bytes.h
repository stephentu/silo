#ifndef _HASH_BYTES_H_
#define _HASH_BYTES_H_

#include <stddef.h>

// a replacement for std::_Hash_bytes, which isn't available
// until we upgrade to C++11

namespace ndb {
  size_t
  hash_bytes(const void *ptr, size_t len, size_t seed);
}

#endif /* _HASH_BYTES_H_ */
