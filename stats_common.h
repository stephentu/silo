#pragma once

#include "counter.h"
#include "macros.h"
#include "fileutils.h"

enum class stats_command : uint8_t { GET_COUNTER_VALUE = 0x1 };

struct get_counter_value_t {
  uint64_t timestamp_us_; // usec
  counter_data d_;
};

class packet {
public:
  static const size_t MAX_DATA = 0xFFFF - 4;
  packet() : size_(0) {}
  inline void clear() { size_ = 0; }
  inline void
  assign(const char *p, size_t n)
  {
    INVARIANT(n <= MAX_DATA);
    NDB_MEMCPY(&data_[0], p, n);
    size_ = n;
  }
  inline void
  assign(const std::string &s)
  {
    assign(s.data(), s.size());
  }
  int
  sendpkt(int fd) const
  {
    // XXX: we don't care about endianness
    return fileutils::writeall(
        fd, (const char *) &size_, sizeof(size_) + size_);
  }
  int
  recvpkt(int fd)
  {
    // XXX: we don't care about endianness
    int r;
    if ((r = fileutils::readall(fd, (char *) &size_, sizeof(size_)))) {
      clear();
      return r;
    }
    if (size_ > packet::MAX_DATA) {
      std::cerr << "bad packet read with excessive size" << std::endl;
      clear();
      return -1;
    }
    if ((r = fileutils::readall(fd, &data_[0], size_))) {
      clear();
      return r;
    }
    return 0;
  }
  inline uint32_t size() const { return size_; }
  inline const char * data() const { return &data_[0]; }
private:
  uint32_t size_;
  char data_[MAX_DATA];
};
