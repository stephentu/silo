#pragma once

#include <string>

#include "macros.h"

// serves over unix socket
class stats_server {
public:

  enum Commands {
    CMD_GET_COUNTER_VALUE = 0x1,
    NCOMMANDS,
  };
  static_assert(NCOMMANDS < 256, "xx");

  struct packet {
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
    uint32_t size_;
    char data_[MAX_DATA];
  };

  stats_server(const std::string &sockfile);
  void serve_forever(); // blocks current thread
private:

  int sendpkt(int fd, const packet &pkt);
  int recvpkt(int fd, packet &pkt);

  bool handle_cmd_get_counter_value(const std::string &name, packet &pkt);

  void serve_client(int fd);
  std::string sockfile_;
};
