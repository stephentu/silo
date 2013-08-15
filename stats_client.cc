/**
 * stats_client.cc
 *
 * stand-alone client to poll a stats server
 *
 */

#include <iostream>
#include <string>
#include <system_error>
#include <thread>

#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>

#include "counter.h"
#include "stats_common.h"
#include "util.h"

using namespace std;
using namespace util;

int
main(int argc, char **argv)
{
  if (argc != 3) {
    cerr << "[usage] " << argv[0] << " sockfile counterspec" << endl;
    return 1;
  }

  const string sockfile(argv[1]);
  const vector<string> counter_names = split(argv[2], ':');

  int fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0)
    throw system_error(errno, system_category(),
        "creating UNIX domain socket");
  struct sockaddr_un addr;
  memset(&addr, 0, sizeof addr);
  addr.sun_family = AF_UNIX;
  if (sockfile.length() + 1 >= sizeof addr.sun_path)
    throw range_error("UNIX domain socket path too long");
  strcpy(addr.sun_path, sockfile.c_str());
  size_t len = strlen(addr.sun_path) + sizeof(addr.sun_family);
  if (connect(fd, (struct sockaddr *) &addr, len) < 0)
    throw system_error(errno, system_category(),
        "connecting to socket");

  packet pkt;
  int r;
  timer loop_timer;
  for (;;) {
    for (auto &name : counter_names) {
      uint8_t buf[1 + name.size()];
      buf[0] = (uint8_t) stats_command::GET_COUNTER_VALUE;
      memcpy(&buf[1], name.data(), name.size());
      pkt.assign((const char *) &buf[0], sizeof(buf));
      if ((r = pkt.sendpkt(fd))) {
        perror("send - disconnecting");
        return 1;
      }
      if ((r = pkt.recvpkt(fd))) {
        if (r == EOF)
          return 0;
        perror("recv - disconnecting");
        return 1;
      }
      const get_counter_value_t *resp = (const get_counter_value_t *) pkt.data();
      cout << name                << " "
           << resp->timestamp_us_ << " "
           << resp->d_.count_     << " "
           << resp->d_.sum_       << " "
           << resp->d_.max_       << endl;
    }

    const uint64_t last_loop_usec  = loop_timer.lap();
    //const uint64_t delay_time_usec = 1000000;
    const uint64_t delay_time_usec = 1000000/1000;
    if (last_loop_usec < delay_time_usec) {
      const uint64_t sleep_ns = (delay_time_usec - last_loop_usec) * 1000;
      struct timespec t;
      t.tv_sec  = sleep_ns / ONE_SECOND_NS;
      t.tv_nsec = sleep_ns % ONE_SECOND_NS;
      nanosleep(&t, nullptr);
    }
  }

  return 0;
}
