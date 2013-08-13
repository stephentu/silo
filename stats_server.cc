#include <system_error>
#include <thread>

#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>

#include "counter.h"
#include "stats_server.h"
#include "util.h"

using namespace std;
using namespace util;

stats_server::stats_server(const string &sockfile)
  : sockfile_(sockfile) {}

void
stats_server::serve_forever()
{
  int fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0)
    throw system_error(errno, system_category(),
        "creating UNIX domain socket");

  struct sockaddr_un addr;
  memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  if (sockfile_.length() + 1 >= sizeof(addr.sun_path))
    throw range_error("UNIX domain socket path too long");
  strcpy(addr.sun_path, sockfile_.c_str());
  unlink(sockfile_.c_str());

  if (::bind(fd, (struct sockaddr *) &addr, sizeof(addr)) < 0)
    throw system_error(errno, system_category(),
        "binding to " + sockfile_);

  if (listen(fd, 5) < 0)
    throw system_error(errno, system_category(),
        "listening on " + sockfile_);

  for (;;) {
    int cfd = accept(fd, nullptr, 0);
    if (cfd < 0)
      throw system_error(errno, system_category(), "accept failed");
    thread(&stats_server::serve_client, this, cfd).detach();
  }
}


bool
stats_server::handle_cmd_get_counter_value(const string &name, packet &pkt)
{
  get_counter_value_t ret;
  ret.timestamp_us_ = timer::cur_usec();
  if (!event_counter::stat(name, ret.d_))
    cerr << "could not find counter " << name << endl;
  pkt.assign((const char *) &ret, sizeof(ret));
  return true;
}

void
stats_server::serve_client(int fd)
{
  packet pkt;
  string scratch;
  for (;;) {
    int r = pkt.recvpkt(fd);
    if (r == EOF) {
      cerr << "client disconnected" << endl;
      return;
    }
    if (r) {
      perror("recv- dropping connection");
      return;
    }
    INVARIANT(pkt.size());
    switch (pkt.data()[0]) {
    case static_cast<uint8_t>(stats_command::GET_COUNTER_VALUE):
      {
        scratch.assign(pkt.data() + 1, pkt.size() - 1);
        if (!handle_cmd_get_counter_value(scratch, pkt)) {
          cerr << "error on handle_cmd_get_counter_value(), dropping" << endl;
          return;
        }
        pkt.sendpkt(fd);
        break;
      }
    default:
      cerr << "bad command- dropping connection" << endl;
      return;
    }
  }
}
