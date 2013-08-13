#include <system_error>
#include <thread>

#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>

#include "counter.h"
#include "stats_server.h"

using namespace std;

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
    thread(&stats_server::serve_client, this, fd).detach();
  }
}

static int
writeall(int fd, const char *buf, int n)
{
  while (n) {
    int r = write(fd, buf, n);
    if (unlikely(r < 0))
      return r;
    buf += r;
    n -= r;
  }
  return 0;
}

static int
readall(int fd, char *buf, int n)
{
  while (n) {
    int r = read(fd, buf, n);
    if (r == 0)
      return EOF;
    if (r < 0) {
      if (!(errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR))
        continue;
      return r;
    }
    buf += r;
    n -= r;
  }
  return 0;
}

int
stats_server::sendpkt(int fd, const packet &pkt)
{
  // XXX: we don't care about endianness
  return writeall(fd, (const char *) &pkt.size_, sizeof(pkt.size_) + pkt.size_);
}

int
stats_server::recvpkt(int fd, packet &pkt)
{
  // XXX: we don't care about endianness
  int r;
  if ((r = readall(fd, (char *) &pkt.size_, sizeof(pkt.size_))))
    return r;
  if (pkt.size_ > packet::MAX_DATA) {
    cerr << "bad packet read with excessive size" << endl;
    return -1;
  }
  if ((r = readall(fd, &pkt.data_[0], pkt.size_)))
    return r;
  return 0;
}

bool
stats_server::handle_cmd_get_counter_value(const string &name, packet &pkt)
{
  counter_data d;
  if (!event_counter::stat(name, d))
    cerr << "could not find counter " << name << endl;
  pkt.assign((const char *) &d, sizeof(d));
  return true;
}

void
stats_server::serve_client(int fd)
{
  packet pkt;
  string scratch;
  for (;;) {
    int r = recvpkt(fd, pkt);
    if (r == EOF) {
      cerr << "client disconnected" << endl;
      return;
    }
    if (r) {
      perror("recv- dropping connection");
      return;
    }
    INVARIANT(pkt.size_);
    switch (pkt.data_[0]) {
    case CMD_GET_COUNTER_VALUE:
      {
        scratch.assign(pkt.data_[1], pkt.size_ - 1);
        if (!handle_cmd_get_counter_value(scratch, pkt)) {
          cerr << "error on handle_cmd_get_counter_value(), dropping" << endl;
          return;
        }
        sendpkt(fd, pkt);
        break;
      }
    default:
      cerr << "bad command- dropping connection" << endl;
      return;
    }
  }
}
