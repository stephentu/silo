#pragma once

#include <unistd.h>
#include <errno.h>

class fileutils {
public:

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
        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)
          continue;
        return r;
      }
      buf += r;
      n -= r;
    }
    return 0;
  }

};
