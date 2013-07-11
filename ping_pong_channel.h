#pragma once

/**
 * A ping-ponging channel meant to be shared amonst two threads exactly
 */

#include <cassert>
#include <mutex>
#include <condition_variable>

template <typename T>
class ping_pong_channel {
public:
  ping_pong_channel(bool sense) : data_(), sense_(sense) {}

  void
  send(const T &data, bool sense)
  {
    std::unique_lock<std::mutex> l(m_);
    assert(sense == sense_);
    data_ = data;
    sense_ = !sense;
    cv_.notify_one();
  }

  void
  recv(T &data, bool sense)
  {
    std::unique_lock<std::mutex> l(m_);
    while (sense_ != sense)
      cv_.wait(l);
    data = data_;
  }

private:
  T data_;
  bool sense_; // who is expected to make the next sending move

  std::mutex m_;
  std::condition_variable cv_;
};
