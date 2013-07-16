#pragma once

/**
 * A ping-ponging channel meant to be shared amonst two threads exactly
 */

#include <cassert>
#include <mutex>
#include <condition_variable>
#include <atomic>

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

// one sender, one receiver
// 1. sender can wait for receiver to consume message
// 2. receiver can wait for sender to post a message
template <typename T>
class one_way_post {
public:
  one_way_post() : data_(), status_(false), m_(), cv_() {}

  /** producer API */

  void
  post(const T &data, bool wait)
  {
    std::unique_lock<std::mutex> l(m_);
    if (wait)
      while (status_.load(std::memory_order_acquire))
        cv_.wait(l);
    else
      assert(!status_.load(std::memory_order_acquire));
    data_ = data;
    status_.store(true, std::memory_order_release);
    cv_.notify_one();
  }

  void
  wait_for_consumption()
  {
    std::unique_lock<std::mutex> l(m_);
    while (status_.load(std::memory_order_acquire))
      cv_.wait(l);
  }

  inline bool
  can_post() const
  {
    return !status_.load(std::memory_order_acquire);
  }

  /** consumer API */

  // waits until there is something, and peeks (but does not consume) at the
  // message
  void
  peek(T &data)
  {
    std::unique_lock<std::mutex> l(m_);
    while (!status_.load(std::memory_order_acquire))
      cv_.wait(l);
    data = data_;
  }

  void
  consume(T &data)
  {
    std::unique_lock<std::mutex> l(m_);
    while (!status_.load(std::memory_order_acquire))
      cv_.wait(l);
    data = data_;
    status_.store(false, std::memory_order_release);
    cv_.notify_one();
  }

private:
  T data_;
  std::atomic<bool> status_; // false means no message, true otherwise
  std::mutex m_;
  std::condition_variable cv_;
};
