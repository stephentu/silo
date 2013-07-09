/**
 * A stand-alone binary which doesn't depend on the system,
 * used to test the current persistence strategy
 */

#include <cassert>
#include <iostream>
#include <cstdint>
#include <random>
#include <vector>
#include <atomic>
#include <thread>

#include <unistd.h>
#include <sys/uio.h>
#include <sys/types.h>
#include <fcntl.h>
#include <libaio.h>

#include "macros.h"
#include "amd64.h"
#include "record/serializer.h"

using namespace std;

struct tidhelpers {
  // copied from txn_proto2_impl.h

  static const uint64_t NBitsNumber = 24;

  static const size_t CoreBits = NMAXCOREBITS; // allow 2^CoreShift distinct threads
  static const size_t NMaxCores = NMAXCORES;

  static const uint64_t CoreMask = (NMaxCores - 1);

  static const uint64_t NumIdShift = CoreBits;
  static const uint64_t NumIdMask = ((((uint64_t)1) << NBitsNumber) - 1) << NumIdShift;

  static const uint64_t EpochShift = CoreBits + NBitsNumber;
  static const uint64_t EpochMask = ((uint64_t)-1) << EpochShift;

  static inline
  uint64_t CoreId(uint64_t v)
  {
    return v & CoreMask;
  }

  static inline
  uint64_t NumId(uint64_t v)
  {
    return (v & NumIdMask) >> NumIdShift;
  }

  static inline
  uint64_t EpochId(uint64_t v)
  {
    return (v & EpochMask) >> EpochShift;
  }

  static inline
  uint64_t MakeTid(uint64_t core_id, uint64_t num_id, uint64_t epoch_id)
  {
    // some sanity checking
    static_assert((CoreMask | NumIdMask | EpochMask) == ((uint64_t)-1), "xx");
    static_assert((CoreMask & NumIdMask) == 0, "xx");
    static_assert((NumIdMask & EpochMask) == 0, "xx");
    return (core_id) | (num_id << NumIdShift) | (epoch_id << EpochShift);
  }
};

template <typename T>
static T vecmax(const vector<T> &v)
{
  T ret = v.front();
  for (size_t i = 1; i < v.size(); i++)
    ret = max(ret, v[i]);
  return ret;
}

// only one concurrent reader + writer allowed

template <typename Tp, unsigned int Capacity>
class circbuf {
public:
  circbuf()
    : head_(0), tail_(0)
  {
    memset(&buf_[0], 0, Capacity * sizeof(buf_[0]));
  }

  inline bool
  empty() const
  {
    return head_ == tail_ && !buf_[head_];
  }


  // assumes there will be capacity
  inline void
  enq(Tp *p)
  {
    assert(!buf_[head_]);
    buf_[postincr(head_)] = p;
  }

  // blocks until something deqs()
  inline Tp *
  deq()
  {
    while (!buf_[tail_])
      nop_pause();
    return buf_[postincr(tail_)];
  }

  inline Tp *
  peek()
  {
    return buf_[tail_];
  }

private:
  static inline unsigned
  postincr(unsigned &i)
  {
    const unsigned ret = i;
    i = (i + 1) % Capacity;
    return ret;
  }

  Tp *buf_[Capacity];
  unsigned head_;
  unsigned tail_;
};

/** simulate global database state and workload*/

static vector<uint64_t> g_database;

static const size_t g_nrecords = 1000000;

static const size_t g_readset = 30;
static const size_t g_writeset = 5;
static const size_t g_keysize = 16; // in bytes
static const size_t g_valuesize = 30; // in bytes
static const size_t g_perthread_buffers = 4; // 4 outstanding buffers

/** global state about our persistence calculations */

// contains the latest TID inclusive, per core, which is (transitively)
// persistent. note that the prefix of the DB which is totally persistent is
// simply the max of this table.

static const size_t g_ntxns_worker = 10000;
static const size_t g_buffer_size = (1<<14); // in bytes

struct pbuffer {
  bool io_scheduled_; // has the logger scheduled IO yet?
  size_t curoff_; // current offset into buf_, either for writing
                  // or during the dep computation phase
  size_t remaining_; // number of deps remaining to compute
  std::string buf_; // the actual buffer, of size g_buffer_size

  inline uint8_t *
  pointer()
  {
    return (uint8_t *) buf_.data() + curoff_;
  }
};

static uint64_t g_persistence_vc[NMAXCORES] = {0};
static circbuf<pbuffer, g_perthread_buffers> g_all_buffers[NMAXCORES];
static circbuf<pbuffer, g_perthread_buffers> g_persist_buffers[NMAXCORES];

// a simulated worker

static void
fillstring(std::string &s, size_t t)
{
  s.clear();
  for (size_t i = 0; i < t; i++)
    s[i] = (char) i;
}

struct logbuf_header {
  uint64_t nentries;
} PACKED;

static void
simulateworker(unsigned int id)
{
  mt19937 prng(id);

  // read/write sets are uniform for now
  uniform_int_distribution<unsigned> dist(0, g_nrecords - 1);

  vector<uint64_t> readset(g_readset);
  string key, value;
  fillstring(key, g_keysize);
  fillstring(value, g_valuesize);

  struct pbuffer *curbuf = nullptr;

  for (size_t i = 0; i < g_ntxns_worker; i++) {
    for (size_t j = 0; j < g_readset; j++)
      readset[j] = g_database[dist(prng)];

    const uint64_t tidmax = vecmax(readset);
    const uint64_t tidcommit =
      tidhelpers::MakeTid(id, tidhelpers::NumId(tidmax) + 1, tidhelpers::EpochId(tidmax));

    // compute how much space we need for this entry
    size_t space_needed = 0;

    // 8 bytes to indicate TID
    space_needed += sizeof(uint64_t);

    // one byte to indicate # of deps
    space_needed += 1;

    // each dep occupies 8 bytes
    space_needed += g_readset * sizeof(uint64_t);

    // one byte to indicate # of records written
    space_needed += 1;

    // each record occupies (1 + key_length + 1 + value_length) bytes
    space_needed += g_writeset * (1 + g_keysize + 1 + g_valuesize);

  renew:
    if (!curbuf) {
      // block until we get a buf
      curbuf = g_all_buffers[id].deq();
      curbuf->io_scheduled_ = false;
      curbuf->buf_.assign(g_buffer_size, 0);
      curbuf->curoff_ = sizeof(logbuf_header);
      curbuf->remaining_ = 0;
    }

    if (g_buffer_size - curbuf->curoff_ < space_needed) {
      // push to logger
      g_persist_buffers[id].enq(curbuf);

      // get a new buf
      curbuf = nullptr;
      goto renew;
    }

    uint8_t *p = curbuf->pointer();

    serializer<uint8_t, false> s_uint8_t;
    serializer<uint64_t, false> s_uint64_t;

    p = s_uint64_t.write(p, tidcommit);
    p = s_uint8_t.write(p, g_readset);
    for (auto t : readset)
      p = s_uint64_t.write(p, t);
    p = s_uint8_t.write(p, g_writeset);
    for (size_t i = 0; i < g_writeset; i++) {
      p = s_uint8_t.write(p, g_keysize);
      memcpy(p, key.data(), g_keysize); p += g_keysize;
      p = s_uint8_t.write(p, g_valuesize);
      memcpy(p, value.data(), g_valuesize); p += g_valuesize;
    }

    curbuf->curoff_ += space_needed;
  }
}

template <typename OnReadRecord>
static const uint8_t *
read_log_entry(const uint8_t *p, uint64_t &tid, OnReadRecord readfunctor)
{
  serializer<uint8_t, false> s_uint8_t;
  serializer<uint64_t, false> s_uint64_t;

  uint8_t readset_sz, writeset_sz, key_sz, value_sz;
  uint64_t v;

  p = s_uint64_t.read(p, &tid);
  p = s_uint8_t.read(p, &readset_sz);
  assert(size_t(readset_sz) == g_readset);
  for (size_t i = 0; i < size_t(readset_sz); i++) {
    p = s_uint64_t.read(p, &v);
    readfunctor(v);
  }

  p = s_uint8_t.read(p, &writeset_sz);
  assert(size_t(writeset_sz) == g_writeset);
  for (size_t i = 0; i < size_t(writeset_sz); i++) {
    p = s_uint8_t.read(p, &key_sz);
    assert(size_t(key_sz) == g_keysize);
    p += size_t(key_sz);
    p = s_uint8_t.read(p, &value_sz);
    assert(size_t(value_sz) == g_valuesize);
    p += size_t(value_sz);
  }

  return p;
}

static void
logger(int fd)
{
  struct iovec iov[NMAXCORES];
  for (;;) {
    // logger is simple:
    // 1) iterate over g_persist_buffers. if there is a top entry that is not
    // done, then schedule for IO.
    //
    // 2) (should be done during IO) compute persistence by iterating
    // over each entry and checking if deps satisfied. this yields an
    // O(n^2) algorithm for now.
    //
    // 3) once all IO has been fsynced() + persistence computed, return the
    // buffers we can return

    size_t nwritten = 0;
    for (size_t i = 0; i < NMAXCORES; i++) {
      struct pbuffer *px = g_persist_buffers[i].peek();
      if (!px)
        continue;
      if (px && !px->io_scheduled_) {
        iov[nwritten].iov_base = (void *) px->buf_.data();
        iov[nwritten].iov_len = px->curoff_;
        px->io_scheduled_ = true;
        px->curoff_ = sizeof(logbuf_header);
        px->remaining_ = reinterpret_cast<const logbuf_header *>(px->buf_.data())->nentries;
        nwritten++;
      }
    }

    ssize_t ret = writev(fd, &iov[0], nwritten);
    if (ret == -1) {
      perror("writev");
      exit(1);
    }

    int fret = fdatasync(fd);
    if (fret == -1) {
      perror("fdatasync");
      exit(1);
    }

    bool changed = true;
    while (changed) {
      changed = false;
      for (size_t i = 0; i < NMAXCORES; i++) {
        struct pbuffer *px = g_persist_buffers[i].peek();
        if (!px || !px->io_scheduled_)
          continue;
        assert(px->remaining_ > 0);
        assert(px->curoff_ < g_buffer_size);

        const uint8_t *p = px->pointer();
        uint64_t committid;
        bool allsat = true;

        while (allsat) {
          allsat = true;
          const uint8_t *nextp =
            read_log_entry(p, committid, [&allsat](uint64_t readdep) {
              if (!allsat)
                return;
              const uint64_t cid = tidhelpers::CoreId(readdep);
              if (readdep > g_persistence_vc[cid])
                allsat = false;
            });
          if (allsat) {
            assert(tidhelpers::CoreId(committid) == i);
            assert(g_persistence_vc[i] < committid);
            g_persistence_vc[i] = committid;
            changed = true;
            p = nextp;
          } else {
            px->curoff_ = intptr_t(p) - intptr_t(px->buf_.data());
            // done, no further entries will be satisfied
          }
        }

        if (allsat) {
          // finished entire buffer
          g_persist_buffers[i].deq();
          g_all_buffers[i].enq(px);
        }
      }
    }
  }
}

int
main(int argc, char **argv)
{
  g_database.resize(g_nrecords); // all start at TID=0

  for (size_t i = 0; i < NMAXCORES; i++) {
    for (size_t j = 0; j < g_perthread_buffers; j++) {
      struct pbuffer *p = new pbuffer;
      g_all_buffers[i].enq(p);
    }
  }

  int fd = open("data.log", O_CREAT);
  if (fd == -1) {
    perror("open");
    return 1;
  }

  thread logger_thread(logger, fd);
  logger_thread.detach();

  const unsigned nworkers = 4;
  vector<thread> workers;
  for (size_t i = 0; i < nworkers; i++)
    workers.emplace_back(simulateworker, i);

  for (auto &p: workers)
    p.join();
  return 0;
}
