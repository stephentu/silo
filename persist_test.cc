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
#include <getopt.h>

#include "macros.h"
#include "amd64.h"
#include "record/serializer.h"
#include "util.h"

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

  static uint64_t
  vecidmax(uint64_t coremax, const vector<uint64_t> &v)
  {
    uint64_t ret = NumId(coremax);
    for (size_t i = 0; i < v.size(); i++)
      ret = max(ret, NumId(v[i]));
    return ret;
  }
};

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
    return head_.load(memory_order_acquire) ==
           tail_.load(memory_order_acquire) &&
           !buf_[head_.load(memory_order_acquire)].load(memory_order_acquire);
  }


  // assumes there will be capacity
  inline void
  enq(Tp *p)
  {
    assert(p);
    assert(!buf_[head_.load(memory_order_acquire)].load(memory_order_acquire));
    buf_[postincr(head_)].store(p, memory_order_release);
  }

  // blocks until something deqs()
  inline Tp *
  deq()
  {
    while (!buf_[tail_.load(memory_order_acquire)].load(memory_order_acquire))
      nop_pause();
    Tp *ret = buf_[tail_.load(memory_order_acquire)].load(memory_order_acquire);
    buf_[postincr(tail_)].store(nullptr, memory_order_release);
    assert(ret);
    return ret;
  }

  inline Tp *
  peek()
  {
    return buf_[tail_.load(memory_order_acquire)].load(memory_order_acquire);
  }

private:
  static inline unsigned
  postincr(atomic<unsigned> &i)
  {
    const unsigned ret = i.load(memory_order_acquire);
    i.store((ret + 1) % Capacity, memory_order_release);
    return ret;
  }

  atomic<Tp *> buf_[Capacity];
  atomic<unsigned> head_;
  atomic<unsigned> tail_;
};

/** simulate global database state and workload*/

static vector<uint64_t> g_database;

static size_t g_ntxns_committed = 0;

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

static const size_t g_ntxns_worker = 100000;
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

  uint64_t lasttid = 0;

  for (size_t i = 0; i < g_ntxns_worker; i++) {
    for (size_t j = 0; j < g_readset; j++)
      readset[j] = g_database[dist(prng)];

    const uint64_t idmax = tidhelpers::vecidmax(lasttid, readset);
    const uint64_t tidcommit = tidhelpers::MakeTid(id, idmax + 1, 0);
    lasttid = tidcommit;

    for (size_t j = 0; j < g_writeset; j++)
      g_database[dist(prng)] = lasttid;

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
      //cerr << "pushing to logger" << endl;

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
    ((logbuf_header *) curbuf->buf_.data())->nentries++;
  }

  if (curbuf)
    g_persist_buffers[id].enq(curbuf);
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

    if (!nwritten) {
      nop_pause();
      continue;
    }

    ssize_t ret = writev(fd, &iov[0], nwritten);
    if (ret == -1) {
      perror("writev");
      exit(1);
    }

    //cerr << "fsync begin" << endl;
    int fret = fdatasync(fd);
    if (fret == -1) {
      perror("fdatasync");
      exit(1);
    }
    //cerr << "...fsync done" << endl;

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

        while (px->remaining_ && allsat) {
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
            //cerr << "committid=" << committid << endl;
            assert(tidhelpers::CoreId(committid) == i);
            assert(g_persistence_vc[i] < committid);
            g_persistence_vc[i] = committid;
            changed = true;
            p = nextp;
            px->remaining_--;
            px->curoff_ = intptr_t(p) - intptr_t(px->buf_.data());
            g_ntxns_committed++;
          } else {
            // done, no further entries will be satisfied
          }
        }

        if (allsat) {
          assert(px->remaining_ == 0);
          // finished entire buffer
          struct pbuffer *pxcheck = g_persist_buffers[i].deq();
          if (pxcheck != px)
            assert(false);
          g_all_buffers[i].enq(px);
        } else {
          assert(px->remaining_ > 0);
        }
      }
    }
  }
}

int
main(int argc, char **argv)
{
  unsigned nworkers = 1;

  while (1) {
    static struct option long_options[] =
    {
      {"num-threads" , required_argument , 0 , 't'} ,
      {0, 0, 0, 0}
    };
    int option_index = 0;
    int c = getopt_long(argc, argv, "t:", long_options, &option_index);
    if (c == -1)
      break;

    switch (c) {
    case 0:
      if (long_options[option_index].flag != 0)
        break;
      abort();
      break;

    case 't':
      nworkers = strtoul(optarg, nullptr, 10);
      break;

    case '?':
      /* getopt_long already printed an error message. */
      exit(1);

    default:
      abort();
    }
  }
  assert(nworkers >= 1);

  {
    // test circbuf
    int values[] = {0, 1, 2, 3, 4};
    circbuf<int, ARRAY_NELEMS(values)> b;
    assert(b.empty());
    for (size_t i = 0; i < ARRAY_NELEMS(values); i++)
      b.enq(&values[i]);
    for (size_t i = 0; i < ARRAY_NELEMS(values); i++) {
      assert(!b.empty());
      assert(b.peek() == &values[i]);
      assert(*b.peek() == values[i]);
      assert(b.deq() == &values[i]);
    }
    assert(b.empty());
  }

  g_database.resize(g_nrecords); // all start at TID=0

  for (size_t i = 0; i < NMAXCORES; i++) {
    for (size_t j = 0; j < g_perthread_buffers; j++) {
      struct pbuffer *p = new pbuffer;
      g_all_buffers[i].enq(p);
    }
  }

  int fd = open("data.log", O_CREAT|O_WRONLY|O_TRUNC, 0664);
  if (fd == -1) {
    perror("open");
    return 1;
  }

  thread logger_thread(logger, fd);
  logger_thread.detach();


  vector<thread> workers;
  util::timer tt;
  for (size_t i = 0; i < nworkers; i++)
    workers.emplace_back(simulateworker, i);
  for (auto &p: workers)
    p.join();
  const double xsec = tt.lap_ms() / 1000.0;
  const double rate = double(g_ntxns_committed) / xsec;
  cout << rate << endl;

  return 0;
}
