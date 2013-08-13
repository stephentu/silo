/**
 * A stand-alone binary which doesn't depend on the system,
 * used to test the current persistence strategy
 */

#include <cassert>
#include <iostream>
#include <cstdint>
#include <random>
#include <vector>
#include <set>
#include <atomic>
#include <thread>
#include <sstream>

#include <unistd.h>
#include <sys/uio.h>
#include <sys/types.h>
#include <fcntl.h>
#include <getopt.h>
#include <time.h>

#include <lz4.h>

#include "macros.h"
#include "circbuf.h"
#include "amd64.h"
#include "record/serializer.h"
#include "util.h"

using namespace std;
using namespace util;

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

  static string
  Str(uint64_t v)
  {
    ostringstream b;
    b << "[core=" << CoreId(v) << " | n="
      << NumId(v) << " | epoch="
      << EpochId(v) << "]";
    return b.str();
  }

};

//static void
//fillstring(std::string &s, size_t t)
//{
//  s.clear();
//  for (size_t i = 0; i < t; i++)
//    s[i] = (char) i;
//}

template <typename PRNG>
static inline void
fillkey(std::string &s, uint64_t idx, size_t sz, PRNG &prng)
{
  s.resize(sz);
  serializer<uint64_t, false> ser;
  ser.write((uint8_t *) s.data(), idx);
}

template <typename PRNG>
static inline void
fillvalue(std::string &s, uint64_t idx, size_t sz, PRNG &prng)
{
  uniform_int_distribution<uint32_t> dist(0, 10000);
  s.resize(sz);
  serializer<uint32_t, false> s_uint32_t;
  for (size_t i = 0; i < sz; i += sizeof(uint32_t)) {
    if (i + sizeof(uint32_t) <= sz) {
      const uint32_t x = dist(prng);
      s_uint32_t.write((uint8_t *) &s[i], x);
    }
  }
}

/** simulate global database state */

static const size_t g_nrecords = 1000000;
static const size_t g_ntxns_worker = 1000000;
static const size_t g_nmax_loggers = 16;

static vector<uint64_t> g_database;
static atomic<uint64_t> g_ntxns_committed(0);
static atomic<uint64_t> g_ntxns_written(0);
static atomic<uint64_t> g_bytes_written[g_nmax_loggers];

static size_t g_nworkers = 1;
static int g_verbose = 0;
static int g_fsync_background = 0;
static size_t g_readset = 30;
static size_t g_writeset = 16;
static size_t g_keysize = 8; // in bytes
static size_t g_valuesize = 32; // in bytes

/** simulation framework */

// all simulations are epoch based
class database_simulation {
public:
  static const unsigned long g_epoch_time_ns = 30000000; /* 30ms in ns */

  database_simulation()
    : keep_going_(true),
      epoch_thread_(),
      epoch_number_(1), // start at 1 so 0 can be fully persistent initially
      system_sync_epoch_(0)
  {
    // XXX: depends on g_nworkers to be set by now
    for (size_t i = 0; i < g_nworkers; i++)
      per_thread_epochs_[i]->store(1, memory_order_release);
    for (size_t i = 0; i < g_nmax_loggers; i++)
      for (size_t j = 0; j < g_nworkers; j++)
        per_thread_sync_epochs_[i].epochs_[j].store(0, memory_order_release);
  }

  virtual ~database_simulation() {}

  virtual void
  init()
  {
    epoch_thread_ = move(thread(&database_simulation::epoch_thread, this));
  }

  virtual void worker(unsigned id) = 0;

  virtual void logger(const vector<int> &fd,
                      const vector<vector<unsigned>> &assignments) = 0;

  virtual void
  terminate()
  {
    keep_going_->store(false, memory_order_release);
    epoch_thread_.join();
  }

  static bool
  AssignmentsValid(const vector<vector<unsigned>> &assignments,
                   unsigned nfds,
                   unsigned nworkers)
  {
    // each worker must be assigned exactly once in the assignment
    // there must be <= nfds assignments

    if (assignments.size() > nfds)
      return false;

    set<unsigned> seen;
    for (auto &assignment : assignments)
      for (auto w : assignment) {
        if (seen.count(w) || w >= nworkers)
          return false;
        seen.insert(w);
      }

    return seen.size() == nworkers;
  }

protected:
  void
  epoch_thread()
  {
    while (keep_going_->load(memory_order_acquire)) {
      struct timespec t;
      t.tv_sec  = g_epoch_time_ns / ONE_SECOND_NS;
      t.tv_nsec = g_epoch_time_ns % ONE_SECOND_NS;
      nanosleep(&t, nullptr);

      // make sure all threads are at the current epoch
      const uint64_t curepoch = epoch_number_->load(memory_order_acquire);

    retry:
      bool allthere = true;
      for (size_t i = 0;
           i < g_nworkers && keep_going_->load(memory_order_acquire);
           i++) {
        if (per_thread_epochs_[i]->load(memory_order_acquire) < curepoch) {
          allthere = false;
          break;
        }
      }
      if (!keep_going_->load(memory_order_acquire))
        return;
      if (!allthere) {
        nop_pause();
        goto retry;
      }

      //cerr << "bumping epoch" << endl;
      epoch_number_->store(curepoch + 1, memory_order_release); // bump it
    }
  }

  aligned_padded_elem<atomic<bool>> keep_going_;

  thread epoch_thread_;

  aligned_padded_elem<atomic<uint64_t>> epoch_number_;

  aligned_padded_elem<atomic<uint64_t>> per_thread_epochs_[NMAXCORES];

  // v = per_thread_sync_epochs_[i].epochs_[j]: logger i has persisted up
  // through (including) all transactions <= epoch v on core j. since core =>
  // logger mapping is static, taking:
  //   min_{core} max_{logger} per_thread_sync_epochs_[logger].epochs_[core]
  // yields the entire system's persistent epoch
  struct {
    atomic<uint64_t> epochs_[NMAXCORES];
    CACHE_PADOUT;
  } per_thread_sync_epochs_[g_nmax_loggers] CACHE_ALIGNED;

  // conservative estimate (<=) for:
  //   min_{core} max_{logger} per_thread_sync_epochs_[logger].epochs_[core]
  aligned_padded_elem<atomic<uint64_t>> system_sync_epoch_;
};

struct logbuf_header {
  uint64_t nentries_; // > 0 for all valid log buffers
  uint64_t last_tid_; // TID of the last commit
} PACKED;

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

  inline logbuf_header *
  header()
  {
    return (logbuf_header *) buf_.data();
  }

  inline const logbuf_header *
  header() const
  {
    return (const logbuf_header *) buf_.data();
  }
};

class onecopy_logbased_simulation : public database_simulation {
public:
  static const size_t g_perthread_buffers = 64; // 64 outstanding buffers
  static const size_t g_buffer_size = (1<<20); // in bytes
  static const size_t g_horizon_size = (1<<16); // in bytes, for compression only

  static circbuf<pbuffer, g_perthread_buffers> g_all_buffers[NMAXCORES];
  static circbuf<pbuffer, g_perthread_buffers> g_persist_buffers[NMAXCORES];

protected:

  virtual const uint8_t *
  read_log_entry(const uint8_t *p, uint64_t &tid,
                 std::function<void(uint64_t)> readfunctor) = 0;

  virtual uint64_t
  compute_log_record_space() const = 0;

  virtual void
  write_log_record(uint8_t *p,
                   uint64_t tidcommit,
                   const vector<uint64_t> &readset,
                   const vector<pair<string, string>> &writeset) = 0;

  virtual void
  logger_on_io_completion() {}

  virtual bool
  do_compression() const = 0;

  pbuffer *
  getbuffer(unsigned id)
  {
    // block until we get a buf
    pbuffer *ret = g_all_buffers[id].deq();
    ret->io_scheduled_ = false;
    ret->buf_.assign(g_buffer_size, 0);
    ret->curoff_ = sizeof(logbuf_header);
    ret->remaining_ = 0;
    return ret;
  }

public:
  void
  init() OVERRIDE
  {
    database_simulation::init();
    for (size_t i = 0; i < g_nworkers; i++) {
      for (size_t j = 0; j < g_perthread_buffers; j++) {
        struct pbuffer *p = new pbuffer;
        g_all_buffers[i].enq(p);
      }
    }
  }

private:
  inline size_t
  inplace_update_persistent_info(
      vector<pair<uint64_t, uint64_t>> &outstanding_commits,
      uint64_t cursyncepoch)
  {
    size_t ncommits_synced = 0;
    // can erase all entries with x.first <= cursyncepoch
    size_t idx = 0;
    for (; idx < outstanding_commits.size(); idx++) {
      if (outstanding_commits[idx].first <= cursyncepoch)
        ncommits_synced += outstanding_commits[idx].second;
      else
        break;
    }

    // erase entries [0, idx)
    // XXX: slow
    outstanding_commits.erase(outstanding_commits.begin(),
        outstanding_commits.begin() + idx);

    return ncommits_synced;
  }

  inline pbuffer *
  ensure_buffer_with_space(unsigned id, pbuffer *cur, size_t space_needed)
  {
    if (!cur) {
      cur = getbuffer(id);
    } else if (g_buffer_size - cur->curoff_ < space_needed) {
      g_persist_buffers[id].enq(cur);
      cur = getbuffer(id);
    }
    INVARIANT(cur);
    INVARIANT(g_buffer_size - cur->curoff_ >= space_needed);
    return cur;
  }

  /**
   * write the horizon from [p, p+sz) into cur, assuming that cur has enough
   * space. space needed is at least:
   *   sizeof(uint32_t) + LZ4_compressBound(sz)
   *
   * also updates the buffer's headers and offset to reflect the write
   *
   * returns the compressed size of the horizon
   */
  inline uint64_t
  write_horizon(void *lz4ctx,
                const uint8_t *p, uint64_t sz,
                uint64_t nentries, uint64_t lasttid,
                pbuffer *cur)
  {
#ifdef CHECK_INVARIANTS
    const uint64_t needed = sizeof(uint32_t) + LZ4_compressBound(sz);
    INVARIANT(g_buffer_size - cur->curoff_ >= needed);
#endif

    const int ret = LZ4_compress_heap(
        lz4ctx,
        (const char *) p,
        (char *) cur->pointer() + sizeof(uint32_t),
        sz);

    INVARIANT(ret >= 0);
    serializer<uint32_t, false> s_uint32_t;
    s_uint32_t.write(cur->pointer(), ret);
    cur->curoff_ += sizeof(uint32_t) + ret;
    cur->header()->nentries_ += nentries;
    cur->header()->last_tid_ = lasttid;

    return ret;
  }

protected:
  void
  worker(unsigned id) OVERRIDE
  {
    const bool compress = do_compression();
    uint8_t horizon[g_horizon_size]; // LZ4 looks at 65kb windows

    // where are we in the window, how many elems in this window?
    size_t horizon_p = 0, horizon_nentries = 0;
    uint64_t horizon_last_tid = 0; // last committed TID in the horizon

    double cratios = 0.0;
    unsigned long ncompressions = 0;

    void *lz4ctx = nullptr; // holds a heap-allocated LZ4 hash table
    if (compress)
      lz4ctx = LZ4_create();

    mt19937 prng(id);

    // read/write sets are uniform for now
    uniform_int_distribution<unsigned> dist(0, g_nrecords - 1);

    vector<uint64_t> readset(g_readset);
    vector<pair<string, string>> writeset(g_writeset);
    for (auto &pr : writeset) {
      pr.first.reserve(g_keysize);
      pr.second.reserve(g_valuesize);
    }

    struct pbuffer *curbuf = nullptr;
    uint64_t lasttid = 0,
             ncommits_currentepoch = 0,
             ncommits_synced = 0;
    vector<pair<uint64_t, uint64_t>> outstanding_commits;
    for (size_t i = 0; i < g_ntxns_worker; i++) {

      // update epoch info
      const uint64_t lastepoch = per_thread_epochs_[id]->load(memory_order_acquire);
      const uint64_t curepoch = epoch_number_->load(memory_order_acquire);

      if (lastepoch != curepoch) {
        // try to sync outstanding commits
        INVARIANT(curepoch == (lastepoch + 1));
        const size_t cursyncepoch = system_sync_epoch_->load(memory_order_acquire);
        ncommits_synced +=
          inplace_update_persistent_info(outstanding_commits, cursyncepoch);

        // add information about the last epoch
        outstanding_commits.emplace_back(lastepoch, ncommits_currentepoch);
        ncommits_currentepoch = 0;

        per_thread_epochs_[id]->store(curepoch, memory_order_release);
      }

      for (size_t j = 0; j < g_readset; j++)
        readset[j] = g_database[dist(prng)];

      const uint64_t idmax = tidhelpers::vecidmax(lasttid, readset);
      // XXX: ignore future epochs for now
      const uint64_t tidcommit = tidhelpers::MakeTid(id, idmax + 1, curepoch);
      lasttid = tidcommit;

      for (size_t j = 0; j < g_writeset; j++) {
        auto idx = dist(prng);
        g_database[idx] = lasttid;
        fillkey(writeset[j].first, idx, g_keysize, prng);
        fillvalue(writeset[j].second, idx, g_valuesize, prng);
      }

      const uint64_t space_needed = compute_log_record_space();
      if (compress) {
        if (horizon_p + space_needed > g_horizon_size) {
          // need to compress and write horizon
          curbuf = ensure_buffer_with_space(id, curbuf,
            sizeof(uint32_t) + LZ4_compressBound(horizon_p));

          const uint64_t compsz =
            write_horizon(lz4ctx, &horizon[0], horizon_p,
                          horizon_nentries, horizon_last_tid,
                          curbuf);

          const double cratio = double(horizon_p) / double(compsz);
          cratios += cratio;
          ncompressions++;

          // can reset horizon
          horizon_p = horizon_nentries = horizon_last_tid = 0;
        }

        write_log_record(&horizon[0] + horizon_p, tidcommit, readset, writeset);
        horizon_p += space_needed;
        horizon_nentries++;
        horizon_last_tid = tidcommit;
        ncommits_currentepoch++;
      } else {
        curbuf = ensure_buffer_with_space(id, curbuf, space_needed);
        uint8_t *p = curbuf->pointer();
        write_log_record(p, tidcommit, readset, writeset);
        //cerr << "write tidcommit=" << tidhelpers::Str(tidcommit) << endl;
        curbuf->curoff_ += space_needed;
        curbuf->header()->nentries_++;
        curbuf->header()->last_tid_ = tidcommit;
        ncommits_currentepoch++;
      }
    }

    if (compress) {
      if (horizon_nentries) {
        curbuf = ensure_buffer_with_space(id, curbuf,
            sizeof(uint32_t) + LZ4_compressBound(horizon_p));

        const uint64_t compsz =
          write_horizon(lz4ctx, &horizon[0], horizon_p,
                        horizon_nentries, horizon_last_tid,
                        curbuf);

        const double cratio = double(horizon_p) / double(compsz);
        cratios += cratio;
        ncompressions++;

        horizon_p = horizon_nentries = horizon_last_tid = 0;
      }
      LZ4_free(lz4ctx);
    }

    if (curbuf) {
      // XXX: hacky - an agreed upon future epoch for all threads to converge
      // on upon finishing
      const uint64_t FutureEpoch = 100000;
      const uint64_t waitfor = tidhelpers::EpochId(
          curbuf->header()->last_tid_);
      INVARIANT(per_thread_epochs_[id]->load(memory_order_acquire) == waitfor);
      ALWAYS_ASSERT(waitfor < FutureEpoch);
      curbuf->header()->last_tid_ =
        tidhelpers::MakeTid(id, 0, FutureEpoch);
      g_persist_buffers[id].enq(curbuf);
      outstanding_commits.emplace_back(waitfor, ncommits_currentepoch);
      //cerr << "worker " << id << " waitfor epoch " << waitfor << endl;
      // get these commits persisted
      while (system_sync_epoch_->load(memory_order_acquire) < waitfor)
        nop_pause();
      ncommits_synced +=
        inplace_update_persistent_info(outstanding_commits, waitfor);
      ALWAYS_ASSERT(outstanding_commits.empty());
    }

    if (g_verbose && compress)
      cerr << "Average compression ratio: " << cratios / double(ncompressions) << endl;

    g_ntxns_committed.fetch_add(ncommits_synced, memory_order_release);
  }

private:
  void
  fsyncer(unsigned id, int fd, one_way_post<int> &channel)
  {
    for (;;) {
      int ret;
      channel.peek(ret);
      if (ret == -1)
        return;
      ret = fdatasync(fd);
      if (ret == -1) {
        perror("fdatasync");
        exit(1);
      }
      channel.consume(ret);
    }
  }

  void
  writer(unsigned id, int fd, const vector<unsigned> &assignment)
  {
    vector<iovec> iovs(g_nworkers * g_perthread_buffers);
    vector<pbuffer *> pxs;
    struct timespec last_io_completed;
    one_way_post<int> *channel =
      g_fsync_background ? new one_way_post<int> : nullptr;
    uint64_t total_nbytes_written = 0,
             total_txns_written = 0;

    bool sense = false; // cur is at sense, prev is at !sense
    uint64_t nbytes_written[2], txns_written[2], epoch_prefixes[2][g_nworkers];
    memset(&nbytes_written[0], 0, sizeof(nbytes_written));
    memset(&txns_written[0], 0, sizeof(txns_written));
    memset(&epoch_prefixes[0], 0, sizeof(epoch_prefixes[0]));
    memset(&epoch_prefixes[1], 0, sizeof(epoch_prefixes[1]));

    clock_gettime(CLOCK_MONOTONIC, &last_io_completed);
    thread fsync_thread;
    if (g_fsync_background) {
      fsync_thread = move(thread(
            &onecopy_logbased_simulation::fsyncer, this, id, fd, ref(*channel)));
      fsync_thread.detach();
    }

    while (keep_going_->load(memory_order_acquire)) {

      // don't allow this loop to proceed less than an epoch's worth of time,
      // so we can batch IO
      struct timespec now, diff;
      clock_gettime(CLOCK_MONOTONIC, &now);
      timespec_utils::subtract(&now, &last_io_completed, &diff);
      if (diff.tv_sec == 0 && diff.tv_nsec < long(g_epoch_time_ns)) {
        // need to sleep it out
        struct timespec ts;
        ts.tv_sec = 0;
        ts.tv_nsec = g_epoch_time_ns - diff.tv_nsec;
        nanosleep(&ts, nullptr);
      }
      clock_gettime(CLOCK_MONOTONIC, &last_io_completed);

      size_t nwritten = 0;
      nbytes_written[sense] = txns_written[sense] = 0;
      for (auto idx : assignment) {
        INVARIANT(idx >= 0 && idx < g_nworkers);
        g_persist_buffers[idx].peekall(pxs);
        for (auto px : pxs) {
          INVARIANT(px);
          INVARIANT(!px->io_scheduled_);
          iovs[nwritten].iov_base = (void *) px->buf_.data();
          iovs[nwritten].iov_len = px->curoff_;
          nbytes_written[sense] += px->curoff_;
          px->io_scheduled_ = true;
          px->curoff_ = sizeof(logbuf_header);
          px->remaining_ = px->header()->nentries_;
          txns_written[sense] += px->header()->nentries_;
          nwritten++;
          INVARIANT(tidhelpers::CoreId(px->header()->last_tid_) == idx);
          INVARIANT(epoch_prefixes[sense][idx] <=
                    tidhelpers::EpochId(px->header()->last_tid_));
          INVARIANT(tidhelpers::EpochId(px->header()->last_tid_) > 0);
          epoch_prefixes[sense][idx] =
            tidhelpers::EpochId(px->header()->last_tid_) - 1;
        }
      }

      if (!nwritten) {
        // XXX: should probably sleep here
        nop_pause();
        if (!g_fsync_background || !channel->can_post()) {
          //cerr << "writer skipping because no work to do" << endl;
          continue;
        }
      }

      //cerr << "writer " << id << " nwritten " << nwritten << endl;

      const ssize_t ret =
        nwritten ? writev(fd, &iovs[0], nwritten) : 0;
      if (ret == -1) {
        perror("writev");
        exit(1);
      }

      bool dosense;
      if (g_fsync_background) {
        // wait for fsync from the previous write
        if (nwritten)
          channel->post(0, true);
        else
          INVARIANT(channel->can_post());
        dosense = !sense;
      } else {
        int ret = fdatasync(fd);
        if (ret == -1) {
          perror("fdatasync");
          exit(1);
        }
        dosense = sense;
      }

      // update metadata from previous write
      for (size_t i = 0; i < g_nworkers; i++) {
        const uint64_t x0 =
          per_thread_sync_epochs_[id].epochs_[i].load(memory_order_acquire);
        const uint64_t x1 = epoch_prefixes[dosense][i];
        if (x1 > x0)
          per_thread_sync_epochs_[id].epochs_[i].store(
              x1, memory_order_release);
      }
      total_nbytes_written += nbytes_written[dosense];
      total_txns_written += txns_written[dosense];

      // bump the sense
      sense = !sense;

      // return all buffers that have been io_scheduled_ - we can do this as
      // soon as write returns
      for (auto idx : assignment) {
        pbuffer *px;
        while ((px = g_persist_buffers[idx].peek()) &&
               px->io_scheduled_) {
          g_persist_buffers[idx].deq();
          g_all_buffers[idx].enq(px);
        }
      }
    }

    g_bytes_written[id].store(total_nbytes_written, memory_order_release);
    g_ntxns_written.fetch_add(total_txns_written, memory_order_release);
  }

  inline void
  advance_system_sync_epoch(const vector<vector<unsigned>> &assignments)
  {
    uint64_t min_so_far = numeric_limits<uint64_t>::max();
    for (size_t i = 0; i < assignments.size(); i++)
      for (auto j : assignments[i])
        min_so_far =
          min(per_thread_sync_epochs_[i].epochs_[j].load(memory_order_acquire), min_so_far);

#ifdef CHECK_INVARIANTS
    const uint64_t syssync = system_sync_epoch_->load(memory_order_acquire);
    INVARIANT(syssync <= min_so_far);
#endif
    system_sync_epoch_->store(min_so_far, memory_order_release);
  }

public:
  void
  logger(const vector<int> &fds,
         const vector<vector<unsigned>> &assignments_given) OVERRIDE
  {
    // compute thread => logger assignment
    vector<thread> writers;
    vector<vector<unsigned>> assignments(assignments_given);

    if (assignments.empty()) {
      // compute assuming homogenous disks
      if (g_nworkers <= fds.size()) {
        // each thread gets its own logging worker
        for (size_t i = 0; i < g_nworkers; i++)
          assignments.push_back({(unsigned) i});
      } else {
        // XXX: currently we assume each logger is equally as fast- we should
        // adjust ratios accordingly for non-homogenous loggers
        const size_t threads_per_logger = g_nworkers / fds.size();
        for (size_t i = 0; i < fds.size(); i++) {
          assignments.emplace_back(
            MakeRange<unsigned>(
                i * threads_per_logger,
                ((i + 1) == fds.size()) ?
                  g_nworkers :
                  (i + 1) * threads_per_logger));
        }
      }
    }

    INVARIANT(AssignmentsValid(assignments, fds.size(), g_nworkers));

    timer tt;
    for (size_t i = 0; i < assignments.size(); i++)
      writers.emplace_back(
        &onecopy_logbased_simulation::writer,
        this, i, fds[i], ref(assignments[i]));
    if (g_verbose)
      cerr << "assignments: " << assignments << endl;
    while (keep_going_->load(memory_order_acquire)) {
      // periodically compute which epoch is the persistence epoch,
      // and update system_sync_epoch_

      struct timespec t;
      t.tv_sec  = g_epoch_time_ns / ONE_SECOND_NS;
      t.tv_nsec = g_epoch_time_ns % ONE_SECOND_NS;
      nanosleep(&t, nullptr);

      advance_system_sync_epoch(assignments);
    }

    for (auto &t : writers)
      t.join();

    if (g_verbose) {
      cerr << "current epoch: " << epoch_number_->load(memory_order_acquire) << endl;
      cerr << "sync epoch   : " << system_sync_epoch_->load(memory_order_acquire) << endl;
      const double xsec = tt.lap_ms() / 1000.0;
      for (size_t i = 0; i < writers.size(); i++)
        cerr << "writer " << i << " " <<
          (double(g_bytes_written[i].load(memory_order_acquire)) /
           double(1UL << 20) /
           xsec) << " MB/sec" << endl;
    }
  }

protected:
  vector<pbuffer *> pxs_; // just some scratch space
};

circbuf<pbuffer, onecopy_logbased_simulation::g_perthread_buffers>
  onecopy_logbased_simulation::g_all_buffers[NMAXCORES];
circbuf<pbuffer, onecopy_logbased_simulation::g_perthread_buffers>
  onecopy_logbased_simulation::g_persist_buffers[NMAXCORES];

class explicit_deptracking_simulation : public onecopy_logbased_simulation {
public:

  /** global state about our persistence calculations */

  // contains the latest TID inclusive, per core, which is (transitively)
  // persistent. note that the prefix of the DB which is totally persistent is
  // simply the max of this table.
  static uint64_t g_persistence_vc[NMAXCORES];

protected:

  bool do_compression() const OVERRIDE { return false; }

  const uint8_t *
  read_log_entry(const uint8_t *p, uint64_t &tid,
                 std::function<void(uint64_t)> readfunctor) OVERRIDE
  {
    serializer<uint8_t, false> s_uint8_t;
    serializer<uint64_t, false> s_uint64_t;

    uint8_t readset_sz, writeset_sz, key_sz, value_sz;
    uint64_t v;

    p = s_uint64_t.read(p, &tid);
    p = s_uint8_t.read(p, &readset_sz);
    INVARIANT(size_t(readset_sz) == g_readset);
    for (size_t i = 0; i < size_t(readset_sz); i++) {
      p = s_uint64_t.read(p, &v);
      readfunctor(v);
    }

    p = s_uint8_t.read(p, &writeset_sz);
    INVARIANT(size_t(writeset_sz) == g_writeset);
    for (size_t i = 0; i < size_t(writeset_sz); i++) {
      p = s_uint8_t.read(p, &key_sz);
      INVARIANT(size_t(key_sz) == g_keysize);
      p += size_t(key_sz);
      p = s_uint8_t.read(p, &value_sz);
      INVARIANT(size_t(value_sz) == g_valuesize);
      p += size_t(value_sz);
    }

    return p;
  }

  uint64_t
  compute_log_record_space() const OVERRIDE
  {
    // compute how much space we need for this entry
    uint64_t space_needed = 0;

    // 8 bytes to indicate TID
    space_needed += sizeof(uint64_t);

    // one byte to indicate # of read deps
    space_needed += 1;

    // each dep occupies 8 bytes
    space_needed += g_readset * sizeof(uint64_t);

    // one byte to indicate # of records written
    space_needed += 1;

    // each record occupies (1 + key_length + 1 + value_length) bytes
    space_needed += g_writeset * (1 + g_keysize + 1 + g_valuesize);

    return space_needed;
  }

  void
  write_log_record(uint8_t *p,
                   uint64_t tidcommit,
                   const vector<uint64_t> &readset,
                   const vector<pair<string, string>> &writeset) OVERRIDE
  {
    serializer<uint8_t, false> s_uint8_t;
    serializer<uint64_t, false> s_uint64_t;

    p = s_uint64_t.write(p, tidcommit);
    p = s_uint8_t.write(p, readset.size());
    for (auto t : readset)
      p = s_uint64_t.write(p, t);
    p = s_uint8_t.write(p, writeset.size());
    for (auto &pr : writeset) {
      p = s_uint8_t.write(p, pr.first.size());
      memcpy(p, pr.first.data(), pr.first.size()); p += pr.first.size();
      p = s_uint8_t.write(p, pr.second.size());
      memcpy(p, pr.second.data(), pr.second.size()); p += pr.second.size();
    }
  }

  void
  logger_on_io_completion() OVERRIDE
  {
    ALWAYS_ASSERT(false); // currently broken
    bool changed = true;
    while (changed) {
      changed = false;
      for (size_t i = 0; i < NMAXCORES; i++) {
        g_persist_buffers[i].peekall(pxs_);
        for (auto px : pxs_) {
          INVARIANT(px);
          if (!px->io_scheduled_)
            break;

          INVARIANT(px->remaining_ > 0);
          INVARIANT(px->curoff_ < g_buffer_size);

          const uint8_t *p = px->pointer();
          uint64_t committid;
          bool allsat = true;

          //cerr << "processing buffer " << px << " with curoff_=" << px->curoff_ << endl
          //     << "  p=" << intptr_t(p) << endl;

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
              //cerr << "committid=" << tidhelpers::Str(committid)
              //     << ", g_persistence_vc=" << tidhelpers::Str(g_persistence_vc[i])
              //     << endl;
              INVARIANT(tidhelpers::CoreId(committid) == i);
              INVARIANT(g_persistence_vc[i] < committid);
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
            INVARIANT(px->remaining_ == 0);
            // finished entire buffer
            struct pbuffer *pxcheck = g_persist_buffers[i].deq();
            if (pxcheck != px)
              INVARIANT(false);
            g_all_buffers[i].enq(px);
            //cerr << "buffer flused at g_persistence_vc=" << tidhelpers::Str(g_persistence_vc[i]) << endl;
          } else {
            INVARIANT(px->remaining_ > 0);
            break; // cannot process core's list any further
          }
        }
      }
    }
  }

};

uint64_t explicit_deptracking_simulation::g_persistence_vc[NMAXCORES] = {0};

class epochbased_simulation : public onecopy_logbased_simulation {
public:
  epochbased_simulation(bool compress)
    : compress_(compress)
  {
  }

protected:
  bool do_compression() const OVERRIDE { return compress_; }

protected:
  const uint8_t *
  read_log_entry(const uint8_t *p, uint64_t &tid,
                 std::function<void(uint64_t)> readfunctor) OVERRIDE
  {
    serializer<uint8_t, false> s_uint8_t;
    serializer<uint64_t, false> s_uint64_t;

    uint8_t writeset_sz, key_sz, value_sz;

    p = s_uint64_t.read(p, &tid);
    p = s_uint8_t.read(p, &writeset_sz);
    INVARIANT(size_t(writeset_sz) == g_writeset);
    for (size_t i = 0; i < size_t(writeset_sz); i++) {
      p = s_uint8_t.read(p, &key_sz);
      INVARIANT(size_t(key_sz) == g_keysize);
      p += size_t(key_sz);
      p = s_uint8_t.read(p, &value_sz);
      INVARIANT(size_t(value_sz) == g_valuesize);
      p += size_t(value_sz);
    }

    return p;
  }

  uint64_t
  compute_log_record_space() const OVERRIDE
  {
    // compute how much space we need for this entry
    uint64_t space_needed = 0;

    // 8 bytes to indicate TID
    space_needed += sizeof(uint64_t);

    // one byte to indicate # of records written
    space_needed += 1;

    // each record occupies (1 + key_length + 1 + value_length) bytes
    space_needed += g_writeset * (1 + g_keysize + 1 + g_valuesize);

    return space_needed;
  }

  void
  write_log_record(uint8_t *p,
                   uint64_t tidcommit,
                   const vector<uint64_t> &readset,
                   const vector<pair<string, string>> &writeset) OVERRIDE
  {
    serializer<uint8_t, false> s_uint8_t;
    serializer<uint64_t, false> s_uint64_t;

    p = s_uint64_t.write(p, tidcommit);
    p = s_uint8_t.write(p, writeset.size());
    for (auto &pr : writeset) {
      p = s_uint8_t.write(p, pr.first.size());
      memcpy(p, pr.first.data(), pr.first.size()); p += pr.first.size();
      p = s_uint8_t.write(p, pr.second.size());
      memcpy(p, pr.second.data(), pr.second.size()); p += pr.second.size();
    }
  }

private:
  bool compress_;
};

int
main(int argc, char **argv)
{
  string strategy = "epoch";
  vector<string> logfiles;
  vector<vector<unsigned>> assignments;

  while (1) {
    static struct option long_options[] =
    {
      {"verbose"     , no_argument       , &g_verbose , 1}   ,
      {"fsync-back"  , no_argument       , &g_fsync_background, 1},
      {"num-threads" , required_argument , 0          , 't'} ,
      {"strategy"    , required_argument , 0          , 's'} ,
      {"readset"     , required_argument , 0          , 'r'} ,
      {"writeset"    , required_argument , 0          , 'w'} ,
      {"keysize"     , required_argument , 0          , 'k'} ,
      {"valuesize"   , required_argument , 0          , 'v'} ,
      {"logfile"     , required_argument , 0          , 'l'} ,
      {"assignment"  , required_argument , 0          , 'a'} ,
      {0, 0, 0, 0}
    };
    int option_index = 0;
    int c = getopt_long(argc, argv, "t:s:r:w:k:v:l:a:", long_options, &option_index);
    if (c == -1)
      break;

    switch (c) {
    case 0:
      if (long_options[option_index].flag != 0)
        break;
      abort();
      break;

    case 't':
      g_nworkers = strtoul(optarg, nullptr, 10);
      break;

    case 's':
      strategy = optarg;
      break;

    case 'r':
      g_readset = strtoul(optarg, nullptr, 10);
      break;

    case 'w':
      g_writeset = strtoul(optarg, nullptr, 10);
      break;

    case 'k':
      g_keysize = strtoul(optarg, nullptr, 10);
      break;

    case 'v':
      g_valuesize = strtoul(optarg, nullptr, 10);
      break;

    case 'l':
      logfiles.emplace_back(optarg);
      break;

    case 'a':
      assignments.emplace_back(
          ParseCSVString<unsigned, RangeAwareParser<unsigned>>(optarg));
      break;

    case '?':
      /* getopt_long already printed an error message. */
      exit(1);

    default:
      abort();
    }
  }
  ALWAYS_ASSERT(g_nworkers >= 1);
  ALWAYS_ASSERT(g_readset >= 0);
  ALWAYS_ASSERT(g_writeset > 0);
  ALWAYS_ASSERT(g_keysize > 0);
  ALWAYS_ASSERT(g_valuesize >= 0);
  ALWAYS_ASSERT(!logfiles.empty());
  ALWAYS_ASSERT(logfiles.size() <= g_nmax_loggers);
  ALWAYS_ASSERT(
      assignments.empty() ||
      database_simulation::AssignmentsValid(
        assignments, logfiles.size(), g_nworkers));

  if (g_verbose)
    cerr << "{nworkers=" << g_nworkers
         << ", readset=" << g_readset
         << ", writeset=" << g_writeset
         << ", keysize=" << g_keysize
         << ", valuesize=" << g_valuesize
         << ", logfiles=" << logfiles
         << ", strategy=" << strategy
         << ", fsync_background=" << g_fsync_background
         << ", assignments=" << assignments
         << "}" << endl;

  if (strategy != "deptracking" &&
      strategy != "epoch" &&
      strategy != "epoch-compress")
    ALWAYS_ASSERT(false);

  g_database.resize(g_nrecords); // all start at TID=0

  vector<int> fds;
  for (auto &fname : logfiles) {
    int fd = open(fname.c_str(), O_CREAT|O_WRONLY|O_TRUNC, 0664);
    if (fd == -1) {
      perror("open");
      return 1;
    }
    fds.push_back(fd);
  }

  unique_ptr<database_simulation> sim;
  if (strategy == "deptracking")
    sim.reset(new explicit_deptracking_simulation);
  else if (strategy == "epoch")
    sim.reset(new epochbased_simulation(false));
  else if (strategy == "epoch-compress")
    sim.reset(new epochbased_simulation(true));
  else
    ALWAYS_ASSERT(false);
  sim->init();

  thread logger_thread(
      &database_simulation::logger, sim.get(), fds, ref(assignments));

  vector<thread> workers;
  util::timer tt, tt1;
  for (size_t i = 0; i < g_nworkers; i++)
    workers.emplace_back(&database_simulation::worker, sim.get(), i);
  for (auto &p: workers)
    p.join();
  sim->terminate();
  logger_thread.join();

  const double ntxns_committed = g_ntxns_committed.load();
  const double xsec = tt.lap_ms() / 1000.0;
  const double rate = double(ntxns_committed) / xsec;
  if (g_verbose) {
    cerr << "txns commited rate: " << rate << " txns/sec" << endl;
    cerr << "  (" << size_t(ntxns_committed) << " in " << xsec << " sec)" << endl;

    const double ntxns_written = g_ntxns_written.load();
    const double rate1 = double(ntxns_written) / xsec;
    cerr << "txns written rate: " << rate1 << " txns/sec" << endl;
    cerr << "  (" << size_t(ntxns_written) << " in " << xsec << " sec)" << endl;
  } else {
    cout << rate << endl;
  }

  return 0;
}
