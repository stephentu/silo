#ifndef _NDB_TXN_PROTO2_IMPL_H_
#define _NDB_TXN_PROTO2_IMPL_H_

#include <iostream>
#include <atomic>
#include <vector>
#include <set>

#include <lz4.h>

#include "txn.h"
#include "txn_impl.h"
#include "txn_btree.h"
#include "macros.h"
#include "circbuf.h"
#include "spinbarrier.h"
#include "record/serializer.h"

// forward decl
template <typename Traits> class transaction_proto2;
template <template <typename> class Transaction>
  class txn_epoch_sync;

// the system has a single logging subsystem (composed of multiple lgogers)
// NOTE: currently, the persistence epoch is tied 1:1 with the ticker's epoch
class txn_logger {
  friend class transaction_proto2_static;
  template <typename T>
    friend class transaction_proto2;
  // XXX: should only allow txn_epoch_sync<transaction_proto2> as friend
  template <template <typename> class T>
    friend class txn_epoch_sync;
public:

  static const size_t g_nmax_loggers = 16;
  static const size_t g_perthread_buffers = 256; // 256 outstanding buffers
  static const size_t g_buffer_size = (1<<20); // in bytes
  static const size_t g_horizon_buffer_size = 2 * (1<<16); // in bytes
  static const size_t g_max_lag_epochs = 128; // cannot lag more than 128 epochs
  static const bool   g_pin_loggers_to_numa_nodes = false;

  static inline bool
  IsPersistenceEnabled()
  {
    return g_persist;
  }

  static inline bool
  IsCompressionEnabled()
  {
    return g_use_compression;
  }

  // init the logging subsystem.
  //
  // should only be called ONCE is not thread-safe.  if assignments_used is not
  // null, then fills it with a copy of the assignment actually computed
  static void Init(
      size_t nworkers,
      const std::vector<std::string> &logfiles,
      const std::vector<std::vector<unsigned>> &assignments_given,
      std::vector<std::vector<unsigned>> *assignments_used = nullptr,
      bool call_fsync = true,
      bool use_compression = false,
      bool fake_writes = false);

  struct logbuf_header {
    uint64_t nentries_; // > 0 for all valid log buffers
    uint64_t last_tid_; // TID of the last commit
  } PACKED;

  struct pbuffer {
    uint64_t earliest_start_us_; // start time of the earliest txn
    bool io_scheduled_; // has the logger scheduled IO yet?

    unsigned curoff_; // current offset into buf_ for writing

    const unsigned core_id_; // which core does this pbuffer belong to?

    const unsigned buf_sz_;

    // must be last field
    uint8_t buf_start_[0];

    // to allocate a pbuffer, use placement new:
    //    const size_t bufsz = ...;
    //    char *p = malloc(sizeof(pbuffer) + bufsz);
    //    pbuffer *pb = new (p) pbuffer(core_id, bufsz);
    //
    // NOTE: it is not necessary to call the destructor for pbuffer, since
    // it only contains PODs
    pbuffer(unsigned core_id, unsigned buf_sz)
      : core_id_(core_id), buf_sz_(buf_sz)
    {
      INVARIANT(((char *)this) + sizeof(*this) == (char *) &buf_start_[0]);
      INVARIANT(buf_sz > sizeof(logbuf_header));
      reset();
    }

    pbuffer(const pbuffer &) = delete;
    pbuffer &operator=(const pbuffer &) = delete;
    pbuffer(pbuffer &&) = delete;

    inline void
    reset()
    {
      earliest_start_us_ = 0;
      io_scheduled_ = false;
      curoff_ = sizeof(logbuf_header);
      NDB_MEMSET(&buf_start_[0], 0, buf_sz_);
    }

    inline uint8_t *
    pointer()
    {
      INVARIANT(curoff_ >= sizeof(logbuf_header));
      INVARIANT(curoff_ <= buf_sz_);
      return &buf_start_[0] + curoff_;
    }

    inline uint8_t *
    datastart()
    {
      return &buf_start_[0] + sizeof(logbuf_header);
    }

    inline size_t
    datasize() const
    {
      INVARIANT(curoff_ >= sizeof(logbuf_header));
      INVARIANT(curoff_ <= buf_sz_);
      return curoff_ - sizeof(logbuf_header);
    }

    inline logbuf_header *
    header()
    {
      return reinterpret_cast<logbuf_header *>(&buf_start_[0]);
    }

    inline const logbuf_header *
    header() const
    {
      return reinterpret_cast<const logbuf_header *>(&buf_start_[0]);
    }

    inline size_t
    space_remaining() const
    {
      INVARIANT(curoff_ >= sizeof(logbuf_header));
      INVARIANT(curoff_ <= buf_sz_);
      return buf_sz_ - curoff_;
    }

    inline bool
    can_hold_tid(uint64_t tid) const;
  } PACKED;

  static bool
  AssignmentsValid(const std::vector<std::vector<unsigned>> &assignments,
                   unsigned nfds,
                   unsigned nworkers)
  {
    // each worker must be assigned exactly once in the assignment
    // there must be <= nfds assignments

    if (assignments.size() > nfds)
      return false;

    std::set<unsigned> seen;
    for (auto &assignment : assignments)
      for (auto w : assignment) {
        if (seen.count(w) || w >= nworkers)
          return false;
        seen.insert(w);
      }

    return seen.size() == nworkers;
  }

  typedef circbuf<pbuffer, g_perthread_buffers> pbuffer_circbuf;

  static std::tuple<uint64_t, uint64_t, double>
  compute_ntxns_persisted_statistics();

  // purge counters from each thread about the number of
  // persisted txns
  static void
  clear_ntxns_persisted_statistics();

  // wait until the logging system appears to be idle.
  //
  // note that this isn't a guarantee, just a best effort attempt
  static void
  wait_for_idle_state();

  // waits until the epoch on invocation time is persisted
  static void
  wait_until_current_point_persisted();

private:

  // data structures

  struct epoch_array {
    // don't use percore<std::atomic<uint64_t>> because we don't want padding
    std::atomic<uint64_t> epochs_[NMAXCORES];
    std::atomic<uint64_t> dummy_work_; // so we can do some fake work
    CACHE_PADOUT;
  };

  struct persist_ctx {
    bool init_;

    void *lz4ctx_;     // for compression
    pbuffer *horizon_; // for compression

    circbuf<pbuffer, g_perthread_buffers> all_buffers_;     // logger pushes to core
    circbuf<pbuffer, g_perthread_buffers> persist_buffers_; // core pushes to logger

    persist_ctx() : init_(false), lz4ctx_(nullptr), horizon_(nullptr) {}
  };

  // context per one epoch
  struct persist_stats {
    // how many txns this thread has persisted in total
    std::atomic<uint64_t> ntxns_persisted_;

    // how many txns have been pushed to the logger (but not necessarily persisted)
    std::atomic<uint64_t> ntxns_pushed_;

    // committed (but not necessarily pushed, nor persisted)
    std::atomic<uint64_t> ntxns_committed_;

    // sum of all latencies (divid by ntxns_persisted_ to get avg latency in
    // us) for *persisted* txns (is conservative)
    std::atomic<uint64_t> latency_numer_;

    // per last g_max_lag_epochs information
    struct per_epoch_stats {
      std::atomic<uint64_t> ntxns_;
      std::atomic<uint64_t> earliest_start_us_;

      per_epoch_stats() : ntxns_(0), earliest_start_us_(0) {}
    } d_[g_max_lag_epochs];

    persist_stats() :
      ntxns_persisted_(0), ntxns_pushed_(0),
      ntxns_committed_(0), latency_numer_(0) {}
  };

  // helpers

  static void
  advance_system_sync_epoch(
      const std::vector<std::vector<unsigned>> &assignments);

  // makes copy on purpose
  static void writer(
      unsigned id, int fd,
      std::vector<unsigned> assignment);

  static void persister(
      std::vector<std::vector<unsigned>> assignments);

  enum InitMode {
    INITMODE_NONE, // no initialization
    INITMODE_REG,  // just use malloc() to init buffers
    INITMODE_RCU,  // try to use the RCU numa aware allocator
  };

  static inline persist_ctx &
  persist_ctx_for(uint64_t core_id, InitMode imode)
  {
    INVARIANT(core_id < g_persist_ctxs.size());
    persist_ctx &ctx = g_persist_ctxs[core_id];
    if (unlikely(!ctx.init_ && imode != INITMODE_NONE)) {
      size_t needed = g_perthread_buffers * (sizeof(pbuffer) + g_buffer_size);
      if (IsCompressionEnabled())
        needed += size_t(LZ4_create_size()) +
          sizeof(pbuffer) + g_horizon_buffer_size;
      char *mem =
        (imode == INITMODE_REG) ?
          (char *) malloc(needed) :
          (char *) rcu::s_instance.alloc_static(needed);
      if (IsCompressionEnabled()) {
        ctx.lz4ctx_ = mem;
        mem += LZ4_create_size();
        ctx.horizon_ = new (mem) pbuffer(core_id, g_horizon_buffer_size);
        mem += sizeof(pbuffer) + g_horizon_buffer_size;
      }
      for (size_t i = 0; i < g_perthread_buffers; i++) {
        ctx.all_buffers_.enq(new (mem) pbuffer(core_id, g_buffer_size));
        mem += sizeof(pbuffer) + g_buffer_size;
      }
      ctx.init_ = true;
    }
    return ctx;
  }

  // static state

  static bool g_persist; // whether or not logging is enabled

  static bool g_call_fsync; // whether or not fsync() needs to be called
                            // in order to be considered durable

  static bool g_use_compression; // whether or not to compress log buffers

  static bool g_fake_writes; // whether or not to fake doing writes (to measure
                             // pure overhead of disk)

  static size_t g_nworkers; // assignments are computed based on g_nworkers
                            // but a logger responsible for core i is really
                            // responsible for cores i + k * g_nworkers, for k
                            // >= 0

  // v = per_thread_sync_epochs_[i].epochs_[j]: logger i has persisted up
  // through (including) all transactions <= epoch v on core j. since core =>
  // logger mapping is static, taking:
  //   min_{core} max_{logger} per_thread_sync_epochs_[logger].epochs_[core]
  // yields the entire system's persistent epoch
  static epoch_array
    per_thread_sync_epochs_[g_nmax_loggers] CACHE_ALIGNED;

  // conservative estimate (<=) for:
  //   min_{core} max_{logger} per_thread_sync_epochs_[logger].epochs_[core]
  static util::aligned_padded_elem<std::atomic<uint64_t>>
    system_sync_epoch_ CACHE_ALIGNED;

  static percore<persist_ctx> g_persist_ctxs CACHE_ALIGNED;

  static percore<persist_stats> g_persist_stats CACHE_ALIGNED;

  // counters

  static event_counter g_evt_log_buffer_epoch_boundary;
  static event_counter g_evt_log_buffer_out_of_space;
  static event_counter g_evt_log_buffer_bytes_before_compress;
  static event_counter g_evt_log_buffer_bytes_after_compress;
  static event_counter g_evt_logger_writev_limit_met;
  static event_counter g_evt_logger_max_lag_wait;
  static event_avg_counter g_evt_avg_log_entry_ntxns;
  static event_avg_counter g_evt_avg_log_buffer_compress_time_us;
  static event_avg_counter g_evt_avg_logger_bytes_per_writev;
  static event_avg_counter g_evt_avg_logger_bytes_per_sec;
};

static inline std::ostream &
operator<<(std::ostream &o, txn_logger::logbuf_header &hdr)
{
  o << "{nentries_=" << hdr.nentries_ << ", last_tid_="
    << g_proto_version_str(hdr.last_tid_) << "}";
  return o;
}

class transaction_proto2_static {
public:

  // NOTE:
  // each epoch is tied (1:1) to the ticker subsystem's tick. this is the
  // speed of the persistence layer.
  //
  // however, read only txns and GC are tied to multiples of the ticker
  // subsystem's tick

#ifdef CHECK_INVARIANTS
  static const uint64_t ReadOnlyEpochMultiplier = 10; /* 10 * 1 ms */
#else
  static const uint64_t ReadOnlyEpochMultiplier = 25; /* 25 * 40 ms */
  static_assert(ticker::tick_us * ReadOnlyEpochMultiplier == 1000000, "");
#endif

  static_assert(ReadOnlyEpochMultiplier >= 1, "XX");

  static const uint64_t ReadOnlyEpochUsec =
    ticker::tick_us * ReadOnlyEpochMultiplier;

  static inline uint64_t constexpr
  to_read_only_tick(uint64_t epoch_tick)
  {
    return epoch_tick / ReadOnlyEpochMultiplier;
  }

  // in this protocol, the version number is:
  // (note that for tid_t's, the top bit is reserved and
  // *must* be set to zero
  //
  // [ core  | number |  epoch | reserved ]
  // [ 0..9  | 9..33  | 33..63 |  63..64  ]

  static inline ALWAYS_INLINE
  uint64_t CoreId(uint64_t v)
  {
    return v & CoreMask;
  }

  static inline ALWAYS_INLINE
  uint64_t NumId(uint64_t v)
  {
    return (v & NumIdMask) >> NumIdShift;
  }

  static inline ALWAYS_INLINE
  uint64_t EpochId(uint64_t v)
  {
    return (v & EpochMask) >> EpochShift;
  }

  // XXX(stephentu): HACK
  static void
  wait_an_epoch()
  {
    INVARIANT(!rcu::s_instance.in_rcu_region());
    const uint64_t e = to_read_only_tick(
        ticker::s_instance.global_last_tick_exclusive());
    if (!e) {
      std::cerr << "wait_an_epoch(): consistent reads happening in e-1, but e=0 so special case"
                << std::endl;
    } else {
      std::cerr << "wait_an_epoch(): consistent reads happening in e-1: "
                << (e-1) << std::endl;
    }
    while (to_read_only_tick(ticker::s_instance.global_last_tick_exclusive()) == e)
      nop_pause();
    COMPILER_MEMORY_FENCE;
  }

  static uint64_t
  ComputeReadOnlyTid(uint64_t global_tick_ex)
  {
    const uint64_t a = (global_tick_ex / ReadOnlyEpochMultiplier);
    const uint64_t b = a * ReadOnlyEpochMultiplier;

    // want to read entries <= b-1, special casing for b=0
    if (!b)
      return MakeTid(0, 0, 0);
    else
      return MakeTid(CoreMask, NumIdMask >> NumIdShift, b - 1);
  }

  static const uint64_t NBitsNumber = 24;

  // XXX(stephentu): need to implement core ID recycling
  static const size_t CoreBits = NMAXCOREBITS; // allow 2^CoreShift distinct threads
  static const size_t NMaxCores = NMAXCORES;

  static const uint64_t CoreMask = (NMaxCores - 1);

  static const uint64_t NumIdShift = CoreBits;
  static const uint64_t NumIdMask = ((((uint64_t)1) << NBitsNumber) - 1) << NumIdShift;

  static const uint64_t EpochShift = CoreBits + NBitsNumber;
  // since the reserve bit is always zero, we don't need a special mask
  static const uint64_t EpochMask = ((uint64_t)-1) << EpochShift;

  static inline ALWAYS_INLINE
  uint64_t MakeTid(uint64_t core_id, uint64_t num_id, uint64_t epoch_id)
  {
    // some sanity checking
    static_assert((CoreMask | NumIdMask | EpochMask) == ((uint64_t)-1), "xx");
    static_assert((CoreMask & NumIdMask) == 0, "xx");
    static_assert((NumIdMask & EpochMask) == 0, "xx");
    return (core_id) | (num_id << NumIdShift) | (epoch_id << EpochShift);
  }

  static inline void
  set_hack_status(bool hack_status)
  {
    g_hack->status_ = hack_status;
  }

  static inline bool
  get_hack_status()
  {
    return g_hack->status_;
  }

  // thread-safe, can be called many times
  static void InitGC();

  static void PurgeThreadOutstandingGCTasks();

#ifdef PROTO2_CAN_DISABLE_GC
  static inline bool
  IsGCEnabled()
  {
    return g_flags->g_gc_init.load(std::memory_order_acquire);
  }
#endif

#ifdef PROTO2_CAN_DISABLE_SNAPSHOTS
  static void
  DisableSnapshots()
  {
    g_flags->g_disable_snapshots.store(true, std::memory_order_release);
  }
  static inline bool
  IsSnapshotsEnabled()
  {
    return !g_flags->g_disable_snapshots.load(std::memory_order_acquire);
  }
#endif

protected:
  struct delete_entry {
#ifdef CHECK_INVARIANTS
    dbtuple *tuple_ahead_;
    uint64_t trigger_tid_;
#endif

    dbtuple *tuple_;
    marked_ptr<std::string> key_;
    concurrent_btree *btr_;

    delete_entry()
      :
#ifdef CHECK_INVARIANTS
        tuple_ahead_(nullptr),
        trigger_tid_(0),
#endif
        tuple_(),
        key_(),
        btr_(nullptr) {}

    delete_entry(dbtuple *tuple_ahead,
                 uint64_t trigger_tid,
                 dbtuple *tuple,
                 const marked_ptr<std::string> &key,
                 concurrent_btree *btr)
      :
#ifdef CHECK_INVARIANTS
        tuple_ahead_(tuple_ahead),
        trigger_tid_(trigger_tid),
#endif
        tuple_(tuple),
        key_(key),
        btr_(btr) {}

    inline dbtuple *
    tuple()
    {
      return tuple_;
    }
  };

  typedef basic_px_queue<delete_entry, 4096> px_queue;

  struct threadctx {
    uint64_t last_commit_tid_;
    unsigned last_reaped_epoch_;
#ifdef ENABLE_EVENT_COUNTERS
    uint64_t last_reaped_timestamp_us_;
#endif
    px_queue queue_;
    px_queue scratch_;
    std::deque<std::string *> pool_;
    threadctx() :
        last_commit_tid_(0)
      , last_reaped_epoch_(0)
#ifdef ENABLE_EVENT_COUNTERS
      , last_reaped_timestamp_us_(0)
#endif
    {
      ALWAYS_ASSERT(((uintptr_t)this % CACHELINE_SIZE) == 0);
      queue_.alloc_freelist(rcu::NQueueGroups);
      scratch_.alloc_freelist(rcu::NQueueGroups);
    }
  };

  static void
  clean_up_to_including(threadctx &ctx, uint64_t ro_tick_geq);

  // helper methods
  static inline txn_logger::pbuffer *
  wait_for_head(txn_logger::pbuffer_circbuf &pull_buf)
  {
    // XXX(stephentu): spinning for now
    txn_logger::pbuffer *px;
    while (unlikely(!(px = pull_buf.peek()))) {
      nop_pause();
      ++g_evt_worker_thread_wait_log_buffer;
    }
    INVARIANT(!px->io_scheduled_);
    return px;
  }

  // pushes horizon to the front entry of pull_buf, pushing
  // to push_buf if necessary
  //
  // horizon is reset after push_horizon_to_buffer() returns
  //
  // returns the number of txns pushed from buffer to *logger*
  // (if doing so was necessary)
  static inline size_t
  push_horizon_to_buffer(txn_logger::pbuffer *horizon,
                         void *lz4ctx,
                         txn_logger::pbuffer_circbuf &pull_buf,
                         txn_logger::pbuffer_circbuf &push_buf)
  {
    INVARIANT(txn_logger::IsCompressionEnabled());
    if (unlikely(!horizon->header()->nentries_))
      return 0;
    INVARIANT(horizon->datasize());

    size_t ntxns_pushed_to_logger = 0;

    // horizon out of space- try to push horizon to buffer
    txn_logger::pbuffer *px = wait_for_head(pull_buf);
    const uint64_t compressed_space_needed =
      sizeof(uint32_t) + LZ4_compressBound(horizon->datasize());

    bool buffer_cond = false;
    if (px->space_remaining() < compressed_space_needed ||
        (buffer_cond = !px->can_hold_tid(horizon->header()->last_tid_))) {
      // buffer out of space- push buffer to logger
      INVARIANT(px->header()->nentries_);
      ntxns_pushed_to_logger = px->header()->nentries_;
      txn_logger::pbuffer *px1 = pull_buf.deq();
      INVARIANT(px == px1);
      push_buf.enq(px1);
      px = wait_for_head(pull_buf);
      if (buffer_cond)
        ++txn_logger::g_evt_log_buffer_epoch_boundary;
      else
        ++txn_logger::g_evt_log_buffer_out_of_space;
    }

    INVARIANT(px->space_remaining() >= compressed_space_needed);
    if (!px->header()->nentries_)
      px->earliest_start_us_ = horizon->earliest_start_us_;
    px->header()->nentries_ += horizon->header()->nentries_;
    px->header()->last_tid_  = horizon->header()->last_tid_;

#ifdef ENABLE_EVENT_COUNTERS
    util::timer tt;
#endif
    const int ret = LZ4_compress_heap_limitedOutput(
        lz4ctx,
        (const char *) horizon->datastart(),
        (char *) px->pointer() + sizeof(uint32_t),
        horizon->datasize(),
        px->space_remaining() - sizeof(uint32_t));
#ifdef ENABLE_EVENT_COUNTERS
    txn_logger::g_evt_avg_log_buffer_compress_time_us.offer(tt.lap());
    txn_logger::g_evt_log_buffer_bytes_before_compress.inc(horizon->datasize());
    txn_logger::g_evt_log_buffer_bytes_after_compress.inc(ret);
#endif
    INVARIANT(ret > 0);
#if defined(CHECK_INVARIANTS) && defined(PARANOID_CHECKING)
    {
      uint8_t decode_buf[txn_logger::g_horizon_buffer_size];
      const int decode_ret =
        LZ4_decompress_safe_partial(
            (const char *) px->pointer() + sizeof(uint32_t),
            (char *) &decode_buf[0],
            ret,
            txn_logger::g_horizon_buffer_size,
            txn_logger::g_horizon_buffer_size);
      INVARIANT(decode_ret >= 0);
      INVARIANT(size_t(decode_ret) == horizon->datasize());
      INVARIANT(memcmp(horizon->datastart(),
                       &decode_buf[0], decode_ret) == 0);
    }
#endif

    serializer<uint32_t, false> s_uint32_t;
    s_uint32_t.write(px->pointer(), ret);
    px->curoff_ += sizeof(uint32_t) + uint32_t(ret);
    horizon->reset();

    return ntxns_pushed_to_logger;
  }

  struct hackstruct {
    std::atomic<bool> status_;
    std::atomic<uint64_t> global_tid_;
    constexpr hackstruct() : status_(false), global_tid_(0) {}
  };

  // use to simulate global TID for comparsion
  static util::aligned_padded_elem<hackstruct>
    g_hack CACHE_ALIGNED;

  struct flags {
    std::atomic<bool> g_gc_init;
    std::atomic<bool> g_disable_snapshots;
    constexpr flags() : g_gc_init(false), g_disable_snapshots(false) {}
  };
  static util::aligned_padded_elem<flags> g_flags;

  static percore_lazy<threadctx> g_threadctxs;

  static event_counter g_evt_worker_thread_wait_log_buffer;
  static event_counter g_evt_dbtuple_no_space_for_delkey;
  static event_counter g_evt_proto_gc_delete_requeue;
  static event_avg_counter g_evt_avg_log_entry_size;
  static event_avg_counter g_evt_avg_proto_gc_queue_len;
};

bool
txn_logger::pbuffer::can_hold_tid(uint64_t tid) const
{
  return !header()->nentries_ ||
         (transaction_proto2_static::EpochId(header()->last_tid_) ==
          transaction_proto2_static::EpochId(tid));
}

// protocol 2 - no global consistent TIDs
template <typename Traits>
class transaction_proto2 : public transaction<transaction_proto2, Traits>,
                           private transaction_proto2_static {

  friend class transaction<transaction_proto2, Traits>;
  typedef transaction<transaction_proto2, Traits> super_type;

public:

  typedef Traits traits_type;
  typedef transaction_base::tid_t tid_t;
  typedef transaction_base::string_type string_type;
  typedef typename super_type::dbtuple_write_info dbtuple_write_info;
  typedef typename super_type::dbtuple_write_info_vec dbtuple_write_info_vec;
  typedef typename super_type::read_set_map read_set_map;
  typedef typename super_type::absent_set_map absent_set_map;
  typedef typename super_type::write_set_map write_set_map;
  typedef typename super_type::write_set_u32_vec write_set_u32_vec;

  transaction_proto2(uint64_t flags,
                     typename Traits::StringAllocator &sa)
    : transaction<transaction_proto2, Traits>(flags, sa)
  {
    if (this->get_flags() & transaction_base::TXN_FLAG_READ_ONLY) {
      const uint64_t global_tick_ex =
        this->rcu_guard_->guard()->impl().global_last_tick_exclusive();
      u_.last_consistent_tid = ComputeReadOnlyTid(global_tick_ex);
    }
#ifdef TUPLE_LOCK_OWNERSHIP_CHECKING
    dbtuple::TupleLockRegionBegin();
#endif
    INVARIANT(rcu::s_instance.in_rcu_region());
  }

  ~transaction_proto2()
  {
#ifdef TUPLE_LOCK_OWNERSHIP_CHECKING
    dbtuple::AssertAllTupleLocksReleased();
#endif
    INVARIANT(rcu::s_instance.in_rcu_region());
  }

  inline bool
  can_overwrite_record_tid(tid_t prev, tid_t cur) const
  {
    INVARIANT(prev <= cur);

#ifdef PROTO2_CAN_DISABLE_SNAPSHOTS
    if (!IsSnapshotsEnabled())
      return true;
#endif

    // XXX(stephentu): the !prev check is a *bit* of a hack-
    // we're assuming that !prev (MIN_TID) corresponds to an
    // absent (removed) record, so it is safe to overwrite it,
    //
    // This is an OK assumption with *no TID wrap around*.
    return (to_read_only_tick(EpochId(prev)) ==
            to_read_only_tick(EpochId(cur))) ||
           !prev;
  }

  // can only read elements in this epoch or previous epochs
  inline bool
  can_read_tid(tid_t t) const
  {
    return true;
  }

  inline void
  on_tid_finish(tid_t commit_tid)
  {
    if (!txn_logger::IsPersistenceEnabled() ||
        this->state != transaction_base::TXN_COMMITED)
      return;
    // need to write into log buffer

    serializer<uint32_t, true> vs_uint32_t;

    // compute how much space is necessary
    uint64_t space_needed = 0;

    // 8 bytes to indicate TID
    space_needed += sizeof(uint64_t);

    // variable bytes to indicate # of records written
#ifdef LOGGER_UNSAFE_FAKE_COMPRESSION
    const unsigned nwrites = 0;
#else
    const unsigned nwrites = this->write_set.size();
#endif

    space_needed += vs_uint32_t.nbytes(&nwrites);

    // each record needs to be recorded
    write_set_u32_vec value_sizes;
    for (unsigned idx = 0; idx < nwrites; idx++) {
      const transaction_base::write_record_t &rec = this->write_set[idx];
      const uint32_t k_nbytes = rec.get_key().size();
      space_needed += vs_uint32_t.nbytes(&k_nbytes);
      space_needed += k_nbytes;

      const uint32_t v_nbytes = rec.get_value() ?
          rec.get_writer()(
              dbtuple::TUPLE_WRITER_COMPUTE_DELTA_NEEDED,
              rec.get_value(), nullptr, 0) : 0;
      space_needed += vs_uint32_t.nbytes(&v_nbytes);
      space_needed += v_nbytes;

      value_sizes.push_back(v_nbytes);
    }

    g_evt_avg_log_entry_size.offer(space_needed);
    INVARIANT(space_needed <= txn_logger::g_horizon_buffer_size);
    INVARIANT(space_needed <= txn_logger::g_buffer_size);

    const unsigned long my_core_id = coreid::core_id();

    txn_logger::persist_ctx &ctx =
      txn_logger::persist_ctx_for(my_core_id, txn_logger::INITMODE_REG);
    txn_logger::persist_stats &stats =
      txn_logger::g_persist_stats[my_core_id];
    txn_logger::pbuffer_circbuf &pull_buf = ctx.all_buffers_;
    txn_logger::pbuffer_circbuf &push_buf = ctx.persist_buffers_;

    util::non_atomic_fetch_add(stats.ntxns_committed_, 1UL);

    const bool do_compress = txn_logger::IsCompressionEnabled();
    if (do_compress) {
      // try placing in horizon
      bool horizon_cond = false;
      if (ctx.horizon_->space_remaining() < space_needed ||
          (horizon_cond = !ctx.horizon_->can_hold_tid(commit_tid))) {
        if (!ctx.horizon_->datasize()) {
          std::cerr << "space_needed: " << space_needed << std::endl;
          std::cerr << "space_remaining: " << ctx.horizon_->space_remaining() << std::endl;
          std::cerr << "can_hold_tid: " << ctx.horizon_->can_hold_tid(commit_tid) << std::endl;
        }
        INVARIANT(ctx.horizon_->datasize());
        // horizon out of space, so we push it
        const uint64_t npushed =
          push_horizon_to_buffer(ctx.horizon_, ctx.lz4ctx_, pull_buf, push_buf);
        if (npushed)
          util::non_atomic_fetch_add(stats.ntxns_pushed_, npushed);
      }

      INVARIANT(ctx.horizon_->space_remaining() >= space_needed);
      const uint64_t written =
        write_current_txn_into_buffer(ctx.horizon_, commit_tid, value_sizes);
      if (written != space_needed)
        INVARIANT(false);

    } else {

    retry:
      txn_logger::pbuffer *px = wait_for_head(pull_buf);
      INVARIANT(px && px->core_id_ == my_core_id);
      bool cond = false;
      if (px->space_remaining() < space_needed ||
          (cond = !px->can_hold_tid(commit_tid))) {
        INVARIANT(px->header()->nentries_);
        txn_logger::pbuffer *px0 = pull_buf.deq();
        INVARIANT(px == px0);
        INVARIANT(px0->header()->nentries_);
        util::non_atomic_fetch_add(stats.ntxns_pushed_, px0->header()->nentries_);
        push_buf.enq(px0);
        if (cond)
          ++txn_logger::g_evt_log_buffer_epoch_boundary;
        else
          ++txn_logger::g_evt_log_buffer_out_of_space;
        goto retry;
      }

      const uint64_t written =
        write_current_txn_into_buffer(px, commit_tid, value_sizes);
      if (written != space_needed)
        INVARIANT(false);
    }
  }

private:

  // assumes enough space in px to hold this txn
  inline uint64_t
  write_current_txn_into_buffer(
      txn_logger::pbuffer *px,
      uint64_t commit_tid,
      const write_set_u32_vec &value_sizes)
  {
    INVARIANT(px->can_hold_tid(commit_tid));

    if (unlikely(!px->header()->nentries_))
      px->earliest_start_us_ = this->rcu_guard_->guard()->start_us();

    uint8_t *p = px->pointer();
    uint8_t *porig = p;

    serializer<uint32_t, true> vs_uint32_t;
    serializer<uint64_t, false> s_uint64_t;

#ifdef LOGGER_UNSAFE_FAKE_COMPRESSION
    const unsigned nwrites = 0;
#else
    const unsigned nwrites = this->write_set.size();
#endif


    INVARIANT(nwrites == value_sizes.size());

    p = s_uint64_t.write(p, commit_tid);
    p = vs_uint32_t.write(p, nwrites);

    for (unsigned idx = 0; idx < nwrites; idx++) {
      const transaction_base::write_record_t &rec = this->write_set[idx];
      const uint32_t k_nbytes = rec.get_key().size();
      p = vs_uint32_t.write(p, k_nbytes);
      NDB_MEMCPY(p, rec.get_key().data(), k_nbytes);
      p += k_nbytes;
      const uint32_t v_nbytes = value_sizes[idx];
      p = vs_uint32_t.write(p, v_nbytes);
      if (v_nbytes) {
        rec.get_writer()(dbtuple::TUPLE_WRITER_DO_DELTA_WRITE, rec.get_value(), p, v_nbytes);
        p += v_nbytes;
      }
    }

    px->curoff_ += (p - porig);
    px->header()->nentries_++;
    px->header()->last_tid_ = commit_tid;

    return uint64_t(p - porig);
  }

public:

  inline ALWAYS_INLINE bool is_snapshot() const {
    return this->get_flags() & transaction_base::TXN_FLAG_READ_ONLY;
  }

  inline transaction_base::tid_t
  snapshot_tid() const
  {
#ifdef PROTO2_CAN_DISABLE_SNAPSHOTS
    if (!IsSnapshotsEnabled())
      // when snapshots are disabled, but we have a RO txn, we simply allow
      // it to read all the latest values and treat them as consistent
      //
      // it's not correct, but its for the factor analysis
      return dbtuple::MAX_TID;
#endif
    return u_.last_consistent_tid;
  }

  void
  dump_debug_info() const
  {
    transaction<transaction_proto2, Traits>::dump_debug_info();
    if (this->is_snapshot())
      std::cerr << "  last_consistent_tid: "
        << g_proto_version_str(u_.last_consistent_tid) << std::endl;
  }

  transaction_base::tid_t
  gen_commit_tid(const dbtuple_write_info_vec &write_tuples)
  {
    const size_t my_core_id = this->rcu_guard_->guard()->core();
    threadctx &ctx = g_threadctxs.get(my_core_id);
    INVARIANT(!this->is_snapshot());

    COMPILER_MEMORY_FENCE;
    u_.commit_epoch = ticker::s_instance.global_current_tick();
    COMPILER_MEMORY_FENCE;

    tid_t ret = ctx.last_commit_tid_;
    INVARIANT(ret == dbtuple::MIN_TID || CoreId(ret) == my_core_id);
    if (u_.commit_epoch != EpochId(ret))
      ret = MakeTid(0, 0, u_.commit_epoch);

    // What is this? Is txn_proto1_impl used?
    if (g_hack->status_.load(std::memory_order_acquire))
      g_hack->global_tid_.fetch_add(1, std::memory_order_acq_rel);

    // XXX(stephentu): I believe this is correct, but not 100% sure
    //const size_t my_core_id = 0;
    //tid_t ret = 0;
    {
      typename read_set_map::const_iterator it     = this->read_set.begin();
      typename read_set_map::const_iterator it_end = this->read_set.end();
      for (; it != it_end; ++it) {
        if (it->get_tid() > ret)
          ret = it->get_tid();
      }
    }

    {
      typename dbtuple_write_info_vec::const_iterator it     = write_tuples.begin();
      typename dbtuple_write_info_vec::const_iterator it_end = write_tuples.end();
      for (; it != it_end; ++it) {
        INVARIANT(it->tuple->is_locked());
        INVARIANT(it->tuple->is_lock_owner());
        INVARIANT(it->tuple->is_write_intent());
        INVARIANT(!it->tuple->is_modifying());
        INVARIANT(it->tuple->is_latest());
        if (it->is_insert())
          // we inserted this node, so we don't want to do the checks below
          continue;
        const tid_t t = it->tuple->version;

        // XXX(stephentu): we are overly conservative for now- technically this
        // abort isn't necessary (we really should just write the value in the correct
        // position)
        //if (EpochId(t) > u_.commit_epoch) {
        //  std::cerr << "t: " << g_proto_version_str(t) << std::endl;
        //  std::cerr << "epoch: " << u_.commit_epoch << std::endl;
        //  this->dump_debug_info();
        //}

        // t == dbtuple::MAX_TID when a txn does an insert of a new tuple
        // followed by 1+ writes to the same tuple.
        INVARIANT(EpochId(t) <= u_.commit_epoch || t == dbtuple::MAX_TID);
        if (t != dbtuple::MAX_TID && t > ret)
          ret = t;
      }

      INVARIANT(EpochId(ret) == u_.commit_epoch);
      ret = MakeTid(my_core_id, NumId(ret) + 1, u_.commit_epoch);
    }

    // XXX(stephentu): this txn hasn't actually been commited yet,
    // and could potentially be aborted - but it's ok to increase this #, since
    // subsequent txns on this core will read this # anyways
    return (ctx.last_commit_tid_ = ret);
  }

  inline ALWAYS_INLINE void
  on_dbtuple_spill(dbtuple *tuple_ahead, dbtuple *tuple)
  {
#ifdef PROTO2_CAN_DISABLE_GC
    if (!IsGCEnabled())
      return;
#endif

    INVARIANT(rcu::s_instance.in_rcu_region());
    INVARIANT(!tuple->is_latest());

    // >= not > only b/c of the special case of inserting a new tuple +
    // overwriting the newly inserted record with a longer sequence of bytes in
    // the *same* txn
    INVARIANT(tuple_ahead->version >= tuple->version);

    if (tuple->is_deleting()) {
      INVARIANT(tuple->is_locked());
      INVARIANT(tuple->is_lock_owner());
      // already on queue
      return;
    }

    const uint64_t ro_tick = to_read_only_tick(this->u_.commit_epoch);
    INVARIANT(to_read_only_tick(EpochId(tuple->version)) <= ro_tick);

#ifdef CHECK_INVARIANTS
    uint64_t exp = 0;
    INVARIANT(tuple->opaque.compare_exchange_strong(exp, 1, std::memory_order_acq_rel));
#endif

    // when all snapshots are happening >= the current epoch,
    // then we can safely remove tuple
    threadctx &ctx = g_threadctxs.my();
    ctx.queue_.enqueue(
        delete_entry(tuple_ahead, tuple_ahead->version,
          tuple, marked_ptr<std::string>(), nullptr),
        ro_tick);
  }

  inline ALWAYS_INLINE void
  on_logical_delete(dbtuple *tuple, const std::string &key, concurrent_btree *btr)
  {
#ifdef PROTO2_CAN_DISABLE_GC
    if (!IsGCEnabled())
      return;
#endif

    INVARIANT(tuple->is_locked());
    INVARIANT(tuple->is_lock_owner());
    INVARIANT(tuple->is_write_intent());
    INVARIANT(tuple->is_latest());
    INVARIANT(tuple->is_deleting());
    INVARIANT(!tuple->size);
    INVARIANT(rcu::s_instance.in_rcu_region());

    const uint64_t ro_tick = to_read_only_tick(this->u_.commit_epoch);
    threadctx &ctx = g_threadctxs.my();

#ifdef CHECK_INVARIANTS
    uint64_t exp = 0;
    INVARIANT(tuple->opaque.compare_exchange_strong(exp, 1, std::memory_order_acq_rel));
#endif

    if (likely(key.size() <= tuple->alloc_size)) {
      NDB_MEMCPY(tuple->get_value_start(), key.data(), key.size());
      tuple->size = key.size();

      // eligible for deletion when all snapshots >= the current epoch
      marked_ptr<std::string> mpx;
      mpx.set_flags(0x1);

      ctx.queue_.enqueue(
          delete_entry(nullptr, tuple->version, tuple, mpx, btr),
          ro_tick);
    } else {
      // this is a rare event
      ++g_evt_dbtuple_no_space_for_delkey;
      std::string *spx = nullptr;
      if (ctx.pool_.empty()) {
        spx = new std::string(key.data(), key.size()); // XXX: use numa memory?
      } else {
        spx = ctx.pool_.front();
        ctx.pool_.pop_front();
        spx->assign(key.data(), key.size());
      }
      INVARIANT(spx);

      marked_ptr<std::string> mpx(spx);
      mpx.set_flags(0x1);

      ctx.queue_.enqueue(
          delete_entry(nullptr, tuple->version, tuple, mpx, btr),
          ro_tick);
    }
  }

  void
  on_post_rcu_region_completion()
  {
#ifdef PROTO2_CAN_DISABLE_GC
    if (!IsGCEnabled())
      return;
#endif
    const uint64_t last_tick_ex = ticker::s_instance.global_last_tick_exclusive();
    if (unlikely(!last_tick_ex))
      return;
    // we subtract one from the global last tick, because of the way
    // consistent TIDs are computed, the global_last_tick_exclusive() can
    // increase by at most one tick during a transaction.
    const uint64_t ro_tick_ex = to_read_only_tick(last_tick_ex - 1);
    if (unlikely(!ro_tick_ex))
      // won't have anything to clean
      return;
    // all reads happening at >= ro_tick_geq
    const uint64_t ro_tick_geq = ro_tick_ex - 1;
    threadctx &ctx = g_threadctxs.my();
    clean_up_to_including(ctx, ro_tick_geq);
  }

private:

  union {
    // the global epoch this txn is running in (this # is read when it starts)
    // -- snapshot txns only
    uint64_t last_consistent_tid;
    // the epoch for this txn -- committing non-snapshot txns only
    uint64_t commit_epoch;
  } u_;
};

// txn_btree_handler specialization
template <>
struct base_txn_btree_handler<transaction_proto2> {
  static inline void
  on_construct()
  {
#ifndef PROTO2_CAN_DISABLE_GC
    transaction_proto2_static::InitGC();
#endif
  }
  static const bool has_background_task = true;
};

template <>
struct txn_epoch_sync<transaction_proto2> : public transaction_proto2_static {
  static void
  sync()
  {
    wait_an_epoch();
    if (txn_logger::IsPersistenceEnabled())
      txn_logger::wait_until_current_point_persisted();
  }
  static void
  finish()
  {
    if (txn_logger::IsPersistenceEnabled())
      txn_logger::wait_until_current_point_persisted();
  }
  static void
  thread_init(bool loader)
  {
    if (!txn_logger::IsPersistenceEnabled())
      return;
    const unsigned long my_core_id = coreid::core_id();
    // try to initialize using numa allocator
    txn_logger::persist_ctx_for(
        my_core_id,
        loader ? txn_logger::INITMODE_REG : txn_logger::INITMODE_RCU);
  }
  static void
  thread_end()
  {
    if (!txn_logger::IsPersistenceEnabled())
      return;
    const unsigned long my_core_id = coreid::core_id();
    txn_logger::persist_ctx &ctx =
      txn_logger::persist_ctx_for(my_core_id, txn_logger::INITMODE_NONE);
    if (unlikely(!ctx.init_))
      return;
    txn_logger::persist_stats &stats =
      txn_logger::g_persist_stats[my_core_id];
    txn_logger::pbuffer_circbuf &pull_buf = ctx.all_buffers_;
    txn_logger::pbuffer_circbuf &push_buf = ctx.persist_buffers_;
    if (txn_logger::IsCompressionEnabled() &&
        ctx.horizon_->header()->nentries_) {
      INVARIANT(ctx.horizon_->datasize());
      const uint64_t npushed =
        push_horizon_to_buffer(ctx.horizon_, ctx.lz4ctx_, pull_buf, push_buf);
      if (npushed)
        util::non_atomic_fetch_add(stats.ntxns_pushed_, npushed);
    }
    txn_logger::pbuffer *px = pull_buf.peek();
    if (!px || !px->header()->nentries_) {
      //std::cerr << "core " << my_core_id
      //          << " nothing to push to logger" << std::endl;
      return;
    }
    //std::cerr << "core " << my_core_id
    //          << " pushing buffer to logger" << std::endl;
    txn_logger::pbuffer *px0 = pull_buf.deq();
    util::non_atomic_fetch_add(stats.ntxns_pushed_, px0->header()->nentries_);
    INVARIANT(px0 == px);
    push_buf.enq(px0);
  }
  static std::tuple<uint64_t, uint64_t, double>
  compute_ntxn_persisted()
  {
    if (!txn_logger::IsPersistenceEnabled())
      return std::make_tuple(0, 0, 0.0);
    return txn_logger::compute_ntxns_persisted_statistics();
  }
  static void
  reset_ntxn_persisted()
  {
    if (!txn_logger::IsPersistenceEnabled())
      return;
    txn_logger::clear_ntxns_persisted_statistics();
  }
};

#endif /* _NDB_TXN_PROTO2_IMPL_H_ */
