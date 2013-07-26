#ifndef _NDB_TXN_PROTO2_IMPL_H_
#define _NDB_TXN_PROTO2_IMPL_H_

#include <iostream>
#include <atomic>
#include <vector>
#include <set>

#include "txn.h"
#include "txn_impl.h"
#include "txn_btree.h"
#include "macros.h"
#include "circbuf.h"
#include "record/serializer.h"

// forward decl
template <typename Traits> class transaction_proto2;

// each txn_btree has a walker_loop associated with it
class txn_walker_loop : public ndb_thread {
  friend class transaction_proto2_static;
  friend class base_txn_btree_handler<transaction_proto2>;
  friend class txn_epoch_sync<transaction_proto2>;
public:
  // not a daemon thread, so we can join when the txn_btree exists
  txn_walker_loop()
    : ndb_thread(false, std::string("walkerloop")),
      running(false),
      btr(0) {}
  virtual void run();
private:
  volatile bool running;
  static volatile bool global_running; // a hacky way to disable all cleaners temporarily
  std::string name;
  btree *btr;
};

// the system has a single logging subsystem (composed of multiple lgogers)
// NOTE: currently, the persistence epoch is tied 1:1 with the ticker's epoch
class txn_logger {
  template <typename T>
    friend class transaction_proto2;
public:
  static bool g_persist; // whether or not logging is enabled

  static const size_t g_nmax_loggers = 16;
  static const size_t g_perthread_buffers = 64; // 64 outstanding buffers
  static const size_t g_buffer_size = (1<<20); // in bytes
  static const size_t g_horizon_size = (1<<16); // in bytes, for compression only
  static const size_t g_max_lag_epochs = 64; // cannot lag more than 64 epochs

  // init the logging subsystem.
  //
  // should only be called ONCE is not thread-safe.  if assignments_used is not
  // null, then fills it with a copy of the assignment actually computed
  static void Init(
      size_t nworkers,
      const std::vector<std::string> &logfiles,
      const std::vector<std::vector<unsigned>> &assignments_given,
      std::vector<std::vector<unsigned>> *assignments_used = nullptr);

  struct logbuf_header {
    uint64_t nentries_; // > 0 for all valid log buffers
    uint64_t last_tid_; // TID of the last commit
  } PACKED;

  struct pbuffer {
    uint64_t earliest_start_us_; // start time of the earliest txn
    bool io_scheduled_; // has the logger scheduled IO yet?
    size_t curoff_; // current offset into buf_, either for writing
    // or during the dep computation phase
    std::string buf_; // the actual buffer, of size g_buffer_size

    const unsigned core_id_; // which core does this pbuffer belong to?

    pbuffer(unsigned core_id) : core_id_(core_id) { reset(); }

    inline void
    reset()
    {
      earliest_start_us_ = 0;
      io_scheduled_ = false;
      curoff_ = sizeof(logbuf_header);
      buf_.assign(g_buffer_size, 0);
    }

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

    inline size_t
    space_remaining() const
    {
      INVARIANT(g_buffer_size >= curoff_);
      return g_buffer_size - curoff_;
    }
  };

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

  // the buffer which a logger thread pushes clean buffers back to the worker
  // thread
  static inline circbuf<pbuffer, g_perthread_buffers> &
  logger_to_core_buffer(size_t core_id)
  {
    INVARIANT(core_id < g_all_buffers.size());
    // make sure its init-ed
    if (unlikely(!g_all_buffers_init[core_id])) {
      for (size_t i = 0; i < g_perthread_buffers; i++)
        g_all_buffers[core_id].enq(new pbuffer(core_id));
      g_all_buffers_init[core_id] = true;
    }
    return g_all_buffers[core_id];
  }

  // the buffer which a worker thread uses to push buffers to the logger
  static inline circbuf<pbuffer, g_perthread_buffers> &
  core_to_logger_buffer(size_t core_id)
  {
    INVARIANT(core_id < g_persist_buffers.size());
    return g_persist_buffers[core_id];
  }

  static std::pair<uint64_t, double>
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

  static void
  advance_system_sync_epoch(
      const std::vector<std::vector<unsigned>> &assignments);

  // makes copy on purpose
  static void writer(
      unsigned id, int fd,
      std::vector<unsigned> assignment);

  static void persister(
      std::vector<std::vector<unsigned>> assignments);

  static size_t g_nworkers; // assignments are computed based on g_nworkers
                            // but a logger responsible for core i is really
                            // responsible for cores i + k * g_nworkers, for k
                            // >= 0

  // v = per_thread_sync_epochs_[i].epochs_[j]: logger i has persisted up
  // through (including) all transactions <= epoch v on core j. since core =>
  // logger mapping is static, taking:
  //   min_{core} max_{logger} per_thread_sync_epochs_[logger].epochs_[core]
  // yields the entire system's persistent epoch

  struct epoch_array {
    // don't use percore<std::atomic<uint64_t>> because we don't want padding
    std::atomic<uint64_t> epochs_[NMAXCORES];
    CACHE_PADOUT;
  };

  static epoch_array per_thread_sync_epochs_[g_nmax_loggers] CACHE_ALIGNED;

  // conservative estimate (<=) for:
  //   min_{core} max_{logger} per_thread_sync_epochs_[logger].epochs_[core]
  static util::aligned_padded_elem<std::atomic<uint64_t>> system_sync_epoch_;

  static percore<circbuf<pbuffer, g_perthread_buffers>> g_all_buffers;

  static percore<bool> g_all_buffers_init; // not cache aligned because
                                           // in steady state is only read-only

  static percore<circbuf<pbuffer, g_perthread_buffers>> g_persist_buffers;

  // context per one epoch
  struct persist_stats {
    // how many txns this thread has persisted in total
    std::atomic<uint64_t> ntxns_persisted_;

    // sum of all latencies (divid by ntxns_persisted_ to get avg latency in
    // us)
    std::atomic<uint64_t> latency_numer_;

    // per last g_max_lag_epochs information
    struct per_epoch_stats {
      std::atomic<uint64_t> ntxns_;
      std::atomic<uint64_t> earliest_start_us_;

      per_epoch_stats() : ntxns_(0), earliest_start_us_(0) {}
    } d_[g_max_lag_epochs];

    persist_stats() : ntxns_persisted_(0), latency_numer_(0) {}
  };

  static percore<persist_stats> g_persist_stats;

  static event_counter g_evt_log_buffer_epoch_boundary;
  static event_counter g_evt_logger_max_lag_wait;
  static event_avg_counter g_evt_avg_log_entry_ntxns;
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
  static const uint64_t ReadOnlyEpochMultiplier = 1; /* 10 ms */
#else
  static const uint64_t ReadOnlyEpochMultiplier = 100; /* 1 sec */
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

  static percore<uint64_t> g_last_commit_tids;

  // can clean <= ro_epoch_clean (note this is in RO epochs, not ticker epochs)
  static void
  do_dbtuple_chain_cleanup(dbtuple *ln, uint64_t ro_epoch_clean);

  static bool
  try_dbtuple_cleanup(btree *btr, const std::string &key,
                      dbtuple *tuple, uint64_t ro_epoch_clean);

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

  // hack
  static util::aligned_padded_elem<std::atomic<uint64_t>> g_max_gc_version_inc;
  static util::aligned_padded_elem<std::atomic<uint64_t>> g_max_unlink_version_inc;

protected:

  struct hackstruct {
    std::atomic<bool> status_;
    std::atomic<uint64_t> global_tid_;
    constexpr hackstruct() : status_(false), global_tid_(0) {}
  };

  // use to simulate global TID for comparsion
  static util::aligned_padded_elem<hackstruct>
    g_hack CACHE_ALIGNED;

  static event_counter g_evt_worker_thread_wait_log_buffer;
  static event_avg_counter g_evt_avg_log_entry_size;
};

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
    : transaction<transaction_proto2, Traits>(flags, sa),
      current_epoch(0),
      last_consistent_tid(0)
  {
    current_epoch = this->rcu_guard_.guard().tick();
    if (this->get_flags() & transaction_base::TXN_FLAG_READ_ONLY) {
      const uint64_t global_tick_ex =
        this->rcu_guard_.guard().impl().global_last_tick_exclusive();

      const uint64_t a = (global_tick_ex / ReadOnlyEpochMultiplier);
      const uint64_t b = a * ReadOnlyEpochMultiplier;

      // want to read entries <= b-1, special casing for b=0
      if (!b)
        last_consistent_tid = MakeTid(0, 0, 0);
      else
        last_consistent_tid = MakeTid(CoreMask, NumIdMask >> NumIdShift, b - 1);
    }
#ifdef TUPLE_LOCK_OWNERSHIP_CHECKING
    dbtuple::TupleLockRegionBegin();
#endif
  }

  ~transaction_proto2()
  {
#ifdef TUPLE_LOCK_OWNERSHIP_CHECKING
    dbtuple::AssertAllTupleLocksReleased();
#endif
  }

  inline bool
  can_overwrite_record_tid(tid_t prev, tid_t cur) const
  {
    INVARIANT(prev <= cur);

    // XXX(stephentu): the !prev check is a *bit* of a hack-
    // we're assuming that !prev (MIN_TID) corresponds to an
    // absent (removed) record, so it is safe to overwrite it,
    //
    // This is an OK assumption with *no TID wrap around*.
    return to_read_only_tick(EpochId(prev)) ==
           to_read_only_tick(EpochId(cur)) || !prev;
  }

  // can only read elements in this epoch or previous epochs
  inline bool
  can_read_tid(tid_t t) const
  {
    return EpochId(t) <= current_epoch;
  }

  inline void
  on_tid_finish(tid_t commit_tid)
  {
    if (!txn_logger::g_persist ||
        this->state != transaction_base::TXN_COMMITED)
      return;
    // need to write into log buffer

    serializer<uint32_t, true> vs_uint32_t;
    serializer<uint64_t, false> s_uint64_t;

    // compute how much space is necessary
    uint64_t space_needed = 0;

    // 8 bytes to indicate TID
    space_needed += sizeof(uint64_t);

    // variable bytes to indicate # of records written
    const uint32_t nwrites = this->write_set.size();
    space_needed += vs_uint32_t.nbytes(&nwrites);

    // each record needs to be recorded
    auto it_end = this->write_set.end();
    write_set_u32_vec value_sizes;
    for (auto it = this->write_set.begin(); it != it_end; ++it) {
      const uint32_t k_nbytes = it->get_key().size();
      space_needed += vs_uint32_t.nbytes(&k_nbytes);
      space_needed += k_nbytes;

      const uint32_t v_nbytes = it->get_value() ?
          it->get_writer()(
              dbtuple::TUPLE_WRITER_COMPUTE_DELTA_NEEDED,
              it->get_value(), nullptr, 0) : 0;
      space_needed += vs_uint32_t.nbytes(&v_nbytes);
      space_needed += v_nbytes;

      value_sizes.push_back(v_nbytes);
    }

    g_evt_avg_log_entry_size.offer(space_needed);

    // XXX(stephentu): spinning for now
    const unsigned long my_core_id = coreid::core_id();
    txn_logger::pbuffer_circbuf &pull_buf =
      txn_logger::logger_to_core_buffer(my_core_id);

  retry:
    txn_logger::pbuffer *px;
    while (unlikely(!(px = pull_buf.peek()))) {
      nop_pause();
      ++g_evt_worker_thread_wait_log_buffer;
    }
    INVARIANT(!px->io_scheduled_);
    INVARIANT(px->core_id_ == my_core_id);

    // check if enough size, or if spans epoch boundary
    // (which we currently disallow for very shoddy reasons)
    bool cond = false;
    if (px->space_remaining() < space_needed ||
        (cond = (px->header()->nentries_ &&
         EpochId(px->header()->last_tid_) != EpochId(commit_tid)))) {
      INVARIANT(px->header()->nentries_);
      txn_logger::pbuffer *px0 = pull_buf.deq();
      INVARIANT(px == px0);
      txn_logger::pbuffer_circbuf &push_buf =
        txn_logger::core_to_logger_buffer(my_core_id);
      push_buf.enq(px0);
      if (cond)
        ++txn_logger::g_evt_log_buffer_epoch_boundary;
      goto retry;
    }

    if (unlikely(!px->header()->nentries_))
      px->earliest_start_us_ = this->rcu_guard_.guard().start_us();

    uint8_t *p = px->pointer();
    p = s_uint64_t.write(p, commit_tid);
    p = vs_uint32_t.write(p, nwrites);

    for (size_t idx = 0; idx < this->write_set.size(); idx++) {
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

    px->curoff_ += space_needed;
    px->header()->nentries_++;
    px->header()->last_tid_ = commit_tid;
  }

  inline std::pair<bool, transaction_base::tid_t>
  consistent_snapshot_tid() const
  {
    if (this->get_flags() & transaction_base::TXN_FLAG_READ_ONLY)
      return std::make_pair(true, last_consistent_tid);
    else
      return std::make_pair(false, 0);
  }

  void
  dump_debug_info() const
  {
    transaction<transaction_proto2, Traits>::dump_debug_info();
    std::cerr << "  current_epoch: "
      << current_epoch << std::endl;
    std::cerr << "  last_consistent_tid: "
      << g_proto_version_str(last_consistent_tid) << std::endl;
    std::cerr << "  max_epoch_removed_inc: "
      << transaction_proto2_static::g_max_gc_version_inc->load(std::memory_order_acquire)
      << std::endl;
    std::cerr << "  max_epoch_unlinked_inc: "
      << transaction_proto2_static::g_max_unlink_version_inc->load(std::memory_order_acquire)
      << std::endl;
  }

  transaction_base::tid_t
  gen_commit_tid(const dbtuple_write_info_vec &write_tuples)
  {
    const size_t my_core_id = this->rcu_guard_.guard().core();
    const tid_t l_last_commit_tid = g_last_commit_tids[my_core_id];
    INVARIANT(l_last_commit_tid == dbtuple::MIN_TID ||
              CoreId(l_last_commit_tid) == my_core_id);
    INVARIANT(!this->is_read_only());

    // XXX(stephentu): wrap-around messes this up
    INVARIANT(EpochId(l_last_commit_tid) <= current_epoch);

    if (g_hack->status_.load(std::memory_order_acquire))
      g_hack->global_tid_.fetch_add(1, std::memory_order_acq_rel);

    tid_t ret = l_last_commit_tid;

    // XXX(stephentu): I believe this is correct, but not 100% sure
    //const size_t my_core_id = 0;
    //tid_t ret = 0;
    {
      typename read_set_map::const_iterator it     = this->read_set.begin();
      typename read_set_map::const_iterator it_end = this->read_set.end();
      for (; it != it_end; ++it) {
        // NB: we don't allow ourselves to do reads in future epochs
        INVARIANT(EpochId(it->get_tid()) <= current_epoch);
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
        INVARIANT(!it->tuple->is_deleting());
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
        INVARIANT(EpochId(t) <= current_epoch);
        if (t > ret)
          ret = t;
      }
      ret = MakeTid(my_core_id, NumId(ret) + 1, current_epoch);
    }

    COMPILER_MEMORY_FENCE;

    // XXX(stephentu): this txn hasn't actually been commited yet,
    // and could potentially be aborted - but it's ok to increase this #, since
    // subsequent txns on this core will read this # anyways
    return (g_last_commit_tids[my_core_id] = ret);
  }

  inline ALWAYS_INLINE void
  on_dbtuple_spill(dbtuple *tuple)
  {
    INVARIANT(tuple->is_locked());
    INVARIANT(tuple->is_lock_owner());
    INVARIANT(tuple->is_write_intent());
    INVARIANT(tuple->is_latest());
    INVARIANT(rcu::s_instance.in_rcu_region());
    // this is too aggressive- better to let the background
    // reaper clean the chains
    //do_dbtuple_chain_cleanup(tuple);
  }

  inline ALWAYS_INLINE void
  on_logical_delete(dbtuple *tuple)
  {
    // we let the background tree walker clean up the node
  }

private:
  // the global epoch this txn is running in (this # is read when it starts)
  uint64_t current_epoch;
  uint64_t last_consistent_tid;
};

// txn_btree_handler specialization
template <>
struct base_txn_btree_handler<transaction_proto2> {
  void
  on_construct(const std::string &name, btree *btr)
  {
    INVARIANT(!walker_loop.running);
    INVARIANT(!walker_loop.btr);
    walker_loop.name = name;
    walker_loop.btr = btr;
    walker_loop.running = true;
    __sync_synchronize();
    walker_loop.start();
  }

  void
  on_destruct()
  {
    INVARIANT(walker_loop.running);
    INVARIANT(walker_loop.btr);
    walker_loop.running = false;
    __sync_synchronize();
    walker_loop.join();
  }

  static const bool has_background_task = true;

private:
  txn_walker_loop walker_loop;
};

template <>
struct txn_epoch_sync<transaction_proto2> {
  static void
  sync()
  {
    transaction_proto2_static::wait_an_epoch();
    if (txn_logger::g_persist)
      txn_logger::wait_until_current_point_persisted();
  }
  static void
  finish()
  {
    txn_walker_loop::global_running = false;
    __sync_synchronize();
    if (txn_logger::g_persist)
      txn_logger::wait_until_current_point_persisted();
  }
  static void
  thread_end()
  {
    if (!txn_logger::g_persist)
      return;
    const unsigned long my_core_id = coreid::core_id();
    txn_logger::pbuffer_circbuf &pull_buf =
      txn_logger::logger_to_core_buffer(my_core_id);
    txn_logger::pbuffer *px = pull_buf.peek();
    if (!px || !px->header()->nentries_)
      return;
    txn_logger::pbuffer *px0 = pull_buf.deq();
    INVARIANT(px0 == px);
    txn_logger::pbuffer_circbuf &push_buf =
      txn_logger::core_to_logger_buffer(my_core_id);
    push_buf.enq(px);
  }
  static std::pair<uint64_t, double>
  compute_ntxn_persisted()
  {
    if (!txn_logger::g_persist)
      return {0, 0.0};
    return txn_logger::compute_ntxns_persisted_statistics();
  }
  static void
  reset_ntxn_persisted()
  {
    if (!txn_logger::g_persist)
      return;
    txn_logger::clear_ntxns_persisted_statistics();
  }
};

#endif /* _NDB_TXN_PROTO2_IMPL_H_ */
