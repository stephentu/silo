#include <iostream>
#include <thread>
#include <fcntl.h>
#include <unistd.h>
#include <sys/uio.h>
#include <limits.h>
#include <numa.h>

#include "txn_proto2_impl.h"
#include "counter.h"
#include "util.h"

using namespace std;
using namespace util;

                    /** logger subsystem **/
/*{{{*/
bool txn_logger::g_persist = false;
bool txn_logger::g_call_fsync = true;
bool txn_logger::g_use_compression = false;
bool txn_logger::g_fake_writes = false;
size_t txn_logger::g_nworkers = 0;
txn_logger::epoch_array
  txn_logger::per_thread_sync_epochs_[txn_logger::g_nmax_loggers];
aligned_padded_elem<atomic<uint64_t>>
  txn_logger::system_sync_epoch_(0);
percore<txn_logger::persist_ctx>
  txn_logger::g_persist_ctxs;
percore<txn_logger::persist_stats>
  txn_logger::g_persist_stats;
event_counter
  txn_logger::g_evt_log_buffer_epoch_boundary("log_buffer_epoch_boundary");
event_counter
  txn_logger::g_evt_log_buffer_out_of_space("log_buffer_out_of_space");
event_counter
  txn_logger::g_evt_log_buffer_bytes_before_compress("log_buffer_bytes_before_compress");
event_counter
  txn_logger::g_evt_log_buffer_bytes_after_compress("log_buffer_bytes_after_compress");
event_counter
  txn_logger::g_evt_logger_writev_limit_met("logger_writev_limit_met");
event_counter
  txn_logger::g_evt_logger_max_lag_wait("logger_max_lag_wait");
event_avg_counter
  txn_logger::g_evt_avg_log_buffer_compress_time_us("avg_log_buffer_compress_time_us");
event_avg_counter
  txn_logger::g_evt_avg_log_entry_ntxns("avg_log_entry_ntxns_per_entry");
event_avg_counter
  txn_logger::g_evt_avg_logger_bytes_per_writev("avg_logger_bytes_per_writev");
event_avg_counter
  txn_logger::g_evt_avg_logger_bytes_per_sec("avg_logger_bytes_per_sec");

static event_avg_counter
  evt_avg_log_buffer_iov_len("avg_log_buffer_iov_len");

void
txn_logger::Init(
    size_t nworkers,
    const vector<string> &logfiles,
    const vector<vector<unsigned>> &assignments_given,
    vector<vector<unsigned>> *assignments_used,
    bool call_fsync,
    bool use_compression,
    bool fake_writes)
{
  INVARIANT(!g_persist);
  INVARIANT(g_nworkers == 0);
  INVARIANT(nworkers > 0);
  INVARIANT(!logfiles.empty());
  INVARIANT(logfiles.size() <= g_nmax_loggers);
  INVARIANT(!use_compression || g_perthread_buffers > 1); // need 1 as scratch buf
  vector<int> fds;
  for (auto &fname : logfiles) {
    int fd = open(fname.c_str(), O_CREAT|O_WRONLY|O_TRUNC, 0664);
    if (fd == -1) {
      perror("open");
      ALWAYS_ASSERT(false);
    }
    fds.push_back(fd);
  }
  g_persist = true;
  g_call_fsync = call_fsync;
  g_use_compression = use_compression;
  g_fake_writes = fake_writes;
  g_nworkers = nworkers;

  for (size_t i = 0; i < g_nmax_loggers; i++)
    for (size_t j = 0; j < g_nworkers; j++)
      per_thread_sync_epochs_[i].epochs_[j].store(0, memory_order_release);

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
              ((i + 1) == fds.size()) ?  g_nworkers : (i + 1) * threads_per_logger));
      }
    }
  }

  INVARIANT(AssignmentsValid(assignments, fds.size(), g_nworkers));

  for (size_t i = 0; i < assignments.size(); i++) {
    writers.emplace_back(
        &txn_logger::writer,
        i, fds[i], assignments[i]);
    writers.back().detach();
  }

  thread persist_thread(&txn_logger::persister, assignments);
  persist_thread.detach();

  if (assignments_used)
    *assignments_used = assignments;
}

void
txn_logger::persister(
    vector<vector<unsigned>> assignments)
{
  timer loop_timer;
  for (;;) {
    const uint64_t last_loop_usec = loop_timer.lap();
    const uint64_t delay_time_usec = ticker::tick_us;
    if (last_loop_usec < delay_time_usec) {
      const uint64_t sleep_ns = (delay_time_usec - last_loop_usec) * 1000;
      struct timespec t;
      t.tv_sec  = sleep_ns / ONE_SECOND_NS;
      t.tv_nsec = sleep_ns % ONE_SECOND_NS;
      nanosleep(&t, nullptr);
    }
    advance_system_sync_epoch(assignments);
  }
}

void
txn_logger::advance_system_sync_epoch(
    const vector<vector<unsigned>> &assignments)
{
  uint64_t min_so_far = numeric_limits<uint64_t>::max();
  const uint64_t best_tick_ex =
    ticker::s_instance.global_current_tick();
  // special case 0
  const uint64_t best_tick_inc =
    best_tick_ex ? (best_tick_ex - 1) : 0;

  for (size_t i = 0; i < assignments.size(); i++)
    for (auto j : assignments[i])
      for (size_t k = j; k < NMAXCORES; k += g_nworkers) {
        persist_ctx &ctx = persist_ctx_for(k, INITMODE_NONE);
        // we need to arbitrarily advance threads which are not "doing
        // anything", so they don't drag down the persistence of the system. if
        // we can see that a thread is NOT in a guarded section AND its
        // core->logger queue is empty, then that means we can advance its sync
        // epoch up to best_tick_inc, b/c it is guaranteed that the next time
        // it does any actions will be in epoch > best_tick_inc
        if (!ctx.persist_buffers_.peek()) {
          spinlock &l = ticker::s_instance.lock_for(k);
          if (!l.is_locked()) {
            bool did_lock = false;
            for (size_t c = 0; c < 3; c++) {
              if (l.try_lock()) {
                did_lock = true;
                break;
              }
            }
            if (did_lock) {
              if (!ctx.persist_buffers_.peek()) {
                min_so_far = min(min_so_far, best_tick_inc);
                per_thread_sync_epochs_[i].epochs_[k].store(
                    best_tick_inc, memory_order_release);
                l.unlock();
                continue;
              }
              l.unlock();
            }
          }
        }
        min_so_far = min(
            per_thread_sync_epochs_[i].epochs_[k].load(
              memory_order_acquire),
            min_so_far);
      }

  const uint64_t syssync =
    system_sync_epoch_->load(memory_order_acquire);

  INVARIANT(min_so_far < numeric_limits<uint64_t>::max());
  INVARIANT(syssync <= min_so_far);

  // need to aggregate from [syssync + 1, min_so_far]
  const uint64_t now_us = timer::cur_usec();
  for (size_t i = 0; i < g_persist_stats.size(); i++) {
    auto &ps = g_persist_stats[i];
    for (uint64_t e = syssync + 1; e <= min_so_far; e++) {
        auto &pes = ps.d_[e % g_max_lag_epochs];
        const uint64_t ntxns_in_epoch = pes.ntxns_.load(memory_order_acquire);
        const uint64_t start_us = pes.earliest_start_us_.load(memory_order_acquire);
        INVARIANT(now_us >= start_us);
        non_atomic_fetch_add(ps.ntxns_persisted_, ntxns_in_epoch);
        non_atomic_fetch_add(
            ps.latency_numer_,
            (now_us - start_us) * ntxns_in_epoch);
        pes.ntxns_.store(0, memory_order_release);
        pes.earliest_start_us_.store(0, memory_order_release);
    }
  }

  system_sync_epoch_->store(min_so_far, memory_order_release);
}

void
txn_logger::writer(
    unsigned id, int fd,
    vector<unsigned> assignment)
{

  if (g_pin_loggers_to_numa_nodes) {
    ALWAYS_ASSERT(!numa_run_on_node(id % numa_num_configured_nodes()));
    ALWAYS_ASSERT(!sched_yield());
  }

  vector<iovec> iovs(
      min(size_t(IOV_MAX), g_nworkers * g_perthread_buffers));
  vector<pbuffer *> pxs;
  timer loop_timer;

  // XXX: sense is not useful for now, unless we want to
  // fsync in the background...
  bool sense = false; // cur is at sense, prev is at !sense
  uint64_t epoch_prefixes[2][NMAXCORES];

  NDB_MEMSET(&epoch_prefixes[0], 0, sizeof(epoch_prefixes[0]));
  NDB_MEMSET(&epoch_prefixes[1], 0, sizeof(epoch_prefixes[1]));

  // NOTE: a core id in the persistence system really represets
  // all cores in the regular system modulo g_nworkers
  size_t nbufswritten = 0, nbyteswritten = 0;
  for (;;) {

    const uint64_t last_loop_usec = loop_timer.lap();
    const uint64_t delay_time_usec = ticker::tick_us;
    // don't allow this loop to proceed less than an epoch's worth of time,
    // so we can batch IO
    if (last_loop_usec < delay_time_usec && nbufswritten < iovs.size()) {
      const uint64_t sleep_ns = (delay_time_usec - last_loop_usec) * 1000;
      struct timespec t;
      t.tv_sec  = sleep_ns / ONE_SECOND_NS;
      t.tv_nsec = sleep_ns % ONE_SECOND_NS;
      nanosleep(&t, nullptr);
    }

    // we need g_persist_stats[cur_sync_epoch_ex % g_nmax_loggers]
    // to remain untouched (until the syncer can catch up), so we
    // cannot read any buffers with epoch >=
    // (cur_sync_epoch_ex + g_max_lag_epochs)
    const uint64_t cur_sync_epoch_ex =
      system_sync_epoch_->load(memory_order_acquire) + 1;
    nbufswritten = nbyteswritten = 0;
    for (auto idx : assignment) {
      INVARIANT(idx >= 0 && idx < g_nworkers);
      for (size_t k = idx; k < NMAXCORES; k += g_nworkers) {
        persist_ctx &ctx = persist_ctx_for(k, INITMODE_NONE);
        ctx.persist_buffers_.peekall(pxs);
        for (auto px : pxs) {
          INVARIANT(px);
          INVARIANT(!px->io_scheduled_);
          INVARIANT(nbufswritten <= iovs.size());
          INVARIANT(px->header()->nentries_);
          INVARIANT(px->core_id_ == k);
          if (nbufswritten == iovs.size()) {
            ++g_evt_logger_writev_limit_met;
            goto process;
          }
          if (transaction_proto2_static::EpochId(px->header()->last_tid_) >=
              cur_sync_epoch_ex + g_max_lag_epochs) {
            ++g_evt_logger_max_lag_wait;
            break;
          }
          iovs[nbufswritten].iov_base = (void *) &px->buf_start_[0];

#ifdef LOGGER_UNSAFE_REDUCE_BUFFER_SIZE
  #define PXLEN(px) (((px)->curoff_ < 4) ? (px)->curoff_ : ((px)->curoff_ / 4))
#else
  #define PXLEN(px) ((px)->curoff_)
#endif

          const size_t pxlen = PXLEN(px);

          iovs[nbufswritten].iov_len = pxlen;
          evt_avg_log_buffer_iov_len.offer(pxlen);
          px->io_scheduled_ = true;
          nbufswritten++;
          nbyteswritten += pxlen;

#ifdef CHECK_INVARIANTS
          auto last_tid_cid = transaction_proto2_static::CoreId(px->header()->last_tid_);
          auto px_cid = px->core_id_;
          if (last_tid_cid != px_cid) {
            cerr << "header: " << *px->header() << endl;
            cerr << g_proto_version_str(last_tid_cid) << endl;
            cerr << "last_tid_cid: " << last_tid_cid << endl;
            cerr << "px_cid: " << px_cid << endl;
          }
#endif

          const uint64_t px_epoch =
            transaction_proto2_static::EpochId(px->header()->last_tid_);
          INVARIANT(
              transaction_proto2_static::CoreId(px->header()->last_tid_) ==
              px->core_id_);
          INVARIANT(epoch_prefixes[sense][k] <= px_epoch);
          INVARIANT(px_epoch > 0);
          epoch_prefixes[sense][k] = px_epoch - 1;
          auto &pes = g_persist_stats[k].d_[px_epoch % g_max_lag_epochs];
          if (!pes.ntxns_.load(memory_order_acquire))
            pes.earliest_start_us_.store(px->earliest_start_us_, memory_order_release);
          non_atomic_fetch_add(pes.ntxns_, px->header()->nentries_);
          g_evt_avg_log_entry_ntxns.offer(px->header()->nentries_);
        }
      }
    }

  process:
    if (!nbufswritten) {
      // XXX: should probably sleep here
      nop_pause();
      continue;
    }

    const bool dosense = sense;

    if (!g_fake_writes) {
#ifdef ENABLE_EVENT_COUNTERS
      timer write_timer;
#endif
      const ssize_t ret = writev(fd, &iovs[0], nbufswritten);
      if (unlikely(ret == -1)) {
        perror("writev");
        ALWAYS_ASSERT(false);
      }

      if (g_call_fsync) {
        const int fret = fdatasync(fd);
        if (unlikely(fret == -1)) {
          perror("fdatasync");
          ALWAYS_ASSERT(false);
        }
      }

#ifdef ENABLE_EVENT_COUNTERS
      {
        g_evt_avg_logger_bytes_per_writev.offer(nbyteswritten);
        const double bytes_per_sec =
          double(nbyteswritten)/(write_timer.lap_ms() / 1000.0);
        g_evt_avg_logger_bytes_per_sec.offer(bytes_per_sec);
      }
#endif
    }

    // update metadata from previous write
    //
    // return all buffers that have been io_scheduled_ - we can do this as
    // soon as write returns. we take care to return to the proper buffer
    epoch_array &ea = per_thread_sync_epochs_[id];
    for (auto idx: assignment) {
      for (size_t k = idx; k < NMAXCORES; k += g_nworkers) {
        const uint64_t x0 = ea.epochs_[k].load(memory_order_acquire);
        const uint64_t x1 = epoch_prefixes[dosense][k];
        if (x1 > x0)
          ea.epochs_[k].store(x1, memory_order_release);

        persist_ctx &ctx = persist_ctx_for(k, INITMODE_NONE);
        pbuffer *px, *px0;
        while ((px = ctx.persist_buffers_.peek()) && px->io_scheduled_) {
#ifdef LOGGER_STRIDE_OVER_BUFFER
          {
            const size_t pxlen = PXLEN(px);
            const size_t stridelen = 1;
            for (size_t p = 0; p < pxlen; p += stridelen)
              if ((&px->buf_start_[0])[p] & 0xF)
                non_atomic_fetch_add(ea.dummy_work_, 1UL);
          }
#endif
          px0 = ctx.persist_buffers_.deq();
          INVARIANT(px == px0);
          INVARIANT(px->header()->nentries_);
          px0->reset();
          INVARIANT(ctx.init_);
          INVARIANT(px0->core_id_ == k);
          ctx.all_buffers_.enq(px0);
        }
      }
    }

    // bump the sense
    sense = !sense;
  }
}

tuple<uint64_t, uint64_t, double>
txn_logger::compute_ntxns_persisted_statistics()
{
  uint64_t acc = 0, acc1 = 0, acc2 = 0;
  uint64_t num = 0;
  for (size_t i = 0; i < g_persist_stats.size(); i++) {
    acc  += g_persist_stats[i].ntxns_persisted_.load(memory_order_acquire);
    acc1 += g_persist_stats[i].ntxns_pushed_.load(memory_order_acquire);
    acc2 += g_persist_stats[i].ntxns_committed_.load(memory_order_acquire);
    num  += g_persist_stats[i].latency_numer_.load(memory_order_acquire);
  }
  INVARIANT(acc <= acc1);
  INVARIANT(acc1 <= acc2);
  if (acc == 0)
    return make_tuple(0, acc1, 0.0);
  return make_tuple(acc, acc1, double(num)/double(acc));
}

void
txn_logger::clear_ntxns_persisted_statistics()
{
  for (size_t i = 0; i < g_persist_stats.size(); i++) {
    auto &ps = g_persist_stats[i];
    ps.ntxns_persisted_.store(0, memory_order_release);
    ps.ntxns_pushed_.store(0, memory_order_release);
    ps.ntxns_committed_.store(0, memory_order_release);
    ps.latency_numer_.store(0, memory_order_release);
    for (size_t e = 0; e < g_max_lag_epochs; e++) {
      auto &pes = ps.d_[e];
      pes.ntxns_.store(0, memory_order_release);
      pes.earliest_start_us_.store(0, memory_order_release);
    }
  }
}

void
txn_logger::wait_for_idle_state()
{
  for (size_t i = 0; i < NMAXCORES; i++) {
    persist_ctx &ctx = persist_ctx_for(i, INITMODE_NONE);
    if (!ctx.init_)
      continue;
    pbuffer *px;
    while (!(px = ctx.all_buffers_.peek()) || px->header()->nentries_)
      nop_pause();
    while (ctx.persist_buffers_.peek())
      nop_pause();
  }
}

void
txn_logger::wait_until_current_point_persisted()
{
  const uint64_t e = ticker::s_instance.global_current_tick();
  cerr << "waiting for system_sync_epoch_="
       << system_sync_epoch_->load(memory_order_acquire)
       << " to be < e=" << e << endl;
  while (system_sync_epoch_->load(memory_order_acquire) < e)
    nop_pause();
}
/*}}}*/

                /** garbage collection subsystem **/

static event_counter evt_local_chain_cleanups("local_chain_cleanups");
static event_counter evt_try_delete_unlinks("try_delete_unlinks");
static event_counter evt_proto_gc_delete_requeue("proto_gc_delete_requeue");
static event_avg_counter evt_avg_proto_gc_queue_len("avg_proto_gc_queue_len");
static event_avg_counter evt_avg_proto_gc_loop_iter_usec("avg_proto_gc_loop_iter_usec");

void
transaction_proto2_static::Init()
{
  static spinlock s_lock;
  static bool s_init = false;
  if (likely(s_init))
    return;
  ::lock_guard<spinlock> l(s_lock);
  if (s_init)
    return;
  for (size_t i = 0; i < g_num_gc_workers; i++)
    thread(&transaction_proto2_static::gcloop, i).detach();
  s_init = true;
}

void
transaction_proto2_static::WaitForGCThroughNow()
{
  INVARIANT(!rcu::s_instance.in_rcu_region());
  const uint64_t ro_tick =
    to_read_only_tick(ticker::s_instance.global_current_tick());
  cerr << "WaitForGCThroughNow(): wait to catch up to ro_tick=" << ro_tick << endl;
  spin_barrier b(g_threadctxs.size());
  for (size_t i = 0; i < g_threadctxs.size(); i++) {
    threadctx &ctx = g_threadctxs[i];
    ::lock_guard<spinlock> lg(ctx.queue_locks_[ro_tick % g_ngcqueues]);
    ctx.queues_[ro_tick % g_ngcqueues].enqueue(delete_entry(&b), ro_tick);
  }
  b.wait_for();
}

void
transaction_proto2_static::gcloop(unsigned i)
{
  // runs as daemon
  timer loop_timer;
  struct timespec t;
  px_queue scratch_queue;
  vector<string *> scratch_vec;
  for (;;) {
    const uint64_t last_loop_usec = loop_timer.lap();
    const uint64_t delay_time_usec = ReadOnlyEpochUsec;
    if (last_loop_usec < delay_time_usec) {
      const uint64_t sleep_ns = (delay_time_usec - last_loop_usec) * 1000;
      t.tv_sec  = sleep_ns / ONE_SECOND_NS;
      t.tv_nsec = sleep_ns % ONE_SECOND_NS;
      nanosleep(&t, nullptr);
    }
    evt_avg_proto_gc_loop_iter_usec.offer(last_loop_usec);

    // figure out what the current cleanable epoch is
    const uint64_t last_tick_ex = ticker::s_instance.global_last_tick_exclusive();
    const uint64_t ro_tick_ex = to_read_only_tick(last_tick_ex);
    if (!ro_tick_ex)
      // won't have anything to clean
      continue;
    // all reads happening at >= ro_tick_geq
    const uint64_t ro_tick_geq = ro_tick_ex - 1;

#ifdef CHECK_INVARIANTS
    uint64_t last_consistent_tid = 0;
    {
      const uint64_t a = (last_tick_ex / ReadOnlyEpochMultiplier);
      const uint64_t b = a * ReadOnlyEpochMultiplier;
      if (!b)
        last_consistent_tid = MakeTid(0, 0, 0);
      else
        last_consistent_tid = MakeTid(CoreMask, NumIdMask >> NumIdShift, b - 1);
    }
#endif

    // NOTE: consistent reads happening >= ro_tick_geq
    for (size_t k = i; k < g_threadctxs.size(); k += g_num_gc_workers) {
      threadctx &ctx = g_threadctxs[k];
      for (size_t qnum = 0; qnum < g_ngcqueues; qnum++) {
        // get work
        {
          ::lock_guard<spinlock> lg(ctx.queue_locks_[qnum]);
          px_queue &cq = ctx.queues_[qnum];
          scratch_queue.empty_accept_from(cq, ro_tick_geq);
          scratch_queue.transfer_freelist(cq);
        }

        px_queue &q = scratch_queue;
        if (q.empty())
          continue;
        size_t n = 0;
        for (auto it = q.begin(); it != q.end(); ++it, ++n) {
          auto &delent = *it;
          if (unlikely(delent.is_barrier())) {
            //cerr << "barrier found in gcloop=" << i << " (worker " << k << ")" << endl;
            delent.barrier()->count_down();
            continue;
          }
          if (!delent.key_.get_flags()) {
            // guaranteed to be gc-able now (even w/o RCU)
#ifdef CHECK_INVARIANTS
            if (delent.trigger_tid_ > last_consistent_tid) {
              cerr << "tuple ahead     : " << g_proto_version_str(delent.tuple_ahead_->version) << endl;
              cerr << "tuple ahead     : " << *delent.tuple_ahead_ << endl;
              cerr << "trigger tid     : " << g_proto_version_str(delent.trigger_tid_) << endl;
              cerr << "tuple           : " << g_proto_version_str(delent.tuple()->version) << endl;
              cerr << "last_consist_tid: " << g_proto_version_str(last_consistent_tid) << endl;
              cerr << "last_tick_ex    : " << last_tick_ex << endl;
              cerr << "ro_tick_geq     : " << ro_tick_geq << endl;
              cerr << "rcu_block_tick  : " << it.tick() << endl;
            }
            INVARIANT(delent.trigger_tid_ <= last_consistent_tid);
#endif
            // XXX: should walk tuple_ahead_'s chain and make sure some element
            // blocks reads
            dbtuple::release_no_rcu(delent.tuple());
          } else {
            INVARIANT(!delent.tuple_ahead_);
            INVARIANT(delent.btr_);
            // check if an element preceeds the (deleted) tuple before doing the delete
            ::lock_guard<dbtuple> lg_tuple(delent.tuple(), false);
#ifdef CHECK_INVARIANTS
            if (!delent.tuple()->is_not_behind(last_consistent_tid)) {
              cerr << "trigger tid     : " << g_proto_version_str(delent.trigger_tid_) << endl;
              cerr << "tuple           : " << g_proto_version_str(delent.tuple()->version) << endl;
              cerr << "last_consist_tid: " << g_proto_version_str(last_consistent_tid) << endl;
              cerr << "last_tick_ex    : " << last_tick_ex << endl;
              cerr << "ro_tick_geq     : " << ro_tick_geq << endl;
              cerr << "rcu_block_tick  : " << it.tick() << endl;
            }
            INVARIANT(delent.tuple()->version == delent.trigger_tid_);
            INVARIANT(delent.tuple()->is_not_behind(last_consistent_tid));
            INVARIANT(delent.tuple()->is_deleting());
#endif
            if (unlikely(!delent.tuple()->is_latest())) {
              // requeue it up, in this threads context, except this
              // time as a regular delete
              const uint64_t my_ro_tick = to_read_only_tick(
                  ticker::s_instance.global_current_tick());
              threadctx &myctx = g_threadctxs.my();
              ::lock_guard<spinlock> lg_queue(
                  myctx.queue_locks_[my_ro_tick % g_ngcqueues]);
              myctx.queues_[my_ro_tick % g_ngcqueues].enqueue(
                  delete_entry(nullptr, 0, delent.tuple(),
                    marked_ptr<string>(), nullptr),
                  my_ro_tick);
              ++evt_proto_gc_delete_requeue;
              // reclaim string ptrs
              string *spx = delent.key_.get();
              if (unlikely(spx))
                scratch_vec.emplace_back(spx);
              continue;
            }
            // if delent.key_ is nullptr, then the key is stored in the tuple
            // record storage location, and the size field contains the length of
            // the key
            //
            // otherwise, delent.key_ is a pointer to a string containing the
            // key
            varkey k;
            string *spx = delent.key_.get();
            if (likely(!spx)) {
              k = varkey(delent.tuple()->get_value_start(), delent.tuple()->size);
            } else {
              k = varkey(*spx);
              scratch_vec.emplace_back(spx);
            }

            scoped_rcu_region rcu_guard;
            btree::value_type removed = 0;
            const bool did_remove = delent.btr_->remove(k, &removed);
            if (!did_remove)
              INVARIANT(false);
            INVARIANT(removed == (btree::value_type) delent.tuple());
            delent.tuple()->clear_latest();
            dbtuple::release(delent.tuple()); // rcu free it
          }
        }
        q.clear();
        evt_avg_proto_gc_queue_len.offer(n);
      }
      // return strings
      if (!scratch_vec.empty()) {
        ::lock_guard<spinlock> lg(ctx.pool_lock_);
        ctx.pool_.insert(ctx.pool_.end(), scratch_vec.begin(), scratch_vec.end());
      }
      scratch_vec.clear();
    }
  }
}

aligned_padded_elem<transaction_proto2_static::hackstruct>
  transaction_proto2_static::g_hack;
percore<transaction_proto2_static::threadctx>
  transaction_proto2_static::g_threadctxs;
event_counter
  transaction_proto2_static::g_evt_worker_thread_wait_log_buffer(
      "worker_thread_wait_log_buffer");
event_counter
  transaction_proto2_static::g_evt_dbtuple_no_space_for_delkey(
      "dbtuple_no_space_for_delkey");
event_avg_counter
  transaction_proto2_static::g_evt_avg_log_entry_size(
      "avg_log_entry_size");
