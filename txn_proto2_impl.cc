#include <iostream>
#include <thread>
#include <fcntl.h>
#include <unistd.h>
#include <sys/uio.h>

#include "txn_proto2_impl.h"
#include "counter.h"
#include "util.h"

using namespace std;
using namespace util;

static event_counter evt_local_chain_cleanups("local_chain_cleanups");
static event_counter evt_try_delete_unlinks("try_delete_unlinks");

bool txn_logger::g_persist = false;
size_t txn_logger::g_nworkers = 0;
txn_logger::epoch_array
  txn_logger::per_thread_sync_epochs_[txn_logger::g_nmax_loggers];
aligned_padded_elem<atomic<uint64_t>>
  txn_logger::system_sync_epoch_(0);
aligned_padded_elem<circbuf<txn_logger::pbuffer, txn_logger::g_perthread_buffers>>
  txn_logger::g_all_buffers[NMAXCORES];
bool
  txn_logger::g_all_buffers_init[NMAXCORES] = {false};
aligned_padded_elem<circbuf<txn_logger::pbuffer, txn_logger::g_perthread_buffers>>
  txn_logger::g_persist_buffers[NMAXCORES];

void
txn_logger::Init(
    size_t nworkers,
    const std::vector<std::string> &logfiles,
    const std::vector<std::vector<unsigned>> &assignments_given,
    std::vector<std::vector<unsigned>> *assignments_used)
{
  INVARIANT(!g_persist);
  INVARIANT(g_nworkers == 0);
  INVARIANT(nworkers > 0);
  INVARIANT(!logfiles.empty());
  INVARIANT(logfiles.size() <= g_nmax_loggers);
  std::vector<int> fds;
  for (auto &fname : logfiles) {
    int fd = open(fname.c_str(), O_CREAT|O_WRONLY|O_TRUNC, 0664);
    if (fd == -1) {
      perror("open");
      ALWAYS_ASSERT(false);
    }
    fds.push_back(fd);
  }
  g_persist = true;
  g_nworkers = nworkers;

  for (size_t i = 0; i < g_nmax_loggers; i++)
    for (size_t j = 0; j < g_nworkers; j++)
      per_thread_sync_epochs_[i].epochs_[j].store(0, std::memory_order_release);

  for (size_t i = 0; i < g_nworkers; i++) {
    for (size_t j = 0; j < g_perthread_buffers; j++) {
      struct pbuffer *p = new pbuffer(i);
      g_all_buffers[i]->enq(p);
    }
    g_all_buffers_init[i] = true;
  }

  std::vector<std::thread> writers;
  std::vector<std::vector<unsigned>> assignments(assignments_given);

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

  std::thread persist_thread(&txn_logger::persister, assignments);
  persist_thread.detach();

  if (assignments_used)
    *assignments_used = assignments;
}

void
txn_logger::persister(
    std::vector<std::vector<unsigned>> assignments)
{
  for (;;) {
    struct timespec t;
    t.tv_sec  = g_epoch_time_ns / ONE_SECOND_NS;
    t.tv_nsec = g_epoch_time_ns % ONE_SECOND_NS;
    nanosleep(&t, nullptr);
    advance_system_sync_epoch(assignments);
  }
}

void
txn_logger::advance_system_sync_epoch(
    const std::vector<std::vector<unsigned>> &assignments)
{
  uint64_t min_so_far = std::numeric_limits<uint64_t>::max();
  for (size_t i = 0; i < assignments.size(); i++)
    for (auto j : assignments[i])
      min_so_far =
        std::min(per_thread_sync_epochs_[i].epochs_[j].load(std::memory_order_acquire), min_so_far);

#ifdef CHECK_INVARIANTS
  const uint64_t syssync = system_sync_epoch_->load(std::memory_order_acquire);
  INVARIANT(syssync <= min_so_far);
#endif
  system_sync_epoch_->store(min_so_far, std::memory_order_release);
}

void
txn_logger::writer(
    unsigned id, int fd,
    std::vector<unsigned> assignment)
{
  std::vector<iovec> iovs(g_nworkers * g_perthread_buffers);
  std::vector<pbuffer *> pxs;
  struct timespec last_io_completed;
  uint64_t total_nbytes_written = 0,
           total_txns_written = 0;

  bool sense = false; // cur is at sense, prev is at !sense
  uint64_t nbytes_written[2], txns_written[2], epoch_prefixes[2][g_nworkers];
  memset(&nbytes_written[0], 0, sizeof(nbytes_written));
  memset(&txns_written[0], 0, sizeof(txns_written));
  memset(&epoch_prefixes[0], 0, sizeof(epoch_prefixes[0]));
  memset(&epoch_prefixes[1], 0, sizeof(epoch_prefixes[1]));

  clock_gettime(CLOCK_MONOTONIC, &last_io_completed);

  // NOTE: a core id in the persistence system really represets
  // all cores in the regular system modulo g_nworkers
  for (;;) {

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
      g_persist_buffers[idx]->peekall(pxs);
      for (auto px : pxs) {
        INVARIANT(px);
        INVARIANT(!px->io_scheduled_);
        INVARIANT(nwritten <= iovs.size());
        INVARIANT(px->header()->nentries_);
        INVARIANT((px->core_id_ % g_nworkers) == id);
        if (nwritten == iovs.size())
          break;
        iovs[nwritten].iov_base = (void *) px->buf_.data();
        iovs[nwritten].iov_len = px->curoff_;
        nbytes_written[sense] += px->curoff_;
        px->io_scheduled_ = true;
        txns_written[sense] += px->header()->nentries_;
        nwritten++;

        auto last_tid_cid = transaction_proto2_static::CoreId(px->header()->last_tid_);
        auto px_cid = px->core_id_;
        if (last_tid_cid != px_cid) {
          cerr << "header: " << *px->header() << endl;
          cerr << g_proto_version_str(last_tid_cid) << endl;
          cerr << "last_tid_cid: " << last_tid_cid << endl;
          cerr << "px_cid: " << px_cid << endl;
        }

        INVARIANT(
            transaction_proto2_static::CoreId(px->header()->last_tid_) ==
            px->core_id_);
        INVARIANT(
            epoch_prefixes[sense][id] <=
            transaction_proto2_static::EpochId(px->header()->last_tid_));
        INVARIANT(
            transaction_proto2_static::EpochId(px->header()->last_tid_) > 0);
        epoch_prefixes[sense][id] =
          transaction_proto2_static::EpochId(px->header()->last_tid_) - 1;
      }
    }

    if (!nwritten) {
      // XXX: should probably sleep here
      nop_pause();
      continue;
    }

    const ssize_t ret =
      nwritten ? writev(fd, &iovs[0], nwritten) : 0;
    if (ret == -1) {
      perror("writev");
      ALWAYS_ASSERT(false);
    }

    const bool dosense = sense;
    const int fret = fdatasync(fd);
    if (fret == -1) {
      perror("fdatasync");
      ALWAYS_ASSERT(false);
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
    // soon as write returns. we take care to return to the proper buffer
    // (not modulo)
    for (auto idx : assignment) {
      pbuffer *px, *px0;
      while ((px = g_persist_buffers[idx]->peek()) &&
             px->io_scheduled_) {
        px0 = g_persist_buffers[idx]->deq();
        INVARIANT(px == px0);
        px0->reset();
        g_all_buffers[px0->core_id_]->enq(px0);
      }
    }
  }

  //g_bytes_written[id].store(total_nbytes_written, memory_order_release);
  //g_ntxns_written.fetch_add(total_txns_written, memory_order_release);
}

void
transaction_proto2_static::do_dbtuple_chain_cleanup(dbtuple *ln, uint64_t ro_epoch_clean)
{
  // try to clean up the chain
  INVARIANT(ln->is_locked());
  INVARIANT(ln->is_lock_owner());
  INVARIANT(ln->is_latest());
  const uint64_t e = (ro_epoch_clean + 1) * ReadOnlyEpochMultiplier - 1;
  struct dbtuple *p = ln, *pprev = 0;
  const bool has_chain = ln->get_next();
  bool do_break = false;
  while (p) {
    INVARIANT(p == ln || !p->is_latest());
    if (do_break)
      break;
    do_break = false;
    if (EpochId(p->version) <= e)
      do_break = true;
    pprev = p;
    p = p->get_next();
  }
  if (p) {
    INVARIANT(p != ln);
    INVARIANT(pprev);
    INVARIANT(!p->is_latest());
    INVARIANT(EpochId(p->version) <= e);
    // check that p can be safely removed because it is covered
    // by pprev
    INVARIANT(EpochId(pprev->version) <= e);
    INVARIANT(pprev->version > p->version);
    g_max_gc_version_inc->store(
      std::max(
        g_max_gc_version_inc->load(std::memory_order_acquire),
        EpochId(p->version)),
      std::memory_order_release);
    pprev->set_next(NULL);
    p->gc_chain();
  }
  if (has_chain && !ln->get_next())
    ++evt_local_chain_cleanups;
}

bool
transaction_proto2_static::try_dbtuple_cleanup(
    btree *btr, const string &key, dbtuple *tuple,
    uint64_t ro_epoch_clean)
{
  // note: we can clean <= ro_epoch_clean
  const uint64_t e = (ro_epoch_clean + 1) * ReadOnlyEpochMultiplier - 1;

  INVARIANT(rcu::s_instance.in_rcu_region());

  const dbtuple::version_t vcheck = tuple->unstable_version();

  if (!dbtuple::IsLatest(vcheck) /* won't be able to do anything */ ||
      tuple->version == dbtuple::MAX_TID /* newly inserted node, nothing to GC */)
    return true;

  // check to see if theres a chain to remove
  dbtuple *p = tuple->get_next();
  bool has_work = !tuple->size;
  while (p && !has_work) {
    if (EpochId(p->version) <= e) {
      has_work = p->get_next();
      break;
    }
    p = p->get_next();
  }
  if (!has_work)
    return true;

  bool ret = false;
  ::lock_guard<dbtuple> lock(tuple, false); // not for write (just for cleanup)

  if (!tuple->is_latest())
    // was replaced, so get it the next time around
    return false;

  do_dbtuple_chain_cleanup(tuple, ro_epoch_clean);

  if (!tuple->size && !tuple->is_deleting()) {
    // latest version is a deleted entry, so try to delete
    // from the tree
    const uint64_t v = EpochId(tuple->version);
    if (e < v) {
      ret = true;
    } else {
      // e >= v: we don't require e > v as in tuple chain cleanup, b/c removes
      // are a special case: whether or not a consistent snapshot reads a
      // removed element by its absense or by an empty record is irrelevant.
      btree::value_type removed = 0;
      const bool did_remove = btr->remove(varkey(key), &removed);
      if (!did_remove) {
        cerr << " *** could not remove key: " << hexify(key) << endl;
#ifdef TUPLE_CHECK_KEY
        cerr << " *** original key        : " << hexify(tuple->key) << endl;
#endif
        INVARIANT(false);
      }
      INVARIANT(removed == (btree::value_type) tuple);
      dbtuple::release(tuple); // release() marks deleted
      ++evt_try_delete_unlinks;
      g_max_unlink_version_inc->store(
        std::max(
          g_max_unlink_version_inc->load(std::memory_order_acquire), v),
        std::memory_order_release);
    }
  } else {
    ret = tuple->get_next();
  }

  return ret;
}

// XXX: kind of a duplicate of btree::leaf_kvinfo
struct leaf_value_desc {
  inline leaf_value_desc()
    : key(), len(), key_big_endian(), suffix(), value() {}
  inline leaf_value_desc(btree::key_slice key, size_t len,
                  const varkey &suffix, btree::value_type value)
    : key(key),
      len(len),
      key_big_endian(big_endian_trfm<btree::key_slice>()(key)),
      suffix(suffix),
      value(value) {}
  inline const char *
  keyslice() const
  {
    return (const char *) &key_big_endian;
  }
  btree::key_slice key;
  size_t len;
  btree::key_slice key_big_endian;
  varkey suffix;
  btree::value_type value;
};

static event_avg_counter evt_avg_txn_walker_loop_iter_us("avg_txn_walker_loop_iter_us");

volatile bool txn_walker_loop::global_running = true;

void
txn_walker_loop::run()
{
#ifdef ENABLE_EVENT_COUNTERS
  event_avg_counter *evt_avg_records_per_walk = nullptr;
  size_t ntuples = 0;
  if (name != "<unknown>")
    evt_avg_records_per_walk = new event_avg_counter("avg_records_walk_" + name);
#endif
  size_t nodesperrun = 100;
  string s; // the starting key of the scan
  timer big_loop_timer, loop_timer;
  struct timespec ts;
  typename vec<btree::leaf_node *>::type q;

  //string name_check = "order_line";

  while (running && global_running) {
    // we don't use btree::tree_walk here because we want
    // to only scan parts of the tree under a single RCU
    // region

    size_t nnodes = 0;
    {
      scoped_rcu_region rcu_region;
      btree::value_type v = 0;

      // round up s to 8 byte boundaries for ease of computation
      if (s.empty())
        s.resize(8);
      else
        s.resize(round_up<size_t, 3>(s.size()));
      q.clear();

      btr->search_impl(varkey(s), v, q);
      INVARIANT(!q.empty());
      INVARIANT(s.size() % 8 == 0);
      INVARIANT(s.size() / 8 >= q.size());

      size_t depth = q.size() - 1;

      //if (name == name_check) {
      //  cerr << "----- starting over from s as min key ----- " << endl;
      //  cerr << "s_start: " << hexify(s) << endl;
      //  cerr << "q.size(): " << q.size() << endl;
      //}

      // NB:
      //   s[0, 8 * (q.size() - 1)) contains the key prefix
      //   s[8 * (q.size() - 1), s.size()) contains the key suffix
      bool include_kmin = true;
      while (!q.empty()) {
      descend:

        //if (name == name_check) {
        //  cerr << "descending depth: " << q.size() - 1 << endl;
        //  cerr << "s: " << hexify(s) << endl;
        //}

        const btree::key_slice kmin =
          host_endian_trfm<btree::key_slice>()(
              *reinterpret_cast<const btree::key_slice *>(
                s.data() + 8 * (q.size() - 1)));

        //if (name == name_check) {
        //  cerr << "kmin: " << hexify(string(s.data() + 8 * (q.size() - 1), 8)) << endl;
        //  cerr << "include_kmin: " << include_kmin << endl;
        //}

        btree::leaf_node *cur = q.back();
        q.pop_back();

        // now:
        //  s[0, 8 * q.size()) contains key prefix
        INVARIANT(depth == q.size());

        while (cur) {
          if (++nnodes >= nodesperrun) {
            //if (name == name_check)
            //  cerr << "rcu recalc" << endl;
            goto recalc;
          }
        process:
          INVARIANT(depth == q.size());

          const uint64_t version = cur->stable_version();
          if (btree::node::IsDeleting(version)) {
            // skip deleted nodes, because their suffixes will be empty
            // NB: can read next ptr w/o validation, because stable deleting
            // nodes will not mutate
            cur = cur->next;
            continue;
          }
          const size_t n = cur->key_slots_used();
          typename vec<pair<btree::key_slice, btree::node *>>::type layers;
          typename vec<leaf_value_desc>::type values;
          for (size_t i = 0; i < n; i++) {
            if ((include_kmin && cur->keys[i] < kmin) ||
                (!include_kmin && cur->keys[i] <= kmin))
              continue;
            if (cur->value_is_layer(i))
              layers.emplace_back(cur->keys[i], cur->values[i].n);
            else
              values.emplace_back(
                  cur->keys[i],
                  cur->keyslice_length(i),
                  cur->suffix(i),
                  cur->values[i].v);
          }
          btree::leaf_node * const next = cur->next;
          if (unlikely(!cur->check_version(version)))
            goto process;

          // figure out what the current cleanable epoch is
          const uint64_t last_tick_ex = ticker::s_instance.global_last_tick_exclusive();
          const uint64_t ro_tick_ex = transaction_proto2_static::to_read_only_tick(last_tick_ex);
          if (ro_tick_ex <= 1)
            // won't have anything to clean
            goto waitepoch;

          // NOTE: consistent reads happening >= (ro_tick_ex - 1),
          // so we can clean < (ro_tick_ex - 1) or <= (ro_tick_ex - 2)

          // process all values
          for (size_t i = 0; i < values.size(); i++) {
            dbtuple * const tuple = reinterpret_cast<dbtuple *>(values[i].value);
            const size_t klen = values[i].len;
            INVARIANT(klen <= 9);
            INVARIANT(tuple);
            if (klen == 9) {
              INVARIANT(values[i].suffix.size() > 0);
              s.resize(8 * (q.size() + 1) + values[i].suffix.size());
              NDB_MEMCPY((char *) s.data() + 8 * q.size(), values[i].keyslice(), 8);
              NDB_MEMCPY((char *) s.data() + 8 * (q.size() + 1),
                         values[i].suffix.data(), values[i].suffix.size());
            } else {
              s.resize(8 * q.size() + klen);
              NDB_MEMCPY((char *) s.data() + 8 * q.size(), values[i].keyslice(), klen);
            }
#ifdef ENABLE_EVENT_COUNTERS
            ntuples++;
#endif
            transaction_proto2_static::try_dbtuple_cleanup(btr, s, tuple, ro_tick_ex - 2);
          }

          // deal w/ the layers
          if (!layers.empty()) {
            const btree::key_slice k =
              big_endian_trfm<btree::key_slice>()(layers[0].first);

            // NB: at this point, s[0, 8 * q.size()) contains the key prefix-
            // ie the bytes *not* including the current layer.
            //
            // adjust s[8 * q.size(), 8 * (q.size() + 1)) to contain the
            // the next key_slice for the next layer
            s.resize(8 * (q.size() + 1));
            *((btree::key_slice *) (s.data() + (8 * q.size()))) = k;

            q.push_back(cur);
            INVARIANT(!q.empty());
            INVARIANT(q.size());

            // find leftmost leaf node of this new layer
            btree::leaf_node * const l = btr->leftmost_descend_layer(layers[0].second);
            INVARIANT(l);
            const btree::key_slice k0 =
              big_endian_trfm<btree::key_slice>()(l->min_key);

            s.resize(8 * (q.size() + 1));
            *((btree::key_slice *) (s.data() + (8 * q.size()))) = k0;

            q.push_back(l);

            include_kmin = true;
            depth++;
            goto descend; // descend the next layer
          }

          cur = next;
        }

        //if (name == name_check)
        //  cout << "finished layer " << depth << endl;

        // finished this layer
        include_kmin = false;
        depth--;
      }

      // finished an entire scan of the tree
#ifdef ENABLE_EVENT_COUNTERS
      if (evt_avg_records_per_walk)
        evt_avg_records_per_walk->offer(ntuples);
      ntuples = 0;
#endif
      //if (name == name_check)
      //  cout << name << " finished tree walk" << endl;
      s.clear();
      goto waitepoch;

    } // end RCU region

    // do TL cleanup
    rcu::s_instance.threadpurge();

  recalc:
    {
      // very simple heuristic
      const uint64_t us = loop_timer.lap();
      const double actual_rate = double(nodesperrun) / double(us); // nodes/usec
      nodesperrun = size_t(actual_rate * double(rcu::EpochTimeUsec));
      // lower and upper bounds
      const size_t nlowerbound = 100;
      const size_t nupperbound = 10000;
      nodesperrun = max(nlowerbound, nodesperrun);
      nodesperrun = min(nupperbound, nodesperrun);
      evt_avg_txn_walker_loop_iter_us.offer(us);

      // sleep if we are going too fast
      //if (us >= rcu::EpochTimeUsec)
      //  continue;
      //const uint64_t sleep_ns = (rcu::EpochTimeUsec - us) * 1000;

      const uint64_t sleep_ns = rcu::EpochTimeUsec * 1000;
      ts.tv_sec  = sleep_ns / ONE_SECOND_NS;
      ts.tv_nsec = sleep_ns % ONE_SECOND_NS;
      nanosleep(&ts, NULL);
      continue;
    }

  waitepoch:
    {
      // since nothing really changes within an epoch, we sleep for an epoch's
      // worth of time if necessary

      //const uint64_t us = big_loop_timer.lap();
      //if (us >= txn_epoch_us)
      //  continue;
      //const uint64_t sleep_ns = (txn_epoch_us - us) * 1000;

      rcu::s_instance.try_release();
      const uint64_t sleep_ns = transaction_proto2_static::ReadOnlyEpochUsec * 1000;
      ts.tv_sec  = sleep_ns / ONE_SECOND_NS;
      ts.tv_nsec = sleep_ns % ONE_SECOND_NS;
      nanosleep(&ts, NULL);
      continue;
    }
  }
}

percore<uint64_t>
  transaction_proto2_static::g_last_commit_tids;
aligned_padded_elem<atomic<uint64_t>>
  transaction_proto2_static::g_max_gc_version_inc(0);
aligned_padded_elem<atomic<uint64_t>>
  transaction_proto2_static::g_max_unlink_version_inc(0);
aligned_padded_elem<transaction_proto2_static::hackstruct>
  transaction_proto2_static::g_hack;
