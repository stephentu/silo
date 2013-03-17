#include <iostream>

#include "txn_proto2_impl.h"
#include "counter.h"
#include "util.h"

using namespace std;
using namespace util;


static event_counter evt_local_cleanup_reschedules("local_cleanup_reschedules");
static event_counter evt_local_chain_cleanups("local_chain_cleanups");
static event_counter evt_try_delete_revivals("try_delete_revivals");
static event_counter evt_try_delete_reschedules("try_delete_reschedules");
static event_counter evt_try_delete_unlinks("try_delete_unlinks");

static inline bool
chain_contains_enqueued(const dbtuple *p)
{
  const dbtuple *cur = p;
  while (cur) {
    if (cur->is_enqueued())
      return true;
    cur = cur->get_next();
  }
  return false;
}

void
transaction_proto2_static::do_dbtuple_chain_cleanup(dbtuple *ln)
{
  // try to clean up the chain
  INVARIANT(ln->is_latest());
  struct dbtuple *p = ln, *pprev = 0;
  const bool has_chain = ln->get_next();
  bool do_break = false;
  while (p) {
    INVARIANT(p == ln || !p->is_latest());
    if (do_break)
      break;
    do_break = false;
    if (EpochId(p->version) <= g_reads_finished_epoch)
      do_break = true;
    pprev = p;
    p = p->get_next();
  }
  if (p) {
    INVARIANT(pprev);
    // can only GC a continous chain of not-enqueued.
    dbtuple *last_enq = NULL, *cur = p;
    while (cur) {
      if (cur->is_enqueued())
        last_enq = cur;
      cur = cur->get_next();
    }
    p = last_enq ? last_enq->get_next() : p;
  }
  if (p) {
    INVARIANT(p != ln);
    INVARIANT(pprev);
    INVARIANT(!p->is_latest());
    INVARIANT(!chain_contains_enqueued(p));
    pprev->set_next(NULL);
    p->gc_chain();
  }
  if (has_chain && !ln->get_next())
    ++evt_local_chain_cleanups;
}

bool
transaction_proto2_static::try_dbtuple_cleanup(btree *btr, const string &key, dbtuple *tuple)
{
  INVARIANT(rcu::in_rcu_region());

  // first read the node w/o latching to see if there is potential work to do
  const dbtuple::version_t vcheck = tuple->unstable_version();

  // easy checks
  if (!dbtuple::IsLatest(vcheck) ||
      dbtuple::IsDeleting(vcheck) ||
      tuple->version == dbtuple::MIN_TID)
    return true;

  // check to see if theres a chain to remove
  dbtuple *p = tuple->get_next();
  bool has_work = !tuple->size;
  while (p && !has_work) {
    if (EpochId(p->version) <= g_reads_finished_epoch)
      has_work = true;
    p = p->get_next();
  }
  if (!has_work)
    return true;

  bool ret = false;
  lock_guard<dbtuple> lock(tuple);

  if (!tuple->is_latest())
    // was replaced, so let the newer handlers do the work
    return false;

  do_dbtuple_chain_cleanup(tuple);

  if (!tuple->size) {
    // latest version is a deleted entry, so try to delete
    // from the tree
    const uint64_t v = EpochId(tuple->version);
    if (g_reads_finished_epoch < v || chain_contains_enqueued(tuple)) {
      ret = true;
    } else {
      btree::value_type removed = 0;
      bool did_remove = btr->remove(varkey(key), &removed);
      if (!did_remove) INVARIANT(false);
      INVARIANT(removed == (btree::value_type) tuple);
      dbtuple::release(tuple);
      ++evt_try_delete_unlinks;
    }
  } else {
    ret = tuple->get_next();
  }
  if (ret)
    ++evt_local_cleanup_reschedules;
  return ret;
}

#ifdef TUPLE_QUEUE_TRACKING
static ostream &
operator<<(ostream &o, const dbtuple::op_hist_rec &h)
{
  o << "[enq=" << h.enqueued << ", type=" << h.type << ", tid=" << h.tid << "]";
  return o;
}
#endif

bool
transaction_proto2_static::InitEpochScheme()
{
  g_epoch_loop.start();
  return true;
}

transaction_proto2_static::epoch_loop transaction_proto2_static::g_epoch_loop;

static const uint64_t txn_epoch_us = 10 * 1000; /* 10 ms */
//static const uint64_t txn_epoch_ns = txn_epoch_us * 1000;

void
transaction_proto2_static::epoch_loop::run()
{
  // runs as daemon thread
  struct timespec t;
  NDB_MEMSET(&t, 0, sizeof(t));
  timer loop_timer;
  for (;;) {

    const uint64_t last_loop_usec = loop_timer.lap();
    const uint64_t delay_time_usec = txn_epoch_us;
    if (last_loop_usec < delay_time_usec) {
      t.tv_nsec = (delay_time_usec - last_loop_usec) * 1000;
      nanosleep(&t, NULL);
    }

    // bump epoch number
    // NB(stephentu): no need to do this as an atomic operation because we are
    // the only writer!
    g_current_epoch++;

    // XXX(stephentu): document why we need this memory fence
    __sync_synchronize();

    // wait for each core to finish epoch (g_current_epoch - 1)
    const size_t l_core_count = coreid::core_count();
    for (size_t i = 0; i < l_core_count; i++) {
      lock_guard<spinlock> l(g_epoch_spinlocks[i].elem);
    }

    COMPILER_MEMORY_FENCE;

    // sync point 1: l_current_epoch = (g_current_epoch - 1) is now finished.
    // at this point, all threads will be operating at epoch=g_current_epoch,
    // which means all values < g_current_epoch are consistent with each other
    g_consistent_epoch++;
    __sync_synchronize(); // XXX(stephentu): same reason as above

    // XXX(stephentu): I would really like to avoid having to loop over
    // all the threads again, but I don't know how else to ensure all the
    // threads will finish any oustanding consistent reads at
    // g_consistent_epoch - 1
    for (size_t i = 0; i < l_core_count; i++) {
      lock_guard<spinlock> l(g_epoch_spinlocks[i].elem);
    }

    // sync point 2: all consistent reads will be operating at
    // g_consistent_epoch = g_current_epoch, which means they will be
    // reading changes up to and including (g_current_epoch - 1)
    g_reads_finished_epoch++;

    COMPILER_MEMORY_FENCE; // XXX(stephentu) do we need?
  }
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

void
txn_walker_loop::run()
{
  size_t nodesperrun = 100;
  string s; // the starting key of the scan
  timer loop_timer;
  struct timespec ts;
  NDB_MEMSET(&ts, 0, sizeof(ts));
  typename vec<btree::leaf_node *>::type q;

  while (running) {
    // we don't use btree::tree_walk here because we want
    // to only scan parts of the tree under a single RCU
    // region

    size_t nnodes = 0;
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
    // NB:
    //   s[0, 8 * (q.size() - 1)) contains the key prefix
    //   s[8 * (q.size() - 1), s.size()) contains the key suffix
    bool include_kmin = true;
    while (!q.empty()) {
    descend:

      //cerr << "on descend: " << endl
      //     << "  q.size(): " << q.size() << endl
      //     << "  s: " << hexify(s) << endl
      //     << "  depth: " << depth << endl
      //     ;

      const btree::key_slice kmin =
        host_endian_trfm<btree::key_slice>()(
            *reinterpret_cast<const btree::key_slice *>(
              s.data() + 8 * (q.size() - 1)));

      // resize
      s.resize(8 * (q.size() - 1));

      btree::leaf_node *cur = q.back();
      q.pop_back();

      // now:
      //  s[0, 8 * q.size()) contains key prefix
      INVARIANT(depth == q.size());
      INVARIANT(s.size() == depth * 8);

      while (cur) {
        if (++nnodes == nodesperrun)
          goto recalc;
      process:
        //cerr << "processing node: " << hexify(cur) << endl
        //     << "  prefix: " << hexify(s) << endl
        //     << "  prefix_size: " << s.size() << endl
        //     << "  kmin: " << kmin << endl
        //     << "  include_kmin: " << include_kmin << endl
        //     << "  depth: " << depth << endl
        //     << "  q.size(): " << q.size() << endl
        //     ;

        INVARIANT(depth == q.size());

        const uint64_t version = cur->stable_version();
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

        // process all values
        for (size_t i = 0; i < values.size(); i++) {
          dbtuple * const tuple = reinterpret_cast<dbtuple *>(values[i].value);
          const size_t klen = values[i].len;
          INVARIANT(klen <= 9);
          INVARIANT(tuple);
          if (klen == 9) {
            s.resize(8 * (q.size() + 1) + values[i].suffix.size());
            NDB_MEMCPY((char *) s.data() + 8 * q.size(), values[i].keyslice(), 8);
            NDB_MEMCPY((char *) s.data() + 8 * (q.size() + 1),
                       values[i].suffix.data(), values[i].suffix.size());
          } else {
            s.resize(8 * q.size() + klen);
            NDB_MEMCPY((char *) s.data() + 8 * q.size(), values[i].keyslice(), klen);
          }

          //cerr << "klen: " << klen << endl;
          //cerr << "values[i].key: " << values[i].key << endl;
          //cerr << "s0: " << hexify(s) << endl;
          transaction_proto2_static::try_dbtuple_cleanup(btr, s, tuple);
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

      // finished this layer
      include_kmin = false;
      //cout << "finished layer at depth: " << q.size() << endl;
      depth--;
    }

    //cerr << "full tree scan" << endl;
    s.clear();

  recalc:
    // very simple heuristic
    const uint64_t us = loop_timer.lap();
    const double actual_rate = double(nodesperrun) / double(us); // nodes/usec
    nodesperrun = size_t(actual_rate * double(rcu::EpochTimeUsec));
    // lower and upper bounds
    const size_t nlowerbound = 10;
    const size_t nupperbound = 1000;
    nodesperrun = max(nlowerbound, nodesperrun);
    nodesperrun = min(nupperbound, nodesperrun);
    evt_avg_txn_walker_loop_iter_us.offer(us);

    // sleep
    ts.tv_nsec = rcu::EpochTimeNsec;
    nanosleep(&ts, NULL);
  }
}

__thread unsigned int transaction_proto2_static::tl_nest_level = 0;
__thread uint64_t transaction_proto2_static::tl_last_commit_tid = dbtuple::MIN_TID;

// start epoch at 1, to avoid some boundary conditions
volatile uint64_t transaction_proto2_static::g_current_epoch = 1;
volatile uint64_t transaction_proto2_static::g_consistent_epoch = 1;
volatile uint64_t transaction_proto2_static::g_reads_finished_epoch = 0;

aligned_padded_elem<spinlock> transaction_proto2_static::g_epoch_spinlocks[NMaxCores];

// put at bottom
bool transaction_proto2_static::_init_epoch_scheme_flag = InitEpochScheme();
