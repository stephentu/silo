#include "txn_proto2_impl.h"
#include "counter.h"
#include "util.h"

using namespace std;
using namespace util;

void
transaction_proto2_static::enqueue_work_after_epoch(
    uint64_t epoch, work_callback_t work, void *p)
{
  const size_t id = coreid::core_id();
  // XXX(stephentu): optimize by running work when we know epoch is over
  g_work_queues[id].elem->emplace_back(epoch, work, p);
}

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
transaction_proto2_static::try_dbtuple_cleanup(const dbtuple_context &ctx)
{
  bool ret = false;
  lock_guard<dbtuple> lock(ctx.ln);
  INVARIANT(rcu::in_rcu_region());
  INVARIANT(ctx.ln->is_enqueued());
  INVARIANT(!ctx.ln->is_deleting());

  ctx.ln->set_enqueued(false, dbtuple::QUEUE_TYPE_LOCAL);
  if (!ctx.ln->is_latest())
    // was replaced, so let the newer handlers do the work
    return false;

  do_dbtuple_chain_cleanup(ctx.ln);

  if (!ctx.ln->size) {
    // latest version is a deleted entry, so try to delete
    // from the tree
    const uint64_t v = EpochId(ctx.ln->version);
    if (g_reads_finished_epoch < v || chain_contains_enqueued(ctx.ln)) {
      ret = true;
    } else {
      btree::value_type removed = 0;
      bool did_remove = ctx.btr->remove(varkey(ctx.key), &removed);
      if (!did_remove) INVARIANT(false);
      INVARIANT(removed == (btree::value_type) ctx.ln);
      dbtuple::release(ctx.ln);
      ++evt_try_delete_unlinks;
    }
  } else {
    ret = ctx.ln->get_next();
  }
  if (ret) {
    ctx.ln->set_enqueued(true, dbtuple::QUEUE_TYPE_LOCAL);
    ++evt_local_cleanup_reschedules;
  }
  return ret;
}

bool
transaction_proto2_static::try_chain_cleanup(const dbtuple_context &ctx)
{
  NDB_UNIMPLEMENTED(__PRETTY_FUNCTION__);
  //const uint64_t last_consistent_epoch = g_consistent_epoch;
  //bool ret = false;
  //ctx.ln->lock();
  //INVARIANT(ctx.ln->is_enqueued());
  //INVARIANT(ctx.ln->is_latest());
  //INVARIANT(!ctx.ln->is_deleting());
  //// find the first value n w/ EpochId < last_consistent_epoch.
  //// call gc_chain() on n->next
  //struct dbtuple *p = ctx.ln, **pp = 0;
  //const bool has_chain = ctx.ln->next;
  //bool do_break = false;
  //while (p) {
  //  if (do_break)
  //    break;
  //  // XXX(stephentu): do we need g_reads_finished_epoch instead?
  //  if (EpochId(p->version) < last_consistent_epoch)
  //    do_break = true;
  //  pp = &p->next;
  //  p = p->next;
  //}
  //if (p) {
  //  INVARIANT(p != ctx.ln);
  //  INVARIANT(pp);
  //  *pp = 0;
  //  p->gc_chain();
  //}
  //if (has_chain && !ctx.ln->next) {
  //  ++evt_local_chain_cleanups;
  //}
  //if (ctx.ln->next) {
  //  // keep enqueued so we can clean up at a later time
  //  ret = true;
  //} else {
  //  ctx.ln->set_enqueued(false, dbtuple::QUEUE_TYPE_GC); // we're done
  //}
  //// XXX(stephentu): I can't figure out why doing the following causes all
  //// sorts of race conditions (seems like the same node gets on the delete
  //// list twice)
  ////if (!ctx.ln->size)
  ////  // schedule for deletion
  ////  on_logical_delete_impl(ctx.btr, ctx.key, ctx.ln);
  //ctx.ln->unlock();
  //return ret;
}

#ifdef dbtuple_QUEUE_TRACKING
static ostream &
operator<<(ostream &o, const dbtuple::op_hist_rec &h)
{
  o << "[enq=" << h.enqueued << ", type=" << h.type << ", tid=" << h.tid << "]";
  return o;
}
#endif

bool
transaction_proto2_static::try_delete_dbtuple(const dbtuple_context &info)
{
  NDB_UNIMPLEMENTED(__PRETTY_FUNCTION__);
//  INVARIANT(info.btr);
//  INVARIANT(info.ln);
//  INVARIANT(!info.key.empty());
//  btree::value_type removed = 0;
//  uint64_t v = 0;
//  scoped_rcu_region rcu_region;
//  info.ln->lock();
//#ifdef dbtuple_QUEUE_TRACKING
//  if (!info.ln->is_enqueued()) {
//    cerr << "try_delete_dbtuple: ln=0x" << hexify(intptr_t(info.ln)) << " is NOT enqueued" << endl;
//    cerr << "  last_queue_type: " << info.ln->last_queue_type << endl;
//    cerr << "  op_hist: " << format_list(info.ln->op_hist.begin(), info.ln->op_hist.end()) << endl;
//  }
//#endif
//  INVARIANT(info.ln->is_enqueued());
//  INVARIANT(!info.ln->is_deleting());
//  //cerr << "try_delete_dbtuple: setting ln=0x" << hexify(intptr_t(info.ln)) << " to NOT enqueued" << endl;
//  info.ln->set_enqueued(false, dbtuple::QUEUE_TYPE_DELETE); // we are processing this record
//  if (!info.ln->is_latest() || info.ln->size) {
//    // somebody added a record again, so we don't want to delete it
//    ++evt_try_delete_revivals;
//    goto unlock_and_free;
//  }
//  VERBOSE(cerr << "dbtuple: 0x" << hexify(intptr_t(info.ln)) << " is being unlinked" << endl
//               << "  g_consistent_epoch=" << g_consistent_epoch << endl
//               << "  ln=" << *info.ln << endl);
//  v = EpochId(info.ln->version);
//  if (g_reads_finished_epoch < v) {
//    // need to reschedule to run when epoch=v ends
//    VERBOSE(cerr << "  rerunning at end of epoch=" << v << endl);
//    info.ln->set_enqueued(true, dbtuple::QUEUE_TYPE_DELETE); // re-queue it up
//    info.ln->unlock();
//    // don't free, b/c we need to run again
//    //epoch = v;
//    ++evt_try_delete_reschedules;
//    return true;
//  }
//  ALWAYS_ASSERT(info.btr->underlying_btree.remove(varkey(info.key), &removed));
//  ALWAYS_ASSERT(removed == (btree::value_type) info.ln);
//  dbtuple::release(info.ln);
//  ++evt_try_delete_unlinks;
//unlock_and_free:
//  info.ln->unlock();
//  return false;
}

static event_avg_counter evt_avg_local_cleanup_queue_len("avg_local_cleanup_queue_len");

void
transaction_proto2_static::process_local_cleanup_nodes()
{
  if (unlikely(!tl_cleanup_nodes))
    return;
  INVARIANT(tl_cleanup_nodes_buf);
  INVARIANT(tl_cleanup_nodes_buf->empty());
  evt_avg_local_cleanup_queue_len.offer(tl_cleanup_nodes->size());
  for (node_cleanup_queue::iterator it = tl_cleanup_nodes->begin();
       it != tl_cleanup_nodes->end(); ++it) {
    scoped_rcu_region rcu_region;
    // XXX(stephentu): try-catch block
    if (it->second(it->first))
      // keep around
      tl_cleanup_nodes_buf->emplace_back(move(*it));
  }
  swap(*tl_cleanup_nodes, *tl_cleanup_nodes_buf);
  tl_cleanup_nodes_buf->clear();
}

void
transaction_proto2_static::wait_for_empty_work_queue()
{
  ALWAYS_ASSERT(!tl_nest_level);
  while (!g_epoch_loop.is_wq_empty)
    nop_pause();
}

bool
transaction_proto2_static::InitEpochScheme()
{
  for (size_t i = 0; i < NMaxCores; i++)
    g_work_queues[i].elem = new work_q;
  g_epoch_loop.start();
  return true;
}

transaction_proto2_static::epoch_loop transaction_proto2_static::g_epoch_loop;

static const uint64_t txn_epoch_us = 10 * 1000; /* 10 ms */
//static const uint64_t txn_epoch_ns = txn_epoch_us * 1000;

static event_avg_counter evt_avg_epoch_thread_queue_len("avg_epoch_thread_queue_len");
static event_avg_counter evt_avg_epoch_work_queue_len("avg_epoch_work_queue_len");

void
transaction_proto2_static::epoch_loop::run()
{
  // runs as daemon thread
  struct timespec t;
  NDB_MEMSET(&t, 0, sizeof(t));
  work_pq pq;
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
    const uint64_t l_current_epoch = g_current_epoch++;

    // XXX(stephentu): document why we need this memory fence
    __sync_synchronize();

    //static uint64_t pq_size_prev = 0;
    //static timer pq_timer;
    //static int _x = 0;
    //const uint64_t pq_size_cur = pq.size();
    //const double pq_loop_time = pq_timer.lap() / 1000000.0; // sec
    //if (((++_x) % 10) == 0) {
    //  const double pq_growth_rate = double(pq_size_cur - pq_size_prev)/pq_loop_time;
    //  cerr << "pq_growth_rate: " << pq_growth_rate << " elems/sec" << endl;
    //  cerr << "pq_current_size: " << pq.size() << " elems" << endl;
    //  cerr << "last_loop_time: " << double(last_loop_usec)/1000.0 << endl;
    //}
    //pq_size_prev = pq_size_cur;

    // wait for each core to finish epoch (g_current_epoch - 1)
    const size_t l_core_count = coreid::core_count();
    for (size_t i = 0; i < l_core_count; i++) {
      lock_guard<spinlock> l(g_epoch_spinlocks[i].elem);
      work_q &core_q = *g_work_queues[i].elem;
      for (work_q::iterator it = core_q.begin(); it != core_q.end(); ++it) {
        if (pq.empty())
          is_wq_empty = false;
        pq.push(*it);
      }
      evt_avg_epoch_thread_queue_len.offer(core_q.size());
      core_q.clear();
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

    VERBOSE(cerr << "epoch_loop: running work <= (l_current_epoch=" << l_current_epoch << ")" << endl);
    evt_avg_epoch_work_queue_len.offer(pq.size());
    while (!pq.empty()) {
      const work_record_t &work = pq.top();
      if (work.epoch > l_current_epoch)
        break;
      try {
        uint64_t e = 0;
        bool resched = work.work(work.p, e);
        if (resched)
          pq.push(work_record_t(e, work.work, work.p));
      } catch (...) {
        cerr << "epoch_loop: uncaught exception from enqueued work fn" << endl;
      }
      pq.pop();
    }

    if (pq.empty())
      is_wq_empty = true;

    COMPILER_MEMORY_FENCE; // XXX(stephentu) do we need?
  }
}

void
transaction_proto2_static::purge_local_work_queue()
{
  if (!tl_cleanup_nodes)
    return;
  // lock and dequeue all the nodes
  // XXX(stephentu): maybe we should run another iteration of
  // process_local_cleanup_nodes()
  INVARIANT(tl_cleanup_nodes_buf);
  for (node_cleanup_queue::iterator it = tl_cleanup_nodes->begin();
       it != tl_cleanup_nodes->end(); ++it) {
    it->first.ln->lock();
    ALWAYS_ASSERT(it->first.ln->is_enqueued());
    it->first.ln->set_enqueued(false, dbtuple::QUEUE_TYPE_GC);
    it->first.ln->unlock();
  }
  tl_cleanup_nodes->clear();
  delete tl_cleanup_nodes;
  delete tl_cleanup_nodes_buf;
  tl_cleanup_nodes = tl_cleanup_nodes_buf = NULL;
}

void
transaction_proto2_static::completion_callback(ndb_thread *p)
{
  purge_local_work_queue();
}
NDB_THREAD_REGISTER_COMPLETION_CALLBACK(transaction_proto2_static::completion_callback)

__thread unsigned int transaction_proto2_static::tl_nest_level = 0;
__thread uint64_t transaction_proto2_static::tl_last_commit_tid = dbtuple::MIN_TID;
__thread uint64_t transaction_proto2_static::tl_last_cleanup_epoch = dbtuple::MIN_TID;
__thread transaction_proto2_static::node_cleanup_queue *transaction_proto2_static::tl_cleanup_nodes = NULL;
__thread transaction_proto2_static::node_cleanup_queue *transaction_proto2_static::tl_cleanup_nodes_buf = NULL;

// start epoch at 1, to avoid some boundary conditions
volatile uint64_t transaction_proto2_static::g_current_epoch = 1;
volatile uint64_t transaction_proto2_static::g_consistent_epoch = 1;
volatile uint64_t transaction_proto2_static::g_reads_finished_epoch = 0;

aligned_padded_elem<spinlock> transaction_proto2_static::g_epoch_spinlocks[NMaxCores];
volatile aligned_padded_elem<transaction_proto2_static::work_q*> transaction_proto2_static::g_work_queues[NMaxCores];

// put at bottom
bool transaction_proto2_static::_init_epoch_scheme_flag = InitEpochScheme();
