#ifndef _NDB_TXN_PROTO2_IMPL_H_
#define _NDB_TXN_PROTO2_IMPL_H_

#include <iostream>

#include "txn.h"
#include "txn_impl.h"
#include "txn_btree.h"
#include "macros.h"

// forward decl
template <typename Traits>
  class transaction_proto2;

// each txn_btree has a walker_loop associated with it
class txn_walker_loop : public ndb_thread {
  friend class transaction_proto2_static;
  friend class txn_btree_handler<transaction_proto2>;
public:
  // not a daemon thread, so we can join when the txn_btree exists
  txn_walker_loop()
    : ndb_thread(false, std::string("walkerloop")),
      running(false),
      btr(0) {}
  virtual void run();
private:
  volatile bool running;
  btree *btr;
};

class transaction_proto2_static {
public:

  // in this protocol, the version number is:
  //
  // [ core  | number |  epoch ]
  // [ 0..10 | 10..27 | 27..64 ]

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
    ALWAYS_ASSERT(!tl_nest_level);
    const uint64_t e = g_consistent_epoch;
    while (g_consistent_epoch == e)
      nop_pause();
    COMPILER_MEMORY_FENCE;
  }

  // XXX(stephentu): need to implement core ID recycling
  static const size_t CoreBits = NMAXCOREBITS; // allow 2^CoreShift distinct threads
  static const size_t NMaxCores = NMAXCORES;

  static const uint64_t CoreMask = (NMaxCores - 1);

  static const uint64_t NumIdShift = CoreBits;
  static const uint64_t NumIdMask = ((((uint64_t)1) << 27) - 1) << NumIdShift;

  static const uint64_t EpochShift = NumIdShift + 27;
  static const uint64_t EpochMask = ((uint64_t)-1) << EpochShift;

  static inline ALWAYS_INLINE
  uint64_t MakeTid(uint64_t core_id, uint64_t num_id, uint64_t epoch_id)
  {
    // some sanity checking
    _static_assert((CoreMask | NumIdMask | EpochMask) == ((uint64_t)-1));
    _static_assert((CoreMask & NumIdMask) == 0);
    _static_assert((NumIdMask & EpochMask) == 0);
    return (core_id) | (num_id << NumIdShift) | (epoch_id << EpochShift);
  }

  // XXX(stephentu): we re-implement another epoch-based scheme- we should
  // reconcile this with the RCU-subsystem, by implementing an epoch based
  // thread manager, which both the RCU GC and this machinery can build on top
  // of

  static bool InitEpochScheme();
  static bool _init_epoch_scheme_flag;

  class epoch_loop : public ndb_thread {
    friend class transaction_proto2_static;
  public:
    epoch_loop()
      : ndb_thread(true, std::string("epochloop")), is_wq_empty(true) {}
    virtual void run();
  private:
    volatile bool is_wq_empty;
  };

  static epoch_loop g_epoch_loop;

  /**
   * Get a (possibly stale) consistent TID
   */
  static uint64_t GetConsistentTid();

  // allows a single core to run multiple transactions at the same time
  // XXX(stephentu): should we allow this? this seems potentially troubling
  static __thread unsigned int tl_nest_level;

  static __thread uint64_t tl_last_commit_tid;

  struct dbtuple_context {
    dbtuple_context() : btr(), key(), ln() {}
    dbtuple_context(btree *btr,
                    const std::string &key,
                    dbtuple *ln)
      : btr(btr), key(key), ln(ln)
    {
      INVARIANT(btr);
      INVARIANT(!key.empty());
      INVARIANT(ln);
    }
    btree *btr;
    std::string key;
    dbtuple *ln;
  };

  static void
  do_dbtuple_chain_cleanup(dbtuple *ln);

  static bool
  try_dbtuple_cleanup(btree *btr, const std::string &key, dbtuple *tuple);

  static inline bool
  try_dbtuple_cleanup(const dbtuple_context &ctx)
  {
    return try_dbtuple_cleanup(ctx.btr, ctx.key, ctx.ln);
  }

protected:

  // XXX(stephentu): think about if the vars below really need to be volatile

  // contains the current epoch number, is either == g_consistent_epoch or
  // == g_consistent_epoch + 1
  static volatile uint64_t g_current_epoch CACHE_ALIGNED;

  // contains the epoch # to take a consistent snapshot at the beginning of
  // (this means g_consistent_epoch - 1 is the last epoch fully completed)
  static volatile uint64_t g_consistent_epoch CACHE_ALIGNED;

  // contains the latest epoch # through which it is known NO readers are in
  // (inclusive). Is either g_consistent_epoch - 1 or g_consistent_epoch - 2
  static volatile uint64_t g_reads_finished_epoch CACHE_ALIGNED;

  // for synchronizing with the epoch incrementor loop
  static util::aligned_padded_elem<spinlock>
    g_epoch_spinlocks[NMaxCores] CACHE_ALIGNED;
};

// protocol 2 - no global consistent TIDs
template <typename Traits = default_transaction_traits>
class transaction_proto2 : public transaction<transaction_proto2, Traits>,
                           private transaction_proto2_static {

  friend class transaction<transaction_proto2, Traits>;
  typedef transaction<transaction_proto2, Traits> super_type;

public:

  typedef Traits traits_type;
  typedef transaction_base::tid_t tid_t;
  typedef transaction_base::string_type string_type;
  typedef typename super_type::dbtuple_info dbtuple_info;
  typedef typename super_type::dbtuple_key_vec dbtuple_key_vec;
  typedef typename super_type::dbtuple_value_vec dbtuple_value_vec;
  typedef typename super_type::read_set_map read_set_map;
  typedef typename super_type::absent_set_map absent_set_map;
  typedef typename super_type::write_set_map write_set_map;
  typedef typename super_type::node_scan_map node_scan_map;
  typedef typename super_type::txn_context txn_context;
  typedef typename super_type::ctx_map_type ctx_map_type;

  transaction_proto2(uint64_t flags = 0)
    : transaction<transaction_proto2, Traits>(flags),
      current_epoch(0),
      last_consistent_tid(0)
  {
    const size_t my_core_id = coreid::core_id();
    VERBOSE(std::cerr << "new transaction_proto2 (core=" << my_core_id
                      << ", nest=" << tl_nest_level << ")" << std::endl);
    if (tl_nest_level++ == 0)
      g_epoch_spinlocks[my_core_id].elem.lock();
    current_epoch = g_current_epoch;
    if (this->get_flags() & transaction_base::TXN_FLAG_READ_ONLY)
      last_consistent_tid = MakeTid(0, 0, g_consistent_epoch);
  }

  ~transaction_proto2()
  {
    const size_t my_core_id = coreid::core_id();
    VERBOSE(std::cerr << "destroy transaction_proto2 (core=" << my_core_id
                      << ", nest=" << tl_nest_level << ")" << std::endl);
    ALWAYS_ASSERT(tl_nest_level > 0);
    if (!--tl_nest_level)
      g_epoch_spinlocks[my_core_id].elem.unlock();
  }

  inline bool
  can_overwrite_record_tid(tid_t prev, tid_t cur) const
  {
    INVARIANT(prev < cur);
    INVARIANT(EpochId(cur) >= g_consistent_epoch);

    // XXX(stephentu): the !prev check is a *bit* of a hack-
    // we're assuming that !prev (MIN_TID) corresponds to an
    // absent (removed) record, so it is safe to overwrite it,
    //
    // This is an OK assumption with *no TID wrap around*.
    return EpochId(prev) == EpochId(cur) || !prev;
  }

  // can only read elements in this epoch or previous epochs
  inline bool
  can_read_tid(tid_t t) const
  {
    return EpochId(t) <= current_epoch;
  }

  inline ALWAYS_INLINE void on_tid_finish(tid_t commit_tid) {}

  inline std::pair<bool, transaction_base::tid_t>
  consistent_snapshot_tid() const
  {
    if (this->get_flags() & transaction_base::TXN_FLAG_READ_ONLY)
      return std::make_pair(true, last_consistent_tid);
    else
      return std::make_pair(false, 0);
  }

  inline transaction_base::tid_t
  null_entry_tid() const
  {
    return MakeTid(0, 0, current_epoch);
  }

  void
  dump_debug_info() const
  {
    transaction<transaction_proto2, Traits>::dump_debug_info();
    std::cerr << "  current_epoch: " << current_epoch << std::endl;
    std::cerr << "  last_consistent_tid: " << g_proto_version_str(last_consistent_tid) << std::endl;
  }

  transaction_base::tid_t
  gen_commit_tid(
      const dbtuple_key_vec &write_node_keys,
      const dbtuple_value_vec &write_node_values)
  {
    const size_t my_core_id = coreid::core_id();
    const tid_t l_last_commit_tid = tl_last_commit_tid;
    INVARIANT(l_last_commit_tid == dbtuple::MIN_TID ||
              CoreId(l_last_commit_tid) == my_core_id);

    // XXX(stephentu): wrap-around messes this up
    INVARIANT(EpochId(l_last_commit_tid) <= current_epoch);

    tid_t ret = l_last_commit_tid;

    // XXX(stephentu): I believe this is correct, but not 100% sure
    //const size_t my_core_id = 0;
    //tid_t ret = 0;
    {
      typename ctx_map_type::const_iterator outer_it = this->ctx_map.begin();
      typename ctx_map_type::const_iterator outer_it_end = this->ctx_map.end();
      for (; outer_it != outer_it_end; ++outer_it) {
        typename read_set_map::const_iterator it = outer_it->second.read_set.begin();
        typename read_set_map::const_iterator it_end = outer_it->second.read_set.end();
        for (; it != it_end; ++it) {
          // NB: we don't allow ourselves to do reads in future epochs
          INVARIANT(EpochId(it->second.t) <= current_epoch);
          if (it->second.t > ret)
            ret = it->second.t;
        }
      }
    }

    {
      typename dbtuple_key_vec::const_iterator it = write_node_keys.begin();
      typename dbtuple_key_vec::const_iterator it_end = write_node_keys.end();
      for (; it != it_end; ++it) {
        const dbtuple_info &info = write_node_values[it->second];
        INVARIANT(it->first->is_locked());
        INVARIANT(!it->first->is_deleting());
        INVARIANT(it->first->is_write_intent());
        INVARIANT(!it->first->is_modifying());
        INVARIANT(it->first->is_latest());
        if (info.insert)
          // we inserted this node, so we don't want to do the checks below
          continue;
        const tid_t t = it->first->version;
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
    return (tl_last_commit_tid = ret);
  }

  inline void
  on_dbtuple_spill(txn_btree<transaction_proto2> *btr, const string_type &key, dbtuple *ln)
  {
    INVARIANT(ln->is_locked());
    INVARIANT(ln->is_write_intent());
    INVARIANT(ln->is_latest());
    INVARIANT(rcu::in_rcu_region());
    //do_dbtuple_chain_cleanup(ln);
  }

  inline ALWAYS_INLINE void
  on_logical_delete(txn_btree<transaction_proto2> *btr, const string_type &key, dbtuple *ln)
  {
  }

private:
  // the global epoch this txn is running in (this # is read when it starts)
  uint64_t current_epoch;
  uint64_t last_consistent_tid;
};

// txn_btree_handler specialization
template <>
struct txn_btree_handler<transaction_proto2> {
  void
  on_construct(btree *btr)
  {
    INVARIANT(!walker_loop.running);
    INVARIANT(!walker_loop.btr);
    walker_loop.btr = btr;
    walker_loop.running = true;
    COMPILER_MEMORY_FENCE;
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
  static inline void
  sync()
  {
    transaction_proto2_static::wait_an_epoch();
  }
  static inline void
  finish()
  {
  }
};

#endif /* _NDB_TXN_PROTO2_IMPL_H_ */
