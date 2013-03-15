#ifndef _NDB_TXN_H_
#define _NDB_TXN_H_

#include <malloc.h>
#include <stdint.h>
#include <sys/types.h>
#include <pthread.h>

#include <map>
#include <iostream>
#include <vector>
#include <string>
#include <utility>
#include <stdexcept>
#include <limits>

#include <unordered_map>

#include "amd64.h"
#include "btree.h"
#include "core.h"
#include "counter.h"
#include "macros.h"
#include "varkey.h"
#include "util.h"
#include "static_assert.h"
#include "rcu.h"
#include "thread.h"
#include "spinlock.h"
#include "small_unordered_map.h"
#include "prefetch.h"
#include "keyrange.h"
#include "tuple.h"

// forward decl
class txn_btree;

class transaction_unusable_exception {};
class transaction_read_only_exception {};

// XXX: hacky
extern std::string (*g_proto_version_str)(uint64_t v);

// base class with very simple definitions- nothing too exciting yet
class transaction_base : private util::noncopyable {
  friend class txn_btree;
public:

  typedef dbtuple::tid_t tid_t;
  typedef dbtuple::record_type record_type;
  typedef dbtuple::const_record_type const_record_type;
  typedef dbtuple::size_type size_type;
  typedef btree::key_type key_type;
  typedef dbtuple::string_type string_type;

  // TXN_EMBRYO - the transaction object has been allocated but has not
  // done any operations yet
  enum txn_state { TXN_EMBRYO, TXN_ACTIVE, TXN_COMMITED, TXN_ABRT, };

  enum {
    // use the low-level scan protocol for checking scan consistency,
    // instead of keeping track of absent ranges
    TXN_FLAG_LOW_LEVEL_SCAN = 0x1,

    // true to mark a read-only transaction- if a txn marked read-only
    // does a write, a transaction_read_only_exception is thrown and the
    // txn is aborted
    TXN_FLAG_READ_ONLY = 0x2,

    // XXX: more flags in the future, things like consistency levels
  };

#define ABORT_REASONS(x) \
    x(ABORT_REASON_USER) \
    x(ABORT_REASON_UNSTABLE_READ) \
    x(ABORT_REASON_FUTURE_TID_READ) \
    x(ABORT_REASON_NODE_SCAN_WRITE_VERSION_CHANGED) \
    x(ABORT_REASON_NODE_SCAN_READ_VERSION_CHANGED) \
    x(ABORT_REASON_WRITE_NODE_INTERFERENCE) \
    x(ABORT_REASON_READ_NODE_INTEREFERENCE) \
    x(ABORT_REASON_READ_ABSENCE_INTEREFERENCE) \

  enum abort_reason {
#define ENUM_X(x) x,
    ABORT_REASONS(ENUM_X)
#undef ENUM_X
  };

  static const char *
  AbortReasonStr(abort_reason reason)
  {
    switch (reason) {
#define CASE_X(x) case x: return #x;
    ABORT_REASONS(CASE_X)
#undef CASE_X
    default:
      break;
    }
    ALWAYS_ASSERT(false);
    return 0;
  }

  inline transaction_base(uint64_t flags)
    : state(TXN_EMBRYO), flags(flags) {}

protected:
#define EVENT_COUNTER_DEF_X(x) \
  static event_counter g_ ## x ## _ctr;
  ABORT_REASONS(EVENT_COUNTER_DEF_X)
#undef EVENT_COUNTER_DEF_X

  static event_counter *
  AbortReasonCounter(abort_reason reason)
  {
    switch (reason) {
#define EVENT_COUNTER_CASE_X(x) case x: return &g_ ## x ## _ctr;
    ABORT_REASONS(EVENT_COUNTER_CASE_X)
#undef EVENT_COUNTER_CASE_X
    default:
      break;
    }
    ALWAYS_ASSERT(false);
    return 0;
  }

public:

  /**
   * throws transaction_unusable_exception if already resolved (commited/aborted)
   */
  inline void
  ensure_active()
  {
    if (state == TXN_EMBRYO)
      state = TXN_ACTIVE;
    else if (unlikely(state != TXN_ACTIVE))
      throw transaction_unusable_exception();
  }

  inline uint64_t
  get_flags() const
  {
    return flags;
  }

  //static void Test();

protected:

  struct read_record_t {
    read_record_t() : t(0), holds_lock(false) {}
    tid_t t;
    bool holds_lock;
  };

  friend std::ostream &
  operator<<(std::ostream &o, const read_record_t &r);

  struct dbtuple_info {
    dbtuple_info() {}
    dbtuple_info(txn_btree *btr,
                 const string_type &key,
                 bool locked,
                 const string_type &r)
      : btr(btr),
        key(key),
        locked(locked),
        r(r)
    {}
    txn_btree *btr;
    string_type key;
    bool locked;
    string_type r;
  };
  typedef std::pair<dbtuple *, dbtuple_info> dbtuple_pair;

  struct LNodeComp {
    inline ALWAYS_INLINE bool
    operator()(const dbtuple_pair &lhs, const dbtuple_pair &rhs) const
    {
      return lhs.first < rhs.first;
    }
  };

  static event_counter g_evt_read_logical_deleted_node_search;
  static event_counter g_evt_read_logical_deleted_node_scan;

  txn_state state;
  abort_reason reason;
  const uint64_t flags;
};

class transaction : public transaction_base {
  friend class txn_btree;
public:

  // declared here so dbtuple::write_record_at() can see it.
  virtual bool
  can_overwrite_record_tid(tid_t prev, tid_t cur) const
  {
    return false;
  }

  transaction(uint64_t flags);
  virtual ~transaction();

  // returns TRUE on successful commit, FALSE on abort
  // if doThrow, signals success by returning true, and
  // failure by throwing an abort exception
  bool commit(bool doThrow = false);

  // abort() always succeeds
  inline void
  abort()
  {
    abort_impl(ABORT_REASON_USER);
  }

  virtual void dump_debug_info() const;

#ifdef DIE_ON_ABORT
  void
  abort_trap(abort_reason reason)
  {
    AbortReasonCounter(reason)->inc();
    this->reason = reason; // for dump_debug_info() to see
    dump_debug_info();
    ::abort();
  }
#else
  inline ALWAYS_INLINE void
  abort_trap(abort_reason reason)
  {
    AbortReasonCounter(reason)->inc();
  }
#endif

  /**
   * XXX: document
   */
  virtual std::pair<bool, tid_t> consistent_snapshot_tid() const = 0;

  virtual tid_t null_entry_tid() const = 0;

protected:

  void abort_impl(abort_reason r);

  /**
   * create a new, unique TID for a txn. at the point which gen_commit_tid(),
   * it still has not been decided whether or not this txn will commit
   * successfully
   */
  virtual tid_t gen_commit_tid(
      const typename util::vec<dbtuple_pair>::type &write_nodes) = 0;

  virtual bool can_read_tid(tid_t t) const { return true; }

  // For GC handlers- note that on_dbtuple_spill() is called
  // with the lock on ln held, to simplify GC code
  //
  // Is also called within an RCU read region
  virtual void on_dbtuple_spill(
      txn_btree *btr, const string_type &key, dbtuple *ln) = 0;

  // Called when the latest value written to ln is an empty
  // (delete) marker. The protocol can then decide how to schedule
  // the logical node for actual deletion
  virtual void on_logical_delete(
      txn_btree *btr, const string_type &key, dbtuple *ln) = 0;

  // if gen_commit_tid() is called, then on_tid_finish() will be called
  // with the commit tid. before on_tid_finish() is called, state is updated
  // with the resolution (commited, aborted) of this txn
  virtual void on_tid_finish(tid_t commit_tid) = 0;

#ifdef USE_SMALL_CONTAINER_OPT
  // XXX(stephentu): these numbers are somewhat tuned for TPC-C
  typedef small_unordered_map<const dbtuple *, read_record_t> read_set_map;
  typedef small_unordered_map<string_type, bool, EXTRA_SMALL_SIZE_MAP> absent_set_map;
  typedef small_unordered_map<string_type, string_type> write_set_map;
  typedef std::vector<key_range_t> absent_range_vec; // only for un-optimized scans
  typedef small_unordered_map<const btree::node_opaque_t *, uint64_t, EXTRA_SMALL_SIZE_MAP> node_scan_map;
#else
  typedef std::unordered_map<const dbtuple *, read_record_t> read_set_map;
  typedef std::unordered_map<string_type, bool> absent_set_map;
  typedef std::unordered_map<string_type, string_type> write_set_map;
  typedef std::vector<key_range_t> absent_range_vec; // only for un-optimized scans
  typedef std::unordered_map<const btree::node_opaque_t *, uint64_t> node_scan_map;
#endif

  struct txn_context {
    read_set_map read_set;
    absent_set_map absent_set;
    write_set_map write_set;
    absent_range_vec absent_range_set; // ranges do not overlap
    node_scan_map node_scan; // we scanned these nodes at verison v

    bool local_search_str(const transaction &t, const string_type &k, string_type &v) const;

    inline bool
    local_search(const transaction &t, const key_type &k, string_type &v) const
    {
      // XXX: we have to make an un-necessary copy of the key each time we search
      // the write/read set- we need to find a way to avoid this
      return local_search_str(t, k.str(), v);
    }

    bool key_in_absent_set(const key_type &k) const;

    void add_absent_range(const key_range_t &range);
  };

  void clear();

#ifdef USE_SMALL_CONTAINER_OPT
  typedef small_unordered_map<txn_btree *, txn_context, EXTRA_SMALL_SIZE_MAP> ctx_map_type;
#else
  typedef std::unordered_map<txn_btree *, txn_context> ctx_map_type;
#endif
  ctx_map_type ctx_map;
};

class transaction_abort_exception : public std::exception {
public:
  transaction_abort_exception(transaction_base::abort_reason r)
    : r(r) {}
  inline transaction_base::abort_reason
  get_reason() const
  {
    return r;
  }
  virtual const char *
  what() const throw()
  {
    return transaction_base::AbortReasonStr(r);
  }
private:
  transaction_base::abort_reason r;
};

// protocol 1 - global consistent TIDs
class transaction_proto1 : public transaction {
public:
  transaction_proto1(uint64_t flags = 0);
  virtual std::pair<bool, tid_t> consistent_snapshot_tid() const;
  virtual tid_t null_entry_tid() const;
  virtual void dump_debug_info() const;

protected:
  static const size_t NMaxChainLength = 10; // XXX(stephentu): tune me?

  virtual tid_t gen_commit_tid(
      const typename util::vec<dbtuple_pair>::type &write_nodes);
  virtual void on_dbtuple_spill(
      txn_btree *btr, const string_type &key, dbtuple *ln);
  virtual void on_logical_delete(
      txn_btree *btr, const string_type &key, dbtuple *ln);
  virtual void on_tid_finish(tid_t commit_tid);

private:
  static tid_t incr_and_get_global_tid();

  const tid_t snapshot_tid;

  volatile static tid_t global_tid CACHE_ALIGNED;
  volatile static tid_t last_consistent_global_tid CACHE_ALIGNED;
};

// protocol 2 - no global consistent TIDs
class transaction_proto2 : public transaction {

  // in this protocol, the version number is:
  //
  // [ core  | number |  epoch ]
  // [ 0..10 | 10..27 | 27..64 ]

public:
  transaction_proto2(uint64_t flags = 0);
  ~transaction_proto2();

  virtual std::pair<bool, tid_t> consistent_snapshot_tid() const;

  virtual tid_t null_entry_tid() const;

  virtual void dump_debug_info() const;

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

  // XXX(stephentu): HACK
  static void wait_for_empty_work_queue();

  virtual bool
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

protected:
  virtual tid_t gen_commit_tid(
      const typename util::vec<dbtuple_pair>::type &write_nodes);

  // can only read elements in this epoch or previous epochs
  virtual bool
  can_read_tid(tid_t t) const
  {
    return EpochId(t) <= current_epoch;
  }

  virtual void on_dbtuple_spill(
      txn_btree *btr, const string_type &key, dbtuple *ln);
  virtual void on_logical_delete(
      txn_btree *btr, const string_type &key, dbtuple *ln);
  virtual void on_tid_finish(tid_t commit_tid) {}

private:
  void on_logical_delete_impl(
      txn_btree *btr, const string_type &key, dbtuple *ln);

  // the global epoch this txn is running in (this # is read when it starts)
  uint64_t current_epoch;
  uint64_t last_consistent_tid;

  static size_t core_id();

  /**
   * If true is returned, reschedule the job to run after epoch.
   * Otherwise, the task is finished
   */
  typedef bool (*work_callback_t)(void *, uint64_t &epoch);

  // work will get called after epoch finishes
  static void enqueue_work_after_epoch(uint64_t epoch, work_callback_t work, void *p);

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
    friend class transaction_proto2;
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

  static __thread uint64_t tl_last_cleanup_epoch;

  // is cleaned-up by an NDB_THREAD_REGISTER_COMPLETION_CALLBACK
  struct dbtuple_context {
    dbtuple_context() : btr(), key(), ln() {}
    dbtuple_context(txn_btree *btr,
                         const string_type &key,
                         dbtuple *ln)
      : btr(btr), key(key), ln(ln)
    {
      INVARIANT(btr);
      INVARIANT(!key.empty());
      INVARIANT(ln);
    }
    txn_btree *btr;
    string_type key;
    dbtuple *ln;
  };

  // returns true if should run again
  // note that work will run under a RCU region
  typedef bool (*local_work_callback_t)(const dbtuple_context &);
  typedef std::pair<dbtuple_context, local_work_callback_t> local_work_t;
  typedef std::vector<local_work_t> node_cleanup_queue;
  //typedef std::list<local_work_t> node_cleanup_queue;

  static const size_t TlNodeQueueBufSize = 16384;

  static __thread node_cleanup_queue *tl_cleanup_nodes;
  static __thread node_cleanup_queue *tl_cleanup_nodes_buf;
  static inline node_cleanup_queue &
  local_cleanup_nodes()
  {
    if (unlikely(!tl_cleanup_nodes)) {
      INVARIANT(!tl_cleanup_nodes_buf);
      tl_cleanup_nodes = new node_cleanup_queue;
      tl_cleanup_nodes_buf = new node_cleanup_queue;
      tl_cleanup_nodes->reserve(TlNodeQueueBufSize);
      tl_cleanup_nodes_buf->reserve(TlNodeQueueBufSize);
    }
    return *tl_cleanup_nodes;
  }

  static void
  do_dbtuple_chain_cleanup(dbtuple *ln);

  static bool
  try_dbtuple_cleanup(const dbtuple_context &ctx);

  static bool
  try_chain_cleanup(const dbtuple_context &ctx);

  static bool
  try_delete_dbtuple(const dbtuple_context &info);

  static void
  process_local_cleanup_nodes();

public:
  static void purge_local_work_queue();

  // public so we can register w/ NDB_THREAD_REGISTER_COMPLETION_CALLBACK
  static void completion_callback(ndb_thread *);

private:

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

  struct work_record_t {
    work_record_t() {}
    work_record_t(uint64_t epoch, work_callback_t work, void *p)
      : epoch(epoch), work(work), p(p)
    {}
    // for work_pq
    inline bool
    operator>(const work_record_t &that) const
    {
      return epoch > that.epoch;
    }
    uint64_t epoch;
    work_callback_t work;
    void *p;
  };

  typedef std::list<work_record_t> work_q;
  typedef util::std_reverse_pq<work_record_t>::type work_pq;

  // for passing work to the epoch loop
  static volatile util::aligned_padded_elem<work_q*>
    g_work_queues[NMaxCores] CACHE_ALIGNED;
};

// XXX(stephentu): stupid hacks
template <typename TxnType>
struct txn_epoch_sync {
  // block until the next epoch
  static inline void sync() {}
  // finish any async jobs
  static inline void finish() {}
};

template <>
struct txn_epoch_sync<transaction_proto2> {
  static inline void
  sync()
  {
    transaction_proto2::wait_an_epoch();
  }
  static inline void
  finish()
  {
    transaction_proto2::purge_local_work_queue();
    transaction_proto2::wait_for_empty_work_queue();
  }
};

#endif /* _NDB_TXN_H_ */
