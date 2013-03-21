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
#include "scopedperf.hh"

// forward decl
template <template <typename> class Transaction> class txn_btree;

class transaction_unusable_exception {};
class transaction_read_only_exception {};

// XXX: hacky
extern std::string (*g_proto_version_str)(uint64_t v);

// base class with very simple definitions- nothing too exciting yet
class transaction_base : private util::noncopyable {
  template <template <typename> class T> friend class txn_btree;
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

  struct write_record_t {
    write_record_t() {}
    write_record_t(const string_type &r, dbtuple *tuple)
      : r(r), tuple(tuple) {}
    string_type r;
    dbtuple *tuple;
  };

  friend std::ostream &
  operator<<(std::ostream &o, const write_record_t &r);

  enum AbsentRecordType {
    ABSENT_REC_READ, // no lock held
    ABSENT_REC_WRITE, // lock held, but did not do insert
    ABSENT_REC_INSERT, // lock held + insert
  };

  struct absent_record_t {
    absent_record_t() : type(ABSENT_REC_READ), tuple(nullptr) {}
    AbsentRecordType type;
    const dbtuple *tuple; // only set if type != ABSENT_REC_READ
  };

  friend std::ostream &
  operator<<(std::ostream &o, const absent_record_t &r);

  static event_counter g_evt_read_logical_deleted_node_search;
  static event_counter g_evt_read_logical_deleted_node_scan;
  static event_counter g_evt_dbtuple_write_search_failed;
  static event_counter g_evt_dbtuple_write_insert_failed;

  static event_counter evt_local_search_lookups;
  static event_counter evt_local_search_write_set_hits;
  static event_counter evt_local_search_absent_set_hits;
  static event_counter evt_dbtuple_latest_replacement;

  CLASS_STATIC_COUNTER_DECL(scopedperf::tsc_ctr, g_txn_commit_probe0, g_txn_commit_probe0_cg);
  CLASS_STATIC_COUNTER_DECL(scopedperf::tsc_ctr, g_txn_commit_probe1, g_txn_commit_probe1_cg);
  CLASS_STATIC_COUNTER_DECL(scopedperf::tsc_ctr, g_txn_commit_probe2, g_txn_commit_probe2_cg);
  CLASS_STATIC_COUNTER_DECL(scopedperf::tsc_ctr, g_txn_commit_probe3, g_txn_commit_probe3_cg);
  CLASS_STATIC_COUNTER_DECL(scopedperf::tsc_ctr, g_txn_commit_probe4, g_txn_commit_probe4_cg);
  CLASS_STATIC_COUNTER_DECL(scopedperf::tsc_ctr, g_txn_commit_probe5, g_txn_commit_probe5_cg);
  CLASS_STATIC_COUNTER_DECL(scopedperf::tsc_ctr, g_txn_commit_probe6, g_txn_commit_probe6_cg);

  txn_state state;
  abort_reason reason;
  const uint64_t flags;
};

inline ALWAYS_INLINE std::ostream &
operator<<(std::ostream &o, const transaction_base::read_record_t &r)
{
  o << "[tid_read=" << g_proto_version_str(r.t)
    << ", locked=" << r.holds_lock << "]";
  return o;
}

inline ALWAYS_INLINE std::ostream &
operator<<(std::ostream &o, const transaction_base::write_record_t &r)
{
  o << "[r=" << r.r << ", tuple=" << util::hexify(r.tuple) << "]";
  return o;
}

inline ALWAYS_INLINE std::ostream &
operator<<(std::ostream &o, const transaction_base::absent_record_t &r)
{
  o << "[type=" << r.type << ", tuple=" << util::hexify(r.tuple) << "]";
  return o;
}

struct default_transaction_traits {
  static const size_t read_set_expected_size = SMALL_SIZE_MAP;
  static const size_t absent_set_expected_size = EXTRA_SMALL_SIZE_MAP;
  static const size_t write_set_expected_size = SMALL_SIZE_MAP;
  static const size_t node_scan_expected_size = EXTRA_SMALL_SIZE_MAP;
  static const size_t context_set_expected_size = EXTRA_SMALL_SIZE_MAP;
};

template <template <typename> class Protocol, typename Traits>
class transaction : public transaction_base {
  friend class txn_btree<Protocol>;
  friend Protocol<Traits>;

  typedef Traits traits_type;

protected:
  // data structures

  inline ALWAYS_INLINE Protocol<Traits> *
  cast()
  {
    return static_cast<Protocol<Traits> *>(this);
  }

  inline ALWAYS_INLINE const Protocol<Traits> *
  cast() const
  {
    return static_cast<const Protocol<Traits> *>(this);
  }

#ifdef USE_SMALL_CONTAINER_OPT
  typedef small_unordered_map<const dbtuple *, read_record_t, traits_type::read_set_expected_size> read_set_map;
  typedef small_unordered_map<string_type, absent_record_t, traits_type::absent_set_expected_size> absent_set_map;
  typedef small_unordered_map<string_type, write_record_t, traits_type::write_set_expected_size> write_set_map;
  typedef std::vector<key_range_t> absent_range_vec; // only for un-optimized scans
  typedef small_unordered_map<const btree::node_opaque_t *, uint64_t, traits_type::node_scan_expected_size> node_scan_map;
#else
  typedef std::unordered_map<const dbtuple *, read_record_t> read_set_map;
  typedef std::unordered_map<string_type, absent_record_t> absent_set_map;
  typedef std::unordered_map<string_type, write_record_t> write_set_map;
  typedef std::vector<key_range_t> absent_range_vec; // only for un-optimized scans
  typedef std::unordered_map<const btree::node_opaque_t *, uint64_t> node_scan_map;
#endif

  struct dbtuple_info {
    dbtuple_info() {}
    dbtuple_info(txn_btree<Protocol> *btr,
                 const string_type &key,
                 bool locked,
                 const string_type &r,
                 bool insert)
      : btr(btr),
        key(key),
        locked(locked),
        r(r),
        insert(insert)
    {}
    txn_btree<Protocol> *btr;
    string_type key;
    bool locked;
    string_type r;
    bool insert;
  };

  // NB: maintain two separate vectors, so we can sort faster (w/o swapping
  // elems). logically, we want a map from dbtuple -> dbtuple_info, but this
  // method is faster
  typedef std::pair<dbtuple *, unsigned int> dbtuple_mapping;
  typedef typename util::vec<dbtuple_mapping, 512>::type dbtuple_key_vec;
  typedef typename util::vec<dbtuple_info, 512>::type    dbtuple_value_vec;

  struct TupleMapComp {
    inline ALWAYS_INLINE bool
    operator()(const dbtuple_mapping &lhs, const dbtuple_mapping &rhs) const
    {
      return lhs.first < rhs.first;
    }
  };

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

public:

  inline transaction(uint64_t flags);
  inline ~transaction();

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

  void dump_debug_info() const;

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

  std::map<std::string, uint64_t> get_txn_counters() const;

protected:
  void abort_impl(abort_reason r);

  // precondition: tuple != nullptr
  void mark_write_tuple(
      txn_context &ctx, const std::string &key,
      dbtuple *tuple, bool did_insert);

  // low-level API for txn_btree

  // try to insert a new "tentative" tuple into the underlying
  // btree associated with the given context.
  //
  // if return.first is not null, then this function will mutate
  // the txn_context such that the node_scan flag is aware of any
  // mutating changes made to the underlying btree. if return.second
  // is true, then this txn should abort, because a conflict was
  // detected w/ the node scan set.
  //
  // if return.first is not null, the returned tuple is locked()!
  // it is the responsibility of the caller to release the lock
  //
  // if the return value is null, then this function has no side effects.
  //
  // NOTE: !ret.first => !ret.second
  std::pair< dbtuple *, bool >
  try_insert_new_tuple(
      btree &btr,
      txn_context &ctx,
      const std::string &key,
      const std::string &value);

public:
  // expected public overrides
  bool can_overwrite_record_tid(tid_t prev, tid_t cur) const;

  /**
   * XXX: document
   */
  std::pair<bool, tid_t> consistent_snapshot_tid() const;

  tid_t null_entry_tid() const;

protected:
  // expected protected overrides

  /**
   * create a new, unique TID for a txn. at the point which gen_commit_tid(),
   * it still has not been decided whether or not this txn will commit
   * successfully
   */
  tid_t gen_commit_tid(
      const dbtuple_key_vec &write_node_keys,
      const dbtuple_value_vec &write_node_values);

  bool can_read_tid(tid_t t) const;

  // For GC handlers- note that on_dbtuple_spill() is called
  // with the lock on ln held, to simplify GC code
  //
  // Is also called within an RCU read region
  void on_dbtuple_spill(
      txn_btree<Protocol> *btr, const string_type &key, dbtuple *ln);

  // Called when the latest value written to ln is an empty
  // (delete) marker. The protocol can then decide how to schedule
  // the logical node for actual deletion
  void on_logical_delete(
      txn_btree<Protocol> *btr, const string_type &key, dbtuple *ln);

  // if gen_commit_tid() is called, then on_tid_finish() will be called
  // with the commit tid. before on_tid_finish() is called, state is updated
  // with the resolution (commited, aborted) of this txn
  void on_tid_finish(tid_t commit_tid);

protected:
  inline void clear();

#ifdef USE_SMALL_CONTAINER_OPT
  typedef small_unordered_map<txn_btree<Protocol> *, txn_context, traits_type::context_set_expected_size> ctx_map_type;
#else
  typedef std::unordered_map<txn_btree<Protocol> *, txn_context> ctx_map_type;
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

// XXX(stephentu): stupid hacks
template <template <typename> class Transaction>
struct txn_epoch_sync {
  // block until the next epoch
  static inline void sync() {}
  // finish any async jobs
  static inline void finish() {}
};

#endif /* _NDB_TXN_H_ */
