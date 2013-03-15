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
template <typename Protocol> class txn_btree;

class transaction_unusable_exception {};
class transaction_read_only_exception {};

// XXX: hacky
extern std::string (*g_proto_version_str)(uint64_t v);

// base class with very simple definitions- nothing too exciting yet
class transaction_base : private util::noncopyable {
  template <typename P> friend class txn_btree;
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

  static event_counter g_evt_read_logical_deleted_node_search;
  static event_counter g_evt_read_logical_deleted_node_scan;

  static event_counter evt_local_search_lookups;
  static event_counter evt_local_search_write_set_hits;
  static event_counter evt_local_search_absent_set_hits;
  static event_counter evt_dbtuple_latest_replacement;

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

template <typename Protocol>
class transaction : public transaction_base {
  friend class txn_btree<Protocol>;
  friend Protocol;

protected:
  // data structures

  inline ALWAYS_INLINE Protocol *
  cast()
  {
    return static_cast<Protocol *>(this);
  }

  inline ALWAYS_INLINE const Protocol *
  cast() const
  {
    return static_cast<const Protocol *>(this);
  }

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

  struct dbtuple_info {
    dbtuple_info() {}
    dbtuple_info(txn_btree<Protocol> *btr,
                 const string_type &key,
                 bool locked,
                 const string_type &r)
      : btr(btr),
        key(key),
        locked(locked),
        r(r)
    {}
    txn_btree<Protocol> *btr;
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

protected:
  void abort_impl(abort_reason r);

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
  tid_t gen_commit_tid(const typename util::vec<dbtuple_pair>::type &write_nodes);

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
  typedef small_unordered_map<txn_btree<Protocol> *, txn_context, EXTRA_SMALL_SIZE_MAP> ctx_map_type;
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
template <typename TxnType>
struct txn_epoch_sync {
  // block until the next epoch
  static inline void sync() {}
  // finish any async jobs
  static inline void finish() {}
};

#endif /* _NDB_TXN_H_ */
