#ifndef _NDB_TXN_PROTO1_IMPL_H_
#define _NDB_TXN_PROTO1_IMPL_H_

#include <iostream>

#include "txn.h"
#include "txn_impl.h"
#include "macros.h"

// protocol 1 - global consistent TIDs
class transaction_proto1_static {
  template <typename>
    friend class transaction_proto1;
public:
  static const size_t NMaxChainLength = 10; // XXX(stephentu): tune me?
private:
  static transaction_base::tid_t incr_and_get_global_tid();
  volatile static transaction_base::tid_t global_tid CACHE_ALIGNED;
  volatile static transaction_base::tid_t last_consistent_global_tid CACHE_ALIGNED;
};

// XXX(stephentu): proto1 is unmaintained for now, will
// need to fix later
template <typename Traits>
class transaction_proto1 : public transaction<transaction_proto1, Traits>,
                           private transaction_proto1_static {
  friend class transaction<transaction_proto1, Traits>;
  typedef transaction<transaction_proto1, Traits> super_type;

public:
  typedef Traits traits_type;
  typedef transaction_base::tid_t tid_t;
  typedef transaction_base::string_type string_type;
  typedef typename super_type::dbtuple_write_info dbtuple_write_info;
  typedef typename super_type::dbtuple_write_info_vec dbtuple_write_info_vec;
  typedef typename super_type::read_set_map read_set_map;
  typedef typename super_type::absent_set_map absent_set_map;
  typedef typename super_type::write_set_map write_set_map;

  transaction_proto1(uint64_t flags,
                     typename Traits::StringAllocator &sa)
    : transaction<transaction_proto1, Traits>(flags, sa),
      snapshot_tid(last_consistent_global_tid)
  {
  }

  inline ALWAYS_INLINE bool
  can_overwrite_record_tid(tid_t prev, tid_t cur) const
  {
    return false;
  }

  inline ALWAYS_INLINE bool
  can_read_tid(tid_t t) const
  {
    return true;
  }

  inline std::pair<bool, tid_t>
  consistent_snapshot_tid() const
  {
    return std::make_pair(true, snapshot_tid);
  }

  void
  dump_debug_info() const
  {
    transaction<transaction_proto1, Traits>::dump_debug_info();
    std::cerr << "  snapshot_tid: " << snapshot_tid << std::endl;
    std::cerr << "  global_tid: " << global_tid << std::endl;
  }

protected:

  tid_t gen_commit_tid(const dbtuple_write_info_vec &write_tuples)
  {
    return incr_and_get_global_tid();
  }

  void on_dbtuple_spill(dbtuple *tuple)
  {
    NDB_UNIMPLEMENTED(__PRETTY_FUNCTION__);
  }

  void on_logical_delete(dbtuple *tuple)
  {
    NDB_UNIMPLEMENTED(__PRETTY_FUNCTION__);
  }

  void on_tid_finish(tid_t commit_tid)
  {
    INVARIANT(this->state == transaction_base::TXN_COMMITED ||
              this->state == transaction_base::TXN_ABRT);
    // XXX(stephentu): handle wrap around
    INVARIANT(commit_tid > last_consistent_global_tid);
    while (!__sync_bool_compare_and_swap(
          &last_consistent_global_tid, commit_tid - 1, commit_tid))
      nop_pause();
  }

private:
  uint64_t snapshot_tid;
};

#endif /* _NDB_TXN_PROTO1_IMPL_H_ */
