#include "txn_proto1_impl.h"

transaction_base::tid_t
transaction_proto1_static::incr_and_get_global_tid()
{
  return __sync_add_and_fetch(&global_tid, 1);
}

volatile transaction_base::tid_t transaction_proto1_static::global_tid = dbtuple::MIN_TID;
volatile transaction_base::tid_t transaction_proto1_static::last_consistent_global_tid = 0;
