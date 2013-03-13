#ifndef _ABSTRACT_DB_H_
#define _ABSTRACT_DB_H_

#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

#include <string>

#include "abstract_ordered_index.h"

/**
 * Abstract interface for a DB. This is to facilitate writing
 * benchmarks for different systems, making each system present
 * a unified interface
 */
class abstract_db {
public:

  /**
   * both get() and put() can throw abstract_abort_exception. If thrown,
   * abort_txn() must be called (calling commit_txn() will result in undefined
   * behavior).  Also if thrown, subsequently calling get()/put() will also
   * result in undefined behavior)
   */
  class abstract_abort_exception {};

  // ctor should open db
  abstract_db() {}

  // dtor should close db
  virtual ~abstract_db() {}

  /**
   * an approximate max batch size for updates in a transaction.
   *
   * A return value of -1 indicates no maximum
   */
  virtual ssize_t txn_max_batch_size() const { return -1; }

  virtual bool index_has_stable_put_memory() const { return false; }

  /**
   * XXX(stephentu): hack
   */
  virtual void do_txn_epoch_sync() const {}

  /**
   * XXX(stephentu): hack
   */
  virtual void do_txn_finish() const {}

  /**
   * for cruftier APIs
   */
  virtual void thread_init() {}

  /**
   * for cruftier APIs
   */
  virtual void thread_end() {}

  /**
   * Allocate and return a new txn object, to use with this instance
   *
   * Flags is only for the ndb protocol for now
   */
  virtual void *new_txn(uint64_t txn_flags) = 0;

  /**
   * Returns true on successful commit.
   *
   * On failure, can either throw abstract_abort_exception, or
   * return false- caller should be prepared to deal with both cases
   */
  virtual bool commit_txn(void *txn) = 0;

  /**
   * XXX
   */
  virtual void abort_txn(void *txn) = 0;

  virtual void print_txn_debug(void *txn) const {}

  virtual abstract_ordered_index *
  open_index(const std::string &name,
             size_t value_size_hint,
             bool mostly_append = false) = 0;

  virtual void
  close_index(abstract_ordered_index *idx) = 0;
};

#endif /* _ABSTRACT_DB_H_ */
