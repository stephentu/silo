#ifndef _ABSTRACT_DB_H_
#define _ABSTRACT_DB_H_

#include <stddef.h>

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
   * for cruftier APIs
   */
  virtual void thread_init() {}

  /**
   * for cruftier APIs
   */
  virtual void thread_end() {}

  /**
   * Allocate and return a new txn object, to use with this instance
   */
  virtual void *new_txn() = 0;

  /**
   * XXX
   */
  virtual bool commit_txn(void *txn) = 0;

  /**
   * XXX
   */
  virtual void abort_txn(void *txn) = 0;

  /**
   * Get a key of length keylen. The underlying DB does not manage
   * the memory associated with key. Returns true if found, false otherwise
   *
   * Return the result in value (of size valuelen). The caller becomes
   * responsible for the memory pointed to by value. This memory is
   * allocated by using malloc().
   */
  virtual bool get(
      void *txn,
      const char *key, size_t keylen,
      char *&value, size_t &valuelen) = 0;

  /**
   * Put a key of length keylen, with mapping of length valuelen.
   * The underlying DB does not manage the memory pointed to by key or value
   * (a copy is made).
   */
  virtual void put(
      void *txn,
      const char *key, size_t keylen,
      const char *value, size_t valuelen) = 0;
};

#endif /* _ABSTRACT_DB_H_ */
