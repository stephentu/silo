#ifndef _ABSTRACT_DB_H_
#define _ABSTRACT_DB_H_

#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

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

  class scan_callback {
  public:
    virtual ~scan_callback() {}

    // caller manages memory of key/value
    virtual bool invoke(const char *key, size_t key_len,
                        const char *value, size_t value_len) = 0;
  };

  /**
   * Search [start_key, end_key) if has_end_key is true, otherwise
   * search [start_key, +infty)
   *
   * Caller manages memory of start_key/end_key
   */
  virtual void scan(
      void *txn,
      const char *start_key, size_t start_len,
      const char *end_key, size_t end_len,
      bool has_end_key,
      scan_callback &callback) = 0;

  /**
   * Put a key of length keylen, with mapping of length valuelen.
   * The underlying DB does not manage the memory pointed to by key or value
   * (a copy is made).
   *
   * If a record with key k exists, overwrites. Otherwise, inserts.
   */
  virtual void put(
      void *txn,
      const char *key, size_t keylen,
      const char *value, size_t valuelen) = 0;

  /**
   * Insert a key of length keylen.
   *
   * If a record with key k exists, behavior is unspecified- this function
   * is only to be used when you can guarantee no such key exists (ie in loading phase)
   *
   * Default implementation calls put()
   */
  virtual void insert(
      void *txn,
      const char *key, size_t keylen,
      const char *value, size_t valuelen)
  {
    put(txn, key, keylen, value, valuelen);
  }
};

#endif /* _ABSTRACT_DB_H_ */
