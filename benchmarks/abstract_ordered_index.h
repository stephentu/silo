#ifndef _ABSTRACT_ORDERED_INDEX_H_
#define _ABSTRACT_ORDERED_INDEX_H_

#include <stdint.h>

/**
 * The underlying index manages memory for keys/values, but
 * may choose to expose the underlying memory to callers
 * (see put() and inesrt()).
 */
class abstract_ordered_index {
public:

  virtual ~abstract_ordered_index() {}

  /**
   * Get a key of length keylen. The underlying DB does not manage
   * the memory associated with key. Returns true if found, false otherwise
   *
   * The return condition for the value is as follows (kind of hacky):
   * A) if the DB from which this index was opened supports direct memory
   *    access, then value points to a pointer of length valuelen which
   *    is NOT the responsibility of the caller to free. This memory is
   *    NOT to be modified (really should be const char *), and is only valid
   *    until the associated txn object is freed.
   * B) otherwise, a pointer allocated by malloc() of length valuelen is returned,
   *    and it is the responsibility of the caller to free() it.
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
   *
   * If the return value is not NULL, then it points to the actual stable
   * location in memory where the value is located. Thus, [ret, ret+valuelen)
   * will be valid memory, bytewise equal to [value, value+valuelen), since the
   * implementations have immutable values for the time being. The value
   * returned is guaranteed to be valid memory until the key associated with
   * value is overriden.
   */
  virtual const char *
  put(void *txn,
      const char *key, size_t keylen,
      const char *value, size_t valuelen) = 0;

  /**
   * Insert a key of length keylen.
   *
   * If a record with key k exists, behavior is unspecified- this function
   * is only to be used when you can guarantee no such key exists (ie in loading phase)
   *
   * Default implementation calls put(). See put() for meaning of return value.
   */
  virtual const char *
  insert(void *txn,
         const char *key, size_t keylen,
         const char *value, size_t valuelen)
  {
    return put(txn, key, keylen, value, valuelen);
  }

  /**
   * Default implementation calls put() with NULL (zero-length) value
   */
  virtual void remove(
      void *txn,
      const char *key, size_t keylen)
  {
    put(txn, key, keylen, NULL, 0);
  }
};

#endif /* _ABSTRACT_ORDERED_INDEX_H_ */
