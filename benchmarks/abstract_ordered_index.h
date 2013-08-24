#ifndef _ABSTRACT_ORDERED_INDEX_H_
#define _ABSTRACT_ORDERED_INDEX_H_

#include <stdint.h>
#include <string>
#include <utility>
#include <map>

#include "../macros.h"
#include "../str_arena.h"

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
   */
  virtual bool get(
      void *txn,
      const std::string &key,
      std::string &value,
      size_t max_bytes_read = std::string::npos) = 0;

  class scan_callback {
  public:
    virtual ~scan_callback() {}
    virtual bool invoke(const std::string &key,
                        const std::string &value) = 0;
  };

  /**
   * Search [start_key, end_key) if has_end_key is true, otherwise
   * search [start_key, +infty)
   */
  virtual void scan(
      void *txn,
      const std::string &start_key,
      const std::string *end_key,
      scan_callback &callback,
      str_arena *arena = nullptr) = 0;

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
      const std::string &key,
      const std::string &value) = 0;

  virtual const char *
  put(void *txn,
      std::string &&key,
      std::string &&value)
  {
    return put(txn, static_cast<const std::string &>(key),
                    static_cast<const std::string &>(value));
  }

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
         const std::string &key,
         const std::string &value)
  {
    return put(txn, key, value);
  }

  virtual const char *
  insert(void *txn,
         std::string &&key,
         std::string &&value)
  {
    return insert(txn, static_cast<const std::string &>(key),
                       static_cast<const std::string &>(value));
  }

  /**
   * Default implementation calls put() with NULL (zero-length) value
   */
  virtual void remove(
      void *txn,
      const std::string &key)
  {
    put(txn, key, "");
  }

  virtual void remove(
      void *txn,
      std::string &&key)
  {
    remove(txn, static_cast<const std::string &>(key));
  }

  /**
   * Only an estimate, not transactional!
   */
  virtual size_t size() const = 0;

  /**
   * Not thread safe for now
   */
  virtual std::map<std::string, uint64_t> clear() = 0;
};

#endif /* _ABSTRACT_ORDERED_INDEX_H_ */
