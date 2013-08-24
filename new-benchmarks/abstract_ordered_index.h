#ifndef _ABSTRACT_ORDERED_INDEX_H_
#define _ABSTRACT_ORDERED_INDEX_H_

#include <stdint.h>
#include <string>
#include <utility>
#include <map>

#include "../macros.h"
#include "../str_arena.h"

class abstract_ordered_index {
public:

  virtual ~abstract_ordered_index() {}

  // virtual interface

  /**
   * Only an estimate, not transactional!
   */
  virtual size_t size() const = 0;

  /**
   * Not thread safe for now
   */
  virtual std::map<std::string, uint64_t> clear() = 0;

  // templated interface

  // typedef [unspecified] key_type;
  // typedef [unspecified] value_type;

  // struct search_range_callback {
  // public:
  //   virtual bool invoke(const key_type &, const value_type &);
  // };

  // struct bytes_search_range_callback {
  // public:
  //   virtual bool invoke(const std::string &, const std::string &);
  // };

  //template <typename FieldsMask>
  //bool search(
  //    /* [unspecified] */ &t, const key_type &k, value_type &v,
  //    FieldsMask fm);

  //template <typename FieldsMask>
  //void search_range_call(
  //    /* [unspecified] */ &t, const key_type &lower, const key_type *upper,
  //    search_range_callback &callback,
  //    bool no_key_results, /* skip decoding of keys? */
  //    FieldsMask fm);

  //void bytes_search_range_call(
  //    /* [unspecified] */ &t, const key_type &lower, const key_type *upper,
  //    bytes_search_range_callback &callback,
  //    size_t value_fields_prefix);

  //template <typename FieldsMask>
  //void put(
  //    /* [unspecified] */ &t, const key_type &k, const value_type &v,
  //    FieldsMask fm);

  //template <typename Traits>
  //void insert(
  //    /* [unspecified] */ &t, const key_type &k, const value_type &v);

  //template <typename Traits>
  //void remove(
  //    /* [unspecified] */ &t, const key_type &k);
};

#endif /* _ABSTRACT_ORDERED_INDEX_H_ */
