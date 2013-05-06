#ifndef _NDB_TYPE_TRAITS_H_
#define _NDB_TYPE_TRAITS_H_

#include <type_traits>

namespace private_ {

  // std::is_trivially_destructible not supported in g++-4.7, so we
  // do some hacky [conservative] variant of it
  template <typename T>
  struct is_trivially_destructible {
    static const bool value = std::is_scalar<T>::value;
  };

  template <typename K, typename V>
  struct is_trivially_destructible<std::pair<K, V>> {
    static const bool value =
      is_trivially_destructible<K>::value &&
      is_trivially_destructible<V>::value;
  };

  // XXX: same for now
  template <typename T>
  struct is_trivially_copyable : public is_trivially_destructible<T> {};

  // user types should add their own specializations

  template <typename T>
  struct typeutil { typedef const T & func_param_type; };

  template <typename T>
  struct primitive_typeutil { typedef T func_param_type; };

  // specialize typeutil for int types to use primitive_typeutil

#define SPECIALIZE_PRIM_TYPEUTIL(tpe) \
  template <> struct typeutil< tpe > : public primitive_typeutil< tpe > {};

  SPECIALIZE_PRIM_TYPEUTIL(bool)
  SPECIALIZE_PRIM_TYPEUTIL(int8_t)
  SPECIALIZE_PRIM_TYPEUTIL(uint8_t)
  SPECIALIZE_PRIM_TYPEUTIL(int16_t)
  SPECIALIZE_PRIM_TYPEUTIL(uint16_t)
  SPECIALIZE_PRIM_TYPEUTIL(int32_t)
  SPECIALIZE_PRIM_TYPEUTIL(uint32_t)
  SPECIALIZE_PRIM_TYPEUTIL(int64_t)
  SPECIALIZE_PRIM_TYPEUTIL(uint64_t)
}

#endif /* _NDB_TYPE_TRAITS_H_ */
