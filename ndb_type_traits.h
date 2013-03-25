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

  // user types should add their own specializations
}

#endif /* _NDB_TYPE_TRAITS_H_ */
