#ifndef _UTIL_H_
#define _UTIL_H_

#include <sstream>
#include <string>

namespace util {


template <typename ForwardIterator>
std::string
format_list(ForwardIterator begin, ForwardIterator end)
{
  std::stringstream ss;
  ss << "[";
  bool first = true;
  while (begin != end) {
    if (!first)
      ss << ", ";
    first = false;
    ss << *begin++;
  }
  ss << "]";
  return ss.str();
}

}

#endif /* _UTIL_H_ */
