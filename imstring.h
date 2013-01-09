#ifndef _NDB_IMSTRING_H_
#define _NDB_IMSTRING_H_

#include <stdint.h>
#include <string.h>
#include <string>

/**
 * Semi-immutable string- It is not fully immutable in order to
 * support assignment and swapping
 */
class imstring {
public:
  imstring() : p(NULL), l(0) {}

  imstring(const uint8_t *src, size_t l)
    : p(new uint8_t[l]), l(l)
  {
    memcpy(p, src, l);
  }

  imstring(const std::string &s)
    : p(new uint8_t[s.size()]), l(s.size())
  {
    memcpy(p, s.data(), l);
  }

  imstring(const imstring &that)
  {
    replaceWith(that);
  }

  imstring &
  operator=(const imstring &that)
  {
    replaceWith(that);
    return *this;
  }

  ~imstring()
  {
    if (p)
      delete []p;
  }

  inline const uint8_t *
  data() const
  {
    return p;
  }

  inline size_t
  size() const
  {
    return l;
  }

  inline void
  swap(imstring &that)
  {
    uint8_t *ptemp = p;
    size_t ltemp = l;
    p = that.p;
    l = that.l;
    that.p = ptemp;
    that.l = ltemp;
  }

private:

  inline void
  replaceWith(const imstring &that)
  {
    if (p)
      delete [] p;
    p = new uint8_t[that.size()];
    l = that.size();
    memcpy(p, that.data(), l);
  }

  uint8_t *p;
  size_t l;
};

#endif /* _NDB_IMSTRING_H_ */
