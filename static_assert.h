#ifndef _STATIC_ASSERT_H_
#define _STATIC_ASSERT_H_

#define _CONCAT0(a, b) a ## b
#define _CONCAT(a, b)  _CONCAT0(a, b)

#define _static_assert(pred) \
  struct _CONCAT(_static_assert, __LINE__) { \
    void asserter() { \
      switch (pred) { case 0: case (pred): default: break; } \
    } \
  }

#endif /* _STATIC_ASSERT_H_ */
