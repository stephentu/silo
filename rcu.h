#ifndef _RCU_H_
#define _RCU_H_

#include <stdint.h>
#include <pthread.h>

#include <map>
#include <vector>
#include <list>
#include <utility>

#include "spinlock.h"

class rcu {
public:
  typedef uint64_t epoch_t;

  typedef void (*deleter_t)(void *);
  typedef std::pair<void *, deleter_t> delete_entry;
  typedef std::vector<delete_entry> delete_queue;
  //typedef std::list<delete_entry> delete_queue;

  template <typename T>
  static inline void
  deleter(void *p)
  {
    delete (T *) p;
  }

  template <typename T>
  static inline void
  deleter_array(void *p)
  {
    delete [] (T *) p;
  }

  // all RCU threads interact w/ the RCU subsystem via
  // a sync struct
  struct sync {
    volatile epoch_t local_epoch;
    delete_queue local_queues[2];
    spinlock local_critical_mutex;
    sync(epoch_t local_epoch)
      : local_epoch(local_epoch)
    {
      local_queues[0].reserve(16000);
      local_queues[1].reserve(16000);
    }
  };

  /**
   * precondition: p must not already be registered - caller is
   * responsible for managing memory of s
   */
  static void register_sync(pthread_t p, sync *s);

  static sync *unregister_sync(pthread_t p);

  static void enable();

  static void region_begin();

  static void free_with_fn(void *p, deleter_t fn);

  template <typename T>
  static inline void
  free(T *p)
  {
    free_with_fn(p, deleter<T>);
  }

  template <typename T>
  static inline void
  free_array(T *p)
  {
    free_with_fn(p, deleter_array<T>);
  }

  static void region_end();

  static bool in_rcu_region();

private:
  static void *gc_thread_loop(void *p);
  static spinlock &rcu_mutex();

  static volatile epoch_t global_epoch;
  static delete_queue global_queues[2];

  static volatile bool gc_thread_started;
  static pthread_t gc_thread_p;

  static std::map<pthread_t, sync *> sync_map; // protected by rcu_mutex

  static __thread sync *tl_sync;
  static __thread unsigned int tl_crit_section_depth;
};

/**
 * Use by data structures which use RCU
 *
 * XXX(stephentu): we are doing a terrible job of annotating all the
 * data structures which use RCU now
 */
class rcu_enabled {
public:
  inline rcu_enabled()
  {
    rcu::enable();
  }
};

class scoped_rcu_region {
public:
  inline scoped_rcu_region()
  {
    rcu::region_begin();
  }

  inline ~scoped_rcu_region()
  {
    rcu::region_end();
  }
};

#endif /* _RCU_H_ */
