#ifndef _RCU_H_
#define _RCU_H_

#include <stdint.h>
#include <pthread.h>

#include <map>
#include <vector>
#include <utility>

class rcu {
public:
  typedef uint64_t epoch_t;

  typedef void (*deleter_t)(void *);
  typedef std::pair<void *, deleter_t> delete_entry;

  template <typename T>
  static inline void
  deleter(T *p)
  {
    delete p;
  }

  template <typename T>
  static inline void
  deleter_array(T *p)
  {
    delete p;
  }

  // all RCU threads interact w/ the RCU subsystem via
  // a sync struct
  struct sync {
    volatile epoch_t local_epoch;
    std::vector<delete_entry> local_queues[2];
    pthread_spinlock_t local_critical_mutex;
    sync(epoch_t local_epoch);
    ~sync();
  };

  /**
   * precondition: p must not already be registered - caller is
   * responsible for managing memory of s
   */
  static void register_sync(pthread_t p, sync *s);

  static sync *unregister_sync(pthread_t p);

  static void enable();

  static void region_begin();

  static void free(void *p, deleter_t fn);

  template <typename T>
  static inline void
  free(T *p)
  {
    free(p, deleter<T>);
  }

  template <typename T>
  static inline void
  free_array(T *p)
  {
    free(p, deleter_array<T>);
  }

  static void region_end();

private:
  static void *gc_thread_loop(void *p);
  static pthread_spinlock_t *rcu_mutex();

  static volatile epoch_t global_epoch;
  static std::vector<delete_entry> global_queues[2];

  static volatile bool gc_thread_started;
  static pthread_t gc_thread_p;

  static std::map<pthread_t, sync *> sync_map; // protected by rcu_mutex

  static __thread sync *tl_sync;
  static __thread unsigned int tl_crit_section_depth;
};

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
