#ifndef _NDB_BENCH_H_
#define _NDB_BENCH_H_

#include <stdint.h>

#include <map>
#include <vector>
#include <utility>
#include <string>

#include "abstract_db.h"
#include "../macros.h"
#include "../thread.h"
#include "../util.h"
#include "../spinbarrier.h"

extern void ycsb_do_test(abstract_db *db);
extern void tpcc_do_test(abstract_db *db);

// benchmark global variables
extern size_t nthreads;
extern volatile bool running;
extern int verbose;
extern uint64_t txn_flags;
extern double scale_factor;

class bench_worker : public ndb_thread {
public:

  bench_worker(unsigned long seed, abstract_db *db,
               const std::map<std::string, abstract_ordered_index *> &open_tables,
               spin_barrier *barrier_a, spin_barrier *barrier_b)
    : r(seed), db(db), open_tables(open_tables),
      barrier_a(barrier_a), barrier_b(barrier_b),
      // the ntxn_* numbers are per worker
      ntxn_commits(0), ntxn_aborts(0)
  {
  }

  virtual ~bench_worker() {}

  typedef void (*txn_fn_t)(bench_worker *);
  typedef std::vector< std::pair<double, txn_fn_t> > workload_desc;

  virtual workload_desc get_workload() = 0;

  virtual void run()
  {
    workload_desc workload = get_workload();
    db->thread_init();
    barrier_a->count_down();
    barrier_b->wait_for();
    while (running) {
      double d = r.next_uniform();
      for (size_t i = 0; i < workload.size(); i++) {
        if ((i + 1) == workload.size() || d < workload[i].first) {
          workload[i].second(this);
          break;
        }
      }
    }
    db->thread_end();
  }

  inline size_t get_ntxn_commits() const { return ntxn_commits; }
  inline size_t get_ntxn_aborts() const { return ntxn_aborts; }

protected:

  util::fast_random r;
  abstract_db *db;
  std::map<std::string, abstract_ordered_index *> open_tables;
  spin_barrier *barrier_a;
  spin_barrier *barrier_b;
  size_t ntxn_commits;
  size_t ntxn_aborts;
};

#endif /* _NDB_BENCH_H_ */
