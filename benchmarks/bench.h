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
extern uint64_t runtime;

class scoped_db_thread_ctx : private util::noncopyable {
public:
  scoped_db_thread_ctx(abstract_db *db)
    : db(db)
  {
    db->thread_init();
  }
  ~scoped_db_thread_ctx()
  {
    db->thread_end();
  }
private:
  abstract_db *const db;
};

class bench_loader : public ndb_thread {
public:
  bench_loader(unsigned long seed, abstract_db *db,
               const std::map<std::string, abstract_ordered_index *> &open_tables)
    : r(seed), db(db), open_tables(open_tables)
  {}
  virtual void
  run()
  {
    scoped_db_thread_ctx ctx(db);
    load();
  }
protected:
  virtual void load() = 0;

  util::fast_random r;
  abstract_db *const db;
  std::map<std::string, abstract_ordered_index *> open_tables;
};

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

  virtual void
  run()
  {
    scoped_db_thread_ctx ctx(db);
    const workload_desc workload = get_workload();
    txn_counts.resize(workload.size());
    barrier_a->count_down();
    barrier_b->wait_for();
    while (running) {
      double d = r.next_uniform();
      for (size_t i = 0; i < workload.size(); i++) {
        if ((i + 1) == workload.size() || d < workload[i].first) {
          workload[i].second(this);
          txn_counts[i]++;
          break;
        }
        d -= workload[i].first;
      }
    }
  }

  inline size_t get_ntxn_commits() const { return ntxn_commits; }
  inline size_t get_ntxn_aborts() const { return ntxn_aborts; }

  inline const std::vector<size_t> &
  get_txn_counts() const
  {
    return txn_counts;
  }

protected:

  util::fast_random r;
  abstract_db *const db;
  std::map<std::string, abstract_ordered_index *> open_tables;
  spin_barrier *const barrier_a;
  spin_barrier *const barrier_b;
  size_t ntxn_commits;
  size_t ntxn_aborts;

  std::vector<size_t> txn_counts; // breakdown of txns
};

class bench_runner : private util::noncopyable {
public:
  bench_runner(abstract_db *db)
    : db(db), barrier_a(nthreads), barrier_b(1) {}
  virtual ~bench_runner() {}
  void run();
protected:
  // only called once
  virtual std::vector<bench_loader*> make_loaders() = 0;

  // only called once
  virtual std::vector<bench_worker*> make_workers() = 0;

  abstract_db *const db;
  std::map<std::string, abstract_ordered_index *> open_tables;

  // barriers for actual benchmark execution
  spin_barrier barrier_a;
  spin_barrier barrier_b;
};

#endif /* _NDB_BENCH_H_ */
