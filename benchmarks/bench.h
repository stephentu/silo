#ifndef _NDB_BENCH_H_
#define _NDB_BENCH_H_

#include <stdint.h>

#include <map>
#include <vector>
#include <utility>
#include <string>

#include "abstract_db.h"
#include "../macros.h"
#include "../static_assert.h"
#include "../thread.h"
#include "../util.h"
#include "../spinbarrier.h"
#include "../rcu.h"

extern void ycsb_do_test(abstract_db *db);
extern void tpcc_do_test(abstract_db *db);
extern void queue_do_test(abstract_db *db);
extern void encstress_do_test(abstract_db *db);

// benchmark global variables
extern size_t nthreads;
extern volatile bool running;
extern int verbose;
extern uint64_t txn_flags;
extern double scale_factor;
extern uint64_t runtime;
extern int enable_parallel_loading;

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
    : r(seed), db(db), open_tables(open_tables), b(0)
  {
    txn_obj_buf.resize(db->sizeof_txn_object(txn_flags));
  }
  inline void
  set_barrier(spin_barrier &b)
  {
    ALWAYS_ASSERT(!this->b);
    this->b = &b;
  }
  virtual void
  run()
  {
    { // XXX(stephentu): this is a hack
      scoped_rcu_region r; // register this thread in rcu region
    }
    ALWAYS_ASSERT(b);
    b->count_down();
    b->wait_for();
    scoped_db_thread_ctx ctx(db);
    load();
  }
protected:
  inline void *txn_buf() { return (void *) txn_obj_buf.data(); }

  virtual void load() = 0;

  util::fast_random r;
  abstract_db *const db;
  std::map<std::string, abstract_ordered_index *> open_tables;
  spin_barrier *b;
  std::string txn_obj_buf;
};

class bench_worker : public ndb_thread {
public:

  bench_worker(unsigned long seed, abstract_db *db,
               const std::map<std::string, abstract_ordered_index *> &open_tables,
               spin_barrier *barrier_a, spin_barrier *barrier_b)
    : r(seed), db(db), open_tables(open_tables),
      barrier_a(barrier_a), barrier_b(barrier_b),
      // the ntxn_* numbers are per worker
      ntxn_commits(0), ntxn_aborts(0), size_delta(0)
  {
    txn_obj_buf.resize(db->sizeof_txn_object(txn_flags));
  }

  virtual ~bench_worker() {}

  // returns how many bytes (of values) changed by the txn
  typedef ssize_t (*txn_fn_t)(bench_worker *);

  struct workload_desc {
    workload_desc() {}
    workload_desc(const std::string &name, double frequency, txn_fn_t fn)
      : name(name), frequency(frequency), fn(fn)
    {
      ALWAYS_ASSERT(frequency > 0.0);
      ALWAYS_ASSERT(frequency <= 1.0);
    }
    std::string name;
    double frequency;
    txn_fn_t fn;
  };
  typedef std::vector<workload_desc> workload_desc_vec;
  virtual workload_desc_vec get_workload() const = 0;

  virtual void
  run()
  {
    { // XXX(stephentu): this is a hack
      scoped_rcu_region r; // register this thread in rcu region
    }
    scoped_db_thread_ctx ctx(db);
    const workload_desc_vec workload = get_workload();
    txn_counts.resize(workload.size());
    barrier_a->count_down();
    barrier_b->wait_for();
    while (running) {
      double d = r.next_uniform();
      for (size_t i = 0; i < workload.size(); i++) {
        if ((i + 1) == workload.size() || d < workload[i].frequency) {
          size_delta += workload[i].fn(this);
          txn_counts[i]++;
          break;
        }
        d -= workload[i].frequency;
      }
    }
  }

  inline size_t get_ntxn_commits() const { return ntxn_commits; }
  inline size_t get_ntxn_aborts() const { return ntxn_aborts; }

  std::map<std::string, size_t> get_txn_counts() const;

  typedef abstract_db::counter_map counter_map;
  typedef abstract_db::txn_counter_map txn_counter_map;

#ifdef ENABLE_BENCH_TXN_COUNTERS
  inline txn_counter_map
  get_local_txn_counters() const
  {
    return local_txn_counters;
  }
#endif

  inline ssize_t get_size_delta() const { return size_delta; }

protected:
  inline void *txn_buf() { return (void *) txn_obj_buf.data(); }

  util::fast_random r;
  abstract_db *const db;
  std::map<std::string, abstract_ordered_index *> open_tables;
  spin_barrier *const barrier_a;
  spin_barrier *const barrier_b;
  size_t ntxn_commits;
  size_t ntxn_aborts;

#ifdef ENABLE_BENCH_TXN_COUNTERS
  txn_counter_map local_txn_counters;
  void measure_txn_counters(void *txn, const char *txn_name);
#else
  inline ALWAYS_INLINE void measure_txn_counters(void *txn, const char *txn_name) {}
#endif

  std::vector<size_t> txn_counts; // breakdown of txns
  ssize_t size_delta; // how many logical bytes (of values) did the worker add to the DB

  std::string txn_obj_buf;
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

class limit_callback : public abstract_ordered_index::scan_callback {
public:
  limit_callback(ssize_t limit = -1)
    : limit(limit), n(0)
  {
    ALWAYS_ASSERT(limit == -1 || limit > 0);
  }

  virtual bool invoke(
      const char *key, size_t key_len,
      const char *value, size_t value_len)
  {
    INVARIANT(limit == -1 || n < size_t(limit));
    values.push_back(
        std::make_pair(
          std::string(key, key_len), std::string(value, value_len)));
    return (limit == -1) || (++n < size_t(limit));
  }

  typedef std::pair<std::string, std::string> kv_pair;
  std::vector<kv_pair> values;

  const ssize_t limit;
private:
  size_t n;
};

template <typename StrAllocator>
class latest_key_callback : public abstract_ordered_index::scan_callback {
public:
  latest_key_callback(const StrAllocator &alloc)
    : n(0), k(&alloc())
  { }

  virtual bool invoke(
      const char *key, size_t key_len,
      const char *value, size_t value_len)
  {
    n++;
    k->assign(key, key_len);
    return true;
  }

  inline size_t size() const { return n; }
  inline std::string &kstr() { return *k; }

private:
  size_t n;
  std::string *k;
};

template <size_t N, typename StrAllocator>
class static_limit_callback : public abstract_ordered_index::scan_callback {
public:
  static_limit_callback(StrAllocator alloc)
    : n(0), alloc(alloc)
  {
    _static_assert(N > 0);
  }

  virtual bool invoke(
      const char *key, size_t key_len,
      const char *value, size_t value_len)
  {
    INVARIANT(n < N);
    std::string &k = alloc();
    std::string &v = alloc();
    k.assign(key, key_len);
    v.assign(value, value_len);
    values.emplace_back(k, v);
    return ++n < N;
  }

  inline size_t
  size() const
  {
    return values.size();
  }

  typedef std::pair<std::string, std::string> kv_pair;
  typename util::vec<kv_pair, N>::type values;

private:
  size_t n;
  StrAllocator alloc;
};

#endif /* _NDB_BENCH_H_ */
