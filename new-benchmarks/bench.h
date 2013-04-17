#ifndef _NDB_BENCH_H_
#define _NDB_BENCH_H_

#include <stdint.h>

#include <map>
#include <memory>
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

//extern void ycsb_do_test(const std::string &dbtype, int argc, char **argv);
extern void tpcc_do_test(const std::string &dbtype, int argc, char **argv);

enum { RUNMODE_TIME = 0,
       RUNMODE_OPS  = 1};

// benchmark global variables
extern size_t nthreads;
extern volatile bool running;
extern int verbose;
extern uint64_t txn_flags;
extern double scale_factor;
extern uint64_t runtime;
extern uint64_t ops_per_worker;
extern int run_mode;
extern int enable_parallel_loading;
extern int pin_cpus;
extern int slow_exit;
extern int retry_aborted_transaction;

namespace {
inline size_t constexpr
Prefix(size_t prefix)
{
#ifdef CHECK_INVARIANTS
  return std::numeric_limits<size_t>::max();
#else
  return prefix;
#endif
}
}

// NOTE: the typed_* versions of classes exist so we don't have to convert all
// classes to templatetized [for sanity in compliation times]; we trade off
// a bit of type-safety for more rapid development cycles

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
  bench_loader(unsigned long seed, abstract_db *db)
    : r(seed), db(db), b(0)
  {
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

  virtual void load() = 0;

  util::fast_random r;
  abstract_db *const db;
  spin_barrier *b;
};

template <typename Database>
class typed_bench_loader : public bench_loader {
public:
  typed_bench_loader(unsigned long seed, Database *db)
    : bench_loader(seed, db) {}
  inline Database *
  typed_db()
  {
    return static_cast<Database *>(db);
  }
  inline const Database *
  typed_db() const
  {
    return static_cast<const Database *>(db);
  }
};

class bench_worker : public ndb_thread {
public:

  bench_worker(unsigned int worker_id,
               unsigned long seed, abstract_db *db,
               spin_barrier *barrier_a, spin_barrier *barrier_b)
    : worker_id(worker_id), r(seed), db(db),
      barrier_a(barrier_a), barrier_b(barrier_b),
      // the ntxn_* numbers are per worker
      ntxn_commits(0), ntxn_aborts(0), size_delta(0)
  {
  }

  virtual ~bench_worker() {}

  // returns [did_commit?, size_increase_bytes]
  typedef std::pair<bool, ssize_t> txn_result;
  typedef txn_result (*txn_fn_t)(bench_worker *);

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

  virtual void run();

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

  virtual void on_run_setup() {}

  inline void *txn_buf() { return (void *) txn_obj_buf.data(); }

  unsigned int worker_id;
  util::fast_random r;
  abstract_db *const db;
  spin_barrier *const barrier_a;
  spin_barrier *const barrier_b;

private:
  size_t ntxn_commits;
  size_t ntxn_aborts;

protected:

//#ifdef ENABLE_BENCH_TXN_COUNTERS
//  txn_counter_map local_txn_counters;
//  void measure_txn_counters(void *txn, const char *txn_name);
//#else
//  inline ALWAYS_INLINE void measure_txn_counters(void *txn, const char *txn_name) {}
//#endif

  std::vector<size_t> txn_counts; // breakdown of txns
  ssize_t size_delta; // how many logical bytes (of values) did the worker add to the DB

  std::string txn_obj_buf;
  str_arena arena;
};

class bench_runner : private util::noncopyable {
public:
  bench_runner(abstract_db *db)
    : db(db), barrier_a(nthreads), barrier_b(1) {}
  virtual ~bench_runner() {}
  void run();
protected:
  // only called once
  virtual std::vector<std::unique_ptr<bench_loader>> make_loaders() = 0;

  // only called once
  virtual std::vector<std::unique_ptr<bench_worker>> make_workers() = 0;

  abstract_db *const db;
  std::map<std::string, std::shared_ptr<abstract_ordered_index>> open_tables;

  // barriers for actual benchmark execution
  spin_barrier barrier_a;
  spin_barrier barrier_b;
};

template <typename Database>
class typed_bench_runner : public bench_runner {
public:
  typed_bench_runner(Database *db)
    : bench_runner(db) {}
  inline Database *
  typed_db()
  {
    return static_cast<Database *>(db);
  }
  inline const Database *
  typed_db() const
  {
    return static_cast<const Database *>(db);
  }
};

template <typename Index>
class latest_key_callback : public Index::bytes_search_range_callback {
public:

  latest_key_callback(std::string &k, ssize_t limit = -1)
    : limit(limit), n(0), k(&k)
  {
    ALWAYS_ASSERT(limit == -1 || limit > 0);
  }

  virtual bool invoke(
      const std::string &key,
      const std::string &value)
  {
    INVARIANT(limit == -1 || n < size_t(limit));
    // see the note in bytes_static_limit_callback for why we explicitly
    // copy over regular (ref-counting) assignment
    k->assign(key.data(), key.size());
    ++n;
    return (limit == -1) || (n < size_t(limit));
  }

  inline size_t size() const { return n; }
  inline std::string &kstr() { return *k; }

private:
  ssize_t limit;
  size_t n;
  std::string *k;
};

namespace private_ {
  template <typename T, bool enable>
  struct container {
    container() {}
    container(const T &t) {}
    T & get(); // not defined
  };

  template <typename T>
  struct container<T, true> {
    container() {}
    container(const T &t) : t(t) {}
    inline T & get() { return t; }
    T t;
  };
}

// explicitly copies keys, because btree::search_range_call() interally
// re-uses a single string to pass keys (so using standard string assignment
// will force a re-allocation b/c of shared ref-counting)
//
// this isn't done for values, because each value has a distinct string from
// the string allocator, so there are no mutations while holding > 1 ref-count
template <typename Index, size_t N, bool ignore_key>
class bytes_static_limit_callback : public Index::bytes_search_range_callback {
public:

  static_assert(N > 0, "xx");

  bytes_static_limit_callback(str_arena *arena)
    : arena(arena)
  {
  }

  virtual bool invoke(
      const std::string &key,
      const std::string &value) OVERRIDE
  {
    INVARIANT(size() < N);
    INVARIANT(arena->manages(&key));
    INVARIANT(arena->manages(&value));
    if (ignore_key) {
      values.emplace_back(nullptr, &value);
    } else {
      // see note above
      std::string * const s_px = arena->next();
      INVARIANT(s_px && s_px->empty());
      s_px->assign(key.data(), key.size());
      values.emplace_back(s_px, &value);
    }
    return size() < N;
  }

  inline size_t
  size() const
  {
    return values.size();
  }

  inline const std::string &
  key(size_t i) const
  {
    return *values[i].first.get();
  }

  inline const std::string &
  value(size_t i) const
  {
    return *values[i].second;
  }

private:
  typedef std::pair<
    private_::container<const std::string *, !ignore_key>,
    const std::string *> kv_pair;
  typename util::vec<kv_pair, N>::type values;
  str_arena *arena;
};

template <typename Index, size_t N, bool ignore_key>
class static_limit_callback : public Index::search_range_callback {
public:

  static_assert(N > 0, "xx");

  virtual bool
  invoke(
      const typename Index::key_type &key,
      const typename Index::value_type &value) OVERRIDE
  {
    INVARIANT(size() < N);
    values.emplace_back(key, value);
    return size() < N;
  }

  inline size_t
  size() const
  {
    return values.size();
  }

  inline typename Index::key_type &
  key(size_t i)
  {
    return values[i].first.get();
  }

  inline typename Index::value_type &
  value(size_t i)
  {
    return values[i].second;
  }

private:
  typedef std::pair<
    private_::container<typename Index::key_type, !ignore_key>,
    typename Index::value_type> kv_pair;
  typename util::vec<kv_pair, N>::type values;
};

#endif /* _NDB_BENCH_H_ */
