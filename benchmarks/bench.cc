#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <utility>
#include <string>

#include <getopt.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/sysinfo.h>

#include "bench.h"
#include "bdb_wrapper.h"
#include "ndb_wrapper.h"
#include "mysql_wrapper.h"

#include "../counter.h"

#ifdef USE_JEMALLOC
//cannot include this header b/c conflicts with malloc.h
//#include <jemalloc/jemalloc.h>
extern "C" void malloc_stats_print(void (*write_cb)(void *, const char *), void *cbopaque, const char *opts);
extern "C" int mallctl(const char *name, void *oldp, size_t *oldlenp, void *newp, size_t newlen);
#endif
#ifdef USE_TCMALLOC
#include <google/heap-profiler.h>
#endif

using namespace std;
using namespace util;

size_t nthreads = 1;
volatile bool running = true;
int verbose = 0;
uint64_t txn_flags = 0;
double scale_factor = 1.0;
uint64_t runtime = 30;

template <typename T>
static void
delete_pointers(const vector<T *> &pts)
{
  for (size_t i = 0; i < pts.size(); i++)
    delete pts[i];
}

template <typename T>
static vector<T>
elemwise_sum(const vector<T> &a, const vector<T> &b)
{
  INVARIANT(a.size() == b.size());
  vector<T> ret(a.size());
  for (size_t i = 0; i < a.size(); i++)
    ret[i] = a[i] + b[i];
  return ret;
}

template <typename K, typename V>
static void
map_agg(map<K, V> &agg, const map<K, V> &m)
{
  for (typename map<K, V>::const_iterator it = m.begin();
       it != m.end(); ++it)
    agg[it->first] += it->second;
}

// returns <free_bytes, total_bytes>
static pair<uint64_t, uint64_t>
get_system_memory_info()
{
  struct sysinfo inf;
  sysinfo(&inf);
  return make_pair(inf.mem_unit * inf.freeram, inf.mem_unit * inf.totalram);
}

static bool
clear_file(const char *name)
{
  ofstream ofs(name);
  ofs.close();
  return true;
}

static void
write_cb(void *p, const char *s) UNUSED;
static void
write_cb(void *p, const char *s)
{
  const char *f = "jemalloc.stats";
  static bool s_clear_file UNUSED = clear_file(f);
  ofstream ofs(f, ofstream::app);
  ofs << s;
  ofs.flush();
  ofs.close();
}

void
bench_runner::run()
{
  // load data
  const vector<bench_loader *> loaders = make_loaders();
  {
    const pair<uint64_t, uint64_t> mem_info_before = get_system_memory_info();
    {
      scoped_timer t("dataloading", verbose);
      for (vector<bench_loader *>::const_iterator it = loaders.begin();
          it != loaders.end(); ++it)
        (*it)->start();
      for (vector<bench_loader *>::const_iterator it = loaders.begin();
          it != loaders.end(); ++it)
        (*it)->join();
    }
    const pair<uint64_t, uint64_t> mem_info_after = get_system_memory_info();
    const int64_t delta = int64_t(mem_info_before.first) - int64_t(mem_info_after.first); // free mem
    const double delta_mb = double(delta)/1048576.0;
    if (verbose)
      cerr << "DB size: " << delta_mb << " MB" << endl;
  }

  db->do_txn_epoch_sync();

  event_counter::reset_all_counters(); // XXX: for now - we really should have a before/after loading

  map<string, size_t> table_sizes_before;
  if (verbose) {
    for (map<string, abstract_ordered_index *>::iterator it = open_tables.begin();
         it != open_tables.end(); ++it) {
      const size_t s = it->second->size();
      cerr << "table " << it->first << " size " << s << endl;
      table_sizes_before[it->first] = s;
    }
    cerr << "starting benchmark..." << endl;
  }

  const pair<uint64_t, uint64_t> mem_info_before = get_system_memory_info();

  const vector<bench_worker *> workers = make_workers();
  ALWAYS_ASSERT(!workers.empty());
  for (vector<bench_worker *>::const_iterator it = workers.begin();
       it != workers.end(); ++it)
    (*it)->start();

  barrier_a.wait_for(); // wait for all threads to start up
  barrier_b.count_down(); // bombs away!
  timer t;
  sleep(runtime);
  running = false;
  __sync_synchronize();
  const unsigned long elapsed = t.lap();
  size_t n_commits = 0;
  size_t n_aborts = 0;
  for (size_t i = 0; i < nthreads; i++) {
    workers[i]->join();
    n_commits += workers[i]->get_ntxn_commits();
    n_aborts += workers[i]->get_ntxn_aborts();
  }

  const double elapsed_sec = double(elapsed) / 1000000.0;
  const double agg_throughput = double(n_commits) / elapsed_sec;
  const double avg_per_core_throughput = agg_throughput / double(workers.size());

  const double agg_abort_rate = double(n_aborts) / elapsed_sec;
  const double avg_per_core_abort_rate = agg_abort_rate / double(workers.size());

  if (verbose) {
    const pair<uint64_t, uint64_t> mem_info_after = get_system_memory_info();
    const int64_t delta = int64_t(mem_info_before.first) - int64_t(mem_info_after.first); // free mem
    const double delta_mb = double(delta)/1048576.0;
    map<string, size_t> agg_txn_counts = workers[0]->get_txn_counts();
    ssize_t size_delta = workers[0]->get_size_delta();
    for (size_t i = 1; i < workers.size(); i++) {
      map_agg(agg_txn_counts, workers[i]->get_txn_counts());
      size_delta += workers[i]->get_size_delta();
    }
    const double size_delta_mb = double(size_delta)/1048576.0;
    map<string, double> ctrs = event_counter::get_all_counters();

    cerr << "--- table statistics ---" << endl;
    for (map<string, abstract_ordered_index *>::iterator it = open_tables.begin();
         it != open_tables.end(); ++it) {
      const size_t s = it->second->size();
      const ssize_t delta = ssize_t(s) - ssize_t(table_sizes_before[it->first]);
      cerr << "table " << it->first << " size " << it->second->size();
      if (delta < 0)
        cerr << " (" << delta << " records)" << endl;
      else
        cerr << " (+" << delta << " records)" << endl;
    }
    cerr << "--- benchmark statistics ---" << endl;
    cerr << "memory delta: " << delta_mb  << " MB" << endl;
    cerr << "memory delta rate: " << (delta_mb / elapsed_sec)  << " MB/sec" << endl;
    cerr << "logical memory delta: " << size_delta_mb << " MB" << endl;
    cerr << "logical memory delta rate: " << (size_delta_mb / elapsed_sec) << " MB/sec" << endl;
    cerr << "agg_throughput: " << agg_throughput << " ops/sec" << endl;
    cerr << "avg_per_core_throughput: " << avg_per_core_throughput << " ops/sec/core" << endl;
    cerr << "agg_abort_rate: " << agg_abort_rate << " aborts/sec" << endl;
    cerr << "avg_per_core_abort_rate: " << avg_per_core_abort_rate << " aborts/sec/core" << endl;
    cerr << "txn breakdown: " << format_list(agg_txn_counts.begin(), agg_txn_counts.end()) << endl;
    cerr << "--- system counters (for benchmark) ---" << endl;
    for (map<string, double>::iterator it = ctrs.begin();
         it != ctrs.end(); ++it)
      cerr << it->first << ": " << it->second << endl;
    cerr << "---------------------------------------" << endl;

#ifdef USE_JEMALLOC
    cerr << "dumping heap profile..." << endl;
    mallctl("prof.dump", NULL, NULL, NULL, 0);
    cerr << "printing jemalloc stats..." << endl;
    malloc_stats_print(write_cb, NULL, "");
#endif
#ifdef USE_TCMALLOC
    HeapProfilerDump("before-exit");
#endif
  }

  // output for plotting script
  cout << agg_throughput << " " << agg_abort_rate << endl;

  db->do_txn_finish();
  for (map<string, abstract_ordered_index *>::iterator it = open_tables.begin();
       it != open_tables.end(); ++it) {
    it->second->clear();
    delete it->second;
  }
  open_tables.clear();

  delete_pointers(loaders);
  delete_pointers(workers);
}

map<string, size_t>
bench_worker::get_txn_counts() const
{
  map<string, size_t> m;
  const workload_desc_vec workload = get_workload();
  for (size_t i = 0; i < txn_counts.size(); i++)
    m[workload[i].name] = txn_counts[i];
  return m;
}

int
main(int argc, char **argv)
{
  abstract_db *db = NULL;
  void (*test_fn)(abstract_db *) = NULL;
  string bench_type = "ycsb";
  string db_type = "ndb-proto2";
  char *curdir = get_current_dir_name();
  string basedir = curdir;
  free(curdir);
  while (1) {
    static struct option long_options[] =
    {
      {"verbose",      no_argument,       &verbose, 1},
      {"bench",        required_argument, 0,       'b'},
      {"scale-factor", required_argument, 0,       's'},
      {"num-threads",  required_argument, 0,       't'},
      {"db-type",      required_argument, 0,       'd'},
      {"basedir",      required_argument, 0,       'B'},
      {"txn-flags",    required_argument, 0,       'f'},
      {"runtime",      required_argument, 0,       'r'},
      {0, 0, 0, 0}
    };
    int option_index = 0;
    int c = getopt_long(argc, argv, "vb:s:t:d:B:f:r:", long_options, &option_index);
    if (c == -1)
      break;

    switch (c) {
    case 0:
      if (long_options[option_index].flag != 0)
        break;
      abort();
      break;

    case 'b':
      bench_type = optarg;
      break;

    case 's':
      scale_factor = strtod(optarg, NULL);
      ALWAYS_ASSERT(scale_factor > 0.0);
      break;

    case 't':
      nthreads = strtoul(optarg, NULL, 10);
      ALWAYS_ASSERT(nthreads > 0);
      break;

    case 'd':
      db_type = optarg;
      break;

    case 'B':
      basedir = optarg;
      break;

    case 'f':
      txn_flags = strtoul(optarg, NULL, 10);
      break;

    case 'r':
      runtime = strtoul(optarg, NULL, 10);
      ALWAYS_ASSERT(runtime > 0);
      break;

    case '?':
      /* getopt_long already printed an error message. */
      break;

    default:
      abort();
    }
  }

  if (bench_type == "ycsb")
    test_fn = ycsb_do_test;
  else if (bench_type == "tpcc")
    test_fn = tpcc_do_test;
  else if (bench_type == "queue")
    test_fn = queue_do_test;
  else if (bench_type == "encstress")
    test_fn = encstress_do_test;
  else
    ALWAYS_ASSERT(false);

  if (db_type == "bdb") {
    string cmd = "rm -rf " + basedir + "/db/*";
    // XXX(stephentu): laziness
    int ret UNUSED = system(cmd.c_str());
    db = new bdb_wrapper("db", bench_type + ".db");
  } else if (db_type == "ndb-proto1") {
    db = new ndb_wrapper(ndb_wrapper::PROTO_1);
  } else if (db_type == "ndb-proto2") {
    db = new ndb_wrapper(ndb_wrapper::PROTO_2);
  } else if (db_type == "mysql") {
    string dbdir = basedir + "/mysql-db";
    db = new mysql_wrapper(dbdir, bench_type);
  } else
    ALWAYS_ASSERT(false);

#ifdef CHECK_INVARIANTS
  cerr << "WARNING: invariant checking is enabled - should disable for benchmark" << endl;
#endif

  if (verbose) {
    cerr << "settings:"                             << endl;
    cerr << "  bench       : " << bench_type        << endl;
    cerr << "  scale       : " << scale_factor      << endl;
    cerr << "  num-threads : " << nthreads          << endl;
    cerr << "  db-type     : " << db_type           << endl;
    cerr << "  basedir     : " << basedir           << endl;
    cerr << "  txn-flags   : " << hexify(txn_flags) << endl;
    cerr << "  runtime     : " << runtime           << endl;
#ifdef USE_VARINT_ENCODING
    cerr << "  var-encode  : 1"                     << endl;
#else
    cerr << "  var-encode  : 0"                     << endl;
#endif

#ifdef USE_JEMALLOC
    cerr << "  allocator   : jemalloc"              << endl;
#elif defined USE_TCMALLOC
    cerr << "  allocator   : tcmalloc"              << endl;
#else
    cerr << "  allocator   : libc"                  << endl;
#endif

    cerr << "system properties:" << endl;
    cerr << "  btree_internal_node_size: " << btree::InternalNodeSize() << endl;
    cerr << "  btree_leaf_node_size    : " << btree::LeafNodeSize() << endl;
  }

  test_fn(db);

  delete db;
  return 0;
}
