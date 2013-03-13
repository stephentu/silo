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
#include "kvdb_wrapper.h"
#include "mysql_wrapper.h"

using namespace std;
using namespace util;

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
      {"verbose"          , no_argument       , &verbose                 , 1}   ,
      {"parallel-loading" , no_argument       , &enable_parallel_loading , 1}   ,
      {"bench"            , required_argument , 0                        , 'b'} ,
      {"scale-factor"     , required_argument , 0                        , 's'} ,
      {"num-threads"      , required_argument , 0                        , 't'} ,
      {"db-type"          , required_argument , 0                        , 'd'} ,
      {"basedir"          , required_argument , 0                        , 'B'} ,
      {"txn-flags"        , required_argument , 0                        , 'f'} ,
      {"runtime"          , required_argument , 0                        , 'r'} ,
      {0, 0, 0, 0}
    };
    int option_index = 0;
    int c = getopt_long(argc, argv, "vpb:s:t:d:B:f:r:", long_options, &option_index);
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
      exit(1);

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
  } else if (db_type == "kvdb") {
    db = new kvdb_wrapper;
  } else if (db_type == "mysql") {
    string dbdir = basedir + "/mysql-db";
    db = new mysql_wrapper(dbdir, bench_type);
  } else
    ALWAYS_ASSERT(false);

#ifdef CHECK_INVARIANTS
  cerr << "WARNING: invariant checking is enabled - should disable for benchmark" << endl;
#endif

  if (verbose) {
    cerr << "settings:"                                   << endl;
    cerr << "  par-loading : " << enable_parallel_loading << endl;
    cerr << "  bench       : " << bench_type              << endl;
    cerr << "  scale       : " << scale_factor            << endl;
    cerr << "  num-threads : " << nthreads                << endl;
    cerr << "  db-type     : " << db_type                 << endl;
    cerr << "  basedir     : " << basedir                 << endl;
    cerr << "  txn-flags   : " << hexify(txn_flags)       << endl;
    cerr << "  runtime     : " << runtime                 << endl;
#ifdef USE_VARINT_ENCODING
    cerr << "  var-encode  : 1"                           << endl;
#else
    cerr << "  var-encode  : 0"                           << endl;
#endif

#ifdef USE_JEMALLOC
    cerr << "  allocator   : jemalloc"                    << endl;
#elif defined USE_TCMALLOC
    cerr << "  allocator   : tcmalloc"                    << endl;
#elif defined USE_FLOW
    cerr << "  allocator   : flow"                        << endl;
#else
    cerr << "  allocator   : libc"                        << endl;
#endif

    cerr << "system properties:" << endl;
    cerr << "  btree_internal_node_size: " << btree::InternalNodeSize() << endl;
    cerr << "  btree_leaf_node_size    : " << btree::LeafNodeSize() << endl;
  }

  test_fn(db);

  delete db;
  return 0;
}
