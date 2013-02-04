#include <iostream>
#include <sstream>
#include <vector>
#include <utility>
#include <string>

#include <getopt.h>
#include <stdlib.h>
#include <unistd.h>

#include "bench.h"
#include "bdb_wrapper.h"
#include "ndb_wrapper.h"
#include "mysql_wrapper.h"

using namespace std;
using namespace util;

size_t nthreads = 1;
volatile bool running = true;
int verbose = 0;
uint64_t txn_flags = 0;
double scale_factor = 100000;

int
main(int argc, char **argv)
{
  abstract_db *db = NULL;
  void (*test_fn)(abstract_db *) = NULL;
  string bench_type;
  string db_type;
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
      {0, 0, 0, 0}
    };
    int option_index = 0;
    int c = getopt_long(argc, argv, "vb:s:t:d:B:f:", long_options, &option_index);
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

  if (verbose) {
    cerr << "settings:"                             << endl;
    cerr << "  bench       : " << bench_type        << endl;
    cerr << "  scale       : " << scale_factor      << endl;
    cerr << "  num-threads : " << nthreads          << endl;
    cerr << "  db-type     : " << db_type           << endl;
    cerr << "  basedir     : " << basedir           << endl;
    cerr << "  txn-flags   : " << hexify(txn_flags) << endl;
  }

  test_fn(db);

  delete db;
  return 0;
}
