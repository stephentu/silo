#include <sys/stat.h>
#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <iostream>
#include <sstream>

#include "../macros.h"
#include "mysql_wrapper.h"

using namespace std;
static bool embed_active = false;

static inline void
check_result(MYSQL *conn, int result)
{
  if (likely(result == 0))
    return;
  cerr << "mysql_error_message: " << mysql_error(conn) << endl;
  ALWAYS_ASSERT(false);
}

mysql_wrapper::mysql_wrapper(const string &dir, const string &db)
  : db(db)
{
  struct stat st;
  if (stat(dir.c_str(), &st) != 0) {
    cerr << "ERROR! The db directory " << dir << " does not exist" << endl;
    ALWAYS_ASSERT(false);
  }

  if (!__sync_bool_compare_and_swap(&embed_active, false, true)) {
    cerr << "only one embedmysql object can exist at once" << endl;
    ALWAYS_ASSERT(false);
  }

  char dir_arg[1024];
  snprintf(dir_arg, sizeof(dir_arg), "--datadir=%s", dir.c_str());

  /**
       --innodb-buffer-pool-size=$SPACE
       --innodb_log_file_size=1792M
       --port=$PORT
       --transaction_isolation=serializable
       --max_connections=300
       --local-infile=1
       --max_allowed_packet=1073741824
       --max_heap_table_size=2147483648
       --group_concat_max_len=1073741824
       --skip-slave-start
       --innodb_flush_method=O_DIRECT
       --log-error
  */

  const char *mysql_av[] =
    {
      "progname",
      "--skip-grant-tables",
      dir_arg,
      "--character-set-server=utf8",
      "--innodb-buffer-pool-size=128M", // XXX: don't hardocde
      "--innodb_log_file_size=1792M",
      "--transaction_isolation=serializable",
      "--innodb_flush_method=O_DIRECT",
    };

  check_result(0, mysql_library_init(ARRAY_NELEMS(mysql_av), (char **) mysql_av, 0));

  MYSQL *conn = new_connection("");

  stringstream b;
  b << "CREATE DATABASE IF NOT EXISTS " << db << ";";
  check_result(conn, mysql_query(conn, b.str().c_str()));
  check_result(conn, mysql_select_db(conn, db.c_str()));

  const char *cmd =
    "CREATE TABLE IF NOT EXISTS tbl ("
    "  tbl_key VARBINARY(256) PRIMARY KEY, "
    "  tbl_value VARBINARY(256) "
    ") ENGINE=InnoDB;";
  check_result(conn, mysql_query(conn, cmd));
  check_result(conn, mysql_commit(conn));
  mysql_close(conn);
}

mysql_wrapper::~mysql_wrapper()
{
  mysql_server_end();
  ALWAYS_ASSERT(__sync_bool_compare_and_swap(&embed_active, true, false));
}

void
mysql_wrapper::thread_init()
{
  ALWAYS_ASSERT(tl_conn == NULL);
  tl_conn = new_connection(db);
  ALWAYS_ASSERT(tl_conn);
}

void
mysql_wrapper::thread_end()
{
  ALWAYS_ASSERT(tl_conn);
  mysql_close(tl_conn);
  tl_conn = NULL;
}

void *
mysql_wrapper::new_txn()
{
  ALWAYS_ASSERT(tl_conn);
  check_result(tl_conn, mysql_real_query(tl_conn, "BEGIN", 5));
  return (void *) tl_conn;
}

bool
mysql_wrapper::commit_txn(void *p)
{
  ALWAYS_ASSERT(tl_conn == p);
  return mysql_commit(tl_conn) == 0;
}

void
mysql_wrapper::abort_txn(void *p)
{
  ALWAYS_ASSERT(tl_conn == p);
  check_result(tl_conn, mysql_rollback(tl_conn));
}

static inline string
my_escape(MYSQL *conn, const char *p, size_t l)
{
  char buf[2*l + 1];
  unsigned long newl = mysql_real_escape_string(conn, &buf[0], p, l);
  return string(&buf[0], newl);
}

bool
mysql_wrapper::get(
    void *txn,
    const char *key, size_t keylen,
    char *&value, size_t &valuelen)
{
  INVARIANT(txn == tl_conn);
  ALWAYS_ASSERT(keylen <= 256);
  stringstream b;
  b << "SELECT tbl_value FROM tbl WHERE tbl_key = '" << my_escape(tl_conn, key, keylen) << "';";
  string q = b.str();
  check_result(tl_conn, mysql_real_query(tl_conn, q.data(), q.size()));
  MYSQL_RES *res = mysql_store_result(tl_conn);
  ALWAYS_ASSERT(res);
  MYSQL_ROW row = mysql_fetch_row(res);
  bool ret = false;
  if (row) {
    unsigned long *lengths = mysql_fetch_lengths(res);
    value = (char *) malloc(lengths[0]);
    ALWAYS_ASSERT(key);
    memcpy(value, row[0], lengths[0]);
    ret = true;
  }
  mysql_free_result(res);
  return ret;
}

void
mysql_wrapper::put(
    void *txn,
    const char *key, size_t keylen,
    const char *value, size_t valuelen)
{
  INVARIANT(txn == tl_conn);
  ALWAYS_ASSERT(keylen <= 256);
  ALWAYS_ASSERT(valuelen <= 256);
  string escaped_value = my_escape(tl_conn, value, valuelen);
  stringstream b;
  b << "INSERT INTO tbl VALUES ('" << my_escape(tl_conn, key, keylen)
    << "', '" << escaped_value << "') ON DUPLICATE KEY UPDATE tbl_value='" << escaped_value << "';";
  string q = b.str();
  check_result(tl_conn, mysql_real_query(tl_conn, q.data(), q.size()));
}

MYSQL *
mysql_wrapper::new_connection(const string &db)
{
  MYSQL *conn = mysql_init(0);
  mysql_options(conn, MYSQL_OPT_USE_EMBEDDED_CONNECTION, 0);
  if (!mysql_real_connect(conn, 0, 0, 0, db.c_str(), 0, 0, CLIENT_MULTI_STATEMENTS)) {
    mysql_close(conn);
    cerr << "mysql_real_connect: " << mysql_error(conn) << endl;
    ALWAYS_ASSERT(false);
  }
  check_result(conn, mysql_autocommit(conn, 0));
  return conn;
}

__thread MYSQL *mysql_wrapper::tl_conn = NULL;
