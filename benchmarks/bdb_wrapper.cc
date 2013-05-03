#include <limits>

#include "bdb_wrapper.h"
#include "../macros.h"

using namespace std;

bdb_wrapper::bdb_wrapper(const string &envdir, const string &dbfile)
  : env(0)
{
  env = new DbEnv(0);
  ALWAYS_ASSERT(env->log_set_config(DB_LOG_IN_MEMORY, 1) == 0);
  ALWAYS_ASSERT(env->set_lg_max(numeric_limits<uint32_t>::max()) == 0);
  ALWAYS_ASSERT(env->set_lg_regionmax(numeric_limits<uint32_t>::max()) == 0);
  //ALWAYS_ASSERT(env->set_lg_bsize(numeric_limits<uint32_t>::max()) == 0);
  ALWAYS_ASSERT(env->set_flags(DB_TXN_NOSYNC, 1) == 0);
  ALWAYS_ASSERT(env->set_cachesize(4, 0, 1) == 0);
  ALWAYS_ASSERT(env->open(envdir.c_str(), DB_INIT_LOCK | DB_INIT_LOG | DB_INIT_MPOOL | DB_INIT_TXN | DB_PRIVATE | DB_THREAD | DB_CREATE, 0) == 0);
}

bdb_wrapper::~bdb_wrapper()
{
  delete env;
}

void *
bdb_wrapper::new_txn(
    uint64_t txn_flags,
    str_arena &arena,
    void *buf, TxnProfileHint hint)
{
  DbTxn *txn = NULL;
  ALWAYS_ASSERT(env->txn_begin(NULL, &txn, 0) == 0);
  ALWAYS_ASSERT(txn != NULL);
  return (void *) txn;
}

bool
bdb_wrapper::commit_txn(void *p)
{
  return ((DbTxn *) p)->commit(0) == 0;
}

void
bdb_wrapper::abort_txn(void *p)
{
  ALWAYS_ASSERT(((DbTxn *) p)->abort() == 0);
}

abstract_ordered_index *
bdb_wrapper::open_index(const string &name, size_t value_size_hint, bool mostly_append)
{
  Db *db = new Db(env, 0);
  ALWAYS_ASSERT(db->set_flags(DB_TXN_NOT_DURABLE) == 0);
  DbTxn *txn = NULL;
  ALWAYS_ASSERT(env->txn_begin(NULL, &txn, 0) == 0);
  ALWAYS_ASSERT(db->open(txn, name.c_str(), NULL, DB_BTREE, DB_CREATE, 0) == 0);
  ALWAYS_ASSERT(txn->commit(0) == 0);
  return new bdb_ordered_index(db);
}

void
bdb_wrapper::close_index(abstract_ordered_index *idx)
{
  bdb_ordered_index *bidx = static_cast<bdb_ordered_index *>(idx);
  delete bidx;
}

bdb_ordered_index::~bdb_ordered_index()
{
  delete db;
}

bool
bdb_ordered_index::get(
    void *txn,
    const string &key,
    string &value,
    size_t max_bytes_read)
{
  Dbt kdbt((void *) key.data(), key.size());
  Dbt vdbt;
  //vdbt.set_flags(DB_DBT_MALLOC);
  int retno = db->get((DbTxn *) txn, &kdbt, &vdbt, 0);
  ALWAYS_ASSERT(retno == 0 || retno == DB_NOTFOUND);
  // XXX(stephentu): do a better job implementing this
  value.assign((char *) vdbt.get_data(), min(static_cast<size_t>(vdbt.get_size()), max_bytes_read));
  return retno == 0;
}

const char *
bdb_ordered_index::put(
    void *txn,
    const string &key,
    const string &value)
{
  Dbt kdbt((void *) key.data(), key.size());
  Dbt vdbt((void *) value.data(), value.size());
  ALWAYS_ASSERT(db->put((DbTxn *) txn, &kdbt, &vdbt, 0) == 0);
  return 0;
}
