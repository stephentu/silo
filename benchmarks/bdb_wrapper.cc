#include "bdb_wrapper.h"
#include "../macros.h"

using namespace std;

bdb_wrapper::bdb_wrapper(const string &envdir, const string &dbfile)
  : env(0), db(0)
{
  env = new DbEnv(0);
  ALWAYS_ASSERT(env->log_set_config(DB_LOG_IN_MEMORY, 1) == 0);
  ALWAYS_ASSERT(env->set_flags(DB_TXN_NOSYNC, 1) == 0);
  ALWAYS_ASSERT(env->open(envdir.c_str(), DB_INIT_LOCK | DB_INIT_LOG | DB_INIT_MPOOL | DB_INIT_TXN | DB_PRIVATE | DB_THREAD | DB_CREATE, 0) == 0);

  db = new Db(env, 0);
  DbTxn *txn = NULL;
  ALWAYS_ASSERT(env->txn_begin(NULL, &txn, 0) == 0);
  ALWAYS_ASSERT(db->open(txn, dbfile.c_str(), NULL, DB_BTREE, DB_CREATE, 0) == 0);
  ALWAYS_ASSERT(txn->commit(0) == 0);
}

bdb_wrapper::~bdb_wrapper()
{
  delete db;
  delete env;
}

void *
bdb_wrapper::new_txn()
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

bool
bdb_wrapper::get(
    void *txn,
    const char *key, size_t keylen,
    char *&value, size_t &valuelen)
{
  Dbt kdbt((void *) key, keylen);
  Dbt vdbt;
  vdbt.set_flags(DB_DBT_MALLOC);
  int retno = db->get((DbTxn *) txn, &kdbt, &vdbt, 0);
  ALWAYS_ASSERT(retno == 0 || retno == DB_NOTFOUND);
  value = (char *) vdbt.get_data();
  valuelen = vdbt.get_size();
  return retno == 0;
}

void
bdb_wrapper::put(
    void *txn,
    const char *key, size_t keylen,
    const char *value, size_t valuelen)
{
  Dbt kdbt((void *) key, keylen);
  Dbt vdbt((void *) value, valuelen);
  ALWAYS_ASSERT(db->put((DbTxn *) txn, &kdbt, &vdbt, 0) == 0);
}
