#include <iostream>
#include <db_cxx.h>

#include "../macros.h"
#include "../varkey.h"

using namespace std;

int main(void)
{
  DbEnv env(0);
  ALWAYS_ASSERT(env.log_set_config(DB_LOG_IN_MEMORY, 1) == 0);
  ALWAYS_ASSERT(env.open("db", DB_INIT_LOCK | DB_INIT_LOG | DB_INIT_MPOOL | DB_INIT_TXN | DB_PRIVATE | DB_THREAD | DB_CREATE, 0) == 0);

  Db db(&env, 0);
  ALWAYS_ASSERT(db.open(NULL, "ycsb.db", NULL, DB_BTREE, DB_CREATE, 0) == 0);

  // load
  for (size_t i = 0; i < 100; i++) {
    string k = u64_varkey(i).str();
    string v(100, 'a');
    Dbt kdbt((void *) k.data(), k.size());
    Dbt vdbt((void *) v.data(), v.size());
    ALWAYS_ASSERT(db.put(NULL, &kdbt, &vdbt, 0) == 0);
  }

  return 0;
}
