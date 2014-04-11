#include <iostream>
#include <sstream>
#include <vector>
#include <utility>
#include <string>

#include <stdlib.h>
#include <unistd.h>

#include "../macros.h"
#include "../varkey.h"
#include "../thread.h"
#include "../util.h"
#include "../spinbarrier.h"

#include "../record/encoder.h"
#include "bench.h"

using namespace std;
using namespace util;

static size_t nusers;
static size_t nproducts;
static const float pricefactor = 10000.0; // bids range from [0, 10000.0)

#define BIDUSER_REC_KEY_FIELDS(x, y) \
  x(uint32_t,uid)
#define BIDUSER_REC_VALUE_FIELDS(x, y) \
  x(uint32_t,bid)
DO_STRUCT(biduser_rec, BIDUSER_REC_KEY_FIELDS, BIDUSER_REC_VALUE_FIELDS)

#define BID_REC_KEY_FIELDS(x, y) \
  x(uint32_t,uid) \
  y(uint32_t,bid)
#define BID_REC_VALUE_FIELDS(x, y) \
  x(uint32_t,pid) \
  y(float,amount)
DO_STRUCT(bid_rec, BID_REC_KEY_FIELDS, BID_REC_VALUE_FIELDS)

#define BIDMAX_REC_KEY_FIELDS(x, y) \
  x(uint32_t,pid)
#define BIDMAX_REC_VALUE_FIELDS(x, y) \
  x(float,amount)
DO_STRUCT(bidmax_rec, BIDMAX_REC_KEY_FIELDS, BIDMAX_REC_VALUE_FIELDS)

class bid_worker : public bench_worker {
public:
  bid_worker(
      unsigned int worker_id,
      unsigned long seed, abstract_db *db,
      const map<string, abstract_ordered_index *> &open_tables,
      spin_barrier *barrier_a, spin_barrier *barrier_b)
    : bench_worker(worker_id, false, seed, db,
                   open_tables, barrier_a, barrier_b),
      bidusertbl(open_tables.at("biduser")),
      bidtbl(open_tables.at("bid")),
      bidmaxtbl(open_tables.at("bidmax"))
  {
  }

  txn_result
  txn_bid()
  {
    void *txn = db->new_txn(txn_flags, arena, txn_buf());
    try {
      // pick user at random
      biduser_rec::key biduser_key(r.next() % nusers);
      ALWAYS_ASSERT(bidusertbl->get(txn, Encode(obj_k0, biduser_key), obj_v0));
      biduser_rec::value biduser_value_temp;
      const biduser_rec::value *biduser_value = Decode(obj_v0, biduser_value_temp);

      // update the user's bid
      const uint32_t bid = biduser_value->bid;
      biduser_value_temp.bid++;
      bidusertbl->put(txn, Encode(str(), biduser_key), Encode(str(), biduser_value_temp));

      // insert the new bid
      const bid_rec::key bid_key(biduser_key.uid, bid);
      const bid_rec::value bid_value(r.next() % nproducts, r.next_uniform() * pricefactor);
      bidtbl->insert(txn, Encode(str(), bid_key), Encode(str(), bid_value));

      // update the max value if necessary
      const bidmax_rec::key bidmax_key(bid_value.pid);
      ALWAYS_ASSERT(bidmaxtbl->get(txn, Encode(obj_k0, bidmax_key), obj_v0));
      bidmax_rec::value bidmax_value_temp;
      const bidmax_rec::value *bidmax_value = Decode(obj_v0, bidmax_value_temp);

      if (bid_value.amount > bidmax_value->amount) {
        bidmax_value_temp.amount = bid_value.amount;
        bidmaxtbl->put(txn, Encode(str(), bidmax_key), Encode(str(), bidmax_value_temp));
      }

      if (likely(db->commit_txn(txn)))
        return txn_result(true, 0);
    } catch (abstract_db::abstract_abort_exception &ex) {
      db->abort_txn(txn);
    }
    return txn_result(false, 0);
  }

  static txn_result
  TxnBid(bench_worker *w)
  {
    return static_cast<bid_worker *>(w)->txn_bid();
  }

  virtual workload_desc_vec
  get_workload() const
  {
    workload_desc_vec w;
    w.push_back(workload_desc("Bid", 1.0, TxnBid));
    return w;
  }

private:
  inline ALWAYS_INLINE string &
  str()
  {
    return *arena.next();
  }

  abstract_ordered_index *bidusertbl;
  abstract_ordered_index *bidtbl;
  abstract_ordered_index *bidmaxtbl;

  // scratch buffer space
  string obj_k0;
  string obj_v0;

};

class bid_loader : public bench_loader {
public:
  bid_loader(unsigned long seed,
             abstract_db *db,
             const map<string, abstract_ordered_index *> &open_tables)
    : bench_loader(seed, db, open_tables)
  {}

protected:
  virtual void
  load()
  {
    abstract_ordered_index *bidusertbl = open_tables.at("biduser");
    abstract_ordered_index *bidmaxtbl = open_tables.at("bidmax");
    try {
      // load
      const size_t batchsize = (db->txn_max_batch_size() == -1) ?
        10000 : db->txn_max_batch_size();
      ALWAYS_ASSERT(batchsize > 0);

      {
        const size_t nbatches = nusers / batchsize;
        if (nbatches == 0) {
          void *txn = db->new_txn(txn_flags, arena, txn_buf());
          for (size_t j = 0; j < nusers; j++) {
            const biduser_rec::key key(j);
            const biduser_rec::value value(0);
            string buf0;
            bidusertbl->insert(txn, Encode(key), Encode(buf0, value));
          }
          if (verbose)
            cerr << "batch 1/1 done" << endl;
          ALWAYS_ASSERT(db->commit_txn(txn));
        } else {
          for (size_t i = 0; i < nbatches; i++) {
            size_t keyend = (i == nbatches - 1) ? nusers : (i + 1) * batchsize;
            void *txn = db->new_txn(txn_flags, arena, txn_buf());
            for (size_t j = i * batchsize; j < keyend; j++) {
              const biduser_rec::key key(j);
              const biduser_rec::value value(0);
              string buf0;
              bidusertbl->insert(txn, Encode(key), Encode(buf0, value));
            }
            if (verbose)
              cerr << "batch " << (i + 1) << "/" << nbatches << " done" << endl;
            ALWAYS_ASSERT(db->commit_txn(txn));
          }
        }
        if (verbose)
          cerr << "[INFO] finished loading BIDUSER table" << endl;
      }

      {
        const size_t nbatches = nproducts / batchsize;
        if (nbatches == 0) {
          void *txn = db->new_txn(txn_flags, arena, txn_buf());
          for (size_t j = 0; j < nproducts; j++) {
            const bidmax_rec::key key(j);
            const bidmax_rec::value value(0.0);
            string buf0;
            bidmaxtbl->insert(txn, Encode(key), Encode(buf0, value));
          }
          if (verbose)
            cerr << "batch 1/1 done" << endl;
          ALWAYS_ASSERT(db->commit_txn(txn));
        } else {
          for (size_t i = 0; i < nbatches; i++) {
            size_t keyend = (i == nbatches - 1) ? nproducts : (i + 1) * batchsize;
            void *txn = db->new_txn(txn_flags, arena, txn_buf());
            for (size_t j = i * batchsize; j < keyend; j++) {
              const bidmax_rec::key key(j);
              const bidmax_rec::value value(0.0);
              string buf0;
              bidmaxtbl->insert(txn, Encode(key), Encode(buf0, value));
            }
            if (verbose)
              cerr << "batch " << (i + 1) << "/" << nbatches << " done" << endl;
            ALWAYS_ASSERT(db->commit_txn(txn));
          }
        }
        if (verbose)
          cerr << "[INFO] finished loading BIDMAX table" << endl;
      }

    } catch (abstract_db::abstract_abort_exception &ex) {
      // shouldn't abort on loading!
      ALWAYS_ASSERT(false);
    }

  }
};

class bid_bench_runner : public bench_runner {
public:
  bid_bench_runner(abstract_db *db)
    : bench_runner(db)
  {
    open_tables["biduser"] = db->open_index("biduser", sizeof(biduser_rec));
    open_tables["bid"] = db->open_index("bid", sizeof(bid_rec));
    open_tables["bidmax"] = db->open_index("bidmax", sizeof(bidmax_rec));
  }

protected:
  virtual vector<bench_loader *>
  make_loaders()
  {
    vector<bench_loader *> ret;
    ret.push_back(new bid_loader(0, db, open_tables));
    return ret;
  }

  virtual vector<bench_worker *>
  make_workers()
  {
    fast_random r(36578943);
    vector<bench_worker *> ret;
    for (size_t i = 0; i < nthreads; i++)
      ret.push_back(
        new bid_worker(
          i, r.next(), db, open_tables,
          &barrier_a, &barrier_b));
    return ret;
  }
};

void
bid_do_test(abstract_db *db, int argc, char **argv)
{
  nusers = size_t(scale_factor * 1000.0);
  nproducts = size_t(scale_factor * 1000.0);
  ALWAYS_ASSERT(nusers > 0);
  ALWAYS_ASSERT(nproducts > 0);
  bid_bench_runner r(db);
  r.run();
}
