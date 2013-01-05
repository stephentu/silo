#include <unistd.h>

#include "txn_btree.h"
#include "thread.h"
#include "util.h"

#define VERBOSE(expr) ((void)0)

using namespace std;
using namespace util;

bool
txn_btree::search(transaction &t, key_type k, value_type &v)
{
  assert(!t.btree || t.btree == this);
  t.btree = this;

  // priority is
  // 1) write set
  // 2) local read set
  // 3) range set
  // 4) query underlying tree
  //
  // note (1)-(3) are served by transaction::local_search()

  if (t.local_search(k, v))
    return (bool) v;

  btree::value_type underlying_v;
  if (!underlying_btree.search(k, underlying_v)) {
    // all records exist in the system at MIN_TID with no value
    transaction::read_record_t *read_rec = &t.read_set[k];
    read_rec->t = transaction::MIN_TID;
    read_rec->r = NULL;
    read_rec->ln = NULL;
    return false;
  } else {
    transaction::logical_node *ln = (transaction::logical_node *) underlying_v;
    assert(ln);
    transaction::tid_t start_t;
    transaction::record_t r;
    if (unlikely(!ln->stable_read(t.snapshot_tid, start_t, r))) {
      t.abort();
      throw transaction_abort_exception();
    }
    transaction::read_record_t *read_rec = &t.read_set[k];
    read_rec->t = start_t;
    read_rec->r = r;
    read_rec->ln = ln;
    v = read_rec->r;
    return read_rec->r;
  }
}

bool
txn_btree::txn_search_range_callback::invoke(key_type k, value_type v)
{
  VERBOSE(cerr << "search range k: " << k << endl);
  transaction::key_range_t r(invoked ? prev_key : lower, k);
  if (!r.is_empty_range())
    t->add_absent_range(r);
  prev_key = k;
  invoked = true;
  value_type local_v = 0;
  bool local_read = t->local_search(k, local_v);
  bool ret = true;
  if (local_read && local_v)
    ret = caller_callback->invoke(k, local_v);
  map<key_type, transaction::read_record_t>::const_iterator it =
    t->read_set.find(k);
  if (it == t->read_set.end()) {
    transaction::logical_node *ln = (transaction::logical_node *) v;
    assert(ln);
    transaction::tid_t start_t;
    transaction::record_t r;
    if (unlikely(!ln->stable_read(t->snapshot_tid, start_t, r))) {
      t->abort();
      throw transaction_abort_exception();
    }
    transaction::read_record_t *read_rec = &t->read_set[k];
    read_rec->t = start_t;
    read_rec->r = r;
    read_rec->ln = ln;
    VERBOSE(cerr << "read <t=" << start_t << ", r=" << size_t(r) << "> (local_read=" << local_read << ")" << endl);
    if (!local_read && r)
      ret = caller_callback->invoke(k, r);
  }
  if (ret)
    caller_stopped = true;
  return ret;
}

bool
txn_btree::absent_range_validation_callback::invoke(key_type k, value_type v)
{
  transaction::logical_node *ln = (transaction::logical_node *) v;
  assert(ln);
  //cout << "absent_range_validation_callback: key " << k << " found ln " << intptr_t(ln) << endl;
  bool did_write = t->write_set.find(k) != t->write_set.end();
  failed_flag = did_write ? !ln->is_latest_version(0) : !ln->stable_is_latest_version(0);
  return !failed_flag;
}

void
txn_btree::search_range_call(transaction &t,
                             key_type lower,
                             const key_type *upper,
                             search_range_callback &callback)
{
  assert(!t.btree || t.btree == this);
  t.btree = this;

  // many cases to consider:
  // 1) for each logical_node returned from the scan, we need to
  //    record it in our local read set. there are several cases:
  //    A) if the logical_node corresponds to a key we have written, then
  //       we emit the version from the local write set
  //    B) if the logical_node corresponds to a key we have previous read,
  //       then we emit the previous version
  // 2) for each logical_node node *not* returned from the scan, we need
  //    to record its absense. we optimize this by recording the absense
  //    of contiguous ranges
  if (upper && *upper <= lower)
    return;
  txn_search_range_callback c(&t, lower, upper, &callback);
  underlying_btree.search_range_call(lower, upper, c);
  if (c.caller_stopped)
    return;
  if (c.invoked && c.prev_key == (key_type)-1)
    return;
  if (upper)
    t.add_absent_range(transaction::key_range_t(c.invoked ? (c.prev_key + 1): lower, *upper));
  else
    t.add_absent_range(transaction::key_range_t(c.invoked ? c.prev_key : lower));
}

void
txn_btree::insert_impl(transaction &t, key_type k, value_type v)
{
  assert(!t.btree || t.btree == this);
  t.btree = this;
  t.write_set[k] = v;
}

struct test_callback_ctr {
  test_callback_ctr(size_t *ctr) : ctr(ctr) {}


  inline bool
  operator()(txn_btree::key_type k, txn_btree::value_type v) const
  {
    (*ctr)++;
    return true;
  }
  size_t *ctr;
};

static void
test1()
{
  txn_btree btr;

  struct rec { uint64_t v; };
  rec recs[10];
  for (size_t i = 0; i < ARRAY_NELEMS(recs); i++)
    recs[i].v = 0;

  {
    transaction t;
    txn_btree::value_type v;
    ALWAYS_ASSERT(!btr.search(t, 0, v));
    btr.insert(t, 0, (txn_btree::value_type) &recs[0]);
    ALWAYS_ASSERT(btr.search(t, 0, v));
    ALWAYS_ASSERT(v == (txn_btree::value_type) &recs[0]);
    t.commit();
    VERBOSE(cout << "------" << endl);
  }

  {
    transaction t0, t1;
    txn_btree::value_type v0, v1;

    ALWAYS_ASSERT(btr.search(t0, 0, v0));
    ALWAYS_ASSERT(v0 == (txn_btree::value_type) &recs[0]);

    btr.insert(t0, 0, (txn_btree::value_type) &recs[1]);

    ALWAYS_ASSERT(btr.search(t1, 0, v1));
    ALWAYS_ASSERT(v1 == (txn_btree::value_type) &recs[0]);

    t0.commit();
    t1.commit();
    VERBOSE(cout << "------" << endl);
  }

  {
    // racy insert
    transaction t0, t1;
    txn_btree::value_type v0, v1;

    ALWAYS_ASSERT(btr.search(t0, 0, v0));
    ALWAYS_ASSERT(v0 == (txn_btree::value_type) &recs[1]);
    btr.insert(t0, 0, (txn_btree::value_type) &recs[2]);

    ALWAYS_ASSERT(btr.search(t1, 0, v1));
    ALWAYS_ASSERT(v1 == (txn_btree::value_type) &recs[1]);
    btr.insert(t1, 0, (txn_btree::value_type) &recs[3]);

    t0.commit(); // succeeds

    try {
      t1.commit(); // fails
      ALWAYS_ASSERT(false);
    } catch (transaction_abort_exception &e) {

    }
    VERBOSE(cout << "------" << endl);
  }

  {
    // racy scan
    transaction t0, t1;

    txn_btree::key_type vend = 5;
    size_t ctr = 0;
    btr.search_range(t0, 1, &vend, test_callback_ctr(&ctr));
    ALWAYS_ASSERT(ctr == 0);

    btr.insert(t1, 2, (txn_btree::value_type) &recs[4]);
    t1.commit();

    btr.insert(t0, 0, (txn_btree::value_type) &recs[0]);
    try {
      t0.commit(); // fails
      ALWAYS_ASSERT(false);
    } catch (transaction_abort_exception &e) {

    }
    VERBOSE(cout << "------" << endl);
  }

  {
    transaction t;
    txn_btree::key_type vend = 20;
    size_t ctr = 0;
    btr.search_range(t, 10, &vend, test_callback_ctr(&ctr));
    ALWAYS_ASSERT(ctr == 0);
    btr.insert(t, 15, (txn_btree::value_type) &recs[5]);
    t.commit();
    VERBOSE(cout << "------" << endl);
  }
}

static void
test2()
{
  txn_btree btr;
  for (size_t i = 0; i < 100; i++) {
    transaction t;
    btr.insert(t, 0, (txn_btree::value_type) 123);
    t.commit();
  }
}

class txn_btree_worker : public ndb_thread {
public:
  txn_btree_worker(txn_btree &btr) : btr(&btr) {}
protected:
  txn_btree *btr;
};

namespace mp_test1_ns {
  // read-modify-write test (counters)

  struct record {
    record() : v(0) {}
    uint64_t v;
  };

  const size_t niters = 1000;

  class worker : public txn_btree_worker {
  public:
    worker(txn_btree &btr) : txn_btree_worker(btr) {}
    ~worker()
    {
      for (vector<record *>::iterator it = recs.begin();
           it != recs.end(); ++it)
        delete *it;
    }
    virtual void run()
    {
      for (size_t i = 0; i < niters; i++) {
      retry:
        transaction t;
        record *rec = new record;
        recs.push_back(rec);
        try {
          txn_btree::value_type v = 0;
          if (!btr->search(t, 0, v)) {
            rec->v = 1;
          } else {
            *rec = *((record *)v);
            rec->v++;
          }
          btr->insert(t, 0, (txn_btree::value_type) rec);
          t.commit();
        } catch (transaction_abort_exception &e) {
          goto retry;
        }
      }
    }
  private:
    vector<record *> recs;
  };

}

static void
mp_test1()
{
  using namespace mp_test1_ns;

  txn_btree btr;

  worker w0(btr);
  worker w1(btr);

  w0.start(); w1.start();
  w0.join(); w1.join();

  transaction t;
  txn_btree::value_type v = 0;
  ALWAYS_ASSERT(btr.search(t, 0, v));
  ALWAYS_ASSERT( ((record *) v)->v == (niters * 2) );
  t.commit();

}

namespace mp_test2_ns {

  static const txn_btree::key_type ctr_key     = 0;
  static const txn_btree::key_type range_begin = 100;
  static const txn_btree::key_type range_end   = 200;

  static volatile bool running = true;

  class mutate_worker : public txn_btree_worker {
  public:
    mutate_worker(txn_btree &btr) : txn_btree_worker(btr), naborts(0) {}
    virtual void run()
    {
      while (running) {
        for (size_t i = range_begin; running && i < range_end; i++) {
        retry:
          transaction t;
          try {
            txn_btree::value_type v = 0, v_ctr = 0;
            ALWAYS_ASSERT(btr->search(t, ctr_key, v_ctr));
            ALWAYS_ASSERT(size_t(v_ctr) > 1);
            if (btr->search(t, i, v)) {
              ALWAYS_ASSERT(v = (txn_btree::value_type) i);
              btr->remove(t, i);
              v_ctr = (txn_btree::value_type)(size_t(v_ctr) - 1);
            } else {
              btr->insert(t, i, (txn_btree::value_type) i);
              v_ctr = (txn_btree::value_type)(size_t(v_ctr) + 1);
            }
            btr->insert(t, ctr_key, v_ctr);
            t.commit();
          } catch (transaction_abort_exception &e) {
            naborts++;
            goto retry;
          }
        }
      }
    }
    size_t naborts;
  };

  class reader_worker : public txn_btree_worker, public txn_btree::search_range_callback {
  public:
    reader_worker(txn_btree &btr) : txn_btree_worker(btr), validations(0), naborts(0), ctr(0) {}
    virtual bool invoke(txn_btree::key_type k, txn_btree::value_type v)
    {
      ctr++;
      return true;
    }
    virtual void run()
    {
      while (running) {
        try {
          transaction t;
          txn_btree::value_type v_ctr = 0;
          ALWAYS_ASSERT(btr->search(t, ctr_key, v_ctr));
          ctr = 0;
          btr->search_range_call(t, range_begin, &range_end, *this);
          t.commit();
          ALWAYS_ASSERT(ctr == size_t(v_ctr));
          validations++;
        } catch (transaction_abort_exception &e) {
          naborts++;
        }
      }
    }
    size_t validations;
    size_t naborts;
  private:
    size_t ctr;
  };
}

static void
mp_test2()
{
  using namespace mp_test2_ns;

  txn_btree btr;
  {
    transaction t;
    size_t n = 0;
    for (size_t i = range_begin; i < range_end; i++)
      if ((i % 2) == 0) {
        btr.insert(t, i, (txn_btree::value_type) i);
        n++;
      }
    btr.insert(t, ctr_key, (txn_btree::value_type) n);
    t.commit();
  }

  mutate_worker w0(btr);
  reader_worker w1(btr);

  running = true;
  w0.start(); w1.start();
  sleep(10);
  running = false;
  w0.join(); w1.join();

  cerr << "mutate naborts: " << w0.naborts << endl;
  cerr << "reader naborts: " << w1.naborts << endl;
  cerr << "validations: " << w1.validations << endl;
}

namespace mp_test3_ns {

  static const size_t amount_per_person = 100;
  static const size_t naccounts = 100;
  struct record { uint64_t v; };

  class transfer_worker : public txn_btree_worker {
  public:
    transfer_worker(txn_btree &btr, unsigned long seed) : txn_btree_worker(btr), seed(seed) {}
    ~transfer_worker()
    {
      for (vector<record *>::iterator it = recs.begin();
           it != recs.end(); ++it)
        delete *it;
    }
    virtual void run()
    {
      fast_random r(seed);
      for (size_t i = 0; i < 300000; i++) {
        record *arec_new = new record;
        record *brec_new = new record;
        recs.push_back(arec_new);
        recs.push_back(brec_new);
      retry:
        try {
          transaction t;
          txn_btree::key_type a = r.next() % naccounts;
          txn_btree::key_type b = r.next() % naccounts;
          while (unlikely(a == b))
            b = r.next() % naccounts;
          txn_btree::value_type arecv, brecv;
          ALWAYS_ASSERT(btr->search(t, a, arecv));
          ALWAYS_ASSERT(btr->search(t, b, brecv));
          record *arec = (record *) arecv;
          record *brec = (record *) brecv;
          if (arec->v == 0) {
            t.abort();
          } else {
            uint64_t xfer = (arec->v > 1) ? (r.next() % (arec->v - 1) + 1) : 1;
            arec_new->v = arec->v - xfer;
            brec_new->v = brec->v + xfer;
            btr->insert(t, a, (txn_btree::value_type) arec_new);
            btr->insert(t, b, (txn_btree::value_type) brec_new);
            t.commit();
          }
        } catch (transaction_abort_exception &e) {
          goto retry;
        }
      }
    }
  private:
    const unsigned long seed;
    vector<record *> recs;
  };

  class invariant_worker_scan : public txn_btree_worker, public txn_btree::search_range_callback {
  public:
    invariant_worker_scan(txn_btree &btr)
      : txn_btree_worker(btr), running(true), validations(0), naborts(0), sum(0) {}
    virtual void run()
    {
      while (running) {
        try {
          transaction t;
          sum = 0;
          btr->search_range_call(t, 0, NULL, *this);
          t.commit();
          ALWAYS_ASSERT(sum == (naccounts * amount_per_person));
          validations++;
        } catch (transaction_abort_exception &e) {
          naborts++;
        }
      }
    }
    virtual bool invoke(txn_btree::key_type k, txn_btree::value_type v)
    {
      sum += ((record *) v)->v;
      return true;
    }
    volatile bool running;
    size_t validations;
    size_t naborts;
    uint64_t sum;
  };

  class invariant_worker_1by1 : public txn_btree_worker {
  public:
    invariant_worker_1by1(txn_btree &btr)
      : txn_btree_worker(btr), running(true), validations(0), naborts(0) {}
    virtual void run()
    {
      while (running) {
        try {
          transaction t;
          uint64_t sum = 0;
          for (txn_btree::key_type i = 0; i < naccounts; i++) {
            txn_btree::value_type v = 0;
            ALWAYS_ASSERT(btr->search(t, i, v));
            sum += ((record *) v)->v;
          }
          t.commit();
          ALWAYS_ASSERT(sum == (naccounts * amount_per_person));
          validations++;
        } catch (transaction_abort_exception &e) {
          naborts++;
        }
      }
    }
    volatile bool running;
    size_t validations;
    size_t naborts;
  };

}

static void
mp_test3()
{
  using namespace mp_test3_ns;

  vector<record *> recs;
  txn_btree btr;
  {
    transaction t;
    for (txn_btree::key_type i = 0; i < naccounts; i++) {
      record *r = new record;
      recs.push_back(r);
      r->v = amount_per_person;
      btr.insert(t, i, (txn_btree::value_type) r);
    }
    t.commit();
  }

  transfer_worker w0(btr, 342), w1(btr, 93852), w2(btr, 23085), w3(btr, 859438989);
  invariant_worker_scan w4(btr);
  invariant_worker_1by1 w5(btr);

  w0.start(); w1.start(); w2.start(); w3.start(); w4.start(); w5.start();
  w0.join(); w1.join(); w2.join(); w3.join();
  w4.running = false; w5.running = false;
  w4.join(); w5.join();

  cerr << "scan validations: " << w4.validations << ", scan aborts: " << w4.naborts << endl;
  cerr << "1by1 validations: " << w5.validations << ", 1by1 aborts: " << w5.naborts << endl;

  for (vector<record *>::iterator it = recs.begin();
      it != recs.end(); ++it)
    delete *it;
}

void
txn_btree::Test()
{
  test1();
  test2();
  mp_test1();
  mp_test2();
  mp_test3();
}
