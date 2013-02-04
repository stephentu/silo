#include <sys/time.h>
#include <string>
#include <ctype.h>
#include <stdlib.h>

#include <stdlib.h>
#include <unistd.h>

#include <set>

#include "bench.h"
#include "tpcc.h"

// tpcc schemas
using namespace std;
using namespace util;

typedef uint uint;

class tpcc_worker_mixin {
public:
  tpcc_worker_mixin(const map<string, abstract_ordered_index *> &open_tables) :
      tbl_customer(open_tables.at("customer")),
      tbl_district(open_tables.at("district")),
      tbl_history(open_tables.at("history")),
      tbl_item(open_tables.at("item")),
      tbl_new_order(open_tables.at("new_order")),
      tbl_oorder(open_tables.at("oorder")),
      tbl_order_line(open_tables.at("order_line")),
      tbl_stock(open_tables.at("stock")),
      tbl_warehouse(open_tables.at("warehouse"))
  {
    assert(NumWarehouses() >= 1);
  }

protected:

  abstract_ordered_index *tbl_customer;
  abstract_ordered_index *tbl_district;
  abstract_ordered_index *tbl_history;
  abstract_ordered_index *tbl_item;
  abstract_ordered_index *tbl_new_order;
  abstract_ordered_index *tbl_oorder;
  abstract_ordered_index *tbl_order_line;
  abstract_ordered_index *tbl_stock;
  abstract_ordered_index *tbl_warehouse;

public:

  static inline uint32_t
  GetCurrentTimeMillis()
  {
    //struct timeval tv;
    //ALWAYS_ASSERT(gettimeofday(&tv, 0) == 0);
    //return tv.tv_sec * 1000;

    // XXX(stephentu): implement a scalable GetCurrentTimeMillis()
    // for now, we just give each core an increasing number

    static __thread uint32_t tl_hack = 0;
    return tl_hack++;
  }

  // config constants

  static inline ALWAYS_INLINE size_t
  NumWarehouses()
  {
    return (size_t) scale_factor;
  }

  static inline ALWAYS_INLINE size_t
  NumItems()
  {
    return 100000;
  }

  static inline ALWAYS_INLINE size_t
  NumDistrictsPerWarehouse()
  {
    return 10;
  }

  static inline ALWAYS_INLINE size_t
  NumCustomersPerDistrict()
  {
    return 3000;
  }

  // utils for generating random #s and strings

  static inline ALWAYS_INLINE int
  CheckBetweenInclusive(int v, int lower, int upper)
  {
    INVARIANT(v >= lower);
    INVARIANT(v <= upper);
    return v;
  }

  static inline ALWAYS_INLINE int
  RandomNumber(fast_random &r, int min, int max)
  {
    return (int) (r.next_uniform() * (max - min + 1) + min);
  }

  static inline ALWAYS_INLINE int
  NonUniformRandom(fast_random &r, int A, int C, int min, int max)
  {
    return (((RandomNumber(r, 0, A) | RandomNumber(r, min, max)) + C) % (max - min + 1)) + min;
  }

  static inline ALWAYS_INLINE int
  GetItemId(fast_random &r)
  {
    return CheckBetweenInclusive(NonUniformRandom(r, 8191, 7911, 1, NumItems()), 1, NumItems());
  }

  static inline ALWAYS_INLINE int
  GetCustomerId(fast_random &r)
  {
    return CheckBetweenInclusive(NonUniformRandom(r, 1023, 259, 1, NumCustomersPerDistrict()), 1, NumCustomersPerDistrict());
  }

  static const char *NameTokens[];

  static inline string
  GetCustomerLastName(fast_random &r, int num)
  {
    stringstream buf;
    buf << NameTokens[num / 100];
    buf << NameTokens[(num / 10) % 10];
    buf << NameTokens[num % 10];
    return buf.str();
  }

  static inline ALWAYS_INLINE string
  GetNonUniformCustomerLastNameLoad(fast_random &r)
  {
    return GetCustomerLastName(r, NonUniformRandom(r, 255, 157, 0, 999));
  }

  static inline ALWAYS_INLINE string
  GetNonUniformCustomerLastNameRun(fast_random &r)
  {
    return GetCustomerLastName(r, NonUniformRandom(r, 255, 223, 0, 999));
  }

  // following oltpbench, we really generate strings of len - 1...
  static inline string
  RandomStr(fast_random &r, uint len)
  {
    // this is a property of the oltpbench implementation...
    if (!len)
      return "";

    uint i = 0;
    stringstream buf;
    while (i < (len - 1)) {
      char c = (char) (r.next_uniform() * 128);
      // XXX(stephentu): oltpbench uses java's Character.isLetter(), which
      // is a less restrictive filter than isalnum()
      if (!isalnum(c))
        continue;
      i++;
    }
    return buf.str();
  }

  // RandomNStr() actually produces a string of length len
  static inline string
  RandomNStr(fast_random &r, uint len)
  {
    char base = '0';
    stringstream buf;
    for (uint i = 0; i < len; i++)
      buf << (char)(base + (r.next() % 10));
    return buf.str();
  }

  // should autogenerate this crap

  static inline string
  CustomerPrimaryKey(int32_t c_w_id, int32_t c_d_id, int32_t c_id)
  {
    char buf[3 * sizeof(int32_t)];
    int32_t *p = (int32_t *) &buf[0];
    *p++ = c_w_id;
    *p++ = c_d_id;
    *p++ = c_id;
    return string(&buf[0], ARRAY_NELEMS(buf));
  }

  static inline string
  DistrictPrimaryKey(int32_t d_w_id, int32_t d_id)
  {
    char buf[2 * sizeof(int32_t)];
    int32_t *p = (int32_t *) &buf[0];
    *p++ = d_w_id;
    *p++ = d_id;
    return string(&buf[0], ARRAY_NELEMS(buf));
  }

  static inline string
  ItemPrimaryKey(int32_t i_id)
  {
    char buf[1 * sizeof(int32_t)];
    int32_t *p = (int32_t *) &buf[0];
    *p++ = i_id;
    return string(&buf[0], ARRAY_NELEMS(buf));
  }

  static inline string
  NewOrderPrimaryKey(int32_t no_w_id, int32_t no_d_id, int32_t no_o_id)
  {
    char buf[3 * sizeof(int32_t)];
    int32_t *p = (int32_t *) &buf[0];
    *p++ = no_w_id;
    *p++ = no_d_id;
    *p++ = no_o_id;
    return string(&buf[0], ARRAY_NELEMS(buf));
  }

  static inline string
  OOrderPrimaryKey(int32_t o_w_id, int32_t o_d_id, int32_t o_id)
  {
    char buf[3 * sizeof(int32_t)];
    int32_t *p = (int32_t *) &buf[0];
    *p++ = o_w_id;
    *p++ = o_d_id;
    *p++ = o_id;
    return string(&buf[0], ARRAY_NELEMS(buf));
  }

  // we depart from OLTPBench here, since we don't include
  // ol_number in the primary key
  static inline string
  OrderLinePrimaryKey(int32_t ol_w_id, int32_t ol_d_id, int32_t ol_o_id) {
    char buf[4 * sizeof(int32_t)];
    int32_t *p = (int32_t *) &buf[0];
    *p++ = ol_w_id;
    *p++ = ol_d_id;
    *p++ = ol_o_id;
    return string(&buf[0], ARRAY_NELEMS(buf));
  }

  static inline string
  StockPrimaryKey(int32_t s_w_id, int32_t s_i_id)
  {
    char buf[2 * sizeof(int32_t)];
    int32_t *p = (int32_t *) &buf[0];
    *p++ = s_w_id;
    *p++ = s_i_id;
    return string(&buf[0], ARRAY_NELEMS(buf));
  }

  // artificial
  static inline string
  HistoryPrimaryKey(int32_t h_c_id, int32_t h_c_d_id, int32_t h_c_w_id,
                    int32_t h_d_id, int32_t h_w_id, uint32_t h_date)
  {
    char buf[6 * sizeof(int32_t)];
    int32_t *p = (int32_t *) &buf[0];
    *p++ = h_c_id;
    *p++ = h_c_d_id;
    *p++ = h_c_w_id;
    *p++ = h_d_id;
    *p++ = h_w_id;
    *p++ = h_date;
    return string(&buf[0], ARRAY_NELEMS(buf));
  }

  static inline string
  WarehousePrimaryKey(int32_t w_id)
  {
    char buf[1 * sizeof(int32_t)];
    int32_t *p = (int32_t *) &buf[0];
    *p++ = w_id;
    return string(&buf[0], ARRAY_NELEMS(buf));
  }
};

const char *tpcc_worker_mixin::NameTokens[] = {
 "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING",
};

class tpcc_worker : public bench_worker, public tpcc_worker_mixin {
public:
  tpcc_worker(unsigned long seed, abstract_db *db,
              const map<string, abstract_ordered_index *> &open_tables,
              spin_barrier *barrier_a, spin_barrier *barrier_b,
              uint warehouse_id)
    : bench_worker(seed, db, open_tables, barrier_a, barrier_b),
      tpcc_worker_mixin(open_tables),
      warehouse_id(warehouse_id)
  {
    assert(warehouse_id >= 1);
    assert(warehouse_id <= NumWarehouses());
  }

  void txn_new_order();

  static void
  TxnNewOrder(bench_worker *w)
  {
    static_cast<tpcc_worker *>(w)->txn_new_order();
  }

  virtual workload_desc
  get_workload()
  {
    workload_desc w;
    w.push_back(make_pair(1.00, TxnNewOrder));
    return w;
  }

private:
  uint warehouse_id;
};

class tpcc_warehouse_loader : public ndb_thread, public tpcc_worker_mixin {
public:
  tpcc_warehouse_loader(unsigned long seed,
                        abstract_db *db,
                        const map<string, abstract_ordered_index *> &open_tables)
    : tpcc_worker_mixin(open_tables), r(seed), db(db)
  {}

  virtual void
  run()
  {
    db->thread_init();
    void *txn = db->new_txn(txn_flags);
    try {
      for (uint i = 1; i <= NumWarehouses(); i++) {
        tpcc::warehouse warehouse;
        warehouse.w_id = i;
        warehouse.w_ytd = 300000;
        warehouse.w_tax = (float) RandomNumber(r, 0, 2000) / 10000.0;

        string w_name = RandomStr(r, RandomNumber(r, 6, 10));
        string w_street_1 = RandomStr(r, RandomNumber(r, 10, 20));
        string w_street_2 = RandomStr(r, RandomNumber(r, 10, 20));
        string w_city = RandomStr(r, RandomNumber(r, 10, 20));
        string w_state = RandomStr(r, 3);
        string w_zip = "123456789";

        warehouse.w_name.assign(w_name);
        warehouse.w_street_1.assign(w_street_1);
        warehouse.w_street_2.assign(w_street_2);
        warehouse.w_city.assign(w_city);
        warehouse.w_state.assign(w_state);
        warehouse.w_zip.assign(w_zip);

        string pk = WarehousePrimaryKey(i);
        tbl_warehouse->insert(txn, pk.data(), pk.size(), (const char *) &warehouse, sizeof(warehouse));
      }
      ALWAYS_ASSERT(db->commit_txn(txn));
    } catch (abstract_db::abstract_abort_exception &ex) {
      // shouldn't abort on loading!
      ALWAYS_ASSERT(false);
    }
    db->thread_end();
    cerr << "[INFO] finished loading warehouse" << endl;
  }

private:
  fast_random r;
  abstract_db *db;
};

class tpcc_item_loader : public ndb_thread, public tpcc_worker_mixin {
public:
  tpcc_item_loader(unsigned long seed,
                   abstract_db *db,
                   const map<string, abstract_ordered_index *> &open_tables)
    : tpcc_worker_mixin(open_tables), r(seed), db(db)
  {}

  virtual void
  run()
  {
    db->thread_init();
    const ssize_t bsize = db->txn_max_batch_size();
    void *txn = db->new_txn(txn_flags);
    try {
      for (uint i = 1; i <= NumItems(); i++) {
        tpcc::item item;
        item.i_id = i;
        string i_name = RandomStr(r, RandomNumber(r, 14, 24));
        item.i_name.assign(i_name);
        item.i_price = (float) RandomNumber(r, 100, 10000) / 100.0;
        int len = RandomNumber(r, 26, 50);
        if (RandomNumber(r, 1, 100) > 10) {
          string i_data = RandomStr(r, len);
          item.i_data.assign(i_data);
        } else {
          int startOriginal = RandomNumber(r, 2, (len - 8));
          string i_data = RandomStr(r, startOriginal + 1) + "ORIGINAL" + RandomStr(r, len - startOriginal - 7);
          item.i_data.assign(i_data);
        }
        item.i_im_id = RandomNumber(r, 1, 10000);
        string pk = ItemPrimaryKey(i);
        tbl_item->insert(txn, pk.data(), pk.size(), (const char *) &item, sizeof(item));

        if (bsize != -1 && !(i % bsize)) {
          ALWAYS_ASSERT(db->commit_txn(txn));
          txn = db->new_txn(txn_flags);
        }
      }
      ALWAYS_ASSERT(db->commit_txn(txn));
    } catch (abstract_db::abstract_abort_exception &ex) {
      // shouldn't abort on loading!
      ALWAYS_ASSERT(false);
    }
    db->thread_end();
    cerr << "[INFO] finished loading item" << endl;
  }

private:
  fast_random r;
  abstract_db *db;
};

class tpcc_stock_loader : public ndb_thread, public tpcc_worker_mixin {
public:
  tpcc_stock_loader(unsigned long seed,
                        abstract_db *db,
                        const map<string, abstract_ordered_index *> &open_tables)
    : tpcc_worker_mixin(open_tables), r(seed), db(db)
  {}

  virtual void
  run()
  {
    db->thread_init();
    const ssize_t bsize = db->txn_max_batch_size();
    void *txn = db->new_txn(txn_flags);
    try {
      uint cnt = 0;
      for (uint i = 1; i <= NumItems(); i++) {
        for (uint w = 1; w <= NumWarehouses(); w++, cnt++) {
          tpcc::stock stock;
          stock.s_i_id = i;
          stock.s_w_id = w;
          stock.s_quantity = RandomNumber(r, 10, 100);
          stock.s_ytd = 0;
          stock.s_order_cnt = 0;
          stock.s_remote_cnt = 0;
          int len = RandomNumber(r, 26, 50);
          if (RandomNumber(r, 1, 100) > 10) {
            string s_data = RandomStr(r, len);
            stock.s_data.assign(s_data);
          } else {
            int startOriginal = RandomNumber(r, 2, (len - 8));
            string s_data = RandomStr(r, startOriginal + 1) + "ORIGINAL" + RandomStr(r, len - startOriginal - 7);
            stock.s_data.assign(s_data);
          }
          stock.s_dist_01.assign(RandomStr(r, 24));
          stock.s_dist_02.assign(RandomStr(r, 24));
          stock.s_dist_03.assign(RandomStr(r, 24));
          stock.s_dist_04.assign(RandomStr(r, 24));
          stock.s_dist_05.assign(RandomStr(r, 24));
          stock.s_dist_06.assign(RandomStr(r, 24));
          stock.s_dist_07.assign(RandomStr(r, 24));
          stock.s_dist_08.assign(RandomStr(r, 24));
          stock.s_dist_09.assign(RandomStr(r, 24));
          stock.s_dist_10.assign(RandomStr(r, 24));

          string pk = StockPrimaryKey(w, i);
          tbl_stock->insert(txn, pk.data(), pk.size(), (const char *) &stock, sizeof(stock));

          if (bsize != -1 && !((cnt + 1) % bsize)) {
            ALWAYS_ASSERT(db->commit_txn(txn));
            txn = db->new_txn(txn_flags);
          }
        }
      }
      ALWAYS_ASSERT(db->commit_txn(txn));
    } catch (abstract_db::abstract_abort_exception &ex) {
      // shouldn't abort on loading!
      ALWAYS_ASSERT(false);
    }
    db->thread_end();
    cerr << "[INFO] finished loading stock" << endl;
  }

private:
  fast_random r;
  abstract_db *db;
};

class tpcc_district_loader : public ndb_thread, public tpcc_worker_mixin {
public:
  tpcc_district_loader(unsigned long seed,
                       abstract_db *db,
                       const map<string, abstract_ordered_index *> &open_tables)
    : tpcc_worker_mixin(open_tables), r(seed), db(db)
  {}

  virtual void
  run()
  {
    db->thread_init();
    const ssize_t bsize = db->txn_max_batch_size();
    void *txn = db->new_txn(txn_flags);
    try {
      uint cnt = 0;
      for (uint w = 1; w <= NumWarehouses(); w++) {
        for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++, cnt++) {
          tpcc::district district;
          district.d_w_id = w;
          district.d_id = d;
          district.d_ytd = 30000;
          district.d_tax = (float) (RandomNumber(r, 0, 2000) / 10000.0);
          district.d_next_o_id = 3001;
          district.d_name.assign(RandomStr(r, RandomNumber(r, 6, 10)));
          district.d_street_1.assign(RandomStr(r, RandomNumber(r, 10, 20)));
          district.d_street_2.assign(RandomStr(r, RandomNumber(r, 10, 20)));
          district.d_city.assign(RandomStr(r, RandomNumber(r, 10, 20)));
          district.d_state.assign(RandomStr(r, 3));
          district.d_zip.assign("123456789");
          string pk = DistrictPrimaryKey(w, d);
          tbl_district->insert(txn, pk.data(), pk.size(), (const char *) &district, sizeof(district));

          if (bsize != -1 && !((cnt + 1) % bsize)) {
            ALWAYS_ASSERT(db->commit_txn(txn));
            txn = db->new_txn(txn_flags);
          }
        }
      }
      ALWAYS_ASSERT(db->commit_txn(txn));
    } catch (abstract_db::abstract_abort_exception &ex) {
      // shouldn't abort on loading!
      ALWAYS_ASSERT(false);
    }
    db->thread_end();
    cerr << "[INFO] finished loading district" << endl;
  }

private:
  fast_random r;
  abstract_db *db;
};

class tpcc_customer_loader : public ndb_thread, public tpcc_worker_mixin {
public:
  tpcc_customer_loader(unsigned long seed,
                       abstract_db *db,
                       const map<string, abstract_ordered_index *> &open_tables)
    : tpcc_worker_mixin(open_tables), r(seed), db(db)
  {}

  virtual void
  run()
  {
    db->thread_init();
    const ssize_t bsize = db->txn_max_batch_size();
    void *txn = db->new_txn(txn_flags);
    try {
      uint ctr = 0;
      for (uint w = 1; w <= NumWarehouses(); w++) {
        for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
          for (uint c = 1; c <= NumCustomersPerDistrict(); c++) {
            tpcc::customer customer;

            customer.c_w_id = w;
            customer.c_d_id = d;
            customer.c_id = c;

            customer.c_discount = (float) (RandomNumber(r, 1, 5000) / 10000.0);
            if (RandomNumber(r, 1, 100) <= 10)
              customer.c_credit.assign("BC");
            else
              customer.c_credit.assign("GC");

            if (c <= 1000)
              customer.c_last.assign(GetCustomerLastName(r, c - 1));
            else
              customer.c_last.assign(GetNonUniformCustomerLastNameLoad(r));

            customer.c_first.assign(RandomStr(r, RandomNumber(r, 8, 16)));
            customer.c_credit_lim = 50000;

            customer.c_balance = -10;
            customer.c_ytd_payment = 10;
            customer.c_payment_cnt = 1;
            customer.c_delivery_cnt = 0;

            customer.c_street_1.assign(RandomStr(r, RandomNumber(r, 10, 20)));
            customer.c_street_2.assign(RandomStr(r, RandomNumber(r, 10, 20)));
            customer.c_city.assign(RandomStr(r, RandomNumber(r, 10, 20)));
            customer.c_state.assign(RandomStr(r, 3));
            customer.c_zip.assign(RandomNStr(r, 4) + "11111");
            customer.c_phone.assign(RandomNStr(r, 16));
            customer.c_since = GetCurrentTimeMillis();
            customer.c_middle.assign("OE");
            customer.c_data.assign(RandomStr(r, RandomNumber(r, 300, 500)));

            string pk = CustomerPrimaryKey(w, d, c);
            tbl_customer->insert(txn, pk.data(), pk.size(), (const char *) &customer, sizeof(customer));

            if (bsize != -1 && !((++ctr) % bsize)) {
              ALWAYS_ASSERT(db->commit_txn(txn));
              txn = db->new_txn(txn_flags);
            }

            tpcc::history history;
            history.h_c_id = c;
            history.h_c_d_id = d;
            history.h_c_w_id = w;
            history.h_d_id = d;
            history.h_w_id = w;
            history.h_date = GetCurrentTimeMillis();
            history.h_amount = 10;
            history.h_data.assign(RandomStr(r, RandomNumber(r, 10, 24)));

            string hpk = HistoryPrimaryKey(c, d, w, d, w, history.h_date);
            tbl_history->insert(txn, pk.data(), pk.size(), (const char *) &hpk, sizeof(hpk));

            if (bsize != -1 && !((++ctr) % bsize)) {
              ALWAYS_ASSERT(db->commit_txn(txn));
              txn = db->new_txn(txn_flags);
            }
          }
        }
      }
      ALWAYS_ASSERT(db->commit_txn(txn));
    } catch (abstract_db::abstract_abort_exception &ex) {
      // shouldn't abort on loading!
      ALWAYS_ASSERT(false);
    }
    db->thread_end();
    cerr << "[INFO] finished loading customer" << endl;
  }

private:
  fast_random r;
  abstract_db *db;
};

class tpcc_order_loader : public ndb_thread, public tpcc_worker_mixin {
public:
  tpcc_order_loader(unsigned long seed,
                    abstract_db *db,
                    const map<string, abstract_ordered_index *> &open_tables)
    : tpcc_worker_mixin(open_tables), r(seed), db(db)
  {}

  virtual void
  run()
  {
    db->thread_init();
    const ssize_t bsize = db->txn_max_batch_size();
    void *txn = db->new_txn(txn_flags);
    try {
      uint ctr = 0;
      for (uint w = 1; w <= NumWarehouses(); w++) {
        for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
          set<uint> c_ids_s;
          while (c_ids_s.size() != NumCustomersPerDistrict())
            c_ids_s.insert((r.next() % NumCustomersPerDistrict()) + 1);
          vector<uint> c_ids(c_ids_s.begin(), c_ids_s.end());
          for (uint c = 1; c <= NumCustomersPerDistrict(); c++) {
            tpcc::oorder oorder;
            oorder.o_id = c;
            oorder.o_w_id = w;
            oorder.o_d_id = d;
            oorder.o_c_id = c_ids[c - 1];
            if (oorder.o_id < 2101)
              oorder.o_carrier_id = RandomNumber(r, 1, 10);
            else
              oorder.o_carrier_id = 0;
            oorder.o_ol_cnt = RandomNumber(r, 5, 15);
            oorder.o_all_local = 1;
            oorder.o_entry_d = GetCurrentTimeMillis();

            string oorderPK = OOrderPrimaryKey(w, d, c);
            tbl_oorder->insert(txn, oorderPK.data(), oorderPK.size(), (const char *) &oorder, sizeof(oorder));

            if (bsize != -1 && !((++ctr) % bsize)) {
              ALWAYS_ASSERT(db->commit_txn(txn));
              txn = db->new_txn(txn_flags);
            }

            if (c >= 2101) {
              tpcc::new_order new_order;
              new_order.no_w_id = w;
              new_order.no_d_id = d;
              new_order.no_o_id = c;
              string newOrderPK = NewOrderPrimaryKey(w, d, c);
              tbl_new_order->insert(txn, newOrderPK.data(), newOrderPK.size(), (const char *) &new_order, sizeof(new_order));

              if (bsize != -1 && !((++ctr) % bsize)) {
                ALWAYS_ASSERT(db->commit_txn(txn));
                txn = db->new_txn(txn_flags);
              }
            }

            for (uint l = 1; l <= uint(oorder.o_ol_cnt); l++) {
              tpcc::order_line order_line;
              order_line.ol_w_id = w;
              order_line.ol_d_id = d;
              order_line.ol_o_id = c;
              order_line.ol_number = l; // ol_number
              order_line.ol_i_id = RandomNumber(r, 1, 100000);
              if (order_line.ol_o_id < 2101) {
                order_line.ol_delivery_d = oorder.o_entry_d;
                order_line.ol_amount = 0;
              } else {
                order_line.ol_delivery_d = 0;
                // random within [0.01 .. 9,999.99]
                order_line.ol_amount = (float) (RandomNumber(r, 1, 999999) / 100.0);
              }

              order_line.ol_supply_w_id = order_line.ol_w_id;
              order_line.ol_quantity = 5;
              order_line.ol_dist_info = RandomStr(r, 24);

              string orderLinePK = OrderLinePrimaryKey(w, d, c);
              tbl_order_line->insert(txn, orderLinePK.data(), orderLinePK.size(), (const char *) &order_line, sizeof(order_line));

              if (bsize != -1 && !((++ctr) % bsize)) {
                ALWAYS_ASSERT(db->commit_txn(txn));
                txn = db->new_txn(txn_flags);
              }
            }
          }
        }
      }
      ALWAYS_ASSERT(db->commit_txn(txn));
    } catch (abstract_db::abstract_abort_exception &ex) {
      // shouldn't abort on loading!
      ALWAYS_ASSERT(false);
    }
    db->thread_end();
    cerr << "[INFO] finished loading order" << endl;
  }

private:
  fast_random r;
  abstract_db *db;
};

void
tpcc_worker::txn_new_order()
{
  const uint districtID = RandomNumber(r, 1, 10);
  const uint customerID = GetCustomerId(r);
  const uint numItems = RandomNumber(r, 5, 15);
  vector<uint> itemIDs(numItems),
               supplierWarehouseIDs(numItems),
               orderQuantities(numItems);
  bool allLocal = true;
  for (uint i = 0; i < numItems; i++) {
    itemIDs[i] = GetItemId(r);
    if (NumWarehouses() == 1 || RandomNumber(r, 1, 100) > 1) {
      supplierWarehouseIDs[i] = warehouse_id;
    } else {
      do {
       supplierWarehouseIDs[i] = RandomNumber(r, 1, NumWarehouses());
      } while (supplierWarehouseIDs[i] == warehouse_id);
      allLocal = false;
    }
    orderQuantities[i] = RandomNumber(r, 1, 10);
  }

  // XXX(stephentu): implement rollback
  vector<char *> delete_me;
  void *txn = db->new_txn(txn_flags);
  try {
    // GetCustWhse:
    // SELECT c_discount, c_last, c_credit, w_tax
    // FROM customer, warehouse
    // WHERE w_id = ? AND c_w_id = w_id AND c_d_id = ? AND c_id = ?;

    string customerPK = CustomerPrimaryKey(warehouse_id, districtID, customerID);
    char *customer_v = 0;
    size_t customer_vlen = 0;
    if (!tbl_customer->get(txn, customerPK.data(), customerPK.size(), customer_v, customer_vlen)) {
      cerr << "error, w_id=" << warehouse_id << ", d_id=" << districtID << ", c_id=" << customerID << endl;
      ALWAYS_ASSERT(false);
    }
    ALWAYS_ASSERT(customer_vlen == sizeof(tpcc::customer));
    tpcc::customer *customer = (tpcc::customer *) customer_v;
    delete_me.push_back(customer_v);

    string warehousePK = WarehousePrimaryKey(warehouse_id);
    char *warehouse_v = 0;
    size_t warehouse_vlen = 0;
    ALWAYS_ASSERT(tbl_warehouse->get(txn, warehousePK.data(), warehousePK.size(), warehouse_v, warehouse_vlen));
    ALWAYS_ASSERT(warehouse_vlen == sizeof(tpcc::warehouse));
    tpcc::warehouse *warehouse = (tpcc::warehouse *) warehouse_v;
    delete_me.push_back(warehouse_v);

    string districtPK = DistrictPrimaryKey(warehouse_id, districtID);
    char *district_v = 0;
    size_t district_vlen = 0;
    ALWAYS_ASSERT(tbl_district->get(txn, districtPK.data(), districtPK.size(), district_v, district_vlen));
    ALWAYS_ASSERT(district_vlen == sizeof(tpcc::district));
    tpcc::district *district = (tpcc::district *) district_v;
    delete_me.push_back(district_v);

    tpcc::new_order new_order;
    new_order.no_w_id = int32_t(warehouse_id);
    new_order.no_d_id = int32_t(districtID);
    new_order.no_o_id = district->d_next_o_id;

    string newOrderPK = NewOrderPrimaryKey(warehouse_id, districtID, district->d_next_o_id);
    tbl_new_order->insert(txn, newOrderPK.data(), newOrderPK.size(), (const char *) &new_order, sizeof(new_order));

    district->d_next_o_id++;
    tbl_district->put(txn, districtPK.data(), districtPK.size(), district_v, district_vlen);

    tpcc::oorder oorder;
    oorder.o_w_id = int32_t(warehouse_id);
    oorder.o_d_id = int32_t(districtID);
    oorder.o_id = new_order.no_o_id;
    oorder.o_c_id = int32_t(customerID);
    oorder.o_carrier_id = 0; // seems to be ignored
    oorder.o_ol_cnt = int8_t(numItems);
    oorder.o_all_local = allLocal;
    oorder.o_entry_d = GetCurrentTimeMillis();

    string oorderPK = OOrderPrimaryKey(warehouse_id, districtID, new_order.no_o_id);
    tbl_oorder->insert(txn, oorderPK.data(), oorderPK.size(), (const char *) &oorder, sizeof(oorder));

    for (uint ol_number = 1; ol_number <= numItems; ol_number++) {
      uint ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
      uint ol_i_id = itemIDs[ol_number - 1];
      uint ol_quantity = orderQuantities[ol_number - 1];

      string itemPK = ItemPrimaryKey(ol_i_id);
      char *item_v = 0;
      size_t item_vlen = 0;
      ALWAYS_ASSERT(tbl_item->get(txn, itemPK.data(), itemPK.size(), item_v, item_vlen));
      ALWAYS_ASSERT(item_vlen == sizeof(tpcc::item));
      tpcc::item *item = (tpcc::item *) item_v;
      delete_me.push_back(item_v);

      string stockPK = StockPrimaryKey(warehouse_id, ol_i_id);
      char *stock_v = 0;
      size_t stock_vlen = 0;
      ALWAYS_ASSERT(tbl_stock->get(txn, stockPK.data(), stockPK.size(), stock_v, stock_vlen));
      ALWAYS_ASSERT(stock_vlen == sizeof(tpcc::stock));
      tpcc::stock *stock = (tpcc::stock *) stock_v;
      delete_me.push_back(stock_v);

      if (stock->s_quantity - ol_quantity >= 10)
        stock->s_quantity -= ol_quantity;
      else
        stock->s_quantity += -int32_t(ol_quantity) + 91;

      stock->s_ytd += ol_quantity;
      stock->s_remote_cnt += (ol_supply_w_id == warehouse_id) ? 0 : 1;

      tbl_stock->insert(txn, stockPK.data(), stockPK.size(), stock_v, stock_vlen);

      tpcc::order_line order_line;
      order_line.ol_w_id = int32_t(warehouse_id);
      order_line.ol_d_id = int32_t(districtID);
      order_line.ol_o_id = new_order.no_o_id;
      order_line.ol_number = int32_t(ol_number);
      order_line.ol_i_id = int32_t(ol_i_id);
      order_line.ol_d_id = 0; // not delivered yet
      order_line.ol_amount = float(ol_quantity) * item->i_price;
      order_line.ol_supply_w_id = int32_t(ol_supply_w_id);
      order_line.ol_quantity = int8_t(ol_quantity);

      inline_str_fixed<24> *ol_dist_info;
      switch (districtID) {
      case 1:
        ol_dist_info = &stock->s_dist_01;
        break;
      case 2:
        ol_dist_info = &stock->s_dist_02;
        break;
      case 3:
        ol_dist_info = &stock->s_dist_03;
        break;
      case 4:
        ol_dist_info = &stock->s_dist_04;
        break;
      case 5:
        ol_dist_info = &stock->s_dist_05;
        break;
      case 6:
        ol_dist_info = &stock->s_dist_06;
        break;
      case 7:
        ol_dist_info = &stock->s_dist_07;
        break;
      case 8:
        ol_dist_info = &stock->s_dist_08;
        break;
      case 9:
        ol_dist_info = &stock->s_dist_09;
        break;
      case 10:
        ol_dist_info = &stock->s_dist_10;
        break;
      default:
        ALWAYS_ASSERT(false);
        break;
      }

      memcpy(&order_line.ol_dist_info, (const char *) ol_dist_info, sizeof(order_line.ol_dist_info));

      string orderLinePK = OrderLinePrimaryKey(warehouse_id, districtID, new_order.no_o_id);
      tbl_order_line->insert(txn, orderLinePK.data(), orderLinePK.size(), (const char *) &order_line, sizeof(order_line));
    }

    if (db->commit_txn(txn))
      ntxn_commits++;
    else
      ntxn_aborts++;
  } catch (abstract_db::abstract_abort_exception &ex) {
    db->abort_txn(txn);
    ntxn_aborts++;
  }
  for (vector<char *>::iterator it = delete_me.begin();
       it != delete_me.end(); ++it)
    free(*it);
}



void
tpcc_do_test(abstract_db *db)
{
  map<string, abstract_ordered_index *> open_tables;
  open_tables["customer"]   = db->open_index("customer");
  open_tables["district"]   = db->open_index("district");
  open_tables["history"]    = db->open_index("history");
  open_tables["item"]       = db->open_index("item");
  open_tables["new_order"]  = db->open_index("new_order");
  open_tables["oorder"]     = db->open_index("oorder");
  open_tables["order_line"] = db->open_index("order_line");
  open_tables["stock"]      = db->open_index("stock");
  open_tables["warehouse"]  = db->open_index("warehouse");

  // load data
  tpcc_warehouse_loader l0(9324, db, open_tables);
  tpcc_item_loader l1(235443, db, open_tables);
  tpcc_stock_loader l2(89785943, db, open_tables);
  tpcc_district_loader l3(129856349, db, open_tables);
  tpcc_customer_loader l4(923587856425, db, open_tables);
  tpcc_order_loader l5(2343352, db, open_tables);

  ndb_thread *thds[] = { &l0, &l1, &l2, &l3, &l4, &l5 };
  for (uint i = 0; i < ARRAY_NELEMS(thds); i++)
    thds[i]->start();
  for (uint i = 0; i < ARRAY_NELEMS(thds); i++)
    thds[i]->join();

  // XXX(stephentu): don't dup code between here and ycsb.cc

  spin_barrier barrier_a(nthreads);
  spin_barrier barrier_b(1);

  fast_random r(23984543);
  vector<tpcc_worker *> workers;
  for (size_t i = 0; i < nthreads; i++)
    workers.push_back(new tpcc_worker(r.next(), db, open_tables, &barrier_a, &barrier_b, (i % tpcc_worker_mixin::NumWarehouses()) + 1));
  for (size_t i = 0; i < nthreads; i++)
    workers[i]->start();
  barrier_a.wait_for();
  barrier_b.count_down();
  timer t;
  sleep(30);
  running = false;
  __sync_synchronize();
  unsigned long elapsed = t.lap();
  size_t n_commits = 0;
  size_t n_aborts = 0;
  for (size_t i = 0; i < nthreads; i++) {
    workers[i]->join();
    n_commits += workers[i]->get_ntxn_commits();
    n_aborts += workers[i]->get_ntxn_aborts();
  }

  double agg_throughput = double(n_commits) / (double(elapsed) / 1000000.0);
  double avg_per_core_throughput = agg_throughput / double(nthreads);

  double agg_abort_rate = double(n_aborts) / (double(elapsed) / 1000000.0);
  double avg_per_core_abort_rate = agg_abort_rate / double(nthreads);

  if (verbose) {
    cerr << "agg_throughput: " << agg_throughput << " ops/sec" << endl;
    cerr << "avg_per_core_throughput: " << avg_per_core_throughput << " ops/sec/core" << endl;
    cerr << "agg_abort_rate: " << agg_abort_rate << " aborts/sec" << endl;
    cerr << "avg_per_core_abort_rate: " << avg_per_core_abort_rate << " aborts/sec/core" << endl;
  }
  cout << agg_throughput << endl;
}
