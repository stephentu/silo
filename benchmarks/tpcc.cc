#include <sys/time.h>
#include <string>
#include <ctype.h>
#include <stdlib.h>

#include <stdlib.h>
#include <unistd.h>

#include <set>
#include <vector>

#include "bench.h"
#include "tpcc.h"
#include "../txn.h"

// tpcc schemas
using namespace std;
using namespace util;

typedef uint uint;

class tpcc_worker_mixin {
public:
  tpcc_worker_mixin(const map<string, abstract_ordered_index *> &open_tables) :
      tbl_customer(open_tables.at("customer")),
      tbl_customer_name_idx(open_tables.at("customer_name_idx")),
      tbl_district(open_tables.at("district")),
      tbl_history(open_tables.at("history")),
      tbl_item(open_tables.at("item")),
      tbl_new_order(open_tables.at("new_order")),
      tbl_oorder(open_tables.at("oorder")),
      tbl_oorder_c_id_idx(open_tables.at("oorder_c_id_idx")),
      tbl_order_line(open_tables.at("order_line")),
      tbl_stock(open_tables.at("stock")),
      tbl_warehouse(open_tables.at("warehouse"))
  {
    assert(NumWarehouses() >= 1);
  }

protected:

  abstract_ordered_index *tbl_customer;
  abstract_ordered_index *tbl_customer_name_idx;
  abstract_ordered_index *tbl_district;
  abstract_ordered_index *tbl_history;
  abstract_ordered_index *tbl_item;
  abstract_ordered_index *tbl_new_order;
  abstract_ordered_index *tbl_oorder;
  abstract_ordered_index *tbl_oorder_c_id_idx;
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
    return CheckBetweenInclusive((int) (r.next_uniform() * (max - min + 1) + min), min, max);
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

  static string NameTokens[];

  static inline string
  GetCustomerLastName(fast_random &r, int num)
  {
    // all tokens are at most 5 chars long
    string ret;
    ret.reserve(5 * 3);
    const string &s0 = NameTokens[num / 100];
    ret.insert(ret.end(), s0.begin(), s0.end());
    const string &s1 = NameTokens[(num / 10) % 10];
    ret.insert(ret.end(), s1.begin(), s1.end());
    const string &s2 = NameTokens[num % 10];
    ret.insert(ret.end(), s2.begin(), s2.end());
    return ret;
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
    string buf(len - 1, 0);
    while (i < (len - 1)) {
      char c = (char) r.next_char();
      // XXX(stephentu): oltpbench uses java's Character.isLetter(), which
      // is a less restrictive filter than isalnum()
      if (!isalnum(c))
        continue;
      buf[i++] = c;
    }
    return buf;
  }

  // RandomNStr() actually produces a string of length len
  static inline string
  RandomNStr(fast_random &r, uint len)
  {
    const char base = '0';
    string buf(len, 0);
    for (uint i = 0; i < len; i++)
      buf[i] = (char)(base + (r.next() % 10));
    return buf;
  }

  // should autogenerate this crap

  static inline string
  CustomerPrimaryKey(int32_t c_w_id, int32_t c_d_id, int32_t c_id)
  {
    big_endian_trfm<int32_t> t;
    string buf(3 * sizeof(int32_t), 0);
    int32_t *p = (int32_t *) &buf[0];
    *p++ = t(c_w_id);
    *p++ = t(c_d_id);
    *p++ = t(c_id);
    return buf;
  }

  static inline string
  CustomerNameIdxKey(int32_t c_w_id, int32_t c_d_id,
                     const string &c_last, const string &c_first)
  {
    big_endian_trfm<int32_t> t;
    INVARIANT(c_last.size() == 16);
    INVARIANT(c_first.size() == 16);
    string buf(2 * sizeof(int32_t) + 2 * 16, 0);
    int32_t *p = (int32_t *) &buf[0];
    *p++ = t(c_w_id);
    *p++ = t(c_d_id);
    memcpy(((char *) p), c_last.data(), 16);
    memcpy(((char *) p) + 16, c_first.data(), 16);
    return buf;
  }

  static inline string
  DistrictPrimaryKey(int32_t d_w_id, int32_t d_id)
  {
    big_endian_trfm<int32_t> t;
    string buf(2 * sizeof(int32_t), 0);
    int32_t *p = (int32_t *) &buf[0];
    *p++ = t(d_w_id);
    *p++ = t(d_id);
    return buf;
  }

  static inline string
  ItemPrimaryKey(int32_t i_id)
  {
    big_endian_trfm<int32_t> t;
    string buf(1 * sizeof(int32_t), 0);
    int32_t *p = (int32_t *) &buf[0];
    *p++ = t(i_id);
    return buf;
  }

  static inline string
  NewOrderPrimaryKey(int32_t no_w_id, int32_t no_d_id, int32_t no_o_id)
  {
    big_endian_trfm<int32_t> t;
    string buf(3 * sizeof(int32_t), 0);
    int32_t *p = (int32_t *) &buf[0];
    *p++ = t(no_w_id);
    *p++ = t(no_d_id);
    *p++ = t(no_o_id);
    return buf;
  }

  static inline string
  OOrderPrimaryKey(int32_t o_w_id, int32_t o_d_id, int32_t o_id)
  {
    big_endian_trfm<int32_t> t;
    string buf(3 * sizeof(int32_t), 0);
    int32_t *p = (int32_t *) &buf[0];
    *p++ = t(o_w_id);
    *p++ = t(o_d_id);
    *p++ = t(o_id);
    return buf;
  }

  static inline string
  OOrderCIDKey(int32_t o_w_id, int32_t o_d_id, int32_t o_c_id, int32_t o_id)
  {
    big_endian_trfm<int32_t> t;
    string buf(4 * sizeof(int32_t), 0);
    int32_t *p = (int32_t *) &buf[0];
    *p++ = t(o_w_id);
    *p++ = t(o_d_id);
    *p++ = t(o_c_id);
    *p++ = t(o_id);
    return buf;
  }

  static inline string
  OrderLinePrimaryKey(int32_t ol_w_id, int32_t ol_d_id, int32_t ol_o_id, int32_t ol_number) {
    big_endian_trfm<int32_t> t;
    string buf(4 * sizeof(int32_t), 0);
    int32_t *p = (int32_t *) &buf[0];
    *p++ = t(ol_w_id);
    *p++ = t(ol_d_id);
    *p++ = t(ol_o_id);
    *p++ = t(ol_number);
    return buf;
  }

  static inline string
  StockPrimaryKey(int32_t s_w_id, int32_t s_i_id)
  {
    big_endian_trfm<int32_t> t;
    string buf(2 * sizeof(int32_t), 0);
    int32_t *p = (int32_t *) &buf[0];
    *p++ = t(s_w_id);
    *p++ = t(s_i_id);
    return buf;
  }

  // artificial
  static inline string
  HistoryPrimaryKey(int32_t h_c_id, int32_t h_c_d_id, int32_t h_c_w_id,
                    int32_t h_d_id, int32_t h_w_id, uint32_t h_date)
  {
    big_endian_trfm<int32_t> t;
    string buf(6 * sizeof(int32_t), 0);
    int32_t *p = (int32_t *) &buf[0];
    *p++ = t(h_c_id);
    *p++ = t(h_c_d_id);
    *p++ = t(h_c_w_id);
    *p++ = t(h_d_id);
    *p++ = t(h_w_id);
    *p++ = t(h_date);
    return buf;
  }

  static inline string
  WarehousePrimaryKey(int32_t w_id)
  {
    big_endian_trfm<int32_t> t;
    string buf(1 * sizeof(int32_t), 0);
    int32_t *p = (int32_t *) &buf[0];
    *p++ = t(w_id);
    return buf;
  }
};

string tpcc_worker_mixin::NameTokens[] =
  {
    string("BAR"),
    string("OUGHT"),
    string("ABLE"),
    string("PRI"),
    string("PRES"),
    string("ESE"),
    string("ANTI"),
    string("CALLY"),
    string("ATION"),
    string("EING"),
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
    INVARIANT(warehouse_id >= 1);
    INVARIANT(warehouse_id <= NumWarehouses());
    memset(&last_no_o_ids[0], 0, sizeof(last_no_o_ids));
  }

  void txn_new_order();

  static void
  TxnNewOrder(bench_worker *w)
  {
    static_cast<tpcc_worker *>(w)->txn_new_order();
  }

  void txn_delivery();

  static void
  TxnDelivery(bench_worker *w)
  {
    static_cast<tpcc_worker *>(w)->txn_delivery();
  }

  void txn_payment();

  static void
  TxnPayment(bench_worker *w)
  {
    static_cast<tpcc_worker *>(w)->txn_payment();
  }

  void txn_order_status();

  static void
  TxnOrderStatus(bench_worker *w)
  {
    static_cast<tpcc_worker *>(w)->txn_order_status();
  }

  void txn_stock_level();

  static void
  TxnStockLevel(bench_worker *w)
  {
    static_cast<tpcc_worker *>(w)->txn_stock_level();
  }

  virtual workload_desc
  get_workload()
  {
    workload_desc w;
    w.push_back(make_pair(0.45, TxnNewOrder));
    w.push_back(make_pair(0.43, TxnPayment));
    w.push_back(make_pair(0.04, TxnDelivery));
    w.push_back(make_pair(0.04, TxnOrderStatus));
    w.push_back(make_pair(0.04, TxnStockLevel));

    //w.push_back(make_pair(1.00, TxnNewOrder));
    //w.push_back(make_pair(1.00, TxnPayment));
    //w.push_back(make_pair(1.00, TxnDelivery));
    //w.push_back(make_pair(1.00, TxnOrderStatus));
    //w.push_back(make_pair(1.00, TxnStockLevel));

    //w.push_back(make_pair(0.50, TxnNewOrder));
    //w.push_back(make_pair(0.50, TxnPayment));
    return w;
  }

private:
  const uint warehouse_id;
  int32_t last_no_o_ids[10]; // XXX(stephentu): hack
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
    const bool direct_mem = db->index_supports_direct_mem_access();
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

            //cerr << "inserting (c_w_id=" << customer.c_w_id << ", c_d_id=" << customer.c_d_id << ", c_last=" << customer.c_last.str() << ")" << endl;

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
            const char *customer_p =
              tbl_customer->insert(txn, pk.data(), pk.size(),
                                   (const char *) &customer, sizeof(customer));
            ALWAYS_ASSERT(!direct_mem || customer_p);

            if (bsize != -1 && !((++ctr) % bsize)) {
              ALWAYS_ASSERT(db->commit_txn(txn));
              txn = db->new_txn(txn_flags);
            }

            // customer name index
            string customerNameKey = CustomerNameIdxKey(
                customer.c_w_id, customer.c_d_id,
                customer.c_last.str(true), customer.c_first.str(true));
            //cerr << "  insert with 2nd key: " << hexify(customerNameKey) << endl;
            if (customer_p) {
              // index structure is:
              // (c_w_id, c_d_id, c_last, c_first) -> (c_id, c_ptr)

              tpcc::customer_name_idx_mem rec;
              rec.c_id = customer.c_id;
              rec.c_ptr = (tpcc::customer *) customer_p;
              tbl_customer_name_idx->insert(txn, customerNameKey.data(), customerNameKey.size(),
                                            (const char *) &rec, sizeof(rec));
            } else {
              // index structure is:
              // (c_w_id, c_d_id, c_last, c_first) -> (c_id)

              tpcc::customer_name_idx_nomem rec;
              rec.c_id = customer.c_id;
              tbl_customer_name_idx->insert(txn, customerNameKey.data(), customerNameKey.size(),
                                            (const char *) &rec, sizeof(rec));
            }

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
            const char *oorder_ret =
              tbl_oorder->insert(txn, oorderPK.data(), oorderPK.size(),
                  (const char *) &oorder, sizeof(oorder));

            string oorderCIDPK = OOrderCIDKey(w, d, oorder.o_c_id, c);
            if (oorder_ret) {
              tpcc::oorder_c_id_idx_mem rec;
              rec.o_id = oorder.o_id;
              rec.o_ptr = (tpcc::oorder *) oorder_ret;
              tbl_oorder_c_id_idx->insert(txn, oorderCIDPK.data(),
                  oorderCIDPK.size(), (const char *) &rec, sizeof(rec));
            } else {
              tpcc::oorder_c_id_idx_nomem rec;
              rec.o_id = oorder.o_id;
              tbl_oorder_c_id_idx->insert(txn, oorderCIDPK.data(),
                  oorderCIDPK.size(), (const char *) &rec, sizeof(rec));
            }

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

              string orderLinePK = OrderLinePrimaryKey(w, d, c, l);
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

class limit_callback : public abstract_ordered_index::scan_callback {
public:
  limit_callback(ssize_t limit = -1)
    : limit(limit), n(0)
  {
    ALWAYS_ASSERT(limit == -1 || limit > 0);
  }

  virtual bool invoke(
      const char *key, size_t key_len,
      const char *value, size_t value_len)
  {
    INVARIANT(limit == -1 || n < size_t(limit));
    values.push_back(make_pair(string(key, key_len), string(value, value_len)));
    return (limit == -1) || (++n < size_t(limit));
  }

  typedef pair<string, string> kv_pair;
  vector<kv_pair> values;

  const ssize_t limit;
private:
  size_t n;
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
  const bool direct_mem = db->index_supports_direct_mem_access();
  try {
    // GetCustWhse:
    // SELECT c_discount, c_last, c_credit, w_tax
    // FROM customer, warehouse
    // WHERE w_id = ? AND c_w_id = w_id AND c_d_id = ? AND c_id = ?;

    string customerPK = CustomerPrimaryKey(warehouse_id, districtID, customerID);
    char *customer_v = 0;
    size_t customer_vlen = 0;
    ALWAYS_ASSERT(tbl_customer->get(txn, customerPK.data(), customerPK.size(), customer_v, customer_vlen));
    ALWAYS_ASSERT(customer_vlen == sizeof(tpcc::customer));
    //tpcc::customer *customer = (tpcc::customer *) customer_v;
    if (!direct_mem) delete_me.push_back(customer_v);

    string warehousePK = WarehousePrimaryKey(warehouse_id);
    char *warehouse_v = 0;
    size_t warehouse_vlen = 0;
    ALWAYS_ASSERT(tbl_warehouse->get(txn, warehousePK.data(), warehousePK.size(), warehouse_v, warehouse_vlen));
    ALWAYS_ASSERT(warehouse_vlen == sizeof(tpcc::warehouse));
    //tpcc::warehouse *warehouse = (tpcc::warehouse *) warehouse_v;
    if (!direct_mem) delete_me.push_back(warehouse_v);

    string districtPK = DistrictPrimaryKey(warehouse_id, districtID);
    char *district_v = 0;
    size_t district_vlen = 0;
    ALWAYS_ASSERT(tbl_district->get(txn, districtPK.data(), districtPK.size(), district_v, district_vlen));
    ALWAYS_ASSERT(district_vlen == sizeof(tpcc::district));
    tpcc::district *district = (tpcc::district *) district_v;
    if (!direct_mem) delete_me.push_back(district_v);

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
      if (!direct_mem) delete_me.push_back(item_v);

      string stockPK = StockPrimaryKey(warehouse_id, ol_i_id);
      char *stock_v = 0;
      size_t stock_vlen = 0;
      ALWAYS_ASSERT(tbl_stock->get(txn, stockPK.data(), stockPK.size(), stock_v, stock_vlen));
      ALWAYS_ASSERT(stock_vlen == sizeof(tpcc::stock));
      tpcc::stock *stock = (tpcc::stock *) stock_v;
      if (!direct_mem) delete_me.push_back(stock_v);

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

      string orderLinePK = OrderLinePrimaryKey(warehouse_id, districtID, new_order.no_o_id, ol_number);
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
tpcc_worker::txn_delivery()
{
  const uint o_carrier_id = RandomNumber(r, 1, NumDistrictsPerWarehouse());
  const uint32_t ts = GetCurrentTimeMillis();

  vector<char *> delete_me;
  void *txn = db->new_txn(txn_flags);
  const bool direct_mem = db->index_supports_direct_mem_access();
  try {
    for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
      string lowkey = NewOrderPrimaryKey(warehouse_id, d, last_no_o_ids[d]);
      string highkey = NewOrderPrimaryKey(warehouse_id, d, numeric_limits<int32_t>::max());
      limit_callback lone_callback(1);
      //{
      //  scoped_timer st("NewOrderScan");
        tbl_new_order->scan(txn, lowkey.data(), lowkey.size(),
                            highkey.data(), highkey.size(), true,
                            lone_callback);
      //}
      if (lone_callback.values.empty())
        continue;
      ALWAYS_ASSERT(lone_callback.values.size() == 1);
      tpcc::new_order *new_order = (tpcc::new_order *) lone_callback.values.front().second.data();
      last_no_o_ids[d] = new_order->no_o_id + 1;

      string oorderPK = OOrderPrimaryKey(warehouse_id, d, new_order->no_o_id);
      char *oorder_v;
      size_t oorder_len;
      ALWAYS_ASSERT(tbl_oorder->get(txn, oorderPK.data(), oorderPK.size(), oorder_v, oorder_len));
      if (!direct_mem) delete_me.push_back(oorder_v);
      tpcc::oorder *oorder = (tpcc::oorder *) oorder_v;
      limit_callback c(-1);
      string order_line_lowkey = OrderLinePrimaryKey(warehouse_id, d, new_order->no_o_id, 0);
      string order_line_highkey = OrderLinePrimaryKey(warehouse_id, d, new_order->no_o_id,
                                           numeric_limits<int32_t>::max());
      tbl_order_line->scan(txn, order_line_lowkey.data(), order_line_lowkey.size(),
          order_line_highkey.data(), order_line_highkey.size(), true,
          c);
      float sum = 0.0;
      for (vector<limit_callback::kv_pair>::iterator it = c.values.begin();
           it != c.values.end(); ++it) {
        tpcc::order_line *order_line = (tpcc::order_line *) it->second.data();
        sum += order_line->ol_amount;
        order_line->ol_delivery_d = ts;
        tbl_order_line->put(txn, it->first.data(), it->first.size(), it->second.data(), it->second.size());
      }

      // delete new order
      string new_orderPK = NewOrderPrimaryKey(warehouse_id, d, new_order->no_o_id);
      tbl_new_order->remove(txn, new_orderPK.data(), new_orderPK.size());

      // update oorder
      oorder->o_carrier_id = o_carrier_id;
      tbl_oorder->put(txn, oorderPK.data(), oorderPK.size(), (const char *) oorder, sizeof(*oorder));

      // update orderlines
      const uint c_id = oorder->o_c_id;
      const float ol_total = sum;

      string customerPK = CustomerPrimaryKey(warehouse_id, d, c_id);
      char *customer_v;
      size_t customer_len;
      ALWAYS_ASSERT(tbl_customer->get(
            txn, customerPK.data(), customerPK.size(),
            customer_v, customer_len));
      if (!direct_mem) delete_me.push_back(customer_v);
      tpcc::customer *customer = (tpcc::customer *) customer_v;
      customer->c_balance += ol_total;
      tbl_customer->put(txn, customerPK.data(), customerPK.size(), customer_v, customer_len);
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
tpcc_worker::txn_payment()
{
  const uint districtID = RandomNumber(r, 1, NumDistrictsPerWarehouse());
  uint customerDistrictID, customerWarehouseID;
  if (NumWarehouses() == 1 || RandomNumber(r, 1, 100) <= 85) {
    customerDistrictID = districtID;
    customerWarehouseID = warehouse_id;
  } else {
    customerDistrictID = RandomNumber(r, 1, NumDistrictsPerWarehouse());
    do {
      customerWarehouseID = RandomNumber(r, 1, NumWarehouses());
    } while (customerWarehouseID == warehouse_id);
  }
  const float paymentAmount = (float) (RandomNumber(r, 100, 500000) / 100.0);
  const uint32_t ts = GetCurrentTimeMillis();
  const bool direct_mem = db->index_supports_direct_mem_access();
  vector<char *> delete_me;
  void *txn = db->new_txn(txn_flags);
  try {
    string warehousePK = WarehousePrimaryKey(warehouse_id);
    char *warehouse_v = 0;
    size_t warehouse_vlen = 0;
    ALWAYS_ASSERT(tbl_warehouse->get(txn, warehousePK.data(), warehousePK.size(), warehouse_v, warehouse_vlen));
    ALWAYS_ASSERT(warehouse_vlen == sizeof(tpcc::warehouse));
    tpcc::warehouse *warehouse = (tpcc::warehouse *) warehouse_v;
    if (!direct_mem) delete_me.push_back(warehouse_v);
    warehouse->w_ytd += paymentAmount;
    tbl_warehouse->put(txn, warehousePK.data(), warehousePK.size(), warehouse_v, warehouse_vlen);

    string districtPK = DistrictPrimaryKey(warehouse_id, districtID);
    char *district_v = 0;
    size_t district_vlen = 0;
    ALWAYS_ASSERT(tbl_district->get(txn, districtPK.data(), districtPK.size(), district_v, district_vlen));
    ALWAYS_ASSERT(district_vlen == sizeof(tpcc::district));
    tpcc::district *district = (tpcc::district *) district_v;
    if (!direct_mem) delete_me.push_back(district_v);
    district->d_ytd += paymentAmount;
    tbl_district->put(txn, districtPK.data(), districtPK.size(), district_v, district_vlen);

    tpcc::customer *customer;
    string custdata;
    string customerPK;
    if (RandomNumber(r, 1, 100) <= 60) {
      // cust by name
      string lastname = GetNonUniformCustomerLastNameRun(r);
      lastname.resize(16);

      string lowkey = CustomerNameIdxKey(customerWarehouseID, customerDistrictID, lastname, string(16, 0));
      string highkey = CustomerNameIdxKey(customerWarehouseID, customerDistrictID, lastname, string(16, 255));
      limit_callback c(-1);
      tbl_customer_name_idx->scan(txn, lowkey.data(), lowkey.size(),
                                  highkey.data(), highkey.size(), true, c);
      //cerr << "searching for lastname: (c_w_id=" << customerWarehouseID << ", c_d_id=" << customerDistrictID << ", c_last=" << lastname << ")" << endl;
      //cerr << "  hexify lowkey: " << hexify(lowkey) << endl;
      //cerr << "  hexify highkey: " << hexify(highkey) << endl;
      ALWAYS_ASSERT(!c.values.empty());
      int index = c.values.size() / 2;
      if (c.values.size() % 2 == 0)
        index--;
      custdata = c.values[index].second;
      if (direct_mem) {
        customer = ((tpcc::customer_name_idx_mem *) custdata.data())->c_ptr;
        customerPK = CustomerPrimaryKey(customerWarehouseID, customerDistrictID, customer->c_id);
      } else {
        tpcc::customer_name_idx_nomem *rec = (tpcc::customer_name_idx_nomem *) custdata.data();
        const uint c_id = rec->c_id;
        customerPK = CustomerPrimaryKey(customerWarehouseID, customerDistrictID, c_id);
        char *customer_v = 0;
        size_t customer_vlen = 0;
        ALWAYS_ASSERT(tbl_customer->get(txn, customerPK.data(), customerPK.size(), customer_v, customer_vlen));
        ALWAYS_ASSERT(customer_vlen == sizeof(tpcc::customer));
        customer = (tpcc::customer *) customer_v;
        if (!direct_mem) delete_me.push_back(customer_v);
      }
    } else {
      // cust by ID
      const uint customerID = GetCustomerId(r);
      customerPK = CustomerPrimaryKey(customerWarehouseID, customerDistrictID, customerID);
      char *customer_v = 0;
      size_t customer_vlen = 0;
      ALWAYS_ASSERT(tbl_customer->get(txn, customerPK.data(), customerPK.size(), customer_v, customer_vlen));
      ALWAYS_ASSERT(customer_vlen == sizeof(tpcc::customer));
      customer = (tpcc::customer *) customer_v;
      if (!direct_mem) delete_me.push_back(customer_v);
    }

    customer->c_balance -= paymentAmount;
    customer->c_ytd_payment += paymentAmount;
    customer->c_payment_cnt++;
    if (strncmp(customer->c_credit.data(), "BC", 2) == 0) {
      ostringstream b;
      b << customer->c_id << " " << customer->c_d_id << " " << customer->c_w_id
        << " " << districtID << " " << warehouse_id << " " << paymentAmount
        << " | " << customer->c_data.str();
      string s = b.str();
      if (s.length() > 500)
        s.resize(500);
      customer->c_data.assign(s);
    }

    tbl_customer->put(txn, customerPK.data(), customerPK.size(), (const char *) customer, sizeof(*customer));
    string w_name = warehouse->w_name.str();
    if (w_name.size() > 10)
      w_name.resize(10);
    string d_name = district->d_name.str();
    if (d_name.size() > 10)
      d_name.resize(10);
    string h_data = w_name + "    " + d_name;

    tpcc::history history;
    history.h_c_d_id = customer->c_d_id;
    history.h_c_w_id = customer->c_w_id;
    history.h_c_id = customer->c_id;
    history.h_d_id = districtID;
    history.h_w_id = warehouse_id;
    history.h_date = ts;
    history.h_amount = paymentAmount;
    history.h_data.assign(h_data);

    string historyPK = HistoryPrimaryKey(history.h_c_id, history.h_c_d_id, history.h_c_w_id,
                                         history.h_d_id, history.h_w_id, history.h_date);
    tbl_history->insert(txn, historyPK.data(), historyPK.size(),
                        (const char *) &history, sizeof(history));

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
tpcc_worker::txn_order_status()
{
  const uint districtID = RandomNumber(r, 1, NumDistrictsPerWarehouse());
  const bool direct_mem = db->index_supports_direct_mem_access();
  vector<char *> delete_me;
  void *txn = db->new_txn(txn_flags | transaction::TXN_FLAG_READ_ONLY);
  try {

    tpcc::customer *customer;
    string custdata;
    string customerPK;
    if (RandomNumber(r, 1, 100) <= 60) {
      // cust by name
      string lastname = GetNonUniformCustomerLastNameRun(r);
      lastname.resize(16);
      string lowkey = CustomerNameIdxKey(warehouse_id, districtID, lastname, string(16, 0));
      string highkey = CustomerNameIdxKey(warehouse_id, districtID, lastname, string(16, 255));
      limit_callback c(-1);
      tbl_customer_name_idx->scan(txn, lowkey.data(), lowkey.size(),
                                  highkey.data(), highkey.size(), true, c);
      ALWAYS_ASSERT(!c.values.empty());
      int index = c.values.size() / 2;
      if (c.values.size() % 2 == 0)
        index--;
      custdata = c.values[index].second;
      if (direct_mem) {
        customer = ((tpcc::customer_name_idx_mem *) custdata.data())->c_ptr;
        customerPK = CustomerPrimaryKey(warehouse_id, districtID, customer->c_id);
      } else {
        tpcc::customer_name_idx_nomem *rec = (tpcc::customer_name_idx_nomem *) custdata.data();
        const uint c_id = rec->c_id;
        customerPK = CustomerPrimaryKey(warehouse_id, districtID, c_id);
        char *customer_v = 0;
        size_t customer_vlen = 0;
        ALWAYS_ASSERT(tbl_customer->get(txn, customerPK.data(), customerPK.size(), customer_v, customer_vlen));
        ALWAYS_ASSERT(customer_vlen == sizeof(tpcc::customer));
        customer = (tpcc::customer *) customer_v;
        if (!direct_mem) delete_me.push_back(customer_v);
      }
    } else {
      // cust by ID
      const uint customerID = GetCustomerId(r);
      customerPK = CustomerPrimaryKey(warehouse_id, districtID, customerID);
      char *customer_v = 0;
      size_t customer_vlen = 0;
      ALWAYS_ASSERT(tbl_customer->get(txn, customerPK.data(), customerPK.size(), customer_v, customer_vlen));
      ALWAYS_ASSERT(customer_vlen == sizeof(tpcc::customer));
      customer = (tpcc::customer *) customer_v;
      if (!direct_mem) delete_me.push_back(customer_v);
    }

    limit_callback c_oorder(-1);
    string oorder_lowkey = OOrderCIDKey(warehouse_id, districtID, customer->c_id, 0);
    string oorder_highkey = OOrderCIDKey(warehouse_id, districtID, customer->c_id,
        numeric_limits<int32_t>::max());
    tbl_oorder_c_id_idx->scan(txn, oorder_lowkey.data(), oorder_lowkey.size(),
        oorder_highkey.data(), oorder_highkey.size(), true, c_oorder);
    ALWAYS_ASSERT(!c_oorder.values.empty());

    uint o_id;
    if (direct_mem) {
      tpcc::oorder_c_id_idx_mem *rec =
        (tpcc::oorder_c_id_idx_mem *) c_oorder.values.back().second.data();
      o_id = rec->o_id;
    } else {
      tpcc::oorder_c_id_idx_nomem *rec =
        (tpcc::oorder_c_id_idx_nomem *) c_oorder.values.back().second.data();
      o_id = rec->o_id;
    }

    limit_callback c_order_line(-1);
    string order_line_lowkey = OrderLinePrimaryKey(warehouse_id, districtID, o_id, 0);
    string order_line_highkey = OrderLinePrimaryKey(warehouse_id, districtID, o_id,
        numeric_limits<int32_t>::max());
    tbl_order_line->scan(txn, order_line_lowkey.data(), order_line_lowkey.size(),
        order_line_highkey.data(), order_line_highkey.size(), true, c_order_line);

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
tpcc_worker::txn_stock_level()
{
  const uint threshold = RandomNumber(r, 10, 20);
  const uint districtID = RandomNumber(r, 1, NumDistrictsPerWarehouse());
  const bool direct_mem = db->index_supports_direct_mem_access();
  vector<char *> delete_me;
  void *txn = db->new_txn(txn_flags | transaction::TXN_FLAG_READ_ONLY);
  try {

    string districtPK = DistrictPrimaryKey(warehouse_id, districtID);
    char *district_v = 0;
    size_t district_vlen = 0;
    ALWAYS_ASSERT(tbl_district->get(txn, districtPK.data(), districtPK.size(), district_v, district_vlen));
    ALWAYS_ASSERT(district_vlen == sizeof(tpcc::district));
    tpcc::district *district = (tpcc::district *) district_v;
    if (!direct_mem) delete_me.push_back(district_v);

    // manual joins are fun!
    limit_callback c(-1);
    int32_t lower = district->d_next_o_id >= 20 ? (district->d_next_o_id - 20) : 0;
    string order_line_lowkey = OrderLinePrimaryKey(warehouse_id, districtID, lower, 0);
    string order_line_highkey = OrderLinePrimaryKey(warehouse_id, districtID, district->d_next_o_id, 0);
    tbl_order_line->scan(txn, order_line_lowkey.data(), order_line_lowkey.size(),
        order_line_highkey.data(), order_line_highkey.size(), true,
        c);
    set<uint> s_i_ids;
    for (vector<limit_callback::kv_pair>::iterator it = c.values.begin();
         it != c.values.end(); ++it) {
      tpcc::order_line *order_line = (tpcc::order_line *) it->second.data();
      s_i_ids.insert(order_line->ol_i_id);
    }
    set<uint> s_i_ids_distinct;
    for (set<uint>::iterator it = s_i_ids.begin();
         it != s_i_ids.end(); ++it) {
      string stockPK = StockPrimaryKey(warehouse_id, *it);
      char *stock_v = 0;
      size_t stock_vlen = 0;
      ALWAYS_ASSERT(tbl_stock->get(txn, stockPK.data(), stockPK.size(), stock_v, stock_vlen));
      ALWAYS_ASSERT(stock_vlen == sizeof(tpcc::stock));
      tpcc::stock *stock = (tpcc::stock *) stock_v;
      if (!direct_mem) delete_me.push_back(stock_v);
      if (stock->s_quantity < int(threshold))
        s_i_ids_distinct.insert(stock->s_i_id);
    }
    // s_i_ids_distinct.size() is the answer now
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

void
tpcc_do_test(abstract_db *db)
{
  map<string, abstract_ordered_index *> open_tables;
  open_tables["customer"]          = db->open_index("customer");
  open_tables["customer_name_idx"] = db->open_index("customer_name_idx");
  open_tables["district"]          = db->open_index("district");
  open_tables["history"]           = db->open_index("history");
  open_tables["item"]              = db->open_index("item");
  open_tables["new_order"]         = db->open_index("new_order");
  open_tables["oorder"]            = db->open_index("oorder");
  open_tables["oorder_c_id_idx"]   = db->open_index("oorder_c_id_idx");
  open_tables["order_line"]        = db->open_index("order_line");
  open_tables["stock"]             = db->open_index("stock");
  open_tables["warehouse"]         = db->open_index("warehouse");

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
  if (verbose) {
    for (map<string, abstract_ordered_index *>::iterator it = open_tables.begin();
         it != open_tables.end(); ++it)
      cerr << "table " << it->first << " size " << it->second->size() << endl;
  }

  spin_barrier barrier_a(nthreads);
  spin_barrier barrier_b(1);

  fast_random r(23984543);
  vector<tpcc_worker *> workers;
  for (size_t i = 0; i < nthreads; i++)
    workers.push_back(new tpcc_worker(r.next(), db, open_tables, &barrier_a, &barrier_b, (i % tpcc_worker_mixin::NumWarehouses()) + 1));
  for (size_t i = 0; i < nthreads; i++)
    workers[i]->start();

  if (verbose)
    cerr << "starting tpcc benchmark..." << endl;

  barrier_a.wait_for();
  barrier_b.count_down();
  timer t;
  sleep(runtime);
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
    vector<size_t> agg_txn_counts = workers[0]->get_txn_counts();
    for (size_t i = 1; i < nthreads; i++)
      agg_txn_counts = elemwise_sum(agg_txn_counts, workers[i]->get_txn_counts());
    cerr << "agg_throughput: " << agg_throughput << " ops/sec" << endl;
    cerr << "avg_per_core_throughput: " << avg_per_core_throughput << " ops/sec/core" << endl;
    cerr << "agg_abort_rate: " << agg_abort_rate << " aborts/sec" << endl;
    cerr << "avg_per_core_abort_rate: " << avg_per_core_abort_rate << " aborts/sec/core" << endl;
    cerr << "txn breakdown: " << format_list(agg_txn_counts.begin(), agg_txn_counts.end()) << endl;
  }
  cout << agg_throughput << endl;
}
