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

  encoder<customer> customer_enc;
  encoder<customer_name_idx_mem> customer_name_idx_mem_enc;
  encoder<customer_name_idx_nomem> customer_name_idx_nomem_enc;
  encoder<district> district_enc;
  encoder<history> history_enc;
  encoder<item> item_enc;
  encoder<new_order> new_order_enc;
  encoder<oorder> oorder_enc;
  encoder<oorder_c_id_idx_mem> oorder_c_id_idx_mem_enc;
  encoder<oorder_c_id_idx_nomem> oorder_c_id_idx_nomem_enc;
  encoder<order_line> order_line_enc;
  encoder<stock> stock_enc;
  encoder<warehouse> warehouse_enc;

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

  virtual workload_desc_vec
  get_workload() const
  {
    workload_desc_vec w;
    w.push_back(workload_desc("NewOrder", 0.45, TxnNewOrder));
    w.push_back(workload_desc("Payment", 0.43, TxnPayment));
    w.push_back(workload_desc("Delivery", 0.04, TxnDelivery));
    w.push_back(workload_desc("OrderStatus", 0.04, TxnOrderStatus));
    w.push_back(workload_desc("StockLevel", 0.04, TxnStockLevel));
    return w;
  }

private:
  const uint warehouse_id;
  int32_t last_no_o_ids[10]; // XXX(stephentu): hack
};

class tpcc_warehouse_loader : public bench_loader, public tpcc_worker_mixin {
public:
  tpcc_warehouse_loader(unsigned long seed,
                        abstract_db *db,
                        const map<string, abstract_ordered_index *> &open_tables)
    : bench_loader(seed, db, open_tables),
      tpcc_worker_mixin(open_tables)
  {}

protected:
  virtual void
  load()
  {
    void *txn = db->new_txn(txn_flags);
    const bool idx_manages_get_mem = db->index_manages_get_memory();
    uint64_t warehouse_total_sz = 0, n_warehouses = 0;
    try {
      vector<warehouse> warehouses;
      for (uint i = 1; i <= NumWarehouses(); i++) {
        warehouse warehouse;
        warehouse.w_id = i;
        warehouse.w_ytd = 300000;
        warehouse.w_tax = (float) RandomNumber(r, 0, 2000) / 10000.0;

        const string w_name = RandomStr(r, RandomNumber(r, 6, 10));
        const string w_street_1 = RandomStr(r, RandomNumber(r, 10, 20));
        const string w_street_2 = RandomStr(r, RandomNumber(r, 10, 20));
        const string w_city = RandomStr(r, RandomNumber(r, 10, 20));
        const string w_state = RandomStr(r, 3);
        const string w_zip = "123456789";

        warehouse.w_name.assign(w_name);
        warehouse.w_street_1.assign(w_street_1);
        warehouse.w_street_2.assign(w_street_2);
        warehouse.w_city.assign(w_city);
        warehouse.w_state.assign(w_state);
        warehouse.w_zip.assign(w_zip);

        const size_t sz = warehouse_enc.nbytes(&warehouse);
        warehouse_total_sz += sz;
        n_warehouses++;
        uint8_t buf[sz];
        const string pk = WarehousePrimaryKey(i);
        tbl_warehouse->insert(
            txn, pk.data(), pk.size(),
            (const char *) warehouse_enc.write(buf, &warehouse), sz);

        warehouses.push_back(warehouse);
      }
      ALWAYS_ASSERT(db->commit_txn(txn));
      txn = db->new_txn(txn_flags);
      for (uint i = 1; i <= NumWarehouses(); i++) {
        warehouse warehouse_tmp;
        const string warehousePK = WarehousePrimaryKey(i);
        char *warehouse_v = 0;
        size_t warehouse_vlen = 0;
        ALWAYS_ASSERT(tbl_warehouse->get(txn, warehousePK.data(), warehousePK.size(), warehouse_v, warehouse_vlen));
        warehouse warehouse_temp;
        const warehouse *warehouse =
          warehouse_enc.read((const uint8_t *) warehouse_v, &warehouse_temp);
        ALWAYS_ASSERT(warehouses[i - 1] == *warehouse);
        if (!idx_manages_get_mem) free(warehouse_v);
        //if (verbose)
        //  cerr << "warehouse_vlen = " << warehouse_vlen << ", sizeof(warehouse) = " << sizeof(*warehouse) << endl;
      }
      ALWAYS_ASSERT(db->commit_txn(txn));
    } catch (abstract_db::abstract_abort_exception &ex) {
      // shouldn't abort on loading!
      ALWAYS_ASSERT(false);
    }
    if (verbose) {
      cerr << "[INFO] finished loading warehouse" << endl;
      cerr << "[INFO]   * average warehouse record length: "
           << (double(warehouse_total_sz)/double(n_warehouses)) << " bytes" << endl;
    }
  }
};

class tpcc_item_loader : public bench_loader, public tpcc_worker_mixin {
public:
  tpcc_item_loader(unsigned long seed,
                   abstract_db *db,
                   const map<string, abstract_ordered_index *> &open_tables)
    : bench_loader(seed, db, open_tables),
      tpcc_worker_mixin(open_tables)
  {}

protected:
  virtual void
  load()
  {
    const ssize_t bsize = db->txn_max_batch_size();
    void *txn = db->new_txn(txn_flags);
    uint64_t total_sz = 0;
    try {
      for (uint i = 1; i <= NumItems(); i++) {
        item item;
        item.i_id = i;
        const string i_name = RandomStr(r, RandomNumber(r, 14, 24));
        item.i_name.assign(i_name);
        item.i_price = (float) RandomNumber(r, 100, 10000) / 100.0;
        const int len = RandomNumber(r, 26, 50);
        if (RandomNumber(r, 1, 100) > 10) {
          const string i_data = RandomStr(r, len);
          item.i_data.assign(i_data);
        } else {
          const int startOriginal = RandomNumber(r, 2, (len - 8));
          const string i_data = RandomStr(r, startOriginal + 1) + "ORIGINAL" + RandomStr(r, len - startOriginal - 7);
          item.i_data.assign(i_data);
        }
        item.i_im_id = RandomNumber(r, 1, 10000);

        const size_t sz = item_enc.nbytes(&item);
        total_sz += sz;
        uint8_t buf[sz];
        const string pk = ItemPrimaryKey(i);
        tbl_item->insert(
            txn, pk.data(), pk.size(),
            (const char *) item_enc.write(buf, &item), sz);

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
    if (verbose) {
      cerr << "[INFO] finished loading item" << endl;
      cerr << "[INFO]   * average item record length: "
           << (double(total_sz)/double(NumItems())) << " bytes" << endl;
    }
  }
};

class tpcc_stock_loader : public bench_loader, public tpcc_worker_mixin {
public:
  tpcc_stock_loader(unsigned long seed,
                    abstract_db *db,
                    const map<string, abstract_ordered_index *> &open_tables)
    : bench_loader(seed, db, open_tables),
      tpcc_worker_mixin(open_tables)
  {}

protected:
  virtual void
  load()
  {
    const ssize_t bsize = db->txn_max_batch_size();
    void *txn = db->new_txn(txn_flags);
    uint64_t stock_total_sz = 0, n_stocks = 0;
    try {
      uint cnt = 0;
      for (uint i = 1; i <= NumItems(); i++) {
        for (uint w = 1; w <= NumWarehouses(); w++, cnt++) {
          stock stock;
          stock.s_i_id = i;
          stock.s_w_id = w;
          stock.s_quantity = RandomNumber(r, 10, 100);
          stock.s_ytd = 0;
          stock.s_order_cnt = 0;
          stock.s_remote_cnt = 0;
          const int len = RandomNumber(r, 26, 50);
          if (RandomNumber(r, 1, 100) > 10) {
            const string s_data = RandomStr(r, len);
            stock.s_data.assign(s_data);
          } else {
            const int startOriginal = RandomNumber(r, 2, (len - 8));
            const string s_data = RandomStr(r, startOriginal + 1) + "ORIGINAL" + RandomStr(r, len - startOriginal - 7);
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

          const size_t sz = stock_enc.nbytes(&stock);
          stock_total_sz += sz;
          n_stocks++;
          uint8_t buf[sz];
          const string pk = StockPrimaryKey(w, i);
          tbl_stock->insert(
              txn, pk.data(), pk.size(),
              (const char *) stock_enc.write(buf, &stock), sz);

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
    if (verbose) {
      cerr << "[INFO] finished loading stock" << endl;
      cerr << "[INFO]   * average stock record length: "
           << (double(stock_total_sz)/double(n_stocks)) << " bytes" << endl;
    }
  }
};

class tpcc_district_loader : public bench_loader, public tpcc_worker_mixin {
public:
  tpcc_district_loader(unsigned long seed,
                       abstract_db *db,
                       const map<string, abstract_ordered_index *> &open_tables)
    : bench_loader(seed, db, open_tables),
      tpcc_worker_mixin(open_tables)
  {}

protected:
  virtual void
  load()
  {
    const ssize_t bsize = db->txn_max_batch_size();
    void *txn = db->new_txn(txn_flags);
    uint64_t district_total_sz = 0, n_districts = 0;
    try {
      uint cnt = 0;
      for (uint w = 1; w <= NumWarehouses(); w++) {
        for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++, cnt++) {
          district district;
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

          const size_t sz = district_enc.nbytes(&district);
          district_total_sz += sz;
          n_districts++;
          uint8_t buf[sz];
          const string pk = DistrictPrimaryKey(w, d);
          tbl_district->insert(
              txn, pk.data(), pk.size(),
              (const char *) district_enc.write(buf, &district), sz);

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
    if (verbose) {
      cerr << "[INFO] finished loading district" << endl;
      cerr << "[INFO]   * average district record length: "
           << (double(district_total_sz)/double(n_districts)) << " bytes" << endl;
    }
  }
};

class tpcc_customer_loader : public bench_loader, public tpcc_worker_mixin {
public:
  tpcc_customer_loader(unsigned long seed,
                       abstract_db *db,
                       const map<string, abstract_ordered_index *> &open_tables)
    : bench_loader(seed, db, open_tables),
      tpcc_worker_mixin(open_tables)
  {}

protected:
  virtual void
  load()
  {
    const ssize_t bsize = db->txn_max_batch_size();
    //const bool idx_manages_get_mem = db->index_manages_get_memory();
    const bool idx_stable_put_mem = db->index_has_stable_put_memory();
    void *txn = db->new_txn(txn_flags);
    uint64_t total_sz = 0;
    try {
      uint ctr = 0;
      for (uint w = 1; w <= NumWarehouses(); w++) {
        for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
          for (uint c = 1; c <= NumCustomersPerDistrict(); c++) {
            customer customer;

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

            const string pk = CustomerPrimaryKey(w, d, c);
            const size_t sz = customer_enc.nbytes(&customer);
            uint8_t buf[sz];
            total_sz += sz;
            const char *customer_p =
              tbl_customer->insert(txn, pk.data(), pk.size(),
                                   (const char *) customer_enc.write(buf, &customer), sz);
            ALWAYS_ASSERT(!idx_stable_put_mem || customer_p);

            if (bsize != -1 && !((++ctr) % bsize)) {
              ALWAYS_ASSERT(db->commit_txn(txn));
              txn = db->new_txn(txn_flags);
            }

            // customer name index
            const string customerNameKey = CustomerNameIdxKey(
                customer.c_w_id, customer.c_d_id,
                customer.c_last.str(true), customer.c_first.str(true));
            if (customer_p) {
              // index structure is:
              // (c_w_id, c_d_id, c_last, c_first) -> (c_id, c_ptr)

              customer_name_idx_mem rec;
              rec.c_id = customer.c_id;
              rec.c_ptr = (intptr_t) customer_p;
              const size_t sz = customer_name_idx_mem_enc.nbytes(&rec);
              uint8_t buf[sz];
              tbl_customer_name_idx->insert(
                  txn, customerNameKey.data(), customerNameKey.size(),
                  (const char *) customer_name_idx_mem_enc.write(buf, &rec), sz);
            } else {
              // index structure is:
              // (c_w_id, c_d_id, c_last, c_first) -> (c_id)

              customer_name_idx_nomem rec;
              rec.c_id = customer.c_id;
              const size_t sz = customer_name_idx_nomem_enc.nbytes(&rec);
              uint8_t buf[sz];
              tbl_customer_name_idx->insert(
                  txn, customerNameKey.data(), customerNameKey.size(),
                  (const char *) customer_name_idx_nomem_enc.write(buf, &rec), sz);
            }

            if (bsize != -1 && !((++ctr) % bsize)) {
              ALWAYS_ASSERT(db->commit_txn(txn));
              txn = db->new_txn(txn_flags);
            }

            history history;
            history.h_c_id = c;
            history.h_c_d_id = d;
            history.h_c_w_id = w;
            history.h_d_id = d;
            history.h_w_id = w;
            history.h_date = GetCurrentTimeMillis();
            history.h_amount = 10;
            history.h_data.assign(RandomStr(r, RandomNumber(r, 10, 24)));

            const string hpk = HistoryPrimaryKey(c, d, w, d, w, history.h_date);
            const size_t history_sz = history_enc.nbytes(&history);
            uint8_t history_buf[history_sz];
            tbl_history->insert(
                txn, pk.data(), pk.size(),
                (const char *) history_enc.write(history_buf, &history), history_sz);

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
    if (verbose) {
      cerr << "[INFO] finished loading customer" << endl;
      cerr << "[INFO]   * average customer record length: "
           << (double(total_sz)/double(NumWarehouses()*NumDistrictsPerWarehouse()*NumCustomersPerDistrict()))
           << " bytes " << endl;
    }
  }
};

class tpcc_order_loader : public bench_loader, public tpcc_worker_mixin {
public:
  tpcc_order_loader(unsigned long seed,
                    abstract_db *db,
                    const map<string, abstract_ordered_index *> &open_tables)
    : bench_loader(seed, db, open_tables),
      tpcc_worker_mixin(open_tables)
  {}

protected:
  virtual void
  load()
  {
    const ssize_t bsize = db->txn_max_batch_size();
    void *txn = db->new_txn(txn_flags);
    uint64_t order_line_total_sz = 0, n_order_lines = 0;
    uint64_t oorder_total_sz = 0, n_oorders = 0;
    uint64_t new_order_total_sz = 0, n_new_orders = 0;
    try {
      uint ctr = 0;
      for (uint w = 1; w <= NumWarehouses(); w++) {
        for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
          set<uint> c_ids_s;
          while (c_ids_s.size() != NumCustomersPerDistrict())
            c_ids_s.insert((r.next() % NumCustomersPerDistrict()) + 1);
          vector<uint> c_ids(c_ids_s.begin(), c_ids_s.end());
          for (uint c = 1; c <= NumCustomersPerDistrict(); c++) {
            oorder oorder;
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

            const string oorderPK = OOrderPrimaryKey(w, d, c);
            const size_t sz = oorder_enc.nbytes(&oorder);
            oorder_total_sz += sz;
            n_oorders++;
            uint8_t buf[sz];
            const char *oorder_ret =
              tbl_oorder->insert(txn, oorderPK.data(), oorderPK.size(),
                  (const char *) oorder_enc.write(buf, &oorder), sz);

            const string oorderCIDPK = OOrderCIDKey(w, d, oorder.o_c_id, c);
            if (oorder_ret) {
              oorder_c_id_idx_mem rec;
              rec.o_id = oorder.o_id;
              rec.o_ptr = (intptr_t) oorder_ret;
              const size_t sz = oorder_c_id_idx_mem_enc.nbytes(&rec);
              uint8_t buf[sz];
              tbl_oorder_c_id_idx->insert(
                  txn, oorderCIDPK.data(), oorderCIDPK.size(),
                  (const char *) oorder_c_id_idx_mem_enc.write(buf, &rec), sz);
            } else {
              oorder_c_id_idx_nomem rec;
              rec.o_id = oorder.o_id;
              const size_t sz = oorder_c_id_idx_nomem_enc.nbytes(&rec);
              uint8_t buf[sz];
              tbl_oorder_c_id_idx->insert(
                  txn, oorderCIDPK.data(), oorderCIDPK.size(),
                  (const char *) oorder_c_id_idx_nomem_enc.write(buf, &rec), sz);
            }

            if (bsize != -1 && !((++ctr) % bsize)) {
              ALWAYS_ASSERT(db->commit_txn(txn));
              txn = db->new_txn(txn_flags);
            }

            if (c >= 2101) {
              new_order new_order;
              new_order.no_w_id = w;
              new_order.no_d_id = d;
              new_order.no_o_id = c;
              const string newOrderPK = NewOrderPrimaryKey(w, d, c);
              const size_t sz = new_order_enc.nbytes(&new_order);
              new_order_total_sz += sz;
              n_new_orders++;
              uint8_t buf[sz];
              tbl_new_order->insert(
                  txn, newOrderPK.data(), newOrderPK.size(),
                  (const char *) new_order_enc.write(buf, &new_order), sz);

              if (bsize != -1 && !((++ctr) % bsize)) {
                ALWAYS_ASSERT(db->commit_txn(txn));
                txn = db->new_txn(txn_flags);
              }
            }

            for (uint l = 1; l <= uint(oorder.o_ol_cnt); l++) {
              order_line order_line;
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

              const string orderLinePK = OrderLinePrimaryKey(w, d, c, l);
              const size_t sz = order_line_enc.nbytes(&order_line);
              order_line_total_sz += sz;
              n_order_lines++;
              uint8_t buf[sz];
              tbl_order_line->insert(
                  txn, orderLinePK.data(), orderLinePK.size(),
                  (const char *) order_line_enc.write(buf, &order_line), sz);

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
    if (verbose) {
      cerr << "[INFO] finished loading order" << endl;
      cerr << "[INFO]   * average order_line record length: "
           << (double(order_line_total_sz)/double(n_order_lines)) << " bytes" << endl;
      cerr << "[INFO]   * average oorder record length: "
           << (double(oorder_total_sz)/double(n_oorders)) << " bytes" << endl;
      cerr << "[INFO]   * average new_order record length: "
           << (double(new_order_total_sz)/double(n_new_orders)) << " bytes" << endl;
    }
  }
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
  const bool idx_manages_get_mem = db->index_manages_get_memory();
  try {
    const string customerPK = CustomerPrimaryKey(warehouse_id, districtID, customerID);
    char *customer_v = 0;
    size_t customer_vlen = 0;
    ALWAYS_ASSERT(tbl_customer->get(txn, customerPK.data(), customerPK.size(), customer_v, customer_vlen));
    if (!idx_manages_get_mem) delete_me.push_back(customer_v);
    customer customer_temp;
    const customer *customer UNUSED =
      customer_enc.read((const uint8_t *) customer_v, &customer_temp);

    const string warehousePK = WarehousePrimaryKey(warehouse_id);
    char *warehouse_v = 0;
    size_t warehouse_vlen = 0;
    ALWAYS_ASSERT(tbl_warehouse->get(txn, warehousePK.data(), warehousePK.size(), warehouse_v, warehouse_vlen));
    if (!idx_manages_get_mem) delete_me.push_back(warehouse_v);
    warehouse warehouse_temp;
    const warehouse *warehouse UNUSED =
      warehouse_enc.read((const uint8_t *) warehouse_v, &warehouse_temp);

    const string districtPK = DistrictPrimaryKey(warehouse_id, districtID);
    char *district_v = 0;
    size_t district_vlen = 0;
    ALWAYS_ASSERT(tbl_district->get(txn, districtPK.data(), districtPK.size(), district_v, district_vlen));
    if (!idx_manages_get_mem) delete_me.push_back(district_v);
    district district_temp, district_new;
    const district *district =
      district_enc.read((const uint8_t *) district_v, &district_temp);

    new_order new_order;
    new_order.no_w_id = int32_t(warehouse_id);
    new_order.no_d_id = int32_t(districtID);
    new_order.no_o_id = district->d_next_o_id;

    const string newOrderPK = NewOrderPrimaryKey(warehouse_id, districtID, district->d_next_o_id);
    const size_t new_order_sz = new_order_enc.nbytes(&new_order);
    uint8_t new_order_buf[new_order_sz];
    tbl_new_order->insert(
        txn, newOrderPK.data(), newOrderPK.size(),
        (const char *) new_order_enc.write(new_order_buf, &new_order), new_order_sz);

    district_new = *district;
    district_new.d_next_o_id++;
    const size_t district_sz = district_enc.nbytes(&district_new);
    uint8_t district_buf[district_sz];
    tbl_district->put(
        txn, districtPK.data(), districtPK.size(),
        (const char *) district_enc.write(district_buf, &district_new), district_sz);

    oorder oorder;
    oorder.o_w_id = int32_t(warehouse_id);
    oorder.o_d_id = int32_t(districtID);
    oorder.o_id = new_order.no_o_id;
    oorder.o_c_id = int32_t(customerID);
    oorder.o_carrier_id = 0; // seems to be ignored
    oorder.o_ol_cnt = int8_t(numItems);
    oorder.o_all_local = allLocal;
    oorder.o_entry_d = GetCurrentTimeMillis();

    const string oorderPK = OOrderPrimaryKey(warehouse_id, districtID, new_order.no_o_id);
    const size_t oorder_sz = oorder_enc.nbytes(&oorder);
    uint8_t oorder_buf[oorder_sz];
    tbl_oorder->insert(
        txn, oorderPK.data(), oorderPK.size(),
        (const char *) oorder_enc.write(oorder_buf, &oorder), oorder_sz);

    for (uint ol_number = 1; ol_number <= numItems; ol_number++) {
      const uint ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
      const uint ol_i_id = itemIDs[ol_number - 1];
      const uint ol_quantity = orderQuantities[ol_number - 1];

      const string itemPK = ItemPrimaryKey(ol_i_id);
      char *item_v = 0;
      size_t item_vlen = 0;
      ALWAYS_ASSERT(tbl_item->get(txn, itemPK.data(), itemPK.size(), item_v, item_vlen));
      if (!idx_manages_get_mem) delete_me.push_back(item_v);
      item item_temp;
      const item *item = item_enc.read((const uint8_t *) item_v, &item_temp);

      const string stockPK = StockPrimaryKey(warehouse_id, ol_i_id);
      char *stock_v = 0;
      size_t stock_vlen = 0;
      ALWAYS_ASSERT(tbl_stock->get(txn, stockPK.data(), stockPK.size(), stock_v, stock_vlen));
      if (!idx_manages_get_mem) delete_me.push_back(stock_v);
      stock stock_temp, stock_new;
      const stock *stock = stock_enc.read((const uint8_t *) stock_v, &stock_temp);
      stock_new = *stock;

      if (stock_new.s_quantity - ol_quantity >= 10)
        stock_new.s_quantity -= ol_quantity;
      else
        stock_new.s_quantity += -int32_t(ol_quantity) + 91;

      stock_new.s_ytd += ol_quantity;
      stock_new.s_remote_cnt += (ol_supply_w_id == warehouse_id) ? 0 : 1;

      const size_t stock_sz = stock_enc.nbytes(&stock_new);
      uint8_t stock_buf[stock_sz];
      tbl_stock->put(
          txn, stockPK.data(), stockPK.size(),
          (const char *) stock_enc.write(stock_buf, &stock_new), stock_sz);

      order_line order_line;
      order_line.ol_w_id = int32_t(warehouse_id);
      order_line.ol_d_id = int32_t(districtID);
      order_line.ol_o_id = new_order.no_o_id;
      order_line.ol_number = int32_t(ol_number);
      order_line.ol_i_id = int32_t(ol_i_id);
      order_line.ol_d_id = 0; // not delivered yet
      order_line.ol_amount = float(ol_quantity) * item->i_price;
      order_line.ol_supply_w_id = int32_t(ol_supply_w_id);
      order_line.ol_quantity = int8_t(ol_quantity);

      const inline_str_fixed<24> *ol_dist_info;
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

      const string orderLinePK = OrderLinePrimaryKey(warehouse_id, districtID, new_order.no_o_id, ol_number);
      const size_t order_line_sz = order_line_enc.nbytes(&order_line);
      uint8_t order_line_buf[order_line_sz];
      tbl_order_line->insert(
          txn, orderLinePK.data(), orderLinePK.size(),
          (const char *) order_line_enc.write(order_line_buf, &order_line), order_line_sz);
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
  const bool idx_manages_get_mem = db->index_manages_get_memory();
  const bool idx_stable_put_mem = db->index_has_stable_put_memory();
  try {
    for (uint d = 1; d <= NumDistrictsPerWarehouse(); d++) {
      const string lowkey = NewOrderPrimaryKey(warehouse_id, d, last_no_o_ids[d]);
      const string highkey = NewOrderPrimaryKey(warehouse_id, d, numeric_limits<int32_t>::max());
      limit_callback new_order_c(1);
      //{
      //  scoped_timer st("NewOrderScan");
        tbl_new_order->scan(txn, lowkey.data(), lowkey.size(),
                            highkey.data(), highkey.size(), true, new_order_c);
      //}
      if (unlikely(new_order_c.values.empty()))
        continue;
      ALWAYS_ASSERT(new_order_c.values.size() == 1);
      new_order new_order_temp;
      const new_order *new_order =
        new_order_enc.read((const uint8_t *) new_order_c.values.front().second.data(), &new_order_temp);
      last_no_o_ids[d] = new_order->no_o_id + 1;

      const string oorderPK = OOrderPrimaryKey(warehouse_id, d, new_order->no_o_id);
      char *oorder_v;
      size_t oorder_len;
      ALWAYS_ASSERT(tbl_oorder->get(txn, oorderPK.data(), oorderPK.size(), oorder_v, oorder_len));
      if (!idx_manages_get_mem) delete_me.push_back(oorder_v);
      oorder oorder_temp, oorder_new;
      const oorder *oorder = oorder_enc.read((const uint8_t *) oorder_v, &oorder_temp);

      limit_callback c(-1);
      const string order_line_lowkey = OrderLinePrimaryKey(warehouse_id, d, new_order->no_o_id, 0);
      const string order_line_highkey = OrderLinePrimaryKey(warehouse_id, d, new_order->no_o_id,
                                           numeric_limits<int32_t>::max());
      tbl_order_line->scan(txn, order_line_lowkey.data(), order_line_lowkey.size(),
          order_line_highkey.data(), order_line_highkey.size(), true, c);
      float sum = 0.0;
      for (vector<limit_callback::kv_pair>::iterator it = c.values.begin();
           it != c.values.end(); ++it) {
        order_line order_line_temp, order_line_new;
        const order_line *order_line =
          order_line_enc.read((const uint8_t *) it->second.data(), &order_line_temp);
        sum += order_line->ol_amount;
        order_line_new = *order_line;
        order_line_new.ol_delivery_d = ts;
        const size_t order_line_sz = order_line_enc.nbytes(&order_line_new);
        uint8_t order_line_buf[order_line_sz];
        tbl_order_line->put(
            txn, it->first.data(), it->first.size(),
            (const char *) order_line_enc.write(order_line_buf, &order_line_new), order_line_sz);
      }

      // delete new order
      const string new_orderPK = NewOrderPrimaryKey(warehouse_id, d, new_order->no_o_id);
      tbl_new_order->remove(txn, new_orderPK.data(), new_orderPK.size());

      // update oorder
      oorder_new = *oorder;
      oorder_new.o_carrier_id = o_carrier_id;
      const size_t oorder_sz = oorder_enc.nbytes(&oorder_new);
      uint8_t oorder_buf[oorder_sz];
      tbl_oorder->put(
          txn, oorderPK.data(), oorderPK.size(),
          (const char *) oorder_enc.write(oorder_buf, &oorder_new), oorder_sz);

      // update orderlines
      const uint c_id = oorder->o_c_id;
      const float ol_total = sum;

      // update customer
      const string customerPK = CustomerPrimaryKey(warehouse_id, d, c_id);
      char *customer_v;
      size_t customer_len;
      ALWAYS_ASSERT(tbl_customer->get(
            txn, customerPK.data(), customerPK.size(),
            customer_v, customer_len));
      if (!idx_manages_get_mem) delete_me.push_back(customer_v);
      customer customer_temp, customer_new;
      const customer *customer = customer_enc.read((const uint8_t *) customer_v, &customer_temp);
      customer_new = *customer;
      customer_new.c_balance += ol_total;
      const size_t customer_sz = customer_enc.nbytes(&customer_new);
      uint8_t customer_buf[customer_sz];
      const char *customer_p =
        tbl_customer->put(
            txn, customerPK.data(), customerPK.size(),
            (const char *) customer_enc.write(customer_buf, &customer_new), customer_sz);
      ALWAYS_ASSERT(!idx_stable_put_mem || customer_p);
      if (customer_p) {
        // need to update secondary index
        const string customerNameKey = CustomerNameIdxKey(
            customer_new.c_w_id, customer_new.c_d_id,
            customer_new.c_last.str(true), customer_new.c_first.str(true));
        customer_name_idx_mem rec;
        rec.c_id = customer_new.c_id;
        rec.c_ptr = intptr_t(customer_p);
        const size_t sz = customer_name_idx_mem_enc.nbytes(&rec);
        uint8_t buf[sz];
        tbl_customer_name_idx->insert(
            txn, customerNameKey.data(), customerNameKey.size(),
            (const char *) customer_name_idx_mem_enc.write(buf, &rec), sz);
      }
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
  const bool idx_manages_get_mem = db->index_manages_get_memory();
  const bool idx_stable_put_mem = db->index_has_stable_put_memory();
  vector<char *> delete_me;
  void *txn = db->new_txn(txn_flags);
  try {
    const string warehousePK = WarehousePrimaryKey(warehouse_id);
    char *warehouse_v = 0;
    size_t warehouse_vlen = 0;
    ALWAYS_ASSERT(tbl_warehouse->get(txn, warehousePK.data(), warehousePK.size(), warehouse_v, warehouse_vlen));
    if (!idx_manages_get_mem) delete_me.push_back(warehouse_v);
    warehouse warehouse_temp, warehouse_new;
    const warehouse *warehouse = warehouse_enc.read((const uint8_t *) warehouse_v, &warehouse_temp);
    warehouse_new = *warehouse;
    warehouse_new.w_ytd += paymentAmount;
    const size_t warehouse_sz = warehouse_enc.nbytes(&warehouse_new);
    uint8_t warehouse_buf[warehouse_sz];
    tbl_warehouse->put(
        txn, warehousePK.data(), warehousePK.size(),
        (const char *) warehouse_enc.write(warehouse_buf, &warehouse_new), warehouse_sz);

    const string districtPK = DistrictPrimaryKey(warehouse_id, districtID);
    char *district_v = 0;
    size_t district_vlen = 0;
    ALWAYS_ASSERT(tbl_district->get(txn, districtPK.data(), districtPK.size(), district_v, district_vlen));
    if (!idx_manages_get_mem) delete_me.push_back(district_v);
    district district_temp, district_new;
    const district *district = district_enc.read((const uint8_t *) district_v, &district_temp);
    district_new = *district;
    district_new.d_ytd += paymentAmount;
    const size_t district_sz = district_enc.nbytes(&district_new);
    uint8_t district_buf[district_sz];
    tbl_district->put(
        txn, districtPK.data(), districtPK.size(),
        (const char *) district_enc.write(district_buf, &district_new), district_sz);

    customer customer;
    string customerPK;
    if (RandomNumber(r, 1, 100) <= 60) {
      // cust by name
      string lastname = GetNonUniformCustomerLastNameRun(r);
      lastname.resize(16);

      const string lowkey = CustomerNameIdxKey(customerWarehouseID, customerDistrictID, lastname, string(16, 0));
      const string highkey = CustomerNameIdxKey(customerWarehouseID, customerDistrictID, lastname, string(16, 255));
      limit_callback c(-1);
      tbl_customer_name_idx->scan(txn, lowkey.data(), lowkey.size(),
                                  highkey.data(), highkey.size(), true, c);
      ALWAYS_ASSERT(!c.values.empty());
      int index = c.values.size() / 2;
      if (c.values.size() % 2 == 0)
        index--;
      if (idx_stable_put_mem) {
        customer_name_idx_mem customer_name_idx_mem_temp;
        const customer_name_idx_mem *customer_name_idx_mem =
          customer_name_idx_mem_enc.read(
              (const uint8_t *) c.values[index].second.data(), &customer_name_idx_mem_temp);
        ::customer customer_temp;
        const ::customer *c = customer_enc.read((const uint8_t *) customer_name_idx_mem->c_ptr, &customer_temp);
        customer = *c;
        customerPK = CustomerPrimaryKey(customerWarehouseID, customerDistrictID, customer.c_id);
      } else {
        customer_name_idx_nomem customer_name_idx_nomem_temp;
        const customer_name_idx_nomem *customer_name_idx_nomem =
          customer_name_idx_nomem_enc.read(
              (const uint8_t *) c.values[index].second.data(), &customer_name_idx_nomem_temp);
        customerPK = CustomerPrimaryKey(customerWarehouseID, customerDistrictID, customer_name_idx_nomem->c_id);
        char *customer_v = 0;
        size_t customer_vlen = 0;
        ALWAYS_ASSERT(tbl_customer->get(txn, customerPK.data(), customerPK.size(), customer_v, customer_vlen));
        if (!idx_manages_get_mem) delete_me.push_back(customer_v);
        ::customer customer_temp;
        const ::customer *c = customer_enc.read((const uint8_t *) customer_v, &customer_temp);
        customer = *c;
      }
    } else {
      // cust by ID
      const uint customerID = GetCustomerId(r);
      customerPK = CustomerPrimaryKey(customerWarehouseID, customerDistrictID, customerID);
      char *customer_v = 0;
      size_t customer_vlen = 0;
      ALWAYS_ASSERT(tbl_customer->get(txn, customerPK.data(), customerPK.size(), customer_v, customer_vlen));
      if (!idx_manages_get_mem) delete_me.push_back(customer_v);
      ::customer customer_temp;
      const ::customer *c = customer_enc.read((const uint8_t *) customer_v, &customer_temp);
      customer = *c;
    }

    customer.c_balance -= paymentAmount;
    customer.c_ytd_payment += paymentAmount;
    customer.c_payment_cnt++;
    if (strncmp(customer.c_credit.data(), "BC", 2) == 0) {
      ostringstream b;
      b << customer.c_id << " " << customer.c_d_id << " " << customer.c_w_id
        << " " << districtID << " " << warehouse_id << " " << paymentAmount
        << " | " << customer.c_data.str();
      string s = b.str();
      if (s.length() > 500)
        s.resize(500);
      customer.c_data.assign(s);
    }

    const size_t customer_sz = customer_enc.nbytes(&customer);
    uint8_t customer_buf[customer_sz];
    const char *customer_p =
      tbl_customer->put(
          txn, customerPK.data(), customerPK.size(),
          (const char *) customer_enc.write(customer_buf, &customer), customer_sz);
    ALWAYS_ASSERT(!idx_stable_put_mem || customer_p);
    if (customer_p) {
      // need to update secondary index
      const string customerNameKey = CustomerNameIdxKey(
          customer.c_w_id, customer.c_d_id,
          customer.c_last.str(true), customer.c_first.str(true));
      customer_name_idx_mem rec;
      rec.c_id = customer.c_id;
      rec.c_ptr = intptr_t(customer_p);
      const size_t sz = customer_name_idx_mem_enc.nbytes(&rec);
      uint8_t buf[sz];
      tbl_customer_name_idx->insert(
          txn, customerNameKey.data(), customerNameKey.size(),
            (const char *) customer_name_idx_mem_enc.write(buf, &rec), sz);
    }

    string w_name = warehouse->w_name.str();
    if (w_name.size() > 10)
      w_name.resize(10);
    string d_name = district->d_name.str();
    if (d_name.size() > 10)
      d_name.resize(10);
    const string h_data = w_name + "    " + d_name;

    history history;
    history.h_c_d_id = customer.c_d_id;
    history.h_c_w_id = customer.c_w_id;
    history.h_c_id = customer.c_id;
    history.h_d_id = districtID;
    history.h_w_id = warehouse_id;
    history.h_date = ts;
    history.h_amount = paymentAmount;
    history.h_data.assign(h_data);

    const string historyPK = HistoryPrimaryKey(history.h_c_id, history.h_c_d_id, history.h_c_w_id,
                                               history.h_d_id, history.h_w_id, history.h_date);
    const size_t history_sz = history_enc.nbytes(&history);
    uint8_t history_buf[history_sz];
    tbl_history->insert(txn, historyPK.data(), historyPK.size(),
                        (const char *) history_enc.write(history_buf, &history), history_sz);

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
  const bool idx_manages_get_mem = db->index_manages_get_memory();
  const bool idx_stable_put_mem = db->index_has_stable_put_memory();
  vector<char *> delete_me;
  void *txn = db->new_txn(txn_flags | transaction::TXN_FLAG_READ_ONLY);
  try {

    customer customer;
    string customerPK;
    if (RandomNumber(r, 1, 100) <= 60) {
      // cust by name
      string lastname = GetNonUniformCustomerLastNameRun(r);
      lastname.resize(16);

      const string lowkey = CustomerNameIdxKey(warehouse_id, districtID, lastname, string(16, 0));
      const string highkey = CustomerNameIdxKey(warehouse_id, districtID, lastname, string(16, 255));
      limit_callback c(-1);
      tbl_customer_name_idx->scan(txn, lowkey.data(), lowkey.size(),
                                  highkey.data(), highkey.size(), true, c);
      ALWAYS_ASSERT(!c.values.empty());
      int index = c.values.size() / 2;
      if (c.values.size() % 2 == 0)
        index--;
      if (idx_stable_put_mem) {
        customer_name_idx_mem customer_name_idx_mem_temp;
        const customer_name_idx_mem *customer_name_idx_mem =
          customer_name_idx_mem_enc.read(
              (const uint8_t *) c.values[index].second.data(), &customer_name_idx_mem_temp);
        ::customer customer_temp;
        const ::customer *c = customer_enc.read((const uint8_t *) customer_name_idx_mem->c_ptr, &customer_temp);
        customer = *c;
        customerPK = CustomerPrimaryKey(warehouse_id, districtID, customer.c_id);
      } else {
        customer_name_idx_nomem customer_name_idx_nomem_temp;
        const customer_name_idx_nomem *customer_name_idx_nomem =
          customer_name_idx_nomem_enc.read(
              (const uint8_t *) c.values[index].second.data(), &customer_name_idx_nomem_temp);
        customerPK = CustomerPrimaryKey(warehouse_id, districtID, customer_name_idx_nomem->c_id);
        char *customer_v = 0;
        size_t customer_vlen = 0;
        ALWAYS_ASSERT(tbl_customer->get(txn, customerPK.data(), customerPK.size(), customer_v, customer_vlen));
        if (!idx_manages_get_mem) delete_me.push_back(customer_v);
        ::customer customer_temp;
        const ::customer *c = customer_enc.read((const uint8_t *) customer_v, &customer_temp);
        customer = *c;
      }
    } else {
      // cust by ID
      const uint customerID = GetCustomerId(r);
      customerPK = CustomerPrimaryKey(warehouse_id, districtID, customerID);
      char *customer_v = 0;
      size_t customer_vlen = 0;
      ALWAYS_ASSERT(tbl_customer->get(txn, customerPK.data(), customerPK.size(), customer_v, customer_vlen));
      if (!idx_manages_get_mem) delete_me.push_back(customer_v);
      ::customer customer_temp;
      const ::customer *c = customer_enc.read((const uint8_t *) customer_v, &customer_temp);
      customer = *c;
    }

    limit_callback c_oorder(-1);
    const string oorder_lowkey = OOrderCIDKey(warehouse_id, districtID, customer.c_id, 0);
    const string oorder_highkey = OOrderCIDKey(warehouse_id, districtID, customer.c_id, numeric_limits<int32_t>::max());
    tbl_oorder_c_id_idx->scan(txn, oorder_lowkey.data(), oorder_lowkey.size(),
        oorder_highkey.data(), oorder_highkey.size(), true, c_oorder);
    //if (unlikely(c_oorder.values.empty())) {
    //  cerr << "oorder_lokey: " << hexify(oorder_lowkey) << endl;
    //  cerr << "oorder_hikey: " << hexify(oorder_highkey) << endl;
    //  db->print_txn_debug(txn);
    //}
    ALWAYS_ASSERT(!c_oorder.values.empty());

    uint o_id;
    if (idx_stable_put_mem) {
      oorder_c_id_idx_mem oorder_c_id_idx_mem_temp;
      const oorder_c_id_idx_mem *oorder_c_id_idx_mem =
        oorder_c_id_idx_mem_enc.read(
            (const uint8_t *) c_oorder.values.back().second.data(), &oorder_c_id_idx_mem_temp);
      o_id = oorder_c_id_idx_mem->o_id;
    } else {
      oorder_c_id_idx_nomem oorder_c_id_idx_nomem_temp;
      const oorder_c_id_idx_nomem *oorder_c_id_idx_nomem =
        oorder_c_id_idx_nomem_enc.read(
            (const uint8_t *) c_oorder.values.back().second.data(), &oorder_c_id_idx_nomem_temp);
      o_id = oorder_c_id_idx_nomem->o_id;
    }

    limit_callback c_order_line(-1);
    const string order_line_lowkey = OrderLinePrimaryKey(warehouse_id, districtID, o_id, 0);
    const string order_line_highkey = OrderLinePrimaryKey(warehouse_id, districtID, o_id,
        numeric_limits<int32_t>::max());
    tbl_order_line->scan(txn, order_line_lowkey.data(), order_line_lowkey.size(),
        order_line_highkey.data(), order_line_highkey.size(), true, c_order_line);
    for (size_t i = 0; i < c_order_line.values.size(); i++) {
      order_line order_line_temp;
      const order_line *order_line UNUSED =
        order_line_enc.read((const uint8_t *) c_order_line.values[i].second.data(), &order_line_temp);
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
tpcc_worker::txn_stock_level()
{
  const uint threshold = RandomNumber(r, 10, 20);
  const uint districtID = RandomNumber(r, 1, NumDistrictsPerWarehouse());
  const bool idx_manages_get_mem = db->index_manages_get_memory();
  vector<char *> delete_me;
  void *txn = db->new_txn(txn_flags | transaction::TXN_FLAG_READ_ONLY);
  try {

    const string districtPK = DistrictPrimaryKey(warehouse_id, districtID);
    char *district_v = 0;
    size_t district_vlen = 0;
    ALWAYS_ASSERT(tbl_district->get(txn, districtPK.data(), districtPK.size(), district_v, district_vlen));
    if (!idx_manages_get_mem) delete_me.push_back(district_v);
    district district_temp;
    const district *district =
      district_enc.read((const uint8_t *) district_v, &district_temp);

    // manual joins are fun!
    limit_callback c(-1);
    int32_t lower = district->d_next_o_id >= 20 ? (district->d_next_o_id - 20) : 0;
    const string order_line_lowkey = OrderLinePrimaryKey(warehouse_id, districtID, lower, 0);
    const string order_line_highkey = OrderLinePrimaryKey(warehouse_id, districtID, district->d_next_o_id, 0);
    tbl_order_line->scan(txn, order_line_lowkey.data(), order_line_lowkey.size(),
        order_line_highkey.data(), order_line_highkey.size(), true, c);
    set<uint> s_i_ids;
    for (vector<limit_callback::kv_pair>::iterator it = c.values.begin();
         it != c.values.end(); ++it) {
      order_line order_line_temp;
      const order_line *order_line =
        order_line_enc.read((const uint8_t *) it->second.data(), &order_line_temp);
      s_i_ids.insert(order_line->ol_i_id);
    }
    set<uint> s_i_ids_distinct;
    for (set<uint>::iterator it = s_i_ids.begin();
         it != s_i_ids.end(); ++it) {
      const string stockPK = StockPrimaryKey(warehouse_id, *it);
      char *stock_v = 0;
      size_t stock_vlen = 0;
      ALWAYS_ASSERT(tbl_stock->get(txn, stockPK.data(), stockPK.size(), stock_v, stock_vlen));
      if (!idx_manages_get_mem) delete_me.push_back(stock_v);
      stock stock_temp;
      const stock *stock = stock_enc.read((const uint8_t *) stock_v, &stock_temp);
      if (stock->s_quantity < int(threshold))
        s_i_ids_distinct.insert(stock->s_i_id);
    }
    // NB(stephentu): s_i_ids_distinct.size() is the computed result of this txn
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

class tpcc_bench_runner : public bench_runner {
public:
  tpcc_bench_runner(abstract_db *db)
    : bench_runner(db)
  {
    const bool idx_stable_put_mem = db->index_has_stable_put_memory();
    open_tables["customer"]          = db->open_index("customer", sizeof(customer));
    open_tables["customer_name_idx"] = db->open_index("customer_name_idx", idx_stable_put_mem ?
        sizeof(customer_name_idx_mem) : sizeof(customer_name_idx_nomem));
    open_tables["district"]          = db->open_index("district", sizeof(district));
    open_tables["history"]           = db->open_index("history", sizeof(history));
    open_tables["item"]              = db->open_index("item", sizeof(item));
    open_tables["new_order"]         = db->open_index("new_order", sizeof(new_order));
    open_tables["oorder"]            = db->open_index("oorder", sizeof(oorder));
    open_tables["oorder_c_id_idx"]   = db->open_index("oorder_c_id_idx", idx_stable_put_mem ?
        sizeof(oorder_c_id_idx_mem) : sizeof(oorder_c_id_idx_nomem));
    open_tables["order_line"]        = db->open_index("order_line", sizeof(order_line));
    open_tables["stock"]             = db->open_index("stock", sizeof(stock));
    open_tables["warehouse"]         = db->open_index("warehouse", sizeof(warehouse));
  }

protected:
  virtual vector<bench_loader *>
  make_loaders()
  {
    vector<bench_loader *> ret;
    ret.push_back(new tpcc_warehouse_loader(9324, db, open_tables));
    ret.push_back(new tpcc_item_loader(235443, db, open_tables));
    ret.push_back(new tpcc_stock_loader(89785943, db, open_tables));
    ret.push_back(new tpcc_district_loader(129856349, db, open_tables));
    ret.push_back(new tpcc_customer_loader(923587856425, db, open_tables));
    ret.push_back(new tpcc_order_loader(2343352, db, open_tables));
    return ret;
  }

  virtual vector<bench_worker *>
  make_workers()
  {
    fast_random r(23984543);
    vector<bench_worker *> ret;
    for (size_t i = 0; i < nthreads; i++)
      ret.push_back(
        new tpcc_worker(
          r.next(), db, open_tables,
          &barrier_a, &barrier_b,
          (i % tpcc_worker_mixin::NumWarehouses()) + 1));
    return ret;
  }
};

void
tpcc_do_test(abstract_db *db)
{
  tpcc_bench_runner r(db);
  r.run();
}
