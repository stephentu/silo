#include <sys/time.h>
#include <string>
#include <ctype.h>

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

  static inline uint32_t
  GetCurrentTimeMillis()
  {
    struct timeval tv;
    ALWAYS_ASSERT(gettimeofday(&tv, 0) == 0);
    return tv.tv_sec * 1000;
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

  static inline int
  RandomNumber(fast_random &r, int min, int max)
  {
    return (int) (r.next_uniform() * (max - min + 1) + min);
  }

  static inline int
  NonUniformRandom(fast_random &r, int A, int C, int min, int max)
  {
    return (((RandomNumber(r, 0, A) | RandomNumber(r, min, max)) + C) % (max - min + 1)) + min;
  }

  static inline int
  GetItemId(fast_random &r)
  {
    return NonUniformRandom(r, 8191, 7911, 1, 100000);
  }

  static inline int
  GetCustomerId(fast_random &r)
  {
    return NonUniformRandom(r, 1023, 259, 1, 3000);
  }

  // following oltpbench, we really generate strings of len - 1...
  static inline string
  RandomStr(fast_random &r, int len)
  {
    assert(len >= 1);
    int i = 0;
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

  static inline string
  WarehousePrimaryKey(int32_t w_id)
  {
    char buf[1 * sizeof(int32_t)];
    int32_t *p = (int32_t *) &buf[0];
    *p++ = w_id;
    return string(&buf[0], ARRAY_NELEMS(buf));
  }
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
  }

  void txn_new_order();

private:
  uint warehouse_id;
};

class tpcc_warehouse_loader : public tpcc_worker_mixin {
public:
  tpcc_warehouse_loader(unsigned long seed,
                        abstract_db *db,
                        const map<string, abstract_ordered_index *> &open_tables)
    : tpcc_worker_mixin(open_tables), r(seed), db(db)
  {}

  virtual void
  run()
  {
    void *txn = db->new_txn(txn_flags);
    try {
      for (uint i = 1; i < NumWarehouses(); i++) {
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
        tbl_warehouse->put(txn, pk.data(), pk.size(), (const char *) &warehouse, sizeof(warehouse));
      }
      db->commit_txn(txn);
    } catch (abstract_db::abstract_abort_exception &ex) {
      // shouldn't abort on loading!
      ALWAYS_ASSERT(false);
    }
  }

private:
  fast_random r;
  abstract_db *db;
};

class tpcc_item_loader : public tpcc_worker_mixin {
public:
  tpcc_item_loader(unsigned long seed,
                        abstract_db *db,
                        const map<string, abstract_ordered_index *> &open_tables)
    : tpcc_worker_mixin(open_tables), r(seed), db(db)
  {}

  virtual void
  run()
  {
    void *txn = db->new_txn(txn_flags);
    try {
      for (uint i = 1; i < NumItems(); i++) {
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
          string i_data = RandomStr(r, startOriginal - 1) + "ORIGINAL" + RandomStr(r, len - startOriginal - 9);
          item.i_data.assign(i_data);
        }
        item.i_im_id = RandomNumber(r, 1, 10000);
        string pk = ItemPrimaryKey(i);
        tbl_item->put(txn, pk.data(), pk.size(), (const char *) &item, sizeof(item));
      }
      db->commit_txn(txn);
    } catch (abstract_db::abstract_abort_exception &ex) {
      // shouldn't abort on loading!
      ALWAYS_ASSERT(false);
    }
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
  void *txn = db->new_txn(txn_flags);
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
    tpcc::customer *customer = (tpcc::customer *) customer_v;

    string warehousePK = WarehousePrimaryKey(warehouse_id);
    char *warehouse_v = 0;
    size_t warehouse_vlen = 0;
    ALWAYS_ASSERT(tbl_warehouse->get(txn, warehousePK.data(), warehousePK.size(), warehouse_v, warehouse_vlen));
    ALWAYS_ASSERT(warehouse_vlen == sizeof(tpcc::warehouse));
    tpcc::warehouse *warehouse = (tpcc::warehouse *) warehouse_v;

    string districtPK = DistrictPrimaryKey(warehouse_id, districtID);
    char *district_v = 0;
    size_t district_vlen = 0;
    ALWAYS_ASSERT(tbl_district->get(txn, districtPK.data(), districtPK.size(), district_v, district_vlen));
    ALWAYS_ASSERT(district_vlen == sizeof(tpcc::district));
    tpcc::district *district = (tpcc::district *) district_v;

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

      string stockPK = StockPrimaryKey(warehouse_id, ol_i_id);
      char *stock_v = 0;
      size_t stock_vlen = 0;
      ALWAYS_ASSERT(tbl_stock->get(txn, stockPK.data(), stockPK.size(), stock_v, stock_vlen));
      ALWAYS_ASSERT(stock_vlen == sizeof(tpcc::stock));
      tpcc::stock *stock = (tpcc::stock *) stock_v;

      if (stock->s_quantity - ol_quantity >= 10)
        stock->s_quantity -= ol_quantity;
      else
        stock->s_quantity += -int32_t(ol_quantity) + 91;

      stock->s_ytd += ol_quantity;
      stock->s_remote_cnt += (ol_supply_w_id == warehouse_id) ? 0 : 1;

      tbl_stock->put(txn, stockPK.data(), stockPK.size(), stock_v, stock_vlen);

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

    db->commit_txn(txn);
    ntxn_commits++;
  } catch (abstract_db::abstract_abort_exception &ex) {
    db->abort_txn(txn);
    ntxn_aborts++;
  }
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



}
