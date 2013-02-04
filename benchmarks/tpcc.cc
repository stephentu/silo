#include <sys/time.h>

#include "bench.h"
#include "tpcc.h"

// tpcc schemas
using namespace std;

class tpcc_worker : public bench_worker {
public:
  tpcc_worker(unsigned long seed, abstract_db *db,
              const map<string, abstract_ordered_index *> &open_tables,
              spin_barrier *barrier_a, spin_barrier *barrier_b,
              unsigned int warehouse_id)
    : bench_worker(seed, db, open_tables, barrier_a, barrier_b),
      warehouse_id(warehouse_id),
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
    assert(warehouse_id >= 1);
  }

  void txn_new_order();

private:

  unsigned int warehouse_id;

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

  static inline size_t
  NumWarehouses()
  {
    return (size_t) scale_factor;
  }

  inline int
  RandomNumber(int min, int max)
  {
    return (int) (r.next_uniform() * (max - min + 1) + min);
  }

  inline int
  NonUniformRandom(int A, int C, int min, int max)
  {
    return (((RandomNumber(0, A) | RandomNumber(min, max)) + C) % (max - min + 1)) + min;
  }

  inline int
  GetItemId()
  {
    return NonUniformRandom(8191, 7911, 1, 100000);
  }

  inline int
  GetCustomerId()
  {
    return NonUniformRandom(1023, 259, 1, 3000);
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

typedef unsigned int uint;

void
tpcc_worker::txn_new_order()
{
  const uint districtID = RandomNumber(1, 10);
  const uint customerID = GetCustomerId();
  const uint numItems = RandomNumber(5, 15);
  vector<uint> itemIDs(numItems),
               supplierWarehouseIDs(numItems),
               orderQuantities(numItems);
  bool allLocal = true;
  for (uint i = 0; i < numItems; i++) {
    itemIDs[i] = GetItemId();
    if (NumWarehouses() == 1 || RandomNumber(1, 100) > 1) {
      supplierWarehouseIDs[i] = warehouse_id;
    } else {
      do {
       supplierWarehouseIDs[i] = RandomNumber(1, NumWarehouses());
      } while (supplierWarehouseIDs[i] == warehouse_id);
      allLocal = false;
    }
    orderQuantities[i] = RandomNumber(1, 10);
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

    tpcc::new_order new_order = {
      .no_w_id = int32_t(warehouse_id),
      .no_d_id = int32_t(districtID),
      .no_o_id = district->d_next_o_id,
    };

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

      char *ol_dist_info = 0;
      switch (districtID) {
      case 1:
        ol_dist_info = &stock->s_dist_01[0];
        break;
      case 2:
        ol_dist_info = &stock->s_dist_02[0];
        break;
      case 3:
        ol_dist_info = &stock->s_dist_03[0];
        break;
      case 4:
        ol_dist_info = &stock->s_dist_04[0];
        break;
      case 5:
        ol_dist_info = &stock->s_dist_05[0];
        break;
      case 6:
        ol_dist_info = &stock->s_dist_06[0];
        break;
      case 7:
        ol_dist_info = &stock->s_dist_07[0];
        break;
      case 8:
        ol_dist_info = &stock->s_dist_08[0];
        break;
      case 9:
        ol_dist_info = &stock->s_dist_09[0];
        break;
      case 10:
        ol_dist_info = &stock->s_dist_10[0];
        break;
      default:
        ALWAYS_ASSERT(false);
        break;
      }

      memcpy(&order_line.ol_dist_info[0], ol_dist_info, ARRAY_NELEMS(order_line.ol_dist_info));

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
