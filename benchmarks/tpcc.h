#ifndef _NDB_BENCH_TPCC_H_
#define _NDB_BENCH_TPCC_H_
#include <stdint.h>
#include "inline_str.h"
#include "../macros.h"
namespace tpcc {
struct customer {
  int32_t c_w_id;
  int32_t c_d_id;
  int32_t c_id;
  float c_discount;
  inline_str_fixed<2> c_credit;
  inline_str_8<16> c_last;
  inline_str_8<16> c_first;
  float c_credit_lim;
  float c_balance;
  float c_ytd_payment;
  int32_t c_payment_cnt;
  int32_t c_delivery_cnt;
  inline_str_8<20> c_street_1;
  inline_str_8<20> c_street_2;
  inline_str_8<20> c_city;
  inline_str_fixed<2> c_state;
  inline_str_fixed<9> c_zip;
  inline_str_fixed<16> c_phone;
  uint32_t c_since;
  inline_str_fixed<2> c_middle;
  inline_str_16<500> c_data;
} PACKED;
struct customer_name_idx_mem {
	int32_t c_id;
	struct customer *c_ptr;
} PACKED;
struct customer_name_idx_nomem {
	int32_t c_id;
} PACKED;
struct district {
  int32_t d_w_id;
  int32_t d_id;
  float d_ytd;
  float d_tax;
  int32_t d_next_o_id;
  inline_str_8<10> d_name;
  inline_str_8<20> d_street_1;
  inline_str_8<20> d_street_2;
  inline_str_8<20> d_city;
  inline_str_fixed<2> d_state;
  inline_str_fixed<9> d_zip;
} PACKED;
struct history {
  int32_t h_c_id;
  int32_t h_c_d_id;
  int32_t h_c_w_id;
  int32_t h_d_id;
  int32_t h_w_id;
  uint32_t h_date;
  float h_amount;
  inline_str_8<24> h_data;
} PACKED;
struct item {
  int32_t i_id;
  inline_str_8<24> i_name;
  float i_price;
  inline_str_8<50> i_data;
  int32_t i_im_id;
} PACKED;
struct new_order {
  int32_t no_w_id;
  int32_t no_d_id;
  int32_t no_o_id;
} PACKED;
struct oorder {
  int32_t o_w_id;
  int32_t o_d_id;
  int32_t o_id;
  int32_t o_c_id;
  int32_t o_carrier_id;
  int8_t o_ol_cnt;
  bool o_all_local;
  uint32_t o_entry_d;
} PACKED;
struct oorder_c_id_idx_mem {
	int32_t o_id;
	struct oorder *o_ptr;
} PACKED;
struct oorder_c_id_idx_nomem {
	int32_t o_id;
} PACKED;
struct order_line {
  int32_t ol_w_id;
  int32_t ol_d_id;
  int32_t ol_o_id;
  int32_t ol_number;
  int32_t ol_i_id;
  uint32_t ol_delivery_d;
  float ol_amount;
  int32_t ol_supply_w_id;
  int8_t ol_quantity;
  inline_str_fixed<24> ol_dist_info;
} PACKED;
struct stock {
  int32_t s_w_id;
  int32_t s_i_id;
  int16_t s_quantity;
  float s_ytd;
  int32_t s_order_cnt;
  int32_t s_remote_cnt;
  inline_str_8<50> s_data;
  inline_str_fixed<24> s_dist_01;
  inline_str_fixed<24> s_dist_02;
  inline_str_fixed<24> s_dist_03;
  inline_str_fixed<24> s_dist_04;
  inline_str_fixed<24> s_dist_05;
  inline_str_fixed<24> s_dist_06;
  inline_str_fixed<24> s_dist_07;
  inline_str_fixed<24> s_dist_08;
  inline_str_fixed<24> s_dist_09;
  inline_str_fixed<24> s_dist_10;
} PACKED;
struct warehouse {
  int32_t w_id;
  float w_ytd;
  float w_tax;
  inline_str_8<10> w_name;
  inline_str_8<20> w_street_1;
  inline_str_8<20> w_street_2;
  inline_str_8<20> w_city;
  inline_str_fixed<2> w_state;
  inline_str_fixed<9> w_zip;
} PACKED;
}
#endif
