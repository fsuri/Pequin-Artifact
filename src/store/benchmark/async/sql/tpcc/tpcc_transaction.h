/***********************************************************************
 *
 * Copyright 2021 Florian Suri-Payer <fsp@cs.cornell.edu>
 *                Matthew Burke <matthelb@cs.cornell.edu>
 *                Liam Arzola <lma77@cornell.edu>
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/
#ifndef TPCC_SQL_TRANSACTION_H
#define TPCC_SQL_TRANSACTION_H

#include "store/common/frontend/sync_transaction.h"
#include "tpcc_schema.h"

namespace tpcc_sql {

template <class T>
void load_row(std::unique_ptr<query_result::Row> row, T& t) {
  row->get(0, &t);
}

inline void load_row(std::unique_ptr<query_result::Row> row,
          WarehouseRow& w)
{
  uint32_t id;
  std::string name;
  std::string street_1;
  std::string street_2;
  std::string city;
  std::string state;
  std::string zip;
  int32_t tax;
  int32_t ytd;
  row->get(0, &id);
  row->get(1, &name);
  row->get(2, &street_1);
  row->get(3, &street_2);
  row->get(4, &city);
  row->get(5, &state);
  row->get(6, &zip);
  row->get(7, &tax);
  row->get(8, &ytd);
  w.set_id(id);
  w.set_name(name);
  w.set_street_1(street_1);
  w.set_street_2(street_2);
  w.set_city(city);
  w.set_state(state);
  w.set_zip(zip);
  w.set_tax(tax);
  w.set_ytd(ytd);
}
 
inline void load_row(std::unique_ptr<query_result::Row> row,
          DistrictRow& d)
{
  uint32_t id;
  uint32_t w_id;
  std::string name;
  std::string street_1;
  std::string street_2;
  std::string city;
  std::string state;
  std::string zip;
  int32_t tax;
  int32_t ytd;
  uint32_t next_o_id;
  row->get(0, &id);
  row->get(1, &w_id);
  row->get(2, &name);
  row->get(3, &street_1);
  row->get(4, &street_2);
  row->get(5, &city);
  row->get(6, &state);
  row->get(7, &zip);
  row->get(8, &tax);
  row->get(9, &ytd);
  row->get(10, &next_o_id);
  d.set_id(id);
  d.set_w_id(w_id);
  d.set_name(name);
  d.set_street_1(street_1);
  d.set_street_2(street_2);
  d.set_city(city);
  d.set_state(state);
  d.set_zip(zip);
  d.set_tax(tax);
  d.set_ytd(ytd);
  d.set_next_o_id(next_o_id);
}
 
inline void load_row(std::unique_ptr<query_result::Row> row,
          OrderRow& o)
{
  uint32_t id;
  uint32_t d_id;
  uint32_t w_id;
  uint32_t c_id;
  uint32_t entry_d;
  uint32_t carrier_id;
  uint32_t ol_cnt;
  bool all_local;
  row->get(0, &id);
  row->get(1, &d_id);
  row->get(2, &w_id);
  row->get(3, &c_id);
  row->get(4, &entry_d);
  row->get(5, &carrier_id);
  row->get(6, &ol_cnt);
  row->get(7, &all_local);
  o.set_id(id);
  o.set_d_id(d_id);
  o.set_w_id(w_id);
  o.set_c_id(c_id);
  o.set_entry_d(entry_d);
  o.set_carrier_id(carrier_id);
  o.set_ol_cnt(ol_cnt);
  o.set_all_local(all_local);
}

inline void load_row(std::unique_ptr<query_result::Row> row,
          OrderLineRow& o)
{
  uint32_t  o_id;
  uint32_t  d_id;
  uint32_t  w_id;
  uint32_t  number;
  uint32_t  i_id;
  uint32_t  supply_w_id;
  uint32_t  delivery_d;
  uint32_t  quantity;
  int32_t amount;
  std::string dist_info;
  row->get(0, &o_id);
  row->get(1, &d_id);
  row->get(2, &w_id);
  row->get(3, &number);
  row->get(4, &i_id);
  row->get(5, &supply_w_id);
  row->get(6, &delivery_d);
  row->get(7, &quantity);
  o.set_o_id(o_id);
  o.set_d_id(d_id);
  o.set_w_id(w_id);
  o.set_number(number);
  o.set_i_id(i_id);
  o.set_supply_w_id(supply_w_id);
  o.set_delivery_d(delivery_d);
  o.set_quantity(quantity);
  o.set_amount(amount);
  o.set_dist_info(dist_info);
}
 
inline void load_row(std::unique_ptr<query_result::Row> row,
          CustomerRow& c)
{
  uint32_t  id;
  uint32_t  d_id;
  uint32_t  w_id;
  std::string first;
  std::string middle;
  std::string last;
  std::string street_1;
  std::string street_2;
  std::string city;
  std::string state;
  std::string zip;
  std::string phone;
  uint32_t  since;
  std::string credit;
  uint32_t credit_lim;
  int32_t discount;
  int32_t balance;
  int32_t ytd_payment;
  uint32_t  payment_cnt;
  uint32_t  delivery_cnt;
  std::string data;
  std::cerr << "trying to load customer row" << std::endl;
  row->get(0, &id);
  row->get(1, &d_id);
  row->get(2, &w_id);
  row->get(3, &first);
  row->get(4, &middle);
  row->get(5, &last);
  row->get(6, &street_1);
  row->get(7, &street_2);
  row->get(8, &city);
  row->get(9, &state);
  row->get(10, &zip);
  row->get(11, &phone);
  row->get(12, &since);
  row->get(13, &credit);
  row->get(14, &credit_lim);
  row->get(15, &discount);
  row->get(16, &balance);
  row->get(17, &ytd_payment);
  row->get(18, &payment_cnt);
  row->get(19, &delivery_cnt);
  row->get(20, &data);
  c.set_id(id);
  c.set_d_id(d_id);
  c.set_w_id(w_id);
  c.set_first(first);
  c.set_middle(middle);
  c.set_last(last);
  c.set_street_1(street_1);
  c.set_street_2(street_2);
  c.set_city(city);
  c.set_state(state);
  c.set_zip(zip);
  c.set_phone(phone);
  c.set_since(since);
  c.set_credit(credit);
  c.set_credit_lim(credit_lim);
  c.set_discount(discount);
  c.set_balance(balance);
  c.set_ytd_payment(ytd_payment);
  c.set_payment_cnt(payment_cnt);
  c.set_delivery_cnt(delivery_cnt);
  c.set_data(data);
}
 
inline void load_row(std::unique_ptr<query_result::Row> row,
          ItemRow& i)
{
  uint32_t id;
  uint32_t im_id;
  std::string name;
  uint32_t price;
  std::string data;
  row->get(0, &id);
  row->get(1, &im_id);
  row->get(2, &name);
  row->get(3, &price);
  row->get(4, &data);
  i.set_id(id);
  i.set_im_id(im_id);
  i.set_name(name);
  i.set_price(price);
  i.set_data(data);
}
 
inline void load_row(std::unique_ptr<query_result::Row> row,
          StockRow& s)
{
  uint32_t i_id;
  uint32_t w_id;
  int32_t quantity;
  std::string dist_01;
  std::string dist_02;
  std::string dist_03;
  std::string dist_04;
  std::string dist_05;
  std::string dist_06;
  std::string dist_07;
  std::string dist_08;
  std::string dist_09;
  std::string dist_10;
  int32_t ytd;
  int32_t order_cnt;
  int32_t remote_cnt;
  std::string data;
  row->get(0, &i_id);
  row->get(1, &w_id);
  row->get(2, &quantity);
  row->get(3, &dist_01);
  row->get(4, &dist_02);
  row->get(5, &dist_03);
  row->get(6, &dist_04);
  row->get(7, &dist_05);
  row->get(8, &dist_06);
  row->get(9, &dist_07);
  row->get(10, &dist_08);
  row->get(11, &dist_09);
  row->get(12, &dist_10);
  row->get(13, &ytd);
  row->get(14, &order_cnt);
  row->get(15, &remote_cnt);
  row->get(16, &data);
  s.set_i_id(i_id);
  s.set_w_id(w_id);
  s.set_quantity(quantity);
  s.set_dist_01(dist_01);
  s.set_dist_02(dist_02);
  s.set_dist_03(dist_03);
  s.set_dist_04(dist_04);
  s.set_dist_05(dist_05);
  s.set_dist_06(dist_06);
  s.set_dist_07(dist_07);
  s.set_dist_08(dist_08);
  s.set_dist_09(dist_09);
  s.set_dist_10(dist_10);
  s.set_ytd(ytd);
  s.set_order_cnt(order_cnt);
  s.set_remote_cnt(remote_cnt);
  s.set_data(data);
}

 
inline void load_row(std::unique_ptr<query_result::Row> row,
          NewOrderRow& new_o)
{
  uint32_t id;
  uint32_t d_id;
  uint32_t w_id;
  row->get(0, &id);
  row->get(1, &d_id);
  row->get(2, &w_id);
  new_o.set_o_id(id);
  new_o.set_d_id(d_id);
  new_o.set_w_id(w_id);
}

template<class T>
void deserialize(T& t, std::unique_ptr<const query_result::QueryResult>& queryResult, const std::size_t row) {
  load_row(queryResult->at(row), t);
}

template<class T>
void deserialize(T& t, std::unique_ptr<const query_result::QueryResult>& queryResult) {
  deserialize(t, queryResult, 0);
}

class TPCCSQLTransaction : public SyncTransaction {
 public:
  TPCCSQLTransaction(uint32_t timeout);
  virtual ~TPCCSQLTransaction();
};

}

#endif /* TPCC_SQL_TRANSACTION_H */
