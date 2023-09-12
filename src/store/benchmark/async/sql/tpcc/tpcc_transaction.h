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

#include "store/benchmark/async/tpcc/tpcc-proto.pb.h"
#include "store/common/frontend/sync_client.h"
#include "store/common/frontend/sync_transaction.h"
#include "lib/cereal/archives/binary.hpp"
#include "lib/cereal/types/string.hpp"

namespace tpcc {

template<class Archive>
void save(Archive & archive, 
          tpcc::WarehouseRow const & w)
{ 
  archive( w.id(), w.name(), w.street_1(), w.street_2(), w.city(), w.state(), w.zip(), w.tax(), w.ytd());
}

template<class Archive>
void load(Archive & archive,
          tpcc::WarehouseRow & w)
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
  archive( id, name, street_1, street_2, city, state, zip, tax, ytd );
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


template<class Archive>
void save(Archive & archive, 
          tpcc::DistrictRow const & d)
{ 
  uint32_t id = d.id();
  uint32_t w_id = d.w_id();
  std::string name = d.name();
  std::string street_1 = d.street_1();
  std::string street_2 = d.street_2();
  std::string city = d.city();
  std::string state = d.state();
  std::string zip = d.zip();
  int32_t tax = d.tax();
  int32_t ytd = d.ytd();
  uint32_t next_o_id = d.next_o_id();
  archive( id, w_id, name, street_1, street_2, city, state, zip, tax, ytd, next_o_id );
}

template<class Archive>
void load(Archive & archive,
          tpcc::DistrictRow & d)
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
  archive( id, w_id, name, street_1, street_2, city, state, zip, tax, ytd, next_o_id );
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

template<class Archive>
void save(Archive & archive, 
          tpcc::OrderRow const & o)
{ 
  archive( o.id(), o.d_id(), o.w_id(), o.c_id(), o.entry_d(), o.carrier_id(), o.ol_cnt(), o.all_local() );
}

template<class Archive>
void load(Archive & archive,
          tpcc::OrderRow & o)
{
  uint32_t id;
  uint32_t d_id;
  uint32_t w_id;
  uint32_t c_id;
  uint32_t entry_d;
  uint32_t carrier_id;
  uint32_t ol_cnt;
  uint32_t all_local;
  archive( id, d_id, w_id, c_id, entry_d, carrier_id, ol_cnt, all_local );
  o.set_id(id);
  o.set_d_id(d_id);
  o.set_w_id(w_id);
  o.set_c_id(c_id);
  o.set_entry_d(entry_d);
  o.set_carrier_id(carrier_id);
  o.set_ol_cnt(ol_cnt);
  o.set_all_local(all_local);
}

template<class Archive>
void save(Archive & archive, 
          tpcc::OrderLineRow const & o)
{ 
  archive( o.o_id(), o.d_id(), o.w_id(), o.number(), o.i_id(), o.supply_w_id(), o.delivery_d(), o.quantity(), o.amount(), o.dist_info() );
}

template<class Archive>
void load(Archive & archive,
          tpcc::OrderLineRow & o)
{
  uint32_t o_id;
  uint32_t d_id;
  uint32_t w_id;
  uint32_t number;
  uint32_t i_id;
  uint32_t supply_w_id;
  uint32_t delivery_d;
  uint32_t quantity;
  int32_t amount;
  std::string dist_info;
  archive( o_id, d_id, w_id, number, i_id, supply_w_id, delivery_d, quantity, amount, dist_info );
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

template<class Archive>
void save(Archive & archive, 
          tpcc::CustomerRow const & c)
{ 
  archive(c.id(), c.d_id(), c.w_id(), c.first(), c.middle(), c.last(), 
          c.street_1(), c.street_2(), c.city(), c.state(), c.zip(), c.phone(), 
          c.since(), c.credit(), c.credit_lim(), c.discount(), c.balance(), 
          c.ytd_payment(), c.payment_cnt(), c.delivery_cnt(), c.data());
}

template<class Archive>
void load(Archive & archive,
          tpcc::CustomerRow & c)
{
  uint32_t id;
  uint32_t d_id;
  uint32_t w_id;
  std::string first;
  std::string middle;
  std::string last;
  std::string street_1;
  std::string street_2;
  std::string city;
  std::string state;
  std::string zip;
  std::string phone;
  uint32_t since;
  std::string credit;
  int32_t credit_lim;
  float discount;
  float balance;
  float ytd_payment;
  uint32_t payment_cnt;
  uint32_t delivery_cnt;
  std::string data;
  archive( id, d_id, w_id, first, middle, last, street_1, street_2, city, state,
           zip, phone, since, credit, credit_lim, discount, balance, ytd_payment, 
           payment_cnt, delivery_cnt, data );
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

template<class Archive>
void save(Archive & archive, 
          tpcc::ItemRow const & i)
{ 
  archive(i.id(), i.im_id(), i.name(), i.price(), i.data());
}

template<class Archive>
void load(Archive & archive,
          tpcc::ItemRow & i)
{
  uint32_t id;
  uint32_t im_id;
  std::string name;
  float price;
  std::string data;
  archive( id, im_id, name, price, data );
  i.set_id(id);
  i.set_im_id(im_id);
  i.set_name(name);
  i.set_price(price);
  i.set_data(data);
}

template<class Archive>
void save(Archive & archive, 
          tpcc::StockRow const & s)
{ 
  archive(s.i_id(), s.w_id(), s.quantity(), s.dist_01(), s.dist_02(),
          s.dist_03(), s.dist_04(), s.dist_05(), s.dist_06(), s.dist_07(),
          s.dist_08(), s.dist_09(), s.dist_10(), s.ytd(), s.order_cnt(),
          s.remote_cnt(), s.data());
}

template<class Archive>
void load(Archive & archive,
          tpcc::StockRow & s)
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
  archive( i_id, w_id, quantity, dist_01, dist_02, dist_03, dist_04, dist_05,
           dist_06, dist_07, dist_08, dist_09, dist_10, ytd, order_cnt, remote_cnt,
           data );
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

template<class Archive>
void save(Archive & archive, 
          tpcc::NewOrderRow const & new_o)
{ 
  archive(new_o.o_id(), new_o.d_id(), new_o.w_id());
}

template<class Archive>
void load(Archive & archive,
          tpcc::NewOrderRow & new_o)
{
  uint32_t id;
  uint32_t d_id;
  uint32_t w_id;
  archive( id, d_id, w_id );
  new_o.set_o_id(id);
  new_o.set_d_id(d_id);
  new_o.set_w_id(w_id);
}
}

namespace tpcc_sql {


template<class T>
void deserialize(T& t, std::unique_ptr<const query_result::QueryResult>& queryResult, const std::size_t row) {
  std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
  for(std::size_t i = 0; i < queryResult->columns(); i++) {
    std::size_t n_bytes = 0;
    const char* r_chars = queryResult->get(row, i, &n_bytes);
    fprintf(stderr, "n_bytes: %lu\n", n_bytes);
    std::string r = std::string(r_chars, n_bytes);
    ss << r;
  }
  {
    cereal::BinaryInputArchive iarchive(ss); // Create an input archive
    iarchive(t); // Read the data from the archive
  }
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
