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
}

namespace tpcc_sql {

template<class T>
void deserialize(T& t, query_result::QueryResult* queryResult) {
  std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
  for(int i = 0; i < queryResult->columns(); i++) {
    std::size_t n_bytes;
    const char* r_chars = queryResult->get(0, i, &n_bytes);
    std::string r = std::string(r_chars, n_bytes);
    ss << r;
  }
  {
    cereal::BinaryInputArchive iarchive(ss); // Create an input archive
    iarchive(t); // Read the data from the archive
  }
}

class TPCCSQLTransaction : public SyncTransaction {
 public:
  TPCCSQLTransaction(uint32_t timeout);
  virtual ~TPCCSQLTransaction();
};

}

#endif /* TPCC_SQL_TRANSACTION_H */
