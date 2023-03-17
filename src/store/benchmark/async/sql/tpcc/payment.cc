/***********************************************************************
 *
 * Copyright 2021 Florian Suri-Payer <fsp@cs.cornell.edu>
 *                Matthew Burke <matthelb@cs.cornell.edu>
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
#include "store/benchmark/async/sql/tpcc/payment.h"

#include <sstream>
#include <format>

#include "store/benchmark/async/tpcc/tpcc_utils.h"

namespace tpcc_sql {

SQLPayment::SQLPayment(uint32_t timeout, uint32_t w_id, uint32_t c_c_last,
    uint32_t c_c_id, uint32_t num_warehouses, std::mt19937 &gen) :
    TPCCSQLTransaction(timeout), w_id(w_id) {
  d_id = std::uniform_int_distribution<uint32_t>(1, 10)(gen); 
  d_w_id = w_id;
  int x = std::uniform_int_distribution<int>(1, 100)(gen);
  int y = std::uniform_int_distribution<int>(1, 100)(gen);
  Debug("w_id: %u", w_id);
  Debug("this->w_id: %u", this->w_id);
  Debug("x: %d; num_warehouses: %u", x, num_warehouses);
  if (x <= 85 || num_warehouses == 1) {
    c_w_id = w_id;
    c_d_id = d_id;
  } else {
    c_w_id = std::uniform_int_distribution<uint32_t>(1, num_warehouses - 1)(gen);
    Debug("c_w_id: %u", c_w_id);
    if (c_w_id == w_id) {
      c_w_id = num_warehouses; // simple swap to ensure uniform distribution
      Debug("c_w_id: %u", c_w_id);
    }
    c_d_id = std::uniform_int_distribution<uint32_t>(1, 10)(gen);
  }
  if (y <= 60) {
    int last = tpcc::NURand(255, 0, 999, static_cast<int>(c_c_last), gen);
    c_last = tpcc::GenerateCustomerLastName(last);
    c_by_last_name = true;
  } else {
    c_id = tpcc::NURand(1023, 1, 3000, static_cast<int>(c_c_id), gen);
    c_by_last_name = false;
  }
  h_amount = std::uniform_int_distribution<uint32_t>(100, 500000)(gen);
  h_date = std::time(0);
}

SQLPayment::~SQLPayment() {
}

transaction_status_t SQLPayment::Execute(SyncClient &client) {
  query_result::QueryResult *queryResult;
  std::string query;
  std::vector<query_result::QueryResult*> results;

  Debug("PAYMENT");
  Debug("Amount: %u", h_amount);
  Debug("Warehouse: %u", w_id);
  //std::cerr << "warehouse: " << w_id << std::endl;

  client.Begin(timeout);

  query = "SELECT FROM Warehouse WHERE pk = ";
  std::string w_key = tpcc::WarehouseRowKey(w_id);
  query.append(w_key);
  client.Query(query, timeout);
  Debug("District: %u", d_id);
  query = "SELECT FROM District WHERE pk = ";
  std::string d_key = tpcc::DistrictRowKey(d_w_id, d_id);
  query.append(d_key);
  client.Query(query, timeout);

  std::string c_key;
  if (c_by_last_name) { // access customer by last name
    Debug("Customer: %s", c_last.c_str());
    Debug("  Get(c_w_id=%u, c_d_id=%u, c_last=%s)", c_w_id, c_d_id,
      c_last.c_str());
    query = "SELECT FROM CustomerByName WHERE pk = ";
    std::string cbn_key = tpcc::CustomerByNameRowKey(c_w_id, c_d_id, c_last);
    query.append(cbn_key);
    client.Query(query, timeout);

    client.Wait(results);

    tpcc::CustomerByNameRow cbn_row;
    deserialize(cbn_row, results[2]);
    int idx = (cbn_row.ids_size() + 1) / 2;
    if (idx == cbn_row.ids_size()) {
      idx = cbn_row.ids_size() - 1;
    }
    c_id = cbn_row.ids(idx);
    Debug("  ID: %u", c_id);

    query = "SELECT FROM Customer WHERE pk = ";
    c_key = tpcc::CustomerRowKey(c_w_id, c_d_id, c_id);
    query.append(c_key);
    client.Query(query, timeout);
  } else {
    Debug("Customer: %u", c_id);

    query = "SELECT FROM Customer WHERE pk = ";
    c_key = tpcc::CustomerRowKey(c_w_id, c_d_id, c_id);
    query.append(c_key);
    client.Query(query, timeout);
    client.Wait(results);
  }

  tpcc::WarehouseRow w_row;
  deserialize(w_row, results[0]);
  w_row.set_ytd(w_row.ytd() + h_amount);
  Debug("  YTD: %u", w_row.ytd());
  query = std::format("INSERT INTO Warehouse\n VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {});", 
            w_row.id(), w_row.name(), w_row.street_1(), w_row.street_2(), 
            w_row.city(), w_row.state(), w_row.zip(), w_row.tax(), w_row.ytd());
  client.Query(query, timeout);

// Checkpoint
  tpcc::DistrictRow d_row;
  UW_ASSERT(d_row.ParseFromString(strs[1]));
  d_row.set_ytd(d_row.ytd() + h_amount);
  Debug("  YTD: %u", d_row.ytd());
  d_row.SerializeToString(&str);
  client.Put(d_key, str, timeout);

  tpcc::CustomerRow c_row;
  UW_ASSERT(c_row.ParseFromString(strs[2]));
  c_row.set_balance(c_row.balance() - h_amount);
  c_row.set_ytd_payment(c_row.ytd_payment() + h_amount);
  c_row.set_payment_cnt(c_row.payment_cnt() + 1);
  Debug("  Balance: %u", c_row.balance());
  Debug("  YTD: %u", c_row.ytd_payment());
  Debug("  Payment Count: %u", c_row.payment_cnt());
  if (c_row.credit() == "BC") {
    std::stringstream ss;
    ss << c_id << "," << c_d_id << "," << c_w_id << "," << d_id << ","
             << w_id << "," << h_amount;
    std::string new_data = ss.str() +  c_row.data();
    new_data = new_data.substr(std::min(new_data.size(), 500UL));
    c_row.set_data(new_data);
  }
  c_row.SerializeToString(&str);
  client.Put(c_key, str, timeout);

  tpcc::HistoryRow h_row;
  h_row.set_c_id(c_id);
  h_row.set_c_d_id(c_d_id);
  h_row.set_c_w_id(c_w_id);
  h_row.set_d_id(d_id);
  h_row.set_w_id(w_id);
  h_row.set_data(w_row.name() + "    " + d_row.name());
  h_row.SerializeToString(&str);
  client.Put(tpcc::HistoryRowKey(w_id, d_id, c_id), str, timeout); //TODO: should write to a unique key

  Debug("COMMIT");
  return client.Commit(timeout);
}

} // namespace tpcc_sql
