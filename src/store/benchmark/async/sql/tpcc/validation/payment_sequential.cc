/***********************************************************************
 *
 * Copyright 2025 Daniel Lee <dhl93@cornell.edu>
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
#include "store/benchmark/async/sql/tpcc/validation/payment.h"

#include <sstream>
#include <fmt/core.h>

#include "store/benchmark/async/sql/tpcc/tpcc_utils.h"

namespace tpcc_sql { 

ValidationSQLPaymentSequential::ValidationSQLPaymentSequential(uint32_t timeout, std::mt19937 &gen,
    const validation::proto::Payment &valPaymentMsg) : 
    ValidationTPCCSQLTransaction(timeout), SQLPaymentSequential(gen) {
  w_id = valPaymentMsg.w_id();
  d_id = valPaymentMsg.d_id();
  d_w_id = valPaymentMsg.d_w_id();
  c_w_id = valPaymentMsg.c_w_id();
  c_d_id = valPaymentMsg.c_d_id();
  c_id = valPaymentMsg.c_id();
  h_amount = valPaymentMsg.h_amount();
  h_date = valPaymentMsg.h_date();
  c_by_last_name = valPaymentMsg.c_by_last_name();
  c_last = valPaymentMsg.c_last();
  random_row_id = valPaymentMsg.random_row_id();
}

ValidationSQLPaymentSequential::~ValidationSQLPaymentSequential() {
}

transaction_status_t ValidationSQLPaymentSequential::Validate(SyncClient &client) {
  std::unique_ptr<const query_result::QueryResult> queryResult;
  std::string statement;
  std::vector<std::unique_ptr<const query_result::QueryResult>> results;

  //Update a customer's balance and reflect payment on district/warehouse sales statistics
  //Type: Light-weight read-write Tx, high frequency. (Uses Non-primar key access to CUSTOMER table)
  Debug("PAYMENT");
  //std::cerr << "PAYMENT" << std::endl;
  Debug("Amount: %u", h_amount);
  Debug("Warehouse: %u", w_id);
  //std::cerr << "warehouse: " << w_id << std::endl;

  client.Begin(timeout);

  // (1) Retrieve WAREHOUSE row. Update year to date balance. 
  statement = fmt::format("SELECT * FROM {} WHERE w_id = {}", WAREHOUSE_TABLE, w_id);
  client.Query(statement, queryResult, timeout);

  WarehouseRow w_row;
  deserialize(w_row, queryResult);
  Debug("  YTD: %u", w_row.get_ytd());

  // (1.5) Retrieve WAREHOUSE row. Update year to date balance. 
  statement = fmt::format("UPDATE {} SET w_ytd = {} WHERE w_id = {}", WAREHOUSE_TABLE, w_row.get_ytd() + h_amount, w_id);
  client.Write(statement, queryResult, timeout);


  // (2) Retrieve DISTRICT row. Update year to date balance. 
  Debug("District: %u", d_id);
  statement = fmt::format("SELECT * FROM {} WHERE d_id = {} AND d_w_id = {}", DISTRICT_TABLE, d_id, d_w_id);
  client.Query(statement, queryResult, timeout);

  DistrictRow d_row;
  deserialize(d_row, queryResult);
  Debug("  YTD: %u", d_row.get_ytd());

  // (2.5) Retrieve DISTRICT row. Update year to date balance.
  statement = fmt::format("UPDATE {} SET d_ytd = {} WHERE d_id = {} AND d_w_id = {}", DISTRICT_TABLE, d_row.get_ytd() + h_amount, d_id, d_w_id);
  client.Write(statement, queryResult, timeout); 


  // (3) Select Customer (based on last name OR customer number)
  CustomerRow c_row;
  if (c_by_last_name) { // access customer by last name
    Debug("Customer: %s", c_last.c_str());
    Debug("  Get(c_w_id=%u, c_d_id=%u, c_last=%s)", c_w_id, c_d_id,
      c_last.c_str());

    // (3. A) Retrieve a list of Customer that share the same Last Name (Secondary Key access; Scan Read). Select middle row.
    statement = fmt::format("SELECT * FROM {} WHERE c_d_id = {} AND c_w_id = {} AND c_last = '{}' ORDER BY c_first", CUSTOMER_TABLE, c_d_id, c_w_id, c_last);
    client.Query(statement, queryResult, timeout);
    int namecnt = queryResult->size();
    int idx = (namecnt + 1) / 2; //round up
    if (idx == namecnt) idx = namecnt - 1;
    deserialize(c_row, queryResult, idx);
    c_id = c_row.get_id();
    Debug("  ID: %u", c_id);
  } else {
    // (3.B) Retrieve Customer based on unique Number (Primary Key access; Point Read)
    statement = fmt::format("SELECT * FROM {} WHERE c_id = {} AND c_d_id = {} AND c_w_id = {}", CUSTOMER_TABLE, c_id, c_d_id, c_w_id);
    client.Query(statement, queryResult, timeout);
   
    deserialize(c_row, queryResult);
    Debug("Customer: %u", c_id);
  }

  if(queryResult->empty()) Panic("couldn't find customer?");
  ////////////Updates



  // (4) Decrease customer balance, increase year to date payment. Increment payment count.
  c_row.set_balance(c_row.get_balance() - h_amount);
  c_row.set_ytd_payment(c_row.get_ytd_payment() + h_amount);
  c_row.set_payment_cnt(c_row.get_payment_cnt() + 1);
  Debug("  Balance: %u", c_row.get_balance());
  Debug("  YTD: %u", c_row.get_ytd_payment());
  Debug("  Payment Count: %u", c_row.get_payment_cnt());
  // (4.5) Additionally: If credit = BC: Retrieve customer data and modify it
  if (c_row.get_credit() == "BC") {
    std::stringstream ss;
    ss << c_id << "," << c_d_id << "," << c_w_id << "," << d_id << "," << w_id << "," << h_amount;
    std::string new_data = ss.str() +  c_row.get_data();
    new_data = new_data.substr(0, std::min(new_data.size(), 500UL));
    c_row.set_data(new_data);
  }
  statement = fmt::format("UPDATE {} SET c_balance = {}, c_ytd_payment = {}, c_payment_cnt = {}, c_data = '{}' "
            "WHERE c_id = {} AND c_d_id = {} AND c_w_id = {};", CUSTOMER_TABLE,
            c_row.get_balance(), c_row.get_ytd_payment(), c_row.get_payment_cnt(), c_row.get_data(), 
            c_row.get_id(), c_row.get_d_id(), c_row.get_w_id());
  client.Write(statement, queryResult, timeout);  
  UW_ASSERT(queryResult->has_rows_affected());

  // (5) Create History entry.


   statement = fmt::format("INSERT INTO {} (row_id, h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data) " 
            "VALUES ({}, {}, {}, {}, {}, {}, {}, {}, '{}')", HISTORY_TABLE, random_row_id, c_id, c_d_id, c_w_id, d_id, w_id, h_date, h_amount, w_row.get_name() + "    " + d_row.get_name());
  client.Write(statement, queryResult, timeout, true); //blind write
  
  //Writes to history are blind, it technically doesn't matter if they are duplicate. But should ideally make it unique (or no primary key at all)
  if(!queryResult->has_rows_affected()){Warning("History row not unique. Might want to investigate");} 
  UW_ASSERT(queryResult->has_rows_affected());

  Debug("COMMIT");
  return client.Commit(timeout);
}

} // namespace tpcc_sql
