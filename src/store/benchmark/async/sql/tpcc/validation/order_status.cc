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
#include "store/benchmark/async/sql/tpcc/validation/order_status.h"

#include <fmt/core.h>

#include "store/benchmark/async/sql/tpcc/tpcc_utils.h"

namespace tpcc_sql {

ValidationSQLOrderStatus::ValidationSQLOrderStatus(uint32_t timeout, uint32_t w_id,
    uint32_t c_c_last, uint32_t c_c_id, std::mt19937 &gen) :
    ValidationTPCCSQLTransaction(timeout), SQLOrderStatus(w_id, c_c_last, c_c_id, gen) {
}

ValidationSQLOrderStatus::ValidationSQLOrderStatus(uint32_t timeout, validation::proto::OrderStatus valOrderStatusMsg) : 
  ValidationTPCCSQLTransaction(timeout) {
  c_w_id = valOrderStatusMsg.c_w_id();
  c_d_id = valOrderStatusMsg.c_d_id();
  c_id = valOrderStatusMsg.c_id();
  o_id = valOrderStatusMsg.o_id();
  c_by_last_name = valOrderStatusMsg.c_by_last_name();
  c_last = valOrderStatusMsg.c_last();
}

ValidationSQLOrderStatus::~ValidationSQLOrderStatus() {
}

transaction_status_t ValidationSQLOrderStatus::Validate(SyncClient &client) {
  std::unique_ptr<const query_result::QueryResult> queryResult;
  std::string query;

  // Query the Status of a Customers last order   
  // Type: Midweight read-only TX, low frequency. Uses non-primary key access to CUSTOMER table.
  Debug("ORDER_STATUS");
  Debug("Warehouse: %u", c_w_id);
  Debug("District: %u", c_d_id);
  //std::cerr << "warehouse: " << w_id << std::endl;

  client.Begin(timeout);

  // (1) Select customer (based on last name OR customer number)
  CustomerRow c_row;
  if (c_by_last_name) { // access customer by last name
    Debug("Customer: %s", c_last.c_str());

    // (1. A) Retrieve a list of Customer that share the same Last Name (Secondary Key access; Scan Read). Select middle row.
    query = fmt::format("SELECT * FROM {} WHERE c_d_id = {} AND c_w_id = {} AND c_last = '{}' ORDER BY c_first", CUSTOMER_TABLE, c_d_id, c_w_id, c_last);
    client.Query(query, queryResult, timeout);
    int namecnt = queryResult->size();
    int idx = (namecnt + 1) / 2; //round up
    if (idx == namecnt) idx = namecnt - 1;
    deserialize(c_row, queryResult, idx);
    c_id = c_row.get_id();
    Debug("  ID: %u", c_id);
  } else {

    // (1.B) Retrieve Customer based on unique Number (Primary Key access; Point Read)
    query = fmt::format("SELECT * FROM {} WHERE c_id = {} AND c_d_id = {} AND c_w_id = {}", CUSTOMER_TABLE, c_id, c_d_id, c_w_id);
    client.Query(query, queryResult, timeout);
    deserialize(c_row, queryResult);
    Debug("Customer: %u", c_id);
  }

  Debug("  First: %s", c_row.get_first().c_str());
  Debug("  Last: %s", c_row.get_last().c_str());

  
  // (2) Select row from Order (from respective client) with the highest ID. This is the most recent order by the client. Retrieve order_id, entry date, and carried id.
  query = fmt::format("SELECT * FROM {} WHERE o_w_id = {} AND o_d_id = {} AND o_c_id = {} ORDER BY o_id DESC LIMIT 1;", ORDER_TABLE, c_w_id, c_d_id, c_id);

  //FIXME: Why perform these two reads separately?? Instead issue: ONE read with ORDER BY DESC, and LIMIT to 1?
  // query = fmt::format("SELECT MAX(id) FROM \"order\" WHERE d_id = {} AND w_id = {} AND c_id = {}", c_d_id, c_w_id, c_id);
  // client.Query(query, queryResult, timeout);
  // int o_id;
  // deserialize(o_id, queryResult);
  // Debug("Order: %u", o_id);
  // 
  // query = fmt::format("SELECT * FROM \"order\" WHERE id = {} AND d_id = {} AND w_id = {}", o_id, c_d_id, c_w_id);
  // Debug(query.c_str());
  client.Query(query, queryResult, timeout);
  OrderRow o_row;
  if(queryResult->empty()) Panic("empty result for Order Row");

  deserialize(o_row, queryResult);
  Debug("  Order Lines: %u", o_row.get_ol_cnt());
  Debug("  Entry Date: %u", o_row.get_entry_d());
  Debug("  Carrier ID: %u", o_row.get_carrier_id());
  //TODO: Eventually clean these up and do a SELECT ol_cnt, entry_d, carrier_id instead

  // (3) Select all rows from ORDER-LINE belonging to the respective order id. Retrieve OrderLine ID, Supply Warehouse id, OrderLine Quantity, OrderLine amount, and OrderLine Delivery date.
  query = fmt::format("SELECT * FROM {} WHERE ol_o_id = {} AND ol_d_id = {} AND ol_w_id = {} AND ol_number < {}", ORDER_LINE_TABLE, o_row.get_id(), c_d_id, c_w_id, o_row.get_ol_cnt());
  client.Query(query, queryResult, timeout);
  Debug("COMMIT");
  return client.Commit(timeout);
}

} // namespace tpcc_sql
