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
#include "store/benchmark/async/sql/tpcc/delivery.h"

#include <fmt/core.h>

#include "store/benchmark/async/sql/tpcc/tpcc_utils.h"

namespace tpcc_sql {

SQLDelivery::SQLDelivery(uint32_t timeout, uint32_t w_id, uint32_t d_id,
    std::mt19937 &gen)
    : TPCCSQLTransaction(timeout), w_id(w_id), d_id(d_id) {
  o_carrier_id = std::uniform_int_distribution<uint32_t>(1, 10)(gen);
  ol_delivery_d = std::time(0);
}

SQLDelivery::~SQLDelivery() {
}

transaction_status_t SQLDelivery::Execute(SyncClient &client) {
  std::unique_ptr<const query_result::QueryResult> queryResult;
  std::string statement;
  std::vector<std::unique_ptr<const query_result::QueryResult>> results;

  // Process a batch of 10 new (not yet delivered) orders. Each order delivery is it's own read/write TX.
  // Low frequency
  Debug("DELIVERY");
  Debug("Warehouse: %u", w_id);
  Debug("District: %u", d_id);
  //std::cerr << "warehouse: " << w_id << std::endl;

  client.Begin(timeout);

  // (1) Retrieve the row from NEW-ORDER with the lowest order id
  //     If none is found, skip delivery of an order for this district. 

  statement = fmt::format("SELECT MIN(o_id) FROM NewOrder WHERE d_id = {} AND w_id = {};", d_id, w_id);
  //statement = fmt::format("SELECT o_id FROM NewOrder WHERE d_id = {} AND w_id = {} ORDER BY o_id ASC LIMIT 1;", d_id, w_id);
  client.Query(statement, queryResult, timeout);

  if (queryResult->empty()) {
    // Note: technically we're supposed to check each district in this warehouse  ==>> We instead are doing a TX for each district sequentially (see tpcc_client.cc)
    return client.Commit(timeout);
  }

  // (2) Delete the found NEW-ORDER row
      // Note: Pesto will turn this into a PointDelete for which no read is required.
  int no_o_id;
  deserialize(no_o_id, queryResult);
  statement = fmt::format("DELETE FROM NewOrder WHERE o_id = {} AND d_id = {} AND w_id = {};", no_o_id, d_id, w_id);
  client.Write(statement, timeout); //This can be async. //TODO: THink through async Writes carefully. Note: Can also make reads async when appropriate.

  // (3) Select the corresponding row from ORDER and extract the customer id. Update the carrier id of the order.
  statement = fmt::format("SELECT c_id FROM \"order\" WHERE id = {} AND d_id = {} AND w_id = {};", no_o_id, d_id, w_id);
  client.Query(statement, queryResult, timeout);

  if (queryResult->empty()) {
    // already delivered all orders for this warehouse
    client.Wait(results); //wait for the potentially async delete.
    return client.Commit(timeout);
  }
  int c_id;
  deserialize(c_id, queryResult);

      //Note: Pesto will turn this into a PointUpdate for which no read is required (TODO: Implement)
  statement = fmt::format("UPDATE \"order\" SET carrier_id = {} WHERE id = {} AND d_id = {} AND w_id = {};", o_carrier_id, no_o_id, d_id, w_id);
  client.Write(statement, timeout); //This can be async.
  Debug("  Carrier ID: %u", o_carrier_id);

  // (4) Select all rows in ORDER-LINE that match the order, and update delivery dates. Retrieve total amount (sum)
  statement = fmt::format("UPDATE OrderLine SET delivery_d = {} WHERE o_id = {} AND d_id = {} AND w_id = {};", ol_delivery_d, no_o_id, d_id, w_id);
  client.Write(statement, timeout); //This can be async.

      //Note: Ideally Pesto does not Scan twice, but Caches the result set to perform the update (TODO: Implement)
  statement = fmt::format("SELECT SUM(amount) FROM OrderLine WHERE o_id = {} AND d_id = {} AND w_id = {};", no_o_id, d_id, w_id);
  client.Query(statement, queryResult, timeout);
  int total_amount;
  deserialize(total_amount, queryResult);
  Debug("Total Amount: %i", total_amount);

  // (5) Update the balance and delivery count of the respective customer (that issued the order)
  Debug("Customer: %u", c_id);
  statement = fmt::format("UPDATE Customer SET balance = balance + {}, delivery_cnt = delivery_cnt + 1 WHERE id = {} AND d_id = {} AND w_id = {};", total_amount, c_id, d_id, w_id);
  client.Write(statement, queryResult, timeout);

  client.Wait(results);

  Debug("COMMIT");
  return client.Commit(timeout);
}

} // namespace tpcc_sql
