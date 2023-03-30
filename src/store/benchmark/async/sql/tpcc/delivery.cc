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

#include "store/benchmark/async/tpcc/tpcc_utils.h"

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
  const query_result::QueryResult *queryResult;
  std::string query;
  std::vector<const query_result::QueryResult*> results;

  Debug("DELIVERY");
  Debug("Warehouse: %u", w_id);
  Debug("District: %u", d_id);
  //std::cerr << "warehouse: " << w_id << std::endl;

  client.Begin(timeout);

  query = fmt::format("SELECT FROM EarliestNewOrder WHERE w_id = {} AND d_id = {};",
        w_id, d_id);
  client.Query(query, queryResult, timeout);

  tpcc::EarliestNewOrderRow eno_row;
  if (queryResult->empty()) {
    // TODO: technically we're supposed to check each district in this warehouse
    return client.Commit(timeout);
  } else {
    deserialize(eno_row, queryResult);
  }
  uint32_t o_id = eno_row.o_id();
  Debug("  Earliest New Order: %u", o_id);

  eno_row.set_o_id(o_id + 1);
  query = fmt::format("UPDATE EarliestNewOrder\n SET o_id = {}\n WHERE w_id = {} AND d_id = {};",
        eno_row.o_id(), w_id, d_id);
  std::vector<std::vector<unsigned int>> compound_key{{0, 1}};
  client.Write(query, compound_key, queryResult, timeout);

  query = fmt::format("SELECT FROM Order WHERE id = {} AND d_id = {} AND w_id = {};",
        o_id, d_id, w_id);
  client.Query(query, queryResult, timeout);
  if (queryResult->empty()) {
    // already delivered all orders for this warehouse
    return client.Commit(timeout);
  }

  query = fmt::format("DELETE FROM NewOrder WHERE o_id = {} AND d_id = {} AND w_id = {};",
        o_id, d_id, w_id);
  compound_key = {{0, 1, 2}};
  client.Write(query, compound_key, queryResult, timeout);

  tpcc::OrderRow o_row;
  deserialize(o_row, queryResult);

  o_row.set_carrier_id(o_carrier_id);
  query = fmt::format("UPDATE Order\n SET carrier_id = {}\n WHERE id = {} AND d_id = {} AND w_id = {};",
        o_row.carrier_id(), o_row.id(), o_row.d_id(), o_row.w_id());
  compound_key = {{0, 1, 2}};
  client.Write(query, compound_key, queryResult, timeout);
  Debug("  Carrier ID: %u", o_carrier_id);
  Debug("  Order Lines: %u", o_row.ol_cnt());

  for (size_t ol_number = 0; ol_number < o_row.ol_cnt(); ++ol_number) {
    query = fmt::format("SELECT FROM OrderLine WHERE o_id = {} AND d_id = {} AND w_id = {} AND number = {};",
          o_id, d_id, w_id, ol_number);
    client.Query(query, timeout);
  }

  client.Wait(results);

  int32_t total_amount = 0;
  for (size_t ol_number = 0; ol_number < o_row.ol_cnt(); ++ol_number) {
    Debug("    Order Line %lu", ol_number);
    tpcc::OrderLineRow ol_row;
    deserialize(ol_row, results[ol_number]);
    Debug("      Amount: %i", ol_row.amount());
    total_amount += ol_row.amount();

    ol_row.set_delivery_d(ol_delivery_d);
    query = fmt::format("UPDATE OrderLine\n SET delivery_d = {}\n WHERE o_id = {} AND d_id = {} AND w_id = {} AND number = {};",
          ol_row.delivery_d(), o_id, d_id, w_id, ol_number);
    compound_key = {{0, 1, 2, 3}};
    client.Write(query, compound_key, queryResult, timeout);
    Debug("      Delivery Date: %u", ol_delivery_d);
  }
  Debug("Total Amount: %i", total_amount);

  Debug("Customer: %u", o_row.c_id());
  query = fmt::format("SELECT FROM Customer WHERE id = {} AND d_id = {} AND w_id = {};",
        o_row.c_id(), d_id, w_id);
  client.Query(query, queryResult, timeout);
  tpcc::CustomerRow c_row;
  deserialize(c_row, queryResult);
  Debug("  Old Balance: %i", c_row.balance());

  c_row.set_balance(c_row.balance() + total_amount);
  Debug("  New Balance: %i", c_row.balance());
  c_row.set_delivery_cnt(c_row.delivery_cnt() + 1);
  query = fmt::format("UPDATE Customer\n SET balance = {}, delivery_cnt = {}\n WHERE id = {} AND d_id = {} AND w_id = {};",
        c_row.balance(), c_row.delivery_cnt(), o_row.c_id(), d_id, w_id);
  compound_key = {{0, 1, 2}};
  client.Write(query, compound_key, queryResult, timeout);
  Debug("  Delivery Count: %u", c_row.delivery_cnt());

  Debug("COMMIT");
  return client.Commit(timeout);
}

} // namespace tpcc_sql
