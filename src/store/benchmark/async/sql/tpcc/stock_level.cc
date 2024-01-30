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
#include "store/benchmark/async/sql/tpcc/stock_level.h"

#include <map>
#include <fmt/core.h>

#include "store/benchmark/async/sql/tpcc/tpcc_utils.h"

namespace tpcc_sql {

SQLStockLevel::SQLStockLevel(uint32_t timeout, uint32_t w_id, uint32_t d_id,
    std::mt19937 &gen) : TPCCSQLTransaction(timeout), w_id(w_id), d_id(d_id) {
  min_quantity = std::uniform_int_distribution<uint8_t>(10, 20)(gen);
}

SQLStockLevel::~SQLStockLevel() {
}

transaction_status_t SQLStockLevel::Execute(SyncClient &client) {
  std::unique_ptr<const query_result::QueryResult> queryResult;
  std::string query;
  std::vector<std::unique_ptr<const query_result::QueryResult>> results;

  //Determine the number of recently sold items with stock below a given threshold
  //Type: Heavy read-only Tx, low frequency
  std::cerr << "STOCK_LEVEL (parallel)" << std::endl;
  Debug("STOCK_LEVEL (parallel)");
  Debug("Warehouse: %u", w_id);
  Debug("District: %u", d_id);
  //std::cerr << "warehouse: " << w_id << std::endl;

  client.Begin(timeout);

  // (1) Select the specified row from District and extract the Next Order Id
  query = fmt::format("SELECT d_next_o_id FROM {} WHERE d_id = {} AND d_w_id = {}", DISTRICT_TABLE, d_id, w_id);
  client.Query(query, queryResult, timeout);
  uint32_t next_o_id;
  deserialize(next_o_id, queryResult);
  Debug("Orders: %u-%u", next_o_id - 20, next_o_id - 1);

  // (2) Select the 20 most recent orders from the district: Select the orders from OrderLine (from this district) with    next_o_id - 20 <= id < next_o_id
  // (3) Count all rows in STOCK with distinct items whose quantity is below the min_quantity threshold.
   query = fmt::format("SELECT COUNT(DISTINCT(s_i_id)) FROM {}, {} "
                      "WHERE ol_w_id = {} AND ol_d_id = {} AND ol_o_id < {} AND ol_o_id >= {} "
                      " AND s_w_id = {} AND s_i_id = ol_i_id AND s_quantity < {}", 
                      ORDER_LINE_TABLE, STOCK_TABLE, w_id, d_id, next_o_id, next_o_id - 20, w_id, min_quantity);
  //TODO: Write it as a an explicit JOIN somehow to more easily extract individual table predicates?
  // query = fmt::format("SELECT COUNT(DISTINCT(Stock.i_id)) FROM (SELECT * FROM OrderLine WHERE w_id = {} AND d_id = {} AND o_id < {} AND o_id >= {}) "
  //                     "LEFT JOIN (SELECT * FROM STOCK WHERE w_id = {} AND quantity < {}) " //This is super inefficient..
  //                     "ON Stock.i_id = OrderLine.i_id;", w_id, d_id, next_o_id, next_o_id - 20, w_id, min_quantity);

  client.Query(query, queryResult, timeout);
  uint32_t stock_count;
  deserialize(stock_count, queryResult);

  Debug("Stock Count: %u", stock_count);
  Debug("COMMIT");
  return client.Commit(timeout);
}

} // namespace tpcc_sql
