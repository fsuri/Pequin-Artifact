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

#include "store/benchmark/async/tpcc/tpcc_utils.h"

namespace tpcc_sql {

SQLStockLevel::SQLStockLevel(uint32_t timeout, uint32_t w_id, uint32_t d_id,
    std::mt19937 &gen) : TPCCSQLTransaction(timeout), w_id(w_id), d_id(d_id) {
  min_quantity = std::uniform_int_distribution<uint8_t>(10, 20)(gen);
}

SQLStockLevel::~SQLStockLevel() {
}

transaction_status_t SQLStockLevel::Execute(SyncClient &client) {
  const query_result::QueryResult *queryResult;
  std::string query;
  std::vector<const query_result::QueryResult*> results;

  Debug("STOCK_LEVEL");
  Debug("Warehouse: %u", w_id);
  Debug("District: %u", d_id);
  //std::cerr << "warehouse: " << w_id << std::endl;

  client.Begin(timeout);

  query = fmt::format("SELECT next_o_id FROM District WHERE id = {} AND w_id = {}", d_id, w_id);
  client.Query(query, queryResult, timeout);
  uint32_t next_o_id;
  deserialize(next_o_id, queryResult);
  Debug("Orders: %u-%u", next_o_id - 20, next_o_id - 1);

  query = fmt::format("SELECT COUNT(DISTINCT(i_id)) FROM OrderLine, Stock WHERE OrderLine.w_id = {} AND OrderLine.d_id = {} "
                      "AND OrderLine.o_id < {} AND OrderLine.o_id >= {} AND Stock.w_id = {} AND Stock.i_id = OrderLine.i_id "
                      "AND Stock.quantity < {}", w_id, d_id, next_o_id, 
                      next_o_id - 20, w_id, min_quantity);
  client.Query(query, queryResult, timeout);
  uint32_t stock_count;
  deserialize(stock_count, queryResult);

  Debug("Stock Count: %u", stock_count);
  Debug("COMMIT");
  return client.Commit(timeout);
}

} // namespace tpcc_sql
