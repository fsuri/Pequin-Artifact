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

  query = fmt::format("SELECT FROM Order WHERE id BETWEEN {} AND {} AND d_id = {} AND w_id = {}", next_o_id - 20, next_o_id - 1, d_id, w_id);
  client.Query(query, timeout);
  client.Wait(results);

  std::map<uint32_t, uint32_t> ol_cnts;
  for (uint32_t ol_o_id = next_o_id - 20; ol_o_id < next_o_id; ++ol_o_id) {
    if (results[ol_o_id + 20 - next_o_id]->empty()) {
      Debug("  Non-existent Order %u", ol_o_id);
      continue;
    }
    tpcc::OrderRow o_row;
    deserialize(o_row, queryResult, ol_o_id + 20 - next_o_id);
    Debug("  Order Lines: %u", o_row.ol_cnt());

    ol_cnts[ol_o_id] = o_row.ol_cnt();
    query = fmt::format("SELECT FROM OrderLine WHERE o_id = {} AND d_id = {} AND w_id = {} AND number < {}", ol_o_id, d_id, w_id, o_row.ol_cnt());
    client.Query(query, timeout);
  }

  results.clear();
  client.Wait(results);
  
  std::map<uint32_t, tpcc::StockRow> stockRows;
  size_t resultsIdx = 0;
  for (const auto order_cnt : ol_cnts) {
    Debug("Order %u", order_cnt.first);
    for (size_t ol_number = 0; ol_number < order_cnt.second; ++ol_number) {
      tpcc::OrderLineRow ol_row;
      Debug("  OL %lu", ol_number);
      Debug("  Total OL index: %lu.", resultsIdx);
      if (!results[resultsIdx]->at(ol_number)) {
        Debug("  Non-existent Order Line %lu", ol_number);
        continue;
      }
      deserialize(ol_row, results[resultsIdx], ol_number);
      Debug("      Item %d", ol_row.i_id());

      if (stockRows.find(ol_row.i_id()) == stockRows.end()) {
        query = fmt::format("SELECT FROM Stock WHERE i_id = {} AND w_id = {}", ol_row.i_id(), w_id);
        client.Query(query, timeout);
      }
    }
    resultsIdx++;
  }

  results.clear();
  client.Wait(results);

  Debug("COMMIT");
  return client.Commit(timeout);
}

} // namespace tpcc_sql
