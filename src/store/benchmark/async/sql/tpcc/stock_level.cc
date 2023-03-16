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

#include "store/benchmark/async/tpcc/tpcc_utils.h"

namespace tpcc_sql {

SQLStockLevel::SQLStockLevel(uint32_t timeout, uint32_t w_id, uint32_t d_id,
    std::mt19937 &gen) : TPCCSQLTransaction(timeout), w_id(w_id), d_id(d_id) {
  min_quantity = std::uniform_int_distribution<uint8_t>(10, 20)(gen);
}

SQLStockLevel::~SQLStockLevel() {
}

transaction_status_t SQLStockLevel::Execute(SyncClient &client) {
  query_result::QueryResult *queryResult;
  std::string query;
  std::vector<query_result::QueryResult*> results;

  Debug("STOCK_LEVEL");
  Debug("Warehouse: %u", w_id);
  Debug("District: %u", d_id);
  //std::cerr << "warehouse: " << w_id << std::endl;

  client.Begin(timeout);

  query = "SELECT FROM District WHERE pk = ";
  std::string d_key = tpcc::DistrictRowKey(w_id, d_id);
  query.append(d_key);
  client.Query(query, queryResult, timeout);
  tpcc::DistrictRow d_row;
  deserialize(d_row, queryResult);

  uint32_t next_o_id = d_row.next_o_id();
  Debug("Orders: %u-%u", next_o_id - 20, next_o_id - 1);

  for (size_t ol_o_id = next_o_id - 20; ol_o_id < next_o_id; ++ol_o_id) {
    Debug("Order %lu", ol_o_id);
    query = "SELECT FROM Order WHERE pk = ";
    query.append(tpcc::OrderRowKey(w_id, d_id, ol_o_id));
    client.Query(query, timeout);
  }

  client.Wait(results);

  // Checkpoint

  std::map<uint32_t, uint32_t> ol_cnts;
  for (uint32_t ol_o_id = next_o_id - 20; ol_o_id < next_o_id; ++ol_o_id) {
    if (results[ol_o_id + 20 - next_o_id]->empty()) {
      Debug("  Non-existent Order %u", ol_o_id);
      continue;
    }
    tpcc::OrderRow o_row;
    deserialize(o_row, results[ol_o_id + 20 - next_o_id]);
    Debug("  Order Lines: %u", o_row.ol_cnt());

    ol_cnts[ol_o_id] = o_row.ol_cnt();
    for (size_t ol_number = 0; ol_number < o_row.ol_cnt(); ++ol_number) {
      Debug("    OL %lu", ol_number);
      query = "SELECT FROM OrderLine WHERE pk = ";
      std::string ol_key = tpcc::OrderLineRowKey(w_id, d_id, ol_o_id, ol_number);
      query.append(ol_key);
      client.Query(query, timeout);
    }
  }

  results.clear();
  client.Wait(results);
  // CHECKPOINT
  std::map<uint32_t, tpcc::StockRow> stockRows;
  size_t resultsIdx = 0;
  for (const auto order_cnt : ol_cnts) {
    Debug("Order %u", order_cnt.first);
    for (size_t ol_number = 0; ol_number < order_cnt.second; ++ol_number) {
      tpcc::OrderLineRow ol_row;
      Debug("  OL %lu", ol_number);
      Debug("  Total OL index: %lu.", resultsIdx);
      if (results[resultsIdx]->empty()) {
        Debug("  Non-existent Order Line %lu", ol_number);
        continue;
      }
      deserialize(ol_row, results[resultsIdx]);
      resultsIdx++;
      Debug("      Item %d", ol_row.i_id());

      if (stockRows.find(ol_row.i_id()) == stockRows.end()) {
        query = "SELECT FROM Stock WHERE pk = ";
        query.append(tpcc::StockRowKey(w_id, ol_row.i_id()));
        client.Query(query, timeout);
      }
    }
  }

  results.clear();
  client.Wait(results);

  Debug("COMMIT");
  return client.Commit(timeout);
}

} // namespace tpcc_sql
