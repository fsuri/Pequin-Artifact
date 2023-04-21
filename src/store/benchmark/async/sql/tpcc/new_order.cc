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
#include "store/benchmark/async/sql/tpcc/new_order.h"

#include <fmt/core.h>

#include "store/benchmark/async/tpcc/tpcc_utils.h"

namespace tpcc_sql {

SQLNewOrder::SQLNewOrder(uint32_t timeout, uint32_t w_id, uint32_t C,
    uint32_t num_warehouses, std::mt19937 &gen) :
    TPCCSQLTransaction(timeout), w_id(w_id) {
  d_id = std::uniform_int_distribution<uint32_t>(1, 10)(gen); 
  c_id = tpcc::NURand(static_cast<uint32_t>(1023), static_cast<uint32_t>(1), static_cast<uint32_t>(3000), C, gen);
  ol_cnt = std::uniform_int_distribution<uint8_t>(5, 15)(gen);
  rbk = std::uniform_int_distribution<uint8_t>(1, 100)(gen);
  all_local = true;
  for (uint8_t i = 0; i < ol_cnt; ++i) {
    if (rbk == 1 && i == ol_cnt - 1) {
      o_ol_i_ids.push_back(0);
    } else {
      o_ol_i_ids.push_back(tpcc::NURand(static_cast<uint32_t>(8191), static_cast<uint32_t>(1), static_cast<uint32_t>(100000), C, gen));
    }
    uint8_t x = std::uniform_int_distribution<uint8_t>(1, 100)(gen);
    if (x == 1 && num_warehouses > 1) {
      uint32_t remote_w_id = std::uniform_int_distribution<uint32_t>(1, num_warehouses - 1)(gen);
      if (remote_w_id == w_id) {
        remote_w_id = num_warehouses; // simple swap to ensure uniform distribution
      }
      o_ol_supply_w_ids.push_back(remote_w_id);
      all_local = false;
    } else {
      o_ol_supply_w_ids.push_back(w_id);
    }
    o_ol_quantities.push_back(std::uniform_int_distribution<uint8_t>(1, 10)(gen));
  }
  o_entry_d = std::time(0);
}

SQLNewOrder::~SQLNewOrder() {
}

transaction_status_t SQLNewOrder::Execute(SyncClient &client) {
  std::unique_ptr<const query_result::QueryResult> queryResult;
  std::string statement;
  std::vector<std::unique_ptr<const query_result::QueryResult>> results;

  Debug("NEW_ORDER");
  Debug("Warehouse: %u", w_id);

  client.Begin(timeout);

  statement = fmt::format("SELECT FROM Warehouse WHERE id = '{}'", w_id);
  client.Query(statement, timeout);
  Debug("District: %u", d_id);
  statement = fmt::format("SELECT FROM District WHERE id = '{}' AND w_id = '{}'", d_id, w_id);
  client.Query(statement, timeout);
  Debug("Customer: %u", c_id);
  statement = fmt::format("SELECT FROM Customer WHERE id = '{}' AND d_id = '{}' AND w_id = '{}'",
                      c_id, d_id, w_id);
  client.Query(statement, timeout);

  client.Wait(results);

  tpcc::WarehouseRow w_row;
  deserialize(w_row, results[0]);
  Debug("  Tax Rate: %u", w_row.tax());

  tpcc::DistrictRow d_row;
  deserialize(d_row, results[1]);
  Debug("  Tax Rate: %u", d_row.tax());
  uint32_t o_id = d_row.next_o_id();
  Debug("  Order Number: %u", o_id);

  d_row.set_next_o_id(d_row.next_o_id() + 1);
  statement = fmt::format("UPDATE District SET next_o_id = '{}' WHERE id = '{}' AND w_id = '{}'",
                      d_row.next_o_id(), d_id, w_id);
  client.Write(statement, queryResult, timeout);

  tpcc::CustomerRow c_row;
  deserialize(c_row, results[2]);
  Debug("  Discount: %i", c_row.discount());
  Debug("  Last Name: %s", c_row.last().c_str());
  Debug("  Credit: %s", c_row.credit().c_str());

  results.clear();

  statement = fmt::format("INSERT INTO NewOrder (o_id, d_id, w_id)\n"
            "VALUES ('{}', '{}', '{}');", 
            o_id, d_id, w_id);
  client.Write(statement, queryResult, timeout);

  statement = fmt::format("INSERT INTO Order (id, d_id, w_id, c_id, entry_d, carrier_id, ol_cnt, all_local)\n"
          "VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}');", 
          o_id, d_id, w_id, c_id, o_entry_d, 0, ol_cnt, all_local);
  client.Write(statement, queryResult, timeout);

  statement = fmt::format("INSERT INTO OrderByCustomer (w_id, d_id, c_id, o_id)\n"
        "VALUES ('{}', '{}', '{}', '{}');", 
        w_id, d_id, c_id, o_id);
  client.Write(statement, queryResult, timeout);

  for (size_t ol_number = 0; ol_number < ol_cnt; ++ol_number) {
    Debug("  Order Line %lu", ol_number);
    Debug("    Item: %u", o_ol_i_ids[ol_number]);
    statement = fmt::format("SELECT FROM Item WHERE id = '{}'", o_ol_i_ids[ol_number]);
    client.Query(statement, timeout);
  }

  for (size_t ol_number = 0; ol_number < ol_cnt; ++ol_number) {
    Debug("  Order Line %lu", ol_number);
    Debug("    Supply Warehouse: %u", o_ol_supply_w_ids[ol_number]);
    statement = fmt::format("SELECT FROM Stock WHERE i_id = '{}' AND w_id = '{}'",
          o_ol_i_ids[ol_number], o_ol_supply_w_ids[ol_number]);
    client.Query(statement, timeout);
  }

  client.Wait(results);

  for (size_t ol_number = 0; ol_number < ol_cnt; ++ol_number) {
    if (results[ol_number]->empty()) {
      client.Abort(timeout);
      return ABORTED_USER;
    } else {
      tpcc::ItemRow i_row;
      deserialize(i_row, results[ol_number]);
      Debug("    Item Name: %s", i_row.name().c_str());

      tpcc::StockRow s_row;
      deserialize(s_row, results[ol_number + ol_cnt]);

      if (s_row.quantity() - o_ol_quantities[ol_number] >= 10) {
        s_row.set_quantity(s_row.quantity() - o_ol_quantities[ol_number]);
      } else {
        s_row.set_quantity(s_row.quantity() - o_ol_quantities[ol_number] + 91);
      }
      Debug("    Quantity: %u", o_ol_quantities[ol_number]);
      s_row.set_ytd(s_row.ytd() + o_ol_quantities[ol_number]);
      s_row.set_order_cnt(s_row.order_cnt() + 1);
      Debug("    Remaining Quantity: %u", s_row.quantity());
      Debug("    YTD: %u", s_row.ytd());
      Debug("    Order Count: %u", s_row.order_cnt());
      if (w_id != o_ol_supply_w_ids[ol_number]) {
        s_row.set_remote_cnt(s_row.remote_cnt() + 1);
      }
      statement = fmt::format("UPDATE Stock\n" 
              "SET quantity = '{}', ytd = '{}', order_cnt = '{}', remote_cnt = '{}'\n"
              "WHERE i_id = '{}' AND w_id = '{}'",
          s_row.quantity(), s_row.ytd(), s_row.order_cnt(), s_row.remote_cnt(),
          o_ol_i_ids[ol_number], o_ol_supply_w_ids[ol_number]);
      client.Write(statement, queryResult, timeout);

      std::string dist_info;
      switch (d_id) {
        case 1:
          dist_info = s_row.dist_01();
          break;
        case 2:
          dist_info = s_row.dist_02();
          break;
        case 3:
          dist_info = s_row.dist_03();
          break;
        case 4:
          dist_info = s_row.dist_04();
          break;
        case 5:
          dist_info = s_row.dist_05();
          break;
        case 6:
          dist_info = s_row.dist_06();
          break;
        case 7:
          dist_info = s_row.dist_07();
          break;
        case 8:
          dist_info = s_row.dist_08();
          break;
        case 9:
          dist_info = s_row.dist_09();
          break;
        case 10:
          dist_info = s_row.dist_10();
          break;
        default:
          NOT_REACHABLE();
      }
      statement = fmt::format("INSERT INTO OrderLine "
            "(o_id, d_id, w_id, number, i_id, supply_w_id, delivery_d, quantity, amount, dist_info)\n"
            "VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}');", 
            o_id, d_id, w_id, ol_number, o_ol_i_ids[ol_number], o_ol_supply_w_ids[ol_number], 0,
            o_ol_quantities[ol_number], o_ol_quantities[ol_number] * i_row.price(), dist_info);
      client.Write(statement, queryResult, timeout);
    }
  }

  Debug("COMMIT");
  return client.Commit(timeout);
}

} // namespace tpcc_sql
