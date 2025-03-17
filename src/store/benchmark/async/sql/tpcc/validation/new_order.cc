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
#include "store/benchmark/async/sql/tpcc/validation/new_order.h"

#include <fmt/core.h>

#include "store/benchmark/async/sql/tpcc/tpcc_utils.h"

namespace tpcc_sql {

ValidationSQLNewOrder::ValidationSQLNewOrder(uint32_t timeout, uint32_t w_id, uint32_t C,
    uint32_t num_warehouses, std::mt19937 &gen) :
    ValidationTPCCSQLTransaction(timeout), SQLNewOrder(w_id, C, num_warehouses, gen) {
}

ValidationSQLNewOrder::ValidationSQLNewOrder(uint32_t timeout, validation::proto::NewOrder valNewOrderMsg) :
  ValidationTPCCSQLTransaction(timeout) {
  w_id = valNewOrderMsg.w_id();
  d_id = valNewOrderMsg.d_id();
  c_id = valNewOrderMsg.c_id();
  ol_cnt = valNewOrderMsg.ol_cnt();
  rbk = valNewOrderMsg.rbk();
  o_ol_i_ids = std::vector(valNewOrderMsg.o_ol_i_ids().begin(), valNewOrderMsg.o_ol_i_ids().end());
  o_ol_supply_w_ids = std::vector(valNewOrderMsg.o_ol_supply_w_ids().begin(), valNewOrderMsg.o_ol_supply_w_ids().end());
  // protobuf only has uint32 type, but here we only need uint8_t in the vector
  o_ol_quantities = std::vector<uint8_t>();
  for (int i = 0; i < valNewOrderMsg.o_ol_quantities().size(); i++) {
    o_ol_quantities.push_back(valNewOrderMsg.o_ol_quantities(i) & 0xFF);
  }
  unique_items = std::set(valNewOrderMsg.unique_items().begin(), valNewOrderMsg.unique_items().end());
  o_entry_d = valNewOrderMsg.o_entry_d();
  all_local = valNewOrderMsg.all_local();
}

ValidationSQLNewOrder::~ValidationSQLNewOrder() {
} 
 
transaction_status_t ValidationSQLNewOrder::Validate(SyncClient &client) {
  std::unique_ptr<const query_result::QueryResult> queryResult;
  std::string statement;
  std::vector<std::unique_ptr<const query_result::QueryResult>> results;

  //Create a new order.
  //Type: Mid-weight read-write TX, high frequency. Backbone of the workload.
  Debug("NEW_ORDER (parallel)"); 
  
  Debug("Warehouse: %u", w_id);

  client.Begin(timeout);

  // (1) Retrieve row from WAREHOUSE, extract tax rate
  statement = fmt::format("SELECT * FROM {} WHERE w_id = {}", WAREHOUSE_TABLE, w_id);
  client.Query(statement, timeout);

  // (2) Retrieve row from DISTRICT, extract tax rate. 
  Debug("District: %u", d_id);
  statement = fmt::format("SELECT * FROM {} WHERE d_id = {} AND d_w_id = {}", DISTRICT_TABLE, d_id, w_id);
  client.Query(statement, timeout);


  // (3) Retrieve customer row from CUSTOMER, extract discount rate, last name, and credit status.
  Debug("Customer: %u", c_id);
  statement = fmt::format("SELECT * FROM {} WHERE c_id = {} AND c_d_id = {} AND c_w_id = {}", CUSTOMER_TABLE, c_id, d_id, w_id);
  client.Query(statement, timeout);

  client.Wait(results);

  WarehouseRow w_row;
  deserialize(w_row, results[0]);
  Debug("  Tax Rate: %u", w_row.get_tax());

  DistrictRow d_row;
  deserialize(d_row, results[1]);
  Debug("  Tax Rate: %u", d_row.get_tax());
  uint32_t o_id = d_row.get_next_o_id();
  Debug("  Order Number: %u", o_id);
  UW_ASSERT(o_id > 2100);

  CustomerRow c_row;
  deserialize(c_row, results[2]);
  Debug("  Discount: %i", c_row.get_discount());
  Debug("  Last Name: %s", c_row.get_last().c_str());
  Debug("  Credit: %s", c_row.get_credit().c_str());

  results.clear();

  // (2.5) Increment next available order number for District
  d_row.set_next_o_id(d_row.get_next_o_id() + 1);
  statement = fmt::format("UPDATE {} SET d_next_o_id = {} WHERE d_id = {} AND d_w_id = {}", DISTRICT_TABLE, d_row.get_next_o_id(), d_id, w_id);
  client.Write(statement, timeout, true); //async

  // (4) Insert new row into NewOrder and Order to reflect the creation of the order. 
  //statement = fmt::format("INSERT INTO {} (no_o_id, no_d_id, no_w_id) VALUES ({}, {}, {})", NEW_ORDER_TABLE, o_id, d_id, w_id);
  statement = fmt::format("INSERT INTO {} (no_w_id, no_d_id, no_o_id) VALUES ({}, {}, {})", NEW_ORDER_TABLE, w_id, d_id, o_id);
  client.Write(statement, timeout, true, true); //async, blind_write

  
  // statement = fmt::format("INSERT INTO {} (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) "
  //         "VALUES ({}, {}, {}, {}, {}, {}, {}, {})", ORDER_TABLE, o_id, d_id, w_id, c_id, o_entry_d, 0, ol_cnt, all_local);
  statement = fmt::format("INSERT INTO {} (o_w_id, o_d_id, o_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) "
          "VALUES ({}, {}, {}, {}, {}, {}, {}, {})", ORDER_TABLE, w_id, d_id, o_id, c_id, o_entry_d, 0, ol_cnt, all_local);
  client.Write(statement, timeout, true, true); //async, blind_write


  // (5) For each ol, select row from ITEM and retrieve: Price, Name, Data
  for (size_t ol_number = 0; ol_number < ol_cnt; ++ol_number) {
    Debug("  Order Line %lu", ol_number);
    Debug("    Item: %u", o_ol_i_ids[ol_number]);
    statement = fmt::format("SELECT * FROM {} WHERE i_id = {}", ITEM_TABLE, o_ol_i_ids[ol_number]);
    client.Query(statement, timeout);
  }

  // (6) For each ol, select row from STOCK and retrieve: Qunatity, District Number, Data
  for (size_t ol_number = 0; ol_number < ol_cnt; ++ol_number) {
    Debug("  Order Line %lu", ol_number);
    Debug("    Supply Warehouse: %u", o_ol_supply_w_ids[ol_number]);
    statement = fmt::format("SELECT * FROM {} WHERE s_i_id = {} AND s_w_id = {}",
          STOCK_TABLE, o_ol_i_ids[ol_number], o_ol_supply_w_ids[ol_number]);
    client.Query(statement, timeout);
  }

  client.Wait(results);

  // (7) For each ol, increase Stock year to date by requested quantity, and increment stock order count. If order is remote, increment remote cnt.
  //                  insert a new row into ORDER-LINE .
  for (size_t ol_number = 0; ol_number < ol_cnt; ++ol_number) {
    if (results[ol_number]->empty()) {  // (4.5) If not found codition -> Abort and rollback TX.
      client.Abort(timeout);
      return ABORTED_USER;
    } else {
      ItemRow i_row;
      deserialize(i_row, results[ol_number]);
      Debug("    Item Name: %s", i_row.get_name().c_str());
      if(i_row.get_price() <= 0){
        Warning("Item row has price: %d", i_row.get_price());
        Panic("Invalid item row");
      }

      StockRow s_row;
      deserialize(s_row, results[ol_number + ol_cnt]);

      // (6.5) If available quantity exceeds requested quantity by more than 10, just reduce available quant by the requested amount.
      // Otherwise, add 91 new items.
      if (s_row.get_quantity() - o_ol_quantities[ol_number] >= 10) {
        s_row.set_quantity(s_row.get_quantity() - o_ol_quantities[ol_number]); 
      } else {
        s_row.set_quantity(s_row.get_quantity() - o_ol_quantities[ol_number] + 91);
      }
      Debug("    Quantity: %u", o_ol_quantities[ol_number]);
      s_row.set_ytd(s_row.get_ytd() + o_ol_quantities[ol_number]);
      s_row.set_order_cnt(s_row.get_order_cnt() + 1);
      Debug("    Remaining Quantity: %u", s_row.get_quantity());
      Debug("    YTD: %u", s_row.get_ytd());
      Debug("    Order Count: %u", s_row.get_order_cnt());
      if (w_id != o_ol_supply_w_ids[ol_number]) {
        s_row.set_remote_cnt(s_row.get_remote_cnt() + 1);
      }
      statement = fmt::format("UPDATE {} SET s_quantity = {}, s_ytd = {}, s_order_cnt = {}, s_remote_cnt = {} WHERE s_i_id = {} AND s_w_id = {}",
          STOCK_TABLE, s_row.get_quantity(), s_row.get_ytd(), s_row.get_order_cnt(), s_row.get_remote_cnt(), o_ol_i_ids[ol_number], o_ol_supply_w_ids[ol_number]);
      client.Write(statement, timeout, true); 

      std::string dist_info;
      switch (d_id) {
        case 1:
          dist_info = s_row.get_dist_01();
          break;
        case 2:
          dist_info = s_row.get_dist_02();
          break;
        case 3:
          dist_info = s_row.get_dist_03();
          break;
        case 4:
          dist_info = s_row.get_dist_04();
          break;
        case 5:
          dist_info = s_row.get_dist_05();
          break;
        case 6:
          dist_info = s_row.get_dist_06();
          break;
        case 7:
          dist_info = s_row.get_dist_07();
          break;
        case 8:
          dist_info = s_row.get_dist_08();
          break;
        case 9:
          dist_info = s_row.get_dist_09();
          break;
        case 10:
          dist_info = s_row.get_dist_10();
          break;
        default:
          NOT_REACHABLE();
      }
      // statement = fmt::format("INSERT INTO {} (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info) "
      //       "VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, '{}')", 
      //       ORDER_LINE_TABLE, o_id, d_id, w_id, ol_number, o_ol_i_ids[ol_number], o_ol_supply_w_ids[ol_number], 0, o_ol_quantities[ol_number], o_ol_quantities[ol_number] * i_row.get_price(), dist_info);
      statement = fmt::format("INSERT INTO {} (ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info) "
            "VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, '{}')", 
            ORDER_LINE_TABLE, w_id, d_id, o_id, ol_number, o_ol_i_ids[ol_number], o_ol_supply_w_ids[ol_number], 0, o_ol_quantities[ol_number], o_ol_quantities[ol_number] * i_row.get_price(), dist_info);
      client.Write(statement, timeout, true, true); //async, blind write
    }
  }

  client.asyncWait();

  Debug("COMMIT");
  return client.Commit(timeout);
}

} // namespace tpcc_sql
