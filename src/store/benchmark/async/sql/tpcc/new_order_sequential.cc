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

#include "store/benchmark/async/sql/tpcc/tpcc_utils.h"

namespace tpcc_sql { 

SQLNewOrder::SQLNewOrder(uint32_t timeout, uint32_t w_id, uint32_t C,
    uint32_t num_warehouses, std::mt19937 &gen) :
    TPCCSQLTransaction(timeout), w_id(w_id) {

  d_id = std::uniform_int_distribution<uint32_t>(1, 10)(gen); 
  c_id = tpcc_sql::NURand(static_cast<uint32_t>(1023), static_cast<uint32_t>(1), static_cast<uint32_t>(3000), C, gen);
  ol_cnt = std::uniform_int_distribution<uint8_t>(5, 15)(gen);
  rbk = std::uniform_int_distribution<uint8_t>(1, 100)(gen);
  all_local = true;
  for (uint8_t i = 0; i < ol_cnt; ++i) {
    if (rbk == 1 && i == ol_cnt - 1) {
      o_ol_i_ids.push_back(0);
    } else {
      uint32_t i_id = tpcc_sql::NURand(static_cast<uint32_t>(8191), static_cast<uint32_t>(1), static_cast<uint32_t>(100000), C, gen); 
      
      //Avoid duplicates
      //TODO: Since we avoid duplicates, should technically adjust quantity value to account for it. Makes no difference for contention though.
      bool unique_item = unique_items.insert(i_id).second;
      if(!unique_item) continue; 
      
      o_ol_i_ids.push_back(i_id);
       
      
      //Alternatively: Pick new item if encounter duplicate.
      // uint32_t i_id = 0;
      // while(!duplicates.insert(i_id).second){
      //  i_id = tpcc_sql::NURand(static_cast<uint32_t>(8191), static_cast<uint32_t>(1), static_cast<uint32_t>(100000), C, gen); 
      // }
      // o_ol_i_ids.push_back(i_id);
    }
    uint8_t x = std::uniform_int_distribution<uint8_t>(1, 100)(gen);
    if (x == 1 && num_warehouses > 1) { //For 1% of the TXs supply from remote warehouse
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

  ol_cnt = o_ol_i_ids.size();
  UW_ASSERT(ol_cnt == o_ol_supply_w_ids.size() && ol_cnt == o_ol_quantities.size());
  //std::cerr << "All local == " << all_local << std::endl;
}

SQLNewOrder::~SQLNewOrder() {
}

transaction_status_t SQLNewOrder::Execute(SyncClient &client) {
  std::unique_ptr<const query_result::QueryResult> queryResult;
  std::string statement;
  std::vector<std::unique_ptr<const query_result::QueryResult>> results;

  //Create a new order.
  //Type: Mid-weight read-write TX, high frequency. Backbone of the workload.
  Debug("NEW_ORDER");
  std::cerr << "NEW ORDER" << std::endl;
  Debug("Warehouse: %u", w_id); 

  std::cerr << "OL_CNT: " << unsigned(ol_cnt) << std::endl;

  client.Begin(timeout);

  // (1) Retrieve row from WAREHOUSE, extract tax rate
  statement = fmt::format("SELECT * FROM {} WHERE w_id = {}", WAREHOUSE_TABLE, w_id);
  client.Query(statement, queryResult, timeout);

  WarehouseRow w_row;
  deserialize(w_row, queryResult);
  Debug("  Tax Rate: %u", w_row.get_tax());

  // (2) Retrieve row from DISTRICT, extract tax rate. 
  Debug("District: %u", d_id);
  statement = fmt::format("SELECT * FROM {} WHERE d_id = {} AND d_w_id = {}", DISTRICT_TABLE, d_id, w_id);
  client.Query(statement, queryResult, timeout);

  DistrictRow d_row;
  
  deserialize(d_row, queryResult);
  Debug("  Tax Rate: %u", d_row.get_tax());
  uint32_t o_id = d_row.get_next_o_id();
  Debug("  Order Number: %u", o_id);


  // (3) Retrieve customer row from CUSTOMER, extract discount rate, last name, and credit status.
  Debug("Customer: %u", c_id);
  statement = fmt::format("SELECT * FROM {} WHERE c_id = {} AND c_d_id = {} AND c_w_id = {}", CUSTOMER_TABLE, c_id, d_id, w_id);
  client.Query(statement, queryResult, timeout);

  CustomerRow c_row;
  deserialize(c_row, queryResult);
  Debug("  Discount: %i", c_row.get_discount());
  Debug("  Last Name: %s", c_row.get_last().c_str());
  Debug("  Credit: %s", c_row.get_credit().c_str());



  // (2.5) Increment next available order number for District
  d_row.set_next_o_id(d_row.get_next_o_id() + 1);
  statement = fmt::format("UPDATE {} SET d_next_o_id = {} WHERE d_id = {} AND d_w_id = {}", DISTRICT_TABLE, d_row.get_next_o_id(), d_id, w_id);
  client.Write(statement, queryResult, timeout);

  // (4) Insert new row into NewOrder and Order to reflect the creation of the order. 
  statement = fmt::format("INSERT INTO {} (no_o_id, no_d_id, no_w_id) VALUES ({}, {}, {});", NEW_ORDER_TABLE, o_id, d_id, w_id);
  client.Write(statement, queryResult, timeout, true); //blind_write
  
  statement = fmt::format("INSERT INTO {} (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) "
          "VALUES ({}, {}, {}, {}, {}, {}, {}, {});", ORDER_TABLE, o_id, d_id, w_id, c_id, o_entry_d, 0, ol_cnt, all_local);
  client.Write(statement, queryResult, timeout, true); //blind write


  // (7) For each ol, increase Stock year to date by requested quantity, and increment stock order count. If order is remote, increment remote cnt.
  //                  insert a new row into ORDER-LINE .
  for (size_t ol_number = 0; ol_number < ol_cnt; ++ol_number) {

    //(5) For each ol, select row from ITEM and retrieve: Price, Name, Data
    Debug("  Order Line %lu", ol_number);
    Debug("    Item: %u", o_ol_i_ids[ol_number]);
    statement = fmt::format("SELECT * FROM {} WHERE i_id = {}", ITEM_TABLE, o_ol_i_ids[ol_number]);
    client.Query(statement, queryResult, timeout);
    if(queryResult->empty()) {
      client.Abort(timeout);
      return ABORTED_USER;
    }
    else {
      ItemRow i_row;
      deserialize(i_row, queryResult);
      Debug("    Item Name: %s", i_row.get_name().c_str());

      // (6) For each ol, select row from STOCK and retrieve: Qunatity, District Number, Data
     
      Debug("  Order Line %lu", ol_number);
      Debug("    Supply Warehouse: %u", o_ol_supply_w_ids[ol_number]);
      statement = fmt::format("SELECT * FROM {} WHERE s_i_id = {} AND s_w_id = {}",
            STOCK_TABLE, o_ol_i_ids[ol_number], o_ol_supply_w_ids[ol_number]);
      client.Query(statement, queryResult, timeout);
    

      StockRow s_row;
      deserialize(s_row, queryResult);

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
      client.Write(statement, queryResult, timeout); 

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
      statement = fmt::format("INSERT INTO {} (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info) "
            "VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, '{}');", 
            ORDER_LINE_TABLE, o_id, d_id, w_id, ol_number, o_ol_i_ids[ol_number], o_ol_supply_w_ids[ol_number], 0, o_ol_quantities[ol_number], o_ol_quantities[ol_number] * i_row.get_price(), dist_info);
      client.Write(statement, queryResult, timeout, true); //blind write
    }
  }


  Debug("COMMIT");
  return client.Commit(timeout);
}

} // namespace tpcc_sql
