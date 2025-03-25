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
#include "store/benchmark/async/sql/tpcc/sync/new_order.h"

#include <fmt/core.h>

#include "store/benchmark/async/sql/tpcc/tpcc_utils.h"
#include "store/benchmark/async/sql/tpcc/tpcc_common.h"
#include "store/benchmark/async/sql/tpcc/tpcc-sql-validation-proto.pb.h"

namespace tpcc_sql { 

SyncSQLNewOrderSequential::SyncSQLNewOrderSequential(uint32_t timeout, uint32_t w_id, uint32_t C,
    uint32_t num_warehouses, std::mt19937 &gen) : SyncTPCCSQLTransaction(timeout),
    SQLNewOrderSequential(w_id, C, num_warehouses, gen) {
}

SyncSQLNewOrderSequential::~SyncSQLNewOrderSequential() {
}

transaction_status_t SyncSQLNewOrderSequential::Execute(SyncClient &client) {
  std::unique_ptr<const query_result::QueryResult> queryResult;
  std::string statement;
  std::vector<std::unique_ptr<const query_result::QueryResult>> results;

  //Create a new order.
  //Type: Mid-weight read-write TX, high frequency. Backbone of the workload.
  Debug("NEW_ORDER");
  //std::cerr << "NEW_ORDER" << std::endl;
  Debug("Warehouse: %u", w_id); 

  // std::cerr << "OL_CNT: " << unsigned(ol_cnt) << std::endl;
  std::string txnState;
  SyncSQLNewOrderSequential::SerializeTxnState(txnState);

  client.Begin(timeout, txnState);

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
  UW_ASSERT(o_id > 2100);


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
  statement = fmt::format("INSERT INTO {} (no_w_id, no_d_id, no_o_id) VALUES ({}, {}, {})", NEW_ORDER_TABLE, w_id, d_id, o_id);
  client.Write(statement, queryResult, timeout, true); //blind_write
  
  statement = fmt::format("INSERT INTO {} (o_w_id, o_d_id, o_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) "
          "VALUES ({}, {}, {}, {}, {}, {}, {}, {})", ORDER_TABLE, w_id, d_id, o_id, c_id, o_entry_d, 0, ol_cnt, all_local);
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
      Notice("Triggering NewOrder Abort");
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
      statement = fmt::format("INSERT INTO {} (ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info) "
            "VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, '{}')", 
            ORDER_LINE_TABLE, w_id, d_id, o_id, ol_number, o_ol_i_ids[ol_number], o_ol_supply_w_ids[ol_number], 0, o_ol_quantities[ol_number], o_ol_quantities[ol_number] * i_row.get_price(), dist_info);
      client.Write(statement, queryResult, timeout, true); //blind write
    }
  }


  Debug("COMMIT");
  return client.Commit(timeout);
}

void SyncSQLNewOrderSequential::SerializeTxnState(std::string &txnState) {
  TxnState currTxnState = TxnState();
  std::string txn_name;
  txn_name.append(BENCHMARK_NAME);
  txn_name.push_back('_');
  txn_name.append(GetBenchmarkTxnTypeName(SQL_TXN_NEW_ORDER_SEQUENTIAL));
  currTxnState.set_txn_name(txn_name);

  validation::proto::NewOrder curr_txn = validation::proto::NewOrder();
  curr_txn.set_w_id(w_id);
  curr_txn.set_d_id(d_id);
  curr_txn.set_c_id(c_id);
  curr_txn.set_ol_cnt(ol_cnt);
  curr_txn.set_rbk(rbk);
  curr_txn.set_sequential(true);
  *curr_txn.mutable_o_ol_i_ids() = {o_ol_i_ids.begin(), o_ol_i_ids.end()};
  *curr_txn.mutable_o_ol_supply_w_ids() = {o_ol_supply_w_ids.begin(), o_ol_supply_w_ids.end()};
  *curr_txn.mutable_o_ol_quantities() = {o_ol_quantities.begin(), o_ol_quantities.end()};
  *curr_txn.mutable_unique_items() = {unique_items.begin(), unique_items.end()};
  curr_txn.set_o_entry_d(o_entry_d);
  curr_txn.set_all_local(all_local);
  std::vector<TPCC_Table> est_tables = SyncSQLNewOrderSequential::HeuristicFunction();
  for(const auto& value : est_tables) {
    curr_txn.add_est_tables((int)value);
  }

  std::string txn_data;
  curr_txn.SerializeToString(&txn_data);
  currTxnState.set_txn_data(txn_data);

  currTxnState.SerializeToString(&txnState);
}


std::vector<TPCC_Table> SyncSQLNewOrderSequential::HeuristicFunction() {
  return {DISTRICT, NEW_ORDER, ORDER, STOCK, ORDER_LINE};
}

} // namespace tpcc_sql
