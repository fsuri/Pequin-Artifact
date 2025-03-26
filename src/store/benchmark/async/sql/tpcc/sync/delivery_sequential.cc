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
#include "store/benchmark/async/sql/tpcc/sync/delivery.h"

#include <fmt/core.h>

#include "store/benchmark/async/sql/tpcc/tpcc_utils.h"
#include "store/benchmark/async/sql/tpcc/tpcc_common.h"
#include "store/benchmark/async/sql/tpcc/tpcc-sql-validation-proto.pb.h"

namespace tpcc_sql { 

SyncSQLDeliverySequential::SyncSQLDeliverySequential(uint32_t timeout, uint32_t w_id, uint32_t d_id,
    std::mt19937 &gen) : SyncTPCCSQLTransaction(timeout), SQLDeliverySequential(w_id, d_id, gen) {
}

SyncSQLDeliverySequential::~SyncSQLDeliverySequential() {
}

transaction_status_t SyncSQLDeliverySequential::Execute(SyncClient &client) {
  std::unique_ptr<const query_result::QueryResult> queryResult;
  std::string statement;
  std::vector<std::unique_ptr<const query_result::QueryResult>> results;

  // Process a batch of 10 new (not yet delivered) orders. Each order delivery is it's own read/write TX.
  // Low frequency
  //std::cerr << "DELIVERY TX" << std::endl;
  Debug("DELIVERY");
  Debug("Warehouse: %u", w_id);
  Debug("District: %u", d_id);
  //std::cerr << "warehouse: " << w_id << std::endl;

  std::string txnState;
  SyncSQLDeliverySequential::SerializeTxnState(txnState);

  client.Begin(timeout, txnState);

  // (1) Retrieve the row from NEW-ORDER with the lowest order id
  //     If none is found, skip delivery of an order for this district. 
  int no_o_id;


  //Issue a Point Read and Point Update to EarliestNewOrder table. (this avoids needing to find Min and then delete it)
  if(use_earliest_new_order_table){
    statement = fmt::format("SELECT * FROM {} WHERE eno_w_id = {} AND eno_d_id = {};", EARLIEST_NEW_ORDER_TABLE, w_id, d_id);
    client.Query(statement, queryResult, timeout);

    if (queryResult->empty()) {
      // Note: technically we're supposed to check each district in this warehouse  ==>> We instead are doing a TX for each district sequentially (see tpcc_client.cc)
      return client.Commit(timeout);
    }

    deserialize(no_o_id, queryResult, 0, 2); //get third col of first row (there is only 1 row, this is a point read).

   statement = fmt::format("UPDATE {} SET eno_o_id = eno_o_id + 1 WHERE eno_w_id = {} AND eno_d_id = {};", EARLIEST_NEW_ORDER_TABLE, w_id, d_id);
    client.Write(statement, timeout);
  }
  else{
    statement = fmt::format("SELECT MIN(no_o_id) FROM {} WHERE no_d_id = {} AND no_w_id = {};", NEW_ORDER_TABLE, d_id, w_id);
    //statement = fmt::format("SELECT o_id FROM NewOrder WHERE d_id = {} AND w_id = {} ORDER BY o_id ASC LIMIT 1;", d_id, w_id);
    client.Query(statement, queryResult, timeout);

    if (queryResult->empty()) {
      // Note: technically we're supposed to check each district in this warehouse  ==>> We instead are doing a TX for each district sequentially (see tpcc_client.cc)
      return client.Commit(timeout);
    }

    // (2) Delete the found NEW-ORDER row
        // Note: Pesto will turn this into a PointDelete for which no read is required.
    deserialize(no_o_id, queryResult);
    statement = fmt::format("DELETE FROM {} WHERE no_o_id = {} AND no_d_id = {} AND no_w_id = {};", NEW_ORDER_TABLE, no_o_id, d_id, w_id);
    client.Write(statement, queryResult, timeout); //This can be async. 

  }
  
  // (3) Select the corresponding row from ORDER and extract the customer id. Update the carrier id of the order.
  //statement = fmt::format("SELECT c_id FROM \"order\" WHERE id = {} AND d_id = {} AND w_id = {};", no_o_id, d_id, w_id);
  statement = fmt::format("SELECT * FROM {} WHERE o_id = {} AND o_d_id = {} AND o_w_id = {};", ORDER_TABLE, no_o_id, d_id, w_id); //Turn into * to cache for the following point Update
  
  client.Query(statement, queryResult, timeout);

  if (queryResult->empty()) {
    // already delivered all orders for this warehouse
    return client.Commit(timeout);
  }
  // int c_id;
  // deserialize(c_id, queryResult);

  OrderRow o_row;
  deserialize(o_row, queryResult);
  int c_id = o_row.get_c_id();

 
  statement = fmt::format("UPDATE {} SET o_carrier_id = {} WHERE o_id = {} AND o_d_id = {} AND o_w_id = {};", ORDER_TABLE, o_carrier_id, no_o_id, d_id, w_id);
  client.Write(statement, queryResult, timeout); //This can be async.
  Debug("  Carrier ID: %u", o_carrier_id);

  //TODO: We already know the ORDER_Lines to touch from the order id? Could just loop over o_row.ol_cnt and do point accesses.

  //Duplicate Scan version
  // // (4) Select all rows in ORDER-LINE that match the order, and update delivery dates. Retrieve total amount (sum)
  // statement = fmt::format("UPDATE {} SET ol_delivery_d = {} WHERE ol_o_id = {} AND ol_d_id = {} AND ol_w_id = {};", ORDER_LINE_TABLE, ol_delivery_d, no_o_id, d_id, w_id);
  // client.Write(statement, queryResult, timeout); //This can be async.

  //     //Note: Ideally Pesto does not Scan twice, but Caches the result set to perform the update (TODO: To make use of that, we'd have to not do the 2 statements in parallel)
  // statement = fmt::format("SELECT SUM(ol_amount) FROM {} WHERE ol_o_id = {} AND ol_d_id = {} AND ol_w_id = {};", ORDER_LINE_TABLE, no_o_id, d_id, w_id);
  // client.Query(statement, queryResult, timeout);
  // int total_amount;
  // deserialize(total_amount, queryResult);
  // Debug("Total Amount: %i", total_amount);


   // (4) Select all rows in ORDER-LINE that match the order, and update delivery dates. Retrieve total amount (sum)
  statement = fmt::format("SELECT * FROM {} WHERE ol_o_id = {} AND ol_d_id = {} AND ol_w_id = {};", ORDER_LINE_TABLE, no_o_id, d_id, w_id);  //=> Pesto client will cache this result
  client.Query(statement, queryResult, timeout, true); //cache result

  statement = fmt::format("UPDATE {} SET ol_delivery_d = {} WHERE ol_o_id = {} AND ol_d_id = {} AND ol_w_id = {};", ORDER_LINE_TABLE, ol_delivery_d, no_o_id, d_id, w_id); //=> Pesto client will do recon-read from cache
  Debug("OP: %s", statement.c_str());
  client.Write(statement, queryResult, timeout); //This can be async.

  int total_amount = 0;
  //for result in result
  Debug("Result size: %d", queryResult->size());
  for(int i = 0; i < queryResult->size(); ++i){
    OrderLineRow olr;
    deserialize(olr, queryResult, i);
    Debug("Olr amount: %i", olr.get_amount());
    total_amount += olr.get_amount();
  }
  Debug("Total Amount: %i", total_amount);



  // (5) Update the balance and delivery count of the respective customer (that issued the order)
  Debug("Customer: %u", c_id);
  statement = fmt::format("UPDATE {} SET c_balance = c_balance + {}, c_delivery_cnt = c_delivery_cnt + 1 WHERE c_id = {} AND c_d_id = {} AND c_w_id = {};", CUSTOMER_TABLE, total_amount, c_id, d_id, w_id);
  client.Write(statement, queryResult, timeout);


  Debug("COMMIT");
  return client.Commit(timeout);
}

void SyncSQLDeliverySequential::SerializeTxnState(std::string &txnState) {
  TxnState currTxnState = TxnState();
  std::string txn_name;
  txn_name.append(BENCHMARK_NAME);
  txn_name.push_back('_');
  txn_name.append(GetBenchmarkTxnTypeName(SQL_TXN_DELIVERY_SEQUENTIAL));
  currTxnState.set_txn_name(txn_name);

  validation::proto::Delivery curr_txn = validation::proto::Delivery();
  curr_txn.set_sequential(true);
  curr_txn.set_w_id(w_id);
  curr_txn.set_d_id(d_id);
  curr_txn.set_o_carrier_id(o_carrier_id);
  curr_txn.set_ol_delivery_d(ol_delivery_d);
  std::vector<TPCC_Table> est_tables = SyncSQLDeliverySequential::HeuristicFunction();
  for(const auto& value : est_tables) {
    curr_txn.add_est_tables((int)value);
  }
  std::string txn_data;
  curr_txn.SerializeToString(&txn_data);
  currTxnState.set_txn_data(txn_data);

  currTxnState.SerializeToString(&txnState);
}

std::vector<TPCC_Table> SyncSQLDeliverySequential::HeuristicFunction() {
  return {NEW_ORDER, ORDER, ORDER_LINE, CUSTOMER, EARLIEST_NEW_ORDER};
}

} // namespace tpcc_sql
