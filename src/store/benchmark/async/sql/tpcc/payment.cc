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
#include "store/benchmark/async/sql/tpcc/payment.h"

#include <sstream>
#include <fmt/core.h>

#include "store/benchmark/async/sql/tpcc/tpcc_common.h"
#include "store/benchmark/async/sql/tpcc/tpcc-sql-validation-proto.pb.h"
#include "store/common/common-proto.pb.h"
#include "store/benchmark/async/sql/tpcc/tpcc_utils.h"
  
namespace tpcc_sql {

SQLPayment::SQLPayment(uint32_t w_id, uint32_t c_c_last,
    uint32_t c_c_id, uint32_t num_warehouses, std::mt19937 &gen) : w_id(w_id), gen(gen) {
  d_id = std::uniform_int_distribution<uint32_t>(1, 10)(gen); 
  d_w_id = w_id;
  int x = std::uniform_int_distribution<int>(1, 100)(gen);
  int y = std::uniform_int_distribution<int>(1, 100)(gen);
  Debug("w_id: %u", w_id);
  Debug("this->w_id: %u", this->w_id);
  Debug("x: %d; num_warehouses: %u", x, num_warehouses);
  if (x <= 85 || num_warehouses == 1) {
    c_w_id = w_id;
    c_d_id = d_id;
  } else {
    c_w_id = std::uniform_int_distribution<uint32_t>(1, num_warehouses - 1)(gen);
    Debug("c_w_id: %u", c_w_id);
    if (c_w_id == w_id) {
      c_w_id = num_warehouses; // simple swap to ensure uniform distribution
      Debug("c_w_id: %u", c_w_id);
    }
    c_d_id = std::uniform_int_distribution<uint32_t>(1, 10)(gen);
  }
  if (y <= 60) {
    int last = tpcc_sql::NURand(255, 0, 999, static_cast<int>(c_c_last), gen);
    c_last = tpcc_sql::GenerateCustomerLastName(last);
    c_by_last_name = true;
  } else {
    c_id = tpcc_sql::NURand(1023, 1, 3000, static_cast<int>(c_c_id), gen);
    c_by_last_name = false;
  }
  h_amount = std::uniform_int_distribution<uint32_t>(100, 500000)(gen);
  h_date = std::time(0);
  // TODO: random row ID is set per transaction not per attempt of transaction
  std::cerr << "PAYMENT (parallel)" << std::endl;
}

SQLPayment::~SQLPayment() {
}


void SQLPayment::SerializeTxnState(std::string &txnState) {
  TxnState currTxnState = TxnState();
  std::string txn_name;
  txn_name.append(BENCHMARK_NAME);
  txn_name.push_back('_');
  txn_name.append(GetBenchmarkTxnTypeName(SQL_TXN_PAYMENT));
  currTxnState.set_txn_name(txn_name);

  validation::proto::Payment curr_txn = validation::proto::Payment();
  curr_txn.set_w_id(w_id);
  curr_txn.set_d_id(d_id);
  curr_txn.set_d_w_id(d_w_id);
  curr_txn.set_c_w_id(c_w_id);
  curr_txn.set_c_d_id(c_d_id);
  curr_txn.set_c_id(c_id);
  curr_txn.set_h_amount(h_amount);
  curr_txn.set_h_date(h_date);
  curr_txn.set_sequential(false);
  curr_txn.set_random_row_id(random_row_id);
  curr_txn.set_c_by_last_name(c_by_last_name);
  curr_txn.set_c_last(c_last);
  std::string txn_data;
  curr_txn.SerializeToString(&txn_data);
  currTxnState.set_txn_data(txn_data);

  currTxnState.SerializeToString(&txnState);
}

} // namespace tpcc_sql
