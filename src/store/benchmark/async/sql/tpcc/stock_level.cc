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

#include "store/benchmark/async/sql/tpcc/tpcc_common.h"
#include "store/benchmark/async/sql/tpcc/tpcc-sql-validation-proto.pb.h"
#include "store/common/common-proto.pb.h"
#include "store/benchmark/async/sql/tpcc/tpcc_utils.h"

namespace tpcc_sql {

SQLStockLevel::SQLStockLevel(uint32_t w_id, uint32_t d_id,
    std::mt19937 &gen) : w_id(w_id), d_id(d_id) {
  min_quantity = std::uniform_int_distribution<uint8_t>(10, 20)(gen);

  std::cerr << "STOCK_LEVEL (parallel)" << std::endl;
}

SQLStockLevel::~SQLStockLevel() {
}

void SQLStockLevel::SerializeTxnState(std::string &txnState) {
  TxnState currTxnState = TxnState();
  std::string txn_name;
  txn_name.append(BENCHMARK_NAME);
  txn_name.push_back('_');
  txn_name.append(GetBenchmarkTxnTypeName(SQL_TXN_STOCK_LEVEL));
  currTxnState.set_txn_name(txn_name);

  validation::proto::StockLevel curr_txn = validation::proto::StockLevel();
  curr_txn.set_w_id(w_id);
  curr_txn.set_d_id(d_id);
  curr_txn.set_min_quantity(min_quantity);
  std::string txn_data;
  curr_txn.SerializeToString(&txn_data);
  currTxnState.set_txn_data(txn_data);

  currTxnState.SerializeToString(&txnState);
}

} // namespace tpcc_sql
