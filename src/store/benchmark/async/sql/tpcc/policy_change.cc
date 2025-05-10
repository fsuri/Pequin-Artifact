/***********************************************************************
 *
 * Copyright 2024 Austin Li <atl63@cornell.edu>
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

#include <fmt/core.h>

#include "store/benchmark/async/sql/tpcc/policy_change.h"
#include "store/benchmark/async/sql/tpcc/tpcc_utils.h"
#include "store/benchmark/async/sql/tpcc/tpcc_common.h"
#include "store/benchmark/async/sql/tpcc/tpcc-sql-validation-proto.pb.h"
#include "store/common/common-proto.pb.h"


namespace tpcc_sql {

PolicyChange::PolicyChange(uint32_t w_id) : w_id(w_id) {
  randWeight = std::uniform_int_distribution<uint32_t>(1, 3)(GetRand());
  std::cerr << "Changing policy p0 to weight " << randWeight << " for warehouse " << w_id << std::endl;
}

PolicyChange::PolicyChange(uint32_t w_id, uint32_t policy_weight) : w_id(w_id), randWeight(policy_weight) {
  std::cerr << "Changing policy p0 to weight " << randWeight << " for warehouse " << w_id << std::endl;
}

PolicyChange::~PolicyChange() {
}

void PolicyChange::SerializeTxnState(std::string &txnState) {
  TxnState currTxnState;
  std::string txn_name;
  txn_name.append(BENCHMARK_NAME);
  txn_name.push_back('_');
  txn_name.append(GetBenchmarkTxnTypeName(SQL_TXN_POLICY_CHANGE));
  currTxnState.set_txn_name(txn_name);

  validation::proto::PolicyChange curr_txn = validation::proto::PolicyChange();
  curr_txn.set_w_id(w_id);
  curr_txn.set_random_weight(randWeight);
  std::string txn_data;
  curr_txn.SerializeToString(&txn_data);
  currTxnState.set_txn_data(txn_data);

  currTxnState.SerializeToString(&txnState);
}

}
