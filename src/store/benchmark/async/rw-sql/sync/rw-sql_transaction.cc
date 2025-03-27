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
#include "store/benchmark/async/rw-sql/sync/rw-sql_transaction.h"
#include <fmt/core.h>
#include "store/common/query_result/query_result.h"
#include <functional>

namespace rwsql {

RWSQLTransaction::RWSQLTransaction(QuerySelector *querySelector, uint64_t &numOps, std::mt19937 &rand, bool readSecondaryCondition, bool fixedRange, 
                                     int32_t value_size, uint64_t value_categories, bool readOnly, bool scanAsPoint, bool execPointScanParallel) 
    : SyncTransaction(10000), RWSQLBaseTransaction(querySelector, numOps, rand, readSecondaryCondition, fixedRange, 
      value_size, value_categories, readOnly, scanAsPoint, execPointScanParallel) {
}

RWSQLTransaction::~RWSQLTransaction() {
}

static int count = 1;

//WARNING: CURRENTLY DO NOT SUPPORT READ YOUR OWN WRITES
transaction_status_t RWSQLTransaction::Execute(SyncClient &client) {
  //Note: Semantic CC cannot help this Transaction avoid aborts. Since it does value++, all TXs that touch value must be totally ordered. 
  
  //reset Tx exec state. When avoiding redundant queries we may split into new queries. liveOps keeps track of total number of attempted queries
  liveOps = numOps;
  statements.clear();
  for (int i = 0; i < numOps; ++i) {
    secondary_values.push_back(GenerateSecondaryCondition());
  }

  Debug("Start next Transaction");

  std::string txnState;
  SerializeTxnState(txnState);

  client.Begin(timeout, txnState);

  //Execute #liveOps queries
  for(int i=0; i < liveOps; ++i){
    Debug("LiveOp: %d",i);
    Debug("starts size: %d",starts.size());
    //UW_ASSERT(liveOps <= (querySelector->numKeys)); //there should never be more ops than keys; those should've been cancelled. FIXME: new splits might only be cancelled later.

    string table_name = "t" + std::to_string(tables[i]);
    int left_bound = starts[i]; 
    int right_bound = ends[i];
    UW_ASSERT(left_bound < querySelector->numKeys && right_bound < querySelector->numKeys);

    UW_ASSERT(left_bound >= 0 && left_bound < querySelector->numKeys && right_bound >= 0 && right_bound < querySelector->numKeys);

    auto &secondary_val = secondary_values[i];

    if(scanAsPoint){
      ExecutePointStatements(client, timeout, table_name, left_bound, right_bound, secondary_val);
    }
    else{
      ExecuteScanStatement(client, timeout, table_name, left_bound, right_bound, secondary_val);
    }
    //TODO: Re-factor into Submit/Get logic so its naturally parallelizable between queries?

    //std::string statement = GenerateStatement(table_name, left_bound, right_bound);
    // statements.push_back(GenerateStatement(table_name, left_bound, right_bound));  
    // std::string &statement = statements.back();

    // Debug("Start new RW-SQL Request: %s", statement);

    // SubmitStatement(client, statement, i);
    //Note: Updates will not conflict on TableVersion -- Because we are not changing primary key, which is the search condition.  
  }

  //GetResults(client);

  
  transaction_status_t commitRes = client.Commit(timeout);

  Debug("TXN COMMIT STATUS: %d",commitRes);

  // if(count++ == 2){
  //    Panic("stop after two"); //Expectation: First TX writes something. Second Transaction will need to do sync protocol.

  // }
   // Panic("stop after one");

 
  //usleep(1000); //sleep to simulate sequential access.
  return commitRes;
}

void RWSQLTransaction::SerializeTxnState(std::string &txnState) {
  TxnState currTxnState;
  std::string txn_name;
  txn_name.append(BENCHMARK_NAME);
  txn_name.push_back('_');
  currTxnState.set_txn_name(txn_name);

  validation::proto::RWSql curr_txn;
  curr_txn.set_num_ops(static_cast<uint64_t>(numOps));
  curr_txn.set_read_only(readOnly);
  curr_txn.set_read_secondary_condition(readSecondaryCondition);
  curr_txn.set_value_size(value_size);
  curr_txn.set_value_categories(value_categories);
  curr_txn.set_scan_as_point(scanAsPoint);
  curr_txn.set_exec_point_scan_parallel(execPointScanParallel);
  curr_txn.set_num_keys(numKeys);
  for (const uint64_t &i : tables) {
    curr_txn.add_tables(i);
  }
  for (const int32_t &i : starts) {
    curr_txn.add_starts(i);
  }
  for (const int32_t &i : ends) {
    curr_txn.add_ends(i);
  }
  for(const std::pair<uint64_t, std::string> &i : secondary_values) {
    validation::proto::SecondaryPair* pair = curr_txn.add_secondary_values();
    pair->set_first(i.first);
    pair->set_second(i.second);
  }

  curr_txn.SerializeToString(currTxnState.mutable_txn_data());
  currTxnState.SerializeToString(&txnState);
}

} // namespace rw
