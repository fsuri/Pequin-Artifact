/***********************************************************************
 *
 * Copyright 2025 Austin Li <atl63@cornel.edu>
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
#ifndef RW_SQL_BASE_TRANSACTION_H
#define RW_SQL_BASE_TRANSACTION_H

#include "store/benchmark/async/common/key_selector.h"
#include "store/common/frontend/sync_client.h"

#include <vector>

namespace rwsql {

const std::string BENCHMARK_NAME = "rwsql";

static bool AVOID_DUPLICATE_READS = true; 
static bool POINT_READS_ENABLED = true;
static bool PARALLEL_QUERIES = true;
static bool DISABLE_WRAP_AROUND = false;


inline int mod(int &x, const int &N){
    return (x % N + N) %N;
}

class RWSQLBaseTransaction {
 public:
  RWSQLBaseTransaction(QuerySelector *querySelector, uint64_t &numOps, std::mt19937 &rand, bool readSecondaryCondition, bool fixedRange, 
                   int32_t value_size, uint64_t value_categories, bool readOnly=false, bool scanAsPoint=false, bool execPointScanParallel=false);
  RWSQLBaseTransaction(const size_t &numOps, bool readSecondaryCondition, int32_t numKeys, int32_t value_size, uint64_t value_categories,std::mt19937 &rand,
    bool readOnly=false, bool scanAsPoint=false, bool execPointScanParallel=false) : numOps(numOps), readSecondaryCondition(readSecondaryCondition),
    numKeys(numKeys), value_size(value_size), value_categories(value_categories), readOnly(readOnly), scanAsPoint(scanAsPoint),
    execPointScanParallel(execPointScanParallel), rand(rand) {}; // initializing rand to avoid initialization error (rand is not used in validation txn)
  virtual ~RWSQLBaseTransaction();

  inline const std::vector<int> getKeyIdxs() const {
    return keyIdxs;
  }
 protected:
  std::string GenerateStatement(const std::string &table_name, int &left_bound, int &right_bound);
  void SubmitStatement(SyncClient &client, uint32_t timeout, std::string &statement, const int &i);
  void GetResults(SyncClient &client, uint32_t timeout);
  bool AdjustBounds(int &left, int &right, uint64_t table, size_t &liveOps, std::vector<std::pair<int, int>> &past_ranges);
  bool AdjustBounds_manual(int &left, int &right, uint64_t table, size_t &liveOps, std::vector<std::pair<int, int>> &past_ranges);

  std::pair<uint64_t, std::string> GenerateSecondaryCondition();
  void ExecuteScanStatement(SyncClient &client, uint32_t timeout, const std::string &table_name, int &left_bound, int &right_bound,
    const std::pair<uint64_t, std::string> &cond_pair);
  void ExecutePointStatements(SyncClient &client, uint32_t timeout, const std::string &table_name, int &left_bound, int &right_bound,
    const std::pair<uint64_t, std::string> &cond_pair);
  void ProcessPointResult(SyncClient &client, uint32_t timeout, const std::string &table_name, const int &key, std::unique_ptr<const query_result::QueryResult> &queryResult, const std::pair<uint64_t, std::string> &cond_pair);
  void Update(SyncClient &client, uint32_t timeout, const std::string &table_name, const int &key, std::unique_ptr<const query_result::QueryResult> &queryResult, uint64_t row);
  
  inline const std::string &GetKey(int i) const {
    return keySelector->GetKey(keyIdxs[i]);
  }
  
  //inline const size_t GetNumOps() const { return numOps; }

  KeySelector *keySelector;
  QuerySelector *querySelector;

  std::mt19937 &rand;

  const size_t numOps;
  const int32_t numKeys;
  
  const bool readOnly;
  const bool readSecondaryCondition;
  const int32_t value_size; 
  const uint64_t value_categories;
  // max_random_size not used in rw sql transaction
  uint64_t max_random_size;
  const bool scanAsPoint;
  const bool execPointScanParallel;

  // not used in the rw sql transaction
  std::vector<int> keyIdxs;

  std::vector<uint64_t> tables;
  std::vector<int32_t> starts;
  std::vector<int32_t> ends;
  std::vector<std::pair<uint64_t, std::string>> secondary_values;

  inline int wrap(int x){
    return mod(x, numKeys);
  }
  
};

template <class T>
void load_row(T& t, std::unique_ptr<query_result::Row> row, const std::size_t col) {
  row->get(col, &t);
}

template<class T>
void deserialize(T& t, std::unique_ptr<const query_result::QueryResult>& queryResult, const std::size_t row, const std::size_t col) {
  load_row(t, queryResult->at(row), col);
}


}

#endif /* RW_TRANSACTION_H */
