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
#ifndef RW_SQL_TRANSACTION_H
#define RW_SQL_TRANSACTION_H

#include <vector>

#include "store/common/frontend/async_transaction.h"
#include "store/common/frontend/client.h"
#include "store/benchmark/async/common/key_selector.h"

#include "store/common/frontend/sync_client.h"
#include "store/common/frontend/sync_transaction.h"

namespace rwsql {

static bool AVOID_DUPLICATE_READS = true; 
static bool POINT_READS_ENABLED = true;
static bool PARALLEL_QUERIES = true;
static bool DISABLE_WRAP_AROUND = false;


inline int mod(int &x, const int &N){
    return (x % N + N) %N;
}

class RWSQLTransaction : public SyncTransaction { //AsyncTransaction
 public:
  RWSQLTransaction(QuerySelector *querySelector, uint64_t &numOps, std::mt19937 &rand, bool readSecondaryCondition, bool fixedRange, 
                   int32_t value_size, uint64_t value_categories, bool readOnly=false, bool scanAsPoint=false, bool execPointScanParallel=false);
  virtual ~RWSQLTransaction();

  transaction_status_t Execute(SyncClient &client);

  inline const std::vector<int> getKeyIdxs() const {
    return keyIdxs;
  }
 private:
  std::string GenerateStatement(const std::string &table_name, int &left_bound, int &right_bound);
  void SubmitStatement(SyncClient &client, std::string &statement, const int &i);
  void GetResults(SyncClient &client);
  bool AdjustBounds(int &left, int &right, uint64_t table);
  bool AdjustBounds_manual(int &left, int &right, uint64_t table);
  
 protected:
  inline const std::string &GetKey(int i) const {
    return keySelector->GetKey(keyIdxs[i]);
  }

  
  //inline const size_t GetNumOps() const { return numOps; }

  KeySelector *keySelector;
  QuerySelector *querySelector;

 private:
  std::pair<uint64_t, std::string> GenerateSecondaryCondition();
  void ExecuteScanStatement(SyncClient &client, const std::string &table_name, int &left_bound, int &right_bound);
  void ExecutePointStatements(SyncClient &client, const std::string &table_name, int &left_bound, int &right_bound);
  void ProcessPointResult(SyncClient &client, const std::string &table_name, const int &key, std::unique_ptr<const query_result::QueryResult> &queryResult, const std::pair<uint64_t, std::string> &cond_pair);
  void Update(SyncClient &client, const std::string &table_name, const int &key, std::unique_ptr<const query_result::QueryResult> &queryResult, uint64_t row);

  std::mt19937 &rand;

  const size_t numOps;
  const int numKeys;
  
  const bool readOnly;
  const bool readSecondaryCondition;
  const int32_t value_size; 
  const uint64_t value_categories;
  uint64_t max_random_size;
  const bool scanAsPoint;
  const bool execPointScanParallel;

  std::vector<int> keyIdxs;

  std::vector<uint64_t> tables;
  std::vector<int> starts;
  std::vector<int> ends;

  size_t liveOps;
  //avoid duplicates
  std::vector<std::pair<int, int>> past_ranges;
  
  std::vector<std::string> statements; //keep statements in scope to allow for parallel Writes

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
