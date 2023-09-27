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
static bool POINT_READS_ENABLED = false;
static bool PARALLEL_QUERIES = false;

inline int mod(int &x, const int &N){
    return (x % N + N) %N;
}

class RWSQLTransaction : public SyncTransaction { //AsyncTransaction
 public:
  RWSQLTransaction(QuerySelector *querySelector, uint64_t &numOps, std::mt19937 &rand, bool readOnly);
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
  const size_t numOps;
  const int numKeys;
  
  const bool readOnly;
  std::vector<int> keyIdxs;

  std::vector<uint64_t> tables;
  std::vector<int> starts;
  std::vector<int> ends;

  size_t liveOps;
  //avoid duplicates
  std::vector<std::pair<int, int>> past_ranges;

  inline int wrap(int x){
    return mod(x, numKeys);
  }
  
};

}

#endif /* RW_TRANSACTION_H */
