#ifndef TPCCH_SQL_CLIENT_H
#define TPCCH_SQL_CLIENT_H

#include "store/benchmark/async/sync_transaction_bench_client.h"
#include <random>

namespace tpcch_sql {

class TPCCHSQLClient : public SyncTransactionBenchClient {
 public:
  TPCCHSQLClient(SyncClient &client, Transport &transport, uint64_t id,
      uint64_t numRequests, uint64_t expDuration, uint64_t delay, uint64_t warmupSec,
      uint64_t cooldownSec, uint64_t tputInterval, uint32_t abortBackoff, bool retryAborted, 
      uint64_t maxBackoff, int64_t maxAttempts,
      uint64_t timeout, const std::string &latencyFilename="");

  virtual ~TPCCHSQLClient();

 protected:
  virtual SyncTransaction *GetNextTransaction();
  virtual std::string GetLastOp() const;
  std::string lastOp;
  std::vector<int> q_weights;
  std::mt19937 gen;

private: 
  SyncTransaction* ConvertTIDToTransaction(int t_id);
};

} 

#endif
