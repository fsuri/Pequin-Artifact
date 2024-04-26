#ifndef SEATS_SQL_CLIENT_H
#define SEATS_SQL_CLIENT_H

#include <random>
#include <queue>
#include "store/benchmark/async/sync_transaction_bench_client.h"


#include "store/benchmark/async/sql/seats/seats_profile.h"

namespace seats_sql {


class SEATSSQLClient : public SyncTransactionBenchClient {
 public:
  SEATSSQLClient(SyncClient &client, Transport &transport, const std::string &profile_file_path, uint32_t scale_factor,
      uint64_t id, uint64_t numRequests, uint64_t expDuration, uint64_t delay, uint64_t warmupSec,
      uint64_t cooldownSec, uint64_t tputInterval, uint32_t abortBackoff, bool retryAborted, 
      uint64_t maxBackoff, int64_t maxAttempts,
      uint64_t timeout, const std::string &latencyFilename="");
  virtual ~SEATSSQLClient();
  
 protected:
  virtual SyncTransaction *GetNextTransaction();
  virtual std::string GetLastOp() const;

  std::string lastOp;
  std::mt19937 gen;      
 
  bool started_workload;      
  std::string last_op_;

  SeatsProfile profile;

};

} //namespace seats_sql

#endif 
