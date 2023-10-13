#ifndef SEATS_SQL_CLIENT_H
#define SEATS_SQL_CLIENT_H

#include <random>
#include "store/benchmark/async/sync_transaction_bench_client.h"

namespace seats_sql {

class SEATSSQLClient : public SyncTransactionBenchClient {
 public:
  SEATSSQLClient(SyncClient &client, Transport &transport, uint64_t id,
      int numRequests, int expDuration, uint64_t delay, int warmupSec,
      int cooldownSec, int tputInterval, double min_reserved_ratio, double max_reserved_ratio,
      uint32_t abortBackoff, bool retryAborted, uint32_t maxBackoff, uint32_t maxAttempts,
      uint32_t timeout,
      const std::string &latencyFilename = "");
  virtual ~SEATSSQLClient();

 protected:
  virtual SyncTransaction *GetNextTransaction();
  virtual std::string GetLastOp() const;

  double min_reserved_ratio;     // minimum % of seats reserved on flight
  double max_reserved_ratio;     // maximum % of seats reserved on flight
  std::string lastOp;
  std::mt19937_64 gen;      
  uint64_t seats_id;              // need this for generating res id
  uint64_t num_res_made;       // number of reservations made by client
};

} //namespace seats_sql

#endif 
