#ifndef TPCCH_SQL_CLIENT_H
#define TPCCH_SQL_CLIENT_H

#include <random>

#include "store/benchmark/async/sync_transaction_bench_client.h"

namespace tpcch_sql {

class TPCCHSQLClient : public SyncTransactionBenchClient {
 public:
  TPCCHSQLClient(SyncClient &client, Transport &transport, uint64_t id,
      int numRequests, int expDuration, uint64_t delay, int warmupSec,
      int cooldownSec, int tputInterval, uint32_t num_warehouses, uint32_t w_id,
      uint32_t C_c_id, uint32_t C_c_last, uint32_t new_order_ratio,
      uint32_t delivery_ratio, uint32_t payment_ratio, uint32_t order_status_ratio,
      uint32_t stock_level_ratio, bool static_w_id,
      uint32_t abortBackoff, bool retryAborted, uint32_t maxBackoff, uint32_t maxAttempts,
      uint32_t timeout,
      const std::string &latencyFilename = "");

  virtual ~TPCCHSQLClient();

 protected:
  virtual SyncTransaction *GetNextTransaction();
  virtual std::string GetLastOp() const;

  uint32_t num_warehouses;
  uint32_t w_id;
  uint32_t C_c_id;
  uint32_t C_c_last;
  uint32_t new_order_ratio;
  uint32_t delivery_ratio;
  uint32_t payment_ratio;
  uint32_t order_status_ratio;
  uint32_t stock_level_ratio;
  bool static_w_id;
  uint32_t stockLevelDId;
  std::string lastOp;

 private:
  bool delivery;
  uint32_t deliveryWId;
  uint32_t deliveryDId;

};

} //namespace tpcc_sql

#endif /* TPCC_SQL_CLIENT_H */
