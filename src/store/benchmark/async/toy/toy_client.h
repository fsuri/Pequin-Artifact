
#ifndef TOY_CLIENT_H
#define TOY_CLIENT_H


#include <gflags/gflags.h>

#include <algorithm>
#include <vector>

#include "lib/latency.h"
#include "lib/tcptransport.h"
#include "lib/timeval.h"
#include "store/benchmark/async/bench_client.h"
#include "store/benchmark/async/toy/toy_client.h"
#include "store/benchmark/async/sync_transaction_bench_client.h"
#include "store/common/frontend/sync_client.h"
#include "store/common/truetime.h"
#include "store/tapirstore/client.h"
#include "store/common/frontend/sync_transaction.h"

namespace toy {

class ToyTransaction : public SyncTransaction {
public:
    ToyTransaction();
    virtual ~ToyTransaction();
    transaction_status_t Execute(SyncClient &client);
};  

class ToyClient : public SyncTransactionBenchClient {
 public:
  ToyClient(SyncClient &client, Transport &transport, uint64_t id,
                  int numRequests, int expDuration, uint64_t delay,
                  int warmupSec, int cooldownSec, int tputInterval,
                  uint32_t abortBackoff, bool retryAborted, uint32_t maxBackoff,
                  uint32_t maxAttempts,
                  const uint32_t timeout, 
                  const std::string &latencyFilename = "");
  virtual ~ToyClient();
  virtual SyncTransaction *GetNextTransaction();
  virtual std::string GetLastOp() const;
  
  void ExecuteToy();

 protected:
 private:
};

}  // namespace toy

#endif  // TOY_CLIENT_H
