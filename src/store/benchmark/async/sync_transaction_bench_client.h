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
#ifndef SYNC_TRANSACTION_BENCH_CLIENT_H
#define SYNC_TRANSACTION_BENCH_CLIENT_H

#include "store/benchmark/async/bench_client.h"
#include "store/common/frontend/sync_client.h"
#include "store/common/frontend/sync_transaction.h"

#include <random>

class SyncTransactionBenchClient : public BenchmarkClient {
 public:
  SyncTransactionBenchClient(SyncClient &client, Transport &transport,
      uint64_t id, int numRequests, int expDuration, uint64_t delay,
      int warmupSec,
      int cooldownSec, int tputInterval, uint64_t abortBackoff,
      bool retryAborted, uint64_t maxBackoff, int64_t maxAttempts, uint64_t timeout,
      const std::string &latencyFilename = "");

  virtual ~SyncTransactionBenchClient();

  void SendNext(transaction_status_t *result);
 protected:
  virtual SyncTransaction *GetNextTransaction() = 0;
  virtual void SendNext() override;
  inline uint32_t GetTimeout() const { return timeout; } 

  SyncClient &client;
 private:
  uint64_t abortBackoff;
  bool retryAborted;
  uint64_t maxBackoff;
  int64_t maxAttempts;
  uint64_t timeout;
  SyncTransaction *currTxn;
  uint64_t currTxnAttempts;

};

#endif /* SYNC_TRANSACTION_BENCH_CLIENT_H */
