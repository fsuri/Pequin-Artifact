/***********************************************************************
 *
 * Copyright 2025 Austin Li <atl63@cornell.edu>
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
#include "store/benchmark/async/rw-sync/rw-sync_client.h"

#include <iostream>

namespace rwsync {

RWSyncClient::RWSyncClient(KeySelector *keySelector, uint64_t numKeys, bool readOnly,
  SyncClient &client,
  Transport &transport, uint64_t id, int numRequests, int expDuration,
  uint64_t delay, int warmupSec, int cooldownSec, int tputInterval,
  uint32_t abortBackoff, bool retryAborted, uint32_t maxBackoff, uint32_t maxAttempts,
  const uint32_t timeout,
  const std::string &latencyFilename)
  : SyncTransactionBenchClient(client, transport, id, numRequests,
      expDuration, delay, warmupSec, cooldownSec, tputInterval, abortBackoff,
      retryAborted, maxBackoff, maxAttempts, timeout, latencyFilename), 
    keySelector(keySelector),
    numKeys(numKeys), readOnly(readOnly) {
}

RWSyncClient::~RWSyncClient() {
}

SyncTransaction *RWSyncClient::GetNextTransaction() {
  RWSyncTransaction *rw_tx = new RWSyncTransaction(keySelector, numKeys, readOnly, GetRand());
  // for(int key : rw_tx->getKeyIdxs()){
  //   //key_counts[key]++;
  //   stats.IncrementList("key distribution", key, 1);
  // }
  return rw_tx;
}

std::string RWSyncClient::GetLastOp() const {
  return "rw_sync";
}

} //namespace rwsync
