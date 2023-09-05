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
#include "store/benchmark/async/rw-sql/rw-sql_client.h"

#include <iostream>
#include "rw-sql_client.h"

namespace rwsql {

//Configuration params:
//numOps: number of R-modify-W statements (or R statements, if Read Only)
//numTables: number of Tables  (input for tableSelector)
//numKeys: number of keys per Table (input for baseSelector)
//maxRange: number of keys to read per Operation (input for rangeSelector)

//querySelector = new querySelector(numKeys, tableSelector, baseSelector, rangeSelector); //TODO: create this in benchmark.

//uint64_t numTables, uint64_t numOps, uint64_t maxRange,
RWSQLClient::RWSQLClient(uint64_t numOps, QuerySelector *querySelector, bool readOnly,
      SyncClient &client, Transport &transport, uint64_t id, int numRequests, 
      int expDuration, uint64_t delay, int warmupSec, int cooldownSec, 
      int tputInterval, uint32_t abortBackoff, bool retryAborted, uint32_t maxBackoff, uint32_t maxAttempts, const uint32_t timeout,
      const std::string &latencyFilename)
     : SyncTransactionBenchClient(client, transport, id, numRequests,
                                 expDuration, delay, warmupSec, cooldownSec,
                                 tputInterval, abortBackoff, retryAborted, maxBackoff, maxAttempts, timeout,
                                 latencyFilename),
        readOnly(readOnly), querySelector(querySelector), numOps(numOps) {

}

RWSQLClient::~RWSQLClient() {
    
}

SyncTransaction *RWSQLClient::GetNextTransaction() {
  RWSQLTransaction *rw_tx = new RWSQLTransaction(querySelector, numOps, GetRand(), readOnly); 
  // for(int key : rw_tx->getKeyIdxs()){
  //   //key_counts[key]++;
  //   stats.IncrementList("key distribution", key, 1);
  // }
  return rw_tx;
}

std::string RWSQLClient::GetLastOp() const {
  return "rw_sql";
}

} // namespace rw

