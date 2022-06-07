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
#include "store/benchmark/async/retwis/retwis_client.h"

#include "store/benchmark/async/retwis/add_user.h"
#include "store/benchmark/async/retwis/follow.h"
#include "store/benchmark/async/retwis/get_timeline.h"
#include "store/benchmark/async/retwis/post_tweet.h"

#include <iostream>

namespace retwis {

RetwisClient::RetwisClient(KeySelector *keySelector, AsyncClient &client,
    Transport &transport, uint64_t id, int numRequests, int expDuration,
    uint64_t delay,
    int warmupSec, int cooldownSec, int tputInterval, uint32_t abortBackoff,
    bool retryAborted, uint32_t maxBackoff, uint32_t maxAttempts, const std::string &latencyFilename)
    : AsyncTransactionBenchClient(client, transport, id, numRequests,
        expDuration,
        delay, warmupSec, cooldownSec, tputInterval, abortBackoff,
        retryAborted, maxBackoff, maxAttempts, latencyFilename), keySelector(keySelector) {
}

RetwisClient::~RetwisClient() {
}

AsyncTransaction *RetwisClient::GetNextTransaction() {
  int ttype = GetRand()() % 100;
  if (ttype < 5) {
    lastOp = "add_user";
    return new AddUser(keySelector, GetRand());
  } else if (ttype < 20) {
    lastOp = "follow";
    return new Follow(keySelector, GetRand());
  } else if (ttype < 50) { //change to 100/true to avoid any timeline.
    lastOp = "post_tweet";
    return new PostTweet(keySelector, GetRand());
  } else {
    lastOp = "get_timeline";
    return new GetTimeline(keySelector, GetRand());
  }
}

std::string RetwisClient::GetLastOp() const {
  return lastOp;
}

} //namespace retwis
