/***********************************************************************
 *
 * Copyright 2022 Florian Suri-Payer <fsp@cs.cornell.edu>
 *                Liam Arzola <lma77@cornell.edu>
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

#include "store/benchmark/async/auctionmark/auctionmark_client.h"

#include <gflags/gflags.h>

#include <algorithm>
#include <random>
#include <vector>

#include "lib/latency.h"
#include "lib/tcptransport.h"
#include "lib/timeval.h"
#include "store/benchmark/async/bench_client.h"

#include "store/common/frontend/sync_client.h"
#include "store/common/truetime.h"
#include "store/tapirstore/client.h"

#include "store/benchmark/async/auctionmark/new_user.h"

namespace auctionmark
{
    AuctionMarkClient::AuctionMarkClient(
        SyncClient &client, Transport &transport, uint64_t id,
        int numRequests, int expDuration, uint64_t delay, int warmupSec,
        int cooldownSec, int tputInterval, uint32_t abortBackoff, bool retryAborted,
        uint32_t maxBackoff, uint32_t maxAttempts, const uint32_t timeout, const std::string &latencyFilename)
        : SyncTransactionBenchClient(client, transport, id, numRequests,
                                     expDuration, delay, warmupSec, cooldownSec,
                                     tputInterval, abortBackoff, retryAborted, maxBackoff, maxAttempts, timeout,
                                     latencyFilename)
    {
        lastOp = "";
    }

    AuctionMarkClient::~AuctionMarkClient() {}

    SyncTransaction *AuctionMarkClient::GetNextTransaction()
    {
        constexpr vector<string> attributes {};
        NewUser auction_mark_tx(10000, 0, 0, &attributes, GetRand());
        lastOp = "NewUser";
        return auction_mark_tx;
    }
    std::string ToyClient::GetLastOp() const { return lastOp; }

} // namespace auctionmark
