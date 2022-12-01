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
#ifndef AUCTIONMARK_CLIENT_H
#define AUCTIONMARK_CLIENT_H

#include <random>

#include "store/benchmark/async/sync_transaction_bench_client.h"

namespace auctionmark
{

    enum AuctionMarkTransactionType
    {
        TXN_NEW_USER = 0,
        TXN_NEW_ITEM,
        TXN_NEW_BID,
        TXN_NEW_COMMENT,
        TXN_NEW_COMMENT_RESPONSE,
        TXN_NEW_PURCHASE,
        TXN_NEW_FEEDBACK,
        TXN_GET_ITEM,
        TXN_UPDATE_ITEM,
        TXN_GET_COMMENT,
        TXN_GET_USER_INFO,
        TXN_GET_WATCHED_ITEM,
        NUM_TXN_TYPES
    };

    class AuctionMarkClient : public SyncTransactionBenchClient
    {
    public:
        // TODO: Should transaction ratios be passed in as parameters or set through config/constants file?
        AuctionMarkClient(SyncClient &client, Transport &transport, uint64_t id,
                          int numRequests, int expDuration, uint64_t delay, int warmupSec,
                          int cooldownSec, int tputInterval,
                          uint32_t abortBackoff, bool retryAborted, uint32_t maxBackoff, uint32_t maxAttempts,
                          uint32_t timeout,
                          const std::string &latencyFilename = "");

        virtual ~AuctionMarkClient();

        virtual SyncTransaction *GetNextTransaction();
        virtual std::string GetLastOp() const;

    private:
        std::string lastOp;
    };

} // namespace auctionmark

#endif /* AUCTIONMARK_CLIENT_H */
