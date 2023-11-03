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

#include "store/benchmark/async/sql/auctionmark/auctionmark_client.h"

#include <random>

#include "store/benchmark/async/sql/auctionmark/check_winning_bids.h"
#include "store/benchmark/async/sql/auctionmark/get_comment.h"
#include "store/benchmark/async/sql/auctionmark/get_item.h"
#include "store/benchmark/async/sql/auctionmark/get_user_info.h"
#include "store/benchmark/async/sql/auctionmark/get_watched_items.h"
#include "store/benchmark/async/sql/auctionmark/new_bid.h"
#include "store/benchmark/async/sql/auctionmark/new_comment_response.h"
#include "store/benchmark/async/sql/auctionmark/new_comment.h"
#include "store/benchmark/async/sql/auctionmark/new_feedback.h"
#include "store/benchmark/async/sql/auctionmark/new_item.h"
#include "store/benchmark/async/sql/auctionmark/new_purchase.h"
#include "store/benchmark/async/sql/auctionmark/new_user.h"
#include "store/benchmark/async/sql/auctionmark/post_auction.h"
#include "store/benchmark/async/sql/auctionmark/update_item.h"

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
  std::mt19937 gen = GetRand();
  uint32_t ttype = std::uniform_int_distribution<uint32_t>(0, TXNS_TOTAL - 1)(gen);
  if (ttype < NEW_USER_RATIO) {
    lastOp = "new_user";
    const std::vector<std::string> attributes {};
    return new NewUser(GetTimeout(), 0, 0, attributes, GetRand());
  } else if (ttype < NEW_ITEM_RATIO + NEW_USER_RATIO) {
    lastOp = "new_item";
    return new NewItem(GetTimeout(), 0, 0, 0, "", "", 0, 0, 0, "", {}, {}, {}, 0, 0, GetRand());
  } else if (ttype < NEW_BID_RATIO + NEW_ITEM_RATIO + NEW_USER_RATIO) {
    lastOp = "new_bid";
    return new NewBid(GetTimeout(), 0, 0, 0, 0, 0, GetRand());
  } else if (ttype < NEW_COMMENT_RATIO + NEW_BID_RATIO + NEW_ITEM_RATIO + 
  NEW_USER_RATIO) {
    lastOp = "new_comment";
    return new NewComment(GetTimeout(), 0, 0, 0, "", GetRand());
  } else if (ttype < NEW_COMMENT_RESPONSE_RATIO + NEW_COMMENT_RATIO + NEW_BID_RATIO + 
  NEW_ITEM_RATIO + NEW_USER_RATIO) {
    lastOp = "new_comment_response";
    return new NewCommentResponse(GetTimeout(), 0, 0, 0, "", GetRand());
  } else if (ttype < NEW_PURCHASE_RATIO + NEW_COMMENT_RESPONSE_RATIO + NEW_COMMENT_RATIO + 
  NEW_BID_RATIO + NEW_ITEM_RATIO + NEW_USER_RATIO) {
    lastOp = "new_purchase";
    return new NewPurchase(GetTimeout(), 0, 0, 0, 0, GetRand());
  } else if (ttype < NEW_FEEDBACK_RATIO + NEW_PURCHASE_RATIO + NEW_COMMENT_RESPONSE_RATIO + 
  NEW_COMMENT_RATIO + NEW_BID_RATIO + NEW_ITEM_RATIO + NEW_USER_RATIO) {
    lastOp = "new_feedback";
    return new NewFeedback(GetTimeout(), 0, 0, 0, 0, 0, GetRand());
  } else if (ttype < GET_ITEM_RATIO + NEW_FEEDBACK_RATIO + NEW_PURCHASE_RATIO + 
  NEW_COMMENT_RESPONSE_RATIO + NEW_COMMENT_RATIO + NEW_BID_RATIO + NEW_ITEM_RATIO + 
  NEW_USER_RATIO) {
    lastOp = "get_item";
    return new GetItem(GetTimeout(), 0, 0, GetRand());
  } else if (ttype < UPDATE_ITEM_RATIO + GET_ITEM_RATIO + NEW_FEEDBACK_RATIO +
  NEW_PURCHASE_RATIO + NEW_COMMENT_RESPONSE_RATIO + NEW_COMMENT_RATIO + NEW_BID_RATIO +
  NEW_ITEM_RATIO + NEW_USER_RATIO) {
    lastOp = "update_item";
    return new UpdateItem(GetTimeout(), 0, 0, "", GetRand());
  } else if (ttype < GET_COMMENT_RATIO + UPDATE_ITEM_RATIO + GET_ITEM_RATIO + NEW_FEEDBACK_RATIO +
  NEW_PURCHASE_RATIO + NEW_COMMENT_RESPONSE_RATIO + NEW_COMMENT_RATIO + NEW_BID_RATIO +
  NEW_ITEM_RATIO + NEW_USER_RATIO) {
    lastOp = "get_comment";
    return new GetComment(GetTimeout(), 0, GetRand());
  } else if (ttype < GET_USER_INFO_RATIO + GET_COMMENT_RATIO + UPDATE_ITEM_RATIO + GET_ITEM_RATIO +
  NEW_FEEDBACK_RATIO + NEW_PURCHASE_RATIO + NEW_COMMENT_RESPONSE_RATIO + NEW_COMMENT_RATIO +
  NEW_BID_RATIO + NEW_ITEM_RATIO + NEW_USER_RATIO) {
    lastOp = "get_user_info";
    return new GetUserInfo(GetTimeout(), 0, 0, 0, 0, GetRand());
  } else if (ttype < GET_WATCHED_ITEMS_RATIO + GET_USER_INFO_RATIO + GET_COMMENT_RATIO +
  UPDATE_ITEM_RATIO + GET_ITEM_RATIO + NEW_FEEDBACK_RATIO + NEW_PURCHASE_RATIO +
  NEW_COMMENT_RESPONSE_RATIO + NEW_COMMENT_RATIO + NEW_BID_RATIO + NEW_ITEM_RATIO +
  NEW_USER_RATIO) {
    lastOp = "get_watched_items";
    return new GetWatchedItems(GetTimeout(), 0, GetRand());
  } else {
    lastOp = "post_auction";
    return new PostAuction(GetTimeout(), {}, {}, {}, {}, GetRand());
  }
}
std::string AuctionMarkClient::GetLastOp() const { return lastOp; }

} // namespace auctionmark
