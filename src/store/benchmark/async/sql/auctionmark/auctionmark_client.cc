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
#include "store/benchmark/async/sql/auctionmark/auctionmark_params.h"

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
  gen.seed(id);
  need_close_auctions = CLOSE_AUCTIONS_ENABLE && id == 0;
  max_u_id = N_USERS;
  max_i_id = N_USERS * 10;
  last_close_auctions = std::chrono::steady_clock::now();
}

AuctionMarkClient::~AuctionMarkClient() {}

SyncTransaction *AuctionMarkClient::GetNextTransaction()
{
  uint32_t ttype = std::uniform_int_distribution<uint32_t>(0, TXNS_TOTAL - 1)(gen);
  uint32_t freq = 0;
  std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();

  if (need_close_auctions && std::chrono::duration_cast<std::chrono::seconds>(now - last_close_auctions).count() >= CLOSE_AUCTIONS_INTERVAL / TIME_SCALE_FACTOR) {
    lastOp = "check_winning_bids";
    last_close_auctions = now;
    post_auction_items = {};
    post_auction_sellers = {};
    post_auction_buyers = {};
    post_auction_ib_ids = {};
    return new CheckWinningBids(GetTimeout(), 0, 1, post_auction_items, post_auction_sellers, 
      post_auction_buyers, post_auction_ib_ids, gen);
  } else if (ttype < (freq = NEW_USER_RATIO)) {
    lastOp = "new_user";
    uint64_t u_r_id = std::uniform_int_distribution<uint64_t>(1, N_REGIONS)(gen);
    std::vector<std::string> attributes;
    for(int i = 0; i < 8; i++) {
      attributes.push_back(auctionmark::RandomAString(12, 64, gen));
    }
    return new NewUser(GetTimeout(), max_u_id, u_r_id, attributes, gen);
  } 
  else if (ttype < (freq += NEW_ITEM_RATIO)) {
    lastOp = "new_item";
    uint64_t u_id = std::binomial_distribution<uint64_t>(max_u_id, 0.5)(gen);   //FIXME: Why binomial instead of normal?
    std::string name = auctionmark::RandomAString(6, 31, gen);
    std::string description = auctionmark::RandomAString(12, 254, gen);
    std::string attributes = auctionmark::RandomAString(20, 254, gen);

    double initial_price = std::uniform_real_distribution<double>(0.0, 1000.0)(gen);
    double reserve_price = std::uniform_real_distribution<double>(0.0, initial_price)(gen);
    double buy_now = std::uniform_real_distribution<double>(initial_price, initial_price + 1000)(gen);

    std::vector<uint64_t> gag_ids; 
    for (int i = 0; i < 3; i++) {
      gag_ids.push_back(std::uniform_int_distribution<uint64_t>(0, N_GAGS)(gen));
    }
    std::vector<uint64_t> gav_ids;
    for (int i = 0; i < gag_ids.size(); i++) {
      gav_ids.push_back(std::uniform_int_distribution<uint64_t>(0, GAV_PER_GROUP)(gen));
    }

    int n_images = std::uniform_int_distribution(0, 16)(gen);
    std::vector<std::string> images;
    for (int i = 0; i < n_images; i++) {
      images.push_back(auctionmark::RandomAString(32, 128, gen));
    }
    
    return new NewItem(GetTimeout(), max_i_id, u_id, name, description, initial_price, 
            reserve_price, buy_now, attributes, gag_ids, gav_ids, images, 0, 0, gen);
  } 
  else if (ttype < (freq += NEW_BID_RATIO)) {
    lastOp = "new_bid";
    uint64_t i_id = std::binomial_distribution<uint64_t>(max_i_id, 0.5)(gen);
    //FIXME: Should also randomly generate user id??
    uint64_t i_buyer_id = std::binomial_distribution<uint64_t>(max_u_id, 0.5)(gen);
    double bid = std::uniform_real_distribution<double>(0.0, 1000.0)(gen);
    double max_bid = std::uniform_real_distribution<double>(bid, 2 * bid)(gen);
    return new NewBid(GetTimeout(), i_id, i_buyer_id, bid, max_bid, gen);
  } 
  else if (ttype < (freq += NEW_COMMENT_RATIO)) {
    lastOp = "new_comment";
    uint64_t i_buyer_id = std::binomial_distribution<uint64_t>(max_u_id, 0.5)(gen);
    std::string question = auctionmark::RandomAString(12, 127, gen);
    return new NewComment(GetTimeout(), question, gen);
  } 
  else if (ttype < (freq += NEW_COMMENT_RESPONSE_RATIO)) {
    lastOp = "new_comment_response";
    std::string response = auctionmark::RandomAString(12, 127, gen);
    return new NewCommentResponse(GetTimeout(), "", gen);
  } 
  else if (ttype < (freq += NEW_PURCHASE_RATIO)) {
    lastOp = "new_purchase";
    return new NewPurchase(GetTimeout(), gen);
  } 
  else if (ttype < (freq += NEW_FEEDBACK_RATIO)) {
    lastOp = "new_feedback";
    uint64_t rating = std::uniform_int_distribution<uint64_t>(-1, 1)(gen);
    std::string comment = auctionmark::RandomAString(12, 127, gen);
    return new NewFeedback(GetTimeout(), rating, comment, gen);
  } 
  else if (ttype < (freq += GET_ITEM_RATIO)) {
    lastOp = "get_item";
    uint64_t i_id = std::uniform_int_distribution<uint64_t>(0, max_i_id)(gen);
    return new GetItem(GetTimeout(), i_id, gen);
  } 
  else if (ttype < (freq += UPDATE_ITEM_RATIO)) {
    lastOp = "update_item";
    std::string description = auctionmark::RandomAString(50, 254, gen);
    return new UpdateItem(GetTimeout(), description, gen);
  } 
  else if (ttype < (freq += GET_COMMENT_RATIO)) {
    lastOp = "get_comment";
    return new GetComment(GetTimeout(), gen);
  } 
  else if (ttype < (freq += GET_USER_INFO_RATIO)) {
    lastOp = "get_user_info";
    uint64_t get_seller_items = std::uniform_int_distribution<uint64_t>(0, 1)(gen);
    uint64_t get_buyer_items = std::uniform_int_distribution<uint64_t>(0, 1)(gen);
    uint64_t get_feedback = std::uniform_int_distribution<uint64_t>(0, 1)(gen);
    return new GetUserInfo(GetTimeout(), get_seller_items, get_buyer_items, get_feedback, gen);
  } 
  else if (ttype < (freq += GET_WATCHED_ITEMS_RATIO)) {
    lastOp = "get_watched_items";
    uint64_t u_id = std::binomial_distribution<uint64_t>(max_u_id, 0.5)(gen);
    return new GetWatchedItems(GetTimeout(), u_id, gen);
  } 
  else {
    Panic("Invalid transaction type %d", ttype);
  }
}

std::string AuctionMarkClient::GetLastOp() const { return lastOp; }

} // namespace auctionmark
