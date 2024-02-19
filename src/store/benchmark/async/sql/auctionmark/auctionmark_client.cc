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

#include "store/benchmark/async/sql/auctionmark/close_auctions.h"
#include "store/benchmark/async/sql/auctionmark/get_item.h"
#include "store/benchmark/async/sql/auctionmark/get_user_info.h"
#include "store/benchmark/async/sql/auctionmark/new_bid.h"
#include "store/benchmark/async/sql/auctionmark/new_comment_response.h"
#include "store/benchmark/async/sql/auctionmark/new_comment.h"
#include "store/benchmark/async/sql/auctionmark/new_feedback.h"
#include "store/benchmark/async/sql/auctionmark/new_item.h"
#include "store/benchmark/async/sql/auctionmark/new_purchase.h"
#include "store/benchmark/async/sql/auctionmark/update_item.h"

namespace auctionmark
{
AuctionMarkClient::AuctionMarkClient(
    SyncClient &client, Transport &transport, std::string profile_file_path, uint64_t client_id, uint64_t num_clients,
    int numRequests, int expDuration, uint64_t delay, int warmupSec,
    int cooldownSec, int tputInterval, uint32_t abortBackoff, bool retryAborted,
    uint32_t maxBackoff, uint32_t maxAttempts, const uint32_t timeout, const std::string &latencyFilename)
    : SyncTransactionBenchClient(client, transport, client_id, numRequests,
                                    expDuration, delay, warmupSec, cooldownSec,
                                    tputInterval, abortBackoff, retryAborted, maxBackoff, maxAttempts, timeout,
                                    latencyFilename), profile(client_id, SCALE_FACTOR, num_clients)
{
  lastOp = "";
  gen.seed(client_id);
  need_close_auctions = CLOSE_AUCTIONS_ENABLE && client_id == 0;  //Close Auctions is only run from the first client
  max_u_id = N_USERS;
  max_i_id = N_USERS * 10;
  struct timeval time;
  gettimeofday(&time, NULL);
  last_close_auctions = get_ts(time);

  //TODO: Initialize/load Auctionmark Profile
  //profile = AuctionMarkProfile(client_id, SCALE_FACTOR, num_clients, gen);
  profile.load_profile(profile_file_path, client_id); 
  Panic("at one");
}

AuctionMarkClient::~AuctionMarkClient() {}

SyncTransaction *AuctionMarkClient::GetNextTransaction()
{
  if(!profile.has_client_start_time()){
    profile.set_and_get_client_start_time();
  }
  profile.update_and_get_current_time();

  uint32_t ttype = std::uniform_int_distribution<uint32_t>(1, TXNS_TOTAL)(gen);
  uint32_t freq = 0;
  struct timeval time;
  gettimeofday(&time, NULL);
  uint64_t now = get_ts(time);

  //Close Auctions runs periodically (only on the first client)
  if (need_close_auctions && now - last_close_auctions >= CLOSE_AUCTIONS_INTERVAL / TIME_SCALE_FACTOR) {
    lastOp = "close_auctions";
    last_close_auctions = now;
    return new CloseAuctions(GetTimeout(), profile, gen);
  } 
  
  else if (ttype <= (freq += FREQUENCY_GET_ITEM)) {
    lastOp = "get_item";
    return new GetItem(GetTimeout(), profile, gen);
  } 
  else if (ttype <= (freq += FREQUENCY_GET_USER_INFO)) {
    lastOp = "get_user_info";
    return new GetUserInfo(GetTimeout(), profile, gen);
  } 
  else if (ttype <= (freq += FREQUENCY_NEW_BID)) {
    lastOp = "new_bid";
    return new NewBid(GetTimeout(), profile, gen);
  } 
  else if (ttype <= (freq += FREQUENCY_NEW_COMMENT)) {
    lastOp = "new_comment";
    return new NewComment(GetTimeout(), profile, gen);
  } 
  else if (ttype <= (freq += FREQUENCY_NEW_COMMENT_RESPONSE)) {
    lastOp = "new_comment_response";
    return new NewCommentResponse(GetTimeout(), profile, gen);
  } 
  
  else if (ttype <= (freq += FREQUENCY_NEW_FEEDBACK)) {
    lastOp = "new_feedback";
    return new NewFeedback(GetTimeout(), profile, gen);
  } 
  else if (ttype <= (freq += FREQUENCY_NEW_ITEM)) {
    lastOp = "new_item";
    return new NewItem(GetTimeout(), profile, gen);
  } 
  else if (ttype <= (freq += FREQUENCY_NEW_PURCHASE)) {
    lastOp = "new_purchase";
    return new NewPurchase(GetTimeout(), profile, gen);
  } 
  else if (ttype <= (freq += FREQUENCY_UPDATE_ITEM)) {
    lastOp = "update_item";
    return new UpdateItem(GetTimeout(), profile, gen);
  } 
  else {
    Panic("Invalid transaction type %d", ttype);
  }
}

std::string AuctionMarkClient::GetLastOp() const { return lastOp; }

} // namespace auctionmark
