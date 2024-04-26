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

#include "store/benchmark/async/sql/auctionmark/transactions/close_auctions.h"
#include "store/benchmark/async/sql/auctionmark/transactions/get_item.h"
#include "store/benchmark/async/sql/auctionmark/transactions/get_user_info.h"
#include "store/benchmark/async/sql/auctionmark/transactions/new_bid.h"
#include "store/benchmark/async/sql/auctionmark/transactions/new_comment_response.h"
#include "store/benchmark/async/sql/auctionmark/transactions/new_comment.h"
#include "store/benchmark/async/sql/auctionmark/transactions/new_feedback.h"
#include "store/benchmark/async/sql/auctionmark/transactions/new_item.h"
#include "store/benchmark/async/sql/auctionmark/transactions/new_purchase.h"
#include "store/benchmark/async/sql/auctionmark/transactions/update_item.h"

namespace auctionmark
{
AuctionMarkClient::AuctionMarkClient(
    SyncClient &client, Transport &transport, const std::string &profile_file_path, uint64_t client_id, uint64_t num_clients,
    int numRequests, int expDuration, uint64_t delay, int warmupSec,
    int cooldownSec, int tputInterval, uint32_t abortBackoff, bool retryAborted,
    uint32_t maxBackoff, uint32_t maxAttempts, const uint32_t timeout, const std::string &latencyFilename)
    : SyncTransactionBenchClient(client, transport, client_id, numRequests,
                                    expDuration, delay, warmupSec, cooldownSec,
                                    tputInterval, abortBackoff, retryAborted, maxBackoff, maxAttempts, timeout,
                                    latencyFilename), profile(client_id, num_clients, DEFAULT_SCALE_FACTOR) //default scale Factor -- Will be overridden by load_profile
{
  lastOp = "";
  gen.seed(client_id);
  need_close_auctions = CLOSE_AUCTIONS_ENABLE && client_id == 0;  //Close Auctions is only run from the first client
  max_u_id = N_USERS;
  max_i_id = N_USERS * 10;
 
  //Initialize/load Auctionmark Profile
  profile.load_profile(profile_file_path, client_id); 
  profile.set_and_get_client_start_time();
  profile.update_and_get_current_time();

  std::cerr << "loader start time (scaled):" << profile.get_loader_start_time() << std::endl;
  std::cerr << "client start time (scaled):" << profile.get_client_start_time() << std::endl; //FIXME: This has not been scaled!

  std::cerr << "pending comment size at start: " << profile.num_pending_comment_responses() << std::endl;
  std::cerr << "available items start: " << profile.get_available_items_count() << std::endl;
  std::cerr << "items waiting for purchase at start: " << profile.get_waiting_for_purchase_items_count() << std::endl;
  std::cerr << "completed items at start: " << profile.get_completed_items_count() << std::endl;

  // std::cerr << "total: " << num_clients << std::endl;
  // std::cerr << "client id: " << client_id << std::endl;
}

AuctionMarkClient::~AuctionMarkClient() {
   profile.clear_cached_profile();
}

SyncTransaction *AuctionMarkClient::GetNextTransaction()
{
  if(!profile.has_client_start_time()){
    profile.set_and_get_client_start_time();
  }
  uint64_t now = profile.update_and_get_current_time();

  uint32_t ttype = std::uniform_int_distribution<uint32_t>(1, TXNS_TOTAL)(gen);
  uint32_t freq = 0;
 
  while(true){
    //Close Auctions runs periodically (only on the first client)
    int real_time_seconds = CLOSE_AUCTIONS_INTERVAL / TIME_SCALE_FACTOR;
    if (need_close_auctions && now - profile.get_last_close_auctions_time() >= real_time_seconds * MILLISECONDS_IN_A_SECOND) {
      std::cerr << "last close auction time (scaled):" << profile.get_last_close_auctions_time() << std::endl;
     std::cerr << "current time (scaled):" << now << std::endl;
      lastOp = "close_auctions";
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
      if(profile.num_pending_comment_responses() == 0) continue;
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
}

std::string AuctionMarkClient::GetLastOp() const { return lastOp; }

} // namespace auctionmark
