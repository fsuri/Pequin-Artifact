/***********************************************************************
 *
 * Copyright 2021 Florian Suri-Payer <fsp@cs.cornell.edu>
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
#include "store/benchmark/async/sql/auctionmark/new_bid.h"
#include <fmt/core.h>

namespace auctionmark {

NewBid::NewBid(uint32_t timeout, uint64_t i_id, uint64_t i_buyer_id,
      double bid, double max_bid, std::mt19937_64 &gen) : AuctionMarkTransaction(timeout), i_id(i_id),
      i_buyer_id(i_buyer_id), bid(bid), max_bid(max_bid) {
}

NewBid::~NewBid(){
}

transaction_status_t NewBid::Execute(SyncClient &client) {
  std::unique_ptr<const query_result::QueryResult> queryResult;
  std::string statement;
  std::vector<std::unique_ptr<const query_result::QueryResult>> results;

  //Create a new Bid for an open item.
  Debug("NEW BID");
  Debug("Item ID: %lu", i_id);
  Debug("Bid: %f", bid);

  //TODO: parallelize queries (only after sequential debugged)

  //TODO: parameterize inputs
  std::string item_id;
  std::string seller_id;
  std::string buyer_id;
  double newBid;
  uint64_t estimatedEndDate;

  client.Begin(timeout);

  double i_initial_price;
  double i_current_price;
  long i_num_bids;
  uint64_t i_end_date;
  ItemStatus i_status;

  //getItem
  statement = fmt::format("SELECT i_initial_price, i_current_price, i_num_bids, i_end_date, i_status FROM {} WHERE i_id = {} AND i_u_id = {}", TABLE_ITEM, ...);
  client.Query(statement, queryResult, timeout);
  if(queryResult.empty()){
    Debug("Invalid item: %s", item_id.c_str());
    return client.Abort(timeout);
  }
   //TODO: deserialize


  uint64_t newBidId = 0;
  std::string newBidMaxBuyerId = buyer_id;

  // If we existing bids, then we need to figure out whether we are the new highest bidder or if the existing one just has their max_bid bumped up
  if(i_num_bids > 0){

     //getMaxBidId:  // Get the next ITEM_BID id for this item
    statement = fmt::format("SELECT MAX(ib_id) FROM {} WHERE ib_i_id = {} AND ib_u_id = {}", TABLE_ITEM_BID, ...);
    client.Query(statement, queryResult, timeout);

    deserialize(newBidId, queryResult);
    ++newBidId;

    // Get the current max bid record for this item
    uint64_t currentBidId;
    double currentBidAmount;
    double currentBidMax;
    std::string currentBuyerId;
    //getItemMaxBid
    statement = fmt::format("SELECT imb_ib_id, ib_bid, ib_max_bid, ib_buyer_id FROM {}, {} 
        WHERE imb_i_id = {} AND imb_u_id = {} AND imb_ib_id = ib_id AND imb_ib_i_id = ib_i_id AND imb_ib_u_id = ib_u_id",
        TABLE_ITEM_MAX_BID, TABLE_ITEM_BID, ...);
    client.Query(statement, queryResult, timeout);
    //TODO: deserialize


    bool updateMaxBid = false;
    // Check whether this bidder is already the max bidder
    // This means we just need to increase their current max bid amount without changing the current auction price
    if(buyer_id == currentBuyerId){
      if(newBid < currentBidMax){
        Debug("%s already the highest bidder for Item %s but is trying to set a new max bid %d that is less than current max bid %d", buyer_id, item_id, newBid, currentBidMax);
        return client.Abort(timeout);
      }

       //updateBid
      statement = fmt::format("UPDATE {} SET ib_bid = {}, ib_max_bid = {}, ib_updated = {} "
                              " WHERE ib_id = {} AND ib_i_id = {} AND ib_u_id = {} ", TABLE_ITEM, ...);
      client.Write(statement, queryResult, timeout);

      Debug("Increasing the max bid the highest bidder %s from %d to %d for Item %s", buyer_id, currentBidMax, newBid, item_id);
    }
    // Otherwise check whether this new bidder's max bid is greater than the current max
    else{
        // The new maxBid trumps the existing guy, so our the buyer_id for this txn becomes the new winning bidder at this time. 
        // The new current price is one step above the previous max bid amount
        if(newBid > currentBidMax) {
          i_current_price = std::min(newBid, currentBidMax + (i_initial_price * ITEM_BID_PERCENT_STEP));
          // Defer the update to ITEM_MAX_BID until after we insert our new ITEM_BID record
          updateMaxBid = true;
        }
        // The current max bidder is still the current one.  We just need to bump up their bid amount to be at least the bidder's amount
        // Make sure that we don't go over the the currentMaxBidMax, otherwise this would mean that we caused the user to bid more than they wanted.
        else{
          newBidMaxBuyerId = currentBuyerId;
           i_current_price = std::min(currentBidMax, newBid + (i_initial_price * ITEM_BID_PERCENT_STEP));

            //updateBid
            statement = fmt::format("UPDATE {} SET ib_bid = {}, ib_max_bid = {}, ib_updated = {} "
                                    " WHERE ib_id = {} AND ib_i_id = {} AND ib_u_id = {} ", TABLE_ITEM, ...);
            client.Write(statement, queryResult, timeout);
            Debug("Keeping the existing highest bidder of Item %s as %s but updating current price from %d to %d", item_id, buyer_id, currentBidAmount, i_current_price);
        }

        // Always insert an new ITEM_BID record even if BuyerId doesn't become the new highest bidder. 
        // We also want to insert a new record even if the BuyerId already has ITEM_BID record, because we want to maintain the history of all the bid attempts
        //insertItemBid
        statement = fmt::format("INSERT INTO {} (ib_id, ib_i_id, ib_u_id, ib_buyer_id, ib_bid, ib_max_bid, ib_created, ib_updated) "
                "VALUES ({}, {}, {}, {}, {}, {}, {}, {})", TABLE_ITEM, ...);
        client.Write(statement,  timeout);

        //updateItemMaxBid
        statement = fmt::format("UPDATE {} SET imb_ib_id = {}, "
                "       imb_ib_i_id = {}, "
                "       imb_ib_u_id = {}, "
                "       imb_updated = {} "
                " WHERE imb_i_id = {} "
                "   AND imb_u_id = {}", TABLE_ITEM_MAX_BID, ...);
        client.Write(statement, queryResult, timeout);
    }

  }
  else{ // There is no existing max bid record, therefore we can just insert ourselves
      //insertItemBid
    statement = fmt::format("INSERT INTO {} (ib_id, ib_i_id, ib_u_id, ib_buyer_id, ib_bid, ib_max_bid, ib_created, ib_updated) "
            "VALUES ({}, {}, {}, {}, {}, {}, {}, {})", TABLE_ITEM, ...);
    client.Write(statement,  timeout);

    //insertItemMaxBid
    statement = fmt::format("INSERT INTO {} (imb_i_id, imb_u_id, imb_ib_id, imb_ib_i_id, imb_ib_u_id, imb_created, imb_updated) "
            "VALUES ({}, {}, {}, {}, {}, {}, {})", TABLE_ITEM_MAX_BID, ...);
    client.Write(statement, timeout);

     //updateItem
    statement = fmt::format("UPDATE {} SET i_num_bids = i_num_bids + 1, i_current_price = {}, i_updated = {} WHERE i_id = {} AND i_u_id = {}", TABLE_ITEM, ...);
    client.Write(statement,  timeout);

    client.Wait(results);
  }
 
  Debug("COMMIT");
  return client.Commit(timeout);
}

} // namespace auctionmark
