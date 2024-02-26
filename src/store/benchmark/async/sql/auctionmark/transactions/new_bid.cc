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
#include "store/benchmark/async/sql/auctionmark/transactions/new_bid.h"
#include <fmt/core.h>

namespace auctionmark {

NewBid::NewBid(uint32_t timeout, AuctionMarkProfile &profile, std::mt19937_64 &gen) : AuctionMarkTransaction(timeout), profile(profile) {

  std::cerr << std::endl << "NEW BID" << std::endl;

  benchmark_times = {profile.get_loader_start_time(), profile.get_client_start_time()};
  std::optional<ItemInfo> itemInfo;
  UserId sellerId;
  UserId buyerId;
  double bid;
  double maxBid;
  


  bool has_available = profile.get_available_items_count() > 0;
  bool has_ending = profile.get_ending_soon_items_count() > 0;
  bool has_waiting = profile.get_waiting_for_purchase_items_count() > 0;
  bool has_completed = profile.get_completed_items_count() > 0;

  // Some NewBids will be for items that have already ended. This will simulate somebody trying to bid at the very end but failing
  int rand = std::uniform_int_distribution<int>(1, 100)(gen);
  if ((has_waiting || has_completed) && (rand <= PROB_NEWBID_CLOSED_ITEM || !has_available)) {
      if (has_waiting) {
        itemInfo = profile.get_random_waiting_for_purchase_item();
    
      } else {
        itemInfo = profile.get_random_completed_item();
      }
      sellerId = itemInfo->get_seller_id();
      buyerId = profile.get_random_buyer_id(sellerId);
 
      // The bid/maxBid do not matter because they won't be able to actually update the auction
      bid = std::uniform_real_distribution<double>(0, 1)(gen);
      maxBid = bid + 100;
    }

    // Otherwise we want to generate information for a real bid
    else {
      rand = std::uniform_int_distribution<int>(1, 100)(gen);
      // 50% of NewBids will be for items that are ending soon
      if ((has_ending && rand <= PROB_NEWBID_CLOSED_ITEM) || !has_available) {
        itemInfo = profile.get_random_ending_soon_item(true); 
      }
      if (!itemInfo.has_value()) {
        itemInfo = profile.get_random_available_item(true);
      }
      if (!itemInfo.has_value()) {
        itemInfo = profile.get_random_item();
      }

      sellerId = itemInfo->get_seller_id();
      buyerId = profile.get_random_buyer_id(sellerId);

      double currentPrice = itemInfo->get_current_price();
      bid = round(std::uniform_real_distribution<double>(currentPrice, currentPrice * (1 + (ITEM_BID_PERCENT_STEP / 2)))(gen) * 100) /100; //round to 2 decimal places
      maxBid = round(std::uniform_real_distribution<double>(bid, bid * (1 + (ITEM_BID_PERCENT_STEP / 2)))(gen) * 100) /100; //round to 2 decimal places
    }

  item_id = itemInfo->get_item_id().encode();
  seller_id = sellerId.encode();
  buyer_id = buyerId.encode();
  newBid = maxBid;
  uint64_t estimatedEndDate = itemInfo->get_end_date();

}

NewBid::~NewBid(){
}

transaction_status_t NewBid::Execute(SyncClient &client) {
  std::unique_ptr<const query_result::QueryResult> queryResult;
  std::string statement;
  std::vector<std::unique_ptr<const query_result::QueryResult>> results;

  //Create a new Bid for an open item.
  Debug("NEW BID");
  Debug("Item ID: %s", item_id);
  Debug("Bid: %f", newBid);


  uint64_t current_time = GetProcTimestamp(benchmark_times);
  double i_current_price;

  client.Begin(timeout);

  //getItem
      //Use Select * so it is cached for future update. In this case, use a different deserializer.
  statement = fmt::format("SELECT * FROM {} WHERE i_id = '{}' AND i_u_id = '{}'", TABLE_ITEM, item_id, seller_id);
  //statement = fmt::format("SELECT i_initial_price, i_current_price, i_num_bids, i_end_date, i_status FROM {} WHERE i_id = '{}' AND i_u_id = '{}'", TABLE_ITEM, item_id, seller_id);
  client.Query(statement, queryResult, timeout);
  if(queryResult->empty()){
    Debug("Invalid item: %s", item_id.c_str());
    client.Abort(timeout);
    return ABORTED_USER;
  }
  getItemRow ir;
  deserialize(ir, queryResult); //i_initial_price, i_current_price, i_num_bids, i_end_date, i_status
  i_current_price = ir.i_current_price;
  

  uint64_t newBidId = 0;
  std::string newBidMaxBuyerId = buyer_id;

  // If we existing bids, then we need to figure out whether we are the new highest bidder or if the existing one just has their max_bid bumped up
  if(ir.i_num_bids > 0){

     //getMaxBidId:  // Get the next ITEM_BID id for this item
    statement = fmt::format("SELECT MAX(ib_id) FROM {} WHERE ib_i_id = '{}' AND ib_u_id = '{}'", TABLE_ITEM_BID, item_id, seller_id);
    client.Query(statement, queryResult, timeout);

    deserialize(newBidId, queryResult);
    ++newBidId;

    // Get the current max bid record for this item

    //getItemMaxBid
    statement = fmt::format("SELECT imb_ib_id, ib_bid, ib_max_bid, ib_buyer_id FROM {}, {} "
        "WHERE imb_i_id = '{}' AND imb_u_id = '{}' " 
        "AND imb_ib_id = ib_id AND ib_i_id = '{}' AND ib_u_id = '{}' " //because imb_i_id == imb_ib_i_id and imb_u_id == imb_ib_u_id
        //"AND imb_ib_id = ib_id AND imb_ib_i_id = ib_i_id AND imb_ib_u_id = ib_u_id",
        "AND ib_id = ib_id", //ADDED REFLEXIVE ARG FOR PELOTON PARSING. TODO: AUTOMATE THIS IN SQL_INTERPRETER 
        TABLE_ITEM_MAX_BID, TABLE_ITEM_BID, item_id, seller_id, item_id, seller_id); // add redundancy.
    client.Query(statement, queryResult, timeout);

    getItemMaxBidRow imbr;
    deserialize(imbr, queryResult);

    bool updateMaxBid = false;
    // Check whether this bidder is already the max bidder
    // This means we just need to increase their current max bid amount without changing the current auction price
    if(buyer_id == imbr.currentBuyerId){
      if(newBid < imbr.currentBidMax){
        Debug("%s already the highest bidder for Item %s but is trying to set a new max bid %d that is less than current max bid %d", buyer_id, item_id, newBid, imbr.currentBidMax);
        client.Abort(timeout);
        return ABORTED_USER;
      }

       //updateBid
      statement = fmt::format("UPDATE {} SET ib_bid = {}, ib_max_bid = {}, ib_updated = {} "
                              " WHERE ib_id = {} AND ib_i_id = '{}' AND ib_u_id = '{}'", TABLE_ITEM_BID, i_current_price, newBid, current_time, imbr.currentBidId, item_id, seller_id);
      client.Write(statement, queryResult, timeout);

      Debug("Increasing the max bid the highest bidder %s from %d to %d for Item %s", buyer_id, imbr.currentBidMax, newBid, item_id);
    }
    // Otherwise check whether this new bidder's max bid is greater than the current max
    else{
        // The new maxBid trumps the existing guy, so our the buyer_id for this txn becomes the new winning bidder at this time. 
        // The new current price is one step above the previous max bid amount
        if(newBid > imbr.currentBidMax) {
          i_current_price = std::min(newBid, imbr.currentBidMax + (ir.i_initial_price * ITEM_BID_PERCENT_STEP));
          // Defer the update to ITEM_MAX_BID until after we insert our new ITEM_BID record
          updateMaxBid = true;
        }
        // The current max bidder is still the current one.  We just need to bump up their bid amount to be at least the bidder's amount
        // Make sure that we don't go over the the currentMaxBidMax, otherwise this would mean that we caused the user to bid more than they wanted.
        else{
          newBidMaxBuyerId = imbr.currentBuyerId;
           i_current_price = std::min(imbr.currentBidMax, newBid + (ir.i_initial_price * ITEM_BID_PERCENT_STEP));

            //updateBid
            statement = fmt::format("UPDATE {} SET ib_bid = {}, ib_max_bid = {}, ib_updated = {} "
                                    " WHERE ib_id = {} AND ib_i_id = '{}' AND ib_u_id = '{}'", TABLE_ITEM_BID, i_current_price, i_current_price, current_time, imbr.currentBidId, item_id, seller_id);
            client.Write(statement, queryResult, timeout);
            Debug("Keeping the existing highest bidder of Item %s as %s but updating current price from %d to %d", item_id, buyer_id, imbr.currentBidAmount, i_current_price);
        }

        // Always insert an new ITEM_BID record even if BuyerId doesn't become the new highest bidder. 
        // We also want to insert a new record even if the BuyerId already has ITEM_BID record, because we want to maintain the history of all the bid attempts
        //insertItemBid
        statement = fmt::format("INSERT INTO {} (ib_i_id, ib_u_id, ib_id, ib_buyer_id, ib_bid, ib_max_bid, ib_created, ib_updated) "
                "VALUES ('{}', '{}', {}, '{}', {}, {}, {}, {})", TABLE_ITEM_BID, item_id, seller_id, newBidId, buyer_id, i_current_price, newBid, current_time, current_time);
        client.Write(statement, queryResult, timeout);

        if(updateMaxBid){
            //updateItemMaxBid
          statement = fmt::format("UPDATE {} SET imb_ib_id = {}, "
                  "       imb_ib_i_id = {}, "
                  "       imb_ib_u_id = {}, "
                  "       imb_updated = {} "
                  " WHERE imb_i_id = '{}' AND imb_u_id = '{}'", TABLE_ITEM_MAX_BID, newBidId, item_id, seller_id, current_time, item_id, seller_id);
          client.Write(statement, queryResult, timeout);
        }
    }
  }
  else{ // There is no existing max bid record, therefore we can just insert ourselves
      //insertItemBid
    statement = fmt::format("INSERT INTO {} (ib_i_id, ib_u_id, ib_id, ib_buyer_id, ib_bid, ib_max_bid, ib_created, ib_updated) "
            "VALUES ('{}', '{}', {}, '{}', {}, {}, {}, {})", TABLE_ITEM_BID, item_id, seller_id, newBidId, buyer_id, ir.i_initial_price, newBid, current_time, current_time);
    client.Write(statement, queryResult, timeout);

    //insertItemMaxBid
    statement = fmt::format("INSERT INTO {} (imb_i_id, imb_u_id, imb_ib_id, imb_ib_i_id, imb_ib_u_id, imb_created, imb_updated) "
            "VALUES ('{}', '{}', {}, '{}', '{}', {}, {})", TABLE_ITEM_MAX_BID, item_id, seller_id, newBidId, item_id, seller_id, current_time, current_time);
    client.Write(statement, queryResult, timeout);

     //updateItem
    statement = fmt::format("UPDATE {} SET i_num_bids = i_num_bids + 1, i_current_price = {}, i_updated = {} WHERE i_id = '{}' AND i_u_id = '{}'", 
                              TABLE_ITEM, i_current_price, current_time, item_id, seller_id);
    client.Write(statement, queryResult, timeout);

  }
 
   
  ItemRecord item_rec(item_id, seller_id, "", i_current_price, ir.i_num_bids + 1, ir.i_end_date, ir.i_status, newBidId, newBidMaxBuyerId);
  ItemId itemId = profile.processItemRecord(item_rec);

  Debug("COMMIT");
  return client.Commit(timeout);
}

} // namespace auctionmark
