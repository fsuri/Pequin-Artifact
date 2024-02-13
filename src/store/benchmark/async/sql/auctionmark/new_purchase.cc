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
#include "store/benchmark/async/sql/auctionmark/new_purchase.h"
#include <fmt/core.h>

namespace auctionmark {

NewPurchase::NewPurchase(uint32_t timeout, AuctionMarkProfile &profile, std::mt19937_64 &gen) : AuctionMarkTransaction(timeout), profile(profile), gen(gen) {
  ItemInfo itemInfo = profile.get_random_waiting_for_purchase_item();
  item_id = itemInfo.get_item_id().encode();
  UserId sellerId = itemInfo.get_seller_id();
  seller_id = sellerId.encode();
  buyer_credit = 0;

  int ip_id_cnt = profile.ip_id_cntrs[item_id];
  
  ip_id = ItemId(sellerId, ip_id_cnt).encode();
  profile.ip_id_cntrs[item_id] = (ip_id_cnt < 127) ? ip_id_cnt + 1 : 0;

  // Whether the buyer will not have enough money
  if (itemInfo.has_current_price()) {
    if (std::uniform_int_distribution<int>(1, 100)(gen) <= PROB_NEWPURCHASE_NOT_ENOUGH_MONEY) {
      buyer_credit = -1 * itemInfo.get_current_price();
    } else {
      buyer_credit = itemInfo.get_current_price();
      itemInfo.set_status(ItemStatus::CLOSED);
    }
  }
}

NewPurchase::~NewPurchase(){
}

transaction_status_t NewPurchase::Execute(SyncClient &client) {
  std::unique_ptr<const query_result::QueryResult> queryResult;
  std::string statement;
  std::vector<std::unique_ptr<const query_result::QueryResult>> results;

  Debug("NEW PURCHASE");

  client.Begin(timeout);

  timestamp_t current_time = GetProcTimestamp({profile.get_loader_start_time(), profile.get_client_start_time()});

  //HACK Check whether we have an ITEM_MAX_BID record. If not, we'll insert one
  std::string getItemMaxBid = fmt::format("SELECT * FROM {} WHERE imb_i_id = {} AND imb_u_id = {}", TABLE_ITEM_MAX_BID, item_id, seller_id);
  client.Query(getItemMaxBid, queryResult, timeout);
  if(queryResult.empty()){
      std::string getMaxBid = fmt::format("SELECT * FROM {} WHERE imb_i_id = {} AND imb_u_id = {} ORDER BY ib_bid DESC LIMIT 1", TABLE_ITEM_BID, item_id, seller_id);
      client.Query(getMaxBid, queryResult, timeout);
      uint64_t bid_id;
      //TODO: deserialize:

      std::string insertItemMaxBid = fmt::format("INSERT INTO {} (imb_i_id, imb_u_id, imb_ib_id, imb_ib_i_id, imb_ib_u_id, imb_created, imb_updated) "
                                                  "VALUES ({}, {}, {}, {}, {}, {}, {})", TABLE_ITEM_MAX_BID, item_id, seller_id, bid_id, item_id, seller_id, current_time, current_time);
      client.Write(insertItemMaxBid, queryResult, timeout); //TODO: Make async. (doesn't matter, inserts are always buffered for us)

      //TODO: We must cache this in order to be able to read from it. //TODO: Try to re-structure this transaction to avoid this write.
  }

  // Get the ITEM_MAX_BID record so that we know what we need to process. At this point we should always have an ITEM_MAX_BID record
 
 
  std::string getItemInfo = fmt::format("SELECT i_num_bids, i_current_price, i_end_date, ib_id, i_end_date, ib_id, ib_buyer_id, u_balance "
                                        "FROM {}, {}, {}, {} "
                                        "WHERE i_id = ? AND i_u_id = ? "
                                          "AND imb_i_id = i_id AND imb_u_id = i_u_id "
                                          "AND imb_ib_id = ib_id AND imb_ib_i_id = ib_i_id AND imb_ib_u_id = ib_u_id "
                                          "AND ib_buyer_id = u_id", TABLE_ITEM, TABLE_ITEM_MAX_BID, TABLE_ITEM_BID, TABLE_USERACCT,
                                          item_id, seller_id);
  client.Query(getItemInfo, queryResult, timeout);
  if(queryResult.empty()){
    Debug("No ITEM_MAX_BID is available record for item")
    return client.Abort(timeout);
  }
  getItemInfoRow iir;
  deserialize(iir, queryResult);
  

  if (iir.i_current_price > (buyer_credit + iir.u_balance)) {
    Debug("Not enough money to buy item");
    return client.Abort(timeout);
  }

  //TODO: parallelize all these writes (make async)
  //std::string getBuyerInfo = fmt::format("SELECT u_id, u_balance FROM {} WHERE u_id = {}", TABLE_USER_ACCT, seller_id);

  // Set item_purchase_id
  std::string insertPurchase = fmt::format("INSERT INTO {} (ip_id, ip_ib_id, ip_ib_i_id, ip_ib_u_id, ip_date) "
                                            "VALUES ({}, {}, {}, {}, {}) ", TABLE_ITEM_PURCHASE, ip_id, iir.ib_id, item_id, seller_id, current_time);
  client.Write(insertPurchase, timeout, true);

  // Update item status to close
  std::string updateItem = fmt::format("UPDATE {} set i_status = {}, i_updated = {} WHERE i_id = {} AND i_u_id = {}", TABLE_ITEM, ItemStatus::CLOSED, current_time, item_id, seller_id);
   client.Write(insertPurchase, timeout, true);

  std::string updateUserBalance = fmt::format("UPDATE {} SET u_balance = u_balance + {} WHERE u_id = {}", TABLE_USERACCT);

   // Decrement the buyer's account
  std::string updateBuyerBalance = fmt::format(updateUserBalance, -1 * (iir.i_current_price) + buyer_credit, iir.ib_buyer_id);
  client.Write(updateBuyerBalance, timeout, true);

  // And credit the seller's account
  std::string updateSellerBalance = fmt::format(updateUserBalance, iir.i_current_price, seller_id);
  client.Write(updateSellerBalance, timeout, true);

  // And update this the USERACT_ITEM record to link it to the new ITEM_PURCHASE record
  // If we don't have a record to update, just go ahead and create it
  std::string updateUserItem = fmt::format("UPDATE {} SET ui_ip_id = {}, ui_ip_ib_id = {}, ui_ip_ib_i_id = {}, ui_ip_ib_u_id = {} "
                                                    "WHERE ui_u_id = {} AND ui_i_id = {} AND ui_i_u_id = {}", TABLE_USERACCT_ITEM, 
                                                    ip_id, iir.ib_id, item_id, seller_id, iir.ib_buyer_id, item_id, seller_id);
  client.Write(updateUserItem, queryResult, timeout);
  if(queryResult->empty() || !queryResult->has_rows_affected()){
    std::string insertUserItem = fmt::format("INSERT INTO {} (ui_u_id, ui_i_id, ui_i_u_id, ui_ip_id, ui_ip_ib_id, ui_ip_ib_i_id, ui_ip_ib_u_id, ui_created) "
                                             "VALUES ({}, {}, {}, {}, {}, {}, {}, {})", TABLE_USERACCT_ITEM,
                                             iir.ib_buyer_id, item_id, seller_id, ip_id, iir.ib_id, item_id, seller_id);
    client.Write(insertPurchase, timeout, true);
  }

  client.asyncWait();

  Debug("COMMIT");
  return client.Commit(timeout);
}

} // namespace auctionmark
