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

NewPurchase::NewPurchase(uint32_t timeout, std::mt19937_64 &gen) : AuctionMarkTransaction(timeout),
      gen(gen) {
}

NewPurchase::~NewPurchase(){
}

transaction_status_t NewPurchase::Execute(SyncClient &client) {
  std::unique_ptr<const query_result::QueryResult> queryResult;
  std::string statement;
  std::vector<std::unique_ptr<const query_result::QueryResult>> results;

  Debug("NEW PURCHASE");

  //TODO: parameterize
  std::string item_id;
  std::string seller_id;
  std::string ip_id;
  double buyer_credit;

  client.Begin(timeout);

  uint64_t current_time = std::time(0); //TODO: Revisit this?

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

      //TODO: We must cache this in order to be able to read from it.
  }

  // Get the ITEM_MAX_BID record so that we know what we need to process. At this point we should always have an ITEM_MAX_BID record
  uint64_t i_num_bids;
  double i_current_price;
  uint64_t i_end_date;
  uint64_t i_status;
  uint64_t ib_id;
  uint64_t ib_buyer_id;
  double u_balance;
 
  std::string getItemInfo = fmt::format("SELECT i_num_bids, i_current_price, i_end_date, ib_id, i_end_date, ib_id, ib_buyer_id, u_balance "
                                        "FROM {}, {}, {}, {} "
                                        "WHERE i_id = ? AND i_u_id = ? "
                                          "AND imb_i_id = i_id AND imb_u_id = i_u_id "
                                          "AND imb_ib_id = ib_id AND imb_ib_i_id = ib_i_id AND imb_ib_u_id = ib_u_id "
                                          "AND ib_buyer_id = u_id", TABLE_ITEM, TABLE_ITEM_MAX_BID, TABLE_ITEM_BID, TABLE_USER_ACCT,
                                          item_id, seller_id);
  client.Query(getItemInfo, queryResult, timeout);
  if(queryResult.empty()){
    Debug("No ITEM_MAX_BID is available record for item")
    return client.Abort(timeout);
  }
  //TODO: deserialize;

  if (i_current_price > (buyer_credit + u_balance)) {
    Debug("Not enough money to buy item");
    return client.Abort(timeout);
  }

  //TODO: parallelize all these writes (make async)
  //std::string getBuyerInfo = fmt::format("SELECT u_id, u_balance FROM {} WHERE u_id = {}", TABLE_USER_ACCT, seller_id);

  // Set item_purchase_id
  std::string insertPurchase = fmt::format("INSERT INTO {} (ip_id, ip_ib_id, ip_ib_i_id, ip_ib_u_id, ip_date) "
                                            "VALUES ({}, {}, {}, {}, {}) ", TABLE_ITEM_PURCHASE, ip_id, ib_id, item_id, seller_id, current_time);
  client.Write(insertPurchase, timeout, true);

  // Update item status to close
  std::string updateItem = fmt::format("UPDATE {} set i_status = {}, i_updated = {} WHERE i_id = {} AND i_u_id = {}", TABLE_ITEM, ItemStatus::CLOSED, current_time, item_id, seller_id);
   client.Write(insertPurchase, timeout, true);

  std::string updateUserBalance = fmt::format("UPDATE {} SET u_balance = u_balance + {} WHERE u_id = {}", TABLE_USER_ACCT);

   // Decrement the buyer's account
  std::string updateBuyerBalance = fmt::format(updateUserBalance, -1 * (i_current_price) + buyer_credit, ib_buyer_id);
  client.Write(updateBuyerBalance, timeout, true);

  // And credit the seller's account
  std::string updateSellerBalance = fmt::format(updateUserBalance, i_current_price, seller_id);
  client.Write(updateSellerBalance, timeout, true);

  // And update this the USERACT_ITEM record to link it to the new ITEM_PURCHASE record
  // If we don't have a record to update, just go ahead and create it
  std::string updateUserItem = fmt::format("UPDATE {} SET ui_ip_id = {}, ui_ip_ib_id = {}, ui_ip_ib_i_id = {}, ui_ip_ib_u_id = {} "
                                                    "WHERE ui_u_id = {} AND ui_i_id = {} AND ui_i_u_id = {}", TABLE_USER_ACCT_ITEM, 
                                                    ip_id, ib_id, item_id, seller_id, ib_buyer_id, item_id, seller_id);
  client.Write(updateUserItem, queryResult, timeout);
  if(queryResult->empty() || !queryResult->has_rows_affected()){
    std::string insertUserItem = fmt::format("INSERT INTO {} (ui_u_id, ui_i_id, ui_i_u_id, ui_ip_id, ui_ip_ib_id, ui_ip_ib_i_id, ui_ip_ib_u_id, ui_created) "
                                             "VALUES ({}, {}, {}, {}, {}, {}, {}, {})", TABLE_USER_ACCT_ITEM,
                                             ib_buyer_id, item_id, seller_id, ip_id, ib_id, item_id, seller_id);
    client.Write(insertPurchase, timeout, true);
  }

  client.asyncWait();

  Debug("COMMIT");
  return client.Commit(timeout);
}

} // namespace auctionmark
