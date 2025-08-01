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
#include "store/benchmark/async/sql/auctionmark/transactions/new_purchase.h"
#include "store/benchmark/async/sql/auctionmark/utils/auctionmark_utils.h"
#include <fmt/core.h>

namespace auctionmark {

NewPurchase::NewPurchase(uint32_t timeout, AuctionMarkProfile &profile, std::mt19937_64 &gen) : AuctionMarkTransaction(timeout), profile(profile), gen(gen) {
  
  std::cerr << std::endl << "NEW PURCHASE" << std::endl;
  std::optional<ItemInfo> maybeItemInfo = profile.get_random_waiting_for_purchase_item();
  ItemInfo itemInfo;
  if (maybeItemInfo.has_value()) {
    itemInfo = maybeItemInfo.value();
  } else {
    throw std::runtime_error("new_purchase construction: failed to get random waiting for purchase item");
  }
  item_id = itemInfo.get_item_id().encode();
  UserId sellerId = itemInfo.get_seller_id();
  seller_id = sellerId.encode();
  buyer_credit = 0;

  int ip_id_cnt = profile.ip_id_cntrs[item_id];
  
  //ip_id = ItemId(sellerId, ip_id_cnt).encode();
  //ip_id = GetUniqueElementId(item_id, ip_id_cnt);  //FIXME: THIS DOESN'T MAKE SENSE? How can ip_id of type BIG_INT be this huge string?
  ip_id = ip_id_cnt;
  profile.ip_id_cntrs[item_id] = (ip_id_cnt < 127) ? ip_id_cnt + 1 : 0;

  // Whether the buyer will not have enough money
  if (itemInfo.has_current_price()) {
    if (std::uniform_int_distribution<int>(1, 100)(gen) <= PROB_NEWPURCHASE_NOT_ENOUGH_MONEY) {
      buyer_credit = -1 * (itemInfo.get_current_price());
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

  uint64_t current_time = GetProcTimestamp({profile.get_loader_start_time(), profile.get_client_start_time()});

  //NOTE: Item max bid MUST exist, otherwise we wouldn't have selected this item for Purchase  => See "new_purchase_old.cc" that includes a benchbase hack that seems unecessary

  // Get the ITEM_MAX_BID record so that we know what we need to process. At this point we should always have an ITEM_MAX_BID record
  std::string getItemInfo = fmt::format("SELECT i_num_bids, i_current_price, i_end_date, ib_id, ib_buyer_id, u_balance "
                                      "FROM {}, {}, {}, {} "
                                      "WHERE i_id = '{}' AND i_u_id = '{}' "
                                      "AND imb_i_id = '{}' AND imb_u_id = '{}' " //added redundancies for better scan choice...
                                      //"AND imb_i_id = i_id AND imb_u_id = i_u_id "
                                      "AND imb_ib_id = ib_id "
                                      "AND ib_i_id = '{}' AND ib_u_id = '{}' "  // because imb_ib_i_id == imb_i_d and imb_ib_u_id = imb_u_id.
                                      //"AND imb_ib_i_id = ib_i_id AND imb_ib_u_id = ib_u_id "
                                      "AND ib_buyer_id = u_id "
                                      "AND ib_id = ib_id "
                                      "AND u_id = u_id", //ADDED REFLEXIVE ARG FOR PELOTON PARSING. TODO: AUTOMATE THIS IN SQL_INTERPRETER 
                                      TABLE_ITEM, TABLE_ITEM_MAX_BID, TABLE_ITEM_BID, TABLE_USERACCT,
                                      item_id, seller_id, item_id, seller_id, item_id, seller_id);
                                      //This query should do Primary index scan for Item, ItemMaxBid
                                      //After that, it should be able to do primary index scan on ItemBid and UserAcct via NestedLoop join
                                      // //The only unknown are: ib_id and u_id
  client.Query(getItemInfo, queryResult, timeout);
 
  if(queryResult->empty()){
    Panic("No ITEM_MAX_BID is available record for item");
    client.Abort(timeout);
    return ABORTED_USER;
  }
  getItemInfoRow iir;
  deserialize(iir, queryResult);

  if (iir.i_current_price > (buyer_credit + iir.u_balance)) {
    Debug("Not enough money to buy item");
    client.Abort(timeout);
    return ABORTED_USER;
  }

  std::string insertPurchase = fmt::format("INSERT INTO {} (ip_ib_i_id, ip_ib_u_id, ip_id, ip_ib_id, ip_date) "
                                            "VALUES ('{}', '{}', {}, {}, {}) ", TABLE_ITEM_PURCHASE, item_id, seller_id, ip_id, iir.ib_id, current_time);
  client.Write(insertPurchase, timeout);

  // Update item status to close
  std::string updateItem = fmt::format("UPDATE {} SET i_status = {}, i_updated = {} WHERE i_id = '{}' AND i_u_id = '{}'", TABLE_ITEM, ItemStatus::CLOSED, current_time, item_id, seller_id);
  client.Write(updateItem, timeout, true);


  std::string updateUserBalance = "UPDATE " + std::string(TABLE_USERACCT) + " SET u_balance = u_balance + {} WHERE u_id = '{}'";

   // Decrement the buyer's account
  std::string updateBuyerBalance = fmt::format(updateUserBalance, -1 * (iir.i_current_price) + buyer_credit, iir.ib_buyer_id);
  client.Write(updateBuyerBalance, timeout, true);

  // And credit the seller's account
  std::string updateSellerBalance = fmt::format(updateUserBalance, iir.i_current_price, seller_id);
  client.Write(updateSellerBalance, timeout, true);

  // And update this the USERACT_ITEM record to link it to the new ITEM_PURCHASE record
  // If we don't have a record to update, just go ahead and create it
  //std::cerr << "Inserting UserAcct_ITEM" << std::endl;
  std::string updateUserItem = fmt::format("UPDATE {} SET ui_ip_id = {}, ui_ip_ib_id = {}, ui_ip_ib_i_id = '{}', ui_ip_ib_u_id = '{}' "
                                                    "WHERE ui_u_id = '{}' AND ui_i_id = '{}' AND ui_i_u_id = '{}'", TABLE_USERACCT_ITEM, 
                                                    ip_id, iir.ib_id, item_id, seller_id, iir.ib_buyer_id, item_id, seller_id);
  client.Write(updateUserItem, queryResult, timeout);

  if(!queryResult->has_rows_affected()){
    UW_ASSERT(current_time <= INT64_MAX);
    std::string insertUserItem = fmt::format("INSERT INTO {} (ui_u_id, ui_i_id, ui_i_u_id, ui_ip_id, ui_ip_ib_id, ui_ip_ib_i_id, ui_ip_ib_u_id, ui_created) "
                                             "VALUES ('{}', '{}', '{}', {}, {}, '{}', '{}', {})", TABLE_USERACCT_ITEM,
                                             iir.ib_buyer_id, item_id, seller_id, ip_id, iir.ib_id, item_id, seller_id, current_time);
    client.Write(insertUserItem, timeout, true, true); //async, blind-write: If Update fails, it already contains a read set.
  }

  client.asyncWait();
  client.Wait(results);

  //NOTE: THIS MAY FAIL BECAUSE CLIENTS CACHE OUT OF SYNC (ANOTHER CLIENT MIGHT HAVE ISSUED THE PURCHASE.) In this case: update cache and pick a different TX.
  if(!results[0]->has_rows_affected() || results[0]->rows_affected() == 0){ //If purchase fails.
    Notice("Item has already been purchased");
    //Update the cache
    ItemRecord item_rec(item_id, seller_id, "", iir.i_current_price, iir.i_num_bids, iir.i_end_date, ItemStatus::CLOSED); // iir.ib_id, iir.ib_buyer_id, ip_id missing? Doesn't seem to be needed.
    ItemId itemId = profile.processItemRecord(item_rec);

    client.Abort(timeout);
    return ABORTED_USER;
  }

  Debug("COMMIT");
  auto tx_result = client.Commit(timeout);
  if(tx_result != transaction_status_t::COMMITTED) return tx_result;
   
   //////////////// UPDATE PROFILE /////////////////////
  ItemRecord item_rec(item_id, seller_id, "", iir.i_current_price, iir.i_num_bids, iir.i_end_date, ItemStatus::CLOSED); // iir.ib_id, iir.ib_buyer_id, ip_id missing? Doesn't seem to be needed.
  ItemId itemId = profile.processItemRecord(item_rec);

  return tx_result;
}

} // namespace auctionmark
