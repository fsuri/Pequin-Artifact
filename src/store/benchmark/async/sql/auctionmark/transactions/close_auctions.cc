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
#include "store/benchmark/async/sql/auctionmark/transactions/close_auctions.h"
#include <fmt/core.h>

namespace auctionmark {

CloseAuctions::CloseAuctions(uint32_t timeout, AuctionMarkProfile &profile, std::mt19937_64 &gen) : AuctionMarkTransaction(timeout), profile(profile)
{
  std::cerr << std::endl << "CLOSE AUCTION" << std::endl;
  //generate params
  start_time = profile.get_last_close_auctions_time();
  end_time = profile.update_and_get_last_close_auctions_time();
  benchmark_times = {profile.get_loader_start_time(), profile.get_client_start_time()};
}

CloseAuctions::~CloseAuctions(){
}

transaction_status_t CloseAuctions::Execute(SyncClient &client) {
  std::unique_ptr<const query_result::QueryResult> queryResult;
  std::string statement;
  std::vector<std::unique_ptr<const query_result::QueryResult>> results;

  Debug("CLOSE AUCTION");
  Debug("Start Time: %lu", start_time);
  Debug("End Time: %lu", end_time);



  client.Begin(timeout);

  int closed_ctr = 0;
  int waiting_ctr = 0;
  int round = CLOSE_AUCTIONS_ROUNDS;

  uint64_t current_time = GetProcTimestamp(benchmark_times);
  std::cerr << "last auction: " << start_time << std::endl;
  std::cerr << "this auction: " << end_time << std::endl;
  std::cerr << "current time: " << current_time << std::endl;
  
  std::string getDueItems = fmt::format("SELECT {} FROM {} WHERE i_start_date >= {} AND i_start_date <= {} AND i_status = {} "
                                        "ORDER BY i_id ASC LIMIT {}", ITEM_COLUMNS_STR, TABLE_ITEM, start_time, end_time, ItemStatus::OPEN, CLOSE_AUCTIONS_ITEMS_PER_ROUND);

  std::string getMaxBid = "SELECT imb_ib_id, ib_buyer_id FROM " + std::string(TABLE_ITEM_MAX_BID) + ", " + std::string(TABLE_ITEM_BID) + 
                                        "WHERE imb_i_id = '{}' AND imb_u_id = '{}' AND ib_id = imb_ib_id AND ib_i_id = imb_i_id AND ib_u_id = imb_u_id "
                                        "AND ib_ib = ib_ib AND ib_i_id = ib_i_id AND ib_u_id = ib_u_id"; //ADDED REFLEXIVE FOR PELOTON PARSING //TODO: AUTOMATIZE IN SQL INTERPRETER
  // std::string getMaxBid = fmt::format("SELECT imb_ib_id, ib_buyer_id FROM {}, {} WHERE imb_i_id = '{}' AND imb_u_id = '{}' "
  //                                     "AND ib_id = imb_ib_id AND ib_i_id = imb_i_id AND ib_u_id = imb_u_id", TABLE_ITEM_MAX_BID, TABLE_ITEM_BID);
                                       //TODO: Add redundant inputs?

  while(round-- > 0){
    std::cerr << "round: " << (CLOSE_AUCTIONS_ROUNDS - round) << std::endl;
    client.Query(getDueItems, queryResult, timeout);
    //skip first row of result. If don't have any, break
    if(queryResult->empty()) break;

    std::cerr << "PROCESSING #items: " << queryResult->size() << std::endl;
    //For row in result:
    for(int i = 1; i<queryResult->size(); ++i){
      getDueItemRow dir;
      deserialize(dir, queryResult, i);

      Debug("Getting max bid for itemId=%s / sellerId=%s", dir.itemId.c_str(), dir.sellerId.c_str());

      ItemStatus itemStatus = dir.itemStatus;

      // Has bid on this item - set status to WAITING_FOR_PURCHASE
      // We'll also insert a new USER_ITEM record as needed
      // We have to do this extra step because H-Store doesn't have good support in the query optimizer for LEFT OUTER JOINs  //THIS IS A COMMENT FROM BENCHBASE

      getMaxBidRow mbr;
      if(dir.numBids > 0){
        waiting_ctr++;
        std::string getMaxBid_stmt = fmt::format(getMaxBid, dir.itemId, dir.sellerId);
        client.Query(getMaxBid_stmt, queryResult, timeout);

  
        deserialize(mbr, queryResult);

        std::string insertUserItem = fmt::format("INSERT INTO {} (ui_u_id, ui_i_id, ui_i_u_id, ui_created) "
                                           "VALUES('{}', '{}', '{}', {})", TABLE_USERACCT_ITEM, mbr.buyerId, dir.itemId, dir.sellerId, current_time);
        client.Write(insertUserItem, timeout, true);

        itemStatus = ItemStatus::WAITING_FOR_PURCHASE;

      }
      // No bid on this item - set status to CLOSED
      else{
        closed_ctr++;
        itemStatus = ItemStatus::CLOSED;
      }


      std::string updateItemStatus = fmt::format("UPDATE {} SET i_status = {}, i_updated = {} WHERE i_id = '{}' AND i_u_id = '{}'", TABLE_ITEM, itemStatus, current_time, dir.itemId, dir.sellerId);
      client.Write(updateItemStatus, timeout, true);


      item_records.push_back(ItemRecord(dir.itemId, dir.sellerId, dir.i_name, dir.currentPrice, dir.numBids, dir.endDate, dir.itemStatus, mbr.bidId, mbr.buyerId));
    }
    client.asyncWait();
  }

  Debug("COMMIT CLOSE AUCTION");
  auto tx_result = client.Commit(timeout);
  if(tx_result != transaction_status_t::COMMITTED) return tx_result;

   //////////////// UPDATE PROFILE /////////////////////
  UpdateProfile();
 
  return tx_result;
}

void CloseAuctions::UpdateProfile(){
  for(auto &item_rec: item_records){
    ItemId itemId = profile.processItemRecord(item_rec);
  }

  profile.update_item_queues();
}

} // namespace auctionmark
