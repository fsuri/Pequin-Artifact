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
#include "store/benchmark/async/sql/auctionmark/close_auctions.h"
#include <fmt/core.h>

namespace auctionmark {

CloseAuctions::CloseAuctions(uint32_t timeout, uint64_t start_time, 
  uint64_t end_time, std::vector<uint64_t> &i_ids, std::vector<uint64_t> &seller_ids,
  std::vector<std::optional<uint64_t>> &buyer_ids, std::vector<std::optional<uint64_t>> &ib_ids, 
  std::mt19937_64 &gen) : AuctionMarkTransaction(timeout), start_time(start_time), 
  end_time(end_time), i_ids(i_ids), seller_ids(seller_ids), buyer_ids(buyer_ids), ib_ids(ib_ids)
{
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
  int col = -1;

  uint64_t current_time = std::time(0);


  std::string getDueItems = fmt::format("SELECT {} FROM {} WHERE i_start_date >= {} AND i_start_date <= {} AND i_status = {} "
                                        "ORDER BY i_id ASC LIMIT {}", ITEM_COLUMNS_STR, TABLE_ITEM, CLOSE_AUCTIONS_ITEMS_PER_ROUND, start_time, end_time, ItemStatus::OPEN);

  std::string getMaxBid = fmt::format("SELECT imb_ib_id, ib_buyer_id FROM {}, {} "
                                        "WHERE imb_i_id = {} AND imb_u_id = {} AND ib_id = imb_ib_id AND ib_i_id = imb_i_id AND ib_u_id = imb_u_id ", 
                                        TABLE_ITEM_MAX_BID, TABLE_ITEM_BID); //TODO: Add redundant inputs?

  while(round-- > 0){
    client.Query(getDueItems, queryResult, timeout);
    //skip first row of result. If don't have any, break
    if(queryResult->empty()) break;

    //For row in result:
    for(int i = 1; i<queryResult->size(); ++i){
      std::unique_ptr<query_result::Row> row = (*q_result)[i]; 

      //TODO: deserialize
      std::string itemId;
      std::string sellerId;
      std::string i_name;
      double currentPrice;
      double numBids;
      uint64_t endDate;
      ItemStatus itemStatus;
      uint64_t bidId = 0;
      std::string buyerId = "";

      Debug("Getting max bid for itemId=%s / sellerId=%s", itemId.c_str(), sellerId.c_str());

      // Has bid on this item - set status to WAITING_FOR_PURCHASE
      // We'll also insert a new USER_ITEM record as needed
      // We have to do this extra step because H-Store doesn't have good support in the query optimizer for LEFT OUTER JOINs  //THIS IS A COMMENT FROM BENCHBASE

      if(numBids > 0){
        waiting_ctr++;
        std::string getMaxBid_stmt = fmt::format(getMaxBid, itemId, sellerId);
        client.Query(getMaxBid_stmt, queryResult, timeout);

        //TODO: deserialize:
        //bidId
        //buyerId

        std::string insertUserItem = fmt::format("INSERT INTO {} (ui_u_id, ui_i_id, ui_i_u_id, ui_created) "
                                           "VALUES({}, {}, {}, {})", TABLE_USER_ACCT_ITEM, buyerId, itemId, sellerId, current_time);
        client.Write(insertUserItem, queryResult, timeout);

        itemStatus = ItemStatus::WAITING_FOR_PURCHASE;

      }
      // No bid on this item - set status to CLOSED
      else{
        closed_ctr++;
        itemStatus = ItemStatus::CLOSED;
      }


      std::string updateItemStatus = fmt::format("UPDATE {} SET i_status = {}, i_updated = {} WHERE i_id = {} AND i_u_id = {}", TABLE_ITEM, itemStatus, current_time, itemId, sellerId);
      client.Write(updateItemStatus, queryResult, timeout);



    }
  }

  
  
  Debug("COMMIT CLOSE AUCTION");
  return client.Commit(timeout);
}

} // namespace auctionmark
