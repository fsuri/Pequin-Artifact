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

  Debug("CHECK WINNING BIDS");
  Debug("Start Time: %lu", start_time);
  Debug("End Time: %lu", end_time);

  client.Begin(timeout);

  statement = fmt::format("SELECT i_id, i_u_id, i_status FROM ITEM WHERE i_start_date "
                          "BETWEEN {} AND {} AND i_status = 0 LIMIT 100;",
                          start_time, end_time);
  client.Query(statement, queryResult, timeout);
  uint64_t query_i_id, query_i_u_id, query_i_status;

  for(std::size_t i = 0; i < queryResult->size(); i++) {
    queryResult->at(i)->get(0, &query_i_id);
    queryResult->at(i)->get(1, &query_i_u_id);
    queryResult->at(i)->get(2, &query_i_status);

    uint64_t max_bid_id;
    statement = fmt::format("SELECT imb_ib_id FROM ITEM_MAX_BID WHERE imb_i_id = {} "
                            "AND imb_u_id = {};",
                            query_i_id, query_i_u_id);
    std::unique_ptr<const query_result::QueryResult> temp_result;
    client.Query(statement, temp_result, timeout);

    Debug("Item Id: %lu", query_i_id);
    Debug("Item User Id: %lu", query_i_u_id);
    if (!temp_result->empty()) {
      temp_result->at(0)->get(0, &max_bid_id);
      uint64_t buyer_id;
      statement = fmt::format("SELECT ib_buyer_id FROM ITEM_BID WHERE ib_id = {} AND "
                              "ib_i_id = {} AND ib_u_id = {};",
                              max_bid_id, query_i_id, query_i_u_id);
      client.Query(statement, temp_result, timeout);
      temp_result->at(0)->get(0, &buyer_id);
      i_ids.push_back(query_i_id);
      seller_ids.push_back(query_i_u_id);
      buyer_ids.push_back(buyer_id);
      ib_ids.push_back(max_bid_id);
      Debug("Buyer Id: %lu", buyer_id);
    } else {
      i_ids.push_back(query_i_id);
      seller_ids.push_back(query_i_u_id);
      buyer_ids.push_back(std::nullopt);
      ib_ids.push_back(std::nullopt);
      Debug("Item Status: %lu", query_i_status);
    }
  }
  
  Debug("COMMIT");
  return client.Commit(timeout);
}

} // namespace auctionmark
