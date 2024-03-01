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
#include "store/benchmark/async/sql/auctionmark/post_auction.h"
#include <fmt/core.h>

namespace auctionmark {

PostAuction::PostAuction(uint32_t timeout, std::vector<uint64_t> i_ids, std::vector<uint64_t> seller_ids,
   std::vector<uint64_t> buyer_ids, std::vector<std::optional<uint64_t>> ib_ids, std::mt19937_64 &gen) : 
    AuctionMarkTransaction(timeout), i_ids(i_ids), seller_ids(seller_ids), buyer_ids(buyer_ids), ib_ids(ib_ids) {
}

PostAuction::~PostAuction(){
}

transaction_status_t PostAuction::Execute(SyncClient &client) {
  std::unique_ptr<const query_result::QueryResult> queryResult;
  std::string statement;
  std::vector<std::unique_ptr<const query_result::QueryResult>> results;

  Debug("POST AUCTION");

  client.Begin(timeout);

  for (std::size_t i = 0; i < i_ids.size(); i++) {
    std::optional<uint64_t> ib_id = ib_ids[i];
    uint64_t i_id = i_ids[i];
    uint64_t seller_id = seller_ids[i];
    uint64_t buyer_id = buyer_ids[i];

    if (!ib_id.has_value()) {
      statement = fmt::format("UPDATE ITEM SET i_status = 2 WHERE i_id = {} AND i_u_id = {};",
                              i_id, seller_id);
      client.Write(statement, queryResult, timeout);
      assert(queryResult->has_rows_affected());
    } else {
      statement = fmt::format("UPDATE ITEM SET i_status = 1 WHERE i_id = {} AND i_u_id = {};",
                              i_id, seller_id);
      client.Write(statement, queryResult, timeout);
      assert(queryResult->has_rows_affected());

      statement = fmt::format("INSERT INTO USER_ITEM (ui_u_id, ui_i_id, ui_i_u_id, ui_created) "
                              "VALUES({}, {}, {}, {});", 
                              buyer_id, i_id, seller_id, 0);
      client.Write(statement, queryResult, timeout);
      assert(queryResult->has_rows_affected());
    }
  }
  
  Debug("COMMIT");
  return client.Commit(timeout);
}

} // namespace auctionmark
