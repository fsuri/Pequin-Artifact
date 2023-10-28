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

NewPurchase::NewPurchase(uint32_t timeout, uint64_t ib_id, uint64_t i_id, uint64_t u_id,
      uint64_t buyer_id, std::mt19937 &gen) : AuctionMarkTransaction(timeout),
      ib_id(ib_id), i_id(i_id), u_id(u_id), buyer_id(buyer_id) {
}

NewPurchase::~NewPurchase(){
}

transaction_status_t NewPurchase::Execute(SyncClient &client) {
  std::unique_ptr<const query_result::QueryResult> queryResult;
  std::string statement;
  std::vector<std::unique_ptr<const query_result::QueryResult>> results;

  Debug("NEW PURCHASE");
  Debug("Item Bid ID: %lu", ib_id);
  Debug("Item ID: %lu", i_id);
  Debug("Buyer ID: %lu", buyer_id);

  client.Begin(timeout);

  statement = fmt::format("SELECT imb_ib_id, imb_ib_i_id, imb_ib_u_id FROM ITEM_MAX_BID "
                          "WHERE imb_i_id = {} AND imb_u_id = {};",
                          i_id, u_id);
  client.Query(statement, queryResult, timeout);
  uint64_t imb_ib_id, imb_ib_i_id, imb_ib_u_id;
  queryResult->at(0)->get(0, &imb_ib_id);
  queryResult->at(0)->get(1, &imb_ib_i_id);
  queryResult->at(0)->get(2, &imb_ib_u_id);

  statement = fmt::format("SELECT ib_buyer_id FROM ITEM_BID WHERE ib_id = {} "
                        "AND ib_i_id = {} AND ib_u_id = {};",
                        imb_ib_id, imb_ib_i_id, imb_ib_u_id);
  client.Query(statement, queryResult, timeout);
  uint64_t ib_buyer_id;
  queryResult->at(0)->get(0, &ib_buyer_id);

  assert(ib_id == imb_ib_id);
  assert(buyer_id == ib_buyer_id);

  statement = fmt::format("(SELECT MAX(ip_id) FROM ITEM_PURCHASE WHERE ip_ib_id = {} "
                          "AND ip_ib_i_id = {} AND ip_ib_u_id = {}) + 1;",
                          ib_id, i_id, u_id);
  client.Query(statement, queryResult, timeout);
  uint64_t ip_id;
  queryResult->at(0)->get(0, &ip_id);

  statement = fmt::format("INSERT INTO ITEM_PURCHASE (ip_id, ip_ib_id, ip_ib_i_id, "
                          "ip_ib_u_id, ip_date) VALUES({}, {}, {}, {}, {});",
                          ip_id, ib_id, i_id, u_id, 0);
  client.Write(statement, queryResult, timeout);
  assert(queryResult->has_rows_affected());

  statement = fmt::format("UPDATE ITEM SET i_status = 2 WHERE i_id = {} AND i_u_id = {};",
                          i_id, u_id);
  client.Write(statement, queryResult, timeout);
  assert(queryResult->has_rows_affected());

  Debug("COMMIT");
  return client.Commit(timeout);
}

} // namespace auctionmark
