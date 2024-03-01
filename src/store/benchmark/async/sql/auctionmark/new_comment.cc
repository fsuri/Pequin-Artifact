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
#include "store/benchmark/async/sql/auctionmark/new_comment.h"
#include <fmt/core.h>

namespace auctionmark {

NewComment::NewComment(uint32_t timeout, std::string question, 
      std::mt19937_64 &gen) : AuctionMarkTransaction(timeout), 
      question(question), gen(gen) {
}

NewComment::~NewComment(){
}

transaction_status_t NewComment::Execute(SyncClient &client) {
  std::unique_ptr<const query_result::QueryResult> queryResult;
  std::string statement;
  std::vector<std::unique_ptr<const query_result::QueryResult>> results;

  Debug("NEW COMMENT");

  client.Begin(timeout);

  // Choose a random puchased item
  statement = fmt::format("SELECT COUNT(*) AS cnt FROM USER_ITEM;");
  client.Query(statement, queryResult, timeout);
  uint64_t user_items_cnt;
  deserialize(user_items_cnt, queryResult);

  uint64_t user_item_id = std::uniform_int_distribution<uint64_t>(0, user_items_cnt - 1)(gen);
  statement = fmt::format("SELECT ui_u_id, ui_i_id, ui_seller_id FROM USER_ITEM LIMIT {}, 1;", user_item_id);
  client.Query(statement, queryResult, timeout);
  queryResult->at(0)->get(0, &buyer_id);
  queryResult->at(0)->get(1, &i_id);
  queryResult->at(0)->get(2, &seller_id);

  Debug("Item ID: %lu", i_id);
  Debug("Buyer ID: %lu", buyer_id);
  Debug("Seller ID: %lu", seller_id);

  // Post comment on selected item
  statement = fmt::format("INSERT INTO ITEM_COMMENT (ic_id, ic_i_id, ic_u_id, "
                          "ic_buyer_id, ic_date, ic_question) VALUES (("
                          "SELECT MAX(ic_id) FROM ITEM_COMMENT WHERE ic_i_id = {} "
                          "AND ic_u_id = {}) + 1, {}, {}, {}, {}, {});",
                          i_id, seller_id, i_id, seller_id, buyer_id, 0, question);
  client.Write(statement, queryResult, timeout);
  assert(queryResult->has_rows_affected());
  
  Debug("COMMIT");
  return client.Commit(timeout);
}

} // namespace auctionmark
