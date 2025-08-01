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
#include "store/benchmark/async/sql/auctionmark/get_comment.h"
#include <fmt/core.h>

namespace auctionmark {

GetComment::GetComment(uint32_t timeout, std::mt19937_64 &gen) : 
    AuctionMarkTransaction(timeout), gen(gen) {
}

GetComment::~GetComment(){
}

transaction_status_t GetComment::Execute(SyncClient &client) {
  std::unique_ptr<const query_result::QueryResult> queryResult;
  std::string statement;
  std::vector<std::unique_ptr<const query_result::QueryResult>> results;

  Debug("GET COMMENT");

  client.Begin(timeout);

  // Choose a random puchased item
  statement = fmt::format("SELECT COUNT(DISTINCT(ui_seller_id)) AS cnt FROM USER_ITEM;");
  client.Query(statement, queryResult, timeout);
  uint64_t sellers_cnt;
  deserialize(sellers_cnt, queryResult);

  uint64_t item_seller_id = std::uniform_int_distribution<uint64_t>(0, sellers_cnt - 1)(gen);
  statement = fmt::format("SELECT DISTINCT(ui_seller_id) FROM USER_ITEM LIMIT {}, 1;", item_seller_id);
  client.Query(statement, queryResult, timeout);
  queryResult->at(0)->get(0, &seller_id);

  Debug("Comment Seller ID: %lu", seller_id);

  statement = fmt::format("SELECT * FROM ITEM_COMMENT WHERE ic_u_id = {} AND "
                          "ic_response IS NULL;", seller_id);
  client.Query(statement, queryResult, timeout);
  for (std::size_t i = 0; i < queryResult->size(); i++) {
    ItemCommentRow ic;
    deserialize(ic, queryResult, i);
    assert(ic.get_ic_u_id() == seller_id);
  }
  
  Debug("COMMIT");
  return client.Commit(timeout);
}

} // namespace auctionmark
