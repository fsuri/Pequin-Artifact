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

NewComment::NewComment(uint32_t timeout, uint64_t i_id, uint64_t seller_id, uint64_t buyer_id,
      std::string question, std::mt19937 &gen) : AuctionMarkTransaction(timeout),
      i_id(i_id), seller_id(seller_id), buyer_id(buyer_id), question(question) {
}

NewComment::~NewComment(){
}

transaction_status_t NewComment::Execute(SyncClient &client) {
  std::unique_ptr<const query_result::QueryResult> queryResult;
  std::string statement;
  std::vector<std::unique_ptr<const query_result::QueryResult>> results;

  Debug("NEW COMMENT");
  Debug("Item ID: %lu", i_id);
  Debug("Seller ID: %lu", seller_id);
  Debug("Buyer ID: %lu", buyer_id);

  client.Begin(timeout);

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
