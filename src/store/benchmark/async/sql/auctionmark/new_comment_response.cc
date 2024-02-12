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
#include "store/benchmark/async/sql/auctionmark/new_comment_response.h"
#include <fmt/core.h>

namespace auctionmark {

NewCommentResponse::NewCommentResponse(uint32_t timeout, AuctionMarkProfile &profile, std::mt19937_64 &gen) : AuctionMarkTransaction(timeout), gen(gen) {
  //TODO: Parameter generation
}

NewCommentResponse::~NewCommentResponse(){
}

transaction_status_t NewCommentResponse::Execute(SyncClient &client) {
  std::unique_ptr<const query_result::QueryResult> queryResult;
  std::string statement;
  std::vector<std::unique_ptr<const query_result::QueryResult>> results;

  Debug("NEW COMMENT RESPONSE");

  client.Begin(timeout);

  uint64_t current_time = std::time(0);

   statement = fmt::format("UPDATE {} SET ic_response = {}, ic_updated = {} WHERE ic_id = {} AND ic_i_id = {} AND ic_u_id = {}", 
                          TABLE_ITEM_COMMENT, response, current_time, comment_id, item_id, seller_id);
  client.Write(statement, timeout, true);

   statement = fmt::format("UPDATE {} SET u_comments = u_comments - 1, u_updated = {} WHERE u_id = {}", TABLE_USERACCT, current_time, seller_id);
   client.Write(statement, timeout, true);

  client.asyncWait();

  return client.Commit(timeout);
}

} // namespace auctionmark
