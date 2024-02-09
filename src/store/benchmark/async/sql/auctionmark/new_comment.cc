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

  //TODO: parameterize
  std::string item_id;
  std::string seller_id;
  std::string buyer_id;
  std::string question;

  client.Begin(timeout);

  //Set comment_id;
  uint64_t ic_id = 0;
  //getItemComments
  statement = fmt::format("SELECT i_num_comments FROM {} WHERE i_id = {} AND i_u_id = {}", TABLE_ITEM, item_id, seller_id);
  client.Query(statement, queryResult, timeout);
  deserialize(ic_id, queryResult);
  ++ic_id;


  //insertItemComment
  statement = fmt::format("INSERT INTO {} (ic_id, ic_i_id, ic_u_id, ic_buyer_id, ic_question, ic_created, ic_updated) "
                        "VALUES ({}, {}, {}, {}, {}, {}, {})", TABLE_ITEM_COMMENT, ic_id, item_id, seller_id, buyer_id, question, std::time(0), std::time(0));
  client.Write(statement, queryResult, timeout);
  if(queryResult->rows_affected() == 0){
    Debug("Item comment id %d already exists for item %s and seller %s", ic_id, item_id.c_str(), seller_id.c_str());
    return client.Abort(timeout);
  }

   //updateItemComments
  statement = fmt::format("UPDATE {} SET i_num_comments = i_num_comments + 1 WHERE i_id = {} AND i_u_id = {}", TABLE_ITEM, item_id, seller_id);
  client.Write(statement, queryResult, timeout);

  //updateUser
  statement = fmt::format("UPDATE {} SET u_comments = u_comments + 1, u_updated = {} WHERE u_id = {}", TABLE_USER_ACCT, seller_id);
  client.Write(statement, queryResult, timeout);
  
  Debug("COMMIT");
  return client.Commit(timeout);
}

} // namespace auctionmark
