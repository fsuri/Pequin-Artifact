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
#include "store/benchmark/async/sql/auctionmark/new_feedback.h"
#include <fmt/core.h>

namespace auctionmark {

NewFeedback::NewFeedback(uint32_t timeout, AuctionMarkProfile &profile, std::mt19937_64 &gen) : AuctionMarkTransaction(timeout), gen(gen) {
  //TODO: generate params
}

NewFeedback::~NewFeedback(){
}

transaction_status_t NewFeedback::Execute(SyncClient &client) {
  std::unique_ptr<const query_result::QueryResult> queryResult;
  std::string statement;
  std::vector<std::unique_ptr<const query_result::QueryResult>> results;

  Debug("NEW FEEDBACK");

  client.Begin(timeout);

  statement = fmt::format("SELECT uf_i_id, uf_i_u_id, uf_from_id FROM {} WHERE uf_u_id = {} AND uf_i_id = {} AND uf_i_u_id = {} AND uf_from_id = {}", 
                                                                  TABLE_USERACCT_FEEDBACK, user_id, i_id, seller_id, from_id);
  client.Query(statement, queryResult, timeout);
  if(!queryResult->empty()){
    Debug("Trying to add feedback for item %s twice", i_id.c_str());
    return client.Abort(timeout);
  }


  statement = fmt::format("INSERT INTO {} (uf_u_id, uf_i_id, uf_i_u_id, uf_from_id, uf_rating, uf_date, uf_sattr0) "
                          "VALUES({}, {}, {}, {}, {}, {}, {})", TABLE_USERACCT_FEEDBACK, user_id, i_id, seller_id, from_id, rating, std::time(0), comment);
  client.Write(statement, queryResult, timeout);

  Debug("COMMIT NEW_FEEDBACK");
  return client.Commit(timeout);
}

} // namespace auctionmark
