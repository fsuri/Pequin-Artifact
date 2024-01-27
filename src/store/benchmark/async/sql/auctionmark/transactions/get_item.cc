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
#include "store/benchmark/async/sql/auctionmark/get_item.h"
#include <fmt/core.h>

namespace auctionmark {

GetItem::GetItem(uint32_t timeout, uint64_t i_id, std::mt19937_64 &gen) : 
    AuctionMarkTransaction(timeout), i_id(i_id) {
}

GetItem::~GetItem(){
}

transaction_status_t GetItem::Execute(SyncClient &client) {
  std::unique_ptr<const query_result::QueryResult> queryResult;
  std::string statement;
  std::vector<std::unique_ptr<const query_result::QueryResult>> results;

  Debug("GET ITEM");
  Debug("Item ID: %lu", i_id);

  client.Begin(timeout);

  statement = fmt::format("SELECT i_id, i_u_id, i_initial_price, i_current_price FROM "
                          "ITEM WHERE i_id = {} AND i_status = 0;", i_id);
  client.Query(statement, queryResult, timeout);
  uint64_t query_i_id, query_i_u_id;
  queryResult->at(0)->get(0, &query_i_id);
  assert(query_i_id == i_id);
  
  Debug("COMMIT");
  return client.Commit(timeout);
}

} // namespace auctionmark
