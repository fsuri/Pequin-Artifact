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
#include "store/benchmark/async/sql/auctionmark/update_item.h"
#include <fmt/core.h>

namespace auctionmark {

UpdateItem::UpdateItem(uint32_t timeout, string description, std::mt19937_64 &gen) : 
  AuctionMarkTransaction(timeout), description(description), gen(gen) {
}

UpdateItem::~UpdateItem(){
};

transaction_status_t UpdateItem::Execute(SyncClient &client) {
  std::unique_ptr<const query_result::QueryResult> queryResult;
  std::string statement;
  std::vector<std::unique_ptr<const query_result::QueryResult>> results;

  Debug("UPDATE ITEM");

  client.Begin(timeout);

  // Choose a random open item
  statement = fmt::format("SELECT COUNT(*) AS cnt FROM ITEM WHERE i_status = 0;");
  client.Query(statement, queryResult, timeout);
  uint64_t items_cnt;
  deserialize(items_cnt, queryResult);

  uint64_t item_id = std::uniform_int_distribution<uint64_t>(0, items_cnt - 1)(gen);
  statement = fmt::format("SELECT i_id, i_u_id FROM ITEM WHERE i_status = 1 LIMIT {}, 1;", item_id);
  client.Query(statement, queryResult, timeout);
  queryResult->at(0)->get(0, &i_id);
  queryResult->at(0)->get(1, &i_u_id);

  Debug("Item ID: %lu", i_id);
  Debug("User ID: %lu", i_u_id);

  statement = fmt::format("UPDATE ITEM SET i_description = {} WHERE i_id = {} AND i_u_id = {}",
                          description, i_id, i_u_id);
  client.Write(statement, queryResult, timeout);
  assert(queryResult->has_rows_affected());
  
  Debug("COMMIT");
  return client.Commit(timeout);
}

} // namespace auctionmark
