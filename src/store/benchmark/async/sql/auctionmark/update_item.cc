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

UpdateItem::UpdateItem(uint32_t timeout, uint64_t i_id, uint64_t i_u_id, 
      string description, std::mt19937 &gen) : AuctionMarkTransaction(timeout), i_id(i_id), 
      i_u_id(i_u_id), description(description) {
}

UpdateItem::~UpdateItem(){
};

transaction_status_t UpdateItem::Execute(SyncClient &client) {
  std::unique_ptr<const query_result::QueryResult> queryResult;
  std::string statement;
  std::vector<std::unique_ptr<const query_result::QueryResult>> results;

  Debug("UPDATE ITEM");
  Debug("Item ID: %lu", i_id);
  Debug("User ID: %lu", i_u_id);

  client.Begin(timeout);

  statement = fmt::format("UPDATE ITEM SET i_description = {} WHERE i_id = {} AND i_u_id = {}",
                          description, i_id, i_u_id);
  client.Write(statement, queryResult, timeout);
  assert(queryResult->has_rows_affected());
  
  Debug("COMMIT");
  return client.Commit(timeout);
}

} // namespace auctionmark
