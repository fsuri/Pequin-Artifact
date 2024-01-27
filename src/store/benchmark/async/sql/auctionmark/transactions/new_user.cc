/***********************************************************************
 *
 * Copyright 2023 Florian Suri-Payer <fsp@cs.cornell.edu>
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

#include "store/benchmark/async/sql/auctionmark/new_user.h"

#include <fmt/core.h>

namespace auctionmark {

NewUser::NewUser(uint32_t timeout, uint64_t &u_id, uint64_t u_r_id,
      const std::vector<std::string> &attributes, std::mt19937_64 &gen) :
    AuctionMarkTransaction(timeout), u_id(u_id), u_r_id(u_r_id), attributes(attributes) {
}

NewUser::~NewUser() {
}

transaction_status_t NewUser::Execute(SyncClient &client) {
  std::unique_ptr<const query_result::QueryResult> queryResult;
  std::string statement;

  Debug("NEW USER");
  Debug("User Region ID: %lu", u_r_id);

  client.Begin(timeout);

  statement = "SELECT MAX(u_id) FROM USER;";
  client.Query(statement, queryResult, timeout);
  deserialize(u_id, queryResult);
  u_id += 1;
  Debug("User ID: %lu", u_id);

  std::string statement_values = fmt::format("VALUES ({}, 0, 0, {}, {}, {}, {}, {}, {}, {}, "
      "{}, {}, {});",
      u_id, u_r_id, attributes[0], attributes[1], attributes[2], attributes[3], 
      attributes[4], attributes[5], attributes[6], attributes[7]);
  statement = 
    "INSERT INTO USER "
    "(u_id, u_rating, u_balance, u_created, u_r_id, u_sattr0, "
    "u_sattr1, u_sattr2, u_sattr3, u_sattr4, u_sttar5, u_sattr6, u_sattr 7) " +
    statement_values;
  client.Write(statement, queryResult, timeout);
  assert(queryResult->has_rows_affected());

  Debug("COMMIT");
  return client.Commit(timeout);
}
} // namespace auctionmark
