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

#include "store/benchmark/async/auctionmark/transactions/new_user.h"

namespace auctionmark {

NewUser::NewUser(uint32_t timeout, uint64_t u_id, uint64_t u_r_id,
      const vector<string_view>& attributes, std::mt19937 &gen) :
    SyncTransaction(timeout), u_id(u_id), u_r_id(u_r_id), attributes(attributes) {
}

NewUser::~NewUser() {
}

transaction_status_t NewUser::Execute(SyncClient &client) {
  client.Begin(timeout);
  string query_values = std::format("VALUES ({}, 0, 0, {}, {}, {}, {}, {}, {}, {}, {}, {}, {});",
      u_id, u_r_id, attributes[0], attributes[1], attributes[2], attributes[3], attributes[4], attributes[5], attributes[6], attributes[7]);
  string query = 
    "INSERT INTO USER (u_id, u_rating, u_balance, u_created, u_r_id, u_sattr0, u_sattr1, u_sattr2, u_sattr3, u_sattr4, u_sttar5, u_sattr6, u_sattr 7) " +
    query_values;
  string result;
  client.Query(query, &result, timeout);
  return client.Commit(timeout);
}
} // namespace auctionmark
