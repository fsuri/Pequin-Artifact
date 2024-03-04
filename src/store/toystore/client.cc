// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/toystore/client.cc:
 *   Client to toystore database.
 *
 * Copyright 2024 Gaurav Bhatnagar <gbhatnagar@berkeley.edu>
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

#include "store/toystore/client.h"

namespace toystore {

using namespace proto;

Client::Client(transport::Configuration config, uint64_t id, int groupIdx,
               Transport *transport)
    : configuration(config),
      transport(transport),
      groupIdx(groupIdx),
      client_id(id) {
  //   Debug("Initializing PostgreSQL client with id [%lu]", client_id);
  //   connection = tao::pq::connection::create(connection_str);
  //   txn_id = 0;
  //   Debug("PostgreSQL client [%lu] created!", client_id);
  transport->Register(this, configuration, groupIdx, client_id);
  Debug("Toystore client created!");
}

Client::~Client() {}

// Get the value corresponding to key.
inline void Client::Query(const std::string &query_statement,
                          query_callback qcb, query_timeout_callback qtcb,
                          uint32_t timeout, bool skip_query_interpretation) {
  // try {
  //   auto result = transaction->execute(query_statement);
  //   auto wrapped_result = new taopq_wrapper::TaoPQQueryResultWrapper(
  //       std::make_unique<tao::pq::result>(result));
  //   qcb(0, wrapped_result);
  // } catch (...) {
  //   qtcb(1);
  // }
  QueryMessage query;
  query.set_clientid(client_id);
  auto reqid = 100 * client_id + (curr_query_num++);
  pending_queries[reqid] = qcb;
  query.set_reqid(reqid);
  query.set_query(query_statement);
  transport->SendMessageToGroup(this, groupIdx, query);
}

void HandleQueryResult(const QueryReplyMessage &reply) {
  auto qcb = pending_queries[reply.reqid()];
  qcb(reply.status(), reply.result());
}

// Execute the write operation and return the result.
inline void Client::Write(std::string &write_statement, write_callback wcb,
                          write_timeout_callback wtcb, uint32_t timeout) {
  Client::Query(write_statement, wcb, wtcb, timeout, false);
}
}  // namespace toystore
