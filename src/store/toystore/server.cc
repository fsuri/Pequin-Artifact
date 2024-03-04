// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/toystore/server.cc
 *   Referencing store/weakstore/server.cc
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

#include "store/toystore/server.h"

namespace toystore {

using namespace proto;

Server::Server(const transport::Configuration &configuration, int groupIdx,
               int myIdx, Transport *transport)
    : configuration(configuration), transport(transport) {
  transport->Register(this, configuration, groupIdx, myIdx);
  executor = QueryExecutor();
  store = VersionedKVStore();
  executor.addTable("table_a", "col1");  // for now just assume this
}

Server::~Server() {}

void Server::ReceiveMessage(const TransportAddress &remote, const string &type,
                            const string &data, void *meta_data) {
  QueryMessage query;
  if (type != query.GetTypeName()) {
    Panic("Received unexpected message type in proto: %s", type.c_str());
  }
  query.ParseFromString(data);
  QueryReplyMessage reply;
  reply.set_reqid(query.reqid());

  // do parsing
  auto ctx = pg_query_parse_init();
  auto result = pg_query_parse(query.query().c_str());

  if (result.error) {
    reply.set_status(-1);
    reply.set_result("Error parsing query");
    transport->SendMessage(this, remote, reply);
    return;
  }

  auto tree_json = pg_parse_tree_json(result.tree);
  json query = json::parse(tree_json);

  // print_pg_parse_tree(result.tree);
  if (query[0].contains("SelectStmt")) {
    auto stmt = query[0]["SelectStmt"];
    // ignore any parsing at first and just return a reasonable set of values.
    auto rows = executor.scan(store, "table_a", 0, 100);
    std::string result;
    for (const auto &s : rows) {
      result += s + "\n";
    }
    reply.set_status(0);
    reply.set_result(result);
  } else if (query[0].contains("InsertStmt")) {
    auto stmt = query[0]["InsertStmt"];
    // we assume that they're all for the same table, and the first value is an
    // integer key.
    auto rows = stmt["selectStmt"]["SelectStmt"]["valuesLists"];
    std::vector<QueryExecutor::Write> rows_contents;
    for (auto &row : rows) {
      Write row_tuple = std::make_tuple(
          row[0]["A_Const"]["val"]["Integer"]["ival"], 0, row.dump());
      rows_contents.push_back(row_tuple);
    }
    executor.insert(store, "table_a", rows_contents);
    reply.set_status(0);
    reply.set_result("Inserted");
  } else {
    reply.set_status(-1);
    reply.set_result("Unsupported query type");
  }

  pg_parse_tree_json_free(tree_json);
  pg_query_parse_finish(ctx);
  pg_query_free_parse_result(result);
  transport->SendMessage(this, remote, reply);
  return;
}
}  // namespace toystore
