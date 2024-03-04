// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/weakstore/server.cc:
 *   Weak consistency storage server executable. Mostly dispatch code.
 *
 * Copyright 2015 Irene Zhang <iyzhang@cs.washington.edu>
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
    : configuration(configuration), transport(transport)
{
    transport->Register(this, configuration, groupIdx, myIdx);
}

Server::~Server() { 
}

void
Server::ReceiveMessage(const TransportAddress &remote,
                       const string &type, const string &data,
                       void *meta_data)
{
    // HandleMessage(remote, type, data);
    QueryMessage query;
    if (type != query.GetTypeName()) {
        Panic("Received unexpected message type in OR proto: %s",
              type.c_str());
    }
    query.ParseFromString(data);
    QueryReplyMessage reply;

    // do parsing
    auto ctx = pg_query_parse_init();
    auto result = pg_query_parse(query.query().c_str());
    // auto result = pg_query_parse("SELECT col1, COUNT(*) from tableA where col2 >= 3 limit 20;");

    if (result.error)
    {
        reply.set_status(-1);
        reply.set_result("Error parsing query");
        transport->SendMessage(this, remote, reply);
        return;
    }


    auto tree_json = pg_parse_tree_json(result.tree);
    json query = json::parse(tree_json);

    // print_pg_parse_tree(result.tree);


    pg_query_parse_finish(ctx);
    pg_query_free_parse_result(result);
    transport->SendMessage(this, remote, reply);
    return;
}

// void
// Server::HandleMessage(const TransportAddress &remote,
//                       const string &type, const string &data)
// {
//     GetMessage get;
//     PutMessage put;
    
//     if (type == get.GetTypeName()) {
//         get.ParseFromString(data);
//         HandleGet(remote, get);
//     } else if (type == put.GetTypeName()) {
//         put.ParseFromString(data);
//         HandlePut(remote, put);
//     } else {
//         Panic("Received unexpected message type in OR proto: %s",
//               type.c_str());
//     }
// }

// void
// Server::HandleGet(const TransportAddress &remote,
//                   const GetMessage &msg)
// {
//     int status;
//     string value;
    
//     status = store.Get(msg.clientid(), msg.key(), value);

//     GetReplyMessage reply;
//     reply.set_status(status);
//     reply.set_value(value);
    
//     transport->SendMessage(this, remote, reply);
// }

// void
// Server::HandlePut(const TransportAddress &remote,
//                   const PutMessage &msg)
// {
//     int status = store.Put(msg.clientid(), msg.key(), msg.value());
//     PutReplyMessage reply;
//     reply.set_status(status);
    
//     transport->SendMessage(this, remote, reply);
// }

// void
// Server::Load(const string &key, const string &value, Timestamp timestamp)
// {
//     store.Load(key, value);
// }

} // namespace toystore
