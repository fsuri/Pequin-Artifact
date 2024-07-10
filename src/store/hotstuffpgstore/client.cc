/***********************************************************************
 *
 * Copyright 2021 Florian Suri-Payer <fsp@cs.cornell.edu>
 *                Yunhao Zhang <yz2327@cornell.edu>
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
#include "store/hotstuffpgstore/client.h"

#include "store/hotstuffpgstore/common.h"
#include "lib/cereal/archives/binary.hpp"
#include "lib/cereal/types/string.hpp"

namespace hotstuffpgstore {

using namespace std;

Client::Client(const transport::Configuration& config, uint64_t id, int nShards, int nGroups,
      const std::vector<int> &closestReplicas,
      Transport *transport, Partitioner *part,
      uint64_t readMessages, uint64_t readQuorumSize, bool signMessages,
      bool validateProofs, KeyManager *keyManager,
      TrueTime timeserver, bool async_server) : config(config), nshards(nShards),
    ngroups(nGroups), transport(transport), part(part), readMessages(readMessages), readQuorumSize(readQuorumSize),
    signMessages(signMessages),
    validateProofs(validateProofs), keyManager(keyManager),
    timeServer(timeserver), async_server(async_server) {
  // just an invariant for now for everything to work ok
  assert(nGroups == nShards);

  client_id = id;
  // generate a random client uuid
  // client_id = 0;
  // while (client_id == 0) {
  //   random_device rd;
  //   mt19937_64 gen(rd());
  //   uniform_int_distribution<uint64_t> dis;
  //   client_id = dis(gen);
  // }
  client_seq_num = 0;

  bclient.reserve(ngroups);

  Debug("Initializing HotStuff Postgres client with id [%lu] %lu", client_id, ngroups);

  /* Start a client for each shard. */
  for (uint64_t i = 0; i < ngroups; i++) {
    bclient[i] = new ShardClient(config, transport, client_id, i, closestReplicas,
        signMessages, validateProofs, keyManager, &stats,
        async_server);
  }

  Debug("HotStuff Postgres client [%lu] created! %lu %lu", client_id, ngroups,
      bclient.size());
}

Client::~Client()
{
    for (auto b : bclient) {
        delete b;
    }
}

/* Begins a transaction. All subsequent operations before a commit() or
 * abort() are part of this transaction.
 */
void Client::Begin(begin_callback bcb, begin_timeout_callback btcb, uint32_t timeout, bool retry) {
  transport->Timer(0, [this, bcb, btcb, timeout]() {
    // Debug("Shir: step 1");
    Debug("BEGIN tx");

    client_seq_num++;
    currentTxn = proto::Transaction();
    // Optimistically choose a read timestamp for all reads in this transaction
    currentTxn.mutable_timestamp()->set_timestamp(timeServer.GetTime());
    currentTxn.mutable_timestamp()->set_id(client_id);
    bcb(client_seq_num);
    // Debug("Shir: step 2");
  });
}

// TODO: consider invoke SQLRequest with the given parameters and a default db
void Client::Get(const std::string &key, get_callback gcb, get_timeout_callback gtcb, uint32_t timeout) {
  Panic("Client GET is not supported.");
}

// TODO: consider invoke SQLRequest with the given parameters and a default db
void Client::Put(const std::string &key, const std::string &value, put_callback pcb, put_timeout_callback ptcb, uint32_t timeout) {
  Panic("Client PUT is not supported.");
}

void Client::Commit(commit_callback ccb, commit_timeout_callback ctcb, uint32_t timeout) {
  transport->Timer(0, [this, ccb, ctcb, timeout]() {
    try_commit_callback tccb = [ccb, this](int status) {
      Debug("Shir: executing try_commit callback");

      if(status == REPLY_OK) {
        Debug("Shir: executing try_commit callback 111");

        ccb(COMMITTED);
      } else {
        ccb(ABORTED_SYSTEM);
      }
    };
    try_commit_timeout_callback tctcb = ctcb;
    Debug("Shir: Client trying to commit txn");
    bclient[0]->Commit(TransactionDigest(currentTxn),  currentTxn.timestamp(), client_id, client_seq_num, tccb, tctcb, timeout);
  });
}

void Client::Abort(abort_callback acb, abort_timeout_callback atcb, uint32_t timeout) {
  transport->Timer(0, [this, acb, atcb, timeout]() {
    bclient[0]->Abort(TransactionDigest(currentTxn), client_id, client_seq_num);

    acb();
  });
}

void Client::SQLRequest(std::string &statement, sql_callback scb, sql_timeout_callback stcb, uint32_t timeout){
  transport->Timer(0, [this, statement, scb, stcb, timeout](){

    Debug("Query called");
    // std::cerr << "Shir:  issue SQLRequest from client:     "<<statement << std::endl;

    sql_rpc_callback srcb = [scb, statement, this](int status, const std::string& sql_res) {
      Debug("Received query response");
      QueryMessage *query_msg = currentTxn.add_queryset();
      query_msg->set_query(statement);
      query_result::QueryResult* query_res;
      if(status == REPLY_OK) {
        Debug("Shir: executing SQL_rpc callback with successful result");
        query_res = new sql::QueryResultProtoWrapper(sql_res);
      } else {
        Debug("Shir: executing SQL_rpc callback after aborting");
        bclient[0]->Abort(TransactionDigest(currentTxn),client_id,client_seq_num);
        // bclient[0]->Abort(TransactionDigest(currentTxn),client_id,client_seq_num);

        query_res = new sql::QueryResultProtoWrapper();
      }

      // std::cerr << "Shir: For the following statement:     "<<statement << std::endl;
      // std::cerr << "Shir: rows affected is:     "<<query_res->rows_affected() << std::endl;
      // // std::cerr << "Shir: WAS IT ABORTED?:     "<<query_res->rows_affected() << std::endl;
      // std::cerr << "Shir: status is:     "<<status << std::endl;
    
      scb(status, query_res);

      // std::cerr << "Shir: does it get here?" << std::endl;


    };
    sql_rpc_timeout_callback srtcb = stcb;


    bclient[0]->Query(statement, currentTxn.timestamp(), client_id, client_seq_num, srcb, srtcb, timeout);

  });
}


void Client::Query(const std::string &query, query_callback qcb, query_timeout_callback qtcb, uint32_t timeout,bool cache_result, bool skip_query_interpretation) {
    Debug("Processing Query Statement: %s", query.c_str());
    std::string non_const_query = query;
    this->SQLRequest(non_const_query,qcb,qtcb,timeout);
}


void Client::Write(std::string &write_statement, write_callback wcb, write_timeout_callback wtcb, uint32_t timeout, bool blind_write){
    Debug("Processing Write Statement: %s", write_statement.c_str());
    this->SQLRequest(write_statement,wcb,wtcb,timeout);
}

bool Client::IsParticipant(int g) {
  for (const auto &participant : currentTxn.participating_shards()) {
    if (participant == (uint64_t) g) {
      return true;
    }
  }
  return false;
}


}
