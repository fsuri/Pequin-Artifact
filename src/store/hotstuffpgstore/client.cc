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

namespace hotstuffpgstore {

using namespace std;

Client::Client(const transport::Configuration& config, uint64_t id, int nShards, int nGroups,
      const std::vector<int> &closestReplicas,
      Transport *transport, Partitioner *part,
      uint64_t readMessages, uint64_t readQuorumSize, bool signMessages,
      bool validateProofs, KeyManager *keyManager,
      bool order_commit, bool validate_abort,
      TrueTime timeserver, bool deterministic) : config(config), nshards(nShards),
    ngroups(nGroups), transport(transport), part(part), readMessages(readMessages), readQuorumSize(readQuorumSize),
    signMessages(signMessages),
    validateProofs(validateProofs), keyManager(keyManager),
    order_commit(order_commit), validate_abort(validate_abort),
    timeServer(timeserver), deterministic(deterministic) {
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
        signMessages, validateProofs, keyManager, &stats, order_commit, validate_abort,
        deterministic);
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

void Client::Get(const std::string &key, get_callback gcb,
    get_timeout_callback gtcb, uint32_t timeout) {
  transport->Timer(0, [this, key, gcb, gtcb, timeout]() {
    Debug("GET [%s]", key.c_str());

    // Contact the appropriate shard to get the value.
    std::vector<int> txnGroups;
    int i = (*part)(key, nshards, -1, txnGroups) % ngroups;

    // If needed, add this shard to set of participants and send BEGIN.
    if (!IsParticipant(i)) {
      currentTxn.add_participating_shards(i);
    }

    read_callback rcb = [gcb, this](int status, const std::string &key,
        const std::string &val, const Timestamp &ts) {
      if (status == REPLY_OK) {
        ReadMessage *read = currentTxn.add_readset();
        read->set_key(key);
        ts.serialize(read->mutable_readtime());
      }
      gcb(status, key, val, ts);
    };
    read_timeout_callback rtcb = gtcb;

    // Send the GET operation to appropriate shard.
    bclient[i]->Get(key, currentTxn.timestamp(), readMessages, readQuorumSize, rcb,
        rtcb, timeout);
  });
}

void Client::Put(const std::string &key, const std::string &value,
    put_callback pcb, put_timeout_callback ptcb, uint32_t timeout) {
  transport->Timer(0, [this, key, value, pcb, ptcb, timeout]() {
    // Contact the appropriate shard to set the value.
    std::vector<int> txnGroups;
    int i = (*part)(key, nshards, -1, txnGroups) % ngroups;

    // If needed, add this shard to set of participants and send BEGIN.
    if (!IsParticipant(i)) {
      currentTxn.add_participating_shards(i);
    }

    WriteMessage *write = currentTxn.add_writeset();
    write->set_key(key);
    write->set_value(value);
    // Buffering, so no need to wait.
    pcb(REPLY_OK, key, value);
  });
}

void Client::Commit(commit_callback ccb, commit_timeout_callback ctcb, uint32_t timeout) {
  transport->Timer(0, [this, ccb, ctcb, timeout]() {
    apply_callback acb = [ccb, this](int status) {

      if(status == REPLY_OK) {
        ccb(COMMITTED);
      } else {
        ccb(ABORTED_SYSTEM);
      }
    };
    apply_timeout_callback atcb = ctcb;

    bclient[0]->Commit(TransactionDigest(currentTxn),  currentTxn.timestamp(), client_id, client_seq_num, acb, atcb, timeout);
  });
}

void Client::Abort(abort_callback acb, abort_timeout_callback atcb, uint32_t timeout) {
  transport->Timer(0, [this, acb, atcb, timeout]() {
    bclient[0]->Abort(TransactionDigest(currentTxn), client_id, client_seq_num);

    acb();
  });
}


void Client::Query(const std::string &query, query_callback qcb, query_timeout_callback qtcb, uint32_t timeout, bool skip_query_interpretation) {
  transport->Timer(0, [this, query, qcb, qtcb, timeout](){

    std::cerr<< "Shir: performing query transaction 1\n";

    Debug("Shir: step 30");

    Debug("Query called");

    inquiry_callback icb = [qcb, query, this](int status, const std::string& sql_res) {
      // if(status == REPLY_OK) {
      //   // Take Query Result Proto serialization and transform it into a query result here
      // }
      // sql::QueryResultProtoWrapper query_obj(sql_res);
      Debug("Received query response");
      QueryMessage *query_msg = currentTxn.add_queryset();
      query_msg->set_query(query);
      query_result::QueryResult* query_res = new sql::QueryResultProtoWrapper(sql_res);
      qcb(status, query_res);
      Debug("Shir: step 45");

    };
    inquiry_timeout_callback itcb = qtcb;
    Debug("Shir: step 40");

    std::cerr<< "Shir: performing query transaction 2\n";


    bclient[0]->Query(query, currentTxn.timestamp(), client_id, client_seq_num, icb, itcb, timeout);
    Debug("Shir: step 50");

  });
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
