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
#include "store/hotstuffpgstore/shardclient.h"
#include "store/hotstuffpgstore/pbft_batched_sigs.h"
#include "store/hotstuffpgstore/common.h"

namespace hotstuffpgstore {

ShardClient::ShardClient(const transport::Configuration& config, Transport *transport,
    uint64_t client_id, uint64_t group_idx, const std::vector<int> &closestReplicas_,
    bool signMessages, bool validateProofs,
    KeyManager *keyManager, Stats* stats, bool async_server) :
    config(config), transport(transport),
    group_idx(group_idx),
    signMessages(signMessages), validateProofs(validateProofs),
    keyManager(keyManager), stats(stats),
    async_server(async_server) {
  transport->Register(this, config, -1, -1);
  SQL_RPCReq = 0;
  tryCommitReq = 0;

  if (closestReplicas_.size() == 0) {
    for  (int i = 0; i < config.n; ++i) {
      closestReplicas.push_back((i + client_id) % config.n);
      // Debug("i: %d; client_id: %d", i, client_id);
      // Debug("Calculations: %d", (i + client_id) % config->n);
    }
  } else {
    closestReplicas = closestReplicas_;
  }

}

ShardClient::~ShardClient() {}

void ShardClient::ReceiveMessage(const TransportAddress &remote, const std::string &t, const std::string &d, void *meta_data) {

  Debug("handling message of type %s", t.c_str());
  proto::SignedMessage signedMessage;
  std::string type;
  std::string data;
  proto::SQL_RPCReply sql_rpcReply;
  proto::TryCommitReply tryCommitReply;

  bool recvSignedMessage = false;
  if (t == signedMessage.GetTypeName()) {
    if (!signedMessage.ParseFromString(d)) {
      return;
    }

    proto::PackedMessage pmsg;
    pmsg.ParseFromString(signedMessage.packed_msg());
    // std::cout << "Inner type: " << pmsg.type() << std::endl;
    if (pmsg.type() == sql_rpcReply.GetTypeName() || pmsg.type() == tryCommitReply.GetTypeName() ) {
      crypto::PubKey* replicaPublicKey = keyManager->GetPublicKey(signedMessage.replica_id());
      if (!hotstuffpgBatchedSigs::verifyBatchedSignature(signedMessage.mutable_signature(), signedMessage.mutable_packed_msg(),
            replicaPublicKey)) {
             Debug("dec signature was invalid");
             return;
            }
      data = pmsg.msg();
      type = pmsg.type();
    } else if(!ValidateSignedMessage(signedMessage, keyManager, data, type)) {
       Debug("signature was invalid");
       return;
    }

    recvSignedMessage = true;
    Debug("signature was valid");
  } else {
    type = t;
    data = d;
  }

  if (type == sql_rpcReply.GetTypeName()) {
    sql_rpcReply.ParseFromString(data);
    if(signMessages && !recvSignedMessage) {
      return;
    }
    HandleSQL_RPCReply(sql_rpcReply, signedMessage);

  } else if (type == tryCommitReply.GetTypeName()) {
    tryCommitReply.ParseFromString(data);
    if(signMessages && !recvSignedMessage) {
      return;
    }
    HandleTryCommitReply(tryCommitReply, signedMessage);
  }
}

// ================================
// ======= MESSAGE HANDLERS =======
// ================================

void ShardClient::HandleSQL_RPCReply(const proto::SQL_RPCReply& reply, const proto::SignedMessage& signedMsg) {
  Debug("Handling a sql_rpc reply");

  uint64_t reqId = reply.req_id();
  Debug("sql_rpc req id: %lu", reqId);
  Debug("Shir: sql_rpc req status: %lu", reply.status());

// OLD CODE FLOW:
//  if(signMessages) { 
//       if(reply.status() == REPLY_OK) {
//         if(pendingSQL_RPC->status == REPLY_FAIL) {
//              pendingSQL_RPC->status = REPLY_OK;

  if(pendingSQL_RPCs.find(reqId) != pendingSQL_RPCs.end()) {
    PendingSQL_RPC* pendingSQL_RPC = &pendingSQL_RPCs[reqId];

    pendingSQL_RPC->numReceivedReplies++;
    // std::cerr <<"Shir: (1) For SQL_rpc req id: "<<reqId<<" There are "<<pendingSQL_RPC->numReceivedReplies<<" replies \n";
    Debug("Shir: got an additional reply. request:  %lu now has  %lu replies.",reqId,pendingSQL_RPC->numReceivedReplies);
    // Debug("Shir: the current reply status is %lu",reply.status());

 
    bool status_changed_to_ok=false;
    if(reply.status() == REPLY_OK && pendingSQL_RPC->status == REPLY_FAIL) {
      status_changed_to_ok=true;
    }

    // Updating a previously failed sql_rpc-- only for deterministic mode
    if(!async_server && status_changed_to_ok) {
        pendingSQL_RPC->status = REPLY_OK;
    }

    if(signMessages) {
      uint64_t replica_id = signedMsg.replica_id();
      if (replica_id / config.n != (uint64_t) group_idx) {
        Debug("sql_rpc Reply: replica not in group");
        return;
      }
      pendingSQL_RPC->receivedReplies[reply.sql_res()].insert(replica_id);
      // The leader has replied      
      if(async_server && replica_id == 0) {
        Debug("Updating leader reply.");
        pendingSQL_RPC->hasLeaderReply=true;
        pendingSQL_RPC->leaderReply = reply.sql_res(); // this might be empty if status is failed
        if (status_changed_to_ok){
          pendingSQL_RPC->status = REPLY_OK;
        }
      }
            // std::cerr <<"Shir: (2) For SQL_rpc req id: "<<reqId<<" There are "<<pendingSQL_RPC->numReceivedReplies<<" replies \n";
    } 
    else {
      pendingSQL_RPC->receivedReplies[reply.sql_res()].insert(pendingSQL_RPC->numReceivedReplies);
    }

    Debug("Shir: 888");
    // std::cerr <<"Shir: Is async?:  "<< async_server <<"\n";
    // std::cerr <<"Shir: Is signed messages?:  "<< signMessages <<"\n";
    // std::cerr <<"Shir: !signMessages:  "<< !signMessages <<"   !async_server   " <<!async_server<<"\n";
    // std::cerr <<"Shir: (3) For SQL_rpc req id: "<<reqId<<" There are "<<pendingSQL_RPC->numReceivedReplies<<" replies \n";

    if(!signMessages || !async_server) { // This is for a fault tolerant system, curently we only look for the leader's opinion (only works in signed system)
      Panic("Deterministic solution is currently not supported because of postgres blocking queries");
      if(pendingSQL_RPC->receivedReplies[reply.sql_res()].size() 
          >= (uint64_t) config.f + 1) {
                SQL_RPCReplyHelper(pendingSQL_RPC, reply.sql_res(), reqId, pendingSQL_RPC->status);
      } else if(pendingSQL_RPC->numReceivedReplies  >= (uint64_t) config.f + 1) {
                SQL_RPCReplyHelper(pendingSQL_RPC, reply.sql_res(), reqId, REPLY_FAIL);
      }
    } else{
      Debug("Shir: 101010"); // not deterministic
      // std::cerr <<"Shir: (4) For SQL_rpc req id: "<<reqId<<" There are "<<pendingSQL_RPC->numReceivedReplies<<" replies \n";

      // std::cerr <<"Shir: will it crash?: "<<pendingSQL_RPC->numReceivedReplies<<" \n";
      // std::cerr <<"Shir: leader reply is :   "<<pendingSQL_RPC->leaderReply<<" \n";
      // std::cerr <<"Shir: has the leader replied :   "<<pendingSQL_RPC->hasLeaderReply<<" \n";
      
      if(pendingSQL_RPC->numReceivedReplies >= (uint64_t) config.f + 1 && pendingSQL_RPC->hasLeaderReply) {
        // std::cerr <<"Shir: (5) For SQL_rpc req id: "<<reqId<<" There are "<<pendingSQL_RPC->numReceivedReplies<<" replies. which is why i'm here \n";
        SQL_RPCReplyHelper(pendingSQL_RPC, pendingSQL_RPC->leaderReply, reqId, pendingSQL_RPC->status);
      }

    }
  }
}

void ShardClient::SQL_RPCReplyHelper(PendingSQL_RPC* pendingSQL_RPC, const std::string sql_rpcReply, 
    uint64_t reqId, uint64_t status) {
  if(pendingSQL_RPC->timeout != nullptr) {
    pendingSQL_RPC->timeout->Stop();
  }
  // Debug("Shir: For redId %d, with status %d and result: %s",reqId,status,sql_rpcReply.c_str());

  sql_rpc_callback srcb = pendingSQL_RPC->srcb;
  pendingSQL_RPCs.erase(reqId);
  Debug("Shir: calling callback");
  srcb(status, sql_rpcReply);
  Debug("Shir: finished callback");
}

void ShardClient::HandleTryCommitReply(const proto::TryCommitReply& reply, const proto::SignedMessage& signedMsg) {
  Debug("Handling a tryCommit reply");

  uint64_t reqId = reply.req_id();
  Debug("tryCommit req id: %lu", reqId);

  if(pendingTryCommits.find(reqId) != pendingTryCommits.end()) {
    PendingTryCommit* pendingTryCommit = &pendingTryCommits[reqId];

    uint64_t replica_id = pendingTryCommit->receivedAcks.size() + pendingTryCommit->receivedFails.size();
    if (signMessages) {
      replica_id = signedMsg.replica_id();
    }
    
    if(reply.status() == REPLY_OK) {
      pendingTryCommit->receivedAcks.insert(replica_id);
    } else {
      pendingTryCommit->receivedFails.insert(replica_id);
      if(async_server && signMessages && replica_id == 0) {
        if(pendingTryCommit->timeout != nullptr) {
          pendingTryCommit->timeout->Stop();
        }
        try_commit_callback tccb = pendingTryCommit->tccb;
        pendingTryCommits.erase(reqId);
                tccb(REPLY_FAIL);
      }
    }
    // Shir: clean code duplications...
    if(!signMessages || !async_server) {
      if(pendingTryCommit->receivedAcks.size() >= (uint64_t) config.f + 1) {
        if(pendingTryCommit->timeout != nullptr) {
          pendingTryCommit->timeout->Stop();
        }
        try_commit_callback tccb = pendingTryCommit->tccb;
        pendingTryCommits.erase(reqId);
        tccb(REPLY_OK);
      } else if(pendingTryCommit->receivedFails.size() >= (uint64_t) config.f + 1) {
        if(pendingTryCommit->timeout != nullptr) {
          pendingTryCommit->timeout->Stop();
        }
        try_commit_callback tccb = pendingTryCommit->tccb;
        pendingTryCommits.erase(reqId);
                tccb(REPLY_FAIL);
      }
    } else {
      if(pendingTryCommit->receivedAcks.size() >= (uint64_t) config.f + 1 && 
      pendingTryCommit->receivedAcks.find(0) != pendingTryCommit->receivedAcks.end()) {
        if(pendingTryCommit->timeout != nullptr) {
          pendingTryCommit->timeout->Stop();
        }
        try_commit_callback tccb = pendingTryCommit->tccb;
        pendingTryCommits.erase(reqId);
        tccb(REPLY_OK);
      }
    }
  }
}


// ================================
// ==== SHARD CLIENT INTERFACE ====
// ================================

// Currently assumes no duplicates, can add de-duping code later if needed
void ShardClient::Query(const std::string &query,  const Timestamp &ts, uint64_t client_id, int client_seq_num, 
      sql_rpc_callback srcb, sql_rpc_timeout_callback srtcb,  uint32_t timeout) {

  proto::SQL_RPC sql_rpc;

  uint64_t reqId = SQL_RPCReq++;
  Debug("Query id: %lu", reqId);

  sql_rpc.set_req_id(reqId);
  sql_rpc.set_query(query);
  sql_rpc.set_client_id(client_id);
  sql_rpc.set_txn_seq_num(client_seq_num);
  ts.serialize(sql_rpc.mutable_timestamp());

  proto::Request request;
  request.set_digest(crypto::Hash(sql_rpc.SerializeAsString()));
  request.mutable_packed_msg()->set_msg(sql_rpc.SerializeAsString());
  request.mutable_packed_msg()->set_type(sql_rpc.GetTypeName());


  Debug("Sending Query id: %lu", reqId);

  transport->SendMessageToGroup(this, group_idx, request);
  // transport->SendMessageToReplica(this,0,request);

  PendingSQL_RPC psr;
  psr.srcb = srcb;
  psr.status = REPLY_FAIL;
    psr.numReceivedReplies = 0;
  psr.leaderReply = "";
  psr.hasLeaderReply=false;
  psr.timeout = new Timeout(transport, timeout, [this, reqId, srtcb]() {
    Debug("Query timeout called (but nothing was done)");
      stats->Increment("q_tout", 1);
      fprintf(stderr,"q_tout recv %lu\n",  (uint64_t) config.f + 1);
  });
  psr.timeout->Start();

  pendingSQL_RPCs[reqId] = psr;

}

void ShardClient::Commit(const std::string& txn_digest, const Timestamp &ts, uint64_t client_id, int client_seq_num, 
  try_commit_callback tccb, try_commit_timeout_callback tctcb, uint32_t timeout) {


  Debug("Shir: shardClient trying to commit the txn");
  proto::TryCommit try_commit;

  uint64_t reqId = tryCommitReq++;
  Debug("Commit id: %lu", reqId);

  try_commit.set_req_id(reqId);
  try_commit.set_client_id(client_id);
  try_commit.set_txn_seq_num(client_seq_num);
  ts.serialize(try_commit.mutable_timestamp());
  Debug("Shir: a");

  proto::Request request;
  request.set_digest(txn_digest);
  Debug("Shir: b");

  request.mutable_packed_msg()->set_msg(try_commit.SerializeAsString());
  request.mutable_packed_msg()->set_type(try_commit.GetTypeName());
  

  Debug("Shir: now going to send a message in order to do that");
  transport->SendMessageToGroup(this, group_idx, request);

  Debug("Shir: trying to send the following message:");
  // std::cerr<< "To group:   "<<group_idx<<"   Send the request:  "<<try_commit.GetTypeName() <<"\n";

  PendingTryCommit ptc;
  ptc.tccb = tccb;
  ptc.timeout = new Timeout(transport, timeout, [this, reqId, tctcb](){
    Debug("Commit timeout called (but nothing was done)");
      stats->Increment("c_tout", 1);
      fprintf(stderr,"c_tout recv %lu\n",  (uint64_t) config.f + 1);
  });
  ptc.timeout->Start();

  pendingTryCommits[reqId] = ptc;
}

void ShardClient::Abort(const std::string& txn_digest,  uint64_t client_id, int client_seq_num) {

  proto::UserAbort user_abort;

  Debug("Abort Triggered");

  user_abort.set_client_id(client_id);
  user_abort.set_txn_seq_num(client_seq_num);

  proto::Request request;
  request.set_digest(txn_digest);
  request.mutable_packed_msg()->set_msg(user_abort.SerializeAsString());
  request.mutable_packed_msg()->set_type(user_abort.GetTypeName());

  transport->SendMessageToGroup(this, group_idx, request);
}

}
