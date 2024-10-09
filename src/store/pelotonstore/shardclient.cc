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
#include "store/pelotonstore/shardclient.h"
#include "store/pelotonstore/pbft_batched_sigs.h"
#include "store/pelotonstore/common.h"

namespace pelotonstore {

static bool SEND_ONLY_TO_LEADER = false;
static bool ONLY_WAIT_FOR_LEADER = true;

ShardClient::ShardClient(const transport::Configuration& config, Transport *transport,
    uint64_t client_id, uint64_t group_idx, const std::vector<int> &closestReplicas_,
    bool signMessages, bool validateProofs, KeyManager *keyManager, Stats* stats, 
    bool fake_SMR, uint64_t SMR_mode, const std::string& PG_BFTSMART_config_path) :
    config(config), transport(transport), group_idx(group_idx), signMessages(signMessages), validateProofs(validateProofs),
    keyManager(keyManager), stats(stats), reqId(0UL), client_id(client_id),
    fake_SMR(fake_SMR), SMR_mode(SMR_mode)  {

  transport->Register(this, config, -1, -1);
  
  //NOTE: This is useless for HS-based stores since HS runs with stable leader...
  if (closestReplicas_.size() == 0) {
    for  (int i = 0; i < config.n; ++i) {
      closestReplicas.push_back((i + client_id) % config.n);
      // Debug("i: %d; client_id: %d", i, client_id);
      // Debug("Calculations: %d", (i + client_id) % config->n);
    }
  } else {
    closestReplicas = closestReplicas_;
  }

  Notice("SMR_mode: %d. SignMessages: %d", SMR_mode, signMessages);
  if(SMR_mode > 0) UW_ASSERT(signMessages); //Must sign messages with SMR mode: Otherwise fakeSMR is bugged in HandleSQL_RPC reply

  if(SMR_mode == 2){
    Debug("created bftsmart agent in shard client!");
    bftsmartagent = new BftSmartAgent(true, this, 1000 + client_id, group_idx, PG_BFTSMART_config_path);

     // notify the servers about my reply address (Note: This is necessary because the replica doesn't receive a ReturnAddress when calling ReceiveFromBFTSmart)
    Debug("Sending connect messages! with client id %d, config n: %d", client_id, config.n);
    proto::Connect connect;
    connect.set_client_id(client_id);
    transport->SendMessageToGroup(this, group_idx, connect);
    Debug("Finished sending connect messages!");
  }

}

ShardClient::~ShardClient() {
   if(SMR_mode == 2){
    Debug("delete bftsmart agent in shard client!");
    delete bftsmartagent;
  }
}

// ================================
// ==== BFT_SMART INTERFACE ====
// ================================

void ShardClient::SendMessageToGroup_viaBFTSMART(proto::Request& msg, int group_idx){
  // Set my address in the request
  // const UDPTransportAddress& addr = dynamic_cast<const UDPTransportAddress&>(*myAddress);
  // const TCPTransportAddress& addr = dynamic_cast<const TCPTransportAddress&>(*myAddress);
  // msg.mutable_client_address()->set_sin_addr(addr.addr.sin_addr.s_addr);
  // msg.mutable_client_address()->set_sin_port(addr.addr.sin_port);
  // msg.mutable_client_address()->set_sin_family(addr.addr.sin_family);
  // Debug("client addr: %d %d %d", addr.addr.sin_port, addr.addr.sin_addr.s_addr, addr.addr.sin_family);
  msg.set_client_id(client_id);
  Debug("sending to group with client id %d", client_id);

  // Serialize message
  string data;
  UW_ASSERT(msg.SerializeToString(&data));
  string type = msg.GetTypeName();
  size_t typeLen = type.length();
  size_t dataLen = data.length();
  size_t totalLen = (typeLen + sizeof(typeLen) +
                      dataLen + sizeof(dataLen) +
                      sizeof(totalLen) +
                      sizeof(uint32_t));

  Debug("Message is %lu total bytes", totalLen);

  char buf[totalLen];
  char *ptr = buf;

  *((uint32_t *) ptr) = MAGIC;
  ptr += sizeof(uint32_t);
  UW_ASSERT((size_t)(ptr-buf) < totalLen);

  *((size_t *) ptr) = totalLen;
  ptr += sizeof(size_t);
  UW_ASSERT((size_t)(ptr-buf) < totalLen);

  *((size_t *) ptr) = typeLen;
  ptr += sizeof(size_t);
  UW_ASSERT((size_t)(ptr-buf) < totalLen);

  UW_ASSERT((size_t)(ptr+typeLen-buf) < totalLen);
  memcpy(ptr, type.c_str(), typeLen);
  ptr += typeLen;
  *((size_t *) ptr) = dataLen;
  ptr += sizeof(size_t);

  UW_ASSERT((size_t)(ptr-buf) < totalLen);
  UW_ASSERT((size_t)(ptr+dataLen-buf) == totalLen);
  memcpy(ptr, data.c_str(), dataLen);
  ptr += dataLen;
  Debug("sending the buffer to the group!");
  this->bftsmartagent->send_to_group(this, group_idx, buf, totalLen);
  Debug("finished sending the buffer to the group!");
}



// ================================
// ==== SHARD CLIENT INTERFACE ====
// ================================

// Currently assumes no duplicates, can add de-duping code later if needed
void ShardClient::Query(const std::string &query, uint64_t client_id, uint64_t client_seq_num, 
      sql_rpc_callback srcb, sql_rpc_timeout_callback srtcb,  uint32_t timeout) {


  // struct timespec ts_start;
  // clock_gettime(CLOCK_MONOTONIC, &ts_start);
  // auto exec_start_us = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;
  

  reqId++;
  Debug("Query id: %lu. Tx_id: %lu", reqId, client_seq_num);

  //Create SQL RPC request (this is what server consumes)
  proto::SQL_RPC sql_rpc;
  sql_rpc.set_req_id(reqId);
  sql_rpc.set_query(query);
  sql_rpc.set_client_id(client_id);
  sql_rpc.set_txn_seq_num(client_seq_num);

  //Register Reply Handler
  PendingSQL_RPC &psr = pendingSQL_RPCs[reqId];
  psr.srcb = std::move(srcb);
  psr.timeout = new Timeout(transport, timeout, [this, query_req_id = reqId, srtcb]() {
          auto itr = pendingSQL_RPCs.find(query_req_id);
          if(itr == pendingSQL_RPCs.end()) return;
          Warning("Query timeout was triggered. Received %d / %d replies", itr->second.numReceivedReplies, config.f + 1);
          stats->Increment("q_tout", 1);
      });
  psr.timeout->Start();

  //TEST
  // Debug("Sending Query. TxnSeq: %lu reqID: %lu", client_seq_num, reqId);
  // transport->SendMessageToReplica(this, 0, sql_rpc);
  // return;

  // // clock_gettime(CLOCK_MONOTONIC, &ts_start);
  // // auto exec_end_us = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;
  // // Notice("Shardclient outbound latency: %lu us", exec_end_us - exec_start_us);
  // return;

  //Wrap it in generic Request (this goes into Hotstuff)
  proto::Request request;
  request.set_digest(crypto::Hash(sql_rpc.SerializeAsString()));
  request.mutable_packed_msg()->set_msg(sql_rpc.SerializeAsString());
  request.mutable_packed_msg()->set_type(sql_rpc.GetTypeName());

  Debug("Sending Query. TxnSeq: %lu reqID: %lu", client_seq_num, reqId);

  

  if(SMR_mode == 0 || SEND_ONLY_TO_LEADER){
    transport->SendMessageToReplica(this, 0, request);
  }
  else{
    if(SMR_mode == 2){
      SendMessageToGroup_viaBFTSMART(request, group_idx);
    }
    else{
      transport->SendMessageToGroup(this, group_idx, request);
    }
  }
}

void ShardClient::Commit(uint64_t client_id, uint64_t client_seq_num, 
  try_commit_callback tccb, try_commit_timeout_callback tctcb, uint32_t timeout) {

  reqId++;
  Debug("Commit id: %lu", reqId);

  //Create TryCommit request (this is what server consumes)
  proto::TryCommit try_commit;
  try_commit.set_req_id(reqId);
  try_commit.set_client_id(client_id);
  try_commit.set_txn_seq_num(client_seq_num);
  
  //Register Reply Handler
  PendingTryCommit &ptc = pendingTryCommits[reqId];
  ptc.tccb = std::move(tccb);
  ptc.timeout = new Timeout(transport, timeout, [this, commit_req_id = reqId, tctcb](){
        auto itr = pendingTryCommits.find(commit_req_id);
        if(itr == pendingTryCommits.end()) return;
        Warning("Commit timeout was triggered. Received %d / %d replies", itr->second.numReceivedReplies, config.f + 1);
        stats->Increment("c_tout", 1);
      });
  ptc.timeout->Start();

  // //TEST
  // Debug("Sending TryCommit. TxId: %lu reqID: %lu", client_seq_num, reqId);
  // transport->SendMessageToReplica(this, 0, try_commit);
  // return;

  //Wrap it in generic Request (this goes into Hotstuff)
  proto::Request request;
  request.set_digest(crypto::Hash(try_commit.SerializeAsString()));
  request.mutable_packed_msg()->set_msg(try_commit.SerializeAsString());
  request.mutable_packed_msg()->set_type(try_commit.GetTypeName());
  

  Debug("Sending TryCommit. TxId: %lu reqID: %lu", client_seq_num, reqId);

  if(SMR_mode == 0 || SEND_ONLY_TO_LEADER){
    transport->SendMessageToReplica(this, 0, request);
  }
  else{
    if(SMR_mode == 2){
      SendMessageToGroup_viaBFTSMART(request, group_idx);
    }
    else{
      transport->SendMessageToGroup(this, group_idx, request);
    }
  }
}

void ShardClient::Abort(uint64_t client_id, uint64_t client_seq_num) {

  Debug("Abort Transaction");

  //Cancel all Reply Handlers for ongoing concurrent requests.
  //FIXME: THIS IS UNSAFE: if we are using the parallel client interface, then there might be concurrent pendingRPCs. 
  //Even though one RPC may have aborted, we need to wait for the other ones to complete in order to fulfill all promises at SyncClient and return to application. Otherwise we will be stuck!
  //pendingSQL_RPCs.clear();
  // pendingTryCommits.clear();

  //Create UerAbort request (this is what server consumes)
  proto::UserAbort user_abort;
  user_abort.set_client_id(client_id);
  user_abort.set_txn_seq_num(client_seq_num);

    // //TEST
  // transport->SendMessageToReplica(this, 0, user_abort);
  // return;


   //Wrap it in generic Request (this goes into Hotstuff)
  proto::Request request;
  request.set_digest(crypto::Hash(user_abort.SerializeAsString()));
  request.mutable_packed_msg()->set_msg(user_abort.SerializeAsString());
  request.mutable_packed_msg()->set_type(user_abort.GetTypeName());

  Debug("Sending Abort TxnSeq: %lu ", client_seq_num);
  if(SMR_mode == 0 || SEND_ONLY_TO_LEADER){
    transport->SendMessageToReplica(this, 0, request);
  }
  else{
    if(SMR_mode == 2){
      SendMessageToGroup_viaBFTSMART(request, group_idx);
    }
    else{
      transport->SendMessageToGroup(this, group_idx, request);
    }
  }
}


// ================================
// ======= MESSAGE HANDLERS =======
// ================================

int ShardClient::ValidateAndExtractData(const std::string &t, const std::string &d, std::string &type, std::string &data){
  //If message is signed
  if (t == signedMessage.GetTypeName()) {
    if (!signedMessage.ParseFromString(d)) {
      Panic("Failed to Parse Signed Message");
      return -1;
    }

    //extract the packed Message
    proto::PackedMessage pmsg;
    pmsg.ParseFromString(signedMessage.packed_msg());
    
    //Verify that message type is valid -- otherwise no need to verify sig
    if (!(pmsg.type() == sql_rpcReply.GetTypeName() || pmsg.type() == tryCommitReply.GetTypeName())){
      Panic("The only replies we should receive are of type SQL_REPLY or TryCommit_REPLY");
      return -1;
    }

    //Validate signature
    crypto::PubKey* replicaPublicKey = keyManager->GetPublicKey(signedMessage.replica_id());
    if (!pelotonstore::verifyBatchedSignature(&signedMessage.signature(), &signedMessage.packed_msg(), replicaPublicKey)) {
        Panic("Signature from replica %d was invalid", signedMessage.replica_id());
        return -1;
    }
    Debug("signature was valid");
    data = pmsg.msg();
    type = pmsg.type();
  
    return signedMessage.replica_id();
  } 
  else { //Message is not signed
    //if(signMessages) Panic("All replies are supposed to be signed");
   
    type = t;
    data = d;

    return -1; //no replica id needed
  }
}

static uint64_t start_us;
static uint64_t end_us;
void ShardClient::ReceiveMessage(const TransportAddress &remote, const std::string &t, const std::string &d, void *meta_data) {

  // struct timespec ts_start;
  // clock_gettime(CLOCK_MONOTONIC, &ts_start);
  // start_us = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;

  Debug("handling message of type %s", t.c_str());
  std::string type;
  std::string data;

  int replica_id = ValidateAndExtractData(t, d, type, data);

  if (type == sql_rpcReply.GetTypeName()) {
    sql_rpcReply.ParseFromString(data);
    HandleSQL_RPCReply(sql_rpcReply, replica_id);

  } else if (type == tryCommitReply.GetTypeName()) {
    tryCommitReply.ParseFromString(data);
    HandleTryCommitReply(tryCommitReply, replica_id);
  }
  else{
    Panic("The only replies we should receive are of type SQL_REPLY or TryCommit_REPLY");
  }
}


void ShardClient::HandleSQL_RPCReply(const proto::SQL_RPCReply& reply, int replica_id) {
  Debug("Handling a sql_rpc reply");

  const uint64_t &req_id = reply.req_id();
  Debug("sql_rpc reply for req id: %lu. Status: %d", req_id, reply.status());

  auto itr = pendingSQL_RPCs.find(req_id);
  if(itr == pendingSQL_RPCs.end()){
    Debug("req_id: %lu is no longer active", req_id);
    return;
  }

  //if(req_id % 1000 == 1) Notice("Finished Postgres: %lu", req_id);

  PendingSQL_RPC &pendingSQL_RPC = itr->second;

  if(replica_id < 0){ //if not signing messages, simply add a new reply
    replica_id = pendingSQL_RPC.numReceivedReplies;   //FIXME: This is bugged with fakeSMR: we might not use leader vote
  }
 
  if (replica_id / config.n != group_idx) {Panic("Received reply from replica_id that is not in group");}
  pendingSQL_RPC.receivedReplies[reply.sql_res()].insert(replica_id);
  
  pendingSQL_RPC.numReceivedReplies++; //TODO: Should check that we are not accepting multiple replies from one replica
  Debug("pendingSQL with req_id: %lu now has  %lu replies.", req_id, pendingSQL_RPC.numReceivedReplies);


  if(fake_SMR){ //Check if we received leader reply and use the status and result from this reply.
    if(replica_id == 0){
      pendingSQL_RPC.hasLeaderReply=true;
      pendingSQL_RPC.status=reply.status();
      pendingSQL_RPC.leaderReply = reply.sql_res(); // this might be empty if status is failed

      if(ONLY_WAIT_FOR_LEADER){
        SQL_RPCReplyHelper(pendingSQL_RPC, pendingSQL_RPC.leaderReply, req_id, pendingSQL_RPC.status);
        return;
      }
    }
   
    //Wait for at least f+1 total replies
    if(pendingSQL_RPC.numReceivedReplies >= (uint64_t) config.f + 1 && pendingSQL_RPC.hasLeaderReply) {
      SQL_RPCReplyHelper(pendingSQL_RPC, pendingSQL_RPC.leaderReply, req_id, pendingSQL_RPC.status);
    }

  }
  else{
    Panic("Deprecated. Only fake_SMR mode supported right now.");
    if(pendingSQL_RPC.receivedReplies[reply.sql_res()].size() == (uint64_t) config.f + 1) {
        SQL_RPCReplyHelper(pendingSQL_RPC, reply.sql_res(), req_id, pendingSQL_RPC.status);
    }
  }
}

//Note: This must only be called once per req_id.
void ShardClient::SQL_RPCReplyHelper(PendingSQL_RPC &pendingSQL_RPC, const std::string sql_rpcReply, uint64_t req_id, uint64_t status) {
  if(pendingSQL_RPC.timeout != nullptr) {
    pendingSQL_RPC.timeout->Stop();
  }

  sql_rpc_callback srcb = pendingSQL_RPC.srcb;
  pendingSQL_RPCs.erase(req_id);

  // struct timespec ts_start;
  // clock_gettime(CLOCK_MONOTONIC, &ts_start);
  // end_us = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;
  // Notice("Shardclient inbound latency: %lu us", end_us - start_us);
  srcb(status, sql_rpcReply);
}

//Note: This must only be called once per req_id.
void ShardClient::TryCommitReplyHelper(PendingTryCommit &pendingTryCommit, uint64_t req_id, uint64_t status) {
  if(pendingTryCommit.timeout != nullptr) {
    pendingTryCommit.timeout->Stop();
  }

  auto tccb = pendingTryCommit.tccb;
  pendingTryCommits.erase(req_id);
  tccb(status);
}

//TODO: Avoid duplicating code with SQL_RPC reply 
void ShardClient::HandleTryCommitReply(const proto::TryCommitReply& reply, int replica_id) {
  

  const uint64_t &req_id = reply.req_id();
  Debug("Receive commit reply from replica %d for req id: %lu", replica_id, req_id);

  auto itr = pendingTryCommits.find(req_id);
  if(itr == pendingTryCommits.end()){
    Debug("req_id: %lu is no longer active", req_id);
    return;
  }


  PendingTryCommit &pendingTryCommit = itr->second;




  if(replica_id < 0){
    replica_id = pendingTryCommit.numReceivedReplies;
  }

  if (replica_id / config.n != group_idx) {Panic("Received reply from replica_id that is not in group");}
 
  if (replica_id / config.n != group_idx) {Panic("Received reply from replica_id that is not in group");}
  pendingTryCommit.receivedReplies[reply.status()].insert(replica_id);
  
  pendingTryCommit.numReceivedReplies++; //TODO: Should check that we are not accepting multiple replies from one replica

  if(fake_SMR){ //Check if we received leader reply and use the status and result from this reply.
    if(replica_id == 0){
      pendingTryCommit.hasLeaderReply=true;
      pendingTryCommit.status=reply.status();

      if(ONLY_WAIT_FOR_LEADER){
        TryCommitReplyHelper(pendingTryCommit, req_id, pendingTryCommit.status);
        return;
      }
    }
   
    //Wait for at least f+1 total replies
    if(pendingTryCommit.numReceivedReplies >= (uint64_t) config.f + 1 && pendingTryCommit.hasLeaderReply) {
       TryCommitReplyHelper(pendingTryCommit, req_id, pendingTryCommit.status);
    }

  }
  else{
    Panic("Deprecated. Only fake_SMR mode supported right now.");
    if(pendingTryCommit.receivedReplies[reply.status()].size() == (uint64_t) config.f + 1) {
         TryCommitReplyHelper(pendingTryCommit, req_id, reply.status());
    }
  }
}

}
