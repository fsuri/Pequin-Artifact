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
#include "store/pg_SMRstore/replica.h"
#include "store/pg_SMRstore/pbft_batched_sigs.h"
#include "store/pg_SMRstore/common.h"

namespace pg_SMRstore {

using namespace std;


Replica::Replica(const transport::Configuration &config, KeyManager *keyManager,App *app, int groupIdx, int idx, bool signMessages, uint64_t maxBatchSize,
  uint64_t batchTimeoutMS, uint64_t EbatchSize, uint64_t EbatchTimeoutMS, bool primaryCoordinator, bool requestTx, int hotstuffpg_cpu, bool local_config, 
  int numShards, Transport *transport, bool fake_SMR, int dummyTO, uint64_t SMR_mode, const std::string& PG_BFTSMART_config_path)
    : config(config), keyManager(keyManager), app(app), groupIdx(groupIdx), idx(idx),
      id(groupIdx * config.n + idx), signMessages(signMessages), maxBatchSize(maxBatchSize), batchTimeoutMS(batchTimeoutMS), EbatchSize(EbatchSize), EbatchTimeoutMS(EbatchTimeoutMS), 
      primaryCoordinator(primaryCoordinator), requestTx(requestTx), numShards(numShards), transport(transport), 
      fake_SMR(fake_SMR), dummyTO(dummyTO), SMR_mode(SMR_mode) {
    
  Notice("SMR_mode: %d", SMR_mode);
  Notice("EbatchSize: %d", EbatchSize);
  UW_ASSERT(SMR_mode < 3);

  if(SMR_mode == 1){
    hotstuffpg_interface = new hotstuffstore::IndicusInterface(groupIdx, idx, hotstuffpg_cpu, local_config);
  }
  else if(SMR_mode == 2){
    Notice("bftsmart config path: %s", PG_BFTSMART_config_path.c_str());
    bftsmartagent = new BftSmartAgent(false, this, idx, groupIdx, PG_BFTSMART_config_path);
  }

  transport->Register(this, config, groupIdx, idx);

  // initial seqnum
  execSeqNum = 0;
  bubbles=0;

  batchTimerRunning = false;

  proposedCounter=0;
  firstReceive=true;

  EbatchTimerRunning = false;
  // for (int i = 0; i < EbatchSize; i++) {
  //   EsignedMessages.push_back(new proto::SignedMessage());
  // }
  for (uint64_t i = 1; i <= EbatchSize; i++) {
   EbStatNames[i] = "ebsize_" + std::to_string(i);
  }

  Notice("Initialized replica at %d %d", groupIdx, idx);
   
  stats = app->mutableStats();
  for (uint64_t i = 1; i <= maxBatchSize; i++) {
   bStatNames[i] = "bsize_" + std::to_string(i);
  }


  // assume these are somehow secretly shared before hand
  for (uint64_t i = 0; i < config.n; i++) {
    if (i > idx) {
      sessionKeys[i] = std::string(8, (char) idx + 0x30) + std::string(8, (char) i + 0x30);
    } else {
      sessionKeys[i] = std::string(8, (char) i + 0x30) + std::string(8, (char) idx + 0x30);
    }
  }
}

Replica::~Replica() {
  if(SMR_mode == 1){
    delete hotstuffpg_interface;
  }
  else if(SMR_mode == 2){
    delete bftsmartagent;
  }
}

void Replica::bubbleCB(uint64_t currProposedCounter){
 
  auto pc =this->proposedCounter; //current counter of received proposals
 
  //If we have not seen a new proposal since the last one -> inject dummy TX to fill HS batches and pipeline
  if (this->proposedCounter == currProposedCounter){ 
    proposeBubble();
  }
 
  //Start Timer again
  transport->TimerMicro(dummyTO, [this,pc](){
    this->bubbleCB(pc); 
  });
  // transport->Timer(dummyTO, [this,pc](){
//     this->bubbleCB(pc); 
//   });
}

void Replica::proposeBubble(){
  // create a dummy digest to propose to Hotstuff. Proposals have to be of length 32 chars, and unique.
  string dummy_digest_init(std::string(32, '0')+std::to_string(bubbles));
  string dummy_digest = dummy_digest_init.substr(dummy_digest_init.length()-32); //take the last 32 chars

  bubbles++;

  // Debug("Bubble %s size is:  %d and capacity is: %d",dummy_digest.c_str(),dummy_digest.length(),dummy_digest.capacity());
  Debug("Proposing Bubble %s ",dummy_digest.c_str());

  auto execb_bubble = [this, dummy_digest](const std::string &digest_paramm, uint32_t seqnumm) {
    auto f = [this, dummy_digest, digest_paramm, seqnumm](){
      Debug("Adding %s to pending executions in seqnum %d",dummy_digest.c_str(),seqnumm);
      const std::pair<std::string, std::string> msg_pair("dummy", "dummy");
      unpacked_requests[dummy_digest] = msg_pair;
      replyAddrs[dummy_digest] = nullptr;
      pendingExecutions[seqnumm] = dummy_digest;
      executeSlots_unpacked();
      return (void*) true;
    };
    transport->DispatchTP_main(f);
  };

  hotstuffpg_interface->propose(dummy_digest, execb_bubble);
  proposedCounter++;
}


void Replica::ReceiveMessage(const TransportAddress &remote, const string &type, const string &data, void *meta_data) {
 
  Debug("Received message of type %s", type.c_str());

  if(type == connect_msg.GetTypeName()){
    connect_msg.ParseFromString(data);
    std::unique_lock lock(client_cache_mutex);
    const uint64_t &client_id = connect_msg.client_id();
    clientCache[client_id] = remote.clone();
    Debug("Registering client ID %d to the Connection cache!", client_id);
    //Handle any buffered requests   //NOTE: Doing so will technically violate the total order from consensus, but we are only simulating a fake SMR anyways.
    if (reqBuffer.find(client_id) != reqBuffer.end()){
      for (proto::Request request: reqBuffer[client_id]){
        Debug("fetching previous buffered requests because we get reads!");
        HandleRequest_noHS(*(clientCache[client_id]), request);
      }
      reqBuffer.erase(client_id);
    }
  }

  else if(type == sql_rpc_template.GetTypeName() || type == try_commit_template.GetTypeName() || type == user_abort_template.GetTypeName()){
    Debug("Handle no Packed");
    // HandleRequest_noPacked(remote, type, data);
    if (SMR_mode == 0){
      HandleRequest_noPacked(remote, type, data);
    }
    else{
      HandleRequest_noPacked_shir(remote, type, data);
    }
  }

  
  else if (type == recvrequest.GetTypeName()) {
    if(SMR_mode == 0){ //TEST_WITHOUT_HOTSTUFF){ //If no SMR enabled, then just execute directly.
      //  recvrequest.ParseFromString(data);
        Debug("Handle no HS");
       HandleRequest_noHS(remote, recvrequest);
      return;
    }
    else if(SMR_mode == 1){ //This path will be invoked for Hotstuff
      // recvrequest.ParseFromString(data);
      auto digest2 = recvrequest.digest();
      Debug("Shir:");
      DebugHash(digest2);

      HandleRequest(remote, recvrequest);
    }
    else if(SMR_mode == 2){ //This path will be invoked if it comes from BFTSmart
      //  recvrequest.ParseFromString(data);
   
      uint64_t client_id = recvrequest.client_id();
      std::unique_lock lock(client_cache_mutex);
      const TransportAddress* client = clientCache[client_id];
      Debug("handling the request here... ");
      if (client == nullptr){
        Debug("Failed to get client ID! %d, putting the request in the buffer...", client_id);
        reqBuffer[client_id].push_back(recvrequest);
      }
      else {
        Debug("handling the request for real!");
        HandleRequest_noHS(*client, recvrequest);
      }
      Debug("finished handling requests");
    }

  } 
  else{
    Panic("Received invalid message type: %s", type.c_str());
  }
}


//TODO: GEt rid off
// void Replica::ReceiveFromBFTSmart(const string &type, const string &data){
//   bool recvSignedMessage = false;

//     // TODO: modify transport address!
//   Notice("message upcalled from BFT smart agent");
//   Debug("Received message of type %s", type.c_str());

//   if (type == recvrequest.GetTypeName()) {

//     // TODO: special processing requests
//     recvrequest.ParseFromString(data);
   
//     uint64_t client_id = recvrequest.client_id();
//     std::unique_lock lock(client_cache_mutex);
//     const TransportAddress* client = clientCache[client_id];
//     Debug("handling the request here... ");
//     if (client == nullptr){
//       Debug("Failed to get client ID! %d, putting the request in the buffer...", client_id);
//       reqBuffer[client_id].push_back(recvrequest);
//     }
//     else {
//       Debug("handling the request for real!");
//       HandleRequest_noHS(*client, recvrequest);
//     }
//     Debug("finished handling requests");
//   }else{
//     Panic("message type not proto::Request, unimplemented");
//   }

// }


static uint64_t req = 0;
void Replica::RW_TEST(){
  auto loops = 0;
  while(loops > 0){
    loops--;
    req++;
    auto key = req % 10;
    std::string statement = "UPDATE t0 SET value = value + 1 WHERE key = " + std::to_string(key) + ";";
    
    proto::SQL_RPC sql_rpc;
    sql_rpc.set_req_id(req);
    sql_rpc.set_query(statement);
    sql_rpc.set_client_id(0);
    sql_rpc.set_txn_seq_num(req);



    app->Execute(sql_rpc.GetTypeName(), sql_rpc.SerializeAsString());
    // app->Execute(sql_rpc.GetTypeName(), "");
      
    proto::TryCommit try_commit;
    try_commit.set_req_id(req);
    try_commit.set_client_id(0);
    try_commit.set_txn_seq_num(req);

  
    app->Execute(try_commit.GetTypeName(), try_commit.SerializeAsString());
    // app->Execute(try_commit.GetTypeName(), "");
  }
}



static bool USE_SYNC_INTERFACE = false; //NOTE: THIS MUST BE FALSE WITH > 1 client
static uint64_t counter = 0;
//Directly call into Server (skip HS)
//Note: BubbleTimer will never be called since we never call HandleRequest
void Replica::HandleRequest_noHS(const TransportAddress &remote, const proto::Request &request){

  // //Reply immediately with a dummy result -- pay deserialization cost, but don't talk to postgres.
  // //1 Sql reply, 1 commit reply.
  // Debug("Use dummy request");
  // counter++;
  // if(counter % 2 == 1){
  //   proto::SQL_RPC sql_rpc;
  //   sql_rpc.ParseFromString(request.packed_msg().msg());
   
  //   proto::SQL_RPCReply reply;
  //   reply.set_req_id(sql_rpc.req_id());
  //   reply.set_status(0);
     

  //   sql::QueryResultProtoBuilder res;
  //   res.set_rows_affected(1);
  //   reply.set_sql_res(res.get_result()->SerializeAsString());
  //   Debug("Send reply");
  //   transport->SendMessage(this, remote, reply);
  //   Debug("Finished reply");
  // }
  // else{
  //   proto::TryCommit try_commit;
  //   try_commit.ParseFromString(request.packed_msg().msg());
   
  //   proto::TryCommitReply reply;
  //   reply.set_req_id(try_commit.req_id());
  //   reply.set_status(0);
  //   Debug("Send reply");
  //   transport->SendMessage(this, remote, reply);
  //   Debug("Finished reply");
  // }

  // return;
  //
  Debug("Received new Request");

  const string &digest = request.digest();
  const proto::PackedMessage &packedMsg = request.packed_msg();
  TransportAddress* clientAddr = remote.clone(); //The return address to reply to


  //TEST: Before every new TX exec a few more.
  //  proto::SQL_RPC sql_rpc;
  // if(packedMsg.type() == sql_rpc.GetTypeName()) RW_TEST();
  //Count total number or ProcessReq invocations remote vs here.
  //If coordination is bottleneck, the number will be way larger. If postgres is bottleneck, they will be close.
  
  auto f = [this, digest, packedMsg, clientAddr](){
    Debug("Executing App interface");
    replyAddrs[digest] = clientAddr; // replyAddress is the address of the client wo sent this request, so we can answer

    auto cb = [this, digest, packedMsg](const std::vector<::google::protobuf::Message*> &replies){
      Debug("Trigger reply callback");
      //TEST: Send back unsigned.
      if(!signMessages){
        UW_ASSERT(replies.size() <= 1);
        for(auto reply: replies){
           if(reply == nullptr){
            Debug("Abort needs no reply");
            continue;
           }
          TransportAddress* clientAddr = replyAddrs[digest];
          transport->SendMessage(this, *clientAddr, *reply);
          delete reply;
        }
      }
      else{
         //Create EBatch
        ProcessReplies(digest, replies);
      }
    };
    if(USE_SYNC_INTERFACE){
      //Use Synchronous interface
      std::vector<::google::protobuf::Message*> replies = app->Execute(packedMsg.type(), packedMsg.msg());
      cb(replies);
    }
    else{
     //Use Asynchronous interface
      app->Execute_Callback(packedMsg.type(), packedMsg.msg(),cb);
    }
  
    return (void*) true;
  };
  Debug("Dispatching to main");
  transport->DispatchTP_main(f);
}

static uint64_t exec_start_us;
static uint64_t exec_end_us;
void Replica::HandleRequest_noPacked(const TransportAddress &remote, const std::string &type, const std::string &data){

 
  TransportAddress* clientAddr = remote.clone(); //The return address to reply to

  //TEST: Before every new TX exec a few more.
  if(type == sql_rpc_template.GetTypeName()) RW_TEST();
  //Count total number or ProcessReq invocations remote vs here.
  //If coordination is bottleneck, the number will be way larger. If postgres is bottleneck, they will be close.


  // if(type == sql_rpc_template.GetTypeName()){
  //    struct timespec ts_start;
  //     clock_gettime(CLOCK_MONOTONIC, &ts_start);
  //     exec_start_us = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;
  // }


  // //skipping dispatch. Not really needed? 
  // std::vector<::google::protobuf::Message*> replies = app->Execute(type, data);

 

  //   for(auto reply: replies){
  //       if(reply == nullptr){
  //       Debug("Abort needs no reply");
  //       continue;
  //       }
  //     transport->SendMessage(this, *clientAddr, *reply);
  //     delete reply;
  //   }

  // //    if(type == sql_rpc_template.GetTypeName()){
  // //   struct timespec ts_start;
  // //       clock_gettime(CLOCK_MONOTONIC, &ts_start);
  // //       uint64_t exec_end_us = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;
  // //       Notice("Server side full latency: %lu us", exec_end_us - exec_start_us);
  // // }

  // return;
 
  auto f = [this, clientAddr, type, data](){
    Debug("Executing App interface");
  
    auto cb = [this, clientAddr](const std::vector<::google::protobuf::Message*> &replies){
      Debug("Trigger reply callback");
      //TEST: Send back unsigned.
      if(!signMessages){
        UW_ASSERT(replies.size() <= 1);
        for(auto reply: replies){
           if(reply == nullptr){
            Debug("Abort needs no reply");
            continue;
           }
          transport->SendMessage(this, *clientAddr, *reply);
          delete reply;
          delete clientAddr;
        }
      }
      else{
         //Create EBatch
        //ProcessReplies(digest, replies);
      }
    };
    if(USE_SYNC_INTERFACE){
      //Use Synchronous interface
      std::vector<::google::protobuf::Message*> replies = app->Execute(type, data);
      cb(replies);
    }
    else{
     //Use Asynchronous interface
      app->Execute_Callback(type, data,cb);
    }
  
    return (void*) true;
  };
  Debug("Dispatching to main");
  f();
  // transport->DispatchTP_main(f);
}



void Replica::HandleRequest_noPacked_shir(const TransportAddress &remote, const std::string &type, const std::string &data){
  Debug("Handling request message");

  const std::string digest = crypto::Hash(data);
  DebugHash(digest);

  //Do not process duplicates.   (Shir: make sure it's needed still)
  if (requests_dup.count(digest)) return;
  requests_dup.insert(digest);
  //if we didn't find the request digest in the map. I.e this is the first time handling this request
  Debug("new request: %s with digest: %s", type, digest);
  stats->Increment("handle_new_count",1);

  TransportAddress* clientAddr = remote.clone(); //The return address to reply to
  // const std::pair<std::string, std::string> msg_pair(type, data);




    struct timespec ts_start;
    clock_gettime(CLOCK_MONOTONIC, &ts_start);
    uint64_t exec_start_us = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;

    
  //Create a callback that will be called once the Request has been ordered by Hotstuff
  std::function<void(const std::string&, uint32_t seqnum)> execb = [this, digest, clientAddr, type, data,exec_start_us](const std::string &digest_param, uint32_t seqnum) {
    
      struct timespec ts_start;
      clock_gettime(CLOCK_MONOTONIC, &ts_start);
      uint64_t exec_end_us = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;
      Notice("Time at HotStuff : %lu us", exec_end_us - exec_start_us);


      Debug("Creating and sending callback");

      //The execb callback will be called from some thread that HS is running => we dispatch it back to our main processing thread (to guarantee sequential processing)
      auto f = [this, digest, digest_param, clientAddr, type, data, seqnum](){
          Debug("Callback: %d, %lu", idx, seqnum);  // This is called once per server
          stats->Increment("hotstuffpg_exec_callback",1);

          // prepare data structures for executeSlots()
          assert(digest == digest_param);
          
          // const std::pair<std::string, std::string> msg_pair(type, data);
          unpacked_requests[digest] = std::make_pair(std::move(type), std::move(data));
          replyAddrs[digest] = clientAddr; // replyAddress is the address of the client wo sent this request, so we can answer him

          Debug("Adding to pending executions");
          pendingExecutions[seqnum] = digest;
          executeSlots_unpacked();
          return (void*) true;
      };

      Debug("Dispatching to main");
      transport->DispatchTP_main(f);
  };


  Debug("Proposing execb");
  DebugHash(digest);
  hotstuffpg_interface->propose(digest, execb);
  proposedCounter++;
  Debug("Execb proposed");


  //Start Timer for dummy TX upon receiving the first Request
  if (this->firstReceive){
    this->firstReceive=false;
    Debug("Starting dummies Timer");
    transport->Timer(0, [this, pc = proposedCounter](){
      this->bubbleCB(pc);
    });
  }
  

}

//Receive Client Requests and Route them via HS first.
void Replica::HandleRequest(const TransportAddress &remote, const proto::Request &request) {
  //TODO: FIXME: Re-factor to get rid of packed message. It's a useless indirection that wastes costs.

  Debug("Handling request message");
  
  const string &digest = request.digest();
  DebugHash(digest);

  //Do not process duplicates.
  if (requests_dup.count(digest)) return;
    
  requests_dup.insert(digest);

  //if we didn't find the request digest in the map. I.e this is the first time handling this request
  Debug("new request: %s with digest: %s", request.packed_msg().type().c_str(), digest);
  stats->Increment("handle_new_count",1);

  TransportAddress* clientAddr = remote.clone(); //The return address to reply to
  const proto::PackedMessage &packedMsg = request.packed_msg();

  //Create a callback that will be called once the Request has been ordered by Hotstuff
  std::function<void(const std::string&, uint32_t seqnum)> execb = [this, digest, packedMsg, clientAddr](const std::string &digest_param, uint32_t seqnum) {
      Debug("Creating and sending callback");

      //The execb callback will be called from some thread that HS is running => we dispatch it back to our main processing thread (to guarantee sequential processing)
      auto f = [this, digest, packedMsg, clientAddr, digest_param, seqnum](){
          Debug("Callback: %d, %lu", idx, seqnum);  // This is called once per server
          stats->Increment("hotstuffpg_exec_callback",1);

          // prepare data structures for executeSlots()
          assert(digest == digest_param);
          requests[digest] = std::move(packedMsg);
          replyAddrs[digest] = clientAddr; // replyAddress is the address of the client wo sent this request, so we can answer him

          Debug("Adding to pending executions");
          pendingExecutions[seqnum] = digest;
          executeSlots();

          return (void*) true;
      };
      Debug("Dispatching to main");
      transport->DispatchTP_main(f);
  };

  Debug("Proposing execb");
  DebugHash(digest);
  hotstuffpg_interface->propose(digest, execb);
  proposedCounter++;
  Debug("Execb proposed");

  //Start Timer for dummy TX upon receiving the first Request
  if (this->firstReceive){
    this->firstReceive=false;
    Debug("Starting dummies Timer");
    transport->Timer(0, [this, pc = proposedCounter](){
      this->bubbleCB(pc);
    });
  }
  
}


void Replica::executeSlots() {
  Debug("Next exec seq num: %lu", execSeqNum);

  //Try to execute all of the available sequenced log prefix
  while(pendingExecutions.find(execSeqNum) != pendingExecutions.end()) { 
    Debug("executing seq num: %lu ", execSeqNum);
 
    const string &digest = pendingExecutions[execSeqNum];
    execSeqNum++;

    auto itr = requests.find(digest);
    UW_ASSERT(itr != requests.end());

    proto::PackedMessage &packedMsg = itr->second;
        
    if (packedMsg.type()=="dummy"){
      //Skip Dummy Requests
      stats->Increment("exec_dummy",1);
      continue;
    }

    stats->Increment("exec_request",1);

    //If we are using the more performant fake SMR mode -> then prepare a callback to be called by the execution threads on the server upon completion of the request
    if(fake_SMR) {
      auto cb = [this, digest, packedMsg](const std::vector<::google::protobuf::Message*> &replies){
        ProcessReplies(digest, replies);
      };
      app->Execute_Callback(packedMsg.type(), packedMsg.msg(),cb);

    } else {
      // Shir: server is synchronous
      // Shir: calling the server with the recieved message, and getting replies
      std::vector<::google::protobuf::Message*> replies = app->Execute(packedMsg.type(), packedMsg.msg());
      ProcessReplies(digest, replies);
      
    }
  }
  Debug("No more new sequenced requests");
}


void Replica::executeSlots_unpacked() {
  Debug("Next exec seq num: %lu", execSeqNum);

  //Try to execute all of the available sequenced log prefix
  while(pendingExecutions.find(execSeqNum) != pendingExecutions.end()) { 
    Debug("executing seq num: %lu ", execSeqNum);
 
    const string &digest = pendingExecutions[execSeqNum];
    execSeqNum++;

    auto itr = unpacked_requests.find(digest);
    UW_ASSERT(itr != unpacked_requests.end());
  
    // std::pair<std::string, std::string> &msg_pair=itr->second;
    auto [type, data] = itr->second;

    if (type=="dummy"){
      //Skip Dummy Requests
      stats->Increment("exec_dummy",1);
      continue;
    }

    stats->Increment("exec_request",1);

    //If we are using the more performant fake SMR mode -> then prepare a callback to be called by the execution threads on the server upon completion of the request
    if(fake_SMR) {
      auto cb = [this, digest](const std::vector<::google::protobuf::Message*> &replies){
        ProcessReplies(digest, replies);
      };
      app->Execute_Callback(type, data, cb);
    } else {
      Panic("Syncronous version is not supported");
    }
  }
  
  
  Debug("No more new sequenced requests");
}

void Replica::ProcessReplies(const std::string &digest, const std::vector<::google::protobuf::Message*> &replies){
  std::unique_lock lock(batchMutex); //ensure mutual exclusion when accessing EBatch
  for (const auto& reply : replies) {
    if(reply == nullptr){
      //Panic("Invalid execution for digest: %s", digest.c_str()); //Note: Abort needs no reply
      continue;
    }

    //TODO: If Batch size = 1: Just sign and send (dispatch to a worker)
      // proto::SignedMessage *signedMessage = new proto::SignedMessage();
      // SignMessage(*msg, keyManager->GetPrivateKey(id), id, *signedMessage);
      // delete msg;
      // return signedMessage;

      //Validate at client with:  if(!ValidateSignedMessage(signedMessage, keyManager, data, type))
  

    Debug("Sending reply");
    stats->Increment("execs_sent",1);
    //Add to EBatch
    EpendingBatchedMessages.push_back(reply);
    EpendingBatchedDigs.push_back(digest);
    Debug("Shir: pushed reply to eBatch for digest");
    DebugHash(digest);

    //If EBatch is full, cancel timer and process the batch
    if (EpendingBatchedMessages.size() >= EbatchSize) {
      Debug("EBatch is full, sending");
      if (EbatchTimerRunning) {
        transport->CancelTimer(EbatchTimerId);
        EbatchTimerRunning = false;
      }
      sendEbatch();
    } //Otherwise, start a timer
    else if (!EbatchTimerRunning) {
      EbatchTimerRunning = true;
      Debug("Starting ebatch timer");
      EbatchTimerId = transport->Timer(EbatchTimeoutMS, [this]() {
        std::unique_lock lock(batchMutex);
        Debug("EBatch timer expired, sending");
        this->EbatchTimerRunning = false;
        if(this->EpendingBatchedMessages.size()==0) return;
        //std::cerr << "calling Timer Ebatch" << std::endl;
        this->sendEbatch();
      });
    }
  }
}



void Replica::sendEbatch(){
  auto f = [this, EpendingBatchedMessages_ = EpendingBatchedMessages,
              EpendingBatchedDigs_ = EpendingBatchedDigs](){
    this->delegateEbatch(EpendingBatchedMessages_,
                EpendingBatchedDigs_);
    return (void*) true;
  };
  EpendingBatchedDigs.clear();
  EpendingBatchedMessages.clear();
  transport->DispatchTP_noCB(std::move(f));
}



//Use:
// auto f = [args](){ delegateEbatch}, Clear structures, dispatch->
void Replica::delegateEbatch(std::vector<::google::protobuf::Message*> EpendingBatchedMessages_,
   std::vector<std::string> EpendingBatchedDigs_){

    std::vector<proto::SignedMessage> EsignedMessages_;
    std::vector<std::string*> messageStrs;
    //std::cerr << "EbatchMessages.size: " << EpendingBatchedMessages.size() << std::endl;
    for (unsigned int i = 0; i < EpendingBatchedMessages_.size(); i++) {
      EsignedMessages_.push_back(proto::SignedMessage());
      //EsignedMessages_[i].Clear();
      EsignedMessages_[i].set_replica_id(id);
      proto::PackedMessage packedMsg;
      *packedMsg.mutable_msg() = EpendingBatchedMessages_[i]->SerializeAsString();
      *packedMsg.mutable_type() = EpendingBatchedMessages_[i]->GetTypeName();
      // The line inside the assert is necessary
      UW_ASSERT(packedMsg.SerializeToString(EsignedMessages_[i].mutable_packed_msg()));
      messageStrs.push_back(EsignedMessages_[i].mutable_packed_msg());
    }

    std::vector<std::string*> sigs;
    for (unsigned int i = 0; i < EpendingBatchedMessages_.size(); i++) {
      sigs.push_back(EsignedMessages_[i].mutable_signature());
    }

    hotstuffpgBatchedSigs::generateBatchedSignatures(messageStrs, keyManager->GetPrivateKey(id), sigs);

    //replyAddrsMutex.lock();
    for (unsigned int i = 0; i < EpendingBatchedMessages_.size(); i++) {
      transport->SendMessage(this, *replyAddrs[EpendingBatchedDigs_[i]], EsignedMessages_[i]);
      //std::cerr << "deleting reply" << std::endl;
      delete EpendingBatchedMessages_[i];
    }

}


}  // namespace pg_SMRstore



// std::pair<::google::protobuf::Message*,uint64_t> Replica::deserializeMsgAndObtainID(const string& type, const string& msg){
//   proto::SQL_RPC sql_rpc;
//   proto::TryCommit try_commit;
//   proto::UserAbort user_abort;
//   uint64_t txnid =0 ;
//   ::google::protobuf::Message* reply=nullptr;

//   if (type == sql_rpc.GetTypeName()) {
//     sql_rpc.ParseFromString(msg);
//     txnid = sql_rpc.client_id() + sql_rpc.txn_seq_num();
//     reply=&sql_rpc;
//   } else if (type == try_commit.GetTypeName()) {
//     try_commit.ParseFromString(msg);
//     txnid = try_commit.client_id() + try_commit.txn_seq_num();
//     reply=&try_commit;
//   } else if (type == user_abort.GetTypeName()) {
//     user_abort.ParseFromString(msg);
//     txnid = user_abort.client_id() + user_abort.txn_seq_num();
//     reply=&user_abort;
//   }
//   return std::make_pair(reply, txnid); 
// }