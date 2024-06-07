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
#include "store/hotstuffpgstore/replica.h"
#include "store/hotstuffpgstore/pbft_batched_sigs.h"
#include "store/hotstuffpgstore/common.h"

namespace hotstuffpgstore {

using namespace std;


Replica::Replica(const transport::Configuration &config, KeyManager *keyManager,
  App *app, int groupIdx, int idx, bool signMessages, uint64_t maxBatchSize,
                 uint64_t batchTimeoutMS, uint64_t EbatchSize, uint64_t EbatchTimeoutMS, bool primaryCoordinator, bool requestTx, int hotstuffpg_cpu, bool local_config, int numShards, Transport *transport,
                 bool asyncServer)
    : config(config),
      hotstuffpg_interface(groupIdx, idx, hotstuffpg_cpu, local_config),
      keyManager(keyManager), app(app), groupIdx(groupIdx), idx(idx),
    id(groupIdx * config.n + idx), signMessages(signMessages), maxBatchSize(maxBatchSize),
      batchTimeoutMS(batchTimeoutMS), EbatchSize(EbatchSize), EbatchTimeoutMS(EbatchTimeoutMS), primaryCoordinator(primaryCoordinator), requestTx(requestTx), numShards(numShards), transport(transport),
      asyncServer(asyncServer) {
  transport->Register(this, config, groupIdx, idx);

  // intial view
  currentView = 0;
  // initial seqnum
  nextSeqNum = 0;
  execSeqNum = 0;
  bubbles=0;

  batchTimerRunning = false;
  nextBatchNum = 0;

  proposedCounter=0;


  EbatchTimerRunning = false;
  for (int i = 0; i < EbatchSize; i++) {
    EsignedMessages.push_back(new proto::SignedMessage());
  }
  for (uint64_t i = 1; i <= EbatchSize; i++) {
   EbStatNames[i] = "ebsize_" + std::to_string(i);
  }

    Debug("Initialized replica at %d %d", groupIdx, idx);
    std::cerr<<"Initialized replica at %d %d\n", groupIdx, idx;

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

  transport->Timer(100, [this, pc = proposedCounter](){
    this->bubbleCB(pc);
  });

}

void Replica::bubbleCB(uint64_t currProposedCounter){
  // executeSlots();
  Debug("Bubble timer expired, check if seqnum has changed");
  // std::cerr << "Shir print:    " << "Bubble timer expired, check if seqnum has changed" << std::endl;

  auto pc =this->proposedCounter;
  Debug("Current was %d, and now is: %d",currProposedCounter,pc);
  // std::cerr << "Shir print:    " << "Current was " <<currProposedCounter<<" and now is:" <<pc << std::endl;

  if (this->proposedCounter == currProposedCounter){ 
    Debug("No progress was made, filling the pipeline with bubbles.");
    // std::cerr << "Shir print:    " << "No progress was made, filling the pipeline with bubbles." << std::endl;

    proposeBubble();
  }
  // else{
  //   Debug("not filling this time");
  // }


  Debug("Starting bubble timer");
  // std::cerr << "Shir print:    " << "Starting bubble timer" << std::endl;

  transport->Timer(1000, [this,pc](){
    this->bubbleCB(pc); 
  });
}



Replica::~Replica() {}


void Replica::ReceiveMessage(const TransportAddress &remote, const string &t,
                          const string &d, void *meta_data) {
  string type;
  string data;
  bool recvSignedMessage = false;

  Debug("Received message of type %s", t.c_str());
  std::cerr << "Shir print:    " << "Received message of type " <<t.c_str() << std::endl;

  
  type = t;
  data = d;

  if (type == recvrequest.GetTypeName()) {
    recvrequest.ParseFromString(data);
    HandleRequest(remote, recvrequest);
  } else if (type == recvrr.GetTypeName()) {
    recvrr.ParseFromString(data);
    std::string digest = recvrr.digest();
    if (requests.find(digest) != requests.end()) {
      Debug("Resending request");
      stats->Increment("request_rr",1);
      DebugHash(digest);
      proto::Request reqReply;
      reqReply.set_digest(digest);
      *reqReply.mutable_packed_msg() = requests[digest];
      transport->SendMessage(this, remote, reqReply);
    }
  } else {
    Debug("Sending request to app");
    handleMessage(remote, type, data);

  }
}

void Replica::handleMessage(const TransportAddress &remote, const string &type, const string &data){
    static int count = 0;
    count++;
    TransportAddress* clientAddr = remote.clone();
    auto f = [this, clientAddr, type, data](){
        //std::unique_lock lock(atomicMutex);
        ::google::protobuf::Message* reply = app->HandleMessage(type, data);
        if (reply != nullptr) {
            this->transport->SendMessage(this, *clientAddr, *reply);
            delete reply;
        } else {
            Debug("Invalid request of type %s", type.c_str());
        }
        return (void*) true;
    };
    transport->DispatchTP_noCB(f);
}

void Replica::HandleRequest(const TransportAddress &remote,
                               const proto::Request &request) {

  Debug("Handling request message");
  std::cerr << "Shir print:    " << "Handling request message" << std::endl;


  string digest = request.digest();
  DebugHash(digest);

  // Shir: requests_dup is a map from string (digest) to an hotstuff msg
  if (requests_dup.find(digest) == requests_dup.end()) {
  
    // Shir: if we didn't find the request digest in the map. I.e this is the first time handling this request
    Debug("new request: %s", request.packed_msg().type().c_str());
    Debug("Shir: the new requests digest is:       %s",digest);
    std::cerr << "Shir print:    " << "new request: "<< request.packed_msg().type().c_str()<< " with digest "<< digest<< std::endl;
    DebugHash(digest);   // Shir

    stats->Increment("handle_new_count",1);
    // This unordered map is only used here so read doesn't require locks.
    requests_dup[digest] = request.packed_msg();

    TransportAddress* clientAddr = remote.clone();
    proto::PackedMessage packedMsg = request.packed_msg();

    std::function<void(const std::string&, uint32_t seqnum)> execb = [this, digest, packedMsg, clientAddr](const std::string &digest_param, uint32_t seqnum) {
        Debug("Creating and sending callback");

        // Shir: execb is a function that is probably being executed by hotstuff (should be verified).
        // Shir: this function it self also creating the function f and dispatching it to main (who is main? need to check).
        auto f = [this, digest, packedMsg, clientAddr, digest_param, seqnum](){

            // Shir: f is probably also being executed by hotstuff
            Debug("Callback: %d, %lu", idx, seqnum);  // This is called once per server
            std::cerr << "Shir print:    " << "Callback: "<<idx <<", "<<seqnum << std::endl;

            stats->Increment("hotstuffpg_exec_callback",1);

            // prepare data structures for executeSlots()
            assert(digest == digest_param);
            requests[digest] = packedMsg;
            replyAddrs[digest] = clientAddr; // replyAddress is the address of the client wo sent this request, so we can answer him

            // Shir: now we're listing all of the executions (execb) that weren't executed yet.
            Debug("Adding to pending executions");
            std::cerr << "Shir print:    " << "Adding to pending executions" << std::endl;

            pendingExecutions[seqnum] = digest;

            Debug("Printing out pendingExecutions");
            std::cerr << "Shir print:    " << "Printing out pendingExecutions" << std::endl;

            for(auto& it: pendingExecutions) {
              // std::cout << it.first << " " << it.second << std::endl;
              std::cerr << "Shir print:    "<< it.first << " " << it.second.c_str() << std::endl;
              DebugHash(it.second);
            }
            Debug("Finished printing out pendingExecutions");

            executeSlots();

            return (void*) true;
        };
        
        Debug("Dispatching to main");
        transport->DispatchTP_main(f);
    

    };
    Debug("Proposing execb");
    Debug("Shir:   hopefully with this digest:");
    DebugHash(digest);
    hotstuffpg_interface.propose(digest, execb);
    proposedCounter++;
    Debug("Execb proposed");
  }
}


void Replica::proposeBubble(){

  // create a dummy digest to propose to Hotstuff. Proposals have to be of length 32 chars, and unique.
  // string dummy_digest_init(std::to_string(bubbles)+"dummy"+std::string(32, '0'));
  // string dummy_digest = dummy_digest_init.substr(0,32);
  string dummy_digest_init(std::string(32, '0')+std::to_string(bubbles));
  string dummy_digest = dummy_digest_init.substr(dummy_digest_init.length()-32);

  // std::cerr<< "Dummy create is: "<<dummy_digest<<"\n";
  bubbles++;

  // Debug("Bubble %s size is:  %d and capacity is: %d",dummy_digest.c_str(),dummy_digest.length(),dummy_digest.capacity());

  proto::PackedMessage bubblePackedMsg;
  bubblePackedMsg.set_msg("dummy");
  bubblePackedMsg.set_type("dummy");

  auto execb_bubble = [this, dummy_digest, bubblePackedMsg](const std::string &digest_paramm, uint32_t seqnumm) {
    auto f = [this, dummy_digest, bubblePackedMsg, digest_paramm, seqnumm](){
      Debug("Adding %s to pending executions in seqnum %d",dummy_digest.c_str(),seqnumm);
      requests[dummy_digest] = bubblePackedMsg;
      replyAddrs[dummy_digest] = nullptr;
      pendingExecutions[seqnumm] = dummy_digest;
      executeSlots();
      return (void*) true;
    };
    transport->DispatchTP_main(f);
  };

  hotstuffpg_interface.propose(dummy_digest, execb_bubble);
  proposedCounter++;
}

void Replica::executeSlots() {
  Debug("Shir: trying to execute new slots");
  Debug("exec seq num: %lu", execSeqNum);
  std::cerr << "Shir print:    " << "trying to execute new slots. exec seq num: "<<execSeqNum << std::endl;


  Debug("Shir: this is the list of current pending executions:  ");
  for(auto& it: pendingExecutions) {
    // std::cout << it.first << " " << it.second << std::endl;
    // Debug("Pending sequence number: %lu", it.first);
    DebugHash(it.second);
  }

  // Shir: looking for pending execution that matches the current exec seq num. This basically means that I can progress and execute the next slot (because hotstuff has already committed it)
  while(pendingExecutions.find(execSeqNum) != pendingExecutions.end()) { 
    Debug("Pending execution exists");
    // std::cerr << "Shir print:    " << "Pending execution exists" << std::endl;

    string digest = pendingExecutions[execSeqNum];

      // only execute if we have the full request      
      // Shir: "requests" is a map from digest to received requests
      if (requests.find(digest) != requests.end()) {
        // Shir: if i'm here it means that i've found the request (returned from hotstuff?), and i'm going to execute it
        stats->Increment("exec_request",1);
        Debug("executing seq num: %lu ", execSeqNum);
        std::cerr << "Shir print:    " << "executing seq num: "<<execSeqNum << std::endl;

        execSeqNum++;


        // Shir: This is the messages recieved from hotstuff
        proto::PackedMessage packedMsg = requests[digest];

        if (packedMsg.type()=="dummy"){
          Debug("Skip bubble execution");
          std::cerr << "Shir print:    " << "Skip bubble execution" << std::endl;

          continue;
        }


        if(asyncServer) {

          auto cb= [this, digest, packedMsg](const std::vector<::google::protobuf::Message*> &replies){
            std::cerr << "Shir print:    " << "executing cb after executing sql_rpc" << std::endl;

            for (const auto& reply : replies) {
              std::cerr << "Shir print:    " << "count replies" << std::endl;

              if (reply != nullptr) {
                Debug("Sending reply");
                std::cerr << "Shir print:    " << "sending reply" << std::endl;

                stats->Increment("execs_sent",1);
                EpendingBatchedMessages.push_back(reply);
                EpendingBatchedDigs.push_back(digest);
                if (EpendingBatchedMessages.size() >= EbatchSize) {
                  Debug("EBatch is full, sending");

                  sendEbatch();
                } else if (!EbatchTimerRunning) {
                  EbatchTimerRunning = true;
                  Debug("Starting ebatch timer");

                }
              } else {
                Debug("Invalid execution for the following:       %s", digest.c_str());
              }
            }
          };
          

   
          app->Execute_Callback(packedMsg.type(), packedMsg.msg(),cb);

        } else {
          // Shir: server is synchronous
          // Shir: calling the server with the recieved message, and getting replies
          std::vector<::google::protobuf::Message*> replies = app->Execute(packedMsg.type(), packedMsg.msg());

          // Shir: dealing with the replies from the server
          for (const auto& reply : replies) {
            if (reply != nullptr) {
              // Shir: for every reply returned frmo server, i need to send it. replies are batched together to batched of size "EbatchSize" before sending them (currently set to 1)
              Debug("Sending reply");
              stats->Increment("execs_sent",1);
              EpendingBatchedMessages.push_back(reply);
              EpendingBatchedDigs.push_back(digest);
              if (EpendingBatchedMessages.size() >= EbatchSize) {
                Debug("EBatch is full, sending");
                sendEbatch();
              } else if (!EbatchTimerRunning) {
                EbatchTimerRunning = true;
                Debug("Starting ebatch timer");
              }
            } else {
              Debug("Invalid execution");
            }
          }

        }
    
      } else {
        // Shir: i didn't find the request by its digest (I'm assuming it should get here but will leave this code for now for debug purposes)       
        Debug("Outside of requests");
        stats->Increment("miss_hotstuffpg_req_txn",1);
        break;
      }
  
  }
  Debug("Out of while");

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


std::pair<::google::protobuf::Message*,uint64_t> Replica::deserializeMsgAndObtainID(const string& type, const string& msg){
  proto::SQL_RPC sql_rpc;
  proto::TryCommit try_commit;
  proto::UserAbort user_abort;
  uint64_t txnid =0 ;
  ::google::protobuf::Message* reply=nullptr;

  if (type == sql_rpc.GetTypeName()) {
    sql_rpc.ParseFromString(msg);
    txnid = sql_rpc.client_id() + sql_rpc.txn_seq_num();
    reply=&sql_rpc;
  } else if (type == try_commit.GetTypeName()) {
    try_commit.ParseFromString(msg);
    txnid = try_commit.client_id() + try_commit.txn_seq_num();
    reply=&try_commit;
  } else if (type == user_abort.GetTypeName()) {
    user_abort.ParseFromString(msg);
    txnid = user_abort.client_id() + user_abort.txn_seq_num();
    reply=&user_abort;
  }
  return std::make_pair(reply, txnid); 
}

}  // namespace hotstuffpgstore
