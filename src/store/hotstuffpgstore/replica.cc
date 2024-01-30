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

// No view change, but still view numbers
// Client -> leader request: (op, ts, client address)
// Leader -> All - leader preprepare: (view, seq, d(m))_i, m=(op, ts, client
// address) All - leader -> All prepare: (view, seq, d(m))_i once 1 preprepare
// and 2f prepares for (view, seq d(m)) then All -> All commit: (view, seq,
// d(m))_i

// Faults
// Primary ignores client request
// client sends request to all, replicas send to primary, start timeout waiting
// for preprepare Primary doesn't send preprepare to all, if some client gets
// prepare for request it doesn't have preprepare, start timeout Primary ignores
// f correct replicas, sends preprepare to f+1, colludes with f incorrect, f
// correct can't remove primary, so whenever you receive a preprepare you need
// to start a timer until you receive 2f prepares multicast commit, still need
// to start timeout
//      - primary sends prepare to f+1 correct, f wrong, f correct ignored (f
//      ignored will send view change messages, not enough)
//      - f+1 correct get 2f prepares (from each other and f wrong)
//      - f+1 send commit to all, should the f correct replicas accept the
//      commit messages

Replica::Replica(const transport::Configuration &config, KeyManager *keyManager,
  App *app, int groupIdx, int idx, bool signMessages, uint64_t maxBatchSize,
                 uint64_t batchTimeoutMS, uint64_t EbatchSize, uint64_t EbatchTimeoutMS, bool primaryCoordinator, bool requestTx, int hotstuffpg_cpu, int numShards, Transport *transport,
                 bool asyncServer)
    : config(config),
      hotstuffpg_interface(groupIdx, idx, hotstuffpg_cpu),
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
  execBatchNum = 0;

  batchTimerRunning = false;
  nextBatchNum = 0;

  EbatchTimerRunning = false;
  for (int i = 0; i < EbatchSize; i++) {
    EsignedMessages.push_back(new proto::SignedMessage());
  }
  for (uint64_t i = 1; i <= EbatchSize; i++) {
   EbStatNames[i] = "ebsize_" + std::to_string(i);
  }

    Debug("Initialized replica at %d %d", groupIdx, idx);
  
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

Replica::~Replica() {}


void Replica::ReceiveMessage(const TransportAddress &remote, const string &t,
                          const string &d, void *meta_data) {
  string type;
  string data;
  bool recvSignedMessage = false;

  Debug("Received message of type %s", t.c_str());
  
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
    if (batchedRequests.find(digest) != batchedRequests.end()) {
      Debug("Resending batch");
      stats->Increment("batch_rr",1);
      DebugHash(digest);
      transport->SendMessage(this, remote, batchedRequests[digest]);
    }
  } else {
    Debug("Sending request to app");
    handleMessage(remote, type, data);

  }
}

void Replica::handleMessage(const TransportAddress &remote, const string &type, const string &data){
    Debug("Shir: XXXXXXX");
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

  string digest = request.digest();
  DebugHash(digest);

  // Shir: requests_dup is a map from string (digest) to an hotstuff msg
  if (requests_dup.find(digest) == requests_dup.end()) {
  
    // Shir: if we didn't find the request digest in the map. I.e this is the first time handling this request
    Debug("new request: %s", request.packed_msg().type().c_str());
    Debug("Shir: the new requests digest is:       %s",digest);
    DebugHash(digest);   // Shir

    stats->Increment("handle_new_count",1);
    // This unordered map is only used here so read doesn't require locks.
    requests_dup[digest] = request.packed_msg();

    TransportAddress* clientAddr = remote.clone();
    proto::PackedMessage packedMsg = request.packed_msg();

    std::function<void(const std::string&, uint32_t seqnum)> execb = [this, digest, packedMsg, clientAddr](const std::string &digest_param, uint32_t seqnum) {
        // Shir: check if i should ever be on the other condition. if not-- delete
        Debug("Creating and sending callback");

        // Shir: execb is a function that is probably being executed by hotstuff (should be verified).
        // Shir: this function it self also creating the function f and dispatching it to main (who is main? need to check).
        auto f = [this, digest, packedMsg, clientAddr, digest_param, seqnum](){

            // Shir: f is probably also being executed by hotstuff
            Debug("Callback: %d, %lu", idx, seqnum); 
            stats->Increment("hotstuffpg_exec_callback",1);

            // prepare data structures for executeSlots()
            assert(digest == digest_param);
            requests[digest] = packedMsg;
            replyAddrs[digest] = clientAddr; // replyAddress is the address of the client wo sent this request, so we can answer him


            //FIXME: For Hotstuff this code seems essentially useless: It just creates a mapping back to itself... (Seems to be a relic of PBFT code handling)

//  later looking for    string batchDigest = pendingExecutions[execSeqNum];  batchDigest->digest
// then: batchedRequests.find(batchDigest) 



            proto::BatchedRequest batchedRequest;
            (*batchedRequest.mutable_digests())[0] = digest_param;
            string batchedDigest = BatchedDigest(batchedRequest);

            // batchedRequests[batchedDigest] = batchedRequest;
            batchedRequests[digest] = batchedRequest;


            Debug("Shir: trying to debug the mapping situation. Digest:");
            DebugHash(digest);
            std::cerr<<"batchedDigest:    " <<"\n";
            DebugHash(batchedDigest);

            // std::cerr<<"batchedRequest:    "<<batchedRequest <<"\n";
            // std::cerr<<"batchedRequest[0]:    "<<batchedRequest[0] <<"\n";
            // std::cerr<<"batchedRequests[batchedDigest]:    "<<batchedRequests[batchedDigest] <<"\n";


            // Shir: can we replace the last statement to:
            // batchedRequests[batchedDigest] = digest;
            //                string batchDigest = pendingExecutions[execSeqNum]; what we look for




            // Shir: now we're listing all of the executions (execb) that weren't executed yet.
            Debug("Adding to pending executions");
            pendingExecutions[seqnum] = digest;
            // pendingExecutions[seqnum] = batchedDigest;
            
            std::cout << batchedDigest << std::endl;
            DebugHash(batchedDigest);

            Debug("Printing out pendingExecutions");
            for(auto& it: pendingExecutions) {
              std::cout << it.first << " " << it.second << std::endl;
              DebugHash(it.second);

              // std::cout << it.first << " " << it.second << std::endl;
            }
            Debug("Finished printing out pendingExecutions");

            executeSlots();

            return (void*) true;
        };
        
        Debug("Dispatching to main");
        transport->DispatchTP_main(f);
    

    };
    Debug("Proposing execb");
    hotstuffpg_interface.propose(digest, execb); // Shir: sending the execb to hotstuff
    Debug("Execb proposed");

    auto need_to_fill_pipeline=true;
    if (need_to_fill_pipeline){

      std::string digest_mb("mitz"+digest);
      //                                                                         [values captured in the function](paramaters taken as input)
      auto execb_bubblem = [this, digest_mb, packedMsg,clientAddr ](const std::string &digest_paramm, uint32_t seqnumm) {
      auto f = [this, digest_mb, packedMsg,clientAddr, digest_paramm, seqnumm](){
        requests[digest_mb] = packedMsg;
        proto::BatchedRequest batchedRequest;
        (*batchedRequest.mutable_digests())[0] = digest_paramm;
        string batchedDigest = BatchedDigest(batchedRequest);
        batchedRequests[digest_mb] = batchedRequest;
        replyAddrs[digest_mb] = clientAddr; // replyAddress is the address of the client wo sent this request, so we can answer him

        pendingExecutions[seqnumm] = digest_mb;


        Debug("Shir: trying to debug the mapping situation. Digest:");
        DebugHash(digest_mb);
        std::cerr<<"batchedDigest:    " <<"\n";
        DebugHash(batchedDigest);

        // std::cout << batchedDigest << std::endl;
        // DebugHash(batchedDigest);
        return (void*) true;
      };
      transport->DispatchTP_main(f);
      
      };
      hotstuffpg_interface.propose(digest_mb, execb_bubblem);


      std::string digest_m1("shir"+digest);
      auto execb_bubblem1 = [this, digest_m1, packedMsg,clientAddr ](const std::string &digest_paramm, uint32_t seqnumm) {
      auto f = [this, digest_m1, packedMsg, clientAddr,digest_paramm, seqnumm](){
        requests[digest_m1] = packedMsg;
        proto::BatchedRequest batchedRequest;
        (*batchedRequest.mutable_digests())[0] = digest_paramm;
        string batchedDigest = BatchedDigest(batchedRequest);
        batchedRequests[digest_m1] = batchedRequest;
        replyAddrs[digest_m1] = clientAddr; // replyAddress is the address of the client wo sent this request, so we can answer him

        pendingExecutions[seqnumm] = digest_m1;


        Debug("Shir: trying to debug the mapping situation. Digest:");
        DebugHash(digest_m1);
        std::cerr<<"batchedDigest:    " <<"\n";
        DebugHash(batchedDigest);

        return (void*) true;
      };
      transport->DispatchTP_main(f);
      };
      hotstuffpg_interface.propose(digest_m1, execb_bubblem1);


      std::string digest_m2("nosh"+digest);
      auto execb_bubblem2 = [this, digest_m2, packedMsg,clientAddr ](const std::string &digest_paramm, uint32_t seqnumm) {
      auto f = [this, digest_m2, packedMsg,clientAddr, digest_paramm, seqnumm](){
        requests[digest_m2] = packedMsg;
        proto::BatchedRequest batchedRequest;
        (*batchedRequest.mutable_digests())[0] = digest_paramm;
        string batchedDigest = BatchedDigest(batchedRequest);
        batchedRequests[digest_m2] = batchedRequest;
        replyAddrs[digest_m2] = clientAddr; // replyAddress is the address of the client wo sent this request, so we can answer him

        pendingExecutions[seqnumm] = digest_m2;


        Debug("Shir: trying to debug the mapping situation. Digest:");
        DebugHash(digest_m2);
        std::cerr<<"batchedDigest:    " <<"\n";
        DebugHash(batchedDigest);

        return (void*) true;
      };
      transport->DispatchTP_main(f);
      };
      hotstuffpg_interface.propose(digest_m2, execb_bubblem2);

      Debug("Printing out pendingExecutions (after bubbles)");
      for(auto& it: pendingExecutions) {
        std::cout << it.first << " " << it.second << std::endl;
        DebugHash(it.second);

      }
      Debug("Finished printing out pendingExecutions");
    }
  
  }
}



void Replica::executeSlots_callback(std::vector<::google::protobuf::Message*> &replies,
  string batchDigest, string digest){
        //std::vector<::google::protobuf::Message*> *replies = (std::vector<::google::protobuf::Message*> *) replies_void;

        //std::cerr << "executing callback on CPU " << sched_getcpu() << std::endl;

        std::unique_lock lock(batchMutex);
        for (const auto& reply : replies) {
          if (reply != nullptr) {
            Debug("Sending reply");
            stats->Increment("execs_sent",1);
            EpendingBatchedMessages.push_back(reply);
            EpendingBatchedDigs.push_back(digest);
            if (EpendingBatchedMessages.size() >= EbatchSize) {
              Debug("EBatch is full, sending");
              // if (false && EbatchTimerRunning) {
              //   transport->CancelTimer(EbatchTimerId);
              //   EbatchTimerRunning = false;
              // }
              //std::cerr << "callingEbatch" << std::endl;
              sendEbatch();
            } else if (!EbatchTimerRunning) {
              EbatchTimerRunning = true;
              Debug("Starting ebatch timer");
              // EbatchTimerId = transport->Timer(EbatchTimeoutMS, [this]() {
              //   std::unique_lock lock(batchMutex);

              //   Debug("EBatch timer expired, sending");
              //   this->EbatchTimerRunning = false;
              //   if(this->EpendingBatchedMessages.size()==0) return;
              //   //std::cerr << "calling Timer Ebatch" << std::endl;
              //   this->sendEbatch();
              // });
            }
          } else {
            Debug("Invalid execution");
          }
        }
        //delete replies;
        // std::unique_lock lock(batchMutex);
        // execBatchNum++;
        // if ((int) execBatchNum >= batchedRequests[batchDigest].digests_size()) {
        //   Debug("Done executing batch");
        //   execBatchNum = 0;
        //   execSeqNum++;
        // }

}


void Replica::executeSlots() {
  Debug("Shir: trying to execute new slots");
  Debug("exec seq num: %lu", execSeqNum);

  Debug("Shir: this is the list of current pending executions:  ");
  for(auto& it: pendingExecutions) {
    std::cout << it.first << " " << it.second << std::endl;
    Debug("Pending sequence number: %lu", it.first);
  }

  // Shir: looking for pending execution that matches the current exec seq num. This basically means that I can progress and execute the next slot (because hotstuff has already committed it)
  while(pendingExecutions.find(execSeqNum) != pendingExecutions.end()) { 
  
    Debug("Pending execution exists");

    // Shir: not sure what the timers are for, lets hide them for now
    // if (seqnumCommitTimers.find(execSeqNum) != seqnumCommitTimers.end()) {
    //   transport->CancelTimer(seqnumCommitTimers[execSeqNum]);
    //   seqnumCommitTimers.erase(execSeqNum);
    // }

    string batchDigest = pendingExecutions[execSeqNum];

    //Shir: assuming requests are batched, proceed to execution only if you find the entire batch ?
    // only execute when we have the batched request

    Debug("looking for the following batch digest:    ");
    DebugHash(batchDigest);

    if (batchedRequests.find(batchDigest) != batchedRequests.end()) {

      // Shir: there should be one request per batch but we should verify that later


      Debug("Batched request");
      string digest = (*batchedRequests[batchDigest].mutable_digests())[execBatchNum];
      Debug("this is the request digest:    ");
      DebugHash(digest);


      // only execute if we have the full request      
      // Shir: "requests" is a map from digest to received requests
      if (requests.find(digest) != requests.end()) {
        // Shir: if i'm here it means that i've found the request (returned from hotstuff?), and i'm going to execute it
        stats->Increment("exec_request",1);
        Debug("executing seq num: %lu %lu", execSeqNum, execBatchNum);

        // Shir: This is the messages recieved from hotstuff
        proto::PackedMessage packedMsg = requests[digest];
        if(asyncServer) {
          // Shir: server is asynchronous (will deal with this scope later)

          execBatchNum++;

          if ((int) execBatchNum >= batchedRequests[batchDigest].digests_size()) {
            Debug("Done executing batch");
            execBatchNum = 0;
            execSeqNum++;

          }
          transport->Timer(0, [this, digest, batchDigest, packedMsg](){
            execute_callback ecb = [this, digest, batchDigest](std::vector<::google::protobuf::Message*> replies) {
              for (const auto& reply : replies) {
                if (reply != nullptr) {
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

            };

            app->Execute_Callback(packedMsg.type(), packedMsg.msg(), ecb);
          });
          
        } else {
          // Shir: server is synchronous (current situation)

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


          // Shir: after dealing with all the replies, we advance the batch seq number.
          execBatchNum++;
          // Debug("Shir: 555 advancing the exec batch num to %d",execBatchNum);  
          // Debug("Shir: exec batch num is %d",execBatchNum);
          // Debug("Shir:   %d",batchedRequests[batchDigest].digests_size());

          // Shir: if the next batch num to execute is greater than _____: it means i'm done executing this batch
          if ((int) execBatchNum >= batchedRequests[batchDigest].digests_size()) {
            Debug("Done executing batch");
            execBatchNum = 0;
            // // SHIR: advancing in 8 because of the bubbles?
            // execSeqNum=execSeqNum+8;
            execSeqNum++;
            // Debug("Shir: 666 advancing the exec seq num to %d",execSeqNum);  

          }



        }
      


      } else {
        // Shir: i didn't find the request by its digest (I'm assuming it should get here but will leave this code for now for debug purposes)       
        Debug("Outside of requests");
        stats->Increment("miss_hotstuffpg_req_txn",1);
        break;
      }
    
    } else {
      Debug("Shir: did not find it");
      //Shir: if you got here it means that you didn't have the batch for the given request (I'm assuming it should get here but will leave this code for now for debug purposes)
      Debug("Outside of batched");
      stats->Increment("miss_hotstuffpg_req_batch",1);
      break;
    }
  
  
  }
  Debug("Out of while");

}




void Replica::sendEbatch(){
  if(true){
    // std::function<void*> f(std::bind(&Replica::delegateEbatch, this, EpendingBatchedMessages,
    //            EsignedMessages, EpendingBatchedDigs));
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
  else{
    sendEbatch_internal();
  }
}

void Replica::sendEbatch_internal() {
  //std::cerr << "executing sendEbatch" << std::endl;
  stats->Increment(EbStatNames[EpendingBatchedMessages.size()], 1);
  std::vector<std::string*> messageStrs;
  //std::cerr << "EbatchMessages.size: " << EpendingBatchedMessages.size() << std::endl;
  for (unsigned int i = 0; i < EpendingBatchedMessages.size(); i++) {
    EsignedMessages[i]->Clear();
    EsignedMessages[i]->set_replica_id(id);
    proto::PackedMessage packedMsg;
    *packedMsg.mutable_msg() = EpendingBatchedMessages[i]->SerializeAsString();
    *packedMsg.mutable_type() = EpendingBatchedMessages[i]->GetTypeName();
    UW_ASSERT(packedMsg.SerializeToString(EsignedMessages[i]->mutable_packed_msg()));
    messageStrs.push_back(EsignedMessages[i]->mutable_packed_msg());
  }

  std::vector<std::string*> sigs;
  for (unsigned int i = 0; i < EpendingBatchedMessages.size(); i++) {
    sigs.push_back(EsignedMessages[i]->mutable_signature());
  }

  hotstuffpgBatchedSigs::generateBatchedSignatures(messageStrs, keyManager->GetPrivateKey(id), sigs);

  //replyAddrsMutex.lock();
  for (unsigned int i = 0; i < EpendingBatchedMessages.size(); i++) {
    transport->SendMessage(this, *replyAddrs[EpendingBatchedDigs[i]], *EsignedMessages[i]);
    //std::cerr << "deleting reply" << std::endl;
    delete EpendingBatchedMessages[i];
  }
  //replyAddrsMutex.unlock();
  EpendingBatchedDigs.clear();
  EpendingBatchedMessages.clear();
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

void Replica::startActionTimer(uint64_t seq_num, uint64_t viewnum, std::string digest) {
  // actionTimers[seq_num][viewnum][digest] = transport->Timer(10, [seq_num, viewnum, digest, this]() {
  //   Debug("action timer expired, sending");
  //   proto::ABRequest abreq;
  //   abreq.set_seqnum(seq_num);
  //   abreq.set_viewnum(viewnum);
  //   abreq.set_digest(digest);
  //   int primaryIdx = this->config.GetLeaderIndex(this->currentView);
  //   this->stats->Increment("sent_ab_req",1);
  //   this->transport->SendMessageToReplica(this, this->groupIdx, primaryIdx, abreq);
  // });
}

void Replica::cancelActionTimer(uint64_t seq_num, uint64_t viewnum, std::string digest) {
  // if (actionTimers[seq_num][viewnum].find(digest) != actionTimers[seq_num][viewnum].end()) {
  //   transport->CancelTimer(actionTimers[seq_num][viewnum][digest]);
  //   actionTimers[seq_num][viewnum].erase(digest);
  // }
}



}  // namespace hotstuffpgstore
