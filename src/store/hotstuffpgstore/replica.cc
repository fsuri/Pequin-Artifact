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
#ifdef USE_HOTSTUFF_STORE
      hotstuffpg_interface(groupIdx, idx, hotstuffpg_cpu),
#endif
      keyManager(keyManager), app(app), groupIdx(groupIdx), idx(idx),
    id(groupIdx * config.n + idx), signMessages(signMessages), maxBatchSize(maxBatchSize),
      batchTimeoutMS(batchTimeoutMS), EbatchSize(EbatchSize), EbatchTimeoutMS(EbatchTimeoutMS), primaryCoordinator(primaryCoordinator), requestTx(requestTx), numShards(numShards), transport(transport),
      asyncServer(asyncServer) {
  transport->Register(this, config, groupIdx, idx);

  // Debug("Shir: creating new replica\n");
  // // Debug(USE_HOTSTUFF_STORE);
  // #ifdef USE_HOTSTUFF_STORE
  //   Debug("Shir: in the if\n");
  // #endif

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

bool Replica::ValidateHMACedMessage(const proto::SignedMessage &signedMessage, std::string &data, std::string &type) {
  proto::PackedMessage packedMessage;
  packedMessage.ParseFromString(signedMessage.packed_msg());
  data = packedMessage.msg();
  type = packedMessage.type();

  proto::HMACs hmacs;
  hmacs.ParseFromString(signedMessage.signature());
  return crypto::verifyHMAC(signedMessage.packed_msg(), (*hmacs.mutable_hmacs())[idx], sessionKeys[signedMessage.replica_id() % config.n]);
}

void Replica::CreateHMACedMessage(const ::google::protobuf::Message &msg, proto::SignedMessage& signedMessage) {
  proto::PackedMessage packedMsg;
  *packedMsg.mutable_msg() = msg.SerializeAsString();
  *packedMsg.mutable_type() = msg.GetTypeName();
  // TODO this is not portable. SerializeAsString may not return the same
  // result every time
  std::string msgData = packedMsg.SerializeAsString();
  signedMessage.set_packed_msg(msgData);
  signedMessage.set_replica_id(id);

  proto::HMACs hmacs;
  for (uint64_t i = 0; i < config.n; i++) {
    (*hmacs.mutable_hmacs())[i] = crypto::HMAC(msgData, sessionKeys[i]);
  }
  signedMessage.set_signature(hmacs.SerializeAsString());
}

void Replica::ReceiveMessage(const TransportAddress &remote, const string &t,
                          const string &d, void *meta_data) {
  string type;
  string data;
  bool recvSignedMessage = false;

  Debug("Received message of type %s", t.c_str());

  if (t == tmpsignedMessage.GetTypeName()) {
    if (!tmpsignedMessage.ParseFromString(d)) {
      return;
    }

    if (!ValidateHMACedMessage(tmpsignedMessage, data, type)) {
      Debug("Message is invalid!");
      stats->Increment("invalid_sig",1);
      return;
    }
    recvSignedMessage = true;
    Debug("Message is valid!");
    Debug("Message is from %lu", tmpsignedMessage.replica_id());
  } else {
    type = t;
    data = d;
  }

  if (type == recvrequest.GetTypeName()) {
    recvrequest.ParseFromString(data);
    HandleRequest(remote, recvrequest);
  } else if (type == recvbatchedRequest.GetTypeName()) {
    recvbatchedRequest.ParseFromString(data);
    HandleBatchedRequest(remote, recvbatchedRequest);
  } else if (type == recvab.GetTypeName()) {
    recvab.ParseFromString(data);
    if (slots.getSlotDigest(recvab.seqnum(), recvab.viewnum()) == recvab.digest()) {
      stats->Increment("valid_ab", 1);

      proto::Preprepare preprepare;
      preprepare.set_seqnum(recvab.seqnum());
      preprepare.set_viewnum(recvab.viewnum());
      preprepare.set_digest(recvab.digest());
      sendMessageToAll(preprepare);
    } else {
      stats->Increment("invalid_ab", 1);
    }
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
  } else if (type == recvpreprepare.GetTypeName()) {
    if (signMessages && !recvSignedMessage) {
      stats->Increment("invalid_sig_pp",1);
      return;
    }

    recvpreprepare.ParseFromString(data);
    HandlePreprepare(remote, recvpreprepare, tmpsignedMessage);
  } else if (type == recvprepare.GetTypeName()) {
    recvprepare.ParseFromString(data);
    if (signMessages && !recvSignedMessage) {
      stats->Increment("invalid_sig_p",1);
      return;
    }

    HandlePrepare(remote, recvprepare, tmpsignedMessage);
  } else if (type == recvcommit.GetTypeName()) {
    recvcommit.ParseFromString(data);
    if (signMessages && !recvSignedMessage) {
      stats->Increment("invalid_sig_c",1);
      return;
    }

    HandleCommit(remote, recvcommit, tmpsignedMessage);
  } else if (type == recvgrouped.GetTypeName()) {
    recvgrouped.ParseFromString(data);

    HandleGrouped(remote, recvgrouped);
  } else {
    Debug("Sending request to app");
    handleMessage(remote, type, data);

  }
}

void Replica::handleMessage(const TransportAddress &remote, const string &type, const string &data){
    static int count = 0;
    count++;
    if((numShards <= 6 || numShards == 12)){
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
        //transport->DispatchTP_main(f);

        if (numShards <= 6)
            transport->DispatchTP_noCB(f);
        else // numShards == 12
            transport->DispatchTP_main(f);
    }
    else{
       std::cerr<< "handle message not being dispatched for pbft" << std::endl;
        // if (numShards != 24)
        //     Panic("Currently only support numShards = 6, 12 or 24");

        ::google::protobuf::Message* reply = app->HandleMessage(type, data);
        if (reply != nullptr) {
            transport->SendMessage(this, remote, *reply);
            delete reply;
        } else {
            Debug("Invalid request of type %s", type.c_str());
        }
    }
}

bool Replica::sendMessageToPrimary(const ::google::protobuf::Message& msg) {
  int primaryIdx = config.GetLeaderIndex(currentView);
  if (signMessages) {
    proto::SignedMessage signedMsg;
    // SignMessage(msg, keyManager->GetPrivateKey(id), id, signedMsg);
    Panic("Unimplemented");
    return transport->SendMessageToReplica(this, groupIdx, primaryIdx, signedMsg);
  } else {
    return transport->SendMessageToReplica(this, groupIdx, primaryIdx, msg);
  }
}

bool Replica::sendMessageToAll(const ::google::protobuf::Message& msg) {

  if (signMessages) {
    // ::google::protobuf::Message* copy = msg.New();
    // copy->CopyFrom(msg);
    // transport->DispatchTP([this, copy] {
    //   proto::SignedMessage* signedMsg = new proto::SignedMessage();
    //   SignMessage(*copy, this->keyManager->GetPrivateKey(this->id), this->id, *signedMsg);
    //   delete copy;
    //   return (void*) signedMsg;
    // }, [this](void* ret) {
    //   proto::SignedMessage* signedMsg = (proto::SignedMessage*) ret;
    //   // send to everyone and to me
    //   this->transport->SendMessageToGroup(this, this->groupIdx, *signedMsg);
    //   this->transport->SendMessageToReplica(this, this->groupIdx, this->idx, *signedMsg);
    //   delete signedMsg;
    // });
    // return true;
    proto::SignedMessage signedMsg;
    CreateHMACedMessage(msg, signedMsg);

    return this->transport->SendMessageToGroup(this, groupIdx, signedMsg) &&
           this->transport->SendMessageToReplica(this, groupIdx, idx, signedMsg);
  } else {
    // send to everyone and to me
    return transport->SendMessageToGroup(this, groupIdx, msg) &&
           transport->SendMessageToReplica(this, groupIdx, idx, msg);
  }
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
        if(numShards <= 6 || numShards == 12){
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
        } else {
            // numShards should be 24
            if (numShards != 24)
                Panic("Currently only support numShards == 6, 12 or 24");

            // Debug("Callback: %d, %ld", idx, seqnum);
            stats->Increment("hotstuffpg_exec_callback",1);

            // prepare data structures for executeSlots()
            assert(digest == digest_param);
            requests[digest] = packedMsg;
            replyAddrs[digest] = clientAddr;


            // Shir: TODO fix here as well
            proto::BatchedRequest batchedRequest;
            (*batchedRequest.mutable_digests())[0] = digest_param;
            string batchedDigest = BatchedDigest(batchedRequest);
            batchedRequests[batchedDigest] = batchedRequest;
            pendingExecutions[seqnum] = batchedDigest;
            executeSlots();
        }

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

void Replica::sendBatchedPreprepare() {
  proto::BatchedRequest batchedRequest;
  stats->Increment(bStatNames[pendingBatchedDigests.size()], 1);
  for (const auto& pair : pendingBatchedDigests) {
    (*batchedRequest.mutable_digests())[pair.first] = pair.second;
  }
  pendingBatchedDigests.clear();
  nextBatchNum = 0;
  // send the batched preprepare to everyone
  sendMessageToAll(batchedRequest);

  // send a preprepare for the batched request
  proto::Preprepare preprepare;
  string digest = BatchedDigest(batchedRequest);
  uint64_t seqnum = nextSeqNum++;
  preprepare.set_seqnum(seqnum);
  preprepare.set_viewnum(currentView);
  preprepare.set_digest(digest);

  SendPreprepare(seqnum, preprepare);
}

void Replica::SendPreprepare(uint64_t seqnum, const proto::Preprepare& preprepare) {
  // send preprepare to everyone
  sendMessageToAll(preprepare);

  // wait 300 ms for tx to complete
  seqnumCommitTimers[seqnum] = transport->Timer(2000, [this, seqnum, preprepare]() {
    Debug("Primary commit timer expired, resending preprepare");
    stats->Increment("prim_expired",1);
    this->SendPreprepare(seqnum, preprepare);
  });
}



void Replica::HandleCommit(const TransportAddress &remote,
                              const proto::Commit &commit,
                            const proto::SignedMessage& signedMsg) {
  Debug("Handling commit message");

  if (signMessages) {
    // make sure this message is from this shard
    if (signedMsg.replica_id() / config.n != (uint64_t) groupIdx) {
      stats->Increment("invalid_c_group",1);
      return;
    }
    if (!slots.addCommit(commit, signedMsg.replica_id(), signedMsg.signature())) {
      stats->Increment("invalid_c_hash",1);
      return;
    }
  } else {
    if (!slots.addCommit(commit)) {
      return;
    }
  }

  uint64_t seqnum = commit.seqnum();
  Debug("commit seq num: %lu", seqnum);
  uint64_t viewnum = commit.viewnum();
  string digest = commit.digest();

  testSlot(seqnum, viewnum, digest, false);
}

void Replica::HandleGrouped(const TransportAddress &remote,
                          const proto::GroupedSignedMessage &msg) {

  Debug("Handling grouped message");

  int primaryIdx = config.GetLeaderIndex(currentView);
  // primary should be the one that sent the grouped message
  if (idx != primaryIdx) {
    proto::PackedMessage packedMsg;
    if (packedMsg.ParseFromString(msg.packed_msg())) {
      proto::Prepare prepare;
      proto::Commit commit;

      proto::SignedMessage signedMsg;
      signedMsg.set_packed_msg(msg.packed_msg());

      if (packedMsg.type() == prepare.GetTypeName()) {
        if (prepare.ParseFromString(packedMsg.msg())) {
          for (const auto& pair : msg.signatures()) {
            Debug("ungrouped prepare for %lu", pair.first);
            signedMsg.set_replica_id(pair.first);
            signedMsg.set_signature(pair.second);

            if (!signMessages || CheckSignature(signedMsg, keyManager)) {
              HandlePrepare(remote, prepare, signedMsg);
            } else {
              Debug("Failed to validate prepare signature for %lu", pair.first);
            }
          }
        }
      } else if (packedMsg.type() == commit.GetTypeName()) {
        if (commit.ParseFromString(packedMsg.msg())) {
          for (const auto& pair : msg.signatures()) {
            Debug("ungrouped prepare for %lu", pair.first);
            signedMsg.set_replica_id(pair.first);
            signedMsg.set_signature(pair.second);

            if (!signMessages || CheckSignature(signedMsg, keyManager)) {
              HandleCommit(remote, commit, signedMsg);
            } else {
              Debug("Failed to validate prepare signature for %lu", pair.first);
            }
          }
        }
      }
    }
  }

}

// tests to see if slot can be executed. Called after receiving preprepare
// prepare, and commit because these messages can arrive in arbitrary order
// and commit messages may be received before preprepares on a sufficiently
// odd network
void Replica::testSlot(uint64_t seqnum, uint64_t viewnum, string digest, bool gotPrepare) {
  // wait for 2f prepare + preprepare all matching and then send commit to
  // everyone start timer for 2f+1 commits
  // the gotPrepare just makes it so that commits don't cause commits to be sent
  // which would just loop forever
  if (gotPrepare && slots.Prepared(seqnum, viewnum, config.f)) {
    Debug("Sending commit to everyone");
    cancelActionTimer(seqnum, viewnum, digest);
    startActionTimer(seqnum, viewnum, digest);

    // Multicast commit to everyone
    proto::Commit commit;
    commit.set_seqnum(seqnum);
    commit.set_viewnum(viewnum);
    commit.set_digest(digest);

    if (primaryCoordinator) {
      int primaryIdx = config.GetLeaderIndex(currentView);
      if (idx == primaryIdx) {
        Debug("Sending prepare proof");
        // send prepare proof to all replicas
        proto::GroupedSignedMessage proof = slots.getPrepareProof(seqnum, viewnum, digest);
        sendMessageToAll(proof);
      }

      sendMessageToPrimary(commit);
    } else {
      sendMessageToAll(commit);
    }
  }

  // wait for 2f+1 matching commit messages, then mark message as committed,
  // clear timer
  if (slots.CommittedLocal(seqnum, viewnum, config.f)) {
    Debug("Committed message");
    cancelActionTimer(seqnum, viewnum, digest);

    if (primaryCoordinator) {
      int primaryIdx = config.GetLeaderIndex(currentView);
      if (idx == primaryIdx) {
        Debug("Sending commit proof");
        // send commit proof to all replicas
        proto::GroupedSignedMessage proof = slots.getCommitProof(seqnum, viewnum, digest);
        sendMessageToAll(proof);
      }
    }

    pendingExecutions[seqnum] = digest;

    Debug("seqnum: %lu", seqnum);

    #ifdef USE_HOTSTUFF_STORE
    assert(false);
    #endif

    executeSlots();
  }
}

void Replica::executeSlots(){
  if(false){
    // auto f = [this](){
    //   //std::unique_lock lock(atomicMutex);
    //   this->executeSlots_internal();
    //   return (void*) true;
    // };
    // transport->DispatchTP_main(f);
#ifdef USE_HOTSTUFF_STORE
      assert(false);
#endif

    executeSlots_internal_multi();
  }
  else{
    executeSlots_shir();
    // executeSlots_internal();
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


void Replica::executeSlots_shir() {
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
pa
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


// Shir: ################################################################ PREVIOUS FUNC

void Replica::executeSlots_internal() {
  Debug("exec seq num: %lu", execSeqNum);
  for(auto& it: pendingExecutions) {
    std::cout << it.first << " " << it.second << std::endl;
    Debug("Pending sequence number: %lu", it.first);
    // execSeqNum = it.first; //Just trying to get it to work
  }
  while(pendingExecutions.find(execSeqNum) != pendingExecutions.end()) { //pendingExecutions.find(execSeqNum) != pendingExecutions.end()
    // cancel the commit timer
    Debug("Pending execution exists"); // count=3
    if (seqnumCommitTimers.find(execSeqNum) != seqnumCommitTimers.end()) {
      transport->CancelTimer(seqnumCommitTimers[execSeqNum]);
      seqnumCommitTimers.erase(execSeqNum);
    }

    string batchDigest = pendingExecutions[execSeqNum];

    // std::string digest_b("bubble");
    // if(batchDigest == digest_b) {
    //   std::cerr<<"Calling bubble dummt execute slots" << std::endl;
    //   execSeqNum++;
    //   continue;
    // }
    // only execute when we have the batched request
    Debug("Shir: count before if"); // count=3 (without execute in each bubble),count=15 (with execute in each bubble)

    if (batchedRequests.find(batchDigest) != batchedRequests.end()) {
      Debug("Batched request");
      string digest = (*batchedRequests[batchDigest].mutable_digests())[execBatchNum];
      DebugHash(digest);
      // only execute if we have the full request
      Debug("Shir: count before looking for digest"); // count=1 (without execute in each bubble),count=2 (with execute in each bubble)
      if (requests.find(digest) != requests.end()) {
        stats->Increment("exec_request",1);
        Debug("executing seq num: %lu %lu", execSeqNum, execBatchNum);
        proto::PackedMessage packedMsg = requests[digest];

        if(asyncServer) {
          execBatchNum++;
          Debug("Shir: 333 advancing the exec batch num to %d",execBatchNum);  

          if ((int) execBatchNum >= batchedRequests[batchDigest].digests_size()) {
            Debug("Done executing batch");
            execBatchNum = 0;
            execSeqNum++;
            Debug("Shir: 444 advancing the exec seq num to %d",execSeqNum);

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

                    // HotStuff: disable timer for HotStuff due to concurrency bugs
                    // if (EbatchTimerRunning) {
                    //   transport->CancelTimer(EbatchTimerId);
                    //   EbatchTimerRunning = false;
                    // }
                    sendEbatch();
                  } else if (!EbatchTimerRunning) {
                    EbatchTimerRunning = true;
                    Debug("Starting ebatch timer");
                    // EbatchTimerId = transport->Timer(EbatchTimeoutMS, [this]() {
                    //   Debug("EBatch timer expired, sending");
                    //   this->EbatchTimerRunning = false;
                    //   this->sendEbatch();
                    // });
                  }
                } else {
                  Debug("Invalid execution");
                }
              }

              // execBatchNum++;
              // if ((int) execBatchNum >= batchedRequests[batchDigest].digests_size()) {
              //   Debug("Done executing batch");
              //   execBatchNum = 0;
              //   execSeqNum++;
              // }
            };
            // execute_timeout_callback etcb

            app->Execute_Callback(packedMsg.type(), packedMsg.msg(), ecb);
          });
          
        } else {
          std::cerr<<"Shir: 22222222\n";

          std::vector<::google::protobuf::Message*> replies = app->Execute(packedMsg.type(), packedMsg.msg());

          for (const auto& reply : replies) {
            if (reply != nullptr) {
              Debug("Sending reply");
              stats->Increment("execs_sent",1);
              EpendingBatchedMessages.push_back(reply);
              EpendingBatchedDigs.push_back(digest);
              if (EpendingBatchedMessages.size() >= EbatchSize) {
                Debug("EBatch is full, sending");

                // HotStuff: disable timer for HotStuff due to concurrency bugs
                // if (EbatchTimerRunning) {
                //   transport->CancelTimer(EbatchTimerId);
                //   EbatchTimerRunning = false;
                // }
                sendEbatch();
              } else if (!EbatchTimerRunning) {
                EbatchTimerRunning = true;
                Debug("Starting ebatch timer");
                // EbatchTimerId = transport->Timer(EbatchTimeoutMS, [this]() {
                //   Debug("EBatch timer expired, sending");
                //   this->EbatchTimerRunning = false;
                //   this->sendEbatch();
                // });
              }
            } else {
              Debug("Invalid execution");
            }
          }

          execBatchNum++;
          Debug("Shir: 555 advancing the exec batch num to %d",execBatchNum);  

          Debug("Shir: exec batch num is %d",execBatchNum);
          Debug("Shir:   %d",batchedRequests[batchDigest].digests_size());
          if ((int) execBatchNum >= batchedRequests[batchDigest].digests_size()) {
            Debug("Done executing batch");
            execBatchNum = 0;
            // SHIR: advancing in 8 because of the bubbles?
            execSeqNum=execSeqNum+8;
            // execSeqNum++;
            Debug("Shir: 666 advancing the exec seq num to %d",execSeqNum);  

          }
        }
      } else {
          #ifdef USE_HOTSTUFF_STORE
                  
                  Debug("Outside of requests");

                    stats->Increment("miss_hotstuffpg_req_txn",1);
                    // request resend not implemented for HotStuff
                    break;

          #else

                    Debug("request from batch %lu not yet received", execSeqNum);
                    if (requestTx) {
                        stats->Increment("req_txn",1);
                        proto::RequestRequest rr;
                        rr.set_digest(digest);
                        int primaryIdx = config.GetLeaderIndex(currentView);
                        if (primaryIdx == idx) {
                            stats->Increment("primary_req_txn",1);
                        }
                        transport->SendMessageToReplica(this, groupIdx, primaryIdx, rr);
                    }
                    break;
          #endif
      }
    } else {
          #ifdef USE_HOTSTUFF_STORE
                  Debug("Outside of batched");

                  stats->Increment("miss_hotstuffpg_req_batch",1);
                  // batch resend not implemented for HotStuff
                  break;

          #else

                  Debug("Batch request not yet received");
                  if (requestTx) {
                      stats->Increment("req_batch",1);
                      proto::RequestRequest rr;
                      rr.set_digest(batchDigest);
                      int primaryIdx = config.GetLeaderIndex(currentView);
                      transport->SendMessageToReplica(this, groupIdx, primaryIdx, rr);
                  }
                  break;
          #endif
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


void Replica::HandleBatchedRequest(const TransportAddress &remote, proto::BatchedRequest &request) {
  Panic("Shir:  HandleBatchedRequest");
}

void Replica::HandlePreprepare(const TransportAddress &remote,const proto::Preprepare &preprepare,const proto::SignedMessage& signedMsg) {
  Panic("Shir:  HandlePreprepare");
}

void Replica::HandlePrepare(const TransportAddress &remote,const proto::Prepare &prepare, const proto::SignedMessage& signedMsg) {
  Panic("Shir:  HandlePrepare");
}

void Replica::executeSlots_internal_multi() {
    Panic("Shir:  executeSlots_internal_multi");
}



}  // namespace hotstuffpgstore
