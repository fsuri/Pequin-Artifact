// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/indicusstore/server.cc:
 *   Implementation of a single transactional key-value server.
 *
 * Copyright 2021 Florian Suri-Payer <fsp@cs.cornell.edu>
 *                Matthew Burke <matthelb@cs.cornell.edu>
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


#include "store/indicusstore/server.h"

#include <bitset>
#include <queue>
#include <ctime>
#include <chrono>
#include <sys/time.h>

#include "lib/assert.h"
#include "lib/tcptransport.h"
#include "store/indicusstore/common.h"
#include "store/indicusstore/phase1validator.h"
#include "store/indicusstore/localbatchsigner.h"
#include "store/indicusstore/sharedbatchsigner.h"
#include "store/indicusstore/basicverifier.h"
#include "store/indicusstore/localbatchverifier.h"
#include "store/indicusstore/sharedbatchverifier.h"
#include "lib/batched_sigs.h"
#include <valgrind/memcheck.h>

namespace indicusstore {

Server::Server(const transport::Configuration &config, int groupIdx, int idx,
    int numShards, int numGroups, Transport *transport, KeyManager *keyManager,
    Parameters params, uint64_t timeDelta, OCCType occType, Partitioner *part,
    unsigned int batchTimeoutMicro, TrueTime timeServer) :
    PingServer(transport),
    config(config), groupIdx(groupIdx), idx(idx), numShards(numShards),
    numGroups(numGroups), id(groupIdx * config.n + idx),
    transport(transport), occType(occType), part(part),
    params(params), keyManager(keyManager),
    timeDelta(timeDelta),
    timeServer(timeServer)
     {

  ongoing = ongoingMap(100000);
  p1MetaData = p1MetaDataMap(100000);
  p2MetaDatas = p2MetaDataMap(100000);
  prepared = preparedMap(100000);
  preparedReads = tbb::concurrent_unordered_map<std::string, std::pair<std::shared_mutex, std::set<const proto::Transaction *>>>(100000);
  preparedWrites = tbb::concurrent_unordered_map<std::string, std::pair<std::shared_mutex,std::map<Timestamp, const proto::Transaction *>>>(100000);
  rts = tbb::concurrent_unordered_map<std::string, std::atomic_uint64_t>(100000);
  committed = tbb::concurrent_unordered_map<std::string, proto::CommittedProof *>(100000);
  writebackMessages = tbb::concurrent_unordered_map<std::string, proto::Writeback>(100000);
  dependents = dependentsMap(100000);
  waitingDependencies = std::unordered_map<std::string, WaitingDependency>(100000);

  store.KVStore_Reserve(4200000);
  //Create simulated MACs that are used for Fallback all to all:
  CreateSessionKeys();
  //////

  stats.Increment("total_equiv_received_adopt", 0);

  fprintf(stderr, "Starting Indicus replica. ID: %d, IDX: %d, GROUP: %d\n", id, idx, groupIdx);
  fprintf(stderr, "Sign Client Proposals? %s\n", params.signClientProposals ? "True" : "False");
  Debug("Starting Indicus replica %d.", id);
  transport->Register(this, config, groupIdx, idx);
  _Latency_Init(&committedReadInsertLat, "committed_read_insert_lat");
  _Latency_Init(&verifyLat, "verify_lat");
  _Latency_Init(&signLat, "sign_lat");
  _Latency_Init(&waitingOnLocks, "lock_lat");
  //_Latency_Init(&waitOnProtoLock, "proto_lock_lat");
  //_Latency_Init(&store.storeLockLat, "store_lock_lat");

  //define signer and verifier for replica signatures
  if (params.signatureBatchSize == 1) {
    //verifier = new BasicVerifier(transport);
    verifier = new BasicVerifier(transport, batchTimeoutMicro, params.validateProofs &&
      params.signedMessages && params.adjustBatchSize, params.verificationBatchSize);
    batchSigner = nullptr;
  } else {
    if (params.sharedMemBatches) {
      batchSigner = new SharedBatchSigner(transport, keyManager, GetStats(),
          batchTimeoutMicro, params.signatureBatchSize, id,
          params.validateProofs && params.signedMessages &&
          params.signatureBatchSize > 1 && params.adjustBatchSize,
          params.merkleBranchFactor);
    } else {
      batchSigner = new LocalBatchSigner(transport, keyManager, GetStats(),
          batchTimeoutMicro, params.signatureBatchSize, id,
          params.validateProofs && params.signedMessages &&
          params.signatureBatchSize > 1 && params.adjustBatchSize,
          params.merkleBranchFactor);
    }

    if (params.sharedMemVerify) {
      verifier = new SharedBatchVerifier(params.merkleBranchFactor, stats); //add transport if using multithreading
    } else {
      //verifier = new LocalBatchVerifier(params.merkleBranchFactor, stats, transport);
      verifier = new LocalBatchVerifier(params.merkleBranchFactor, stats, transport,
        batchTimeoutMicro, params.validateProofs && params.signedMessages &&
        params.signatureBatchSize > 1 && params.adjustBatchSize, params.verificationBatchSize);
    }
  }
  //define verifier that handles client signatures -- this can always be a basic verifier.
  client_verifier = new BasicVerifier(transport, batchTimeoutMicro, params.validateProofs &&
      params.signedMessages && params.adjustBatchSize, params.verificationBatchSize);

  // this is needed purely from loading data without executing transactions
  proto::CommittedProof *proof = new proto::CommittedProof();
  proof->mutable_txn()->set_client_id(0);
  proof->mutable_txn()->set_client_seq_num(0);
  proof->mutable_txn()->mutable_timestamp()->set_timestamp(0);
  proof->mutable_txn()->mutable_timestamp()->set_id(0);

  committed.insert(std::make_pair("", proof));
}

Server::~Server() {
  std::cerr << "KVStore size: " << store.KVStore_size() << std::endl;
  std::cerr << "ReadStore size: " << store.ReadStore_size() << std::endl;
  std::cerr << "commitGet count: " << commitGet_count << std::endl;

  std::cerr << "Hash count: " << BatchedSigs::hashCount << std::endl;
  std::cerr << "Hash cat count: " << BatchedSigs::hashCatCount << std::endl;
  std::cerr << "Total count: " << BatchedSigs::hashCount + BatchedSigs::hashCatCount << std::endl;

  std::cerr << "Store wait latency (ms): " << store.lock_time << std::endl;
  std::cerr << "parallel OCC lock wait latency (ms): " << total_lock_time_ms << std::endl;
  Latency_Dump(&waitingOnLocks);
  //Latency_Dump(&waitOnProtoLock);
  //Latency_Dump(&batchSigner->waitOnBatchLock);
  //Latency_Dump(&(store.storeLockLat));
  Notice("Freeing verifier.");
  delete verifier;
   //if(params.mainThreadDispatching) committedMutex.lock();
  for (const auto &c : committed) {   ///XXX technically not threadsafe
    delete c.second;
  }
   //if(params.mainThreadDispatching) committedMutex.unlock();
   ////if(params.mainThreadDispatching) ongoingMutex.lock();
  for (const auto &o : ongoing) {     ///XXX technically not threadsafe
    //delete o.second;
    delete o.second.txn;
  }
   ////if(params.mainThreadDispatching) ongoingMutex.unlock();
   if(params.mainThreadDispatching) readReplyProtoMutex.lock();
  for (auto r : readReplies) {
    delete r;
  }
   if(params.mainThreadDispatching) readReplyProtoMutex.unlock();
   if(params.mainThreadDispatching) p1ReplyProtoMutex.lock();
  for (auto r : p1Replies) {
    delete r;
  }
   if(params.mainThreadDispatching) p1ReplyProtoMutex.unlock();
   if(params.mainThreadDispatching) p2ReplyProtoMutex.lock();
  for (auto r : p2Replies) {
    delete r;
  }
   if(params.mainThreadDispatching) p2ReplyProtoMutex.unlock();
  Notice("Freeing signer.");
  if (batchSigner != nullptr) {
    delete batchSigner;
  }
  Latency_Dump(&verifyLat);
  Latency_Dump(&signLat);

}

 void PrintSendCount(){
   send_count++;
   fprintf(stderr, "send count: %d \n", send_count);
 }

 void PrintRcvCount(){
   rcv_count++;
   fprintf(stderr, "rcv count: %d\n", rcv_count);
 }

 void ParseProto(::google::protobuf::Message *msg, std::string &data){
   msg->ParseFromString(data);
 }
// use like this: main dispatches lambda
// auto f = [this, msg, type, data](){
//   ParseProto;
//   auto g = [this, msg, type](){
//     this->HandleType(msg, type);  //HandleType checks which handler to use...
//   };
//   this->transport->DispatchTP_main(g);
//   return (void*) true;
// };
// DispatchTP_noCB(f);
// Main dispatches serialization, and lets serialization thread dispatch to main2

//Upcall from Network layer with new message. Called by TCPReadableCallback(..)
 void Server::ReceiveMessage(const TransportAddress &remote,
       const std::string &type, const std::string &data, void *meta_data) {

  if(params.dispatchMessageReceive){
    Debug("Dispatching message handling to Support Main Thread");
    //using this path results in an extra copy
    //Can I move the data or release the message to avoid duplicates?
   transport->DispatchTP_main([this, &remote, type, data, meta_data]() {
     this->ReceiveMessageInternal(remote, type, data, meta_data);
     return (void*) true;
   });
  }
  else{
    ReceiveMessageInternal(remote, type, data, meta_data);
  }
 }

//Calls function handlers for respective message types.
// ManageDsipatch_ Manages respective Multithread assignments
void Server::ReceiveMessageInternal(const TransportAddress &remote,
      const std::string &type, const std::string &data, void *meta_data) {

  if (type == read.GetTypeName()) {
    ManageDispatchRead(remote, data);

  // Normal Protocol Messages
  } else if (type == phase1.GetTypeName()) {
    ManageDispatchPhase1(remote, data);

  } else if (type == phase2.GetTypeName()) {
    ManageDispatchPhase2(remote, data);

  } else if (type == writeback.GetTypeName()) {
    ManageDispatchWriteback(remote, data);

  } else if (type == abort.GetTypeName()) {
    // abort.ParseFromString(data);
    // HandleAbort(remote, abort);
    ManageDispatchAbort(remote, data);
  }
  else if (type == ping.GetTypeName()) {
    ping.ParseFromString(data);
    Debug("Ping is called");
    HandlePingMessage(this, remote, ping);

  // Fallback Messages
  } else if (type == phase1FB.GetTypeName()) {
    ManageDispatchPhase1FB(remote, data);

  } else if (type == phase2FB.GetTypeName()) {
    ManageDispatchPhase2FB(remote, data);

  } else if (type == invokeFB.GetTypeName()) {
    ManageDispatchInvokeFB(remote, data);

  } else if (type == electFB.GetTypeName()) {
    ManageDispatchElectFB(remote, data);

  } else if (type == decisionFB.GetTypeName()) {
    ManageDispatchDecisionFB(remote, data);

  } else if (type == moveView.GetTypeName()) {
    ManageDispatchMoveView(remote, data);

  } else {
    Panic("Received unexpected message type: %s", type.c_str());
  }
}

//Adds new key-value store entry
//Used for initialization (loading) the key-value store contents at startup.
//Debug("Stuck at line %d", __LINE__);
void Server::Load(const std::string &key, const std::string &value,
    const Timestamp timestamp) {
  Value val;
  val.val = value;
  auto committedItr = committed.find("");
  UW_ASSERT(committedItr != committed.end());
  val.proof = committedItr->second;
  store.put(key, val, timestamp);
  if (key.length() == 5 && key[0] == 0) {
    std::cerr << std::bitset<8>(key[0]) << ' '
              << std::bitset<8>(key[1]) << ' '
              << std::bitset<8>(key[2]) << ' '
              << std::bitset<8>(key[3]) << ' '
              << std::bitset<8>(key[4]) << ' '
              << std::endl;
  }
}

//Handle Read Message
//Dispatches to reader thread if params.parallel_reads = true, and multithreading enabled
//Returns a signed message including i) the latest committed write (+ cert), and ii) the latest prepared write (both w.r.t to Timestamp of reader)
void Server::HandleRead(const TransportAddress &remote,
     proto::Read &msg) {

  Debug("READ[%lu:%lu] for key %s with ts %lu.%lu.", msg.timestamp().id(),
      msg.req_id(), BytesToHex(msg.key(), 16).c_str(),
      msg.timestamp().timestamp(), msg.timestamp().id());
  Timestamp ts(msg.timestamp());
  if (CheckHighWatermark(ts)) {
    // ignore request if beyond high watermark
    Debug("Read timestamp beyond high watermark.");
    if(params.mainThreadDispatching && (!params.dispatchMessageReceive || params.parallel_reads)) FreeReadmessage(&msg);
    return;
  }

  std::pair<Timestamp, Server::Value> tsVal;
  //find committed write value to read from
  bool committed_exists = store.get(msg.key(), ts, tsVal);

  proto::ReadReply* readReply = GetUnusedReadReply();
  readReply->set_req_id(msg.req_id());
  readReply->set_key(msg.key());
  if (committed_exists) {
    //if(tsVal.first > ts) Panic("Should not read committed value with larger TS than read");
    Debug("READ[%lu:%lu] Committed value of length %lu bytes with ts %lu.%lu.",
        msg.timestamp().id(), msg.req_id(), tsVal.second.val.length(), tsVal.first.getTimestamp(),
        tsVal.first.getID());
    readReply->mutable_write()->set_committed_value(tsVal.second.val);
    tsVal.first.serialize(readReply->mutable_write()->mutable_committed_timestamp());
    if (params.validateProofs) {
      *readReply->mutable_proof() = *tsVal.second.proof;
    }
  }

  TransportAddress *remoteCopy = remote.clone();


  //auto sendCB = [this, remoteCopy, readReply, c_id = msg.timestamp().id(), req_id=msg.req_id()]() {
  //Debug("Sent ReadReply[%lu:%lu]", c_id, req_id);  
  auto sendCB = [this, remoteCopy, readReply]() {
    this->transport->SendMessage(this, *remoteCopy, *readReply);
    delete remoteCopy;
    FreeReadReply(readReply);
  };

  //If MVTSO: Read prepared, Set RTS
  if (occType == MVTSO) {
  
    //Sets RTS timestamp. Favors readers commit chances.
    //Disable if worried about Byzantine Readers DDos, or if one wants to favor writers.
    if(params.rtsMode == 1){
      Debug("Set up RTS for READ[%lu:%lu]", msg.timestamp().id(), msg.req_id());
     auto itr = rts.find(msg.key());
     if(itr != rts.end()){
       if(ts.getTimestamp() > itr->second ) {
         rts[msg.key()] = ts.getTimestamp();
       }
     }
     else{
       rts[msg.key()] = ts.getTimestamp();
     }
     /* update rts */
    // TODO: For "proper Aborts": how to track RTS by transaction without knowing transaction digest?
    }
    else if(params.rtsMode == 2){
      //XXX multiple RTS as set:
      Debug("Set up RTS for READ[%lu:%lu]", msg.timestamp().id(), msg.req_id());
      std::pair<std::shared_mutex, std::set<Timestamp>> &rts_set = rts_list[msg.key()];
      {
        std::unique_lock lock(rts_set.first);
        rts_set.second.insert(ts);
      }
    }
    else{
      //No RTS
    }
      

    //find prepared write to read from
    /* add prepared deps */
    if (params.maxDepDepth > -2) {
      Debug("Look for prepared value to READ[%lu:%lu]", msg.timestamp().id(), msg.req_id());
      const proto::Transaction *mostRecent = nullptr;

      //std::pair<std::shared_mutex,std::map<Timestamp, const proto::Transaction *>> &x = preparedWrites[write.key()];
      auto itr = preparedWrites.find(msg.key());
      if (itr != preparedWrites.end()){

        //std::pair &x = preparedWrites[write.key()];
        std::shared_lock lock(itr->second.first);
        if(itr->second.second.size() > 0) {

          // //Find biggest prepared write smaller than TS.
          // auto it = itr->second.second.lower_bound(ts); //finds smallest element greater equal TS
          // if(it != itr->second.second.begin()) { //If such an elem exists; go back one == greates element less than TS
          //     --it;
          // }
          // if(it != itr->second.second.begin()){ //if such elem exists: read from it.
          //     mostRecent = it->second;
          //     if(exists && tsVal.first > Timestamp(mostRecent->timestamp())) mostRecent = nullptr; // don't include prepared read if it is smaller than committed.
          // }

          // there is a prepared write for the key being read
          for (const auto &t : itr->second.second) {
            if(t.first > ts) break; //only consider it if it is smaller than TS (Map is ordered, so break should be fine here.)
            if(committed_exists && t.first <= tsVal.first) continue; //only consider it if bigger than committed value. 
            if (mostRecent == nullptr || t.first > Timestamp(mostRecent->timestamp())) { 
              mostRecent = t.second;
            }
          }

          if (mostRecent != nullptr) {
            //if(Timestamp(mostRecent->timestamp()) > ts) Panic("Reading prepared write with TS larger than read ts");
            std::string preparedValue;
            for (const auto &w : mostRecent->write_set()) {
              if (w.key() == msg.key()) {
                preparedValue = w.value();
                break;
              }
            }

            Debug("Prepared write with most recent ts %lu.%lu.",
                mostRecent->timestamp().timestamp(), mostRecent->timestamp().id());
            //std::cerr << "Dependency depth: " << (DependencyDepth(mostRecent)) << std::endl;
            if (params.maxDepDepth == -1 || DependencyDepth(mostRecent) <= params.maxDepDepth) {
              readReply->mutable_write()->set_prepared_value(preparedValue);
              *readReply->mutable_write()->mutable_prepared_timestamp() = mostRecent->timestamp();
              *readReply->mutable_write()->mutable_prepared_txn_digest() = TransactionDigest(*mostRecent, params.hashDigest);
            }
          }
        }
      }
    }
  }


  if (params.validateProofs && params.signedMessages &&
      (readReply->write().has_committed_value() || (params.verifyDeps && readReply->write().has_prepared_value()))) { //remove params.verifyDeps requirement to sign prepared. Not sure if it causes a bug so I kept it for now -- realistically never triggered
    Debug("Sign Read Reply for READ[%lu:%lu]", msg.timestamp().id(), msg.req_id());
//If readReplyBatch is false then respond immediately, otherwise respect batching policy
    if (params.readReplyBatch) {
      proto::Write* write = new proto::Write(readReply->write());
      // move: sendCB = std::move(sendCB) or {std::move(sendCB)}
      MessageToSign(write, readReply->mutable_signed_write(), [sendCB, write]() {
        sendCB();
        delete write;
      });

    } else if (params.signatureBatchSize == 1) {

      if(params.multiThreading){
        proto::Write* write = new proto::Write(readReply->write());
        auto f = [this, readReply, sendCB = std::move(sendCB), write]()
        {
          SignMessage(write, keyManager->GetPrivateKey(id), id, readReply->mutable_signed_write());
          sendCB();
          delete write;
          return (void*) true;
        };
        transport->DispatchTP_noCB(std::move(f));
      }
      else{
        proto::Write write(readReply->write());
        SignMessage(&write, keyManager->GetPrivateKey(id), id,
            readReply->mutable_signed_write());
        sendCB();
      }

    } else {

      if(params.multiThreading){

        std::vector<::google::protobuf::Message *> msgs;
        proto::Write* write = new proto::Write(readReply->write()); //TODO might want to add re-use buffer
        msgs.push_back(write);
        std::vector<proto::SignedMessage *> smsgs;
        smsgs.push_back(readReply->mutable_signed_write());

        auto f = [this, msgs, smsgs, sendCB = std::move(sendCB), write]()
        {
          SignMessages(msgs, keyManager->GetPrivateKey(id), id, smsgs, params.merkleBranchFactor);
          sendCB();
          delete write;
          return (void*) true;
        };
        transport->DispatchTP_noCB(std::move(f));
      }
      else{
        proto::Write write(readReply->write());
        std::vector<::google::protobuf::Message *> msgs;
        msgs.push_back(&write);
        std::vector<proto::SignedMessage *> smsgs;
        smsgs.push_back(readReply->mutable_signed_write());
        SignMessages(msgs, keyManager->GetPrivateKey(id), id, smsgs, params.merkleBranchFactor);
        sendCB();
      }
    }
  } else {
    sendCB();
  }
  if(params.mainThreadDispatching && (!params.dispatchMessageReceive || params.parallel_reads)) FreeReadmessage(&msg);
}

//////////////////////

//DEPRECATED
//Optional Code Handler in case one wants to parallelize P1 handling as well. Currently deprecated (possibly not working)
//Skip to HandlePhase1(...)
void Server::HandlePhase1_atomic(const TransportAddress &remote,
    proto::Phase1 &msg) {

  //atomic_testMutex.lock();
  std::string txnDigest = TransactionDigest(msg.txn(), params.hashDigest); //could parallelize it too hypothetically
  Debug("PHASE1[%lu:%lu][%s] with ts %lu.", msg.txn().client_id(),
      msg.txn().client_seq_num(), BytesToHex(txnDigest, 16).c_str(),
      msg.txn().timestamp().timestamp());

  proto::Transaction *txn = msg.release_txn();

  //NOTE: Ongoing *must* be added before p2/wb since the latter dont include it themselves as an optimization
  //TCP guarantees that this happens, but I cannot dispatch parallel P1 before logging ongoing or else it could be ordered after p2/wb.
  ongoingMap::accessor b;
  //ongoing.insert(b, std::make_pair(txnDigest, txn));
  ongoing.insert(b, txnDigest);
  b->second.txn = txn;
  b->second.num_concurrent_clients++;
  b.release();

  //NOTE: "Problem": If client comes after writeback, it will try to do a p2/wb itself but fail
  //due to ongoing being removed already.
  //XXX solution: add to ongoing always, and call Clean() for redundant Writebacks as well.
  //XXX alternative solution: Send a Forward Writeback message for normal path clients too.
          // make sure that Commit+Clean is atomic; and that the check for commit and remainder is atomic.
            // use overarching lock on ongoing.

  if(params.parallel_CCC){
    TransportAddress* remoteCopy = remote.clone();
    auto f = [this, remoteCopy, p1 = &msg, txn, txnDigest]() mutable {
        this->ProcessPhase1_atomic(*remoteCopy, *p1, txn, txnDigest);
        delete remoteCopy;
        return (void*) true;
      };
      transport->DispatchTP_noCB(std::move(f));
  }
  else{
    ProcessPhase1_atomic(remote, msg, txn, txnDigest);
  }
}

void Server::ProcessPhase1_atomic(const TransportAddress &remote,
    proto::Phase1 &msg, proto::Transaction *txn, std::string &txnDigest){

  proto::ConcurrencyControl::Result result;
  const proto::CommittedProof *committedProof;
  const proto::Transaction *abstain_conflict = nullptr;
      //NOTE : make sure c and b dont conflict on lock order
  p1MetaDataMap::accessor c;
  bool hasP1 = p1MetaData.find(c, txnDigest);
  // p1MetaData.insert(c, txnDigest);
  // bool hasP1 = c->second.hasP1;
  // no-replays property, i.e. recover existing decision/result from storage
  //Ignore duplicate requests that are already committed, aborted, or ongoing
  if(hasP1){
    result = c->second.result;

    if(result == proto::ConcurrencyControl::WAIT){
        ManageDependencies(txnDigest, *txn, remote, msg.req_id());
    }
    else if (result == proto::ConcurrencyControl::ABORT) {
      committedProof = c->second.conflict;
      UW_ASSERT(committedProof != nullptr);
    }

  } else{

    if (params.validateProofs && params.signedMessages && params.verifyDeps) {
      for (const auto &dep : txn->deps()) {
        if (!dep.has_write_sigs()) {
          Debug("Dep for txn %s missing signatures.",
              BytesToHex(txnDigest, 16).c_str());
          if((params.mainThreadDispatching && (!params.dispatchMessageReceive || params.parallel_CCC)) || (params.multiThreading && params.signClientProposals)) FreePhase1message(&msg);
          return;
        }
        if (!ValidateDependency(dep, &config, params.readDepSize, keyManager,
              verifier)) {
          Debug("VALIDATE Dependency failed for txn %s.",
              BytesToHex(txnDigest, 16).c_str());
          // safe to ignore Byzantine client
          if((params.mainThreadDispatching && (!params.dispatchMessageReceive || params.parallel_CCC)) || (params.multiThreading && params.signClientProposals)) FreePhase1message(&msg);
          return;
        }
      }
    }
    //c.release();
    //current_views[txnDigest] = 0;
    p2MetaDataMap::accessor p;
    p2MetaDatas.insert(p, txnDigest);
    p.release();
    //p2MetaDatas.insert(std::make_pair(txnDigest, P2MetaData()));

    Timestamp retryTs;

    result = DoOCCCheck(msg.req_id(), remote, txnDigest, *txn, retryTs,
          committedProof, abstain_conflict);

    BufferP1Result(c, result, committedProof, txnDigest);

  }
  c.release();
  //atomic_testMutex.unlock();
  HandlePhase1CB(&msg, result, committedProof, txnDigest, remote, abstain_conflict, false);
}

//Helper function that forwards a received P1 message to a neighboring replica. Only does so if param.replica_gossip = true
//Goal: Increases robustness against Byzantine Clients that only send P1 to some replicas. 
//      This can cause concurrent transactions to miss forming dependencies, but later have to abort due to the conflict.
void Server::ForwardPhase1(proto::Phase1 &msg){
  //std::vector<uint64_t> recipients;
  msg.set_replica_gossip(true);
  //Optionally send to f+1 neighboring replicas instead of 1 for increased robustness
  // for(int i = 1; i <= config.f+1; ++i){
  //   uint64_t nextReplica = (idx + i) % config.n;
  //   transport->SendMessageToReplica(this, groupIdx, nextReplica, msg);
  // }

  uint64_t nextReplica = (idx + 1) % config.n;
  transport->SendMessageToReplica(this, groupIdx, nextReplica, msg);
}

//Helper function to elect a replica leader responsible for completing the commit protocol of a tx if no client leader finishes it 
//Actual garbage collection (finishing commit protocol NOT currently implemented)
void Server::Inform_P1_GC_Leader(proto::Phase1Reply &reply, proto::Transaction &txn, std::string &txnDigest, int64_t grpLeader){ //default -1
  int64_t logGrp = GetLogGroup(txn, txnDigest);
  if(grpLeader == -1) grpLeader = txnDigest[0] % config.n; //Default *first* leader,
  transport->SendMessageToReplica(this, logGrp, grpLeader,reply);
}

//Handler for Phase1 message
//Manages ongoing transactions
//Executes Concurrency Control Check
//Replies with signed Phase1Reply voting on whether to Commit/Abort the transaction
//NOTE: Current Client Implementation in the Normal Case is "non-adaptive", i.e. it always expects a P1Reply, and cannot handle a P2Reply or WritebackAck (e.g. if the replica already has committed the tx). 
                                                                           // Optimize this in future iterations.
void Server::HandlePhase1(const TransportAddress &remote,
    proto::Phase1 &msg) {
  //dummyTx = msg.txn(); //PURELY TESTING PURPOSES!!: NOTE WARNING
 
 Debug("Received Phase1 message");
  proto::Transaction *txn;
  if(params.signClientProposals){
    txn = new proto::Transaction();
     txn->ParseFromString(msg.signed_txn().data());
      //  Alternatively change proto to contain txn in addition to signed bytes: its easier if it contains both the TX + the signed bytes.
          //      Tradeoff: deserialization cost vs double bytes...
          // In remainder of code: Signature is on serialized message, not on hash. That is easier than defining our own hash function for all messages. Since we compute the hash here its a bit redundant but thats okay.
  }
  else{
     txn = msg.mutable_txn();
  }
  
  std::string txnDigest = TransactionDigest(*txn, params.hashDigest); //could parallelize it too hypothetically
  if(params.signClientProposals) *txn->mutable_txndigest() = txnDigest; //Hack to have access to txnDigest inside TXN later (used for abstain conflict)
  

  Debug("PHASE1[%lu:%lu][%s] with ts %lu.", txn->client_id(),
      txn->client_seq_num(), BytesToHex(txnDigest, 16).c_str(),
      txn->timestamp().timestamp());
  proto::ConcurrencyControl::Result result;
  const proto::CommittedProof *committedProof;
  const proto::Transaction *abstain_conflict = nullptr;

  if(msg.has_crash_failure() && msg.crash_failure()){
    stats.Increment("total_crash_received", 1);
  }
  //KEEP track of interested client //TODO: keep track of original client? 
  //--> Not doing this currently. Instead: We let the original client always do its P1/P2/Writeback processing
  //                                       even if it is possibly redundant. The current code does this for simplicity,
  //                                       but it can be updated in the future
          // interestedClientsMap::accessor i;
          // bool interestedClientsItr = interestedClients.insert(i, txnDigest);
          // i->second.insert(remote.clone());
          // i.release();
          // //interestedClients[txnDigest].insert(remote.clone());

  
  bool isGossip = msg.replica_gossip(); //Check if P1 was forwarded by another replica.

// no-replays property, i.e. recover existing decision/result from storage
  //Ignore duplicate requests that are already committed, aborted, or ongoing
  p1MetaDataMap::const_accessor c;
  // p1MetaData.insert(c, txnDigest);  //TODO: next: make P1 part of ongoing? same for P2?
  //p1MetaData.find(c, txnDigest);
  bool hasP1result = p1MetaData.find(c, txnDigest) ? c->second.hasP1 : false;
  if(hasP1result && isGossip){  // If P1 has already been received and current message is a gossip one, do nothing.
    //Do not need to reply to forwarded P1. If adding replica GC -> want to forward it to leader.
    //Inform_P1_GC_Leader(proto::Phase1Reply &reply, proto::Transaction &txn, std::string &txnDigest, int64_t grpLeader);
    Debug("P1 message for txn[%s] received is of type Gossip, and P1 has already been received", BytesToHex(txnDigest, 16).c_str());
    c.release();
    if(params.signClientProposals) delete txn;
    if((params.mainThreadDispatching && (!params.dispatchMessageReceive || params.parallel_CCC)) || (params.multiThreading && params.signClientProposals)) FreePhase1message(&msg);
    return;
  } 
  else if(hasP1result){ // If P1 has already been received (sent by a fallback) and current message is from original client, then only inform client of blocked dependencies so it can issue fallbacks of its own
    result = c->second.result;
    // need to check if result is WAIT: if so, need to add to waitingDeps original client..
        //(TODO) Instead: use original client list and store pairs <txnDigest, <reqID, remote>>
    if(result == proto::ConcurrencyControl::WAIT){
        ManageDependencies(txnDigest, *txn, remote, msg.req_id());
    }

    if (result == proto::ConcurrencyControl::ABORT) {
      committedProof = c->second.conflict;
      UW_ASSERT(committedProof != nullptr);
    }
    c.release();
    Debug("P1 message for txn[%s] received is of type Normal, and P1 has already been received with result %d", BytesToHex(txnDigest, 16).c_str(), result);
    HandlePhase1CB(&msg, result, committedProof, txnDigest, remote, abstain_conflict, isGossip); // Reply directly without doing 
    if(params.signClientProposals) delete txn;
  } 
  else if(committed.find(txnDigest) != committed.end()){ //has already committed Txn
      Debug("Already committed txn[%s]. Replying with result %d", BytesToHex(txnDigest, 16).c_str(), 0);
      c.release();
      SendPhase1Reply(msg.req_id(), proto::ConcurrencyControl::COMMIT, nullptr, txnDigest, &remote, nullptr); //TODO: Eventually update to send direct WritebackAck
      if((params.mainThreadDispatching && (!params.dispatchMessageReceive || params.parallel_CCC)) || (params.multiThreading && params.signClientProposals)) FreePhase1message(&msg);
      if(params.signClientProposals) delete txn;
  } 
  else if(aborted.find(txnDigest) != aborted.end()){ //has already aborted Txn
      c.release();
       Debug("Already committed txn[%s]. Replying with result %d", BytesToHex(txnDigest, 16).c_str(), 1);
      SendPhase1Reply(msg.req_id(), proto::ConcurrencyControl::ABSTAIN, nullptr, txnDigest, &remote, nullptr); //TODO: Eventually update to send direct WritebackAck
     if((params.mainThreadDispatching && (!params.dispatchMessageReceive || params.parallel_CCC)) || (params.multiThreading && params.signClientProposals)) FreePhase1message(&msg);
     if(params.signClientProposals) delete txn;

  } 
  else{ // FIRST P1 request received (i.e. from original client). Gossip if desired and check whether dependencies are valid
    c.release();
    if(params.replicaGossip) ForwardPhase1(msg); //If params.replicaGossip is enabled then set msg.replica_gossip to true and forward.
    if(!isGossip) msg.set_replica_gossip(false); //unset msg.replica_gossip (which we possibly just set to foward) if the message was received by the client

    //TODO: DispatchTP_noCB(Verify Client Proposals) 
    // Verification calls DispatchTP_main (ProcessProposal.) -- Re-cecheck hasP1 (if used multithread branch)
    
    Debug("P1 message for txn[%s] received is of type Normal, no P1 result exist. Calling ProcessProposal", BytesToHex(txnDigest, 16).c_str());
    ProcessProposal(msg, remote, txn, txnDigest, committedProof, abstain_conflict, isGossip, result);
  }
  //HandlePhase1CB(&msg, result, committedProof, txnDigest, remote, abstain_conflict, isGossip);
}

//Called after Concurrency Control Check completes
//Sends P1Reply to client. Sends no reply if P1 receives was simply forwarded by another replica.
//TODO: move p1Decision into this function (not sendp1: Then, can unlock here.)
void Server::HandlePhase1CB(proto::Phase1 *msg, proto::ConcurrencyControl::Result result,
  const proto::CommittedProof* &committedProof, std::string &txnDigest, const TransportAddress &remote, const proto::Transaction *abstain_conflict, bool isGossip){

  if (result != proto::ConcurrencyControl::WAIT && !isGossip) { //forwarded P1 needs no reply.
    //XXX setting client time outs for Fallback
    // if(client_starttime.find(txnDigest) == client_starttime.end()){
    //   struct timeval tv;
    //   gettimeofday(&tv, NULL);
    //   uint64_t start_time = (tv.tv_sec*1000000+tv.tv_usec)/1000;  //in miliseconds
    //   client_starttime[txnDigest] = start_time;
    // }//time(NULL); //TECHNICALLY THIS SHOULD ONLY START FOR THE ORIGINAL CLIENT, i.e. if another client manages to do it first it shouldnt count... Then again, that client must have gotten it somewhere, so the timer technically started.

    SendPhase1Reply(msg->req_id(), result, committedProof, txnDigest, &remote, abstain_conflict);
  }
  if((params.mainThreadDispatching && (!params.dispatchMessageReceive || params.parallel_CCC)) || (params.multiThreading && params.signClientProposals)) FreePhase1message(msg);
}

void Server::SendPhase1Reply(uint64_t reqId,
    proto::ConcurrencyControl::Result result,
    const proto::CommittedProof *conflict, const std::string &txnDigest,
    const TransportAddress *remote,
    const proto::Transaction *abstain_conflict) {

  Debug("Normal sending P1 result:[%d] for txn: %s", result, BytesToHex(txnDigest, 16).c_str());
  //BufferP1Result(result, conflict, txnDigest);

  proto::Phase1Reply* phase1Reply = GetUnusedPhase1Reply();
  phase1Reply->set_req_id(reqId);
  TransportAddress *remoteCopy = remote->clone();

      //if(result == proto::ConcurrencyControl::ABSTAIN) *phase1Reply->mutable_abstain_conflict() = dummyTx; //NOTE WARNING: PURELY for testing
  if(abstain_conflict != nullptr){
    //Panic("setting abstain_conflict");
    //phase1Reply->mutable_abstain_conflict()->set_req_id(0);
    if(params.signClientProposals){
      p1MetaDataMap::accessor c;
      bool p1MetaExists = p1MetaData.find(c, abstain_conflict->txndigest());
      if(p1MetaExists && c->second.hasSignedP1){ //only send if conflicting tx not yet finished
        phase1Reply->mutable_abstain_conflict()->set_req_id(0);
        *phase1Reply->mutable_abstain_conflict()->mutable_signed_txn() = *c->second.signed_txn;
      } 
      c.release();
    }
    else{ //TODO: ideally also check if ongoing still exists, and only send conflict if so.
      phase1Reply->mutable_abstain_conflict()->set_req_id(0);
      *phase1Reply->mutable_abstain_conflict()->mutable_txn() = *abstain_conflict;
    }
    // *phase1Reply->mutable_abstain_conflict() = *abstain_conflict;
    
 }

  auto sendCB = [remoteCopy, this, phase1Reply]() {
    this->transport->SendMessage(this, *remoteCopy, *phase1Reply);
    FreePhase1Reply(phase1Reply);
    delete remoteCopy;
  };

  phase1Reply->mutable_cc()->set_ccr(result);
  if (params.validateProofs) {
    *phase1Reply->mutable_cc()->mutable_txn_digest() = txnDigest;
    phase1Reply->mutable_cc()->set_involved_group(groupIdx);
    if (result == proto::ConcurrencyControl::ABORT) {
      *phase1Reply->mutable_cc()->mutable_committed_conflict() = *conflict;
    } else if (params.signedMessages) {
      proto::ConcurrencyControl* cc = new proto::ConcurrencyControl(phase1Reply->cc());
      //Latency_Start(&signLat);
      Debug("PHASE1[%s] Batching Phase1Reply.",
            BytesToHex(txnDigest, 16).c_str());

      MessageToSign(cc, phase1Reply->mutable_signed_cc(),
        [sendCB, cc, txnDigest, this, phase1Reply]() {
          Debug("PHASE1[%s] Sending Phase1Reply with signature %s from priv key %lu.",
            BytesToHex(txnDigest, 16).c_str(),
            BytesToHex(phase1Reply->signed_cc().signature(), 100).c_str(),
            phase1Reply->signed_cc().process_id());

          sendCB();
          delete cc;
        });
      //Latency_End(&signLat);
      return;
    }
  }
  sendCB();
}


////////////////////////// Phase2 Handling

//TODO: ADD AUTHENTICATION IN ORDER TO ENFORCE FB TIMEOUTS. ANYBODY THAT IS NOT THE ORIGINAL CLIENT SHOULD ONLY BE ABLE TO SEND P2FB messages and NOT normal P2!!!!!!
//     if(params.clientAuthenticated){ //TODO: this branch needs to go around the validate Proofs.
//       if(!msg.has_auth_p2dec()){
//         Debug("PHASE2 message not authenticated");
//         return;
//       }
//       proto::Phase2ClientDecision p2cd;
//       p2cd.ParseFromString(msg.auth_p2dec().data());
//       //TODO: the parsing of txn and txn_digest
//
//       if(msg.auth_p2dec().process_id() != txn.client_id()){
//         Debug("PHASE2 message from unauthenticated client");
//         return;
//       }
//       //Todo: Get client key... Currently unimplemented. Clients not authenticated.
//       crypto::Verify(clientPublicKey, &msg.auth_p2dec().data()[0], msg.auth_p2dec().data().length(), &msg.auth_p2dec().signature()[0]);
//
//
//       //TODO: 1) check that message is signed
//       //TODO: 2) check if id matches txn id. Access
//       //TODO: 3) check whether signature validates.
//     }
//     else{
//
//     }

//Handler for Phase2
//Verifies correctness of Phase2 message (quorum signatures of p1 replies) and sends P2 reply
//Dispatches verification to worker threads if multiThreading enabled.
void Server::HandlePhase2(const TransportAddress &remote,
       proto::Phase2 &msg) {

//  std::cerr << "Received Phase2 msg with special id: " << msg.req_id() << std::endl;

  const proto::Transaction *txn;
  std::string computedTxnDigest;
  const std::string *txnDigest = &computedTxnDigest;
  if (params.validateProofs) {
    if (!msg.has_txn() && !msg.has_txn_digest()) {
      Debug("PHASE2 message contains neither txn nor txn_digest.");
      return;
    }

    if (msg.has_txn_digest() ) {
      txnDigest = &msg.txn_digest();
    } else {
      txn = &msg.txn();
      computedTxnDigest = TransactionDigest(msg.txn(), params.hashDigest);
      txnDigest = &computedTxnDigest;
    }

  }
  else{
    txnDigest = &dummyString;  //just so I can run with validate turned off while using params.mainThreadDispatching
  }

  proto::Phase2Reply* phase2Reply = GetUnusedPhase2Reply();
  TransportAddress *remoteCopy = remote.clone();

  auto sendCB = [this, remoteCopy, phase2Reply, txnDigest]() {
    this->transport->SendMessage(this, *remoteCopy, *phase2Reply);
    Debug("PHASE2[%s] Sent Phase2Reply.", BytesToHex(*txnDigest, 16).c_str());
    //std::cerr << "Send Phase2Reply for special Id: " << phase2Reply->req_id() << std::endl;
    FreePhase2Reply(phase2Reply);
    delete remoteCopy;
  };
  auto cleanCB = [this, remoteCopy, phase2Reply]() {
    FreePhase2Reply(phase2Reply);
    delete remoteCopy;
  };

  phase2Reply->set_req_id(msg.req_id());
  *phase2Reply->mutable_p2_decision()->mutable_txn_digest() = *txnDigest;
  phase2Reply->mutable_p2_decision()->set_involved_group(groupIdx);

// if (!(params.validateProofs && params.signedMessages)){
//         //TransportAddress *remoteCopy2 = remote.clone();
//           phase2Reply->mutable_p2_decision()->set_decision(msg.decision());
//           SendPhase2Reply(&msg, phase2Reply, sendCB);
//           //HandlePhase2CB(remoteCopy2, &msg, txnDigest, sendCB, phase2Reply, cleanCB, (void*) true);
//           return;
//         }

//   // ELSE: i.e. if (params.validateProofs && params.signedMessages)

  // no-replays property, i.e. recover existing decision/result from storage (do this for HandlePhase1 as well.)
  p2MetaDataMap::accessor p;
  p2MetaDatas.insert(p, *txnDigest);
  bool hasP2 = p->second.hasP2;
  //NOTE: If Tx already went P2 or is committed/aborted: Just re-use that decision + view.
  //Since we currently do not garbage collect P2: anything in committed/aborted but not in metaP2 must have gone FastPath,
  //so choosing view=0 is ok, since equivocation/split-decisions cannot be possible.
  if(hasP2){
      //p.release();
      //std::cerr << "Already have P2 for special id: " << msg.req_id() << std::endl;
      phase2Reply->mutable_p2_decision()->set_decision(p->second.p2Decision);
      if (params.validateProofs) {
        phase2Reply->mutable_p2_decision()->set_view(p->second.decision_view);
      }
      //NOTE: Temporary hack fix to subscribe original client to p2 decisions from views > 0;;
      //TODO: Replace with ForwardWriteback eventually
      p->second.has_original = true;
      p->second.original_msg_id = msg.req_id();
      p->second.original_address = remote.clone();
      p.release();
      SendPhase2Reply(&msg, phase2Reply, std::move(sendCB));
      //TransportAddress *remoteCopy2 = remote.clone();
      //HandlePhase2CB(remoteCopy2, &msg, txnDigest, sendCB, phase2Reply, cleanCB, (void*) true);
  }
  //TODO: Replace both Commit/Abort check with ForwardWriteback at some point. (this should happen before the hasP2 case then.)
  //NOTE: Either approach only works if atomicity of adding to committed/aborted and removing from ongoing is given.
          //Currently, this is that case as HandlePhase2 and WritebackCB are always on the same thread.
  else if(committed.find(*txnDigest) != committed.end()){
      p.release();
      phase2Reply->mutable_p2_decision()->set_decision(proto::COMMIT);
      phase2Reply->mutable_p2_decision()->set_view(0);
      SendPhase2Reply(&msg, phase2Reply, std::move(sendCB));
  }
  else if(aborted.find(*txnDigest) != aborted.end()){
      p.release();
      phase2Reply->mutable_p2_decision()->set_decision(proto::ABORT);
      phase2Reply->mutable_p2_decision()->set_view(0);
      SendPhase2Reply(&msg, phase2Reply, std::move(sendCB));
  }
  //first time receiving p2 message:
  else{
      p.release();
      //std::cerr << "First time P2 for special id: " << msg.req_id() << std::endl;
      Debug("PHASE2[%s].", BytesToHex(*txnDigest, 16).c_str());

      int64_t myProcessId;
      proto::ConcurrencyControl::Result myResult;

      if(msg.has_real_equiv() && msg.real_equiv()){
        stats.Increment("total_real_equiv_received_p2", 1);
      }
      if(msg.has_simulated_equiv() && msg.simulated_equiv()){
        //std::cerr<< "phase 2 has simulated equiv with decision: " << msg.decision() << std::endl;
        stats.Increment("total_simul_received_p2", 1);
        myProcessId = -1;
        if(msg.decision() == proto::COMMIT) stats.Increment("total_received_equiv_COMMIT", 1);
        if(msg.decision() == proto::ABORT) stats.Increment("total_received_equiv_ABORT", 1);
      }
      else{
        LookupP1Decision(*txnDigest, myProcessId, myResult);
      }

      if (!(params.validateProofs && params.signedMessages)){
        //TransportAddress *remoteCopy2 = remote.clone();
          phase2Reply->mutable_p2_decision()->set_decision(msg.decision());
          SendPhase2Reply(&msg, phase2Reply, sendCB);
          //HandlePhase2CB(remoteCopy2, &msg, txnDigest, sendCB, phase2Reply, cleanCB, (void*) true);
          return;
        }

  // ELSE: i.e. if (params.validateProofs && params.signedMessages)


       // i.e. if (params.validateProofs && params.signedMessages) {
        if(msg.has_txn_digest()) {
          ongoingMap::const_accessor b;
          auto txnItr = ongoing.find(b, msg.txn_digest());
          if(txnItr){
            txn = b->second.txn;
            b.release();
          }
          else{
         
            b.release();
            if(msg.has_txn()){
              txn = &msg.txn();
              // check that digest and txn match..
               if(*txnDigest !=TransactionDigest(*txn, params.hashDigest)) return;
            }
            else{
              Debug("PHASE2[%s] message does not contain txn, but have not seen"
                  " txn_digest previously.", BytesToHex(msg.txn_digest(), 16).c_str());
              //std::cerr << "Aborting for txn: " << BytesToHex(msg.txn_digest(), 16) << std::endl;
              if(params.multiThreading || (params.mainThreadDispatching && !params.dispatchMessageReceive)){
                  FreePhase2message(&msg); //const_cast<proto::Phase2&>(msg));
              }

              // if(normal.count(msg.txn_digest()) > 0){ std::cerr << "was added on normal P1" << std::endl;}
              // else if(fallback.count(msg.txn_digest()) > 0){ std::cerr << "was added on ExecP1 FB" << std::endl;}
              // else{ std::cerr << "have not seen p1" << std::endl;}
              // waiting.insert(msg.txn_digest());
              //transport->Timer(10000, [this](){Panic("Fail in P2");});
              //Panic("Cannot validate p2 because server does not have tx for this reqId");
              Warning("Cannot validate p2 because server does not have tx for this reqId");
              Panic("This should not be happening with TCP");
              //std::cerr << "Cannot validate p2 because do not have tx for special id: " << msg.req_id() << std::endl;
              return;
            }
          }
        }

        ManagePhase2Validation(remote, msg, txnDigest, txn, sendCB, phase2Reply, cleanCB, myProcessId, myResult);
  }
}

//Sends a signed P2 reply to the client, containing Commit/Abort respectively. 
//Echos the decision carried in P2 if none buffered, and returns previously buffered decision otherwise
void Server::HandlePhase2CB(TransportAddress *remote, proto::Phase2 *msg, const std::string* txnDigest,
  signedCallback sendCB, proto::Phase2Reply* phase2Reply, cleanCallback cleanCB, void* valid){

  Debug("HandlePhase2CB invoked");

  if(msg->has_simulated_equiv() && msg->simulated_equiv()){
    valid = (void*) true; //Code to simulate equivocation even when necessary equiv signatures do not exist.
    if(msg->decision() == proto::COMMIT) stats.Increment("total_equiv_COMMIT", 1);
    if(msg->decision() == proto::ABORT) stats.Increment("total_equiv_ABORT", 1);
  }

  if(!valid){
    stats.Increment("total_p2_invalid", 1);
    Debug("VALIDATE P1Replies for TX %s failed.", BytesToHex(*txnDigest, 16).c_str());
    cleanCB(); //deletes SendCB resources should SendCB not be called.
    if(params.multiThreading || (params.mainThreadDispatching && !params.dispatchMessageReceive)){
      FreePhase2message(msg); //const_cast<proto::Phase2&>(msg));
    }
    delete remote;
    Panic("P2 verification was invalid -- this should never happen?");
    return;
  }

  auto f = [this, remote, msg, txnDigest, sendCB = std::move(sendCB), phase2Reply, cleanCB = std::move(cleanCB), valid ]() mutable {

    p2MetaDataMap::accessor p;
    p2MetaDatas.insert(p, *txnDigest);
    bool hasP2 = p->second.hasP2;
    if(hasP2){
      phase2Reply->mutable_p2_decision()->set_decision(p->second.p2Decision);
      //std::cerr << "Already had P2 from FB. Setting P2 Decision for specialal id: " << phase2Reply->req_id() << std::endl;
    }
    else{
      p->second.p2Decision = msg->decision();
      p->second.hasP2 = true;
      phase2Reply->mutable_p2_decision()->set_decision(msg->decision());
      //std::cerr << "First time: Setting P2 Decision for speical id: " << phase2Reply->req_id() << std::endl;
    }

    if (params.validateProofs) {
      phase2Reply->mutable_p2_decision()->set_view(p->second.decision_view);
    }
    //NOTE: Temporary hack fix to subscribe original client to p2 decisions from views > 0
    p->second.has_original = true;
    p->second.original_msg_id = msg->req_id();
    p->second.original_address = remote;
    p.release();

    //XXX start client timeout for Fallback relay
    // if(client_starttime.find(*txnDigest) == client_starttime.end()){
    //   struct timeval tv;
    //   gettimeofday(&tv, NULL);
    //   uint64_t start_time HandlePhase2CB= (tv.tv_sec*1000000+tv.tv_usec)/1000;  //in miliseconds
    //   client_starttime[*txnDigest] = start_time;
    // }

    SendPhase2Reply(msg, phase2Reply, sendCB);
    //return;
    return (void*) true;
 };

 if(params.multiThreading && params.mainThreadDispatching && params.dispatchCallbacks){
   transport->DispatchTP_main(std::move(f));
 }
 else{
   f();
 }
}

//Dispatch Message signing and sending to a worker thread
void Server::SendPhase2Reply(proto::Phase2 *msg, proto::Phase2Reply *phase2Reply, signedCallback sendCB){

  if (params.validateProofs && params.signedMessages) {
    proto::Phase2Decision* p2Decision = new proto::Phase2Decision(phase2Reply->p2_decision());

    MessageToSign(p2Decision, phase2Reply->mutable_signed_p2_decision(),
      [sendCB, p2Decision, msg, this]() {
        sendCB();
        delete p2Decision;
        //Free allocated memory
        if(params.multiThreading || params.mainThreadDispatching){
          FreePhase2message(msg); // const_cast<proto::Phase2&>(msg));
        }
      });
    return;
  }
  else{
    sendCB();
    //Free allocated memory
    if(params.multiThreading || params.mainThreadDispatching){
      FreePhase2message(msg); // const_cast<proto::Phase2&>(msg));
    }
    return;
  }
}

/////////////////////////////// Writeback Handling

//Handler for Writeback Message
//Verifies correctness of request (quorum of P1replies or P2 replies depending on Fast/Slow Path)
//Dispatches verification to worker threads if multiThreading enabled
void Server::HandleWriteback(const TransportAddress &remote,
    proto::Writeback &msg) {

  stats.Increment("total_writeback_received", 1);
        //simulating failures in local experiment
        // fail_writeback++;
        // if(fail_writeback %2 == 1){
        //   return;
        // }


  proto::Transaction *txn;
  const std::string *txnDigest;
  std::string computedTxnDigest;
  if (!msg.has_txn() && !msg.has_txn_digest()) {
    Debug("WRITEBACK message contains neither txn nor txn_digest.");
    return WritebackCallback(&msg, txnDigest, txn, (void*) false);
  }

  if (msg.has_txn_digest() ) {
    txnDigest = &msg.txn_digest();
     Debug("WRITEBACK[%s] received with decision %d.",
      BytesToHex(*txnDigest, 16).c_str(), msg.decision());

    if(committed.find(*txnDigest) != committed.end() || aborted.find(*txnDigest) != aborted.end()){
      if(params.multiThreading || (params.mainThreadDispatching && !params.dispatchMessageReceive)){
        Debug("Calling Re-CLEAN on Writeback for Txn[%s]", BytesToHex(*txnDigest, 16).c_str());
        Clean(*txnDigest); //XXX Clean again since client could have added it back to ongoing...
        FreeWBmessage(&msg);
      }
      return;  //TODO: Forward to all interested clients and empty it?
    }
   
    ongoingMap::const_accessor b;
    bool hasTxn = ongoing.find(b, msg.txn_digest());
    if(hasTxn){
      txn = b->second.txn;
      b.release();
    }
    else{
      b.release();
      if(msg.has_txn()){
        txn = msg.release_txn();
        // check that digest and txn match..
         if(*txnDigest !=TransactionDigest(*txn, params.hashDigest)) return;
      }
      else{
        Debug("Writeback[%s] message does not contain txn, but have not seen"
            " txn_digest previously.", BytesToHex(msg.txn_digest(), 16).c_str());
        Warning("Cannot process Writeback because ongoing does not contain tx for this request. Should not happen with TCP.... CPU %d", sched_getcpu());
        Panic("When using TCP the tx should always be ongoing before doing WB");
        return WritebackCallback(&msg, txnDigest, txn, (void*) false);
      }
    }
  } else {
    txn = msg.release_txn();
    computedTxnDigest = TransactionDigest(*txn, params.hashDigest);
    txnDigest = &computedTxnDigest;

    if(committed.find(*txnDigest) != committed.end() || aborted.find(*txnDigest) != aborted.end()){
      if(params.multiThreading || (params.mainThreadDispatching && !params.dispatchMessageReceive)){
        Clean(*txnDigest); //XXX Clean again since client could have added it back to ongoing...
        FreeWBmessage(&msg);
        delete txn;
      }
      return;  //TODO: Forward to all interested clients and empty it?
    }
  }

  Debug("WRITEBACK[%s] with decision %d. Begin Validation",
      BytesToHex(*txnDigest, 16).c_str(), msg.decision());

  //Verify Writeback Proofs + Call WritebackCallback
  ManageWritebackValidation(msg, txnDigest, txn);
  
}

//Called after Writeback request has been verified
//Updates committed/aborted datastructures and key-value store accordingly
//Garbage collects ongoing meta-data
void Server::WritebackCallback(proto::Writeback *msg, const std::string* txnDigest,
  proto::Transaction* txn, void* valid){


  if(!valid){
    Debug("VALIDATE Writeback for TX %s failed.", BytesToHex(*txnDigest, 16).c_str());
    if(params.multiThreading || (params.mainThreadDispatching && !params.dispatchMessageReceive)){
      FreeWBmessage(msg);
    }
     Panic("Writeback Validation should not fail for TX %s ", BytesToHex(*txnDigest, 16).c_str());
    return;
  }

  auto f = [this, msg, txnDigest, txn, valid]() mutable {
      Debug("WRITEBACK Callback[%s] being called", BytesToHex(*txnDigest, 16).c_str());

      ///////////////////////////// Below: Only executed by MainThread
      // tbb::concurrent_hash_map<std::string, std::mutex>::accessor z;
      // completing.insert(z, *txnDigest);
      // //z->second.lock();
      // z.release();
      if(committed.find(*txnDigest) != committed.end() || aborted.find(*txnDigest) != aborted.end()){
        //duplicate, do nothing. TODO: Forward to all interested clients and empty it?
      }
      else if (msg->decision() == proto::COMMIT) {
        stats.Increment("total_transactions", 1);
        stats.Increment("total_transactions_commit", 1);
        Debug("WRITEBACK[%s] successfully committing.", BytesToHex(*txnDigest, 16).c_str());
        bool p1Sigs = msg->has_p1_sigs();
        uint64_t view = -1;
        if(!p1Sigs){
          if(msg->has_p2_sigs() && msg->has_p2_view()){
            view = msg->p2_view();
          }
          else{
            Debug("Writeback for P2 does not have view or sigs");
            //return; //XXX comment back
            view = 0;
            //return (void*) false;
          }
        }
        Debug("COMMIT ONLY RUN BY MAINTHREAD: %d", sched_getcpu());
        Commit(*txnDigest, txn, p1Sigs ? msg->release_p1_sigs() : msg->release_p2_sigs(), p1Sigs, view);
      } else {
        stats.Increment("total_transactions", 1);
        stats.Increment("total_transactions_abort", 1);
        Debug("WRITEBACK[%s] successfully aborting.", BytesToHex(*txnDigest, 16).c_str());
        //msg->set_allocated_txn(txn); //dont need to set since client will?
        writebackMessages[*txnDigest] = *msg;  //Only necessary for fallback... (could avoid storing these, if one just replied with a p2 vote instead - but that is not as responsive)
        ///CAUTION: msg might no longer hold txn; could have been released in HanldeWriteback

        Abort(*txnDigest);
      }

      if(params.multiThreading || params.mainThreadDispatching){
        FreeWBmessage(msg);
      }
      //return;
      return (void*) true;
  };

 if(params.multiThreading && params.mainThreadDispatching && params.dispatchCallbacks){
   transport->DispatchTP_main(std::move(f));
 }
 else{
   f();
 }

}


void Server::HandleAbort(const TransportAddress &remote,
    const proto::Abort &msg) {
  const proto::AbortInternal *abort;
  if (params.validateProofs && params.signedMessages && params.signClientProposals) {
    if (!msg.has_signed_internal()) {
      return;
    }

    Latency_Start(&verifyLat);
    if (!client_verifier->Verify(keyManager->GetPublicKey(keyManager->GetClientKeyId(msg.signed_internal().process_id())),
          msg.signed_internal().data(),
          msg.signed_internal().signature())) {
      //Latency_End(&verifyLat);
      return;
    }
    Latency_End(&verifyLat);

    if (!abortInternal.ParseFromString(msg.signed_internal().data())) {
      return;
    }

    if (abortInternal.ts().id() != msg.signed_internal().process_id()) {
      return;
    }

    abort = &abortInternal;
  } else {
    UW_ASSERT(msg.has_internal());
    abort = &msg.internal();
  }

  //RECOMMENT XXX currently displaced by RTS implementation that has no set, but only a single RTS version that keeps getting replaced.
  //  if(params.mainThreadDispatching) rtsMutex.lock();
  // for (const auto &read : abort->read_set()) {
  //   rts[read].erase(abort->ts());
  // }
  //  if(params.mainThreadDispatching) rtsMutex.unlock();
  if(params.rtsMode == 1){
    //Do nothing -- If we removed latest RTS then smaller ones that should be subsumed become inactive too.
  }
  else if(params.rtsMode == 2){
    for (const auto &read : abort->read_set()) {
    std::pair<std::shared_mutex, std::set<Timestamp>> &rts_set = rts_list[read];
      {
        std::unique_lock lock(rts_set.first);
        rts_set.second.erase(abort->ts());
      }
  }
  }
  else{
    //No RTS
  }


  if(params.multiThreading || (params.mainThreadDispatching && !params.dispatchMessageReceive))  FreeAbortMessage(&msg);
}


/////////////////////////////////////// PREPARE, COMMIT AND ABORT LOGIC  + Cleanup

void Server::Prepare(const std::string &txnDigest,
    const proto::Transaction &txn) {
  Debug("PREPARE[%s] agreed to commit with ts %lu.%lu.",
      BytesToHex(txnDigest, 16).c_str(), txn.timestamp().timestamp(), txn.timestamp().id());


  ongoingMap::const_accessor b;
  auto ongoingItr = ongoing.find(b, txnDigest);
  if(!ongoingItr){
  //if(ongoingItr == ongoing.end()){
    Debug("Already concurrently Committed/Aborted txn[%s]", BytesToHex(txnDigest, 16).c_str());
    return;
  }
  const proto::Transaction *ongoingTxn = b->second.txn;
  //const proto::Transaction *ongoingTxn = ongoing.at(txnDigest);

  preparedMap::accessor a;
  auto p = prepared.insert(a, std::make_pair(txnDigest, std::make_pair(
          Timestamp(txn.timestamp()), ongoingTxn)));

  for (const auto &read : txn.read_set()) {
    if (IsKeyOwned(read.key())) {
      //preparedReads[read.key()].insert(p.first->second.second);
      //preparedReads[read.key()].insert(a->second.second);

      std::pair<std::shared_mutex, std::set<const proto::Transaction *>> &y = preparedReads[read.key()];
      std::unique_lock lock(y.first);
      y.second.insert(a->second.second);

      // if(params.rtsMode == 2){ //remove RTS now that its prepared? --> Kind of pointless, since prepare fulfills the same thing.
      //   std::pair<std::shared_mutex, std::set<Timestamp>> &rts_set = rts_list[read.key()];
      //   {
      //     std::unique_lock lock(rts_set.first);
      //     rts_set.second.erase(txn.timestamp());
      //   }
      // }
    }
  }

  std::pair<Timestamp, const proto::Transaction *> pWrite =
    std::make_pair(a->second.first, a->second.second);
  a.release();
    //std::make_pair(p.first->second.first, p.first->second.second);
  for (const auto &write : txn.write_set()) {
    if (IsKeyOwned(write.key())) {
      std::pair<std::shared_mutex,std::map<Timestamp, const proto::Transaction *>> &x = preparedWrites[write.key()];
      std::unique_lock lock(x.first);
      x.second.insert(pWrite);
      // std::unique_lock lock(preparedWrites[write.key()].first);
      // preparedWrites[write.key()].second.insert(pWrite);
    }
  }
  b.release(); //Relase only at the end, so that Prepare and Clean in parallel for the same TX are atomic.
}


void Server::Commit(const std::string &txnDigest, proto::Transaction *txn,
      proto::GroupedSignatures *groupedSigs, bool p1Sigs, uint64_t view) {

  Timestamp ts(txn->timestamp());

  Value val;
  proto::CommittedProof *proof = nullptr;
  if (params.validateProofs) {
    Debug("Access only by CPU: %d", sched_getcpu());
    proof = new proto::CommittedProof();
    //proof = testing_committed_proof.back();
    //testing_committed_proof.pop_back();
  }
  val.proof = proof;

  auto committedItr = committed.insert(std::make_pair(txnDigest, proof));
  Debug("Inserted txn %s into Committed on CPU %d",BytesToHex(txnDigest, 16).c_str(), sched_getcpu());
  //auto committedItr =committed.emplace(txnDigest, proof);

  if (params.validateProofs) {
    // CAUTION: we no longer own txn pointer (which we allocated during Phase1
    //    and stored in ongoing)
    proof->set_allocated_txn(txn);
    if (params.signedMessages) {
      if (p1Sigs) {
        proof->set_allocated_p1_sigs(groupedSigs);
      } else {
        proof->set_allocated_p2_sigs(groupedSigs);
        proof->set_p2_view(view);
        //view = groupedSigs.begin()->second.sigs().begin()
      }
    }
  }

  for (const auto &read : txn->read_set()) {
    if (!IsKeyOwned(read.key())) {
      continue;
    }


    //store.commitGet(read.key(), read.readtime(), ts);   //SEEMINGLY NEVER USED XXX
    //commitGet_count++;

    //Latency_Start(&committedReadInsertLat);

     std::pair<std::shared_mutex, std::set<committedRead>> &z = committedReads[read.key()];
     std::unique_lock lock(z.first);
     z.second.insert(std::make_tuple(ts, read.readtime(),
           committedItr.first->second));
    // committedReads[read.key()].insert(std::make_tuple(ts, read.readtime(),
    //       committedItr.first->second));

    //uint64_t ns = Latency_End(&committedReadInsertLat);
    //stats.Add("committed_read_insert_lat_" + BytesToHex(read.key(), 18), ns);
  }


  for (const auto &write : txn->write_set()) {
    if (!IsKeyOwned(write.key())) {
      continue;
    }

    Debug("COMMIT[%lu,%lu] Committing write for key %s.",
        txn->client_id(), txn->client_seq_num(),
        BytesToHex(write.key(), 16).c_str());
    val.val = write.value();

    store.put(write.key(), val, ts);


    if(params.rtsMode == 1){
      //Do nothing
    }
    else if(params.rtsMode == 2){
      //Remove obsolete RTS. TODO: Can be more refined: Only remove
      std::pair<std::shared_mutex, std::set<Timestamp>> &rts_set = rts_list[write.key()];
      {
          std::unique_lock lock(rts_set.first);
          auto itr = rts_set.second.lower_bound(ts); //begin
          auto endItr = rts_set.second.end();        //upper_bound  --> Old version felt wrong, why would we remove the smaller RTS..
          //delete all RTS >= committed TS. Those RTS can no longer claim the space < RTS since a write successfully committed.
          //TODO: Ideally this would be more refined: RTS should include the timestamp of the write read, i.e. an RTS claims range <read-write, timestamp>
          while (itr != endItr) {
            itr = rts_set.second.erase(itr);
          }
      }
    }
    else{
      //No RTS
    }
  }

  Debug("Calling CLEAN for committing txn[%s]", BytesToHex(txnDigest, 16).c_str());
  Clean(txnDigest);
  CheckDependents(txnDigest);
  CleanDependencies(txnDigest);
}

void Server::Abort(const std::string &txnDigest) {
   //if(params.mainThreadDispatching) abortedMutex.lock();
  aborted.insert(txnDigest);
  Debug("Inserted txn %s into ABORTED on CPU: %d", BytesToHex(txnDigest, 16).c_str(), sched_getcpu());
   //if(params.mainThreadDispatching) abortedMutex.unlock();
  Debug("Calling CLEAN for aborting txn[%s]", BytesToHex(txnDigest, 16).c_str());
  Clean(txnDigest, true);
  CheckDependents(txnDigest);
  CleanDependencies(txnDigest);
}

void Server::Clean(const std::string &txnDigest, bool abort) {

  //Latency_Start(&waitingOnLocks);
  //auto ongoingMutexScope = params.mainThreadDispatching ? std::unique_lock<std::shared_mutex>(ongoingMutex) : std::unique_lock<std::shared_mutex>();
  //auto preparedMutexScope = params.mainThreadDispatching ? std::unique_lock<std::shared_mutex>(preparedMutex) : std::unique_lock<std::shared_mutex>();
  //auto preparedReadsMutexScope = params.mainThreadDispatching ? std::unique_lock<std::shared_mutex>(preparedReadsMutex) : std::unique_lock<std::shared_mutex>();
  //auto preparedWritesMutexScope = params.mainThreadDispatching ? std::unique_lock<std::shared_mutex>(preparedWritesMutex) : std::unique_lock<std::shared_mutex>();

  //auto interestedClientsMutexScope = params.mainThreadDispatching ? std::unique_lock<std::mutex>(interestedClientsMutex) : std::unique_lock<std::mutex>();
  //auto p1ConflictsMutexScope = params.mainThreadDispatching ? std::unique_lock<std::mutex>(p1ConflictsMutex) : std::unique_lock<std::mutex>();
  //p1Decisions..
  // auto p2DecisionsMutexScope = params.mainThreadDispatching ? std::unique_lock<std::mutex>(p2DecisionsMutex) : std::unique_lock<std::mutex>();
  // auto current_viewsMutexScope = params.mainThreadDispatching ? std::unique_lock<std::mutex>(current_viewsMutex) : std::unique_lock<std::mutex>();
  // auto decision_viewsMutexScope = params.mainThreadDispatching ? std::unique_lock<std::mutex>(decision_viewsMutex) : std::unique_lock<std::mutex>();
  //auto ElectQuorumMutexScope = params.mainThreadDispatching ? std::unique_lock<std::mutex>(ElectQuorumMutex) : std::unique_lock<std::mutex>();
  //Latency_End(&waitingOnLocks);

  ongoingMap::accessor b;
  bool is_ongoing = ongoing.find(b, txnDigest);
  if(is_ongoing) ongoing.erase(b);

  preparedMap::accessor a;
  bool is_prepared = prepared.find(a, txnDigest);
  if(is_prepared){
  //if (itr != prepared.end()) {
    for (const auto &read : a->second.second->read_set()) {
    //for (const auto &read : itr->second.second->read_set()) {
      if (IsKeyOwned(read.key())) {
        //preparedReads[read.key()].erase(a->second.second);
        //preparedReads[read.key()].erase(itr->second.second);
        std::pair<std::shared_mutex, std::set<const proto::Transaction *>> &y = preparedReads[read.key()];
        std::unique_lock lock(y.first);
        y.second.erase(a->second.second);
      }
    }
    for (const auto &write : a->second.second->write_set()) {
    //for (const auto &write : itr->second.second->write_set()) {
      if (IsKeyOwned(write.key())) {
        //preparedWrites[write.key()].erase(itr->second.first);
        std::pair<std::shared_mutex,std::map<Timestamp, const proto::Transaction *>> &x = preparedWrites[write.key()];
        std::unique_lock lock(x.first);
        x.second.erase(a->second.first);
        //x.second.erase(itr->second.first);
      }
    }
    prepared.erase(a);
  }
  a.release();

  // if(is_ongoing){
  //     std::cerr << "ONGOING ERASE: " << BytesToHex(txnDigest, 16).c_str() << " On CPU: " << sched_getcpu()<< std::endl;
  //     //if(abort) delete b->second.txn; //delete allocated txn.
  //     //ongoing.erase(b);
  // }
  b.release(); //Release only at the end, so that Prepare and Clean in parallel for the same TX are atomic.
  //TODO: might want to move release all the way to the end.

  //XXX: Fallback related cleans

  //interestedClients[txnDigest].insert(remote.clone());
  interestedClientsMap::accessor i;
  auto jtr = interestedClients.find(i, txnDigest);
  if(jtr){
  //if (jtr != interestedClients.end()) {
    for (const auto client : i->second) {
      delete client.second;
    }
    interestedClients.erase(i);
  }
  i.release();


  ElectQuorumMap::accessor q;
  auto ktr = ElectQuorums.find(q, txnDigest);
  if (ktr) {
    // ElectFBorganizer &electFBorganizer = q->second;
    //   //delete all sigs allocated in here
    //   auto it=electFBorganizer.view_quorums.begin();
    //   while(it != electFBorganizer.view_quorums.end()){
    //       for(auto decision_sigs : it->second){
    //         for(auto sig : decision_sigs.second.second){  //it->second[decision_sigs].second
    //           delete sig;
    //         }
    //       }
    //       it = electFBorganizer.view_quorums.erase(it);
    //   }

    ElectQuorums.erase(q);
  }
  q.release();

  //TODO: try to merge more/if not all tx local state into ongoing and hold ongoing locks for simpler atomicity.

//Currently commented out so that original client does not re-do work if p1/p2 has already happened
//--> TODO: Make original client dynamic, i.e. it can directly receive a writeback if it exists, instead of HAVING to finish normal case.
  p1MetaDataMap::accessor c;
  bool p1MetaExists = p1MetaData.find(c, txnDigest);
  if(p1MetaExists){
    if(c->second.hasSignedP1) delete c->second.signed_txn; //Delete signed txn if not needed anymore.
    c->second.hasSignedP1 = false;
  } 
  //if(hasP1) p1MetaData.erase(c);
  c.release();

  
  // p2MetaDataMap::accessor p;
  // if(p2MetaDatas.find(p, txnDigest)){
  //   p2MetaDatas.erase(p);
  // }
  // p.release();

  //TODO: erase all timers if we end up using them again

  // tbb::concurrent_hash_map<std::string, std::mutex>::accessor z;
  // if(completing.find(z, txnDigest)){
  //   //z->second.unlock();
  //   completing.erase(z);
  // }
  // z.release();
}



///////////////////////////////////////// FALLBACK PROTOCOL LOGIC (Enter the Fallback realm!)

//TODO: change arguments (move strings) to avoid the copy in Timer.
void Server::RelayP1(const std::string &dependency_txnDig, bool fallback_flow, uint64_t reqId, const TransportAddress &remote, const std::string &txnDigest){
  stats.Increment("Relays_Called", 1);
  //schedule Relay for client timeout only..
  uint64_t conflict_id = !fallback_flow ? reqId : -1;
  const std::string &dependent_txnDig = !fallback_flow ? std::string() : txnDigest;
  TransportAddress *remoteCopy = remote.clone();
  uint64_t relayDelay = !fallback_flow ? params.relayP1_timeout : 0;
  transport->Timer(relayDelay, [this, remoteCopy, dependency_txnDig, conflict_id, dependent_txnDig]() mutable {
    this->SendRelayP1(*remoteCopy, dependency_txnDig, conflict_id, dependent_txnDig);
    delete remoteCopy;
  });
}

//RELAY DEPENDENCY IN ORDER FOR CLIENT TO START FALLBACK
//params: dependent_it = client tx identifier for blocked tx; dependency_txnDigest = tx that is stalling
void Server::SendRelayP1(const TransportAddress &remote, const std::string &dependency_txnDig, uint64_t dependent_id, const std::string &dependent_txnDig){

  Debug("RelayP1[%s] timed out. Sending now!", BytesToHex(dependent_txnDig, 256).c_str());
  proto::Transaction *tx;
  proto::SignedMessage *signed_tx;

  //ongoingMap::const_accessor b;
  ongoingMap::accessor b;
  bool ongoingItr = ongoing.find(b, dependency_txnDig);
  if(!ongoingItr) return;  //If txnDigest no longer ongoing, then no FB necessary as it has completed already
  tx = b->second.txn;
  p1MetaDataMap::accessor c;
  //b.release();

  //proto::RelayP1 relayP1; //use global object.
  relayP1.Clear();
  relayP1.set_dependent_id(dependent_id);
  relayP1.mutable_p1()->set_req_id(0); //doesnt matter, its not used for fallback requests really.
  //*relayP1.mutable_p1()->mutable_txn() = *tx; //TODO:: avoid copy by allocating, and releasing again after.

  if(params.signClientProposals){
    //b.release();
    bool p1MetaExists = p1MetaData.find(c, dependency_txnDig); //If txn is in ongoing, then it must have been added to P1Meta --> since we add to ongoing in HandleP1 or HandleP1FB. 
                                                                                          // (TODO FIX: Current verification --adds signed_txn-- happens only after inster ongoing. Should be swapped to guarantee the above.)
     if(!p1MetaExists || c->second.hasSignedP1 == false) Panic("Should exist since ongoing is still true");
    signed_tx = c->second.signed_txn;
    relayP1.mutable_p1()->set_allocated_signed_txn(signed_tx);
  }
  else{ //no Client sigs --> just send txn.
    relayP1.mutable_p1()->set_allocated_txn(tx);  
  }
  

  if(dependent_id == -1) relayP1.set_dependent_txn(dependent_txnDig);

  if(dependent_id == -1){
    Debug("Sending relayP1 for dependent txn: %s stuck waiting for dependency: %s", BytesToHex(dependent_txnDig, 64).c_str(), BytesToHex(dependency_txnDig,64).c_str());
  }

  stats.Increment("Relays_Sent", 1);
  transport->SendMessage(this, remote, relayP1);

  if(params.signClientProposals){
    c->second.signed_txn = relayP1.mutable_p1()->release_signed_txn();
    c.release();
  }
  else{
    b->second.txn = relayP1.mutable_p1()->release_txn();
    //b.release();
  }
  b.release(); //keep ongoing locked the full duration: this guarantees that if ongoing exists, p1Meta.signed_tx exists too, since it is deleted after ongoing in Clean()

  Debug("Sent RelayP1[%s].", BytesToHex(dependent_txnDig, 256).c_str());
}

bool Server::ForwardWriteback(const TransportAddress &remote, uint64_t ReqId, const std::string &txnDigest){
  
  Debug("Checking for existing WB message for txn %s", BytesToHex(txnDigest, 16).c_str());
  //1) COMMIT CASE
  if(committed.find(txnDigest) != committed.end()){
      Debug("ForwardingWriteback Commit for txn: %s", BytesToHex(txnDigest, 64).c_str());
      proto::Phase1FBReply phase1FBReply;
      phase1FBReply.Clear();
      phase1FBReply.set_req_id(ReqId);
      phase1FBReply.set_txn_digest(txnDigest);

      proto::Writeback *wb = phase1FBReply.mutable_wb();
      wb->Clear();
      wb->set_decision(proto::COMMIT);
      wb->set_txn_digest(txnDigest);
      proto::CommittedProof* proof = committed[txnDigest];

      //*wb->mutable_txn() = proof->txn();

      if(proof->has_p1_sigs()){
        *wb->mutable_p1_sigs() = proof->p1_sigs();
      }
      else if(proof->has_p2_sigs()){
        *wb->mutable_p2_sigs() = proof->p2_sigs();
        wb->set_p2_view(proof->p2_view());
      }
      else{
        Panic("Commit proof has no signatures"); //error, should not happen
        // A Commit proof
        return false;
      }

      transport->SendMessage(this, remote, phase1FBReply);

      //TODO: delete interested client addres too, should there be an interested one. (or always use ForwardMulti.)
      // interestedClientsMap::accessor i;
      // bool interestedClientsItr = interestedClients.find(i, txnDigest);
      // i->second.erase(remote.clone());
      // i.release();
      // delete addr;
      // interestedClients.erase(i);
      // i.release();
      return true;
  }

  //2) ABORT CASE
  //currently for simplicity just forward writeback message that we received and stored.
  //writebackMessages only contains Abort copies. (when can one delete these?)
  //(A blockchain stores all request too, whether commit/abort)
  if(writebackMessages.find(txnDigest) != writebackMessages.end()){
      Debug("ForwardingWriteback Abort for txn: %s", BytesToHex(txnDigest, 64).c_str());
      proto::Phase1FBReply phase1FBReply;
      phase1FBReply.Clear();
      phase1FBReply.set_req_id(ReqId);
      phase1FBReply.set_txn_digest(txnDigest);
      *phase1FBReply.mutable_wb() = writebackMessages[txnDigest];

      transport->SendMessage(this, remote, phase1FBReply);
      return true;
  }
  
  Debug("No existing WB message found for txn %s", BytesToHex(txnDigest, 16).c_str());
  return false;
}

bool Server::ForwardWritebackMulti(const std::string &txnDigest, interestedClientsMap::accessor &i){

  //interestedClientsMap::accessor i;
  //auto jtr = interestedClients.find(i, txnDigest);
  //if(!jtr) return true; //no interested clients, return
  proto::Phase1FBReply phase1FBReply;

  if(committed.find(txnDigest) != committed.end()){
      Debug("ForwardingWritebackMulti Commit for txn: %s", BytesToHex(txnDigest, 64).c_str());
      phase1FBReply.Clear();
      phase1FBReply.set_req_id(0);
      phase1FBReply.set_txn_digest(txnDigest);

      proto::Writeback *wb = phase1FBReply.mutable_wb();
      wb->Clear();
      wb->set_decision(proto::COMMIT);
      wb->set_txn_digest(txnDigest);
      proto::CommittedProof* proof = committed[txnDigest];

      //*wb->mutable_txn() = proof->txn();

      if(proof->has_p1_sigs()){
        *wb->mutable_p1_sigs() = proof->p1_sigs();
      }
      else if(proof->has_p2_sigs()){
        *wb->mutable_p2_sigs() = proof->p2_sigs();
        wb->set_p2_view(proof->p2_view());
      }
      else{
        Panic("Commit proof has no signatures"); //error, should not happen
        // A Commit proof
        return false;
      }
  }
  //2) ABORT CASE
  else if(writebackMessages.find(txnDigest) != writebackMessages.end()){
      Debug("ForwardingWritebackMulti Abort for txn: %s", BytesToHex(txnDigest, 64).c_str());
      phase1FBReply.Clear();
      phase1FBReply.set_req_id(0);
      phase1FBReply.set_txn_digest(txnDigest);
      *phase1FBReply.mutable_wb() = writebackMessages[txnDigest];
  }
  else{
    return false;
  }

  for (const auto client : i->second) {
    Debug("ForwardingWritebackMulti for txn: %s to +1 clients", BytesToHex(txnDigest, 64).c_str()); //would need to store client ID with it to print.

    transport->SendMessage(this, *client.second, phase1FBReply);
    delete client.second;
  }
  interestedClients.erase(i);
  //i.release();
  return true;
}

//TODO: all requestID entries can be deleted.. currently unused for FB
//TODO:: CURRENTLY IF CASES ARE NOT ATOMIC (only matters if one intends to parallelize):
//For example, 1) case for committed could fail, but all consecutive fail too because it was committed inbetween.
//Could just put abort cases last; but makes for redundant work if it should occur inbetween.
void Server::HandlePhase1FB(const TransportAddress &remote, proto::Phase1FB &msg) {

  proto::Transaction *txn; 
  if(params.signClientProposals){
    txn = new proto::Transaction();
     txn->ParseFromString(msg.signed_txn().data());
  }
  else{
     txn = msg.mutable_txn();
  }

  stats.Increment("total_p1FB_received", 1);
  std::string txnDigest = TransactionDigest(*txn, params.hashDigest);
  Debug("Received PHASE1FB[%lu][%s] from client %lu. This is server: %lu", msg.req_id(), BytesToHex(txnDigest, 16).c_str(), msg.client_id(), id);


  //check if already committed. reply with whole proof so client can forward that.
  //1) COMMIT CASE, 2) ABORT CASE

  if(ForwardWriteback(remote, msg.req_id(), txnDigest)){
    if((params.mainThreadDispatching && (!params.dispatchMessageReceive || params.parallel_CCC)) || (params.multiThreading && params.signClientProposals)) FreePhase1FBmessage(&msg);
    if(params.signClientProposals) delete txn;
    return;
  }
  
  //Otherwise, keep track of interested clients to message in the future
  interestedClientsMap::accessor i;
  bool interestedClientsItr = interestedClients.insert(i, txnDigest);
  const TransportAddress* remoteCopy = remote.clone();
  //i->second.insert(remoteCopy);
  i->second[msg.client_id()] = remoteCopy;
  i.release();

  //interestedClients[txnDigest].insert(remote.clone());


  //3) BOTH P2 AND P1 CASE
  //might want to include the p1 too in order for there to exist a quorum for p1r (if not enough p2r). if you dont have a p1, then execute it yourself.
  //Alternatively, keep around the decision proof and send it. For now/simplicity, p2 suffices
  //TODO: could store p2 and p1 signatures (until writeback) in order to avoid re-computation
  //XXX CHANGED IT TO ACCESSOR FOR LOCK TEST
  p1MetaDataMap::const_accessor c;
  //p1MetaData.find(c, txnDigest);
  // p1MetaData.insert(c, txnDigest);
  bool hasP1result = p1MetaData.find(c, txnDigest) ? c->second.hasP1 : false;
  //std::cerr << "[FB] acquire lock for txn: " << BytesToHex(txnDigest, 64) << std::endl;
  p2MetaDataMap::const_accessor p;
  p2MetaDatas.insert(p, txnDigest);
  bool hasP2result = p->second.hasP2;

  if(hasP2result && hasP1result){
    Debug("Txn[%s] has both P1 and P2", BytesToHex(txnDigest, 64).c_str());
       proto::ConcurrencyControl::Result result = c->second.result; //p1Decisions[txnDigest];
       //if(result == proto::ConcurrencyControl::ABORT);
       const proto::CommittedProof *conflict = c->second.conflict;
       //c->second.P1meta_mutex.unlock();
       //std::cerr << "[FB:1] release lock for txn: " << BytesToHex(txnDigest, 64) << std::endl;
       c.release();

       proto::CommitDecision decision = p->second.p2Decision;
       uint64_t decision_view = p->second.decision_view;
       p.release();

       P1FBorganizer *p1fb_organizer = new P1FBorganizer(msg.req_id(), txnDigest, remote, this);
       //recover stored commit proof.
       if (result != proto::ConcurrencyControl::WAIT) { //if the result is WAIT, then the p1 is not necessary..
         SetP1(msg.req_id(), p1fb_organizer->p1fbr->mutable_p1r(), txnDigest, result, conflict);
       }
       else{
         //Relay deeper depths.
        ManageDependencies(txnDigest, *txn, remote, 0, true);
        Debug("P1 decision for txn: %s is WAIT. Not including in reply.", BytesToHex(txnDigest, 16).c_str());
       }
       //c.release();

       SetP2(msg.req_id(), p1fb_organizer->p1fbr->mutable_p2r(), txnDigest, decision, decision_view);
       SendPhase1FBReply(p1fb_organizer, txnDigest);

       Debug("Sent Phase1FBReply on path hasP2+hasP1 for txn: %s, sent by client: %d. P1 result = %d", BytesToHex(txnDigest, 16).c_str(), msg.client_id(), result);
       if(params.signClientProposals) delete txn;
       // c.release();
       // p.release();
  }

  //4) ONLY P1 CASE: (already did p1 but no p2)
  else if(hasP1result){
    Debug("Txn[%s] has only P1", BytesToHex(txnDigest, 64).c_str());
        proto::ConcurrencyControl::Result result = c->second.result; //p1Decisions[txnDigest];
        //if(result == proto::ConcurrencyControl::ABORT);
        const proto::CommittedProof *conflict = c->second.conflict;
        //c->second.P1meta_mutex.unlock();
        //std::cerr << "[FB:2] release lock for txn: " << BytesToHex(txnDigest, 64) << std::endl;
        c.release();
        p.release();

        if (result != proto::ConcurrencyControl::WAIT) {
          P1FBorganizer *p1fb_organizer = new P1FBorganizer(msg.req_id(), txnDigest, remote, this);
          SetP1(msg.req_id(), p1fb_organizer->p1fbr->mutable_p1r(), txnDigest, result, conflict);
          SendPhase1FBReply(p1fb_organizer, txnDigest);
          Debug("Sent Phase1FBReply on path hasP1 for txn: %s, sent by client: %d", BytesToHex(txnDigest, 16).c_str(), msg.req_id());

        }
        else{
          ManageDependencies(txnDigest, *txn, remote, 0, true);
          Debug("WAITING on dep in order to send Phase1FBReply on path hasP1 for txn: %s, sent by client: %d", BytesToHex(txnDigest, 16).c_str(), msg.client_id());
        }
        if(params.signClientProposals) delete txn;
        // c.release();
        // p.release();

  }
  //5) ONLY P2 CASE  (received p2, but was not part of p1)
  // if you dont have a p1, then execute it yourself. (see case 3) discussion)
  else if(hasP2result){ // With TCP this case will never be triggered: Any replica that receives P2 also has P1.
      Debug("Txn[%s] has only P2, execute P1 as well", BytesToHex(txnDigest, 64).c_str());

      c.release();
      proto::CommitDecision decision = p->second.p2Decision;
      uint64_t decision_view = p->second.decision_view;
      p.release();


      P1FBorganizer *p1fb_organizer = new P1FBorganizer(msg.req_id(), txnDigest, remote, this);
      SetP2(msg.req_id(), p1fb_organizer->p1fbr->mutable_p2r(), txnDigest, decision, decision_view);

      //Exec p1 if we do not have it.
      const proto::CommittedProof *committedProof;
      proto::ConcurrencyControl::Result result;
      const proto::Transaction *abstain_conflict = nullptr;

      if(!VerifyDependencies(msg, txn, txnDigest, true)) return; //Verify Deps explicitly -- since its no longer part of ExecP1
     
      //Add to ongoing before Exec
      AddOngoing(msg, txnDigest, txn);
      if (ExecP1(msg, remote, txnDigest, txn, result, committedProof, abstain_conflict)) { //only send if the result is not Wait
          SetP1(msg.req_id(), p1fb_organizer->p1fbr->mutable_p1r(), txnDigest, result, committedProof, abstain_conflict);
      }
      //c->second.P1meta_mutex.unlock();
      //c.release();
      //std::cerr << "[FB:3] release lock for txn: " << BytesToHex(txnDigest, 64) << std::endl;
      SendPhase1FBReply(p1fb_organizer, txnDigest);
      Debug("Sent Phase1FBReply on path P2 + ExecP1 for txn: %s, sent by client: %d", BytesToHex(txnDigest, 16).c_str(), msg.client_id());
  }

  //6) NO STATE STORED: Do p1 normally. copied logic from HandlePhase1(remote, msg)
  else{
      Debug("Txn[%s] has no P1 or P2, execute P1", BytesToHex(txnDigest, 64).c_str());
      c.release();
      p.release();

      //Calls Validation before trying to Exec 
      //Do not need to validate P1 proposal if we have P2 (case 5) --> it follow transitively from the verification of P2 proof.
      ProcessProposalFB(msg, remote, txnDigest, txn);
      return; //msg free are handled within

      //c->second.P1meta_mutex.unlock();
      //c.release();
      //std::cerr << "[FB:4] release lock for txn: " << BytesToHex(txnDigest, 64) << std::endl;
  }

  if((params.mainThreadDispatching && (!params.dispatchMessageReceive || params.parallel_CCC)) || (params.multiThreading && params.signClientProposals)) FreePhase1FBmessage(&msg); 
}

void Server::AddOngoing(proto::Phase1FB &msg, std::string &txnDigest, proto::Transaction* txn ){
   //Add to ongoing Before calling Exec
      if(!params.signClientProposals) txn = msg.release_txn();
      if(params.signClientProposals) *txn->mutable_txndigest() = txnDigest; //HACK to include txnDigest to lookup signed_tx.

      ongoingMap::accessor b;
      //std::cerr << "ONGOING INSERT (Fallback): " << BytesToHex(txnDigest, 16).c_str() << " On CPU: " << sched_getcpu()<< std::endl;
      //ongoing.insert(b, std::make_pair(txnDigest, txn));
      ongoing.insert(b, txnDigest);
      b->second.txn = txn;
      b->second.num_concurrent_clients++;
      b.release();

    return;
}

void Server::ProcessProposalFB(proto::Phase1FB &msg, const TransportAddress &remote, std::string &txnDigest, proto::Transaction* txn){
  

  AddOngoing(msg, txnDigest, txn);

  //Todo: Improve efficiency if Valid: insert into P1Meta and check conditions again: If now already in P1Meta then use this existing result.
  if(!params.multiThreading || !params.signClientProposals){
    if(!CheckProposalValidity(msg, txn, txnDigest, true)){
       ongoingMap::accessor b;
       //std::cerr << "ONGOING ERASE (Fallback-INVALID): " << BytesToHex(txnDigest, 16).c_str() << " On CPU: " << sched_getcpu()<< std::endl;
       ongoing.find(b, txnDigest);
       b->second.num_concurrent_clients--;
       if(b->second.num_concurrent_clients==0){
          delete b->second.txn;
          ongoing.erase(b);
       }
       b.release();
       return;
    } 
    TryExec(msg, remote, txnDigest, txn);
  }
  else{
    auto try_exec(std::bind(&Server::TryExec, this, std::ref(msg), std::ref(remote), txnDigest, txn));
        auto f = [this, msg_ptr = &msg, txn, txnDigest, try_exec]() mutable {
            void* valid = CheckProposalValidity(*msg_ptr, txn, txnDigest, true);
            if(!valid){
              ongoingMap::accessor b;
              //std::cerr << "ONGOING ERASE (Fallback-INVALID): " << BytesToHex(txnDigest, 16).c_str() << " On CPU: " << sched_getcpu()<< std::endl;
              ongoing.find(b, txnDigest);
              b->second.num_concurrent_clients--;
              if(b->second.num_concurrent_clients==0){
                  delete b->second.txn;
                  ongoing.erase(b);
              }
              b.release();
              return (void*) false;
            } 
            
            transport->DispatchTP_main(try_exec);
            return (void*) true;
        };
        transport->DispatchTP_noCB(std::move(f));
  }
}

void* Server::TryExec(proto::Phase1FB &msg, const TransportAddress &remote, std::string &txnDigest, proto::Transaction* txn){

  const proto::CommittedProof *committedProof;
  proto::ConcurrencyControl::Result result;
  const proto::Transaction *abstain_conflict = nullptr;

  if (ExecP1(msg, remote, txnDigest, txn, result, committedProof, abstain_conflict)) { //only send if the result is not Wait
          P1FBorganizer *p1fb_organizer = new P1FBorganizer(msg.req_id(), txnDigest, remote, this);
          SetP1(msg.req_id(), p1fb_organizer->p1fbr->mutable_p1r(), txnDigest, result, committedProof, abstain_conflict);
          SendPhase1FBReply(p1fb_organizer, txnDigest);
          Debug("Sent Phase1FBReply on path ExecP1 for txn: %s, sent by client: %d", BytesToHex(txnDigest, 16).c_str(), msg.req_id());
      }
  else{
        Debug("WAITING on dep in order to send Phase1FBReply on path ExecP1 for txn: %s, sent by client: %d", BytesToHex(txnDigest, 16).c_str(), msg.req_id());
  }
  if((params.mainThreadDispatching && (!params.dispatchMessageReceive || params.parallel_CCC)) || (params.multiThreading && params.signClientProposals)) FreePhase1FBmessage(&msg); 

  return (void*) true;
}

//TODO: Should now be fully safe to be executed multithread (parallel_CCC)
//TODO: merge this code with the normal case operation.
//p1MetaDataMap::accessor &c, 
bool Server::ExecP1(proto::Phase1FB &msg, const TransportAddress &remote,
  const std::string &txnDigest, proto::Transaction *txn, proto::ConcurrencyControl::Result &result,
  const proto::CommittedProof* &committedProof, const proto::Transaction *abstain_conflict){
  Debug("FB exec PHASE1[%lu:%lu][%s] with ts %lu.", txn->client_id(),
     txn->client_seq_num(), BytesToHex(txnDigest, 16).c_str(),
     txn->timestamp().timestamp());

  //start new current view
  // current_views[txnDigest] = 0;
  // p2MetaDataMap::accessor p;
  // p2MetaDatas.insert(p, txnDigest);
  // p.release();

  // if(!params.signClientProposals) txn = msg.release_txn();
  // *txn->mutable_txndigest() = txnDigest; //HACK to include txnDigest to lookup signed_tx.
  
  // ongoingMap::accessor b;
  // std::cerr << "ONGOING INSERT (Fallback): " << BytesToHex(txnDigest, 16).c_str() << " On CPU: " << sched_getcpu()<< std::endl;
  // //ongoing.insert(b, std::make_pair(txnDigest, txn));
  // ongoing.insert(b, txnDigest);
  // b->second.txn = txn;
  // b->second.num_concurrent_clients++;
  // b.release();
  //fallback.insert(txnDigest);
  //std::cerr << "[FB] Added tx to ongoing: " << BytesToHex(txnDigest, 16) << std::endl;

  Timestamp retryTs;

  //TODO: add parallel OCC check logic here:
  result = DoOCCCheck(msg.req_id(),
      remote, txnDigest, *txn, retryTs, committedProof, abstain_conflict, true);

  //std::cerr << "Exec P1 called, for txn: " << BytesToHex(txnDigest, 64) << std::endl;
  BufferP1Result(result, committedProof, txnDigest, 1);
  //std::cerr << "FB: Buffered result:" << result << " for txn: " << BytesToHex(txnDigest, 64) << std::endl;


  //What happens in the FB case if the result is WAIT?
  //Since we limit to depth 1, we expect this to not be possible.
  //But if it happens, the CheckDependents call will send a P1FB reply to all interested clients.
  if (result == proto::ConcurrencyControl::WAIT) return false; //Dont use p1 result if its Wait.

  //BufferP1Result(result, committedProof, txnDigest);
  // if(client_starttime.find(txnDigest) == client_starttime.end()){
  //   struct timeval tv;
  //   gettimeofday(&tv, NULL);
  //   uint64_t start_time = (tv.tv_sec*1000000+tv.tv_usec)/1000;  //in miliseconds
  //   client_starttime[txnDigest] = start_time;
  // }
  return true;
}


void Server::SetP1(uint64_t reqId, proto::Phase1Reply *p1Reply, const std::string &txnDigest, proto::ConcurrencyControl::Result &result,
  const proto::CommittedProof *conflict, const proto::Transaction *abstain_conflict){
  //proto::Phase1Reply *p1Reply = p1fb_organizer->p1fbr->mutable_p1r();

  p1Reply->set_req_id(reqId);
  p1Reply->mutable_cc()->set_ccr(result);
  if (params.validateProofs) {
    *p1Reply->mutable_cc()->mutable_txn_digest() = txnDigest;
    p1Reply->mutable_cc()->set_involved_group(groupIdx);
    if (result == proto::ConcurrencyControl::ABORT) {
      *p1Reply->mutable_cc()->mutable_committed_conflict() = *conflict;
    }
  }
  //if(abstain_conflict != nullptr) *p1Reply->mutable_abstain_conflict() = *abstain_conflict;

  if(abstain_conflict != nullptr){
    //Panic("setting abstain_conflict");
     //p1Reply->mutable_abstain_conflict()->set_req_id(0);
    if(params.signClientProposals){
      p1MetaDataMap::accessor c;
      bool p1MetaExists = p1MetaData.find(c, abstain_conflict->txndigest());
      if(p1MetaExists && c->second.hasSignedP1){ //only send a abstain conflict if the signed message still exists -- if it does not, then the conflict in question has already committed/aborted
        p1Reply->mutable_abstain_conflict()->set_req_id(0);
        *p1Reply->mutable_abstain_conflict()->mutable_signed_txn() = *c->second.signed_txn;
      } 
      c.release();
    }
    else{ //TODO: ideally update to check whether ongoing still exists or not before sending
      p1Reply->mutable_abstain_conflict()->set_req_id(0);
      *p1Reply->mutable_abstain_conflict()->mutable_txn() = *abstain_conflict;
    }
  
 }

}

void Server::SetP2(uint64_t reqId, proto::Phase2Reply *p2Reply, const std::string &txnDigest, proto::CommitDecision &decision, uint64_t decision_view){
  //proto::Phase2Reply *p2Reply = p1fb_organizer->p1fbr->mutable_p2r();
  p2Reply->set_req_id(reqId);
  p2Reply->mutable_p2_decision()->set_decision(decision);

  //add decision view
  // if(decision_views.find(txnDigest) == decision_views.end()) decision_views[txnDigest] = 0;
  p2Reply->mutable_p2_decision()->set_view(decision_view);

  if (params.validateProofs) {
    *p2Reply->mutable_p2_decision()->mutable_txn_digest() = txnDigest;
    p2Reply->mutable_p2_decision()->set_involved_group(groupIdx);
  }
}

//TODO: add a way to buffer this message/organizer until commit/abort
//So that only the first interested client ever creates the object.
//XXX need to keep changing p2 and current views though...
void Server::SendPhase1FBReply(P1FBorganizer *p1fb_organizer, const std::string &txnDigest, bool multi) {

    proto::Phase1FBReply *p1FBReply = p1fb_organizer->p1fbr;
    if(p1FBReply->has_wb()){
      transport->SendMessage(this, *p1fb_organizer->remote, *p1FBReply);
      delete p1fb_organizer;
    }

    proto::AttachedView *attachedView = p1FBReply->mutable_attached_view();
    if(!params.all_to_all_fb){
      uint64_t current_view;
      LookupCurrentView(txnDigest, current_view);
      //std::cerr << "SendPhase1FBreply: Lookup current view " << current_view << " for txn:" << BytesToHex(txnDigest, 16) << std::endl;
      attachedView->mutable_current_view()->set_current_view(current_view);
      attachedView->mutable_current_view()->set_txn_digest(txnDigest);
      attachedView->mutable_current_view()->set_replica_id(id);
    }

    auto sendCB = [this, p1fb_organizer, multi](){
      if(p1fb_organizer->c_view_sig_outstanding || p1fb_organizer->p1_sig_outstanding || p1fb_organizer->p2_sig_outstanding){
        p1fb_organizer->sendCBmutex.unlock();
        Debug("Not all message components of Phase1FBreply are signed: CurrentView: %s, P1R: %s, P2R: %s.",
          p1fb_organizer->c_view_sig_outstanding ? "outstanding" : "complete",
          p1fb_organizer->p1_sig_outstanding ? "outstanding" : "complete",
          p1fb_organizer->p2_sig_outstanding ? "outstanding" : "complete");
        return;
      }
      Debug("All message components of Phase1FBreply signed. Sending.");
      p1fb_organizer->sendCBmutex.unlock();
      if(!multi){
          transport->SendMessage(this, *p1fb_organizer->remote, *p1fb_organizer->p1fbr);
      }
      else{
        interestedClientsMap::const_accessor i;
        bool has_interested = interestedClients.find(i, p1fb_organizer->p1fbr->txn_digest());
        if(has_interested){
          for (const auto client : i->second) {
            transport->SendMessage(this, *client.second, *p1fb_organizer->p1fbr);
          }
        }
        i.release();
      }
      delete p1fb_organizer;
    };


    if (params.signedMessages) {
      p1fb_organizer->sendCBmutex.lock();

      //First, "atomically" set the outstanding flags. (Need to do this before dispatching anything)
      if(p1FBReply->has_p1r() && p1FBReply->p1r().cc().ccr() != proto::ConcurrencyControl::ABORT){
        Debug("FB sending P1 result:[%d] for txn: %s", p1FBReply->p1r().cc().ccr(), BytesToHex(txnDigest, 16).c_str());
        p1fb_organizer->p1_sig_outstanding = true;
      }
      if(p1FBReply->has_p2r()){
        Debug("FB sending P2 result:[%d] for txn: %s", p1FBReply->p2r().p2_decision().decision(), BytesToHex(txnDigest, 16).c_str());
        p1fb_organizer->p2_sig_outstanding = true;
      }
      //Next, dispatch respective signature functions
      //TODO: Improve code to buffer previous signatures, instead of always re-generating.

      //1) sign current view
      if(!params.all_to_all_fb){
        p1fb_organizer->c_view_sig_outstanding = true;
        proto::CurrentView *cView = new proto::CurrentView(attachedView->current_view());
        MessageToSign(cView, attachedView->mutable_signed_current_view(),
        [sendCB, p1fb_organizer, cView](){
            Debug("Finished signing CurrentView for Phase1FBreply.");
            p1fb_organizer->sendCBmutex.lock();
            p1fb_organizer->c_view_sig_outstanding = false;
            sendCB(); //lock is unlocked in here...
            delete cView;
          });
      }
      //2) sign p1
      if(p1FBReply->has_p1r() && p1FBReply->p1r().cc().ccr() != proto::ConcurrencyControl::ABORT){
        proto::ConcurrencyControl* cc = new proto::ConcurrencyControl(p1FBReply->p1r().cc());
        MessageToSign(cc, p1FBReply->mutable_p1r()->mutable_signed_cc(), [sendCB, p1fb_organizer, cc](){
            Debug("Finished signing P1R for Phase1FBreply.");
            p1fb_organizer->sendCBmutex.lock();
            p1fb_organizer->p1_sig_outstanding = false;
            sendCB(); //lock is unlocked in here...
            delete cc;
          });
      }
      //3) sign p2
      if(p1FBReply->has_p2r()){
        proto::Phase2Decision* p2Decision = new proto::Phase2Decision(p1FBReply->p2r().p2_decision());
        MessageToSign(p2Decision, p1FBReply->mutable_p2r()->mutable_signed_p2_decision(),
        [sendCB, p1fb_organizer, p2Decision](){
            Debug("Finished signing P2R for Phase1FBreply.");
            p1fb_organizer->sendCBmutex.lock();
            p1fb_organizer->p2_sig_outstanding = false;
            sendCB(); //lock is unlocked in here...
            delete p2Decision;
          });
      }
      p1fb_organizer->sendCBmutex.unlock();
    }

    else{
      p1fb_organizer->sendCBmutex.lock(); //just locking in order to support the unlock in sendCB
      sendCB(); //lock is unlocked in here...
    }
}

void Server::HandlePhase2FB(const TransportAddress &remote,
    const proto::Phase2FB &msg) {

  //std::string txnDigest = TransactionDigest(msg.txn(), params.hashDigest);
  const std::string &txnDigest = msg.txn_digest();
  Debug("Received PHASE2FB[%lu][%s] from client %lu. This is server %lu", msg.req_id(), BytesToHex(txnDigest, 16).c_str(), msg.client_id(), id);
  //Debug("PHASE2FB[%lu:%lu][%s] with ts %lu.", msg.txn().client_id(),
  //    msg.txn().client_seq_num(), BytesToHex(txnDigest, 16).c_str(),
  //    msg.txn().timestamp().timestamp());

  //TODO: change to multi and delete all interested clients?
  if(ForwardWriteback(remote, msg.req_id(), txnDigest)){
    if(params.multiThreading || (params.mainThreadDispatching && !params.dispatchMessageReceive)){
      FreePhase2FBmessage(&msg); //const_cast<proto::Phase2&>(msg));
    }
    return;
  }

  p2MetaDataMap::const_accessor p;
  p2MetaDatas.insert(p, txnDigest);
  bool hasP2 = p->second.hasP2;
  // HandePhase2 just returns an existing decision.
  if(hasP2){
      proto::CommitDecision decision = p->second.p2Decision;
      uint64_t decision_view = p->second.decision_view;
      p.release();

      P2FBorganizer *p2fb_organizer = new P2FBorganizer(msg.req_id(), txnDigest, remote, this);
      SetP2(msg.req_id(), p2fb_organizer->p2fbr->mutable_p2r() ,txnDigest, decision, decision_view);
      SendPhase2FBReply(p2fb_organizer, txnDigest);
      Debug("PHASE2FB[%s] Sent Phase2Reply with stored decision.", BytesToHex(txnDigest, 16).c_str());

      if(params.multiThreading || (params.mainThreadDispatching && !params.dispatchMessageReceive)){
        FreePhase2FBmessage(&msg); //const_cast<proto::Phase2&>(msg));
      }
      return;
  }
  //just do normal handle p2 otherwise after timeout
  else{
      p.release();
      //TODO: start a timer after mvtso check returns with != WAIT. That timer sets a bool,
      //When the bool is set then Handle all P2 requests. (assuming relay only starts after the timeout anyways
      // then this is not necessary to run a simulation - but it would not be robust to byz clients)

      //The timer should start running AFTER the Mvtso check returns.
      // I could make the timeout window 0 if I dont expect byz clients. An honest client will likely only ever start this on conflict.
      //std::chrono::high_resolution_clock::time_point current_time = high_resolution_clock::now();

      //TODO: call HandleP2FB again instead
      ProcessP2FB(remote, txnDigest, msg);
      //transport->Timer((CLIENTTIMEOUT), [this, &remote, &txnDigest, &msg](){ProcessP2FB(remote, txnDigest, msg);});

//TODO: for time being dont do this smarter scheduling.
// if(false){
//       struct timeval tv;
//       gettimeofday(&tv, NULL);
//       uint64_t current_time = (tv.tv_sec*1000000+tv.tv_usec)/1000;  //in miliseconds
//
//
//       //std::time_t current_time;
//       //std::chrono::high_resolution_clock::time_point
//       uint64_t elapsed;
//       if(client_starttime.find(txnDigest) != client_starttime.end())
//           elapsed = current_time - client_starttime[txnDigest];
//       else{
//         //PANIC, have never seen the tx that is mentioned. Start timer ourselves.
//         client_starttime[txnDigest] = current_time;
//         transport->Timer((CLIENTTIMEOUT), [this, &remote, &txnDigest, &msg](){VerifyP2FB(remote, txnDigest, msg);});
//         return;
//
//       }
//
// 	    //current_time = time(NULL);
//       //std::time_t elapsed = current_time - FBclient_timeouts[txnDigest];
//       //TODO: Replay this toy time logic with proper MS timer.
//       if (elapsed >= CLIENTTIMEOUT){
//         VerifyP2FB(remote, txnDigest, msg);
//       }
//       else{
//         //schedule for once original client has timed out.
//         transport->Timer((CLIENTTIMEOUT-elapsed), [this, &remote, &txnDigest, &msg](){VerifyP2FB(remote, txnDigest, msg);});
//         return;
//       }
//  }
    }
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

//TODO: Refactor both P1Reply and P2Reply into a single message and remove all redundant functions.
void Server::SendPhase2FBReply(P2FBorganizer *p2fb_organizer, const std::string &txnDigest, bool multi, bool sub_original) {

    proto::Phase2FBReply *p2FBReply = p2fb_organizer->p2fbr;

    proto::AttachedView *attachedView = p2FBReply->mutable_attached_view();
    if(!params.all_to_all_fb){
      uint64_t current_view;
      LookupCurrentView(txnDigest, current_view);
      attachedView->mutable_current_view()->set_current_view(current_view);
      attachedView->mutable_current_view()->set_txn_digest(txnDigest);
      attachedView->mutable_current_view()->set_replica_id(id);
    }

    auto sendCB = [this, p2fb_organizer, multi, sub_original](){
      if(p2fb_organizer->c_view_sig_outstanding || p2fb_organizer->p2_sig_outstanding){
        p2fb_organizer->sendCBmutex.unlock();
        return;
      }
      p2fb_organizer->sendCBmutex.unlock();
      if(sub_original){ //XXX sending normal p2 to subscribed original client.
        //std::cerr << "Sending to subscribed original" << std::endl;
        transport->SendMessage(this, *p2fb_organizer->original, p2fb_organizer->p2fbr->p2r());
        //std::cerr << "Sending to subscribed original success" << std::endl;
      }
      if(!multi){
          transport->SendMessage(this, *p2fb_organizer->remote, *p2fb_organizer->p2fbr);
      }
      else{
        interestedClientsMap::const_accessor i;
        bool has_interested = interestedClients.find(i, p2fb_organizer->p2fbr->txn_digest());
        //std::cerr << "Number of interested clients: " << i->second.size() << std::endl;
        if(has_interested){
          for (const auto client : i->second) {
            transport->SendMessage(this, *client.second, *p2fb_organizer->p2fbr);
          }
        }
        i.release();
      }
      delete p2fb_organizer;
    };

    if (params.signedMessages) {
      p2fb_organizer->sendCBmutex.lock();
      //First, "atomically" set the outstanding flags. (Need to do this before dispatching anything)
      if(p2FBReply->has_p2r()){
        p2fb_organizer->p2_sig_outstanding = true;
      }
      //Next, dispatch respective signature functions

      //1) sign current view
      if(!params.all_to_all_fb){
        p2fb_organizer->c_view_sig_outstanding = true;
        proto::CurrentView *cView = new proto::CurrentView(attachedView->current_view());
        MessageToSign(cView, attachedView->mutable_signed_current_view(),
        [sendCB, p2fb_organizer, cView](){
            p2fb_organizer->sendCBmutex.lock();
            p2fb_organizer->c_view_sig_outstanding = false;
            sendCB();
            delete cView;
          });
      }
      //2) sign p2
      if(p2FBReply->has_p2r()){
        proto::Phase2Decision* p2Decision = new proto::Phase2Decision(p2FBReply->p2r().p2_decision());
        MessageToSign(p2Decision, p2FBReply->mutable_p2r()->mutable_signed_p2_decision(),
        [sendCB, p2fb_organizer, p2Decision](){
            p2fb_organizer->sendCBmutex.lock();
            p2fb_organizer->p2_sig_outstanding = false;
            sendCB();
            delete p2Decision;
          });
      }
      p2fb_organizer->sendCBmutex.unlock();
    }

    else{
      p2fb_organizer->sendCBmutex.lock();
      sendCB();
    }
}


void Server::ProcessP2FB(const TransportAddress &remote, const std::string &txnDigest, const proto::Phase2FB &p2fb){
  //Shotcircuit if request already processed once.
  if(ForwardWriteback(remote, 0, txnDigest)){
    if(params.multiThreading || (params.mainThreadDispatching && !params.dispatchMessageReceive)){
      FreePhase2FBmessage(&p2fb); //const_cast<proto::Phase2&>(msg));
    }
    return;
  }

  // returns an existing decision.
  p2MetaDataMap::const_accessor p;
  p2MetaDatas.insert(p, txnDigest);
  bool hasP2 = p->second.hasP2;
  // HandePhase2 just returns an existing decision.
  if(hasP2){
      proto::CommitDecision decision = p->second.p2Decision;
      uint64_t decision_view = p->second.decision_view;
      p.release();

      P2FBorganizer *p2fb_organizer = new P2FBorganizer(0, txnDigest, remote, this);
      SetP2(0, p2fb_organizer->p2fbr->mutable_p2r() ,txnDigest, decision, decision_view);
      SendPhase2FBReply(p2fb_organizer, txnDigest);
      Debug("PHASE2FB[%s] Sent Phase2Reply with stored decision.", BytesToHex(txnDigest, 16).c_str());
      if(params.multiThreading || (params.mainThreadDispatching && !params.dispatchMessageReceive)){
        FreePhase2FBmessage(&p2fb); //const_cast<proto::Phase2&>(msg));
      }
      return;
  }
  p.release();

  uint8_t groupIndex = txnDigest[0];
  const proto::Transaction *txn;
  // find txn. that maps to txnDigest. if not stored locally, and not part of the msg, reject msg.
  ongoingMap::const_accessor b;
  bool isOngoing = ongoing.find(b, txnDigest);
  if(isOngoing){
    txn = b->second.txn;
    b.release();
  }
  else{
    b.release();
    if(p2fb.has_txn()){
        txn = &p2fb.txn();
      }
      else{
         Debug("Txn[%s] neither in ongoing nor in FallbackP2 message.", BytesToHex(txnDigest, 64).c_str());
         if(params.multiThreading || (params.mainThreadDispatching && !params.dispatchMessageReceive)){
           FreePhase2FBmessage(&p2fb); //const_cast<proto::Phase2&>(msg));
         }
        return;
      }
  }

  //P2FB either contains P1 GroupedSignatures, OR it just contains forwarded P2Replies.
  // Case A: The FbP2 message has f+1 matching P2replies from logShard replicas
  if(p2fb.has_p2_replies()){
    Debug("ProcessP2FB verifying p2 replies for txn[%s]", BytesToHex(txnDigest, 64).c_str());
    if(params.signedMessages){
      mainThreadCallback mcb(std::bind(&Server::ProcessP2FBCallback, this,
         &p2fb, txnDigest, remote.clone(), std::placeholders::_1));
      int64_t myProcessId;
      proto::CommitDecision myDecision;
      LookupP2Decision(txnDigest, myProcessId, myDecision);
      asyncValidateFBP2Replies(p2fb.decision(), txn, &txnDigest, p2fb.p2_replies(),
         keyManager, &config, myProcessId, myDecision, verifier, std::move(mcb), transport, params.multiThreading);
      return;
    }
    else{
      proto::P2Replies p2Reps = p2fb.p2_replies();
      uint32_t counter = config.f + 1;
      for(auto & p2_reply : p2Reps.p2replies()){
        if(p2_reply.has_p2_decision()){
          if(p2_reply.p2_decision().decision() == p2fb.decision() && p2_reply.p2_decision().txn_digest() == p2fb.txn_digest()){
            counter--;
          }
        }
        if(counter == 0){
          ProcessP2FBCallback(&p2fb, txnDigest, &remote, (void*) true);
          return;
        }
      }
      ProcessP2FBCallback(&p2fb, txnDigest, &remote, (void*) false);
    }

  }
  // Case B: The FbP2 message has standard P1 Quorums that match the decision
  else if(p2fb.has_p1_sigs()){
    Debug("ProcessP2FB verify p1 sigs for txn[%s]", BytesToHex(txnDigest, 64).c_str());

      const proto::GroupedSignatures &grpSigs = p2fb.p1_sigs();
      int64_t myProcessId;
      proto::ConcurrencyControl::Result myResult;
      LookupP1Decision(txnDigest, myProcessId, myResult);

      if(params.multiThreading){
        mainThreadCallback mcb(std::bind(&Server::ProcessP2FBCallback, this,
           &p2fb, txnDigest, remote.clone(), std::placeholders::_1));
           asyncValidateP1Replies(p2fb.decision(),
                 false, txn, &txnDigest, grpSigs, keyManager, &config, myProcessId,
                 myResult, verifier, std::move(mcb), transport, true);
           return;
      }
      else{
        bool valid = ValidateP1Replies(p2fb.decision(), false, txn, &txnDigest, grpSigs, keyManager, &config, myProcessId, myResult, verifier);
        ProcessP2FBCallback(&p2fb, txnDigest, &remote, (void*) valid);
        return;
      }
  }
  else{
    Debug("FallbackP2 message for Txn[%s] has no proofs.", BytesToHex(txnDigest, 64).c_str());
    if(params.multiThreading || (params.mainThreadDispatching && !params.dispatchMessageReceive)){
      FreePhase2FBmessage(&p2fb); //const_cast<proto::Phase2&>(msg));
    }
    return;
  }
  ProcessP2FBCallback(&p2fb, txnDigest, &remote, (void*) true); //should never be called
}

void Server::ProcessP2FBCallback(const proto::Phase2FB *p2fb, const std::string &txnDigest,
  const TransportAddress *remote, void* valid){

    if(!valid || ForwardWriteback(*remote, 0, txnDigest)){
      if(params.multiThreading || (params.mainThreadDispatching && !params.dispatchMessageReceive)){
        FreePhase2FBmessage(p2fb);
      }
      if(params.multiThreading) delete remote;
      return;
    }

    proto::CommitDecision decision;
    uint64_t decision_view;

    p2MetaDataMap::accessor p;
    p2MetaDatas.insert(p, txnDigest);
    bool hasP2 = p->second.hasP2;
    if(hasP2){
      decision = p->second.p2Decision;
      decision_view = p->second.decision_view;
    }
    else{
      p->second.p2Decision = p2fb->decision();
      p->second.hasP2 = true;
      p->second.decision_view = 0;
      decision = p2fb->decision();
      decision_view = 0;
    }
    p.release();


    P2FBorganizer *p2fb_organizer = new P2FBorganizer(0, txnDigest, *remote, this);
    SetP2(0, p2fb_organizer->p2fbr->mutable_p2r() ,txnDigest, decision, decision_view);
    SendPhase2FBReply(p2fb_organizer, txnDigest);

    // TODO: could also instantiate the p2fb_org object earlier, and delete if false
    if(params.multiThreading || (params.mainThreadDispatching && !params.dispatchMessageReceive)){
      FreePhase2FBmessage(p2fb);
    }
    if(params.multiThreading) delete remote;
    Debug("PHASE2FB[%s] Sent Phase2Reply.", BytesToHex(txnDigest, 16).c_str());
}

void Server::SendView(const TransportAddress &remote, const std::string &txnDigest){

  //std::cerr << "Called SendView for txn " << BytesToHex(txnDigest, 16) << std::endl;

  proto::SendView *sendView = GetUnusedSendViewMessage();
  sendView->set_req_id(0);
  sendView->set_txn_digest(txnDigest);

  proto::AttachedView *attachedView = sendView->mutable_attached_view();

  uint64_t current_view;
  LookupCurrentView(txnDigest, current_view);
  attachedView->mutable_current_view()->set_current_view(current_view);
  attachedView->mutable_current_view()->set_txn_digest(txnDigest);
  attachedView->mutable_current_view()->set_replica_id(id);

  proto::CurrentView *cView = new proto::CurrentView(attachedView->current_view());
  const TransportAddress *remoteCopy = remote.clone();
  MessageToSign(cView, attachedView->mutable_signed_current_view(),
  [this, sendView, remoteCopy, cView](){
      transport->SendMessage(this, *remoteCopy, *sendView);
      delete cView;
      delete remoteCopy;
      FreeSendViewMessage(sendView);
    });

  //std::cerr << "Dispatched SendView for view " << current_view << " for txn: " << BytesToHex(txnDigest, 16) << std::endl;
  return;
}


//TODO remove remote argument, it is useless here. Instead add and keep track of INTERESTED CLIENTS REMOTE MAP
//TODO  Schedule request for when current leader timeout is complete --> check exp timeouts; then set new one.
//   struct timeval tv;
//   gettimeofday(&tv, NULL);
//   uint64_t current_time = (tv.tv_sec*1000000+tv.tv_usec)/1000;  //in miliseconds
//
//   uint64_t elapsed;
//   if(client_starttime.find(txnDigest) != client_starttime.end())
//       elapsed = current_time - client_starttime[txnDigest];
//   else{
//     //PANIC, have never seen the tx that is mentioned. Start timer ourselves.
//     client_starttime[txnDigest] = current_time;
//     transport->Timer((CLIENTTIMEOUT), [this, &remote, &msg](){HandleInvokeFB(remote, msg);});
//     return;
//   }
//   if (elapsed < CLIENTTIMEOUT ){
//     transport->Timer((CLIENTTIMEOUT-elapsed), [this, &remote, &msg](){HandleInvokeFB(remote, msg);});
//     return;
//   }
// //check for current FB reign
//   uint64_t FB_elapsed;
//   if(exp_timeouts.find(txnDigest) != exp_timeouts.end()){
//       FB_elapsed = current_time - FBtimeouts_start[txnDigest];
//       if(FB_elapsed < exp_timeouts[txnDigest]){
//           transport->Timer((exp_timeouts[txnDigest]-FB_elapsed), [this, &remote, &msg](){HandleInvokeFB(remote, msg);});
//           return;
//       }
//
//   }
//otherwise pass and invoke for the first time!
void Server::HandleInvokeFB(const TransportAddress &remote, proto::InvokeFB &msg) {


    // CHECK if part of logging shard. (this needs to be done at all p2s, reject if its not ourselves)
    const std::string &txnDigest = msg.txn_digest();

    Debug("Received InvokeFB request for txn: %s", BytesToHex(txnDigest, 64).c_str());
    stats.Increment("total_equiv_received_invoke", 1);
    //std::cerr << "HandleInvokeFB with proposed view:" << msg.proposed_view() << " for txn: " << BytesToHex(txnDigest, 16) << std::endl;

    if(ForwardWriteback(remote, msg.req_id(), txnDigest)){
      if( (!params.all_to_all_fb && params.multiThreading) || (params.mainThreadDispatching && !params.dispatchMessageReceive)) FreeInvokeFBmessage(&msg);
      return;
    }

    p2MetaDataMap::const_accessor p;
    p2MetaDatas.insert(p, txnDigest);
    uint64_t current_view = p->second.current_view;

    if(!params.all_to_all_fb && msg.proposed_view() <= current_view){
      p.release();
      Debug("Proposed view %lu < current view %lu. Sending updated view for txn: %s", msg.proposed_view(), current_view, BytesToHex(txnDigest, 64).c_str());
      SendView(remote, txnDigest);
      if( (!params.all_to_all_fb && params.multiThreading) || (params.mainThreadDispatching && !params.dispatchMessageReceive)) FreeInvokeFBmessage(&msg);
      return; //Obsolete Invoke Message, send newer view
    }

    //process decision if one does not have any yet.
    //This is safe even if current_view > 0 because this replica could not have taken part in any elections yet (can only elect once you have decision), nor has yet received a dec from a larger view which it would adopt.
    if(!p->second.hasP2){
        p.release();
        if(!msg.has_p2fb()){
          if( (!params.all_to_all_fb && params.multiThreading) || (params.mainThreadDispatching && !params.dispatchMessageReceive)) FreeInvokeFBmessage(&msg);
          Debug("Transaction[%s] has no phase2 decision yet needs to SendElectFB", BytesToHex(txnDigest, 64).c_str());
          return;
        }
        const proto::Phase2FB *p2fb = msg.release_p2fb();
        InvokeFBProcessP2FB(remote, txnDigest, *p2fb, &msg);
        return;
    }

    proto::CommitDecision decision = p->second.p2Decision;
    p.release();


    // find txn. that maps to txnDigest. if not stored locally, and not part of the msg, reject msg.
    const proto::Transaction *txn;
    ongoingMap::const_accessor b;
    bool isOngoing = ongoing.find(b, txnDigest);
    if(isOngoing){
        txn = b->second.txn;
    }
    else if(msg.has_p2fb()){
        const proto::Phase2FB &p2fb = msg.p2fb();
        if(p2fb.has_txn()){
            txn = &p2fb.txn();
          }
        else{
          if( (!params.all_to_all_fb && params.multiThreading) || (params.mainThreadDispatching && !params.dispatchMessageReceive)) FreeInvokeFBmessage(&msg);
          return; //REPLICA HAS NEVER SEEN THIS TXN
        }
    }
    else{
        if( (!params.all_to_all_fb && params.multiThreading) || (params.mainThreadDispatching && !params.dispatchMessageReceive)) FreeInvokeFBmessage(&msg);
        return; //REPLICA HAS NEVER SEEN THIS TXN
    }
    b.release();

    int64_t logGrp = GetLogGroup(*txn, txnDigest);
    if(groupIdx != logGrp){
          if( (!params.all_to_all_fb && params.multiThreading) || (params.mainThreadDispatching && !params.dispatchMessageReceive)) FreeInvokeFBmessage(&msg);
          return;  //This replica is not part of the shard responsible for Fallback.
    }

    if(params.all_to_all_fb){
      uint64_t proposed_view = current_view + 1; //client does not propose view.
      Debug("txn[%s] in current view: %lu, proposing view:", BytesToHex(txnDigest, 64).c_str(), current_view, proposed_view);
      ProcessMoveView(txnDigest, proposed_view, true);
      SendElectFB(&msg, txnDigest, proposed_view, decision, logGrp); //can already send before moving to view since we do not skip views during synchrony (even when no correct clients interested)
    }
    else{
      //verify views & Send ElectFB
      VerifyViews(msg, logGrp, remote);
    }
}

//TODO: merge with normal ProcessP2FB
void Server::InvokeFBProcessP2FB(const TransportAddress &remote, const std::string &txnDigest, const proto::Phase2FB &p2fb, proto::InvokeFB *msg){

  //std::cerr << "InvokeFBProcessP2FB for txn: " << BytesToHex(txnDigest, 16) << std::endl;
  Debug("Processing P2FB before processing InvokeFB request for txn: %s", BytesToHex(txnDigest, 64).c_str());
  // find txn. that maps to txnDigest. if not stored locally, and not part of the msg, reject msg.
  const proto::Transaction *txn;
  ongoingMap::const_accessor b;
  bool isOngoing = ongoing.find(b, txnDigest);
  if(isOngoing){
    txn = b->second.txn;
    b.release();
  }
  else{
    b.release();
    if(p2fb.has_txn()){
        txn = &p2fb.txn();
      }
      else{
         Debug("Txn[%s] neither in ongoing nor in FallbackP2 message.", BytesToHex(txnDigest, 64).c_str());
         if( (!params.all_to_all_fb && params.multiThreading) || (params.mainThreadDispatching && !params.dispatchMessageReceive)) FreeInvokeFBmessage(msg);
         if(params.multiThreading || (params.mainThreadDispatching && !params.dispatchMessageReceive)){
           FreePhase2FBmessage(&p2fb); //const_cast<proto::Phase2&>(msg));
         }
        return;
      }
  }
  int64_t logGrp = GetLogGroup(*txn, txnDigest);
  if(groupIdx != logGrp){
        if( (!params.all_to_all_fb && params.multiThreading) || (params.mainThreadDispatching && !params.dispatchMessageReceive)) FreeInvokeFBmessage(msg);
        if(params.multiThreading || (params.mainThreadDispatching && !params.dispatchMessageReceive)){
          FreePhase2FBmessage(&p2fb); //const_cast<proto::Phase2&>(msg));
        }
        return;  //This replica is not part of the shard responsible for Fallback.
  }
  //P2FB either contains P1 GroupedSignatures, OR it just contains forwarded P2Replies.
  // Case A: The FbP2 message has f+1 matching P2replies from logShard replicas
  if(p2fb.has_p2_replies()){
    if(params.signedMessages){
      mainThreadCallback mcb(std::bind(&Server::InvokeFBProcessP2FBCallback, this,
         msg, &p2fb, txnDigest, remote.clone(), std::placeholders::_1));
      int64_t myProcessId;
      proto::CommitDecision myDecision;
      LookupP2Decision(txnDigest, myProcessId, myDecision);
      asyncValidateFBP2Replies(p2fb.decision(), txn, &txnDigest, p2fb.p2_replies(),
         keyManager, &config, myProcessId, myDecision, verifier, std::move(mcb), transport, params.multiThreading);
      return;
    }
    else{
      proto::P2Replies p2Reps = p2fb.p2_replies();
      uint32_t counter = config.f + 1;
      for(auto & p2_reply : p2Reps.p2replies()){
        if(p2_reply.has_p2_decision()){
          if(p2_reply.p2_decision().decision() == p2fb.decision() && p2_reply.p2_decision().txn_digest() == p2fb.txn_digest()){
            counter--;
          }
        }
        if(counter == 0){
          InvokeFBProcessP2FBCallback(msg, &p2fb, txnDigest, &remote, (void*) true);
          return;
        }
      }
      InvokeFBProcessP2FBCallback(msg, &p2fb, txnDigest, &remote, (void*) false);
    }
  }
  // Case B: The FbP2 message has standard P1 Quorums that match the decision
  else if(p2fb.has_p1_sigs()){
      const proto::GroupedSignatures &grpSigs = p2fb.p1_sigs();
      int64_t myProcessId;
      proto::ConcurrencyControl::Result myResult;
      LookupP1Decision(txnDigest, myProcessId, myResult);

      if(params.multiThreading){
        mainThreadCallback mcb(std::bind(&Server::InvokeFBProcessP2FBCallback, this,
           msg, &p2fb, txnDigest, remote.clone(), std::placeholders::_1));
           asyncValidateP1Replies(p2fb.decision(),
                 false, txn, &txnDigest, grpSigs, keyManager, &config, myProcessId,
                 myResult, verifier, std::move(mcb), transport, true);
         return;
      }
      else{
        bool valid = ValidateP1Replies(p2fb.decision(), false, txn, &txnDigest, grpSigs, keyManager, &config, myProcessId, myResult, verifier);
        InvokeFBProcessP2FBCallback(msg, &p2fb, txnDigest, &remote, (void*) valid);
        return;
      }
  }
  else{
    Debug("FallbackP2 message for Txn[%s] has no proofs.", BytesToHex(txnDigest, 64).c_str());
    if( (!params.all_to_all_fb && params.multiThreading) || (params.mainThreadDispatching && !params.dispatchMessageReceive)) FreeInvokeFBmessage(msg);
    if(params.multiThreading || (params.mainThreadDispatching && !params.dispatchMessageReceive)){
      FreePhase2FBmessage(&p2fb); //const_cast<proto::Phase2&>(msg));
    }
    return;
  }
  //InvokeFBProcessP2FBCallback(msg, &p2fb, txnDigest, &remote, (void*) true);
}

void Server::InvokeFBProcessP2FBCallback(proto::InvokeFB *msg, const proto::Phase2FB *p2fb, const std::string &txnDigest,
  const TransportAddress *remote, void* valid){

    //std::cerr << "InvokeFBProcessP2FBCallback for txn: " << BytesToHex(txnDigest, 16) << std::endl;

    if(!valid || ForwardWriteback(*remote, 0, txnDigest)){
      if( (!params.all_to_all_fb && params.multiThreading) || (params.mainThreadDispatching && !params.dispatchMessageReceive)) FreeInvokeFBmessage(msg);
      if(params.multiThreading || (params.mainThreadDispatching && !params.dispatchMessageReceive)){
        FreePhase2FBmessage(p2fb);
      }
      if(params.multiThreading) delete remote;
      return;
    }

    p2MetaDataMap::accessor p;
    p2MetaDatas.insert(p, txnDigest);
    proto::CommitDecision decision;
    uint64_t decision_view = p->second.decision_view;
    uint64_t current_view = p->second.current_view;
    bool hasP2 = p->second.hasP2;
    if(hasP2){
      decision = p->second.p2Decision;
      //decision_view = p->second.decision_view;
    }
    else{
      p->second.p2Decision = p2fb->decision();
      p->second.hasP2 = true;
      //p->second.decision_view = 0;
      decision = p2fb->decision();
      //decision_view = 0;
    }
    p.release();

    //XXX send P2 message to client too?
    // P2FBorganizer *p2fb_organizer = new P2FBorganizer(0, txnDigest, *remote, this);
    // SetP2(0, p2fb_organizer->p2fbr->mutable_p2r() ,txnDigest, decision, decision_view);
    // SendPhase2FBReply(p2fb_organizer, txnDigest);
    // Debug("PHASE2FB[%s] Sent Phase2Reply.", BytesToHex(txnDigest, 16).c_str());

    //Call InvokeFB handling
    if(params.all_to_all_fb){
      uint64_t proposed_view = current_view + 1; //client does not propose view.
      ProcessMoveView(txnDigest, proposed_view, true);
      SendElectFB(msg, txnDigest, proposed_view, decision, groupIdx); //can already send before moving to view since we do not skip views during synchrony (even when no correct clients interested)
    }
    else{ //verify views & Send ElectFB
      VerifyViews(*msg, groupIdx, *remote);
    }

    //clean up
    if(params.multiThreading || (params.mainThreadDispatching && !params.dispatchMessageReceive)){
      FreePhase2FBmessage(p2fb);
    }
    if(params.multiThreading) delete remote;

}

void Server::VerifyViews(proto::InvokeFB &msg, uint32_t logGrp, const TransportAddress &remote){

  //Assuming Invoke Message contains SignedMessages for view instead of Signatures.
  if(!msg.has_view_signed()){
    if( (!params.all_to_all_fb && params.multiThreading) || (params.mainThreadDispatching && !params.dispatchMessageReceive)) FreeInvokeFBmessage(&msg);
    return;
  }
  const proto::SignedMessages &signed_messages = msg.view_signed();


  const std::string &txnDigest = msg.txn_digest();
  Debug("VerifyingView for txn: %s", BytesToHex(txnDigest, 64).c_str());
  uint64_t myCurrentView;
  LookupCurrentView(txnDigest, myCurrentView);

  const TransportAddress *remoteCopy = remote.clone();
  if(params.multiThreading){
    mainThreadCallback mcb(std::bind(&Server::InvokeFBcallback, this, &msg, txnDigest, msg.proposed_view(), logGrp, remoteCopy, std::placeholders::_1));
    asyncVerifyFBViews(msg.proposed_view(), msg.catchup(), logGrp, &txnDigest, signed_messages,
    keyManager, &config, id, myCurrentView, verifier, std::move(mcb), transport, params.multiThreading);
  }
  else{
    bool valid = VerifyFBViews(msg.proposed_view(), msg.catchup(), logGrp, &txnDigest, signed_messages,
    keyManager, &config, id, myCurrentView, verifier);
    InvokeFBcallback(&msg, txnDigest, msg.proposed_view(), logGrp, remoteCopy, (void*) valid);
  }

}

void Server::InvokeFBcallback(proto::InvokeFB *msg, const std::string &txnDigest, uint64_t proposed_view, uint64_t logGrp, const TransportAddress *remoteCopy, void* valid){

  //std::cerr << "InvokeFBcallback for txn:" << BytesToHex(txnDigest, 16) << std::endl;

  if(!valid || ForwardWriteback(*remoteCopy, 0, txnDigest)){
    Debug("Invalid InvokeFBcallback request for txn: %s", BytesToHex(txnDigest, 64).c_str());
    delete remoteCopy;
    if( (!params.all_to_all_fb && params.multiThreading) || (params.mainThreadDispatching && !params.dispatchMessageReceive)) FreeInvokeFBmessage(msg);
    return; //View verification failed.
  }

  Debug("Processing InvokeFBcallback for txn: %s", BytesToHex(txnDigest, 64).c_str());

  p2MetaDataMap::accessor p;
  p2MetaDatas.insert(p, txnDigest);
  uint64_t current_view = p->second.current_view;
  if(!p->second.hasP2){
    Debug("Transaction[%s] has no phase2 decision needed in order to SendElectFB", BytesToHex(txnDigest, 64).c_str());
    return;
  }

  if(!params.all_to_all_fb && current_view >= proposed_view){
    p.release();
    Debug("Decline InvokeFB[%s] as Proposed view %lu <= Current View %lu", BytesToHex(txnDigest, 64).c_str(), proposed_view, current_view);
    SendView(*remoteCopy, txnDigest);
    delete remoteCopy;
    if( (!params.all_to_all_fb && params.multiThreading) || (params.mainThreadDispatching && !params.dispatchMessageReceive)) FreeInvokeFBmessage(msg);
    return; //Obsolete Invoke Message, send newer view
  }
  p->second.current_view = proposed_view;
  proto::CommitDecision decision = p->second.p2Decision;
  p.release();

  SendElectFB(msg, txnDigest, proposed_view, decision, logGrp);

}

void Server::SendElectFB(proto::InvokeFB *msg, const std::string &txnDigest, uint64_t proposed_view, proto::CommitDecision decision, uint64_t logGrp){

  //std::cerr << "SendingElectFB for txn: " << BytesToHex(txnDigest, 16) << std::endl;
  Debug("Sending ElectFB message [decision: %s][proposed_view: %lu] for txn: %s", decision ? "ABORT" : "COMMIT", proposed_view, BytesToHex(txnDigest, 64).c_str());

  size_t replicaIdx = (proposed_view + txnDigest[0]) % config.n;
  //Form and send ElectFB message to all replicas within logging shard.
  proto::ElectFB* electFB = GetUnusedElectFBmessage();
  proto::ElectMessage* electMessage = GetUnusedElectMessage();
  electMessage->set_req_id(0);  //What req id to put here. (should i carry along message?)
  //Answer:: Should not have any. It must be consistent (0 is easiest) across all messages so that verifiation will succeed
  electMessage->set_txn_digest(txnDigest);
  electMessage->set_decision(decision);
  electMessage->set_elect_view(proposed_view);

  //SendElectFB message to proposed leader - unless it is self, then call processing directly
  //TODO: after signing, call ProcessElectFB()

    if (params.signedMessages) {
      MessageToSign(electMessage, electFB->mutable_signed_elect_fb(),
        [this, electMessage, electFB, logGrp, replicaIdx](){
          if(idx != replicaIdx) {
            this->transport->SendMessageToReplica(this, logGrp, replicaIdx, *electFB);
          }
          else{
            if(PreProcessElectFB(electMessage->txn_digest(), electMessage->elect_view(), electMessage->decision(), electFB->signed_elect_fb().process_id())){
              ProcessElectFB(electMessage->txn_digest(), electMessage->elect_view(), electMessage->decision(),
                                electFB->mutable_signed_elect_fb()->release_signature(), electFB->signed_elect_fb().process_id());
            }
          }
          FreeElectMessage(electMessage);
          FreeElectFBmessage(electFB);
        }
      );
    }
    else{
      if(idx != replicaIdx) {
        *electFB->mutable_elect_fb() = std::move(*electMessage);
        transport->SendMessageToReplica(this, logGrp, replicaIdx, *electFB);

      }
      else{
        if(PreProcessElectFB(txnDigest, proposed_view, decision, id)){
          ProcessElectFB(txnDigest, proposed_view, decision, nullptr, id); //TODO: add non-signed version
        }
      }
      FreeElectMessage(electMessage);
      FreeElectFBmessage(electFB); //must free in this order.
    }



  if( (!params.all_to_all_fb && params.multiThreading) || (params.mainThreadDispatching && !params.dispatchMessageReceive)) FreeInvokeFBmessage(msg);
  //XXX Set Fallback timeouts new.
  // gettimeofday(&tv, NULL);
  // current_time = (tv.tv_sec*1000000+tv.tv_usec)/1000;
  // if(exp_timeouts.find(txnDigest) == exp_timeouts.end()){
  //    exp_timeouts[txnDigest] = CLIENTTIMEOUT; //start first timeout. //TODO: Make this a config parameter.
  //    FBtimeouts_start[txnDigest] = current_time;
  // }
  // else{
  //    exp_timeouts[txnDigest] = exp_timeouts[txnDigest] * 2; //TODO: increase timeouts exponentially. SET INCREASE RATIO IN CONFIG
  //    FBtimeouts_start[txnDigest] = current_time;
  // }
  //(TODO 4) Send MoveView message for new view to all other replicas)
}


bool Server::PreProcessElectFB(const std::string &txnDigest, uint64_t elect_view, proto::CommitDecision decision, uint64_t process_id){
  //create management object (if necessary) and insert appropriate replica id to avoid duplicates

  //std::cerr << "PreProcessElectFB for txn: " << BytesToHex(txnDigest, 16) << std::endl;
  ElectQuorumMap::accessor q;
  ElectQuorums.insert(q, txnDigest);
  ElectFBorganizer &electFBorganizer = q->second;
  replica_sig_sets_pair &view_decision_quorum = electFBorganizer.view_quorums[elect_view][decision];
  if(!view_decision_quorum.first.insert(process_id).second){
    return false;
  }
  q.release();
  return true;
  //std::cerr << "Not failing during PreProcessElectFB for txn: " << BytesToHex(txnDigest, 16) << std::endl;
}

void Server::HandleElectFB(proto::ElectFB &msg){

  stats.Increment("total_equiv_received_elect", 1);

  if (!params.signedMessages) {Panic("ERROR HANDLE ELECT FB: NON SIGNED VERSION NOT IMPLEMENTED");}
  if(!msg.has_signed_elect_fb()){
    if(params.mainThreadDispatching && !params.dispatchMessageReceive) FreeElectFBmessage(&msg);
    return;
  }
  proto::SignedMessage *signed_msg = msg.mutable_signed_elect_fb();
  if(!IsReplicaInGroup(signed_msg->process_id(), groupIdx, &config)){
    if(params.mainThreadDispatching && !params.dispatchMessageReceive) FreeElectFBmessage(&msg);
    return;
  }

  proto::ElectMessage electMessage;
  electMessage.ParseFromString(signed_msg->data());
  const std::string &txnDigest = electMessage.txn_digest();
  Debug("Received ElectFB request for txn: %s", BytesToHex(txnDigest, 64).c_str());

  //std::cerr << "Started HandleElectFB for txn: " << BytesToHex(txnDigest, 16) << std::endl;

  size_t leaderID = (electMessage.elect_view() + txnDigest[0]) % config.n;
  if(leaderID != idx){ //Not the right leader
    if(params.mainThreadDispatching && !params.dispatchMessageReceive) FreeElectFBmessage(&msg);
    return;
  }

  //return if this txnDigest already committed/aborted
  interestedClientsMap::accessor i;
  auto jtr = interestedClients.find(i, txnDigest);
  if(jtr){
    if(ForwardWritebackMulti(txnDigest, i)){
      if(params.mainThreadDispatching && !params.dispatchMessageReceive) FreeElectFBmessage(&msg);
       return;
    }
  }
  i.release();


  //create management object (if necessary) and insert appropriate replica id to avoid duplicates
  if(!PreProcessElectFB(txnDigest, electMessage.elect_view(), electMessage.decision(), signed_msg->process_id())){
    if(params.mainThreadDispatching && !params.dispatchMessageReceive) FreeElectFBmessage(&msg);
    return;
  }

  //verify signature before adding it.
  std::string *signature = signed_msg->release_signature();
  if(params.multiThreading){
      // verify this async. Dispatch verification with a callback to the callback.
      std::string *msg = signed_msg->release_data();
      auto comb = [this, process_id = signed_msg->process_id(), msg, signature, txnDigest,
                      elect_view = electMessage.elect_view(), decision = electMessage.decision()]() mutable
        {
        bool valid = verifier->Verify2(keyManager->GetPublicKey(process_id), msg, signature);
        ElectFBcallback(txnDigest, elect_view, decision, signature, process_id, (void*) valid);
        delete msg;
        return (void*) true;
        };

      transport->DispatchTP_noCB(std::move(comb));
  }
  else{
    if(!verifier->Verify(keyManager->GetPublicKey(signed_msg->process_id()),
          signed_msg->data(), signed_msg->signature())) return;
    ProcessElectFB(txnDigest, electMessage.elect_view(), electMessage.decision(), signature, signed_msg->process_id());
  }
  if(params.mainThreadDispatching && !params.dispatchMessageReceive) FreeElectFBmessage(&msg);

  //std::cerr << "Not failing during Handle ElectFB for txn: " << BytesToHex(txnDigest, 16) << std::endl;
}

  //Callback (might need to do lookup again.)
void Server::ElectFBcallback(const std::string &txnDigest, uint64_t elect_view, proto::CommitDecision decision, std::string *signature, uint64_t process_id, void* valid){

  Debug("ElectFB callback [decision: %s][elect_view: %lu] for txn: %s", decision ? "ABORT" : "COMMIT", elect_view, BytesToHex(txnDigest, 64).c_str());

  if(!valid){
    Debug("ElectFB request not valid for txn: %s", BytesToHex(txnDigest, 64).c_str());
    delete signature;
    return;
  }

  interestedClientsMap::accessor i;
  auto jtr = interestedClients.find(i, txnDigest);
  if(jtr){
    if(ForwardWritebackMulti(txnDigest, i)) return;
  }
  i.release();


  ProcessElectFB(txnDigest, elect_view, decision, signature, process_id);
}

void Server::ProcessElectFB(const std::string &txnDigest, uint64_t elect_view, proto::CommitDecision decision, std::string *signature, uint64_t process_id){

  //std::cerr << "ProcessElectFB for txn: " << BytesToHex(txnDigest, 16) << std::endl;
  Debug("Processing Elect FB [decision: %s][elect_view: %lu] for txn %s", decision ? "ABORT" : "COMMIT", elect_view, BytesToHex(txnDigest, 64).c_str());
  //Add signature
  ElectQuorumMap::accessor q;
  if(!ElectQuorums.find(q, txnDigest)) return;
  ElectFBorganizer &electFBorganizer = q->second;

  bool &complete = electFBorganizer.view_complete[elect_view]; //false by default
  //Only make 1 decision per view. A Byz Fallback leader may do two.
  if(complete) return;

  std::pair<proto::Signatures, uint64_t> &view_decision_quorum = electFBorganizer.view_quorums[elect_view][decision].second;

  proto::Signature *sig = view_decision_quorum.first.add_sigs();
  sig->set_allocated_signature(signature);
  sig->set_process_id(process_id);
  view_decision_quorum.second++; //count the number of valid sigs. (Counting seperately so that the size is monotonous if I move Signatures...)

  if(!complete && view_decision_quorum.second == 2*config.f +1){
    //Set message
    complete = true;
    proto::DecisionFB decisionFB;
    decisionFB.set_req_id(0);
    decisionFB.set_txn_digest(txnDigest);
    decisionFB.set_decision(decision);
    decisionFB.set_view(elect_view);
    //*decisionFB.mutable_elect_sigs() = std::move(view_decision_quorum.first); //Or use Swap
    decisionFB.mutable_elect_sigs()->Swap(&view_decision_quorum.first);
    view_decision_quorum.first.Clear(); //clear it so it resets cleanly. probably not necessary.

    // auto itr = view_decision_quorum.second.begin();
    // while(itr != view_decision_quorum.second.end()){
    //   decisionFB.mutable_elect_sigs()->mutable_sigs()->AddAllocated(*itr);
    //   itr = view_decision_quorum.second.erase(itr);
    // }


  //delete all released signatures that we dont need/use - If i copied instead would not need this.
  // auto it=electFBorganizer.view_quorums.begin();
  // while(it != electFBorganizer.view_quorums.end()){
  //   if(it->first > elect_view){
  //     // for(auto sig : electFBorganizer.view_quorums[elect_view][1-decision].second){
  //     break;
  //   }
  //   else{
  //     for(auto decision_sigs : it->second){
  //       for(auto sig : decision_sigs.second.second){  //it->second[decision_sigs].second
  //         delete sig;
  //       }
  //     }
  //     it = electFBorganizer.view_quorums.erase(it);
  //   }
  // }
    q.release();

    //Send decision to all replicas (besides itself) and handle Decision FB directly onself.
    //transport->SendMessageToReplica(this, groupIdx, idx, decisionFB);
    transport->SendMessageToGroup(this, groupIdx, decisionFB);
    Debug("Sent DecisionFB message [decision: %s][elect_view: %lu] for txn: %s", decision ? "ABORT" : "COMMIT", elect_view, BytesToHex(txnDigest, 64).c_str());

    //std::cerr<<"This replica is the leader: " << id << " Adopting directly, without sending" << std::endl;
    AdoptDecision(txnDigest, elect_view, decision);
  }
  else{
    q.release();
  }
  //std::cerr << "Not failing during ProcessElectFB for txn: " << BytesToHex(txnDigest, 16) << std::endl;
}


void Server::HandleDecisionFB(proto::DecisionFB &msg){

    const std::string &txnDigest = msg.txn_digest();
    Debug("Received DecisionFB request for txn: %s", BytesToHex(txnDigest, 64).c_str());
    //fprintf(stderr, "Received DecisionFB request for txn: %s \n", BytesToHex(txnDigest, 64).c_str());

    interestedClientsMap::accessor i;
    auto jtr = interestedClients.find(i, txnDigest);
    if(jtr){
      if(ForwardWritebackMulti(txnDigest, i)){
        if(params.multiThreading || (params.mainThreadDispatching && !params.dispatchMessageReceive)) FreeDecisionFBmessage(&msg);
        return;
      }
    }
    i.release();


    //outdated request
    p2MetaDataMap::const_accessor p;
    p2MetaDatas.insert(p, txnDigest);
    uint64_t current_view = p->second.current_view;
    p.release();
    if(current_view > msg.view() || msg.view() <= 0){
      if(params.multiThreading || (params.mainThreadDispatching && !params.dispatchMessageReceive)) FreeDecisionFBmessage(&msg);
      return;
    }
    //Note: dont need to explicitly verify leader Id. view number suffices. This is because
    //correct replicas would only send their ElectFB message (which includes a view) to the
    //according replica. So this Quorum could only come from that replica
    //(Knowing that is not required for safety anyways, only for liveness)

    const proto::Transaction *txn;
    ongoingMap::const_accessor b;
    bool isOngoing = ongoing.find(b, txnDigest);
    if(isOngoing){
        txn = b->second.txn;
    }
    else{
      if(params.multiThreading || (params.mainThreadDispatching && !params.dispatchMessageReceive)) FreeDecisionFBmessage(&msg);
      return; //REPLICA HAS NEVER SEEN THIS TXN OR TXN NO LONGER ONGOING
    }
    b.release();

    //verify signatures
    int64_t myProcessId;
    proto::CommitDecision myDecision;
    LookupP2Decision(txnDigest, myProcessId, myDecision);
    if(params.multiThreading){
      mainThreadCallback mcb(std::bind(&Server::FBDecisionCallback, this, &msg, txnDigest, msg.view(), msg.decision(), std::placeholders::_1));
      asyncValidateFBDecision(msg.decision(), msg.view(), txn, &txnDigest, msg.elect_sigs(), keyManager,
                        &config, myProcessId, myDecision, verifier, std::move(mcb), transport, params.multiThreading);
    }
    else{
      ValidateFBDecision(msg.decision(), msg.view(), txn, &txnDigest, msg.elect_sigs(), keyManager,
                        &config, myProcessId, myDecision, verifier);
      FBDecisionCallback(&msg, txnDigest, msg.view(), msg.decision(), (void*) true);
    }

}

void Server::FBDecisionCallback(proto::DecisionFB *msg, const std::string &txnDigest, uint64_t view, proto::CommitDecision decision, void* valid){

    if(!valid){
      if(params.multiThreading || (params.mainThreadDispatching && !params.dispatchMessageReceive)) FreeDecisionFBmessage(msg);
      return;
    }

    interestedClientsMap::accessor i;
    auto jtr = interestedClients.find(i, txnDigest);
    if(jtr){
      if(ForwardWritebackMulti(txnDigest, i)) return;
    }
    i.release();


    AdoptDecision(txnDigest, view, decision);
    //std::cerr << "Not failing during Handle DecisionFBcallback for txn: " << BytesToHex(txnDigest, 16) << std::endl;

    if(params.multiThreading || (params.mainThreadDispatching && !params.dispatchMessageReceive)) FreeDecisionFBmessage(msg);

}

void Server::AdoptDecision(const std::string &txnDigest, uint64_t view, proto::CommitDecision decision){

  p2MetaDataMap::accessor p;
  p2MetaDatas.insert(p, txnDigest);

  //NOTE: send p2 to subscribed original client.
  //This is a temporary hack to subscribe the original client to receive p2 replies for higher views.
  //Should be reconciled with Phase2FB message eventually.
  bool has_original = p->second.has_original;
  uint64_t req_id = has_original? p->second.original_msg_id : 0;
  TransportAddress *original_address = p->second.original_address;

  uint64_t current_view = p->second.current_view;
  uint64_t decision_view = p->second.decision_view;
  if(current_view > view){ //outdated request
    return;
  }
  else if(current_view < view){
    p->second.current_view = view;
  }
  if(decision_view < view){
    p->second.decision_view = view;
    p->second.p2Decision = decision;
    p->second.hasP2 = true;
  }
  p.release();

  Debug("Adopted new decision [dec: %s][dec_view: %lu] for txn %s", decision ? "ABORT" : "COMMIT", view, BytesToHex(txnDigest, 64).c_str());
  stats.Increment("total_equiv_received_adopt", 1);
  //fprintf(stderr, "Adopted new decision [dec: %s][dec_view: %lu] for txn %s.\n", decision ? "ABORT" : "COMMIT", view, BytesToHex(txnDigest, 64).c_str());

  //send a p2 message anyways, even if we have a newer one, just so clients can still form quorums on past views.
  P2FBorganizer *p2fb_organizer = new P2FBorganizer(req_id, txnDigest, this);
  p2fb_organizer->original = original_address; //XXX has_remote must be false so its not deleted!!!
  SetP2(req_id, p2fb_organizer->p2fbr->mutable_p2r(), txnDigest, decision, view);
  SendPhase2FBReply(p2fb_organizer, txnDigest, true, has_original);
}


void Server::BroadcastMoveView(const std::string &txnDigest, uint64_t proposed_view){
  // make sure we dont broadcast twice for a view.. (set flag in ElectQuorum organizer)

  proto::MoveViewMessage move_msg;
  move_msg.set_req_id(0);
  move_msg.set_txn_digest(txnDigest);
  move_msg.set_view(proposed_view);

  if(params.signedMessages){
    proto::SignedMessage signed_move_msg;
    CreateHMACedMessage(move_msg, signed_move_msg);
    transport->SendMessageToGroup(this, groupIdx, signed_move_msg);
  }
  else{
    transport->SendMessageToGroup(this, groupIdx, move_msg);
  }
}

void Server::HandleMoveView(proto::MoveView &msg){
  //Can send ElectFB message for view v+1 *before* having adopted v+1.

  std::string txnDigest;
  uint64_t proposed_view;

  if(params.signedMessages){
    if(!ValidateHMACedMessage(msg.signed_msg())){
      if(params.mainThreadDispatching && !params.dispatchMessageReceive) FreeMoveView(&msg);
      return;
    }
    proto::MoveViewMessage move_msg;
    move_msg.ParseFromString(msg.signed_msg().data());
    txnDigest = move_msg.txn_digest();
    proposed_view = move_msg.view();
  }
  else{
    const proto::MoveViewMessage &move_msg = msg.msg();
    txnDigest = move_msg.txn_digest();
    proposed_view = move_msg.view();
  }

  //Ignore if tx already finished.
  interestedClientsMap::accessor i;
  auto jtr = interestedClients.find(i, txnDigest);
  if(jtr){
    if(ForwardWritebackMulti(txnDigest, i)){
      if(params.mainThreadDispatching && !params.dispatchMessageReceive) FreeMoveView(&msg);
      return;
    }
  }
  i.release();


  ProcessMoveView(txnDigest, proposed_view);

  if(params.mainThreadDispatching && !params.dispatchMessageReceive) FreeMoveView(&msg);
}

void Server::ProcessMoveView(const std::string &txnDigest, uint64_t proposed_view, bool self){
  ElectQuorumMap::accessor q;
  ElectQuorums.insert(q, txnDigest);
  ElectFBorganizer &electFBorganizer = q->second;
  if(electFBorganizer.move_view_counts.find(proposed_view) == electFBorganizer.move_view_counts.end()){
    electFBorganizer.move_view_counts[proposed_view] = std::make_pair(0, true);
  }

  uint64_t count = 0;
  if(self){
    //count our own vote once.
    if(electFBorganizer.move_view_counts[proposed_view].second){
      BroadcastMoveView(txnDigest, proposed_view);
      count = ++(electFBorganizer.move_view_counts[proposed_view].first); //have not broadcast yet, count our own vote (only once total).
      electFBorganizer.move_view_counts[proposed_view].second = false;
    }
  }
  else{
    //count the messages received from other replicas.
    count = ++(electFBorganizer.move_view_counts[proposed_view].first); //TODO: dont count duplicate replicas.
  }

  p2MetaDataMap::accessor p;
  p2MetaDatas.insert(p, txnDigest);
  if(proposed_view > p->second.current_view){

    if(count == config.f + 1){
      if(electFBorganizer.move_view_counts[proposed_view].second){
        BroadcastMoveView(txnDigest, proposed_view);
        count = ++(electFBorganizer.move_view_counts[proposed_view].first); //have not broadcast yet, count our own vote (only once total).
        electFBorganizer.move_view_counts[proposed_view].second = false;
      }
    }

    if(count == 2*config.f + 1){
        p->second.current_view = proposed_view;
    }
  }
  p.release();
  q.release();
}
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

} // namespace indicusstore
