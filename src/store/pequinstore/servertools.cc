/***********************************************************************
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


#include <cryptopp/sha.h>
#include <cryptopp/blake2.h>


#include "store/pequinstore/server.h"

#include <bitset>
#include <queue>
#include <ctime>
#include <chrono>
#include <sys/time.h>
#include <sstream>
#include <list>
#include <utility>

#include "lib/assert.h"
#include "lib/tcptransport.h"
#include "store/common/timestamp.h"
#include "store/common/transaction.h"
#include "store/pequinstore/common.h"
#include "store/pequinstore/phase1validator.h"
#include "store/pequinstore/localbatchsigner.h"
#include "store/pequinstore/sharedbatchsigner.h"
#include "store/pequinstore/basicverifier.h"
#include "store/pequinstore/localbatchverifier.h"
#include "store/pequinstore/sharedbatchverifier.h"
#include "lib/batched_sigs.h"
#include <valgrind/memcheck.h>

namespace pequinstore {

///////////////////// Receive Message Dispatch Handlers: Determine on which thread to run message handlers -- and how objects need to be allocated accordingly
// Parameters:
// 1. mainThreadDispatching: Deserialize messages on thread receiving messages, but dispatch message handling to main worker thread
// 2. dispatchMessageReceive: Dispatch both message deserialization and message handling to main worker thread. (Note == 1 + 2 should never both be true.)
// 3. parallel_reads: Dispatch all read requests to workers threads
// 4. parallel_CCC: Dispatch Concurrency Control Check to worker threads
// 5. multiThreading: Dispatch all crypto verification to worker threads (TODO: rename flag for clarity --> crypto_multiThreading)
// 6. all_to_all_fb: Use all to all election for fallback -- uses Macs: Indicates that no crypto verification dispatch will be needed.
// 7. parallel_queries: Dispatch all query related requests to worker threads
//TODO: Full CPU utilization parallelism: Assign all handler functions to different threads.

//TODO: Simplify all if clauses. Note that mainThreadDispatching and dispatchMessageReceive are mutually exclusive.

//Note: parallel_reads is only enabled for mainThreadDispatching //TODO: Re-factor (also for queries) such that parallel works for dispatch too. Not urgent: dispatchReceive not currently used.
void Server::ManageDispatchRead(const TransportAddress &remote, const std::string &data){
    //if no dispatching OR if dispatching both deser and Handling to 2nd main thread (no workers)
    if(!params.mainThreadDispatching || (params.dispatchMessageReceive && !params.parallel_reads) ){
      //I.e If ManageDispatchRead is not supposed to be dispatched; OR it was already dispatched ==> don't need to allocate & can exec. If parallel_reads true, then do need to allocate
      read.ParseFromString(data);
      HandleRead(remote, read);
    }
    //if dispatching to second main or other workers
    else{ //I.e. ManageDispatch is called from Network Thread (mainThreadDispatching -> !dispatchMessageReceive)
      proto::Read* readCopy = GetUnusedReadmessage();
      readCopy->ParseFromString(data);
      auto f = [this, &remote, readCopy](){
        this->HandleRead(remote, *readCopy);
        return (void*) true;
      };
      if(params.parallel_reads){
        transport->DispatchTP_noCB(std::move(f));
      }
      else{
        transport->DispatchTP_main(std::move(f));
      }
    }
}

//void Server::ManageDispatchQuery(const TransportAddress &remote, const std::string &data){}

void Server::ManageDispatchPhase1(const TransportAddress &remote, const std::string &data){
    //Use only with OCC parallel, not full parallel P1. Suffers from non-atomicity in the latter case
    if((!params.mainThreadDispatching || (params.dispatchMessageReceive && !params.parallel_CCC)) && (!params.multiThreading || !params.signClientProposals)){
      //if no dispatching intended, or already on main worker thread but no parallel OCC needed
                                    //i.e. resources do not need to be copied again.
     phase1.ParseFromString(data);
     HandlePhase1(remote, phase1);
    }
    else{ // (mainThreadDispatch == true && (dispatchMessageReceive == false, or parallel_CCC == true))  ||  (multiThreading && signClientProposals)
      proto::Phase1 *phase1Copy = GetUnusedPhase1message();
      phase1Copy->ParseFromString(data);
      auto f = [this, &remote, phase1Copy]() {
        this->HandlePhase1(remote, *phase1Copy);
        return (void*) true;
      };
      if(params.dispatchMessageReceive){ // == if parallel OCC and currently already on main worker thread
        f();
      }
      else{ //== if currently on receiving thread
        Debug("Dispatching HandlePhase1");
        transport->DispatchTP_main(f);
        //transport->DispatchTP_noCB(f); //use if want to dispatch to all workers
      }
    }

    // edit for atomic parallel P1. (OUTDATED)
    // if(!params.mainThreadDispatching || (params.dispatchMessageReceive && !params.parallel_CCC)){
    //  phase1.ParseFromString(data);
    //  HandlePhase1_atomic(remote, phase1);
    // }
    // else{
    //   proto::Phase1 *phase1Copy = GetUnusedPhase1message();
    //   phase1Copy->ParseFromString(data);
    //   auto f = [this, &remote, phase1Copy]() {
    //     this->HandlePhase1_atomic(remote, *phase1Copy);
    //     return (void*) true;
    //   };
    //   // if(params.parallel_CCC){
    //   //   transport->DispatchTP_noCB(std::move(f));
    //   // }
    //   // else
    //   if(params.dispatchMessageReceive){
    //     f();
    //   }
    //   else{
    //     Debug("Dispatching HandlePhase1");
    //     transport->DispatchTP_main(std::move(f));
    //     //transport->DispatchTP_noCB(f); //use if want to dispatch to all workers
    //   }
    // }
}

void Server::ManageDispatchPhase2(const TransportAddress &remote, const std::string &data){
    if(!params.multiThreading && (!params.mainThreadDispatching || params.dispatchMessageReceive)){
        phase2.ParseFromString(data);
        HandlePhase2(remote, phase2);
      }
      else{
        proto::Phase2* p2 = GetUnusedPhase2message();
        p2->ParseFromString(data);
        if(!params.mainThreadDispatching || params.dispatchMessageReceive){  //If multithreading verification, must allocate object even if we don't dispatch.
          HandlePhase2(remote, *p2);
        }
        else{
          auto f = [this, &remote, p2](){
            this->HandlePhase2(remote, *p2);
            return (void*) true;
          };
          transport->DispatchTP_main(std::move(f));
        }
      }
}    

void Server::ManageDispatchWriteback(const TransportAddress &remote, const std::string &data){
    if(!params.multiThreading && (!params.mainThreadDispatching || params.dispatchMessageReceive)){
        writeback.ParseFromString(data);
        HandleWriteback(remote, writeback);
      }
      else{
        proto::Writeback *wb = GetUnusedWBmessage();
        wb->ParseFromString(data);
        if(!params.mainThreadDispatching || params.dispatchMessageReceive){
          HandleWriteback(remote, *wb);
        }
        else{ //mainthreadDispatching = true && dispatchMsgReceive= false.
          auto f = [this, &remote, wb](){
            this->HandleWriteback(remote, *wb);
            return (void*) true;
          };
          transport->DispatchTP_main(std::move(f));
        }
      }
}

void Server::ManageDispatchPhase1FB(const TransportAddress &remote, const std::string &data){
    if(!params.mainThreadDispatching || (params.dispatchMessageReceive && !params.parallel_CCC) && (!params.multiThreading || !params.signClientProposals)){
      phase1FB.ParseFromString(data);
      HandlePhase1FB(remote, phase1FB);
    }
    else{
      proto::Phase1FB *phase1FBCopy = GetUnusedPhase1FBmessage();
      phase1FBCopy->ParseFromString(data);
      auto f = [this, &remote, phase1FBCopy]() {
        this->HandlePhase1FB(remote, *phase1FBCopy);
        return (void*) true;
      };
      if(params.dispatchMessageReceive){
        f();
      }
      else{
        Debug("Dispatching HandlePhase1");
        transport->DispatchTP_main(std::move(f));
        //transport->DispatchTP_noCB(f); //use if want to dispatch to all workers
      }
    }
}

void Server::ManageDispatchPhase2FB(const TransportAddress &remote, const std::string &data){
    if(!params.multiThreading && (!params.mainThreadDispatching || params.dispatchMessageReceive)){
        phase2FB.ParseFromString(data);
        HandlePhase2FB(remote, phase2FB);
      }
      else{
        proto::Phase2FB* p2FB = GetUnusedPhase2FBmessage();
        p2FB->ParseFromString(data);
        if(!params.mainThreadDispatching || params.dispatchMessageReceive){
          HandlePhase2FB(remote, *p2FB);
        }
        else{
          auto f = [this, &remote, p2FB](){
            this->HandlePhase2FB(remote, *p2FB);
            return (void*) true;
          };
          transport->DispatchTP_main(std::move(f));
        }
      }
}

void Server::ManageDispatchInvokeFB(const TransportAddress &remote, const std::string &data){
    if((params.all_to_all_fb || !params.multiThreading) && (!params.mainThreadDispatching || params.dispatchMessageReceive)){
      invokeFB.ParseFromString(data);
      HandleInvokeFB(remote, invokeFB);
    }
    else{
      proto::InvokeFB* invFB = GetUnusedInvokeFBmessage();
      invFB->ParseFromString(data);
      if(!params.mainThreadDispatching || params.dispatchMessageReceive){
        HandleInvokeFB(remote, *invFB);
      }
      else{
        auto f = [this, &remote, invFB](){
          this->HandleInvokeFB(remote, *invFB);
          return (void*) true;
        };
        transport->DispatchTP_main(std::move(f));
      }
    }
}

void Server::ManageDispatchElectFB(const TransportAddress &remote, const std::string &data){
    if(!params.mainThreadDispatching || params.dispatchMessageReceive){
      electFB.ParseFromString(data);
      HandleElectFB(electFB);
    }
    else{
      proto::ElectFB* elFB = GetUnusedElectFBmessage();
      elFB->ParseFromString(data);
      auto f = [this, elFB](){
          this->HandleElectFB(*elFB);
          return (void*) true;
      };
      transport->DispatchTP_main(std::move(f));
    }
}

void Server::ManageDispatchDecisionFB(const TransportAddress &remote, const std::string &data){
    if(!params.multiThreading && (!params.mainThreadDispatching || params.dispatchMessageReceive)){
      decisionFB.ParseFromString(data);
      HandleDecisionFB(decisionFB);
    }
    else{
      proto::DecisionFB* decFB = GetUnusedDecisionFBmessage();
      decFB->ParseFromString(data);
      if(!params.mainThreadDispatching || params.dispatchMessageReceive){
        HandleDecisionFB(*decFB);
      }
      else{
        auto f = [this, decFB](){
          this->HandleDecisionFB( *decFB);
          return (void*) true;
        };
        transport->DispatchTP_main(std::move(f));
      }
    }
}

void Server::ManageDispatchMoveView(const TransportAddress &remote, const std::string &data){
    if(!params.mainThreadDispatching || params.dispatchMessageReceive){
      moveView.ParseFromString(data);
      HandleMoveView(moveView); //Send only to other replicas
    }
    else{
      proto::MoveView* mvView = GetUnusedMoveView();
      mvView->ParseFromString(data);
      auto f = [this, mvView](){
          this->HandleMoveView( *mvView);
          return (void*) true;
      };
      transport->DispatchTP_main(std::move(f));
    }
}

//Queries
////Note: parallel_queries only enabled for MainThreadDispatching -- Disabled for dispatchMessageReceive. //TODO: Enable also for dispatchMsgReceive

void Server::ManageDispatchQuery(const TransportAddress &remote, const std::string &data){

   if(!params.mainThreadDispatching || (params.dispatchMessageReceive && !params.query_params.parallel_queries ) ){
      queryReq.ParseFromString(data);
      HandleQuery(remote, queryReq);
    }
    //if dispatching to second main or other workers
    else{
      proto::QueryRequest* queryCopy = GetUnusedQueryRequestMessage();
      queryCopy->ParseFromString(data);
      auto f = [this, &remote, queryCopy](){
        this->HandleQuery(remote, *queryCopy);
        return (void*) true;
      };
      if(params.query_params.parallel_queries){ 
        transport->DispatchTP_noCB(std::move(f));  //dispatch to worker
      }
      else{
        transport->DispatchTP_main(std::move(f)); //dispatch to mainthread
      }
    }
}


void Server::ManageDispatchSync(const TransportAddress &remote, const std::string &data){

   if(!params.mainThreadDispatching || (params.dispatchMessageReceive && !params.query_params.parallel_queries ) ){
       syncMsg.ParseFromString(data);
       HandleSync(remote, syncMsg);
    }
    //if dispatching to second main or other workers
    else{ //mainThreadDispatching
      proto::SyncClientProposal* syncCopy = GetUnusedSyncClientProposalMessage();
      syncCopy->ParseFromString(data);
      auto f = [this, &remote, syncCopy](){
        this->HandleSync(remote, *syncCopy);
        return (void*) true;
      };
      if(params.query_params.parallel_queries){ 
        transport->DispatchTP_noCB(std::move(f));  //dispatch to worker
      }
      else{
        transport->DispatchTP_main(std::move(f)); //dispatch to mainthread
      }
    }
}


void Server::ManageDispatchRequestTx(const TransportAddress &remote, const std::string &data){

   if(!params.mainThreadDispatching || (params.dispatchMessageReceive && !params.query_params.parallel_queries ) ){
        //requestTx.Clear();
        requestTx.ParseFromString(data);
        HandleRequestTx(remote, requestTx);
    }
    //if dispatching to second main or other workers
    else{
      proto::RequestMissingTxns* reqCopy = GetUnusedRequestTxMessage();
      reqCopy->ParseFromString(data);
      auto f = [this, &remote, reqCopy](){
        this->HandleRequestTx(remote, *reqCopy);
        return (void*) true;
      };
      if(params.query_params.parallel_queries){ 
        transport->DispatchTP_noCB(std::move(f));  //dispatch to worker
      }
      else{
        transport->DispatchTP_main(std::move(f)); //dispatch to mainthread
      }
    }
  
}

// dont parallize further, since this is treated as P1 or Writeback
void Server::ManageDispatchSupplyTx(const TransportAddress &remote, const std::string &data){
  
   if(!params.mainThreadDispatching || (params.dispatchMessageReceive && !params.query_params.parallel_queries )){  // ==  if(params.mainThreadDispatching && !params.dispatchMessageReceive 
        supplyTx.ParseFromString(data);
        HandleSupplyTx(remote, supplyTx); 
    } 
    //if dispatching to second main or other workers
    else{ //params.mainThreadDispatching && (!params.dispatchMessageReceive ||params.query_params.parallel_queries)
      proto::SupplyMissingTxns* supplyCopy = GetUnusedSupplyTxMessage();
      supplyCopy->ParseFromString(data);
      auto f = [this, &remote, supplyCopy](){
        this->HandleSupplyTx(remote, *supplyCopy);
        return (void*) true;
      };
     
      if(params.query_params.parallel_queries){ 
        transport->DispatchTP_noCB(std::move(f));  //dispatch to worker
      }
      else{
        transport->DispatchTP_main(std::move(f)); //dispatch to mainthread
      }
      
    }
}

//Backup Code for Managing thread location for querysync-server.cc SupplyTxn HandleWriteback
 // if(!params.mainThreadDispatching ){
            //      //if !mainThreadDispatching: -> parallel_queries cannot be used. 
            //    //==> Implies that query is either on network thread or on main thread (if dispatchMessageReceive is true).
            //    //==> Implies also that Writeback is either on network or on main thread (if dispatchMessageReceive is true).
            //     if(params.dispatchMessageReceive && !params.query_params.parallel_queries){
            //         //Query must be on main Thread already. dispatchImplies Writeback should be there too.
            //         f();
            //     }
            //     else if(!params.dispatchMessageReceive && !params.query_params_parallel_queries){
            //         //Query must be on network thread. // Writeback should be on network.
            //         f();
            //     } 
            //     else if(params.dispatchMessageReceive && params.query_params_parallel_queries){  
            //         //Query must be on worker thread. //Writeback should be on main // 
            //         transport->DispatchTP_main(std::move(f));
            //     }
            //     else if(!params.dispatchMessageReceive && params.query_params_parallel_queries){
            //         //Query must be on worker thread. //Writeback should be on network.
            //             transport->IssueCB(std::move(f));
            //     } 
                
            // }

            // else if(params.query_params.parallel_queries){ //params.mainThreadDispatching == true. ==> dispatchMessageReceive must be false.
            //     //Query is on worker
            //     //Writeback must be on mainThread: dispatchMainCB
            //     transport->DispatchTP_main(std::move(f));
            // }
            // else{ //params.mainThreadDispatching == true ==> dispatchMessageReceive must be false. params.query_params.parallel_queries = false
            //     //Query is on mainThread
            //     //Writeback can  stay
            //     f();
            // }
          

//////////////////////////////////////////////////////// Protocol Helper Functions

void Server::FindTableVersion(const std::string &key_name, const Timestamp &ts, bool read_or_snapshot, QueryReadSetMgr *readSetMgr, SnapshotManager *snapshotMgr){
  //Read the current TableVersion or TableColVersion from CC-store  -- I.e. key_name = "table_name" OR "table_name + delim + column_name" 
  //Note: TableVersion is updated AFTER all TableWrites of a TX have been written. So the TableVersion from CC-store is a pessimistic version; if it is outdated we abort, but that is safe.

  //Read committed
  std::pair<Timestamp, Server::Value> tsVal;
  //find committed write value to read from
  bool committed_exists = store.get(key_name, ts, tsVal);
  if(!committed_exists){
    Panic("All Tables must have a genesis version");  //Note: CreateTable() writes the genesis version
  }

  const proto::Transaction *mostRecentPrepared = nullptr;

  //Read prepared
  if(occType == MVTSO && params.maxDepDepth > -2){    //TODO: possibly set RTS too here. Note: currently being set for whole Query ReadSet after exec. 
      mostRecentPrepared = FindPreparedVersion(key_name, ts, committed_exists, tsVal);
  }


  if(read_or_snapshot){ //Creating ReadSet
    UW_ASSERT(readSetMgr);

    if(mostRecentPrepared != nullptr){ //Read prepared
      readSetMgr->AddToReadSet(key_name, mostRecentPrepared->timestamp());
      readSetMgr->AddToDepSet(TransactionDigest(*mostRecentPrepared, params.hashDigest), mostRecentPrepared->timestamp());
    }
    else{ //Read committed
      TimestampMessage tsm;
      tsVal.first.serialize(&tsm);
      readSetMgr->AddToReadSet(key_name, tsm);
    }
  }
  else{ //Creating Snapshot
    UW_ASSERT(snapshotMgr);

    if(mostRecentPrepared != nullptr){ //Read prepared
      snapshotMgr->AddToLocalSnapshot(TransactionDigest(*mostRecentPrepared, params.hashDigest), mostRecentPrepared, false);
    }
    else{ //Read committed
      snapshotMgr->AddToLocalSnapshot(TransactionDigest(*mostRecentPrepared, params.hashDigest), tsVal.first.getTimestamp(), tsVal.first.getID(), true);
    }
  }

  return;
}

const proto::Transaction* Server::FindPreparedVersion(const std::string &key, const Timestamp &ts, bool committed_exists, std::pair<Timestamp, Server::Value> const &tsVal){

  const proto::Transaction *mostRecent = nullptr;
  auto itr = preparedWrites.find(key);
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
    }
  }
  return mostRecent;
}

// void Server::SetTableVersion(proto::Transaction *txn){
//   //for all table write keys in table_writes.
// }




void* Server::CheckProposalValidity(::google::protobuf::Message &msg, const proto::Transaction *txn, std::string &txnDigest, bool fallback){
    if (params.validateProofs && params.signedMessages && params.verifyDeps) { 
      //Check whether claimed dependencies are actually dependencies, and whether f+1 replicas signed them
      //Currently not used: Instead, replicas only accept dependencies they have seen already. (More pessimistic, but more cost efficient)
         if(!VerifyDependencies(msg, txn, txnDigest, fallback)){
             Debug("Failed VerifyDependencies for txn[%s] on MainThread %d", BytesToHex(txnDigest, 16).c_str(), sched_getcpu());
             return (void*) false; 
         } 
      //CURRENTLY NOT USED: IF WE USE IT --> REFACTOR TO ALSO BE MULTITHREADED VERIFICATION: ADD IT TOGETHER WITH THE OTHER CLIENT SIG VERIFICATION> 
    }
    if (params.signClientProposals){
        if(!VerifyClientProposal(msg, txn, txnDigest, fallback)){
            Debug("Failed VerifyClientProposal for txn[%s] on MainThread %d", BytesToHex(txnDigest, 16).c_str(), sched_getcpu());
            return (void*) false; 
        } 
    }
    return (void*) true;

}

//Checks for a given client proposals read set whether all dependencies that are claimed are valid
//A dependency is valid if: 1) It was signed by f+1 replicas (proving existence)
//                (TODO)    2) It corresponds to an actual read in the read set 
//                           (currently not checked -- a correct client wouldnt lie, and a byz could always add more reads)
//                            -- But we should still check to make sure read set isnt artificially smaller than dep set (which would improve prepare chances, while increasing opportunity to block maliciously)
// CURRENTLY NOT USED -- instead, servers pessimistically only accept dependencies they have locally prepared (or committed)                         
bool Server::VerifyDependencies(::google::protobuf::Message &msg, const proto::Transaction *txn, std::string &txnDigest, bool fallback){
    for (const auto &dep : txn->deps()) {
    //  for (const auto &dep : txn->deps()) {
        if (!dep.has_write_sigs()) {
          Debug("Dep for txn %s missing signatures.",
              BytesToHex(txnDigest, 16).c_str());
           if((params.mainThreadDispatching && (!params.dispatchMessageReceive || params.parallel_CCC)) || (params.multiThreading && params.signClientProposals)){
                if(fallback)  FreePhase1FBmessage(&static_cast<proto::Phase1FB&>(msg));
                if(!fallback) FreePhase1message(&static_cast<proto::Phase1&>(msg));
            } 
          //if(params.signClientProposals) delete txn;
          return false;
        }
        if (!ValidateDependency(dep, &config, params.readDepSize, keyManager,
              verifier)) {
          Debug("VALIDATE Dependency failed for txn %s.",
              BytesToHex(txnDigest, 16).c_str());
          // safe to ignore Byzantine client
           if((params.mainThreadDispatching && (!params.dispatchMessageReceive || params.parallel_CCC)) || (params.multiThreading && params.signClientProposals)){
                if(fallback)  FreePhase1FBmessage(&static_cast<proto::Phase1FB&>(msg));
                if(!fallback) FreePhase1message(&static_cast<proto::Phase1&>(msg));
            } 
          //if(params.signClientProposals) delete txn;
          return false;
        }
      }
    return true;
}

//Called on network thread. cont// is dispatch wrapper 
// void* Server::ManageClientProposal(proto::Phase1 *msg, const TransportAddress &remote){

//     proto::Transaction *txn;
//     if(params.signClientProposals){
//         txn = new proto::Transaction();
//         txn->ParseFromString(msg.signed_txn().data());
//     }
//     else{
//         txn = msg.mutable_txn();
//     }

//     std::string txnDigest = TransactionDigest(*txn, params.hashDigest); //could parallelize it too hypothetically
//   *txn->mutable_txndigest() = txnDigest; //Hack to have access to txnDigest inside TXN later (used for abstain conflict)

//    //check if p1MetaData contains 
//    p1MetaDataMap::const_accessor c;
//    bool hasP1 = p1MetaData.find(c, txnDigest); // why does it have to be insert. Is there a way to get hasP1 without?
//    c.release();
//    if(hasP1) return (void*) txn;

   

//    //Offload: to other thread, include callback cont. 
//    auto f =[](){
//        //Verification
//    };
//    auto cb = [this, msg](void* valid_tx){
//        if(valid_tx){ //implies valid_tx was proto tx.
//             auto cont = [this, msg, tx](){
//                 this->HandlePhase1(remote, msg, (proto::Transaction*) tx);
//                 return (void*) true;
//             }
//             transport->DispatchTP_main(std::move(cont));
//        }
//        else{ //implies valid_tx was bool==false
//        // Free MSG
//        // Delete Tx
//          return;
//        }
//    };

//    if(params.signClientProposals){
//         //if(!VerifyDependencies()) return (void*) false;
//         if(!VerifyClientProposal(msg, txn, txnDigest)) return (void*) false;
//     }
//     return (void*) txn;
// }
//Network handler then calls:
// HandlePhase1(msg, (proto::Transaction*) txn)


//For fallback: If has p2 or writeback. Also skip.


bool Server::VerifyClientProposal(::google::protobuf::Message &msg, const proto::Transaction *txn, std::string &txnDigest, bool fallback)
    {
        if(fallback){
            return VerifyClientProposal(static_cast<proto::Phase1FB&>(msg), txn, txnDigest);
        }
        else{
            return VerifyClientProposal(static_cast<proto::Phase1&>(msg), txn, txnDigest);
        }
    }

//Verifies whether Client proposal was signed by correct client.
//A proposal (p1) is valid if: 1) The client_id included in the txn (in TS) matches the signer
//                             2) The signature matches the txn contents
bool Server::VerifyClientProposal(proto::Phase1 &msg, const proto::Transaction *txn, std::string &txnDigest)
    {
       
       Debug("Verifying Client proposal on Normal path. txn: %s", BytesToHex(txnDigest, 16).c_str());

         //1. check TXN.TS.id = client_id (only signing client should claim this id in timestamp
         if(txn->timestamp().id() != msg.signed_txn().process_id()){
            Debug("Client id[%d] does not match Timestamp with id[%d] for txn %s", 
                   msg.signed_txn().process_id(), txn->timestamp().id(), BytesToHex(txnDigest, 16).c_str());
            if((params.mainThreadDispatching && (!params.dispatchMessageReceive || params.parallel_CCC)) || (params.multiThreading && params.signClientProposals)) FreePhase1message(&msg);
            //if(params.signClientProposals) delete txn; //true by definition of reaching this function (kept param for readability)
            return false;
         }
    
         //2. check signature matches txn signed by client (use GetClientID)
         //Verify directly (without verifier object) -- alternatively 
         //std::cerr << "Verifying txn: " << BytesToHex(txnDigest, 16).c_str() << " client id: " << msg.signed_txn().process_id() << "; clientKeyid: " << keyManager->GetClientKeyId(msg.signed_txn().process_id()) << std::endl;
         if (!client_verifier->Verify(keyManager->GetPublicKey(keyManager->GetClientKeyId(msg.signed_txn().process_id())),
          msg.signed_txn().data(),
          msg.signed_txn().signature())) {
              Debug("Client signatures invalid for txn %s", BytesToHex(txnDigest, 16).c_str());
            if((params.mainThreadDispatching && (!params.dispatchMessageReceive || params.parallel_CCC)) || (params.multiThreading && params.signClientProposals)) FreePhase1message(&msg);
            //if(params.signClientProposals) delete txn; //true by definition of reaching this function (kept param for readability)
            return false;
          }
        
          //3. Store signed p1 for future relays.
          Debug("Storing signed_txn %s for future relays", BytesToHex(txnDigest, 16).c_str());
          p1MetaDataMap::accessor c;
          p1MetaData.insert(c, txnDigest);
          c->second.hasSignedP1 = true;
          c->second.signed_txn = msg.release_signed_txn();
          UW_ASSERT(c->second.signed_txn);
          c.release();

          Debug("Client verification successful for txn %s", BytesToHex(txnDigest, 16).c_str());
      //Latency_End(&verifyLat);
      return true;
    
    }

bool Server::VerifyClientProposal(proto::Phase1FB &msg, const proto::Transaction *txn, std::string &txnDigest)
    {
       
         Debug("Verifying Client proposal on FB path. txn: %s", BytesToHex(txnDigest, 16).c_str());

         //1. check TXN.TS.id = client_id (only signing client should claim this id in timestamp
         if(txn->timestamp().id() != msg.signed_txn().process_id()){
            Debug("Client id[%d] does not match Timestamp with id[%d] for txn %s", 
                   msg.signed_txn().process_id(), txn->timestamp().id(), BytesToHex(txnDigest, 16).c_str());
            if((params.mainThreadDispatching && (!params.dispatchMessageReceive || params.parallel_CCC)) || (params.multiThreading && params.signClientProposals))FreePhase1FBmessage(&msg);          
            //if(params.signClientProposals) delete txn; //true by definition of reaching this function (kept param for readability)
            return false;
         }
         //2. check signature matches txn signed by client (use GetClientID)
         //Verify directly (without verifier object) -- alternatively 
         //std::cerr << "Verifying txn: " << BytesToHex(txnDigest, 16).c_str() << " client id: " << msg.signed_txn().process_id() << "; clientKeyid: " << keyManager->GetClientKeyId(msg.signed_txn().process_id()) << std::endl;
         if (!client_verifier->Verify(keyManager->GetPublicKey(keyManager->GetClientKeyId(msg.signed_txn().process_id())),
          msg.signed_txn().data(),
          msg.signed_txn().signature())) {
              Debug("Client signatures invalid for txn %s", BytesToHex(txnDigest, 16).c_str());
            if((params.mainThreadDispatching && (!params.dispatchMessageReceive || params.parallel_CCC)) || (params.multiThreading && params.signClientProposals)) FreePhase1FBmessage(&msg);          
            //if(params.signClientProposals) delete txn; //true by definition of reaching this function (kept param for readability)
            return false;
          }
          //3. Store signed p1 for future relays.
          Debug("Storing signed_txn %s for future relays", BytesToHex(txnDigest, 16).c_str());
          p1MetaDataMap::accessor c;
          p1MetaData.insert(c, txnDigest);
          c->second.hasSignedP1 = true;
          c->second.signed_txn = msg.release_signed_txn();
          c.release();

          Debug("Client verification successful for txn %s", BytesToHex(txnDigest, 16).c_str());
      //Latency_End(&verifyLat);
      return true;
    
    }


//Tries to Prepare a transaction by calling the OCC-Check and the Reply Handler afterwards
//Dispatches the job to a worker thread if parallel_CCC = true
void* Server::TryPrepare(uint64_t reqId, const TransportAddress &remote, proto::Transaction *txn,
                        std::string &txnDigest, bool isGossip) //, const proto::CommittedProof *committedProof, const proto::Transaction *abstain_conflict, proto::ConcurrencyControl::Result &result)
  {
    Debug("Calling TryPrepare for txn[%s] on MainThread %d", BytesToHex(txnDigest, 16).c_str(), sched_getcpu());

    CheckWaitingQueries(txnDigest, txn->timestamp(), false, true); //is_abort = false //Check for waiting queries in non-blocking fashion.
    //NOTE: If want to incorporate the result from prepare (in case it is abort), then need to move this after Occ Check.

    //current_views[txnDigest] = 0;
    p2MetaDataMap::accessor p;
    p2MetaDatas.insert(p, txnDigest);
    p.release();

    // if(!params.signClientProposals) txn = msg.release_txn(); //Only release it here so that we can forward complete P1 message without making any wasteful copies

    //  ongoingMap::accessor o;
    //  std::cerr << "ONGOING INSERT (Normal): " << BytesToHex(txnDigest, 16).c_str() << " On CPU: " << sched_getcpu()<< std::endl;
    //  //ongoing.insert(o, std::make_pair(txnDigest, txn));
    //  ongoing.insert(o, txnDigest);
    //  o->second.txn = txn;
    //  o->second.num_concurrent_clients++;
    //  o.release();
     //TODO: DO BOTH OF THESE META DATA INSERTS ONLY IF VALIDATION PASSES, i.e. move them into TryPrepare?
     //OR DELETE THEM AGAIN IF VALIDATION FAILS. (requires a counter of num_ongoing inserts to gaurantee its not removed if a parallel client added it.)
  //NOTE: Ongoing *must* be added before p2/wb since the latter dont include it themselves as an optimization
  //TCP FIFO guarantees that this happens, but one cannot dispatch parallel P1 before logging ongoing or else it could be ordered after p2/wb. (p2/wb can be received based on other replicas/shards replies -- i.e. without this replicas p1 completion)
    

    Timestamp retryTs;
    //TODO: Remove arguments of TryPrepare and ProcessProposal.
    proto::ConcurrencyControl::Result result;
    const proto::CommittedProof *committedProof = nullptr;
    const proto::Transaction *abstain_conflict = nullptr;

    if(!params.parallel_CCC || !params.mainThreadDispatching){

      result = DoOCCCheck(reqId, remote, txnDigest, *txn, retryTs,
            committedProof, abstain_conflict, false, isGossip); //forwarded messages dont need to be treated as original client.
      
      HandlePhase1CB(reqId, result, committedProof, txnDigest, remote, abstain_conflict, isGossip);

      return (void*) true;
    }
    else{ // if mainThreadDispatching && parallel OCC.
      auto f = [this, reqId, remote_ptr = remote.clone(), txnDigest, txn, committedProof, abstain_conflict, isGossip]() mutable {
        Timestamp retryTs;
          //check if concurrently committed/aborted already, and if so return
          ongoingMap::const_accessor o;
          if(!ongoing.find(o, txnDigest)){
            Debug("Already concurrently Committed/Aborted txn[%s]", BytesToHex(txnDigest, 16).c_str());
            if(committed.find(txnDigest) != committed.end()){
                SendPhase1Reply(reqId, proto::ConcurrencyControl::COMMIT, nullptr, txnDigest, remote_ptr, nullptr);
                //TODO: Eventually update to send direct WritebackAck
            }
            else if(aborted.find(txnDigest) != aborted.end()){
                SendPhase1Reply(reqId, proto::ConcurrencyControl::ABSTAIN, nullptr, txnDigest, remote_ptr, nullptr);
                //TODO: Eventually update to send direct WritebackAck
            }
            else{
                Panic("No longer ongoing, but neither committed nor aborted");
            }
            //if((params.mainThreadDispatching && (!params.dispatchMessageReceive || params.parallel_CCC)) || (params.multiThreading && params.signClientProposals)) FreePhase1message(msg_ptr);
            //if(params.signClientProposals) delete txn; //Could've been concurrently moved to committed --> cannot risk deleting that version. Risking possible leak here instead (although will never really be called)
            return (void*) false;
          }
          o.release();


        Debug("starting occ check for txn: %s", BytesToHex(txnDigest, 16).c_str());
        proto::ConcurrencyControl::Result *result = new proto::ConcurrencyControl::Result(this->DoOCCCheck(reqId,
        *remote_ptr, txnDigest, *txn, retryTs, committedProof, abstain_conflict, false, isGossip));

        HandlePhase1CB(reqId, *result, committedProof, txnDigest, *remote_ptr, abstain_conflict, isGossip);
      
        delete result;
        delete remote_ptr;
        return (void*) true;
      };
      transport->DispatchTP_noCB(std::move(f));
      return (void*) true;
    }
  }

  void Server::AddOngoing( std::string &txnDigest, proto::Transaction* txn){

      if(params.query_params.optimisticTxID){ //If using optimisticTxID: Store ts to Tx mapping
        ts_to_txMap::accessor t; 
        Debug("TS_TO_TX insert TX[%s] with TS[%lu:%lu]", BytesToHex(txnDigest, 16).c_str(), txn->timestamp().timestamp(), txn->timestamp().id());
        bool first = ts_to_tx.insert(t, MergeTimestampId(txn->timestamp().timestamp(), txn->timestamp().id()));
        if(!first && !TEST_PREPARE_SYNC && t->second != txnDigest){
          Panic("Two different Transactions [%s:%s] have the same Timestamp: [%lu:%lu]. Equivocation", t->second, txnDigest, txn->timestamp().timestamp(), txn->timestamp().id()); 
                  // Report issuing client (txn->client_id() = txn->timestamp.id()) 
                  //TODO: Hard Abort/Clean this TX & forward to other replicas so they can resolve TXs
        } 
        t->second = txnDigest;
        t.release();
        //Note: Timestamp mappings are not garbage collected during clean. They may only be deleted when garbage collecting < Low Watermark ==> because then we no longer need to access the Tx
      }
  
      ongoingMap::accessor o;
      //std::cerr << "ONGOING INSERT (Fallback): " << BytesToHex(txnDigest, 16).c_str() << " On CPU: " << sched_getcpu()<< std::endl;
      //ongoing.insert(o, std::make_pair(txnDigest, txn));
      ongoing.insert(o, txnDigest);
      o->second.txn = txn;
      o->second.num_concurrent_clients++;  
      o.release();

    return;
  }

  void Server::RemoveOngoing(std::string &txnDigest){
        ongoingMap::accessor o;
        //std::cerr << "ONGOING ERASE (Normal-INVALID): " << BytesToHex(txnDigest, 16).c_str() << " On CPU: " << sched_getcpu()<< std::endl;
        if(!ongoing.find(o, txnDigest)) return;
        o->second.num_concurrent_clients--;
        if(o->second.num_concurrent_clients==0){
            delete o->second.txn;    //TODO: instead of this manual deletion just store a shared_ptr to txn. //Note: still need the shared counting, since we don't want to erase ongoing
            ongoing.erase(o);
        }
        o.release();
        Panic("Proposal should be valid");
      return;
  }

//TODO: Add remote.clone() for better style..
  void Server::ProcessProposal(proto::Phase1 &msg, const TransportAddress &remote, proto::Transaction *txn,
                        std::string &txnDigest, bool isGossip)// ,const proto::CommittedProof *committedProof, const proto::Transaction *abstain_conflict,proto::ConcurrencyControl::Result &result)
  {

    if(!params.signClientProposals) txn = msg.release_txn(); //Only release it here so that we can forward complete P1 message without making any wasteful copies
    

    //Add txn speculative to ongoing BEFORE validation to ensure it exists in ongoing before any P2 or Writeback could arrive; (Follows from the fact that HandleP1, P2, Writeback are called on same thread)
    //If verification fails, remove it again. Keep track of num_concurrent_clients to make sure we don't delete if it is still necessary.
    AddOngoing(txnDigest, txn);

    if(!params.multiThreading || !params.signClientProposals){
    //if(!params.multiThreading){
        Debug("ProcessProposal for txn[%s] on MainThread %d", BytesToHex(txnDigest, 16).c_str(), sched_getcpu());
        if(!CheckProposalValidity(msg, txn, txnDigest)){  //Check Proposal Validity already cleans up message in this case.
          RemoveOngoing(txnDigest);
          return; 
        }
        
        TryPrepare(msg.req_id(), remote, txn, txnDigest, isGossip); //, committedProof, abstain_conflict, result); //Includes call to HandlePhase1CB(..);
         //FREE MESSAGE HERE! --> Async TryPrepare only needs reqId.
        if((params.mainThreadDispatching && (!params.dispatchMessageReceive || params.parallel_CCC)) || (params.multiThreading && params.signClientProposals)) FreePhase1message(&msg);
    }
    else{ //multithreading && sign Client Proposal
        //Note: Ideally would clone remote
        auto try_prep(std::bind(&Server::TryPrepare, this, msg.req_id(), std::ref(remote), txn, txnDigest, isGossip)); //,committedProof, abstain_conflict, result));
        auto f = [this, msg_ptr = &msg, txn, txnDigest, try_prep]() mutable {
            Debug("ProcessProposal for txn[%s] on WorkerThread %d", BytesToHex(txnDigest, 16).c_str(), sched_getcpu());
            if(!CheckProposalValidity(*msg_ptr, txn, txnDigest)){  //Check Proposal Validity already cleans up message in this case.
              RemoveOngoing(txnDigest);
              return (void*) false; 
           }
            transport->DispatchTP_main(try_prep);
              //FREE MESSAGE HERE! --> Async TryPrepare only needs reqId.
           if((params.mainThreadDispatching && (!params.dispatchMessageReceive || params.parallel_CCC)) || (params.multiThreading && params.signClientProposals)) FreePhase1message(msg_ptr);
            return (void*) true;
        };
        transport->DispatchTP_noCB(std::move(f));
    }

   //////Old.

    // if(!params.signClientProposals){
    //   TryPrepare(msg, remote, txn, txnDigest, committedProof, abstain_conflict, isGossip, result); //Includes call to HandlePhase1CB(..);
    // } 
    // else{ //signClientProposals == true (requires verification)
    //   Debug("Verifying client signature for transaction %s", BytesToHex(txnDigest, 16).c_str());
    //   if(!params.multiThreading){
    //     if(!VerifyClientProposal(msg, txn, txnDigest)){
    //       return; //Ignore P1 request.
    //     }
    //     TryPrepare(msg, remote, txn, txnDigest, committedProof, abstain_conflict, isGossip, result); //Includes call to HandlePhase1CB(..);
    //   }
    //   else{ //multiThreading == true
    //     if(params.parallel_CCC && params.mainThreadDispatching){ //if OCC check is multithreaded: just dispatch proposal verification with it together!
    //       TryPrepare(msg, remote, txn, txnDigest, committedProof, abstain_conflict, isGossip, result, true); //Calls VerifyClientProposal
    //     }
    //     else{ //parallel_CCC == false, or mainThreadDispatching = false Dispatch verification, and then callback to mainthread to continue with OCC check
    //         proto::Phase1 *msg_ptr = new proto::Phase1(msg);
    //         if((params.mainThreadDispatching && (!params.dispatchMessageReceive || params.parallel_CCC)) || (params.multiThreading && params.signClientProposals)) FreePhase1message(&msg); //create a copy and delete the old one if applicable.
    //         auto f(std::bind(&Server::VerifyClientProposal, this, std::ref(*msg_ptr), txn, txnDigest));
    //         auto g(std::bind(&Server::TryPrepare, this, std::ref(*msg_ptr), std::ref(remote), txn, txnDigest, committedProof, abstain_conflict, isGossip, result, false));
            
    //         auto cb = [this, g](void* valid){
    //           if(valid){ //ingore p1 otherwise
    //             transport->DispatchTP_main(std::move(g));
    //           }
    //         };
    //         transport->DispatchTP(std::move(f), std::move(cb));
    //     }
    //   }
    // }
  }


//BufferP1Result. Ensures that only the first P1 result is buffered and used.
//It is possible for multiple different (fallback) clients to execute OCC check -- but only one result should ever be used.
//XXX if you *DONT* want to buffer Wait results then call BufferP1Result only inside SendPhase1Reply
bool Server::BufferP1Result(proto::ConcurrencyControl::Result &result,
  const proto::CommittedProof *conflict, const std::string &txnDigest, uint64_t &reqId, const TransportAddress *&remote, bool &wake_fallbacks, bool isGossip, int fb){  // fb = 0 => normal, fb = 1 => fallback, fb = 2 => dependency woke up

    p1MetaDataMap::accessor c;
    bool original_sub = BufferP1Result(c, result, conflict, txnDigest, reqId, remote, wake_fallbacks, isGossip, fb);
    c.release();
    return original_sub;
}

bool Server::BufferP1Result(p1MetaDataMap::accessor &c, proto::ConcurrencyControl::Result &result,
  const proto::CommittedProof *conflict, const std::string &txnDigest, uint64_t &reqId, const TransportAddress *&remote, bool &wake_fallbacks, bool isGossip, int fb){

    //TODO: ideally buffer abstain_conflict too.

    if(result == proto::ConcurrencyControl::IGNORE){
      Clean(txnDigest, true, true); // This is an invalid transaction (will never succeed)  -> can remove any accumulated meta data. Clean with hard = true
      return false; 
    } 

    p1MetaData.insert(c, txnDigest); //TODO: Refactor to use insert bool return value (instead of .hasP1)
    if(!c->second.hasP1){
      c->second.result = result;
      //std::cerr << "Path[" << fb << "] Buffered initial result: " << c->second.result << " for txn: " << BytesToHex(txnDigest, 64) << std::endl;

      //add abort proof so we can use it for fallbacks easily.
      //if(result == proto::ConcurrencyControl::ABORT) XXX //by default nullptr if passed
      c->second.conflict = conflict;
      c->second.hasP1 = true;
    }
    else{
      if(result != proto::ConcurrencyControl::WAIT){
        //If a result was already logged (in parallel), adopt it.
        if(c->second.result != proto::ConcurrencyControl::WAIT){
          //std::cerr << "Path[" << fb << "] Tried replacing result: " << c->second.result << " with result:" << result << " for txn: " << BytesToHex(txnDigest, 64) << std::endl;
          result = c->second.result;
          conflict = c->second.conflict;
        }
        else{ //Override Wait
          c->second.result = result;
          c->second.conflict = conflict; //by default nullptr if passed; should never be called here since WAIT can only change to COMMIT/ABSTAIN
          //std::cerr << "Path[" << fb << "] Replacing result: " << c->second.result << " with result:" << result << " for txn: " << BytesToHex(txnDigest, 64) << std::endl;
        }
      }
      else{
        //std::cerr << "Path[" << fb << "] Unable to buffer result: " << result << " for txn: " << BytesToHex(txnDigest, 64) << std::endl;
      }
    }

    bool original_sub = false;
     //if BufferP1 request was issued by original client (fb==0 && gossip==false) and result == Wait -- subscribe original client.
    if(!isGossip && result == proto::ConcurrencyControl::WAIT){
      if(fb == 0){
        Debug("Subscribing original client for transaction %s.", BytesToHex(txnDigest, 16).c_str());
        c->second.SubscribeOriginal(*remote, reqId);
      }
      if(fb == 1){
        Debug("Subscribing all interested fallback clients for transaction %s.", BytesToHex(txnDigest, 16).c_str());
        c->second.SubscribeAllInterestedFallbacks(); 
      }
    }
    

    //Check for subscribed client. Only need to reply to client once -- the first time result != Wait.
    if(result != proto::ConcurrencyControl::WAIT){
      if(c->second.sub_original){
        original_sub = true;
        remote = c->second.original;
        reqId = c->second.reqId;
        c->second.sub_original = false;
        Debug("Found subscribed original client for transaction %s with reqId %d.", BytesToHex(txnDigest, 16).c_str(), reqId);
      }
      if(c->second.fallbacks_interested){
        wake_fallbacks = true;
        c->second.fallbacks_interested = false;
      }
    }

    return original_sub;

}

void Server::WakeAllInterestedFallbacks(const std::string &txnDigest, const proto::ConcurrencyControl::Result &result, const proto::CommittedProof *conflict){  
  interestedClientsMap::accessor i;
  bool hasInterested = interestedClients.find(i, txnDigest);
  if(hasInterested){
    if(!ForwardWritebackMulti(txnDigest, i)){
      
      P1FBorganizer *p1fb_organizer = new P1FBorganizer(0, txnDigest, this);
      SetP1(0, p1fb_organizer->p1fbr->mutable_p1r(), txnDigest, result, conflict);
      Debug("Sending Phase1FBReply MULTICAST for txn: %s with result %d", BytesToHex(txnDigest, 64).c_str(), result);

      p2MetaDataMap::const_accessor p;
      p2MetaDatas.insert(p, txnDigest);
      if(p->second.hasP2){
        proto::CommitDecision decision = p->second.p2Decision;
        uint64_t decision_view = p->second.decision_view;
        SetP2(0, p1fb_organizer->p1fbr->mutable_p2r(), txnDigest, decision, decision_view);
        Debug("Including P2 Decision %d in Phase1FBReply MULTICAST for txn: %s", decision, BytesToHex(txnDigest, 64).c_str());
      }
      p.release();
      //TODO: If need reqId, can store it as pairs with the interested client.
      
      SendPhase1FBReply(p1fb_organizer, txnDigest, true);
    }
  }
  i.release();  
}

void Server::LookupP1Decision(const std::string &txnDigest, int64_t &myProcessId,
    proto::ConcurrencyControl::Result &myResult) const {
  myProcessId = -1;
  // see if we participated in this decision
   //if(params.mainThreadDispatching) p1DecisionsMutex.lock();
  p1MetaDataMap::const_accessor c;

  //p1MetaData.find(c, txnDigest);
  bool hasP1result = p1MetaData.find(c, txnDigest)? c->second.hasP1 : false;
  if(hasP1result){
  //if (p1DecisionItr != p1Decisions.end()) {
    if(c->second.result != proto::ConcurrencyControl::WAIT){
      myProcessId = id;
      myResult = c->second.result;
    }
    // else{
    //   std::cerr << "LookupP1Decision returned WAIT for txn: " <<  BytesToHex(txnDigest, 64) << std::endl;
    // }
  }
  c.release();
   //if(params.mainThreadDispatching) p1DecisionsMutex.unlock();
}

void Server::LookupP2Decision(const std::string &txnDigest, int64_t &myProcessId,
    proto::CommitDecision &myDecision) const {
  myProcessId = -1;
  // see if we participated in this decision

  p2MetaDataMap::const_accessor p;
  bool hasP2Meta = p2MetaDatas.find(p, txnDigest);
  if(hasP2Meta){
    bool hasP2 = p->second.hasP2;
    if (hasP2) {
      myProcessId = id;
      myDecision = p->second.p2Decision;
    }
  }
  p.release();
}

void Server::LookupCurrentView(const std::string &txnDigest,
    uint64_t &myCurrentView) const {

  // get our current view for a txn, by default = 0
  p2MetaDataMap::const_accessor p;
  bool hasP2Meta = p2MetaDatas.find(p, txnDigest);
  if(hasP2Meta){
    myCurrentView = p->second.current_view;
  }
  else{
    myCurrentView = 0;
  }
  p.release();

}

void Server::SetRTS(Timestamp &ts, const std::string &key){ 
    //Sets RTS timestamp. Favors readers commit chances.
    //Disable if worried about Byzantine Readers DDos, or if one wants to favor writers.
    if(params.rtsMode == 1){
   
    auto itr = rts.find(key);
    if(itr != rts.end()){
    if(ts.getTimestamp() > itr->second ) {
        rts[key] = ts.getTimestamp();              
    }
    }
    else{
    rts[key] = ts.getTimestamp();
    }
    /* update rts */
    // TODO: For "proper Aborts": how to track RTS by transaction without knowing transaction digest?
    }
    else if(params.rtsMode == 2){
    //XXX multiple RTS as set:
    
    std::pair<std::shared_mutex, std::set<Timestamp>> &rts_set = rts_list[key];
    {
        std::unique_lock lock(rts_set.first);
        rts_set.second.insert(ts);
    }
    }
    else{
    //No RTS
    }
}

void Server::ClearRTS(const google::protobuf::RepeatedPtrField<std::string> &read_set, const TimestampMessage &ts){

  if(params.rtsMode == 1){
    //Do nothing -- If we removed latest RTS then smaller ones that should be subsumed become inactive too.
  }
  else if(params.rtsMode == 2){
    for (const auto &read : read_set) {
      std::pair<std::shared_mutex, std::set<Timestamp>> &rts_set = rts_list[read];
      {
        std::unique_lock lock(rts_set.first);
        rts_set.second.erase(ts);
      }
    }
  }
  else{
    //No RTS
  }
}

void Server::ClearRTS(const google::protobuf::RepeatedPtrField<ReadMessage> &read_set, const Timestamp &ts){

  if(params.rtsMode == 1){
    //Do nothing -- If we removed latest RTS then smaller ones that should be subsumed become inactive too.
  }
  else if(params.rtsMode == 2){
    for (const auto &read : read_set) {
      std::pair<std::shared_mutex, std::set<Timestamp>> &rts_set = rts_list[read.key()];
      {
        std::unique_lock lock(rts_set.first);
        rts_set.second.erase(ts);
      }
    }
  }
  else{
    //No RTS
  }
}

//TODO: Clear ReadSet as part of Abort() and Commit()
// void Server::ClearQueryRTS(proto::Transaction *txn){

// }

//////////////////////////////////////////////////////////////// MESSAGE SIGNING/VERIFICATION HELPER FUNCTIONS

void Server::SignSendReadReply(proto::Write *write, proto::SignedMessage *signed_write, const std::function<void()> &sendCB){
      
    //If readReplyBatch is false then respond immediately, otherwise respect batching policy
    if (params.readReplyBatch) {
        // move: sendCB = std::move(sendCB) or {std::move(sendCB)}
        MessageToSign(write, signed_write, [sendCB, write]() {
            sendCB();
            delete write;
        });

    } else if (params.signatureBatchSize == 1) {

        if(params.multiThreading){
            auto f = [this, signed_write, sendCB = std::move(sendCB), write]()
            {
              SignMessage(write, keyManager->GetPrivateKey(id), id, signed_write);
              sendCB();
              delete write;
              return (void*) true;
            };
            transport->DispatchTP_noCB(std::move(f));
        }
        else{
            SignMessage(write, keyManager->GetPrivateKey(id), id, signed_write);
            sendCB();
            delete write;
        }

    } else {

        if(params.multiThreading){ //TODO: If read is already on a worker thread, then it does not make sense to defer signing again...

            std::vector<::google::protobuf::Message *> msgs;
            msgs.push_back(write);
            std::vector<proto::SignedMessage *> smsgs;
            smsgs.push_back(signed_write);

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
           
            std::vector<::google::protobuf::Message *> msgs;
            msgs.push_back(write);
            std::vector<proto::SignedMessage *> smsgs;
            smsgs.push_back(signed_write);
            SignMessages(msgs, keyManager->GetPrivateKey(id), id, smsgs, params.merkleBranchFactor);
            sendCB();
            delete write;
        }
    }
}

void Server::ManagePhase2Validation(const TransportAddress &remote, proto::Phase2 &msg, const std::string *txnDigest, const proto::Transaction *txn, const std::function<void()> &sendCB, proto::Phase2Reply* phase2Reply, 
     const std::function<void()> &cleanCB, const int64_t &myProcessId, const proto::ConcurrencyControl::Result &myResult)
{
    TransportAddress *remoteCopy2 = remote.clone();
        if(params.multiThreading){
          mainThreadCallback mcb(std::bind(&Server::HandlePhase2CB, this, remoteCopy2,
             &msg, txnDigest, sendCB, phase2Reply, cleanCB, std::placeholders::_1));

          //OPTION 1: Validation itself is synchronous, i.e. is one job (Would need to be extended with thread safety)
              //std::function<void*()> f (std::bind(&ValidateP1RepliesWrapper, msg_copy.decision(), false, txn, txnDigest, msg.grouped_sigs(), keyManager, &config, myProcessId, myResult, verifier));
              //transport->DispatchTP(f, mcb);

          //OPTION2: Validation itself is asynchronous (each verification = 1 job)
          if(params.batchVerification){
            asyncBatchValidateP1Replies(msg.decision(),
                  false, txn, txnDigest, msg.grouped_sigs(), keyManager, &config, myProcessId,
                  myResult, verifier, std::move(mcb), transport, true);
            return;

          }
          else{
            asyncValidateP1Replies(msg.decision(),
                  false, txn, txnDigest, msg.grouped_sigs(), keyManager, &config, myProcessId,
                  myResult, verifier, std::move(mcb), transport, true);
            return;
          }
        }
        else{
          if(params.batchVerification){
            mainThreadCallback mcb(std::bind(&Server::HandlePhase2CB, this, remoteCopy2,
              &msg, txnDigest, sendCB, phase2Reply, cleanCB, std::placeholders::_1));

            asyncBatchValidateP1Replies(msg.decision(),
                  false, txn, txnDigest, msg.grouped_sigs(), keyManager, &config, myProcessId,
                  myResult, verifier, std::move(mcb), transport, false);
            return;
          }
          else{
            if(!ValidateP1Replies(msg.decision(),
                  false, txn, txnDigest, msg.grouped_sigs(), keyManager, &config, myProcessId,
                  myResult, verifier)) {
              Debug("VALIDATE P1Replies failed.");
              return HandlePhase2CB(remoteCopy2, &msg, txnDigest, sendCB, phase2Reply, cleanCB, (void*) false);
            }
          }
        }
}

void Server::ManageWritebackValidation(proto::Writeback &msg, const std::string *txnDigest, proto::Transaction *txn){
     //Verifying signatures
  //XXX batchVerification branches are currently deprecated
  if (params.validateProofs ) {
      if(params.multiThreading){

          Debug("1: TAKING MULTITHREADING BRANCH, generating MCB for txn %s WB validation", BytesToHex(*txnDigest, 16).c_str());
          mainThreadCallback mcb(std::bind(&Server::WritebackCallback, this, &msg,
            txnDigest, txn, std::placeholders::_1));

          if(params.signedMessages && msg.has_p1_sigs()){
            if(msg.decision() == proto::COMMIT){
                stats.Increment("total_transactions_fast_commit", 1);
            }
            else if(msg.decision() == proto::ABORT){
                stats.Increment("total_transactions_fast_Abort_sigs", 1);
            }
            else{
                Panic("Only valid fast path Writeback is Commit or Abort");
            }
            int64_t myProcessId;
            proto::ConcurrencyControl::Result myResult;
            LookupP1Decision(*txnDigest, myProcessId, myResult);

            if(params.batchVerification){
              Debug("2: Taking batch branch p1 commit/abort for txn %s WB validation", BytesToHex(*txnDigest, 16).c_str());
              asyncBatchValidateP1Replies(msg.decision(),
                    true, txn, txnDigest, msg.p1_sigs(), keyManager, &config, myProcessId,
                    myResult, verifier, std::move(mcb), transport, true);
            }
            else{
              Debug("2: Taking non-batch branch p1 commit/abort for txn %s WB validation", BytesToHex(*txnDigest, 16).c_str());
              asyncValidateP1Replies(msg.decision(),
                  true, txn, txnDigest, msg.p1_sigs(), keyManager, &config, myProcessId,
                  myResult, verifier, std::move(mcb), transport, true);
            }
            return;
          }

          else if (params.signedMessages && msg.has_p2_sigs()) {
             stats.Increment("total_transactions_slow", 1);
              // require clients to include view for easier matching
              if(!msg.has_p2_view()) return;
              int64_t myProcessId;
              proto::CommitDecision myDecision;
              LookupP2Decision(*txnDigest, myProcessId, myDecision);

              if(params.batchVerification){
                Debug("2: Taking batch branch p2 for txn %s WB validation", BytesToHex(*txnDigest, 16).c_str());
                asyncBatchValidateP2Replies(msg.decision(), msg.p2_view(),
                      txn, txnDigest, msg.p2_sigs(), keyManager, &config, myProcessId,
                      myDecision, verifier, std::move(mcb), transport, true);
              }
              else{
                Debug("2: Taking non-batch branch p2 for txn %s WB validation", BytesToHex(*txnDigest, 16).c_str());
                asyncValidateP2Replies(msg.decision(), msg.p2_view(),
                      txn, txnDigest, msg.p2_sigs(), keyManager, &config, myProcessId,
                      myDecision, verifier, std::move(mcb), transport, true);
              }
              return;
          }

          else if (msg.decision() == proto::ABORT && msg.has_conflict()) {
             stats.Increment("total_transactions_fast_Abort_conflict", 1);

            Debug("2: Taking Aborted conflict branch for txn %s WB validation", BytesToHex(*txnDigest, 16).c_str());
              std::string committedTxnDigest = TransactionDigest(msg.conflict().txn(),
                  params.hashDigest);
              asyncValidateCommittedConflict(msg.conflict(), &committedTxnDigest, txn,
                    txnDigest, params.signedMessages, keyManager, &config, verifier,
                    std::move(mcb), transport, true, params.batchVerification);
              return;
          }
          else if (params.signedMessages) {

             Debug("WRITEBACK[%s] decision %d, has_p1_sigs %d, has_p2_sigs %d, and"
                 " has_conflict %d.", BytesToHex(*txnDigest, 16).c_str(),
                 msg.decision(), msg.has_p1_sigs(), msg.has_p2_sigs(), msg.has_conflict());
             return WritebackCallback(&msg, txnDigest, txn, (void*) false);;
          }

      }
      //If I make the else case use the async function too, then I can collapse the duplicate code here
      //and just pass params.multiThreading as argument...
      //Currently NOT doing that because the async version does additional copies (binds) that are avoided in the single threaded code.
      else{

          if (params.signedMessages && msg.has_p1_sigs()) {
            int64_t myProcessId;
            proto::ConcurrencyControl::Result myResult;
            LookupP1Decision(*txnDigest, myProcessId, myResult);

            if(params.batchVerification){
              mainThreadCallback mcb(std::bind(&Server::WritebackCallback, this, &msg, txnDigest, txn, std::placeholders::_1));
              asyncBatchValidateP1Replies(msg.decision(),
                    true, txn, txnDigest,msg.p1_sigs(), keyManager, &config, myProcessId,
                    myResult, verifier, std::move(mcb), transport, false);
              return;
            }
            else{
              if (!ValidateP1Replies(msg.decision(), true, txn, txnDigest, msg.p1_sigs(),
                    keyManager, &config, myProcessId, myResult, verifyLat, verifier)) {
                if(msg.decision() == proto::COMMIT){
                    Debug("WRITEBACK[%s] Failed to validate P1 replies for fast commit.", BytesToHex(*txnDigest, 16).c_str());
                }
                else if(msg.decision() == proto::ABORT){
                    Debug("WRITEBACK[%s] Failed to validate P1 replies for fast abort.", BytesToHex(*txnDigest, 16).c_str());
                }
                return WritebackCallback(&msg, txnDigest, txn, (void*) false);
              }
            }
          }

          else if (params.signedMessages && msg.has_p2_sigs()) {
            if(!msg.has_p2_view()) return;
            int64_t myProcessId;
            proto::CommitDecision myDecision;
            LookupP2Decision(*txnDigest, myProcessId, myDecision);


            if(params.batchVerification){
              mainThreadCallback mcb(std::bind(&Server::WritebackCallback, this, &msg, txnDigest, txn, std::placeholders::_1));
              asyncBatchValidateP2Replies(msg.decision(), msg.p2_view(),
                    txn, txnDigest, msg.p2_sigs(), keyManager, &config, myProcessId,
                    myDecision, verifier, std::move(mcb), transport, false);
              return;
            }
            else{
              if (!ValidateP2Replies(msg.decision(), msg.p2_view(), txn, txnDigest, msg.p2_sigs(),
                    keyManager, &config, myProcessId, myDecision, verifyLat, verifier)) {
                Debug("WRITEBACK[%s] Failed to validate P2 replies for decision %d.",
                    BytesToHex(*txnDigest, 16).c_str(), msg.decision());
                return WritebackCallback(&msg, txnDigest, txn, (void*) false);
              }
            }

          } else if (msg.decision() == proto::ABORT && msg.has_conflict()) {
            std::string committedTxnDigest = TransactionDigest(msg.conflict().txn(),
                params.hashDigest);

                if(params.batchVerification){
                  mainThreadCallback mcb(std::bind(&Server::WritebackCallback, this, &msg, txnDigest, txn, std::placeholders::_1));
                  asyncValidateCommittedConflict(msg.conflict(), &committedTxnDigest, txn,
                        txnDigest, params.signedMessages, keyManager, &config, verifier,
                        std::move(mcb), transport, false, params.batchVerification);
                  return;
                }
                else{
                  if (!ValidateCommittedConflict(msg.conflict(), &committedTxnDigest, txn,
                        txnDigest, params.signedMessages, keyManager, &config, verifier)) {
                    Debug("WRITEBACK[%s] Failed to validate committed conflict for fast abort.",
                        BytesToHex(*txnDigest, 16).c_str());
                    return WritebackCallback(&msg, txnDigest, txn, (void*) false);
                }

            }
          } else if (params.signedMessages) {
            Debug("WRITEBACK[%s] decision %d, has_p1_sigs %d, has_p2_sigs %d, and"
                " has_conflict %d.", BytesToHex(*txnDigest, 16).c_str(),
                msg.decision(), msg.has_p1_sigs(), msg.has_p2_sigs(), msg.has_conflict());
            return WritebackCallback(&msg, txnDigest, txn, (void*) false);
          }
        }

  }

  WritebackCallback(&msg, txnDigest, txn, (void*) true);
}


//////////////////////////////////////////////////////////////// MESSAGE SIGNING HELPER FUNCTIONS

void Server::MessageToSign(::google::protobuf::Message* msg,
      proto::SignedMessage *signedMessage, signedCallback cb) {


  ////auto lockScope = params.mainThreadDispatching ? std::unique_lock<std::mutex>(mainThreadMutex) : std::unique_lock<std::mutex>();
  Debug("Exec MessageToSign by CPU: %d", sched_getcpu());

  if(params.multiThreading){
      if (params.signatureBatchSize == 1) {

          Debug("(multithreading) dispatching signing");
          auto f = [this, msg, signedMessage, cb = std::move(cb)](){
            SignMessage(msg, keyManager->GetPrivateKey(id), id, signedMessage);
            cb();
            return (void*) true;
          };
          transport->DispatchTP_noCB(std::move(f));
      }

      else {
        Debug("(multithreading) adding sig request to localbatchSigner");

        batchSigner->asyncMessageToSign(msg, signedMessage, std::move(cb));
      }
  }
  else{
    if (params.signatureBatchSize == 1) {

      SignMessage(msg, keyManager->GetPrivateKey(id), id, signedMessage);
      cb();
      //if multithread: Dispatch f: SignMessage and cb.
    } else {
      batchSigner->MessageToSign(msg, signedMessage, std::move(cb));
    }
  }
}

//XXX Simulated HMAC code
std::unordered_map<uint64_t, std::string> sessionKeys;
void CreateSessionKeys();
bool ValidateHMACedMessage(const proto::SignedMessage &signedMessage);
void CreateHMACedMessage(const ::google::protobuf::Message &msg, proto::SignedMessage* signedMessage);
// assume these are somehow secretly shared before hand

//TODO: If one wants to use Macs for Clients, need to add it to keymanager (in advance or dynamically based off id)
//can use client id to replica id (group * n + idx)
void Server::CreateSessionKeys(){
  for (uint64_t i = 0; i < config.n; i++) {
    if (i > idx) {
      sessionKeys[i] = std::string(8, (char) idx + 0x30) + std::string(8, (char) i + 0x30);
    } else {
      sessionKeys[i] = std::string(8, (char) i + 0x30) + std::string(8, (char) idx + 0x30);
    }
  }
}

// create MAC messages and verify them: Used for all to all leader election.
bool Server::ValidateHMACedMessage(const proto::SignedMessage &signedMessage) {

  proto::HMACs hmacs;
  hmacs.ParseFromString(signedMessage.signature());
  return crypto::verifyHMAC(signedMessage.data(), (*hmacs.mutable_hmacs())[idx], sessionKeys[signedMessage.process_id() % config.n]);
}

void Server::CreateHMACedMessage(const ::google::protobuf::Message &msg, proto::SignedMessage* signedMessage) {

  const std::string &msgData = msg.SerializeAsString();
  signedMessage->set_data(msgData);
  signedMessage->set_process_id(id);

  proto::HMACs hmacs;
  for (uint64_t i = 0; i < config.n; i++) {
    (*hmacs.mutable_hmacs())[i] = crypto::HMAC(msgData, sessionKeys[i]);
  }
  signedMessage->set_signature(hmacs.SerializeAsString());
}

//////////////////////////////////////////////////////////////////////////// PROTOBUF HELPER FUNCTIONS
//Note: Re-use protobuf messages to minimize allocation costs
//Note: When multi-threading, it turns out that holding locks adds more overheads than allocation (e.g. acquiring lock for GetUnused on network thread, but acquiring lock to Free on main or worker thread)

//TODO: replace all of these with moodycamel queue (just check that try_dequeue is successful)
proto::ReadReply *Server::GetUnusedReadReply() {
  return new proto::ReadReply();

  // std::unique_lock<std::mutex> lock(readReplyProtoMutex);
  // proto::ReadReply *reply;
  // if (readReplies.size() > 0) {
  //   reply = readReplies.back();
  //   reply->Clear();
  //   readReplies.pop_back();
  // } else {
  //   reply = new proto::ReadReply();
  // }
  // return reply;
}

proto::Phase1Reply *Server::GetUnusedPhase1Reply() {
  return new proto::Phase1Reply();

  // //Latency_Start(&waitOnProtoLock);
  // std::unique_lock<std::mutex> lock(p1ReplyProtoMutex);
  // //Latency_End(&waitOnProtoLock);
  // proto::Phase1Reply *reply;
  // if (p1Replies.size() > 0) {
  //   reply = p1Replies.back();
  //   //reply->Clear(); //can move this to Free if want more work at threads
  //   p1Replies.pop_back();
  // } else {
  //   reply = new proto::Phase1Reply();
  // }
  // return reply;
}

proto::Phase2Reply *Server::GetUnusedPhase2Reply() {
  return new proto::Phase2Reply();

  // //Latency_Start(&waitOnProtoLock);
  // std::unique_lock<std::mutex> lock(p2ReplyProtoMutex);
  // //Latency_End(&waitOnProtoLock);
  // proto::Phase2Reply *reply;
  // if (p2Replies.size() > 0) {
  //   reply = p2Replies.back();
  //   //reply->Clear();
  //   p2Replies.pop_back();
  // } else {
  //   reply = new proto::Phase2Reply();
  // }
  // return reply;
}

proto::Read *Server::GetUnusedReadmessage() {
  return new proto::Read();

  // std::unique_lock<std::mutex> lock(readProtoMutex);
  // proto::Read *msg;
  // if (readMessages.size() > 0) {
  //   msg = readMessages.back();
  //   msg->Clear();
  //   readMessages.pop_back();
  // } else {
  //   msg = new proto::Read();
  // }
  // return msg;
}

proto::Phase1 *Server::GetUnusedPhase1message() {
  return new proto::Phase1();

  // //Latency_Start(&waitOnProtoLock);
  // std::unique_lock<std::mutex> lock(p1ProtoMutex);
  // //Latency_End(&waitOnProtoLock);
  // proto::Phase1 *msg;
  // if (p1messages.size() > 0) {
  //   msg = p1messages.back();
  //   msg->Clear();
  //   p1messages.pop_back();
  // } else {
  //   msg = new proto::Phase1();
  // }
  // return msg;
}

proto::Phase2 *Server::GetUnusedPhase2message() {
  return new proto::Phase2();

  // //Latency_Start(&waitOnProtoLock);
  // std::unique_lock<std::mutex> lock(p2ProtoMutex);
  // //Latency_End(&waitOnProtoLock);
  // proto::Phase2 *msg;
  // if (p2messages.size() > 0) {
  //   msg = p2messages.back();
  //   msg->Clear();
  //   p2messages.pop_back();
  // } else {
  //   msg = new proto::Phase2();
  // }
  // return msg;
}

proto::Writeback *Server::GetUnusedWBmessage() {
  return new proto::Writeback();

  // //Latency_Start(&waitOnProtoLock);
  // std::unique_lock<std::mutex> lock(WBProtoMutex);
  // //Latency_End(&waitOnProtoLock);
  // proto::Writeback *msg;
  // if (WBmessages.size() > 0) {
  //   msg = WBmessages.back();
  //   msg->Clear();
  //   WBmessages.pop_back();
  // } else {
  //   msg = new proto::Writeback();
  // }
  // return msg;
}

void Server::FreeReadReply(proto::ReadReply *reply) {
  delete reply;
  // std::unique_lock<std::mutex> lock(readReplyProtoMutex);
  // //reply->Clear();
  // readReplies.push_back(reply);
}

void Server::FreePhase1Reply(proto::Phase1Reply *reply) {
  delete reply;
  // std::unique_lock<std::mutex> lock(p1ReplyProtoMutex);
  //
  // reply->Clear();
  // p1Replies.push_back(reply);
}

void Server::FreePhase2Reply(proto::Phase2Reply *reply) {
  delete reply;
  // std::unique_lock<std::mutex> lock(p2ReplyProtoMutex);
  // reply->Clear();
  // p2Replies.push_back(reply);
}

void Server::FreeReadmessage(proto::Read *msg) {
  delete msg;
  // std::unique_lock<std::mutex> lock(readProtoMutex);
  // readMessages.push_back(msg);
}

void Server::FreePhase1message(proto::Phase1 *msg) {
  delete msg;
  // //Latency_Start(&waitOnProtoLock);
  // std::unique_lock<std::mutex> lock(p1ProtoMutex);
  // //Latency_End(&waitOnProtoLock);
  // p1messages.push_back(msg);
}

void Server::FreePhase2message(proto::Phase2 *msg) {
  delete msg;
  // //Latency_Start(&waitOnProtoLock);
  // std::unique_lock<std::mutex> lock(p2ProtoMutex);
  // //Latency_End(&waitOnProtoLock);
  // p2messages.push_back(msg);
}

void Server::FreeWBmessage(proto::Writeback *msg) {
  delete msg;
  // //Latency_Start(&waitOnProtoLock);
  // std::unique_lock<std::mutex> lock(WBProtoMutex);
  // //Latency_End(&waitOnProtoLock);
  // WBmessages.push_back(msg);
}


//Fallback message re-use allocators

proto::Phase1FB *Server::GetUnusedPhase1FBmessage() {
  return new proto::Phase1FB();

  // //Latency_Start(&waitOnProtoLock);
  // std::unique_lock<std::mutex> lock(p1ProtoMutex);
  // //Latency_End(&waitOnProtoLock);
  // proto::Phase1FB *msg;
  // if (p1FBmessages.size() > 0) {
  //   msg = p1FBmessages.back();
  //   msg->Clear();
  //   p1FBmessages.pop_back();
  // } else {
  //   msg = new proto::Phase1FB();
  // }
  // return msg;
}

void Server::FreePhase1FBmessage(proto::Phase1FB *msg) {
  delete msg;
  // //Latency_Start(&waitOnProtoLock);
  // std::unique_lock<std::mutex> lock(p1FBProtoMutex);
  // //Latency_End(&waitOnProtoLock);
  // p1FBmessages.push_back(msg);
}

proto::Phase1FBReply *Server::GetUnusedPhase1FBReply(){
  return new proto::Phase1FBReply();
  // //Latency_Start(&waitOnProtoLock);
  // std::unique_lock<std::mutex> lock(P1FBRProtoMutex);
  // //Latency_End(&waitOnProtoLock);
  // proto::Phase1FBReply *msg;
  // if (P1FBReplies.size() > 0) {
  //   msg = P1FBReplies.back();
  //   msg->Clear();
  //   P1FBReplies.pop_back();
  // } else {
  //   msg = new proto::proto::Phase1FBReply();
  // }
  // return msg;
}

void Server::FreePhase1FBReply(proto::Phase1FBReply *msg){
  delete msg;
  // //Latency_Start(&waitOnProtoLock);
  // std::unique_lock<std::mutex> lock(P1FBRProtoMutex);
  // //Latency_End(&waitOnProtoLock);
  // P1FBReplies.push_back(msg);
}

proto::Phase2FB *Server::GetUnusedPhase2FBmessage() {
  return new proto::Phase2FB();

  // //Latency_Start(&waitOnProtoLock);
  // std::unique_lock<std::mutex> lock(p2FBProtoMutex);
  // //Latency_End(&waitOnProtoLock);
  // proto::Phase2FB *msg;
  // if (p2FBmessages.size() > 0) {
  //   msg = p2FBmessages.back();
  //   msg->Clear();
  //   p2FBmessages.pop_back();
  // } else {
  //   msg = new proto::Phase2FB();
  // }
  // return msg;
}

void Server::FreePhase2FBmessage(const proto::Phase2FB *msg) {
  delete msg;
  // //Latency_Start(&waitOnProtoLock);
  // std::unique_lock<std::mutex> lock(p2FBProtoMutex);
  // //Latency_End(&waitOnProtoLock);
  // p2FBmessages.push_back(msg);
}

proto::Phase2FBReply *Server::GetUnusedPhase2FBReply(){
  return new proto::Phase2FBReply();
  // //Latency_Start(&waitOnProtoLock);
  // std::unique_lock<std::mutex> lock(P2FBRProtoMutex);
  // //Latency_End(&waitOnProtoLock);
  // proto::Phase2FBReply *msg;
  // if (P2FBReplies.size() > 0) {
  //   msg = P2FBReplies.back();
  //   msg->Clear();
  //   P2FBReplies.pop_back();
  // } else {
  //   msg = new proto::proto::Phase2FBReply();
  // }
  // return msg;
}

void Server::FreePhase2FBReply(proto::Phase2FBReply *msg){
  delete msg;
  // //Latency_Start(&waitOnProtoLock);
  // std::unique_lock<std::mutex> lock(P2FBRProtoMutex);
  // //Latency_End(&waitOnProtoLock);
  // P2FBReplies.push_back(msg);
}


proto::InvokeFB *Server::GetUnusedInvokeFBmessage(){
  return new proto::InvokeFB();
}

void Server::FreeInvokeFBmessage(proto::InvokeFB *msg){
  delete msg;
}

proto::SendView *Server::GetUnusedSendViewMessage(){
  return new proto::SendView();
}

void Server::FreeSendViewMessage(proto::SendView *msg){
  delete msg;
}

proto::ElectMessage *Server::GetUnusedElectMessage(){
  return new proto::ElectMessage();
}

void Server::FreeElectMessage(proto::ElectMessage *msg){
  delete msg;
}

proto::ElectFB *Server::GetUnusedElectFBmessage(){
  return new proto::ElectFB();
}

void Server::FreeElectFBmessage(proto::ElectFB *msg){
  delete msg;
}

proto::DecisionFB *Server::GetUnusedDecisionFBmessage(){
  return new proto::DecisionFB();
}

void Server::FreeDecisionFBmessage(proto::DecisionFB *msg){
  delete msg;
}

proto::MoveView *Server::GetUnusedMoveView(){
  return new proto::MoveView();
}

void Server::FreeMoveView(proto::MoveView *msg){
  delete msg;
}

proto::QueryRequest* Server::GetUnusedQueryRequestMessage(){
  return new proto::QueryRequest();
}

void Server::FreeQueryRequestMessage(proto::QueryRequest *msg){
  delete msg;
}

proto::SyncClientProposal* Server::GetUnusedSyncClientProposalMessage(){
  return new proto::SyncClientProposal();
}

void Server::FreeSyncClientProposalMessage(proto::SyncClientProposal *msg){
  delete msg;
}

proto::RequestMissingTxns* Server::GetUnusedRequestTxMessage(){
  return new proto::RequestMissingTxns();

}
void Server::FreeRequestTxMessage(proto::RequestMissingTxns *msg){
  delete msg;
}

proto::SupplyMissingTxns* Server::GetUnusedSupplyTxMessage(){
  return new proto::SupplyMissingTxns();
}

void Server::FreeSupplyTxMessage(proto::SupplyMissingTxns *msg){
  delete msg;
}

proto::PointQueryResultReply* Server::GetUnusedPointQueryResultReply(){
  return new proto::PointQueryResultReply();
}

void Server::FreePointQueryResultReply(proto::PointQueryResultReply *msg){
  delete msg;
}


//generic delete function.
void Server::FreeMessage(::google::protobuf::Message *msg){
  delete msg;
}

} // namespace pequinstore
