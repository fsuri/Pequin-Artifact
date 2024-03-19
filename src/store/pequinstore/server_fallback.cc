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

  //ongoingMap::const_accessor o;
  ongoingMap::accessor o;
  bool ongoingItr = ongoing.find(o, dependency_txnDig);
  if(!ongoingItr) return;  //If txnDigest no longer ongoing, then no FB necessary as it has completed already
  tx = o->second.txn;
  p1MetaDataMap::accessor c;
  //o.release();

  proto::RelayP1 relayP1; // Use local object instead of global.

  // proto::RelayP1 *relayP1;
  // if(params.parallel_CCC){
  //   relayP1 = GetUnusedRelayP1(); //if invokations of SendRelayP1 come from different threads --> use separate objects.
  // } 
  // else{
  //   relayP1 = &relayP1Msg; // if all invokations of SendRelayP1 come from the same thread --> use global object.
  // } 
  relayP1.Clear();
  relayP1.set_dependent_id(dependent_id);
  relayP1.mutable_p1()->set_req_id(0); //doesnt matter, its not used for fallback requests really.
  //*relayP1.mutable_p1()->mutable_txn() = *tx; //TODO:: avoid copy by allocating, and releasing again after.
  relayP1.set_replica_id(id);

  if(params.signClientProposals){
    //b.release();
    //Note: RelayP1 is only triggered if dependency_txnDig is prepared but not committed -> implies it passed verification -> implies ongoing was added and is still true (since not yet committed)
    //If txn is in ongoing, then it must have been added to P1Meta --> since we add to ongoing in HandleP1 or HandleP1FB. 
    bool p1MetaExists = p1MetaData.find(c, dependency_txnDig); // Ignore (does not matter): Current verification --adds signed_txn-- happens only after inster ongoing. Should be swapped to guarantee the above.)
     if(!p1MetaExists || c->second.hasSignedP1 == false) Panic("Should exist since ongoing is still true");
    signed_tx = c->second.signed_txn;
    relayP1.mutable_p1()->set_allocated_signed_txn(signed_tx);
  }
  else{ //no Client sigs --> just send txn.
    relayP1.mutable_p1()->set_allocated_txn(tx);  
    //*relayP1.mutable_p1()->mutable_txn() = *tx;
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
    o->second.txn = relayP1.mutable_p1()->release_txn();
    //b.release();
  }
  o.release(); //keep ongoing locked the full duration: this guarantees that if ongoing exists, p1Meta.signed_tx exists too, since it is deleted after ongoing is locked in Clean() 

  Debug("Sent RelayP1[%s].", BytesToHex(dependent_txnDig, 256).c_str());
}

// //RELAY DEPENDENCY IN ORDER FOR CLIENT TO START FALLBACK
// //params: dependent_it = client tx identifier for blocked tx; dependency_txnDigest = tx that is stalling
// void Server::_SendRelayP1(const TransportAddress &remote, const std::string &dependency_txnDig, uint64_t dependent_id, const std::string &dependent_txnDig){

//   Debug("RelayP1[%s] timed out. Sending now!", BytesToHex(dependent_txnDig, 256).c_str());

//    //proto::RelayP1 relayP1; //use global object.
//   relayP1.Clear();
//   relayP1.set_dependent_id(dependent_id);
//   relayP1.mutable_p1()->set_req_id(0); //doesnt matter, its not used for fallback requests really.

//   if(params.signClientProposals){
//      proto::SignedMessage *signed_tx;

//     p1MetaDataMap::accessor c;
//     //Note: RelayP1 is only triggered if dependency_txnDig is prepared but not committed -> implies it passed verification -> implies ongoing was added and is still true (since not yet committed)
//     bool p1MetaExists = p1MetaData.find(c, dependency_txnDig); //If txn is in ongoing, then it must have been added to P1Meta --> since we add to ongoing in HandleP1 or HandleP1FB. 
//                                                               // (TODO: FIXME: Current verification --adds signed_txn-- happens only after inster ongoing. Should be swapped to guarantee the above.)
//      if(!p1MetaExists || c->second.hasSignedP1 == false) Panic("Should exist since ongoing is still true");
//     signed_tx = c->second.signed_txn;
//     relayP1.mutable_p1()->set_allocated_signed_txn(signed_tx);

//     if(dependent_id == -1) relayP1.set_dependent_txn(dependent_txnDig);
//     if(dependent_id == -1){
//       Debug("Sending relayP1 for dependent txn: %s stuck waiting for dependency: %s", BytesToHex(dependent_txnDig, 64).c_str(), BytesToHex(dependency_txnDig,64).c_str());
//     }

//     stats.Increment("Relays_Sent", 1);
//     transport->SendMessage(this, remote, relayP1);

//     c->second.signed_txn = relayP1.mutable_p1()->release_signed_txn();
//     c.release();
//   }
//   else{
//     proto::Transaction *tx;
    
//     ongoingMap::accessor o;
//     bool ongoingItr = ongoing.find(o, dependency_txnDig);
//     if(!ongoingItr) return;  //If txnDigest no longer ongoing, then no FB necessary as it has completed already
//     tx = o->second.txn;

//       //relayP1.mutable_p1()->set_allocated_txn(tx);  
//     *relayP1.mutable_p1()->mutable_txn() = *tx;

//     if(dependent_id == -1) relayP1.set_dependent_txn(dependent_txnDig);
//     if(dependent_id == -1){
//       Debug("Sending relayP1 for dependent txn: %s stuck waiting for dependency: %s", BytesToHex(dependent_txnDig, 64).c_str(), BytesToHex(dependency_txnDig,64).c_str());
//     }

//     stats.Increment("Relays_Sent", 1);
//     transport->SendMessage(this, remote, relayP1);

//     //o->second.txn = relayP1.mutable_p1()->release_txn();
//     o.release(); //keep ongoing locked the full duration: this guarantees that if ongoing exists, p1Meta.signed_tx exists too, since it is deleted after ongoing is locked in Clean() 
//   }
//   Debug("Sent RelayP1[%s].", BytesToHex(dependent_txnDig, 256).c_str());
// }

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
  p1MetaDataMap::accessor c; //Note: In order to subscribe clients must be accessor. p1MetaDataMap::const_accessor c;
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
       //c.release();

       proto::CommitDecision decision = p->second.p2Decision;
       uint64_t decision_view = p->second.decision_view;
       p.release();

       P1FBorganizer *p1fb_organizer = new P1FBorganizer(msg.req_id(), txnDigest, remote, this);
       //recover stored commit proof.
       if (result != proto::ConcurrencyControl::WAIT) { //if the result is WAIT, then the p1 is not necessary..
         c.release();
         SetP1(msg.req_id(), p1fb_organizer->p1fbr->mutable_p1r(), txnDigest, result, conflict);
       }
       else{
        c->second.SubscribeAllInterestedFallbacks();
        c.release();
         //Relay deeper depths.
        ManageDependencies(txnDigest, *txn, remote, 0, true); //fallback flow = true, gossip = false
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
        //c.release();
        p.release();

        if (result != proto::ConcurrencyControl::WAIT) {
          c.release();
          P1FBorganizer *p1fb_organizer = new P1FBorganizer(msg.req_id(), txnDigest, remote, this);
          SetP1(msg.req_id(), p1fb_organizer->p1fbr->mutable_p1r(), txnDigest, result, conflict);
          SendPhase1FBReply(p1fb_organizer, txnDigest);
          Debug("Sent Phase1FBReply on path hasP1 for txn: %s, sent by client: %d", BytesToHex(txnDigest, 16).c_str(), msg.req_id());

        }
        else{
          c->second.SubscribeAllInterestedFallbacks();
          c.release();
          ManageDependencies(txnDigest, *txn, remote, 0, true); //relay deeper deps
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
      if(!params.signClientProposals) txn = msg.release_txn();
      //if(params.signClientProposals) *txn->mutable_txndigest() = txnDigest; //HACK to include txnDigest to lookup signed_tx.
      *txn->mutable_txndigest() = txnDigest; //Hack to have access to txnDigest inside TXN later (used for abstain conflict, FindTableVersion, and to lookup signed_tx)
      AddOngoing(txnDigest, txn);
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

void Server::ProcessProposalFB(proto::Phase1FB &msg, const TransportAddress &remote, std::string &txnDigest, proto::Transaction* txn){
  
  if(!params.signClientProposals) txn = msg.release_txn();
  //if(params.signClientProposals) *txn->mutable_txndigest() = txnDigest; //HACK to include txnDigest to lookup signed_tx and have access to txnDigest inside TXN later (used for abstain conflict)
  *txn->mutable_txndigest() = txnDigest; //Hack to have access to txnDigest inside TXN later (used for abstain conflict, and for FindTableVersion)
  AddOngoing(txnDigest, txn);

  //Todo: Improve efficiency if Valid: insert into P1Meta and check conditions again: If now already in P1Meta then use this existing result.
  if(!params.multiThreading || !params.signClientProposals){
    if(!CheckProposalValidity(msg, txn, txnDigest, true)){
      RemoveOngoing(txnDigest);
       return;
    } 

    if(!CheckProposalValidity(msg, txn, txnDigest)) return ;
    TryExec(msg, remote, txnDigest, txn);
  }
  else{
    auto try_exec(std::bind(&Server::TryExec, this, std::ref(msg), std::ref(remote), txnDigest, txn));
    auto f = [this, &msg, txn, txnDigest, try_exec]() mutable {
        if(!CheckProposalValidity(msg, txn, txnDigest, true)){
        RemoveOngoing(txnDigest);
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

  //CheckWaitingQueries(txnDigest, txn->timestamp().timestamp(), txn->timestamp().id(), false, true); //is_abort = false //Check for waiting queries in non-blocking fashion.

  //start new current view
  // current_views[txnDigest] = 0;
  // p2MetaDataMap::accessor p;
  // p2MetaDatas.insert(p, txnDigest);
  // p.release();

  // if(!params.signClientProposals) txn = msg.release_txn();
  // *txn->mutable_txndigest() = txnDigest; //HACK to include txnDigest to lookup signed_tx.
  
  // ongoingMap::accessor o;
  // std::cerr << "ONGOING INSERT (Fallback): " << BytesToHex(txnDigest, 16).c_str() << " On CPU: " << sched_getcpu()<< std::endl;
  // //ongoing.insert(o, std::make_pair(txnDigest, txn));
  // ongoing.insert(o, txnDigest);
  // o->second.txn = txn;
  // o->second.num_concurrent_clients++;
  // o.release();
  //fallback.insert(txnDigest);
  //std::cerr << "[FB] Added tx to ongoing: " << BytesToHex(txnDigest, 16) << std::endl;

  Timestamp retryTs;

  //TODO: add parallel OCC check logic here:
  result = DoOCCCheck(msg.req_id(),
      remote, txnDigest, *txn, retryTs, committedProof, abstain_conflict, true);


  //std::cerr << "Exec P1 called, for txn: " << BytesToHex(txnDigest, 64) << std::endl;
  //BufferP1Result(result, committedProof, txnDigest, 1);
  //std::cerr << "FB: Buffered result:" << result << " for txn: " << BytesToHex(txnDigest, 64) << std::endl;

  const TransportAddress *remote_original = nullptr;
  uint64_t req_id;
  bool wake_fallbacks = false;
  bool forceMaterialize = false;
  bool sub_original = BufferP1Result(result, committedProof, txnDigest, req_id, remote_original, wake_fallbacks, forceMaterialize, false, 1);
        //std::cerr << "[Normal] release lock for txn: " << BytesToHex(txnDigest, 64) << std::endl;
  if(sub_original){ //Send to original client too if it is subscribed (This may happen if the original client was waiting on a query, and then a fallback client re-does Occ check in parallel)
        Debug("Sending Phase1 Reply for txn: %s, id: %d", BytesToHex(txnDigest, 64).c_str(), req_id);
        SendPhase1Reply(req_id, result, committedProof, txnDigest, remote_original, abstain_conflict); //TODO: Confirm that this does not consume committedProof.
  }

  //If result is Abstain, and ForceMaterialization was requested => materialize Txn.
  if(forceMaterialize) ForceMaterialization(result, txnDigest, txn);


  if(result == proto::ConcurrencyControl::IGNORE) return false; //Note: If result is Ignore, then BufferP1 will already Clean tx.

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


void Server::SetP1(uint64_t reqId, proto::Phase1Reply *p1Reply, const std::string &txnDigest, const proto::ConcurrencyControl::Result &result,
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
  ongoingMap::const_accessor o;
  bool isOngoing = ongoing.find(o, txnDigest);
  if(isOngoing){
    txn = o->second.txn;
    o.release();
  }
  else{
    o.release();
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
    ongoingMap::const_accessor o;
    bool isOngoing = ongoing.find(o, txnDigest);
    if(isOngoing){
        txn = o->second.txn;
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
    o.release();

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
  ongoingMap::const_accessor o;
  bool isOngoing = ongoing.find(o, txnDigest);
  if(isOngoing){
    txn = o->second.txn;
    o.release();
  }
  else{
    o.release();
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
  ElectQuorumMap::accessor e;
  ElectQuorums.insert(e, txnDigest);
  ElectFBorganizer &electFBorganizer = e->second;
  replica_sig_sets_pair &view_decision_quorum = electFBorganizer.view_quorums[elect_view][decision];
  if(!view_decision_quorum.first.insert(process_id).second){
    return false;
  }
  e.release();
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
  ElectQuorumMap::accessor e;
  if(!ElectQuorums.find(e, txnDigest)) return;
  ElectFBorganizer &electFBorganizer = e->second;

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
    e.release();

    //Send decision to all replicas (besides itself) and handle Decision FB directly onself.
    //transport->SendMessageToReplica(this, groupIdx, idx, decisionFB);
    transport->SendMessageToGroup(this, groupIdx, decisionFB);
    Debug("Sent DecisionFB message [decision: %s][elect_view: %lu] for txn: %s", decision ? "ABORT" : "COMMIT", elect_view, BytesToHex(txnDigest, 64).c_str());

    //std::cerr<<"This replica is the leader: " << id << " Adopting directly, without sending" << std::endl;
    AdoptDecision(txnDigest, elect_view, decision);
  }
  else{
    e.release();
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
    ongoingMap::const_accessor o;
    bool isOngoing = ongoing.find(o, txnDigest);
    if(isOngoing){
        txn = o->second.txn;
    }
    else{
      if(params.multiThreading || (params.mainThreadDispatching && !params.dispatchMessageReceive)) FreeDecisionFBmessage(&msg);
      return; //REPLICA HAS NEVER SEEN THIS TXN OR TXN NO LONGER ONGOING
    }
    o.release();

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

  moveView.Clear(); //Is safe to use the global object?
  proto::MoveViewMessage move_msg;
  move_msg.set_req_id(0);
  move_msg.set_txn_digest(txnDigest);
  move_msg.set_view(proposed_view);

  if(params.signedMessages){
    CreateHMACedMessage(move_msg, moveView.mutable_signed_move_msg());
    transport->SendMessageToGroup(this, groupIdx, moveView);
  }
  else{
    *moveView.mutable_move_msg() = std::move(move_msg);
    transport->SendMessageToGroup(this, groupIdx, moveView);
  }
}

void Server::HandleMoveView(proto::MoveView &msg){
  //Can send ElectFB message for view v+1 *before* having adopted v+1.

  std::string txnDigest;
  uint64_t proposed_view;

  if(params.signedMessages){
    if(!ValidateHMACedMessage(msg.signed_move_msg())){
      if(params.mainThreadDispatching && !params.dispatchMessageReceive) FreeMoveView(&msg);
      return;
    }
    proto::MoveViewMessage move_msg;
    move_msg.ParseFromString(msg.signed_move_msg().data());
    txnDigest = move_msg.txn_digest();
    proposed_view = move_msg.view();
  }
  else{
    const proto::MoveViewMessage &move_msg = msg.move_msg();
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
  ElectQuorumMap::accessor e;
  ElectQuorums.insert(e, txnDigest);
  ElectFBorganizer &electFBorganizer = e->second;
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
  e.release();
}
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


} // namespace pequinstore
