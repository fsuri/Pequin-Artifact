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
#include "store/pequinstore/common.h"

#include <sstream>
#include <list>

#include <cryptopp/sha.h>
#include <cryptopp/blake2.h>

#include "store/common/timestamp.h"
#include "store/common/transaction.h"
//#include "../../query-engine/type/value.h"
#include <utility>


#include "lib/batched_sigs.h"

namespace pequinstore {

void* BoolPointerWrapper(std::function<bool()> func){
    if(func()){
      return (void*) true;
    }
    else{
      return (void*) false;
    }
  }
//
std::string* GetUnusedMessageString(){
    std::unique_lock<std::mutex> lock(msgStr_mutex);
    std::string* msg;
    if(MessageStrings.size() > 0){
      msg = MessageStrings.back();
      MessageStrings.pop_back();
    }
    else{
      msg = new string();
    }
    return msg;
}
void FreeMessageString(std::string *msg){
  std::unique_lock<std::mutex> lock(msgStr_mutex);
  msg->clear();
  MessageStrings.push_back(msg);
}

void SignMessage(const ::google::protobuf::Message* msg,
    crypto::PrivKey* privateKey, uint64_t processId,
    proto::SignedMessage *signedMessage) {
  signedMessage->set_process_id(processId);
  UW_ASSERT(msg->SerializeToString(signedMessage->mutable_data()));
  Debug("Signing data %s with priv key %s.",
      BytesToHex(signedMessage->data(), 128).c_str(),
      BytesToHex(std::string(reinterpret_cast<const char*>(privateKey), 64), 128).c_str());
  *signedMessage->mutable_signature() = crypto::Sign(privateKey,
      signedMessage->data());
}

void* asyncSignMessage(const ::google::protobuf::Message* msg,
    crypto::PrivKey* privateKey, uint64_t processId,
    proto::SignedMessage *signedMessage) {

  signedMessage->set_process_id(processId);
  UW_ASSERT(msg->SerializeToString(signedMessage->mutable_data()));
  Debug("Signing data %s with priv key %s.",
      BytesToHex(signedMessage->data(), 128).c_str(),
      BytesToHex(std::string(reinterpret_cast<const char*>(privateKey), 64), 128).c_str());
  *signedMessage->mutable_signature() = crypto::Sign(privateKey, signedMessage->data());

    return (void*) signedMessage;
}

void SignMessages(const std::vector<::google::protobuf::Message*>& msgs,
    crypto::PrivKey* privateKey, uint64_t processId,
    const std::vector<proto::SignedMessage*>& signedMessages,
    uint64_t merkleBranchFactor) {
  UW_ASSERT(msgs.size() == signedMessages.size());

  std::vector<const std::string*> messageStrs;
  for (unsigned int i = 0; i < msgs.size(); i++) {
    if(signedMessages[i]){
      Debug("signedMessages[%d] exists", i);
    }
    else{
      Debug("signedMessages[%d] was already freed", i);
    }
    signedMessages[i]->set_process_id(processId);
    UW_ASSERT(msgs[i]->SerializeToString(signedMessages[i]->mutable_data()));
    messageStrs.push_back(&signedMessages[i]->data());
  }

  std::vector<std::string> sigs;
  BatchedSigs::generateBatchedSignatures(messageStrs, privateKey, sigs, merkleBranchFactor);
  for (unsigned int i = 0; i < msgs.size(); i++) {
    *signedMessages[i]->mutable_signature() = sigs[i];
  }
}

void SignMessages(const std::vector<Triplet>& batch,
    crypto::PrivKey* privateKey, uint64_t processId,
    uint64_t merkleBranchFactor) {

  std::vector<const std::string*> messageStrs;
  for (auto &triplet : batch) {
    triplet.sig_msg->set_process_id(processId);
    triplet.msg->SerializeToString(triplet.sig_msg->mutable_data());
    messageStrs.push_back(&triplet.sig_msg->data());
  }

  std::vector<std::string> sigs;
  BatchedSigs::generateBatchedSignatures(messageStrs, privateKey, sigs, merkleBranchFactor);
  for (unsigned int i = 0; i < batch.size(); i++) {
    *batch[i].sig_msg->mutable_signature() = sigs[i];
  }
}

void* asyncSignMessages(const std::vector<::google::protobuf::Message*> msgs,
    crypto::PrivKey* privateKey, uint64_t processId,
    const std::vector<proto::SignedMessage*> signedMessages,
    uint64_t merkleBranchFactor) {

  UW_ASSERT(msgs.size() == signedMessages.size());

  std::vector<const std::string*> messageStrs;
  for (unsigned int i = 0; i < msgs.size(); i++) {
    signedMessages[i]->set_process_id(processId);
    UW_ASSERT(msgs[i]->SerializeToString(signedMessages[i]->mutable_data()));
    messageStrs.push_back(&signedMessages[i]->data());
  }
  std::vector<std::string> sigs;
  BatchedSigs::generateBatchedSignatures(messageStrs, privateKey, sigs, merkleBranchFactor);
  for (unsigned int i = 0; i < msgs.size(); i++) {
    *signedMessages[i]->mutable_signature() = sigs[i];
  }

  return (void*) &signedMessages;
}

void asyncValidateCommittedConflict(const proto::CommittedProof &proof,
    const std::string *committedTxnDigest, const proto::Transaction *txn,
    const std::string *txnDigest, bool signedMessages, KeyManager *keyManager,
    const transport::Configuration *config, Verifier *verifier,
    mainThreadCallback mcb, Transport *transport, bool multithread, bool batchVerification){

    if (!TransactionsConflict(proof.txn(), *txn)) {
      Debug("Committed txn [%lu:%lu][%s] does not conflict with this txn [%lu:%lu][%s].",
          proof.txn().client_id(), proof.txn().client_seq_num(),
          BytesToHex(*committedTxnDigest, 16).c_str(),
          txn->client_id(), txn->client_seq_num(),
          BytesToHex(*txnDigest, 16).c_str());
        mcb((void*) false);
        return;
    }
    if(signedMessages && multithread){
      asyncValidateCommittedProof(proof, committedTxnDigest,
            keyManager, config, verifier, std::move(mcb), transport, multithread);
    }
    return;
}

//use params.batchVerification .. Add additional argument to asyncFunctions batch.
void asyncValidateCommittedProof(const proto::CommittedProof &proof,
    const std::string *committedTxnDigest, KeyManager *keyManager,
    const transport::Configuration *config, Verifier *verifier,
    mainThreadCallback mcb, Transport *transport, bool multithread, bool batchVerification) {
  if (proof.txn().client_id() == 0UL && proof.txn().client_seq_num() == 0UL) {
    // TODO: this is unsafe, but a hack so that we can bootstrap a benchmark
    //    without needing to write all existing data with transactions

    //bool* ret = new bool(true);
    //mcb((void*) ret);
    mcb((void*) true);
    return;
  }

  if (proof.has_p1_sigs()) {
    if(batchVerification){
      asyncBatchValidateP1Replies(proto::COMMIT, true, &proof.txn(), committedTxnDigest,
          proof.p1_sigs(), keyManager, config, -1, proto::ConcurrencyControl::ABORT,
          verifier, std::move(mcb), transport, multithread);
      return;
    }
    else{
      asyncValidateP1Replies(proto::COMMIT, true, &proof.txn(), committedTxnDigest,
          proof.p1_sigs(), keyManager, config, -1, proto::ConcurrencyControl::ABORT,
          verifier, std::move(mcb), transport, multithread);
      return;
    }
  } else if (proof.has_p2_sigs()) {
    if(batchVerification){
      asyncBatchValidateP2Replies(proto::COMMIT, proof.p2_view(), &proof.txn(), committedTxnDigest,
          proof.p2_sigs(), keyManager, config, -1, proto::ABORT, verifier, std::move(mcb), transport, multithread);
      return;
    }
    else{
      asyncValidateP2Replies(proto::COMMIT, proof.p2_view(), &proof.txn(), committedTxnDigest,
          proof.p2_sigs(), keyManager, config, -1, proto::ABORT, verifier, std::move(mcb), transport, multithread);
      return;
    }
  } else {
    Debug("Proof has neither P1 nor P2 sigs.");
    mcb((void*) false);
    return;
  }
}

bool ValidateCommittedConflict(const proto::CommittedProof &proof,
    const std::string *committedTxnDigest, const proto::Transaction *txn,
    const std::string *txnDigest, bool signedMessages, KeyManager *keyManager,
    const transport::Configuration *config, Verifier *verifier) {


  if (!TransactionsConflict(proof.txn(), *txn)) {
    Panic("invalid conflict");
    Debug("Committed txn [%lu:%lu][%s] does not conflict with this txn [%lu:%lu][%s].",
        proof.txn().client_id(), proof.txn().client_seq_num(),
        BytesToHex(*committedTxnDigest, 16).c_str(),
        txn->client_id(), txn->client_seq_num(),
        BytesToHex(*txnDigest, 16).c_str());
    return false;
  }

  if (signedMessages && !ValidateCommittedProof(proof, committedTxnDigest,
        keyManager, config, verifier)) {
    return false;
  }


  return true;
}

bool ValidateCommittedProof(const proto::CommittedProof &proof,
    const std::string *committedTxnDigest, KeyManager *keyManager,
    const transport::Configuration *config, Verifier *verifier) {
  if (proof.txn().client_id() == 0UL && proof.txn().client_seq_num() == 0UL) {
    // TODO: this is unsafe, but a hack so that we can bootstrap a benchmark
    //    without needing to write all existing data with transactions
    return true;
  }

  if (proof.has_p1_sigs()) {
    return ValidateP1Replies(proto::COMMIT, true, &proof.txn(), committedTxnDigest,
        proof.p1_sigs(), keyManager, config, -1, proto::ConcurrencyControl::ABORT,
        verifier);
  } else if (proof.has_p2_sigs()) {
    return ValidateP2Replies(proto::COMMIT, proof.p2_view(), &proof.txn(), committedTxnDigest,
        proof.p2_sigs(), keyManager, config, -1, proto::ABORT, verifier);
  } else {
    Debug("Proof has neither P1 nor P2 sigs.");
    return false;
  }
}

void* ValidateP1RepliesWrapper(proto::CommitDecision decision,
    bool fast,
    const proto::Transaction *txn,
    const std::string *txnDigest,
    const proto::GroupedSignatures &groupedSigs,
    KeyManager *keyManager,
    const transport::Configuration *config,
    int64_t myProcessId, proto::ConcurrencyControl::Result myResult, Verifier *verifier){

  Latency_t dummyLat;
  bool* result = (bool*) malloc(sizeof(bool));
  *result =  ValidateP1Replies(decision, fast, txn, txnDigest, groupedSigs,
      keyManager, config, myProcessId, myResult, dummyLat, verifier);
  return (void*) result;

}


// TODO: Make the verifier functions threadsafe? I.e. add to batch etc.
bool ValidateP1Replies(proto::CommitDecision decision,
    bool fast,
    const proto::Transaction *txn,
    const std::string *txnDigest,
    const proto::GroupedSignatures &groupedSigs,
    KeyManager *keyManager,
    const transport::Configuration *config,
    int64_t myProcessId, proto::ConcurrencyControl::Result myResult, Verifier *verifier) {
  Latency_t dummyLat;
  //_Latency_Init(&dummyLat, "dummy_lat");
  return ValidateP1Replies(decision, fast, txn, txnDigest, groupedSigs,
      keyManager, config, myProcessId, myResult, dummyLat, verifier);
}



//TODO: Need to handle duplicate P1/P2/Writeback requests from same client? Othererwise work could be duplicated in parallel.
// AND replica might change its decision!!!!!


void asyncValidateP1RepliesCallback(asyncVerification* verifyObj, uint32_t groupId, void* result){

  Debug("(CPU:%d - mainthread) asyncValidateP1RepliesCallback with result: %s", sched_getcpu(), result ? "true" : "false");

  auto lockScope = LocalDispatch ? std::unique_lock<std::mutex>(verifyObj->objMutex) : std::unique_lock<std::mutex>();
  // std::unique_lock<std::mutex> lock;
  // if(LocalDispatch) lock = std::unique_lock<std::mutex>(verifyObj->objMutex);

  //Debug("Obj QuorumSize: %d", verifyObj->quorumSize);
  //Debug("Obj groupTotals: %d", verifyObj->groupTotals);
  //Need to delete only after "last count" has finished.
  verifyObj->deletable--;
  //altneratively: keep shared datastructure (set) for verifyObject: If not in structure anymore = deleted. (remove terminate bool)

  if(verifyObj->terminate){
      if(verifyObj->deletable == 0){
        if(verifyObj->callback) verifyObj->mcb((void*) false);
        Debug("Return to CB UNSUCCESSFULLY");
        //verifyObj->deleteMessages();
        if(LocalDispatch) lockScope.unlock();
        delete verifyObj;
      }
      return;
  }
  if(!result){
      verifyObj->terminate = true;
        // delete verifyObj;
        // verifyObj = NULL;
      if(verifyObj->deletable == 0){
         Debug("Return to CB UNSUCCESSFULLY");
         verifyObj->mcb((void*) false);
         //verifyObj->deleteMessages();
         if(LocalDispatch) lockScope.unlock();
         delete verifyObj;
      }
      return;
    }
  verifyObj->groupCounts[groupId]++;
  Debug("Group %d verified %d out of necessary %d", groupId, verifyObj->groupCounts[groupId], verifyObj->quorumSize);
  if (verifyObj->groupCounts[groupId] == verifyObj->quorumSize) {
          //verifyObj->groupsVerified.insert(sigs.first);
    Debug("Completed verification of group: %d", groupId);
      verifyObj->groupsVerified++;
  }
  else{
    if(verifyObj->deletable == 0){
      verifyObj->mcb((void*) false);
      //verifyObj->deleteMessages();
       Debug("Return to CB UNSUCCESSFULLY");
       if(LocalDispatch) lockScope.unlock();
      delete verifyObj;
    }
      return;
  }

  Debug("Obj GroupsVerified: %d", verifyObj->groupsVerified);

  if (verifyObj->decision == proto::COMMIT) {
    if(!(verifyObj->groupsVerified == verifyObj->groupTotals)){
          Debug("Phase1Replies for involved_group %d not complete.", (int)groupId);
          if(verifyObj->deletable == 0){
            verifyObj->mcb((void*) false);
            Debug("Return to CB UNSUCCESSFULLY");
            //verifyObj->deleteMessages();
            if(LocalDispatch) lockScope.unlock();
            delete verifyObj;
          }
            return;
    }
  }
  //bool* ret = new bool(true);
    verifyObj->terminate = true;
    verifyObj->callback = false;
  Debug("Calling HandlePhase2CB or HandleWritebackCB");
  //verifyObj->mcb((void*) ret);
  if(!LocalDispatch){
    verifyObj->mcb((void*) true);
  }
  else{
    Debug("Issuing MCB to be scheduled as mainthread event ");
    verifyObj->tp->IssueCB(std::move(verifyObj->mcb), (void*) true);
    //verifyObj->tp->IssueCB_main(std::move(verifyObj->mcb), (void*) true);
  }

  if(verifyObj->deletable == 0){
    //verifyObj->deleteMessages();
    if(LocalDispatch) lockScope.unlock();
    delete verifyObj;
  }
  return;
}


//Currently structured to dispatch only AFTER size has been determined AND it is guaranteed that all
//jobs are "valid" (for example no duplicate replicas)

//ALTERNATIVE (not-implemented) structure that can avoid this: keep track of global map of verification objects and checks
//if map.find(object) == map.end(). Then we can delete asap, and not just at the end.
//Although still no way of knowing when to delete in case no trigger option is pulled...
void asyncBatchValidateP1Replies(proto::CommitDecision decision, bool fast, const proto::Transaction *txn,
    const std::string *txnDigest, const proto::GroupedSignatures &groupedSigs, KeyManager *keyManager,
    const transport::Configuration *config, int64_t myProcessId, proto::ConcurrencyControl::Result myResult,
    Verifier *verifier, mainThreadCallback mcb, Transport *transport, bool multithread) {

  proto::ConcurrencyControl concurrencyControl;
  concurrencyControl.Clear();
  *concurrencyControl.mutable_txn_digest() = *txnDigest;
  uint32_t quorumSize = 0;

  if (fast && decision == proto::COMMIT) {
    concurrencyControl.set_ccr(proto::ConcurrencyControl::COMMIT);
    quorumSize = config->n;
  } else if (decision == proto::COMMIT) {
    concurrencyControl.set_ccr(proto::ConcurrencyControl::COMMIT);
    quorumSize = SlowCommitQuorumSize(config);
  } else if (fast && decision == proto::ABORT) {
    concurrencyControl.set_ccr(proto::ConcurrencyControl::ABSTAIN);
    quorumSize = FastAbortQuorumSize(config);
  } else if (decision == proto::ABORT) {
    concurrencyControl.set_ccr(proto::ConcurrencyControl::ABSTAIN);
    quorumSize = SlowAbortQuorumSize(config);
  } else {
    // NOT_REACHABLE();
    mcb((void*) false);
    return;
  }
  asyncVerification *verifyObj = new asyncVerification(quorumSize, std::move(mcb), txn->involved_groups_size(), decision, transport);
  std::vector<std::function<void()>> asyncBatchingVerificationJobs;

  for (const auto &sigs : groupedSigs.grouped_sigs()) {
    concurrencyControl.set_involved_group(sigs.first);
    // std::string ccMsg;
    // concurrencyControl.SerializeToString(&ccMsg);
    std::string* ccMsg = GetUnusedMessageString();//new string();
    concurrencyControl.SerializeToString(ccMsg);
    verifyObj->ccMsgs.push_back(ccMsg); //TODO: delete at callback

    std::unordered_set<uint64_t> replicasVerified;

    for (const auto &sig : sigs.second.sigs()) {
      if (!IsReplicaInGroup(sig.process_id(), sigs.first, config)) {
        Debug("Signature for group %lu from replica %lu who is not in group.", sigs.first, sig.process_id());
        // {
        // //std::lock_guard<std::mutex> lock(verifyObj->objMutex);
        // verifyObj->terminate = true;  .
        // }
        verifyObj->mcb((void*) false);
        delete verifyObj;
        return;
      }
      auto insertItr = replicasVerified.insert(sig.process_id());
      if (!insertItr.second) {
        Debug("Already verified sig from replica %lu in group %lu.",
            sig.process_id(), sigs.first);
        // {
        // //std::lock_guard<std::mutex> lock(verifyObj->objMutex);
        // verifyObj->terminate = true;
        // }
        verifyObj->mcb((void*) false);
        delete verifyObj;
        return;
      }
      // {
      // //std::lock_guard<std::mutex> lock(verifyObj->objMutex);
      // if(verifyObj->terminate == true) return; //return preemtively if concurrent thread has already called back?
      // }
      Debug("Verifying %lu byte signature from replica %lu in group %lu.",
          sig.signature().size(), sig.process_id(), sigs.first);


      std::function<void(void*)> vb(std::bind(asyncValidateP1RepliesCallback, verifyObj, sigs.first, std::placeholders::_1));

      std::function<void()> func(std::bind(&Verifier::asyncBatchVerify, verifier, keyManager->GetPublicKey(sig.process_id()),
                std::ref(*ccMsg), std::ref(sig.signature()), std::move(vb), multithread, false)); //autocomplete set to false by default.
      asyncBatchingVerificationJobs.push_back(std::move(func));
      }
    }


  verifyObj->deletable = asyncBatchingVerificationJobs.size();

  for (auto &asyncBatchVerify : asyncBatchingVerificationJobs){
    asyncBatchVerify();
    Debug("adding job to verification batch");
  }
  Debug("Calling complete");
  //check fill and stop.

  verifier->Complete(multithread, false); //force set to false by default.
}

//OR: could create a libevent base. Create events for each waiting verification
void asyncValidateP1Replies(proto::CommitDecision decision,
    bool fast,
    const proto::Transaction *txn,
    const std::string *txnDigest,
    const proto::GroupedSignatures &groupedSigs,
    KeyManager *keyManager,
    const transport::Configuration *config,
    int64_t myProcessId, proto::ConcurrencyControl::Result myResult, Verifier *verifier,
    mainThreadCallback mcb, Transport *transport, bool multithread) {
  proto::ConcurrencyControl concurrencyControl;
  concurrencyControl.Clear();
  *concurrencyControl.mutable_txn_digest() = *txnDigest;
  uint32_t quorumSize = 0;

  if (fast && decision == proto::COMMIT) {
    concurrencyControl.set_ccr(proto::ConcurrencyControl::COMMIT);
    quorumSize = config->n;
  } else if (decision == proto::COMMIT) {
    concurrencyControl.set_ccr(proto::ConcurrencyControl::COMMIT);
    quorumSize = SlowCommitQuorumSize(config);
  } else if (fast && decision == proto::ABORT) {
    concurrencyControl.set_ccr(proto::ConcurrencyControl::ABSTAIN);
    quorumSize = FastAbortQuorumSize(config);
  } else if (decision == proto::ABORT) {
    concurrencyControl.set_ccr(proto::ConcurrencyControl::ABSTAIN);
    quorumSize = SlowAbortQuorumSize(config);
  } else {
    // NOT_REACHABLE();
    Panic("decision neither Commit nor Abort, should not be reachable");
    mcb((void*) false);
    return; //false; //dont need to return anything
  }

  //TODO: need to check if all involved groups are included... (for commit)
  // For abort check whether the one group is part of the involved groups.

  asyncVerification *verifyObj = new asyncVerification(quorumSize, std::move(mcb), txn->involved_groups_size(), decision, transport);
  std::unique_lock<std::mutex> lock(verifyObj->objMutex);

  std::vector<std::pair<std::function<void*()>,std::function<void(void*)>>> verificationJobs;
  std::vector<std::function<void*()>> verificationJobs2;
  std::vector<std::function<void*()> *> verificationJobs3;

  int no_of_groups = 0;

  for (const auto &sigs : groupedSigs.grouped_sigs()) {
    //only need to verify a single group for Abort decisions.
    if(decision == proto::ABORT && no_of_groups > 0) {
      //Panic("stopping at ABort group break");
      break;
    }
    no_of_groups++;

    concurrencyControl.set_involved_group(sigs.first);
    std::string* ccMsg = GetUnusedMessageString();//new string();
    concurrencyControl.SerializeToString(ccMsg);
    verifyObj->ccMsgs.push_back(ccMsg); //TODO: delete at callback
    //Redundant copy version:
    // std::string ccMsg;
    // concurrencyControl.SerializeToString(&ccMsg);

    //std::set<uint64_t> replicasVerified;
    std::unordered_set<uint64_t> replicasVerified;

    for (const auto &sig : sigs.second.sigs()) {

      if (!IsReplicaInGroup(sig.process_id(), sigs.first, config)) {
        Debug("Signature for group %lu from replica %lu who is not in group.", sigs.first, sig.process_id());
        Panic("Received sig from replica[%lu] not in group", sig.process_id());
        //{
        //std::lock_guard<std::mutex> lock(verifyObj->objMutex);
        //verifyObj->terminate = true;
        //}
        verifyObj->mcb((void*) false);
        delete verifyObj;
        return;
        //OR call the callback with negative result, but kind of unecessary mcb();
      }

      auto insertItr = replicasVerified.insert(sig.process_id());  //maybe use unordered_set
      if (!insertItr.second) {
        Debug("Already verified sig from replica %lu in group %lu.",
            sig.process_id(), sigs.first);
        Panic("Received duplicate signature from server %u", sig.process_id());
        //{
        //std::lock_guard<std::mutex> lock(verifyObj->objMutex);
        //verifyObj->terminate = true;
        //}
        verifyObj->mcb((void*) false);
        delete verifyObj;
        return;

      }

      //IS THIS SAFE?
      bool skip = false;
      if (sig.process_id() == myProcessId && myProcessId >= 0) {

        if (concurrencyControl.ccr() == myResult) {
          skip = true;
          if(myResult == proto::ConcurrencyControl::WAIT) Panic("Aborting due to Wait Sent");

          verifyObj->groupCounts[sigs.first]++;
          if (verifyObj->groupCounts[sigs.first] == verifyObj->quorumSize) {
                  //verifyObj->groupsVerified.insert(sigs.first);
            Debug("Completed verification of group: %d", sigs.first);
              verifyObj->groupsVerified++;
              if (verifyObj->decision == proto::COMMIT) {
                if(verifyObj->groupsVerified == verifyObj->groupTotals){
                  if(!LocalDispatch){
                    verifyObj->mcb((void*) true);
                  }
                  else{
                    Debug("Issuing MCB to be scheduled as mainthread event ");
                    verifyObj->tp->IssueCB(std::move(verifyObj->mcb), (void*) true);
                    //verifyObj->tp->IssueCB_main(std::move(verifyObj->mcb), (void*) true);
                  }
                  delete verifyObj;
                  return;
                }
              }
              else{ //Abort only needs 1 group.
                if(!LocalDispatch){
                  verifyObj->mcb((void*) true);
                }
                else{
                  Debug("Issuing MCB to be scheduled as mainthread event ");
                  verifyObj->tp->IssueCB(std::move(verifyObj->mcb), (void*) true);
                  //verifyObj->tp->IssueCB_main(std::move(verifyObj->mcb), (void*) true);
                }
                delete verifyObj;
                return;
              }
          }
        } else {
          Debug("Signature with result %u, purportedly from replica %lu"
              " (= my id %ld) doesn't match my response %u.",
              concurrencyControl.ccr(), sig.process_id(), myProcessId, myResult);
          std::cerr << "stored CCR[" <<  myResult << "] does not match signed CCR[ " << concurrencyControl.ccr() << "] for txn " << BytesToHex(*txnDigest, 64) << std::endl;
          Panic("Aborting due to mismatch");
          verifyObj->mcb((void*) false);
          delete verifyObj;
          return;
        }
      }
      if(skip) continue;



      Debug("Verifying %lu byte signature from replica %lu in group %lu.",
          sig.signature().size(), sig.process_id(), sigs.first);


      //create copy of ccMsg, and signature on heap, put them in the verifyObj, and then delete then when deleting the object.
      // std::function<bool()> func(std::bind(&Verifier::Verify, verifier, keyManager->GetPublicKey(sig.process_id()),
      //           std::ref(*ccMsg), std::ref(sig.signature())));

      crypto::PubKey* pubKey = keyManager->GetPublicKey(sig.process_id());
      const std::string* mut_sig = &sig.signature();
      uint64_t grpId = sigs.first;
      std::function<void*()>* f = new std::function([verifier, pubKey, ccMsg, mut_sig, verifyObj, grpId](){
        void* res = (void*) verifier->Verify2(pubKey, ccMsg, mut_sig);
        asyncValidateP1RepliesCallback(verifyObj, grpId, res);
        return (void*) res;
      });

      //std::function<void*()> f(std::bind(BoolPointerWrapper, std::move(func)));
      //turn into void* function in order to dispatch
      //std::function<void*()> f(std::bind(pointerWrapper<bool>, std::move(func)));

      //std::function<void(void*)> cb(std::bind(asyncValidateP1RepliesCallback, verifyObj, sigs.first, std::placeholders::_1));

      //verificationJobs.emplace_back(std::make_pair(std::move(f), std::move(cb)));
      //verificationJobs2.emplace_back(std::move(f));
      verificationJobs3.push_back(f);
      //transport->DispatchTP_noCB_ptr(f);
    }
  }

  //verifyObj->deletable = verificationJobs.size();
  //verifyObj->deletable = verificationJobs2.size();
  verifyObj->deletable = verificationJobs3.size();

  //does ref & make a difference here?
  for (std::function<void*()>* f : verificationJobs3){
    transport->DispatchTP_noCB_ptr(f);
  }

  // for (auto &verification : verificationJobs2){
  //   transport->DispatchTP_noCB(std::move(verification));
  // }
  //
  // for (auto &verification : verificationJobs){
  //
  //   //a)) Multithreading: Dispatched f: verify , cb: async Callback
  //   if(multithread && LocalDispatch){
  //     // Debug("P1 Validation is dispatched and parallel");
  //     auto comb = [f = std::move(verification.first), cb = std::move(verification.second)](){
  //       cb(f());
  //       return (void*) true;
  //     };
  //     transport->DispatchTP_noCB(std::move(comb));
  //     //transport->DispatchTP_local(std::move(verification.first), std::move(verification.second));
  //   }
  //   else if(multithread){
  //     transport->DispatchTP(std::move(verification.first), std::move(verification.second));
  //   }
  //   //b) No multithreading: Calls verify + async Callback. Problem: Unecessary copying for bind.
  //   else{
  //     Debug("P1 Validation is local and serial");
  //     verification.second(verification.first());
  //   }
  // }
}



bool ValidateP1Replies(proto::CommitDecision decision,
    bool fast,
    const proto::Transaction *txn,
    const std::string *txnDigest,
    const proto::GroupedSignatures &groupedSigs,
    KeyManager *keyManager,
    const transport::Configuration *config,
    int64_t myProcessId, proto::ConcurrencyControl::Result myResult,
    Latency_t &lat, Verifier *verifier) {
  proto::ConcurrencyControl concurrencyControl;
  concurrencyControl.Clear();
  *concurrencyControl.mutable_txn_digest() = *txnDigest;
  uint32_t quorumSize = 0;
  if (fast && decision == proto::COMMIT) {
    concurrencyControl.set_ccr(proto::ConcurrencyControl::COMMIT);
    quorumSize = config->n;
  } else if (decision == proto::COMMIT) {
    concurrencyControl.set_ccr(proto::ConcurrencyControl::COMMIT);
    quorumSize = SlowCommitQuorumSize(config);
  } else if (fast && decision == proto::ABORT) {
    concurrencyControl.set_ccr(proto::ConcurrencyControl::ABSTAIN);
    quorumSize = FastAbortQuorumSize(config);
  } else if (decision == proto::ABORT) {
    concurrencyControl.set_ccr(proto::ConcurrencyControl::ABSTAIN);
    quorumSize = SlowAbortQuorumSize(config);
  } else {
    // NOT_REACHABLE();
    return false;
  }


  std::set<int> groupsVerified;
  for (const auto &sigs : groupedSigs.grouped_sigs()) {
    concurrencyControl.set_involved_group(sigs.first);
    std::string ccMsg;
    concurrencyControl.SerializeToString(&ccMsg);
    std::unordered_set<uint64_t> replicasVerified;
    uint32_t verified = 0;
    for (const auto &sig : sigs.second.sigs()) {

      if (!IsReplicaInGroup(sig.process_id(), sigs.first, config)) {
        Debug("Signature for group %lu from replica %lu who is not in group.",
            sigs.first, sig.process_id());
        return false;
      }

      bool skip = false;

      if (sig.process_id() == myProcessId && myProcessId >= 0) {
        if (concurrencyControl.ccr() == myResult) {
          skip = true;
        } else {
          Debug("Signature purportedly from replica %lu"
              " (= my id %ld) doesn't match my response %u.",
              sig.process_id(), myProcessId, concurrencyControl.ccr());
          return false;
        }
      }

      Debug("Verifying %lu byte signature from replica %lu in group %lu.",
          sig.signature().size(), sig.process_id(), sigs.first);
      //Latency_Start(&lat);
      if (!skip && !verifier->Verify(keyManager->GetPublicKey(sig.process_id()), ccMsg,
              sig.signature())) {
        //Latency_End(&lat);
        Debug("Signature from replica %lu in group %lu is not valid.",
            sig.process_id(), sigs.first);
        return false;
      }
      //Latency_End(&lat);
      //
      auto insertItr = replicasVerified.insert(sig.process_id());
      if (!insertItr.second) {
        Debug("Already verified sig from replica %lu in group %lu.",
            sig.process_id(), sigs.first);
        return false;
      }
      verified++;
    }

    if (verified != quorumSize) {
      Debug("Expected exactly %u sigs but processed %u.", quorumSize, verified);
      return false;
    }

    groupsVerified.insert(sigs.first);
  }

  if (decision == proto::COMMIT) {
    for (auto group : txn->involved_groups()) {
      if (groupsVerified.find(group) == groupsVerified.end()) {
        Debug("No Phase1Replies for involved_group %ld.", group);
        return false;
      }
    }
  }

  return true;
}

//ADD Wrapper
void* ValidateP2RepliesWrapper(proto::CommitDecision decision, uint64_t view,
    const proto::Transaction *txn,
    const std::string *txnDigest, const proto::GroupedSignatures &groupedSigs,
    KeyManager *keyManager, const transport::Configuration *config,
    int64_t myProcessId, proto::CommitDecision myDecision, Verifier *verifier) {

  Latency_t dummyLat;
  bool* result = (bool*) malloc(sizeof(bool));
  *result =  ValidateP2Replies(decision, view, txn, txnDigest, groupedSigs,
      keyManager, config, myProcessId, myDecision, dummyLat, verifier);
  return (void*) result;
}

bool ValidateP2Replies(proto::CommitDecision decision, uint64_t view,
    const proto::Transaction *txn,
    const std::string *txnDigest, const proto::GroupedSignatures &groupedSigs,
    KeyManager *keyManager, const transport::Configuration *config,
    int64_t myProcessId, proto::CommitDecision myDecision, Verifier *verifier) {
  Latency_t dummyLat;
  //_Latency_Init(&dummyLat, "dummy_lat");
  return ValidateP2Replies(decision, view, txn, txnDigest, groupedSigs,
      keyManager, config, myProcessId, myDecision, dummyLat, verifier);
}


void asyncValidateP2RepliesCallback(asyncVerification* verifyObj, uint32_t groupId, void* result){

  //bool verification_result = * ((bool*) result);
  //delete (bool*) result;

  Debug("(CPU:%d - mainthread) asyncValidateP2RepliesCallback with result: %s", sched_getcpu(), result ? "true" : "false");


  auto lockScope = LocalDispatch ? std::unique_lock<std::mutex>(verifyObj->objMutex) : std::unique_lock<std::mutex>();
  // std::unique_lock<std::mutex> lock;
  // if(LocalDispatch) lock = std::unique_lock<std::mutex>(verifyObj->objMutex);

  //Need to delete only after "last count" has finished.
  verifyObj->deletable--;

  if(verifyObj->terminate){
    if(verifyObj->deletable == 0){
      if(verifyObj->callback) verifyObj->mcb((void*) false);
      if(LocalDispatch) lockScope.unlock();
      delete verifyObj;
    }
    return;
  }
  if(!result){
      verifyObj->terminate = true;

      if(verifyObj->deletable == 0){
        verifyObj->mcb((void*) false);
        if(LocalDispatch) lockScope.unlock();
        delete verifyObj;
      }
      return;
    }
  verifyObj->groupCounts[groupId]++;
  Debug("%d out of necessary %d Phase2Replies for logging group %d verified.",
    verifyObj->groupCounts[groupId],verifyObj->quorumSize,(int)groupId);


  if (verifyObj->groupCounts[groupId] == verifyObj->quorumSize) {
     Debug("Phase2Replies for logging group %d successfully verified.", (int)groupId);
    verifyObj->terminate = true;
    verifyObj->callback = false;
    //bool* ret = new bool(true);
    //verifyObj->mcb((void*) ret);
    if(!LocalDispatch){
      verifyObj->mcb((void*) true);
    }
    else{
      verifyObj->tp->IssueCB(std::move(verifyObj->mcb), (void*) true);
      Debug("Dispatching Writeback Callback back to Main Thread.");
      //verifyObj->tp->IssueCB_main(std::move(verifyObj->mcb), (void*) true);
    }

    if(verifyObj->deletable == 0){
      if(LocalDispatch) lockScope.unlock();
      delete verifyObj;
    }
    return;

  }
  else{
      Debug("Phase2Replies for logging group %d insufficient to complete.", (int)groupId);
      if(verifyObj->deletable == 0){
        verifyObj->mcb((void*) false);
        if(LocalDispatch) lockScope.unlock();
        delete verifyObj;
      }
      return;
  }
}

void asyncBatchValidateP2Replies(proto::CommitDecision decision, uint64_t view,
    const proto::Transaction *txn,
    const std::string *txnDigest, const proto::GroupedSignatures &groupedSigs,
    KeyManager *keyManager, const transport::Configuration *config,
    int64_t myProcessId, proto::CommitDecision myDecision, Verifier *verifier,
    mainThreadCallback mcb, Transport* transport, bool multithread){

    proto::Phase2Decision p2Decision;
    p2Decision.Clear();
    p2Decision.set_decision(decision);
    p2Decision.set_view(view);
    p2Decision.set_involved_group(GetLogGroup(*txn, *txnDigest));
    *p2Decision.mutable_txn_digest() = *txnDigest;

    // std::string p2DecisionMsg;
    // p2Decision.SerializeToString(&p2DecisionMsg);
    std::string* p2DecisionMsg = GetUnusedMessageString();
    p2Decision.SerializeToString(p2DecisionMsg);

    if (groupedSigs.grouped_sigs().size() != 1) {
      Debug("Expected exactly 1 group but saw %lu", groupedSigs.grouped_sigs().size());
      mcb((void*) false);
      return;
    }

    asyncVerification *verifyObj = new asyncVerification(QuorumSize(config), std::move(mcb), 1, decision, transport);
    verifyObj->ccMsgs.push_back(p2DecisionMsg);
    std::vector<std::function<void()>> asyncBatchingVerificationJobs;

    const auto &sigs = groupedSigs.grouped_sigs().begin(); //this is an iterator
    // verifyObj->deletable = sigs->second.sigs_size();  // redundant

    std::unordered_set<uint64_t> replicasVerified;
    int64_t logGrp = GetLogGroup(*txn, *txnDigest);
    //verify that this group corresponds to the log group
    if(sigs->first != logGrp){
      Debug("P2 replies from group (%lu) that is not logging group (%lu).", sigs->first, logGrp);
      verifyObj->mcb((void*) false);
      delete verifyObj;
      return;
    }

    for (const auto &sig : sigs->second.sigs()) {

      if (!IsReplicaInGroup(sig.process_id(), sigs->first, config)) {
        Debug("Signature for group %lu from replica %lu who is not in group.", sigs->first, sig.process_id());
        verifyObj->mcb((void*) false);
        delete verifyObj;
        return;
      }
      if (!replicasVerified.insert(sig.process_id()).second) {
        Debug("Already verified signature from %lu.", sig.process_id());
        verifyObj->mcb((void*) false);
        delete verifyObj;
        return;
      }

      std::function<void(void*)> vb(std::bind(asyncValidateP2RepliesCallback, verifyObj, sigs->first, std::placeholders::_1));

      std::function<void()> func(std::bind(&Verifier::asyncBatchVerify, verifier, keyManager->GetPublicKey(sig.process_id()),
                std::ref(*p2DecisionMsg), std::ref(sig.signature()), std::move(vb), multithread, false)); //autocomplete set to false by default.
      asyncBatchingVerificationJobs.push_back(std::move(func));

    }
    verifyObj->deletable = asyncBatchingVerificationJobs.size();
    for (auto &asyncBatchVerify : asyncBatchingVerificationJobs){
      asyncBatchVerify();
    }
    verifier->Complete(multithread, false); //force set to false by default.
}

void asyncValidateP2Replies(proto::CommitDecision decision, uint64_t view,
    const proto::Transaction *txn,
    const std::string *txnDigest, const proto::GroupedSignatures &groupedSigs,
    KeyManager *keyManager, const transport::Configuration *config,
    int64_t myProcessId, proto::CommitDecision myDecision, Verifier *verifier,
    mainThreadCallback mcb, Transport* transport, bool multithread){

    proto::Phase2Decision p2Decision;
    p2Decision.Clear();
    p2Decision.set_decision(decision);
    p2Decision.set_view(view);
    p2Decision.set_involved_group(GetLogGroup(*txn, *txnDigest));
    *p2Decision.mutable_txn_digest() = *txnDigest;

    // std::string p2DecisionMsg;
    // p2Decision.SerializeToString(&p2DecisionMsg);
    std::string* p2DecisionMsg = GetUnusedMessageString();
    p2Decision.SerializeToString(p2DecisionMsg);

    if (groupedSigs.grouped_sigs().size() != 1) {
      Debug("Expected exactly 1 group for txn %s but saw %lu", BytesToHex(*txnDigest, 16).c_str(), groupedSigs.grouped_sigs().size());
      mcb((void*) false);
      return;
    }

    const auto &sigs = groupedSigs.grouped_sigs().begin(); //this is an iterator
    // verifyObj->deletable = sigs->second.sigs_size();  // redundant

    std::unordered_set<uint64_t> replicasVerified;
    int64_t logGrp = GetLogGroup(*txn, *txnDigest);
    //verify that this group corresponds to the log group
    if(sigs->first != logGrp){
      Debug("P2 replies from group (%lu) that is not logging group (%lu) for txn %s.", sigs->first, logGrp, BytesToHex(*txnDigest, 16).c_str());
      //delete verifyObj;
      mcb((void*) false);
      return;
    }
    asyncVerification *verifyObj = new asyncVerification(QuorumSize(config), std::move(mcb), 1, decision, transport);
    verifyObj->ccMsgs.push_back(p2DecisionMsg);
    std::vector<std::pair<std::function<void*()>,std::function<void(void*)>>> verificationJobs;

    Debug("%d P2 signatures included for txn %s", sigs->second.sigs().size(), BytesToHex(*txnDigest, 16).c_str());

    for (const auto &sig : sigs->second.sigs()) {

      if (!IsReplicaInGroup(sig.process_id(), sigs->first, config)) {
        Debug("Signature for group %lu from replica %lu who is not in group; txn %s.", sigs->first, sig.process_id(), BytesToHex(*txnDigest, 16).c_str());
        verifyObj->mcb((void*) false);
        delete verifyObj;
        return;
      }
      if (!replicasVerified.insert(sig.process_id()).second) {
        Debug("Duplicate signature from %lu for txn %s", sig.process_id(), BytesToHex(*txnDigest, 16).c_str() );
        verifyObj->mcb((void*) false);
        delete verifyObj;
        return;
      }
      //TODO: does this work as expected?
      bool skip = false;
      if (sig.process_id() == myProcessId && myProcessId >= 0) {
        if (p2Decision.decision() == myDecision) {
          skip = true;
          Debug("Skipping verification of local signature for txn %s", BytesToHex(*txnDigest, 16).c_str() );
          verifyObj->groupCounts[sigs->first]++;
          if (verifyObj->groupCounts[sigs->first] == verifyObj->quorumSize) {
            Debug("Completed Quorum for txn %s", BytesToHex(*txnDigest, 16).c_str() );
            verifyObj->mcb((void*) true);
            delete verifyObj;
            return;
          }
        } else {
          //XXX!!! do not return; since p2 decisions can change, need to eval sig in this case
          // delete verifyObj;
          // mcb((void*) false);
          // return;
        }
      }
      if(skip) continue;

      //sanity checks
      // Debug("P2 VERIFICATION TX:[%s] with Sig:[%s] from replica %lu with Msg:[%s].",
      //     BytesToHex(*txnDigest, 128).c_str(),
      //     BytesToHex(sig.signature(), 1024).c_str(), sig.process_id(),
      //     BytesToHex(p2DecisionMsg, 1024).c_str());
      // Debug("p2 verification expected_result %s", verifier->Verify(keyManager->GetPublicKey(sig.process_id()),
      //       p2DecisionMsg, sig.signature())? "true" : "false");

      //TODO: add to job list
      std::function<bool()> func(std::bind(&Verifier::Verify, verifier, keyManager->GetPublicKey(sig.process_id()),
      std::ref(*p2DecisionMsg), std::ref(sig.signature())));
      //std::function<void*()> f(std::bind(pointerWrapper<bool>, std::move(func))); //turn into void* function in order to dispatch
      //callback
      std::function<void*()> f(std::bind(BoolPointerWrapper, std::move(func)));

      //turn into void* function in order to dispatch
      //std::function<void*()> f(std::bind(pointerWrapper<bool>, std::move(func)));
      std::function<void(void*)> cb(std::bind(asyncValidateP2RepliesCallback, verifyObj, sigs->first, std::placeholders::_1));
      verificationJobs.emplace_back(std::make_pair(std::move(f), std::move(cb)));

    }

    verifyObj->deletable = verificationJobs.size();
    for (auto &verification : verificationJobs){

      //a)) Multithreading: Dispatched f: verify , cb: async Callback
      if(multithread && LocalDispatch){
        // Debug("P2 Validation is dispatched and parallel");
        auto comb = [f = std::move(verification.first), cb = std::move(verification.second)](){
          cb(f());
          return (void*) true;
        };
        transport->DispatchTP_noCB(std::move(comb));
        //transport->DispatchTP_local(std::move(verification.first), std::move(verification.second));
      }
      else if(multithread){

          transport->DispatchTP(std::move(verification.first), std::move(verification.second));

      }
      //b) No multithreading: Calls verify + async Callback.
      else{
        Debug("P2 Validation is local and serial");
        verification.second(verification.first());
      }
    }
    //TODO: ADD SKIP LOGIC BACK TO P1!!!!!!

}



bool ValidateP2Replies(proto::CommitDecision decision, uint64_t view,
    const proto::Transaction *txn,
    const std::string *txnDigest, const proto::GroupedSignatures &groupedSigs,
    KeyManager *keyManager, const transport::Configuration *config,
    int64_t myProcessId, proto::CommitDecision myDecision,
    Latency_t &lat, Verifier *verifier) {
  proto::Phase2Decision p2Decision;
  p2Decision.Clear();
  p2Decision.set_decision(decision);
  p2Decision.set_view(view);
  p2Decision.set_involved_group(GetLogGroup(*txn, *txnDigest));
  *p2Decision.mutable_txn_digest() = *txnDigest;

  std::string p2DecisionMsg;
  p2Decision.SerializeToString(&p2DecisionMsg);

  if (groupedSigs.grouped_sigs().size() != 1) {
    Debug("Expected exactly 1 group but saw %lu", groupedSigs.grouped_sigs().size());
    return false;
  }

  const auto &sigs = groupedSigs.grouped_sigs().begin();
  uint32_t verified = 0;
  std::unordered_set<uint64_t> replicasVerified;
  for (const auto &sig : sigs->second.sigs()) {
    //Latency_Start(&lat);

    bool skip = false;
    if (sig.process_id() == myProcessId && myProcessId >= 0) {
      if (p2Decision.decision() == myDecision) {
        skip = true;
      } else {
        return false;
      }
    }
    //Debug("NON MULTITHREAD p2 verification expected_result %s", verifier->Verify(keyManager->GetPublicKey(sig.process_id()),
    //      p2DecisionMsg, sig.signature())? "true" : "false");
    if (!skip && !verifier->Verify(keyManager->GetPublicKey(sig.process_id()),
          p2DecisionMsg, sig.signature())) {
      //Latency_End(&lat);
      Debug("Signature from %lu is not valid.", sig.process_id());
      return false;
    }
    //Latency_End(&lat);

    if (!replicasVerified.insert(sig.process_id()).second) {
      Debug("Already verified signature from %lu.", sig.process_id());
      return false;
    }
    verified++;
  }

  if (verified != QuorumSize(config)) {
    Debug("Expected exactly %lu sigs but processed %u.", QuorumSize(config),
        verified);
    return false;
  }

  return true;
}

void asyncValidateFBP2Replies(proto::CommitDecision decision,
    const proto::Transaction *txn,
    const std::string *txnDigest, const proto::P2Replies &p2Replies,
    KeyManager *keyManager, const transport::Configuration *config,
    int64_t myProcessId, proto::CommitDecision myDecision, Verifier *verifier,
    mainThreadCallback mcb, Transport* transport, bool multithread){

    proto::Phase2Decision p2Decision;

    if (p2Replies.p2replies().size() != config->f + 1) {
      Debug("Expected f+1=%d p2 replies but saw %lu", config->f+1, p2Replies.p2replies().size());
      mcb((void*) false);
      return;
    }

    std::unordered_set<uint64_t> replicasVerified;
    int64_t logGrp = GetLogGroup(*txn, *txnDigest);

    asyncVerification *verifyObj = new asyncVerification(config->f+1, std::move(mcb), 1, decision, transport);
    std::vector<std::pair<std::function<void*()>,std::function<void(void*)>>> verificationJobs;

    for (const auto &p2_reply : p2Replies.p2replies()) {

      if(!p2_reply.has_signed_p2_decision()) return;
      const proto::SignedMessage &sig = p2_reply.signed_p2_decision();

      //Confirm that p2reply matches in decision and digest; need NOT match in view.
      p2Decision.ParseFromString(sig.data());
      if(p2Decision.decision() != decision || p2Decision.txn_digest() != *txnDigest){
        Debug("P2Reply for P2FB message does not match decision %s or txnDigest %s.",
            1-decision ? "COMMIT" : "ABORT", BytesToHex(*txnDigest, 64).c_str());
        verifyObj->mcb((void*) false);
        delete verifyObj;
        return;
      }

      if (!IsReplicaInGroup(sig.process_id(), logGrp, config)) {
        Debug("Signature for group %lu from replica %lu who is not in group.", logGrp, sig.process_id());
        verifyObj->mcb((void*) false);
        delete verifyObj;
        return;
      }
      if (!replicasVerified.insert(sig.process_id()).second) {
        Debug("Already verified signature from %lu.", sig.process_id());
        verifyObj->mcb((void*) false);
        delete verifyObj;
        return;
      }
      //TODO: does this work as expected?
      bool skip = false;
      if (sig.process_id() == myProcessId && myProcessId >= 0) {
        if (decision == myDecision) {
          skip = true;
          verifyObj->groupCounts[logGrp]++;
          if (verifyObj->groupCounts[logGrp] == verifyObj->quorumSize) {
            verifyObj->mcb((void*) true);
            delete verifyObj;
            return;
          }
        }
      }
      if(skip) continue;

      // add to job list
      std::function<bool()> func(std::bind(&Verifier::Verify, verifier, keyManager->GetPublicKey(sig.process_id()),
      std::ref(sig.data()), std::ref(sig.signature())));
      //turn into void* function in order to dispatch
      std::function<void*()> f(std::bind(BoolPointerWrapper, std::move(func)));

      std::function<void(void*)> cb(std::bind(asyncValidateP2RepliesCallback, verifyObj, logGrp, std::placeholders::_1));
      verificationJobs.emplace_back(std::make_pair(std::move(f), std::move(cb)));

    }

    verifyObj->deletable = verificationJobs.size();
    for (auto &verification : verificationJobs){

      //a)) Multithreading: Dispatched f: verify , cb: async Callback
      if(multithread && LocalDispatch){
        // Debug("P2 Validation is dispatched and parallel");
        auto comb = [f = std::move(verification.first), cb = std::move(verification.second)](){
          cb(f());
          return (void*) true;
        };
        transport->DispatchTP_noCB(std::move(comb));
        //transport->DispatchTP_local(std::move(verification.first), std::move(verification.second));
      }
      else if(multithread){

          transport->DispatchTP(std::move(verification.first), std::move(verification.second));

      }
      //b) No multithreading: Calls verify + async Callback.
      else{
        Debug("P2 Validation is local and serial");
        verification.second(verification.first());
      }
    }
}

//TODO: use myProcessId etc, to look up myCurrentView (in async, this might be outdated, so
//we must not accept the view in the callback function if we have a higher one.)
bool VerifyFBViews(uint64_t proposed_view, bool catch_up, uint64_t logGrp,
    const std::string *txnDigest, const proto::SignedMessages &signed_messages,
    KeyManager *keyManager, const transport::Configuration *config,
    int64_t myProcessId, uint64_t myCurrentView, Verifier *verifier){

    uint64_t quorumSize;
    if(catch_up){
      quorumSize = config->f+1;
    }
    else{
      quorumSize = 3*config->f+1;
    }

    uint64_t verified = 0;
    proto::CurrentView view_s;
    std::unordered_set<uint64_t> replicasVerified;

    for(const auto &signed_view : signed_messages.sig_msgs()){

          view_s.ParseFromString(signed_view.data());

          if(!IsReplicaInGroup(signed_view.process_id(), logGrp, config)){
            Debug("Signature for group %lu from replica %lu who is not in group.", logGrp, signed_view.process_id());
            return false;
          }
          if (!replicasVerified.insert(signed_view.process_id()).second) {
            Debug("Already verified signature from %lu.", signed_view.process_id());
            return false;
          }
          //check for catchup that replica view is not smaller than proposed view
          //check for non-catchup that replica view is not smaller than proposed view - 1
          if(view_s.txn_digest() != *txnDigest || view_s.current_view() < proposed_view - (1-catch_up) ){
            return false;
          }

          bool skip = false;
          if (signed_view.process_id() == myProcessId && myProcessId >= 0) {
            if (myCurrentView >= proposed_view - (1-catch_up)) {
              skip = true;
            }
          }

          if(!skip && !verifier->Verify(keyManager->GetPublicKey(signed_view.process_id()),
                    signed_view.data(), signed_view.signature())) {
              return false;
          }
          verified++;

          if(verified == quorumSize) return true;
    }

    return false;

        // //USE THIS CODE IF ASSUMING VIEWS ARE JUST SIGNATURES - HOWEREVER, DUE TO VOTE SUBSUMPTION NEED TO DISTINGUISH IN CASE VIEWS ARE >= proposed view, not just =
        // std::string viewMsg;
        // proto::CurrentView curr_view;
        //curr_view.mutable_txn_digest(msg.txn_digest());

        //*curr_view.mutable_txn_digest() = txnDigest;

        //   proto::Signatures sigs = msg.view_sigs();
        //   if(msg.catchup()){
        //     curr_view.set_current_view(msg.proposed_view());
        //     curr_view.SerializeToString(&viewMsg);
        //     uint64_t counter = config.f +1;
        //     for(const auto &sig : sigs.sigs()){
        //       //TODO: check that this id was from the loggin shard. sig.process_id() in lG
        //       if(IsReplicaInGroup(sig.process_id(), lG, &config)){
        //           if(crypto::Verify(keyManager->GetPublicKey(sig.process_id()), viewMsg, sig.signature())) { counter--;} else{return false;}
        //       }
        //       if(counter == 0) return true;
        //     }
        //   }
        //   else{
        //     curr_view.set_current_view(msg.proposed_view()-1);
        //     curr_view.SerializeToString(&viewMsg);
        //     uint64_t counter = 3* config.f +1;
        //     for(const auto &sig : sigs.sigs()){
        //       if(IsReplicaInGroup(sig.process_id(), lG, &config)){
        //           if(crypto::Verify(keyManager->GetPublicKey(sig.process_id()), viewMsg, sig.signature() )) { counter--;} else{return false;}
        //       }
        //       if(counter == 0) return true;
        //     }
        //
        //   }
        //   return false;
}

void asyncVerifyFBViews(uint64_t proposed_view, bool catch_up, uint64_t logGrp,
    const std::string *txnDigest, const proto::SignedMessages &signed_messages,
    KeyManager *keyManager, const transport::Configuration *config,
    int64_t myProcessId, uint64_t myCurrentView, Verifier *verifier,
    mainThreadCallback mcb, Transport* transport, bool multithread){

    uint64_t quorumSize;
    if(catch_up){
      quorumSize = config->f+1;
    }
    else{
      quorumSize = 3*config->f+1;
    }

    uint64_t verified = 0;
    proto::CurrentView view_s;
    std::unordered_set<uint64_t> replicasVerified;

    //TODO: does not need a decision arg
    asyncVerification *verifyObj = new asyncVerification(quorumSize, std::move(mcb), 1, proto::COMMIT, transport);
    std::vector<std::pair<std::function<void*()>,std::function<void(void*)>>> verificationJobs;

    for(const auto &signed_view : signed_messages.sig_msgs()){

        view_s.ParseFromString(signed_view.data());

        if(!IsReplicaInGroup(signed_view.process_id(), logGrp, config)){
            Debug("Signature for group %lu from replica %lu who is not in group.", logGrp, signed_view.process_id());
            verifyObj->mcb((void*) false);
            delete verifyObj;
            return;
        }
        if (!replicasVerified.insert(signed_view.process_id()).second) {
            Debug("Already verified signature from %lu.", signed_view.process_id());
            verifyObj->mcb((void*) false);
            delete verifyObj;
            return;
        }
        //check for catchup that replica view is not smaller than proposed view
        //check for non-catchup that replica view is not smaller than proposed view - 1
        if(view_s.txn_digest() != *txnDigest){
            Debug("Txn digest of View message [%s] does not match Invoked txn digest [%s]", BytesToHex(view_s.txn_digest(), 64).c_str(), BytesToHex(*txnDigest, 64).c_str());
            verifyObj->mcb((void*) false);
            delete verifyObj;
            return;
        }
        //XXX signed view only counts as vote if it is >= than proposed view - (1-catch_up)
        if(view_s.current_view() < proposed_view - (1-catch_up) ){
            Debug("View message [%s]: signed view = %lu < %lu proposed_view. Catch_up = %s", BytesToHex(*txnDigest, 64).c_str(), view_s.current_view(), proposed_view - (1-catch_up), catch_up ? "True" : "False");
            verifyObj->mcb((void*) false);
            delete verifyObj;
            return;
        }

        bool skip = false;
        if (signed_view.process_id() == myProcessId && myProcessId >= 0) {
          //XXX Useless for catchup. If we are already in a larger >= view then we do not adopt it anyways.
          if (myCurrentView >= proposed_view - (1-catch_up)) {
              skip = true;
              verifyObj->groupCounts[logGrp]++;
              if (verifyObj->groupCounts[logGrp] == verifyObj->quorumSize) {
                  verifyObj->mcb((void*) true);
                  delete verifyObj;
                  return;
              }
          }
        }
        if(skip) continue;

        // add to job list
        std::function<bool()> func(std::bind(&Verifier::Verify, verifier, keyManager->GetPublicKey(signed_view.process_id()),
        std::ref(signed_view.data()), std::ref(signed_view.signature())));
        //turn into void* function in order to dispatch
        std::function<void*()> f(std::bind(BoolPointerWrapper, std::move(func)));

        //TODO: need a different callback? probably not, the p2 one is generic (rename it?)
        std::function<void(void*)> cb(std::bind(asyncValidateP2RepliesCallback, verifyObj, logGrp, std::placeholders::_1));
        verificationJobs.emplace_back(std::make_pair(std::move(f), std::move(cb)));

    }

    verifyObj->deletable = verificationJobs.size();
    //std::cerr << "deletable: " << verifyObj->deletable << "  vs  quorumSize: " << quorumSize << " -- current group count: " << verifyObj->groupCounts[logGrp] << std::endl;
    UW_ASSERT(verifyObj->deletable <= quorumSize);// If not, then we might be verifying more than necessary.
    for (auto &verification : verificationJobs){

        //a)) Multithreading: Dispatched f: verify , cb: async Callback
        if(multithread && LocalDispatch){
          // Debug("P2 Validation is dispatched and parallel");
          auto comb = [f = std::move(verification.first), cb = std::move(verification.second)](){
            cb(f());
            return (void*) true;
          };
          transport->DispatchTP_noCB(std::move(comb));
          //transport->DispatchTP_local(std::move(verification.first), std::move(verification.second));
        }
        else if(multithread){

            transport->DispatchTP(std::move(verification.first), std::move(verification.second));

        }
        //b) No multithreading: Calls verify + async Callback.
        else{
          Debug("View Validation is local and serial");
          verification.second(verification.first());
        }
    }
}

bool ValidateFBDecision(proto::CommitDecision decision, uint64_t view,
    const proto::Transaction *txn,
    const std::string *txnDigest, const proto::Signatures &sigs,
    KeyManager *keyManager, const transport::Configuration *config,
    int64_t myProcessId, proto::CommitDecision myDecision, Verifier *verifier) {

  proto::ElectMessage electMessage;
  electMessage.Clear();
  electMessage.set_req_id(0);
  electMessage.set_decision(decision);
  electMessage.set_elect_view(view);
  electMessage.set_txn_digest(*txnDigest);

  std::string electMsg;
  electMessage.SerializeToString(&electMsg);

  uint64_t quorumSize = 2*config->f +1;

  uint32_t verified = 0;
  std::unordered_set<uint64_t> replicasVerified;
  int64_t logGrp = GetLogGroup(*txn, *txnDigest);

  for (const auto &sig : sigs.sigs()) {
    //Latency_Start(&lat);

    if(!IsReplicaInGroup(sig.process_id(), logGrp, config)){
      Debug("Signature for group %lu from replica %lu who is not in group.", logGrp, sig.process_id());
      return false;
    }
    if (!replicasVerified.insert(sig.process_id()).second) {
      Debug("Already verified signature from %lu.", sig.process_id());
      return false;
    }

    bool skip = false;
    if (sig.process_id() == myProcessId && myProcessId >= 0) {
      if (decision == myDecision) {
        skip = true;
      }
    }
    //Debug("NON MULTITHREAD p2 verification expected_result %s", verifier->Verify(keyManager->GetPublicKey(sig.process_id()),
    //      p2DecisionMsg, sig.signature())? "true" : "false");
    if (!skip && !verifier->Verify(keyManager->GetPublicKey(sig.process_id()),
          electMsg, sig.signature())) {
      //Latency_End(&lat);
      Debug("Signature from %lu is not valid.", sig.process_id());
      return false;
    }
    //Latency_End(&lat);
    verified++;

    if(verified == quorumSize) return true;
  }


  Debug("Expected exactly %lu sigs but processed %u.", quorumSize,
        verified);
  return false;
}

void asyncValidateFBDecision(proto::CommitDecision decision, uint64_t view,
    const proto::Transaction *txn,
    const std::string *txnDigest, const proto::Signatures &sigs,
    KeyManager *keyManager, const transport::Configuration *config,
    int64_t myProcessId, proto::CommitDecision myDecision, Verifier *verifier,
    mainThreadCallback mcb, Transport* transport, bool multithread){

    proto::ElectMessage electMessage;
    electMessage.Clear();
    electMessage.set_req_id(0);
    electMessage.set_decision(decision);
    electMessage.set_elect_view(view);
    electMessage.set_txn_digest(*txnDigest);

    std::string* electMsg = GetUnusedMessageString();
    electMessage.SerializeToString(electMsg);

    uint64_t quorumSize = 2*config->f +1;

    std::unordered_set<uint64_t> replicasVerified;
    int64_t logGrp = GetLogGroup(*txn, *txnDigest);


    asyncVerification *verifyObj = new asyncVerification(quorumSize, std::move(mcb), 1, decision, transport);
    verifyObj->ccMsgs.push_back(electMsg);
    std::vector<std::pair<std::function<void*()>,std::function<void(void*)>>> verificationJobs;

    for (const auto &sig : sigs.sigs()) {

      if (!IsReplicaInGroup(sig.process_id(), logGrp, config)) {
        Debug("Signature for group %lu from replica %lu who is not in group.", logGrp, sig.process_id());
        verifyObj->mcb((void*) false);
        delete verifyObj;
        return;
      }
      if (!replicasVerified.insert(sig.process_id()).second) {
        Debug("Already verified signature from %lu.", sig.process_id());
        verifyObj->mcb((void*) false);
        delete verifyObj;
        return;
      }
      //TODO: does this work as expected?
      bool skip = false;
      if (sig.process_id() == myProcessId && myProcessId >= 0) {
        if (decision == myDecision) {
          skip = true;
          verifyObj->groupCounts[logGrp]++;
          if (verifyObj->groupCounts[logGrp] == verifyObj->quorumSize) {
            verifyObj->mcb((void*) true);
            delete verifyObj;
            return;
          }
        }
      }
      if(skip) continue;

      //TODO: add to job list
      std::function<bool()> func(std::bind(&Verifier::Verify, verifier, keyManager->GetPublicKey(sig.process_id()),
      std::ref(*electMsg), std::ref(sig.signature())));
        //turn into void* function in order to dispatch
      std::function<void*()> f(std::bind(BoolPointerWrapper, std::move(func)));

      std::function<void(void*)> cb(std::bind(asyncValidateP2RepliesCallback, verifyObj, logGrp, std::placeholders::_1));
      verificationJobs.emplace_back(std::make_pair(std::move(f), std::move(cb)));

    }

    verifyObj->deletable = verificationJobs.size();
    for (auto &verification : verificationJobs){

      //a)) Multithreading: Dispatched f: verify , cb: async Callback
      if(multithread && LocalDispatch){
        // Debug("P2 Validation is dispatched and parallel");
        auto comb = [f = std::move(verification.first), cb = std::move(verification.second)](){
          cb(f());
          return (void*) true;
        };
        transport->DispatchTP_noCB(std::move(comb));
        //transport->DispatchTP_local(std::move(verification.first), std::move(verification.second));
      }
      else if(multithread){

          transport->DispatchTP(std::move(verification.first), std::move(verification.second));

      }
      //b) No multithreading: Calls verify + async Callback.
      else{
        Debug("P2 Validation is local and serial");
        verification.second(verification.first());
      }
    }
}


void asyncValidateTransactionWrite(const proto::CommittedProof &proof,
    const std::string *txnDigest,
    const std::string &key, const std::string &val, const Timestamp &timestamp,
    const transport::Configuration *config, bool signedMessages,
    KeyManager *keyManager, Verifier *verifier, mainThreadCallback mcb, Transport* transport,
    bool multithread){
      if (proof.txn().client_id() == 0UL && proof.txn().client_seq_num() == 0UL) {
        // Genesis objects have no proofs ==> pass validation by default.
        // TODO: this is unsafe, but a hack so that we can bootstrap a benchmark
        //    without needing to write all existing data with transactions
        return mcb((void*) true);
      }
      if (Timestamp(proof.txn().timestamp()) != timestamp) {
        Debug("VALIDATE timestamp failed for txn %lu.%lu: txn ts %lu.%lu != returned"
            " ts %lu.%lu.", proof.txn().client_id(), proof.txn().client_seq_num(),
            proof.txn().timestamp().timestamp(), proof.txn().timestamp().id(),
            timestamp.getTimestamp(), timestamp.getID());
        return mcb((void*) false);
      }

      bool keyInWriteSet = false;
      for (const auto &write : proof.txn().write_set()) {
        if (write.key() == key) {
          keyInWriteSet = true;
          if (write.value() != val) {
            Debug("VALIDATE value failed for txn %lu.%lu key %s: txn value %s != "
                "returned value %s.", proof.txn().client_id(),
                proof.txn().client_seq_num(), BytesToHex(key, 16).c_str(),
                BytesToHex(write.value(), 16).c_str(), BytesToHex(val, 16).c_str());
            return mcb((void*) false);
          }
          break;
        }
      }

      if (!keyInWriteSet) {
        Debug("VALIDATE value failed for txn %lu.%lu; key %s not written.",
            proof.txn().client_id(), proof.txn().client_seq_num(),
            BytesToHex(key, 16).c_str());
        return mcb((void*) false);
      }

      if (!signedMessages){
        return mcb((void*) true);
      }
      else{
        auto cb = [mcb, &proof](void* result){
          if(!result){
            Debug("VALIDATE CommittedProof failed for txn %lu.%lu.",
                proof.txn().client_id(), proof.txn().client_seq_num());
          }
          mcb(result);
        };

        asyncValidateCommittedProof(proof, txnDigest, keyManager, config, verifier,
          std::move(cb), transport, multithread, false);
      }
}

void asyncValidateTransactionWriteCB(const proto::CommittedProof &proof,
   const std::string &key, const std::string &val, mainThreadCallback mcb, void* result){
   mcb(result);
}






bool ValidateTransactionWrite(const proto::CommittedProof &proof,
    const std::string *txnDigest,
    const std::string &key, const std::string &val, const Timestamp &timestamp,
    const transport::Configuration *config, bool signedMessages,
    KeyManager *keyManager, Verifier *verifier) {
  if (proof.txn().client_id() == 0UL && proof.txn().client_seq_num() == 0UL) {
    // TODO: this is unsafe, but a hack so that we can bootstrap a benchmark
    //    without needing to write all existing data with transactions
    return true;
  }

  if (signedMessages && !ValidateCommittedProof(proof, txnDigest,
        keyManager, config, verifier)) {
    Debug("VALIDATE CommittedProof failed for txn %lu.%lu.",
        proof.txn().client_id(), proof.txn().client_seq_num());
    return false;
  }

  if (Timestamp(proof.txn().timestamp()) != timestamp) {
    Debug("VALIDATE timestamp failed for txn %lu.%lu: txn ts %lu.%lu != returned"
        " ts %lu.%lu.", proof.txn().client_id(), proof.txn().client_seq_num(),
        proof.txn().timestamp().timestamp(), proof.txn().timestamp().id(),
        timestamp.getTimestamp(), timestamp.getID());
    return false;
  }

  bool keyInWriteSet = false;
  for (const auto &write : proof.txn().write_set()) {
    if (write.key() == key) {
      keyInWriteSet = true;
      if (write.value() != val) {
        Debug("VALIDATE value failed for txn %lu.%lu key %s: txn value %s != "
            "returned value %s.", proof.txn().client_id(),
            proof.txn().client_seq_num(), BytesToHex(key, 16).c_str(),
            BytesToHex(write.value(), 16).c_str(), BytesToHex(val, 16).c_str());
        return false;
      }
      break;
    }
  }

  if (!keyInWriteSet) {
    Debug("VALIDATE value failed for txn %lu.%lu; key %s not written.",
        proof.txn().client_id(), proof.txn().client_seq_num(),
        BytesToHex(key, 16).c_str());
    return false;
  }

  return true;
}

bool ValidateDependency(const proto::Dependency &dep,
    const transport::Configuration *config, uint64_t readDepSize,
    KeyManager *keyManager, Verifier *verifier) {
  if (dep.write_sigs().sigs_size() < readDepSize) {
    return false;
  }

  std::string preparedData;
  dep.write().SerializeToString(&preparedData);
  for (const auto &sig : dep.write_sigs().sigs()) {
    if (!verifier->Verify(keyManager->GetPublicKey(sig.process_id()), preparedData,
          sig.signature())) {
      return false;
    }
  }
  return true;
}

bool operator==(const proto::Write &pw1, const proto::Write &pw2) {
  return //pw1.committed_value() == pw2.committed_value() &&
    //pw1.committed_timestamp().timestamp() == pw2.committed_timestamp().timestamp() &&
    //pw1.committed_timestamp().id() == pw2.committed_timestamp().id() &&
    pw1.prepared_value() == pw2.prepared_value() &&
    pw1.prepared_timestamp().timestamp() == pw2.prepared_timestamp().timestamp() &&
    pw1.prepared_timestamp().id() == pw2.prepared_timestamp().id() &&
    pw1.prepared_txn_digest() == pw2.prepared_txn_digest();
}

bool operator!=(const proto::Write &pw1, const proto::Write &pw2) {
  return !(pw1 == pw2);
}


//should hashing be parallelized?
//ignores txnDigest field --> this is not part of protocol contents, just a hack for storage.
std::string TransactionDigest(const proto::Transaction &txn, bool hashDigest) {
  if (hashDigest) {
    blake3_hasher hasher;
    blake3_hasher_init(&hasher);

    std::string digest(BLAKE3_OUT_LEN, 0);

    uint64_t client_id = txn.client_id();
    uint64_t client_seq_num = txn.client_seq_num();

    blake3_hasher_update(&hasher, (unsigned char *) &client_id, sizeof(client_id));
    blake3_hasher_update(&hasher, (unsigned char *) &client_seq_num, sizeof(client_seq_num));
    for (const auto &group : txn.involved_groups()) {
      blake3_hasher_update(&hasher, (unsigned char *) &group, sizeof(group));
    }
    for (const auto &read : txn.read_set()) {
      uint64_t readtimeId = read.readtime().id();
      uint64_t readtimeTs = read.readtime().timestamp();
      blake3_hasher_update(&hasher, (unsigned char *) &read.key()[0], read.key().length());
      blake3_hasher_update(&hasher, (unsigned char *) &readtimeId,
          sizeof(read.readtime().id()));
      blake3_hasher_update(&hasher, (unsigned char *) &readtimeTs,
          sizeof(read.readtime().timestamp()));
    }
    for (const auto &write : txn.write_set()) {
      blake3_hasher_update(&hasher, (unsigned char *) &write.key()[0], write.key().length());
      blake3_hasher_update(&hasher, (unsigned char *) &write.value()[0], write.value().length());
    }
    for (const auto &dep : txn.deps()) {
      blake3_hasher_update(&hasher, (unsigned char *) &dep.write().prepared_txn_digest()[0],
          dep.write().prepared_txn_digest().length());
    }
    uint64_t timestampId = txn.timestamp().id();
    uint64_t timestampTs = txn.timestamp().timestamp();
    blake3_hasher_update(&hasher, (unsigned char *) &timestampId, sizeof(timestampId));
    blake3_hasher_update(&hasher, (unsigned char *) &timestampTs, sizeof(timestampTs));

    //Account for Queries now too:
    for (const auto &query : txn.query_set()) {
    
      blake3_hasher_update(&hasher, (unsigned char *) &query.query_id()[0], query.query_id().length());
      uint64_t retry_version = query.retry_version();
      blake3_hasher_update(&hasher, (unsigned char *) &retry_version, sizeof(retry_version));

      for(const auto &[group, group_md] : query.group_meta()){
        blake3_hasher_update(&hasher, (unsigned char *) &group, sizeof(group));
        if(group_md.has_read_set_hash()){
          blake3_hasher_update(&hasher, (unsigned char *) &group_md.read_set_hash()[0], group_md.read_set_hash().length());
        }
        else{
          //hash read set explicitly
          //  for (auto const &[key, ts] : group_md.read_set()) {
          //     // hash the input leafs. I.e. (key, version) pairs 
          //     blake3_hasher_update(&hasher, (unsigned char*) &key[0], key.length());
          //     uint64_t timestampId = ts.id(); // getID();
          //     uint64_t timestampTs = ts.timestamp(); // getTimestamp(); 
          //     blake3_hasher_update(&hasher, (unsigned char *) &timestampId, sizeof(timestampId));
          //     blake3_hasher_update(&hasher, (unsigned char *) &timestampTs, sizeof(timestampTs));
          //  }
          for (auto const &read : group_md.query_read_set().read_set()){
              uint64_t readtimeId = read.readtime().id();
              uint64_t readtimeTs = read.readtime().timestamp();
              blake3_hasher_update(&hasher, (unsigned char *) &read.key()[0], read.key().length());
              blake3_hasher_update(&hasher, (unsigned char *) &readtimeId, sizeof(readtimeId));
              blake3_hasher_update(&hasher, (unsigned char *) &readtimeTs, sizeof(readtimeTs));
          }
          for (const auto &dep : group_md.query_read_set().deps()) {
              blake3_hasher_update(&hasher, (unsigned char *) &dep.write().prepared_txn_digest()[0], dep.write().prepared_txn_digest().length());
          }
          for(const auto &pred: group_md.query_read_set().read_predicates()){
              blake3_hasher_update(&hasher, (unsigned char *) &pred.table_name()[0], pred.table_name().length()); 
              uint64_t readtimeId = pred.table_version().id();
              uint64_t readtimeTs = pred.table_version().timestamp();
              blake3_hasher_update(&hasher, (unsigned char *) &readtimeId, sizeof(readtimeId));
              blake3_hasher_update(&hasher, (unsigned char *) &readtimeTs, sizeof(readtimeTs));
              for(auto const &instance: pred.instantiations()){
                for(auto const &col_value: instance.col_values()){
                  blake3_hasher_update(&hasher, (unsigned char *) &col_value[0], col_value.length());
                }  
              }
          }
        }
      }
    }
    //Hash read_predicates directly (in case was merged by client already)
    for(const auto &pred: txn.read_predicates()){
      blake3_hasher_update(&hasher, (unsigned char *) &pred.table_name()[0], pred.table_name().length()); 
      uint64_t readtimeId = pred.table_version().id();
      uint64_t readtimeTs = pred.table_version().timestamp();
      blake3_hasher_update(&hasher, (unsigned char *) &readtimeId, sizeof(readtimeId));
      blake3_hasher_update(&hasher, (unsigned char *) &readtimeTs, sizeof(readtimeTs));
      for(auto const &instance: pred.instantiations()){
        for(auto const &col_value: instance.col_values()){
          blake3_hasher_update(&hasher, (unsigned char *) &col_value[0], col_value.length());
        }  
      }
    }


    //Account for TableWrites too: 
    //Protobuf Map has undefined order; in particular, every INSTANCE of the object could have a different order 
    //=> must sort to be deterministic. BLAKE3 hash is not commutative, the order matters

     //TODO: Ideally sort map directly to avoid redundant loop.  //  std::sort(txn.mutable_table_writes()->begin(), txn.mutable_table_writes()->end());   
     //However, this requires txn to not be const, which requires changes across the codebase. For simplicity: just sort a vector of references.
    
    std::vector<std::pair<const std::string*, const TableWrite*>> tt;
    for (const auto &[table, table_write]: txn.table_writes()) {
       tt.emplace_back(&table, &table_write);
    }
    std::sort(tt.begin(), tt.end(), [](auto l, auto r){ return (*l.first) < (*r.first); });

    for (const auto &[table, table_write]: tt) {
      blake3_hasher_update(&hasher, (unsigned char *) &(*table)[0], table->length());

      for(const auto &row: table_write->rows()){
          if(row.has_deletion()){
            // bool del = row.deletion();
            // blake3_hasher_update(&hasher, (unsigned char *) &del, sizeof(del));
            blake3_hasher_update(&hasher, &(const unsigned char &) row.deletion(), sizeof(row.deletion()));
          }
          for(const auto &val: row.column_values()){
            blake3_hasher_update(&hasher, (unsigned char *) &val[0], val.length());
          }
      }
    }

    blake3_hasher_finalize(&hasher, (unsigned char *) &digest[0], BLAKE3_OUT_LEN);

    return digest;
  } else {
    char digestChar[16];
    *reinterpret_cast<uint64_t *>(digestChar) = txn.client_id();
    *reinterpret_cast<uint64_t *>(digestChar + 8) = txn.client_seq_num();
    return std::string(digestChar, 16);
  }
}

std::string QueryDigest(const proto::Query &query, bool queryHashDigest){
    //TODO: Change Txn to also include Query + query digest.
  if (queryHashDigest) {
    blake3_hasher hasher;
    blake3_hasher_init(&hasher);
    std::string digest(BLAKE3_OUT_LEN, 0);

    uint64_t client_id = query.client_id();
    uint64_t query_seq_num = query.query_seq_num();
    blake3_hasher_update(&hasher, (unsigned char *) &client_id, sizeof(client_id));
    blake3_hasher_update(&hasher, (unsigned char *) &query_seq_num, sizeof(query_seq_num));
  
    blake3_hasher_update(&hasher, (unsigned char *) &query.query_cmd()[0], query.query_cmd().length());

    uint64_t timestampId = query.timestamp().id();
    uint64_t timestampTs = query.timestamp().timestamp();
    blake3_hasher_update(&hasher, (unsigned char *) &timestampId,
        sizeof(timestampId));
    blake3_hasher_update(&hasher, (unsigned char *) &timestampTs,
        sizeof(timestampTs));

     uint64_t query_manager = query.query_manager();
     blake3_hasher_update(&hasher, (unsigned char *) &query_manager,
         sizeof(query_manager));

    blake3_hasher_finalize(&hasher, (unsigned char *) &digest[0], BLAKE3_OUT_LEN);

    return digest;
  } else {
    char digestChar[16];
    *reinterpret_cast<uint64_t *>(digestChar) = query.client_id();
    *reinterpret_cast<uint64_t *>(digestChar + 8) = query.query_seq_num();
    return std::string(digestChar, 16);
  }
}

std::string QueryRetryId(const std::string &queryId, const uint64_t &retry_version, bool queryHashDigest){
  if (queryHashDigest) {
    blake3_hasher hasher;
    blake3_hasher_init(&hasher);
    std::string digest(BLAKE3_OUT_LEN, 0);

    blake3_hasher_update(&hasher, (unsigned char *) &queryId[0], queryId.length());
    blake3_hasher_update(&hasher, (unsigned char *) &retry_version, sizeof(retry_version));
  
    blake3_hasher_finalize(&hasher, (unsigned char *) &digest[0], BLAKE3_OUT_LEN);
    return digest;
  } else {
    char digestChar[8];
    *reinterpret_cast<uint64_t *>(digestChar) = retry_version;
    return queryId + std::string(digestChar, 8);
  }
}

// TODO:  Could change input directly to google::protobuf::RepeatedPtrField<ReadMessage>
std::string generateReadSetSingleHash(const proto::ReadSet &query_read_set) { 
  blake3_hasher hasher;
  blake3_hasher_init(&hasher);
  std::string hash_chain(BLAKE3_OUT_LEN, 0);
  //hash the read_set
  for (auto const &read : query_read_set.read_set()){
      uint64_t readtimeId = read.readtime().id();
      uint64_t readtimeTs = read.readtime().timestamp();
      blake3_hasher_update(&hasher, (unsigned char *) &read.key()[0], read.key().length());
      blake3_hasher_update(&hasher, (unsigned char *) &readtimeId, sizeof(read.readtime().id()));
      blake3_hasher_update(&hasher, (unsigned char *) &readtimeTs, sizeof(read.readtime().timestamp()));
  }

  //Note: Dependencies do not need to be hashed

  //hash the read_predicates
  for (auto const &pred: query_read_set.read_predicates()){
    //Note: Table Version need not match.
     //Note: Technicaly don't need to hash instantiations either. 
      //If there is more than 1 then it is a right join clause. In that case, the result/read-set already uniquely captures this pred set, and the client could set it himself...
      for(auto const &instance: pred.instantiations()){
        for(auto const &col_value: instance.col_values()){
          blake3_hasher_update(&hasher, (unsigned char *) &col_value[0], col_value.length());
        }  
      }
    //Everything else in the predicate (table name, where clause is nothing the replica computed -- the client input it so it already knows it.)
  }

   // copy the digest into the output array
  blake3_hasher_finalize(&hasher, (unsigned char *) &hash_chain[0], BLAKE3_OUT_LEN);
  return hash_chain;
}

std::string generateReadSetSingleHash(const std::map<std::string, TimestampMessage> &read_set) { 

  blake3_hasher hasher;
  blake3_hasher_init(&hasher);
  std::string hash_chain(BLAKE3_OUT_LEN, 0);
  //hash the read_set
  for (auto const &[key, ts] : read_set) {
    // hash the input leafs. I.e. (key, version) pairs 
    blake3_hasher_update(&hasher, (unsigned char*) &key[0], key.length());
    uint64_t timestampId = ts.id(); // getID();
    uint64_t timestampTs = ts.timestamp(); // getTimestamp(); 
    blake3_hasher_update(&hasher, (unsigned char *) &timestampId, sizeof(timestampId));
    blake3_hasher_update(&hasher, (unsigned char *) &timestampTs, sizeof(timestampTs));
  }
   // copy the digest into the output array
  blake3_hasher_finalize(&hasher, (unsigned char *) &hash_chain[0], BLAKE3_OUT_LEN);
  return hash_chain;
}

std::string generateReadSetMerkleRoot(const std::map<std::string, TimestampMessage> &read_set, uint64_t m) { 
  //This function generates and computes a full, static Merkle tree in place -- it directly assigns positions in a well-balanced tree.
  //Alternative, greedy approach: Could dynamically build a non-perfect tree by using a queue: Pick (up to) first m available elements and hash. Add hash back to end. Cont until queue has 1 element = root.
  //Alternative, non-static approach: Could build a tree dynamically (support insert/remove operations) and implement a compute function that bubbles up values
  blake3_hasher hasher;

  unsigned int n = read_set.size();
  // m is branch_factor
  assert(n > 0);
  size_t hash_size = BLAKE3_OUT_LEN;

  // allocate the merkle tree in heap form (i.left = 2i, i.right = 2i+1)
  uint64_t num_nodes = (m * (n - 1) + (m - 2)) / (m - 1) + 1; // add (m-2) to ensure ceil
                  // Closed form solution of:   (sum_x = 1 to log_m(n){ n / (m^(x-1)) } + 1    (e.g. bottom layer has n nodes, next layer n/m, next n/m^2 ... until 1; there is log_m(n)+1 layers, the last layer is just root )
                  // ==> == n * 1/(m^log_m(n) * sum_x to log_m(n){ m^x} + 1
                  // ==> == 1 * sum_x to log_m(n){ m^x} + 1
                  // ==> == 1 * sum_x to log_2(n)/log_2(m){ m^x} + 1
                  // ==> == (m * (n-1)) / (m-1) + 1

  unsigned char* tree = (unsigned char*) malloc(hash_size*num_nodes);
  // insert the message hashes into the tree

  unsigned int i = 0;
  for (auto const &[key, ts] : read_set) {
    // need to initialize on every hash 
    blake3_hasher_init(&hasher);

    // hash the input leafs. I.e. (key, version) pairs 
    blake3_hasher_update(&hasher, (unsigned char*) &key[0], key.length());
    uint64_t timestampId = ts.id(); // getID();
    uint64_t timestampTs = ts.timestamp(); // getTimestamp(); 
    blake3_hasher_update(&hasher, (unsigned char *) &timestampId, sizeof(timestampId));
    blake3_hasher_update(&hasher, (unsigned char *) &timestampTs, sizeof(timestampTs));
    
    // copy the digest into the output array
    blake3_hasher_finalize(&hasher, &tree[((n - 1 + (m - 2)) / (m - 1) + i) * hash_size], BLAKE3_OUT_LEN);
    i++;
  }

  int min_leaf = ((n - 1 + (m - 2)) / (m - 1));         //index of left most bottom leaf in tree.   -- Note, this is the number of non-leaf nodes in the tree
  int max_leaf = ((n - 1 + (m - 2)) / (m - 1) + n - 1); //index of right most bottom leaf in tree.  -- Note, this is the same as num_nodes -1 
  // compute the hashes going up the tree ;; (starting from right to left)
  for (int r = max_leaf; r > 1; ) {
    int l = (r-1)/m * m + 1;  //find left index of the group of size m that will be hashed together. //Note that (r-1)/m will round down ==> finds next lower group

    blake3_hasher_init(&hasher);
  
    blake3_hasher_update(&hasher, &tree[l * hash_size], (r + 1 - l) * BLAKE3_OUT_LEN); //hash together concatentation of (r+1-l) = m sibling hashes in the tree

    blake3_hasher_finalize(&hasher, &tree[(r - 1) / m * hash_size], BLAKE3_OUT_LEN); //write the output hash to the parent node in the tree.

    r = l - 1;
  }

  // sign the hash at the root of the tree
  std::string rootHash(&tree[0], &tree[hash_size]);

  //free tree
  free(tree);

  return rootHash;
}


std::string BytesToHex(const std::string &bytes, size_t maxLength) {
  static const char digits[] = "0123456789abcdef";
  std::string hex;
  size_t length = (bytes.size() < maxLength) ? bytes.size() : maxLength;
  for (size_t i = 0; i < length; ++i) {
    hex.push_back(digits[static_cast<uint8_t>(bytes[i]) >> 4]);
    hex.push_back(digits[static_cast<uint8_t>(bytes[i]) & 0xF]);
  }
  return hex;
}

//FIXME: This Function is not taking into account the Timestamps. TODO: Add the timestamp checks (like in concurrencycontrol.cc)
bool TransactionsConflict(const proto::Transaction &a, const proto::Transaction &b) { //a is the conflict proof, b the current txn
  for (const auto &ra : a.read_set()) {
    std::cerr << "a key: " << ra.key() << std::endl;
    for (const auto &wb : b.write_set()) {
      if (ra.key() == wb.key()) {
        return true;
      }
    }
  }
  for (const auto &rb : b.read_set()) {
    std::cerr << "b key: " << rb.key() << std::endl;
    for (const auto &wa : a.write_set()) {
      if (rb.key() == wa.key()) {
        return true;
      }
    }
  }

  //TODO: FIXME: Add support for Conflict detection when using Cached Read Set
                //Need to compare the full merged read sets (I.e. merged_rs_a vs write set b, and vice versa)
                //TODO: Need so somehow authenticate merged read set correctness. Need to map it back to the hashes.
                  //Note: CC check compares against locally stored prepared/committed read sets (which are the merged sets)
  //Problem: For our own TX we don't have the read set either... 3 options
      //1: Wait for f+1 ABORT votes, not just singular one. 2: Conflict must include our MergedTX as well, 3: ZK proof that TX conflict.
      
  //JUST A HACK TO ACKNOWLEDGE THAT CONFLICT "MIGHT" BE VALID TODO: Make it proper
  if(a.merged_read_set().read_set_size() > a.read_set_size()) return true; //If the conflict TX has a merged read set that we aren't checking
  if(b.query_set().size() > 0) return true; //Or our TX has some query read set that may be cached and not accessible...
            //TODO: We cannot accept a singular replica Abort Vote when caching read set. We must wait for f+1...

  //TODO: Add support for conflict detection when using predicates.. (check all read preds vs writes, and vice versa)
  //FIXME: For now just allow to pass.
  if(a.query_set().size() > 0) return true;

  //Note: There should be no write/write conflicts
  // for (const auto &wa : a.write_set()) {
  //   for (const auto &wb : b.write_set()) {
  //     if (wa.key() == wb.key()) {
  //       return true;
  //     }
  //   }
  // }
  return false;
}

uint64_t QuorumSize(const transport::Configuration *config) {
  return 4 * static_cast<uint64_t>(config->f) + 1;
}

uint64_t FastQuorumSize(const transport::Configuration *config) {
  return static_cast<uint64_t>(config->n);
}

uint64_t SlowCommitQuorumSize(const transport::Configuration *config) {
  return 3 * static_cast<uint64_t>(config->f) + 1;
}

uint64_t FastAbortQuorumSize(const transport::Configuration *config) {
  return 3 * static_cast<uint64_t>(config->f) + 1;
}

uint64_t SlowAbortQuorumSize(const transport::Configuration *config) {
  return static_cast<uint64_t>(config->f) + 1;
}

bool IsReplicaInGroup(uint64_t id, uint32_t group,
    const transport::Configuration *config) {
  int host = config->replicaHost(id / config->n, id % config->n);
  for (int i = 0; i < config->n; ++i) {
    if (host == config->replicaHost(group, i)) {
      return true;
    }
  }
  return false;
}

int64_t GetLogGroup(const proto::Transaction &txn, const std::string &txnDigest) {
  uint8_t groupIdx = txnDigest[0];
  groupIdx = groupIdx % txn.involved_groups_size();
  UW_ASSERT(groupIdx < txn.involved_groups_size());
  return txn.involved_groups(groupIdx);
}

} // namespace pequinstore

