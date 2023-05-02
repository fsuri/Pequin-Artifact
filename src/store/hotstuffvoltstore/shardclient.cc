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

#include "store/hotstuffvoltstore/shardclient.h"
#include "store/hotstuffvoltstore/pbft_batched_sigs.h"
#include "store/hotstuffvoltstore/common.h"

namespace hotstuffvoltstore {

ShardClient::ShardClient(const transport::Configuration& config, Transport *transport,
    uint64_t client_id, uint64_t group_idx, const std::vector<int> &closestReplicas_,
    bool signMessages, bool validateProofs,
    KeyManager *keyManager, Stats* stats, bool order_commit, bool validate_abort) :
    config(config), transport(transport),
    group_idx(group_idx),
    signMessages(signMessages), validateProofs(validateProofs),
    keyManager(keyManager), stats(stats), order_commit(order_commit), validate_abort(validate_abort) {
  transport->Register(this, config, -1, -1);
  readReq = 0;
  queryReq = 0;
  commitReq = 0;
  abortReq = 0;

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

bool ShardClient::validateReadProof(const proto::CommitProof& commitProof, const std::string& key,
  const std::string& value, const Timestamp& timestamp) {
    // hack for load:
    if (timestamp.getID() == 0 && timestamp.getTimestamp() == 0) {
      Debug("Using preloaded key");
      return true;
    }

    // First, verify the transaction
    Debug("Validating read proof");

    // txn must have timestamp of write
    if (Timestamp(commitProof.txn().timestamp()) != timestamp) {
      return false;
    }
    Debug("timestamp valid");

    bool found_write = false;

    for (const auto& write : commitProof.txn().writeset()) {
      if (write.key() == key && write.value() == value) {
        found_write = true;
        break;
      }
    }

    if (!found_write) {
      return false;
    }
    Debug("write valid");

    // Verified Transaction at this point

    // Next, verify that the decision is valid for the transaction

    std::string proofTxnDigest = TransactionDigest(commitProof.txn());

    // make sure the writeback message is for the transaction
    if (commitProof.writeback_message().txn_digest() != proofTxnDigest) {
      return false;
    }
    Debug("commit digest valid");

    if (commitProof.writeback_message().status() != REPLY_OK) {
      return false;
    }
    Debug("writeback status valid");

    if (!verifyGDecision(commitProof.writeback_message(), commitProof.txn(), keyManager, signMessages, config.f)) {
      return false;
    }
    Debug("proof valid");

    return true;
  }

void ShardClient::ReceiveMessage(const TransportAddress &remote,
    const std::string &t, const std::string &d,
    void *meta_data) {
      Debug("handling message of type %s", t.c_str());
  proto::SignedMessage signedMessage;
  std::string type;
  std::string data;
  proto::TransactionDecision transactionDecision;

  bool recvSignedMessage = false;
  if (t == signedMessage.GetTypeName()) {
    if (!signedMessage.ParseFromString(d)) {
      return;
    }

    proto::PackedMessage pmsg;
    pmsg.ParseFromString(signedMessage.packed_msg());
    if (pmsg.type() == transactionDecision.GetTypeName()) {
    crypto::PubKey* replicaPublicKey = keyManager->GetPublicKey(
        signedMessage.replica_id());
      if (!hotstuffvoltBatchedSigs::verifyBatchedSignature(signedMessage.mutable_signature(), signedMessage.mutable_packed_msg(),
            replicaPublicKey)) {
             Debug("dec signature was invalid");
             return;
            }
      data = pmsg.msg();
      type = pmsg.type();

    } else {
      if(!ValidateSignedMessage(signedMessage, keyManager, data, type)) {
       Debug("signature was invalid");
       return;
      }
    }

    recvSignedMessage = true;
    Debug("signature was valid");
  } else {
    type = t;
    data = d;
  }

  proto::ReadReply readReply;
  proto::GroupedDecisionAck groupedDecisionAck;
  proto::QueryReply queryReply;
  proto::CommitReply commitReply;
  proto::AbortReply abortReply;
  if (type == readReply.GetTypeName()) {
    readReply.ParseFromString(data);

    if (signMessages && !recvSignedMessage) {
      return;
    }

    HandleReadReply(readReply, signedMessage);
  } else if (type == transactionDecision.GetTypeName()) {
    transactionDecision.ParseFromString(data);

    if (signMessages && !recvSignedMessage) {
      return;
    }

    HandleTransactionDecision(transactionDecision, signedMessage);
  } else if (type == groupedDecisionAck.GetTypeName()) {
    groupedDecisionAck.ParseFromString(data);

    if (signMessages && !recvSignedMessage) {
      return;
    }

    HandleWritebackReply(groupedDecisionAck, signedMessage);
  } else if(type == queryReply.GetTypeName()) {
    queryReply.ParseFromString(data);

    HandleQueryReply(queryReply, signedMessage);
  } else if(type == commitReply.GetTypeName()) {
    commitReply.ParseFromString(data);

    HandleCommitReply(commitReply, signedMessage);
  } else if(type == abortReply.GetTypeName()) {
    abortReply.ParseFromString(data);

    HandleAbortReply(abortReply, signedMessage);
  }
}

// ================================
// ======= MESSAGE HANDLERS =======
// ================================

void ShardClient::HandleReadReply(const proto::ReadReply& readReply, const proto::SignedMessage& signedMsg) {
  Debug("Handling a read reply");

  // get the read request id from the reply
  uint64_t reqId = readReply.req_id();
  Debug("Read red id: %lu", reqId);

  // try and find a matching pending read based on the request
  if (pendingReads.find(reqId) != pendingReads.end()) {
    PendingRead* pendingRead = &pendingReads[reqId];
    // we always mark a reply even if it fails because the read could fail
    if (signMessages) {
      uint64_t replica_id = signedMsg.replica_id();
      // make sure the replica is from this shard
      if (replica_id / config.n != (uint64_t) group_idx) {
        Debug("Read Reply: replica not in group");
        return;
      }
      // insert the signed replica id as a received reply
      pendingRead->receivedReplies.insert(replica_id);
    } else {
      // insert a new fake id into the received replies
      pendingRead->receivedReplies.insert(pendingRead->receivedReplies.size());
    }
    if (readReply.status() == REPLY_OK) {
      Timestamp rts(readReply.value_timestamp());
      if (validateProofs) {
        // if the proof is invalid, stop processing this reply
        if (!validateReadProof(readReply.commit_proof(), readReply.key(), readReply.value(), rts)) {
          return;
        }
      }
      // if we haven't recorded a read result yet or we have a higher read,
      // make this reply the new max
      if (pendingRead->status == REPLY_FAIL || rts > pendingRead->maxTs) {
        Debug("Updating max read reply");
        pendingRead->maxTs = rts;
        pendingRead->maxValue = readReply.value();
        pendingRead->maxCommitProof = readReply.commit_proof();
        pendingRead->status = REPLY_OK;
      }
    }

    Debug("reply size: %lu", pendingRead->receivedReplies.size());
    if (pendingRead->receivedReplies.size() >= pendingRead->numResultsRequired) {
      if (pendingRead->timeout != nullptr) {
        pendingRead->timeout->Stop();
      }
      read_callback rcb = pendingRead->rcb;
      std::string value = pendingRead->maxValue;
      Timestamp readts = pendingRead->maxTs;
      std::string key = readReply.key();
      uint64_t status = pendingRead->status;
      pendingReads.erase(reqId);
      rcb(status, key, value, readts);
    }
  }
}


// Not a finished method currently
void ShardClient::HandleQueryReply(const proto::QueryReply& queryReply, const proto::SignedMessage& signedMsg) {//Need to write this function out I guess, maybe not
  Debug("Handling transaction decision");

  std::string digest = queryReply.sql_res().SerializeAsString();
  SQLResult queryRes = queryReply.sql_res();
  // DebugHash(digest);
  uint64_t reqId = queryReply.req_id();
  // only handle decisions for my shard
  // NOTE: makes the assumption that numshards == numgroups
  if (queryReply.group_id() == (uint64_t) group_idx) {
    if (signMessages) { //Will handle this later once I understand signed messages better
      stats->Increment("handle_query_res_s",1);
      // Debug("signed packed msg: %s", string_to_hex(signedMsg.packed_msg()).c_str());
      // get the pending signed preprepare
      if (pendingSignedQueries.find(reqId) != pendingSignedQueries.end()) {
        Debug("Adding signed id to a set: %lu", signedMsg.replica_id());

        PendingSignedQuery* psq = &pendingSignedQueries[reqId];
        uint64_t add_id = signedMsg.replica_id();
        // make sure this id is actually in the group
        if (add_id / config.n == (uint64_t) group_idx) {
          if (queryReply.status() == REPLY_OK) {
            // add the decision to the list as proof
            psq->receivedValidSigs[add_id] = signedMsg.signature();
            // Debug("signature for %lu: %s", add_id, string_to_hex(signedMsg.signature()).c_str());
          } else {
            if(validate_abort){
              psq->receivedFailedSigs[add_id] = signedMsg.signature();
            }
            else{
              psq->receivedFailedIds.insert(add_id);
            }

          }
        }

        proto::GroupedSignedMessage groupSignedMsg;

        // once we have enough valid requests, construct the grouped decision
        // and return success
        if (psq->receivedValidSigs.size() >= (uint64_t) config.f + 1) {
          Debug("Got enough *valid* transaction decisions, executing callback");
          // set the packed decision

          //groupSignedMsg.set_packed_msg(psp->validDecisionPacked);
          groupSignedMsg.set_packed_msg(CreateValidQueryDecision(queryRes));
          // Debug("packed decision: %s", string_to_hex(psp->validDecisionPacked).c_str());

          // add the signatures
          for (const auto& pair : psq->receivedValidSigs) {
            (*groupSignedMsg.mutable_signatures())[pair.first] = pair.second;
          }

          // invoke the callback with the signed grouped decision
          signed_query_callback qcb = psq->qcb;
          if (psq->timeout != nullptr) {
            psq->timeout->Stop();
          }
          pendingSignedQueries.erase(reqId);
          ////
          // struct timeval tv;
          // gettimeofday(&tv, NULL);
          // uint64_t current_time = (tv.tv_sec*1000000+tv.tv_usec);  //in microseconds
          // total_elapsed += (current_time - start_time);
          // //std::cerr << "time measured: " << (current_time - start_time) << std::endl;;
          // total_prepare++;
          // if(total_prepare == 50) std::cerr << "Average time to prepare: " << (total_elapsed / total_prepare) << " us" << std::endl;
          //std::cerr << "Elapsed time for Prepare Phase: " << (current_time - start_time) << std::endl;
          ////
          qcb(REPLY_OK, groupSignedMsg);
          return;
        }
        else if(validate_abort && psq->receivedFailedSigs.size() >= (uint64_t) config.f + 1 ){
          Debug("Got enough *failed* transaction decisions, executing callback");
          // set the packed decision

          //groupSignedMsg.set_packed_msg(psp->validDecisionPacked);
          groupSignedMsg.set_packed_msg(CreateFailedQueryDecision(queryRes));

          // add the signatures
          for (const auto& pair : psq->receivedFailedSigs) {
            (*groupSignedMsg.mutable_signatures())[pair.first] = pair.second;
          }

          // invoke the callback with the signed grouped decision
          signed_query_callback qcb = psq->qcb;
          if (psq->timeout != nullptr) {
            psq->timeout->Stop();
          }
          pendingSignedQueries.erase(reqId);
          ////
          // struct timeval tv;
          // gettimeofday(&tv, NULL);
          // uint64_t current_time = (tv.tv_sec*1000000+tv.tv_usec);  //in microseconds
          // total_elapsed += (current_time - start_time);
          // //std::cerr << "time measured: " << (current_time - start_time) << std::endl;
          // total_prepare++;
          // if(total_prepare == 50) std::cerr << "Average time to prepare: " << (total_elapsed / total_prepare) << " us" << std::endl;
          // //std::cerr << "Elapsed time for Prepare Phase: " << (current_time - start_time) << std::endl;
          ////
          qcb(REPLY_FAIL, groupSignedMsg);
          return;
        }
        // if we get f+1 failures, we can return early
        else if (!validate_abort && psq->receivedFailedIds.size() >= (uint64_t) config.f + 1) {
          Debug("Not enough valid query decisions, failing");
          signed_query_callback qcb = psq->qcb;
          if (psq->timeout != nullptr) {
            psq->timeout->Stop();
          }
          pendingSignedQueries.erase(reqId);
          // adding sigs to the grouped signed msg would be worthless
          qcb(REPLY_FAIL, groupSignedMsg);
          return;
        }
      }
    } else { 
      stats->Increment("handle_query_reply",1);
      if (pendingQueries.find(reqId) != pendingQueries.end()) {
        PendingQuery* pq = &pendingQueries[reqId];
        if (queryReply.status() == REPLY_OK) {
          uint64_t add_id = pq->receivedOkIds[digest].size();
          // Need to figure out how to get unique ids here
          pq->receivedOkIds[digest].insert(add_id);
        } else {
          // Kinda jank but just don't use these ids
          uint64_t add_id = pq->receivedFailedIds.size();
          pq->receivedFailedIds.insert(add_id);
        }

        // once we have enough valid requests, construct the grouped decision
        // and return success
        if (pq->receivedOkIds[digest].size() >= (uint64_t) config.f + 1) {
          proto::QueryReply validQueryRes = pq->validQueryRes;
          // invoke the callback if we have enough of the same decision
          query_callback qcb = pq->qcb;
          if (pq->timeout != nullptr) {
            pq->timeout->Stop();
          }
          pendingQueries.erase(reqId);
          qcb(REPLY_OK, validQueryRes); //  May need to look into type of query callback
          return;
        }
        // f+1 failures mean that we will always return fail
        if (pq->receivedFailedIds.size() >= (uint64_t) config.f + 1 || pq->receivedFailedIds.size() + pq->receivedOkIds.size() > config.f) {
          proto::QueryReply failedQueryReply;
          failedQueryReply.set_status(REPLY_FAIL);
          query_callback qcb = pendingQueries[reqId].qcb;
          if (pq->timeout != nullptr) {
            pq->timeout->Stop();
          }
          pendingQueries.erase(reqId);
          qcb(REPLY_FAIL, failedQueryReply);
          return;
        }
      }
    }
  } else {
    stats->Increment("wrong_query_shard",1);
  }
}

void ShardClient::HandleCommitReply(const proto::CommitReply& commitReply, const proto::SignedMessage& signedMsg) {
  Debug("Handling transaction decision");

  // DebugHash(digest);
  // only handle decisions for my shard
  // NOTE: makes the assumption that numshards == numgroups
  // std::pair<uint64_t, uint64_t> mapKey = make_pair(commitReply.client_id(), commitReply.txn_seq_num());
  uint64_t reqId = commitReply.req_id();
  if (commitReply.group_id() == (uint64_t) group_idx) {
    if (signMessages) { //Will handle this later, currently the implementation of signed messages is wrong
      stats->Increment("handle_commit_res_s",1);
      // Debug("signed packed msg: %s", string_to_hex(signedMsg.packed_msg()).c_str());
      // get the pending signed preprepare
      if (pendingSignedCommits.find(reqId) != pendingSignedCommits.end()) {
        Debug("Adding signed id to a set: %lu", signedMsg.replica_id());

        PendingSignedCommit* psq = &pendingSignedCommits[reqId];
        uint64_t add_id = signedMsg.replica_id();
        // make sure this id is actually in the group
        if (add_id / config.n == (uint64_t) group_idx) {
          if (commitReply.status() == REPLY_OK) {
            // add the decision to the list as proof
            psq->receivedValidSigs[add_id] = signedMsg.signature();
            // Debug("signature for %lu: %s", add_id, string_to_hex(signedMsg.signature()).c_str());
          } else {
            if(validate_abort){
              psq->receivedFailedSigs[add_id] = signedMsg.signature();
            }
            else{
              psq->receivedFailedIds.insert(add_id);
            }

          }
        }

        proto::GroupedSignedMessage groupSignedMsg;

        // once we have enough valid requests, construct the grouped decision
        // and return success
        if (psq->receivedValidSigs.size() >= (uint64_t) config.f + 1) {
          Debug("Got enough *valid* transaction decisions, executing callback");
          // set the packed decision

          //groupSignedMsg.set_packed_msg(psp->validDecisionPacked);
          groupSignedMsg.set_packed_msg(CreateValidCommitDecision());
          // Debug("packed decision: %s", string_to_hex(psp->validDecisionPacked).c_str());

          // add the signatures
          for (const auto& pair : psq->receivedValidSigs) {
            (*groupSignedMsg.mutable_signatures())[pair.first] = pair.second;
          }

          // invoke the callback with the signed grouped decision
          signed_commit_callback ccb = psq->ccb;
          if (psq->timeout != nullptr) {
            psq->timeout->Stop();
          }
          pendingSignedQueries.erase(reqId);
          ////
          // struct timeval tv;
          // gettimeofday(&tv, NULL);
          // uint64_t current_time = (tv.tv_sec*1000000+tv.tv_usec);  //in microseconds
          // total_elapsed += (current_time - start_time);
          // //std::cerr << "time measured: " << (current_time - start_time) << std::endl;;
          // total_prepare++;
          // if(total_prepare == 50) std::cerr << "Average time to prepare: " << (total_elapsed / total_prepare) << " us" << std::endl;
          //std::cerr << "Elapsed time for Prepare Phase: " << (current_time - start_time) << std::endl;
          ////
          ccb(REPLY_OK, groupSignedMsg);
          return;
        }
        else if(validate_abort && psq->receivedFailedSigs.size() >= (uint64_t) config.f + 1 ){
          Debug("Got enough *failed* transaction decisions, executing callback");
          // set the packed decision

          //groupSignedMsg.set_packed_msg(psp->validDecisionPacked);
          groupSignedMsg.set_packed_msg(CreateFailedCommitDecision());

          // add the signatures
          for (const auto& pair : psq->receivedFailedSigs) {
            (*groupSignedMsg.mutable_signatures())[pair.first] = pair.second;
          }

          // invoke the callback with the signed grouped decision
          signed_commit_callback ccb = psq->ccb;
          if (psq->timeout != nullptr) {
            psq->timeout->Stop();
          }
          pendingSignedCommits.erase(reqId);
          ////
          // struct timeval tv;
          // gettimeofday(&tv, NULL);
          // uint64_t current_time = (tv.tv_sec*1000000+tv.tv_usec);  //in microseconds
          // total_elapsed += (current_time - start_time);
          // //std::cerr << "time measured: " << (current_time - start_time) << std::endl;
          // total_prepare++;
          // if(total_prepare == 50) std::cerr << "Average time to prepare: " << (total_elapsed / total_prepare) << " us" << std::endl;
          // //std::cerr << "Elapsed time for Prepare Phase: " << (current_time - start_time) << std::endl;
          ////
          ccb(REPLY_FAIL, groupSignedMsg);
          return;
        }
        // if we get f+1 failures, we can return early
        else if (!validate_abort && psq->receivedFailedIds.size() >= (uint64_t) config.f + 1) {
          Debug("Not enough valid query decisions, failing");
          signed_commit_callback ccb = psq->ccb;
          if (psq->timeout != nullptr) {
            psq->timeout->Stop();
          }
          pendingSignedCommits.erase(reqId);
          // adding sigs to the grouped signed msg would be worthless
          ccb(REPLY_FAIL, groupSignedMsg);
          return;
        }
      }
    } else { 
      stats->Increment("handle_commit_reply",1);
      if (pendingCommits.find(reqId) != pendingCommits.end()) {
        PendingCommit* pq = &pendingCommits[reqId];
        if (commitReply.status() == REPLY_OK) {
          uint64_t add_id = pq->receivedOkIds.size(); //  I do the below because they do this
          // If the ids are to be confirmed distinct, this doesn't work
          // add the decision to the list as proof
          pq->receivedOkIds.insert(add_id);
        } else {
          // Kinda jank but just don't use these ids
          uint64_t add_id = pq->receivedFailedIds.size();
          pq->receivedFailedIds.insert(add_id);
        }

        // once we have enough valid requests, construct the grouped decision
        // and return success
        if (pq->receivedOkIds.size() >= (uint64_t) config.f + 1) {
          proto::CommitReply validCommitRes = pq->validDecision;
          // invoke the callback if we have enough of the same decision
          commit_callback ccb = pq->ccb;
          if (pq->timeout != nullptr) {
            pq->timeout->Stop();
          }
          pendingCommits.erase(reqId);
          ccb(REPLY_OK, validCommitRes);
          return;
        }
        // f+1 failures mean that we will always return fail
        if (pq->receivedFailedIds.size() >= (uint64_t) config.f + 1) {
          proto::CommitReply failedCommitReply;
          failedCommitReply.set_status(REPLY_FAIL);
          commit_callback ccb = pendingCommits[reqId].ccb;
          if (pq->timeout != nullptr) {
            pq->timeout->Stop();
          }
          pendingCommits.erase(reqId);
          ccb(REPLY_FAIL, failedCommitReply);
          return;
        }
      }
    }
  } else {
    stats->Increment("wrong_commit_shard",1);
  }
}

void ShardClient::HandleAbortReply(const proto::AbortReply& abortReply, const proto::SignedMessage& signedMsg) {
  Debug("Handling transaction decision");

  // DebugHash(digest);
  // only handle decisions for my shard
  // NOTE: makes the assumption that numshards == numgroups
  // std::pair<uint64_t, uint64_t> mapKey = make_pair(abortReply.client_id(), abortReply.txn_seq_num());
  uint64_t reqId = abortReply.req_id();
  if (abortReply.group_id() == (uint64_t) group_idx) {
    if (signMessages) { //Will handle this later, currently the implementation of signed messages is wrong
      stats->Increment("handle_Abort_res_s",1);
      // Debug("signed packed msg: %s", string_to_hex(signedMsg.packed_msg()).c_str());
      // get the pending signed preprepare
      if (pendingSignedAborts.find(reqId) != pendingSignedAborts.end()) {
        Debug("Adding signed id to a set: %lu", signedMsg.replica_id());

        PendingSignedAbort* psq = &pendingSignedAborts[reqId];
        uint64_t add_id = signedMsg.replica_id();
        // make sure this id is actually in the group
        if (add_id / config.n == (uint64_t) group_idx) {
          if (abortReply.status() == REPLY_OK) {
            // add the decision to the list as proof
            psq->receivedValidSigs[add_id] = signedMsg.signature();
            // Debug("signature for %lu: %s", add_id, string_to_hex(signedMsg.signature()).c_str());
          } else {
            if(validate_abort){
              psq->receivedFailedSigs[add_id] = signedMsg.signature();
            }
            else{
              psq->receivedFailedIds.insert(add_id);
            }

          }
        }

        proto::GroupedSignedMessage groupSignedMsg;

        // once we have enough valid requests, construct the grouped decision
        // and return success
        if (psq->receivedValidSigs.size() >= (uint64_t) config.f + 1) {
          Debug("Got enough *valid* transaction decisions, executing callback");
          // set the packed decision

          //groupSignedMsg.set_packed_msg(psp->validDecisionPacked);
          groupSignedMsg.set_packed_msg(CreateValidAbortDecision());
          // Debug("packed decision: %s", string_to_hex(psp->validDecisionPacked).c_str());

          // add the signatures
          for (const auto& pair : psq->receivedValidSigs) {
            (*groupSignedMsg.mutable_signatures())[pair.first] = pair.second;
          }

          // invoke the callback with the signed grouped decision
          signed_abort_callback acb = psq->acb;
          if (psq->timeout != nullptr) {
            psq->timeout->Stop();
          }
          pendingSignedQueries.erase(reqId);
          ////
          // struct timeval tv;
          // gettimeofday(&tv, NULL);
          // uint64_t current_time = (tv.tv_sec*1000000+tv.tv_usec);  //in microseconds
          // total_elapsed += (current_time - start_time);
          // //std::cerr << "time measured: " << (current_time - start_time) << std::endl;;
          // total_prepare++;
          // if(total_prepare == 50) std::cerr << "Average time to prepare: " << (total_elapsed / total_prepare) << " us" << std::endl;
          //std::cerr << "Elapsed time for Prepare Phase: " << (current_time - start_time) << std::endl;
          ////
          acb(REPLY_OK, groupSignedMsg);
          return;
        }
        else if(validate_abort && psq->receivedFailedSigs.size() >= (uint64_t) config.f + 1 ){
          Debug("Got enough *failed* transaction decisions, executing callback");
          // set the packed decision

          //groupSignedMsg.set_packed_msg(psp->validDecisionPacked);
          groupSignedMsg.set_packed_msg(CreateFailedAbortDecision());

          // add the signatures
          for (const auto& pair : psq->receivedFailedSigs) {
            (*groupSignedMsg.mutable_signatures())[pair.first] = pair.second;
          }

          // invoke the callback with the signed grouped decision
          signed_abort_callback acb = psq->acb;
          if (psq->timeout != nullptr) {
            psq->timeout->Stop();
          }
          pendingSignedAborts.erase(reqId);
          ////
          // struct timeval tv;
          // gettimeofday(&tv, NULL);
          // uint64_t current_time = (tv.tv_sec*1000000+tv.tv_usec);  //in microseconds
          // total_elapsed += (current_time - start_time);
          // //std::cerr << "time measured: " << (current_time - start_time) << std::endl;
          // total_prepare++;
          // if(total_prepare == 50) std::cerr << "Average time to prepare: " << (total_elapsed / total_prepare) << " us" << std::endl;
          // //std::cerr << "Elapsed time for Prepare Phase: " << (current_time - start_time) << std::endl;
          ////
          acb(REPLY_FAIL, groupSignedMsg);
          return;
        }
        // if we get f+1 failures, we can return early
        else if (!validate_abort && psq->receivedFailedIds.size() >= (uint64_t) config.f + 1) {
          Debug("Not enough valid query decisions, failing");
          signed_abort_callback acb = psq->acb;
          if (psq->timeout != nullptr) {
            psq->timeout->Stop();
          }
          pendingSignedAborts.erase(reqId);
          // adding sigs to the grouped signed msg would be worthless
          acb(REPLY_FAIL, groupSignedMsg);
          return;
        }
      }
    } else { 
      stats->Increment("handle_abort_reply",1);
      if (pendingAborts.find(reqId) != pendingAborts.end()) {
        PendingAbort* pq = &pendingAborts[reqId];
        if (abortReply.status() == REPLY_OK) {
          uint64_t add_id = pq->receivedOkIds.size(); //  I do the below because they do this
          // If the ids are to be confirmed distinct, this doesn't work
          // add the decision to the list as proof
          pq->receivedOkIds.insert(add_id);
        } else {
          // Kinda jank but just don't use these ids
          uint64_t add_id = pq->receivedFailedIds.size();
          pq->receivedFailedIds.insert(add_id);
        }

        // once we have enough valid requests, construct the grouped decision
        // and return success
        if (pq->receivedOkIds.size() >= (uint64_t) config.f + 1) {
          proto::AbortReply validAbortRes = pq->validDecision;
          // invoke the callback if we have enough of the same decision
          abort_callback acb = pq->acb;
          if (pq->timeout != nullptr) {
            pq->timeout->Stop();
          }
          pendingAborts.erase(reqId);
          acb(REPLY_OK, validAbortRes);
          return;
        }
        // f+1 failures mean that we will always return fail
        if (pq->receivedFailedIds.size() >= (uint64_t) config.f + 1) {
          proto::AbortReply failedAbortReply;
          failedAbortReply.set_status(REPLY_FAIL);
          abort_callback acb = pendingAborts[reqId].acb;
          if (pq->timeout != nullptr) {
            pq->timeout->Stop();
          }
          pendingAborts.erase(reqId);
          acb(REPLY_FAIL, failedAbortReply);
          return;
        }
      }
    }
  } else {
    stats->Increment("wrong_abort_shard",1);
  }
}

void ShardClient::HandleTransactionDecision(const proto::TransactionDecision& transactionDecision, const proto::SignedMessage& signedMsg) {
  Debug("Handling transaction decision");

  std::string digest = transactionDecision.txn_digest();
  DebugHash(digest);
  // only handle decisions for my shard
  // NOTE: makes the assumption that numshards == numgroups
  if (transactionDecision.shard_id() == (uint64_t) group_idx) {
    if (signMessages) {
      stats->Increment("handle_tx_dec_s",1);
      // Debug("signed packed msg: %s", string_to_hex(signedMsg.packed_msg()).c_str());
      // get the pending signed preprepare
      if (pendingSignedPrepares.find(digest) != pendingSignedPrepares.end()) {
        Debug("Adding signed id to a set: %lu", signedMsg.replica_id());

        PendingSignedPrepare* psp = &pendingSignedPrepares[digest];
        uint64_t add_id = signedMsg.replica_id();
        // make sure this id is actually in the group
        if (add_id / config.n == (uint64_t) group_idx) {
          if (transactionDecision.status() == REPLY_OK) {
            // add the decision to the list as proof
            psp->receivedValidSigs[add_id] = signedMsg.signature();
            // Debug("signature for %lu: %s", add_id, string_to_hex(signedMsg.signature()).c_str());
          } else {
            if(validate_abort){
              psp->receivedFailedSigs[add_id] = signedMsg.signature();
            }
            else{
              psp->receivedFailedIds.insert(add_id);
            }

          }
        }

        proto::GroupedSignedMessage groupSignedMsg;

        // once we have enough valid requests, construct the grouped decision
        // and return success
        if (psp->receivedValidSigs.size() >= (uint64_t) config.f + 1) {
          Debug("Got enough *valid* transaction decisions, executing callback");
          // set the packed decision

          //groupSignedMsg.set_packed_msg(psp->validDecisionPacked);
          groupSignedMsg.set_packed_msg(CreateValidPackedDecision(digest));
          // Debug("packed decision: %s", string_to_hex(psp->validDecisionPacked).c_str());

          // add the signatures
          for (const auto& pair : psp->receivedValidSigs) {
            (*groupSignedMsg.mutable_signatures())[pair.first] = pair.second;
          }

          // invoke the callback with the signed grouped decision
          signed_prepare_callback pcb = psp->pcb;
          if (psp->timeout != nullptr) {
            psp->timeout->Stop();
          }
          pendingSignedPrepares.erase(digest);
          ////
          // struct timeval tv;
          // gettimeofday(&tv, NULL);
          // uint64_t current_time = (tv.tv_sec*1000000+tv.tv_usec);  //in microseconds
          // total_elapsed += (current_time - start_time);
          // //std::cerr << "time measured: " << (current_time - start_time) << std::endl;;
          // total_prepare++;
          // if(total_prepare == 50) std::cerr << "Average time to prepare: " << (total_elapsed / total_prepare) << " us" << std::endl;
          //std::cerr << "Elapsed time for Prepare Phase: " << (current_time - start_time) << std::endl;
          ////
          pcb(REPLY_OK, groupSignedMsg);
          return;
        }
        else if(validate_abort && psp->receivedFailedSigs.size() >= (uint64_t) config.f + 1 ){
          Debug("Got enough *failed* transaction decisions, executing callback");
          // set the packed decision

          //groupSignedMsg.set_packed_msg(psp->validDecisionPacked);
          groupSignedMsg.set_packed_msg(CreateFailedPackedDecision(digest));

          // add the signatures
          for (const auto& pair : psp->receivedFailedSigs) {
            (*groupSignedMsg.mutable_signatures())[pair.first] = pair.second;
          }

          // invoke the callback with the signed grouped decision
          signed_prepare_callback pcb = psp->pcb;
          if (psp->timeout != nullptr) {
            psp->timeout->Stop();
          }
          pendingSignedPrepares.erase(digest);
          ////
          // struct timeval tv;
          // gettimeofday(&tv, NULL);
          // uint64_t current_time = (tv.tv_sec*1000000+tv.tv_usec);  //in microseconds
          // total_elapsed += (current_time - start_time);
          // //std::cerr << "time measured: " << (current_time - start_time) << std::endl;
          // total_prepare++;
          // if(total_prepare == 50) std::cerr << "Average time to prepare: " << (total_elapsed / total_prepare) << " us" << std::endl;
          // //std::cerr << "Elapsed time for Prepare Phase: " << (current_time - start_time) << std::endl;
          ////
          pcb(REPLY_FAIL, groupSignedMsg);
          return;
        }
        // if we get f+1 failures, we can return early
        else if (!validate_abort && psp->receivedFailedIds.size() >= (uint64_t) config.f + 1) {
          Debug("Not enough valid txn decisions, failing");
          signed_prepare_callback pcb = psp->pcb;
          if (psp->timeout != nullptr) {
            psp->timeout->Stop();
          }
          pendingSignedPrepares.erase(digest);
          // adding sigs to the grouped signed msg would be worthless
          pcb(REPLY_FAIL, groupSignedMsg);
          return;
        }
      }
    } else {
      stats->Increment("handle_tx_dec",1);
      if (pendingPrepares.find(digest) != pendingPrepares.end()) {
        PendingPrepare* pp = &pendingPrepares[digest];
        if (transactionDecision.status() == REPLY_OK) {
          uint64_t add_id = pp->receivedOkIds.size();
          // add the decision to the list as proof
          pp->receivedOkIds.insert(add_id);
        } else {
          // Kinda jank but just don't use these ids
          uint64_t add_id = pp->receivedFailedIds.size();
          pp->receivedFailedIds.insert(add_id);
        }

        // once we have enough valid requests, construct the grouped decision
        // and return success
        if (pp->receivedOkIds.size() >= (uint64_t) config.f + 1) {
          proto::TransactionDecision validDecision = pp->validDecision;
          // invoke the callback if we have enough of the same decision
          prepare_callback pcb = pp->pcb;
          if (pp->timeout != nullptr) {
            pp->timeout->Stop();
          }
          pendingPrepares.erase(digest);
          pcb(REPLY_OK, validDecision);
          return;
        }
        // f+1 failures mean that we will always return fail
        if (pp->receivedFailedIds.size() >= (uint64_t) config.f + 1) {
          proto::TransactionDecision failedDecision;
          failedDecision.set_status(REPLY_FAIL);
          prepare_callback pcb = pendingPrepares[digest].pcb;
          if (pp->timeout != nullptr) {
            pp->timeout->Stop();
          }
          pendingPrepares.erase(digest);
          pcb(REPLY_FAIL, failedDecision);
          return;
        }
      }
    }
  } else {
    stats->Increment("wrong_dec_shard",1);
  }
}

//deprecated
void ShardClient::HandleWritebackReply(const proto::GroupedDecisionAck& groupedDecisionAck, const proto::SignedMessage& signedMsg) {
  Debug("Handling Writeback reply");

  std::string digest = groupedDecisionAck.txn_digest();
  DebugHash(digest);
  if (pendingWritebacks.find(digest) != pendingWritebacks.end()) {
    PendingWritebackReply* pw = &pendingWritebacks[digest];

    uint64_t replica_id = pw->receivedAcks.size() + pw->receivedFails.size();
    if (signMessages) {
      replica_id = signedMsg.replica_id();
    }

    if (groupedDecisionAck.status() == REPLY_OK) {
      Debug("got a decision ack from %lu", replica_id);
      pw->receivedAcks.insert(replica_id);
    } else {
      Debug("got a decision failure from %lu", replica_id);
      pw->receivedFails.insert(replica_id);
    }

    // 2f+1 because we want a quorum of honest users to acknowledge (for fault tolerance)
    if (pw->receivedAcks.size() >= (uint64_t) 2*config.f + 1) {
      Debug("Got enough writeback acks");
      // if the list has enough replicas, we can invoke the callback
      writeback_callback wcb = pendingWritebacks[digest].wcb;
      if (pendingWritebacks[digest].timeout != nullptr) {
        pendingWritebacks[digest].timeout->Stop();
      }
      pendingWritebacks.erase(digest);
      wcb(REPLY_OK);
      return;
    }

    // once we get f + 1 fails, impossible to get 2f+1 succeeds
    if (pw->receivedFails.size() >= (uint64_t) config.f + 1) {
      Debug("Unable to get enough writeback acks, failing");
      writeback_callback wcb = pendingWritebacks[digest].wcb;
      if (pendingWritebacks[digest].timeout != nullptr) {
        pendingWritebacks[digest].timeout->Stop();
      }
      pendingWritebacks.erase(digest);
      wcb(REPLY_FAIL);
    }
  }
}

// ================================
// ==== SHARD CLIENT INTERFACE ====
// ================================

void ShardClient::Query(const std::string &query, const Timestamp &ts,
    uint64_t client_id, uint64_t txn_seq_num, query_callback qcb, query_timeout_callback qtcb,
    uint32_t timeout) {

	proto::SQLMessage msg;
  proto::Request request;
  uint64_t reqId = queryReq++;
  // DebugHash(digest);
  // msg.set_digest(digest);
  msg.set_req_id(reqId);
  msg.set_client_id(client_id);
  msg.set_txn_seq_num(txn_seq_num);
  // msg.mutable_packed_msg()->set_msg(query);
  // msg.mutable_packed_msg()->set_type(msg.GetTypeName());
  msg.set_msg(query);

  request.mutable_packed_msg()->set_msg(msg.SerializeAsString());
  request.mutable_packed_msg()->set_type(msg.GetTypeName());

  request.set_client_id(client_id);
  request.set_tx_seq_num(txn_seq_num);


  transport->SendMessageToGroup(this, group_idx, request);

  PendingQuery pq;
  pq.qcb = qcb;
  // pq.numResultsRequired = numResults;
  pq.status = REPLY_FAIL;
  // every ts should be bigger than this one
  pq.maxTs = Timestamp();
  pq.timeout = new Timeout(transport, timeout, [this, reqId, qtcb]() {
    Debug("Get timeout called (but nothing was done)");
      stats->Increment("g_tout", 1);
      // fprintf(stderr,"g_tout recv %lu\n",  this->pendingQueries[query].numResultsRequired);
      fprintf(stderr,"g_tout recv ");
      // for (const auto& recq : this->pendingQueries[query].receivedReplies) {
      //   fprintf(stderr,"%lu\n", recq);
      // }
    // this->pendingReads.erase(reqId);
    // gtcb(reqId, key);
  });
  pq.timeout->Start();
  // pr.timeout = nullptr;

  pendingQueries[reqId] = pq;
}

// Get the value corresponding to key.
void ShardClient::Get(const std::string &key, const Timestamp &ts,
    uint64_t readMessages, uint64_t numResults, read_callback gcb, read_timeout_callback gtcb,
    uint32_t timeout) {
  Debug("Client get for %s", key.c_str());


  // Transform to SQL here
  // Maybe SELECT * FROM db WHERE key=?
  proto::Read read;
  uint64_t reqId = readReq++;
  Debug("Get id: %lu", reqId);
  read.set_req_id(reqId);
  read.set_key(key);
  ts.serialize(read.mutable_timestamp());

  
  UW_ASSERT(readMessages <= closestReplicas.size());
  for (size_t i = 0; i < readMessages; ++i) {
    Debug("[group %i] Sending GET to replica %lu", group_idx, GetNthClosestReplica(i));
    transport->SendMessageToReplica(this, group_idx, GetNthClosestReplica(i), read);
  }
  // transport->SendMessageToGroup(this, group_idx, read);

  
  PendingRead pr;
  pr.rcb = gcb;
  pr.numResultsRequired = numResults;
  pr.status = REPLY_FAIL;
  // every ts should be bigger than this one
  pr.maxTs = Timestamp();
  pr.timeout = new Timeout(transport, timeout, [this, reqId, gtcb]() {
    Debug("Get timeout called (but nothing was done)");
      stats->Increment("g_tout", 1);
      fprintf(stderr,"g_tout recv %lu\n",  this->pendingReads[reqId].numResultsRequired);
      for (const auto& recv : this->pendingReads[reqId].receivedReplies) {
        fprintf(stderr,"%lu\n", recv);
      }
    // this->pendingReads.erase(reqId);
    // gtcb(reqId, key);
  });
  pr.timeout->Start();
  // pr.timeout = nullptr;

  pendingReads[reqId] = pr;

}

std::string ShardClient::CreateValidPackedDecision(std::string digest) {
  proto::TransactionDecision validDecision;
  validDecision.set_status(REPLY_OK);
  validDecision.set_txn_digest(digest);
  validDecision.set_shard_id(group_idx);

  proto::PackedMessage packedDecision;
  packedDecision.set_type(validDecision.GetTypeName());
  packedDecision.set_msg(validDecision.SerializeAsString());

  return packedDecision.SerializeAsString();
}

std::string ShardClient::CreateFailedPackedDecision(std::string digest) {
  proto::TransactionDecision validDecision;
  validDecision.set_status(REPLY_FAIL);
  validDecision.set_txn_digest(digest);
  validDecision.set_shard_id(group_idx);

  proto::PackedMessage packedDecision;
  packedDecision.set_type(validDecision.GetTypeName());
  packedDecision.set_msg(validDecision.SerializeAsString());

  return packedDecision.SerializeAsString();
}

std::string ShardClient::CreateValidQueryDecision(SQLResult digest) {
  proto::QueryReply validDecision;
  validDecision.set_status(REPLY_OK);
  validDecision.set_sql_res(digest);
  validDecision.set_shard_id(group_idx);

  proto::PackedMessage packedDecision;
  packedDecision.set_type(validDecision.GetTypeName());
  packedDecision.set_msg(validDecision.SerializeAsString());

  return packedDecision.SerializeAsString();
}

std::string ShardClient::CreateFailedQueryDecision(SQLResult digest) {
  proto::QueryReply validDecision;
  validDecision.set_status(REPLY_FAIL);
  validDecision.set_sql_res(digest);
  validDecision.set_shard_id(group_idx);

  proto::PackedMessage packedDecision;
  packedDecision.set_type(validDecision.GetTypeName());
  packedDecision.set_msg(validDecision.SerializeAsString());

  return packedDecision.SerializeAsString();
}

std::string ShardClient::CreateValidCommitDecision() {
  proto::CommitReply validDecision;
  validDecision.set_status(REPLY_OK);
  validDecision.set_shard_id(group_idx);

  proto::PackedMessage packedDecision;
  packedDecision.set_type(validDecision.GetTypeName());
  packedDecision.set_msg(validDecision.SerializeAsString());

  return packedDecision.SerializeAsString();
}

std::string ShardClient::CreateFailedCommitDecision() {
  proto::CommitReply validDecision;
  validDecision.set_status(REPLY_FAIL);
  validDecision.set_shard_id(group_idx);

  proto::PackedMessage packedDecision;
  packedDecision.set_type(validDecision.GetTypeName());
  packedDecision.set_msg(validDecision.SerializeAsString());

  return packedDecision.SerializeAsString();
}

std::string ShardClient::CreateValidAbortDecision() {
  proto::AbortReply validDecision;
  validDecision.set_status(REPLY_OK);
  validDecision.set_shard_id(group_idx);

  proto::PackedMessage packedDecision;
  packedDecision.set_type(validDecision.GetTypeName());
  packedDecision.set_msg(validDecision.SerializeAsString());

  return packedDecision.SerializeAsString();
}

std::string ShardClient::CreateFailedAbortDecision() {
  proto::AbortReply validDecision;
  validDecision.set_status(REPLY_FAIL);
  validDecision.set_shard_id(group_idx);

  proto::PackedMessage packedDecision;
  packedDecision.set_type(validDecision.GetTypeName());
  packedDecision.set_msg(validDecision.SerializeAsString());

  return packedDecision.SerializeAsString();
}

// send a request with this as the packed message
void ShardClient::Prepare(const proto::Transaction& txn, prepare_callback pcb,
    prepare_timeout_callback ptcb, uint32_t timeout) {
  Debug("Handling client prepare");

  std::string digest = TransactionDigest(txn);
  if (pendingPrepares.find(digest) == pendingPrepares.end()) {
    stats->Increment("shard_prepare",1);
    proto::Request request;
    DebugHash(digest);
    request.set_digest(digest);
    request.mutable_packed_msg()->set_msg(txn.SerializeAsString());
    request.mutable_packed_msg()->set_type(txn.GetTypeName());

    Debug("Sending txn to all replicas in shard");
    transport->SendMessageToGroup(this, group_idx, request);

    PendingPrepare pp;
    pp.pcb = pcb;
    proto::TransactionDecision validDecision;
    validDecision.set_status(REPLY_OK);
    validDecision.set_txn_digest(digest);
    validDecision.set_shard_id(group_idx);
    // this is what the tx decisions should look like for valid replies
    pp.validDecision = validDecision;
    pp.timeout = new Timeout(transport, timeout, [this, digest, ptcb]() {
      Debug("Prepare timeout called (but nothing was done)");
      stats->Increment("p_tout", 1);
      fprintf(stderr,"p_tout recv %d\n", group_idx);
      fprintf(stderr,"ack\n");
      for (const auto& recv : this->pendingPrepares[digest].receivedOkIds) {
        fprintf(stderr,"%lu\n", recv);
      }
      fprintf(stderr,"nak:\n");
      for (const auto& recv : this->pendingPrepares[digest].receivedFailedIds) {
        fprintf(stderr,"%lu\n", recv);
      }
      // this->pendingPrepares.erase(digest);
      // ptcb(REPLY_FAIL);
    });
    pp.timeout->Start();
    // pp.timeout = nullptr;

    pendingPrepares[digest] = pp;

  } else {
    Debug("prepare called on already prepared tx");
  }
}

void ShardClient::SignedPrepare(const proto::Transaction& txn, signed_prepare_callback pcb,
    prepare_timeout_callback ptcb, uint32_t timeout) {
  Debug("Handling client signed prepare");
  std::string digest = TransactionDigest(txn);
  if (pendingSignedPrepares.find(digest) == pendingSignedPrepares.end()) {
    proto::Request request;
    request.set_digest(digest);
    request.mutable_packed_msg()->set_msg(txn.SerializeAsString());
    request.mutable_packed_msg()->set_type(txn.GetTypeName());
    stats->Increment("shard_prepare_s",1);

    Debug("Sending txn to all replicas in shard");
    transport->SendMessageToGroup(this, group_idx, request);

    // struct timeval tv;
    // gettimeofday(&tv, NULL);
    // start_time = (tv.tv_sec*1000000+tv.tv_usec);  //in microseconds

    PendingSignedPrepare psp;
    psp.pcb = pcb;
    // this is what the tx decisions should look like for valid replies

    //psp.validDecisionPacked = CreateValidPackedDecision(digest);  //XXX do this work only later..
    psp.timeout = new Timeout(transport, timeout, [this, digest, ptcb]() {
      Debug("Prepare signed timeout called (but nothing was done)");
      stats->Increment("ps_tout", 1);
      fprintf(stderr,"ps_tout recv %d\n", group_idx);
      fprintf(stderr,"ack\n");
      for (const auto& recv : this->pendingSignedPrepares[digest].receivedValidSigs) {
        fprintf(stderr,"%lu\n", recv.first);
      }
      fprintf(stderr,"nak:\n");
      for (const auto& recv : this->pendingSignedPrepares[digest].receivedFailedIds) {
        fprintf(stderr,"%lu\n", recv);
      }
      // this->pendingSignedPrepares.erase(digest);
      // ptcb(REPLY_FAIL);
    });
    psp.timeout->Start();
    // psp.timeout = nullptr;

    pendingSignedPrepares[digest] = psp;

  } else {
    Debug("prepare signed called on already prepared tx");
  }
}

// void ShardClient::Commit(const std::string& txn_digest, const proto::ShardDecisions& dec,
//     writeback_callback wcb, writeback_timeout_callback wtcp, uint32_t timeout) {
//   Debug("Handling client commit");
//   if (pendingWritebacks.find(txn_digest) == pendingWritebacks.end()) {
//     proto::GroupedDecision groupedDecision;
//     groupedDecision.set_status(REPLY_OK);
//     groupedDecision.set_txn_digest(txn_digest);
//     *groupedDecision.mutable_decisions() = dec;
//     stats->Increment("shard_commit", 1);

//     Debug("Sending commit to all replicas in shard");
//     transport->SendMessageToGroup(this, group_idx, groupedDecision);

//     PendingWritebackReply pwr;
//     pwr.wcb = wcb;
//     pwr.timeout = new Timeout(transport, timeout, [this, txn_digest, wtcp]() {
//       Debug("Writeback timeout called (but nothing was done)");
//       stats->Increment("c_tout", 1);
//       fprintf(stderr,"c_tout recv %d\n", group_idx);
//       fprintf(stderr, "txn: %s\n", txn_digest.c_str());
//       fprintf(stderr,"ack\n");
//       for (const auto& recv : this->pendingWritebacks[txn_digest].receivedAcks) {
//         fprintf(stderr,"%lu\n", recv);
//       }
//       fprintf(stderr,"nak:\n");
//       for (const auto& recv : this->pendingWritebacks[txn_digest].receivedFails) {
//         fprintf(stderr,"%lu\n", recv);
//       }

//       // this->pendingWritebacks.erase(digest);
//       // wtcp(REPLY_FAIL);
//     });
//     // pwr.timeout->Start();
//     // pwr.timeout = nullptr;

//     pendingWritebacks[txn_digest] = pwr;

//   } else {
//     Debug("commit called on already committed tx");
//   }
// }

void ShardClient::Abort(
    uint64_t client_id, uint64_t txn_seq_num, abort_callback acb, abort_timeout_callback atcb,
    uint32_t timeout) {
  Debug("Handling client abort");
	proto::AbortMessage msg;
  proto::Request request;
  // DebugHash(digest);
  // msg.set_digest(digest);
  msg.set_client_id(client_id);
  msg.set_txn_seq_num(txn_seq_num);
  uint64_t reqId = abortReq++;
  // DebugHash(digest);
  // msg.set_digest(digest);
  msg.set_req_id(reqId);
  // msg.mutable_packed_msg()->set_msg(query);
  // msg.mutable_packed_msg()->set_type(msg.GetTypeName());

  request.mutable_packed_msg()->set_msg(msg.SerializeAsString());
  request.mutable_packed_msg()->set_type(msg.GetTypeName());

  request.set_client_id(client_id);
  request.set_tx_seq_num(txn_seq_num);

  transport->SendMessageToGroup(this, group_idx, request);


  PendingAbort pa;
  pa.acb = acb;
  // pa.numResultsRequired = numResults;
  pa.status = REPLY_FAIL;
  // every ts should be bigger than this one
  pa.maxTs = Timestamp();
  pa.timeout = new Timeout(transport, timeout, [this, reqId, atcb]() {
    Debug("Get timeout called (but nothing was done)");
    //   stats->Increment("g_tout", 1);
    //   fprintf(stderr,"g_tout recv %lu\n",  this->pendingCommits[query].numResultsRequired);
    //   for (const auto& recq : this->pendingQueries[query].receivedReplies) {
    //     fprintf(stderr,"%lu\n", recq);
    //   }
    // this->pendingReads.erase(reqId);
    // qtcb(reqId, key);
  });
  pa.timeout->Start();
  // pr.timeout = nullptr;

  pendingAborts[reqId] = pa;
}


void ShardClient::Commit(
    uint64_t client_id, uint64_t txn_seq_num, commit_callback ccb, commit_timeout_callback ctcb,
    uint32_t timeout) {
  Debug("Handling client commit");
	proto::CommitMessage msg;
  proto::Request request;
  // DebugHash(digest);
  // msg.set_digest(digest);
  msg.set_client_id(client_id);
  msg.set_txn_seq_num(txn_seq_num);
  uint64_t reqId = commitReq++;
  msg.set_req_id(reqId);
  // msg.mutable_packed_msg()->set_msg(query);
  // msg.mutable_packed_msg()->set_type(msg.GetTypeName());

  request.mutable_packed_msg()->set_msg(msg.SerializeAsString());
  request.mutable_packed_msg()->set_type(msg.GetTypeName());

  request.set_client_id(client_id);
  request.set_tx_seq_num(txn_seq_num);

  transport->SendMessageToGroup(this, group_idx, request);

  PendingCommit pc;
  pc.ccb = ccb;
  // pc.numResultsRequired = numResults;
  pc.status = REPLY_FAIL;
  // every ts should be bigger than this one
  pc.maxTs = Timestamp();
  pc.timeout = new Timeout(transport, timeout, [this, reqId, ctcb]() {
    Debug("Get timeout called (but nothing was done)");
    //   stats->Increment("g_tout", 1);
    //   fprintf(stderr,"g_tout recv %lu\n",  this->pendingCommits[query].numResultsRequired);
    //   for (const auto& recq : this->pendingQueries[query].receivedReplies) {
    //     fprintf(stderr,"%lu\n", recq);
    //   }
    // this->pendingReads.erase(reqId);
    // qtcb(reqId, key);
  });
  pc.timeout->Start();
  // pr.timeout = nullptr;

  pendingCommits[reqId] = pc; // Changer everywhere
}

//TODO: add flag, and wrap Commit in a Request in that case. In doing so, it will automatically be ordered.
// THEN: make sure to adapt Execute to also handle Commits.
void ShardClient::CommitSigned(const std::string& txn_digest, const proto::ShardSignedDecisions& dec,
    writeback_callback wcb, writeback_timeout_callback wtcp, uint32_t timeout) {
  Debug("Handling client commit signed");
  if (pendingWritebacks.find(txn_digest) == pendingWritebacks.end()) {
    proto::GroupedDecision groupedDecision;
    groupedDecision.set_status(REPLY_OK);
    groupedDecision.set_txn_digest(txn_digest);
    *groupedDecision.mutable_signed_decisions() = dec;
    stats->Increment("shard_commit_s", 1);

    Debug("Sending commit to all replicas in shard");

    if(order_commit){
      proto::Request request;
      request.set_digest(crypto::Hash(groupedDecision.SerializeAsString()));
      request.mutable_packed_msg()->set_msg(groupedDecision.SerializeAsString());
      request.mutable_packed_msg()->set_type(groupedDecision.GetTypeName());

      transport->SendMessageToGroup(this, group_idx, request);
    }
    else{
      transport->SendMessageToGroup(this, group_idx, groupedDecision);
    }

    PendingWritebackReply pwr;
    pwr.wcb = wcb;
    pwr.timeout = new Timeout(transport, timeout, [this, txn_digest, wtcp]() {
      Debug("Writeback signed timeout called (but nothing was done)");
      stats->Increment("cs_tout", 1);
      fprintf(stderr,"cs_tout recv %d\n", group_idx);
      fprintf(stderr,"ack\n");
      for (const auto& recv : this->pendingWritebacks[txn_digest].receivedAcks) {
        fprintf(stderr,"%lu\n", recv);
      }
      fprintf(stderr,"nak:\n");
      for (const auto& recv : this->pendingWritebacks[txn_digest].receivedFails) {
        fprintf(stderr,"%lu\n", recv);
      }
      // this->pendingWritebacks.erase(digest);
      // wtcp(REPLY_FAIL);
    });
    // pwr.timeout->Start();
    // pwr.timeout = nullptr;

    pendingWritebacks[txn_digest] = pwr;

    // TODO timeout
  } else {
    Debug("commit signed called on already committed tx");
  }
}

void ShardClient::CommitSigned(const std::string& txn_digest, const proto::ShardSignedDecisions& dec) {
  Debug("Handling client commit signed");
  if (pendingWritebacks.find(txn_digest) == pendingWritebacks.end()) {
    proto::GroupedDecision groupedDecision;
    groupedDecision.set_status(REPLY_OK);
    groupedDecision.set_txn_digest(txn_digest);
    *groupedDecision.mutable_signed_decisions() = dec;
    stats->Increment("shard_commit_s", 1);

    Debug("Sending commit to all replicas in shard");

    if(order_commit){
      proto::Request request;
      request.set_digest(crypto::Hash(groupedDecision.SerializeAsString()));
      request.mutable_packed_msg()->set_msg(groupedDecision.SerializeAsString());
      request.mutable_packed_msg()->set_type(groupedDecision.GetTypeName());

      transport->SendMessageToGroup(this, group_idx, request);
    }
    else{
      transport->SendMessageToGroup(this, group_idx, groupedDecision);
    }

    // TODO timeout
  } else {
    Debug("commit signed called on already committed tx");
  }
}

// void ShardClient::Abort(std::string& txn_digest, const proto::ShardSignedDecisions& dec) {
//   Debug("Handling client abort");
//   // TODO should techincally include a proof
//   if (pendingWritebacks.find(txn_digest) == pendingWritebacks.end()) {
//     proto::GroupedDecision groupedDecision;
//     groupedDecision.set_status(REPLY_FAIL);
//     groupedDecision.set_txn_digest(txn_digest);

//     if(validate_abort){

//         *groupedDecision.mutable_signed_decisions() = dec;
//     }
//     else{
//       proto::ShardDecisions sd;
//       *groupedDecision.mutable_decisions() = sd;
//     }

//     proto::Request request;
//     request.set_digest(crypto::Hash(groupedDecision.SerializeAsString()));
//     request.mutable_packed_msg()->set_msg(groupedDecision.SerializeAsString());
//     request.mutable_packed_msg()->set_type(groupedDecision.GetTypeName());

//     stats->Increment("shard_abort", 1);
//     Debug("AB abort to all replicas in shard");
//     transport->SendMessageToGroup(this, group_idx, request);

//     PendingWritebackReply pwr;
//     pendingWritebacks[txn_digest] = pwr;  //not sure what use this has

//   } else {
//     Debug("abort called on already aborted tx");
//   }
// }

}
