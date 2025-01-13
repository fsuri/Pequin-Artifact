// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/sintr/client2client.cc:
 *   Sintr client to client.
 *
 * Copyright 2024 Austin Li <atl63@cornell.edu>
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

#include "store/sintrstore/client2client.h"
#include "store/sintrstore/basicverifier.h"
#include "store/sintrstore/validation_client.h"
#include "store/common/frontend/validation_transaction.h"
#include "store/benchmark/async/tpcc/tpcc-validation-proto.pb.h"
#include "store/sintrstore/common.h"

#include <google/protobuf/util/message_differencer.h>
#include <sched.h>
#include <pthread.h>

namespace sintrstore {

Client2Client::Client2Client(transport::Configuration *config, transport::Configuration *clients_config, Transport *transport,
      uint64_t client_id, uint64_t nshards, uint64_t ngroups, int group, bool pingClients,
      Parameters params, KeyManager *keyManager, Verifier *verifier,
      Partitioner *part, EndorsementClient *endorseClient) :
      PingInitiator(this, transport, clients_config->n),
      client_id(client_id), transport(transport), config(config), clients_config(clients_config), 
      nshards(nshards), ngroups(ngroups),
      group(group), part(part), pingClients(pingClients), params(params),
      keyManager(keyManager), verifier(verifier), endorseClient(endorseClient) {
  
  // separate verifier from main client instance
  clients_verifier = new BasicVerifier(transport);

  valClient = new ValidationClient(transport, client_id, nshards, ngroups, part); 
  valParseClient = new ValidationParseClient(10000); // TODO: pass arg for timeout length
  transport->Register(this, *clients_config, group, client_id); 

  // assume these are somehow secretly shared before hand
  uint64_t idx = client_id;
  for (uint64_t i = 0; i < clients_config->n; i++) {
    if (i > idx) {
      sessionKeys[i] = std::string(8, (char) idx + 0x30) + std::string(8, (char) i + 0x30);
    } else {
      sessionKeys[i] = std::string(8, (char) i + 0x30) + std::string(8, (char) idx + 0x30);
    }
  }

  for (size_t i = 0; i < params.sintr_params.maxValThreads; i++) {
    valThreads.push_back(new std::thread(&Client2Client::ValidationThreadFunction, this));
    // // set cpu affinity
    // cpu_set_t cpuset;
    // CPU_ZERO(&cpuset);
    // int num_cpus = std::thread::hardware_concurrency();
    // CPU_SET(i % num_cpus, &cpuset);
    // pthread_setaffinity_np(valThreads[i]->native_handle(), sizeof(cpu_set_t), &cpuset);
  }
}

Client2Client::~Client2Client() {
  for (auto t : valThreads) {
    t->join();
  }
  delete valClient;
  delete clients_verifier;
  delete valParseClient;
}

void Client2Client::ReceiveMessage(const TransportAddress &remote,
      const std::string &type, const std::string &data, void *meta_data) {

  if (type == ping.GetTypeName()) {
    Debug("ping received");
    ping.ParseFromString(data);
    HandlePingResponse(ping);
  }
  else if (type == beginValTxnMsg.GetTypeName()) {
    beginValTxnMsg.ParseFromString(data);
    HandleBeginValidateTxnMessage(remote, beginValTxnMsg);
  }
  else if (type == fwdReadResultMsg.GetTypeName()) {
    fwdReadResultMsg.ParseFromString(data);
    HandleForwardReadResultMessage(fwdReadResultMsg);
  }
  else if (type == finishValTxnMsg.GetTypeName()) {
    finishValTxnMsg.ParseFromString(data);
    HandleFinishValidateTxnMessage(finishValTxnMsg);
  }
  else {
    Panic("Received unexpected message type: %s", type.c_str());
  }
}

bool Client2Client::SendPing(size_t replica, const PingMessage &ping) {
  // do not ping self
  if (replica != client_id) {
    transport->SendMessageToReplica(this, group, replica, ping);
  }
  return true;
}

void Client2Client::SendBeginValidateTxnMessage(uint64_t client_seq_num, const std::string &txnState, uint64_t txnStartTime) {
  this->client_seq_num = client_seq_num;

  sentBeginValTxnMsg = proto::BeginValidateTxnMessage();
  sentBeginValTxnMsg.set_client_id(client_id);
  sentBeginValTxnMsg.set_client_seq_num(client_seq_num);
  TxnState *protoTxnState = new TxnState();
  protoTxnState->ParseFromString(txnState);
  sentBeginValTxnMsg.set_allocated_txn_state(protoTxnState);
  sentBeginValTxnMsg.mutable_timestamp()->set_timestamp(txnStartTime);
  sentBeginValTxnMsg.mutable_timestamp()->set_id(client_id);

  beginValSent.clear();
  sentFwdReadResults.clear();

  Debug("beginValTxnMsg client id %lu, seq num %lu", client_id, client_seq_num);
  for (int i = 0; i < clients_config->n; i++) {
    // do not send to self
    if (i == client_id) {
      continue;
    }
    beginValSent.insert(i);
    transport->SendMessageToReplica(this, i, sentBeginValTxnMsg);
  }
}

void Client2Client::ForwardReadResultMessage(const std::string &key, const std::string &value, const Timestamp &ts,
    const proto::CommittedProof &proof, const std::string &serializedWrite, const std::string &serializedWriteTypeName, 
    const proto::Dependency &dep, bool hasDep, bool addReadset) {

  proto::ForwardReadResultMessage fwdReadResultMsg = proto::ForwardReadResultMessage();
  fwdReadResultMsg.set_client_id(client_id);
  fwdReadResultMsg.set_client_seq_num(client_seq_num);
  proto::ForwardReadResult fwdReadResult = proto::ForwardReadResult();
  fwdReadResult.set_key(key);
  fwdReadResult.set_value(value);
  fwdReadResult.mutable_timestamp()->set_timestamp(ts.getTimestamp());
  fwdReadResult.mutable_timestamp()->set_id(ts.getID());

  if (params.sintr_params.signFwdReadResults) {
    proto::SignedMessage signedMsg;
    CreateHMACedMessage(fwdReadResult, signedMsg);
    *fwdReadResultMsg.mutable_signed_fwd_read_result() = signedMsg;
  }
  else {
    *fwdReadResultMsg.mutable_fwd_read_result() = fwdReadResult;
  }

  // only if addReadset is true did this result come from server
  // otherwise it came from the buffer and there is no dependency or committed proof
  if (addReadset) {
    // this will contain the prepared txn dependency
    if (hasDep) {
      *fwdReadResultMsg.mutable_dep() = dep;
      // must be oneof write or signed write
      *fwdReadResultMsg.mutable_write() = proto::Write();
      UW_ASSERT(dep.IsInitialized());
    }
    else {
      if (params.validateProofs) {
        if (proof.IsInitialized()) {
          *fwdReadResultMsg.mutable_proof() = proof;
        }
        // if no proof then it is possible the value is empty
        else {
          UW_ASSERT(value.length() == 0);
        }
      }

      // depending on if signatures are enabled and if the value is non empty
      proto::SignedMessage signedWrite;
      proto::Write write;
      if (serializedWriteTypeName == signedWrite.GetTypeName()) {
        signedWrite.ParseFromString(serializedWrite);
        *fwdReadResultMsg.mutable_signed_write() = signedWrite;
      }
      else if (serializedWriteTypeName == write.GetTypeName()) {
        write.ParseFromString(serializedWrite);
        *fwdReadResultMsg.mutable_write() = write;
      }
      else {
        // this should only happen if value is empty
        UW_ASSERT(value.length() == 0);
        *fwdReadResultMsg.mutable_write() = write;
      }
    }
  }

  fwdReadResultMsg.set_add_readset(addReadset);

  sentFwdReadResults.push_back(fwdReadResultMsg);

  Debug(
    "ForwardReadResult: client id %lu, seq num %lu, key %s, value %s",
    client_id,
    client_seq_num,
    BytesToHex(key, 16).c_str(),
    BytesToHex(value, 16).c_str()
  );
  for (const auto &i : beginValSent) {
    transport->SendMessageToReplica(this, i, fwdReadResultMsg);
  }
}

void Client2Client::HandlePolicyUpdate(const Policy *policy) {
  UW_ASSERT(policy != nullptr);
  std::vector<int> diff = endorseClient->UpdateRequirement(policy);
  if (diff.size() > 0) {
    Debug("Initiating more beginValTxnMsg");
    // need to initiate more endorsements
    int numAdditional = diff.size();
    
    for (const auto &acl_client_id : diff) {
      // -1 represents a generic client id, so don't send to a specific client
      if (acl_client_id < 0) {
        continue;
      }
      auto ret = beginValSent.insert(acl_client_id);
      if (ret.second == false) {
        Panic("Client %lu already sent beginValTxnMsg to client %d", client_id, acl_client_id);
      }
      numAdditional--;
      transport->SendMessageToReplica(this, acl_client_id, sentBeginValTxnMsg);
      for (const auto &fwdReadResultMsg : sentFwdReadResults) {
        transport->SendMessageToReplica(this, acl_client_id, fwdReadResultMsg);
      }
    }

    int last_offset = 1;
    while (numAdditional > 0) {
      bool sent = false;
      for (; last_offset < clients_config->n; last_offset++) {
        // try to send to the next client after this client id
        uint64_t target = (this->client_id + last_offset) % clients_config->n;
        if (beginValSent.find(target) == beginValSent.end()) {
          beginValSent.insert(target);
          transport->SendMessageToReplica(this, target, sentBeginValTxnMsg);
          for (const auto &fwdReadResultMsg : sentFwdReadResults) {
            transport->SendMessageToReplica(this, target, fwdReadResultMsg);
          }
          sent = true;
          break;
        }
      }
      if (!sent) {
        Panic("Policy requires more endorsements than available clients");
      }
      numAdditional--;
    }
  }
  else {
    Debug("Received policy with weight %lu", policy->GetMinSatisfyingSet().size());
  }
}

void Client2Client::HandleBeginValidateTxnMessage(const TransportAddress &remote, 
    const proto::BeginValidateTxnMessage &beginValTxnMsg) {
  uint64_t curr_client_id = beginValTxnMsg.client_id();
  uint64_t curr_client_seq_num = beginValTxnMsg.client_seq_num();
  TxnState txnState = beginValTxnMsg.txn_state();
  Timestamp ts(beginValTxnMsg.timestamp());
  Debug(
    "HandleBeginValidateTxnMessage: from client id %lu, seq num %lu", 
    curr_client_id, 
    curr_client_seq_num
  );
  ValidationTransaction *valTxn = valParseClient->Parse(txnState);
  TransportAddress *remoteCopy = remote.clone();
  ValidationInfo *valInfo = new ValidationInfo(curr_client_id, curr_client_seq_num, ts, std::move(valTxn), std::move(remoteCopy));
  validationQueue.push(valInfo);
}

void Client2Client::HandleForwardReadResultMessage(const proto::ForwardReadResultMessage &fwdReadResultMsg) {
  uint64_t curr_client_id = fwdReadResultMsg.client_id();
  uint64_t curr_client_seq_num = fwdReadResultMsg.client_seq_num();
  proto::ForwardReadResult fwdReadResult;
  if (params.sintr_params.signFwdReadResults) {
    // first check client signature
    if (!fwdReadResultMsg.has_signed_fwd_read_result()) {
      Debug(
        "Missing client signature on forwarded read result from client id %lu, seq num %lu", 
        curr_client_id, 
        curr_client_seq_num
      );
      return;
    }
    std::string data;
    if (!ValidateHMACedMessage(fwdReadResultMsg.signed_fwd_read_result(), data)) {
      Debug(
        "Invalid client signature on forwarded read result from client id %lu, seq num %lu", 
        curr_client_id, 
        curr_client_seq_num
      );
      return;
    }
    fwdReadResult.ParseFromString(data);
  }
  else {
    fwdReadResult = fwdReadResultMsg.fwd_read_result();
  }

  std::string curr_key = fwdReadResult.key();
  std::string curr_value = fwdReadResult.value();

  proto::Write write;
  bool hasDep = fwdReadResultMsg.has_dep();
  proto::Dependency dep;
  bool addReadset = fwdReadResultMsg.add_readset();
  // only if addReadset is true will there be dep or committed proofs
  if (addReadset && params.sintr_params.clientCheckEvidence) {
    if (!CheckPreparedCommittedEvidence(fwdReadResultMsg, write, dep)) {
      return;
    }
    // if there is an actual value, expect matches
    if (curr_value.length() > 0) {
      UW_ASSERT(write.key() == curr_key);
      if (hasDep) {
        UW_ASSERT(write.prepared_value() == curr_value);
        UW_ASSERT(google::protobuf::util::MessageDifferencer::Equals(write.prepared_timestamp(), fwdReadResult.timestamp()));
      }
      else {
        UW_ASSERT(write.committed_value() == curr_value);
        UW_ASSERT(google::protobuf::util::MessageDifferencer::Equals(write.committed_timestamp(), fwdReadResult.timestamp()));
      }
    }
    // otherwise the write should be empty
    else {
      UW_ASSERT(!write.has_key());
    }

    // curr_key is essentially what the forwarding client is claiming is the key
    // write contains the server's claim as to what the key is
    // these two should match
    // also if value is empty, then no need to check since server makes no claims about it
    if (curr_value.length() > 0 && curr_key != write.key()) {
      Debug(
        "Mismatch in forwarded key and the server key: from client id %lu, seq num %lu, forwarded key %s, server key %s",
        curr_client_id, 
        curr_client_seq_num,
        BytesToHex(curr_key, 16).c_str(),
        BytesToHex(write.key(), 16).c_str()
      );
      return;
    }
  }

  Debug(
    "HandleForwardReadResult: from client id %lu, seq num %lu, key %s, value %s", 
    curr_client_id, 
    curr_client_seq_num,
    BytesToHex(curr_key, 16).c_str(),
    BytesToHex(curr_value, 16).c_str()
  );
  // tell valClient about this forwardedReadResult
  valClient->ProcessForwardReadResult(curr_client_id, curr_client_seq_num, fwdReadResult, dep, hasDep, addReadset);
}

void Client2Client::HandleFinishValidateTxnMessage(const proto::FinishValidateTxnMessage &finishValTxnMsg) {
  uint64_t peer_client_id = finishValTxnMsg.client_id();
  uint64_t val_txn_seq_num = finishValTxnMsg.validation_txn_seq_num();

  // stale finish validation message
  if (val_txn_seq_num != client_seq_num) {
    Debug(
      "Received stale finishValidateTxnMessage from client id %lu, seq num %lu; curr seq num %lu", 
      peer_client_id, 
      val_txn_seq_num,
      client_seq_num
    );
    return;
  }

  proto::SignedMessage signedMsg;
  std::string valTxnDigest;
  if (params.sintr_params.signFinishValidation) {
    // verify signature
    if (!finishValTxnMsg.has_signed_validation_txn_digest()) {
      Debug("Missing signed validation txn digest sent from client id %lu", peer_client_id);
      return;
    }
    signedMsg = finishValTxnMsg.signed_validation_txn_digest();
    if(!clients_verifier->Verify(keyManager->GetPublicKey(keyManager->GetClientKeyId(signedMsg.process_id())),
        signedMsg.data(), signedMsg.signature())) {
      Debug("Invalid signature on validation txn digest sent from client id %lu", peer_client_id);
      return;
    }
    valTxnDigest = signedMsg.data();
  }
  else {
    valTxnDigest = finishValTxnMsg.validation_txn_digest();
  }

  Debug("HandleFinishValidateTxnMessage: txn digest %s", BytesToHex(valTxnDigest, 16).c_str());

  if (params.sintr_params.debugEndorseCheck) {
    endorseClient->DebugCheck(finishValTxnMsg.val_txn());
  }

  endorseClient->AddValidation(peer_client_id, valTxnDigest, signedMsg);
}

bool Client2Client::CheckPreparedCommittedEvidence(const proto::ForwardReadResultMessage &fwdReadResultMsg, 
    proto::Write &write, proto::Dependency &dep) {
  uint64_t curr_client_id = fwdReadResultMsg.client_id();
  uint64_t curr_client_seq_num = fwdReadResultMsg.client_seq_num();

  // if has dependency, then this is based on a prepared txn
  if (fwdReadResultMsg.has_dep()) {
    if (params.validateProofs && params.signedMessages && params.verifyDeps) {
      if (!ValidateDependency(fwdReadResultMsg.dep(), config, params.readDepSize, 
          keyManager, verifier)) {
        Debug(
          "Invalid dependency on forwarded read result from client id %lu, seq num %lu",
          curr_client_id, 
          curr_client_seq_num
        );
        return false;
      }
    }
    dep = fwdReadResultMsg.dep();
    write = fwdReadResultMsg.dep().write();
  } 
  else {
    // otherwise can check committed proof and signature

    if (params.validateProofs && params.signedMessages) {
      // check server signature
      if (fwdReadResultMsg.has_signed_write()) {
        proto::SignedMessage signedWrite = fwdReadResultMsg.signed_write();
        if (!verifier->Verify(keyManager->GetPublicKey(signedWrite.process_id()),
            signedWrite.data(), signedWrite.signature())) {
          Debug(
            "Invalid server signature on forwarded read result from client id %lu, seq num %lu", 
            curr_client_id, 
            curr_client_seq_num
          );
          return false;
        }

        write.ParseFromString(signedWrite.data());
      }
      else {
        if (fwdReadResultMsg.has_write() && fwdReadResultMsg.write().has_committed_value()) {
          Debug(
            "Missing server signature on forwarded read result with committed value from client id %lu, seq num %lu", 
            curr_client_id, 
            curr_client_seq_num
          );
          return false;
        }

        write = fwdReadResultMsg.write();
      }
    }
    else {
      write = fwdReadResultMsg.write();
    }
      
    if (params.validateProofs) {
      // check committed proof
      if (write.has_committed_value() && write.has_committed_timestamp()) {
        if (!fwdReadResultMsg.has_proof()) {
          Debug(
            "Missing committed value proof for forwarded read result from client id %lu, seq num %lu",
            curr_client_id,
            curr_client_seq_num
          );
          return false;
        }
        
        std::string committedTxnDigest = TransactionDigest(fwdReadResultMsg.proof().txn(), params.hashDigest);
        if (!ValidateTransactionWrite(fwdReadResultMsg.proof(), &committedTxnDigest,
            write.key(), write.committed_value(), write.committed_timestamp(),
            config, params.signedMessages, keyManager, verifier)) {
          Debug(
            "Failed to validate committed value for forwarded read result from client id %lu, seq num %lu",
            curr_client_id,
            curr_client_seq_num
          );
          return false;
        }
      }
    }
  }

  return true;
}

void Client2Client::ValidationThreadFunction() {
  ::SyncClient syncClient(valClient);
  for(;;) {
    ValidationInfo *valInfo;
    validationQueue.pop(valInfo);
    uint64_t curr_client_id = valInfo->txn_client_id;
    uint64_t curr_client_seq_num = valInfo->txn_client_seq_num;
    Timestamp curr_ts = valInfo->txn_ts;
    ValidationTransaction *valTxn = valInfo->valTxn;
    Debug(
      "%lu will validate for client %lu, seq num %lu",
      std::this_thread::get_id(),
      curr_client_id,
      curr_client_seq_num
    );
    // std::cerr << std::this_thread::get_id() << " will validate for client " << curr_client_id 
    //           << ", seq num " << curr_client_seq_num << std::endl;

    valClient->SetThreadValTxnId(curr_client_id, curr_client_seq_num);
    valClient->SetTxnTimestamp(curr_client_id, curr_client_seq_num, curr_ts);

    transaction_status_t result = valTxn->Validate(syncClient);

    if (result == COMMITTED) {
      Debug("Completed validation for client id %lu, seq num %lu", curr_client_id, curr_client_seq_num);
      proto::Transaction *txn = valClient->GetCompletedTxn(curr_client_id, curr_client_seq_num);

      if (params.parallel_CCC) {
        std::sort(txn->mutable_read_set()->begin(), txn->mutable_read_set()->end(), sortReadSetByKey);
        std::sort(txn->mutable_write_set()->begin(), txn->mutable_write_set()->end(), sortWriteSetByKey);
      }
      std::sort(txn->mutable_involved_groups()->begin(), txn->mutable_involved_groups()->end());

      proto::FinishValidateTxnMessage finishValTxnMsg = proto::FinishValidateTxnMessage();
      finishValTxnMsg.set_client_id(client_id);
      finishValTxnMsg.set_validation_txn_seq_num(curr_client_seq_num);

      // only send over digest, not actual contents
      std::string digest = TransactionDigest(*txn, params.hashDigest);
      if (params.sintr_params.signFinishValidation) {
        // sign the digest
        proto::SignedMessage signedMessage;
        SignBytes(
          digest, 
          keyManager->GetPrivateKey(keyManager->GetClientKeyId(client_id)), 
          client_id, 
          &signedMessage
        );
        *finishValTxnMsg.mutable_signed_validation_txn_digest() = signedMessage;
      }
      else {
        finishValTxnMsg.set_validation_txn_digest(digest);
      }

      if (params.sintr_params.debugEndorseCheck) {
        *finishValTxnMsg.mutable_val_txn() = *txn;
      }

      transport->SendMessage(this, *valInfo->remote, finishValTxnMsg);
      Debug("transport->SendMessage complete");
      delete txn;
    }

    delete valInfo;
    Debug("thread exiting for validation for client id %lu, seq num %lu", curr_client_id, curr_client_seq_num);
  }
}

bool Client2Client::ValidateHMACedMessage(const proto::SignedMessage &signedMessage, std::string &data) {
  data = signedMessage.data();
  proto::HMACs hmacs;
  hmacs.ParseFromString(signedMessage.signature());
  return crypto::verifyHMAC(
    signedMessage.data(), 
    (*hmacs.mutable_hmacs())[client_id], 
    sessionKeys[signedMessage.process_id() % clients_config->n]
  );
}

void Client2Client::CreateHMACedMessage(const ::google::protobuf::Message &msg, proto::SignedMessage& signedMessage) {
  std::string msgData = msg.SerializeAsString();
  signedMessage.set_data(msgData);
  signedMessage.set_process_id(client_id);
  proto::HMACs hmacs;
  for (uint64_t i = 0; i < clients_config->n; i++) {
    (*hmacs.mutable_hmacs())[i] = crypto::HMAC(msgData, sessionKeys[i]);
  }
  signedMessage.set_signature(hmacs.SerializeAsString());
}

} // namespace sintrstore
