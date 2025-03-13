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
#include "store/sintrstore/common2.h"

#include <google/protobuf/util/message_differencer.h>
#include <sched.h>
#include <pthread.h>

namespace sintrstore {

Client2Client::Client2Client(transport::Configuration *config, transport::Configuration *clients_config, Transport *transport,
      uint64_t client_id, uint64_t nshards, uint64_t ngroups, int group, bool pingClients,
      Parameters params, KeyManager *keyManager, Verifier *verifier,
      Partitioner *part, EndorsementClient *endorseClient, SQLTransformer *sql_interpreter,
      const std::vector<std::string> &keys) :
      PingInitiator(this, transport, clients_config->n),
      client_id(client_id), transport(transport), config(config), clients_config(clients_config), 
      nshards(nshards), ngroups(ngroups),
      group(group), part(part), pingClients(pingClients), params(params),
      keyManager(keyManager), verifier(verifier), endorseClient(endorseClient), sql_interpreter(sql_interpreter),
      keys(keys) {
  
  // separate verifier from main client instance
  clients_verifier = new BasicVerifier(transport);

  valClient = new ValidationClient(transport, client_id, nshards, ngroups, part); 
  valParseClient = new ValidationParseClient(10000, keys); // TODO: pass arg for timeout length
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

  done = false;

  // for each client process, have 1 core for main client thread and maxValThreads for validation threads
  // if multi-threading message processing, need to reserve 1 more core per client
  // so each client process takes up a total of maxValThreads + (1 or 2) cores
  int num_cpus = std::thread::hardware_concurrency();
  int main_client_cpu;
  if (params.sintr_params.client2clientMultiThreading) {
    main_client_cpu = client_id * (params.sintr_params.maxValThreads + 2) % num_cpus;
  }
  else {
    main_client_cpu = client_id * (params.sintr_params.maxValThreads + 1) % num_cpus;
  }
  for (size_t i = 0; i < params.sintr_params.maxValThreads; i++) {
    valThreads.push_back(new std::thread(&Client2Client::ValidationThreadFunction, this));
    if (params.sintr_params.clientPinCores) {
      // set cpu affinity
      cpu_set_t cpuset;
      CPU_ZERO(&cpuset);      
      CPU_SET(main_client_cpu + i + 1 % num_cpus, &cpuset);
      pthread_setaffinity_np(valThreads[i]->native_handle(), sizeof(cpu_set_t), &cpuset);
    }
  }

  if (params.sintr_params.client2clientMultiThreading) {
    c2cThread = new std::thread(&Client2Client::Client2ClientMessageThreadFunction, this);
    if (params.sintr_params.clientPinCores) {
      // set cpu affinity
      cpu_set_t cpuset;
      CPU_ZERO(&cpuset);
      // try to pin to core following validation threads
      CPU_SET(main_client_cpu + params.sintr_params.maxValThreads + 1 % num_cpus, &cpuset);
      pthread_setaffinity_np(c2cThread->native_handle(), sizeof(cpu_set_t), &cpuset);
    }
  }
}

Client2Client::~Client2Client() {
  done = true;
  // send a dummy message to unblock any waiting threads before joining
  for (auto t : valThreads) {
    validationQueue.push(nullptr);
  }
  for (auto t : valThreads) {
    t->join();
    delete t;
  }
  if (params.sintr_params.client2clientMultiThreading) {
    c2cQueue.push(nullptr);
    c2cThread->join();
    delete c2cThread;
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
    ManageDispatchBeginValidateTxnMessage(remote, data);
  }
  else if (type == fwdReadResultMsg.GetTypeName()) {
    ManageDispatchForwardReadResultMessage(remote, data);
  }
  else if (type == fwdPointQueryResultMsg.GetTypeName()) {
    ManageDispatchForwardPointQueryResultMessage(remote, data);
  }
  else if (type == fwdQueryResultMsg.GetTypeName()) {
    ManageDispatchForwardQueryResultMessage(remote, data);
  }
  else if (type == finishValTxnMsg.GetTypeName()) {
    ManageDispatchFinishValidateTxnMessage(remote, data);
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

void Client2Client::SendBeginValidateTxnMessage(uint64_t client_seq_num, const TxnState &protoTxnState, uint64_t txnStartTime,
    PolicyClient *policyClient) {

  // if (create_hmac_ms.size() > 0 && create_hmac_ms.size() % 2000 == 0) {
  //   if (create_hmac_ms.size() > 0) {
  //     double mean_create_hmac_latency = std::accumulate(create_hmac_ms.begin(), create_hmac_ms.end(), 0.0) / create_hmac_ms.size();
  //     std::cerr << "Mean create HMAC latency: " << mean_create_hmac_latency << std::endl;
  //   }
  //   if (verify_endorse_ms.size() > 0) {
  //     double mean_verify_endorse_latency = std::accumulate(verify_endorse_ms.begin(), verify_endorse_ms.end(), 0.0) / verify_endorse_ms.size();
  //     std::cerr << "Mean verify endorsement latency: " << mean_verify_endorse_latency << std::endl;
  //   }
  // }
  this->client_seq_num = client_seq_num;

  sentBeginValTxnMsg.Clear();
  sentBeginValTxnMsg.set_client_id(client_id);
  sentBeginValTxnMsg.set_client_seq_num(client_seq_num);
  *sentBeginValTxnMsg.mutable_txn_state() = protoTxnState;
  sentBeginValTxnMsg.mutable_timestamp()->set_timestamp(txnStartTime);
  sentBeginValTxnMsg.mutable_timestamp()->set_id(client_id);

  beginValSent.clear();
  std::unique_lock lock(sentFwdResultsMutex);
  sentFwdResults.clear();

  Debug("beginValTxnMsg client id %lu, seq num %lu", client_id, client_seq_num);

  // for tracking purposes, must have self in beginValSent
  beginValSent.insert(client_id);
  // send to all clients so no need to bother with 
  
  if(params.sintr_params.clientValidationHeuristic == CLIENT_VALIDATION_HEURISTIC::ALL) {
    for (int i = 0; i < clients_config->n; i++) {
      // do not send to self
      if (i == client_id) {
        continue;
      }
      beginValSent.insert(i);
      transport->SendMessageToReplica(this, i, sentBeginValTxnMsg);
    }
  }
  // other heuristics depend on actual policy that was estimated
  else {
    // extract out the clients that need to be contacted
    std::set<uint64_t> clients;
    // need to use DifferenceToSatisfied to account for self
    ExtractFromPolicyClientsToContact(policyClient->DifferenceToSatisfied(beginValSent), clients);
    
    if (params.sintr_params.clientValidationHeuristic == CLIENT_VALIDATION_HEURISTIC::EXACT) {
    }
    else if (params.sintr_params.clientValidationHeuristic == CLIENT_VALIDATION_HEURISTIC::ONE_MORE) {
      for (int i = 0; i < clients_config->n; i++) {
        if (i != client_id && clients.find(i) == clients.end()) {
          clients.insert(i);
        }
      }
    }
    else {
      Panic("Invalid clientValidationHeuristic value");
    }

    for (const auto &i : clients) {
      // do not send to self
      if (i == client_id) {
        continue;
      }
      beginValSent.insert(i);
      transport->SendMessageToReplica(this, i, sentBeginValTxnMsg);
    }
    // sanity check - policy should be satisfied by the clients we are sending to
    UW_ASSERT(policyClient->IsSatisfied(beginValSent));
  }
}

void Client2Client::ForwardReadResultMessage(const std::string &key, const std::string &value, const Timestamp &ts,
    const proto::CommittedProof &proof, const std::string &serializedWrite, const std::string &serializedWriteTypeName, 
    const proto::Dependency &dep, bool hasDep, bool addReadset, const proto::Dependency &policyDep, bool hasPolicyDep) {

  // get the current client seq num so it doesn't change during the forwarding process
  uint64_t client_seq_num = this->client_seq_num;
  if (!params.sintr_params.client2clientMultiThreading) {
    ForwardReadResultMessageHelper(client_seq_num, key, value, ts, proof, serializedWrite, serializedWriteTypeName,
      dep, hasDep, addReadset, policyDep, hasPolicyDep);
  }
  else {
    auto f = [=]() {
      this->ForwardReadResultMessageHelper(
        client_seq_num, key, value, ts, proof, serializedWrite, 
        serializedWriteTypeName, dep, hasDep, addReadset,
        policyDep, hasPolicyDep
      );
      return (void*) true;
    };
    Client2ClientMessageExecutor *executor = new Client2ClientMessageExecutor(std::move(f));
    c2cQueue.push(executor);
  }
}

void Client2Client::ForwardReadResultMessageHelper(const uint64_t client_seq_num,
    const std::string &key, const std::string &value, const Timestamp &ts,
    const proto::CommittedProof &proof, const std::string &serializedWrite, const std::string &serializedWriteTypeName, 
    const proto::Dependency &dep, bool hasDep, bool addReadset, const proto::Dependency &policyDep, bool hasPolicyDep) {

  proto::ForwardReadResultMessage *fwdReadResultMsgToSend = new proto::ForwardReadResultMessage();
  fwdReadResultMsgToSend->set_client_id(client_id);
  fwdReadResultMsgToSend->set_client_seq_num(client_seq_num);
  proto::ForwardReadResult fwdReadResult;
  fwdReadResult.set_key(key);
  fwdReadResult.set_value(value);
  fwdReadResult.mutable_timestamp()->set_timestamp(ts.getTimestamp());
  fwdReadResult.mutable_timestamp()->set_id(ts.getID());

  if (params.sintr_params.signFwdReadResults) {
    // struct timespec ts_start;
    // clock_gettime(CLOCK_MONOTONIC, &ts_start);
    // uint64_t start = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;
    CreateHMACedMessage(fwdReadResult, *fwdReadResultMsgToSend->mutable_signed_fwd_read_result());
    // struct timespec ts_end;
    // clock_gettime(CLOCK_MONOTONIC, &ts_end);
    // uint64_t end = ts_end.tv_sec * 1000 * 1000 + ts_end.tv_nsec / 1000;
    // auto duration = end - start;
    // create_hmac_ms.push_back(duration);
  }
  else {
    *fwdReadResultMsgToSend->mutable_fwd_read_result() = std::move(fwdReadResult);
  }

  // only if addReadset is true did this result come from server
  // otherwise it came from the buffer and there is no dependency or committed proof
  if (addReadset) {
    // this will contain the prepared txn dependency
    if (hasDep) {
      UW_ASSERT(dep.IsInitialized());
      *fwdReadResultMsgToSend->mutable_dep() = std::move(dep);
      // must be oneof write or signed write
      *fwdReadResultMsgToSend->mutable_write() = proto::Write();
    }
    else {
      if (params.validateProofs) {
        if (proof.IsInitialized()) {
          *fwdReadResultMsgToSend->mutable_proof() = std::move(proof);
        }
        // if no proof then it is possible the value is empty
        else {
          UW_ASSERT(value.length() == 0);
        }
      }

      // depending on if signatures are enabled and if the value is non empty
      if (serializedWriteTypeName == fwdReadResultMsgToSend->signed_write().GetTypeName()) {
        UW_ASSERT(fwdReadResultMsgToSend->mutable_signed_write()->ParseFromString(serializedWrite));
      }
      else if (serializedWriteTypeName == fwdReadResultMsgToSend->write().GetTypeName()) {
        UW_ASSERT(fwdReadResultMsgToSend->mutable_write()->ParseFromString(serializedWrite));
      }
      else {
        // this should only happen if value is empty
        UW_ASSERT(value.length() == 0);
        *fwdReadResultMsgToSend->mutable_write() = proto::Write();
      }
    }

    // separately include policy change txn dependency if there is one
    if (hasPolicyDep) {
      UW_ASSERT(policyDep.IsInitialized());
      *fwdReadResultMsgToSend->mutable_policy_dep() = std::move(policyDep);
    }
  }

  fwdReadResultMsgToSend->set_add_readset(addReadset);

  std::unique_lock lock(sentFwdResultsMutex);
  sentFwdResults.insert(fwdReadResultMsgToSend);

  Debug(
    "ForwardReadResult: client id %lu, seq num %lu, key %s, value %s",
    client_id,
    client_seq_num,
    BytesToHex(key, 16).c_str(),
    BytesToHex(value, 16).c_str()
  );
  for (const auto &i : beginValSent) {
    // do not send to self
    if (i == client_id) {
      continue;
    }
    transport->SendMessageToReplica(this, i, *fwdReadResultMsgToSend);
  }
}

void Client2Client::ForwardPointQueryResultMessage(const std::string &key, const std::string &value, const Timestamp &ts,
    const std::string &table_name, const proto::CommittedProof &proof,
    const std::string &serializedWrite, const std::string &serializedWriteTypeName,
    const proto::Dependency &dep, bool hasDep, bool addReadset) {
  
  uint64_t client_seq_num = this->client_seq_num;
  if (!params.sintr_params.client2clientMultiThreading) {
    ForwardPointQueryResultMessageHelper(
      client_seq_num, key, value, ts, table_name, proof, serializedWrite, 
      serializedWriteTypeName, dep, hasDep, addReadset
    );
  }
  else {
    auto f = [=]() {
      this->ForwardPointQueryResultMessageHelper(
        client_seq_num, key, value, ts, table_name, proof, serializedWrite, 
        serializedWriteTypeName, dep, hasDep, addReadset
      );
      return (void*) true;
    };
    Client2ClientMessageExecutor *executor = new Client2ClientMessageExecutor(std::move(f));
    c2cQueue.push(executor);
  }
}

// basically same logic as ForwardReadResultMessageHelper
// no policy dep but additional table_name field
void Client2Client::ForwardPointQueryResultMessageHelper(const uint64_t client_seq_num,
    const std::string &key, const std::string &value, const Timestamp &ts,
    const std::string &table_name, const proto::CommittedProof &proof,
    const std::string &serializedWrite, const std::string &serializedWriteTypeName,
    const proto::Dependency &dep, bool hasDep, bool addReadset) {
  
  proto::ForwardPointQueryResultMessage *fwdPointQueryResultMsgToSend = new proto::ForwardPointQueryResultMessage();
  fwdPointQueryResultMsgToSend->set_client_id(client_id);
  fwdPointQueryResultMsgToSend->set_client_seq_num(client_seq_num);
  fwdPointQueryResultMsgToSend->set_table_name(table_name);
  proto::ForwardReadResult fwdReadResult;
  fwdReadResult.set_key(key);
  fwdReadResult.set_value(value);
  fwdReadResult.mutable_timestamp()->set_timestamp(ts.getTimestamp());
  fwdReadResult.mutable_timestamp()->set_id(ts.getID());

  if (params.sintr_params.signFwdReadResults) {
    CreateHMACedMessage(fwdReadResult, *fwdPointQueryResultMsgToSend->mutable_signed_fwd_read_result());
  }
  else {
    *fwdPointQueryResultMsgToSend->mutable_fwd_read_result() = std::move(fwdReadResult);
  }

  // only if addReadset is true did this result come from server
  // otherwise it came from the buffer and there is no dependency or committed proof
  if (addReadset) {
    // this will contain the prepared txn dependency
    if (hasDep) {
      UW_ASSERT(dep.IsInitialized());
      *fwdPointQueryResultMsgToSend->mutable_dep() = std::move(dep);
      // must be oneof write or signed write
      *fwdPointQueryResultMsgToSend->mutable_write() = proto::Write();
    }
    else {
      if (params.validateProofs) {
        if (proof.IsInitialized()) {
          *fwdPointQueryResultMsgToSend->mutable_proof() = std::move(proof);
        }
        // if no proof then it is possible the value is empty
        else {
          UW_ASSERT(value.length() == 0);
        }
      }

      // depending on if signatures are enabled and if the value is non empty
      if (serializedWriteTypeName == fwdPointQueryResultMsgToSend->signed_write().GetTypeName()) {
        UW_ASSERT(fwdPointQueryResultMsgToSend->mutable_signed_write()->ParseFromString(serializedWrite));
      }
      else if (serializedWriteTypeName == fwdPointQueryResultMsgToSend->write().GetTypeName()) {
        UW_ASSERT(fwdPointQueryResultMsgToSend->mutable_write()->ParseFromString(serializedWrite));
      }
      else {
        // this should only happen if value is empty
        UW_ASSERT(value.length() == 0);
        *fwdPointQueryResultMsgToSend->mutable_write() = proto::Write();
      }
    }
  }

  fwdPointQueryResultMsgToSend->set_add_readset(addReadset);

  std::unique_lock lock(sentFwdResultsMutex);
  sentFwdResults.insert(fwdPointQueryResultMsgToSend);

  Debug(
    "ForwardPointQueryResult: client id %lu, seq num %lu, key %s, result %s",
    client_id,
    client_seq_num,
    BytesToHex(key, 16).c_str(),
    BytesToHex(value, 16).c_str()
  );
  for (const auto &i : beginValSent) {
    // do not send to self
    if (i == client_id) {
      continue;
    }
    transport->SendMessageToReplica(this, i, *fwdPointQueryResultMsgToSend);
  }
}

void Client2Client::ForwardQueryResultMessage(const std::string &query_id, const std::string &query_result,
    const std::map<uint64_t, proto::ReadSet*> &group_read_sets, const std::map<uint64_t, std::string> &group_result_hashes,
    const std::map<uint64_t, std::vector<proto::SignedMessage>> &group_sigs, bool addReadset) {

  uint64_t client_seq_num = this->client_seq_num;
  if (!params.sintr_params.client2clientMultiThreading) {
    ForwardQueryResultMessageHelper(
      client_seq_num, query_id, query_result,
      group_read_sets, group_result_hashes, group_sigs, addReadset
    );
  }
  else {
    auto f = [=]() {
      this->ForwardQueryResultMessageHelper(
        client_seq_num, query_id, query_result,
        group_read_sets, group_result_hashes, group_sigs, addReadset
      );
      return (void*) true;
    };
    Client2ClientMessageExecutor *executor = new Client2ClientMessageExecutor(std::move(f));
    c2cQueue.push(executor);
  }
}

void Client2Client::ForwardQueryResultMessageHelper(const uint64_t client_seq_num,
    const std::string &query_id, const std::string &query_result,
    const std::map<uint64_t, proto::ReadSet*> &group_read_sets, const std::map<uint64_t, std::string> &group_result_hashes,
    const std::map<uint64_t, std::vector<proto::SignedMessage>> &group_sigs, bool addReadset) {
  
  proto::ForwardQueryResultMessage *fwdQueryResultMsgToSend = new proto::ForwardQueryResultMessage();
  fwdQueryResultMsgToSend->set_client_id(client_id);
  fwdQueryResultMsgToSend->set_client_seq_num(client_seq_num);
  proto::ForwardQueryResult fwdQueryResult;
  fwdQueryResult.set_query_id(query_id);
  fwdQueryResult.set_query_result(query_result);
  
  if (params.sintr_params.signFwdReadResults) {
    CreateHMACedMessage(fwdQueryResult, *fwdQueryResultMsgToSend->mutable_signed_fwd_query_result());
  }
  else {
    *fwdQueryResultMsgToSend->mutable_fwd_query_result() = std::move(fwdQueryResult);
  }

  if (addReadset) {
    if(params.query_params.cacheReadSet){ 
      for(auto &[group, read_set_hash] : group_result_hashes){
        proto::QueryGroupMeta &queryMD = (*fwdQueryResultMsgToSend->mutable_group_meta())[group]; 
        queryMD.set_read_set_hash(read_set_hash);
      }
    }
    else {
      for(auto &[group, read_set] : group_read_sets){
        proto::QueryGroupMeta &queryMD = (*fwdQueryResultMsgToSend->mutable_group_meta())[group]; 
        *queryMD.mutable_query_read_set() = *read_set;
      }
    }

    if (params.validateProofs) {
      for (const auto &[group, query_sigs] : group_sigs) {
        proto::SignedMessages &curr_group_sigs = (*fwdQueryResultMsgToSend->mutable_query_sigs())[group];
        for (const auto &query_sig : query_sigs) {
          *curr_group_sigs.add_sig_msgs() = query_sig;
        }
      }
    }
  }

  fwdQueryResultMsgToSend->set_add_readset(addReadset);

  std::unique_lock lock(sentFwdResultsMutex);
  sentFwdResults.insert(fwdQueryResultMsgToSend);

  Debug(
    "ForwardQueryResult: client id %lu, seq num %lu, query_id %s",
    client_id,
    client_seq_num,
    BytesToHex(query_id, 16).c_str()
  );
  for (const auto &i : beginValSent) {
    // do not send to self
    if (i == client_id) {
      continue;
    }
    transport->SendMessageToReplica(this, i, *fwdQueryResultMsgToSend);
  }
}

void Client2Client::HandlePolicyUpdate(const Policy *policy) {
  UW_ASSERT(policy != nullptr);
  endorseClient->UpdateRequirement(policy);
  std::vector<int> diff = endorseClient->DifferenceToSatisfied(beginValSent);
  // if after updating the policy, and the current set of validations is not enough, initiate more
  if (diff.size() > 0) {
    std::set<uint64_t> clients;
    ExtractFromPolicyClientsToContact(diff, clients);
    Debug("Initiating %ld more beginValTxnMsg", clients.size());
    std::shared_lock lock(sentFwdResultsMutex);
    for (const auto &i : clients) {
      // do not send to self
      if (i == client_id) {
        continue;
      }
      auto ret = beginValSent.insert(i);
      // should be first time sending to this client
      UW_ASSERT(ret.second);
      transport->SendMessageToReplica(this, i, sentBeginValTxnMsg);
      for (const auto &fwdReadResultMsg : sentFwdResults) {
        transport->SendMessageToReplica(this, i, *fwdReadResultMsg);
      }
    }
  }
}

void Client2Client::ManageDispatchBeginValidateTxnMessage(const TransportAddress &remote, const std::string &data) {
  if (!params.sintr_params.client2clientMultiThreading) {
    beginValTxnMsg.ParseFromString(data);
    HandleBeginValidateTxnMessage(remote, beginValTxnMsg);
  }
  else {
    proto::BeginValidateTxnMessage *beginValTxnMsg = new proto::BeginValidateTxnMessage();
    beginValTxnMsg->ParseFromString(data);
    auto f = [this, &remote, beginValTxnMsg](){
      this->HandleBeginValidateTxnMessage(remote, *beginValTxnMsg);
      delete beginValTxnMsg;
      return (void*) true;
    };
    Client2ClientMessageExecutor *executor = new Client2ClientMessageExecutor(std::move(f));
    c2cQueue.push(executor);
  }
}

void Client2Client::ManageDispatchForwardReadResultMessage(const TransportAddress &remote, const std::string &data) {
  if (!params.sintr_params.client2clientMultiThreading) {
    fwdReadResultMsg.ParseFromString(data);
    HandleForwardReadResultMessage(fwdReadResultMsg);
  }
  else {
    proto::ForwardReadResultMessage *fwdReadResultMsg = new proto::ForwardReadResultMessage();
    fwdReadResultMsg->ParseFromString(data);
    auto f = [this, fwdReadResultMsg](){
      this->HandleForwardReadResultMessage(*fwdReadResultMsg);
      delete fwdReadResultMsg;
      return (void*) true;
    };
    Client2ClientMessageExecutor *executor = new Client2ClientMessageExecutor(std::move(f));
    c2cQueue.push(executor);
  }
}

void Client2Client::ManageDispatchForwardPointQueryResultMessage(const TransportAddress &remote, const std::string &data) {
  if (!params.sintr_params.client2clientMultiThreading) {
    fwdPointQueryResultMsg.ParseFromString(data);
    HandleForwardPointQueryResultMessage(fwdPointQueryResultMsg);
  }
  else {
    proto::ForwardPointQueryResultMessage *fwdPointQueryResultMsg = new proto::ForwardPointQueryResultMessage();
    fwdPointQueryResultMsg->ParseFromString(data);
    auto f = [this, fwdPointQueryResultMsg](){
      this->HandleForwardPointQueryResultMessage(*fwdPointQueryResultMsg);
      delete fwdPointQueryResultMsg;
      return (void*) true;
    };
    Client2ClientMessageExecutor *executor = new Client2ClientMessageExecutor(std::move(f));
    c2cQueue.push(executor);
  }
}

void Client2Client::ManageDispatchForwardQueryResultMessage(const TransportAddress &remote, const std::string &data) {
  if (!params.sintr_params.client2clientMultiThreading) {
    fwdQueryResultMsg.ParseFromString(data);
    HandleForwardQueryResultMessage(fwdQueryResultMsg);
  }
  else {
    proto::ForwardQueryResultMessage *fwdQueryResultMsg = new proto::ForwardQueryResultMessage();
    fwdQueryResultMsg->ParseFromString(data);
    auto f = [this, fwdQueryResultMsg](){
      this->HandleForwardQueryResultMessage(*fwdQueryResultMsg);
      delete fwdQueryResultMsg;
      return (void*) true;
    };
    Client2ClientMessageExecutor *executor = new Client2ClientMessageExecutor(std::move(f));
    c2cQueue.push(executor);
  }
}

void Client2Client::ManageDispatchFinishValidateTxnMessage(const TransportAddress &remote, const std::string &data) {
  if (!params.sintr_params.client2clientMultiThreading) {
    finishValTxnMsg.ParseFromString(data);
    HandleFinishValidateTxnMessage(finishValTxnMsg);
  }
  else {
    proto::FinishValidateTxnMessage *finishValTxnMsg = new proto::FinishValidateTxnMessage();
    finishValTxnMsg->ParseFromString(data);
    auto f = [this, finishValTxnMsg](){
      this->HandleFinishValidateTxnMessage(*finishValTxnMsg);
      delete finishValTxnMsg;
      return (void*) true;
    };

    if (params.sintr_params.parallelEndorsementCheck) {
      // fully parallelize the endorsement check so that each one can be handled by a worker thread
      transport->DispatchTP_noCB(std::move(f));
    }
    else {
      // only moves the function to be off the main client thread, but still sequential on client2client message thread
      Client2ClientMessageExecutor *executor = new Client2ClientMessageExecutor(std::move(f));
      c2cQueue.push(executor);
    }
  }
}

void Client2Client::HandleBeginValidateTxnMessage(const TransportAddress &remote, 
    const proto::BeginValidateTxnMessage &beginValTxnMsg) {
  // if (verify_hmac_ms.size() > 0 && verify_hmac_ms.size() % 2000 == 0) {
  //   std::cerr << "Number of prepared vs committed: " << check_prepared << ", " << check_committed << std::endl;
  //   if (verify_hmac_ms.size() > 0) {
  //     double mean_verify_hmac_latency = std::accumulate(verify_hmac_ms.begin(), verify_hmac_ms.end(), 0.0) / verify_hmac_ms.size();
  //     std::cerr << "Mean verify HMAC latency: " << mean_verify_hmac_latency << std::endl;
  //   }
  //   if (check_committed_prepared_ms.size() > 0) {
  //     double mean_check_committed_prepared_latency = std::accumulate(check_committed_prepared_ms.begin(), check_committed_prepared_ms.end(), 0.0) / check_committed_prepared_ms.size();
  //     std::cerr << "Mean check committed prepared latency: " << mean_check_committed_prepared_latency << std::endl;
  //   }
  //   if (send_finish_val_ms.size() > 0) {
  //     double mean_send_finish_val_latency = std::accumulate(send_finish_val_ms.begin(), send_finish_val_ms.end(), 0.0) / send_finish_val_ms.size();
  //     std::cerr << "Mean send finish validation latency: " << mean_send_finish_val_latency << std::endl;
  //   }
  // }
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
    // struct timespec ts_start;
    // clock_gettime(CLOCK_MONOTONIC, &ts_start);
    // uint64_t start = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;

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

    // struct timespec ts_end;
    // clock_gettime(CLOCK_MONOTONIC, &ts_end);
    // uint64_t end = ts_end.tv_sec * 1000 * 1000 + ts_end.tv_nsec / 1000;
    // auto duration = end - start;
    // verify_hmac_ms.push_back(duration);

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

  bool hasPolicyDep = fwdReadResultMsg.has_policy_dep();
  proto::Dependency policyDep;
  if (hasPolicyDep) {
    policyDep = fwdReadResultMsg.policy_dep();
  }

  Debug(
    "HandleForwardReadResult: from client id %lu, seq num %lu, key %s, value %s", 
    curr_client_id, 
    curr_client_seq_num,
    BytesToHex(curr_key, 16).c_str(),
    BytesToHex(curr_value, 16).c_str()
  );
  // tell valClient about this forwardedReadResult
  valClient->ProcessForwardReadResult(curr_client_id, curr_client_seq_num, fwdReadResult, 
      dep, hasDep, addReadset, policyDep, hasPolicyDep);
}

void Client2Client::HandleForwardPointQueryResultMessage(const proto::ForwardPointQueryResultMessage &fwdPointQueryResultMsg) {
  uint64_t curr_client_id = fwdPointQueryResultMsg.client_id();
  uint64_t curr_client_seq_num = fwdPointQueryResultMsg.client_seq_num();

  proto::ForwardReadResult fwdReadResult;
  if (params.sintr_params.signFwdReadResults) {
    // first check client signature
    if (!fwdPointQueryResultMsg.has_signed_fwd_read_result()) {
      Debug(
        "Missing client signature on forwarded read result from client id %lu, seq num %lu", 
        curr_client_id, 
        curr_client_seq_num
      );
      return;
    }
    std::string data;
    if (!ValidateHMACedMessage(fwdPointQueryResultMsg.signed_fwd_read_result(), data)) {
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
    fwdReadResult = fwdPointQueryResultMsg.fwd_read_result();
  }

  std::string curr_key = fwdReadResult.key();
  std::string curr_value = fwdReadResult.value();

  proto::Write write;
  bool hasDep = fwdPointQueryResultMsg.has_dep();
  proto::Dependency dep;
  bool addReadset = fwdPointQueryResultMsg.add_readset();
  // only if addReadset is true will there be dep or committed proofs
  if (addReadset && params.sintr_params.clientCheckEvidence) {
    if (!CheckPreparedCommittedEvidence(fwdPointQueryResultMsg, write, dep)) {
      Panic("Invalid prepared or committed evidence on forwarded point query result");
      return;
    }
    // point query dependencies don't contain full information about the write, only txn digest
    // so we can't check the key 
    if (!fwdPointQueryResultMsg.has_dep()) {
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
  }

  Debug(
    "HandleForwardPointQueryResult: from client id %lu, seq num %lu, key %s, value %s", 
    curr_client_id, 
    curr_client_seq_num,
    BytesToHex(curr_key, 16).c_str(),
    BytesToHex(curr_value, 16).c_str()
  );
  // tell valClient about this forwardedReadResult
}

void Client2Client::HandleForwardQueryResultMessage(const proto::ForwardQueryResultMessage &fwdQueryResultMsg) {
  uint64_t curr_client_id = fwdQueryResultMsg.client_id();
  uint64_t curr_client_seq_num = fwdQueryResultMsg.client_seq_num();

  proto::ForwardQueryResult fwdQueryResult;
  if (params.sintr_params.signFwdReadResults) {
    // first check client signature
    if (!fwdQueryResultMsg.has_signed_fwd_query_result()) {
      Debug(
        "Missing client signature on forwarded query result from client id %lu, seq num %lu", 
        curr_client_id, 
        curr_client_seq_num
      );
      return;
    }
    std::string data;
    if (!ValidateHMACedMessage(fwdQueryResultMsg.signed_fwd_query_result(), data)) {
      Debug(
        "Invalid client signature on forwarded query result from client id %lu, seq num %lu", 
        curr_client_id, 
        curr_client_seq_num
      );
      return;
    }

    fwdQueryResult.ParseFromString(data);
  }
  else {
    fwdQueryResult = fwdQueryResultMsg.fwd_query_result();
  }

  std::string curr_query_id = fwdQueryResult.query_id();
  std::string curr_query_result = fwdQueryResult.query_result();

  bool addReadset = fwdQueryResultMsg.add_readset();
  if (addReadset && params.sintr_params.clientCheckEvidence) {
    if (!CheckPreparedCommittedEvidence(fwdQueryResultMsg, curr_query_id, curr_query_result)) {
      Panic("Invalid prepared or committed evidence on forwarded query result");
      return;
    }
  }

  Debug(
    "HandleForwardQueryResult: from client id %lu, seq num %lu, query id %s, query result %s", 
    curr_client_id, 
    curr_client_seq_num,
    BytesToHex(curr_query_id, 16).c_str(),
    BytesToHex(curr_query_result, 16).c_str()
  );
  // tell valClient about this forwardedReadResult
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

    // struct timespec ts_start;
    // clock_gettime(CLOCK_MONOTONIC, &ts_start);
    // uint64_t start = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;

    if(!clients_verifier->Verify(keyManager->GetPublicKey(keyManager->GetClientKeyId(signedMsg.process_id())),
        signedMsg.data(), signedMsg.signature())) {
      Debug("Invalid signature on validation txn digest sent from client id %lu", peer_client_id);
      return;
    }
    // struct timespec ts_end;
    // clock_gettime(CLOCK_MONOTONIC, &ts_end);
    // uint64_t end = ts_end.tv_sec * 1000 * 1000 + ts_end.tv_nsec / 1000;
    // auto duration = end - start;
    // verify_endorse_ms.push_back(duration);

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
        // struct timespec ts_start;
        // clock_gettime(CLOCK_MONOTONIC, &ts_start);
        // uint64_t start = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;

        if (!verifier->Verify(keyManager->GetPublicKey(fwdReadResultMsg.signed_write().process_id()),
            fwdReadResultMsg.signed_write().data(), fwdReadResultMsg.signed_write().signature())) {
          Debug(
            "Invalid server signature on forwarded read result from client id %lu, seq num %lu", 
            curr_client_id, 
            curr_client_seq_num
          );
          return false;
        }
        // struct timespec ts_end;
        // clock_gettime(CLOCK_MONOTONIC, &ts_end);
        // uint64_t end = ts_end.tv_sec * 1000 * 1000 + ts_end.tv_nsec / 1000;
        // auto duration = end - start;
        // check_committed_prepared_ms.push_back(duration);

        write.ParseFromString(fwdReadResultMsg.signed_write().data());
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
        if(params.sintr_params.hashEndorsements && fwdReadResultMsg.proof().txn().has_txndigest()) {
          committedTxnDigest = fwdReadResultMsg.proof().txn().txndigest();
        } else if(params.sintr_params.hashEndorsements) {
          Debug("NO TXN DIGEST IN PROOF FOR CLIENT2CLIENT forward read result");
        }
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

bool Client2Client::CheckPreparedCommittedEvidence(const proto::ForwardPointQueryResultMessage &fwdPointQueryResultMsg, 
    proto::Write &write, proto::Dependency &dep) {
  uint64_t curr_client_id = fwdPointQueryResultMsg.client_id();
  uint64_t curr_client_seq_num = fwdPointQueryResultMsg.client_seq_num();

  // if has dependency, then this is based on a prepared txn
  if (fwdPointQueryResultMsg.has_dep()) {
    if (params.validateProofs && params.signedMessages && params.verifyDeps) {
      if (!ValidateDependency(fwdPointQueryResultMsg.dep(), config, params.readDepSize, 
          keyManager, verifier)) {
        Debug(
          "Invalid dependency on forwarded point query result from client id %lu, seq num %lu",
          curr_client_id, 
          curr_client_seq_num
        );
        return false;
      }
    }
    dep = fwdPointQueryResultMsg.dep();
    write = fwdPointQueryResultMsg.dep().write();
  }
  else {
    // otherwise can check committed proof and signature

    if (params.validateProofs && params.signedMessages) {
      // check server signature
      if (fwdPointQueryResultMsg.has_signed_write()) {
        if (!verifier->Verify(keyManager->GetPublicKey(fwdPointQueryResultMsg.signed_write().process_id()),
            fwdPointQueryResultMsg.signed_write().data(), fwdPointQueryResultMsg.signed_write().signature())) {
          Debug(
            "Invalid server signature on forwarded point query result from client id %lu, seq num %lu", 
            curr_client_id, 
            curr_client_seq_num
          );
          return false;
        }

        write.ParseFromString(fwdPointQueryResultMsg.signed_write().data());
      }
      else {
        if (fwdPointQueryResultMsg.has_write() && fwdPointQueryResultMsg.write().has_committed_value()) {
          Debug(
            "Missing server signature on forwarded read result with committed value from client id %lu, seq num %lu", 
            curr_client_id, 
            curr_client_seq_num
          );
          return false;
        }

        write = fwdPointQueryResultMsg.write();
      }
    }
    else {
      write = fwdPointQueryResultMsg.write();
    }
      
    if (params.validateProofs) {
      // check committed proof
      if (write.has_committed_value() && write.has_committed_timestamp()) {
        if (!fwdPointQueryResultMsg.has_proof()) {
          Debug(
            "Missing committed value proof for forwarded point query result from client id %lu, seq num %lu",
            curr_client_id,
            curr_client_seq_num
          );
          return false;
        }

        std::string committedTxnDigest = TransactionDigest(fwdPointQueryResultMsg.proof().txn(), params.hashDigest);
        if(params.sintr_params.hashEndorsements && fwdPointQueryResultMsg.proof().txn().has_txndigest()) {
          committedTxnDigest = fwdPointQueryResultMsg.proof().txn().txndigest();
        } else if(params.sintr_params.hashEndorsements) {
          Debug("NO TXN DIGEST IN PROOF FOR CLIENT2CLIENT forward point query result");
        }

        sql::QueryResultProtoWrapper query_result;
        if (!ValidateTransactionTableWrite(fwdPointQueryResultMsg.proof(), &committedTxnDigest,
            write.committed_timestamp(), write.key(), write.committed_value(),
            fwdPointQueryResultMsg.table_name(), &query_result, sql_interpreter,
            config, params.signedMessages, keyManager, verifier)) {
          Debug(
            "Failed to validate committed value for forwarded point query result from client id %lu, seq num %lu",
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

bool Client2Client::CheckPreparedCommittedEvidence(const proto::ForwardQueryResultMessage &fwdQueryResultMsg,
    const std::string &query_id, const std::string &query_result) {
  
  uint64_t curr_client_id = fwdQueryResultMsg.client_id();
  uint64_t curr_client_seq_num = fwdQueryResultMsg.client_seq_num();

  uint64_t num_matches = 0;
  if (params.validateProofs && params.signedMessages) {
    for (const auto &[group, curr_query_sigs] : fwdQueryResultMsg.query_sigs()) {
      for (const auto &query_sig : curr_query_sigs.sig_msgs()) {
        proto::QueryResult validated_result;
        // first check signature
        if (!verifier->Verify(keyManager->GetPublicKey(query_sig.process_id()),
            query_sig.data(), query_sig.signature())) {
          Debug(
            "Invalid server signature on forwarded query result from client id %lu, seq num %lu", 
            curr_client_id, 
            curr_client_seq_num
          );
          return false;
        }

        if (!validated_result.ParseFromString(query_sig.data())) {
          Debug(
            "Failed to parse query result from client id %lu, seq num %lu", 
            curr_client_id, 
            curr_client_seq_num
          );
          return false;
        }

        // next make sure that we have matches
        // TODO: query id
        if (validated_result.query_result() != query_result) {
          Debug(
            "Mismatch in query result for forwarded query result from client id %lu, seq num %lu", 
            curr_client_id, 
            curr_client_seq_num
          );
          continue;
        }

        // check readset
        if (params.query_params.cacheReadSet) {
          // only expect hash
          if (validated_result.query_result_hash() != fwdQueryResultMsg.group_meta().at(group).read_set_hash()) {
            Debug(
              "Mismatch in read set hash for forwarded query result from client id %lu, seq num %lu", 
              curr_client_id, 
              curr_client_seq_num
            );
            continue;
          }
        }
        else {
          // expect full readset
          // compute hash to compare
          std::string validated_result_hash = generateReadSetSingleHash(validated_result.query_read_set());
          std::string fwd_read_set_hash = generateReadSetSingleHash(fwdQueryResultMsg.group_meta().at(group).query_read_set());
          if (validated_result_hash != fwd_read_set_hash) {
            Debug(
              "Mismatch in read set for forwarded query result from client id %lu, seq num %lu", 
              curr_client_id, 
              curr_client_seq_num
            );
            continue;
          }
        }

        num_matches++;
      }
    }

    if (num_matches < params.query_params.resultQuorum) {
      Debug(
        "Insufficient number of matches for forwarded query result from client id %lu, seq num %lu", 
        curr_client_id, 
        curr_client_seq_num
      );
      return false;
    }
  }

  return true;
}

void Client2Client::ExtractFromPolicyClientsToContact(const std::vector<int> &policySatSet, std::set<uint64_t> &clients) {
  int offset = 1;
  for (const auto &i : policySatSet) {
    if (i == client_id) {
      continue;
    }
    else if (i < 0) {
      for (; offset < clients_config->n; offset++) {
        uint64_t target = (client_id + offset) % clients_config->n;
        if (beginValSent.find(target) == beginValSent.end() && clients.find(target) == clients.end()) {
          clients.insert(target);
          break;
        }
      }
      // if we reach the end of the loop, then we have exhausted all clients
      if (offset == clients_config->n) {
        Panic("Policy requires more endorsements than available clients");
      }
    }
    else {
      if (beginValSent.find(i) == beginValSent.end()) {
        clients.insert(i);
      }
      else {
        Panic("Client %lu already sent beginValTxnMsg to client %d", client_id, i);
      }
    }
  }
}

void Client2Client::ValidationThreadFunction() {
  ::SyncClient syncClient(valClient);
  while(!done) {
    ValidationInfo *valInfo;
    validationQueue.pop(valInfo);
    if (valInfo == nullptr) {
      continue;
    }
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

    valClient->SetThreadValTxnId(curr_client_id, curr_client_seq_num);
    valClient->SetTxnTimestamp(curr_client_id, curr_client_seq_num, curr_ts);

    struct timespec ts_start;
    clock_gettime(CLOCK_MONOTONIC, &ts_start);
    uint64_t start = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;

    transaction_status_t result = valTxn->Validate(syncClient);

    struct timespec ts_end;
    clock_gettime(CLOCK_MONOTONIC, &ts_end);
    uint64_t end = ts_end.tv_sec * 1000 * 1000 + ts_end.tv_nsec / 1000;
    auto duration = end - start;
    // Warning("validation took %lu us", duration);

    if (result == COMMITTED) {
      Debug("Completed validation for client id %lu, seq num %lu", curr_client_id, curr_client_seq_num);
      proto::Transaction *txn = valClient->GetCompletedTxn(curr_client_id, curr_client_seq_num);

      if (params.parallel_CCC) {
        std::sort(txn->mutable_read_set()->begin(), txn->mutable_read_set()->end(), sortReadSetByKey);
        std::sort(txn->mutable_write_set()->begin(), txn->mutable_write_set()->end(), sortWriteSetByKey);
      }
      std::sort(txn->mutable_involved_groups()->begin(), txn->mutable_involved_groups()->end());

      // struct timespec ts_start;
      // clock_gettime(CLOCK_MONOTONIC, &ts_start);
      // uint64_t start = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;

      proto::FinishValidateTxnMessage finishValTxnMsg;
      finishValTxnMsg.set_client_id(client_id);
      finishValTxnMsg.set_validation_txn_seq_num(curr_client_seq_num);

      // only send over digest, not actual contents
      std::string digest = TransactionDigest(*txn, params.hashDigest);
      if (params.sintr_params.signFinishValidation) {
        // sign the digest
        SignBytes(
          digest, 
          keyManager->GetPrivateKey(keyManager->GetClientKeyId(client_id)), 
          client_id, 
          finishValTxnMsg.mutable_signed_validation_txn_digest()
        );
      }
      else {
        finishValTxnMsg.set_validation_txn_digest(digest);
      }

      if (params.sintr_params.debugEndorseCheck) {
        *finishValTxnMsg.mutable_val_txn() = *txn;
      }

      // struct timespec ts_end;
      // clock_gettime(CLOCK_MONOTONIC, &ts_end);
      // uint64_t end = ts_end.tv_sec * 1000 * 1000 + ts_end.tv_nsec / 1000;
      // auto duration = end - start;
      // send_finish_val_ms.push_back(duration);

      transport->SendMessage(this, *valInfo->remote, finishValTxnMsg);
      delete txn;
    }

    delete valInfo;
    Debug("thread exiting for validation for client id %lu, seq num %lu", curr_client_id, curr_client_seq_num);
  }
  Debug("done true, exiting validation thread");
}

void Client2Client::Client2ClientMessageThreadFunction() {
  while (!done) {
    Client2ClientMessageExecutor *executor;
    c2cQueue.pop(executor);
    if (executor == nullptr) {
      continue;
    }
    executor->f();
    delete executor;
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
