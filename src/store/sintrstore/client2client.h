// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/sintr/client2client.h:
 *   Sintr client to client interface.
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

#ifndef _SINTR_CLIENT2CLIENT_H_
#define _SINTR_CLIENT2CLIENT_H_


#include "lib/keymanager.h"
#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/crypto.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "store/common/timestamp.h"
#include "store/common/truetime.h"
#include "store/common/transaction.h"
#include "store/common/partitioner.h"
#include "store/common/common-proto.pb.h"
#include "store/sintrstore/sintr-proto.pb.h"
#include "store/common/pinginitiator.h"
#include "store/sintrstore/common.h"
#include "store/sintrstore/validation_client.h"
#include "store/sintrstore/validation_parse_client.h"
#include "store/sintrstore/endorsement_client.h"
#include "store/sintrstore/policy/policy.h"
#include "store/sintrstore/sql_interpreter.h"

#include <map>
#include <string>
#include <vector>
#include <set>
#include <atomic>
#include <shared_mutex>

#include "tbb/concurrent_queue.h"

namespace sintrstore {

class Client2Client : public TransportReceiver, public PingInitiator, public PingTransport {
 public:
  Client2Client(transport::Configuration *config, transport::Configuration *clients_config, Transport *transport,
      uint64_t client_id, uint64_t nshards, uint64_t ngroups, int group, bool pingClients,
      Parameters params, KeyManager *keyManager, Verifier *verifier,
      Partitioner *part, EndorsementClient *endorseClient, SQLTransformer *sql_interpreter, std::string &table_registry,
      const std::vector<std::string> &keys = std::vector<std::string>());
  virtual ~Client2Client();

  virtual void ReceiveMessage(const TransportAddress &remote,
      const std::string &type, const std::string &data,
      void *meta_data) override;

  virtual bool SendPing(size_t replica, const PingMessage &ping);

  // start up the sintr validation for current transaction
  // sends BeginValidateTxnMessage to peers
  // policy is the estimated policy for the transaction
  void SendBeginValidateTxnMessage(uint64_t client_seq_num, const TxnState &protoTxnState, uint64_t txnStartTime,
    PolicyClient *policyClient);

  // forward server read reply to other peers
  void SendForwardReadResultMessage(const std::string &key, const std::string &value, const Timestamp &ts,
    const proto::CommittedProof &proof, const std::string &serializedWrite, const std::string &serializedWriteTypeName, 
    const proto::Dependency &dep, bool hasDep, bool addReadset, const proto::Dependency &policyDep, bool hasPolicyDep);
  
  // forward server point query result to other peers
  void SendForwardPointQueryResultMessage(const std::string &key, const std::string &value, const Timestamp &ts,
    const std::string &table_name, const proto::CommittedProof &proof,
    const std::string &serializedWrite, const std::string &serializedWriteTypeName,
    const proto::Dependency &dep, bool hasDep, bool addReadset);
  
  // forward query results to other clients
  void SendForwardQueryResultMessage(const std::string &query_gen_id, const std::string &query_result, 
    const proto::QueryResultMetaData &query_res_meta,
    const std::map<uint64_t, std::vector<proto::SignedMessage>> &group_sigs, bool addReadset);

  // send a blind write message to other clients
  // indicates that blind writes can now be written locally
  void SendBlindWriteMessage();

  // given a new policy, update the endorsement policy for this client 
  // also contact additional peers as necessary
  void HandlePolicyUpdate(const Policy *policy);

  void SetFailureFlag(bool f) {
    failureActive = f;
  }

 private:

  // contains necessary information for ValidationClient to validate
  struct ValidationInfo {
    ValidationInfo(uint64_t txn_client_id, uint64_t txn_client_seq_num, Timestamp txn_ts,
        ValidationTransaction *valTxn, TransportAddress *remote) : 
        txn_client_id(txn_client_id), txn_client_seq_num(txn_client_seq_num), txn_ts(txn_ts),
        valTxn(valTxn), remote(remote) {
      struct timespec ts_start;
      clock_gettime(CLOCK_MONOTONIC, &ts_start);
      start_time_us = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;
    }
    ~ValidationInfo() {
      delete valTxn;
      delete remote;
    }
    // client id that initiated this validation
    uint64_t txn_client_id;
    // sequence number of transaction on initiating client
    uint64_t txn_client_seq_num;
    // timestamp chosen for this transaction
    Timestamp txn_ts;
    // actual transaction that we can call Validate on
    ValidationTransaction *valTxn;
    // address of initiating client
    TransportAddress *remote;
    // boolean whether or not this is a policy transaction
    bool isPolicyTransaction;
    // start time in microseconds
    uint64_t start_time_us;
  };

  // for sending/receiving messages from other clients
  struct Client2ClientMessageExecutor {
    Client2ClientMessageExecutor(std::function<void*(void)> f) : f(std::move(f)) {}
    std::function<void*(void)> f;
  };

  // this represents a resizing buffer but does not eagerly delete everything on clear
  // instead it will delete buffer elements as they are replaced
  template <typename T>
  struct LazyBuffer {
    LazyBuffer() : size(0) {}
    ~LazyBuffer() {
      for (auto &b : buffer) {
        delete b;
      }
      buffer.clear();
    }
    void insert(T *t) {
      if (size < buffer.size()) {
        delete buffer[size];
        buffer[size] = t;
      }
      else {
        buffer.push_back(t);
      }
      size++;
    }
    T *operator[](size_t i) {
      if (i >= size) {
        Panic("LazyBuffer index out of bounds");
      }
      return buffer[i];
    }
    size_t getSize() {
      return size;
    }
    void clear() {
      size = 0;
    }
    // iterators
    typename std::vector<T *>::iterator begin() {
      return buffer.begin();
    }
    typename std::vector<T *>::iterator end() {
      return buffer.begin() + size;
    }

    std::vector<T *> buffer;
    size_t size;
  };

  struct AsyncQuerySigCheck {
    AsyncQuerySigCheck(uint64_t resultQuorum) : resultQuorum(resultQuorum), num_check_passed(0), num_finished(0) {} 

    const uint64_t resultQuorum;
    std::atomic<uint64_t> num_check_passed;
    std::atomic<uint64_t> num_finished;
  };
  
  void SendForwardReadResultMessageHelper(const uint64_t client_seq_num, 
    const std::string &key, const std::string &value, const Timestamp &ts,
    const proto::CommittedProof &proof, const std::string &serializedWrite, const std::string &serializedWriteTypeName, 
    const proto::Dependency &dep, bool hasDep, bool addReadset, const proto::Dependency &policyDep, bool hasPolicyDep);
  
  void SendForwardPointQueryResultMessageHelper(const uint64_t client_seq_num,
    const std::string &key, const std::string &value, const Timestamp &ts,
    const std::string &table_name, const proto::CommittedProof &proof,
    const std::string &serializedWrite, const std::string &serializedWriteTypeName,
    const proto::Dependency &dep, bool hasDep, bool addReadset);
  
  void SendForwardQueryResultMessageHelper(const uint64_t client_seq_num,
    const std::string &query_gen_id, const std::string &query_result,
    const proto::QueryResultMetaData &query_res_meta,
    const std::map<uint64_t, std::vector<proto::SignedMessage>> &group_sigs, bool addReadset);
  
  void SendBlindWriteMessageHelper(const uint64_t client_seq_num);

  void ManageDispatchBeginValidateTxnMessage(const TransportAddress &remote, const std::string &data);
  void ManageDispatchForwardReadResultMessage(const TransportAddress &remote, const std::string &data);
  void ManageDispatchForwardPointQueryResultMessage(const TransportAddress &remote, const std::string &data);
  void ManageDispatchForwardQueryResultMessage(const TransportAddress &remote, const std::string &data);
  void ManageDispatchBlindWriteMessage(const TransportAddress &remote, const std::string &data);
  void ManageDispatchFinishValidateTxnMessage(const TransportAddress &remote, const std::string &data);

  void HandleBeginValidateTxnMessage(const TransportAddress &remote, const proto::BeginValidateTxnMessage &beginValTxnMsg);
  void HandleForwardReadResultMessage(const proto::ForwardReadResultMessage &fwdReadResultMsg);
  void HandleForwardPointQueryResultMessage(const proto::ForwardPointQueryResultMessage &fwdPointQueryResultMsg);
  void HandleForwardQueryResultMessage(const proto::ForwardQueryResultMessage &fwdQueryResultMsg);
  void HandleBlindWriteMessage(const proto::BlindWriteMessage &blindWriteMsg);
  void HandleFinishValidateTxnMessage(const proto::FinishValidateTxnMessage &finishValTxnMsg);

  // check if fwdReadResultMsg is valid based on either prepared dependency or committed proof
  // also extract write and dep from fwdReadResultMsg
  bool CheckPreparedCommittedEvidence(const proto::ForwardReadResultMessage &fwdReadResultMsg, 
    proto::Write &write, proto::Dependency &dep, const proto::ForwardReadResult &fwdReadResult);
  // check if fwdPointQueryResultMsg is valid based on either prepared dependency or committed proof
  // also extract write and dep from fwdPointQueryResultMsg
  bool CheckPreparedCommittedEvidence(const proto::ForwardPointQueryResultMessage &fwdPointQueryResultMsg, 
    proto::Write &write, proto::Dependency &dep, const proto::ForwardReadResult &fwdPointQueryResult);
  // check if fwdQueryResult is valid based on f+1 matching server responses in fwdQueryResultMsg
  bool CheckPreparedCommittedEvidence(const proto::ForwardQueryResult &fwdQueryResult,
    const proto::ForwardQueryResultMessage &fwdQueryResultMsg);
  // helper for query result check evidence
  bool CheckQuerySigHelper(const proto::SignedMessage &query_sig,
    const std::string &query_gen_id, const std::string &query_result,
    const proto::ReadSet &query_read_set, const std::string &query_read_set_hash);

  // extract client ids not currently in beginValSent from policy satisfying set
  void ExtractFromPolicyClientsToContact(const std::vector<int> &policySatSet, std::set<uint64_t> &clients);

  void ValidationThreadFunction();
  void Client2ClientMessageThreadFunction();

  bool ValidateHMACedMessage(const proto::SignedMessage &signedMessage, std::string &data);
  // create an hmac from msg and place into signature
  void CreateHMACedMessage(const ::google::protobuf::Message &msg, proto::SignedMessage& signedMessage);

  const uint64_t client_id; // Unique ID for this client.
  Transport *transport; // Transport layer.
  // client to server transport configuration state
  transport::Configuration *config;
  // client to client transport configuration state
  transport::Configuration *clients_config;
  // Number of shards.
  uint64_t nshards;
  // Number of replica groups.
  uint64_t ngroups;
  const int group; // which group this client belongs to
  const bool pingClients;
  const Parameters params;
  KeyManager *keyManager;
  Verifier *verifier;
  Verifier *clients_verifier;
  Partitioner *part;
  bool failureActive;
  // for keySelector based benchmark validation, need copy of keys for validator as well
  const std::vector<std::string> &keys;
  // current transaction sequence number (to send to others)
  uint64_t client_seq_num;
  // current set of transport ids begin validation message has been sent to
  std::set<uint64_t> beginValSent;
  // track most recently sent begin validation message
  proto::BeginValidateTxnMessage sentBeginValTxnMsg;
  // track all sent forward read/query results for current transaction
  LazyBuffer<::google::protobuf::Message> sentFwdResults;
  mutable std::shared_mutex sentFwdResultsMutex;
  // endorsement client can inform client of received validations
  EndorsementClient *endorseClient;

  // TODO: actually initialize and use this
  SQLTransformer *sql_interpreter;

  // threads for validation
  std::vector<std::thread *> valThreads;
  std::atomic<bool> done;
  ValidationClient *valClient;
  ValidationParseClient *valParseClient;
  // concurrent queue of transactions to be validated, has blocking semantics for pop
  tbb::concurrent_bounded_queue<ValidationInfo *> validationQueue;

  // separate thread for message processing (send/receive), stays sequential
  std::thread *c2cThread;
  // concurrent queue of messages to be processed
  tbb::concurrent_bounded_queue<Client2ClientMessageExecutor *> c2cQueue;

  // for hmacs
  std::unordered_map<uint64_t, std::string> sessionKeys;

  // for received messages
  proto::BeginValidateTxnMessage beginValTxnMsg;
  proto::ForwardReadResultMessage fwdReadResultMsg;
  proto::ForwardPointQueryResultMessage fwdPointQueryResultMsg;
  proto::ForwardQueryResultMessage fwdQueryResultMsg;
  proto::BlindWriteMessage blindWriteMsg;
  proto::FinishValidateTxnMessage finishValTxnMsg;
  PingMessage ping;

  uint64_t send_begin_time_us;
  uint64_t send_fwd_read_time_us;
  uint64_t send_fwd_point_query_time_us;

  mean_tracker create_hmac_us;
  mean_tracker verify_hmac_us;
  mean_tracker check_committed_prepared_us;
  mean_tracker send_finish_val_us;
  mean_tracker verify_endorse_us;
  mean_tracker validation_time_us;
  mean_tracker validation_queue_time_us;
  mean_tracker send_begin_to_receive_endorse_us;
  mean_tracker fwd_read_to_receive_endorse_us;
  mean_tracker fwd_point_query_to_receive_endorse_us;
};

} // namespace sintrstore

#endif /* _SINTR_CLIENT2CLIENT_H_ */
