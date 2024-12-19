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

#include <map>
#include <string>
#include <vector>
#include <set>

#include "tbb/concurrent_queue.h"

namespace sintrstore {

class Client2Client : public TransportReceiver, public PingInitiator, public PingTransport {
 public:
  Client2Client(transport::Configuration *config, transport::Configuration *clients_config, Transport *transport,
      uint64_t client_id, uint64_t nshards, uint64_t ngroups, int group, bool pingClients,
      Parameters params, KeyManager *keyManager, Verifier *verifier,
      Partitioner *part,  EndorsementClient *endorseClient);
  virtual ~Client2Client();

  virtual void ReceiveMessage(const TransportAddress &remote,
      const std::string &type, const std::string &data,
      void *meta_data) override;

  virtual bool SendPing(size_t replica, const PingMessage &ping);

  // start up the sintr validation for current transaction
  // txnState should be parsable as proto::TxnState
  // sends BeginValidateTxnMessage to peers
  void SendBeginValidateTxnMessage(uint64_t client_seq_num, const std::string &txnState, uint64_t txnStartTime);

  // forward server read reply to other peers
  void ForwardReadResultMessage(const std::string &key, const std::string &value, const Timestamp &ts,
    const proto::CommittedProof &proof, const std::string &serializedWrite, const std::string &serializedWriteTypeName, 
    const proto::Dependency &dep, bool hasDep, bool addReadset);

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
        valTxn(valTxn), remote(remote) {}
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
  };
  
  void HandleBeginValidateTxnMessage(const TransportAddress &remote, const proto::BeginValidateTxnMessage &beginValTxnMsg);
  void HandleForwardReadResultMessage(const proto::ForwardReadResultMessage &fwdReadResultMsg);
  void HandleFinishValidateTxnMessage(const proto::FinishValidateTxnMessage &finishValTxnMsg);
  // check if fwdReadResultMsg is valid based on either prepared dependency or committed proof
  // also extract write and dep from fwdReadResultMsg
  bool CheckPreparedCommittedEvidence(const proto::ForwardReadResultMessage &fwdReadResultMsg, 
    proto::Write &write, proto::Dependency &dep);
  void ValidationThreadFunction();
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
  // current transaction sequence number (to send to others)
  uint64_t client_seq_num;
  // current set of transport ids begin validation message has been sent to
  std::set<int> beginValSent;
  // track most recently sent begin validation message
  proto::BeginValidateTxnMessage sentBeginValTxnMsg;
  // track all sent forward read results for current transaction
  std::vector<proto::ForwardReadResultMessage> sentFwdReadResults;
  // endorsement client can inform client of received validations
  EndorsementClient *endorseClient;

  // threads for validation
  std::vector<std::thread *> valThreads;
  ValidationClient *valClient;
  ValidationParseClient *valParseClient;
  // concurrent queue of transactions to be validated, has blocking semantics for pop
  tbb::concurrent_bounded_queue<ValidationInfo *> validationQueue;

  // for hmacs
  std::unordered_map<uint64_t, std::string> sessionKeys;

  proto::BeginValidateTxnMessage beginValTxnMsg;
  proto::ForwardReadResultMessage fwdReadResultMsg;
  proto::FinishValidateTxnMessage finishValTxnMsg;
  PingMessage ping;
};

} // namespace sintrstore

#endif /* _SINTR_CLIENT2CLIENT_H_ */
