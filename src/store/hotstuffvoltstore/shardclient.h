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
#ifndef _HOTSTUFF_VOLT_SHARDCLIENT_H_
#define _HOTSTUFF_VOLT_SHARDCLIENT_H_

#include "lib/keymanager.h"
#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/crypto.h"
#include "lib/message.h"
#include "lib/transport.h"
#include "store/common/stats.h"
#include "store/common/timestamp.h"
#include "store/common/transaction.h"
#include "store/common/common-proto.pb.h"
#include "store/hotstuffvoltstore/pbft-proto.pb.h"
#include "store/hotstuffvoltstore/server-proto.pb.h"
#include "store/common/query-result-proto.pb.h"
#include <sys/time.h>

#include <map>
#include <string>

namespace hotstuffvoltstore {

// status, key, value
typedef std::function<void(int, const std::string&, const std::string &, const Timestamp&)> read_callback;
typedef std::function<void(int, const std::string&)> read_timeout_callback;

typedef std::function<void(int, const proto::QueryReply&)> query_callback;
typedef std::function<void(int, const proto::GroupedSignedMessage&)> signed_query_callback;
typedef std::function<void(int)> query_timeout_callback;

typedef std::function<void(int, const proto::CommitReply&)> commit_callback;
typedef std::function<void(int, const proto::GroupedSignedMessage&)> signed_commit_callback;
typedef std::function<void(int)> commit_timeout_callback;

typedef std::function<void(int, const proto::AbortReply&)> abort_callback;
typedef std::function<void(int, const proto::GroupedSignedMessage&)> signed_abort_callback;
typedef std::function<void(int)> abort_timeout_callback;

typedef std::function<void(int, const proto::TransactionDecision&)> prepare_callback;
typedef std::function<void(int, const proto::GroupedSignedMessage&)> signed_prepare_callback;
typedef std::function<void(int)> prepare_timeout_callback;

typedef std::function<void(int)> writeback_callback;
typedef std::function<void(int)> writeback_timeout_callback;

class ShardClient : public TransportReceiver {
 public:
  /* Constructor needs path to shard config. */
  ShardClient(const transport::Configuration& config, Transport *transport,
      uint64_t client_id, uint64_t group_idx, const std::vector<int> &closestReplicas_,
      bool signMessages, bool validateProofs,
      KeyManager *keyManager, Stats* stats, bool order_commit = false, bool validate_abort = false);
  ~ShardClient();

  void ReceiveMessage(const TransportAddress &remote,
      const std::string &type, const std::string &data,
      void *meta_data);

  // Get the value corresponding to key.
  void Get(const std::string &key, const Timestamp &ts,
      uint64_t readMessages, uint64_t numResults, read_callback gcb, read_timeout_callback gtcb,
      uint32_t timeout);

  void Query(const string&, const Timestamp&, uint64_t, uint64_t, 
    query_callback, query_timeout_callback, uint32_t);

  // send a request with this as the packed message
  void Prepare(const proto::Transaction& txn, prepare_callback pcb,
      prepare_timeout_callback ptcb, uint32_t timeout);
  void SignedPrepare(const proto::Transaction& txn, signed_prepare_callback pcb,
      prepare_timeout_callback ptcb, uint32_t timeout);

  // void Commit(const std::string& txn_digest, const proto::ShardDecisions& dec,
  //     writeback_callback wcb, writeback_timeout_callback wtcp, uint32_t timeout);
  void Commit(
    uint64_t client_id, uint64_t txn_seq_num, commit_callback ccb, commit_timeout_callback ctcb,
    uint32_t timeout);
  void CommitSigned(const std::string& txn_digest, const proto::ShardSignedDecisions& dec,
      writeback_callback wcb, writeback_timeout_callback wtcp, uint32_t timeout);

  void CommitSigned(const std::string& txn_digest, const proto::ShardSignedDecisions& dec);

  // void Abort(std::string& txn_digest, const proto::ShardSignedDecisions& dec);
  void Abort(uint64_t client_id, uint64_t txn_seq_num, abort_callback acb, 
  abort_timeout_callback atcb, uint32_t timeout);

 private:
   uint64_t start_time;
   uint64_t total_elapsed = 0 ;
   uint64_t total_prepare = 0;

  transport::Configuration config;
  Transport *transport; // Transport layer.
  int group_idx; // which shard this client accesses
  bool signMessages;
  bool validateProofs;
  KeyManager *keyManager;

  //addtional knobs: 1) order commit, 2) validate abort
  bool order_commit = false;
  bool validate_abort = false;

  uint64_t readReq;
  uint64_t queryReq;
  uint64_t commitReq;
  uint64_t abortReq;
  std::vector<int> closestReplicas;
  inline size_t GetNthClosestReplica(size_t idx) const {
    return closestReplicas[idx];
  }

  struct PendingRead {
    // the set of ids that we have received a read reply for
    std::unordered_set<uint64_t> receivedReplies;
    // the max read timestamp for a valid reply
    Timestamp maxTs;
    std::string maxValue;
    proto::CommitProof maxCommitProof;

    // the current status of the reply (default to fail)
    uint64_t status;

    read_callback rcb;
    uint64_t numResultsRequired;

    Timeout* timeout;
  };



  std::string CreateValidQueryDecision(SQLResult digest);
  std::string CreateFailedQueryDecision(SQLResult digest);
  std::string CreateValidCommitDecision();
  std::string CreateFailedCommitDecision();
  std::string CreateValidAbortDecision();
  std::string CreateFailedAbortDecision();

  struct PendingQuery { //Will need to add a pendingSignedQuery that can manage signed messages
    // the set of ids that we have received a query reply for
    proto::QueryReply validQueryRes;

    std::unordered_map<std::string, std::unordered_set<uint64_t>> receivedOkIds;
    std::unordered_set<uint64_t> receivedFailedIds;
    // the max query timestamp for a valid reply
    Timestamp maxTs;
    // std::string maxValue;
    // proto::CommitProof maxCommitProof;

    // the current status of the reply (default to fail)
    uint64_t status;

    query_callback qcb;
    // uint64_t numResultsRequired;

    Timeout* timeout;
  };

  struct PendingSignedQuery {
    // the serialized packed message containing the valid transaction decision
    std::string validQueryResPacked;
    std::string failedQueryResPacked;
    // map from id to valid signature
    std::unordered_map<uint64_t, std::string> receivedValidSigs;
    std::unordered_map<uint64_t, std::string> receivedFailedSigs;
    std::unordered_set<uint64_t> receivedFailedIds;
    signed_query_callback qcb;

    Timeout* timeout;
  };

  struct PendingCommit {
    proto::CommitReply validDecision;
    // if we get f+1 valid decs -> return ok
    std::unordered_set<uint64_t> receivedOkIds;
    // else, once we get f+1 failures -> return failed
    std::unordered_set<uint64_t> receivedFailedIds;
    commit_callback ccb;

    Timestamp maxTs;
    uint64_t status;

    Timeout* timeout;
  };

  struct PendingAbort {
    proto::AbortReply validDecision;
    // if we get f+1 valid decs -> return ok
    std::unordered_set<uint64_t> receivedOkIds;
    // else, once we get f+1 failures -> return failed
    std::unordered_set<uint64_t> receivedFailedIds;
    abort_callback acb;

    Timestamp maxTs;
    uint64_t status;


    Timeout* timeout;
  };

  struct PendingSignedCommit {

    std::string validCommitResPacked;
    std::string failedCommitResPacked;
    // map from id to valid signature
    std::unordered_map<uint64_t, std::string> receivedValidSigs;
    std::unordered_map<uint64_t, std::string> receivedFailedSigs;
    std::unordered_set<uint64_t> receivedFailedIds;
    signed_commit_callback ccb;

    Timeout* timeout;
  };

  struct PendingSignedAbort {
    std::string validAbortResPacked;
    std::string failedAbortResPacked;
    // map from id to valid signature
    std::unordered_map<uint64_t, std::string> receivedValidSigs;
    std::unordered_map<uint64_t, std::string> receivedFailedSigs;
    std::unordered_set<uint64_t> receivedFailedIds;
    signed_abort_callback acb;

    Timeout* timeout;
  };
  

  void HandleReadReply(const proto::ReadReply& reply, const proto::SignedMessage& signedMsg);
  void HandleQueryReply(const proto::QueryReply& queryReply, const proto::SignedMessage& signedMsg);
  void HandleCommitReply(const proto::CommitReply& commitReply, const proto::SignedMessage& signedMsg);
  void HandleAbortReply(const proto::AbortReply& abortReply, const proto::SignedMessage& signedMsg);

  std::string CreateValidPackedDecision(std::string digest);
  std::string CreateFailedPackedDecision(std::string digest);

  struct PendingPrepare {
    proto::TransactionDecision validDecision;
    // if we get f+1 valid decs -> return ok
    std::unordered_set<uint64_t> receivedOkIds;
    // else, once we get f+1 failures -> return failed
    std::unordered_set<uint64_t> receivedFailedIds;
    prepare_callback pcb;

    Timeout* timeout;
  };

  struct PendingSignedPrepare {
    // the serialized packed message containing the valid transaction decision
    std::string validDecisionPacked;
    std::string failedDecisionPacked;
    // map from id to valid signature
    std::unordered_map<uint64_t, std::string> receivedValidSigs;
    std::unordered_map<uint64_t, std::string> receivedFailedSigs;
    std::unordered_set<uint64_t> receivedFailedIds;
    signed_prepare_callback pcb;

    Timeout* timeout;
  };

  void HandleTransactionDecision(const proto::TransactionDecision& transactionDecision, const proto::SignedMessage& signedMsg);

  struct PendingWritebackReply {
    // set of processes we have received writeback acks from
    std::unordered_set<uint64_t> receivedAcks;
    std::unordered_set<uint64_t> receivedFails;
    writeback_callback wcb;

    Timeout* timeout;
  };

  void HandleWritebackReply(const proto::GroupedDecisionAck& groupedDecisionAck, const proto::SignedMessage& signedMsg);

  // req id to (read)
  std::unordered_map<uint64_t, PendingRead> pendingReads;
  std::unordered_map<std::string, PendingPrepare> pendingPrepares;
  std::unordered_map<std::string, PendingSignedPrepare> pendingSignedPrepares;
  std::unordered_map<std::string, PendingWritebackReply> pendingWritebacks;
  std::unordered_map<uint64_t, PendingQuery> pendingQueries;
  std::unordered_map<uint64_t, PendingSignedQuery> pendingSignedQueries;
  std::unordered_map<uint64_t, PendingCommit> pendingCommits;
  std::unordered_map<uint64_t, PendingSignedCommit> pendingSignedCommits;
  std::unordered_map<uint64_t, PendingAbort> pendingAborts;
  std::unordered_map<uint64_t, PendingSignedAbort> pendingSignedAborts;


  // verify that the proof asserts that the the value was written to the key
  // at the given timestamp
  bool validateReadProof(const proto::CommitProof& commitProof, const std::string& key,
    const std::string& value, const Timestamp& timestamp);


  Stats* stats;
};

} // namespace hotstuffvoltstore

#endif /* _INDICUS_SHARDCLIENT_H_ */
