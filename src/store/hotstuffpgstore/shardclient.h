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
#ifndef _HOTSTUFF_PG_SHARDCLIENT_H_
#define _HOTSTUFF_PG_SHARDCLIENT_H_

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
#include "store/hotstuffpgstore/pbft-proto.pb.h"
#include "store/common/query_result/query-result-proto.pb.h"
#include "store/hotstuffpgstore/server-proto.pb.h"
#include <sys/time.h>

#include <map>
#include <string>

namespace hotstuffpgstore {

// status, key, value

typedef std::function<void(int, const std::string&)> inquiry_callback;
typedef std::function<void(int)> inquiry_timeout_callback;

typedef std::function<void(int)> apply_callback;
typedef std::function<void()> apply_timeout_callback;

typedef std::function<void(int)> rollback_callback;
typedef std::function<void()> rollback_timeout_callback;

class ShardClient : public TransportReceiver {
 public:
  /* Constructor needs path to shard config. */
  ShardClient(const transport::Configuration& config, Transport *transport,
      uint64_t client_id, uint64_t group_idx, const std::vector<int> &closestReplicas_,
      bool signMessages, bool validateProofs,
      KeyManager *keyManager, Stats* stats, bool order_commit = false, bool validate_abort = false,
      bool deterministic = false);
  ~ShardClient();

  void ReceiveMessage(const TransportAddress &remote,
      const std::string &type, const std::string &data,
      void *meta_data);

  void Query(const std::string &query,  const Timestamp &ts, uint64_t client_id, int client_seq_num, 
      inquiry_callback icb, inquiry_timeout_callback itcb,  uint32_t timeout);

  void Commit(const std::string& txn_digest, const Timestamp &ts, uint64_t client_id, int client_seq_num, 
      apply_callback acb, apply_timeout_callback atcb, uint32_t timeout);

  void Abort(const std::string& txn_digest, uint64_t client_id, int client_seq_num);

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

  // This flag is to determine if the test should run deterministically.
  // If this is false, then results are returned based on f + 1 including the
  // leader's results to get consistent results. If it is, then it is based on a 
  // simple f + 1, returning the result of any replica's execution
  bool deterministic;

  //addtional knobs: 1) order commit, 2) validate abort
  bool order_commit = false;
  bool validate_abort = false;

  uint64_t readReq;
  uint64_t inquiryReq;
  uint64_t applyReq;
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

    uint64_t numResultsRequired;

    Timeout* timeout;
  };

  struct PendingInquiry {
    // the set of ids that we have received a read reply for
    std::unordered_map<std::string, std::unordered_set<uint64_t>> receivedReplies;
    std::unordered_set<uint64_t> receivedSuccesses;
    std::string leaderReply;
    std::unordered_set<uint64_t> receivedFails;
    proto::CommitProof maxCommitProof;

    // the current status of the reply (default to fail)
    uint64_t status;

    inquiry_callback icb;
    uint64_t numReceivedReplies;

    Timeout* timeout;
  };
  
  struct PendingApply {
    // the set of ids that we have received a read reply for
    std::unordered_set<uint64_t> receivedAcks;
    std::unordered_set<uint64_t> receivedFails;
    proto::CommitProof maxCommitProof;

    // the current status of the reply (default to fail)
    uint64_t status;

    apply_callback acb;

    Timeout* timeout;
  };

  void HandleInquiryReply(const proto::InquiryReply& reply, const proto::SignedMessage& signedMsg);
  void InquiryReplyHelper(PendingInquiry* pendingInquiry, const std::string inquiryReply, 
    uint64_t reqId, uint64_t status);

  void HandleApplyReply(const proto::ApplyReply& reply, const proto::SignedMessage& signedMsg);

  // req id to (read)
  std::unordered_map<uint64_t, PendingInquiry> pendingInquiries;
  std::unordered_map<uint64_t, PendingApply> pendingApplies;

  Stats* stats;
};

} // namespace hotstuffpgstore

#endif /* _INDICUS_SHARDCLIENT_H_ */
