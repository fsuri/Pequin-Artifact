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
#include "store/pg_SMRstore/pbft-proto.pb.h"
#include "store/common/query_result/query-result-proto.pb.h"
#include "store/pg_SMRstore/server-proto.pb.h"
#include <sys/time.h>

#include "store/pg_SMRstore/bftsmartagent.h"

#include <map>
#include <string>

namespace pg_SMRstore {

// status, key, value

typedef std::function<void(int, const std::string&)> sql_rpc_callback;
typedef std::function<void(int)> sql_rpc_timeout_callback;

typedef std::function<void(int)> try_commit_callback;
typedef std::function<void()> try_commit_timeout_callback;

typedef std::function<void(int)> user_abort_callback;
typedef std::function<void()> user_abort_timeout_callback;

class ShardClient : public TransportReceiver {
 public:
  /* Constructor needs path to shard config. */
  ShardClient(const transport::Configuration& config, Transport *transport,
      uint64_t client_id, uint64_t group_idx, const std::vector<int> &closestReplicas_,
      bool signMessages, bool validateProofs,
      KeyManager *keyManager, Stats* stats,
      bool fake_SMR = false, uint64_t SMR_mode = 0, const std::string& PG_BFTSMART_config_path = "");
  ~ShardClient();

  void ReceiveMessage(const TransportAddress &remote, const std::string &type, const std::string &data, void *meta_data);

  void Query(const std::string &query, uint64_t client_id, uint64_t client_seq_num, sql_rpc_callback srcb, sql_rpc_timeout_callback srtcb,  uint32_t timeout);

  void Commit(uint64_t client_id, uint64_t client_seq_num, try_commit_callback tccb, try_commit_timeout_callback tctcb, uint32_t timeout);

  void Abort(uint64_t client_id, uint64_t client_seq_num);

 private:
  pg_SMRstore::BftSmartAgent* bftsmartagent;
  void SendMessageToGroup_viaBFTSMART(proto::Request& msg, int group_idx);

  uint64_t start_time;
  uint64_t total_elapsed = 0 ;
  uint64_t total_prepare = 0;

  uint64_t client_id;
  uint64_t reqId;

  transport::Configuration config;
  Transport *transport; // Transport layer.
  int group_idx; // which shard this client accesses
  bool signMessages;
  bool validateProofs;
  KeyManager *keyManager;

  // If this flag is set, then we are simulating a fake SMR in which we only care about the reply from a single replica ("leader").
  //We use this to simulate an upper bound of performance that would be achievable with a parallel SMR execution engine (akin to Block-STM)
  bool fake_SMR;
  uint64_t SMR_mode;


  std::vector<int> closestReplicas;
  inline size_t GetNthClosestReplica(size_t idx) const {
    return closestReplicas[idx];
  }

  
  //REPLY HANDLING

  struct PendingSQL_RPC {
    PendingSQL_RPC(): hasLeaderReply(false), leaderReply(""), status(REPLY_FAIL), numReceivedReplies(0){
      receivedReplies.clear();
    }

    // the current status of the reply (default to fail)
    uint64_t status;
    
    sql_rpc_callback srcb;
    Timeout* timeout;

    // the set of ids that we have received a read reply for. 
    // Note: If running deterministically (DEPRECATED), then all replies must match. (non-matching = byz) If running non-deterministic (ONLY CURRENTLY SUPPORTED MODE) we only care about the leader reply.
    std::unordered_map<std::string, std::unordered_set<uint64_t>> receivedReplies;
    uint64_t numReceivedReplies;

    bool hasLeaderReply;
    std::string leaderReply;
  };
  
  struct PendingTryCommit {
    PendingTryCommit(): hasLeaderReply(false), numReceivedReplies(0UL)
    {
      receivedReplies.clear();
    }

    // the current status of the reply (default to fail)
    uint64_t status;
    try_commit_callback tccb;
    Timeout* timeout;

    // the set of ids that we have received a read reply for
    std::unordered_map<uint64_t, std::unordered_set<uint64_t>> receivedReplies;
    uint64_t numReceivedReplies;

    bool hasLeaderReply; 
  };

  proto::SignedMessage signedMessage;
  proto::SQL_RPCReply sql_rpcReply;
  proto::TryCommitReply tryCommitReply;

  int ValidateAndExtractData(const std::string &t, const std::string &d, std::string &type, std::string &data);

  void HandleSQL_RPCReply(const proto::SQL_RPCReply& reply, int replica_id);
    void SQL_RPCReplyHelper(PendingSQL_RPC &PendingSQL_RPC, const std::string sql_rpcReply, uint64_t req_id, uint64_t status);

  void HandleTryCommitReply(const proto::TryCommitReply& reply, int replica_id);
    void TryCommitReplyHelper(PendingTryCommit &pendingTryCommit, uint64_t req_id, uint64_t status); 



  // req id to (read)
  std::unordered_map<uint64_t, PendingSQL_RPC> pendingSQL_RPCs;
  std::unordered_map<uint64_t, PendingTryCommit> pendingTryCommits;

  Stats* stats;
};

} // namespace pg_SMRstore

#endif /* _INDICUS_SHARDCLIENT_H_ */
