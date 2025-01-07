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
#ifndef _HOTSTUFF_PG_REPLICA_H_
#define _HOTSTUFF_PG_REPLICA_H_

#include <memory>

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/message.h"
#include "lib/persistent_register.h"
#include "lib/udptransport.h"
#include "lib/crypto.h"
#include "lib/keymanager.h"

#include "store/common/stats.h"
#include "store/pg_SMRstore/pbft-proto.pb.h"
#include "store/pg_SMRstore/app.h"
#include "store/pg_SMRstore/common.h"
#include <mutex>
#include "tbb/concurrent_unordered_map.h"
#include "store/pg_SMRstore/server.h"

// use HotStuff library
// comment out the below macro to switch back to pbftstore
#include "store/hotstuffstore/libhotstuff/examples/indicus_interface.h"

#include "store/pg_SMRstore/bftsmartagent.h"

namespace pg_SMRstore {

static bool TEST_WITHOUT_HOTSTUFF = true;

class Replica : public TransportReceiver {
public:
  Replica(const transport::Configuration &config, KeyManager *keyManager,
    App *app, int groupIdx, int idx, bool signMessages, uint64_t maxBatchSize,
          uint64_t batchTimeoutMS, uint64_t EbatchSize, uint64_t EbatchTimeoutMS, bool primaryCoordinator, bool requestTx, int hotstuffpg_cpu, bool local_config, int numShards, Transport *transport,
          bool fake_SMR = false, int dummyTO = 100, uint64_t SMR_mode = 0, const std::string& PG_BFTSMART_config_path = "");
  ~Replica();

  // Message handlers.
  void ReceiveMessage(const TransportAddress &remote, const std::string &type, const std::string &data, void *meta_data);
    void ReceiveFromBFTSmart(const string &type, const string &data);

  void HandleRequest(const TransportAddress &remote,
                           const proto::Request &msg);

 private:
  void HandleRequest_noHS(const TransportAddress &remote, const proto::Request &request);
  void HandleRequest_noPacked(const TransportAddress &remote, const std::string &type, const std::string &data);
  void HandleRequest_noPacked_shir(const TransportAddress &remote, const std::string &type, const std::string &data);

  uint64_t SMR_mode;

  hotstuffstore::IndicusInterface* hotstuffpg_interface;

  pg_SMRstore::BftSmartAgent* bftsmartagent;
    std::mutex client_cache_mutex;
    std::unordered_map<uint64_t, const TransportAddress*> clientCache;
    std::unordered_map<uint64_t, std::vector<proto::Request>> reqBuffer;



  std::unordered_set<std::string> requests_dup;

  const transport::Configuration &config;
  KeyManager *keyManager;
  App *app;
  int groupIdx;
  int idx; // the replica index within the group
  int id; // unique replica id (across all shards)
  bool signMessages;
  uint64_t maxBatchSize;
  uint64_t batchTimeoutMS;
  uint64_t EbatchSize;
  uint64_t EbatchTimeoutMS;
  bool primaryCoordinator;
  bool requestTx;
  Transport *transport;
  int currentView;
  int nextSeqNum;
  int numShards;
  bool fake_SMR;
  int dummyTO;

  // members to reduce alloc
  proto::Connect connect_msg;
  proto::Request recvrequest;
  proto::RequestRequest recvrr;

  proto::SQL_RPC sql_rpc_template;
  proto::TryCommit try_commit_template;
  proto::UserAbort user_abort_template;

  std::unordered_map<uint64_t, std::string> sessionKeys;

  bool batchTimerRunning;
  int batchTimerId;
  int nextBatchNum;
  // the map from 0..(N-1) to pending digests
  std::unordered_map<uint64_t, std::string> pendingBatchedDigests;
  std::unordered_map<uint64_t, std::string> bStatNames;

  int proposedCounter;
  bool firstReceive;
  
  bool EbatchTimerRunning;
  int EbatchTimerId;
  std::vector<::google::protobuf::Message*> EpendingBatchedMessages;
  std::vector<std::string> EpendingBatchedDigs;
  std::unordered_map<uint64_t, std::string> EbStatNames;

  void ProcessReplies(const std::string &digest, const std::vector<::google::protobuf::Message*> &replies);
  void sendEbatch();
  void delegateEbatch(std::vector<::google::protobuf::Message*> EpendingBatchedMessages_,
     std::vector<std::string> EpendingBatchedDigs_);
  std::vector<proto::SignedMessage*> EsignedMessages;

  // map from digest to received requests
  std::unordered_map<std::string, proto::PackedMessage> requests;

  // map from digest to received requests
  std::unordered_map<std::string, std::pair<std::string, std::string>> unpacked_requests;

  // the next sequence number to be executed
  uint64_t execSeqNum;
  uint64_t execBatchNum;
  uint64_t bubbles;
  
  // map from seqnum to the digest pending execution at that sequence number
  std::unordered_map<uint64_t, std::string> pendingExecutions;

  // map from tx digest to reply address
  //std::unordered_map<std::string, TransportAddress*> replyAddrs;
  tbb::concurrent_unordered_map<std::string, TransportAddress*> replyAddrs;
  //std::mutex replyAddrsMutex;

  void executeSlots();
  void executeSlots_unpacked();

  std::mutex batchMutex;

  void handleMessage(const TransportAddress &remote, const string &type, const string &data);

  void proposeBubble();
  
  void bubbleCB(uint64_t currProposedCounter);
  Stats* stats;


  void RW_TEST();

};

} // namespace pg_SMRstore

#endif
