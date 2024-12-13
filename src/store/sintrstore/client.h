// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/sintrstore/client.h:
 *   Indicus client interface.
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

#ifndef _SINTR_CLIENT_H_
#define _SINTR_CLIENT_H_

#include "lib/assert.h"
#include "lib/keymanager.h"
#include "lib/latency.h"
#include "lib/message.h"
#include "lib/configuration.h"
#include "lib/udptransport.h"
#include "replication/ir/client.h"
#include "store/common/timestamp.h"
#include "store/common/truetime.h"
#include "store/common/frontend/client.h"
#include "store/common/frontend/bufferclient.h"
#include "store/sintrstore/shardclient.h"
#include "store/sintrstore/sintr-proto.pb.h"
#include <sys/time.h>
#include "store/common/stats.h"
#include <unistd.h>

#include <thread>
#include <set>

#include "store/sintrstore/sql_interpreter.h"

#define RESULT_COMMITTED 0
#define RESULT_USER_ABORTED 1
#define RESULT_SYSTEM_ABORTED 2
#define RESULT_MAX_RETRIES 3

namespace sintrstore {

static bool PROFILING_LAT = true; 

static bool TEST_READ_SET = false;  //print out read set

static bool FORCE_SCAN_CACHING = false;
static bool relax_point_cond = true;

static uint64_t start_time = 0;
static uint64_t total_failure_injections=0;
static uint64_t total_writebacks=0;

static int callInvokeFB = 0;

class Client : public ::Client {
 public:
  Client(transport::Configuration *config, uint64_t id, int nShards,
      int nGroups, const std::vector<int> &closestReplicas, bool pingReplicas,
      Transport *transport, Partitioner *part, bool syncCommit,
      uint64_t readMessages, uint64_t readQuorumSize,
      Parameters params, std::string &table_registry,
      KeyManager *keyManager, uint64_t phase1DecisionTimeout,
      uint64_t warmup_secs,
      uint64_t consecutiveMax = 1UL,
      bool sql_bench = false,
      TrueTime timeserver = TrueTime(0,0));
  virtual ~Client();

  // Begin a transaction.
  virtual void Begin(begin_callback bcb, begin_timeout_callback btcb,
      uint32_t timeout, bool retry = false, const std::string &txnState = std::string()) override;

  // Get the value corresponding to key.
  virtual void Get(const std::string &key, get_callback gcb,
      get_timeout_callback gtcb, uint32_t timeout = GET_TIMEOUT) override;

  // Set the value for the given key.
  virtual void Put(const std::string &key, const std::string &value,
      put_callback pcb, put_timeout_callback ptcb,
      uint32_t timeout = PUT_TIMEOUT) override;

  virtual void SQLRequest(std::string &statement, sql_callback scb,
    sql_timeout_callback stcb, uint32_t timeout) override;

  virtual void Write(std::string &write_statement, write_callback wcb,
      write_timeout_callback wtcb, uint32_t timeout, bool blind_write = false) override;

  virtual void Query(const std::string &query, query_callback qcb,
    query_timeout_callback qtcb, uint32_t timeout, bool cache_result = false, bool skip_query_interpretation = false) override; //TODO: ::Client client class needs to expose Query interface too.. --> All other clients need to support the interface.

  // Commit all Get(s) and Put(s) since Begin().
  virtual void Commit(commit_callback ccb, commit_timeout_callback ctcb,
      uint32_t timeout) override;

  // Abort all Get(s) and Put(s) since Begin().
  virtual void Abort(abort_callback acb, abort_timeout_callback atcb,
      uint32_t timeout) override;

  //inline const Stats &GetStats() const { return stats; }
 private:
   //Stats stats;
   uint64_t consecutiveMax;
   uint64_t faulty_counter;
   int fast_path_counter;
   int total_counter;
   std::unordered_set<uint64_t> conflict_ids;

   bool warmup_done = false;

  //Query protocol structures and functions

  struct PendingQuery {
    PendingQuery(Client *client, uint64_t query_seq_num, const std::string &query_cmd, const query_callback &qcb, bool cache_result) : version(0UL), group_replies(0UL), qcb(qcb), cache_result(cache_result){
      queryMsg.Clear();
      queryMsg.set_client_id(client->client_id);
      queryMsg.set_query_seq_num(query_seq_num);
      *queryMsg.mutable_query_cmd() = std::move(query_cmd);
      *queryMsg.mutable_timestamp() = client->txn.timestamp();
      queryMsg.set_retry_version(0);
    }
    ~PendingQuery(){
       ClearReplySets();
    }

   void ClearReplySets(){
    for(auto [group, rs]: group_read_sets){
        if(rs!=nullptr) delete rs;
    }
    group_read_sets.clear();
    group_result_hashes.clear();
   }

   void SetInvolvedGroups(std::vector<uint64_t> &involved_groups_){
      involved_groups = std::move(involved_groups_);
      queryMsg.set_query_manager(involved_groups[0]);
    }

    void SetQueryId(Client *client){
      bool hash_query_id = client->params.query_params.signClientQueries && client->params.query_params.cacheReadSet && client->params.hashDigest;
      queryId = QueryDigest(queryMsg, hash_query_id); 

      // if(client->params.query_params.signClientQueries && client->params.query_params.cacheReadSet){ //TODO: when to use hash id? always?
      //     queryId = QueryDigest(queryMsg, client->params.hashDigest); 
      // }
      // else{
      //     queryId =  "[" + std::to_string(queryMsg.query_seq_num()) + ":" + std::to_string(queryMsg.client_id()) + "]";
      // }
    }
    bool cache_result;
    query_callback qcb;

    uint64_t version;
    std::string queryId;

    proto::Query queryMsg;
    
    std::vector<uint64_t> involved_groups;
    //std::map<uint64_t, std::map<std::string, TimestampMessage>> group_read_sets;
    std::map<uint64_t, proto::ReadSet*> group_read_sets;
    std::map<uint64_t, std::string> group_result_hashes;
    std::string result;
    uint64_t group_replies;
   
    bool is_point;
    std::string key;
    std::string table_name;
    std::vector<std::string> p_col_values; //if point read: this contains primary_key_col_vaues (in order) ==> Together with table_name can be used to compute encoding.
  };
  std::map<uint64_t, PendingQuery*> pendingQueries;

  SQLTransformer sql_interpreter;
  std::vector<std::string> pendingWriteStatements; //Just a temp cache to keep Translated Write statements in scope during a TX.
  std::map<std::string, std::string> point_read_cache; // Cache the read results from point reads. 
                                                      // If we want to do a Point Update afterwards, then we can use the cache to skip straight to a put.
                                                      // Note: Only works if we did Select * in the first point read. (Can improve this if we make Updates Put deltas instead of full row client side)
  std::map<std::string, std::string> scan_read_cache; //Cache results from scan reads (only for Select *)

  void TestReadSet(PendingQuery *pendingQuery);
  void PointQueryResultCallback(PendingQuery *pendingQuery,  
                            int status, const std::string &key, const std::string &result, const Timestamp &read_time, const proto::Dependency &dep, bool hasDep, bool addReadSet); 
  void QueryResultCallback(PendingQuery *pendingQuery,      //bound parameters
                            int status, int group, proto::ReadSet *query_read_set, std::string &result_hash, std::string &result, bool success);  //free parameters
  void ClearTxnQueries();
  void ClearQuery(PendingQuery *pendingQuery);
  void RetryQuery(PendingQuery *pendingQuery);
  // void ClearQuery(uint64_t query_seq_num, std::vector<uint64_t> &involved_groups);
  // void RetryQuery(uint64_t query_seq_num, std::vector<uint64_t> &involved_groups);

  void AddWriteSetIdx(proto::Transaction &txn);

  ///////////////   Commit protocol structures and functions

  struct PendingRequest {
    PendingRequest(uint64_t id, Client *client) : id(id), outstandingPhase1s(0),
        outstandingPhase2s(0), commitTries(0), maxRepliedTs(0UL),
        decision(proto::COMMIT), fast(true), conflict_flag(false),
        startedPhase2(false), startedWriteback(false),
        callbackInvoked(false), timeout(0UL), slowAbortGroup(-1),
        decision_view(0UL), startFB(false), eqv_ready(false), client(client) {
    }

    ~PendingRequest() {
      //delete all potentially dependent FB instances..
      // for(auto &fb_instance : req_FB_instances){
      //   auto itr = client->FB_instances.find(fb_instance);
      //   if(itr != client->FB_instances.end()){
      //     std::cerr << "Req: " << id << " terminates. Clean up dependency FB txnDigest: " << BytesToHex(fb_instance, 64) << std::endl;
      //     client->CleanFB(itr->second, fb_instance);
      //   }
      // }
    }

    Client *client;
    commit_callback ccb;
    commit_timeout_callback ctcb;
    uint64_t id;
    int outstandingPhase1s;
    int outstandingPhase2s;
    int commitTries;
    uint64_t maxRepliedTs;
    proto::CommitDecision decision;
    uint64_t decision_view;
    bool fast;
    bool conflict_flag;
    bool startedPhase2;
    bool startedWriteback;
    bool callbackInvoked;
    uint32_t timeout;
    proto::GroupedSignatures p1ReplySigsGrouped;
    proto::GroupedSignatures p2ReplySigsGrouped;
    std::string txnDigest;
    int slowAbortGroup;
    int fastAbortGroup;
    proto::CommittedProof conflict;
    //added this for fallback handling
    proto::Transaction txn;
    proto::SignedMessage signed_txn;
    proto::P2Replies p2Replies;
    proto::Writeback writeback;

    int64_t logGrp;
    bool startFB;
    std::unordered_map<std::string, proto::Phase1*> RelayP1s;
    std::unordered_set<std::string> req_FB_instances; //TODO: refactor so that FB_instances only exists as local var.
    //std::vector<std::pair<proto::Phase1*, std::string>> RelayP1s;

    uint64_t conflict_id; //id of request that is dependent (directly or through intermediaries) on this tx.
    bool has_dependent;
    std::string dependent; //txnDigest of txn that depends on this tx directly.

    // equivocation utility
    proto::GroupedSignatures eqvAbortSigsGrouped;
    bool eqv_ready;

  };

  void Phase1(PendingRequest *req);

  void Phase1Callback(uint64_t reqId, int group, proto::CommitDecision decision,
      bool fast, bool conflict_flag, const proto::CommittedProof &conflict,
      const std::map<proto::ConcurrencyControl::Result,
      proto::Signatures> &sigs, bool eqv_ready = false);

  void Phase1CallbackProcessing(PendingRequest *req, int group,
      proto::CommitDecision decision, bool fast, bool conflict_flag,
      const proto::CommittedProof &conflict,
      const std::map<proto::ConcurrencyControl::Result, proto::Signatures> &sigs,
      bool eqv_ready = false, bool fb = true);


  void Phase1TimeoutCallback(int group, uint64_t reqId, int status);
  void HandleAllPhase1Received(PendingRequest *req);

  void Phase2(PendingRequest *req);
  void Phase2Processing(PendingRequest *req);
  void Phase2SimulateEquivocation(PendingRequest *req);
  void Phase2Equivocate(PendingRequest *req);

  void Phase2Callback(uint64_t reqId, int group, proto::CommitDecision decision, uint64_t decision_view,
      const proto::Signatures &p2ReplySigs);
  void Phase2TimeoutCallback(int group, uint64_t reqId, int status);
  void WritebackProcessing(PendingRequest *req);
  void Writeback(PendingRequest *req);
  void FailureCleanUp(PendingRequest *req);
  void ForwardWBcallback(uint64_t txnId, int group, proto::ForwardWriteback &forwardWB);

  // Fallback logic
  void FinishConflict(uint64_t reqId, const std::string &txnDigest, proto::Phase1 *p1);
  bool isDep(const std::string &txnDigest, proto::Transaction &Req_txn, const proto::Transaction* txn);
  bool StillActive(uint64_t conflict_id, std::string &txnDigest);
  void CleanFB(PendingRequest *pendingFB, const std::string &txnDigest, bool clean_shards = true);
  void EraseRelays(proto::RelayP1 &relayP1, std::string &txnDigest);
  void RelayP1callback(uint64_t reqId, proto::RelayP1 &relayP1, std::string& txnDigest);
  void RelayP1TimeoutCallback(uint64_t reqId);
  void RelayP1callbackFB(uint64_t reqId, const std::string &dependent_txnDigest, proto::RelayP1 &relayP1, std::string& txnDigest);
  void Phase1FB(const std::string &txnDigest, uint64_t conflict_id, proto::Phase1 *p1);
  void Phase1FB_deeper(uint64_t conflict_id, const std::string &txnDigest, const std::string &dependent_txnDigest, proto::Phase1 *p1);
  void SendPhase1FB(uint64_t conflict_id, const std::string &txnDigest, PendingRequest *pendingFB);
  void Phase2FB(PendingRequest *req);
  void WritebackFB(PendingRequest *req);
  void Phase1FBcallbackA(uint64_t conflict_id, std::string txnDigest, int64_t group, proto::CommitDecision decision,
     bool fast, bool conflict_flag, const proto::CommittedProof &conflict, const std::map<proto::ConcurrencyControl::Result, proto::Signatures> &sigs);
  void FBHandleAllPhase1Received(PendingRequest *req);
  bool Phase1FBcallbackB(uint64_t conflict_id, std::string txnDigest, int64_t group, proto::CommitDecision decision,
    const proto::P2Replies &p2replies);
  void Phase2FBcallback(uint64_t conflict_id, std::string txnDigest, int64_t group, proto::CommitDecision decision,
    const proto::Signatures &p2ReplySig, uint64_t view);
  void WritebackFBcallback(uint64_t conflict_id, std::string txnDigest, proto::Writeback &wb);
  bool ValidateWB(proto::Writeback &msg, std::string *txnDigest, proto::Transaction *txn);
  bool InvokeFBcallback(uint64_t conflict_id, std::string txnDigest, int64_t group);
  //keep track of pending Fallback instances. Maps from txnDigest, req Id is oblivious to us.
  std::unordered_map<std::string, PendingRequest*> FB_instances;
  std::unordered_set<std::string> Completed_transactions;

  //also: keep map <txnDigest -> normal case pending requests, i.e. reqId as well>

  //DO THIS: TODO: XXX: Keep map from (txnDigest to reqId): When receiving a RelayP1 with 2 txnDigest and no reqId,
  //check this map to find the reqId and make that the conflict ID!!!

  //TODO: add a map from <reqID, set of digests>? delete all digest FB instances when req Id finsihes.?
  //TODO:: create another map from  <reqIds, string> and treat FB instances as normal reqID too.

       //TODO: should the FB_instances be part of a pendingRequest?
          // I.e. every pendingRequest has its nested pendingRequests? (That makes it too hard to find). Need flat hierarchy.
  //Question: How can client have multiple pendingReqs?
  //TODO: would this simplify having a deeper depth?
  // --> would allow normal OCC handling on Wait results at the server?



  bool IsParticipant(int g) const;

  /* Configuration State */
  transport::Configuration *config;
  // Unique ID for this client.
  uint64_t client_id;
  // Number of shards.
  uint64_t nshards;
  // Number of replica groups.
  uint64_t ngroups;
  // Transport used by shard clients.
  Transport *transport;
  // Client for each shard
  std::vector<ShardClient *> bclient;
  Partitioner *part;
  bool syncCommit;
  const bool pingReplicas;
  const uint64_t readMessages;
  const uint64_t readQuorumSize;
  const Parameters params;
  KeyManager *keyManager;
  Verifier *verifier;
  Stats dummyStats;
  // TrueTime server.
  TrueTime timeServer;

  // true after client waits params.injectFailure.timeMs
  bool failureEnabled;
  // true when client attempts to fail the CURRENT txn
  bool failureActive;

  bool first;
  bool startedPings;

  /* Query Execution State */
  // Ongoing query ID.
  uint64_t query_seq_num;
  proto::Query queryMsg;
  std::map<uint64_t, proto::Query> queryBuffer;

  /* Transaction Execution State */
  // Ongoing transaction ID.
  uint64_t client_seq_num;
  // Read timestamp for transaction.
  Timestamp rts;
  // Last request ID.
  uint64_t lastReqId;
  // Number of retries for current transaction.
  long retries;
  // Current transaction.
  proto::Transaction txn;
  // Outstanding requests.
  std::unordered_map<uint64_t, PendingRequest *> pendingReqs;

  std::unordered_map<uint64_t, uint64_t> pendingReqs_starttime;


  /* Debug State */
  std::unordered_map<std::string, uint32_t> statInts;
  struct Latency_t executeLatency;
  struct Latency_t getLatency;
  size_t getIdx;
  struct Latency_t commitLatency;

  uint64_t exec_start_ms;
  uint64_t exec_end_ms;
  std::map<uint64_t, uint64_t> query_start_times;
  // uint64_t query_start_ms;
  // uint64_t query_end_ms;
  uint64_t commit_start_ms;
  uint64_t commit_end_ms;
};

} // namespace sintrstore

#endif /* _SINTR_CLIENT_H_ */
