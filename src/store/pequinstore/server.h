// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/pequinstore/server.h:
 *   A single transactional server replica.
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

#ifndef _PEQUIN_SERVER_H_
#define _PEQUIN_SERVER_H_

#include "lib/message.h"
#include "lib/latency.h"
#include "lib/transport.h"
#include "store/common/backend/pingserver.h"
#include "store/server.h"
#include "store/common/partitioner.h"
#include "store/common/timestamp.h"
#include "store/common/truetime.h"
#include "store/pequinstore/common.h"
#include "store/pequinstore/store.h"
#include "store/pequinstore/pequin-proto.pb.h"
#include "store/pequinstore/batchsigner.h"
#include "store/pequinstore/verifier.h"
#include "store/pequinstore/table_store_interface.h"
#include "store/pequinstore/table_store_interface_toy.h"
#include "store/pequinstore/table_store_interface_peloton.h"
//#include "store/pequinstore/sql_interpreter.h"
#include <sys/time.h>

#include <set>
#include <unordered_map>
#include <unordered_set>
#include <ctime>
#include <mutex>
#include <shared_mutex>
#include <atomic>
#include <iostream>
#include <random>

#include "tbb/concurrent_unordered_map.h"
#include "tbb/concurrent_hash_map.h"
#include "tbb/concurrent_unordered_set.h"


//#include "lib/threadpool.cc"

namespace pequinstore {

class ServerTest;

enum OCCType {
  MVTSO = 0,
  TAPIR = 1
};

static bool ASYNC_WRITES = true; //Perform TableWrites asynchronously.

//TEST/DEBUG variables
static bool PRINT_READ_SET = false; //print out the read set of a query
static bool PRINT_RESULT_ROWS = false; //print out the result of a query
static bool PRINT_SNAPSHOT = false; //print out the snapshot set of a query
//static bool PROFILE_EXEC_LAT = true; //record exec time 

static bool TEST_QUERY = false; //true;   //create toy results for queries
static bool TEST_SNAPSHOT = false; //true;  //create toy snapshots for queries
static bool TEST_READ_SET = false; //true;  //create toy read sets for queries
//set all above 3 to test sync protocol
static bool TEST_FAIL_QUERY = false;  //create an artificial retry for queries
static bool TEST_PREPARE_SYNC = false;  //Create artificial sync for queries that supplies prepares even though value is committed
static bool TEST_SYNC = false;  //create an artificial sync for queries

static bool TEST_MATERIALIZE = false; //artificially cause a wait on materialize.
static bool TEST_MATERIALIZE_TS = false; //artificially cause a wait on materialize for a TS based sync
static bool TEST_MATERIALIZE_FORCE = false; //artificially force a materialization.  //Note: TEST_MATERIALIZE or TEST_MATERIALIZE_TS must be set.
static bool TEST_READ_FROM_SS = false; //artificially create a new prepared/committed TX in order to test whether or not read from SS ignores it. (currently creates prepared => should not be read; committed would be read)
static bool TEST_READ_MATERIALIZED = false; //artificially create a new force materialized TX to confirm that read skips it, but if it's in ss then it reads it (currently not part of snapshot)

static int fail_writeback = 0;
typedef std::vector<std::unique_lock<std::mutex>> locks_t;
static int rcv_count = 0;
static int send_count = 0;
static int commitGet_count = 0;
//static unordered_map<TransportAddress*, int> debug_counters;
void PrintSendCount();
void PrintRcvCount();
void ParseProto(::google::protobuf::Message *msg, std::string &data);

//static bool param_parallelOCC = true;

class Server : public TransportReceiver, public ::Server, public PingServer {
 public:
  Server(const transport::Configuration &config, int groupIdx, int idx,
      int numShards, int numGroups,
      Transport *transport, KeyManager *keyManager, Parameters params, std::string &table_registry_path,
      uint64_t timeDelta, OCCType occType, Partitioner *part, unsigned int batchTimeoutMS, bool sql_bench = false, bool simulate_point_kv = false, bool simulate_replica_failure = false,
      TrueTime timeServer = TrueTime(0, 0));
  virtual ~Server();

  virtual void ReceiveMessage(const TransportAddress &remote,
      const std::string &type, const std::string &data,
      void *meta_data) override;

  virtual void Load(const std::string &key, const std::string &value,
      const Timestamp timestamp) override;
  
  virtual void CreateTable(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, 
      const std::vector<uint32_t> &primary_key_col_idx) override;
  
  virtual void CreateIndex(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, 
      const std::string &index_name, const std::vector<uint32_t> &index_col_idx) override;

  virtual void CacheCatalog(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, 
      const std::vector<uint32_t> &primary_key_col_idx) override;

  //Deprecated
  void LoadTableData_SQL(const std::string &table_name, const std::string &table_data_path, const std::vector<uint32_t> &primary_key_col_idx);
  
  virtual void LoadTableData(const std::string &table_name, const std::string &table_data_path, 
      const std::vector<std::pair<std::string, std::string>> &column_names_and_types, const std::vector<uint32_t> &primary_key_col_idx) override;
    //Helper function:
    std::vector<row_segment_t*> ParseTableDataFromCSV(const std::string &table_name, const std::string &table_data_path, 
    const std::vector<std::pair<std::string, std::string>> &column_names_and_types, const std::vector<uint32_t> &primary_key_col_idx);

  virtual void LoadTableRows(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, 
      const row_segment_t *row_segment, const std::vector<uint32_t> &primary_key_col_idx, int segment_no = 1, bool load_cc = true) override;

  void LoadTableRows_Old(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, 
      const std::vector<std::vector<std::string>> &row_values, const std::vector<uint32_t> &primary_key_col_idx, int segment_no = 1);
      
  virtual void LoadTableRow(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, 
      const std::vector<std::string> &values, const std::vector<uint32_t> &primary_key_col_idx) override;

  virtual inline Stats &GetStats() override { return stats; }

 private:
    bool simulate_point_kv;
    bool simulate_replica_failure;

   uint64_t total_lock_time_ms =0 ;

   std::string dummyString;
   proto::Transaction dummyTx;

  friend class ServerTest;
  struct Value {
    std::string val;
    const proto::CommittedProof *proof;
  };

//Protocol
  void ReceiveMessageInternal(const TransportAddress &remote,
      const std::string &type, const std::string &data,
      void *meta_data);

  //Read Handling
  void ManageDispatchRead(const TransportAddress &remote, const std::string &data);
  void HandleRead(const TransportAddress &remote, proto::Read &msg);

  //Phase1 Handling
  void ManageDispatchPhase1(const TransportAddress &remote, const std::string &data);
  void HandlePhase1(const TransportAddress &remote,
      proto::Phase1 &msg);
  void HandlePhase1CB(uint64_t reqId, proto::ConcurrencyControl::Result result,
        const proto::CommittedProof* &committedProof, std::string &txnDigest, proto::Transaction *txn, const TransportAddress &remote,
        const proto::Transaction *abstain_conflict, bool isGossip = false, bool forceMaterialize = false);
  void SendPhase1Reply(uint64_t reqId, proto::ConcurrencyControl::Result result,
        const proto::CommittedProof *conflict, const std::string &txnDigest, const TransportAddress *remote,
        const proto::Transaction *abstain_conflict = nullptr);

  //Gossip
  void ForwardPhase1(proto::Phase1 &msg);
  void Inform_P1_GC_Leader(proto::Phase1Reply &reply, proto::Transaction &txn, std::string &txnDigest, int64_t grpLeader);
      //Atomic Phase1 Handlers for fully parallel P1: Currently deprecated.
      void HandlePhase1_atomic(const TransportAddress &remote,
          proto::Phase1 &msg);
      void ProcessPhase1_atomic(const TransportAddress &remote,
          proto::Phase1 &msg, proto::Transaction *txn, std::string &txnDigest);

 //Phase2 Handling
  void ManageDispatchPhase2(const TransportAddress &remote, const std::string &data);
  void HandlePhase2(const TransportAddress &remote, proto::Phase2 &msg);
  void ManagePhase2Validation(const TransportAddress &remote, proto::Phase2 &msg, const std::string *txnDigest, const proto::Transaction *txn,
      const std::function<void()> &sendCB, proto::Phase2Reply* phase2Reply, const std::function<void()> &cleanCB, 
      const int64_t &myProcessId, const proto::ConcurrencyControl::Result &myResult);
  void HandlePhase2CB(TransportAddress *remote, proto::Phase2 *msg, const std::string* txnDigest,
        signedCallback sendCB, proto::Phase2Reply* phase2Reply, cleanCallback cleanCB, void* valid); //bool valid);
  void SendPhase2Reply(proto::Phase2 *msg, proto::Phase2Reply *phase2Reply, signedCallback sendCB);

 //Writeback Handling
  void ManageDispatchWriteback(const TransportAddress &remote, const std::string &data);
  void HandleWriteback(const TransportAddress &remote,
      proto::Writeback &msg);
  void ManageWritebackValidation(proto::Writeback &msg, const std::string *txnDigest, proto::Transaction *txn);
  void WritebackCallback(proto::Writeback *msg, const std::string* txnDigest,
    proto::Transaction* txn, void* valid); //bool valid);
  
  //Handle Abort during Execution
  void ManageDispatchAbort(const TransportAddress &remote, const std::string &data);
  void HandleAbort(const TransportAddress &remote, const proto::Abort &msg);

  //Fallback handler functions
  //Phase1FB Handling
  void ManageDispatchPhase1FB(const TransportAddress &remote, const std::string &data);
  void HandlePhase1FB(const TransportAddress &remote, proto::Phase1FB &msg);
  //Phase2FB Handling
  void ManageDispatchPhase2FB(const TransportAddress &remote, const std::string &data);
  void HandlePhase2FB(const TransportAddress &remote, const proto::Phase2FB &msg);
  //InvokeFB Handling (start fallback reconciliaton)
  void ManageDispatchInvokeFB(const TransportAddress &remote, const std::string &data);
  void HandleInvokeFB(const TransportAddress &remote,proto::InvokeFB &msg);
  //ElectFB Handling (elect fallback leader)
  void ManageDispatchElectFB(const TransportAddress &remote, const std::string &data);
  //void HandleFB_Elect: If 4f+1 Elect messages received -> form Decision based on majority, and forward the elect set (send FB_Dec) to all replicas in logging shard. (This includes the FB replica itself - Just skip ahead to HandleFB_Dec automatically: send P2R to clients)
  void HandleElectFB(proto::ElectFB &msg);
  //DecisionFB Handling (receive fallback leader decision)
  void ManageDispatchDecisionFB(const TransportAddress &remote, const std::string &data);
  void HandleDecisionFB(proto::DecisionFB &msg);
  //MoveView Handling (all to all view change)
  void ManageDispatchMoveView(const TransportAddress &remote, const std::string &data);
  void HandleMoveView(proto::MoveView &msg);

  //Query handler functions
  void ManageDispatchQuery(const TransportAddress &remote, const std::string &data);
  void HandleQuery(const TransportAddress &remote, proto::QueryRequest &msg);

  void ManageDispatchSync(const TransportAddress &remote, const std::string &data);
  void HandleSync(const TransportAddress &remote, proto::SyncClientProposal &msg);

  void ManageDispatchRequestTx(const TransportAddress &remote, const std::string &data);
  void HandleRequestTx(const TransportAddress &remote, proto::RequestMissingTxns &msg);

  void ManageDispatchSupplyTx(const TransportAddress &remote, const std::string &data);
  void HandleSupplyTx(const TransportAddress &remote, proto::SupplyMissingTxns &msg);
  


///////////////////////

    //Query helper functions:

    //Query helper data structures

    //Query objects
    
    //QueryReadSetMgr lives in common.h
    //SnapshotMgr lives in Snapshot_mgr.cc

    struct QueryMetaData {
      QueryMetaData(const std::string &_query_cmd, const TimestampMessage &timestamp, const TransportAddress &remote, const uint64_t &req_id, 
                    const uint64_t &query_seq_num, const uint64_t &client_id, const QueryParameters *query_params, const uint64_t &retry_version): 
         failure(false), retry_version(retry_version), waiting_sync(false), started_sync(false), has_result(false), 
         query_cmd(_query_cmd), ts(timestamp), original_client(remote.clone()), req_id(req_id), query_seq_num(query_seq_num), client_id(client_id), is_waiting(false),
         snapshot_mgr(query_params), useOptimisticTxId(false), executed_query(false), merged_ss_msg(nullptr), designated_for_reply(false)
      {
          //queryResult = new proto::QueryResult();
          queryResultReply = new proto::QueryResultReply(); //TODO: Replace with GetUnused.
          has_query = !query_cmd.empty();

      }
       QueryMetaData(const uint64_t &query_seq_num, const uint64_t &client_id, const QueryParameters *query_params): 
          failure(false), retry_version(0UL), has_query(false), waiting_sync(false), started_sync(false), 
          has_result(false), query_seq_num(query_seq_num), client_id(client_id), is_waiting(false) ,
          snapshot_mgr(query_params), useOptimisticTxId(false), executed_query(false), original_client(nullptr), merged_ss_msg(nullptr), designated_for_reply(false)
      {
          //queryResult = new proto::QueryResult();
          queryResultReply = new proto::QueryResultReply(); //TODO: Replace with GetUnused.
      }
      ~QueryMetaData(){ 
        if(original_client != nullptr) delete original_client;
        //delete queryResult;
        delete queryResultReply;
        if(merged_ss_msg != nullptr) delete merged_ss_msg; //Delete obsolete sync snapshot
      }
      void ClearMetaData(const std::string &queryId){
        queryResultReply->Clear(); //FIXME: Confirm that all data that is cleared is re-set
        merged_ss.clear();
        //merged_ss.Clear();
        //local_ss.clear(); //for now don't clear, will get overridden anyways.
        missing_txns.clear();
        executed_query = false;
        has_result = false;
        is_waiting = false;
        started_sync = false;
        waiting_sync = false;
        if(merged_ss_msg != nullptr){
          delete merged_ss_msg; //Delete obsolete sync snapshot
          merged_ss_msg = nullptr;
        }
      }
      void SetQuery(const std::string &_query_cmd, const TimestampMessage &timestamp, const TransportAddress &remote, const uint64_t &_req_id){
        has_query = true;
        query_cmd = _query_cmd;
        ts = timestamp;
        if(original_client == nullptr) original_client = remote.clone();
        req_id = std::max(req_id, _req_id); //if we already received a retry, keep it's req_id.
      }
      void RegisterWaitingSync(proto::MergedSnapshot *_merged_ss_msg, const TransportAddress &remote){
        waiting_sync = true;
        merged_ss_msg = _merged_ss_msg;
        if(original_client == nullptr) original_client = remote.clone(); 
      }

      bool failure;

      TransportAddress *original_client;
      uint64_t req_id;  //can use this as implicit retry version.
      uint64_t query_seq_num;
      uint64_t client_id;

       Timestamp ts;
      bool has_query;
      std::string query_cmd; //query to execute

      uint64_t retry_version;            //query retry version (0 for original)
      bool designated_for_reply; //whether to reply with result to client or not.

      bool executed_query; //whether already executed query for retry version.

      
      proto::MergedSnapshot *merged_ss_msg; //TODO: check that ClearMetaData resets appropriate fields.    //this contains map: merged_txns => this is the set of materialized txns.
      bool started_sync;  //whether already received/processed snapshot for given retry version.
      bool waiting_sync; //a waiting sync msg.

      SnapshotManager snapshot_mgr;

      std::unordered_set<std::string> local_ss;  //local snapshot   //DEPRECATED
      std::unordered_set<std::string> merged_ss; //merged snapshot  //DEPRECATED 

      bool useOptimisticTxId;  
     
      //google::protobuf::RepeatedPtrField<std::string> merged_ss;

      bool is_waiting; //waiting on txn for sync.
      std::unordered_map<std::string, uint64_t> missing_txns; //map from txn-id --> number of responses max waiting for; if client is byz, no specified replica may have it --> if so, return immediately and blacklist/report tx (requires sync to be signed)

      bool has_result;
      proto::QueryResultReply *queryResultReply; //contains proto::QueryResult, which in turn contains: query_result, read set, read set hash/result hash, dependencies
      //Note: After signing queryResultReply we restore the result (i.e. we do not store the signature), and thus read_set and result remain accessible.
      //proto::QueryResult *queryResult;   //Use this if we want to switch back to storing QueryResult and generate queryResultReply on demand. 
      std::string signature; //TODO: Possibly buffer sig itself.

      //proto::QueryResult result; 
      //proto::ReadSet *query_read_set;

      // //Note: If we always send read-set, and don't want to use caching and hashing, then store this directly in the reply message to avoid copying.
      // std::map<std::string, TimestampMessage> read_set;     //read set = map from key-> Timestamp version   //ordered_map ==> all replicas agree on key order.
      
      // std::set<std::string> dependencies; 
      // std::string result;      //result
      // std::string result_hash; //result_hash

    };
    typedef tbb::concurrent_hash_map<std::string, QueryMetaData*> queryMetaDataMap; //map from query_id -> QueryMetaData
    queryMetaDataMap queryMetaData;

    tbb::concurrent_unordered_set<std::string> alreadyDeleted; //FIXME: JUST FOR TESTING

    //tbb::concurrent_unordered_map<uint64_t, uint64_t> clientQueryWatermark; //map from client_id to timestamp of last committed Tx. Ignore all queries below.
    typedef tbb::concurrent_hash_map<uint64_t, uint64_t> clientQueryWatermarkMap; //map from client_id to highest committed query_seq. Ignore all queries below.
    clientQueryWatermarkMap clientQueryWatermark;

    typedef std::pair<std::string, uint64_t> query_id_version;
    struct waitingOnQueriesMeta {
      //waitingOnQueriesMeta(uint64_t reqId, const proto::Transaction *txn) : reqId(reqId), txn(txn)  {}
      waitingOnQueriesMeta(uint64_t reqId, proto::Transaction *txn, const TransportAddress *remote, bool isGossip, uint8_t prepare_or_commit=0) : reqId(reqId), txn(txn), remote(remote->clone()), isGossip(isGossip), prepare_or_commit(prepare_or_commit)  {
        //if(isGossip) original_client = false;
      }
      //deprecated
      waitingOnQueriesMeta(proto::Transaction *txn, proto::GroupedSignatures *groupedSigs, bool p1Sigs, uint64_t view, uint8_t prepare_or_commit=1) : txn(txn), groupedSigs(groupedSigs), p1Sigs(p1Sigs), view(view), prepare_or_commit(prepare_or_commit)  {
      }
      waitingOnQueriesMeta(proto::Transaction *txn, proto::CommittedProof *proof, uint8_t prepare_or_commit=1) : txn(txn), proof(proof), prepare_or_commit(prepare_or_commit) {
      }
      ~waitingOnQueriesMeta(){
        if(remote != nullptr) delete remote;
      }
      //deprecated
      void setToCommit(proto::GroupedSignatures *groupedSigs, bool p1Sigs, uint64_t view, uint8_t prepare_or_commit=1){
        groupedSigs = groupedSigs;
        p1Sigs = p1Sigs;
        view = view;
        prepare_or_commit = prepare_or_commit;  
      }
      void setToCommit(proto::CommittedProof *proof){
        proof = proof;
        prepare_or_commit = 1;
      }
      //std::mutex deps_mutex;
      //queries we are waiting for
      std::unordered_map<std::string, uint64_t> missing_query_id_versions;  

      uint8_t prepare_or_commit; //identify whether we are supposed to wake and prepare or wake and commit //TODO: In Commit, if already registered, set this int to 1 (if not registered, create new struct with 1)

      // bool original_client;
      //Arguments needed for wakeup callback 

      //Prepare
      bool isGossip;
      uint64_t reqId;
      const TransportAddress *remote;

      //Prepare or Commit
      proto::Transaction *txn;

      //Commit
      proto::CommittedProof *proof;
      //deprecated:
      proto::GroupedSignatures *groupedSigs;
      bool p1Sigs;
      uint64_t view;

    };

    //Maps for Query Caching -- TX waiting for queries.
    typedef tbb::concurrent_hash_map<std::string, std::string> subscribedQueryMap; //Mapping from Query to the Tx waiting for it. (1-1 mapping since we assume each honest query is unique. Only byz clients may try to re-use queries; Byz clients may not use other clients queries)
    subscribedQueryMap subscribedQuery;
    typedef tbb::concurrent_hash_map<std::string, waitingOnQueriesMeta*> TxnMissingQueriesMap; // Mapping from Tx to all queries it is waiting for. ==> Resume Tx OCC processing when complete
    TxnMissingQueriesMap TxnMissingQueries;


     //map from tx-id to query-ids (that are waiting to sync on the tx-id)  (GC inside UpdateWaitingQueries.)
    typedef tbb::concurrent_hash_map<std::string, std::unordered_set<std::string>> waitingQueryMap; 
    waitingQueryMap waitingQueries;
    //Same for optimistic ids: Map from ts-id to query-ids
    typedef tbb::concurrent_hash_map<uint64_t, std::unordered_set<std::string>> waitingQueryTSMap; 
    waitingQueryTSMap waitingQueriesTS;

    //map from <query_id, retry_version> to list of missing txn (+ num supply replies received ).
    struct MissingTxns {  
        std::unordered_map<std::string, uint64_t> missing_txns;
        std::unordered_map<uint64_t, uint64_t> missing_ts; // for optimistic id's use this
        std::string query_id;
        uint64_t retry_version;
    };
    typedef tbb::concurrent_hash_map<std::string, MissingTxns> queryMissingTxnsMap;  //std::unordered_set<std::string>
    queryMissingTxnsMap queryMissingTxns;  //List of Transactions missing for a Query to materialize snapshot.

    void FindTableVersion(const std::string &key_name, const Timestamp &ts, 
                              bool add_to_read_set, QueryReadSetMgr *readSetMgr, 
                              bool add_to_snapshot, SnapshotManager *snapshotMgr,
                              bool materialize_from_snapshot, const snapshot *ss_txns);
    void FindTableVersionOld(const std::string &key_name, const Timestamp &ts, bool add_to_read_set, QueryReadSetMgr *readSetMgr, bool add_to_snapshot, SnapshotManager *snapshotMgr);
    const proto::Transaction* FindPreparedVersion(const std::string &key, const Timestamp &ts, bool committed_exists, std::pair<Timestamp, Server::Value> const &tsVal);

    void ProcessPointQuery(const uint64_t &reqId, proto::Query *query, const TransportAddress &remote);
    void ProcessQuery(queryMetaDataMap::accessor &q, const TransportAddress &remote, proto::Query *query, QueryMetaData *query_md);
    void FindSnapshot(QueryMetaData *query_md, proto::Query *query);
    void ProcessSync(queryMetaDataMap::accessor &q, const TransportAddress &remote, proto::MergedSnapshot *merged_ss, const std::string *queryId, QueryMetaData *query_md);

    bool CheckPresence(const std::string &tx_id, const std::string &query_retry_id, QueryMetaData *query_md, 
        std::map<uint64_t, proto::RequestMissingTxns> &replica_requests, const pequinstore::proto::ReplicaList &replica_list,
        std::unordered_map<std::string, uint64_t> &missing_txns);
    bool CheckPresence(const uint64_t &ts_id, const std::string &query_retry_id, QueryMetaData *query_md, 
        std::map<uint64_t, proto::RequestMissingTxns> &replica_requests, const pequinstore::proto::ReplicaList &replica_list,
        std::unordered_map<std::string, uint64_t> &missing_txns, std::unordered_map<uint64_t, uint64_t> &missing_ts);
    void RequestMissing(const proto::ReplicaList &replica_list, std::map<uint64_t, proto::RequestMissingTxns> &replica_requests, const std::string &tx_id);
    void RequestMissing(const proto::ReplicaList &replica_list, std::map<uint64_t, proto::RequestMissingTxns> &replica_requests, const uint64_t &ts_id);

    // void SetWaiting(std::unordered_map<std::string, uint64_t> &missing_txns, const std::string &tx_id, const std::string *queryId, const std::string &query_retry_id,
    //     const proto::ReplicaList &replica_list, std::map<uint64_t, proto::RequestMissingTxns> &replica_requests);
    // void SetWaitingTS(std::unordered_map<uint64_t, uint64_t> &missing_ts, const uint64_t &ts_id, const std::string *queryId, const std::string &query_retry_id,
    //     const proto::ReplicaList &replica_list, std::map<uint64_t, proto::RequestMissingTxns> &replica_requests);

    void CheckLocalAvailability(const std::string &txn_id, proto::TxnInfo &txn_info);
      //void CheckLocalAvailability(const std::string &txn_id, proto::SupplyMissingTxnsMessage &supply_txn, bool sync_on_ts = false);

    std::string ExecQuery(QueryReadSetMgr &queryReadSetMgr, QueryMetaData *query_md, bool read_materialized = false, bool eager = false);
    void ExecQueryEagerly(queryMetaDataMap::accessor &q, QueryMetaData *query_md, const std::string &queryId);
    void HandleSyncCallback(queryMetaDataMap::accessor &q, QueryMetaData *query_md, const std::string &queryId);
    void SendQueryReply(QueryMetaData *query_md);
    void CacheReadSet(QueryMetaData *query_md, proto::QueryResult *&result, proto::ReadSet *&query_read_set);
    void ProcessSuppliedTxn(const std::string &txn_id, proto::TxnInfo &txn_info, bool &stop);

   
    void CheckWaitingQueries(const std::string &txnDigest, const uint64_t &ts, const uint64_t ts_id, bool is_abort = false, bool non_blocking = false, int tx_ts_mode = 0);

    void UpdateWaitingQueries(const std::string &txnDigest, bool is_abort = false);
    void UpdateWaitingQueriesTS(const uint64_t &txnTS, const std::string &txnDigest, bool is_abort = false);

    void FailWaitingQueries(const std::string &txnDigest);
    void FailQuery(QueryMetaData *query_md);
    bool VerifyClientQuery(proto::QueryRequest &msg, const proto::Query *query, std::string &queryId);
    bool VerifyClientSyncProposal(proto::SyncClientProposal &msg, const std::string &queryId);
    void CleanQueries(const proto::Transaction *txn, bool is_commit = true);


    //Materialization
    std::vector<std::string> ApplyTableWrites(const proto::Transaction &txn, const Timestamp &ts,
                const std::string &txn_digest, const proto::CommittedProof *commit_proof, bool commit_or_prepare = true, bool forceMaterialize = false);
    // void ApplyTableWrites(const std::string &table_name, const TableWrite &table_write, const Timestamp &ts,
    //             const std::string &txn_digest, const proto::CommittedProof *commit_proof, bool commit_or_prepare = true, bool forceMaterialize = false);
    bool WaitForMaterialization(const std::string &tx_id, const std::string &query_retry_id, std::unordered_map<std::string, uint64_t> &missing_txns);
    void WaitForTX(const uint64_t &ts_id, const std::string &query_retry_id, std::unordered_map<uint64_t, uint64_t> &missing_ts);
      bool WaitForMaterialization(const uint64_t &ts_id, const std::string &query_retry_id, std::unordered_map<uint64_t, uint64_t> &missing_ts); //DEPRECATED

    bool RegisterForceMaterialization(const std::string &txnDigest, const proto::Transaction *txn);
    void ForceMaterialization(const proto::ConcurrencyControl::Result &result, const std::string &txnDigest, const proto::Transaction *txn);
    typedef tbb::concurrent_hash_map<std::string, bool> materializedMap; //second argument is void: set of materialized txns.
    materializedMap materialized;
     typedef tbb::concurrent_hash_map<uint64_t, bool> materializedTSMap; //second argument is void: set of materialized txns.
    materializedTSMap materializedTS;



    ////////////////////////////////////// Fallback helper functions

    //FALLBACK helper datastructures

    struct P1MetaData {
        P1MetaData(): conflict(nullptr), hasP1(false), sub_original(false), hasSignedP1(false), reqId(0), fallbacks_interested(false), forceMaterialize(false) {}
        P1MetaData(proto::ConcurrencyControl::Result result): result(result), conflict(nullptr), hasP1(true), sub_original(false), hasSignedP1(false), reqId(0), fallbacks_interested(false), forceMaterialize(false) {}
        ~P1MetaData(){
          if(signed_txn != nullptr) delete signed_txn;
          if(original != nullptr) delete original;
        }
        void SubscribeOriginal(const TransportAddress &remote, uint64_t reqId){
          //Debug("Subscribing original client with req_id: %d", req_id);
          sub_original = true; 
          original = remote.clone();
          this->reqId = reqId;
          Debug("Subscribing original client with reqId: %d", reqId);
        }
        void SubscribeAllInterestedFallbacks(){  //TODO: Instead, may want to only subscribe individual ones.
          fallbacks_interested = true;
        }


        uint64_t reqId;

        proto::ConcurrencyControl::Result result;
        const proto::CommittedProof *conflict;
        bool hasP1;
        std::mutex P1meta_mutex;
        bool hasSignedP1;
        proto::SignedMessage *signed_txn;
        // Not used currently: In case we want to subscribe original client to P1 also to avoid ongoing bug.
        bool sub_original; 
        const TransportAddress *original;

        bool fallbacks_interested;
        //could have a list of interested clients.

        bool forceMaterialize;
        bool alreadyForceMaterialized;
      };
      typedef tbb::concurrent_hash_map<std::string, P1MetaData> p1MetaDataMap;
    p1MetaDataMap p1MetaData;

    struct P2MetaData {
      P2MetaData() : current_view(0UL), decision_view(0UL), hasP2(false),
          has_original(false),  original_address(nullptr){}
      P2MetaData(proto::CommitDecision decision) : current_view(0UL), decision_view(0UL),
                      p2Decision(decision), hasP2(true),
                      has_original(false), original_address(nullptr){}
      ~P2MetaData(){
        if(original_address != nullptr) delete original_address;
      }
      uint64_t current_view;
      uint64_t decision_view;
      bool hasP2;
      proto::CommitDecision p2Decision;

      bool has_original;
      uint64_t original_msg_id;
      TransportAddress *original_address;
    };
    //tbb::concurrent_hash_map<std::string, uint64_t> current_views;
    typedef tbb::concurrent_hash_map<std::string, P2MetaData> p2MetaDataMap;
    p2MetaDataMap p2MetaDatas;

    struct P1FBorganizer {
      P1FBorganizer(uint64_t ReqId, const std::string &txnDigest, const TransportAddress &remote, Server *server) :
        remote(remote.clone()), has_remote(true), server(server),
        p1_sig_outstanding(false), p2_sig_outstanding(false), c_view_sig_outstanding(false) {
          p1fbr = server->GetUnusedPhase1FBReply();
          p1fbr->Clear();
          p1fbr->set_req_id(ReqId);
          p1fbr->set_txn_digest(txnDigest);
      }
      P1FBorganizer(uint64_t ReqId, const std::string &txnDigest, Server *server) : has_remote(false), server(server),
        p1_sig_outstanding(false), p2_sig_outstanding(false), c_view_sig_outstanding(false){
          p1fbr = server->GetUnusedPhase1FBReply();
          p1fbr->Clear();
          p1fbr->set_req_id(ReqId);
          p1fbr->set_txn_digest(txnDigest);
      }
      ~P1FBorganizer() {
        if(has_remote) delete remote;
        server->FreePhase1FBReply(p1fbr);
      }
      Server *server;

      uint64_t req_id;
      std::string txnDigest;
      bool has_remote;
      const TransportAddress *remote;

      proto::Phase1FBReply *p1fbr;
      //manage outstanding Sigs
      std::mutex sendCBmutex;
      bool p1_sig_outstanding;
      bool p2_sig_outstanding;
      bool c_view_sig_outstanding;
    };

    struct P2FBorganizer {
      P2FBorganizer(uint64_t ReqId, const std::string &txnDigest, const TransportAddress &remote, Server *server) :
        remote(remote.clone()), has_remote(true), server(server),
        p2_sig_outstanding(false), c_view_sig_outstanding(false) {
          p2fbr = server->GetUnusedPhase2FBReply();
          p2fbr->Clear();
          p2fbr->set_req_id(ReqId);
          p2fbr->set_txn_digest(txnDigest);
      }
      P2FBorganizer(uint64_t ReqId, const std::string &txnDigest, Server *server) : has_remote(false), server(server),
        p2_sig_outstanding(false), c_view_sig_outstanding(false){
          p2fbr = server->GetUnusedPhase2FBReply();
          p2fbr->Clear();
          p2fbr->set_req_id(ReqId);
          p2fbr->set_txn_digest(txnDigest);
      }
      ~P2FBorganizer() {
        if(has_remote) delete remote;
        server->FreePhase2FBReply(p2fbr);
      }
      Server *server;

      uint64_t req_id;
      std::string txnDigest;
      bool has_remote;
      const TransportAddress *remote;
      const TransportAddress *original;

      proto::Phase2FBReply *p2fbr;
      //manage outstanding Sigs
      std::mutex sendCBmutex;
      bool p2_sig_outstanding;
      bool c_view_sig_outstanding;
    };

    void RelayP1(const std::string &dependency_txnDig, bool fallback_flow, uint64_t reqId, const TransportAddress &remote, const std::string &txnDigest);
    void SendRelayP1(const TransportAddress &remote, const std::string &dependency_txnDig, uint64_t dependent_id, const std::string &dependent_txnDig);

    
    void ProcessProposalFB(proto::Phase1FB &msg, const TransportAddress &remote, std::string &txnDigest, proto::Transaction* txn);
    void* TryExec(proto::Phase1FB &msg, const TransportAddress &remote, std::string &txnDigest, proto::Transaction* txn);
    //p1MetaDataMap::accessor &c, 
    bool ExecP1(proto::Phase1FB &msg, const TransportAddress &remote,
      const std::string &txnDigest, proto::Transaction* txn, proto::ConcurrencyControl::Result &result,
      const proto::CommittedProof* &committedProof, const proto::Transaction *abstain_conflict = nullptr);

    void SetP1(uint64_t reqId, proto::Phase1Reply *p1Reply, const std::string &txnDigest,
      const proto::ConcurrencyControl::Result &result, const proto::CommittedProof *conflict,
      const proto::Transaction *abstain_conflict = nullptr);
    void SetP2(uint64_t reqId, proto::Phase2Reply *p2Reply, const std::string &txnDigest,
      proto::CommitDecision &decision, uint64_t decision_view);

    void SendPhase1FBReply(P1FBorganizer *p1fb_organizer, const std::string &txnDigest, bool multi = false);
    void SendPhase2FBReply(P2FBorganizer *p2fb_organizer, const std::string &txnDigest, bool multi = false, bool sub_original = false);

    void ProcessP2FB(const TransportAddress &remote, const std::string &txnDigest, const proto::Phase2FB &p2fb);
    void ProcessP2FBCallback(const proto::Phase2FB *p2fb, const std::string &txnDigest,
      const TransportAddress *remote, void* valid);

    void SendView(const TransportAddress &remote, const std::string &txnDigest);

    void InvokeFBProcessP2FB(const TransportAddress &remote, const std::string &txnDigest, const proto::Phase2FB &p2fb, proto::InvokeFB *msg);
    void InvokeFBProcessP2FBCallback(proto::InvokeFB *msg, const proto::Phase2FB *p2fb, const std::string &txnDigest,
      const TransportAddress *remote, void* valid);
    void VerifyViews(proto::InvokeFB &msg, uint32_t logGrp, const TransportAddress &remote);
    void InvokeFBcallback(proto::InvokeFB *msg, const std::string &txnDigest, uint64_t proposed_view, uint64_t logGrp, const TransportAddress *remoteCopy, void* valid);
    void SendElectFB(proto::InvokeFB *msg, const std::string &txnDigest, uint64_t proposed_view, proto::CommitDecision decision, uint64_t logGrp);

    void ElectFBcallback(const std::string &txnDigest, uint64_t elect_view, proto::CommitDecision decision, std::string *signature, uint64_t process_id, void* valid);
    bool PreProcessElectFB(const std::string &txnDigest, uint64_t elect_view, proto::CommitDecision decision, uint64_t process_id);
    void ProcessElectFB(const std::string &txnDigest, uint64_t elect_view, proto::CommitDecision decision, std::string *signature, uint64_t process_id);

    void FBDecisionCallback(proto::DecisionFB *msg, const std::string &txnDigest, uint64_t view, proto::CommitDecision decision, void* valid);
    void AdoptDecision(const std::string &txnDigest, uint64_t view, proto::CommitDecision decision);
    void BroadcastMoveView(const std::string &txnDigest, uint64_t proposed_view);
    void ProcessMoveView(const std::string &txnDigest, uint64_t proposed_view, bool self = false);

    //keep list of all remote addresses == interested client_seq_num
    //TODO: store original client separately..
    struct interestedClient {
      interestedClient(): client_id(0UL), client_address(nullptr) {}
      ~interestedClient() {
        if(client_address != nullptr) delete client_address;
      }
      uint64_t client_id;
      const TransportAddress* client_address;
    };
    //typedef tbb::concurrent_hash_map<std::string, tbb::concurrent_unordered_set<interestedClient>> interestedClientsMap;
    //typedef tbb::concurrent_hash_map<std::string, tbb::concurrent_unordered_set<const TransportAddress*>> interestedClientsMap;
    typedef tbb::concurrent_hash_map<std::string, tbb::concurrent_unordered_map<uint64_t, const TransportAddress*>> interestedClientsMap;
    interestedClientsMap interestedClients;

    tbb::concurrent_hash_map<std::string, std::pair<uint64_t, const TransportAddress*>> originalClient;

    void WakeAllInterestedFallbacks(const std::string &txnDigest, const proto::ConcurrencyControl::Result &result, const proto::CommittedProof *conflict);
    bool ForwardWriteback(const TransportAddress &remote, uint64_t ReqId, const std::string &txnDigest);
    bool ForwardWritebackMulti(const std::string &txnDigest, interestedClientsMap::accessor &i);

  //general helper functions & tools -- Implementations are located in concurrencycontrol.cc and servertools.cc

  typedef google::protobuf::RepeatedPtrField<ReadMessage> ReadSet;
  typedef google::protobuf::RepeatedPtrField<WriteMessage> WriteSet;
  typedef google::protobuf::RepeatedPtrField<proto::Dependency> DepSet;
  //typedef std::vector<proto::ReadPredicate*> PredSet;
  typedef google::protobuf::RepeatedPtrField<proto::ReadPredicate> PredSet;
  
  void subscribeTxOnMissingQuery(const std::string &query_id, const std::string &txnDigest);
  void wakeSubscribedTx(const std::string query_id, const uint64_t &retry_version);
  void restoreTxn(proto::Transaction &txn);
  proto::ConcurrencyControl::Result fetchReadSet(queryMetaDataMap::const_accessor &q, const proto::QueryResultMetaData &query_md, const proto::ReadSet *&query_rs, const std::string &txnDigest, const proto::Transaction &txn);
  proto::ConcurrencyControl::Result mergeTxReadSets(const ReadSet *&readSet, const DepSet *&depSet, const PredSet *&predSet, proto::Transaction &txn, 
                                                    const std::string &txnDigest, uint64_t req_id, const TransportAddress &remote, bool isGossip);
  proto::ConcurrencyControl::Result mergeTxReadSets(const ReadSet *&readSet, const DepSet *&depSet, const PredSet *&predSet, proto::Transaction &txn, 
                                                    const std::string &txnDigest, proto::CommittedProof *proof); // proto::GroupedSignatures *groupedSigs, bool p1Sigs, uint64_t view);
  proto::ConcurrencyControl::Result mergeTxReadSets(const ReadSet *&readSet, const DepSet *&depSet, const PredSet *&predSet, proto::Transaction &txn, 
                                                          const std::string &txnDigest, uint8_t prepare_or_commit,
                                                          uint64_t req_id, const TransportAddress *remote, bool isGossip,      //Args for Prepare
     proto::CommittedProof *proof); //Args for commit  //proto::GroupedSignatures *groupedSigs, bool p1Sigs, uint64_t view);
  
  

  proto::ConcurrencyControl::Result DoOCCCheck(
      uint64_t reqId, const TransportAddress &remote,
      const std::string &txnDigest, proto::Transaction &txn, //const proto::Transaction &txn,
      Timestamp &retryTs, const proto::CommittedProof* &conflict,
      const proto::Transaction* &abstain_conflict,
      bool fallback_flow = false, bool isGossip = false);
  proto::ConcurrencyControl::Result DoTAPIROCCCheck(
      const std::string &txnDigest, const proto::Transaction &txn,
      Timestamp &retryTs);
  proto::ConcurrencyControl::Result DoMVTSOOCCCheck(
      uint64_t reqId, const TransportAddress &remote,
      const std::string &txnDigest, const proto::Transaction &txn, const ReadSet &readSet, const DepSet &depSet, const PredSet &predSet,
      const proto::CommittedProof* &conflict, const proto::Transaction* &abstain_conflict,
      bool fallback_flow = false, bool isGossip = false);

  void RegisterTxTS(const std::string &txnDigest, const proto::Transaction *txn);
  void AddOngoing(std::string &txnDigest, proto::Transaction* txn);
  void RemoveOngoing(std::string &txnDigest);
  void* CheckProposalValidity(::google::protobuf::Message &msg, const proto::Transaction *txn, std::string &txnDigest, bool fallback = false);
  bool VerifyDependencies(::google::protobuf::Message &msg, const proto::Transaction *txn, std::string &txnDigest, bool fallback = false);
  bool VerifyClientProposal(::google::protobuf::Message &msg, const proto::Transaction *txn, std::string &txnDigest, bool fallback = false);
      bool VerifyClientProposal(proto::Phase1 &msg, const proto::Transaction *txn, std::string &txnDigest);
      bool VerifyClientProposal(proto::Phase1FB &msg, const proto::Transaction *txn, std::string &txnDigest);
  void* TryPrepare(uint64_t reqId, const TransportAddress &remote, proto::Transaction *txn,
                        std::string &txnDigest, bool isGossip = false, bool forceMaterialize = false); //,const proto::CommittedProof *committedProof,const proto::Transaction *abstain_conflict,proto::ConcurrencyControl::Result &result);
  void ProcessProposal(proto::Phase1 &msg, const TransportAddress &remote, proto::Transaction *txn,
                        std::string &txnDigest, bool isGossip = false, bool forceMaterialize = false); //,const proto::CommittedProof *committedProof,const proto::Transaction *abstain_conflict,proto::ConcurrencyControl::Result &result);

  void CheckTxLocalPresence(const std::string &txn_id, proto::ConcurrencyControl::Result &res);
  void CheckDepLocalPresence(const proto::Transaction &txn, const DepSet &depSet, proto::ConcurrencyControl::Result &res);
  bool RegisterWaitingTxn(const std::string &dep_id, const std::string &txnDigest, const proto::Transaction &txn, const TransportAddress &remote, 
        const uint64_t &reqId, bool fallback_flow, bool isGossip, std::vector<const std::string*> &missing_deps, const bool new_waiting_dep);
  bool ManageDependencies(const std::string &txnDigest, const proto::Transaction &txn, const TransportAddress &remote, const uint64_t reqId, bool fallback_flow = false, bool isGossip = false);
  bool ManageDependencies(const DepSet &depSet, const std::string &txnDigest, const proto::Transaction &txn, const TransportAddress &remote, const uint64_t &reqId, bool fallback_flow = false, bool isGossip = false);
      bool ManageDependencies_WithMutex(const std::string &txnDigest, const proto::Transaction &txn, const TransportAddress &remote, uint64_t reqId, bool fallback_flow = false, bool isGossip = false);
  
  void GetWriteTimestamps(std::unordered_map<std::string, std::set<Timestamp>> &writes);
  void GetWrites(std::unordered_map<std::string, std::vector<const proto::Transaction *>> &writes);
  void GetPreparedReadTimestamps(std::unordered_map<std::string, std::set<Timestamp>> &reads);
  void GetPreparedReads(std::unordered_map<std::string, std::vector<const proto::Transaction *>> &reads);
  void Prepare(const std::string &txnDigest, const proto::Transaction &txn, const ReadSet &readSet);
  void GetCommittedWrites(const std::string &key, const Timestamp &ts, std::vector<std::pair<Timestamp, Value>> &writes);
  bool GetPreceedingCommittedWrite(const std::string &key, const Timestamp &ts, std::pair<Timestamp, Server::Value> &write);
  void GetPreceedingPreparedWrite(const std::map<Timestamp, const proto::Transaction *> &preparedKeyWrites, const Timestamp &ts,
    std::vector<std::pair<Timestamp, const proto::Transaction *>> &writes);

  void Commit(const std::string &txnDigest, proto::Transaction *txn, proto::GroupedSignatures *groupedSigs, bool p1Sigs, uint64_t view);
  void CommitWithProof(const std::string &txnDigest,  proto::CommittedProof *proof);
  void UpdateCommittedReads(proto::Transaction *txn, const std::string &txnDigest, Timestamp &ts, proto::CommittedProof *proof);
  void CommitToStore(proto::CommittedProof *proof, proto::Transaction *txn, const std::string &txnDigest, Timestamp &ts, Value &val);
  
  void Abort(const std::string &txnDigest, proto::Transaction *txn);
  void CheckDependents(const std::string &txnDigest);
      void CheckDependents_WithMutex(const std::string &txnDigest);
  proto::ConcurrencyControl::Result CheckDependencies(const std::string &txnDigest);
  proto::ConcurrencyControl::Result CheckDependencies(const proto::Transaction &txn);
  proto::ConcurrencyControl::Result CheckDependencies(const proto::Transaction &txn, const DepSet &depSet);
  bool CheckHighWatermark(const Timestamp &ts);
  bool BufferP1Result(proto::ConcurrencyControl::Result &result, const proto::CommittedProof *conflict, const std::string &txnDigest, 
      uint64_t &reqId, const TransportAddress *&remote, bool &wake_fallbacks, bool &forceMaterialize, bool isGossip = false, int fb =0);
  bool BufferP1Result(p1MetaDataMap::accessor &c, proto::ConcurrencyControl::Result &result, const proto::CommittedProof *conflict, const std::string &txnDigest, 
      uint64_t &reqId, const TransportAddress *&remote, bool &wake_fallbacks, bool &forceMaterialize, bool isGossip = false, int fb = 0);
  
  void Clean(const std::string &txnDigest, bool abort = false, bool hard = false);
  void CleanDependencies(const std::string &txnDigest);
        void CleanDependencies_WithMutex(const std::string &txnDigest);
  void LookupP1Decision(const std::string &txnDigest, int64_t &myProcessId,
      proto::ConcurrencyControl::Result &myResult) const;
  void LookupP2Decision(const std::string &txnDigest,
      int64_t &myProcessId, proto::CommitDecision &myDecision) const;
  void LookupCurrentView(const std::string &txnDigest, uint64_t &myCurrentView) const;
  uint64_t DependencyDepth(const std::string &txn_digest) const;
  uint64_t DependencyDepth(const proto::Transaction *txn) const;
  void MessageToSign(::google::protobuf::Message* msg,
      proto::SignedMessage *signedMessage, signedCallback cb);
  void SignSendReadReply(proto::Write *write, proto::SignedMessage *signed_write, const std::function<void()> &sendCB);

  /* BEGIN Semantic CC functions */ 

//TODO: Parameterize. 
  uint64_t write_monotonicity_grace;
  std::string GetEncodedRow(const proto::Transaction &txn, const RowUpdates &row, const std::string &table_name);
  bool CheckMonotonicTableColVersions(const std::string &txn_digest, const proto::Transaction &txn); 
  proto::ConcurrencyControl::Result CheckPredicates(const proto::Transaction &txn, const Timestamp &txn_ts, const ReadSet &txn_read_set, const PredSet &pred_set, std::set<std::string> &dynamically_active_dependencies);
  proto::ConcurrencyControl::Result CheckPredicates(const proto::Transaction &txn, const ReadSet &txn_read_set, std::set<std::string> &dynamically_active_dependencies); 
  proto::ConcurrencyControl::Result CheckReadPred(const Timestamp &txn_ts, const proto::ReadPredicate &pred, const ReadSet &txn_read_set, std::set<std::string> &dynamically_active_dependencies);
  proto::ConcurrencyControl::Result CheckTableWrites(const proto::Transaction &txn, const Timestamp &txn_ts, const std::string &table_name, const TableWrite &table_write);
  void RecordReadPredicatesAndWrites(const proto::Transaction &txn, const Timestamp &ts, bool commit_or_prepare);
  void ClearPredicateAndWrites(const proto::Transaction &txn);
  bool CheckGCWatermark(const Timestamp &ts); 
  bool EvaluatePred(const std::string &pred, const RowUpdates &row, const std::string &table_name);
  bool EvaluatePred_peloton(const std::string &pred, const RowUpdates &row, const std::string &table_name);
  bool Eval(peloton::expression::AbstractExpression *predicate, const RowUpdates row, peloton::catalog::Schema *schema);
  peloton::catalog::Schema* ConvertColRegistryToSchema(ColRegistry *col_registry);

  /* END Semantic CC functions */

  //main protocol messages
  proto::ReadReply *GetUnusedReadReply();
  proto::Phase1Reply *GetUnusedPhase1Reply();
  proto::Phase2Reply *GetUnusedPhase2Reply();
  proto::Read *GetUnusedReadmessage();
  proto::Phase1 *GetUnusedPhase1message();
  proto::Phase2 *GetUnusedPhase2message();
  proto::Writeback *GetUnusedWBmessage();
  proto::Abort *GetUnusedAbortMessage();
  void FreeReadReply(proto::ReadReply *reply);
  void FreePhase1Reply(proto::Phase1Reply *reply);
  void FreePhase2Reply(proto::Phase2Reply *reply);
  void FreeReadmessage(proto::Read *msg);
  void FreePhase1message(proto::Phase1 *msg);
  void FreePhase2message(proto::Phase2 *msg);
  void FreeWBmessage(proto::Writeback *msg);
  void FreeAbortMessage(const proto::Abort *msg);

  //Fallback messages:
  proto::Phase1FB *GetUnusedPhase1FBmessage();
  proto::Phase2FB *GetUnusedPhase2FBmessage();
  proto::Phase1FBReply *GetUnusedPhase1FBReply();
  proto::Phase2FBReply *GetUnusedPhase2FBReply();
  proto::InvokeFB *GetUnusedInvokeFBmessage();
  proto::SendView *GetUnusedSendViewMessage();
  proto::ElectMessage *GetUnusedElectMessage();
  proto::ElectFB *GetUnusedElectFBmessage();
  proto::DecisionFB *GetUnusedDecisionFBmessage();
  proto::MoveView *GetUnusedMoveView();
  void FreePhase1FBmessage(proto::Phase1FB *msg);
  void FreePhase2FBmessage(const proto::Phase2FB *msg);
  void FreePhase1FBReply(proto::Phase1FBReply *msg);
  void FreePhase2FBReply(proto::Phase2FBReply *msg);
  void FreeInvokeFBmessage(proto::InvokeFB *msg);
  void FreeSendViewMessage(proto::SendView *msg);
  void FreeElectMessage(proto::ElectMessage *msg);
  void FreeElectFBmessage(proto::ElectFB *msg);
  void FreeDecisionFBmessage(proto::DecisionFB *msg);
  void FreeMoveView(proto::MoveView *msg);

  //Query messages:
  proto::QueryRequest* GetUnusedQueryRequestMessage();
  void FreeQueryRequestMessage(proto::QueryRequest *msg);
  proto::SyncClientProposal* GetUnusedSyncClientProposalMessage();
  void FreeSyncClientProposalMessage(proto::SyncClientProposal *msg);
  proto::RequestMissingTxns* GetUnusedRequestTxMessage();
  void FreeRequestTxMessage(proto::RequestMissingTxns *msg);
  proto::SupplyMissingTxns* GetUnusedSupplyTxMessage();
  void FreeSupplyTxMessage(proto::SupplyMissingTxns *msg);
  proto::PointQueryResultReply* GetUnusedPointQueryResultReply();
  void FreePointQueryResultReply(proto::PointQueryResultReply *msg);

  //generic delete function.
  void FreeMessage(::google::protobuf::Message *msg);


  inline bool IsKeyOwned(const std::string &key) const {
    if(sql_bench){
      return static_cast<int>((*part)("", key, numShards, groupIdx, dummyTxnGroups, true) % numGroups) == groupIdx; 
      //It's wasteful to incur a copy of the "table-name" for each single key... 
      //TODO: For writes: Try to get table_name from Write. //TODO: Maybe just check anyways?
      //TODO: Instead of copying Table Name + copying values to w_id => can we try just casting? reinterpret cast?
    }
    else{
      return static_cast<int>((*part)(key, numShards, groupIdx, dummyTxnGroups) % numGroups) == groupIdx;
    }
  }

  // Global objects.

  Stats stats;
  std::unordered_set<std::string> active;
  Latency_t committedReadInsertLat;
  Latency_t verifyLat;
  Latency_t signLat;

  Latency_t waitingOnLocks;
  //Latency_t waitOnProtoLock;


  const transport::Configuration &config;
  const int groupIdx;
  const int idx;
  const int numShards;
  const int numGroups;
  const int id;
  Transport *transport;
  const OCCType occType;
  Partitioner *part;
  const Parameters params;
  KeyManager *keyManager;
  const uint64_t timeDelta;
  TrueTime timeServer;
  BatchSigner *batchSigner;
  Verifier *verifier;
  Verifier *client_verifier;

  //ThreadPool* tp;
  std::mutex transportMutex;

  std::mutex mainThreadMutex;
  //finer mainThreadMutexes
  //if requiring multiple ones, acquire in this order:
  std::mutex storeMutex;
  std::mutex dependentsMutex;
  std::mutex waitingDependenciesMutex;
  mutable std::shared_mutex ongoingMutex;
  std::shared_mutex committedMutex;
  std::shared_mutex abortedMutex;
  std::shared_mutex preparedMutex;
  std::shared_mutex preparedReadsMutex;
  std::shared_mutex preparedWritesMutex;
  std::shared_mutex committedReadsMutex;

  std::shared_mutex rtsMutex;

  std::mutex atomic_testMutex;



 //FB datastructure mutexes //TODO make them shared too //TODO: use them in all FB functions...
  std::mutex p1ConflictsMutex;
  mutable std::mutex p1DecisionsMutex;
  mutable std::mutex p2DecisionsMutex;
  std::mutex interestedClientsMutex;
  mutable std::mutex current_viewsMutex;
  std::mutex decision_viewsMutex;
  std::mutex ElectQuorumMutex;
  std::mutex writebackMessagesMutex;


  std::mutex signMutex;

  //proto mutexes
  std::mutex protoMutex;
  std::mutex readReplyProtoMutex;
  std::mutex p1ReplyProtoMutex;
  std::mutex p2ReplyProtoMutex;
  std::mutex readProtoMutex;
  std::mutex p1ProtoMutex;
  std::mutex p2ProtoMutex;
  std::mutex WBProtoMutex;


  //std::vector<proto::CommittedProof*> testing_committed_proof;

  /* Declare protobuf objects as members to avoid stack alloc/dealloc costs */
  proto::SignedMessage signedMessage;
  proto::Read read;
  proto::Phase1 phase1;
  proto::Phase2 phase2;
  proto::Writeback writeback;
  proto::Abort abort;

  proto::Write preparedWrite;
  proto::ConcurrencyControl concurrencyControl;
  proto::AbortInternal abortInternal;
  std::vector<int> dummyTxnGroups;

  std::vector<proto::ReadReply *> readReplies;
  std::vector<proto::Phase1Reply *> p1Replies;
  std::vector<proto::Phase2Reply *> p2Replies;
  std::vector<proto::Read *> readMessages;
  std::vector<proto::Phase1 *> p1messages;
  std::vector<proto::Phase2 *> p2messages; //
  std::vector<proto::Writeback *> WBmessages; //
  proto::Phase1Reply phase1Reply;
  proto::Phase2Reply phase2Reply;
  proto::RelayP1 relayP1Msg;
  proto::Phase1FB phase1FB;
  //proto::Phase1FBReply phase1FBReply;
  proto::Phase2FB phase2FB;
  proto::Phase2FBReply phase2FBReply;
  proto::InvokeFB invokeFB;
  proto::ElectFB electFB;
  proto::DecisionFB decisionFB;
  proto::MoveView moveView;

  //Query messages
  proto::QueryRequest queryReq;
  proto::SyncClientProposal syncMsg;
  proto::RequestMissingTxns requestTx;
  proto::SupplyMissingTxns supplyTx;

  PingMessage ping;


//Simulated HMAC code
  std::unordered_map<uint64_t, std::string> sessionKeys;
  void CreateSessionKeys();
  bool ValidateHMACedMessage(const proto::SignedMessage &signedMessage);
  void CreateHMACedMessage(const ::google::protobuf::Message &msg, proto::SignedMessage *signedMessage);

// DATA STRUCTURES
  bool sql_bench;
  TableStore *table_store;
  // void ApplyTableWrites(const std::string &table_name, const TableWrite &table_write, const Timestamp &ts,
  //               const std::string &txn_digest, const proto::CommittedProof *commit_proof = nullptr, bool commit_or_prepare = true, bool forceMaterialize = false);
  //SQLTransformer sql_interpreter;

  VersionedKVStore<Timestamp, Value> store;
  // Key -> V
  //std::unordered_map<std::string, std::set<std::tuple<Timestamp, Timestamp, const proto::CommittedProof *>>> committedReads;
  typedef std::tuple<Timestamp, Timestamp, const proto::CommittedProof *> committedRead; //1) timestamp of reading tx, 2) timestamp of read value, 3) proof that Tx that wrote the read value (2) committed
  tbb::concurrent_unordered_map<std::string, std::pair<std::shared_mutex, std::set<committedRead>>> committedReads; //Note: implicitly ordered by timestamp of reading Tx (i.e. first tuple arg)
  //std::unordered_map<std::string, std::set<Timestamp>> rts;
  tbb::concurrent_unordered_map<std::string, std::atomic_uint64_t> rts;
  //tbb::concurrent_hash_map<std::string, std::set<Timestamp>> rts; //TODO: if want to use this again: need per key locks like below.
  tbb::concurrent_unordered_map<std::string, std::pair<std::shared_mutex, std::set<Timestamp>>> rts_list;

  void SetRTS(Timestamp &ts, const std::string &key);
  void ClearRTS(const google::protobuf::RepeatedPtrField<std::string> &read_set, const TimestampMessage &ts);
  void ClearRTS(const google::protobuf::RepeatedPtrField<ReadMessage> &read_set, const Timestamp&ts);

  // Digest -> V
  //std::unordered_map<std::string, proto::Transaction *> ongoing;
  // typedef tbb::concurrent_hash_map<std::string, proto::Transaction *> ongoingMap;
  // ongoingMap ongoing;

  struct ongoingData {
    ongoingData() : txn(nullptr), num_concurrent_clients(0UL){}
    ~ongoingData(){}
    proto::Transaction *txn;
    uint64_t num_concurrent_clients;
  };
  typedef tbb::concurrent_hash_map<std::string, ongoingData> ongoingMap;
  ongoingMap ongoing;

  tbb::concurrent_unordered_set<std::string> ongoingErased; //FIXME: REMOVE. Just for testing.

  struct ooMsg { //out of order msg
    ooMsg() : mode(0) {}
    ~ooMsg(){}
      uint8_t mode; //2 = wb, 1 =p2
      proto::Phase2 *p2;
      proto::Writeback *wb;
      TransportAddress *remoteCopy; 
  };
  typedef tbb::concurrent_hash_map<std::string, ooMsg> ooMap;
  ooMap ooMessages;

  typedef tbb::concurrent_hash_map<uint64_t, std::string> ts_to_txMap;
  ts_to_txMap ts_to_tx;

  // std::unordered_set<std::string> normal;
  // std::unordered_set<std::string> fallback;
  // std::unordered_set<std::string> waiting;

  // Prepared: Txn Digest -> V (Tiemstamp, Tx)  
      // Avoids duplicate CC
      // Used to check dependency presence
      // Used for sync availability
  //std::unordered_map<std::string, std::pair<Timestamp, const proto::Transaction *>> prepared;
  typedef tbb::concurrent_hash_map<std::string, std::pair<Timestamp, const proto::Transaction *>> preparedMap; //TODO: does this need to store Tx? (TS not necessary... it's part of TX)
  preparedMap prepared;

  // Prpepared Reads/Writes: Key -> Tx
      // Helper data structures to read prepared Values (preparedWrites); or to check for write-read conflicts during CC 
  //std::unordered_map<std::string, std::set<const proto::Transaction *>> preparedReads;
  tbb::concurrent_unordered_map<std::string, std::pair<std::shared_mutex, std::set<const proto::Transaction *>>> preparedReads;
  //std::unordered_map<std::string, std::map<Timestamp, const proto::Transaction *>> preparedWrites;
  tbb::concurrent_unordered_map<std::string, std::pair<std::shared_mutex,std::map<Timestamp, const proto::Transaction *>>> preparedWrites; //map from: key ->

  /* MAPS FOR SEMANTIC CC */
  //typedef tbb::concurrent_hash_map<std::string, std::map<Timestamp, ReadPredicate>> TablePredicateMap; //table_name => map(TS, Read Pred)  
  typedef tbb::concurrent_hash_map<std::string, std::map<Timestamp, std::pair<const proto::Transaction*, bool>>> TablePredicateMap; //table_name => map(TS, <Tx*, commit_or_prepare>)  
  TablePredicateMap tablePredicates;

  typedef tbb::concurrent_hash_map<std::string, std::map<Timestamp, std::pair<const proto::Transaction*, bool>>> TableWriteMap; //table_name => map(TS, <Tx*, commit_or_prepare>)  
  TableWriteMap tableWrites;

  typedef tbb::concurrent_hash_map<std::string, Timestamp> HighTblVmap;
  HighTblVmap highTableVersions;
  /* END MAPS FOR SEMANTIC CC*/

  //XXX key locks for atomicity of OCC check
  tbb::concurrent_unordered_map<std::string, std::mutex> lock_keys;
  void LockTxnKeys(proto::Transaction &txn);
  void UnlockTxnKeys(proto::Transaction &txn);

///XXX Sagars lock implementation.
  tbb::concurrent_unordered_map<std::string, std::mutex> mutex_map;
  //typedef std::vector<std::unique_lock<std::mutex>> locks_t;
  locks_t LockTxnKeys_scoped(const proto::Transaction &txn, const ReadSet &readSet);
  inline static bool sortReadByKey(const ReadMessage &lhs, const ReadMessage &rhs) { return lhs.key() < rhs.key(); }
  inline static bool sortWriteByKey(const WriteMessage &lhs, const WriteMessage &rhs) { return lhs.key() < rhs.key(); }

  //lock to make dependency handling atomic (per tx)  //TODO: Use this instead of waitingdep global mutex.
  tbb::concurrent_hash_map<std::string, std::mutex> completing;

  //std::unordered_map<std::string, proto::ConcurrencyControl::Result> p1Decisions;
  //std::unordered_map<std::string, const proto::CommittedProof *> p1Conflicts;
  //std::unordered_map<std::string, proto::CommitDecision> p2Decisions;
  //std::unordered_map<std::string, proto::CommittedProof *> committed;
  //std::unordered_set<std::string> aborted;
  //XXX TODO: p1Decisions and p1Conflicts erase only threadsafe if Clean called by a single thread.
  typedef tbb::concurrent_hash_map<std::string, proto::ConcurrencyControl::Result> p1DecisionsMap;
  p1DecisionsMap p1Decisions;
  typedef tbb::concurrent_hash_map<std::string, const proto::CommittedProof *> p1ConflictsMap;
  p1ConflictsMap p1Conflicts;
  tbb::concurrent_unordered_map<std::string, proto::CommittedProof *> committed;
  tbb::concurrent_unordered_set<std::string> aborted;
  tbb::concurrent_unordered_map<std::string, proto::Writeback> writebackMessages;
  //ADD Aborted proof to it.(in order to reply to Fallback)
  //creating new map to store writeback messages..  Need to find a better way, but suffices as placeholder


  //FB HELPER DATA STRUCTURES
  //keep list of timeouts
  //std::unordered_map<std::string, std::chrono::high_resolution_clock::time_point> FBclient_timeouts;
  std::unordered_map<std::string, uint64_t> client_starttime;

  //keep list for exponential timeouts for views.
  std::unordered_map<std::string, uint64_t> FBtimeouts_start; //Timer start time
  std::unordered_map<std::string, uint64_t> exp_timeouts; //current exp timeout size.

  //keep list for current view.
  //std::unordered_map<std::string, uint64_t> current_views;
  //keep list of the views in which the p2Decision is from
  //std::unordered_map<std::string, uint64_t> decision_views;


  //typedef std::pair< std::unordered_set<uint64_t>, std::unordered_set<proto::Signature*>>replica_sig_sets_pair;
  typedef std::pair< std::unordered_set<uint64_t>, std::pair<proto::Signatures, uint64_t>> replica_sig_sets_pair;
  struct ElectFBorganizer {
    std::map<uint64_t, bool> view_complete; //TODO: inefficient to do 2 lookups, merge with view_quorums.
    std::map<uint64_t, std::unordered_map<proto::CommitDecision, replica_sig_sets_pair>> view_quorums;
    std::map<uint64_t, std::pair<uint64_t, bool >> move_view_counts;
  };
  typedef tbb::concurrent_hash_map<std::string, ElectFBorganizer> ElectQuorumMap;
  ElectQuorumMap ElectQuorums;

  tbb::concurrent_hash_map<std::string, P1FBorganizer*> fallbackStates;
  //TODO: put all other info such as current views, Quorums etc in this?

  //std::unordered_map<std::string, std::unordered_set<std::string>> dependents; // Each V depends on K
  //tbb hashmap<string,
  struct WaitingDependency {
    uint64_t reqId;
    const TransportAddress *remote;
    std::unordered_set<std::string> deps;  //needs to be a tbb::hashmap.
  };
  std::unordered_map<std::string, WaitingDependency> waitingDependencies; // K depends on each V

  //XXX re-writing concurrent:
  typedef tbb::concurrent_hash_map<std::string, std::unordered_set<std::string> > dependentsMap; //can be unordered set, as long as i keep lock access long enough
  dependentsMap dependents;

  struct WaitingDependency_new {
    bool original_client;
    std::string txnDigest;
    uint64_t reqId;
    const TransportAddress *remote;
    std::mutex deps_mutex;
    std::unordered_set<std::string> deps; //acquire mutex before erasing (or use hashmap)
  };
  typedef tbb::concurrent_hash_map<std::string, WaitingDependency_new> waitingDependenciesMap;
  waitingDependenciesMap waitingDependencies_new;

  //Note: 
  //dependents is a map from tx -> all tx that are waiting for it
  //waitingDependencies: map from waiting tx -> list of tx that are missing


  //TEST FUNCTIONS:
  std::string TEST_QUERY_f(uint64_t q_seq_no);
  void TEST_QUERY_f(std::string &result);
  void TEST_QUERY_f(proto::Write *write, proto::PointQueryResultReply *pointQueryReply);
  void TEST_READ_SET_f(pequinstore::proto::QueryResult *result);
  void TEST_SNAPSHOT_f(proto::Query *query, QueryMetaData *query_md);

  void TEST_MATERIALIZE_f();
  void TEST_MATERIALIZE_FORCE_f(const proto::Transaction *txn, const std::string &tx_id);
  void TEST_READ_MATERIALIZED_f();
  void TEST_READ_FROM_SS_f();

};

} // namespace pequinstore

#endif /* _PEQUIN_SERVER_H_ */
