/***********************************************************************
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

#ifndef _VALIDATION_CLIENT_API_H_
#define _VALIDATION_CLIENT_API_H_

#include "lib/transport.h"
#include "store/common/frontend/client.h"
#include "store/common/promise.h"
#include "store/common/timestamp.h"
#include "store/common/partitioner.h"
#include "store/sintrstore/sintr-proto.pb.h"
#include "store/sintrstore/common.h"

#include <string>
#include <vector>
#include <thread>

#include "tbb/concurrent_hash_map.h"

namespace sintrstore {

typedef std::function<void(int, uint64_t, uint64_t, const std::string &,
  const std::string &, const Timestamp &)> validation_read_callback;
typedef std::function<void(int, const std::string &)> validation_read_timeout_callback;

// this acts as a dummy workload client for validation of one transaction at a time
// validation transactions will invoke this through a SyncClient interface
// note that this class is shared memory between threads
// in particular, each thread where a validation transaction is being validated (client2client::ValidationThreadFunction)
// will call Begin, Get, Put, Commit, Abort (these through SyncClient interface), 
// SetThreadValTxnId, SetTxnTimestamp, GetCompletedTxn
// on a different thread, client2client will call ProcessForwardReadResult upon receiving forwarded read results
class ValidationClient : public ::Client {
 public:
  ValidationClient(Transport *transport, uint64_t client_id, uint64_t nshards, uint64_t ngroups, Partitioner *part);
  virtual ~ValidationClient();

  // Begin a transaction.
  virtual void Begin(begin_callback bcb, begin_timeout_callback btcb,
    uint32_t timeout, bool retry = false, const std::string &txnState = std::string()) override;

  // Get the value corresponding to key.
  virtual void Get(const std::string &key, get_callback gcb,
    get_timeout_callback gtcb, uint32_t timeout) override;

  // Set the value for the given key.
  virtual void Put(const std::string &key, const std::string &value,
    put_callback pcb, put_timeout_callback ptcb, uint32_t timeout) override;

  // Commit all Get(s) and Put(s) since Begin().
  virtual void Commit(commit_callback ccb, commit_timeout_callback ctcb, uint32_t timeout) override;

  // Abort all Get(s) and Put(s) since Begin().
  virtual void Abort(abort_callback acb, abort_timeout_callback atcb, uint32_t timeout) override;

  // Set the current transaction id (client that initiated and seq num)
  // associate transaction id with current thread id
  void SetThreadValTxnId(uint64_t txn_client_id, uint64_t txn_client_seq_num);

  // Set the timestamp for the txn
  // timestamp was chosen by initiating client
  // this is expected to be called before the validation transaction begins
  void SetTxnTimestamp(uint64_t txn_client_id, uint64_t txn_client_seq_num, const Timestamp &ts);

  // either fill one of the pending validation gets or put into readset for future validation get
  void ProcessForwardReadResult(uint64_t txn_client_id, uint64_t txn_client_seq_num, 
    const proto::ForwardReadResult &fwdReadResult, const proto::Dependency &dep, bool hasDep, bool addReadset,
    const proto::Dependency &policyDep, bool hasPolicyDep);

  // return completed transaction for requested id
  proto::Transaction *GetCompletedTxn(uint64_t txn_client_id, uint64_t txn_client_seq_num);

 private:
  struct PendingValidationGet {
    PendingValidationGet(uint64_t txn_client_id, uint64_t txn_client_seq_num) : 
        txn_client_id(txn_client_id), txn_client_seq_num(txn_client_seq_num) {
      struct timespec ts_start;
      clock_gettime(CLOCK_MONOTONIC, &ts_start);
      start_time = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;
    }
    ~PendingValidationGet() {
      if (timeout != nullptr) {
        delete timeout;
      }
    }
    uint64_t txn_client_id;
    uint64_t txn_client_seq_num;
    std::string key;
    std::string value;
    Timestamp ts;
    validation_read_callback vrcb;
    validation_read_timeout_callback vrtcb;
    Timeout *timeout;
    uint64_t start_time;
  };

  struct PendingValidationQuery {
    // difference between query seq num and client seq num?
    PendingValidationQuery(uint64_t txn_client_id, const TimestampMessage &ts, uint64_t txn_client_seq_num, uint64_t query_seq_num,
        const std::string &query_cmd, const query_callback &qcb, bool cache_result) : txn_client_seq_num(txn_client_seq_num), 
        version(0UL), group_replies(0UL), vqcb(qcb), cache_result(cache_result){
      queryMsg.Clear();
      queryMsg.set_client_id(txn_client_id);
      queryMsg.set_query_seq_num(query_seq_num);
      *queryMsg.mutable_query_cmd() = std::move(query_cmd);
      *queryMsg.mutable_timestamp() = std::move(ts);
      queryMsg.set_retry_version(0);

      query_gen_id = QueryGenId(queryMsg.query_cmd(), queryMsg.timestamp());
      struct timespec ts_start;
      clock_gettime(CLOCK_MONOTONIC, &ts_start);
      start_time = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;
    }
    ~PendingValidationQuery(){
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
    void SetQueryId(bool hash_query_id){
      //bool hash_query_id = client->params.query_params.signClientQueries && client->params.query_params.cacheReadSet && client->params.hashDigest;
      queryId = QueryDigest(queryMsg, hash_query_id); 

      // if(client->params.query_params.signClientQueries && client->params.query_params.cacheReadSet){ //TODO: when to use hash id? always?
      //     queryId = QueryDigest(queryMsg, client->params.hashDigest); 
      // }
      // else{
      //     queryId =  "[" + std::to_string(queryMsg.query_seq_num()) + ":" + std::to_string(queryMsg.client_id()) + "]";
      // }
    }
    bool cache_result;
    query_callback vqcb;
    query_timeout_callback vqcb_timeout;

    uint64_t version;
    std::string queryId;

    std::string query_gen_id;

    proto::Query queryMsg;
    
    std::vector<uint64_t> involved_groups;
    //std::map<uint64_t, std::map<std::string, TimestampMessage>> group_read_sets;
    std::map<uint64_t, proto::ReadSet*> group_read_sets;
    std::map<uint64_t, std::string> group_result_hashes;
    std::map<uint64_t, std::vector<proto::SignedMessage>> group_sigs;
    std::string result;
    uint64_t group_replies;
    uint64_t start_time;
    uint64_t txn_client_seq_num;

    bool is_point;
    std::string key;
    std::string table_name;
    std::vector<std::string> p_col_values; //if point read: this contains primary_key_col_vaues (in order) ==> Together with table_name can be used to compute encoding.
  };


  // for a (txn_client_id, txn_client_seq_num) pair, keep track of all relevant transaction state
  struct AllValidationTxnState {
    AllValidationTxnState() {}
    AllValidationTxnState(uint64_t txn_client_id, uint64_t txn_client_seq_num, proto::Transaction *txn) : 
      txn_client_id(txn_client_id), txn_client_seq_num(txn_client_seq_num), txn(txn) {}
    ~AllValidationTxnState() {
      // do not delete txn, since it is returned from GetCompletedTxn
      // delete all pendingGets
      for (auto &pendingGet : pendingGets) {
        delete pendingGet;
      }
    }
    
    uint64_t txn_client_id;
    uint64_t txn_client_seq_num;
    // this tracks the readset/writeset etc. of the transaction
    proto::Transaction *txn;
    // this tracks the locally buffered key-value pairs
    std::map<std::string, std::string> readValues;
    // this tracks the pending validation gets
    std::vector<PendingValidationGet *> pendingGets;
  };
  
  bool BufferGet(const AllValidationTxnState *allValTxnState, const std::string &key, 
    validation_read_callback vrcb);
  // add (key, ts) to the readset of transaction txn_id
  void AddReadset(AllValidationTxnState *allValTxnState, const std::string &key, 
    const std::string &value, const Timestamp &ts);
  // add dep to the dependencies of transaction 
  void AddDep(AllValidationTxnState *allValTxnState, const proto::Dependency &dep);
  // is group g involved in txn
  bool IsTxnParticipant(proto::Transaction *txn, int g);
  // read from threadValTxnIds and set the passed in pointers to the current threads txn id 
  void GetThreadValTxnId(uint64_t *txn_client_id, uint64_t *txn_client_seq_num);
  std::string ToTxnId(uint64_t txn_client_id, uint64_t txn_client_seq_num);

  // transport for timeout functionality
  Transport *transport;
  // My own client ID
  uint64_t client_id;
  // Number of shards.
  uint64_t nshards;
  // Number of replica groups.
  uint64_t ngroups;
  // for computing txn involved groups
  Partitioner *part;

  // map from thread id to (txn_client_id, txn_client_seq_num) tracks what each thread is doing
  typedef tbb::concurrent_hash_map<std::thread::id, std::pair<uint64_t, uint64_t>> threadValTxnIdsMap;
  threadValTxnIdsMap threadValTxnIds;
  // map from (txn_client_id, txn_client_seq_num) to all relevant validation txn state
  typedef tbb::concurrent_hash_map<std::string, AllValidationTxnState *> allValTxnStatesMap;
  allValTxnStatesMap allValTxnStates;
};

} // namespace sintrstore

#endif /* _VALIDATION_CLIENT_API_H_ */
