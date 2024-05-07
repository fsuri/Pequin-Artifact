/***********************************************************************
 *
 * store/pequinstore/querysync-servertools.cc: 
 *      Implementation of server-side query orchestration in Pesto
 *
 * Copyright 2023 Florian Suri-Payer <fsp@cs.cornell.edu>
 *            
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), 
 * to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, 
 * and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. 
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, 
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 **********************************************************************/


#include <cryptopp/sha.h>
#include <cryptopp/blake2.h>


#include "store/pequinstore/server.h"

#include <bitset>
#include <queue>
#include <ctime>
#include <chrono>
#include <sys/time.h>
#include <sstream>
#include <list>
#include <utility>

#include "lib/assert.h"
#include "lib/tcptransport.h"
#include "store/common/timestamp.h"
#include "store/common/transaction.h"
#include "store/pequinstore/common.h"
#include "store/pequinstore/phase1validator.h"
#include "store/pequinstore/localbatchsigner.h"
#include "store/pequinstore/sharedbatchsigner.h"
#include "store/pequinstore/basicverifier.h"
#include "store/pequinstore/localbatchverifier.h"
#include "store/pequinstore/sharedbatchverifier.h"
#include "lib/batched_sigs.h"
#include <valgrind/memcheck.h>

#include "store/common/query_result/query_result_proto_builder.h"

namespace pequinstore {

/////////////////////////// EXECUTION ////////////////////////////

std::string Server::ExecQuery(QueryReadSetMgr &queryReadSetMgr, QueryMetaData *query_md, bool read_materialized, bool eager){
    //TODO: Must take as input some Materialization info... (whether to use a materialized snapshot (details are in query_md), or whether to just use current state)
    //TODO: Must be able to report exec failure (e.g. materialized snapshot inconsistent) -- note that if eagerly executiong (no materialization) there is no concept of failure.


    struct timespec ts_start;
    clock_gettime(CLOCK_MONOTONIC, &ts_start);
    uint64_t microseconds_start = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;

     if(TEST_READ_MATERIALIZED) TEST_READ_MATERIALIZED_f();

        /////////////////////////////////////////////////////////////
    //                                                         //
    //                                                         //
    //                                                         //
    //
    //              EXEC BLACKBOX 

    std::string serialized_result;
    if(!read_materialized){
        if(eager && params.query_params.eagerPlusSnapshot){ //If eager exec Plus Snapshot => find snapshot as part of ExecRead
            proto::LocalSnapshot *local_ss = query_md->queryResultReply->mutable_result()->mutable_local_ss();
            query_md->snapshot_mgr.InitLocalSnapshot(local_ss, query_md->query_seq_num, query_md->client_id, id, query_md->useOptimisticTxId);
            std::cerr << "USE OPT???? " << query_md->useOptimisticTxId << std::endl;
            serialized_result = table_store->EagerExecAndSnapshot(query_md->query_cmd, query_md->ts, query_md->snapshot_mgr, queryReadSetMgr, params.query_params.snapshotPrepared_k);
            query_md->snapshot_mgr.SealLocalSnapshot(); 
             Debug("Number of Txn in snapshot (post seal): [com:%d; prep:%d]. Number of TS in snapshot: [com:%d; prep:%d]", local_ss->local_txns_committed_size(), local_ss->local_txns_prepared_size(), local_ss->local_txns_committed_ts_size(), local_ss->local_txns_prepared_ts_size());
            for(auto &ts: local_ss->local_txns_committed_ts()){
                Debug("Snapshot Txn TS (commit): %lu", ts);
            }
            for(auto &ts: local_ss->local_txns_prepared_ts()){
                Debug("Snapshot Txn TS (prep): %lu", ts);
            }
        } 
        else{
            serialized_result = table_store->ExecReadQuery(query_md->query_cmd, query_md->ts, queryReadSetMgr);
        }
    } 
    if(read_materialized){
        Warning("READ FROM MATERIALIZED SNAPSHOT NOT YET STABLE TESTED");
        serialized_result = table_store->ExecReadQueryOnMaterializedSnapshot(query_md->query_cmd, query_md->ts, queryReadSetMgr, query_md->merged_ss_msg->merged_txns());
    } 

    //                                                         //
    //                                                         //
    //                                                         //
    //                                                         //
    /////////////////////////////////////////////////////////////
    //TODO: Result should be of protobuf result type: --> can either return serialized value, or result object (probably easiest) -- but need serialized value anyways to check for equality.



    Debug("SERIALIZED RESULT: %lu", std::hash<std::string>{}(serialized_result));

     //If MVTSO: Read prepared (handled by predicate in table_store), Set RTS
    if(occType == MVTSO && params.rtsMode > 0){
        Debug("Set up all RTS for Query[%lu:%lu]", query_md->client_id, query_md->query_seq_num);
        for(auto &read: queryReadSetMgr.read_set->read_set()){
            SetRTS(query_md->ts, read.key());
        }
        //TODO: On Abort, Clear RTS.
    }

    /////////////////// THE FOLLOWING IS JUST DEBUG CODE

            if(TEST_READ_SET){

                for(auto const&[tx_id, proof] : committed){
                    if(tx_id == "toy_txn") continue;
                    const proto::Transaction *txn = &proof->txn();
                    for(auto &write: txn->write_set()){
                        queryReadSetMgr.AddToReadSet(write.key(), txn->timestamp());
                        Debug("Added read key %s and ts to read set", write.key().c_str());
                    }
                    //queryReadSetMgr.AddToDepSet(tx_id, query_md->useOptimisticTxId, txn->timestamp());
                }
                
                //Creating Dummy keys for testing 
                for(int i=5;i > 0; --i){
                    TimestampMessage ts;
                    ts.set_id(query_md->ts.getID());
                    ts.set_timestamp(query_md->ts.getTimestamp());
                    std::string dummy_key = groupIdx == 0 ? "dummy_key_g1_" + std::to_string(i) : "dummy_key_g2_" + std::to_string(i);

                    queryReadSetMgr.AddToReadSet(dummy_key, ts);
                    //query_md->read_set[dummy_key] = ts; //query_md->ts;
                    //replaced with repeated field -> directly in result object.
                
                    // ReadMessage *read = query_md->queryResultReply->mutable_result()->mutable_query_read_set()->add_read_set();
                    // //ReadMessage *read = query_md->queryResult->mutable_query_read_set()->add_read_set();
                    // read->set_key(dummy_key);
                    // *read->mutable_readtime() = ts;
                }
                //Creating Dummy deps for testing 
            
                    //Write to Query Result; Release/Re-allocate temporarily if not sending;
                    //For caching:
                        // Cache the deps --> During CC: look through the data structure.
                    //For non-caching:
                        // Add the deps to SyncReply --> Let client choose whether to include them (only if proposed them in merge; marked as prep) --> During CC: Look through the included deps.

                //During execution only read prepared if depth allowed.
                //  i.e. if (params.maxDepDepth == -1 || DependencyDepth(txn) <= params.maxDepDepth)  (maxdepth = -1 means no limit)
                if (params.query_params.readPrepared && params.maxDepDepth > -2) {

                    //JUST FOR TESTING.
                    for(preparedMap::const_iterator i=prepared.begin(); i!=prepared.end(); ++i ) {
                        const std::string &tx_id = i->first;
                        const proto::Transaction *txn = i->second.second;

                        queryReadSetMgr.AddToDepSet(tx_id, txn->timestamp());

                        // proto::Dependency *add_dep = query_md->queryResultReply->mutable_result()->mutable_query_read_set()->add_deps();
                        // add_dep->set_involved_group(groupIdx);
                        // add_dep->mutable_write()->set_prepared_txn_digest(tx_id);
                        // Debug("Adding Dep: %s", BytesToHex(add_dep->write().prepared_txn_digest(), 16).c_str());
                        // //Note: Send merged TS.
                        // if(query_md->useOptimisticTxId){
                        //     //MergeTimestampId(txn->timestamp().timestamp(), txn->timestamp().id()
                        //     add_dep->mutable_write()->mutable_prepared_timestamp()->set_timestamp(txn->timestamp().timestamp());
                        //     add_dep->mutable_write()->mutable_prepared_timestamp()->set_id(txn->timestamp().id());
                        // }
                    }
                }
            }


            if(PRINT_READ_SET){
                for(auto &read : queryReadSetMgr.read_set->read_set()){
                    Debug("Read key %s with version [%lu:%lu]", read.key().c_str(), read.readtime().timestamp(), read.readtime().id());
                }
                for(auto &dep : queryReadSetMgr.read_set->deps()){
                    Debug("Dependency on Txn: %s", BytesToHex(dep.write().prepared_txn_digest(), 16).c_str());
                }
            }

            //Just for testing: Creating Dummy result 
            if(TEST_QUERY) return TEST_QUERY_f(query_md->query_seq_num);

            if(PRINT_RESULT_ROWS){
                query_result::QueryResult *q_result = new sql::QueryResultProtoWrapper(serialized_result);
                Debug("Result size: %d. Result rows affected: %d", q_result->size(), q_result->rows_affected());

                for(int i = 0; i < q_result->size(); ++i){
                    std::unique_ptr<query_result::Row> row = (*q_result)[i]; 
                    Debug("Checking row at index: %d", i);
                    // For col in col_updates update the columns specified by update_cols. Set value to update_values
                    for(int j=0; j<row->num_columns(); ++j){
                        const std::string &col = row->name(j);
                        std::unique_ptr<query_result::Field> field = (*row)[j];
                        const std::string &field_val = field->get();
                        Debug("  %s:  %s", col.c_str(), field_val.c_str());
                    }
                }
            }
    //// END DEBUG CODE        
    
    //FIXME: REMOVE
    struct timespec ts_end;
    clock_gettime(CLOCK_MONOTONIC, &ts_end);
    uint64_t microseconds_end = ts_end.tv_sec * 1000 * 1000 + ts_end.tv_nsec / 1000;
    auto duration = microseconds_end - microseconds_start;
    Warning("Query exec duration: %d us. Q[%s] [%lu:%lu]", duration, query_md->query_cmd.c_str(), query_md->client_id, query_md->query_seq_num);
    
   
    return serialized_result;
}

void Server::ExecQueryEagerly(queryMetaDataMap::accessor &q, QueryMetaData *query_md, const std::string &queryId){

    query_md->executed_query = true;

    Debug("Eagerly Execute Query[%lu:%lu:%lu].", query_md->client_id, query_md->query_seq_num, query_md->retry_version);
    QueryReadSetMgr queryReadSetMgr(query_md->queryResultReply->mutable_result()->mutable_query_read_set(), groupIdx, false); 

    std::string result(ExecQuery(queryReadSetMgr, query_md, false, true)); //eager = true
    
    Debug("Got result for Query[%lu:%lu:%lu].", query_md->client_id, query_md->query_seq_num, query_md->retry_version);

    if(TEST_QUERY) TEST_QUERY_f(result);
   

    query_md->has_result = true; 

    if(query_md->designated_for_reply){
            query_md->queryResultReply->mutable_result()->set_query_result(result);
            //query_md->queryResult->set_query_result(dummy_result); //TODO: replace with real result
    }
    else{
            query_md->queryResultReply->mutable_result()->set_query_result(result); //set for non-query manager.
            //query_md->queryResult->set_query_result(dummy_result);
    }

    SendQueryReply(query_md);
    uint64_t retry_version = query_md->retry_version;
    q.release();

      //After executing and caching read set -> Try to wake possibly subscribed transaction that has started to prepare, but was blocked waiting on it's cached read set.
    if(params.query_params.cacheReadSet) wakeSubscribedTx(queryId, retry_version); //TODO: Instead of passing it along, just store the queryId...
}


////////////////////////// SNAPSHOTTING /////////////////////////

void Server::FindSnapshot(QueryMetaData *query_md, proto::Query *query){

    // 1) Parse & Execute Query
    // SQL glue. How to execute from query plan object.
    /////////////////////////////////////////////////////////////
    //                                                         //
    //                                                         //
    //                                                         //
    //
    //              EXEC BLACKBOX -- TBD
    table_store->FindSnapshot(query_md->query_cmd, query_md->ts, query_md->snapshot_mgr, params.query_params.snapshotPrepared_k);
    //                                                         //
    //                                                         //
    //                                                         //
    //                                                         //
    /////////////////////////////////////////////////////////////

    // 2) Execute all Scans in query --> find txnSet (for each key, last few tx)  --   //TODO: Make sure you don't just report the latest tx for a given key, but a couple latest versions (sync merge might lose the latest)
    
    // Generate Snapshot:Create list of all txn-ids necessary for state
            // if txn exists locally as committed and prepared, only include as committed
            // Use optimistic Tx-ids (= timestamp) if param set

      //How to find txnSet efficiently for key WITH RESPECT to Timestamp. Is scanning the only option?
    //Could already store a whole tx map for each key: map<key, deque<TxnIds>> --? replace tx_ids evertime a newer one comes along (pop front, push_back). 
    // Problem: May come in any TS order. AND: Query with TS only cares about TxId < TS


    //FindSnapshot(local_ss, query_cmd); //TODO: Function that calls blackbox exec and sets snapshot.

    //FIXME: TOY TX PROOF IN COMMITTED
    if(TEST_SNAPSHOT) TEST_SNAPSHOT_f(query, query_md);
}

//const std::string *queryId, 
bool Server::CheckPresence(const std::string &tx_id, const std::string &query_retry_id, QueryMetaData *query_md, 
        std::map<uint64_t, proto::RequestMissingTxns> &replica_requests, const pequinstore::proto::ReplicaList &replica_list, 
        std::unordered_map<std::string, uint64_t> &missing_txns)
{
   
    const proto::Transaction *txn = nullptr;

     //i) Check whether replica has the txn.: If not ongoing, and not commit/abort --> then we have not received it yet. (Follows from commit/aborted being updated before erasing from ongoing)
    //CheckPresence(tx_id, merged_ss, queryId);
    ongoingMap::const_accessor o;
    //bool has_txn_locally = ongoing.find(o, tx_id)? true : (committed.find(tx_id) != committed.end() || aborted.find(tx_id) != aborted.end());


    bool is_ongoing = ongoing.find(o, tx_id);
    bool is_final = false;
   
    if (committed.find(tx_id) != committed.end()) is_final = true;
    else if (aborted.find(tx_id) != aborted.end()){  //If Query not ongoing/committed --> Fail Query early if aborted. 
        is_final = true;
        //Remove from snapshot.
        query_md->merged_ss_msg->mutable_merged_txns()->erase(tx_id); //Note: Don't need to erase in new model -> simply won't be materialized! 
        //However, it is still useful: A Tx could've been prepared (hence materialized), but also aborted (but not yet purged). Removing from snapshot allows us to ignore.

        // Note: CURRENTLY NOT USING FAILQuery here: An aborted tx in the snapshot might not be on the execution frontier... -> Fail only during exec. Just proceed here (mark tx as "has_locally")
        // FailQuery(query_md);   // PLUS: May actually WANT to not read from the aborted tx.
        // delete merged_ss;
        // return;
    }

    bool has_txn_locally = is_ongoing || is_final;

    bool preparing = is_ongoing && !is_final;   //Note: If ~has_txn_locally => preparing is false.
    if(preparing) txn = o->second.txn;
    o.release();

    Debug("Checked presence for txn[%s]. Present? %d. Is final? %d. Is ongoing? %d. Is preparing? %d", BytesToHex(tx_id, 16).c_str(), has_txn_locally, is_final, is_ongoing, preparing);
    // bool has_txn_locally = ongoing.find(o, tx_id);
    // ///o.release();
    // if(!has_txn_locally){
    //     if (committed.find(tx_id) != committed.end()) has_txn_locally = true;
    //     else if (aborted.find(tx_id) != aborted.end()){  //If Query not ongoing/committed --> Fail Query early if aborted. 
    //         has_txn_locally = true;
    //         //Remove from snapshot.
    //         merged_ss->mutable_merged_txns()->erase(tx_id); //TODO: Don't need to erase in new model -> simply won't be materialized! 

    //         // Note: CURRENTLY NOT USING FAILQuery here: An aborted tx in the snapshot might not be on the execution frontier... -> Fail only during exec. Just proceed here (mark tx as "has_locally")
    //         // FailQuery(query_md);   // PLUS: May actually WANT to not read from the aborted tx.
    //         // delete merged_ss;
    //         // return;
    //     }
    // }
    // o.release(); 

     //1) If we have never seen the Txn, we must request it
     if(TEST_SYNC || !has_txn_locally){                                      //TODO: TEST_SYNC will trigger message sync, but won't cause Query to wait for it. 
        RequestMissing(replica_list, replica_requests, tx_id);
    }

    //2) regardless of whether we've seen the Txn: wait for it to be applied (if it is not already applied or aborted)
         
         //TODO: if we don't sync on a TX that has already TryPrepared (or is in the process of doing so): need to make so that it force materializes too
         // only check for the Txns that are ongoing, but not committed/aborted
         //  If BufferP1 result exists => call ForceMaterialize
         // If it doesn't => register in BufferP1 to call ForceMaterialize       ~~~TODO: Move ForceMaterialize into HandlePhase1CB (currently in TryPrepare)~~~

    bool ready = WaitForMaterialization(tx_id, query_retry_id, missing_txns);

    Debug("Check materialization of txn: %s. Is mat? %d", BytesToHex(tx_id, 16).c_str(), ready);
    if(!ready && (preparing || TEST_MATERIALIZE_FORCE)){
        //if Tx not yet materialized (ready) but it is in process of preparing => Register Force Materialization (i.e. force apply TableWrite in case Prepare turns out to be Abstain)

        if(TEST_MATERIALIZE_FORCE) TEST_MATERIALIZE_FORCE_f(txn, tx_id);
     
        RegisterForceMaterialization(tx_id, txn); 
    }

   
    return ready;
}

bool Server::CheckPresence(const uint64_t &ts_id, const std::string &query_retry_id, QueryMetaData *query_md, 
        std::map<uint64_t, proto::RequestMissingTxns> &replica_requests, const pequinstore::proto::ReplicaList &replica_list, 
        std::unordered_map<std::string, uint64_t> &missing_txns, std::unordered_map<uint64_t, uint64_t> &missing_ts)
{

    //TODO: Update ts_to_tx at more places. Whenever we see the TX for the first time.
    //Eg. in HandlePhase2, and HandleWriteback. (Also in FB)

    //transform ts_id to txnDigest if using optimistc ids.. // Check local mapping from Timestamp to TxnDigest 
    ts_to_txMap::const_accessor t;
    bool has_txn_locally = ts_to_tx.find(t, ts_id);//.. find in map. If yes, add tx_id to merged_txns. If no, try to sync on TS.
    if(has_txn_locally){
        //Add Txn to snapshot if present (and not aborted)
        const std::string &tx_id = t->second;
        if(aborted.find(tx_id) == aborted.end()) (*query_md->merged_ss_msg->mutable_merged_txns())[tx_id]; //(Just add with default constructor --> empty ReplicaList)
        //query_md->merged_ss.insert(t->second); //store snapshot locally.  
        t.release();

        Debug("TX translation exists: TS[%lu] -> TX[%s]", ts_id, BytesToHex(tx_id, 16).c_str());
        return CheckPresence(tx_id, query_retry_id, query_md, replica_requests, replica_list, missing_txns);
    }

    else{
        Debug("TX translation does not exist: TS[%lu] WAITING for TX", ts_id);
        RequestMissing( replica_list, replica_requests, ts_id);
        
        WaitForTX(ts_id,  query_retry_id, missing_ts);

        return false;

         //TS based Sync/Materialize procedure: 
        // 0) If TS to TX translation exists locally => Use TX based materialization orchestration (CheckPresence)
        // => Otherwise: 
        // 1) Request Synchronization & Add to TX to WaitingTS 
        // 2) Upon registering TX (TryPrepare, Commit, Abort) => Call CheckWaitingQueriesTS    
                        //Note: For prepare: called after TX is added to ongoing (i.e. we won't sync again) -- Notably, this is before TableWrites may or may not be applied. (Could also do it inside AddOngoing)
                        //For commit/abort: called after TableWrites and materialized maps have been written
                                                                        
        // 3) For all queries waiting on the TS:
                // add Tx to snapshot if it is prep/commit; don't add/erase if it is abort
                // Call TX-based materialization: Check Presence  (Note: locality checks will pass (no re-sync); but we will check WaitingForMaterialization, which might be triggered if the Tx prepares abstain)
                    //Note: Realistically, if Tx translation does not exist => we learn of TX via sync. But if we receive it in parallel we'd need a way to register the TS to forceMaterialize.
                    //Unfortunately, we cannot use the RegisterForceMaterialization interface for TS (it would require to add yet another BufferP1 like structure indexed by TS. 
                        //The TS to TX based wakeup translation resolves this issue, and allows us to omit any additional maps indexed by TS (e.g. materializedTS is not necessary)
                // If all TS/TX are done (all materialized) => Call SyncCallback directly.
    }
}

void Server::RequestMissing(const proto::ReplicaList &replica_list, std::map<uint64_t, proto::RequestMissingTxns> &replica_requests, const std::string &tx_id){
   
     Debug("Request missing TX[%s]", BytesToHex(tx_id, 16).c_str());
    uint64_t count = 0;
    for(auto const &replica_id: replica_list.replicas()){ 
        Debug("    Request missing Txn[%s] from replica id[%d]", BytesToHex(tx_id, 16).c_str(), replica_id);
        if(count > config.f +1) return; //only send to f+1 --> an honest client will never include more than f+1 replicas to request from. --> can ignore byz request.
        
        
        uint64_t replica_idx = replica_id % config.n;  //since  id = local-groupIdx * config.n + idx
        if(replica_idx != idx){
            proto::RequestMissingTxns &req_txn = replica_requests[replica_idx];

            req_txn.add_missing_txn(tx_id);
            req_txn.set_replica_idx(idx);
            // std::string *next_txn = replica_requests[replica_idx].add_missing_txn();
            // *next_txn = tx_id;
            // replica_requests[replica_idx].set_replica_idx(idx);
        }
        else {
            Panic("Should not be trying to request missing from a TX whose snapshot claims Replica was part. This is a byz behavior that should not trigger in tests.");
        }
        count++;
    }
}

void Server::RequestMissing(const proto::ReplicaList &replica_list, std::map<uint64_t, proto::RequestMissingTxns> &replica_requests, const uint64_t &ts_id) {
  
    Debug("Request missing TS[%lu]", ts_id);
    uint64_t count = 0;
    for(auto const &replica_id: replica_list.replicas()){ 
        Debug("    Request missing TS[%lu] from replica %d", ts_id, replica_id);
        if(count > config.f +1) return; //only send to f+1 --> an honest client will never include more than f+1 replicas to request from. --> can ignore byz request.
        
        
        uint64_t replica_idx = replica_id % config.n;  //since  id = local-groupIdx * config.n + idx
        if(replica_idx != idx){
            proto::RequestMissingTxns &req_txn = replica_requests[replica_idx];
            req_txn.add_missing_txn_ts(ts_id);
            req_txn.set_replica_idx(idx);
        }
        else if(!TEST_MATERIALIZE_TS) {
            Panic("Should not be trying to request missing from a TX whose snapshot claims Replica was part. This is a byz behavior that should not trigger in tests.");
        }
        count++;
    }
}

///////////////// SNAPSHOT MATERIALIZATION

//TODO: is it possible, through some weird interleaving to forceMaterialize after it was already WAIT, but turned into ABSTAIN?
      //Yes, if we call RegisterForceMat after it woke. : TODO: Could add a flag: wasMaterialized (which is true if it was WAIT at SOME point)

      //NO:~  Wouldn't call RegisterMaterialize to begin with!!!! Since already materialized in map.  TODO: Think through interleaving carefully, probably fine either way.
      //  bool hasMaterialized; //already Mat once, don't need to re-force. E.g. if it was WAIT at some point, but then turned into abstain.
      //    c->second.hasMaterialized = true; //TODO: Do not need this. Wouldn't call RegisterMaterialize to begin with!!!!
bool Server::RegisterForceMaterialization(const std::string &txnDigest, const proto::Transaction *txn){
 //Note: This is called if a Tx is ongoing but not committed/aborted & it is not yet materialized. (ongoing indicates that it has or will TryPrepare)

  p1MetaDataMap::accessor c;
  p1MetaData.insert(c, txnDigest); 
  if(c->second.hasP1){  //if already has a p1 result => ForceMat
    Debug("Txn[%s] hasP1 result. Attempting force materialization.", BytesToHex(txnDigest, 16).c_str());
    ForceMaterialization(c->second.result, txnDigest, txn); //Note: This only force materializes if the result is abstain (i.e. if it's not materialized yet)
    return false;
  }
  else{  //if not ==> register.
    Debug("Txn[%s] does not have P1 result. Registering force materialization.", BytesToHex(txnDigest, 16).c_str());
    c->second.forceMaterialize = true;
    return true;
  }
  c.release();
 
}


void Server::ForceMaterialization(const proto::ConcurrencyControl::Result &result, const std::string &txnDigest, const proto::Transaction *txn){
    // this is called when "normal" preparing votes to Abstain

    //Only Materialize if not already. 
    if(result == proto::ConcurrencyControl::COMMIT || result == proto::ConcurrencyControl::WAIT){
        Debug("Txn[%s] is already applied via Prepare (Commit/Wait: %d). No need to forcefully materialize", BytesToHex(txnDigest, 16).c_str(), result);
        //Note: In this case, Tx did Prepare, and thus add to materialize
        return; 
    }

    if(result == proto::ConcurrencyControl::ABORT || result == proto::ConcurrencyControl::IGNORE){
        Debug("Txn[%s] is Aborted/Ignored (%d). No point in force materializing", BytesToHex(txnDigest, 16).c_str(), result);
         //Note: In this case, Tx did call Abort, and thus add to materialize   (IGNORE is an exception -- this is an illegal cornercase)
        return; 
    }

    //Materialize only if result is ABSTAIN:
    Debug("Result is Abstain. Forcefully Materialize Txn[%s]!", BytesToHex(txnDigest, 16).c_str());
  
    ApplyTableWrites(*txn, Timestamp(txn->timestamp()), txnDigest, nullptr, false, true); //forceMaterialize = true
    
}


//Note: for Multi-shard TXs only want to write the rows that are relevant to the shard managed by this replica. The ownership logic happens inside the sql_interpreter GenerateWriteSatement logic.
void Server::ApplyTableWrites(const proto::Transaction &txn, const Timestamp &ts,
                const std::string &txn_digest, const proto::CommittedProof *commit_proof, bool commit_or_prepare, bool forceMaterialize){

    Debug("Apply all TableWrites for Txn[%s]. TS[%lu:%lu]. ForceMat? %d", BytesToHex(txn_digest, 16).c_str(), ts.getTimestamp(), ts.getID(), forceMaterialize);

    //TODO: Parallelize application of each table's writes. Note: This requires multi-threading + callback to ensure synchronous join
        //Not urgent: Realistically most transactions do not touch that many tables...
    for (const auto &[table_name, table_write] : txn.table_writes()){
        table_store->ApplyTableWrite(table_name, table_write, ts, txn_digest, commit_proof, commit_or_prepare, forceMaterialize); 
    }

    materializedMap::accessor mat;
    materialized.insert(mat, txn_digest);
    mat.release();

    Debug("Materialized Txn[%s]. TS[%lu:%lu]. CheckWaitingQueries", BytesToHex(txn_digest, 16).c_str(), ts.getTimestamp(), ts.getID());

    //Wake any queries waiting for materialization of Txn:
    CheckWaitingQueries(txn_digest, ts.getTimestamp(), ts.getID());  
}

//DEPRECATED:
// void Server::ApplyTableWrites(const std::string &table_name, const TableWrite &table_write, const Timestamp &ts,
//                 const std::string &txn_digest, const proto::CommittedProof *commit_proof, bool commit_or_prepare, bool forceMaterialize){

//     materializedMap::accessor mat;
//     //materializedTSMap::accessor apt;

//     materialized.insert(mat, txn_digest);
//     //materialized.insert(apt, MergeTimestampId(ts.getTimestamp(), ts.getID()));  //TODO FIXME: Reduce redundancy of computing optimistic TXid..  (It is re-computed inside CheckWaitingQueries)
    
//     //Note: Must allow duplicate application in order to upgrade from mat to prepared, or from mat/prep to commit
//             //Why was it there to begin with? To avoid 
//     // if(!materialized.insert(ap, txn_digest)) return; //Already applied  
//     // if(!materialized.insert(apt, MergeTimestampId(ts.getTimestamp(), ts.getID()))) return; //Already applied  
   

//     table_store->ApplyTableWrite(table_name, table_write, ts, txn_digest, commit_proof, commit_or_prepare, forceMaterialize); //FIXME: Loop here!!!! not outside.

//     //Wake any queries waiting for full materialization:
//     CheckWaitingQueries(txn_digest, ts.getTimestamp(), ts.getID());   //TODO: Inside or outside mat lock?

//     //apt.release();
//     mat.release();
// }


bool Server::WaitForMaterialization(const std::string &tx_id, const std::string &query_retry_id, std::unordered_map<std::string, uint64_t> &missing_txns){
      //Note: Sync was requested on all TXs that aren't locally ongoing (i.e. haven't been received yet). But just because they are ongoing, doesn't mean they already were materialized (applied/aborted)

    //TODO: if locally abstained, force materialize now.

    materializedMap::accessor mat;
    bool not_yet_materialized = materialized.insert(mat, tx_id);
    if(not_yet_materialized){
    //if(!materialized.find(ap, tx_id)){  //FIXME: If it doesnt exist, this does not take a lock. Is this threadsafe w.r.t to concurrent Write?
                                            //Tsafe yes, since no read is happening; but not atomic, since we want to do more. ==> FIXME: replace with insert + erase combination.

        Debug("Txn[%s] is not yet materialized. Add query_retry_version[%s] to Queries waiting for Txn", BytesToHex(tx_id, 16).c_str(), BytesToHex(query_retry_id, 16).c_str());
        waitingQueryMap::accessor w;
        waitingQueries.insert(w, tx_id);
        bool already_requested = !w->second.empty(); //TODO: if there is already a waiting query, don't need to request the txn again. Problem: Could have been requested by a byz client that gave wrong replica_ids...
        //w->second.insert(*queryId);
        w->second.insert(query_retry_id); //i.e. add to missing txns.
        w.release();

        missing_txns[tx_id];

        materialized.erase(mat); //just undo the insert (semantically it was meant to be a read)

        return false;
    }

    mat.release();

    Debug("Txn[%s] is already materialized.", BytesToHex(tx_id, 16).c_str());
    return true; //return true if Ready (i.e. not waiting)
}

//DEPRECATED
bool Server::WaitForMaterialization(const uint64_t &ts_id, const std::string &query_retry_id, std::unordered_map<uint64_t, uint64_t> &missing_ts){

    materializedTSMap::accessor apt;
    bool not_yet_materialized = materializedTS.insert(apt, ts_id);  //FIXME: PROBLEM, Cannot look up by Tx_id...
    if(not_yet_materialized){
    //if(!materialized.find(ap, tx_id)){  //FIXME: If it doesnt exist, this does not take a lock. Is this threadsafe w.r.t to concurrent Write?
                                            //Tsafe yes, since no read is happening; but not atomic, since we want to do more. ==> FIXME: replace with insert + erase combination.
        waitingQueryTSMap::accessor w;
        waitingQueriesTS.insert(w, ts_id);
        bool already_requested = !w->second.empty(); //TODO: if there is already a waiting query, don't need to request the txn again. Problem: Could have been requested by a byz client that gave wrong replica_ids...
        //w->second.insert(*queryId);
        w->second.insert(query_retry_id);
        w.release();

        missing_ts[ts_id];

        materializedTS.erase(apt); //just undo the insert (semantically it was meant to be a read)

        return false;
    }

    apt.release();

    return true; //return true if Ready (i.e. not waiting)
}



void Server::WaitForTX(const uint64_t &ts_id, const std::string &query_retry_id, std::unordered_map<uint64_t, uint64_t> &missing_ts){
    //wait for Tx to arrive. Upon Tx arriving (TryPrepare, Commit, Abort) wake up and CheckPresence

    Debug("TS[%lu] not local. Add query_retry_version[%s] to Queries waiting for TS", ts_id, BytesToHex(query_retry_id, 16).c_str());
    waitingQueryTSMap::accessor w;
    waitingQueriesTS.insert(w, ts_id);
    bool already_requested = !w->second.empty(); //TODO: if there is already a waiting query, don't need to request the txn again. Problem: Could have been requested by a byz client that gave wrong replica_ids...
    //w->second.insert(*queryId);
    w->second.insert(query_retry_id);
    w.release();

    missing_ts[ts_id];

}


//Note: Call this only from Network or from MainThread (can do so before calling DoOCC)
void Server::CheckWaitingQueries(const std::string &txnDigest, const uint64_t &ts, const uint64_t ts_id, bool is_abort, bool non_blocking, int tx_ts_mode){ 
    //Non_blocking makes it so that the request is schedules asynchronously, i.e. does not block calling function
    //tx_ts_mode: 0 = wake both TX and TS (called from ), 1 = wake only TS, 2 = wake only TX. 

  Debug("Check whether any queries are waiting on txn %s", BytesToHex(txnDigest, 16).c_str());
  if(params.query_params.optimisticTxID && tx_ts_mode < 2){ //Wake both Queries that use normal tx-ids (e.g. retries) and queries that use optimistic Id's
    uint64_t txnTS(MergeTimestampId(ts, ts_id));
    //Wake waiting queries.
    if(params.mainThreadDispatching && (params.query_params.parallel_queries || non_blocking)){   //if mainThreadDispatching = true then dispatchMessageReceive = false
        //Dispatch job to worker thread (since it may wake and excute sync)
        auto f = [this, txnTS, txnDigest, is_abort, tx_ts_mode]() mutable {
            Debug("Dispatch UpdateWaitingQueries(%s) to a worker thread.", BytesToHex(txnDigest, 16).c_str());
            if(tx_ts_mode != 1) UpdateWaitingQueries(txnDigest, is_abort);
            if(tx_ts_mode != 2) UpdateWaitingQueriesTS(txnTS, txnDigest, is_abort);
            return (void*) true;
        };
        if(params.query_params.parallel_queries) transport->DispatchTP_noCB(std::move(f));
        else if(non_blocking) transport->DispatchTP_main(std::move(f));
    }
    else{
        Debug("Calling UpdateWaitingQueries(%s) on same thread.", BytesToHex(txnDigest, 16).c_str());
        if(tx_ts_mode != 1) UpdateWaitingQueries(txnDigest, is_abort);
        if(tx_ts_mode != 2) UpdateWaitingQueriesTS(txnTS, txnDigest, is_abort);
    }
  }
  else{ //Running only with real TX-ids
    //Wake waiting queries.
    if(params.mainThreadDispatching && (params.query_params.parallel_queries || non_blocking)){   //if mainThreadDispatching = true then dispatchMessageReceive = false
        //Dispatch job to worker thread (since it may wake and excute sync)
        auto f = [this, txnDigest, is_abort]() mutable {
            Debug("Dispatch UpdateWaitingQueries(%s) to a worker thread.", BytesToHex(txnDigest, 16).c_str());
            UpdateWaitingQueries(txnDigest, is_abort);
            return (void*) true;
        };
        if(params.query_params.parallel_queries) transport->DispatchTP_noCB(std::move(f));
        else if(non_blocking) transport->DispatchTP_main(std::move(f));
    }
    else{
        UpdateWaitingQueries(txnDigest, is_abort);
    }
  }
  

    //TODO: Alternatively: Could call this in DoOCCCheck -- but then need to account for the fact that it might be called from a worker thread (only on normal case, not fallback)

  // if(!params.mainThreadDispatching || (params.parallel_CCC && params.query_params.parallel_queries)){  //IF already on worker, stay
  //     UpdateWaitingQueries(txnDigest);
  // }
  // else if(!params.parallel_CCC && params.query_params.parallel_queries){
  //   //Dispatch job to worker thread (since it may wake and excute sync)
  //     auto f = [this, txnDigest]() mutable {
  //       Debug("Dispatch UpdateWaitingQueries(%s) to a worker thread.", BytesToHex(txnDigest, 16).c_str());
  //       UpdateWaitingQueries(txnDigest);
  //       return (void*) true;
  //     };
  //     transport->DispatchTP_noCB(std::move(f));
  // }
  // else if(params.parallel_CCC && !params.query_params.parallel_queries){
  //     auto f = [this, txnDigest]() mutable {
  //       Debug("Dispatch UpdateWaitingQueries(%s) to a worker thread.", BytesToHex(txnDigest, 16).c_str());
  //       UpdateWaitingQueries(txnDigest);
  //       return (void*) true;
  //     };
  //     transport->DispatchTP_main(std::move(f));
  // }
}
            //TODO: Add ABORT/Delete option.

// void Server::UpdateWaitingQueries(const std::string &txnDigest, bool is_abort){
//     //when receiving a requested sync msg, use it to update waiting data structures for all potentially ongoing queries.
//     //waiting queries are registered in map from txn-id to query id:

//      Debug("Checking whether can wake all queries waiting on txn_id %s", BytesToHex(txnDigest, 16).c_str());
//      //Notes on Concurrency liveness:
//         //HandleSync will first lock q (queryMetaData) and then try to lock w (waitingQueries) in an effort to register a waitingQuery
//         //UpdateWaitingQueries will first lock w (waitingQueries) and then try to lock q (queryMetaData) to wake waitingQueries
//         //This does not cause a lock order inversion, because UpdateWaitingQueries only attempts to lock q if waitingQueries contains a registered transaction; which is only possible if HandleSync released both q and w
//         //Note that it is guaranteed for a waitingQuery to wake up, because whenever HandleSync registers a waitingQuery, it also sends out a new RequestTx message. Upon receiving a reply, UpdateWaitingQueries will be called.
//             //This is because a waiting query is registered and RequestTX is sent out even if the tx is locally committed after checking for missing, but before registering.
//        //Cornercase: If HandleSync is called twice, and tries to lock w for a tx that already waits on q, then it can deadlock. 
//             //However, this should never happen, as consecutive Sync's Clear the meta data inside missing_txns

//         //Example: Same Query syncs twice. First SS contains tx A, second snapshot also contains A.
//         // Handle Sync locks q, then w to add A->query
//         // UpdateWaiting locks w, then tries to lock query.
//         // Concurrently, second Sync locks q, then tries to re-set w ==> deadlock.
//         //Solutions: a) Separate syncs need to have different q...  => want meta data to still be in the same q. Store <query_verion, missing_txn> in separate map. Hold q while adding to this map (and remove)
//                                                                                                                                                                     //This ensures only one entry per query exists.
//                                                                                                                                                                 // key = hash(query_id, retry_version)
//                                                                                                                                                                 //Then take lock qw which is unique.
//         //           b) If sync fails, remove from waiting -> same deadlock problem. Must remove while not holding q, 
//         //           c) for second sync, remember the previously waiting; don't add again. Requires storing all previous versions
   

//      //1) find queries that were waiting on this txn-id
//     waitingQueryMap::accessor w;
//     bool hasWaiting = waitingQueries.find(w, txnDigest);
//     if(hasWaiting){
//         for(const std::string &waiting_query : w->second){

//             //2) update their missing data structures
//             queryMetaDataMap::accessor q;
//             bool queryActive = queryMetaData.find(q, waiting_query);
//             if(queryActive){
//                 QueryMetaData *query_md = q->second;
//                 bool was_present = query_md->missing_txn.erase(txnDigest);
//                  //Cornercase: What if we clear missing queries (ro Retry Sync) and then UpdateWaiting triggers. ==> was_present should be false => won't call Update

//                  Debug("Query[%lu:%lu] is still waiting on (%d) transactions", query_md->query_seq_num, query_md->client_id, query_md->missing_txn.size());

//                 //3) if missing data structure is empty for any query: Start Callback.
//                 if(was_present && query_md->is_waiting && query_md->missing_txn.empty()){ 
//                     //Note: was_present -> only call this the first time missing_txn goes empty: present captures the fact that map was non-empty before erase.
//                     //Note: is_waiting -> make sure query is waiting. E.g. missing_txn could be empty because we re-tried the query and now are not missing any. In this case is_waiting will be set to false. -> no need to call callback
//                     HandleSyncCallback(query_md, waiting_query); //TODO: Should this be dispatched again? So that multiple waiting queries don't execute sequentially?
//                 }
//             }
//             q.release();
//             //w->second.erase(waiting_query); //FIXME: Delete safely while iterating... ==> Just erase all after
//         }
//         //4) remove key from waiting data structure if no more queries waiting on it to avoid key set growing infinitely...
//         waitingQueries.erase(w);
//     }
//     w.release();
// }


void Server::UpdateWaitingQueries(const std::string &txnDigest, bool is_abort){
    //when receiving a requested sync msg, use it to update waiting data structures for all potentially ongoing queries.
    //waiting queries are registered in map from txn-id to query id:

     Debug("Checking whether can wake all queries waiting on txn_id %s", BytesToHex(txnDigest, 16).c_str());
  //Notes on Concurrency liveness:
    //ProcessSync will first lock q (queryMetaData), then qm (queryMissingTxns) and then try to lock w (waitingQueries) in an effort to register a waitingQuery
    //UpdateWaitingQueries will first lock w (waitingQueries) and then try to lock qm (queryMissingTxns) to wake waitingQueries. It wakes queries by locking q (queryMetaData) only after releasing both.
    //This does not cause a lock order inversion, because UpdateWaitingQueries only attempts to lock qm if waitingQueries contains a registered transaction; 
                                                                                                //which is only possible if ProcessSync released both qm and w
    //Note that it is guaranteed for a waitingQuery to wake up, because whenever ProcessSync registers a waitingQuery, it also sends out a new RequestTx message. 
        //Upon receiving a reply, UpdateWaitingQueries will be called.
        //This is because a waiting query is registered and RequestTX is sent out even if the tx is locally committed after checking for missing, but before registering.
    //Cornercase: If HandleSync is called twice, and tries to lock w for a tx that already waits on qm, then it can deadlock. 
        //However, this should never happen, as Process Sync should only be called exactly once per retry_version.

     //Note also, that consecutive Sync's Clear the meta data of previous retry_versions, thus deleting queryMissingTxns ==> this avoids waking queries on old retry versions. 
     //Note: Not strictly necessary -> Could remove ClearMetaData ==> the version to wake will not be valid, and thus no wake happens.

     
    std::map<std::string, uint64_t> queries_to_wake;
    std::map<std::string, uint64_t> queries_to_rm_txn; //All queries (besides the ones we wake anyways) from whose snapshot we want to remove the txn. Note: This is just an optimization to not loop twice

     //1) find queries that were waiting on this txn-id
    waitingQueryMap::accessor w;
    bool hasWaiting = waitingQueries.find(w, txnDigest);
    if(hasWaiting){
         Debug("%d Queries are waiting on txn_id %s", w->second.size(), BytesToHex(txnDigest, 16).c_str());
        for(const std::string &waiting_query : w->second){

             //2) update their missing data structures
            // Lookup queryMissingTxns ... => mark map of "waking query + retry_version" ==> after releasing w try to lock them all.
            queryMissingTxnsMap::accessor qm;
            if(!queryMissingTxns.find(qm, waiting_query)) continue; //releases qm implicitly (or rather: qm is not actually held)

            MissingTxns &missingTxns = qm->second;

            bool was_present = missingTxns.missing_txns.erase(txnDigest); 
            //Cornercase: What if we clear missing queries (ro Retry Sync) and then UpdateWaiting triggers. ==> was_present should be false => won't call Update
           
            Debug("QueryId[%s] is still waiting on (%d) transactions and (%d) TS", BytesToHex(missingTxns.query_id, 16).c_str(), missingTxns.missing_txns.size(), missingTxns.missing_ts.size());
            if(was_present && missingTxns.missing_txns.empty() && missingTxns.missing_ts.empty()){  //don't wake up unless there is NO missing TX OR TS
                   //Note: was_present -> only call this the first time missing_txn goes empty: present captures the fact that map was non-empty before erase.
                Debug("Wake QueryId[%s]!",  BytesToHex(missingTxns.query_id, 16).c_str());
                queries_to_wake[missingTxns.query_id] = missingTxns.retry_version;
            }
            else if(is_abort) queries_to_rm_txn[missingTxns.query_id] = missingTxns.retry_version;
            qm.release();
        }
        //3) remove key from waiting data structure if no more queries waiting on it to avoid key set growing infinitely...
        waitingQueries.erase(w);
    }
    else{
        Debug("No Queries are waiting on txn_id %s ", BytesToHex(txnDigest, 16).c_str());
        return;
    }
    w.release();

   
    //4) Try to wake all ready queries.
    for(auto &[queryId, retry_version]: queries_to_wake){
        queryMetaDataMap::accessor q;
        bool queryActive = queryMetaData.find(q, queryId);
        if(queryActive){
                QueryMetaData *query_md = q->second;
               
                //5) if query is waiting and retry_version is still current: Start Callback.
                Debug("Stored Query: %s has retry version %lu, and is %s waiting", BytesToHex(queryId, 16).c_str(), query_md->retry_version, query_md->is_waiting ? "" : "not");
                if(query_md->is_waiting && query_md->retry_version == retry_version){ 

                    //6) Erase txn from snapshot if abort.
                    if(is_abort) query_md->merged_ss_msg->mutable_merged_txns()->erase(txnDigest); 

                     //Note: is_waiting -> make sure query is waiting. E.g. missing_txn could be empty because we re-tried the query and now are not missing any. In this case is_waiting will be set to false. -> no need to call callback
                     // (Note: This is handled by retry_version check now. Can remove is_waiting.)
                     Debug("Waking Query[%lu:%lu:%lu]", query_md->client_id, query_md->query_seq_num, query_md->retry_version);
                    //Note: is_waiting -> make sure query is waiting. E.g. missing_txn could be empty because we re-tried the query and now are not missing any. In this case is_waiting will be set to false. -> no need to call callback
                    HandleSyncCallback(q, query_md, queryId); //TODO: Should this be dispatched again? So that multiple waiting queries don't execute sequentially?
                }
        }
        q.release();
    }

    //6 For all other queries that are still waiting, but not ready to wake: Erase txn from snapshot if abort
    for(auto &[queryId, retry_version]: queries_to_rm_txn){
        queryMetaDataMap::accessor q;
        bool queryActive = queryMetaData.find(q, queryId);
        if(queryActive){
            QueryMetaData *query_md = q->second;

            if(query_md->is_waiting && query_md->retry_version == retry_version){ 
                //6) Erase txn from snapshot if abort.
                Debug("Remove TX[%s] from merged_ss of Query[%lu:%lu:%lu]", BytesToHex(txnDigest, 16).c_str(), query_md->client_id, query_md->query_seq_num, query_md->retry_version);
                query_md->merged_ss_msg->mutable_merged_txns()->erase(txnDigest); 
            }
        }
        q.release();
    }
}

//DEPRECATED
// //Note: For waitingTS: call updateWaitingQueries with Ts:  --> Then: Set snapshot for all Ts --> When snapshot ready: All txn_ids are available. 
// //(Alternatively: Could transform later to save lookups to query_md inside UpdateWaitingQueries. But it's less clean if later layers need to be aware of TS sync still.)
//     //In this case: During sync callback: For every Ts in merged_ts ==> lookup tx and add to mergedTS.
// void Server::UpdateWaitingQueriesTS(const uint64_t &txnTS, const std::string &txnDigest, bool is_abort){

//      Debug("Checking whether can wake all queries waiting on txn_id %s with ts_id %lu", BytesToHex(txnDigest, 16).c_str(), txnTS);

//     std::map<std::string, uint64_t> queries_to_wake;
//     std::map<std::string, uint64_t> queries_to_update_txn; //All queries (besides the ones we wake anyways) from whose snapshot we want to add/remove the txn. Note: This is just an optimization to not loop twice

//      //1) find queries that were waiting on this txn-id
//     waitingQueryTSMap::accessor w;
//     bool hasWaiting = waitingQueriesTS.find(w, txnTS);
//     if(hasWaiting){
//         Debug("Some Queries are waiting on txn_id %s with ts_id %lu", BytesToHex(txnDigest, 16).c_str(), txnTS);
//         for(const std::string &waiting_query : w->second){
//             Debug("Query_retry_version %s waiting on txn_id %s with ts_id %lu", BytesToHex(waiting_query, 16).c_str(), BytesToHex(txnDigest, 16).c_str(), txnTS);
//              //2) update their missing data structures
//             // Lookup queryMissingTxns ... => mark map of "waking query + retry_version" ==> after releasing w try to lock them all.
//             queryMissingTxnsMap::accessor qm;
//             if(!queryMissingTxns.find(qm, waiting_query)) continue; //releases qm implicitly (or rather: qm is not actually held)

//             MissingTxns &missingTxns = qm->second;

//             bool was_present = missingTxns.missing_ts.erase(txnTS); 
//             //Cornercase: What if we clear missing queries (ro Retry Sync) and then UpdateWaiting triggers. ==> was_present should be false => won't call Update
           
//             Debug("QueryId[%s] is still waiting on (%d) transactions (TS)", BytesToHex(missingTxns.query_id, 16).c_str(), missingTxns.missing_ts.size());
//             if(was_present && missingTxns.missing_ts.empty()){ 
//                    //Note: was_present -> only call this the first time missing_txn goes empty: present captures the fact that map was non-empty before erase.
//                 queries_to_wake[missingTxns.query_id] = missingTxns.retry_version;
//             }
//             else{
//                 queries_to_update_txn[missingTxns.query_id] = missingTxns.retry_version;
//             }
//             qm.release();
//         }
//         //3) remove key from waiting data structure if no more queries waiting on it to avoid key set growing infinitely...
//         waitingQueriesTS.erase(w);
//     }
//     w.release();

//     //4) Try to wake all ready queries.
//     for(auto &[queryId, retry_version]: queries_to_wake){
//         Debug("Trying to wake Query: %s with retry version %lu", BytesToHex(queryId, 16).c_str(), retry_version);
//         queryMetaDataMap::accessor q;
//         bool queryActive = queryMetaData.find(q, queryId);
//         if(queryActive){
//                 QueryMetaData *query_md = q->second;

//                 Debug("Stored Query: %s has retry version %lu, and is %s waiting", BytesToHex(queryId, 16).c_str(), query_md->retry_version, query_md->is_waiting ? "" : "not");
//                 //5) if query is waiting and retry_version is still current: Start Callback.
//                 if(query_md->is_waiting && query_md->retry_version == retry_version){ 
//                      Debug("Waking Query[%lu:%lu:%lu]", query_md->query_seq_num, query_md->client_id, query_md->retry_version);

//                     //6) Add txn to snapshot, or erase txn from snapshot if abort
//                     if(is_abort) query_md->merged_ss_msg->mutable_merged_txns()->erase(txnDigest);  //Note: Might have been added to snapshot because TS/Tx was ongoing/prepared. But now remove since it is aborted.
//                                                                                                 //However: We would never have added the txn to waiting.
//                     else{
//                     (*query_md->merged_ss_msg->mutable_merged_txns())[txnDigest]; //(Just add with default constructor --> empty ReplicaList)
//                     }

//                      query_md->merged_ss_msg->clear_merged_ts(); //These are no longer needed.
//                     //Note: is_waiting -> make sure query is waiting. E.g. missing_txn could be empty because we re-tried the query and now are not missing any. In this case is_waiting will be set to false. -> no need to call callback
//                     HandleSyncCallback(q, query_md, queryId); //TODO: Should this be dispatched again? So that multiple waiting queries don't execute sequentially?
//                 }
//         }
//         q.release();
//     }

//     //6 For all other queries that are still waiting, but not ready to wake: Erase txn from snapshot if abort
//     for(auto &[queryId, retry_version]: queries_to_update_txn){
//         queryMetaDataMap::accessor q;
//         bool queryActive = queryMetaData.find(q, queryId);
//         if(queryActive){
//             QueryMetaData *query_md = q->second;

//             if(query_md->is_waiting && query_md->retry_version == retry_version){ 
//                 //6) Erase txn from snapshot if abort.
//                 if(is_abort) query_md->merged_ss_msg->mutable_merged_txns()->erase(txnDigest);  //Note: Might have been added to snapshot because TS/Tx was ongoing/prepared. But now remove since it is aborted.
//                                                                                             //However: We would never have added the txn to waiting.
//                 else{
//                  (*query_md->merged_ss_msg->mutable_merged_txns())[txnDigest]; //(Just add with default constructor --> empty ReplicaList)
//                 }
//             }
//         }
//         q.release();
//     }
//     Debug("Completed all possible wake-ups for queries waiting on txn_id %s with ts_id %lu", BytesToHex(txnDigest, 16).c_str(), txnTS);
// }


void Server::UpdateWaitingQueriesTS(const uint64_t &txnTS, const std::string &txnDigest, bool is_abort){
    //Process Sync locks: q (queryId) -> qm (query_id_version) -> w (txnTS)
    //UpdateWQTS locks: w -> qm; q -> qm
    //No lock order inversion between w and qm (see above in detail): lock of qm in UpdateWQTS only happens if lock to w was released in ProcessSync.

     Debug("Checking whether can wake all queries waiting on txn_id %s with ts_id %lu", BytesToHex(txnDigest, 16).c_str(), txnTS);

    std::map<std::pair<std::string, std::string>, uint64_t> queries_to_update_txn; //All queries (besides the ones we wake anyways) from whose snapshot we want to add/remove the txn. Note: This is just an optimization to not loop twice

     //1) find queries that were waiting on this txn-id
    waitingQueryTSMap::accessor w;
    bool hasWaiting = waitingQueriesTS.find(w, txnTS);
    if(hasWaiting){
        Debug("%d Queries are waiting on txn_id %s with ts_id %lu", w->second.size(), BytesToHex(txnDigest, 16).c_str(), txnTS);
        for(const std::string &waiting_query : w->second){ //= query_retry_version_id
            Debug("Query_retry_version %s waiting on txn_id %s with ts_id %lu", BytesToHex(waiting_query, 16).c_str(), BytesToHex(txnDigest, 16).c_str(), txnTS);
             //2) update their missing data structures
            // Lookup queryMissingTxns ... => mark map of "waking query + retry_version" ==> after releasing w try to lock them all.
            queryMissingTxnsMap::accessor qm;
            if(!queryMissingTxns.find(qm, waiting_query)) continue; //releases qm implicitly (or rather: qm is not actually held)

            MissingTxns &missingTxns = qm->second;

            bool was_present = missingTxns.missing_ts.erase(txnTS); 

           
            //Cornercase: What if we clear missing queries (ro Retry Sync) and then UpdateWaiting triggers. ==> was_present should be false => won't call Update
           
            Debug("QueryId[%s] is still waiting on (%d) transactions and (%d) TS", BytesToHex(missingTxns.query_id, 16).c_str(), missingTxns.missing_txns.size(), missingTxns.missing_ts.size());
            if(was_present){ 
                   //Note: was_present -> only call this the first time missing_txn goes empty: present captures the fact that map was non-empty before erase.
                queries_to_update_txn[std::make_pair(missingTxns.query_id, waiting_query)] = missingTxns.retry_version;
            }
            qm.release();
        }
        //3) remove key from waiting data structure if no more queries waiting on it to avoid key set growing infinitely...
        waitingQueriesTS.erase(w);
    }
    else{
        Debug("No Queries are waiting on txn_id %s with ts_id %lu", BytesToHex(txnDigest, 16).c_str(), txnTS);
        return;
    }
    w.release();


    //6 For all waiting queries, check whether Tx is ready for materialization: Erase txn from snapshot if abort
    for(const auto &[id_pair, retry_version]: queries_to_update_txn){   //TODO: if this doesn't work with tuple, make pair of pair
        const std::string &queryId = id_pair.first;
        const std::string &query_id_version = id_pair.second; //TODO: remove useless queryId
      
         
        queryMetaDataMap::accessor q;
        bool queryActive = queryMetaData.find(q, queryId);
        if(queryActive){
            QueryMetaData *query_md = q->second;

            if(query_md->is_waiting && query_md->retry_version == retry_version){ 
                Debug("Trying to wake Query: %s with retry version %lu; waiting on TS[%lu]. ", BytesToHex(queryId, 16).c_str(), retry_version, txnTS);
                //Note: is_waiting -> make sure query is waiting. E.g. missing_txn could be empty because we re-tried the query and now are not missing any. 
                //In this case is_waiting will be set to false. -> no need to call callback
                
                //6) Erase txn from snapshot if abort.
                if(is_abort) query_md->merged_ss_msg->mutable_merged_txns()->erase(txnDigest);  //Note: Might have been added to snapshot because TS/Tx was ongoing/prepared. But now remove since it is aborted.
                                                                                            //However: We would never have added the txn to waiting.
                else (*query_md->merged_ss_msg->mutable_merged_txns())[txnDigest]; //(Just add with default constructor --> empty ReplicaList)

                //7) Check whether TX is materialized or not; orchestrate waiting.
                queryMissingTxnsMap::accessor qm;
                if(!queryMissingTxns.find(qm, query_id_version)) continue; //releases qm implicitly (or rather: qm is not actually held)

                MissingTxns &missingTxns = qm->second;

                std::map<uint64_t, proto::RequestMissingTxns> replica_requests; //dummy arg
                const pequinstore::proto::ReplicaList replica_list;             //dummy arg

                bool isFinished = CheckPresence(txnDigest, query_id_version, query_md, replica_requests, replica_list, missingTxns.missing_txns);
                UW_ASSERT(replica_requests.empty()); //no sync requests should be made! otherwise we shouldn't have woken up...
              
                 bool ready = missingTxns.missing_ts.empty() && missingTxns.missing_txns.empty(); 
                // I.e. all missing TS have been resolved, and all of their translations into TX have been materialized
                Debug("Is Query[%s] with retry version %lu ready? isTXFinished? %d, isQueryReady? %d ", BytesToHex(queryId, 16).c_str(), retry_version, isFinished, ready);

                qm.release();

                //If CheckPresence = yes && missing_txns.empty => HandleSyncCallback()
                if(ready && isFinished) HandleSyncCallback(q, query_md, queryId);
                
            }
        }
        q.release();
    }
    Debug("Completed all possible wake-ups for queries waiting on txn_id %s with ts_id %lu", BytesToHex(txnDigest, 16).c_str(), txnTS);
}
 


////////////////////////// VERIFICATION //////////////////////////////////////

bool Server::VerifyClientQuery(proto::QueryRequest &msg, const proto::Query *query, std::string &queryId)
{
    Debug("Verifying Client Query: %s", BytesToHex(queryId, 16).c_str());

    //1. check Query.TS.id = client_id (only signing client should claim this id in timestamp
    if(query->timestamp().id() != msg.signed_query().process_id()){
        Debug("Client id[%d] does not match Timestamp with id[%d] for txn %s", msg.signed_query().process_id(), query->timestamp().id(), BytesToHex(queryId, 16).c_str());
        return false;
    }

    //2. check signature matches txn signed by client (use GetClientID)
    if (!client_verifier->Verify(keyManager->GetPublicKey(keyManager->GetClientKeyId(msg.signed_query().process_id())), msg.signed_query().data(), msg.signed_query().signature())) {
        Debug("Client signatures invalid for query %s", BytesToHex(queryId, 16).c_str());
        return false;
    }

    Debug("Client verification successful for query %s", BytesToHex(queryId, 16).c_str());
    return true; 
}

bool Server::VerifyClientSyncProposal(proto::SyncClientProposal &msg, const std::string &queryId)
{
    Debug("Verifying Client Sync Proposal: %s", BytesToHex(queryId, 16).c_str());

    if (!client_verifier->Verify(keyManager->GetPublicKey(keyManager->GetClientKeyId(msg.signed_merged_ss().process_id())), msg.signed_merged_ss().data(), msg.signed_merged_ss().signature())) {
        Debug("Client signatures invalid for sync proposal %s", BytesToHex(queryId, 16).c_str());
    return false;
    }

    Debug("Client verification successful for query sync proposal %s", BytesToHex(queryId, 16).c_str());
    return true;
}









} // namespace pequinstore

