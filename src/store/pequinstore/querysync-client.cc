// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/pequinstore/groupclient.cc:
 *   Single group indicus transactional client.
 *
 * Copyright 2021 Florian Suri-Payer <fsp@cs.cornell.edu>
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

#include "store/pequinstore/shardclient.h"

#include <google/protobuf/util/message_differencer.h>

#include "store/pequinstore/common.h"

namespace pequinstore {

//TODO: Add: Handle Query Fail
//-> Every shard (not just query_manager shard) should be able to send this if it observes a committed query was missed; or if the materialized snapshot frontier includes a prepare that aborted (or is guaranteed to, e.g. vote Abort)

void ShardClient::Query(uint64_t client_seq_num, uint64_t query_seq_num, proto::Query &queryMsg, // const std::string &query, const TimestampMessage &ts,
      uint32_t timeout, result_timeout_callback &rtcb, result_callback &rcb,                        //range query args
      point_result_callback &prcb, bool is_point, std::string *table_name, std::string *key) {      //point query args

 Debug("Invoked QueryRequest [%lu] on ShardClient for group %d", query_seq_num, group);
  
  //TODO: (Very low priority) how to execute query in such a way that it includes possibly buffered write values. --> Could imagine sending Put Buffer alongside query, such that servers use it to compute result. 
  // No clue how that would affect read set though (such versions should always pass CC check), and whether it can be used by byz to equivocate read set, causing abort.

  uint64_t reqId = lastReqId++;
  PendingQuery *pendingQuery = new PendingQuery(reqId, &params.query_params);
  query_seq_num_mapping[query_seq_num] = reqId;
  pendingQueries[reqId] = pendingQuery;
  pendingQuery->client_seq_num = client_seq_num;
  pendingQuery->query_seq_num = query_seq_num;

  pendingQuery->queryDigest = std::move(QueryDigest(queryMsg, (params.query_params.signClientQueries && params.query_params.cacheReadSet && params.hashDigest)));
   
//   if(params.query_params.signClientQueries && params.query_params.cacheReadSet){
//         pendingQuery->queryDigest = std::move(QueryDigest(queryMsg, params.hashDigest));
//   }
   

  //pendingQuery->query = query; //Is this necessary to store? In case of re-send? --> Cannot move since other shards will use same reference.
  //pendingQuery->qts = ts;

  //pendingQuery->retry = retry;

  pendingQuery->query_manager = (queryMsg.query_manager() == group);
  pendingQuery->rtcb = rtcb;
  pendingQuery->rcb = rcb;

  pendingQuery->is_point = is_point;
  pendingQuery->prcb = prcb;
  pendingQuery->key = key;
  pendingQuery->table_name = table_name;
  
 
  RequestQuery(pendingQuery, queryMsg);

}

void ShardClient::ClearQuery(uint64_t query_seq_num){
    auto itr_q = query_seq_num_mapping.find(query_seq_num);
    if(itr_q == query_seq_num_mapping.end()){
        Panic("No reqId logged for query seq num");
        return;
    }
    auto itr = pendingQueries.find(itr_q->second);
    if (itr == pendingQueries.end()) {
        return; // this is a stale request
    }
    delete itr->second;
    pendingQueries.erase(itr);
    query_seq_num_mapping.erase(itr_q);
}


//Note: Use new req-id for new query sync version
void ShardClient::RetryQuery(uint64_t query_seq_num, proto::Query &queryMsg, bool is_point, point_result_callback prcb){

     //Support for QueryRetry:
       // Re-do sync and exec on same query id. (Update req id)
            //If receive new sync set for query that already exists, replace it (this is a client issued retry because sync failed.);
            // problem: byz could abuse this to retry only at some replicas --> resulting in different read sets   //Solution: Include retry-id in prepare: Replicas Wait to receive read set for it. 

            //Question: Need new Query ID for retries? -- or same digest (supplied by client and known in advance) 
            //--> for now just use single one (assuming I won't simulate a byz attack); that way garbage collection is easier when re-trying a tx.
            // --> Clients can specify in its Tx which retry number (version) of its query attempts it wants to use. 
            //If replicas have a larger version cached than submitted then this is a proof of misbehavior. If replicas have a smaller version cached, then they must wait.
                  // With FIFO channels its always guaranteed to be sent before the prepare --> thus can wait
            // (Note, that due to multithreading processing order at receiver may not be FIFO --> Solution: Implemented a QuerySet Waiter struct and simply WAIT)

            //problem?: byz does not have to retry if sync fails  (due to optimistic TxId, or aborts, or missed commits)
            //--> replicas may have different read sets --> some may prepare and some may abort. (Thats ok, indistinguishable from correct one failing tx.)
                //importantly however: byz client cannot fail sync on purpose ==> will either be detectable (equiv syncMsg or Query), or it could've happened naturally (for a correct client too)

     Debug("Invoked Retry QueryRequest [%lu] on ShardClient for group %d", query_seq_num, group);

    //find pendingQuery from query_seq_num map.
    auto itr_q = query_seq_num_mapping.find(query_seq_num);
    if(itr_q == query_seq_num_mapping.end()){
        Panic("No reqId logged for query seq num"); //would not call retry if it was not still ongoing.
        return;
    }
    auto itr = pendingQueries.find(itr_q->second);
    if (itr == pendingQueries.end()) {
        Panic("Query Request no longer ongoing."); //would not call retry if it was not still ongoing.
        return; // this is a stale request
    }
    PendingQuery *pendingQuery = itr->second;
    pendingQueries.erase(itr);

    //Assing new req id. --> need new reqId so late replies for old req aren't accidentally used here.
    uint64_t reqId = lastReqId++;
    pendingQuery->reqId = reqId;
    pendingQueries[reqId] = pendingQuery;
    query_seq_num_mapping[query_seq_num] = reqId;

      //TODO: FIXME: Need new Query ID for retries? -- or same digest (supplied by client and known in advance) - (only necessary if supporting optimistic ids where sync can fail.)
    //-> for now just use single one (assuming I won't simulate a byz attack); that way garbage collection is easier when re-trying a tx.
    //--> Could include Retry field in Query, and use it to determine uniuqe id.
    
    //Reset all datastructures -- 
     // Alternatively, create new object and copy relevant contents. //PendingQuery *newPendingQuery = new PendingQuery(reqId);
    
    pendingQuery->is_point = is_point; //For point queries set the correct callback
    pendingQuery->prcb = std::move(prcb);

    pendingQuery->snapshotsVerified.clear();
    // These 3 are handled by InitMergedSnapshot.
    // pendingQuery->numSnapshotReplies = 0;
    // pendingQuery->txn_freq.clear();
    // pendingQuery->merged_ss.Clear();

    pendingQuery->resultsVerified.clear();
    pendingQuery->numResults = 0;
    pendingQuery->numFails = 0;
    //pendingQuery->failsVerified.clear();
    pendingQuery->result_freq.clear();

    pendingQuery->retry_version++;
    queryMsg.clear_query_cmd(); //NOTE: Don't need to re-send query command for retries. (Assuming we're sending it only to the same replicas)

    Debug("group[%d], QueryRequest[%lu]. Retry version: %lu", group, query_seq_num, pendingQuery->retry_version);
     
  
    RequestQuery(pendingQuery, queryMsg);
}

//pass a query object already from client: This way it avoids copying the query string across multiple shards and for retries
void ShardClient::RequestQuery(PendingQuery *pendingQuery, proto::Query &queryMsg){

  //Init new Merged Snapshot
  pendingQuery->snapshot_mgr.InitMergedSnapshot(&pendingQuery->merged_ss, pendingQuery->query_seq_num, client_id, pendingQuery->retry_version, config->f);

  //Set up queryMsg
//   queryMsg.Clear();
//   queryMsg.query_seq_num(pendingQuery->query_seq_num);
//   queryMsg.set_client_id(client_id);
//   *queryMsg.mutable_query() = pendingQuery->query;
//   *queryMsg.mutable_timestamp() = pendingQuery->qts;  //contains client_id as well. 

  //Note: Byz client can also equivocate query contents for same id. It could then send same sync set to all. This would produce different read sets, but it would not be detected.
  // ---> Implies that query contents must be uniquely hashed too? To guarantee every replica gets same query. I.e. Query id = hash(seq_no, client_id, query-string, timestamp)?
  //pendingQuery->query_id = QueryDigest(query, params.hashDigest); 
  
  queryReq.set_req_id(pendingQuery->reqId);
  queryReq.set_optimistic_txid(params.query_params.optimisticTxID && !pendingQuery->retry_version);//On retry use unique/deterministic tx id only.
  //queryReq.set_retry_version(pendingQuery->retry_version);
  
  queryReq.set_is_point(pendingQuery->is_point);

  queryReq.set_eager_exec(true);
  Notice("SET EAGER TO TRUE ALWAYS -- FOR REAL RUN UNCOMMENT CORRECT EAGER EXEC LINE");
  //queryReq.set_eager_exec(!pendingQuery->retry_version && (pendingQuery->is_point? params.query_params.eagerPointExec : params.query_params.eagerExec));
  Debug("Sending TX eagerly? %s", queryReq.eager_exec()? "yes" : "no");
  if(!queryReq.eager_exec()) Panic("Currently only testing eager exec");
  if(queryReq.is_point()) Panic("Not testing point query currently");

  if(pendingQuery->is_point && !queryReq.eager_exec()){ //If point query is eager: treat as normal eager query. If non-eager, manage as pointQuery
    pendingQuery->pendingPointQuery.prcb = std::move(pendingQuery->prcb); //Move callback
    UW_ASSERT(pendingQuery->key != nullptr && pendingQuery->table_name != nullptr); //Both of these should be set for point queries.
    pendingQuery->pendingPointQuery.key = std::move(*pendingQuery->key);  //NOTE: key no longer owned by client.cc after this.
    pendingQuery->pendingPointQuery.table_name = std::move(*pendingQuery->table_name);
    queryReq.mutable_query()->set_primary_enc_key(pendingQuery->pendingPointQuery.key); //Alternatively, can let server compute it.
  }
  //queryReq.set_eager_exec(params.query_params.eagerExec && !pendingQuery->retry_version); //On retry use sync.


  // This is proof that client does not equivocate query contents --> Otherwise could intentionally produce different read sets at replicas, which -- if caching read set -- can be used to abort partially.
  //NOTE: Hash should suffice to achieve non-equiv --> 2 different queries have different hash.
  if(params.query_params.signClientQueries){
     SignMessage(&queryMsg, keyManager->GetPrivateKey(keyManager->GetClientKeyId(client_id)), client_id, queryReq.mutable_signed_query());
  }
  else{
    *queryReq.mutable_query() = queryMsg; // NOTE: cannot use std::move(queryMsg) because queryMsg objet may be passed to multiple shardclients.
  }
 

  if(pendingQuery->is_point && !queryReq.eager_exec()){
    UW_ASSERT(readMessages <= closestReplicas.size());
    for (size_t i = 0; i < readMessages; ++i) {
        Debug("[group %i] Sending PointQuery to replica %lu", group, GetNthClosestReplica(i));
        transport->SendMessageToReplica(this, group, GetNthClosestReplica(i), queryReq);
    }
    return;
  }

  uint64_t total_msg;
  //uint64_t num_designated_replies;
  if(queryReq.eager_exec()){
    total_msg = params.query_params.cacheReadSet? config->n : params.query_params.syncMessages;
    pendingQuery->num_designated_replies = params.query_params.syncMessages; 
  }

  else{
    total_msg = params.query_params.cacheReadSet? config->n : params.query_params.queryMessages;
    pendingQuery->num_designated_replies = params.query_params.queryMessages;
  }
  

  UW_ASSERT(total_msg <= closestReplicas.size());
  for (size_t i = 0; i < total_msg; ++i) {
    queryReq.set_designated_for_reply(i < pendingQuery->num_designated_replies);
    Debug("[group %i] Sending QUERY to replica id %lu", group, group * config->n + GetNthClosestReplica(i));
    transport->SendMessageToReplica(this, group, GetNthClosestReplica(i), queryReq);
  }

  Debug("[group %i] Sent Query Request [seq:ver] [%lu : %lu] \n", group, pendingQuery->query_seq_num, pendingQuery->retry_version);
}



void ShardClient::HandleQuerySyncReply(proto::SyncReply &SyncReply){
    // 0) find PendingQuery object via request id;
    auto itr = this->pendingQueries.find(SyncReply.req_id());
    if (itr == this->pendingQueries.end()) {
        return; // this is a stale request
    }
    PendingQuery *pendingQuery = itr->second;

    // 1) authenticate reply -- record duplicates   --> could use MACs instead of signatures? Don't need to forward sigs... --> but this requires establishing a MAC between every client/replica pair. Sigs is easier.
    // 2) If signed -- parse contents
    proto::LocalSnapshot *local_ss;

     if (params.validateProofs && params.signedMessages) {
        if (SyncReply.has_signed_local_ss()) {

            if (!verifier->Verify(keyManager->GetPublicKey(SyncReply.signed_local_ss().process_id()),
                    SyncReply.signed_local_ss().data(), SyncReply.signed_local_ss().signature())) {
                Debug("[group %i] Failed to validate signature for query sync reply from replica %lu.", group, SyncReply.signed_local_ss().process_id());
                return;
            }
            if(!validated_local_ss.ParseFromString(SyncReply.signed_local_ss().data())) {
                Debug("[group %i] Invalid serialization of Local Snapshot.", group);
                return;
            }
            local_ss = &validated_local_ss;

            if(local_ss->replica_id() != SyncReply.signed_local_ss().process_id()){
                Debug("Replica %lu falsely claims to be replica %lu", SyncReply.signed_local_ss().process_id(), local_ss->replica_id());
                return;
            } 
      
        } else {
            Panic("Query Sync Reply without required signature");
        }
    } else {
        local_ss = SyncReply.mutable_local_ss();
    }
    Debug("[group %i] QuerySyncReply for request %lu from replica %d.", group, SyncReply.req_id(), local_ss->replica_id());

    //3) check for duplicates -- (ideally check before verifying sig)
    if (!pendingQuery->snapshotsVerified.insert(local_ss->replica_id()).second) {
      Debug("Already received query sync reply from replica %lu.", local_ss->replica_id());
      return;
    }
    //4) check whether replica in group.
    if (!IsReplicaInGroup(local_ss->replica_id(), group, config)) {
      Debug("[group %d] QuerySyncReply from replica %lu who is not in group.",
          group, local_ss->replica_id());
      return;
    }

    // 5) Create Merged Snapshot
        //Add all tx in list to filtered Datastructure --> everytime a tx reaches the MergeThreshold directly add it to the ProtoReply
        //If necessary, decode tx list
      
    bool mergeComplete = pendingQuery->snapshot_mgr.ProcessReplicaLocalSnapshot(local_ss); //TODO: Need to make local_ss non-const.

    // 6) Once #QueryQuorum replies received, send SyncMessages
    if(mergeComplete){
        Debug("Merge complete, Syncing for query [%lu : %lu]:", pendingQuery->query_seq_num, pendingQuery->retry_version);
        SyncReplicas(pendingQuery);
    } 

    // //what if some replicas have it as committed, and some as prepared. If >=f+1 committed ==> count as committed, include only those replicas in list.. If mixed, count as prepared
    // //DOES client need to consider at all whether a txn is committed/prepared? --> don't think so; replicas can determine dependency set at exec time (and either inform client, or cache locally)
    // //TODO: probably don't need separate lists! --> FIXME: Change back to single list in protobuf.
    // for(const std::string &txn_dig : local_ss->local_txns_committed()){
    //    std::set<uint64_t> &replica_set = pendingQuery->txn_freq[txn_dig];
    //    replica_set.insert(local_ss->replica_id());
    //    if(replica_set.size() == params.query_params.mergeThreshold){
    //       *(*pendingQuery->merged_ss.mutable_merged_txns())[txn_dig].mutable_replicas() = {replica_set.begin(), replica_set.end()}; //creates a temp copy, and moves it into replica list.
    //    }

    // }
    // // for(std::string &txn_dig : local_ss.local_txns_prepared()){ 
    // //    pendingQueries->txn_freq[txn_dig].insert(local_ss->replica_id());
    // // }
    
    // // 6) Once #QueryQuorum replies received, send SyncMessages
    // pendingQuery->numSnapshotReplies++;
    // if(pendingQuery->numSnapshotReplies == params.query_params.syncQuorum){
    //     SyncReplicas(pendingQuery);
    // }
}

void ShardClient::SyncReplicas(PendingQuery *pendingQuery){
    //1) Compose SyncMessage
    pendingQuery->merged_ss.set_query_seq_num(pendingQuery->query_seq_num);
    pendingQuery->merged_ss.set_client_id(client_id);
    pendingQuery->merged_ss.set_retry_version(pendingQuery->retry_version);
 
    //proto::SyncClientProposal syncMsg;

    syncMsg.set_req_id(pendingQuery->reqId); //Use Same Req-Id per Query Sync Version

    //2) Sign SyncMessage (this authenticates client, and is proof that client does not equivocate proposed snapshot) --> only necessary if not using Cached Reads: authentication ensures correct client can replicate consistently
            //e.g. don't want any client to submit a different/wrong/empty sync on behalf of client --> without cached read set wouldn't matter: 
                                            //replica replies to a sync msg -> so if client sent a correct one, replica execs that one and replies -- regardless of previous duplicates using same query id.

    pendingQuery->merged_ss.set_query_digest(pendingQuery->queryDigest);

    if(params.query_params.signClientQueries && params.query_params.cacheReadSet){ //FIXME: For now, only signing if using Cached Read Set. --> only then need to avoid equivocation
      //pendingQuery->merged_ss.set_query_digest(pendingQuery->queryDigest);
      SignMessage(&pendingQuery->merged_ss, keyManager->GetPrivateKey(keyManager->GetClientKeyId(client_id)), client_id, syncMsg.mutable_signed_merged_ss());
    }
    else{
        *syncMsg.mutable_merged_ss() = std::move(pendingQuery->merged_ss);
    }


    //3) Send SyncMessage to SyncMessages many replicas; designate which replicas for execution
    uint64_t num_designated_replies = params.query_params.syncMessages; 
    if(params.query_params.optimisticTxID && !pendingQuery->retry_version){
        num_designated_replies += config->f;  //If using optimisticTxID for sync send to f additional replicas to guarantee result. (If retry is on, then we always use determinstic ones.)
    }
    num_designated_replies = std::min((uint64_t) config->n, num_designated_replies); //send at most n messages.
    pendingQuery->num_designated_replies = num_designated_replies;

    uint64_t total_msg = params.query_params.cacheReadSet? config->n : num_designated_replies;
    UW_ASSERT(total_msg <= closestReplicas.size());

    for (size_t i = 0; i < total_msg; ++i) {
        syncMsg.set_designated_for_reply(i < num_designated_replies); //only designate num_designated_replies many replicas for exec replies.

        Debug("[group %i] Sending Query Sync Msg to replica %lu", group, group * config->n + GetNthClosestReplica(i));
        transport->SendMessageToReplica(this, group, GetNthClosestReplica(i), syncMsg);
    }

    Debug("[group %i] Sent Query Sync Messages for query [seq:ver] [%lu : %lu], id: %s \n", group, pendingQuery->query_seq_num, pendingQuery->retry_version, BytesToHex(pendingQuery->queryDigest, 16).c_str());
}


void ShardClient::HandleQueryResult(proto::QueryResultReply &queryResult){
    //0) find PendingQuery object via request id
     auto itr = this->pendingQueries.find(queryResult.req_id());
    if (itr == this->pendingQueries.end()){
        //Panic("Stale Query Result");
        return; // this is a stale request
    } 

    PendingQuery *pendingQuery = itr->second;
    
    Debug("[group %i] Received QueryResult Reply for req-id [%lu]", group, queryResult.req_id());
    // if(!pendingQuery->query_manager){
    //     Debug("[group %i] is not Transaction Manager for request %lu", group, queryResult.req_id());
    //     return;
    // }

    //1) authenticate reply & parse contents
    proto::QueryResult *replica_result;

     if (params.validateProofs && params.signedMessages) {
        if (queryResult.has_signed_result()) {

            if (!verifier->Verify(keyManager->GetPublicKey(queryResult.signed_result().process_id()),
                    queryResult.signed_result().data(), queryResult.signed_result().signature())) {
                Debug("[group %i] Failed to validate signature for query result reply from replica %lu.", group, queryResult.signed_result().process_id());
                return;
            }
            if(!validated_result.ParseFromString(queryResult.signed_result().data())) {
                Debug("[group %i] Invalid serialization of Result.", group);
                return;
            }
           replica_result = &validated_result;

            if(replica_result->replica_id() != queryResult.signed_result().process_id()){
                Debug("Replica %lu falsely claims to be replica %lu", queryResult.signed_result().process_id(), replica_result->replica_id());
                return;
            } 
      
        } else {
            Panic("Query Sync Reply without required signature"); //Note: Only panic for debugging purposes
        }
    } else {
        replica_result = queryResult.mutable_result();
        
    }

    Debug("[group %i] Received Valid QueryResult Reply for request [%lu : %lu] from replica %lu.", group, pendingQuery->query_seq_num, pendingQuery->retry_version, replica_result->replica_id());

    //3) check whether replica in group.
    if (!IsReplicaInGroup(replica_result->replica_id(), group, config)) {
      Debug("[group %d] Query Result from replica %lu who is not in group.",
          group, replica_result->replica_id());
      return;
    }

    //4) check for duplicates -- (ideally check before verifying sig)
    if (!pendingQuery->resultsVerified.insert(replica_result->replica_id()).second) {
      Debug("Already received query result from replica %lu.", replica_result->replica_id());
      return;
    }

     //Debug("[group %i] QueryResult Reply for req %lu is valid. Processing result %s:", group, queryResult.req_id(), replica_result->query_result());
    pendingQuery->numResults++;
    
    
    int matching_res;
    //std::map<std::string, TimestampMessage> read_set;

    //3) wait for up to result_threshold many matching replies (result + result_hash/read set)
    if(params.query_params.cacheReadSet){
        Debug("Read-set hash: %s", BytesToHex(replica_result->query_result_hash(), 16).c_str());
        Debug("Result: %lu", std::hash<std::string>{}(replica_result->query_result()));
         matching_res = ++pendingQuery->result_freq[replica_result->query_result_hash()][replica_result->query_result()].freq; //map should be default initialized to 0.

          if(pendingQuery->result_freq[replica_result->query_result_hash()].size() > 1) Panic("Two different results with the same read hash...");

    }
    else{ //manually compare that read sets match. Easy way to compare: Hash ReadSet.
        Debug("[group %i] Validating ReadSet for QueryResult Reply %lu", group, queryResult.req_id());
        //  read_set = {replica_result->query_read_set().begin(), replica_result->query_read_set().end()}; //Copying to map automatically orders it.
        //  std::string validated_result_hash = std::move(generateReadSetSingleHash(read_set));
        //std::string validated_result_hash = std::move(generateReadSetMerkleRoot(read_set, params.merkleBranchFactor));

        //   Debug("TESTING: Read-set pre sort");
        //     for(auto &read: replica_result->query_read_set().read_set()){
        //         Debug("Read key %s with version [%lu:%lu]", read.key().c_str(), read.readtime().timestamp(), read.readtime().id());
        //     }
        try {
            std::sort(replica_result->mutable_query_read_set()->mutable_read_set()->begin(), replica_result->mutable_query_read_set()->mutable_read_set()->end(), sortReadSetByKey); 
            //Note: Only necessary because we use repeated field; Not necessary if we used ordered map
        }
        catch(...){
            Panic("Read set contains two reads of the same key with different timestamp. Sent by replica %d", replica_result->replica_id());
        }
       std::string validated_result_hash = std::move(generateReadSetSingleHash(replica_result->query_read_set()));
        //TODO: Instead of hashing, could also use "compareReadSets" function from common.h to compare two maps/lists
        
            // //TESTING:
            Debug("TESTING Read set:");
            for(auto &read: replica_result->query_read_set().read_set()){
                Debug("Read key %s with version [%lu:%lu]", read.key().c_str(), read.readtime().timestamp(), read.readtime().id());
            }
           
        //matching_res = ++pendingQuery->result_freq[replica_result->query_result()][validated_result_hash].freq; //map should be default initialized to 0.
        Debug("Validated_read_set_hash: %s", BytesToHex(validated_result_hash, 16).c_str());
        Debug("Result: %lu", std::hash<std::string>{}(replica_result->query_result()));


        Result_mgr &result_mgr = pendingQuery->result_freq[validated_result_hash][replica_result->query_result()]; //[validated_result_hash];  //Could flatten this into 2D structure if make result part of result_hash... But we need access to result
        matching_res = ++result_mgr.freq; //map should be default initialized to 0.

        if(pendingQuery->result_freq[validated_result_hash].size() > 1) Panic("Two different results with the same read hash...");

        //if(pendingQuery->result_freq[replica_result->query_result()].size() > 1) Panic("When testing without optimistic id's all hashes should be the same."); //Switched the order

        //  std::cerr << "current freq: " << result_mgr.freq << std::endl;
        // std::cerr<< "current result_mgr " << result_mgr.rand_id << std::endl;

        // if(pendingQuery->resultsVerified.size() == 1){
        //     pendingQuery->first_result = replica_result->query_result();
        // }
        // else{
        //     std::cerr << "first result: " << pendingQuery->first_result << std::endl;
        //     std::cerr << "current result:" << replica_result->query_result() << std::endl;
        //     UW_ASSERT(pendingQuery->first_result == replica_result->query_result());
        // }

        //Record the dependencies.
       
        for(auto dep: *replica_result->mutable_query_read_set()->mutable_deps()){ //For normal Tx-id
            Debug("TESTING: Received Dep: %s", BytesToHex(dep.write().prepared_txn_digest(), 16).c_str());
            if(dep.write().has_prepared_timestamp()){ //I.e. using optimisticTxID
                auto itr = pendingQuery->merged_ss.merged_ts().find(MergeTimestampId(dep.write().prepared_timestamp().timestamp(), dep.write().prepared_timestamp().id()));
                if(itr != pendingQuery->merged_ss.merged_ts().end() && itr->second.prepared()){ //Check whether tx was recorded in snapshot (as prepared)
                //if(pendingQuery->merged_ss.merged_ts().count(MergeTimestampId(dep.write().prepared_timestamp().timestamp(), dep.write().prepared_timestamp().id()))){
                    dep.mutable_write()->clear_prepared_timestamp();
                    result_mgr.merged_deps.insert(dep.release_write());
                } 
            }
            else{
                auto itr = pendingQuery->merged_ss.merged_txns().find(dep.write().prepared_txn_digest());
                if(itr != pendingQuery->merged_ss.merged_txns().end() && itr->second.prepared()){ //Check whether tx was recorded in snapshot (as prepared)
                //if(pendingQuery->merged_ss.merged_txns().count(dep.write().prepared_txn_digest())){
                     result_mgr.merged_deps.insert(dep.release_write());
                } 
            }            
        }
         //Set deps to merged deps == recorded dependencies from f+1 replicas -> one correct replica reported upper bound on deps
        if(matching_res == params.query_params.resultQuorum){
            proto::ReadSet *query_read_set = replica_result->mutable_query_read_set();
            query_read_set->clear_deps(); //Reset and override with merged deps
            for(auto write: result_mgr.merged_deps){
                Debug("TEST: Adding dep %s", BytesToHex(write->prepared_txn_digest(), 16).c_str());
                proto::Dependency *add_dep = query_read_set->add_deps();
                add_dep->set_involved_group(group);
                add_dep->set_allocated_write(write);
            }
        }
        // for(auto tx_id: *replica_result->mutable_query_read_set()->mutable_dep_ids()){ //For normal Tx-id
        //     //Add to dependencies only if it was a tx that was seen during sync, and that was marked as prepare.
        //     if(pendingQuery->merged_ss.merged_txns().count(tx_id)){
        //         result_mgr.merged_deps.insert(std::move(tx_id));
        //     }
        // }
        // for(auto dep_ts: *replica_result->mutable_query_read_set()->mutable_dep_ts_ids()){  //For optimistic Tx-id (TS)
        //     //Add to dependencies only if it was a tx that was seen during sync, and that was marked as prepare.
        //     if(pendingQuery->merged_ss.merged_ts().count(dep_ts.dep_ts())){
        //         result_mgr.merged_deps.insert(std::move(*dep_ts.mutable_dep_id()));
        //     }
        // }
        
        // //Set deps to merged deps == recorded dependencies from f+1 replicas -> one correct replica reported upper bound on deps
        // if(matching_res == params.query_params.resultQuorum){
        //     proto::ReadSet *query_read_set =  replica_result->mutable_query_read_set();
        //     query_read_set->clear_dep_ids(); //Reset and override with merged deps
        //     query_read_set->clear_dep_ts_ids(); //Reset and override with merged deps
        //     for(auto tx_id: result_mgr.merged_deps){
        //         query_read_set->add_dep_ids(std::move(tx_id));
        //     }
        // }
        

         
    }
  
    //4) if receive enough --> upcall;  At client: Add query identifier and result to Txn

    Debug("[group %i] Req %lu. Matching_res %d. resultQuorum: %d \n", group, queryResult.req_id(), matching_res, params.query_params.resultQuorum);
        // Only need results from "result" shard (assuming simple migration scheme)
    if(matching_res == params.query_params.resultQuorum){
        Debug("[group %i] Reached sufficient matching results for QueryResult Reply %lu", group, queryResult.req_id());
        
        //pendingQuery->rcb(REPLY_OK, group, read_set, *replica_result->mutable_query_result_hash(), *replica_result->mutable_query_result(), true);
        pendingQuery->rcb(REPLY_OK, group, replica_result->release_query_read_set(), *replica_result->mutable_query_result_hash(), *replica_result->mutable_query_result(), true);
        // Remove/Deltete pendingQuery happens in upcall
        return;
    }

    //5) if not enough matching --> retry; 
    // if not optimistic id: wait for up to result Quorum many messages (f+1). With optimistic id, wait for f additional.

    bool no_bonus = (params.query_params.eagerExec) || (params.query_params.optimisticTxID && pendingQuery->retry_version);
    //bool request_bonus = (!params.query_params.eagerExec && params.query_params.optimisticTxID && pendingQuery->retry_version == 0);
    uint64_t expectedResults = no_bonus ? params.query_params.resultQuorum : params.query_params.resultQuorum + config->f;

    std::cerr << "Designated replies: " << pendingQuery->num_designated_replies << std::endl;
    std::cerr << "ExpectedResults: " << expectedResults << std::endl;
    int maxWait = std::max(pendingQuery->num_designated_replies - config->f, expectedResults); //wait for at least maxWait many, but can wait up to #syncMessages sent - f. (if that is larger). 
    UW_ASSERT(maxWait > 0);
    //Note that expectedResults <= num_designated_replies, since params.resultQuorum <= params.syncMessages, and +f optimisticID is applied to both.
    
    //Waited for max number of result replies that can be expected. //TODO: Can be "smarter" about this. E.g. if waiting for at most f+1 replies, as soon as first non-matching arrives return...
    if(pendingQuery->resultsVerified.size() == maxWait){
        //Panic("Testing");
        Debug("[group %i] Received sufficient inconsistent replies to determine Failure for QueryResult %lu", group, queryResult.req_id());
       //pendingQuery->rcb(REPLY_FAIL, group, read_set, *replica_result->mutable_query_result_hash(), *replica_result->mutable_query_result(), false);
       pendingQuery->rcb(REPLY_FAIL, group, replica_result->release_query_read_set(), *replica_result->mutable_query_result_hash(), *replica_result->mutable_query_result(), false);
        //Remove/Delete pendingQuery happens in upcall
       return;
    }
   
    Debug("[group %i] Waiting for additional QueryResult Replies for Req %lu. So far: %d. maxWait %d \n", group, queryResult.req_id(), pendingQuery->resultsVerified.size(), maxWait);
   

    //6) remove pendingQuery object --> happens in upcall to client (calls ClearQuery)

    //TODO: edit syncQueryReply such that it can also function as HandleQueryResult...
}

void ShardClient::HandleFailQuery(proto::FailQuery &queryFail){
    //0) find PendingQuery object via request id
     auto itr = this->pendingQueries.find(queryFail.req_id());
    if (itr == this->pendingQueries.end()) return; // this is a stale request

    PendingQuery *pendingQuery = itr->second;
    Debug("[group %i] QueryFail Reply for request %lu.", group, queryResult.req_id());

    //1) authenticate reply & parse contents
    proto::FailQueryMsg *query_fail;

     if (params.validateProofs && params.signedMessages) {
        if (queryFail.has_signed_fail()) {

            if (!verifier->Verify(keyManager->GetPublicKey(queryFail.signed_fail().process_id()),
                    queryFail.signed_fail().data(), queryFail.signed_fail().signature())) {
                Debug("[group %i] Failed to validate signature for query fail reply from replica %lu.", group, queryFail.signed_fail().process_id());
                return;
            }
            if(!validated_fail.ParseFromString(queryFail.signed_fail().data())) {
                Debug("[group %i] Invalid serialization of Fail.", group);
                return;
            }
          query_fail = &validated_fail;

        } else {
            Panic("Query Fail Reply without required signature"); //Note: Only panic for debugging purposes
        }
    } else {
       query_fail = queryFail.mutable_fail();
    }

    //3) check whether replica in group.
    if (!IsReplicaInGroup(query_fail->replica_id(), group, config)) {
      Debug("[group %d] Query Fail from replica %lu who is not in group.",
          group, query_fail->replica_id());
      return;
    }

    //4) check for duplicates -- (ideally check before verifying sig)
    if (!pendingQuery->resultsVerified.insert(query_fail->replica_id()).second) {
      Debug("Already received query fail from replica %lu.", query_fail->replica_id());
      return;
    }
    //Note: Use the same resultVerified set, but keep separate result/fail count -- this guarantees that byz replica canot add itself to both resultVerified and failsVerified, thus artificially increasing the count of replies.
    pendingQuery->numFails++;
    

    //5) if enough failures to imply one correct reported failure OR not enough replies to conclude success ==> retry
      // if eager or not optimistic id: wait for up to result Quorum many messages (f+1). With optimistic id, wait for f additional.
        
        bool no_bonus = (params.query_params.eagerExec) || (params.query_params.optimisticTxID && pendingQuery->retry_version);
        //bool request_bonus = (!params.query_params.eagerExec && params.query_params.optimisticTxID && pendingQuery->retry_version == 0);
        uint64_t expectedResults = no_bonus ? params.query_params.resultQuorum : params.query_params.resultQuorum + config->f;
        int maxWait = std::max(pendingQuery->num_designated_replies - config->f, expectedResults); //wait for at least maxWait many, but can wait up to #syncMessages sent - f. (if that is larger). 
        //Note that expectedResults <= num_designated_replies, since params.resultQuorum <= params.syncMessages, and +f optimisticID is applied to both.

    if(pendingQuery->numFails == config->f + 1 || pendingQuery->resultsVerified.size() == maxWait){
        //FIXME: Use a different callback to differentiate Fail due to optimistic ID, and fail due to abort/missed tx?
        //std::map<std::string, TimestampMessage> dummy_read_set;
        proto::ReadSet *dummy_read_set = nullptr;
        std::string dummy("");
        pendingQuery->rcb(REPLY_FAIL, group, dummy_read_set, dummy, dummy, false);
    }
    return;
}


void ShardClient::HandlePointQueryResult(proto::PointQueryResultReply &queryResult){

    //TODO: In Client.cc: When calling Query --> attach bool = point + Create new callback. In querysync-client.cc: In Query send ==> Set Point bool
    //Re-factor callback code to be a function that is bound. Cleaner code...

    //TODO: Fetch PendingQuery

    //TODO: Copy code from HandleGetReply
        //1)  Check signature
        //2) Check correctness of committed proof
        //3) Check for matching prepare

    // ==> Call exact same code as in Get function (just re-cycle it?) but with different proof.
    //TODO: For proof: store in write set the index --> use it to look up.
    // Store table_name in pending or get it from key.

    //Add winner read to ReadSet
    //Upcall Query callback (use a different one for point read) ==> simply stores to read set and upcalls to app with result.
        //Note: No retries needed; No multi shard replies needed; No storing


    //) check whether replica in group.
    if (!IsReplicaInGroup(queryResult.replica_id(), group, config)) {
        Debug("[group %d] PointQueryResult from replica %lu who is not in group.", group, queryResult.replica_id());
        return;
    }

    auto itr = this->pendingQueries.find(queryResult.req_id());
    if (itr == this->pendingQueries.end()){
        //Panic("Stale Query Result");
        return; // this is a stale request
    } 

    PendingQuery *pendingQuery = itr->second;
    
    Debug("[group %i] Received PointQueryResult Reply for req-id [%lu]", group, queryResult.req_id());

    //1) authenticate reply & parse contents
    proto::Write *write;

    if (params.validateProofs && params.signedMessages) {
        if (queryResult.has_signed_write()) {

            if(queryResult.replica_id() != queryResult.signed_write().process_id()){
                Debug("Replica %lu falsely claims to be replica %lu", queryResult.signed_write().process_id(), queryResult.replica_id());
                return;
            } 

            if (!verifier->Verify(keyManager->GetPublicKey(queryResult.signed_write().process_id()),
                    queryResult.signed_write().data(), queryResult.signed_write().signature())) {
                Debug("[group %i] Failed to validate signature for query result reply from replica %lu.", group, queryResult.signed_write().process_id());
                return;
            }
            if(!validatedPrepared.ParseFromString(queryResult.signed_write().data())) {
                Debug("[group %i] Invalid serialization of Result.", group);
                return;
            }
            write = &validatedPrepared;

        } else {
            //Note: If queryResult write = empty (no committed/pepared) ==> has_write() will be false
            // if(queryResult.has_write()){
            //     Panic("PointQuery result has neither signed write, nor plain write");
            //     return;
            // }

             //TODO: For committed writes could use just authenticated channels (since committed writes come with a proof)
               //Currently we are signing ReadReplies only to prove that message indeed came for a certain replica -- we never need to forward the sig though (so we don't need disamibiguation)
            if(queryResult.has_write() && queryResult.write().has_committed_value()) {      
                Debug("[group %i] queryResult contains unsigned committed value.", group);
                return;
            }
    

            //If write has only a prepared value --> it only needs to be verified if params.verifyDeps is set (in order to forwarded dep sigs + assert that they are valid)
            if (params.verifyDeps && queryResult.has_write() && queryResult.write().has_prepared_value()) {
                //TODO: remove params.verifyDeps if one wants to always sign prepared (this edge case realistically never happens)
                Debug("[group %i] Reply contains unsigned prepared value.", group);
                return;
            }

            write = queryResult.mutable_write();
            //if(!write->has_committed_value() && write->has_prepared_value()) Panic("Prepared write was not signed.\n");
            UW_ASSERT(!write->has_committed_value());
            UW_ASSERT(!write->has_prepared_value() || !params.verifyDeps);
        }
    } else {
        write = queryResult.mutable_write();
    }


    //4) check for duplicates -- (ideally check before verifying sig)
    if (!pendingQuery->resultsVerified.insert(queryResult.replica_id()).second) {
      Debug("Already received query fail from replica %lu.", queryResult.replica_id());
      return;
    }

    PendingQuorumGet *req = &pendingQuery->pendingPointQuery;

    const proto::CommittedProof *proof = queryResult.has_proof() ? &queryResult.proof() : nullptr;
    bool finished = ProcessRead(queryReq.req_id(), req, read_t::POINT, write, queryResult.has_proof(), proof, queryResult);

    if(finished){
        query_seq_num_mapping.erase(pendingQuery->query_seq_num);
         pendingQueries.erase(itr);
         delete pendingQuery;
    } 
}

//All of this code is borrowed from HandleReadReply
bool ShardClient::ProcessRead(const uint64_t &reqId, PendingQuorumGet *req, read_t read_type, proto::Write *write, bool has_proof, const proto::CommittedProof *proof, proto::PointQueryResultReply &reply){

    sql::QueryResultProtoWrapper query_result;
    //query_result::QueryResult *query_result; //TODO: Augment callback to return this instead of serialized value to avoid redundant deserialization.
                                                    //Note: However, winning Value could be prepared too. Would have to deser prepared values too, if winners
                                                    // ==> Would need to store QueryResult as maxValue instead of value string.

    //check whether value and timestamp are valid
    req->numReplies++;
    if (write->has_committed_value() && write->has_committed_timestamp()) {
        if (params.validateProofs) {
        if (!has_proof) {
            Debug("[group %i] Missing proof for committed write.", group);
            return false;
        }

        std::string committedTxnDigest = TransactionDigest(proof->txn(), params.hashDigest);

        bool valid = false; 
        if(read_type == read_t::GET){
            valid = ValidateTransactionWrite(*proof, &committedTxnDigest, req->key, write->committed_value(), write->committed_timestamp(), config, params.signedMessages, keyManager, verifier);
        } 
        else { //if read type POINT 
            //std::cerr << "WriteValue: " << write->committed_value() << std::endl;   
            valid = ValidateTransactionTableWrite(*proof, &committedTxnDigest, write->committed_timestamp(), req->key, write->committed_value(), req->table_name, &query_result);
        }

        if (!valid) {
            Debug("[group %i] Failed to validate committed value for pointQuery %lu.", group, reqId);
            // invalid replies can be treated as if we never received a reply from   a crashed replica
            return false;
        }
        }

        Timestamp replyTs(write->committed_timestamp());
        Debug("[group %i] PointQueryReply for %lu with committed %lu byte value and ts %lu.%lu.", group, reqId, write->committed_value().length(),replyTs.getTimestamp(), replyTs.getID());

        if (req->firstCommittedReply || req->maxTs < replyTs) {
            req->maxTs = replyTs;
            req->maxValue = write->committed_value();
        }
        req->firstCommittedReply = false;
    }

    //TODO: change so client does not accept reads with depth > some t... (fine for now since servers use the same param setting, and we wait for f+1 matching servers)
    if (params.maxDepDepth > -2 && write->has_prepared_value() && write->has_prepared_timestamp() && write->has_prepared_txn_digest()) {
        // Timestamp preparedTs(write->prepared_timestamp());
        // Debug("[group %i] ReadReply for %lu with prepared %lu byte value and ts %lu.%lu.", group, reqId, write->prepared_value().length(), preparedTs.getTimestamp(), preparedTs.getID());
        // auto preparedItr = req->prepared.find(preparedTs);
        // if (preparedItr == req->prepared.end()) {
        //     req->prepared.insert(std::make_pair(preparedTs, std::make_pair(*write, 1)));
        // } else if (preparedItr->second.first == *write) {
        //     preparedItr->second.second += 1;
        // }
        // else{
        //     Panic("Illegal branch -- 2 different txns with same ts"); // TODO: FIXME: Want to handle this!!! Byz one could be the first. Want to keep counting. 
        //                                                                                 //FIX ALSO FOR READS FIXME:
        // }
        // //if(!write->has_committed_value() && write->has_prepared_value()) std::cerr << "Prepared write was processed.\n";
        // if (params.validateProofs && params.signedMessages && params.verifyDeps) {
        //     proto::Signature *sig = req->preparedSigs[preparedTs].add_sigs();
        //     sig->set_process_id(reply.signed_write().process_id());
        //     *sig->mutable_signature() = reply.signed_write().signature();
        // }

        //std::cerr << "WriteValue (prepared): " << write->prepared_value() << std::endl;   

        Timestamp preparedTs(std::move(*write->mutable_prepared_timestamp()));
        Debug("[group %i] ReadReply for %lu with prepared %lu byte value and ts %lu.%lu.", group, reqId, write->prepared_value().length(), preparedTs.getTimestamp(), preparedTs.getID());

        Debug("Read reply has txn_dig %s / %s (hex).", write->prepared_txn_digest().c_str(), BytesToHex(write->prepared_txn_digest(), 16).c_str());
        std::tuple<Timestamp, std::string, std::string> prepVal; // = std::make_tuple();   //tuple (timestamp, txn_digest, value)
        std::get<0>(prepVal) = std::move(*write->mutable_prepared_timestamp());
        std::get<1>(prepVal) = std::move(*write->mutable_prepared_txn_digest());
        std::get<2>(prepVal) = std::move(*write->mutable_prepared_value());

        
       
        auto &[count, sigs] = req->prepared_new[std::move(prepVal)];
        count++;
                                                                  
        if (params.validateProofs && params.signedMessages && params.verifyDeps) {
            proto::Signature *sig = sigs.add_sigs();
            sig->set_process_id(reply.signed_write().process_id());
            *sig->mutable_signature() = reply.signed_write().signature();
        }
        
    }
    
    
    if (req->numReplies >= readQuorumSize) {
        if (params.maxDepDepth > -2) {
            // for (auto preparedItr = req->prepared.rbegin();preparedItr != req->prepared.rend(); ++preparedItr) {
            //     if (preparedItr->first < req->maxTs) {
            //      break;
            //     }   
            //     //  std::cerr << "Read PREPARED RESULT n times: " << preparedItr->second.second << std::endl;
            //     // std::cerr << "Read PREPARED RESULT: " << preparedItr->second.first.prepared_value() << std::endl;
            //     if (preparedItr->second.second >= params.readDepSize) {
            //         req->maxTs = preparedItr->first;
            //         req->maxValue = preparedItr->second.first.prepared_value();
            //         *req->dep.mutable_write() = preparedItr->second.first;
            //         if (params.validateProofs && params.signedMessages && params.verifyDeps) {
            //             *req->dep.mutable_write_sigs() = req->preparedSigs[preparedItr->first];
            //         }
            //         req->dep.set_involved_group(group);
            //         req->hasDep = true;
            //         break;
            //     }
            // }

            //TODO:  Check that dependency in both code version matches...
            // Should contain toy dep. 
            //TODO: Need to add toy dep to commit, or else we will be stuck waiting on dependent.
            
            for (auto preparedItr = req->prepared_new.rbegin();preparedItr != req->prepared_new.rend(); ++preparedItr) {
                //Reverse order by timestamp
                const Timestamp &ts = std::get<0>(preparedItr->first);
                if (ts < req->maxTs) {
                 break;
                }   
                auto &[count, sigs] = preparedItr->second;
                if (count >= params.readDepSize) {
                    req->maxTs = ts;
                    req->maxValue = std::get<2>(preparedItr->first);
                    *req->dep.mutable_write()->mutable_prepared_txn_digest() = std::get<1>(preparedItr->first);
                    if (params.validateProofs && params.signedMessages && params.verifyDeps) {
                        //FIXME: To succeed in verifyDeps verification: Need to set whole Write... ==> However, that makes no sense. Deprecate verifyDeps.
                        *req->dep.mutable_write()->mutable_prepared_value() = req->maxValue; 
                        ts.serialize(req->dep.mutable_write()->mutable_prepared_timestamp());
                        *req->dep.mutable_write_sigs() = std::move(sigs);
                    }
                    req->dep.set_involved_group(group);
                    req->hasDep = true;
                    break;
                }
            }
        }
        //Only read once.
        const auto [it, first_read] = readValues.emplace(req->key, req->maxValue); // readValues.insert(std::make_pair(req->key, req->maxValue));

        // std::cerr << "Key: " << req->key << std::endl;
        //  std::cerr << "MaxValue: " << req->maxValue << std::endl;
        //  std::cerr << "Max TS: " << req->maxTs.getTimestamp() << ":" << req->maxTs.getID() << std::endl;

        if(first_read){ //for first read
            ReadMessage *read = txn.add_read_set();
            *read->mutable_key() = req->key;
            req->maxTs.serialize(read->mutable_readtime());
            
            req->prcb(REPLY_OK, req->key, req->maxValue, req->maxTs, req->dep,req->hasDep, true);
        }
        else{ //TODO: Could optimize to do this right at the start of Handle Read to avoid any validation costs... -> Does mean all reads have to lookup twice though.
            std::string &prev_read = it->second;
            req->maxTs = Timestamp();
            req->prcb(REPLY_OK, req->key, prev_read, req->maxTs, req->dep, false, false); //Don't add to read set.

        } 
        return true;
  }
    
  return false;
}

bool ShardClient::ValidateTransactionTableWrite(const proto::CommittedProof &proof, const std::string *txnDigest, const Timestamp &timestamp, 
    const std::string &key, const std::string &value, const std::string &table_name, sql::QueryResultProtoWrapper *query_result)
{

    Debug("[group %i] Trying to validate committed TableWrite.", group);
    
    //*query_result = std::move(sql::QueryResultProtoWrapper(value));
    SQLResultProto proto_result;
    if(!value.empty()) proto_result.ParseFromString(value);
    query_result->SetResult(proto_result);
    
    //query_result = new sql::QueryResultProtoWrapper(value); //query_result takes ownership
    //turn value into Object //TODO: Can we avoid the redundant de-serialization in client.cc? ==> Modify prcb callback to take QueryResult as arg. 
                                //Then need to change that gcb = prcb (no longer true)

    //NOTE: Currently useless line of code: If empty ==> no Write ==> We would never even enter Validate Transaction branch  
            //We don't send empty results, we just send nothing.
            //if we did send result: if query_result empty => return true. No proof needed, since replica is reporting that no value for the requested read exists (at the TS)
    if(query_result->empty()){
        return true;
    } 


    if (proof.txn().client_id() == 0UL && proof.txn().client_seq_num() == 0UL) {
        // TODO: this is unsafe, but a hack so that we can bootstrap a benchmark
        //    without needing to write all existing data with transactions
        Debug("Accept genesis proof");
        return true; //query_result->empty(); //Confirm that result is empty. (Result must be empty..)
    }

    UW_ASSERT(query_result->size() == 1); //Point read should have just one row.

    //Check that txn in proof matches reported timestamp
    if (Timestamp(proof.txn().timestamp()) != timestamp) {
        Debug("VALIDATE timestamp failed for txn %lu.%lu: txn ts %lu.%lu != returned ts %lu.%lu.", proof.txn().client_id(), proof.txn().client_seq_num(),
            proof.txn().timestamp().timestamp(), proof.txn().timestamp().id(), timestamp.getTimestamp(), timestamp.getID());
        return false;
    }

    //Check that Commit Proof is correct
    if (false && params.signedMessages && !ValidateCommittedProof(proof, txnDigest, keyManager, config, verifier)) {
        Debug("VALIDATE CommittedProof failed for txn %lu.%lu.", proof.txn().client_id(), proof.txn().client_seq_num());
        Panic("Verification should be working");
        return false;
    }

    uint32_t row_idx;
    //Check that write set of proof contains key.
    bool keyInWriteSet = false;
    for (const auto &write : proof.txn().write_set()) {
        if (write.key() == key) {
            keyInWriteSet = true;

            if(!write.has_rowupdates() || !write.rowupdates().has_row_idx()) return false;
            row_idx = write.rowupdates().row_idx();
            break;
        }
    }
    
    if (!keyInWriteSet) {
        Debug("VALIDATE value failed for txn %lu.%lu; key %s not written.", proof.txn().client_id(), proof.txn().client_seq_num(), BytesToHex(key, 16).c_str());
        return false;
    }

    //Then check that row idx of TableWrite wrote a row whose column values == result.column_values (and is not a deletion)
            //Note: check result column name --> find matching column name in TableWrite and compare value
               // ==> For Select * or Select subset of columns statements this is sufficient
            //If column name is some "creation" (e.g. new col name, or some operation like Count, Max) then ignore --> this is too complex to prototype


    //TODO: For real system need to replay Query statement on the TableWrite row. For our prototype we just approximate it.

    // size_t pos = key.find(unique_delimiter); 
    // UW_ASSERT(pos != std::string::npos);
    // std::string table_name = key.substr(0, pos); //Extract from Key
    const TableWrite &table_write = proof.txn().table_writes().at(table_name); //FIXME: Throw exception if not existent. /-->change to find
    const RowUpdates &row_update = table_write.rows()[row_idx];

    ColRegistry *col_registry = sql_interpreter->GetColRegistry(table_name); 
    int col_idx = 0;
    for(int i = 0; i < query_result->num_columns(); ++i){
        //find index of column name  -- if not present in table write --> return false
        const std::string &col_name = query_result->name(i);
        
       //then find right col value and compare
       col_idx = col_registry->col_name_index[col_name]; 
         //while(col_name != table_write.column_names) If storing column names in table write --> iterate through them to find matching col (idx).  Assuming here column names are in the same order.

    }
    Debug("VALIDATE TableWrite value successfully for txn %lu.%lu key %s", proof.txn().client_id(), proof.txn().client_seq_num(), key.c_str());
  return true;
}


bool ShardClient::isValidQueryDep(const uint64_t &query_seq_num, const std::string &txnDigest, const proto::Transaction* txn){

    Debug("Check if Txn: %s is a valid dep for query seq no: %d", BytesToHex(txnDigest, 16).c_str(), query_seq_num);
    auto itr_q = query_seq_num_mapping.find(query_seq_num);
    if(itr_q == query_seq_num_mapping.end()){
        return false;
    }
    auto itr = pendingQueries.find(itr_q->second);
    if (itr == pendingQueries.end()) {
        return false; // this is a stale request
    }
    
    PendingQuery *pendingQuery = itr->second;
    proto::MergedSnapshot &merged_ss =pendingQuery->merged_ss;

    if(params.query_params.eagerExec){  //Make exception if current Query is eager and we are caching   
        return true; // CURRENTLY always eager.
        Debug("With caching + always eager exec: Accept any dependency");
        Debug("FOR REAL RUN UNCOMMENT CORRECT EAGER EXEC LINE");
        bool is_eager = (!pendingQuery->retry_version && (pendingQuery->is_point? params.query_params.eagerPointExec : params.query_params.eagerExec));
        if(is_eager) return true;

        //Note: If one really wanted to avoid false positive deps even during the eager case -> send a bloom filter of tx_ids in addition to the read_set_hash. 
        //Check whether or not Txn is in the bloom filter. (there might be some false positives, but that's fine) BF should be sized relative to txns.
    }

    //else: cannot be eager. Thus, in order to have dependencies, the sync protocol must have issued a snapshot 
     //Note: pointQueries that were not eager don't cache

  //TODO: also support TS version ==> TODO: Add Timestamp to arguments here and in isDep. Then compute MergedTimestamp from Timestamp, and look it up.
  if(params.query_params.optimisticTxID && !pendingQuery->retry_version){
    //Warning("Currently don't yet support isValidQueryDep for merged snapshots with TS only");
    uint64_t merged_ts = MergeTimestampId(txn->timestamp().timestamp(), txn->timestamp().id());
    for(auto &[ts_id, _]: merged_ss.merged_ts()){
        if(ts_id == merged_ts) return true;
    }
    return true;
  }

  for(auto &[tx_id, _]: merged_ss.merged_txns()){
    if(tx_id == txnDigest) return true;
  }
 
  return false;
}

} //namespace pequinstore

