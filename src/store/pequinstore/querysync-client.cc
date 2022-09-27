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
      result_callback rcb, result_timeout_callback rtcb, uint32_t timeout) {

 Debug("Invoked QueryRequest [%lu] on ShardClient for group %d", query_seq_num, group);
  
  //TODO: (Very low priority) how to execute query in such a way that it includes possibly buffered write values. --> Could imagine sending Put Buffer alongside query, such that servers use it to compute result. 
  // No clue how that would affect read set though (such versions should always pass CC check), and whether it can be used by byz to equivocate read set, causing abort.

  uint64_t reqId = lastReqId++;
  PendingQuery *pendingQuery = new PendingQuery(reqId);
  query_seq_num_mapping[query_seq_num] = reqId;
  pendingQueries[reqId] = pendingQuery;
  pendingQuery->client_seq_num = client_seq_num;
  pendingQuery->query_seq_num = query_seq_num;

   if(params.query_params.signClientQueries && params.query_params.cacheReadSet){
        pendingQuery->queryDigest = std::move(QueryDigest(queryMsg, params.hashDigest));
   }
   

  //pendingQuery->query = query; //Is this necessary to store? In case of re-send? --> Cannot move since other shards will use same reference.
  //pendingQuery->qts = ts;

  //pendingQuery->retry = retry;

  pendingQuery->query_manager = (queryMsg.query_manager() == group);
  pendingQuery->rcb = rcb;
  pendingQuery->rtcb = rtcb;
  
 
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
void ShardClient::RetryQuery(uint64_t query_seq_num, proto::Query &queryMsg){

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

    pendingQuery->snapshotsVerified.clear();
    pendingQuery->numSnapshotReplies = 0;
    pendingQuery->txn_freq.clear();
    pendingQuery->merged_ss.Clear();

    pendingQuery->resultsVerified.clear();
    pendingQuery->numResults = 0;
    pendingQuery->numFails = 0;
    //pendingQuery->failsVerified.clear();
    pendingQuery->result_freq.clear();

    pendingQuery->retry_version++;
    queryMsg.clear_query_cmd(); //NOTE: Don't need to re-send query command for retries. (Assuming we're sending it only to the same replicas)
  
    RequestQuery(pendingQuery, queryMsg);
}

//pass a query object already from client: This way it avoids copying the query string across multiple shards and for retries
void ShardClient::RequestQuery(PendingQuery *pendingQuery, proto::Query &queryMsg){

//   queryMsg.Clear();
//   queryMsg.query_seq_num(pendingQuery->query_seq_num);
//   queryMsg.set_client_id(client_id);
//   *queryMsg.mutable_query() = pendingQuery->query;
//   *queryMsg.mutable_timestamp() = pendingQuery->qts;  //contains client_id as well. 

  //Note: Byz client can also equivocate query contents for same id. It could then send same sync set to all. This would produce different read sets, but it would not be detected.
  // ---> Implies that query contents must be uniquely hashed too? To guarantee every replica gets same query. I.e. Query id = hash(seq_no, client_id, query-string, timestamp)?
  //pendingQuery->query_id = QueryDigest(query, params.hashDigest); 
  
  queryReq.set_req_id(pendingQuery->reqId);
  queryReq.set_optimistic_txid(!pendingQuery->retry_version && params.query_params.optimisticTxID);//On retry use unique/deterministic tx id only.
  //queryReq.set_retry_version(pendingQuery->retry_version);

  // This is proof that client does not equivocate query contents --> Otherwise could intentionally produce different read sets at replicas, which -- if caching read set -- can be used to abort partially.
  //NOTE: Hash should suffice to achieve non-equiv --> 2 different queries have different hash.
  if(params.query_params.signClientQueries){
     SignMessage(&queryMsg, keyManager->GetPrivateKey(keyManager->GetClientKeyId(client_id)), client_id, queryReq.mutable_signed_query());
  }
  else{
    *queryReq.mutable_query() = queryMsg; // NOTE: cannot use std::move(queryMsg) because queryMsg objet may be passed to multiple shardclients.
  }
 
  UW_ASSERT(params.query_params.queryMessages <= closestReplicas.size());
  for (size_t i = 0; i < params.query_params.queryMessages; ++i) {
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
    const proto::LocalSnapshot *local_ss;

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
        local_ss = &SyncReply.local_ss();
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

    // 5) Add all tx in list to filtered Datastructure --> everytime a tx reaches the MergeThreshold directly add it to the ProtoReply
      //If necessary, decode tx list.

    //what if some replicas have it as committed, and some as prepared. If >=f+1 committed ==> count as committed, include only those replicas in list.. If mixed, count as prepared
    //DOES client need to consider at all whether a txn is committed/prepared? --> don't think so; replicas can determine dependency set at exec time (and either inform client, or cache locally)
    //TODO: probably don't need separate lists! --> FIXME: Change back to single list in protobuf.
    for(const std::string &txn_dig : local_ss->local_txns_committed()){
       std::set<uint64_t> &replica_set = pendingQuery->txn_freq[txn_dig];
       replica_set.insert(local_ss->replica_id());
       if(replica_set.size() == params.query_params.mergeThreshold){
          *(*pendingQuery->merged_ss.mutable_merged_txns_committed())[txn_dig].mutable_replicas() = {replica_set.begin(), replica_set.end()}; //creates a temp copy, and moves it into replica list.
       }

    }
    // for(std::string &txn_dig : local_ss.local_txns_prepared()){ 
    //    pendingQueries->txn_freq[txn_dig].insert(local_ss->replica_id());
    // }
    
    // 6) Once #QueryQuorum replies received, send SyncMessages
    pendingQuery->numSnapshotReplies++;
    if(pendingQuery->numSnapshotReplies == params.query_params.syncQuorum){
        SyncReplicas(pendingQuery);
    }
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

    if(params.query_params.signClientQueries && params.query_params.cacheReadSet){ //FIXME: For now, only signing if using Cached Read Set. --> only then need to avoid equivocation
      pendingQuery->merged_ss.set_query_digest(pendingQuery->queryDigest);
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

    Debug("[group %i] Sent Query Sync Messages for query [seq:ver] [%lu : %lu] \n", group, pendingQuery->query_seq_num, pendingQuery->retry_version);
}


void ShardClient::HandleQueryResult(proto::QueryResult &queryResult){
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
    proto::Result *replica_result;

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
    std::map<std::string, TimestampMessage> read_set;

    //3) wait for up to result_threshold many matching replies (result + result_hash/read set)
    if(params.query_params.cacheReadSet){
        Debug("Read-set hash: %s", BytesToHex(replica_result->query_result_hash(), 16).c_str());
         matching_res = ++pendingQuery->result_freq[replica_result->query_result()][replica_result->query_result_hash()]; //map should be default initialized to 0.
    }
    else{ //manually compare that read sets match. Easy way to compare: Hash ReadSet.
        Debug("[group %i] Validating ReadSet for QueryResult Reply %lu", group, queryResult.req_id());
         read_set = {replica_result->query_read_set().begin(), replica_result->query_read_set().end()}; //FIXME: Does the map automatically become ordered?
         std::string validated_result_hash = std::move(generateReadSetSingleHash(read_set));
         //std::string validated_result_hash = std::move(generateReadSetMerkleRoot(read_set, params.merkleBranchFactor));
            // //TESTING:
             Debug("Read-set hash: %s", BytesToHex(validated_result_hash, 16).c_str());
            // for(auto [key, ts] : read_set){
            //     Debug("Read key %s with version [%lu:%lu]", key, ts.timestamp(), ts.id());
            // }
            // //


         matching_res = ++pendingQuery->result_freq[replica_result->query_result()][validated_result_hash]; //map should be default initialized to 0.
         if(pendingQuery->result_freq[replica_result->query_result()].size() > 1) Panic("When testing without optimistic id's all hashes should be the same.");
    }
  
    //4) if receive enough --> upcall;  At client: Add query identifier and result to Txn
        // Only need results from "result" shard (assuming simple migration scheme)
    if(matching_res == params.query_params.resultQuorum){
        Debug("[group %i] Reached sufficient matching results for QueryResult Reply %lu", group, queryResult.req_id());
        
        pendingQuery->rcb(REPLY_OK, group, read_set, *replica_result->mutable_query_result_hash(), *replica_result->mutable_query_result(), true);
        // Remove/Deltete pendingQuery happens in upcall
        return;
    }

    //5) if not enough matching --> retry; 
    // if not optimistic id: wait for up to result Quorum many messages (f+1). With optimistic id, wait for f additional.
    uint64_t expectedResults = (params.query_params.optimisticTxID && !pendingQuery->retry_version) ? params.query_params.resultQuorum + config->f : params.query_params.resultQuorum;
    int maxWait = std::max(pendingQuery->num_designated_replies - config->f, expectedResults); //wait for at least maxWait many, but can wait up to #syncMessages sent - f. (if that is larger). 
    //Note that expectedResults <= num_designated_replies, since params.resultQuorum <= params.syncMessages, and +f optimisticID is applied to both.
    
    //Waited for max number of result replies that can be expected. //TODO: Can be "smarter" about this. E.g. if waiting for at most f+1 replies, as soon as first non-matching arrives return...
    if(pendingQuery->resultsVerified.size() == maxWait){
        Debug("[group %i] Received sufficient inconsistent replies to determine Failure for QueryResult %lu", group, queryResult.req_id());
       pendingQuery->rcb(REPLY_FAIL, group, read_set, *replica_result->mutable_query_result_hash(), *replica_result->mutable_query_result(), false);
        //Remove/Delete pendingQuery happens in upcall
       return;
    }
   
    Debug("[group %i] Waiting for additional QueryResult Replies for Req %lu \n", group, queryResult.req_id());

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
      // if not optimistic id: wait for up to result Quorum many messages (f+1). With optimistic id, wait for f additional.
        uint64_t expectedResults = (params.query_params.optimisticTxID && !pendingQuery->retry_version) ? params.query_params.resultQuorum + config->f : params.query_params.resultQuorum;
        int maxWait = std::max(pendingQuery->num_designated_replies - config->f, expectedResults); //wait for at least maxWait many, but can wait up to #syncMessages sent - f. (if that is larger). 
        //Note that expectedResults <= num_designated_replies, since params.resultQuorum <= params.syncMessages, and +f optimisticID is applied to both.

    if(pendingQuery->numFails == config->f + 1 || pendingQuery->resultsVerified.size() == maxWait){
        //FIXME: Use a different callback to differentiate Fail due to optimistic ID, and fail due to abort/missed tx?
        std::map<std::string, TimestampMessage> dummy_read_set;
        std::string dummy("");
        pendingQuery->rcb(REPLY_FAIL, group, dummy_read_set, dummy, dummy, false);
    }
    return;
}


} //namespace pequinstore