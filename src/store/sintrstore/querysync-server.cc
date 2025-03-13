/***********************************************************************
 *
 * store/sintrstore/querysync-server.cc: 
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


#include "store/sintrstore/server.h"

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
#include "store/sintrstore/common.h"
#include "store/sintrstore/phase1validator.h"
#include "store/sintrstore/localbatchsigner.h"
#include "store/sintrstore/sharedbatchsigner.h"
#include "store/sintrstore/basicverifier.h"
#include "store/sintrstore/localbatchverifier.h"
#include "store/sintrstore/sharedbatchverifier.h"
#include "lib/batched_sigs.h"
#include <valgrind/memcheck.h>

#include "store/common/query_result/query_result_proto_builder.h"

namespace sintrstore {

   
//Receive Query Request: Parse and Validate Signatures & Retry Version. Execute query if applicable -> generate and store local snapshot
void Server::HandleQuery(const TransportAddress &remote, proto::QueryRequest &msg){

    // 1) Parse Message
    proto::Query *query;
  
    if(params.query_params.signClientQueries){
        query = new proto::Query();
        query->ParseFromString(msg.signed_query().data());
    }
    else{
        query = msg.release_query(); //mutable_query()
    }

    if (params.rtsMode > 0){
        //Ignore Query requests that are too far in the future. Such requests can produce a lot of RTS
        Timestamp ts(query->timestamp()); 
        if (CheckHighWatermark(ts)) {
            // ignore request if beyond high watermark
             Debug("IGNORE Query[%lu:%lu:%d] (client:q-seq:retry-ver). Timestamp above Watermark", query->client_id(), query->query_seq_num(), query->retry_version());
            delete query;
            if(params.mainThreadDispatching && (!params.dispatchMessageReceive || params.query_params.parallel_queries)) FreeQueryRequestMessage(&msg);
            return;
        }
    }

    //Only process if above watermark. I.e. ignore old queries from the same client
    clientQueryWatermarkMap::const_accessor qw;
    if(clientQueryWatermark.find(qw, query->client_id()) && qw->second >= query->query_seq_num()){
    //if(clientQueryWatermark[query->client_id()] >= query->query_seq_num()){
        Debug("IGNORE Query[%lu:%lu:%d] (client:q-seq:retry-ver). Below WM: %d", query->client_id(), query->query_seq_num(), query->retry_version(), qw->second);
        delete query;
        if(params.mainThreadDispatching && (!params.dispatchMessageReceive || params.query_params.parallel_queries)) FreeQueryRequestMessage(&msg);
        return;
    }
    qw.release();

     // 2) Compute unique hash ID 
    bool hash_query_id = params.query_params.signClientQueries && params.query_params.cacheReadSet && params.hashDigest;
    std::string queryId = QueryDigest(*query, hash_query_id); 

    // std::string queryId;
    // if(params.query_params.signClientQueries && params.query_params.cacheReadSet){ //TODO: when to use hash id? always?
    //     queryId = QueryDigest(*query, params.hashDigest); 
    // }
    // else{
    //     queryId =  "[" + std::to_string(query->query_seq_num()) + ":" + std::to_string(query->client_id()) + "]";
    // }
     Debug("\n Received Query Request Query[%lu:%lu:%d] (client:q-seq:retry-ver), queryId: %s (bytes). Req-id: %d", 
            query->client_id(), query->query_seq_num(), query->retry_version(), BytesToHex(queryId, 16).c_str(), msg.req_id());
   
    //TODO: Ideally check whether already have result or retry version is outdated Before VerifyClientQuery.

    //3) Check whether retry version still relevant.
    queryMetaDataMap::accessor q;
    bool hasQuery = queryMetaData.find(q, queryId);
    if(hasQuery){
        QueryMetaData *query_md = q->second;
        bool valid = true;
        if(query->retry_version() == 0 && !query_md->has_query){
            //This is the first query mention. This will lead to an execution.
            Debug("Received Query cmd. Query Request[%lu:%lu:%d] (client:q-seq:retry-ver)", query->client_id(), query->query_seq_num(), query->retry_version(), query_md->retry_version);
        }
        else if(query->retry_version() < query_md->retry_version){
            Debug("Retry version for Query Request Query[%lu:%lu:%d] (client:q-seq:retry-ver) is outdated. Currently %d", query->client_id(), query->query_seq_num(), query->retry_version(), query_md->retry_version);
            valid = false;
        }
        else if(query->retry_version() == query_md->retry_version){ //TODO: if have result, return result
            //Two cases for which proposed retry version could be == stored retry_version:
                //a) have already received query for this retry version 
                //b) have already received a sync for this retry version (and the sync is not waiting for query)
            ////Return if already received query or sync for the retry version, and sync is not waiting for query. (I.e. no need to process Query) (implies result will be sent.)
            if(query_md->executed_query || query_md->started_sync && !query_md->waiting_sync){ 
                Debug("Already received Sync or Query for Query[%lu:%lu:%d] (client:q-seq:retry-ver). Skipping Query", query->client_id(), query->query_seq_num(),  query->retry_version(), query_md->retry_version);
                valid = false;
            }
        }
        if(!valid){
            Debug("INVALID Query[%lu:%lu:%d] (client:q-seq:retry-ver).", query->client_id(), query->query_seq_num(), query->retry_version());
            delete query;
            if(params.mainThreadDispatching && (!params.dispatchMessageReceive || params.query_params.parallel_queries)) FreeQueryRequestMessage(&msg);
            return;
        }
    }

    //4) Authenticate Query Signature if applicable. If invalid, return.
    if(params.query_params.signClientQueries){  //TODO: Not sure if sigs necessary: authenticated channels (for access control) and hash ids (for uniqueness/non-equivocation) should suffice. NOTE: non-equiv only necessary if caching read set.
        if(!VerifyClientQuery(msg, query, queryId)){ // Does not really need to be parallelized, since query handling is probably already on a worker thread.
            delete query;
            if(params.mainThreadDispatching && (!params.dispatchMessageReceive || params.query_params.parallel_queries)) FreeQueryRequestMessage(&msg);
            return;
        }
    }

    //If PointQuery: 
    if(msg.is_point() && !msg.eager_exec()){ 
        q.release(); //Release if hold
        //Note: If Point uses Eager Exec --> Just use normal protocol path in order to possibly cache read set etc. //FIXME: Make sure is_designated_For_reply
        ProcessPointQuery(msg.req_id(), query, remote);
        if(params.mainThreadDispatching && (!params.dispatchMessageReceive || params.query_params.parallel_queries)) FreeQueryRequestMessage(&msg);
        return;
    }

    //5) Buffer Query conent and timestamp (only buffer the first time)

    bool re_check = false;
            //Note: tbb find and insert are not atomic: Find does not take a lock if noQuery; before insert can claim lock another thread might add query. 
            //==> Must check whether query is the first -- and if not, must re-check (technically it's the first check since hasQuery must have been false) retry version and sync status
    if(!hasQuery){
        bool new_insert = queryMetaData.insert(q, queryId); //If not first insert -> must re-check.  //NOTE: this will unlock and re-lock q
        if(new_insert){
            //UW_ASSERT(query->retry_version() == 0); // We might get them out of order with multi-threading
            q->second = new QueryMetaData(query->query_cmd(), query->timestamp(), remote, msg.req_id(), query->query_seq_num(), query->client_id(), &params.query_params, query->retry_version());
            
            // Debug("\n Insert QueryMd Query[%lu:%lu:%d] (client:q-seq:retry-ver), queryId: %s (bytes). Req-id: %d", 
            //             query->client_id(), query->query_seq_num(), query->retry_version(), BytesToHex(queryId, 16).c_str(), msg.req_id());

        }
        re_check = !new_insert;
    }
    
    QueryMetaData *query_md = q->second;

    if(!query->query_cmd().empty()){ //If queryMetaData was inserted by Sync first, or we received retry first (i.e. query has not been processed yet), set query.
        //UW_ASSERT(query->has_query_cmd()); 
        //We avoid re-sending query_cmd in retry messages (but might have to wait for first query attempt in case multithreading violates FIFO)
        // Debug("Setting QueryMD [%lu:%lu:%d] (client:q-seq:retry-ver), queryId: %s (bytes). Req-id: %d", query->client_id(), query->query_seq_num(), query->retry_version(), BytesToHex(queryId, 16).c_str(), msg.req_id());
        query_md->SetQuery(query->query_cmd(), query->timestamp(), remote, msg.req_id());  //Keep current req_id if it's greater (i.e. belongs to retry that is waiting for original query)
    }

    // //3 Buffer Query content and timestamp (only buffer the first time)
    // queryMetaDataMap::accessor q;
    // bool newQuery = queryMetaData.insert(q, queryId);
    // if(newQuery){ 
    //     q->second = new QueryMetaData(query->query_cmd(), query->timestamp(), remote, msg.req_id(), query->query_seq_num(), query->client_id(), &params.query_params);
    //     //Note: Retry will not contain query_cmd again.
    // }
    // QueryMetaData *query_md = q->second;
    // if(!query_md->has_query){ //If metaData.insert did not return newQuery=true, but query has not been processed yet (e.g. Sync set md first), set query.
    //     newQuery = true;
    //     query_md->SetQuery(query->query_cmd(), query->timestamp(), remote, msg.req_id());  //TODO: Could avoid re-sending query_cmd if implemented FIFO (then only first version == 0 needs to have it.)
    // }

    if(re_check){ //must re-check retry-version because tbb lookup and insert are not atomic...
        //Ignore if retry version old, or we already started sync for this retry version.
        bool valid = true;
        if(query->retry_version() == 0 && !query_md->has_query){
            //This is the first query mention. This will lead to an execution.
            Debug("Received Query cmd. Query Request[%lu:%lu:%d] (client:q-seq:retry-ver)", query->client_id(), query->query_seq_num(), query->retry_version(), query_md->retry_version);
        }
        else if(query->retry_version() < query_md->retry_version){
            Debug("Retry version for Query Request Query[%lu:%lu:%d] (client:q-seq:retry-ver) is outdated. Currently %d", query->client_id(), query->query_seq_num(), query->retry_version(), query_md->retry_version);
            valid = false;
        }
        else if(query->retry_version() == query_md->retry_version){  //TODO: if have result, return result
            ////Return if already received query or sync for the retry version, and sync is not waiting for query. (I.e. no need to process Query) (implies result will be sent.)
            if(query_md->executed_query || query_md->started_sync && !query_md->waiting_sync){ 
                Debug("Already received Sync or Query for Query[%lu:%lu:%d] (client:q-seq:retry-ver). Skipping Query", query->client_id(), query->query_seq_num(), query->retry_version(), query_md->retry_version);
                valid = false;
            }
        }
        if(!valid){
            delete query;
            if(params.mainThreadDispatching && (!params.dispatchMessageReceive || params.query_params.parallel_queries)) FreeQueryRequestMessage(&msg);
            return;
        }
    }

    //6) Update retry version and reset MetaData if new; skip if old/existing retry version.
    if(query->retry_version() > query_md->retry_version){     
        // Debug("Retrying Query [%lu:%lu] %s. Retry version: %d. Last version: %d. New req_id: %d", query->client_id(), query->query_seq_num(), query_md->query_cmd.c_str(), query->retry_version(), query_md->retry_version, msg.req_id());
        if(query->retry_version()>1) Warning("Investigate what is causing retry");
        query_md->ClearMetaData(queryId); //start new sync round
        query_md->req_id = msg.req_id();
         //Delete current missingTxns.   -- NOTE: Currently NOT necessary for correctness, because UpdateWaitingQueries checks whether retry version is still current. But good for garbage collection.
        queryMissingTxns.erase(QueryRetryId(queryId, query_md->retry_version, (params.query_params.signClientQueries && params.query_params.cacheReadSet && params.hashDigest)));
        query_md->retry_version = query->retry_version();
    }
    // else if(query->retry_version() == query_md->retry_version){
    //     // //Return if already received sync for the retry version, and sync is not waiting for query. (I.e. no need to process Query)
    //     // if(query_md->started_sync && !query_md->waiting_sync){
    //     //     delete query;
    //     //     if(params.mainThreadDispatching && (!params.dispatchMessageReceive || params.query_params.parallel_queries)) FreeQueryRequestMessage(&msg);
    //     //     return;
    //     // }
    //     //ignore if already processed query once (i.e. don't exec twice per version) 
    //     if(!newQuery){ 
    //         if(query_md->has_result){
                
    //             Panic("Duplicate query Request for current retry version"); //TODO: FIXME: Reply directly with result for current version. (or do nothing)
    //             delete query;
    //             if(params.mainThreadDispatching && (!params.dispatchMessageReceive || params.query_params.parallel_queries)) FreeQueryRequestMessage(&msg);
    //             return;
    //         }
    //         else{
    //             Panic("Duplicate query Request for current retry version, but no result"); //FIXME: Do Nothing.
    //             delete query;
    //             if(params.mainThreadDispatching && (!params.dispatchMessageReceive || params.query_params.parallel_queries)) FreeQueryRequestMessage(&msg);
    //             return;
    //         }
    //     } 
    // }
    // else{
    //     Panic("Requesting Query with outdating retry version");
    //     return;
    // }


     if(!query_md->has_query){ //Wait for command. E.g. when retry version arrives before original version
    //Note: Have updated retry versions and req_id's, but have not started execution
        //q automatically released
        if(params.mainThreadDispatching && (!params.dispatchMessageReceive || params.query_params.parallel_queries)) FreeQueryRequestMessage(&msg);
        return;
    }

    //EXECUTE:

    //7) Record whether current retry version uses optimistic tx-ids or not
    if(msg.has_optimistic_txid()) query_md->useOptimisticTxId = msg.optimistic_txid(); 
    Debug("Query using Optimistic Txid? %d", msg.optimistic_txid());

    //8) Process Query only if designated for reply; and if there is no Sync already waiting for this retry version
    query_md->designated_for_reply = msg.designated_for_reply();

    //If Eager Exec --> Skip sync and just execute on local state --> Call EagerExec function: Calls same exec as HandleSyncCallback (but without materializing snapshot) + SendQueryResult.
    if((query_md->designated_for_reply || params.query_params.cacheReadSet) && msg.has_eager_exec() && msg.eager_exec() && !query_md->waiting_sync){  //If waiting_sync => ProcessSync. (must be on eagerPlusSnapshot)
        //Note: If eager exec on && caching read set --> all must execute.
        Debug("ExecQueryEagerly. QueryId[%s]. Designated for reply? %d", BytesToHex(queryId, 16).c_str(), msg.designated_for_reply());
        ExecQueryEagerly(q, query_md, queryId);

        Debug("Finish ExecQueryEagerly. QueryId[%s]. Designated for reply? %d", BytesToHex(queryId, 16).c_str(), msg.designated_for_reply());
        if(params.mainThreadDispatching && (!params.dispatchMessageReceive || params.query_params.parallel_queries)) FreeQueryRequestMessage(&msg);

        return;
    }


    if(query_md->designated_for_reply && !query_md->waiting_sync){
        ProcessQuery(q, remote, query, query_md);
    }
    else{  //If not designated for reply, or sync is already waiting -> no need to process query. //TODO: In this case ideally shouldn't send Query separately at all -> Send it together with Sync and add to q_md then.
        delete query;
        if(query_md->waiting_sync){ //Wake waiting Sync
            UW_ASSERT(query_md->merged_ss_msg != nullptr);
            ProcessSync(q, *query_md->original_client, query_md->merged_ss_msg, &queryId, query_md);
        }
    }
    //q automatically released
    if(params.mainThreadDispatching && (!params.dispatchMessageReceive || params.query_params.parallel_queries)) FreeQueryRequestMessage(&msg);
   
}


void Server::ProcessPointQuery(const uint64_t &reqId, proto::Query *query, const TransportAddress &remote){

    Timestamp ts(query->timestamp()); 

    Debug("PointQuery[%lu:%lu] (client_id, query_seq) %s.", query->client_id(), query->query_seq_num(), query->query_cmd().c_str());

    proto::PointQueryResultReply *pointQueryReply = GetUnusedPointQueryResultReply(); 
    pointQueryReply->set_req_id(reqId);
    pointQueryReply->set_replica_id(id);

    //1) Execute
    proto::Write *write = pointQueryReply->mutable_write();
    const proto::CommittedProof *committedProof = nullptr;
    std::string enc_primary_key;  //TODO: Replace with query->primary_enc_key()
    write->set_key(query->primary_enc_key());
    UW_ASSERT(write->key().size() > 0);
   
    //If MVTSO: Read prepared, Set RTS
    if (occType == MVTSO) {
        //Sets RTS timestamp. Favors readers commit chances.
        Debug("Set up RTS for PointQuery[%lu:%lu]", query->client_id(), query->query_seq_num());
        SetRTS(ts, query->primary_enc_key());
    }

    // struct timespec ts_end2;
    // clock_gettime(CLOCK_MONOTONIC, &ts_end2);
    // uint64_t microseconds_end2 = ts_end2.tv_sec * 1000 * 1000 + ts_end2.tv_nsec / 1000;
    // auto duration2 = microseconds_end2 - microseconds_start;
    // Warning("PointQuery exec PRE duration: %d us. Q[%s] [%lu:%lu]", duration2, query->query_cmd().c_str(), query->client_id(), query->query_seq_num());
    
    if(simulate_point_kv){ //WARNING: FIXME: This is only Test/Profile logic -> TODO: Implement KV-store version directly. 
        //Read committed value.
         std::pair<Timestamp, Server::Value> tsVal;
        //find committed write value to read from
        Debug("Simulate point kv for key: %s", query->primary_enc_key().c_str());
        bool committed_exists = store.get(query->primary_enc_key(), ts, tsVal);
        UW_ASSERT(committed_exists);

        //Create dummy result.
        sql::QueryResultProtoBuilder queryResultBuilder;
        RowProto *row = queryResultBuilder.new_row();
            
        queryResultBuilder.add_column("key");
        queryResultBuilder.add_column("value");
                
        std::string key = query->primary_enc_key();
        queryResultBuilder.AddToRow(row, key); 

        //std::string value = "101"; //TODO: Make a value of the right size...
        std::string value = tsVal.second.val;
        queryResultBuilder.AddToRow(row, value); 
            
        write->set_committed_value(queryResultBuilder.get_result()->SerializeAsString()); // Note: This "clears" the builder

        tsVal.first.serialize(write->mutable_committed_timestamp());

        *pointQueryReply->mutable_proof() = *tsVal.second.proof;
        
    }
    

    if(!simulate_point_kv){
        table_store->ExecPointRead(query->query_cmd(), enc_primary_key, ts, write, committedProof);
    
   
        if(write->has_committed_value()){ 
            UW_ASSERT(committedProof); //proof must exist
            *pointQueryReply->mutable_proof() = *committedProof; //FIXME: Debug Seg here
        } 
    }

    // Notice("Query[%lu:%lu] read set. committed[%lu:%lu], prepared[%lu][%lu]", ts.getTimestamp(), ts.getID(), 
    //             write->committed_timestamp().timestamp(), write->committed_timestamp().id(),
    //             write->prepared_timestamp().timestamp(), write->prepared_timestamp().timestamp());
    
    if(TEST_QUERY) TEST_QUERY_f(write, pointQueryReply);

    delete query;
    

    //2) Sign & Send Reply
    TransportAddress *remoteCopy = remote.clone();

    //auto sendCB = [this, remoteCopy, readReply, c_id = msg.timestamp().id(), req_id=msg.req_id()]() {
    //Debug("Sent ReadReply[%lu:%lu]", c_id, req_id);  
    auto sendCB = [this, remoteCopy, pointQueryReply]() {
        this->transport->SendMessage(this, *remoteCopy, *pointQueryReply);
        delete remoteCopy;
        //Panic("stop here");
        FreePointQueryResultReply(pointQueryReply);
    }; 

     //TODO: This code does not sign a message if there is no value at all (or if verifyDeps == false and there is only a prepared, i.e. no committed, value) -- change it so it always signs. 
    if (params.validateProofs && params.signedMessages && (write->has_committed_value() || (params.verifyDeps && write->has_prepared_value()))) { 
        write = pointQueryReply->release_write();
         
        SignSendReadReply(write, pointQueryReply->mutable_signed_write(), sendCB);
    }
    else{
       
        sendCB();
    }
}

void Server::ProcessQuery(queryMetaDataMap::accessor &q, const TransportAddress &remote, proto::Query *query, QueryMetaData *query_md){

    query_md->executed_query = true;

    //Reply object.
     proto::SyncReply *syncReply = new proto::SyncReply(); //TODO: change to GetUnused
    syncReply->set_req_id(query_md->req_id);
    
    // 1) Find Snapshot
    proto::LocalSnapshot *local_ss = syncReply->mutable_local_ss();
   
    //Set LocalSnapshot
    syncReply->set_optimistic_tx_id(query_md->useOptimisticTxId);
    query_md->snapshot_mgr.InitLocalSnapshot(local_ss, query->query_seq_num(), query->client_id(), id, query_md->useOptimisticTxId);
    Debug("Use optimistic TxId for snapshot: %d", query_md->useOptimisticTxId);
    //TODO: perform exec and snapshot together if flag query_params.eagerPlusSnapshot is set.
    FindSnapshot(query_md, query);

    query_md->snapshot_mgr.SealLocalSnapshot(); //Remove duplicate ids and compress if applicable.
    q.release();
  

    // 3) Send Snapshot in SyncReply

    //sign & send reply.
    if (params.validateProofs && params.signedMessages) {
        Debug("Sign Query Sync Reply for Query[%lu:%lu:%d]", query->query_seq_num(), query->client_id(), query->retry_version());

     if(false) { //params.queryReplyBatch){
         TransportAddress *remoteCopy = remote.clone();
         auto sendCB = [this, remoteCopy, syncReply]() {
            this->transport->SendMessage(this, *remoteCopy, *syncReply); 
            delete remoteCopy;
            delete syncReply;
        };
         proto::LocalSnapshot *ls = syncReply->release_local_ss();
         MessageToSign(ls, syncReply->mutable_signed_local_ss(), [sendCB, ls]() {
            sendCB();
             Debug("Sent Signed Query Sync Snapshot for Query[%lu:%lu]", ls->query_seq_num(), ls->client_id());
            delete ls;
        });
     }
     else{ //realistically don't ever need to batch query sigs --> batching helps with amortized sig generation, but not with verificiation since client don't forward proofs.
        proto::LocalSnapshot *ls = syncReply->release_local_ss();
        if(params.signatureBatchSize == 1){
            SignMessage(ls, keyManager->GetPrivateKey(id), id, syncReply->mutable_signed_local_ss());
        }
        else{
            std::vector<::google::protobuf::Message *> msgs;
            msgs.push_back(ls);
            std::vector<proto::SignedMessage *> smsgs;
            smsgs.push_back(syncReply->mutable_signed_local_ss());
            SignMessages(msgs, keyManager->GetPrivateKey(id), id, smsgs, params.merkleBranchFactor);
        }
        this->transport->SendMessage(this, remote, *syncReply);
        Debug("Sent Signed Query Sync Snapshot for Query[%lu:%lu:%d]", ls->query_seq_num(), ls->client_id(), query->retry_version());
        delete syncReply;
        delete ls;
     }
    }
    else{
        this->transport->SendMessage(this, remote, *syncReply);
    }
   
    delete query;
}



////////////////////Handle Sync

 
void Server::HandleSync(const TransportAddress &remote, proto::SyncClientProposal &msg){
    // 1) Parse Message
     proto::MergedSnapshot *merged_ss;
     const std::string *queryId;
     std::string query_id;

    // 2) Compute query Digest 
     // needed locate the query state cached (e.g. local snapshot, intermediate read sets, etc.)
    if(params.query_params.signClientQueries && params.query_params.cacheReadSet){
        merged_ss = new proto::MergedSnapshot(); //TODO: replace with GetUnused
        merged_ss->ParseFromString(msg.signed_merged_ss().data());
        queryId = &merged_ss->query_digest();
    }
    else{
         //For now, can also index via (client id, query seq_num) pair. Just define an ordering function for query id pair. (In this case, unique string combination)
        merged_ss = msg.release_merged_ss();
        //query_id =  "[" + std::to_string(merged_ss->query_seq_num()) + ":" + std::to_string(merged_ss->client_id()) + "]";
        //queryId = &query_id;
        queryId = &merged_ss->query_digest();
    }
    Debug("Received Query Sync Proposal for Query[%lu:%lu:%d] (client:q-seq:retry-ver)", merged_ss->client_id(), merged_ss->query_seq_num(), merged_ss->retry_version());

    // if(merged_ss->retry_version() > 0){
    //     for(auto &[ts, replica_list] : merged_ss->merged_ts()){
    //         Notice("MergedSS contains TS_id: %lu", ts);
    //         for(auto &replica: replica_list.replicas()){
    //             Notice("   Replica list has replica: %d", replica);
    //         }
    //     }
    //     for(auto &[tx, replica_list] : merged_ss->merged_txns()){
    //         Notice("MergedSS contains TX_id: %s", BytesToHex(tx, 256).c_str());
    //         for(auto &replica: replica_list.replicas()){
    //             Notice("   Replica list has replica: %d", replica);
    //         }
    //     }
    // }

     //Only process if below watermark.
    clientQueryWatermarkMap::const_accessor qw;
    if(clientQueryWatermark.find(qw, merged_ss->client_id()) && qw->second >= merged_ss->query_seq_num()){
    //if(clientQueryWatermark[merged_ss->client_id()] >= merged_ss->query_seq_num()){
        Debug("IGNORE Sync Proposal for Query[%lu:%lu:%d] (client:q-seq:retry-ver). Below WM: %d", merged_ss->client_id(), merged_ss->query_seq_num(), merged_ss->retry_version(), qw->second);
        delete merged_ss; 
        if(params.mainThreadDispatching && (!params.dispatchMessageReceive || params.query_params.parallel_queries)) FreeSyncClientProposalMessage(&msg);
        return;
    }
    qw.release();


    // 3) Check whether retry version is still relevant
    queryMetaDataMap::accessor q;
    bool hasQuery = queryMetaData.find(q, *queryId);
    if(hasQuery){
        QueryMetaData *query_md = q->second;
        if(merged_ss->retry_version() < query_md->retry_version || (merged_ss->retry_version() == query_md->retry_version && query_md->started_sync)){
            Debug("Retry version for Sync Request Query[%lu:%lu:%d] (client:q-seq:retry-ver) is outdated (currently %d) OR started sync.", merged_ss->client_id(), merged_ss->query_seq_num(), merged_ss->retry_version(), query_md->retry_version);
             //TODO: if have result, return result
            //if(query_md->has_result){}    // Note: if already received sync for the retry version then result will be sent...
            delete merged_ss; 
            if(params.mainThreadDispatching && (!params.dispatchMessageReceive || params.query_params.parallel_queries)) FreeSyncClientProposalMessage(&msg);
            return;
        }
    }
    else{
        Debug("Have not received Query[%lu:%lu:%d] with Id: %s", merged_ss->client_id(), merged_ss->query_seq_num(), merged_ss->retry_version(), BytesToHex(*queryId, 16).c_str());
    }

     //4) Authenticate Client Proposal if applicable     
            //TODO: need it to be signed not only for read set equiv, but so that only original client can send this request. Authenticated channels may suffice.
    if(params.query_params.signClientQueries && params.query_params.cacheReadSet){ 
        if(!VerifyClientSyncProposal(msg, *queryId)){ // Does not really need to be parallelized, since query handling is probably already on a worker thread.
            delete merged_ss;
            if(params.mainThreadDispatching && (!params.dispatchMessageReceive || params.query_params.parallel_queries)) FreeSyncClientProposalMessage(&msg);
            Panic("Invalid client signature"); //Report/blacklist client.
            return;
        }
    }

    //5) Update meta data if new retry version
    //queryMetaDataMap::accessor q;
    //if(queryMetaData.insert(q, *queryId)){

    bool re_check = false;
            //Note: tbb find and insert are not atomic: Find does not take a lock if noQuery; before insert can claim lock another thread might add query. 
            //==> Must check whether query is the first -- and if not, must re-check (technically it's the first check since hasQuery must have been false) retry version and sync status
    if(!hasQuery){
        re_check = !queryMetaData.insert(q, *queryId);  
        if(!re_check){
            q->second = new QueryMetaData(merged_ss->query_seq_num(), merged_ss->client_id(), &params.query_params);
        }
    }
    QueryMetaData *query_md = q->second;    

    if(re_check){ //must re-check retry-version because tbb lookup and insert are not atomic...
        //Ignore if retry version old, or we already started sync for this retry version.
        if(merged_ss->retry_version() < query_md->retry_version || (merged_ss->retry_version() == query_md->retry_version && query_md->started_sync)){
            Debug("Retry version for Sync Request Query[%lu:%lu:%d] (client:q-seq:retry-ver) is outdated (currently %d) OR started sync.", merged_ss->client_id(), merged_ss->query_seq_num(), merged_ss->retry_version(), query_md->retry_version);
            delete merged_ss; 
            if(params.mainThreadDispatching && (!params.dispatchMessageReceive || params.query_params.parallel_queries)) FreeSyncClientProposalMessage(&msg);
            return;
        }
    }

    if(merged_ss->retry_version() > query_md->retry_version){ 
        query_md->ClearMetaData(*queryId);
        Debug("Sync Request Query[%lu:%lu:%d] (client:q-seq:retry-ver) update retry version. Clear Meta Data", merged_ss->client_id(), merged_ss->query_seq_num(), merged_ss->retry_version());
        query_md->req_id = msg.req_id();
          //Delete current missingTxns.   -- NOTE: Currently NOT necessary for correctness, because UpdateWaitingQueries checks whether retry version is still current. But good for garbage collection.
        queryMissingTxns.erase(QueryRetryId(*queryId, query_md->retry_version, (params.query_params.signClientQueries && params.query_params.cacheReadSet && params.hashDigest)));
        query_md->retry_version = merged_ss->retry_version();
    }

    query_md->started_sync = true;
    query_md->designated_for_reply = msg.designated_for_reply();

    Debug("Process Sync Proposal for Query[%lu:%lu:%d] (client:q-seq:retry-ver) or register waiting for Query", merged_ss->client_id(), merged_ss->query_seq_num(), merged_ss->retry_version());

    if(query_md->has_query){
        ProcessSync(q, remote, merged_ss, queryId, query_md);
    }
    else{ //Wait for Query to arrive first. (With FIFO channels Query should arrive first; but with multithreading, sync might be processed first.)
        query_md->RegisterWaitingSync(merged_ss, remote); //query_md -> waiting_sync = true
    }
   
    if(params.mainThreadDispatching && (!params.dispatchMessageReceive || params.query_params.parallel_queries)) FreeSyncClientProposalMessage(&msg);

}


void Server::ProcessSync(queryMetaDataMap::accessor &q, const TransportAddress &remote, proto::MergedSnapshot *merged_ss, const std::string *queryId, QueryMetaData *query_md) { 

    query_md->merged_ss_msg = merged_ss; 

    bool fullyMaterialized = true;

    //1) Determine all missing transactions 
    std::map<uint64_t, proto::RequestMissingTxns> replica_requests = {};

    std::string query_retry_id = QueryRetryId(*queryId, query_md->retry_version, (params.query_params.signClientQueries && params.query_params.cacheReadSet && params.hashDigest)); 
    Debug("Query id: %s. Query retry id: %s", BytesToHex(*queryId, 16).c_str(), BytesToHex(query_retry_id, 24).c_str());
    
    queryMissingTxnsMap::accessor qm;
    bool first_qm = queryMissingTxns.insert(qm, query_retry_id); //Note: ClearMetaData: Deletes previous retry_version.
    UW_ASSERT(first_qm); //ProcessSync should never be called twice for one retry version.

    // In SetWaiting -> add missing to qm->second. (pass qm->second as arg.)
    std::unordered_map<std::string, uint64_t> &missing_txns = qm->second.missing_txns; //query_md->missing_txns;  //Set of TX waiting to be materialized
    std::unordered_map<uint64_t, uint64_t> &missing_ts = qm->second.missing_ts; //querHandleReqy_md->missing_ts;
    // If missing empty after checking snapshot -> erase again
    
     //Note: SetWaiting needs to pass query_retry_id, not queryId.
    query_md->is_waiting = true; //Note: Tentatively needs to set first before registering waiters. If not actually waiting, correct after!
    //TODO: is _waiting_ still necessary now that Update orchestration is on a per retry basis?

    //Using normal tx-id
    if(!query_md->useOptimisticTxId){
         //txn_replicas_pair
        for(auto const &[tx_id, replica_list] : merged_ss->merged_txns()){
            Debug("Snapshot for Query Sync Proposal[%lu:%lu:%d] contains tx_id [%s]", merged_ss->query_seq_num(), merged_ss->client_id(), merged_ss->retry_version(), BytesToHex(tx_id, 16).c_str());
            //query_md->merged_ss.insert(tx_id); //store snapshot locally. //DEPRECATED -->  now just storing the merged_ss_msg directly
            
            //Check whether replica has the txn & whether it is already materialized
            fullyMaterialized &= CheckPresence(tx_id, query_retry_id, query_md, replica_requests, replica_list, missing_txns); 
        }

        //if using UTF8_safe_mode then read the tx_ids from merged_txns_utf, but move them into the map so the remainingcode works as intended
        for(auto &tx_data : merged_ss->merged_txns_utf()){
            //move over into map
            (*merged_ss->mutable_merged_txns())[tx_data.txn()];

             Debug("Snapshot for Query Sync Proposal[%lu:%lu:%d] contains tx_id [%s]", merged_ss->query_seq_num(), merged_ss->client_id(), merged_ss->retry_version(), BytesToHex(tx_data.txn(), 16).c_str());    
            //Check whether replica has the txn & whether it is already materialized
            fullyMaterialized &= CheckPresence(tx_data.txn(), query_retry_id, query_md, replica_requests, tx_data.replica_list(), missing_txns); 
        }
        merged_ss->clear_merged_txns_utf();
    }
    //else: Using optimistic tx-id (i.e. TS)
    if(query_md->useOptimisticTxId){
        query_md->snapshot_mgr.OpenMergedSnapshot(merged_ss); //Decompresses if applicable 
        for(auto const &[ts_id, replica_list] : merged_ss->merged_ts()){
            Debug("Snapshot for Query Sync Proposal[%lu:%lu:%d] contains ts_id [%lu]", merged_ss->query_seq_num(), merged_ss->client_id(), merged_ss->retry_version(), ts_id);
            
            //Check whether replica has the txn & whether it is already materialized
            fullyMaterialized &= CheckPresence(ts_id, query_retry_id, query_md, replica_requests, replica_list, missing_txns, missing_ts);
        }
    }
    
    //Update queryMissingTxns meta data.
    if(!missing_txns.empty() || !missing_ts.empty()){
        qm->second.query_id = *queryId;  //Needed to lookup QueryMetaData upon waking.
        qm->second.retry_version = query_md->retry_version;
    } 
    else{ //missing_txns.empty() && missing_ts.empty()
        Debug("No missing txns for Query Retry id %s", BytesToHex(query_retry_id, 16).c_str());
        queryMissingTxns.erase(qm); //No missing transactions -> no need to wait.
    }
    qm.release();

    //2)  //Request any missing transactions (via txid) & add to state

    //NOTE: May want to avoid redundant sync of same Tx-id.
    //TODO: Check waitingQueries map: If it already has an entry for a tx-id, then we DONT need to request it again.. Already in flight. ==> Just update the waitingList
    //Currently processing/verifying duplicate supply messages.
    //HOWEVER: Correlating sync request messages can cause byz independence issues. Different clients could include different f+1 replicas to fetch from (cannot tell which are honest/byz)
            //Can solve this by recording in WaitingQueries from which replicas we requested  //Or for simplicity we can send to all.
  
    //If no missing_txn ==> already fully synced. Exec callback direclty
    if(!replica_requests.empty()){
    //if there are missng txn, i.e. replica_requests not empty ==> send out sync requests.
        //query_md->is_waiting = true; //Note: If query is waiting, but (byz) client supplied wrong/insufficient replicas to sync from ==> query loses liveness.
        Debug("Sync State incomplete for Query[%lu:%lu:%d]", merged_ss->query_seq_num(), merged_ss->client_id(), merged_ss->retry_version()); 
        for(auto const &[replica_idx, replica_req] : replica_requests){
            if(replica_idx == idx) Panic("Should never request from self");
            transport->SendMessageToReplica(this, groupIdx, replica_idx, replica_req);
            Debug("Replica %d Request Data Sync from replica %d", replica_req.replica_idx(), replica_idx); 
            // for(auto const& txn : replica_req.missing_txn()){ std::cerr << "Requesting txn : " << (BytesToHex(txn, 16)) << std::endl;}
        }
        stats.Increment("RequestMissing");
    }

    Debug("Query[%s][ver:%d] has been fullyMat? %d", BytesToHex(*queryId, 16).c_str(), query_md->retry_version, fullyMaterialized);
    
    //else: if no missing Txns & all already materialized   
    //Note: fullyMaterialized == missing_txns.empty() && missing_ts.empty()
    if(fullyMaterialized){
        query_md->is_waiting = false;
        return HandleSyncCallback(q, query_md, *queryId);
    }


    //Note: If query is waiting, but (byz) client supplied wrong/insufficient replicas to sync from ==> query loses liveness.
    Debug("Query[%s][ver:%d] is waiting!", BytesToHex(*queryId, 16).c_str(), query_md->retry_version);

    q.release();

    if(TEST_MATERIALIZE) TEST_MATERIALIZE_f();
       
    //delete merged_ss; // ==> Deleting only upon ClearMetaData or delete query_md 
    return;
}

//Note: WARNING: must be called while holding a lock on query_md. 
void Server::HandleSyncCallback(queryMetaDataMap::accessor &q, QueryMetaData *query_md, const std::string &queryId){

    Debug("Sync complete for Query[%lu:%lu]. Starting Execution", query_md->query_seq_num, query_md->client_id);
    query_md->is_waiting = false;
    
    // 1) Materialize Snapshot -- do nothing! already done.
        //Invariant: HandleSyncCallback is only called if snapshot is fully materialized. I.e. all txns in snapshot must be either applied to table_store or aborted

    if(TEST_READ_FROM_SS) TEST_READ_FROM_SS_f();

    // 2) Execute Query & Construct Read Set
    //Execute Query -- Go through store, and check if latest tx in store is present in syncList. If it is missing one (committed) --> reply EarlyAbort (tx cannot succeed). 
    //If prepared is missing, ignore, skip to next
    // Build Read Set while executing; Add dependencies on demand as we observe uncommitted txn touched. read set = map from key-> versions  //Note: Convert Timestamp to TimestampMessage
 
    QueryReadSetMgr queryReadSetMgr(query_md->queryResultReply->mutable_result()->mutable_query_read_set(), groupIdx, query_md->useOptimisticTxId); 

    //std::string result(ExecQuery(queryReadSetMgr, query_md, true)); //TODO: Call the Read from Snapshot interface.
    query_md->queryResultReply->mutable_result()->set_query_result(ExecQuery(queryReadSetMgr, query_md, true));
    query_md->has_result = true; 

    Debug("Finished Execution for Query[%lu:%lu].", query_md->query_seq_num, query_md->client_id);
   
    //Note: Blackbox might do multi-replica coordination to compute result and full read-set (though read set can actually be reported directly by each shard...)
    //Client waits to receive SyncReply from all shards ==> with read set, or read set hash. ==> in Tx_manager (marked by query) reply also include the result
        //Always callback from shardclient to client, but only call-up from client to app if a) result has been received, b) all shards replied with read-set (or read-set hash)
        //-- want to do this so that Exec can be a better blackbox: This way data exchange might just be a small intermediary data, yet client learns full read set. 
            //In this case, read set hash from a shard is not enough to prove integrity to another shard (since less data than full read set might be exchanged)

    //Clear set snapshot. Could've been set by EagerPlusSnapshot path.
    query_md->queryResultReply->mutable_result()->clear_local_ss();

   
    if(TEST_FAIL_QUERY && query_md->retry_version == 0){ //Test to simulate a retry once.
        FailQuery(query_md);
        TEST_FAIL_QUERY = false;
    }
    else{
        query_md->failure = false;
    
        Debug("Send Query Reply. Query[%lu:%lu]", query_md->query_seq_num, query_md->client_id);
        SendQueryReply(query_md);
            //query_md->queryResultReply->mutable_result()->set_query_result(result);
        //TODO: If not query_manager => don't need to send the result. Only need read set
    }
   

    uint64_t retry_version = query_md->retry_version;
    q.release();

     //After executing and caching read set -> Try to wake possibly subscribed transaction that has started to prepare, but was blocked waiting on it's cached read set.
    if(params.query_params.cacheReadSet) wakeSubscribedTx(queryId, retry_version); //TODO: Instead of passing it along, just store the queryId...
}



void Server::SendQueryReply(QueryMetaData *query_md){ 


    Debug("SendQuery Reply for Query[%lu:%lu:%lu]. ", query_md->query_seq_num, query_md->client_id, query_md->retry_version);
    
    proto::QueryResultReply *queryResultReply = query_md->queryResultReply;
    proto::QueryResult *result = queryResultReply->mutable_result();
    proto::ReadSet *query_read_set;

    //proto::LocalDeps *query_local_deps; //Deprecated --> made deps part of read set
    Debug("QueryResult[%lu:%lu:%lu]: %s", query_md->query_seq_num, query_md->client_id, query_md->retry_version, BytesToHex(result->query_result(), 16).c_str());
    
    // 3) Cache read set
    bool testing_hash = false; //note, if this is on, the client will crash since it expects a read set but does not get one.
    if(testing_hash || params.query_params.cacheReadSet) CacheReadSet(query_md, result, query_read_set);



    //4) Create Result reply --  // only send if chosen for reply
    if(!query_md->designated_for_reply) return;

    //FIXME: set these directly when instantiating.
    result->set_query_seq_num(query_md->query_seq_num); 
    result->set_client_id(query_md->client_id); 
    result->set_replica_id(id);
    result->set_query_gen_id(QueryGenId(query_md->query_cmd, query_md->ts));
    
    queryResultReply->set_req_id(query_md->req_id); //this implicitly captures retry-version

    //5) (Sign and) send reply 

     if (params.validateProofs && params.signedMessages) {
        //Debug("Sign Query Result Reply for Query[%lu:%lu]", query_reply->query_seq_num(), query_reply->client_id());
        Debug("Sign Query Result Reply for Query[%lu:%lu]", result->query_seq_num(), result->client_id());

        result = queryResultReply->release_result();   //Temporarily release result in order to sign.

        //TODO: FIXME: If want to be able to dispatch it again, need to somehow keep lock longer...  This current version is not safe.
        if(false) { //params.queryReplyBatch){
            TransportAddress *remoteCopy = query_md->original_client->clone();
            auto sendCB = [this, remoteCopy, queryResultReply]() {
                this->transport->SendMessage(this, *remoteCopy, *queryResultReply); 
                delete remoteCopy;
                //delete queryResultReply;
            };
          
             //TODO: if this is already called from a worker, no point in dispatching it again. Add a Flag to MessageToSign that specifies "already worker"
            MessageToSign(result, queryResultReply->mutable_signed_result(), [this, sendCB, result, queryResultReply, query_read_set]() mutable { //query_local_deps
                sendCB();
                 Debug("Sent Signed Query Result for Query[%lu:%lu]", result->query_seq_num(), result->client_id());

                if(params.query_params.cacheReadSet){ //Restore read set and deps to be cached
                    result->set_allocated_query_read_set(query_read_set);
                    //result->set_allocated_query_local_deps(query_local_deps);
                } 
                queryResultReply->set_allocated_result(result);  //NOTE: This returns the unsigned result, including the readset. If we want to cache the signature, would have to change this code.
               
                //delete res;
            });

        }
        else{ //realistically don't ever need to batch query sigs --> batching helps with amortized sig generation, but not with verificiation since client don't forward proofs.


            if(params.signatureBatchSize == 1){
                SignMessage(result, keyManager->GetPrivateKey(id), id, queryResultReply->mutable_signed_result());
            }
            else{
                std::vector<::google::protobuf::Message *> msgs;
                msgs.push_back(result);
                std::vector<proto::SignedMessage *> smsgs;
                smsgs.push_back(queryResultReply->mutable_signed_result());
                SignMessages(msgs, keyManager->GetPrivateKey(id), id, smsgs, params.merkleBranchFactor);
            }

            this->transport->SendMessage(this, *query_md->original_client, *queryResultReply);
            Debug("Sent Signed Query Result for Query[%lu:%lu]", result->query_seq_num(), result->client_id()); 
            //delete queryResultReply;
            
            if(params.query_params.cacheReadSet){ //Restore read set and deps to be cached
                result->set_allocated_query_read_set(query_read_set);
                //result->set_allocated_query_local_deps(query_local_deps);
            } 
            queryResultReply->set_allocated_result(result);  //NOTE: This returns the unsigned result, including the readset. If we want to cache the signature, would have to change this code.
            //delete res;
        }
    }
    else{
        this->transport->SendMessage(this, *query_md->original_client, *queryResultReply);
        Debug("Sent Query Result (no sigs) for Query[%lu:%lu]", result->query_seq_num(), result->client_id());
        //Note: In this branch result is still part of queryResultReply; thus it suffices to only allocate back to result.
        if(params.query_params.cacheReadSet){ //Restore read set and deps to be cached
            result->set_allocated_query_read_set(query_read_set);
            //result->set_allocated_query_local_deps(query_local_deps);
        } 

    }

    if(TEST_READ_SET) TEST_READ_SET_f(result);
   
    return;

}

void Server::CacheReadSet(QueryMetaData *query_md, proto::QueryResult *&result, proto::ReadSet *&query_read_set){
    //Generate unique hash over Read Set (hash-chain or Merkle Tree), (optionally can also make it be over result, query id)
    //4) If Caching Read Set: Buffer Read Set (map: query_digest -> <result_hash, read set>) ==> implicitly done by storing read set + result hash in query_md 

    Debug("Cache read set for Query[%lu:%lu:%lu]. ", query_md->query_seq_num, query_md->client_id, query_md->retry_version);
    try {
        std::sort(result->mutable_query_read_set()->mutable_read_set()->begin(), result->mutable_query_read_set()->mutable_read_set()->end(), sortReadSetByKey); 
    }
    catch(...) {
        Panic("Trying to send QueryReply with two different reads for the same key");
    }

    Debug("Query[%lu:%lu:%lu] has %d read preds: ", query_md->query_seq_num, query_md->client_id, query_md->retry_version, result->query_read_set().read_predicates_size());
    
    //Note: Sorts by key to ensure all replicas create the same hash. (Note: Not necessary if using ordered map)
    result->set_query_result_hash(generateReadSetSingleHash(result->query_read_set()));
    //Temporarily release read-set and deps: This way we don't send it. Afterwards, re-allocate it. This trick avoid copying.
    if(query_md->designated_for_reply)  query_read_set = result->release_query_read_set();
    //query_local_deps = result->release_query_local_deps();
    Debug("Read-set hash: %s", BytesToHex(result->query_result_hash(), 16).c_str());
    
    //query_md->result_hash = std::move(generateReadSetSingleHash(query_md->read_set));  
    //query_md->result_hash = std::move(generateReadSetMerkleRoot(query_md->read_set, params.merkleBranchFactor)); //by default: merkleBranchFactor = 2 ==> might want to use flatter tree to minimize hashes.
                                                                                                    //TODO: Can avoid hashing leaves by making them unique strings? "[key:version]" should do the trick?
    //Debug("Read-set hash: %s", BytesToHex(query_md->result_hash, 16).c_str());
}

////////////////////////// Replica To Replica Sync exchange ////////////////////////////////

void Server::HandleRequestTx(const TransportAddress &remote, proto::RequestMissingTxns &req_txn){

     Debug("   Received RequestMissingTxn from replica %d. MissingTxs: %d, MissingTS: %d", req_txn.replica_idx(), req_txn.missing_txn_size(), req_txn.missing_txn_ts_size()); 

    //1) Parse Message
    proto::SupplyMissingTxns supplyMissingTxn; // = new proto::SupplyMissingTxns();
    proto::SupplyMissingTxnsMessage supply_txn; // = supplyMissingTxn->mutable_supply_txn(); //TODO: change to unused operation.
    supply_txn.set_replica_idx(idx);
    //*supplyMissingTxn.mutable_supply_txn() = supply_txn;
    
     if(idx == req_txn.replica_idx()) Panic("Received HandleMissingTx from self");
    
   
    //2) Check if requested tx present local (might not be if a byz sent request; or a byz client falsely told an honest replica which replicas have tx)
            // TODO: If using optimistic TX-id: map from optimistic ID to real txid to find Txn. (if present)  --> with optimistic ones we cant distinguish whether the sync client was byz and falsely suggested replica has tx
    
     //3) If present, reply to replica with it; If not, reply that it is not present (reply explicitly to catch byz clients). --> Note: byz replica could always report this; 
            // to avoid false reports would need to include signed replica snapshot vote
            // (if we want to avoid byz replicas falsely requesting, then clients would also need to include signed snapshot vote. and we would have to forward it here to...)
            //can log requests, so a byz can request at most once (which is legal anyways)

    //Check requested Tx-ids
    for(auto const &txn_id : req_txn.missing_txn()){
        Debug("Replica %d is requesting txn_id [%s]", req_txn.replica_idx(), BytesToHex(txn_id, 16).c_str());
        // proto::TxnInfo &txn_info = (*supply_txn.mutable_txns())[txn_id];
        // CheckLocalAvailability(txn_id, txn_info);
        // //CheckLocalAvailability(txn_id, supply_txn);

        //Re-factor to use repeated.
        proto::TxnInfo *txn_info = supply_txn.add_txns();
        txn_info->set_txn_id(txn_id);
        CheckLocalAvailability(txn_id, *txn_info);
    }

    //Check requested optimistic Tx-ids (TS)
    for(auto const &ts_id : req_txn.missing_txn_ts()){
        Debug("Replica %d is requesting ts_id [%lu]", req_txn.replica_idx(), ts_id);
        //Translate to tx-id if available -- else, reply stop and reply invalid
         ts_to_txMap::const_accessor t;
        bool hasTx = ts_to_tx.find(t, ts_id);//.. find in map. If yes, add tx_id to merged_txns. If no, try to sync on TS.
        if(!hasTx){
            Panic("Replica does not have txn-id for requested timestamp %lu. Shouldn't happen in testing", ts_id);
            if(params.mainThreadDispatching && (!params.dispatchMessageReceive || params.query_params.parallel_queries)) FreeRequestTxMessage(&req_txn);
            return;
        }
        const std::string &txn_id = t->second;

        proto::TxnInfo &txn_info = (*supply_txn.mutable_txns_ts())[ts_id];
        txn_info.set_txn_id(txn_id);
        CheckLocalAvailability(txn_id, txn_info);
        //CheckLocalAvailability(txn_id, supply_txn, true);
       
    }

    //4) Use MAC to authenticate own reply
    if(params.query_params.signReplicaToReplicaSync){
        const std::string &msgData = supply_txn.SerializeAsString();
        proto::SignedMessage *signedMessage = supplyMissingTxn.mutable_signed_supply_txn();
        signedMessage->set_data(msgData);
        signedMessage->set_process_id(idx);
        signedMessage->set_signature(crypto::HMAC(msgData, sessionKeys[req_txn.replica_idx()]));
    }
    else{
        *supplyMissingTxn.mutable_supply_txn() = std::move(supply_txn);
    }
   
    //5) Send reply.

    Debug("Trying to Send SupplyTx to replica %d", req_txn.replica_idx()); 
    transport->SendMessageToReplica(this, groupIdx, req_txn.replica_idx(), supplyMissingTxn);

    if(params.mainThreadDispatching && (!params.dispatchMessageReceive || params.query_params.parallel_queries)) FreeRequestTxMessage(&req_txn);
    return;
}

void Server::CheckLocalAvailability(const std::string &txn_id, proto::TxnInfo &txn_info){
//void Server::CheckLocalAvailability(const std::string &txn_id, proto::SupplyMissingTxnsMessage &supply_txn, bool sync_on_ts){

        //proto::TxnInfo &tx_info = (*supply_txn.mutable_txns())[txn_id];

        //1) If committed attatch certificate
        auto itr = committed.find(txn_id);
        if(!TEST_PREPARE_SYNC && itr != committed.end()){
            //copy committed Proof from committed list to map of tx replies -- note: committed proof contains txn.
            proto::CommittedProof *commit_proof = itr->second;
            //proto::TxnInfo &tx_info = (*supply_txn.mutable_txns())[txn_id];
            //*tx_info.mutable_commit_proof() = *commit_proof;

           *txn_info.mutable_commit_proof() = *commit_proof;
            //*(*supply_txn.mutable_txns())[txn_id].mutable_commit_proof() = *commit_proof;
            Debug("Supplying committed txn_id %s", BytesToHex(txn_id, 16).c_str());
            return; //continue;
        }

        //2) if abort --> mark query for abort and reply. 
        auto itr2 = aborted.find(txn_id);
        if(itr2 != aborted.end()){
            //proto::TxnInfo &txn_info = (*supply_txn.mutable_txns())[txn_id];
            txn_info.set_abort(true);
            *txn_info.mutable_abort_proof() = writebackMessages[txn_id];
            return;
        }
        //Corner case: If replica voted prepare, but is now abort, what should happen? Should query ReportFail? 
        //Or should query just go through without this tx ==> The latter. After all, it is correct to ignore.

        //3) if Prepared //TODO: check for prepared first, to avoid sending unecessary certs?

        //Note: Should we be checking for ongoing (irrespective of prepared or not)? ==> and include signature if necessary. 
        //==> No: A correct replica (instructed by a correct client) will only request the transaction from replicas that DO have it prepared. So checking prepared is enough -- don't need to check ongoing

        preparedMap::const_accessor a;
        bool hasPrepared = prepared.find(a, txn_id);
        if(hasPrepared){
            //copy txn from prepared list to map of tx replies.
            proto::Phase1 *p1 = txn_info.mutable_p1();
            //proto::Phase1 *p1 = (*supply_txn.mutable_txns())[txn_id].mutable_p1();
            const proto::Transaction *txn = (a->second.second);

            if(params.signClientProposals){
                 p1MetaDataMap::const_accessor c;
                bool hasP1Meta = p1MetaData.find(c, txn_id);
                if(!hasP1Meta) Panic("Tx %s is prepared but has no p1MetaData entry (should be created during ProcessProposal-VerifyClientProposal)", BytesToHex(txn_id, 16).c_str());  
                if(!c->second.hasSignedP1) Panic("No Signed Txn cached for txn %s", BytesToHex(txn_id, 16).c_str());
                //NOTE: P1 hasP1 (result) might not be set yet, but signed_txn has been buffered.
                //Note: signature in p1MetaData is only removed AFTER prepared is removed. Thus signed message must be present when we access p1Meta while holding prepared lock.
               //if(c->second.signed_txn == nullptr) Panic("Signed txn is nullptr");
                *p1->mutable_signed_txn() = *(c->second.signed_txn); 
                // *p1->mutable_signed_txn()->mutable_data() = c->second.signed_txn->data();
                // *p1->mutable_signed_txn()->mutable_signature() = c->second.signed_txn->signature();
                // p1->mutable_signed_txn()->set_process_id(c->second.signed_txn->process_id());
                c.release();
            }
            else{
                *p1->mutable_txn() = *txn;
            }
            p1->set_req_id(0);
            p1->set_crash_failure(false);
            p1->set_replica_gossip(true);  //ensures that no RelayP1 is sent, and no original client is registered for reply upon completion.
            a.release();

             Debug("Supplying prepared txn_id [%s]", BytesToHex(txn_id, 16).c_str());
            return; //continue;
        }
        a.release();

        //4) if neither --> Mark invalid return, and report byz client
        txn_info.set_invalid(true);
        //(*supply_txn.mutable_txns())[txn_id].set_invalid(true);
        Debug("Falsely requesting tx-id [%s] which replica %lu does not have committed or prepared locally", BytesToHex(txn_id, 16).c_str(), id);
        Panic("Testing Sync: Replica does not have tx.");
        return; //break; //return;  //For debug purposes sending invalid reply.
}


void Server::HandleSupplyTx(const TransportAddress &remote, proto::SupplyMissingTxns &msg){

    //Note: this will be called on a worker thread -- directly call Query handler callback.
    //Note: SupplyTx can wake up multiple concurrent queries that are waiting on the same tx  
    //TODO: Currently just waiting indefinitely. --> how to GC? => Switch to wait for at most f+1 replies? 


    // 1) Parse Message & Check 
    proto::SupplyMissingTxnsMessage *supply_txn;
    // 2) Check MAC authenticator
    if(params.query_params.signReplicaToReplicaSync){
        if(!crypto::verifyHMAC(msg.signed_supply_txn().data(), msg.signed_supply_txn().signature(), sessionKeys[msg.signed_supply_txn().process_id() % config.n])){
            Debug("Authentication failed for SupplyTxn received from replica %lu.", msg.signed_supply_txn().process_id());
            return;
        }
        supply_txn = new proto::SupplyMissingTxnsMessage();
        supply_txn->ParseFromString(msg.signed_supply_txn().data());
    }
    else{
        supply_txn = msg.mutable_supply_txn();
    }

    Debug("\n Received Supply Txns from Replica %d with %d transactions", supply_txn->replica_idx(), supply_txn->txns().size());

    //TODO: To support TS sync: Loop over ts, tx map. Re-factor loop contents into separate function. -- for sync on Tx-id, also add timestamp (happens in ongoing -- ideally report client that equived)
     //TODO: (Request sends TS, Supply replies with map from TS to TX)
                    //TODO: Turn supply map into repeated TxnInfo; move tx-id into txninfo; add map from Ts to txinfo
                    // OR: Easier: Add optional tx-id field to tx-info. add map.
    bool stop = false;
    // for(auto &[txn_id, txn_info] : *supply_txn->mutable_txns()){
    //     Debug("Trying to locally apply tx-id: [%s]", BytesToHex(txn_id, 16).c_str());
    //     ProcessSuppliedTxn(txn_id, txn_info, stop);   
    //     if(stop) break;
    // }
    //Re-factor from map to repeated field
    for(auto &txn_info : *supply_txn->mutable_txns()){
        UW_ASSERT(txn_info.has_txn_id());
        const std::string &txn_id = txn_info.txn_id();
        Debug("Trying to locally apply tx-id: [%s]", BytesToHex(txn_id, 16).c_str());
        ProcessSuppliedTxn(txn_id, txn_info, stop);   
        if(stop) break;
    }

    for(auto &[txn_ts, txn_info] : *supply_txn->mutable_txns_ts()){
        UW_ASSERT(txn_info.has_txn_id());
        const std::string &txn_id = txn_info.txn_id();
        Debug("Trying to locally apply tx-id: [%s] from ts-id [%lu]", BytesToHex(txn_id, 16).c_str(), txn_ts);
        ProcessSuppliedTxn(txn_id, txn_info, stop);   
        if(stop) break;
    }
    

    if(params.query_params.signReplicaToReplicaSync) delete supply_txn;
    if(params.mainThreadDispatching && !params.dispatchMessageReceive) FreeSupplyTxMessage(&msg);
    return;

}

//Consider moving this to servertools?
//NOTE: All calls to UpdateWaiting must wake waitingQueriesTS too!
void Server::ProcessSuppliedTxn(const std::string &txn_id, proto::TxnInfo &txn_info, bool &stop){
     //check if locally committed; if not, check cert and apply

    Debug("Received supply for txn[%s]", BytesToHex(txn_id, 16).c_str());

    // bool first_supply = false;
    // if(simulate_inconsistency){
    //     auto [_, first] = supplied_sync.insert(txn_id);
    //     first_supply = first;
    // }
    auto [_, first] = supplied_sync.insert(txn_id);
    if(!first){
        Debug("Already processed supply for txn[%s]", BytesToHex(txn_id, 16).c_str());
        return;
    }
        
    auto itr = committed.find(txn_id);
    if(!TEST_SYNC && itr != committed.end()){
        Debug("Already have committed tx-id: [%s]", BytesToHex(txn_id, 16).c_str());
        //UpdateWaitingQueries(txn_id);   //Note: Already called in Commit function --> Guarantees that queries wake up as soon as Commit happens, not once Supply happens
        //(RESOLVED) Cornercase: But: Couldve been added to commit only after checking for presence but before being added to WaitingTxn --> thus must wake again. 
        return;
    }
    //check if locally aborted; if so, no point in syncing on txn: --> could mark this and reply to client with query fail (include tx that has abort vote + proof 
    //--> client can confirm that this is part of snapshot).. query is doomed to fail.
    //BETTER: Just ignore the txn for materialization. After all, not reading from an aborted tx is the serializable decision. 
        //Note: As a result of ignoring, some replicas may read it, and some won't ==> this might fail sync, but that's ok: Just retry.

    auto itr2 = aborted.find(txn_id);
    if(itr2 != aborted.end()){
            Debug("Already have aborted tx-id: %s", BytesToHex(txn_id, 16).c_str());
        //Don't need to wait on this txn (Abort call UpdatedWaitingQueries)
        //UpdateWaitingQueries(txn_id); 
                // Alternatively: Could Mark all waiting queries as doomed. FailWaitingQueries(txn_id);
        return;
    }


    //Check if other replica had aborted (If so, exclude this tx from snapshot; Alternatively could fail sync eagerly, but that seems unecessary)
    if(txn_info.abort()){ 
        stats.Increment("Supply_abort", 1);
        Debug("Replica indicates that previously prepared Tx is now aborted. tx-id: %s", BytesToHex(txn_id, 16).c_str());
        UW_ASSERT(txn_info.has_abort_proof());
        // Only trust the abort vote if there is a proof attached, or if there is f+1 supply messages that say the same...

        auto f = [this, msg= txn_info.release_abort_proof()](){
            const TCPTransportAddress dummy_remote = TCPTransportAddress(sockaddr_in());
            HandleWriteback(dummy_remote, *msg);
            if(!params.multiThreading && (!params.mainThreadDispatching || params.dispatchMessageReceive)) FreeWBmessage(msg); //I.e. if ReceiveMsg would not be allocating (See ManageDispatchWriteback)
            return (void*) true;
        };

        if(!params.query_params.parallel_queries || !params.mainThreadDispatching){  //TODO: Realistically: Always running with multiThreading now. Just configure parallel_queries?
            //If !parallel_queries.  ==> both Query and Writeback follow the same dispatch rules (either both on network or both on main)
            //if parallel_queries && !mainThreadDispatching  => parallel_queries has no effect. Thus both Query and Writeback follow same dispatch rules (depends on dispatchMessageReceive)
            f();
        }
        else{ //params.mainThreadDispatching = true && parallel_queries == true ==> Query is on worker; writeback is on main
                transport->DispatchTP_main(std::move(f));
        }
        //Dispatch HandleWriteback  //(RESOLVED -- made atomic) Cornercase: Tx was already written back (but only after checking for presence); but didn't wake Waiting (because it wasn't set yet).
        // Calling Writeback again here will short-circuit and not wait. ==> Can fix this by switching order of aborted check...
        
        //UpdateWaitingQueries(txn_id); //Don't need to wait on this txn. 
        return;
    }

      ///Note (FIXME:?): A Replica that has a prepare but receives an abort proof might want to remove the tx from the snapshot. TODO: For this reason, may want to move prepare check after abort proof check.



    //Check if other replica supplied commit
    if(txn_info.has_commit_proof()){   
         stats.Increment("Supply_commit", 1);
         //TODO: it's possible for the txn to be in process of committing while entering this branch; that's fine safety wise, but can cause redundant verification. Might want to hold a lock to avoid (if it happens)
        Debug("Trying to commit tx-id: [%s]", BytesToHex(txn_id, 16).c_str());
        proto::CommittedProof *proof = txn_info.release_commit_proof();

        bool valid;
        if (proof->txn().client_id() == 0UL && proof->txn().client_seq_num() == 0UL) {
            // Genesis tx are valid by default. TODO: this is unsafe, but a hack so that we can bootstrap a benchmark without needing to write all existing data with transactions
            // Note: Genesis Tx will NEVER by exchanged by sync since by definition EVERY replica has them (and thus will never request them) -- this branch is only used for testing.
            valid = true;
            Debug("Accepted Genesis tx-id: [%s]", BytesToHex(txn_id, 16).c_str());
            UpdateWaitingQueries(txn_id); //Does not seem necessary for Genesis, but shouldn't hurt
            UpdateWaitingQueriesTS(MergeTimestampId(proof->txn().timestamp().timestamp(), proof->txn().timestamp().id()), txn_id);
        }
        else{
            //Confirm that replica supplied correct transaction.     //TODO: Note: Since one should do this anyways, there is no point in storing txn_id as part of supply message.
                // TODO: for the hack of storing digest in txn, think if it's safe to do server side...
                std::string tempDigest = TransactionDigest(proof->txn(), params.hashDigest);
                if(params.sintr_params.hashEndorsements) {
                    tempDigest = EndorsedTxnDigest(tempDigest, proof->txn(), params.hashDigest);
                }
                if(txn_id != tempDigest){
                    Debug("Tx-id: [%s], TxDigest: [%s]", txn_id, tempDigest);
                    Panic("Supplied Wrong Txn for given tx-id");
                    delete proof;
                    stop = true;
                    return; //break; //Can ignore supply msg from this replica (must be byz)
                }
                //Confirm that proof of transaction commit is valid.

                //Synchronous code: 
                    // valid = ValidateCommittedProof(*proof, &txn_id, keyManager, &config, verifier); 
                    // if(!valid){
                    //     delete proof;
                    //     Panic("Commit Proof not valid");
                    //     return;
                    // }
                    // Debug("Validated Commit Proof for tx-id: [%s]", BytesToHex(txn_id, 16).c_str());
                    // CommitWithProof(txn_id, proof);
            Debug("Verifying commit proof for tx-id: [%s]", BytesToHex(txn_id, 16).c_str());
            //Asynchronous code: 
            auto mcb = [this, txn_id, proof](void* valid) mutable { 
                if(!valid){
                    delete proof;
                    Panic("Commit Proof not valid");
                    return (void*) false;
                }
                //Note: Mcb will be called on network thread --> dispatch to worker again.
                auto f = [this, txn_id, proof]() mutable{
                    Debug("Via supply: Committing proof for tx-id: [%s]", BytesToHex(txn_id, 16).c_str());
                    // if(committed.find(txn_id) != committed.end()) return (void*) true; //duplicate, do nothing. TODO: Forward to all interested clients and empty it?
                    CommitWithProof(txn_id, proof);
                    return (void*) true;
                };
                transport->DispatchTP_noCB(std::move(f));  //Technically Commit Callback should go onto mainthread, but its probably fine either way.
                return (void*) true;
            };
            asyncValidateCommittedProof(*proof, &txn_id, keyManager, &config, verifier, std::move(mcb), transport, params.multiThreading, params.batchVerification);
        }
        return;
    } 

    //SIMULATION CODE: If we previously dropped a commit, and are now syncing on prepare. Make sure the dropped commit is getting applied still. Otherwise it will never happen. 
    //Effectively, we just "delay" the commit to force a sync!
    if(simulate_inconsistency){
        droppedMap::accessor d;
        bool previously_dropped_c = dropped_c.find(d, txn_id);
        if(previously_dropped_c){
            proto::Writeback *wb = d->second;
            d->second = nullptr;
            d.release();
            if(wb){
                sockaddr_in addr;
                TCPTransportAddress dummy(addr); //TODO: Cache the remote addres also...
                Debug("Invoke the previously delayed Writeback for txn: [%s]", BytesToHex(txn_id, 16).c_str());
                HandleWriteback(dummy, *wb);
                //If we previously allocated the wb -> delete it here.
                if(!(params.multiThreading || (params.mainThreadDispatching && !params.dispatchMessageReceive))){
                    delete wb;
                }
                //return; //Could return here, but it might be faster to just let sync apply.
            }
        }
    }

    Debug("Sync for Tx[%s] must have prepare.", BytesToHex(txn_id, 16).c_str());
    stats.Increment("Supply_prepare", 1);

    //If not committed/aborted ==> Check if locally present.
    //Just check if ongoing. (Ongoing is added before prepare is finished) -- Since onging might be a temporary ongoing that gets removed again due to invalidity -> check P1MetaData
    p1MetaDataMap::const_accessor c;
    if(!TEST_SYNC && p1MetaData.find(c, txn_id)){
        // c.release();
        
        bool previously_simulated_drop;
        if(simulate_inconsistency){
            previously_simulated_drop = dropped_p.find(txn_id) != dropped_p.end();
        }
        //
        if(previously_simulated_drop){ //If we previously had done Prepare but simulated drop: Now finish the job. To distinguish for CC result, just apply writes. Note: only do this once (on "first supply")
            Debug("Previously simulated drop of prepare for Tx-id [%s]. Now trying to apply. Has P1 res? %d. AlreadyFmat? %d", BytesToHex(txn_id, 16).c_str(), c->second.hasP1, c->second.alreadyForceMaterialized);
            UW_Assert(txn_info.has_p1());
            if(c->second.hasP1){//} && !c->second.alreadyForceMaterialized){  //alreadyForceMat will have been set during an earlier "register" but it won't actually happen
            
                auto result = c->second.result;
                c.release();

                UW_ASSERT(result != proto::ConcurrencyControl::ABORT); //if it was Abort, then we would've already materialized it. Sync shouldn't happen. Or we already applied abort.
                bool force_mat = result == proto::ConcurrencyControl::ABSTAIN;

                Debug("Trying to apply writes of Tx-id [%s]. Result:%d", BytesToHex(txn_id, 16).c_str(), result);
        
                const std::string &txnDigest = txn_id;

                ongoingMap::const_accessor o;
                auto ongoingItr = ongoing.find(o, txnDigest);
                if(!ongoingItr){
                //if(ongoingItr == ongoing.end()){
                    Debug("Already concurrently Committed/Aborted txn[%s]", BytesToHex(txnDigest, 16).c_str());
                    return;
                }
                proto::Transaction *ongoingTxn = o->second.txn; //const
                o.release();

                Timestamp ts = Timestamp(ongoingTxn->timestamp());
                

                preparedMap::accessor a;
                bool first_prepare = prepared.insert(a, std::make_pair(txnDigest, std::make_pair(ts, ongoingTxn)));  //TODO: Might not want to do this in forceMat case?

                if(!first_prepare) return;
                a.release();

                Debug("Have not prepared Tx-id [%s] successfully before. Writing now!", BytesToHex(txn_id, 16).c_str());
    
                // auto &tx = txn_info.p1().txn();
                // Timestamp ts(tx.timestamp());
                auto f = [this, force_mat, ongoingTxn, ts, txnDigest](){   //not very safe: Need to rely on fact that ongoingTxn won't be deleted => maybe make a copy?
                    UW_ASSERT(ongoingTxn);
                    
                    std::vector<std::string> locally_relevant_table_changes = ApplyTableWrites(*ongoingTxn, ts, txnDigest, nullptr, false, force_mat);
                    
                    std::pair<Timestamp, const proto::Transaction *> pWrite = std::make_pair(ts, ongoingTxn);
                    //Apply TableVersion 
                    for(auto table: locally_relevant_table_changes){   //TODO: Ideally also only update Writes in RecordReadPredicatesAndWrites for the relevant table changes
                        Debug("Preparing TableVersion or TableColVersion: %s with TS: [%lu:%lu]", table.c_str(), ts.getTimestamp(), ts.getID());
                        std::pair<std::shared_mutex,std::map<Timestamp, const proto::Transaction *>> &x = preparedWrites[std::move(table)];
                        std::unique_lock lock(x.first);
                        x.second.insert(pWrite);
                    }
                    return (void*) true;
                };
                transport->DispatchTP_noCB(std::move(f));

                return;

            }
            c.release();
            // 
              //If we simulated drop, then we can't rely on P1 finishing. We must do the work.
              //TODO: Ideally ProcesSPrepare should do it..
              //But if it doesn't work, then do the following:
              //If p1MetaData = commit/wait => Call Prepare (this is not forceMat)
              //If p1MetaData = abstain => ForceMat
              //If p1Meta = abort => should aleady be materialized..
              Debug("Tx-id is in skip case. [%s]. It must explicitly be prepared!!", BytesToHex(txn_id, 16).c_str());
            // }
        }
        else{
            c.release();
            Debug("Via sync. Already started P1 handling for tx-id: [%s]. Just Register ForceMat", BytesToHex(txn_id, 16).c_str());
            if(txn_info.has_p1()){
                RegisterForceMaterialization(txn_id, &txn_info.p1().txn());
                return; //Tx already in process of preparing: Will call UpdateWaitingQueries.
            }
        }
        // if(c->second.hasP1){
        //     // Panic("This line shouldn't be necessary anymore: RegisterForceMaterialization should take care of it?");
        //     //if(txn_info.has_p1()) ForceMaterialization(c->second.result, txn_id, &txn_info.p1().txn()); //Try and materialize Transaction; the ongoing prepare may or may not materialize it itself.
        //     // // if(c->second.result == proto::ConcurrencyControl::ABORT){
        //     // //      //Mark all waiting queries as failed.  ==> Better: Just remove from snapshot.   NOTE: Nothing needs to be done to support this -- it simply won't be materialized and read.
        //     // // //FailWaitingQueries(txn_id);
        //     // // }
        // }
        // return; //Tx already in process of preparing: Will call UpdateWaitingQueries.
    } 
    //c.release();

    Debug("About to check if Sync for Tx[%s] has prepare.", BytesToHex(txn_id, 16).c_str());

    //2) Check whether other replica supplies P1 -- If so, try to validate and prepare ourselves     
    //Otherwise: Validate ourselves.
    if(txn_info.has_p1()){
        // Handle incoming p1 as a normal P1 and Update Waiting Queries. ==> If update waiting queries is done as part of Prepare (whether visible or invisible) nothing else is necessary)
           
         Debug("Received Phase1 message");
        Debug("Via sync: Trying to prepare tx-id: [%s]", BytesToHex(txn_id, 16).c_str());
    
        proto::Phase1 *p1 = txn_info.release_p1();

        proto::Transaction *txn;
        if(params.signClientProposals){
            txn = new proto::Transaction(); //Note: txn deletion is covered by ProcessProposal. 
            txn->ParseFromString(p1->signed_txn().data());
        }
        else{
            txn = p1->mutable_txn();
        }

        if (TEST_PREPARE_SYNC && txn->client_id() == 0UL && txn->client_seq_num() == 0UL) {
            // Genesis tx are valid by default. TODO: this is unsafe, but a hack so that we can bootstrap a benchmark without needing to write all existing data with transactions
            // Note: Genesis Tx will NEVER by exchanged by sync since by definition EVERY replica has them (and thus will never request them) -- this branch is only used for testing.
            Debug("Accepted Genesis Prepared tx-id: [%s]", BytesToHex(txn_id, 16).c_str());
            UpdateWaitingQueries(txn_id); //Does not seem necessary for Genesis, but shouldn't hurt
            UpdateWaitingQueriesTS(MergeTimestampId(txn->timestamp().timestamp(),txn->timestamp().id()), txn_id);
            if(params.signClientProposals) delete txn;
            delete p1;
            return;
        }

        //Check whether txn matches requested tx-id
        std::string tempDigest = TransactionDigest(*txn, params.hashDigest);
        if(params.sintr_params.hashEndorsements) {
            tempDigest = EndorsedTxnDigest(tempDigest, *txn, params.hashDigest);
        }
        if(txn_id != tempDigest){
            Debug("Tx-id: [%s], TxDigest: [%s]", BytesToHex(txn_id, 16).c_str(), BytesToHex(tempDigest, 16).c_str());
            Panic("Supplied Wrong Txn for given tx-id");
            if(params.signClientProposals) delete txn;
            delete p1;
            stop = true;
            return;
        }

         //==> Call ProcessProposal. 

            //TODO: If using optimistic Ids'
            // If optimistic ID maps to 2 txn-ids --> report issuing client (do this when you receive the tx already); vice versa, if we notice 2 optimistic ID's map to same tx --> report! 
            // (Can early abort query to not waste exec since sync might fail- or optimistically execute and hope for best) --> won't happen in simulation (unless testing failures)
    
        auto f = [this, p1, txn, txn_dig = txn_id]() mutable {
            //if(params.signClientProposals) *txn->mutable_txndigest() = txn_dig; //Hack to have access to txnDigest inside TXN later (used for abstain conflict)
            *txn->mutable_txndigest() = txn_dig; //Hack to have access to txnDigest inside TXN later (used for abstain conflict, and for FindTableVersion)

            Debug("[CPU:%d] ProcessProposal via Sync. Txn: %s", sched_getcpu(), BytesToHex(txn_dig, 16).c_str());
            const TCPTransportAddress *dummy_remote = new TCPTransportAddress(sockaddr_in()); //must allocate because ProcessProposal binds ref...
            ProcessProposal(*p1, *dummy_remote, txn, txn_dig, true, true); //Set gossip to true ==> No reply; set forceMaterialize to true   (Shouldn't be necessary anymore with the RegisterForce logic)                    
            if((!params.mainThreadDispatching || (params.dispatchMessageReceive && !params.parallel_CCC)) && (!params.multiThreading || !params.signClientProposals)){
                delete p1; //I.e. if receiveMessage would not be allocating (See ManageDispatchP1)
                delete dummy_remote;
                //Note: txn deletion is covered by ProcessProposal.
            } 
            return (void*) true;
        };

        if(!params.query_params.parallel_queries || !params.mainThreadDispatching){  //TODO: Realistically: Always running with multiThreading now. Just configure parallel_queries?
            //If !parallel_queries.  ==> both Query and P1 follow the same dispatch rules (either both on network or both on main)
            //if parallel_queries && !mainThreadDispatching  => parallel_queries has no effect. Thus both Query and P1 follow same dispatch rules (depends on dispatchMessageReceive)
            f();
        }
        else{ //params.mainThreadDispatching = true && parallel_queries == true ==> Query is on worker; but P1 should be on main
                transport->DispatchTP_main(std::move(f));
        }
        return;    
    }

    //Check whether supplier reports tx as invalid (wrong Request)
    else if (txn_info.has_invalid()){
        Panic("Invalid Supply Txn: Replica didn't have requested txn"); //Panicing for debug purposes only. Just return normally.
        return;
            //TODO: Let correct replica report the fact that it did not have tx committed/prepared/aborted --> implies that Client formed an invalid sync proposal  
            //Note: currently waitingQueries doesn't distinguish individual queries: some may have had honest clients, others not. ==> Could add req_id that invoked Req/Supply
        //Note: We currently receive SupplyTxn for each query separately (even though UpdateWaiting wakes multiple)
    }

    else{
        Panic("Ill-formed supply TxnProof");
    }
}
   


//////////////////////////// Short-circuit Queries


//TODO: DONT USE THIS. It aborts queries whenever a snapshot contains an aborted tx. But maybe this aborted tx is not part of frontier, so it does not matter.
void Server::FailWaitingQueries(const std::string &txnDigest){

     Debug("All queries waiting on txn_id %s are invalid, because %s is abort.", BytesToHex(txnDigest, 16).c_str());

     //1) find queries that were waiting on this txn-id
    waitingQueryMap::accessor w;
    bool hasWaiting = waitingQueries.find(w, txnDigest);
    if(hasWaiting){
        for(const std::string &waiting_query : w->second){

            //2) update their missing data structures
            queryMetaDataMap::accessor q;
            bool queryActive = queryMetaData.find(q, waiting_query);
            if(queryActive){
                QueryMetaData *query_md = q->second;
                FailQuery(query_md);
            }
            q.release();
            //w->second.erase(waiting_query); //FIXME: Delete safely while iterating... ==> Just erase all after
        }
        //4) remove key from waiting data structure since no more queries are waiting on it 
        waitingQueries.erase(w);
    }
    w.release();
}


void Server::FailQuery(QueryMetaData *query_md){

    query_md->failure = true;
    query_md->has_result = false;
    
    proto::FailQuery failQuery;
    failQuery.set_req_id(query_md->req_id);
    failQuery.mutable_fail()->set_replica_id(id);

    if (params.validateProofs && params.signedMessages) {
        Debug("Sign Query Fail Reply for Query[%lu:%lu:%lu]", query_md->query_seq_num, query_md->client_id, query_md->retry_version);
        proto::FailQueryMsg *failQueryMsg = failQuery.release_fail();

        if(params.signatureBatchSize == 1){
            SignMessage(failQueryMsg, keyManager->GetPrivateKey(id), id, failQuery.mutable_signed_fail());
        }
        else{
            std::vector<::google::protobuf::Message *> msgs;
            msgs.push_back(failQueryMsg);
            std::vector<proto::SignedMessage *> smsgs;
            smsgs.push_back(failQuery.mutable_signed_fail());
            SignMessages(msgs, keyManager->GetPrivateKey(id), id, smsgs, params.merkleBranchFactor);
        }
    }


    transport->SendMessage(this, *query_md->original_client, failQuery);

    return;

}

//////////////////////////////// CLEAN UP / GARBAGE COLLECTION ///////////////////////////////////////////////


//TODO: Clean Query as part of HandleAbort
// -> handle abort should specify list of query_ids to just be deleted. (erase fully)

void Server::CleanQueries(const proto::Transaction *txn, bool is_commit){
  //Move read sets into txn + Remove QueryMd completely. Store a map: <client-id, timestamp> disallowing clients to issue requests to the past (this way old/late queries won't be accepted anymore.)
    
  if(!txn->has_last_query_seq()) return;
  // TODO: use endorsement with txn hash if params.sintr_params.hashEndorsements is enabled
  Debug("Clean all Query Md associated with txn: %s", BytesToHex(TransactionDigest(*txn, params.hashDigest), 16).c_str());

  clientQueryWatermarkMap::accessor qw;
  clientQueryWatermark.insert(qw, txn->client_id());
  if(txn->last_query_seq() > qw->second) qw->second = txn->last_query_seq();
  qw.release();
  //clientQueryWatermark[txn->client_id()] = txn->last_query_seq(); //only update timestamp for commit if greater than last one... //To do this atomically need hashmap lock.

  //For every query in txn: 
  for(const proto::QueryResultMetaData &tx_query_md : txn->query_set()){
    queryMetaDataMap::accessor q;
    bool hasQuery = queryMetaData.find(q, tx_query_md.query_id());
    if(hasQuery){
        Debug("Deleting all QueryMD for queryId: %s (bytes)", BytesToHex(tx_query_md.query_id(), 16).c_str());
        QueryMetaData *local_query_md = q->second; //Local query_md

        //THIS IS JUST TEST/DEBUG CODE
        auto [_, first_deletion] = alreadyDeleted.insert(tx_query_md.query_id());
        if(!first_deletion) Panic("duplicate delete");

    
        // //Try to clear RTS TODO: For commit: Could optimize RTS GC to remove all RTS >= committed TS (see CommitToStore) 
        if(params.query_params.cacheReadSet){ //Try to clear RTS in case it was cached 
            ClearRTS(local_query_md->queryResultReply->result().query_read_set().read_set(), local_query_md->ts); 

            if(is_commit){ //Move read set if caching. Note: Don't need to move read_set_hash -> tx already stores it. 
                //FIXME: Moving the read set is unsafe because it changes the Tx digest. Possible fix: with caching ON we should not be changing digest
                // auto itr = tx_query_md.group_meta().find(groupIdx);
                // if(false && itr != tx_query_md.group_meta().end()){  //only do this if shard was involved in the query.
                //     proto::QueryGroupMeta &query_group_meta = itr->second;
                //     //Note: only move if read_set hash matches. It might not. But at least 2f+1 correct replicas do have it matching.
                //     if(query_group_meta.read_set_hash() == local_query_md->queryResultReply->result().query_result_hash()){
                //         proto::ReadSet *read_set = local_query_md->queryResultReply->mutable_result()->release_query_read_set();
                //         query_group_meta.set_allocated_query_read_set(read_set);
                //     }
                // }
            }
        }   
        else{  //Try to clear RTS in case tx had it all along: 
            auto itr = tx_query_md.group_meta().find(groupIdx);
            if(itr != tx_query_md.group_meta().end()){  //only do this if shard was involved in the query.
                const proto::QueryGroupMeta &query_group_meta = itr->second;
                ClearRTS(query_group_meta.query_read_set().read_set(), local_query_md->ts);
            }
        }
                                                                                            
        //erase current retry version from missing (Note: all previous ones must have been deleted via ClearMetaData)
        queryMissingTxns.erase(QueryRetryId(tx_query_md.query_id(), q->second->retry_version, (params.query_params.signClientQueries && params.query_params.cacheReadSet && params.hashDigest)));

    
        if(local_query_md != nullptr) delete local_query_md;
        //local_query_md = nullptr;
        queryMetaData.erase(q); 
    }
    //Don't erase Md entry --> Keeping it disallows future queries. ==> Improve by adding the client TS map forcing monotonic queries. (Map size O(clients) instead of O(queries))
    //queryMetaData.erase(tx_query_md.query_id())
    q.release();

    //Delete any possibly subscribed queries.
    subscribedQuery.erase(tx_query_md.query_id());
  }
       
  //TODO: Fallback;:
   // Ideally: Use mergedReadSet.. However, can't prove the validity of it to other replicas.
    //      Note: Don't want to send mergedReadSet //Note: If you send Txn that includes queries while Cache query params is on => will ignore the sent ones.
    //      Notably: queries are not part of txnDigest. //If one receives a forwarded Txn ==> check that supplied read sets match hashes. Then either cache read-sets ourselves. Or process directly (more efficient)
    //  I.e. even if caching is enabled and thus replicas don't expect tx to contain read set ==> if it is forwarded, DO look for it's read set.
}


} // namespace sintrstore
