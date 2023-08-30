/***********************************************************************
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

       //TODO: Next:
       //Query Exec engine: 
       //   a) Server parses, scans, and replies with real sync state 
       //   b) Server receives snapshot, materializes, executes and replies.
       //Query Concurrency Control ==> part of Tx, part of TxDigest, part of MVTSO check.

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

    //Only process if above watermark. I.e. ignore old queries
    clientQueryWatermarkMap::const_accessor qw;
    if(clientQueryWatermark.find(qw, query->client_id()) && qw->second >= query->query_seq_num()){
    //if(clientQueryWatermark[query->client_id()] >= query->query_seq_num()){
        delete query;
        if(params.mainThreadDispatching && (!params.dispatchMessageReceive || params.query_params.parallel_queries)) FreeQueryRequestMessage(&msg);
        return;
    }
    qw.release();

     // 2) Compute unique hash ID 
    std::string queryId = QueryDigest(*query, (params.query_params.signClientQueries && params.query_params.cacheReadSet && params.hashDigest)); 
    
    // std::string queryId;
    // if(params.query_params.signClientQueries && params.query_params.cacheReadSet){ //TODO: when to use hash id? always?
    //     queryId = QueryDigest(*query, params.hashDigest); 
    // }
    // else{
    //     queryId =  "[" + std::to_string(query->query_seq_num()) + ":" + std::to_string(query->client_id()) + "]";
    // }
     Debug("\n Received Query Request Query[%lu:%lu:%d] (seq:client:ver), queryId: %s", query->query_seq_num(), query->client_id(), query->retry_version(), BytesToHex(queryId, 16).c_str());
   
    //TODO: Ideally check whether already have result or retry version is outdated Before VerifyClientQuery.

    //3) Check whether retry version still relevant.
    queryMetaDataMap::accessor q;
    bool hasQuery = queryMetaData.find(q, queryId);
    if(hasQuery){
        QueryMetaData *query_md = q->second;
        bool valid = true;
        if(query->retry_version() < query_md->retry_version){
            Debug("Retry version for Query Request Query[%lu:%lu:%d] (seq:client:ver) is outdated. Currently %d", query->query_seq_num(), query->client_id(), query->retry_version(), query_md->retry_version);
            valid = false;
        }
        if(query->retry_version() == query_md->retry_version){ //TODO: if have result, return result
            //Two cases for which proposed retry version could be == stored retry_version:
                //a) have already received query for this retry version 
                //b) have already received a sync for this retry version (and the sync is not waiting for query)
            ////Return if already received query or sync for the retry version, and sync is not waiting for query. (I.e. no need to process Query) (implies result will be sent.)
            if(query_md->executed_query || query_md->started_sync && !query_md->waiting_sync){ 
                Debug("Already received Sync or Query for Query[%lu:%lu:%d] (seq:client:ver). Skipping Query", query->query_seq_num(), query->client_id(), query->retry_version(), query_md->retry_version);
                valid = false;
            }
        }
        if(!valid){
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
        bool new_insert = queryMetaData.insert(q, queryId); //If not first insert -> must re-check.
        if(new_insert){
            q->second = new QueryMetaData(query->query_cmd(), query->timestamp(), remote, msg.req_id(), query->query_seq_num(), query->client_id(), &params.query_params);
        }
        re_check = !new_insert;
    }
    
    QueryMetaData *query_md = q->second;

    if(!query_md->has_query){ //If queryMetaData was inserted by Sync first (i.e. query has not been processed yet), set query.
        UW_ASSERT(query->has_query_cmd());  //TODO: Could avoid re-sending query_cmd in retry messages (but then might have to wait for first query attempt in case multithreading violates FIFO)
        query_md->SetQuery(query->query_cmd(), query->timestamp(), remote, msg.req_id());  
       
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
        if(query->retry_version() < query_md->retry_version){
            Debug("Retry version for Query Request Query[%lu:%lu:%d] (seq:client:ver) is outdated. Currently %d", query->query_seq_num(), query->client_id(), query->retry_version(), query_md->retry_version);
            valid = false;
        }
        if(query->retry_version() == query_md->retry_version){  //TODO: if have result, return result
            ////Return if already received query or sync for the retry version, and sync is not waiting for query. (I.e. no need to process Query) (implies result will be sent.)
            if(query_md->executed_query || query_md->started_sync && !query_md->waiting_sync){ 
                Debug("Already received Sync or Query for Query[%lu:%lu:%d] (seq:client:ver). Skipping Query", query->query_seq_num(), query->client_id(), query->retry_version(), query_md->retry_version);
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


    //If Eager Exec --> Skip sync and just execute on local state --> Call EagerExec function: Calls same exec as HandleSyncCallback (but without materializing snapshot) + SendQueryResult.
    if((msg.designated_for_reply() || params.query_params.cacheReadSet) && msg.has_eager_exec() && msg.eager_exec()){ //Note: If eager exec on && caching read set --> all must execute.
        ExecQueryEagerly(q, query_md, queryId);
        if(params.mainThreadDispatching && (!params.dispatchMessageReceive || params.query_params.parallel_queries)) FreeQueryRequestMessage(&msg);
        return;
    }

    //7) Record whether current retry version uses optimistic tx-ids or not
    if(msg.has_optimistic_txid()) query_md->useOptimisticTxId = msg.optimistic_txid(); 

    //8) Process Query only if designated for reply; and if there is no Sync already waiting for this retry version
    if(msg.designated_for_reply() && !query_md->waiting_sync){
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

    Debug("PointQuery[%lu:%lu] %s.", query->query_seq_num(), query->client_id(), query->query_cmd().c_str());

    if (CheckHighWatermark(ts)) {
        // ignore request if beyond high watermark
        Debug("Point Read timestamp beyond high watermark.");
        delete query;
        return;
    }

    proto::PointQueryResultReply *pointQueryReply = GetUnusedPointQueryResultReply(); 
    pointQueryReply->set_req_id(reqId);
    pointQueryReply->set_replica_id(id);

    //1) Execute
    proto::Write *write = pointQueryReply->mutable_write();
    const proto::CommittedProof *committedProof;
    std::string enc_primary_key;  //TODO: Replace with query->primary_enc_key()

   
    //If MVTSO: Read prepared, Set RTS
    if (occType == MVTSO) {
        //Sets RTS timestamp. Favors readers commit chances.
        Debug("Set up RTS for PointQuery[%lu:%lu]", query->query_seq_num(), query->client_id());
        SetRTS(ts, query->primary_enc_key());
    }

    table_store->ExecPointRead(query->query_cmd(), enc_primary_key, ts, write, committedProof);
    delete query;

    if(write->has_committed_value()){
        UW_ASSERT(committedProof); //proof must exist
        *pointQueryReply->mutable_proof() = *committedProof;
    } 

    if(TEST_QUERY){
        ///////////
        //Toy Transaction
        std::string toy_txn("toy_txn");
        Timestamp toy_ts(0, 2); //set to genesis time.
        sql::QueryResultProtoBuilder queryResultBuilder;
        queryResultBuilder.add_columns({"key_", "val_"});
        std::vector<std::string> result_row = {"alice", "blonde"};
        queryResultBuilder.add_row(result_row.begin(), result_row.end());
        std::string toy_result = queryResultBuilder.get_result()->SerializeAsString();

        //Panic("stop here");
        // //Create Toy prepared Tx
        if(id ==0){
        
            write->set_prepared_value(toy_result);
            std::cerr << "SENT PREPARED RESULT: " << write->prepared_value() << std::endl;
            write->set_prepared_txn_digest(toy_txn);
            toy_ts.serialize(write->mutable_prepared_timestamp());
        }
    

        //Create Toy committed Tx with genesis proof
        // proto::CommittedProof *genesis_proof = new proto::CommittedProof();
        // genesis_proof->mutable_txn()->set_client_id(0);
        // genesis_proof->mutable_txn()->set_client_seq_num(0);
        // toy_ts.serialize(genesis_proof->mutable_txn()->mutable_timestamp());

        // committed.insert(std::make_pair(toy_txn, genesis_proof));  //TODO: Need to move this elsewhere so all servers have it.

        // write->set_committed_value(toy_result);
        // toy_ts.serialize(write->mutable_committed_timestamp());
        // *pointQueryReply->mutable_proof() = *genesis_proof;


        //Create Toy committed Tx with real proof tx -- create toy "real" QC

        sql::QueryResultProtoBuilder queryResultBuilder2;
        queryResultBuilder2.add_columns({"key_", "val_"});
        result_row = {"alice", "black"};
        queryResultBuilder2.add_row(result_row.begin(), result_row.end());
        std::string toy_result2 = queryResultBuilder2.get_result()->SerializeAsString();

        Timestamp toy_ts_c(0, 1);

        proto::CommittedProof *real_proof = new proto::CommittedProof();
        proto::Transaction *txn = real_proof->mutable_txn();
        real_proof->mutable_txn()->set_client_id(0);
        real_proof->mutable_txn()->set_client_seq_num(1);
        toy_ts_c.serialize(real_proof->mutable_txn()->mutable_timestamp());
        TableWrite &table_write = (*real_proof->mutable_txn()->mutable_table_writes())["datastore"];
        RowUpdates *row = table_write.add_rows();
        row->add_column_values("alice");
        row->add_column_values("black");
        WriteMessage *write_msg = real_proof->mutable_txn()->add_write_set();
        write_msg->set_key("datastore#alice");
        write_msg->mutable_rowupdates()->set_row_idx(0);

        //Add relal qC:
        // proto::Signatures &sigs = (*real_proof->mutable_p1_sigs())[id];
        // sigs.add_sigs();
        // SignMessage()

        //committed[toy_txn]  = real_proof;  //TODO: Need to move this elsewhere so all servers have it.

        write->set_committed_value(toy_result2);
        
        toy_ts_c.serialize(write->mutable_committed_timestamp());
        *pointQueryReply->mutable_proof() = *real_proof;
    }



    ////////////
    

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

void Server::FindSnapshot(QueryMetaData *query_md, proto::Query *query){

    // 1) Parse & Execute Query
    // SQL glue. How to execute from query plan object.
    /////////////////////////////////////////////////////////////
    //                                                         //
    //                                                         //
    //                                                         //
    //
    //              EXEC BLACKBOX -- TBD
    table_store->FindSnapshot(query_md->query_cmd, query_md->ts, query_md->snapshot_mgr);
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
    if(TEST_SNAPSHOT){

        std::string test_txn_id = "[test_id_of_length_32 bytes----]";
        proto::CommittedProof *proof = new proto::CommittedProof();
        //Genesis proof: client id + client seq = 0.
            proof->mutable_txn()->set_client_id(0);
        proof->mutable_txn()->set_client_seq_num(0);
        uint64_t ts = 5UL << 32; //timeServer.GetTime(); 
        proof->mutable_txn()->mutable_timestamp()->set_timestamp(ts);
        proof->mutable_txn()->mutable_timestamp()->set_id(0UL);
        committed[test_txn_id] = proof;
        
        ts_to_tx.insert(std::make_pair(MergeTimestampId(ts, 0UL), test_txn_id));
        
        proto::Phase1 p1 = proto::Phase1();
        p1.set_req_id(1);
    
        std::vector<::google::protobuf::Message *> p1s;
        p1s.push_back(proof->mutable_txn());
        std::vector<proto::SignedMessage *> sp1s;
        sp1s.push_back(p1.mutable_signed_txn());
        SignMessages(p1s, keyManager->GetPrivateKey(keyManager->GetClientKeyId(0)), 0, sp1s, params.merkleBranchFactor);

        p1MetaDataMap::accessor c;
        p1MetaData.insert(c, test_txn_id);
        c->second.hasSignedP1 = true;
        c->second.signed_txn = p1.release_signed_txn();
        c.release();


    //FIXME: TOY INSERT TESTING.
        //-- real tx-ids are cryptographic hashes of length 256bit = 32 byte.
            for(auto const&[tx_id, proof] : committed){
                if(tx_id == "" || tx_id == "toy_txn") continue;
                const proto::Transaction *txn = &proof->txn();
                query_md->snapshot_mgr.AddToLocalSnapshot(tx_id, txn, true);
                Debug("Proposing committed txn_id [%s] for local Query Sync State[%lu:%lu:%d]", BytesToHex(tx_id, 16).c_str(), query->query_seq_num(), query->client_id(), query->retry_version());
                
                //Adding some dummy tx to prepared.
                preparedMap::accessor p;
                prepared.insert(p, tx_id);
                Timestamp ts(txn->timestamp());
                p->second = std::make_pair(ts, txn);
                p.release();
            }
            //Not threadsafe, but just for testing purposes.
            for(preparedMap::const_iterator i=prepared.begin(); i!=prepared.end(); ++i ) {
                const std::string &tx_id = i->first;
                const proto::Transaction *txn = i->second.second;
                query_md->snapshot_mgr.AddToLocalSnapshot(tx_id, txn, false);
                Debug("Proposing prepared txn_id [%s] for local Query Sync State[%lu:%lu:%d]", BytesToHex(tx_id, 16).c_str(), query->query_seq_num(), query->client_id(), query->retry_version());
            }
    
    }
}

////////////////////Handle Sync
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
        queryId = merged_ss->mutable_query_digest();
    }
    else{
         //For now, can also index via (client id, query seq_num) pair. Just define an ordering function for query id pair. (In this case, unique string combination)
        merged_ss = msg.release_merged_ss();
        //query_id =  "[" + std::to_string(merged_ss->query_seq_num()) + ":" + std::to_string(merged_ss->client_id()) + "]";
        //queryId = &query_id;
        queryId = merged_ss->mutable_query_digest();
    }
    Debug("\n Received Query Sync Proposal for Query[%lu:%lu:%d] (seq:client:ver)", merged_ss->query_seq_num(), merged_ss->client_id(), merged_ss->retry_version());

     //Only process if below watermark.
    clientQueryWatermarkMap::const_accessor qw;
    if(clientQueryWatermark.find(qw, merged_ss->client_id()) && qw->second >= merged_ss->query_seq_num()){
    //if(clientQueryWatermark[merged_ss->client_id()] >= merged_ss->query_seq_num()){
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
            Debug("Retry version for Sync Request Query[%lu:%lu:%d] (seq:client:ver) is outdated (currently %d) OR started sync.", merged_ss->query_seq_num(), merged_ss->client_id(), merged_ss->retry_version(), query_md->retry_version);
             //TODO: if have result, return result
            //if(query_md->has_result){}    // Note: if already received sync for the retry version then result will be sent...
            delete merged_ss; 
            if(params.mainThreadDispatching && (!params.dispatchMessageReceive || params.query_params.parallel_queries)) FreeSyncClientProposalMessage(&msg);
            return;
        }
    }
    else{
        Debug("Have not received Query[%lu:%lu:%d] with Id: %s", merged_ss->query_seq_num(), merged_ss->client_id(), merged_ss->retry_version(), BytesToHex(*queryId, 16).c_str());
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
            Debug("Retry version for Sync Request Query[%lu:%lu:%d] (seq:client:ver) is outdated (currently %d) OR started sync.", merged_ss->query_seq_num(), merged_ss->client_id(), merged_ss->retry_version(), query_md->retry_version);
            delete merged_ss; 
            if(params.mainThreadDispatching && (!params.dispatchMessageReceive || params.query_params.parallel_queries)) FreeSyncClientProposalMessage(&msg);
            return;
        }
    }

    if(merged_ss->retry_version() > query_md->retry_version){ 
        query_md->ClearMetaData(*queryId);
        query_md->req_id = msg.req_id();
          //Delete current missingTxns.   -- NOTE: Currently NOT necessary for correctness, because UpdateWaitingQueries checks whether retry version is still current. But good for garbage collection.
        queryMissingTxns.erase(QueryRetryId(*queryId, query_md->retry_version, (params.query_params.signClientQueries && params.query_params.cacheReadSet && params.hashDigest)));
        query_md->retry_version = merged_ss->retry_version();
    }

    query_md->started_sync = true;
    query_md->designated_for_reply = msg.designated_for_reply();

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

    //1) Determine all missing transactions 
    //query_md->missing_txns.clear();
    //query_md->missing_ts.clear();
    std::map<uint64_t, proto::RequestMissingTxns> replica_requests = {};

    std::string query_retry_id = QueryRetryId(*queryId, query_md->retry_version, (params.query_params.signClientQueries && params.query_params.cacheReadSet && params.hashDigest)); 
    Debug("Query id: %s. Query retry id: %s", BytesToHex(*queryId, 16).c_str(), BytesToHex(query_retry_id, 24).c_str());
    queryMissingTxnsMap::accessor qm;
    bool first_qm = queryMissingTxns.insert(qm, query_retry_id); //Note: ClearMetaData: Deletes previous retry_version.
    UW_ASSERT(first_qm); //ProcessSync should never be called twice for one retry version.
    // In SetWaiting -> add missing to qm->second. (pass qm->second as arg.)
    std::unordered_map<std::string, uint64_t> &missing_txns = qm->second.missing_txns; //query_md->missing_txns;
    std::unordered_map<uint64_t, uint64_t> &missing_ts = qm->second.missing_ts; //querHandleReqy_md->missing_ts;
    // If missing empty after checking snapshot -> erase again
    
     //Note: SetWaiting needs to pass query_retry_id, not queryId.

    //Using normal tx-id
    if(!query_md->useOptimisticTxId){
         //txn_replicas_pair
        for(auto const &[tx_id, replica_list] : merged_ss->merged_txns()){
            Debug("Snapshot for Query Sync Proposal[%lu:%lu:%d] contains tx_id [%s]", merged_ss->query_seq_num(), merged_ss->client_id(), merged_ss->retry_version(), BytesToHex(tx_id, 16).c_str());
            //query_md->merged_ss.insert(tx_id); //store snapshot locally. //DEPRECATED -->  now just storing the merged_ss_msg directly
            
            //i) Check whether replica has the txn.: If not ongoing, and not commit/abort --> then we have not received it yet. (Follows from commit/aborted being updated before erasing from ongoing)
            //CheckPresence(tx_id, merged_ss, queryId);
            ongoingMap::const_accessor o;
            //bool has_txn_locally = ongoing.find(o, tx_id)? true : (committed.find(tx_id) != committed.end() || aborted.find(tx_id) != aborted.end());
        
            bool has_txn_locally = ongoing.find(o, tx_id);
            ///o.release();
            if(!has_txn_locally){
                if (committed.find(tx_id) != committed.end()) has_txn_locally = true;
                else if (aborted.find(tx_id) != aborted.end()){  //If Query not ongoing/committed --> Fail Query early if aborted. 
                    has_txn_locally = true;
                    //Remove from snapshot.
                    merged_ss->mutable_merged_txns()->erase(tx_id);

                   // Note: CURRENTLY NOT USING FAILQuery here: An aborted tx in the snapshot might not be on the execution frontier... -> Fail only during exec. Just proceed here (mark tx as "has_locally")
                    // FailQuery(query_md);   // PLUS: May actually WANT to not read from the aborted tx.
                    // delete merged_ss;
                    // return;
                }
            }
            
            //ii) Register tx that this query is waiting on.
            if(TEST_SYNC || !has_txn_locally){
                SetWaiting(missing_txns, tx_id, queryId, query_retry_id, replica_list, replica_requests);
            }
            o.release(); //Make sure SetWaiting is set while holding ongoing ==> Then it is guaranteed that either the Txn has Written back (is present) or SetWaiting is set before calling UpdateWaiting (in Commit/abort)
                        // Case: if ongoing.find = true ==> Then Commit/Abort must wait at Clean; --> Call UpdateWaiting only after SetWaiting is done.
                        // Case: if ongoing.find = false ==> Then Commit/Abort has already called Clean --> thus has already been added to committed/aborted --> has_locally = true
                                                            //Or: Txn has not been received yet at all.
        }
    }
    //else: Using optimistic tx-id
    if(query_md->useOptimisticTxId){
        query_md->snapshot_mgr.OpenMergedSnapshot(merged_ss); //Decompresses if applicable 
        for(auto const &[ts_id, replica_list] : merged_ss->merged_ts()){
            Debug("Snapshot for Query Sync Proposal[%lu:%lu:%d] contains ts_id [%lu]", merged_ss->query_seq_num(), merged_ss->client_id(), merged_ss->retry_version(), ts_id);
            //i) Check whether Tx is locally present.
                 //transform ts_id to txnDigest if using optimistc ids.. // Check local mapping from Timestamp to TxnDigest 
            ts_to_txMap::const_accessor t;
            bool hasTx = ts_to_tx.find(t, ts_id);//.. find in map. If yes, add tx_id to merged_txns. If no, try to sync on TS.
            if(hasTx){
                //Add Txn to snapshot if present (and not aborted)
                const std::string &tx_id = t->second;
                if(aborted.find(tx_id) == aborted.end()) (*query_md->merged_ss_msg->mutable_merged_txns())[tx_id]; //(Just add with default constructor --> empty ReplicaList)
                //query_md->merged_ss.insert(t->second); //store snapshot locally.  
                t.release();
            }
            else{
                t.release();
                SetWaitingTS(missing_ts, ts_id, queryId, query_retry_id, replica_list, replica_requests); 
            }
        }
    }
    
    //Update queryMissingTxns meta data.
    if(!missing_txns.empty() || !missing_ts.empty()){
        qm->second.query_id = *queryId;  //Needed to lookup QueryMetaData upon waking.
        qm->second.retry_version = query_md->retry_version;
    } 
    else{
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
    if(replica_requests.empty()){
        return HandleSyncCallback(q, query_md, *queryId);
    }
    else{  //if there are missng txn, i.e. replica_requests not empty ==> send out sync requests.
        query_md->is_waiting = true; //Note: If query is waiting, but (byz) client supplied wrong/insufficient replicas to sync from ==> query loses liveness.
        q.release();
        Debug("Sync State incomplete for Query[%lu:%lu:%d]", merged_ss->query_seq_num(), merged_ss->client_id(), merged_ss->retry_version()); 
        for(auto const &[replica_idx, replica_req] : replica_requests){
            if(replica_idx == idx) Panic("Should never request from self");
            transport->SendMessageToReplica(this, groupIdx, replica_idx, replica_req);
            Debug("Replica %d Request Data Sync from replica %d", replica_req.replica_idx(), replica_idx); 
            // for(auto const& txn : replica_req.missing_txn()){ std::cerr << "Requesting txn : " << (BytesToHex(txn, 16)) << std::endl;}
        }
    }
      
    //delete merged_ss; //Deleting only upon ClearMetaData or delete query_md 
    return;
}

void Server::SetWaiting(std::unordered_map<std::string, uint64_t> &missing_txns, const std::string &tx_id, const std::string *queryId, const std::string &query_retry_id,
    const proto::ReplicaList &replica_list, std::map<uint64_t, proto::RequestMissingTxns> &replica_requests)
{
    //Add to waiting data structure.
    waitingQueryMap::accessor w;
    waitingQueries.insert(w, tx_id);
    bool already_requested = !w->second.empty(); //TODO: if there is already a waiting query, don't need to request the txn again. Problem: Could have been requested by a byz client that gave wrong replica_ids...
    //w->second.insert(*queryId);
    w->second.insert(query_retry_id);
    w.release();

        // Wait for up f+1 replies for each missing. (if none successful, then client must have been byz. Vote Early abort (if anything) and report client.)
    missing_txns[tx_id]; //= config.f + 1;  we don't stop waiting for f+1 currently. 

    uint64_t count = 0;
    for(auto const &replica_id: replica_list.replicas()){ 
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
        count++;
    }
}

void Server::SetWaitingTS(std::unordered_map<uint64_t, uint64_t> &missing_ts, const uint64_t &ts_id, const std::string *queryId, const std::string &query_retry_id,
    const proto::ReplicaList &replica_list, std::map<uint64_t, proto::RequestMissingTxns> &replica_requests)
{
    //Panic("Sync on TS-Ids not yet supported");

    // Upon receiving Tx from sync -> need to update merged_ss with txn.
      //Add to waiting data structure.
    waitingQueryTSMap::accessor w;
    waitingQueriesTS.insert(w, ts_id);
    bool already_requested = !w->second.empty(); //TODO: if there is already a waiting query, don't need to request the txn again. Problem: Could have been requested by a byz client that gave wrong replica_ids...
    //w->second.insert(*queryId);
    w->second.insert(query_retry_id);
    w.release();

        // Wait for up f+1 replies for each missing. (if none successful, then client must have been byz. Vote Early abort (if anything) and report client.)
    missing_ts[ts_id]; //= config.f + 1;  we don't stop waiting for f+1 currently. 

    uint64_t count = 0;
    for(auto const &replica_id: replica_list.replicas()){ 
        if(count > config.f +1) return; //only send to f+1 --> an honest client will never include more than f+1 replicas to request from. --> can ignore byz request.
        
        
        uint64_t replica_idx = replica_id % config.n;  //since  id = local-groupIdx * config.n + idx
        if(replica_idx != idx){
            proto::RequestMissingTxns &req_txn = replica_requests[replica_idx];
            req_txn.add_missing_txn_ts(ts_id);
            req_txn.set_replica_idx(idx);
        }
        count++;
        }
}


////////////////////////// Replica To Replica Sync exchange

void Server::HandleRequestTx(const TransportAddress &remote, proto::RequestMissingTxns &req_txn){

     Debug("\n Received RequestMissingTxn from replica %d", req_txn.replica_idx()); 

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
        proto::TxnInfo &txn_info = (*supply_txn.mutable_txns())[txn_id];
        CheckLocalAvailability(txn_id, txn_info);
        //CheckLocalAvailability(txn_id, supply_txn);
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
        }
        //Corner case: If replica voted prepare, but is now abort, what should happen? Should query ReportFail? Or should query just go through without this tx ==> The latter. After all, it is correct to ignore.

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
    for(auto &[txn_id, txn_info] : *supply_txn->mutable_txns()){
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

     ///Note (FIXME:?): A Replica that has a prepare but receives an abort proof might want to remove the tx from the snapshot. TODO: For this reason, may want to move prepare check after abort proof check.

    //If not committed/aborted ==> Check if locally present.
    //Just check if ongoing. (Ongoing is added before prepare is finished) -- Since onging might be a temporary ongoing that gets removed again due to invalidity -> check P1MetaData
    p1MetaDataMap::const_accessor c;
    if(!TEST_SYNC && p1MetaData.find(c, txn_id)){
         Debug("Already started P1 handling for tx-id: [%s]", BytesToHex(txn_id, 16).c_str());
        if(c->second.hasP1){
            if(c->second.result == proto::ConcurrencyControl::ABORT){
                 //Mark all waiting queries as failed.  ==> Better: Just remove from snapshot.
            //FailWaitingQueries(txn_id);
            }
        }
        return; //Tx already in process of preparing: Will call UpdateWaitingQueries.
    } 
    c.release();
    
    //Check if other replica had aborted (If so, exclude this tx from snapshot; Alternatively could fail sync eagerly, but that seems unecessary)
    if(txn_info.abort()){ 
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

    //Check if other replica supplied commit
    if(txn_info.has_commit_proof()){   
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
                if(txn_id != TransactionDigest(proof->txn(), params.hashDigest)){
                    Debug("Tx-id: [%s], TxDigest: [%s]", txn_id, TransactionDigest(proof->txn(), params.hashDigest));
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
                     Debug("Committing proof for tx-id: [%s]", BytesToHex(txn_id, 16).c_str());
                    CommitWithProof(txn_id, proof);
                    return (void*) true;
                };
                transport->DispatchTP_noCB(std::move(f));
                return (void*) true;
            };
            asyncValidateCommittedProof(*proof, &txn_id, keyManager, &config, verifier, std::move(mcb), transport, params.multiThreading, params.batchVerification);
        }
        return;
    } 

    //2) Check whether other replica supplies P1 -- If so, try to validate and prepare ourselves     
    //Otherwise: Validate ourselves.
    else if(txn_info.has_p1()){
        // Handle incoming p1 as a normal P1 and Update Waiting Queries. ==> If update waiting queries is done as part of Prepare (whether visible or invisible) nothing else is necessary)
           
         Debug("Received Phase1 message");
         Debug("Trying to prepare tx-id: [%s]", BytesToHex(txn_id, 16).c_str());
    
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
        if(txn_id != TransactionDigest(*txn, params.hashDigest)){
            Debug("Tx-id: [%s], TxDigest: [%s]", BytesToHex(txn_id, 16).c_str(), BytesToHex(TransactionDigest(*txn, params.hashDigest), 16).c_str());
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
            if(params.signClientProposals) *txn->mutable_txndigest() = txn_dig; //Hack to have access to txnDigest inside TXN later (used for abstain conflict)

            const TCPTransportAddress *dummy_remote = new TCPTransportAddress(sockaddr_in()); //must allocate because ProcessProposal binds ref...
            ProcessProposal(*p1, *dummy_remote, txn, txn_dig, true); //Set gossip to true ==> No reply                       
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
   

//Note: Call this only from Network or from MainThread (can do so before calling DoOCC)
void Server::CheckWaitingQueries(const std::string &txnDigest, const TimestampMessage &ts, bool is_abort, bool non_blocking){ //Non_blocking makes it so that the request is schedules asynchronously, i.e. does not block calling function
  Debug("Check whether any queries are waiting on txn %s", BytesToHex(txnDigest, 16).c_str());
  if(params.query_params.optimisticTxID){ //Wake both Queries that use normal tx-ids (e.g. retries) and queries that use optimistic Id's
    uint64_t txnTS(MergeTimestampId(ts.timestamp(), ts.id()));
    //Wake waiting queries.
    if(params.mainThreadDispatching && (params.query_params.parallel_queries || non_blocking)){   //if mainThreadDispatching = true then dispatchMessageReceive = false
        //Dispatch job to worker thread (since it may wake and excute sync)
        auto f = [this, txnTS, txnDigest, is_abort]() mutable {
            Debug("Dispatch UpdateWaitingQueries(%s) to a worker thread.", BytesToHex(txnDigest, 16).c_str());
            UpdateWaitingQueries(txnDigest, is_abort);
            UpdateWaitingQueriesTS(txnTS, txnDigest, is_abort);
            return (void*) true;
        };
        if(params.query_params.parallel_queries) transport->DispatchTP_noCB(std::move(f));
        else if(non_blocking) transport->DispatchTP_main(std::move(f));
    }
    else{
        Debug("Calling UpdateWaitingQueries(%s) on same thread.", BytesToHex(txnDigest, 16).c_str());
        UpdateWaitingQueries(txnDigest, is_abort);
        UpdateWaitingQueriesTS(txnTS, txnDigest, is_abort);
    }
  }
  else{
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

//TODO: Alternatively, set query_id field in request missing and supply missing and wake only respective query. That might be easier to debug.
//TODO: If Tx gets locally committed/prepared/abstained ignore it, and wait for SupplyTxn reply anyways ==> This is slower/less optimal, but definitely simpler to implement at first.

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
    //Note that it is guaranteed for a waitingQuery to wake up, because whenever ProcessSync registers a waitingQuery, it also sends out a new RequestTx message. Upon receiving a reply, UpdateWaitingQueries will be called.
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
         Debug("Some Queries are waiting on txn_id %s", BytesToHex(txnDigest, 16).c_str());
        for(const std::string &waiting_query : w->second){

             //2) update their missing data structures
            // Lookup queryMissingTxns ... => mark map of "waking query + retry_version" ==> after releasing w try to lock them all.
            queryMissingTxnsMap::accessor qm;
            if(!queryMissingTxns.find(qm, waiting_query)) continue; //releases qm implicitly (or rather: qm is not actually held)

            MissingTxns &missingTxns = qm->second;

            bool was_present = missingTxns.missing_txns.erase(txnDigest); 
            //Cornercase: What if we clear missing queries (ro Retry Sync) and then UpdateWaiting triggers. ==> was_present should be false => won't call Update
           
            Debug("QueryId[%s] is still waiting on (%d) transactions", BytesToHex(missingTxns.query_id, 16).c_str(), missingTxns.missing_txns.size());
            if(was_present && missingTxns.missing_txns.empty()){ 
                   //Note: was_present -> only call this the first time missing_txn goes empty: present captures the fact that map was non-empty before erase.
                queries_to_wake[missingTxns.query_id] = missingTxns.retry_version;
            }
            else if(is_abort) queries_to_rm_txn[missingTxns.query_id] = missingTxns.retry_version;
            qm.release();
        }
        //3) remove key from waiting data structure if no more queries waiting on it to avoid key set growing infinitely...
        waitingQueries.erase(w);
    }
    w.release();

    //4) Try to wake all ready queries.
    for(auto &[queryId, retry_version]: queries_to_wake){
        queryMetaDataMap::accessor q;
        bool queryActive = queryMetaData.find(q, queryId);
        if(queryActive){
                QueryMetaData *query_md = q->second;

                //6) Erase txn from snapshot if abort.
                query_md->merged_ss_msg->mutable_merged_txns()->erase(txnDigest); 
               
                //5) if query is waiting and retry_version is still current: Start Callback.
                Debug("Stored Query: %s has retry version %lu, and is %s waiting", BytesToHex(queryId, 16).c_str(), query_md->retry_version, query_md->is_waiting ? "" : "not");
                if(query_md->is_waiting && query_md->retry_version == retry_version){ 
                     //Note: is_waiting -> make sure query is waiting. E.g. missing_txn could be empty because we re-tried the query and now are not missing any. In this case is_waiting will be set to false. -> no need to call callback
                     // (Note: This is handled by retry_version check now. Can remove is_waiting.)
                     Debug("Waking Query[%lu:%lu:%lu]", query_md->query_seq_num, query_md->client_id, query_md->retry_version);
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
                query_md->merged_ss_msg->mutable_merged_txns()->erase(txnDigest); 
            }
        }
        q.release();
    }
}

//Note: For waitingTS: call updateWaitingQueries with Ts:  --> Then: Set snapshot for all Ts --> When snapshot ready: All txn_ids are available. 
//(Alternatively: Could transform later to save lookups to query_md inside UpdateWaitingQueries. But it's less clean if later layers need to be aware of TS sync still.)
    //In this case: During sync callback: For every Ts in merged_ts ==> lookup tx and add to mergedTS.
void Server::UpdateWaitingQueriesTS(const uint64_t &txnTS, const std::string &txnDigest, bool is_abort){

     Debug("Checking whether can wake all queries waiting on txn_id %s with ts_id %lu", BytesToHex(txnDigest, 16).c_str(), txnTS);

    std::map<std::string, uint64_t> queries_to_wake;
    std::map<std::string, uint64_t> queries_to_update_txn; //All queries (besides the ones we wake anyways) from whose snapshot we want to add/remove the txn. Note: This is just an optimization to not loop twice

     //1) find queries that were waiting on this txn-id
    waitingQueryTSMap::accessor w;
    bool hasWaiting = waitingQueriesTS.find(w, txnTS);
    if(hasWaiting){
        Debug("Some Queries are waiting on txn_id %s with ts_id %lu", BytesToHex(txnDigest, 16).c_str(), txnTS);
        for(const std::string &waiting_query : w->second){
            Debug("Query_retry_version %s waiting on txn_id %s with ts_id %lu", BytesToHex(waiting_query, 16).c_str(), BytesToHex(txnDigest, 16).c_str(), txnTS);
             //2) update their missing data structures
            // Lookup queryMissingTxns ... => mark map of "waking query + retry_version" ==> after releasing w try to lock them all.
            queryMissingTxnsMap::accessor qm;
            if(!queryMissingTxns.find(qm, waiting_query)) continue; //releases qm implicitly (or rather: qm is not actually held)

            MissingTxns &missingTxns = qm->second;

            bool was_present = missingTxns.missing_ts.erase(txnTS); 
            //Cornercase: What if we clear missing queries (ro Retry Sync) and then UpdateWaiting triggers. ==> was_present should be false => won't call Update
           
            Debug("QueryId[%s] is still waiting on (%d) transactions (TS)", BytesToHex(missingTxns.query_id, 16).c_str(), missingTxns.missing_ts.size());
            if(was_present && missingTxns.missing_ts.empty()){ 
                   //Note: was_present -> only call this the first time missing_txn goes empty: present captures the fact that map was non-empty before erase.
                queries_to_wake[missingTxns.query_id] = missingTxns.retry_version;
            }
            else{
                queries_to_update_txn[missingTxns.query_id] = missingTxns.retry_version;
            }
            qm.release();
        }
        //3) remove key from waiting data structure if no more queries waiting on it to avoid key set growing infinitely...
        waitingQueriesTS.erase(w);
    }
    w.release();

    //4) Try to wake all ready queries.
    for(auto &[queryId, retry_version]: queries_to_wake){
        Debug("Trying to wake Query: %s with retry version %lu", BytesToHex(queryId, 16).c_str(), retry_version);
        queryMetaDataMap::accessor q;
        bool queryActive = queryMetaData.find(q, queryId);
        if(queryActive){
                QueryMetaData *query_md = q->second;

                //6) Add txn to snapshot, or erase txn from snapshot if abort
                if(is_abort) query_md->merged_ss_msg->mutable_merged_txns()->erase(txnDigest);  //Note: Might have been added to snapshot because TS/Tx was ongoing/prepared. But now remove since it is aborted.
                                                                                            //However: We would never have added the txn to waiting.
                else{
                 (*query_md->merged_ss_msg->mutable_merged_txns())[txnDigest]; //(Just add with default constructor --> empty ReplicaList)
                }
            
                Debug("Stored Query: %s has retry version %lu, and is %s waiting", BytesToHex(queryId, 16).c_str(), query_md->retry_version, query_md->is_waiting ? "" : "not");
                //5) if query is waiting and retry_version is still current: Start Callback.
                if(query_md->is_waiting && query_md->retry_version == retry_version){ 
                     Debug("Waking Query[%lu:%lu:%lu]", query_md->query_seq_num, query_md->client_id, query_md->retry_version);
                     query_md->merged_ss_msg->clear_merged_ts(); //These are no longer needed.
                    //Note: is_waiting -> make sure query is waiting. E.g. missing_txn could be empty because we re-tried the query and now are not missing any. In this case is_waiting will be set to false. -> no need to call callback
                    HandleSyncCallback(q, query_md, queryId); //TODO: Should this be dispatched again? So that multiple waiting queries don't execute sequentially?
                }
        }
        q.release();
    }

    //6 For all other queries that are still waiting, but not ready to wake: Erase txn from snapshot if abort
    for(auto &[queryId, retry_version]: queries_to_update_txn){
        queryMetaDataMap::accessor q;
        bool queryActive = queryMetaData.find(q, queryId);
        if(queryActive){
            QueryMetaData *query_md = q->second;

            if(query_md->is_waiting && query_md->retry_version == retry_version){ 
                //6) Erase txn from snapshot if abort.
                if(is_abort) query_md->merged_ss_msg->mutable_merged_txns()->erase(txnDigest);  //Note: Might have been added to snapshot because TS/Tx was ongoing/prepared. But now remove since it is aborted.
                                                                                            //However: We would never have added the txn to waiting.
                else{
                 (*query_md->merged_ss_msg->mutable_merged_txns())[txnDigest]; //(Just add with default constructor --> empty ReplicaList)
                }
            }
        }
        q.release();
    }
    Debug("Completed all possible wake-ups for queries waiting on txn_id %s with ts_id %lu", BytesToHex(txnDigest, 16).c_str(), txnTS);
}


std::string Server::ExecQuery(QueryReadSetMgr &queryReadSetMgr, QueryMetaData *query_md, bool materialize){
    //TODO: Must take as input some Materialization info... (whether to use a materialized snapshot (details are in query_md), or whether to just use current state)
    //TODO: Must be able to report exec failure (e.g. materialized snapshot inconsistent) -- note that if eagerly executiong (no materialization) there is no concept of failure.

        /////////////////////////////////////////////////////////////
    //                                                         //
    //                                                         //
    //                                                         //
    //
    //              EXEC BLACKBOX -- TBD


       //If MVTSO: Read prepared, Set RTS
    std::string serialized_result;
    if(!materialize) serialized_result = table_store->ExecReadQuery(query_md->query_cmd, query_md->ts, queryReadSetMgr);
    if(materialize) Warning("Do not yet support Snapshot materialization");

    if(occType == MVTSO && params.rtsMode > 0){
        Debug("Set up all RTS for Query[%lu:%lu]", query_md->query_seq_num, query_md->client_id);
        for(auto &read: queryReadSetMgr.read_set->read_set()){
            SetRTS(query_md->ts, read.key());
        }
        //TODO: On Abort, Clear RTS.
    }

    //                                                         //
    //                                                         //
    //                                                         //
    //                                                         //
    /////////////////////////////////////////////////////////////
    //TODO: Result should be of protobuf result type: --> can either return serialized value, or result object (probably easiest) -- but need serialized value anyways to check for equality.

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

    //Just for testing: Creating Dummy result 
    if(TEST_QUERY){
  
        std::string test_result_string = "success" + std::to_string(query_md->query_seq_num);
        std::vector<std::string> result_row;
        result_row.push_back(test_result_string);
        sql::QueryResultProtoBuilder queryResultBuilder;
        queryResultBuilder.add_column("result");
        queryResultBuilder.add_row(result_row.begin(), result_row.end());

        std::string dummy_result = queryResultBuilder.get_result()->SerializeAsString();
        return dummy_result;
    }

    return serialized_result;
}

void Server::ExecQueryEagerly(queryMetaDataMap::accessor &q, QueryMetaData *query_md, const std::string &queryId){

    query_md->executed_query = true;

    Debug("Eagerly Execute Query[%lu:%lu:%lu].", query_md->query_seq_num, query_md->client_id, query_md->retry_version);
    QueryReadSetMgr queryReadSetMgr(query_md->queryResultReply->mutable_result()->mutable_query_read_set(), groupIdx, false); 

    std::string result(ExecQuery(queryReadSetMgr, query_md, false));

    if(TEST_QUERY){
        std::string toy_txn("toy_txn");
        Timestamp toy_ts(0, 2); //set to genesis time.
        sql::QueryResultProtoBuilder queryResultBuilder;
        queryResultBuilder.add_columns({"key_", "val_"});
        std::vector<std::string> result_row = {"alice", "blonde"};
        queryResultBuilder.add_row(result_row.begin(), result_row.end());
        result = queryResultBuilder.get_result()->SerializeAsString();
    }



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



//Note: WARNING: must be called while holding a lock on query_md. 
void Server::HandleSyncCallback(queryMetaDataMap::accessor &q, QueryMetaData *query_md, const std::string &queryId){

    Debug("Sync complete for Query[%lu:%lu]. Starting Execution", query_md->query_seq_num, query_md->client_id);
    query_md->is_waiting = false;
    
    // 1) Materialize Snapshot
    // Materialize all tx in snapshot: Loop through snapshot: If tx in prepared/committed -> do nothing (already implicitly materialized); If not, materialize it from ongoing. During exec --> if trying to use aborted tx ==> FailQuery.
        //Alternatively: Materialize full phyiscal table (instead of virtual as above) from everything in snapshot. ==> exec on that. (Con: Cannot determine whether exec missed newer commit; or read aborted)

             //TODO: Materialize tx from ongoing (doesnt matter if prepared yet) ==> Create another readable map (from key -> {(Timestamp, [value, eligible-list])). After exec, delete from eligible-list -- if empty, remove ts/val pair
                    // Can materialize during sync, or during exec only. Pro of doing it later: More tx might be prepared/committed/aborted; Con: Another loop.
                // during exec: Check commit/prepare; If not present -> materialize from ongoing. After all, this check + request missing guarantees that tx must be at least ongoing.
                //Note: if its not prepared locally, but is ongoing (i.e. prepare vote = none/abort/abstain) we can immediately add it to state but marked only for query
    
         //Materialization: Possible solution: During OCC check, Also "prepare" all tx that are locally abstained --> that way we can directly detect them as not necessary for sync. Mark them "invisible" by default.
            //Garbage collect for good from prepared map once it is aborted. -- Ignore Aborted Tx during materialization (We already remove them from snapshot during ProcessSync and UpdateWaiting)
                                                                    //Note: There might still be prepared Abstained/Aborted tx - but we currently do read those, since we call UpdateWaiting before the prepare result
            //Alternative: During exec, materialize all remaining items in snapshot. 

    // 2) Execute Query & Construct Read Set
    //Execute Query -- Go through store, and check if latest tx in store is present in syncList. If it is missing one (committed) --> reply EarlyAbort (tx cannot succeed). If prepared is missing, ignore, skip to next
    // Build Read Set while executing; Add dependencies on demand as we observe uncommitted txn touched.
    //read set = map from key-> versions  //Note: Convert Timestamp to TimestampMessage
 
    QueryReadSetMgr queryReadSetMgr(query_md->queryResultReply->mutable_result()->mutable_query_read_set(), groupIdx, query_md->useOptimisticTxId); 

    std::string result(ExecQuery(queryReadSetMgr, query_md, true));
    query_md->has_result = true; 
   

    //Note: Blackbox might do multi-replica coordination to compute result and full read-set (though read set can actually be reported directly by each shard...)
    //Client waits to receive SyncReply from all shards ==> with read set, or read set hash. ==> in Tx_manager (marked by query) reply also include the result
        //Always callback from shardclient to client, but only call-up from client to app if a) result has been received, b) all shards replied with read-set (or read-set hash)
        //-- want to do this so that Exec can be a better blackbox: This way data exchange might just be a small intermediary data, yet client learns full read set. 
            //In this case, read set hash from a shard is not enough to prove integrity to another shard (since less data than full read set might be exchanged)


    bool exec_success = !(TEST_FAIL_QUERY && query_md->retry_version == 0); //Global Test var to simulate a retry once. //FIXME: Remove
    if(exec_success){
         query_md->failure = false;
        
         if(query_md->designated_for_reply){
            query_md->queryResultReply->mutable_result()->set_query_result(result);
            //query_md->queryResult->set_query_result(dummy_result); //TODO: replace with real result
        }
        else{
            query_md->queryResultReply->mutable_result()->set_query_result(result); //set for non-query manager.
            //query_md->queryResult->set_query_result(dummy_result);
        }

         SendQueryReply(query_md);
    }
    else{
        FailQuery(query_md);
        TEST_FAIL_QUERY = false;
    }

    uint64_t retry_version = query_md->retry_version;
    q.release();

     //After executing and caching read set -> Try to wake possibly subscribed transaction that has started to prepare, but was blocked waiting on it's cached read set.
    if(params.query_params.cacheReadSet) wakeSubscribedTx(queryId, retry_version); //TODO: Instead of passing it along, just store the queryId...
}

void Server::SendQueryReply(QueryMetaData *query_md){ 

    proto::QueryResultReply *queryResultReply = query_md->queryResultReply;
    proto::QueryResult *result = queryResultReply->mutable_result();
    proto::ReadSet *query_read_set;
    //proto::LocalDeps *query_local_deps; //Deprecated --> made deps part of read set


    // 3) Generate Merkle Tree over Read Set, (optionally can also make it be over result, query id)

    bool testing_hash = false; //note, if this is on, the client will crash since it expects a read set but does not get one.
    if(testing_hash || params.query_params.cacheReadSet){
        std::sort(result->mutable_query_read_set()->mutable_read_set()->begin(), result->mutable_query_read_set()->mutable_read_set()->end(), sortReadSetByKey); //Note: Sorts by key to ensure all replicas create the same hash. (Note: Not necessary if using ordered map)
        result->set_query_result_hash(generateReadSetSingleHash(result->query_read_set()));
        //Temporarily release read-set and deps: This way we don't send it. Afterwards, re-allocate it. This avoid copying.
        query_read_set = result->release_query_read_set();
        //query_local_deps = result->release_query_local_deps();
        Debug("Read-set hash: %s", BytesToHex(result->query_result_hash(), 16).c_str());
       
        //query_md->result_hash = std::move(generateReadSetSingleHash(query_md->read_set));  
        //query_md->result_hash = std::move(generateReadSetMerkleRoot(query_md->read_set, params.merkleBranchFactor)); //by default: merkleBranchFactor = 2 ==> might want to use flatter tree to minimize hashes.
                                                                                                        //TODO: Can avoid hashing leaves by making them unique strings? "[key:version]" should do the trick?
        //Debug("Read-set hash: %s", BytesToHex(query_md->result_hash, 16).c_str());
    }
    

    //4) If Caching Read Set: Buffer Read Set (map: query_digest -> <result_hash, read set>) ==> implicitly done by storing read set + result hash in query_md 
   
    //5) Create Result reply --  // only include result if chosen for reply.

    result->set_query_seq_num(query_md->query_seq_num); //FIXME: put this directly when instantiating.
    result->set_client_id(query_md->client_id); //FIXME: set this directly when instantiating.
    result->set_replica_id(id);
    
    queryResultReply->set_req_id(query_md->req_id);

    //6) (Sign and) send reply 

     if (params.validateProofs && params.signedMessages) {
        //Debug("Sign Query Result Reply for Query[%lu:%lu]", query_reply->query_seq_num(), query_reply->client_id());
        Debug("Sign Query Result Reply for Query[%lu:%lu]", result->query_seq_num(), result->client_id());

        result = queryResultReply->release_result();   //Temporarily release result in order to sign.
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
             Debug("Sent Signed Query Resut for Query[%lu:%lu]", result->query_seq_num(), result->client_id());
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

        //Note: In this branch result is still part of queryResultReply; thus it suffices to only allocate back to result.
        if(params.query_params.cacheReadSet){ //Restore read set and deps to be cached
            result->set_allocated_query_read_set(query_read_set);
            //result->set_allocated_query_local_deps(query_local_deps);
        } 
    }

    if(TEST_READ_SET){
      Debug("BEGIN READ SET:"); // just for testing
              
                for(auto &read : result->query_read_set().read_set()){
                //for(auto &[key, ts] : read_set){
                  //std::cerr << "key: " << key << std::endl;
                  Debug("Cached Read key %s with version [%lu:%lu]", read.key().c_str(), read.readtime().timestamp(), read.readtime().id());
                  //Debug("[group %d] Read key %s with version [%lu:%lu]", group, key.c_str(), ts.timestamp(), ts.id());
                }
              
       Debug("END READ SET.");
    }

    return;

}

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

//TODO: Compile CleanQueries. 

//TODO: Clean Query as part of HandleAbort
// -> handle abort should specify list of query_ids to just be deleted. (erase fully)

void Server::CleanQueries(proto::Transaction *txn, bool is_commit){
  //Move read sets into txn + Remove QueryMd completely. Store a map: <client-id, timestamp> disallowing clients to issue requests to the past (this way old/late queries won't be accepted anymore.)
    
  if(!txn->has_last_query_seq()) return;
  Debug("Clean all Query Md associated with txn");

  clientQueryWatermarkMap::accessor qw;
  clientQueryWatermark.insert(qw, txn->client_id());
  if(txn->last_query_seq() > qw->second) qw->second = txn->last_query_seq();
  qw.release();
  //clientQueryWatermark[txn->client_id()] = txn->last_query_seq(); //only update timestamp for commit if greater than last one... //To do this atomically need hashmap lock.

  //For every query in txn: 
  for(proto::QueryResultMetaData &tx_query_md : *txn->mutable_query_set()){
    queryMetaDataMap::accessor q;
    bool hasQuery = queryMetaData.find(q, tx_query_md.query_id());
    if(hasQuery){
    QueryMetaData *local_query_md = q->second; //Local query_md

    //Move read set if caching. Note: Don't need to move read_set_hash -> tx already stores it. 
    if(is_commit && params.query_params.cacheReadSet){
        proto::QueryGroupMeta &query_group_meta = (*tx_query_md.mutable_group_meta())[groupIdx];
        //Note: only move if read_set hash matches. It might not. But at least 2f+1 correct replicas do have it matching.
        if(query_group_meta.read_set_hash() == local_query_md->queryResultReply->result().query_result_hash()){
            proto::ReadSet *read_set = local_query_md->queryResultReply->mutable_result()->release_query_read_set();
            query_group_meta.set_allocated_query_read_set(read_set);
        }
         //Try to clear RTS in case it was moved:  //TODO: For commit: Could optimize RTS GC to remove all RTS >= committed TS (see CommitToStore) 
        ClearRTS(query_group_meta.query_read_set().read_set(), local_query_md->ts);
    }
    else if(params.query_params.cacheReadSet){ //Try to clear RTS in case it was cached 
        ClearRTS(local_query_md->queryResultReply->result().query_read_set().read_set(), local_query_md->ts);
    }   
    else{  //Try to clear RTS in case tx had it all along: 
        proto::QueryGroupMeta &query_group_meta = (*tx_query_md.mutable_group_meta())[groupIdx];
        ClearRTS(query_group_meta.query_read_set().read_set(), local_query_md->ts);
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



// void Server::FindSnapshot(){

// }

// void Server::Materialize(){

// }

// void Server::ExecuteQuery(){

// }

// void Server::ParseQuery(){

// }

} // namespace pequinstore
