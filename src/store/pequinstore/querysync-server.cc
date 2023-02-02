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

namespace pequinstore {

       //TODO: Next:
       //Query Exec engine: 
       //   a) Server parses, scans, and replies with real sync state 
       //   b) Server receives snapshot, materializes, executes and replies.
       //Query Concurrency Control ==> part of Tx, part of TxDigest, part of MVTSO check.

       //TODO: Add Handler for QueryRetry:
       // Re-do sync and exec on same query id. (Update req id)
            //If receive new sync set for query that already exists, replace it (this is a client issued retry because sync failed.);
            // problem: byz could abuse this to retry only at some replicas --> resulting in different read sets
            //TODO: FIXME: Need new Query ID for retries? -- or same digest (supplied by client and known in advance) --> for now just use single one (assuming I won't simulate a byz attack); that way garbage collection is easier when re-trying a tx.
            // --> Client should be able to specify in its Tx which retry number (version) of its query attempts it wants to use. If replicas have a different one cached than submitted then this is a proof of misbehavior
                                                                                                                                // With FIFO channels its always guaranteed to arrive before the prepare

                // problem: byz does not have to retry if sync fails --> replicas may have different read sets --> some may prepare and some may abort. (Thats ok, indistinguishable from correct one failing tx.)
                            //importantly: cannot fail sync on purpose ==> will either be detectable (equiv syncMsg or Query), or 

    //TODO: If using optimistic Ids'
        // If optimistic ID maps to 2 txn-ids --> report issuing client (do this when you receive the tx already); vice versa, if we notice 2 optimistic ID's map to same tx --> report! 
        // (Can early abort query to not waste exec since sync might fail- or optimistically execute and hope for best) --> won't happen in simulation (unless testing failures)


//Server handling 
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

     // 2) Authenticate Query Signature if applicable. Compute unique hash ID 
    std::string queryId;
    
    if(params.query_params.signClientQueries && params.query_params.cacheReadSet){ //TODO: when to use hash id? always?
        queryId = QueryDigest(*query, params.hashDigest); 
    }
    else{
        queryId =  "[" + std::to_string(query->query_seq_num()) + ":" + std::to_string(query->client_id()) + "]";
    }
     Debug("\n Received Query Request Query[%lu:%lu:%d] (seq:client:ver)", query->query_seq_num(), query->client_id(), query->retry_version());
   
    //TODO:  //if already issued query reply, reply with cached val; else if new, or retry set, re-compute

    if(params.query_params.signClientQueries){  //TODO: Not sure if sigs necessary: authenticated channels (for access control) and hash ids (for uniqueness/non-equivocation) should suffice. NOTE: non-equiv only necessary if caching read set.
        if(!VerifyClientQuery(msg, query, queryId)){ // Does not really need to be parallelized, since query handling is probably already on a worker thread.
            delete query;
            return;
        }
    }

    //Buffer Query content and timestamp
    queryMetaDataMap::accessor q;
    bool newQuery = queryMetaData.insert(q, queryId);
    if(newQuery){ 
        q->second = new QueryMetaData(query->query_cmd(), query->timestamp(), remote, msg.req_id(), query->query_seq_num(), query->client_id());
        //Note: Retry will not contain query_cmd again.
    }
    QueryMetaData *query_md = q->second;
    if(!query_md->has_query){ //If query was not newQuery (e.g. Sync set md first), set query.
        query_md->SetQuery(query->query_cmd(), query->timestamp(), remote, msg.req_id());
    }

    if(query->retry_version() > query_md->retry_version){
        query_md->req_id = msg.req_id();
        query_md->retry_version = query->retry_version();
        query_md->started_sync = false; //start new sync round
        query_md->ClearMetaData();
    }
    else if(query->retry_version() == query_md->retry_version){
        if(!newQuery){ //ignore if already processed query once (i.e. don't exec twice for query version 0)
            if(params.query_params.signClientQueries) delete query;

            if(query_md->has_result){
                //TODO: Reply directly with result for current version.
            }
            Panic("Duplicate query Request for current retry version");
            delete query;
            return;
        } 
    }
    else{
        Panic("Requesting Query with outdating retry version");
        return;
    }


    if(msg.designated_for_reply() && !query_md->waiting_sync){
        ProcessQuery(q, remote, query, query_md);
    }
    else{  //If not designated for reply, or sync is already waiting -> no need to process query. //TODO: In this case shouldn't send Query separately at all -> Send it together with Sync and add to q_md then.
        delete query;
        if(query_md->waiting_sync){ //Wake waiting Sync
            UW_ASSERT(query_md->merged_ss_msg != nullptr);
            ProcessSync(q, *query_md->original_client, query_md->merged_ss_msg, &queryId, query_md);
        }
    }
    //q automatically released
    if(params.mainThreadDispatching && (!params.dispatchMessageReceive || params.query_params.parallel_queries)) FreeQueryRequestMessage(&msg);
   
}
void Server::ProcessQuery(queryMetaDataMap::accessor &q, const TransportAddress &remote, proto::Query *query, QueryMetaData *query_md){
    
    // 3) Parse Query
    // const std::string &query_cmd = query->query_cmd();
    // Timestamp ts(query->timestamp);


    //TODO: Insert Hyrise parsing or whatever here...
    // SQL glue. How to execute from query plan object.

    /////////////////////////////////////////////////////////////
    //                                                         //
    //                                                         //
    //                                                         //
    //
    //              EXEC BLACKBOX -- TBD
    //                                                         //
    //                                                         //
    //                                                         //
    //                                                         //
    /////////////////////////////////////////////////////////////

    std::unordered_set<std::string> &local_txns = query_md->local_ss; //Store this as protobuf directly? that avoids copy.   Or: copy as part of "close" function that can be parametarized to compress ids. 
    //TODO: move to separate file. inputs to add: "tx_id, txn". close returns: compressed vector. open returns uncompressed vector. iterate should return tx-id if vector contains ts.

    // 4) Execute all Scans in query --> find txnSet (for each key, last few tx)
    // FindSnapshot(local_txns, query_cmd);


    // 5) Create list of all txn-ids necessary for state
            // if txn exists locally as committed and prepared, only include as committed
            // Use optimistic Tx-ids (= timestamp) if param set

    //FIXME: Toy insert -- real tx-ids are cryptographic hashes of length 256bit = 32 byte.
    // std::string test_txn_id = "[test_id_of_length_32 bytes----]";
    // proto::CommittedProof *test_proof = new proto::CommittedProof();
    // local_txns.insert(test_txn_id);
    //committed[test_txn_id] = test_proof;  //this should allow other replicas to find it during sync.; but validation of commit proof will fail. Note: Will probably fail Panic because fields not set.
    
    //TESTING.
    for(auto const&[tx_id, proof] : committed){
        local_txns.insert(tx_id);
         Debug("Proposing txn_id [%s] for local Query Sync State[%lu:%lu:%d]", BytesToHex(tx_id, 16).c_str(), query->query_seq_num(), query->client_id(), query->retry_version());
    }
    //committed[test_txn_id] = test_proof;  //this should allow other replicas to find it during sync.; but validation of commit proof will fail. Note: Will probably fail Panic because fields not set.

    
    //query_md->local_ss.insert("test_id_of_length_32 bytes------");

      //How to find txnSet efficiently for key WITH RESPECT to Timestamp. Is scanning the only option?
    //Could already store a whole tx map for each key: map<key, deque<TxnIds>> --? replace tx_ids evertime a newer one comes along (pop front, push_back). 
    // Problem: May come in any TS order. AND: Query with TS only cares about TxId < TS


    // 6) Compress list (only applicable if using optimistic IDs)
    if(params.query_params.optimisticTxID){
        //CompressTxnIds(list)
        //Note: Format of optimistic tx ids is uint64_t, not string. (can cast)
        //Output: uint32_t or smaller.
    }

    // 7) Send list in SyncReply
    proto::SyncReply *syncReply = new proto::SyncReply(); //TODO: change to GetUnused
    syncReply->set_req_id(query_md->req_id);

    proto::LocalSnapshot *local_ss = syncReply->mutable_local_ss();
    local_ss->set_query_seq_num(query->query_seq_num());
    local_ss->set_client_id(query->client_id());
    local_ss->set_replica_id(id);
    *local_ss->mutable_local_txns_committed() = {local_txns.begin(), local_txns.end()}; //TODO: can we avoid this copy? --> If I move it, then we cannot cache local_txns to edit later.

    q.release();

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
    //if(params.mainThreadDispatching && (!params.dispatchMessageReceive || params.query_params.parallel_queries)) FreeQueryRequestMessage(&msg);
    //if(params.query_params.signClientQueries) delete query;
    delete query;
}

bool Server::VerifyClientQuery(proto::QueryRequest &msg, const proto::Query *query, std::string &queryId)
    {

       Debug("Verifying Client Query: %s", BytesToHex(queryId, 16).c_str());

         //1. check Query.TS.id = client_id (only signing client should claim this id in timestamp
         if(query->timestamp().id() != msg.signed_query().process_id()){
            Debug("Client id[%d] does not match Timestamp with id[%d] for txn %s", 
                   msg.signed_query().process_id(), query->timestamp().id(), BytesToHex(queryId, 16).c_str());
           // if((params.mainThreadDispatching && (!params.dispatchMessageReceive || params.parallel_CCC)) || (params.multiThreading && params.signClientProposals)) FreePhase1message(&msg);
           
            return false;
         }

         //2. check signature matches txn signed by client (use GetClientID)
         if (!client_verifier->Verify(keyManager->GetPublicKey(keyManager->GetClientKeyId(msg.signed_query().process_id())), msg.signed_query().data(), msg.signed_query().signature())) {
              Debug("Client signatures invalid for query %s", BytesToHex(queryId, 16).c_str());
            //if((params.mainThreadDispatching && (!params.dispatchMessageReceive || params.parallel_CCC)) || (params.multiThreading && params.signClientProposals)) FreePhase1message(&msg);
            return false;
          }

          Debug("Client verification successful for query %s", BytesToHex(queryId, 16).c_str());
      return true;
    
    }



 //TODO: Compute query Digest always?
 
void Server::HandleSync(const TransportAddress &remote, proto::SyncClientProposal &msg){
    // 1) Parse Message
     proto::MergedSnapshot *merged_ss;
     const std::string *queryId;
     std::string query_id;

  
    if(params.query_params.signClientQueries && params.query_params.cacheReadSet){
        merged_ss = new proto::MergedSnapshot(); //TODO: replace with GetUnused
        merged_ss->ParseFromString(msg.signed_merged_ss().data());
        queryId = merged_ss->mutable_query_digest();
    }
    else{
        merged_ss = msg.release_merged_ss();
        query_id =  "[" + std::to_string(merged_ss->query_seq_num()) + ":" + std::to_string(merged_ss->client_id()) + "]";
        queryId = &query_id;
    }
    Debug("\n Received Query Sync Proposal for Query[%lu:%lu:%d] (seq:client:ver)", merged_ss->query_seq_num(), merged_ss->client_id(), merged_ss->retry_version());

    //FIXME: Message needs to include query hash id in order to locate the query state cached (e.g. local snapshot, intermediate read sets, etc.)
    //For now, can also index via (client id, query seq_num) pair. Just define an ordering function for query id pair.
     //TODO:  //if already issued query reply, reply with cached val; else if new, or retry set, re-compute
    queryMetaDataMap::accessor q;
    if(queryMetaData.insert(q, *queryId)){
        q->second = new QueryMetaData(merged_ss->query_seq_num(), merged_ss->client_id());
    }
    QueryMetaData *query_md = q->second;    

    query_md->designated_for_reply = msg.designated_for_reply();

    if(merged_ss->retry_version() > query_md->retry_version){ 
        query_md->req_id = msg.req_id();
        query_md->retry_version = merged_ss->retry_version();
        query_md->started_sync = true;
        query_md->ClearMetaData();
    }
    else if(merged_ss->retry_version() == query_md->retry_version){ //if current version -- only process if have not yet done sync. If we have result, reply with it early.

        if(query_md->has_result){
            //TODO: Reply directly with result if the submitted version is the current.
            //if(params.query_params.signClientQueries && params.query_params.cacheReadSet) delete merged_ss;
            delete merged_ss; 
            if(params.mainThreadDispatching && (!params.dispatchMessageReceive || params.query_params.parallel_queries)) FreeSyncClientProposalMessage(&msg);
            Panic("current retry versionhas result already. TODO: Reply with result.");
            return;
        }

         if(query_md->started_sync){ //ignore if already processed query once (i.e. don't exec twice for query version 0)
             //if(params.query_params.signClientQueries && params.query_params.cacheReadSet) delete merged_ss;
            delete merged_ss; 
            if(params.mainThreadDispatching && (!params.dispatchMessageReceive || params.query_params.parallel_queries)) FreeSyncClientProposalMessage(&msg);
            Panic("duplicate sync request for current retry version");
            return;
        } 
        query_md->started_sync = true;
    }
    else{
        Panic("outdated query sync request.");
        //FIXME: Note: Any interested client that learns of prepared dependency will learn correct version. Problem: byz client couldve updated version at other replicas in the meantime.
        //To avoid them defaulting, they must be able to either a) keep older versions read-sets, or b) (better) accept older versions read set on demand IF supported by Prepare msg that picks version. (ONLY accept older version in this case)
        //Note: If byz client prepares with version (v), but before that updated the version to v'>v ==> proof of misbehavior (report client, and abort by default -- in this case thats ok).
    }
    
    

    // 2) Authenticate Client      
    if(params.query_params.signClientQueries && params.query_params.cacheReadSet){ //TODO: need it to be signed not only for read set equiv, but so that only original client can send this request. Authenticated channels may suffice.
        if(!VerifyClientSyncProposal(msg, *queryId)){ // Does not really need to be parallelized, since query handling is probably already on a worker thread.
            delete merged_ss;
            if(params.mainThreadDispatching && (!params.dispatchMessageReceive || params.query_params.parallel_queries)) FreeSyncClientProposalMessage(&msg);
            Panic("Invalid client signature");
            return;
        }
    }

    if(query_md->has_query){
        ProcessSync(q, remote, merged_ss, queryId, query_md);
    }
    else{ //Wait for Query to arrive first.
        query_md->SetSync(merged_ss, remote);
    }
   
    if(params.mainThreadDispatching && (!params.dispatchMessageReceive || params.query_params.parallel_queries)) FreeSyncClientProposalMessage(&msg);

}
void Server::ProcessSync(queryMetaDataMap::accessor &q, const TransportAddress &remote, proto::MergedSnapshot *merged_ss, const std::string *queryId, QueryMetaData *query_md) { 
    // 3) Request any missing transactions (via txid) & add to state
            // Wait for up f+1 replies for each missing. (if none successful, then client must have been byz. Vote Early abort (if anything) and report client.)

    query_md->missing_txn.clear();

    std::map<uint64_t, proto::RequestMissingTxns> replica_requests = {};

    //txn_replicas_pair
    for(auto const &[tx_id, replica_list] : merged_ss->merged_txns_committed()){

        Debug("Snapshot for Query Sync Proposal[%lu:%lu:%d] contains tx_id [%s]", merged_ss->query_seq_num(), merged_ss->client_id(), merged_ss->retry_version(), BytesToHex(tx_id, 16).c_str());
         //TODO: 0) transform txn_id to txnDigest if using optimistc ids..
         // Check local mapping from Timestamp to TxnDigest (TO CREATE)

        query_md->merged_ss.insert(tx_id); //store snapshot locally.  // Is there a nice way to copy the whole key set of a map? //TODO: Replace with protobuf RepeatedField and release from merged_ss.
      
        bool has_txn_locally;
        //has_txn_locally = !query_md->local_ss.count(tx_id);
          //for all txn-ids that are in merged_ss but NOT in local_ss  //Should check current state instead of local snapshot... might have updated since (this would avoid some wasteful requests).
                                                                
        ongoingMap::const_accessor o;
        //Check whether replica has the txn.: If not ongoing, and not commit/abort --> then we have not received it yet. (Follows from commit/aborted being updated before erasing from ongoing)
        has_txn_locally = ongoing.find(o, tx_id)? true : (committed.find(tx_id) != committed.end() || aborted.find(tx_id) != aborted.end());
        o.release();
        // //FIXME: CURRENTLY NOT USING FAILQuery here: Failed tx might not be on frontier... -> Fail only during exec. Just proceed here (mark tx as "has_locally")
        // has_txn_locally = ongoing.find(o, tx_id);
        // o.release();
        // if(!has_txn_locally){
        //     if (committed.find(tx_id) != committed.end()) has_txn_locally = true;
        //     else if (aborted.find(tx_id) != aborted.end()){  //If Query not ongoing/committed --> Fail Query early if aborted. 
        //         FailQuery(query_md); 
        //         delete merged_ss;
        //         return;
        //     }
        // }
       
        //TODO: during exec: Check commit/prepare; If not present -> materialize from ongoing. After all, this check + request missing guarantees that tx must be at least ongoing.
        //Note: if its not prepared locally, but is ongoing (i.e. prepare vote = none/abort/abstain) we can immediately add it to state but marked only for query
        
        bool testing_sync = false;
        if(testing_sync || has_txn_locally){ //FIXME: Check ongoing and commit/abort --> if not prepared, need to materialize it anyways.
            //request the tx-id from the replicas that supposedly have it --> put all tx-id to be requested from one replica in one message (and send in one go afterwards)

              Debug("Missing txn_id [%s] for Query Sync Proposal[%lu:%lu:%d]", BytesToHex(tx_id, 16).c_str(), merged_ss->query_seq_num(), merged_ss->client_id(), merged_ss->retry_version());
            query_md->is_waiting = true;

             //Add to waiting data structure.
            waitingQueryMap::accessor w;
            waitingQueries.insert(w, tx_id);
            bool already_requested = !w->second.empty(); //TODO: if there is already a waiting query, don't need to request the txn again. Problem: Could have been requested by a byz client that gave wrong replica_ids...
            w->second.insert(*queryId);
            w.release();

            query_md->missing_txn[tx_id]= config.f + 1;  //FIXME: Useless line: we don't stop waiting currently.
            uint64_t count = 0;
            for(auto const &replica_id: replica_list.replicas()){ 
                if(count > config.f +1) return; //only send to f+1 --> an honest client will never include more than f+1 replicas to request from. --> can ignore byz request.
               
                uint64_t replica_idx = replica_id % config.n;  //since  id = local-groupIdx * config.n + idx
                if(replica_idx != idx){
                   std::string *next_txn = replica_requests[replica_idx].add_missing_txn();
                   *next_txn = tx_id;
                   replica_requests[replica_idx].set_replica_idx(idx);
                }
                count++;
            }
          
        }
    }
  
    //If no missing_txn = already fully synced. Exec callback direclty
    if(replica_requests.empty()){
        HandleSyncCallback(query_md, *queryId);
        q.release();
       
    }
    else{
        q.release();
        Debug("Sync State incomplete for Query[%lu:%lu:%d]", merged_ss->query_seq_num(), merged_ss->client_id(), merged_ss->retry_version()); 
        //if there are missng txn, i.e. replica_requests not empty ==> send out sync requests.
        for(auto const &[replica_idx, replica_req] : replica_requests){
            if(replica_idx == idx) Panic("Should never request from self");
            // if(rcv_count == 0){ //TESTING: Ensure we only send sync request once 2 queries are waiting.
            //      std::cerr << "WAITING FOR SECOND QUERY" << std::endl; rcv_count++; return;
            // }
            //  std::cerr << "REQUESTING FOR BOTH QUERIES" << std::endl;
            transport->SendMessageToReplica(this, groupIdx, replica_idx, replica_req);
            Debug("Replica %d Request Data Sync from replica %d", replica_req.replica_idx(), replica_idx); 
            // for(auto const& txn : replica_req.missing_txn()){ std::cerr << "Requesting txn : " << (BytesToHex(txn, 16)) << std::endl;}
        }
    }
      
     delete merged_ss; 
     //if(params.query_params.signClientQueries && params.query_params.cacheReadSet) delete merged_ss;
     //if(params.mainThreadDispatching && (!params.dispatchMessageReceive || params.query_params.parallel_queries)) FreeSyncClientProposalMessage(&msg);
     return;
}

bool Server::VerifyClientSyncProposal(proto::SyncClientProposal &msg, const std::string &queryId)
    {
       Debug("Verifying Client Sync Proposal: %s", BytesToHex(queryId, 16).c_str());

         if (!client_verifier->Verify(keyManager->GetPublicKey(keyManager->GetClientKeyId(msg.signed_merged_ss().process_id())), msg.signed_merged_ss().data(), msg.signed_merged_ss().signature())) {
              Debug("Client signatures invalid for sync proposal %s", BytesToHex(queryId, 16).c_str());
            //if((params.mainThreadDispatching && (!params.dispatchMessageReceive || params.parallel_CCC)) || (params.multiThreading && params.signClientProposals)) FreePhase1message(&msg);
            return false;
          }

          Debug("Client verification successful for query sync proposal %s", BytesToHex(queryId, 16).c_str());
      return true;
    
}

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

    for(auto const &txn_id : req_txn.missing_txn()){
         Debug("Replica %d is requesting txn_id [%s]", req_txn.replica_idx(), BytesToHex(txn_id, 16).c_str());
         //TODO: 0) transform txn_id to txnDigest if using optimistc ids..
         // Check local mapping from Timestamp to TxnDigest (TO CREATE)

        //1) If committed attatch certificate
        auto itr = committed.find(txn_id);
        if(itr != committed.end()){
            //copy committed Proof from committed list to map of tx replies -- note: committed proof contains txn.
            proto::CommittedProof *commit_proof = itr->second;
            //proto::TxnInfo &tx_info = (*supply_txn.mutable_txns())[txn_id];
            //*tx_info.mutable_commit_proof() = *commit_proof;

           *(*supply_txn.mutable_txns())[txn_id].mutable_commit_proof() = *commit_proof;
            Debug("Supplying committed txn_id %s", BytesToHex(txn_id, 16).c_str());
            continue;
        }

        //2) if abort --> mark query for abort and reply.
        //TODO:

        //3) if Prepared //TODO: check for prepared first, to avoid sending unecessary certs?

        //TODO: SHould be checking for ongoing (irrespective of prepared or not) ==> and include signature if necessary. 
        //==> Note: A correct replica (instructed by a correct client) will only request the transaction from replicas that DO have it prepared. So checking prepared is enough -- don't need to check ongoing

        preparedMap::const_accessor a;
        bool hasPrepared = prepared.find(a, txn_id);
        if(hasPrepared){
            //copy txn from prepared list to map of tx replies.
            proto::Phase1 *p1 = (*supply_txn.mutable_txns())[txn_id].mutable_p1();
            const proto::Transaction *txn = (a->second.second);

            if(params.signClientProposals){
                 p1MetaDataMap::const_accessor c;
                bool hasP1Meta = p1MetaData.find(c, txn_id);
                if(!hasP1Meta) Panic("Tx %s is prepared but has no p1MetaData entry (should be created during ProcessProposal-VerifyClientProposal)", BytesToHex(txn_id, 16).c_str());  //NOTE: P1 hasP1 (result) might not be set yet, but signed_txn has been buffered.
                //Note: signature in p1MetaData is only removed AFTER prepared is removed. Thus signed message must be present when we access p1Meta while holding prepared lock.
                *p1->mutable_signed_txn() = *(c->second.signed_txn); 
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
            continue;
        }
        a.release();

          //4) if neither --> Mark invalid return, and report byz client
        (*supply_txn.mutable_txns())[txn_id].set_invalid(true);
        Debug("Falsely requesting tx-id [%s] which replica %lu does not have committed or prepared locally", BytesToHex(txn_id, 16).c_str(), id);
        Panic("Testing Sync: Replica does not have tx.");
        break; //return;  //For debug purposes sending invalid reply.

    }

    //4) Use MAC to authenticate own reply
    if(true){
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

 //this will be called on a worker thread -- directly call Query handler callback.
 //Use supply tx to sync for multiple concurrent queries
 //TODO: Change waitingQueries map: If it already has an entry for a tx-id, then we DONT need to request it again.. Already in flight.
 //Currently processing/verifying duplicate supply messages.
void Server::HandleSupplyTx(const TransportAddress &remote, proto::SupplyMissingTxns &msg){

    //TODO: Waiting for up to f+1 replies? Currently just waiting indefinitely. --> how to GC?

    // 1) Parse Message
    proto::SupplyMissingTxnsMessage *supply_txn;
    // 2) Check MAC authenticator
    bool sign_reply = true;
    if(sign_reply){

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

   
    for(auto &[txn_id, txn_info] : *supply_txn->mutable_txns()){
        // std::string &txn_id = tx.first;
        // proto::TxnInfo &txn_info = tx.second;
        Debug("Trying to locally apply tx-id: [%s]", BytesToHex(txn_id, 16).c_str());
   
        //check if locally committed; if not, check cert and apply
       //FIXME: either update waiting data structures anyways; or add their update in Prepare/Commit function as well.
        bool testing_sync = false;
        auto itr = committed.find(txn_id);
        if(!testing_sync && itr != committed.end()){
            Debug("Already have committed tx-id: [%s]", BytesToHex(txn_id, 16).c_str());
            UpdateWaitingQueries(txn_id);   //TODO: Ideally should call UpdateWaiting Query directly in Commit function already/additionally --> Guarantees that queries wake up as soon as Commit happens, not once Supply happens
                                                         // If the Commit has already happened, then WaitingQueries for this tx is empty and nothing will happen.
            continue;
        }
        //check if locally aborted; if so, no point in syncing on txn: --> should mark this and reply to client with query fail (include tx that has abort vote + proof 
        //--> client can confirm that this is part of snapshot).. query is doomed to fai.
        auto itr2 = aborted.find(txn_id);
        if(itr2 != aborted.end()){
             Debug("Already have aborted tx-id: %s", BytesToHex(txn_id, 16).c_str());
            //Mark all waiting queries as doomed.
            //FailWaitingQueries(txn_id);
            continue;
        }

        if(txn_info.has_commit_proof()){

            Debug("Trying to commit tx-id: [%s]", BytesToHex(txn_id, 16).c_str());
            proto::CommittedProof *proof = txn_info.release_commit_proof();

            //TODO: it's possible for the txn to be in process of committing while entering this branch; that's fine safety wise, but can cause redundant verification. Might want to hold a lock to avoid (if it happens)

            bool valid;
            if (proof->txn().client_id() == 0UL && proof->txn().client_seq_num() == 0UL) {
                // Genesis tx are valid by default. TODO: this is unsafe, but a hack so that we can bootstrap a benchmark without needing to write all existing data with transactions
                // Note: Genesis Tx will NEVER by exchanged by sync since by definition EVERY replica has them (and thus will never request them) -- this branch is only used for testing.
                valid = true;
                 Debug("Accepted Genesis tx-id: [%s]", BytesToHex(txn_id, 16).c_str());
            }
            else{
                //Confirm that replica supplied correct transaction.     //TODO: Note: Since one should do this anyways, there is no point in storing txn_id as part of supply message.
                 if(txn_id != TransactionDigest(proof->txn(), params.hashDigest)){
                    Debug("Tx-id: [%s], TxDigest: [%s]", txn_id, TransactionDigest(proof->txn(), params.hashDigest));
                    Panic("Supplied Wrong Txn for given tx-id");
                 }
                 //Confirm that proof of transaction commit is valid.
                 valid = ValidateCommittedProof(*proof, &txn_id, keyManager, &config, verifier); //TODO: MAke this Async ==> Requires rest of code (CommitWithProof/UpdateWaiting) to go into callback.
                                                                                                                    //E.g.  AsyncValidateCommittedProof(mcb = {CommitWithProof, UpdateWaiting})
                //asyncValidateCommittedProof(*proof, &txn_id, keyManager, &config, verifier, mcb, transport, params.multiThreading, params.batchVerification);  //FIXME: Must pass copy of txn_id.
    

                if(!valid){
                    delete proof;
                    Panic("Commit Proof not valid");
                    return;
                }

                 Debug("Validated Commit Proof for tx-id: [%s]", BytesToHex(txn_id, 16).c_str());

                CommitWithProof(txn_id, proof);
            }
          


            UpdateWaitingQueries(txn_id); //TODO: want this to be dispatched/async (or rather: Want Callback to be. Note: Take care of accessors)
            continue;
        } 

        // NOTE: CURRENTLY WILL NOT YET SUPPORT READING PREPARED TRANSACTIONS ==> All code below here should never be called
        Panic("Not yet supporting prepared reads for queries");

        //2) if Prepared //TODO: check for prepared first, to avoid sending unecessary certs?

        proto::ConcurrencyControl::Result result;
        const proto::CommittedProof *conflict;

        p1MetaDataMap::const_accessor c;
        bool hasP1result = p1MetaData.find(c, txn_id) ? c->second.hasP1 : false;
        if(hasP1result){
            result = c->second.result; //p1Decisions[txnDigest];
            conflict = c->second.conflict;
             c.release();

             Debug("Already have prepared tx-id: [%s] with result %d", BytesToHex(txn_id, 16).c_str(), result);
             continue;

            //TODO: If commit, skip;
            if(result == proto::ConcurrencyControl::COMMIT) continue; //TODO: UpdateWaitingQueries 
            else if(result == proto::ConcurrencyControl::ABORT){
                //Mark all waiting queries as failed.
                //FailWaitingQueries(txn_id);
                continue;
            }
            else if(result == proto::ConcurrencyControl::ABSTAIN){
                //TODO: somehow add tx to store, and mark it as "viewable" only for waitingQueries --> //FIXME: if a new query arrives that also wants to see this tx --> edit the marker to make it visible too.
                // //TODO: Shouldnt even sync on the tx if it is locally prepared Abstain --> directly make visible. 
                //FIXME: Easiest solution: During OCC check, Also "prepare" all tx that are locally abstained --> that way we can directly detect them as not necessary for sync. Mark them "invisible" by default.
                //Garbage collect for good from prepared map once it is aborted.
            }
            //If abstain, apply only for query
            //if abort, vote early to abort query.       //if(result == proto::ConcurrencyControl::ABORT)
        }

        c.release();
         //check if locally prepared; if not do OCC check. --> If not successful, apply tx anyways with special marker only useable by marked queries.
        if(txn_info.has_p1()){

            Debug("Trying to prepare tx-id: [%s]", BytesToHex(txn_id, 16).c_str());
            //TODO: Handle incoming p1 as a normal P1 and after it is done, Update Waiting Queries. ==> If update waiting queries is done as part of Prepare (whether visible or invisible) nothing else is necessary)
            proto::Phase1 *p1 = txn_info.release_p1();

              if(txn_id != TransactionDigest(p1->txn(), params.hashDigest)){
                    Debug("Tx-id: [%s], TxDigest: [%s]", txn_id, TransactionDigest(p1->txn(), params.hashDigest));
                    Panic("Supplied Wrong Txn for given tx-id");
                 }

            //Call OCC check for P1. -- 

            //FIXME: Can hack CC such that if the request is of type isGossip, it tries to call Update Waiting Queries once result is known; --> then can multithread no problem.
            //FIXME: Alternatively Modify HandlePhase1 to not use multithreading here -- but that would hurt concurrency since its now on the mainthread.
            HandlePhase1(remote, *p1);
            //FIXME: WARNING!!! HandlePhase1 is only allowed to be called on MainThread!!! --> The whole SupplyTxn handler must be called on MainThread. -->QueryExec can then be dispatched again
            //(this makes sense, since supply txn is effectively a Commit or P1)
        }
        else if (txn_info.has_invalid()){
            //TODO: Edit into handler for replica reporting that it doesnt have either. False request. 
            //FIXME: currently waitingQueries doesn't distinguish individual queries: some may have had honest clients, others not. ==> Just add req_id that invoked Req/Supply
            Panic("Invalid Supply Txn: Replica didn't have requested txn"); //Panicing for debug purposes only. Just return normally.
            return;
        }
        else{
            Panic("Ill-formed supply TxnProof");
        }
            
    }

    if(sign_reply) delete supply_txn;
    if(params.mainThreadDispatching && !params.dispatchMessageReceive) FreeSupplyTxMessage(&msg);
    return;

}

void Server::CommitWithProof(const std::string &txnDigest, proto::CommittedProof *proof){

    proto::Transaction *txn = proof->mutable_txn();
    //std::string txnDigest(TransactionDigest(*txn, params.hashDigest));

    Timestamp ts(txn->timestamp());
    Value val;
    val.proof = proof;

    committed.insert(std::make_pair(txnDigest, proof)); //Note: This may override an existing commit proof -- that's fine.

    CommitToStore(proof, txn, txnDigest, ts, val);

    Debug("Calling CLEAN for committing txn[%s]", BytesToHex(txnDigest, 16).c_str());
    Clean(txnDigest);
    CheckDependents(txnDigest);
    CleanDependencies(txnDigest);

    //TODO: Add UpdateWaitingQueries here. (and in normal Commit too? --> Tricky since that commit is only on mainthread --> don't want it to call callback directly --> would want to dispatch)
}
            
//TODO: Alternatively, set query_id field in request missing and supply missing and wake only respective query. That might be easier to debug.
//TODO: If Tx gets locally committed/prepared/abstained ignore it, and wait for SupplyTxn reply anyways ==> This is slower/less optimal, but definitely simpler to implement at first.

//FIXME: WARNING: Possible Inverted lock order (accessors w and q) in this function and HandleSync. Should be fine though, since this function will only try to call a q for which a w was added;
                                                     //while HandleSync only calls each w once. I.e. HandleSync must lock&release w first, in order for this function to even request the same q.
void Server::UpdateWaitingQueries(const std::string &txnDigest){
    //when receiving a requested sync msg, use it to update waiting data structures for all potentially ongoing queries.
    //waiting queries are registered in map from txn-id to query id:

     Debug("Checking whether can wake all queries waiting on txn_id %s", BytesToHex(txnDigest, 16).c_str());
     //Notes on Concurrency liveness:
        //HandleSync will first lock q (queryMetaData) and then try to lock w (waitingQueries) in an effort to register a waitingQuery
        //UpdateWaitingQueries will first lock w (waitingQueries) and then try to lock q (queryMetaData) to wake waitingQueries
        //This does not cause a lock order inversion, because UpdateWaitingQueries only attempts to lock q if waitingQueries contains a registered transaction; which is only possible if HandleSync released both q and w
        //Note that it is guaranteed for a waitingQuery to wake up, because whenever HandleSync registers a waitingQuery, it also sends out a new RequestTx message. Upon receiving a reply, UpdateWaitingQueries will be called.
            //This is because a waiting query is registered and RequestTX is sent out even if the tx is locally committed after checking for missing, but before registering.

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
                bool was_present = query_md->missing_txn.erase(txnDigest);

                 Debug("Query[%lu:%lu] is still waiting on (%d) transactions", query_md->query_seq_num, query_md->client_id, query_md->missing_txn.size());

                //3) if missing data structure is empty for any query: Start Callback.
                if(was_present && query_md->is_waiting && query_md->missing_txn.empty()){ 
                    //Note: was_present -> only call this the first time missing_txn goes empty: present captures the fact that map was non-empty before erase.
                    //Note: is_waiting -> make sure query is waiting. E.g. missing_txn could be empty because we re-tried the query and now are not missing any. In this case is_waiting will be set to false. -> no need to call callback
                    HandleSyncCallback(query_md, waiting_query); //TODO: Should this be dispatched again? So that multiple waiting queries don't execute sequentially?
                }
            }
            q.release();
            //w->second.erase(waiting_query); //FIXME: Delete safely while iterating... ==> Just erase all after
        }
        //4) remove key from waiting data structure if no more queries waiting on it to avoid key set growing infinitely...
        waitingQueries.erase(w);
    }
    w.release();
}


//TODO:
//NEXT: handle sync callback
// supply helper functions to: exchange and catch up on missing data  ==> "Done" ==> Need to be fixed.
//                             generate merkle tree and root hash ==> Done
// simulate dummy result + read set ==> Done

//TODO: must be called while holding a lock on query_md. 
void Server::HandleSyncCallback(QueryMetaData *query_md, const std::string &queryId){

    Debug("Sync complete for Query[%lu:%lu]. Starting Execution", query_md->query_seq_num, query_md->client_id);
    query_md->is_waiting = false;
    
    // 1) Execute Query
    //Execute Query -- Go through store, and check if latest tx in store is present in syncList. If it is missing one (committed) --> reply EarlyAbort (tx cannot succeed). If prepared is missing, ignore, skip to next
    // Build Read Set while executing; Add dependencies on demand as we observe uncommitted txn touched.

      // 2) Construct Read Set
    //read set = map from key-> versions  //Note: Convert Timestamp to TimestampMessage
 
    /////////////////////////////////////////////////////////////
    //                                                         //
    //                                                         //
    //                                                         //
    //
    //              EXEC BLACKBOX -- TBD
    //                                                         //
    //                                                         //
    //                                                         //
    //                                                         //
    /////////////////////////////////////////////////////////////
     //Creating Dummy keys for testing //FIXME: REPLACE 
    for(int i=5;i > 0; --i){
        TimestampMessage ts;
        ts.set_id(query_md->ts.getID());
        ts.set_timestamp(query_md->ts.getTimestamp());

        std::string dummy_key = groupIdx == 0 ? "dummy_key_g1_" + std::to_string(i) : "dummy_key_g2_" + std::to_string(i);
        //query_md->read_set[dummy_key] = ts; //query_md->ts;
        //replaced with repeated field -> directly in result object.
        ReadMessage *read = query_md->queryResultReply->mutable_result()->mutable_query_read_set()->add_read_set();
        //ReadMessage *read = query_md->queryResult->mutable_query_read_set()->add_read_set();
        read->set_key(dummy_key);
        *read->mutable_readtime() = ts;
        //TODO: Add more keys; else I cant test order.
    }
    std::string dummy_result = "success" + std::to_string(query_md->query_seq_num);
    query_md->has_result = true; 

    //Blackbox might do multi-replica coordination to compute result and full read-set (though read set can actually be reported directly by each shard...)
    //TODO: Receive SyncReply from all shards ==> with read set, or read set hash. ==> in Tx_manager (marked by query) reply also include the result
    //FIXME: Always callback at shardclient, just only call-up to app if a) result has been received, b) all shards replied with read-set (or read-set hash)
    //-- want to do this so that Exec can be a better blackbox: This way data exchange might just be a small intermediary data, yet client learns full read set. 
        //In this case, read set hash from a shard is not enough to prove integrity to another shard (since less data than full read set might be exchanged)

    //After executing and caching read set -> Try to wake possibly subscribed queries.
    wakeSubscribedTx(queryId, query_md->retry_version); //TODO: Instead of passing it along, just store the queryId...


    bool exec_success = !test_fail_query; //FIXME: REMOVE: This tests one retry.
    if(exec_success){
         query_md->failure = false;
        
         if(query_md->designated_for_reply){
            query_md->queryResultReply->mutable_result()->set_query_result(dummy_result);
            //query_md->queryResult->set_query_result(dummy_result); //TODO: replace with real result
        }
        else{
            query_md->queryResultReply->mutable_result()->set_query_result(dummy_result); //set for non-query manager.
            //query_md->queryResult->set_query_result(dummy_result);
        }

         SendQueryReply(query_md);
    }
    else{
        FailQuery(query_md);
        test_fail_query = false;
    }
}

void Server::SendQueryReply(QueryMetaData *query_md){ 

//TODO: CALL WAKE OPERATION
    
    // proto::queryResultReplyReply *queryResultReply = new proto::queryResultReply(); //TODO: replace with GetUnused
    // proto::QueryResult *result = query_md->queryResult;
    proto::QueryResultReply *queryResultReply = query_md->queryResultReply;
    proto::QueryResult *result = queryResultReply->mutable_result();
    proto::ReadSet *query_read_set;
    //query_md->result = "success";
    //result->set_query_result("success");


    // 3) Generate Merkle Tree over Read Set, result, query id  (FIXME:: Currently only over read set:  )
    bool testing_hash = false; //note, if this is on, the client will crash since it expects a read set but does not get one.
    if(testing_hash || params.query_params.cacheReadSet){
        std::sort(result->mutable_query_read_set()->mutable_read_set()->begin(), result->mutable_query_read_set()->mutable_read_set()->end(), sortReadSetByKey); //Note: Sorts by key to ensure all replicas create the same hash. (Note: Not necessary if using ordered map)
        result->set_query_result_hash(generateReadSetSingleHash(result->query_read_set()));
        //Temporarily release read-set: This way we don't send it. Afterwards, re-allocate it. This avoid copying.
        query_read_set = result->release_query_read_set();
        Debug("Read-set hash: %s", BytesToHex(result->query_result_hash(), 16).c_str());
       
        //query_md->result_hash = std::move(generateReadSetSingleHash(query_md->read_set));  
        //query_md->result_hash = std::move(generateReadSetMerkleRoot(query_md->read_set, params.merkleBranchFactor)); //by default: merkleBranchFactor = 2 ==> might want to use flatter tree to minimize hashes.
                                                                                                        //TODO: Can avoid hashing leaves by making them unique strings? "[key:version]" should do the trick?
        //Debug("Read-set hash: %s", BytesToHex(query_md->result_hash, 16).c_str());
    }
    

    // 4) Possibly buffer Read Set (map: query_digest -> <result_hash, read set>) ==> implicitly done by storing read set + result hash in query_md 
   
    //5) Create Result reply --  // only include result if chosen for reply.
  
   

    //proto::queryResultReply *query_reply = queryResultReply->mutable_result();
    // query_reply->set_query_seq_num(query_md->query_seq_num); //TODO: store these in query_md?
    // query_reply->set_client_id(query_md->client_id);
    //query_reply->set_replica_id(id);
    result->set_query_seq_num(query_md->query_seq_num); //FIXME: put this directly when instantiating.
    result->set_client_id(query_md->client_id); //FIXME: set this directly when instantiating.
    result->set_replica_id(id);

    // if(params.query_params.cacheReadSet){
    //    query_reply->set_query_result_hash(query_md->result_hash);
       
    // }
    // else{
    //     *query_reply->mutable_query_read_set() = {query_md->read_set.begin(), query_md->read_set.end()}; //FIXME: Protobuf may serialize map into arbitrary order --> make sure it's ordered when Hashing.
    // }
    
    
    queryResultReply->set_req_id(query_md->req_id);

    //6) (Sign and) send reply 

     if (params.validateProofs && params.signedMessages) {
        //Debug("Sign Query Result Reply for Query[%lu:%lu]", query_reply->query_seq_num(), query_reply->client_id());
        Debug("Sign Query Result Reply for Query[%lu:%lu]", result->query_seq_num(), result->client_id());

        proto::QueryResult *res = queryResultReply->release_result();   
        if(false) { //params.queryReplyBatch){
            TransportAddress *remoteCopy = query_md->original_client->clone();
            auto sendCB = [this, remoteCopy, queryResultReply]() {
                this->transport->SendMessage(this, *remoteCopy, *queryResultReply); 
                delete remoteCopy;
                //delete queryResultReply;
            };
          
             //TODO: if this is already called from a worker, no point in dispatching it again. Add a Flag to MessageToSign that specifies "already worker"
            MessageToSign(res, queryResultReply->mutable_signed_result(), [this, sendCB, res, queryResultReply, query_read_set]() mutable {
                sendCB();
                 Debug("Sent Signed Query Result for Query[%lu:%lu]", res->query_seq_num(), res->client_id());

                if(params.query_params.cacheReadSet) res->set_allocated_query_read_set(query_read_set);
                queryResultReply->set_allocated_result(res);  //NOTE: This returns the unsigned result, including the readset. If we want to cache the signature, would have to change this code.
               
                //delete res;
            });

        }
        else{ //realistically don't ever need to batch query sigs --> batching helps with amortized sig generation, but not with verificiation since client don't forward proofs.
            if(params.signatureBatchSize == 1){
                SignMessage(res, keyManager->GetPrivateKey(id), id, queryResultReply->mutable_signed_result());
            }
            else{
                std::vector<::google::protobuf::Message *> msgs;
                msgs.push_back(res);
                std::vector<proto::SignedMessage *> smsgs;
                smsgs.push_back(queryResultReply->mutable_signed_result());
                SignMessages(msgs, keyManager->GetPrivateKey(id), id, smsgs, params.merkleBranchFactor);
            }
            
            this->transport->SendMessage(this, *query_md->original_client, *queryResultReply);
             Debug("Sent Signed Query Resut for Query[%lu:%lu]", res->query_seq_num(), res->client_id());
            //delete queryResultReply;
            if(params.query_params.cacheReadSet) res->set_allocated_query_read_set(query_read_set);
            queryResultReply->set_allocated_result(res);  //NOTE: This returns the unsigned result, including the readset. If we want to cache the signature, would have to change this code.
            //delete res;
        }
    }
    else{
        this->transport->SendMessage(this, *query_md->original_client, *queryResultReply);

         if(params.query_params.cacheReadSet) result->set_allocated_query_read_set(query_read_set); //Note: In this branch result is still part of queryResultReply; thus it suffices to only allocate back to result.
    }

      Debug("BEGIN READ SET:"); //TODO: Remove -- just for testing
              
                for(auto &read : result->query_read_set().read_set()){
                //for(auto &[key, ts] : read_set){
                  //std::cerr << "key: " << key << std::endl;
                  Debug("Cached Read key %s with version [%lu:%lu]", read.key().c_str(), read.readtime().timestamp(), read.readtime().id());
                  //Debug("[group %d] Read key %s with version [%lu:%lu]", group, key.c_str(), ts.timestamp(), ts.id());
                }
              
              Debug("END READ SET.");

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
        Debug("Sign Query Fail Reply for Query Req[%lu]", failQuery.req_id());
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



// void Server::FindSnapshot(){

// }

// void Server::Materialize(){

// }

// void Server::ExecuteQuery(){

// }

// void Server::ParseQuery(){

// }

} // namespace pequinstore