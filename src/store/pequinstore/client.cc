// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/pequinstore/client.cc:
 *   Client to INDICUS transactional storage system.
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

#include "store/pequinstore/client.h"

#include "store/pequinstore/localbatchverifier.h"
#include "store/pequinstore/basicverifier.h"
#include "store/pequinstore/common.h"
#include <sys/time.h>
#include <algorithm>

#include "store/common/query_result/query_result_proto_wrapper.h"
#include "store/common/query_result/query_result_proto_builder.h"

namespace pequinstore {

using namespace std;

//TODO: add argument for p1Timeout, pass down to Shardclient as well.
Client::Client(transport::Configuration *config, uint64_t id, int nShards,
    int nGroups,
    const std::vector<int> &closestReplicas, bool pingReplicas, Transport *transport,
    Partitioner *part, bool syncCommit, uint64_t readMessages,
    uint64_t readQuorumSize, Parameters params,
    KeyManager *keyManager, uint64_t phase1DecisionTimeout, uint64_t consecutiveMax, TrueTime timeServer)
    : config(config), client_id(id), nshards(nShards), ngroups(nGroups),
    transport(transport), part(part), syncCommit(syncCommit), pingReplicas(pingReplicas),
    readMessages(readMessages), readQuorumSize(readQuorumSize),
    params(params),
    keyManager(keyManager),
    timeServer(timeServer), first(true), startedPings(false),
    query_seq_num(0UL), client_seq_num(0UL), lastReqId(0UL), getIdx(0UL),
    failureEnabled(false), failureActive(false), faulty_counter(0UL),
    consecutiveMax(consecutiveMax) {


  Debug("Initializing Indicus client with id [%lu] %lu", client_id, nshards);
  std::cerr<< "P1 Decision Timeout: " <<phase1DecisionTimeout<< std::endl;
  if(params.injectFailure.enabled) stats.Increment("total_byz_clients", 1);

  if (params.signatureBatchSize == 1) {
    verifier = new BasicVerifier(transport);//transport, 1000000UL,false); //Need to change interface so client can use it too?
  } else {
    verifier = new LocalBatchVerifier(params.merkleBranchFactor, dummyStats, transport);
  }

  /* Start a client for each shard. */
  for (uint64_t i = 0; i < ngroups; i++) {
    bclient.push_back(new ShardClient(config, transport, client_id, i,
        closestReplicas, pingReplicas, params,
        keyManager, verifier, timeServer, phase1DecisionTimeout, consecutiveMax));
  }

  Debug("Indicus client [%lu] created! %lu %lu", client_id, nshards,
      bclient.size());
  _Latency_Init(&executeLatency, "execute");
  _Latency_Init(&getLatency, "get");
  _Latency_Init(&commitLatency, "commit");

  if (params.injectFailure.enabled) {
    transport->Timer(params.injectFailure.timeMs, [this](){
        failureEnabled = true;
        // TODO: restore the client after it stalls from phase2_callback from previous txn
      });
  }

  // struct timeval tv;
  // gettimeofday(&tv, NULL);
  // start_time = (tv.tv_sec*1000000+tv.tv_usec)/1000;  //in miliseconds
  //
  // transport->Timer(9500, [this](){
  //   std::cerr<< "experiment about to elapse 10 seconds";
  // });
}

Client::~Client()
{
  //std::cerr << "total failure injections: " << total_failure_injections << std::endl;
  //std::cerr << "total writebacks: " << total_writebacks << std::endl;
  //std::cerr<< "total prepares: " << total_counter << std::endl;
  //std::cerr<< "fast path prepares: " << fast_path_counter << std::endl;
  Latency_Dump(&executeLatency);
  Latency_Dump(&getLatency);
  Latency_Dump(&commitLatency);
  for (auto b : bclient) {
      delete b;
  }
  delete verifier;
}

/* Begins a transaction. All subsequent operations before a commit() or
 * abort() are part of this transaction.
 */
void Client::Begin(begin_callback bcb, begin_timeout_callback btcb,
      uint32_t timeout, bool retry) {
  // fail the current txn iff failuer timer is up and
  // the number of txn is a multiple of frequency
  //only fail fresh transactions
  if(!retry) {
    faulty_counter++;
    failureActive = failureEnabled &&
      (faulty_counter % params.injectFailure.frequency == 0);
    for (auto b : bclient) {
      b->SetFailureFlag(failureActive);
    }
    if(failureActive) stats.Increment("failure_attempts", 1);
    if(failureEnabled) stats.Increment("total_fresh_tx_byz", 1);
    if(!failureEnabled) stats.Increment("total_fresh_tx_honest", 1);
  }

  transport->Timer(0, [this, bcb, btcb, timeout]() { 
    if (pingReplicas) {
      if (!first && !startedPings) {
        startedPings = true;
        for (auto sclient : bclient) {
          sclient->StartPings();
        }
      }
      first = false;
    }

    Latency_Start(&executeLatency);
    client_seq_num++;
    //std::cerr<< "BEGIN TX with client_seq_num: " << client_seq_num << std::endl;
    Debug("BEGIN [%lu]", client_seq_num);

    txn = proto::Transaction();
    txn.set_client_id(client_id);
    txn.set_client_seq_num(client_seq_num);
    // Optimistically choose a read timestamp for all reads in this transaction
    txn.mutable_timestamp()->set_timestamp(timeServer.GetTime());
    txn.mutable_timestamp()->set_id(client_id);

    bcb(client_seq_num);
  });
}

void Client::Get(const std::string &key, get_callback gcb,
    get_timeout_callback gtcb, uint32_t timeout) {

  transport->Timer(0, [this, key, gcb, gtcb, timeout]() {
    // Latency_Start(&getLatency);

    Debug("GET[%lu:%lu] for key %s", client_id, client_seq_num,
        BytesToHex(key, 16).c_str());

    // Contact the appropriate shard to get the value.
    std::vector<int> txnGroups(txn.involved_groups().begin(), txn.involved_groups().end());
    int i = (*part)(key, nshards, -1, txnGroups) % ngroups;

    // If needed, add this shard to set of participants and send BEGIN.
    if (!IsParticipant(i)) {
      txn.add_involved_groups(i);
      bclient[i]->Begin(client_seq_num);
    }

    read_callback rcb = [gcb, this](int status, const std::string &key,
        const std::string &val, const Timestamp &ts, const proto::Dependency &dep,
        bool hasDep, bool addReadSet) {

      uint64_t ns = 0; //Latency_End(&getLatency);
      if (Message_DebugEnabled(__FILE__)) {
        Debug("GET[%lu:%lu] Callback for key %s with %lu bytes and ts %lu.%lu after %luus.",
            client_id, client_seq_num, BytesToHex(key, 16).c_str(), val.length(),
            ts.getTimestamp(), ts.getID(), ns / 1000);
        if (hasDep) {
          Debug("GET[%lu:%lu] Callback for key %s with dep ts %lu.%lu.",
              client_id, client_seq_num, BytesToHex(key, 16).c_str(),
              dep.write().prepared_timestamp().timestamp(),
              dep.write().prepared_timestamp().id());
        }
      }
      if (addReadSet) {
        Debug("Adding read to read set");
        ReadMessage *read = txn.add_read_set();
        read->set_key(key);
        ts.serialize(read->mutable_readtime());
      }
      if (hasDep) {
        *txn.add_deps() = dep;
      }
      gcb(status, key, val, ts);
    };
    read_timeout_callback rtcb = gtcb;

    // Send the GET operation to appropriate shard.
    bclient[i]->Get(client_seq_num, key, txn.timestamp(), readMessages,
        readQuorumSize, params.readDepSize, rcb, rtcb, timeout);
  });
}

void Client::Put(const std::string &key, const std::string &value,
    put_callback pcb, put_timeout_callback ptcb, uint32_t timeout) {
  transport->Timer(0, [this, key, value, pcb, ptcb, timeout]() {

    //std::cerr << "value size: " << value.size() << "; key " << BytesToHex(key,16).c_str() << std::endl;
    Debug("PUT[%lu:%lu] for key %s", client_id, client_seq_num, BytesToHex(key,
          16).c_str());

    // Contact the appropriate shard to set the value.
    std::vector<int> txnGroups(txn.involved_groups().begin(), txn.involved_groups().end());
    int i = (*part)(key, nshards, -1, txnGroups) % ngroups;

    // If needed, add this shard to set of participants and send BEGIN.
    if (!IsParticipant(i)) {
      txn.add_involved_groups(i);
      bclient[i]->Begin(client_seq_num);
    }

    WriteMessage *write = txn.add_write_set();
    write->set_key(key);
    write->set_value(value);



    // Buffering, so no need to wait.
    bclient[i]->Put(client_seq_num, key, value, pcb, ptcb, timeout);
  });
}


//primary_key_encoding_support is an encoding_helper function: Specify which columns of a write statement correspond to the primary key; each vector belongs to one insert. 
//In case of nesting or concat --> order = order of reading
void Client::Write(std::string &write_statement, std::vector<std::vector<uint32_t>> primary_key_encoding_support, write_callback wcb,
      write_timeout_callback wtcb, uint32_t timeout){

    
    //////////////////
    // Write Statement parser/interpreter:   //For now design to supports only individual Insert/Update/Delete statements. No nesting, no concatenation
    //TODO: parse write statement into table, column list, values_list, and read condition
    

    std::string read_statement;
    std::function<void(int, query_result::QueryResult*)>  write_continuation;

    TransformWriteStatement(write_statement, primary_key_encoding_support, read_statement, write_continuation, wcb);
    
  
    if(read_statement.empty()){
      //TODO: Add to writes directly.
      sql::QueryResultProtoWrapper *write_result = new sql::QueryResultProtoWrapper(""); //TODO: replace with real result.
      wcb(REPLY_OK, write_result);
    }
    else{
      //  auto qcb = [this, write_continuation, wcb](int status, const query_result::QueryResult *result) mutable { 

      //   //result ==> replace with protoResult type
      //   const query_result::QueryResult *write_result = write_continuation(result);
      //   wcb(REPLY_OK, write_result);
      //   return;
      // };
      // auto qtcb = [this, wtcb](int status) {
      //   wtcb(status);
      //   return;
      // };
      Query(read_statement, write_continuation, wtcb, timeout);
    }
    return;
  }

    //statement like: INSERT INTO test VALUES (1001);
    //REPLACE INTO table(column_list) VALUES(value_list);  is a shorter form of  INSERT OR REPLACE INTO table(column_list) VALUES(value_list);

    //Maybe simpler: Let Write take table, column list, and values as attributes. And then turn it into a REPLACE INTO statement.
    //I.e. our frontend currently implements a manual insert (just so we can ignore automizing "Update Where" statements)

    

    //TODO: How can we only update certain attributes while creating a new version?
    // a) In table: Mark version per attribute...
    // b) When updating in table: Copy the version we read -- to identify that, the write needs to pass the version...
            //Or is it fine to just copy the last version known to the server? No... this might produce non-serializable state...

    //Note: if we had all attributes here, then we could just insert a whole new row.
    // --> However, a query read does not necessarily return the whole rows.  ==> But maybe our Select dialect (turning Update into Select + multiple inserts) does retrive the whole row:

    //I..e for updates: Whole row is available: Write new:
    //For independent inserts: Write new row with empty attributes? No.. only write if no previous row existed.   I.e. also split into Select + Write. ==> If Select == empty, then write. Add to readSet: 0 Timestamp.

    //It seems easiest for the frontend to not worry about the details at all: In order to support that, must turn the request into read/writes at THIS level.
    //That way it would be easy to keep track of readmodify write ops to track read timestamps too
    //Write type Statement function:
    //  -- turn into a Query statement:
    // On reply, don't use query callback though, and instead issue the updates to ReadSet + WriteSet (table writes), and then callback.

    //Insert: Conditional Read Mod Write
    //Replace: Unconditional (Blind) Write  -- REPLACE INTO == INSERT OR REPLACE --> weaker cond holds: Blind Write.
    //Update: Conditional Read Mod Write
    //Delete: Conditional Read Mod Write

    //Conditional Read lets us now read version for serializability checks
    //Insert and Delete don't need to know row values
    //But don't necessarily know full row for Update -- if we only want to update certain rows then that is a problem.  Same for Replace certain rows?
       // --> Because we are multi-versioned, we want to create a separate row. What we can do is include the "read version" as part of the Write req, so that we can fetch+copythe row on demand and only update the few missing
      //Note: If the read row was deleted in the meantime -- then there would be a new row that is empty with a higher TS. The write would conflict with this delete row and thus abort.

    //TODO: Need a way to infer encoded key (essentially primary key) from the column list?
     // for conditional read mod writes one can just use the read key!
     // but for unconditional (blind) writes one needs to figure out the column list. Maybe one can add it to the Write function as argument? (Difficult for nested queries, but should be easy for single blind writes)

    //Primary key that is auto-increment? Insert has to also read last primary key? And then propose new one?


//NOTE: Unlike Get, Query currently cannot read own write, or previous reads -> consequently, different queries may read the same key differently
// (Could edit query to include "previoudReads" + writes and use it for materialization)

//Simulate Select * for now
// TODO: --> Return all rows in the store.
void Client::Query(const std::string &query, query_callback qcb,
    query_timeout_callback qtcb, uint32_t timeout) {

  UW_ASSERT(query.length() < ((uint64_t)1<<32)); //Protobuf cannot handle strings longer than 2^32 bytes --> cannot handle "arbitrarily" complex queries: If this is the case, we need to break down the query command.

  transport->Timer(0, [this, query, qcb, qtcb, timeout]() mutable {
    // Latency_Start(&getLatency);

    query_seq_num++;
    txn.set_last_query_seq(query_seq_num);
    Debug("\n Query[%lu:%lu:%lu]", client_id, client_seq_num, query_seq_num);

 
    // Contact the appropriate shard to execute the query on.
    //TODO: Determine involved groups
    //Requires parsing the Query statement to extract tables touched? Might touch multiple shards...
    //Assume for now only touching one group. (single sharded system)
    PendingQuery *pendingQuery = new PendingQuery(this, query_seq_num, query);

    std::vector<uint64_t> involved_groups = {0};//{0UL, 1UL};
    pendingQuery->SetInvolvedGroups(involved_groups);
    Debug("[group %i] designated as Query Execution Manager for query [%lu:%lu]", pendingQuery->queryMsg.query_manager(), client_seq_num, query_seq_num);
    pendingQuery->SetQueryId(this);

    // std::vector<uint64_t> involved_groups = {0UL};
    // uint64_t query_manager = involved_groups[0];
    // Debug("[group %i] designated as Query Execution Manager for query [%lu:%lu]", query_manager, client_seq_num, query_seq_num);

        //TODO: just create a new object.. allocate is easier..
    
    // queryMsg.Clear();
    // queryMsg.set_client_id(client_id);
    // queryMsg.set_query_seq_num(query_seq_num);
    // *queryMsg.mutable_query_cmd() = std::move(query);
    // *queryMsg.mutable_timestamp() = txn.timestamp();
    // queryMsg.set_query_manager(query_manager);
    
    //TODO: store --> so we can access it with query_seq_num if necessary for retry.
      


    // If needed, add this shard to set of participants and send BEGIN.
    for(auto &i: pendingQuery->involved_groups){
       if (!IsParticipant(i)) {
        txn.add_involved_groups(i);
        bclient[i]->Begin(client_seq_num);
      }
    }
  

    //result_callback rcb = qcb;
    //NOTE: result_hash = read_set hash. ==> currently not hashing queryId, version or result contents into it. Appears unecessary.
    //result_callback rcb = [qcb, pendingQuery, this](int status, int group, std::map<std::string, TimestampMessage> &read_set, std::string &result_hash, std::string &result, bool success) mutable { 
    result_callback rcb = [qcb, pendingQuery, this](int status, int group, proto::ReadSet *query_read_set, std::string &result_hash, std::string &result, bool success) mutable { 
      //FIXME: If success: add readset/result hash to datastructure. If group==query manager, record result. If all shards received ==> upcall. 
      //If failure: re-set datastructure and try again. (any shard can report failure to sync)
      //Note: Ongoing shard clients PendingQuery implicitly maps to current retry_version
    
          UW_ASSERT(pendingQuery != nullptr);
          if(success){
            Debug("[group %i] Reported success for QuerySync [seq : ver] [%lu : %lu] \n", group, pendingQuery->queryMsg.query_seq_num(), pendingQuery->version);
            pendingQuery->group_replies++;
    
            if(params.query_params.cacheReadSet){ 
               pendingQuery->group_result_hashes[group] = std::move(result_hash);
            }
            else{
               //pendingQuery->group_read_sets[group] = std::move(read_set);
               pendingQuery->group_read_sets[group] = query_read_set; //Note: this is an allocated object, must be freed eventually.
            }
            if(group == pendingQuery->involved_groups[0]) pendingQuery->result = std::move(result); 

            //wait for all shard read-sets to arrive before reporting result. (Realistically result shard replies last, since it has to coordinate data transfer for computation)
            if(pendingQuery->involved_groups.size() == pendingQuery->group_replies){
              Debug("Received all required group replies for QuerySync[%lu:%lu] (seq:ver). UPCALLING \n", group, pendingQuery->queryMsg.query_seq_num(), pendingQuery->version);


//BEGIN FIXME: DELETE THIS. IT IS TEST CODE ONLY
              //TESTING Read-set
              Debug("BEGIN READ SET:");
              for(auto &[group, query_read_set] : pendingQuery->group_read_sets){
                for(auto &read : query_read_set->read_set()){
                //for(auto &[key, ts] : read_set){
                  //std::cerr << "key: " << key << std::endl;
                  Debug("[group %d] Read key %s with version [%lu:%lu]", group, read.key().c_str(), read.readtime().timestamp(), read.readtime().id());
                  //Debug("[group %d] Read key %s with version [%lu:%lu]", group, key.c_str(), ts.timestamp(), ts.id());
                }
              }
              Debug("END READ SET.");

              // Debug("Test Read Set merge:");
              // proto::ReadSet* read_set_0 = pendingQuery->group_read_sets[0];
              // proto::ReadSet* read_set_1 = pendingQuery->group_read_sets[1];
              // // ReadMessage* read = read_set_0->add_read_set();
              // // read = read_set_1->release_read_set();
              //  for(auto &read : *(read_set_1->mutable_read_set())){
              //    ReadMessage* add_read = read_set_0->add_read_set();
              //    *add_read = std::move(read);
              //  }
              //  //This code moves read sets instead of coyping. However, if we are caching then we want to copy during merge in order to preserve the cached value.


              // //read_set_0->mutable_read_set()->MergeFrom(read_set_1->read_set());
              // for(auto &read : read_set_0->read_set()){
              //     Debug("[group Merged] Read key %s with version [%lu:%lu]", read.key().c_str(), read.readtime().timestamp(), read.readtime().id());
              //   }
              // for(auto &read : read_set_1->read_set()){
              //     Debug("[group Removed] Read key %s with version [%lu:%lu]", read.key().c_str(), read.readtime().timestamp(), read.readtime().id());
              //   }
              //TODO: merge all group read sets together before sending tx. Or clear all read-sets that are not relevant to a group (==> Need to use a sub-hash to represent groups read set)
//END FIXME:



            
               //Make query meta data part of current transaction. 
               //==> Add repeated item <QueryReply> with query_id, final version, and QueryMeta field per involved shard. Query Meta = optional read_sets, optional_result_hashes (+version)
              proto::QueryResultMetaData *queryRep = txn.add_query_set();
              queryRep->set_query_id(pendingQuery->queryId);
              queryRep->set_retry_version(pendingQuery->version); //technically only needed for caching

              //TODO: Add dependencies once prepared reads are implemented. Note: Replicas can also just check for every key in read-set that it is commmitted. However, the client may already know a given tx is committed.
                                                                                                                        // Specifying an explicit dependency set can "reduce" the amount of tx a replica needs to wait for.

              //*queryRep->mutable_query_group_meta() = {pendingQuery->}
            

              if(params.query_params.cacheReadSet){ 
                for(auto &[group, read_set_hash] : pendingQuery->group_result_hashes){
                  proto::QueryGroupMeta &queryMD = (*queryRep->mutable_group_meta())[group]; 
                  queryMD.set_read_set_hash(read_set_hash);
                }
                  //When caching read sets: Check that client reported version matches local one. If not, report Client. (FIFO guarantees that client wouldn't send prepare before retry)
                      //Problem: What if client crashes, and another interested client proposes the prepare for a version whose retry has not yet reached some replicas (no FIFO across channels). Could cause deterministic tx abort.
                      //==> Replica waits for query-id/version to arrive before processing P1 request. Interested client is guaranteed to be able to supply it.
                     // ==> What if client used different versions of same query id for different prepares?
                         // Should never happen: Client must only use each query id ONCE. Thus, if there are two prepares with the same query-id but different version ==> report client
              }
              else{
                for(auto &[group, query_read_set] : pendingQuery->group_read_sets){
                  
                  if(params.query_params.mergeActiveAtClient){
                     //Option 1): Merge all active read sets into main_read set. When sorting, catch errors and abort early.
                    for(auto &read : *query_read_set->mutable_read_set()){
                      ReadMessage* add_read = txn.add_read_set();
                      *add_read = std::move(read);
                    }
                    //Merge all deps as well. Note: Optimistic Id's were already reverted back to real tx-ids at this point. (Must ensure that all dep is on correct txn - cannot be optimistic anymore)
                    for(auto &dep : *query_read_set->mutable_deps()){
                      proto::Dependency *add_dep = txn.add_deps();
                      *add_dep = std::move(dep);
                    }
                    // for(auto &dep_id : *query_read_set->mutable_dep_ids()){
                    //   Dependency *add_dep = txn.add_deps();
                    //   add_dep->set_involved_group(group);
                    //   *add_dep->mutable_write()->prepared_txn_digest() = std::move(dep_id);
                    // }

                    delete query_read_set;
                  }
                  else{
                    //Option 2): send all active read sets individually per query
                    proto::QueryGroupMeta &queryMD = (*queryRep->mutable_group_meta())[group]; 
                    queryMD.set_allocated_query_read_set(query_read_set);
                  }
                }
                pendingQuery->group_read_sets.clear(); //Note: Clearing here early to avoid double deletions on read sets whose allocated memory was moved.
              }
        
              Debug("Upcall with result");
              sql::QueryResultProtoWrapper *q_result = new sql::QueryResultProtoWrapper(pendingQuery->result);
              qcb(REPLY_OK, q_result); //callback to application 
              //clean pendingQuery and query_seq_num_mapping in all shards.
              ClearQuery(pendingQuery);      
            }
          }
          else{
            delete query_read_set;
            Debug("[group %i] Reported failure for QuerySync [seq : ver] [%lu : %lu] \n", group, pendingQuery->queryMsg.query_seq_num(), pendingQuery->version);
            RetryQuery(pendingQuery);
          }                     
    };
    result_timeout_callback rtcb = qtcb;

    // Send the Query operation to involved shards & select transaction manager (shard responsible for result reply) 
     for(auto &i: pendingQuery->involved_groups){
        Debug("[group %i] starting Query [%lu:%lu]", i, client_seq_num, query_seq_num);
        bclient[i]->Query(client_seq_num, query_seq_num, pendingQuery->queryMsg, rcb, rtcb, timeout);
     }
    // Shard Client upcalls only if it is the leader for the query, and if it gets matching result hashes  ..........const std::string &resultHash
       //store QueryID + result hash in transaction.

    //queryBuffer[query_seq_num] = std::move(queryMsg);  //Buffering only after sending, so we can move contents for free.

  });
}


void Client::ClearQuery(PendingQuery *pendingQuery){
   for(auto &g: pendingQuery->involved_groups){
    bclient[g]->ClearQuery(pendingQuery->queryMsg.query_seq_num()); //-->Remove mapping + pendingRequest.
   }

   delete pendingQuery;
}
void Client::RetryQuery(PendingQuery *pendingQuery){
  pendingQuery->version++;
  pendingQuery->group_replies = 0;
  pendingQuery->queryMsg.set_retry_version(pendingQuery->version);
  pendingQuery->ClearReplySets();
  for(auto &g: pendingQuery->involved_groups){
    bclient[g]->RetryQuery(pendingQuery->queryMsg.query_seq_num(), pendingQuery->queryMsg); //--> Retry Query, shard clients already have the rcb.
  }
}

// void Client::ClearQuery(uint64_t query_seq_num, std::vector<uint64_t> &involved_groups){

//    queryBuffer.erase(query_seq_num);

//    for(auto &g: involved_groups){
//     bclient[g]->ClearQuery(query_seq_num); //-->Remove mapping + pendingRequest.
//    }
// }


// void Client::RetryQuery(uint64_t query_seq_num, std::vector<uint64_t> &involved_groups){
//   //find buffered query_seq_num Query.
//   // pass query_id to all involved shards.
//   // shards find their RequestInstance and reset it.
//   proto::Query &query_msg = queryBuffer[query_seq_num]; //TODO: Assert this exists. -- it must, or else we wouldnt' have called Retry.
//   for(auto &g: involved_groups){
//     bclient[g]->RetryQuery(query_seq_num, query_msg); //--> Retry Query, shard clients already have the rcb.
//   }


//    // 1) Re-send query, but this time don't use optimistic id's, 2) start fall-back for responsible ids. 
//                                                             //TODO: Replica that observes duplicate: Send Report message with payload: list<pairs: txn>> (pairs of txn with same optimistic id)
//                                                             //   Note: For replica to observe duplicate: Gossip is necessary to receive both.
//                                                             // If replica instead sends full read set to client, then client can look at read sets to figure out divergence: 
//                                                                           // Find mismatched keys, or keys with different versions. Send to replicas request for tx for (key, version) --> replica replies with txn-digest.
// }


void Client::Commit(commit_callback ccb, commit_timeout_callback ctcb,
    uint32_t timeout) {
  transport->Timer(0, [this, ccb, ctcb, timeout]() {
    uint64_t ns = Latency_End(&executeLatency);
    Latency_Start(&commitLatency);

    if(!txn.query_set().empty() && !params.query_params.cacheReadSet && params.query_params.mergeActiveAtClient){
      //If has queries, and query deps are meant to be reported by client:
      // Sort and erase all duplicate dependencies. (equality = same txn_id and same involved group.)
      std::sort(txn.mutable_deps()->begin(), txn.mutable_deps()->end(), sortDepSet);
      txn.mutable_deps()->erase(std::unique(txn.mutable_deps()->begin(), txn.mutable_deps()->end(), equalDep), txn.mutable_deps()->end());  //erases all but last appearance
      
    }

    //XXX flag to sort read/write sets for parallel OCC
    if(params.parallel_CCC){
      try {
        std::sort(txn.mutable_read_set()->begin(), txn.mutable_read_set()->end(), sortReadSetByKey);
        std::sort(txn.mutable_write_set()->begin(), txn.mutable_write_set()->end(), sortWriteSetByKey);
        //Note: Use stable_sort to guarantee order respects duplicates; Altnernatively: Can try to delete from write sets to save redundant size.

        //If write set can contain duplicates use the following: Reverse + sort --> "latest" put is first. Erase all but first entry. 
        //(Note: probably cheaper to erase all duplicates manually during insert? Or use map?)
        // std::reverse(txn.mutable_write_set()->begin(), txn.mutable_write_set()->end());
        // std::stable_sort(txn.mutable_write_set()->begin(), txn.mutable_write_set()->end(), sortWriteSetByKey);  //sorts while maintaining relative order
        // txn.mutable_write_set()->erase(std::unique(txn.mutable_write_set()->begin(), txn.mutable_write_set()->end(), equalWriteMsg), txn.mutable_write_set()->end());  //erases all but last appearance
      }
      catch(...) {
        Debug("Preemptive Abort: Trying to commit a transaction with 2 different reads for the same key");
        uint64_t ns = Latency_End(&executeLatency);

        Debug("ABORT[%lu:%lu]", client_id, client_seq_num);
        if(!params.query_params.mergeActiveAtClient) Panic("Without Client-side query merge Client should never read same key twice");

        for (auto group : txn.involved_groups()) {
          bclient[group]->Abort(client_seq_num, txn.timestamp(), txn);
        }
        ccb(ABORTED_SYSTEM);
        //Panic("Client should never read same key twice");
        return;
      }
    }

    PendingRequest *req = new PendingRequest(client_seq_num, this);
    pendingReqs[client_seq_num] = req;
    req->txn = txn; //Is this a copy or just reference?
    req->ccb = ccb;
    req->ctcb = ctcb;
    req->callbackInvoked = false;
    req->txnDigest = TransactionDigest(txn, params.hashDigest);
    req->timeout = timeout; //20000UL; //timeout;
    stats.IncrementList("txn_groups", txn.involved_groups().size());

    Phase1(req);
  });
}

void Client::Phase1(PendingRequest *req) {
  Debug("PHASE1 [%lu:%lu] for txn_id %s at TS %lu", client_id, client_seq_num,
      BytesToHex(TransactionDigest(req->txn, params.hashDigest), 16).c_str(), txn.timestamp().timestamp());


  UW_ASSERT(txn.involved_groups().size() > 0);

  for (auto group : txn.involved_groups()) {
    bclient[group]->Phase1(client_seq_num, txn, req->txnDigest,
        std::bind( &Client::Phase1Callback, this, req->id, group, std::placeholders::_1,
          std::placeholders::_2, std::placeholders::_3,
          std::placeholders::_4, std::placeholders::_5, std::placeholders::_6),
        std::bind(&Client::Phase1TimeoutCallback, this, group, req->id,
          std::placeholders::_1),
        std::bind(&Client::RelayP1callback, this, req->id, std::placeholders::_1, std::placeholders::_2),
        std::bind(&Client::FinishConflict, this, req->id, std::placeholders::_1, std::placeholders::_2),
        req->timeout);
    req->outstandingPhase1s++;
  }
  //schedule timeout for when we allow starting FB P1.
  transport->Timer(params.relayP1_timeout, [this, reqId = req->id](){RelayP1TimeoutCallback(reqId);});

  if(!failureEnabled) stats.Increment("total_honest_p1_started", 1);

  //FAIL right after sending P1

  if (failureActive && (params.injectFailure.type == InjectFailureType::CLIENT_CRASH || params.injectFailure.type == InjectFailureType::CLIENT_SEND_PARTIAL_P1)) {
    Debug("INJECT CRASH FAILURE[%lu:%lu] with decision %d. txnDigest: %s", client_id, req->id, req->decision,
          BytesToHex(TransactionDigest(req->txn, params.hashDigest), 16).c_str());
    stats.Increment("inject_failure_crash");
    //total_failure_injections++;
    FailureCleanUp(req);
    return;
  }
}



void Client::Phase1CallbackProcessing(PendingRequest *req, int group,
    proto::CommitDecision decision, bool fast, bool conflict_flag,
    const proto::CommittedProof &conflict,
    const std::map<proto::ConcurrencyControl::Result, proto::Signatures> &sigs,
    bool eqv_ready, bool fb){

  if (decision == proto::ABORT && fast && conflict_flag) {
      req->conflict = conflict;
      req->conflict_flag = true;
  }

  if (params.validateProofs && params.signedMessages) {
    if (eqv_ready) {
      // saving abort sigs in req->eqvAbortSigsGrouped
      auto itr_abort = sigs.find(proto::ConcurrencyControl::ABSTAIN);
      UW_ASSERT(itr_abort != sigs.end());
      Debug("Have %d ABSTAIN replies from group %d. Saving in eqvAbortSigsGrouped",
          itr_abort->second.sigs_size(), group);
      (*req->eqvAbortSigsGrouped.mutable_grouped_sigs())[group] = itr_abort->second;
      req->slowAbortGroup = group;
      // saving commit sigs in req->p1ReplySigsGrouped
      auto itr_commit = sigs.find(proto::ConcurrencyControl::COMMIT);
      UW_ASSERT(itr_commit != sigs.end());
      Debug("Have %d COMMIT replies from group %d.", itr_commit->second.sigs_size(),
          group);
      (*req->p1ReplySigsGrouped.mutable_grouped_sigs())[group] = itr_commit->second;

    }
    //non-equivocation processing
    else if(decision == proto::ABORT && fast && !conflict_flag){
      auto itr = sigs.find(proto::ConcurrencyControl::ABSTAIN);
      UW_ASSERT(itr != sigs.end());
      Debug("Have %d ABSTAIN replies from group %d.", itr->second.sigs_size(),
          group);
      (*req->p1ReplySigsGrouped.mutable_grouped_sigs())[group] = itr->second;
      req->fastAbortGroup = group;
    } else if (decision == proto::ABORT && !fast) {
      auto itr = sigs.find(proto::ConcurrencyControl::ABSTAIN);
      UW_ASSERT(itr != sigs.end());
      Debug("Have %d ABSTAIN replies from group %d.", itr->second.sigs_size(),
          group);
      (*req->p1ReplySigsGrouped.mutable_grouped_sigs())[group] = itr->second;
      req->slowAbortGroup = group;
    } else if (decision == proto::COMMIT) {
      auto itr = sigs.find(proto::ConcurrencyControl::COMMIT);
      // if(itr == sigs.end()){
      //   if(fb){
      //     std::cerr << "abort on fallback path for txn: " << BytesToHex(req->txnDigest, 16) << std::endl;
      //   }
      //   if(!fb){
      //     std::cerr << "abort on normal path for txn: " << BytesToHex(req->txnDigest, 16) << std::endl;
      //   }
      // }
      UW_ASSERT(itr != sigs.end());
      Debug("Have %d COMMIT replies from group %d.", itr->second.sigs_size(),
          group);
      (*req->p1ReplySigsGrouped.mutable_grouped_sigs())[group] = itr->second;
    }
  }

  if (fast && decision == proto::ABORT) {
    req->fast = true;
  } else {
    req->fast = req->fast && fast;
  }
  if (eqv_ready) req->eqv_ready = true;

  --req->outstandingPhase1s;
  switch(decision) {
    case proto::COMMIT:
      break; //decision is proto::COMMIT by default
    case proto::ABORT:
      // abort!
      req->decision = proto::ABORT;
      req->eqv_ready = false; //if there is a single shard that is only abort, then equiv not possible
      req->outstandingPhase1s = 0;
      break;
    default:
      break;
  }
}

void Client::Phase1Callback(uint64_t txnId, int group,
    proto::CommitDecision decision, bool fast, bool conflict_flag,
    const proto::CommittedProof &conflict,
    const std::map<proto::ConcurrencyControl::Result, proto::Signatures> &sigs,
    bool eqv_ready) {
  auto itr = this->pendingReqs.find(txnId);
  if (itr == this->pendingReqs.end()) {
    Debug("Phase1Callback for terminated request %lu (txn already committed"
        " or aborted.", txnId);
    return;
  }

  //total_counter++;
  //if(fast) fast_path_counter++;
  stats.Increment("total_prepares", 1);
  if(fast) stats.Increment("total_prepares_fast", 1);

  Debug("PHASE1[%lu:%lu] callback decision %d [Fast:%s][Conflict:%s] from group %d", client_id,
      client_seq_num, decision, fast ? "yes" : "no", conflict_flag ? "yes" : "no", group);

  PendingRequest *req = itr->second;

  if (req->startedPhase2 || req->startedWriteback) {
    Debug("Already started Phase2/Writeback for request id %lu. Ignoring Phase1"
        " response from group %d.", txnId, group);
    return;
  }

  Phase1CallbackProcessing(req, group, decision, fast, conflict_flag, conflict, sigs, eqv_ready, false);

  if (req->outstandingPhase1s == 0) {
    HandleAllPhase1Received(req);
  }
    //XXX use StopP1 to shortcircuit all shard clients
  //bclient[group]->StopP1(txnId);
}


void Client::Phase1TimeoutCallback(int group, uint64_t txnId, int status) {
  auto itr = this->pendingReqs.find(txnId);
  if (itr == this->pendingReqs.end()) {
    return;
  }
  Debug("P1 TIMEOUT IS TRIGGERED for tx_id %d on group %d", txnId, group);
  return;  //TODO:: REMOVE AND REPLACE

  PendingRequest *req = itr->second;
  if (req->startedPhase2 || req->startedWriteback) {
    return;
  }

  Warning("PHASE1[%lu:%lu] group %d timed out.", client_id, txnId, group);

  req->outstandingPhase1s = 0;
  if (params.validateProofs && params.signedMessages) {
    req->slowAbortGroup = -1;
    req->p1ReplySigsGrouped.mutable_grouped_sigs()->clear();
    req->fast = true;
    req->decision = proto::COMMIT;
  }
  Phase1(req);

  //TODO:: alternatively upon timeout: just start Phase1FB for ones own TX:
  //Todo so: shard client needs to upcall with the respective reqId from shard client.. re-create the p1 message.
  // proto::Phase1 *p1 = new proto::Phase1();
  // p1->set_req_id(reqId); //TODO: probably can remove this reqId again.
  // *p1->mutable_txn()= req->txn;
  // Phase1FB(p1, txnId, req->txnDigest);
}

void Client::HandleAllPhase1Received(PendingRequest *req) {
  Debug("All PHASE1's [%lu] received", client_seq_num);

  //NOTE: Forcefully imulated failures, even if according to protocol logic it is not possible:
  if(failureActive && params.injectFailure.type == InjectFailureType::CLIENT_EQUIVOCATE_SIMULATE
       && req->decision == proto::COMMIT){
    Phase2SimulateEquivocation(req);
  }
  else if (req->fast) { //TO force P2, add "req->conflict_flag". Conflict Aborts *must* go fast path.
    Writeback(req);
  } else {
    // slow path, must log final result to 1 group
    if (req->eqv_ready) {
      Phase2Equivocate(req);
    }
    else {
      Phase2(req);
    }
  }
}

void Client::Phase2Processing(PendingRequest *req){

  if (params.validateProofs && params.signedMessages) {
    if (req->decision == proto::ABORT) {
      UW_ASSERT(req->slowAbortGroup >= 0);
      UW_ASSERT(req->p1ReplySigsGrouped.grouped_sigs().find(req->slowAbortGroup) != req->p1ReplySigsGrouped.grouped_sigs().end());
      while (req->p1ReplySigsGrouped.grouped_sigs().size() > 1) {
        auto itr = req->p1ReplySigsGrouped.mutable_grouped_sigs()->begin();
        if (itr->first == req->slowAbortGroup) {
          itr++;  //skips this group, i.e. deletes all besides this one.
        }
        req->p1ReplySigsGrouped.mutable_grouped_sigs()->erase(itr);
      }
    }

    uint64_t quorumSize = req->decision == proto::COMMIT ?
        SlowCommitQuorumSize(config) : SlowAbortQuorumSize(config);
    for (auto &groupSigs : *req->p1ReplySigsGrouped.mutable_grouped_sigs()) {
      while (static_cast<uint64_t>(groupSigs.second.sigs_size()) > quorumSize) {
        groupSigs.second.mutable_sigs()->RemoveLast();
      }
    }
  }

  req->startedPhase2 = true;
}

void Client::Phase2(PendingRequest *req) {
  int64_t logGroup = GetLogGroup(txn, req->txnDigest);

  Debug("PHASE2[%lu:%lu][%s] logging to group %ld", client_id, client_seq_num,
      BytesToHex(req->txnDigest, 16).c_str(), logGroup);

  Phase2Processing(req);

  bclient[logGroup]->Phase2(client_seq_num, txn, req->txnDigest, req->decision,
      req->p1ReplySigsGrouped,
      std::bind(&Client::Phase2Callback, this, req->id, logGroup,
        std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
      std::bind(&Client::Phase2TimeoutCallback, this, logGroup, req->id,
        std::placeholders::_1), req->timeout);

    //XXX use StopP1 to shortcircuit all shard clients
  // for(auto group : req->txn.involved_groups()){
  //     bclient[group]->StopP1(req->id);
  // }
}

void Client::Phase2SimulateEquivocation(PendingRequest *req){
  int64_t logGroup = GetLogGroup(txn, req->txnDigest);

  Debug("SIMULATE EQUIVOCATION PHASE2[%lu:%lu][%s] logging to group %ld", client_id, client_seq_num,
      BytesToHex(req->txnDigest, 16).c_str(), logGroup);

  Phase2Processing(req);  //trim commit quorums if applicable.

  //Simulating equiv failures, only if current decision is Commit

  bclient[logGroup]->Phase2Equivocate_Simulate(client_seq_num, txn, req->txnDigest,
        req->p1ReplySigsGrouped);

  //std::cerr << "SIMULATED EQUIVOCATION. STOPPING" << std::endl;
    //Panic("simulated equiv.");
    //terminate ongoing tx mangagement and move to next tx:
  stats.Increment("inject_equiv_forced", 1);
  FailureCleanUp(req);
  return;

}

void Client::Phase2Equivocate(PendingRequest *req) {
  int64_t logGroup = GetLogGroup(txn, req->txnDigest);

  Debug("PHASE2[%lu:%lu][%s] logging to group %ld with equivocation", client_id, client_seq_num,
      BytesToHex(req->txnDigest, 16).c_str(), logGroup);

  if (params.validateProofs && params.signedMessages) {
    // build grouped commits sigs with size of commitQuorum
    for (auto &groupSigs : *req->p1ReplySigsGrouped.mutable_grouped_sigs()) {
      while (static_cast<uint64_t>(groupSigs.second.sigs_size()) > SlowCommitQuorumSize(config)) {
        groupSigs.second.mutable_sigs()->RemoveLast();
      }
    }

    // build grouped abort sigs with *one* shard Quorum the size of abortQuorum

      UW_ASSERT(req->slowAbortGroup >= 0);
      UW_ASSERT(req->eqvAbortSigsGrouped.grouped_sigs().find(req->slowAbortGroup) != req->eqvAbortSigsGrouped.grouped_sigs().end());
      while (req->eqvAbortSigsGrouped.grouped_sigs().size() > 1) {
        auto itr = req->eqvAbortSigsGrouped.mutable_grouped_sigs()->begin();
        if (itr->first == req->slowAbortGroup) {
          itr++;  //skips this group, i.e. deletes all besides this one.
        }
        req->eqvAbortSigsGrouped.mutable_grouped_sigs()->erase(itr);
      }



    for (auto &groupSigs : *req->eqvAbortSigsGrouped.mutable_grouped_sigs()) {
      while (static_cast<uint64_t>(groupSigs.second.sigs_size()) > SlowAbortQuorumSize(config)) {
        groupSigs.second.mutable_sigs()->RemoveLast();
      }
    }
  }

  req->startedPhase2 = true;
  bclient[logGroup]->Phase2Equivocate(client_seq_num, txn, req->txnDigest,
      req->p1ReplySigsGrouped, req->eqvAbortSigsGrouped);
  //NOTE DONT need to use version with callbacks... callbacks are obsolete if we return directly.
  // bclient[logGroup]->Phase2Equivocate(client_seq_num, txn, req->txnDigest,
  //     req->p1ReplySigsGrouped, req->eqvAbortSigsGrouped,
  //     std::bind(&Client::Phase2Callback, this, req->id, logGroup,
  //       std::placeholders::_1),
  //     std::bind(&Client::Phase2TimeoutCallback, this, logGroup, req->id,
  //       std::placeholders::_1), req->timeout);

  //terminate ongoing tx mangagement and move to next tx:
  stats.Increment("inject_equiv_real");
  FailureCleanUp(req);
}

void Client::Phase2Callback(uint64_t txnId, int group, proto::CommitDecision decision, uint64_t decision_view,
    const proto::Signatures &p2ReplySigs) {

  auto itr = this->pendingReqs.find(txnId);
  if (itr == this->pendingReqs.end()) {
    Debug("Phase2Callback for terminated request id %lu (txn already committed or aborted).", txnId);
    return;
  }

  Debug("PHASE2[%lu:%lu] callback", client_id, txnId);

  PendingRequest *req = itr->second;

  if (req->startedWriteback) {
    Debug("Already started Writeback for request id %lu. Ignoring Phase2 response.",
        txnId);
    return;
  }

  if (params.validateProofs && params.signedMessages) {
    (*req->p2ReplySigsGrouped.mutable_grouped_sigs())[group] = p2ReplySigs;
  }

  req->decision = decision;
  req->decision_view = decision_view;

  Writeback(req);
}

void Client::Phase2TimeoutCallback(int group, uint64_t txnId, int status) {
  auto itr = this->pendingReqs.find(txnId);
  if (itr == this->pendingReqs.end()) {
    return;
  }

  PendingRequest *req = itr->second;
  if (req->startedWriteback) {
    return;
  }

  Warning("PHASE2[%lu:%lu] group %d timed out.", client_id, txnId, group);
  Panic("P2 timing out for txnId: %lu; honest client: %s", txnId, failureActive ? "False" : "True");

  Phase2(req);
}

void Client::WritebackProcessing(PendingRequest *req){

  //////Block to handle Fast Abort case with no conflict: Reduce groups sent... and total replies sent
  if (params.validateProofs && params.signedMessages) {
    if (req->decision == proto::ABORT && req->fast && !req->conflict_flag) {
      UW_ASSERT(req->fastAbortGroup >= 0);
      UW_ASSERT(req->p1ReplySigsGrouped.grouped_sigs().find(req->fastAbortGroup) != req->p1ReplySigsGrouped.grouped_sigs().end());
      while (req->p1ReplySigsGrouped.grouped_sigs().size() > 1) {
        auto itr = req->p1ReplySigsGrouped.mutable_grouped_sigs()->begin();
        if (itr->first == req->fastAbortGroup) {
          itr++;
        }
        req->p1ReplySigsGrouped.mutable_grouped_sigs()->erase(itr);
      }

      uint64_t quorumSize = FastAbortQuorumSize(config);
      for (auto &groupSigs : *req->p1ReplySigsGrouped.mutable_grouped_sigs()) {
        while (static_cast<uint64_t>(groupSigs.second.sigs_size()) > quorumSize) {
          groupSigs.second.mutable_sigs()->RemoveLast();
        }
      }
     }
  }

  req->writeback.Clear();
  // create commit request
  req->writeback.set_decision(req->decision);
  if (params.validateProofs && params.signedMessages) {
    if (req->fast && req->decision == proto::COMMIT) {
      *req->writeback.mutable_p1_sigs() = std::move(req->p1ReplySigsGrouped);
    }
    else if (req->fast && !req->conflict_flag && req->decision == proto::ABORT) {
      *req->writeback.mutable_p1_sigs() = std::move(req->p1ReplySigsGrouped);
    }
    else if (req->fast && req->conflict_flag && req->decision == proto::ABORT) {
      if(req->conflict.has_p2_view()){
        req->writeback.set_p2_view(req->conflict.p2_view()); //XXX not really necessary, we never check it
      }
      else{
        req->writeback.set_p2_view(0); //implies that this was a p1 proof for the conflict, attaching a view anyway..
      }
      *req->writeback.mutable_conflict() = std::move(req->conflict);

    } else {
      *req->writeback.mutable_p2_sigs() = std::move(req->p2ReplySigsGrouped);
      req->writeback.set_p2_view(req->decision_view); //TODO: extend this to process other views too? Bookkeeping should only be needed
      // for fallback though. Either combine the logic, or change it so that the orignial client issues FB function too

    }
  }
  req->writeback.set_txn_digest(req->txnDigest);
}

void Client::Writeback(PendingRequest *req) {

  //total_writebacks++;
  Debug("WRITEBACK[%lu:%lu] result %s", client_id, req->id, req->decision ?  "ABORT" : "COMMIT");
  req->startedWriteback = true;

  if (failureActive && params.injectFailure.type == InjectFailureType::CLIENT_STALL_AFTER_P1) {
    Debug("INJECT CRASH FAILURE[%lu:%lu] with decision %d. txnDigest: %s", client_id, req->id, req->decision,
          BytesToHex(TransactionDigest(req->txn, params.hashDigest), 16).c_str());
    stats.Increment("inject_stall_after_p1", 1);
    //stats.Increment("total_stall_after_p1");
    //total_failure_injections++;
    FailureCleanUp(req);
    return;
  }


  transaction_status_t result;
  switch (req->decision) {
    case proto::COMMIT: {
      Debug("WRITEBACK[%lu:%lu][%s] COMMIT.", client_id, req->id,
          BytesToHex(req->txnDigest, 16).c_str());
      result = COMMITTED;
      break;
    }
    case proto::ABORT: {
      result = ABORTED_SYSTEM;
      Debug("WRITEBACK[%lu:%lu][%s] ABORT.", client_id, req->id,
          BytesToHex(req->txnDigest, 16).c_str());
      break;
    }
    default: {
      NOT_REACHABLE();
    }
  }
  //this function truncates sigs for the Abort p1 fast case
  WritebackProcessing(req);

  // if(!ValidateWB(req->writeback, &req->txnDigest, &req->txn)){ //FIXME: Remove: Just for testing.
  //   Panic("Writeback Validation should never be false for own proposal");
  //   return;
  // }

  //if(req->decision ==0 && client_id == 1) Panic("Testing client 1 fail stop after preparing."); //Manual testing.

  for (auto group : txn.involved_groups()) {
    bclient[group]->Writeback(client_seq_num, req->writeback);
    // bclient[group]->Writeback(client_seq_num, txn, req->txnDigest,
    //     req->decision, req->fast, req->conflict_flag, req->conflict, req->p1ReplySigsGrouped,
    //     req->p2ReplySigsGrouped, req->decision_view);
  }

  if (!req->callbackInvoked) {
    uint64_t ns = Latency_End(&commitLatency);
    //For Failures (byz clients), do not count any commits in order to isolate honest throughput.
    if(failureEnabled && result == COMMITTED){ //--> breaks overall tput???
      //stats.Increment("total_user_abort_orig_commit", 1);
      stats.Increment("total_commit_byz", 1);
      result = ABORTED_USER;
    }
    else if(!failureEnabled && result == COMMITTED){
      stats.Increment("total_commit_honest", 1);
    }
    if(failureEnabled && result == ABORTED_SYSTEM){ //--> breaks overall tput???
      stats.Increment("total_abort_byz", 1);
      //stats.Increment("total_user_abort_orig_abort", 1);
      //result = ABORTED_USER;
    }
    else if(!failureEnabled && result == ABORTED_SYSTEM){ //--> breaks overall tput???
      stats.Increment("total_abort_honest", 1);
      //result = ABORTED_USER;
    }
    req->ccb(result);
    req->callbackInvoked = true;
  }
  //XXX use StopP1 to shortcircuit all shard clients
  // for(auto group : req->txn.involved_groups()){
  //   bclient[group]->StopP1(req->id);
  // }

  this->pendingReqs.erase(req->id);
  delete req;
}

bool Client::IsParticipant(int g) const {
  for (const auto &participant : txn.involved_groups()) {
    if (participant == g) {
      return true;
    }
  }
  return false;
}

void Client::Abort(abort_callback acb, abort_timeout_callback atcb,
    uint32_t timeout) {
  transport->Timer(0, [this, acb, atcb, timeout]() {
    // presumably this will be called with empty callbacks as the application can
    // immediately move on to its next transaction without waiting for confirmation
    // that this transaction was aborted

    uint64_t ns = Latency_End(&executeLatency);

    Debug("ABORT[%lu:%lu]", client_id, client_seq_num);

    for (auto group : txn.involved_groups()) {
      bclient[group]->Abort(client_seq_num, txn.timestamp(), txn); 
    }

    // TODO: can we just call callback immediately?
    acb();
  });
}

// for crash, report the result of p1
// for equivocation, always report ABORT, always delete
void Client::FailureCleanUp(PendingRequest *req) {

  //usleep(3000); //sleep X/1000 miliseconds as to not return immediately...
  stats.Increment("inject_failure");
  stats.Increment("total_user_abort", 1);
  UW_ASSERT(failureActive);
  transaction_status_t result;
  Debug("FailureCleanUp[%lu:%lu] for type[%s]", client_id, req->id,
    params.injectFailure.type == InjectFailureType::CLIENT_CRASH ? "CRASH" : "EQUIVOCATE");
  if (!failureEnabled && params.injectFailure.type == InjectFailureType::CLIENT_CRASH) {
    if (req->decision == proto::COMMIT) {
      result = COMMITTED;
    } else {
      result = ABORTED_USER;
      //result = ABORTED_SYSTEM;
    }
  } else {
    // always report ABORT for equivocation
    result = ABORTED_USER;
    //result = ABORTED_SYSTEM;

  }
  if (!req->callbackInvoked) {
    uint64_t ns = Latency_End(&commitLatency);
    req->ccb(result);
    req->callbackInvoked = true;
  }
  this->pendingReqs.erase(req->id);
  delete req;
}

//DEPRECATED/UNUSED
//INSTEAD: just use the existing ForwardWriteback function from the FB, and then add ongoing txn to it..
void Client::ForwardWBcallback(uint64_t txnId, int group, proto::ForwardWriteback &forwardWB){ 
  auto itr = this->pendingReqs.find(txnId);
  if (itr == this->pendingReqs.end()) {
    Debug("ForwardWBcallback for terminated request id %lu (txn already committed or aborted).", txnId);
    return;
  }
  PendingRequest *req = itr->second;

  if (req->startedWriteback) {
    Debug("Already started Writeback for request id %lu. Ignoring Phase2 response.",
        txnId);
    return;
  }

  req->startedWriteback = true;
  req->decision = forwardWB.decision();
  if(forwardWB.has_p1_sigs()){
    req->fast = true;
    req->p1ReplySigsGrouped.Swap(forwardWB.mutable_p1_sigs());
  }
  else if(forwardWB.has_p2_sigs()){
    req->fast = false;
    req->p2ReplySigsGrouped.Swap(forwardWB.mutable_p2_sigs());
  }
  else if(forwardWB.has_conflict()){
    req->conflict_flag = true;
    req->conflict.Swap(forwardWB.mutable_conflict());
  }
  else{
    Panic("ForwardWB message has no proofs");
  }

  transaction_status_t result;
  switch (req->decision) {
    case proto::COMMIT: {
      Debug("WRITEBACK[%lu:%lu][%s] COMMIT.", client_id, req->id,
          BytesToHex(req->txnDigest, 16).c_str());
      result = COMMITTED;
      break;
    }
    case proto::ABORT: {
      result = ABORTED_SYSTEM;
      Debug("WRITEBACK[%lu:%lu][%s] ABORT.", client_id, req->id,
          BytesToHex(req->txnDigest, 16).c_str());
      break;
    }
    default: {
      NOT_REACHABLE();
    }
  }

  for (auto group : txn.involved_groups()) {
    //bclient[group]->Writeback(client_seq_num, writeback);
    bclient[group]->Writeback(client_seq_num, txn, req->txnDigest,
        req->decision, req->fast, req->conflict_flag, req->conflict, req->p1ReplySigsGrouped,
        req->p2ReplySigsGrouped);
  }

  if (!req->callbackInvoked) {
    uint64_t ns = Latency_End(&commitLatency);
    req->ccb(result);
    req->callbackInvoked = true;
  }

}
///////////////////////// Fallback logic starts here ///////////////////////////////////////////////////////////////

void Client::FinishConflict(uint64_t reqId, const std::string &txnDigest, proto::Phase1 *p1){
  //check whether we already have FB instance for this going.. dont do multiple and replace old FB.
  if(FB_instances.find(txnDigest) != FB_instances.end()) return;

  // i.e. did a re-do still not help. THEN start this.


  PendingRequest* pendingFB = new PendingRequest(0, this); //Id doesnt really matter here
  //pendingFB->txn = std::move(*txn);
  if(params.signClientProposals){
     pendingFB->signed_txn = std::move(p1->signed_txn());
  }
  //else{ //Always move txn as well: SendingPhase1FB requires the txn. Shardclient parses txn from signed_txn and adds it to P1.
     pendingFB->txn = std::move(p1->txn());
  //}

  pendingFB->txnDigest = txnDigest;
  pendingFB->has_dependent = false;
  FB_instances[txnDigest] = pendingFB; //relies on altruism.

  Debug("Started Phase1FB for txn: %s, for conflicting ID: %d", BytesToHex(txnDigest, 16).c_str(), reqId);
  SendPhase1FB(reqId, txnDigest, pendingFB); //TODO change so that it does not require p1.
  //delete p1; already deleted inside shard client PendingPhase1 destructor.
  //delete txn; //WARNING dont delete, since shard client already deletes itself.
  if(!failureEnabled) stats.Increment("total_honest_conflict_FB_started", 1);
}

bool Client::isDep(const std::string &txnDigest, proto::Transaction &Req_txn){

  for(auto & dep: Req_txn.deps()){
   if(dep.write().prepared_txn_digest() == txnDigest){ return true;}
  }
  return false;
}

bool Client::StillActive(uint64_t conflict_id, std::string &txnDigest){
  //check if FB instance is (still) active.
  auto itr = FB_instances.find(txnDigest);
  if(itr == FB_instances.end()){
    Debug("FB instances %s already committed or aborted.", txnDigest.c_str());
    return false;
  }

  return true; //Altruism
  //TODO: Can be altruistic and finish FB instance even if not blocking.
  auto itrReq = pendingReqs.find(conflict_id);
  if(itrReq == pendingReqs.end() || itrReq->second->outstandingPhase1s == 0){
    Debug("Request id %lu already committed, aborted, or in the process of doing so.", conflict_id);
    CleanFB(itr->second, txnDigest); //call into Shard clients to clean up state as well!
    return false;
  }

  //check if we have a dependent (has_dependent), and whether it is done? If so, call cleanCB()
  if(itr->second->has_dependent && FB_instances.find(itr->second->dependent) == FB_instances.end()){
    Debug("Dependent of txn[%s] already committed, aborted, or in the process of doing so.", BytesToHex(txnDigest, 64).c_str());
    CleanFB(itr->second, txnDigest); //call into Shard clients to clean up state as well!
    return false;
  }

  return true;
}

void Client::CleanFB(PendingRequest *pendingFB, const std::string &txnDigest, bool clean_shards){
  Debug("Called CleanFB for txnDigest: %s ", BytesToHex(txnDigest, 64).c_str());
  if(clean_shards){
    for(auto group : pendingFB->txn.involved_groups()){
      Debug("Cleaned shard client: %d", group);
      bclient[group]->CleanFB(txnDigest);
    } //TODO: this is not actually necessary if it is part of pendingFB destructor...
  }
  FB_instances.erase(txnDigest);
  delete pendingFB;
}

void Client::EraseRelays(proto::RelayP1 &relayP1, std::string &txnDigest){
  for(auto group : relayP1.p1().txn().involved_groups()){
    bclient[group]->EraseRelay(txnDigest);
  }
}

void Client::RelayP1callback(uint64_t reqId, proto::RelayP1 &relayP1, std::string& txnDigest){

  auto itr = pendingReqs.find(reqId);
  if(itr == pendingReqs.end()){
    Debug("ReqId[%d] has already completed", reqId);
    //EraseRelays(relayP1, txnDigest);
    return;
  }

  //const std::string &txnDigest = TransactionDigest(p1->txn(), params.hashDigest);

  //do not start multiple FB instances for the same TX
  //if(itr->second->req_FB_instances.find(txnDigest) != itr->second->req_FB_instances.end()) return;
  if(Completed_transactions.find(txnDigest) != Completed_transactions.end()) return;
  if(FB_instances.find(txnDigest) != FB_instances.end()) return;
  //Check if the current pending request has this txn as dependency.
  if(!isDep(txnDigest, itr->second->txn)){
    Debug("Tx[%s] is not a dependency of ReqId: %d", BytesToHex(txnDigest, 16).c_str(), reqId);
    return;
  }

  proto::Phase1 *p1 = relayP1.release_p1();

  if(itr->second->startFB){
    Debug("Starting Phase1FB directly for txn: %s", BytesToHex(txnDigest, 16).c_str());
    //itr->second->req_FB_instances.insert(txnDigest); //XXX mark connection between reqId and FB instance
    Phase1FB(txnDigest, reqId, p1);
  }
  else{
    Debug("Adding to FB buffer for txn: %s", BytesToHex(txnDigest, 16).c_str());
    //itr->second->RelayP1s.emplace_back(p1, txnDigest);
    itr->second->RelayP1s[txnDigest] = p1;
  }
}

void Client::RelayP1TimeoutCallback(uint64_t reqId){
  auto itr = pendingReqs.find(reqId);
  if(itr == pendingReqs.end()){
    Debug("ClientSeqNum[%d] has already completed", reqId);
    return;
  }

  itr->second->startFB = true;
  for(auto p1_pair : itr->second->RelayP1s){
    Debug("Starting Phase1FB from FB buffer for dependent txnId: %d", reqId);
    //itr->second->req_FB_instances.insert(p1_pair.first); //XXX mark connection between reqId and FB instance
    Phase1FB(p1_pair.first, reqId, p1_pair.second);
  }
  //
}

// Additional Relay FB handler for Fallbacks for dependencies of dependencies.
void Client::RelayP1callbackFB(uint64_t reqId, const std::string &dependent_txnDigest, proto::RelayP1 &relayP1, std::string& txnDigest){
  //dont need to respect a time out here?

  auto itr = pendingReqs.find(reqId);
  if(itr == pendingReqs.end()){
    Debug("ReqId[%d] has already completed", reqId);
    //EraseRelays(relayP1, txnDigest);
    return;
  }

  auto itrFB = FB_instances.find(dependent_txnDigest);
  if(itrFB == FB_instances.end()){
    Debug("FB txn[%s] has already completed", BytesToHex(dependent_txnDigest, 64).c_str());
    //EraseRelays(relayP1, txnDigest);
    return;
  }

  //const std::string &txnDigest = TransactionDigest(p1->txn(), params.hashDigest);

  //do not start multiple FB instances for the same TX
  //if(itr->second->req_FB_instances.find(txnDigest) != itr->second->req_FB_instances.end()) return; //Unlike FB_instances, this set only gets erased upon Request completion
  if(Completed_transactions.find(txnDigest) != Completed_transactions.end()) return;
  if(FB_instances.find(txnDigest) != FB_instances.end()) return;
   //Check if the current pending request has this txn as dependency.
  if(!isDep(txnDigest, itrFB->second->txn)){
    Debug("Tx[%s] is not a dependency of ReqId: %d", BytesToHex(txnDigest, 128).c_str(), reqId);
    return;
  }

  proto::Phase1 *p1 = relayP1.release_p1();

  Debug("Starting Phase1FB for deeper depth right away");

  //itr->second->req_FB_instances.insert(txnDigest); //XXX mark connection between reqId and FB instance
  Phase1FB_deeper(reqId, txnDigest, dependent_txnDigest, p1);
}


void Client::Phase1FB(const std::string &txnDigest, uint64_t conflict_id, proto::Phase1 *p1){  //passes callbacks

  Debug("Started Phase1FB for txn: %s, for dependent ID: %d", BytesToHex(txnDigest, 16).c_str(), conflict_id);

  PendingRequest* pendingFB = new PendingRequest(p1->req_id(), this); //Id doesnt really matter here
  if(params.signClientProposals){
     pendingFB->signed_txn = std::move(p1->signed_txn()); 
     // QUESTION: Should Clients verify signed txn they receive themselves before proposing them for fallback?
     // ANSWER: No need. If a byz replica forwarded a txn with a wrong signature, then the client is doing some wasteful work by starting a FB that will be rejected, but it doesnt affect anything else.
  }
  //else{ //Always move txn as well: SendingPhase1FB requires the txn. Shardclient parses txn from signed_txn and adds it to P1.
     pendingFB->txn = std::move(p1->txn());
  //}
  pendingFB->txnDigest = txnDigest;
  pendingFB->has_dependent = false;
  FB_instances[txnDigest] = pendingFB;

  SendPhase1FB(conflict_id, txnDigest, pendingFB);
  delete p1;

  if(!failureEnabled){
    stats.Increment("total_honest_dep_FB_started", 1);
    // if(conflict_ids.insert(conflict_id).second){
    //   stats.Increment("total_honest_p1_needed_FB", 1);
    // }
  }

}

void Client::Phase1FB_deeper(uint64_t conflict_id, const std::string &txnDigest, const std::string &dependent_txnDigest, proto::Phase1 *p1){

  Debug("Starting Phase1FB for deeper depth. Original conflict id: %d, Dependent txnDigest: %s, txnDigest of tx causing the stall %s", conflict_id, BytesToHex(dependent_txnDigest, 16).c_str(), BytesToHex(txnDigest, 16).c_str());

  PendingRequest* pendingFB = new PendingRequest(p1->req_id(), this); //Id doesnt really matter here
  if(params.signClientProposals){
     pendingFB->signed_txn = std::move(p1->signed_txn());
  }
  //else{ //Always move txn as well: SendingPhase1FB requires the txn. Shardclient parses txn from signed_txn and adds it to P1.
     pendingFB->txn = std::move(p1->txn());
  //}
  pendingFB->txnDigest = txnDigest;
  pendingFB->has_dependent = true;
  pendingFB->dependent = dependent_txnDigest;
  FB_instances[txnDigest] = pendingFB;

  SendPhase1FB(conflict_id, txnDigest, pendingFB);
  delete p1;

  Debug("PendingFB request tx:[%s] has default decision: %s", BytesToHex(txnDigest, 16).c_str(), pendingFB->decision ? "ABORT" : "COMMIT");
  if(!failureEnabled) stats.Increment("total_honest_deeper_FB_started", 1);
}

void Client::SendPhase1FB(uint64_t conflict_id, const std::string &txnDigest, PendingRequest *pendingFB){
  Debug("trying to send Phase1FB for txn %s", BytesToHex(txnDigest, 16).c_str());
  pendingFB->logGrp = GetLogGroup(pendingFB->txn, txnDigest);
  for (auto group : pendingFB->txn.involved_groups()) {
      Debug("Client %d, Send Phase1FB for txn %s to involved group %d", client_id, BytesToHex(txnDigest, 16).c_str(), group);

    //define all the callbacks here
      //bind conflict_id (i.e. the top level dependent) to all, so we can check if it is still waiting.
      //TODO: dont bind it but make it a field of the PendingRequest..
      auto p1Relay = std::bind(&Client::RelayP1callbackFB, this, conflict_id, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);

      auto p1fbA = std::bind(&Client::Phase1FBcallbackA, this, conflict_id, txnDigest, group, std::placeholders::_1,
          std::placeholders::_2, std::placeholders::_3, std::placeholders::_4, std::placeholders::_5);
      auto p1fbB = std::bind(&Client::Phase1FBcallbackB, this, conflict_id, txnDigest, group, std::placeholders::_1,
        std::placeholders::_2);
      auto p2fb = std::bind(&Client::Phase2FBcallback, this, conflict_id, txnDigest, group, std::placeholders::_1,
        std::placeholders::_2, std::placeholders::_3);
      auto wb = std::bind(&Client::WritebackFBcallback, this, conflict_id, txnDigest, std::placeholders::_1);
      auto invoke = std::bind(&Client::InvokeFBcallback, this, conflict_id, txnDigest, group); //technically only needed at logging shard

      //bclient[group]->Phase1FB(p1->req_id(), pendingFB->txn, txnDigest, p1Relay, p1fbA, p1fbB, p2fb, wb, invoke);
      bclient[group]->Phase1FB(client_id, pendingFB->txn, pendingFB->signed_txn, txnDigest, p1Relay, p1fbA, p1fbB, p2fb, wb, invoke, pendingFB->logGrp);

      pendingFB->outstandingPhase1s++;
    }
  Debug("Sent all Phase1FB for txn[%s]", BytesToHex(txnDigest, 64).c_str());
  return;

}



//If Receive a finished FB just forward it.
void Client::WritebackFBcallback(uint64_t conflict_id, std::string txnDigest, proto::Writeback &wb) {
  Debug("Received WritebackFB callback fast for txn: %s", BytesToHex(txnDigest, 16).c_str());

  if(!StillActive(conflict_id, txnDigest)) return;
  auto itr = FB_instances.find(txnDigest);
  PendingRequest *pendingFB = itr->second;
  if(pendingFB->startedWriteback) return;
  pendingFB->startedWriteback = true;

  Debug("Forwarding WritebackFB fast for txn: %s",BytesToHex(txnDigest, 16).c_str());
  //Note: Currently optimistically just accepting forwarded Writeback and stopping Fallback.
  //Note: TODO: May want to pessimistically validate WB message: Don't need to validate it for safety! (replicas will verify it). Only for liveness, since currently we stop fallback processing. Either verify or continue FB processing.
  //TODO: Would want it to be asynchronous though, such that it doesn't block the client from receiving and processing its ongoing tx.
  // if(!ValidateWB(wb, &txnDigest, &pendingFB->txn)){
  //   Panic("Wb validation should never be false while testing without byz replica sending corrupt message");
  //   return;
  // }
  
  //CHECK THAT fbtxn matches digest? Not necessary if we just set the contents ourselves.
  //Also: server side message might not include txn, hence include it ourselves just in case.
  *wb.mutable_txn() = std::move(pendingFB->txn);

 for (auto group : wb.txn().involved_groups()) {
   bclient[group]->WritebackFB_fast(txnDigest, wb);
 }
 //delete FB instance. (doing so early will make sure other ShardClients dont waste work.)
 Completed_transactions.insert(txnDigest);
 CleanFB(pendingFB, txnDigest, false); //NOTE: In this case, shard clients clean themselves (in order to support the move operator)
}

bool Client::ValidateWB(proto::Writeback &msg, std::string *txnDigest, proto::Transaction *txn){
  Debug("Validating Writeback msg for txn %s", BytesToHex(*txnDigest, 16).c_str());
  //Note: Does not support multithreading or batchVerification currently.

  // 1) check that txnDigest matches txn content
  if (msg.has_txn_digest()){
    if(*txnDigest != msg.txn_digest()){
      std::cerr << "txnDigs don't match" << std::endl;
      return false;
    } 
  }
  else if(msg.has_txn()){
    if(*txnDigest != TransactionDigest(msg.txn(), params.hashDigest)){
      std::cerr << "txnDig doesnt match Transaction" << std::endl;
      return false;
    } 
  }
  else {
    Panic("Should have txn or txn_digest");
    return false;
  }
  
   // 2) check that sigs match decision and txnDigest
  if (params.validateProofs) {
    if (params.signedMessages && msg.has_p1_sigs()) {
        proto::ConcurrencyControl::Result myResult;

        if (!ValidateP1Replies(msg.decision(), true, txn, txnDigest, msg.p1_sigs(), keyManager, config, -1, myResult, verifier)) {
              Debug("WRITEBACK[%s] Failed to validate P1 replies for fast decision %s.", BytesToHex(*txnDigest, 16).c_str(), (msg.decision() == proto::CommitDecision::COMMIT) ? "commit" : "abort");
              std::cerr << "Wb failed P1 fast validation" << std::endl;
              return false;
        }   
    }
    else if (params.signedMessages && msg.has_p2_sigs()) {
        if(!msg.has_p2_view()) return false;
        proto::CommitDecision myDecision;
  
        if (!ValidateP2Replies(msg.decision(), msg.p2_view(), txn, txnDigest, msg.p2_sigs(), keyManager, config, -1, myDecision, verifier)) {
                Debug("WRITEBACK[%s] Failed to validate P2 replies for decision %s.", BytesToHex(*txnDigest, 16).c_str(), (msg.decision() == proto::CommitDecision::COMMIT) ? "commit" : "abort");
                std::cerr << "Wb failed P2 slow validation" << std::endl;
                return false;
        }
    } 
    else if (msg.decision() == proto::ABORT && msg.has_conflict()) {
      std::string committedTxnDigest = TransactionDigest(msg.conflict().txn(), params.hashDigest);

      if (!ValidateCommittedConflict(msg.conflict(), &committedTxnDigest, txn, txnDigest, params.signedMessages, keyManager, config, verifier)) {
            Debug("WRITEBACK[%s] Failed to validate committed conflict for fast abort.", BytesToHex(*txnDigest, 16).c_str());
            std::cerr << "Wb failed conflict validation" << endl;
            return false;
      }
    } 
    else if (params.signedMessages) {
      Debug("WRITEBACK[%s] decision %d, has_p1_sigs %d, has_p2_sigs %d, and has_conflict %d.", BytesToHex(*txnDigest, 16).c_str(), msg.decision(), msg.has_p1_sigs(), msg.has_p2_sigs(), msg.has_conflict());
      Panic("Wb without proof.");
      return false;
    }
  }  
  return true;
}



void Client::Phase1FBcallbackA(uint64_t conflict_id, std::string txnDigest, int64_t group,
  proto::CommitDecision decision, bool fast, bool conflict_flag,
  const proto::CommittedProof &conflict,
  const std::map<proto::ConcurrencyControl::Result, proto::Signatures> &sigs)  {

  Debug("Phase1FBcallbackA called for txn[%s] with decision: %d", BytesToHex(txnDigest, 64).c_str(), decision);

  if(!StillActive(conflict_id, txnDigest)) return;

  PendingRequest* req = FB_instances[txnDigest];

  if (req->startedPhase2 || req->startedWriteback) {
    Debug("Already started Phase2FB/WritebackFB for tx [%s[]. Ignoring Phase1 callback"
        " response from group %d.", BytesToHex(txnDigest, 128).c_str(), group);
    return;
  }

  Debug("Processing Phase1CallbackA for txn: %s with decision %d", BytesToHex(txnDigest, 16).c_str(), decision);
  Phase1CallbackProcessing(req, group, decision, fast, conflict_flag, conflict, sigs);

  if (req->outstandingPhase1s == 0) {
    FBHandleAllPhase1Received(req);
  }
  //TODO: find mechanism to include this work in InvokeFB too.
}



void Client::FBHandleAllPhase1Received(PendingRequest *req) {
  Debug("FB instance [%s]: All PHASE1's received", BytesToHex(req->txnDigest, 16).c_str());
  if (req->fast) {
    WritebackFB(req);
  } else {
    // slow path, must log final result to 1 group
    Phase2FB(req);
  }
}


//XXX cannot just send decision, but need to send whole p2 replies because the decision views might differ
//making it of type bool so that shard client does not require req Id dependency
bool Client::Phase1FBcallbackB(uint64_t conflict_id, std::string txnDigest, int64_t group,
   proto::CommitDecision decision, const proto::P2Replies &p2replies){

     Debug("Phase1FBcallbackB called for txn[%s] with decision %d", BytesToHex(txnDigest, 64).c_str(), decision);

     // check if conflict transaction still active
    if(!StillActive(conflict_id, txnDigest)) return false;

    PendingRequest* req = FB_instances[txnDigest];

    if (req->startedPhase2 || req->startedWriteback) {
      Debug("Already started Phase2FB/WritebackFB for tx [%s[]. Ignoring Phase1 callback"
          " response from group %d.", BytesToHex(txnDigest, 128).c_str(), group);
      return true;
    }

    req->decision = decision;
    req->p2Replies = std::move(p2replies);
    //WARNING: CURRENTLY HAVE PHASE1FBCALLBACK B DISABLED. BUT STILL USING P2Replies for Invoke.
    return true;
       //Issue P2FB.
    Phase2FB(req);

    return true;
        //TODO: find a mechanism to include this work in InvokeFB too.
}

void Client::Phase2FB(PendingRequest *req){

      Debug("Sending Phase2FB for txn[%s] with decision: %d", BytesToHex(req->txnDigest, 16).c_str(), req->decision);

      const proto::Transaction &fb_txn = req->txn;

      // uint8_t groupIdx = req->txnDigest[0];
      // groupIdx = groupIdx % fb_txn.involved_groups_size();
      // UW_ASSERT(groupIdx < fb_txn.involved_groups_size());
      // int64_t logGroup = fb_txn.involved_groups(groupIdx);
      Debug("PHASE2FB[%lu][%s] logging to group %ld", client_id,
          BytesToHex(req->txnDigest, 16).c_str(), req->logGrp);

      //CASE THAT CHECKS FOR P2 REPLIES AS VALID  PROOFS
      // check if p2Replies is of size f+1
      if(req->p2Replies.p2replies().size() >= config->f +1){
        req->startedPhase2 = true;
        bclient[req->logGrp]->Phase2FB(req->id, req->txn, req->txnDigest, req->decision, req->p2Replies);
      }
      else{ //OTHERWISE: Use p1 sigs just like in normal case.
        Phase2Processing(req);

        bclient[req->logGrp]->Phase2FB(req->id, req->txn, req->txnDigest, req->decision,
          req->p1ReplySigsGrouped);
      }
      for (auto group : req->txn.involved_groups()) {
        bclient[group]->StopP1FB(req->txnDigest);
      }
}

//TODO: extend with view.
void Client::Phase2FBcallback(uint64_t conflict_id, std::string txnDigest, int64_t group,
   proto::CommitDecision decision, const proto::Signatures &p2ReplySigs, uint64_t view){

  Debug("Phase2FBcallback called for txn[%s] with decision %d", BytesToHex(txnDigest, 16).c_str(), decision);
  // check if conflict transaction still active
  if(!StillActive(conflict_id, txnDigest)) return;

  Debug("PHASE2FB[%lu:%s] callback", client_id, txnDigest.c_str());

  PendingRequest* req = FB_instances[txnDigest];

  if (req->startedWriteback) {
      Debug("Already started WritebackFB for FB request id %s. Ignoring Phase2FB response.",
               txnDigest.c_str());
          return;
  }
  req->decision = decision;
  req->decision_view = view;
  req->fast = false;

  if (params.validateProofs && params.signedMessages) {
    (*req->p2ReplySigsGrouped.mutable_grouped_sigs())[group] = p2ReplySigs;
  }

  WritebackFB(req);
}

void Client::WritebackFB(PendingRequest *req){

  Debug("WRITEBACKFB[%lu:%s] result: %s", client_id, BytesToHex(req->txnDigest, 16).c_str(), req->decision ? "ABORT" : "COMMIT");

  req->startedWriteback = true;
  WritebackProcessing(req);

  for (auto group : req->txn.involved_groups()) {
    bclient[group]->WritebackFB(req->txnDigest, req->writeback);
    // bclient[group]->Writeback(0, req->txn, req->txnDigest,
    //     req->decision, req->fast, req->conflict_flag, req->conflict, req->p1ReplySigsGrouped,
    //     req->p2ReplySigsGrouped, req->decision_view);
  }
  //delete FB instance. (doing so early will make sure other ShardClients dont waste work.)
  Completed_transactions.insert(req->txnDigest);
  CleanFB(req, req->txnDigest);
}

//TODO: add view here:
bool Client::InvokeFBcallback(uint64_t conflict_id, std::string txnDigest, int64_t group){
  //Just send InvokeFB request to the logging shard. but only if the tx has not already finished. and only if we have already sent a P2
  //Otherwise, Include the P2 here!.

  // check if conflict transaction still active
  if(!StillActive(conflict_id, txnDigest)) return false;


  //TODO: add flags for P2 sent: If not sent yet, need to wait for that.
  PendingRequest* req = FB_instances[txnDigest];
  if(req->startedWriteback){
    Debug("Already sent WB - unecessary InvokeFB");
    return true;
  }
  //TODO: RECOMMENT. Currently assuming that all servers already have p2 decision.
  if(req->p2Replies.p2replies().size() < config->f +1){
    Debug("No p2 decision included - invalid InvokeFB");
    return true;
  }

  Debug("Called InvokeFB on logging shard group %lu, for txn: %s", group, BytesToHex(txnDigest, 16).c_str());
  //we know this group is the FB group, only that group would have invoked this callback.
  bclient[group]->InvokeFB(conflict_id, txnDigest, req->txn, req->decision, req->p2Replies);

return true;
  //TODO: add logic to include a P2 based of P1's.
  // else if (req->outstandingPhase1s == 0) {
  //     req->p1ReplySigsGrouped
  // else return;
}

//void Phase2FB: passes callbacks
//void InvokeFB: calls Phase2FB and adds that to the InvokeFB message.
//void Phase2FBcallback: call either WritebackFB, or InvokeFB




} // namespace pequinstore
