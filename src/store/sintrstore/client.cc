// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/sintrstore/client.cc:
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

#include "store/sintrstore/client.h"

#include "store/sintrstore/localbatchverifier.h"
#include "store/sintrstore/basicverifier.h"
#include "store/sintrstore/common.h"
#include "store/sintrstore/common2.h"
#include "store/sintrstore/policy/policy.h"
#include "store/sintrstore/estimate_policy.h"
#include <sys/time.h>
#include <algorithm>

#include "store/common/query_result/query_result.h"
#include "store/common/query_result/query_result_proto_wrapper.h"
#include "store/common/query_result/query_result_proto_builder.h"

namespace sintrstore {

using namespace std;

//TODO: add argument for p1Timeout, pass down to Shardclient as well.
Client::Client(transport::Configuration *config, uint64_t id, int nShards,
    int nGroups,
    const std::vector<int> &closestReplicas, bool pingReplicas, Transport *transport,
    Partitioner *part, bool syncCommit, uint64_t readMessages,
    uint64_t readQuorumSize, Parameters params, std::string &table_registry,
    KeyManager *keyManager, uint64_t phase1DecisionTimeout, uint64_t warmup_secs, uint64_t consecutiveMax, bool sql_bench,
    TrueTime timeServer, transport::Configuration *clients_config, const std::vector<std::string> &keys)
    : config(config), client_id(id), nshards(nShards), ngroups(nGroups),
    transport(transport), part(part), syncCommit(syncCommit), pingReplicas(pingReplicas),
    readMessages(readMessages), readQuorumSize(readQuorumSize),
    params(params),
    keyManager(keyManager),
    timeServer(timeServer), first(true), startedPings(false),
    query_seq_num(0UL), client_seq_num(0UL), lastReqId(0UL), getIdx(0UL),
    failureEnabled(false), failureActive(false), faulty_counter(0UL),
    consecutiveMax(consecutiveMax),
    sql_interpreter(&params.query_params), clients_config(clients_config), keys(keys) {

  Notice("Sintrstore currently does not support Read-your-own-Write semantics for Queries. Adjust application accordingly!!");

  Notice("Initializing Sintr client with id [%lu] %lu", client_id, nshards);
  Notice("P1 Decision Timeout: %d", phase1DecisionTimeout);

  if(params.injectFailure.enabled) stats.Increment("total_byz_clients", 1);

  if (params.signatureBatchSize == 1) {
    verifier = new BasicVerifier(transport);//transport, 1000000UL,false); //Need to change interface so client can use it too?
  } else {
    verifier = new LocalBatchVerifier(params.merkleBranchFactor, dummyStats, transport);
  }

  /* Start a client for each shard. */
  for (uint64_t i = 0; i < ngroups; i++) {
    bclient.push_back(new ShardClient(config, transport, client_id, i,
        closestReplicas, pingReplicas, readMessages, readQuorumSize, params,
        keyManager, verifier, &sql_interpreter, &stats,
        timeServer, phase1DecisionTimeout, consecutiveMax));
  }

  policyParseClient = new PolicyParseClient();
  policyIdFunction = GetPolicyIdFunction(params.sintr_params.policyFunctionName);
  std::map<uint64_t, Policy *> policies = policyParseClient->ParseConfigFile(params.sintr_params.policyConfigPath);

  endorseClient = new EndorsementClient(client_id, keyManager, policyIdFunction);
  endorseClient->InitializePolicyCache(policies);

  // create client for other clients
  // right now group is always 0, maybe configure later
  c2client = new Client2Client(
    config, clients_config, transport, client_id, nshards, ngroups, 0,
    pingReplicas, params, keyManager, verifier, part, endorseClient, &sql_interpreter,
    table_registry, keys
  );

  Debug("Sintr client [%lu] created! %lu %lu", client_id, nshards,
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
  if(sql_bench){
     Debug("Register tables from: %s", table_registry.c_str());
     sql_interpreter.RegisterTables(table_registry);
     sql_interpreter.RegisterPartitioner(part, nShards, nGroups, -1);
  }

  Notice("Start Timer for %d warmup secs", warmup_secs);
  transport->Timer(warmup_secs * 1000, [this](){
    Notice("Warmup complete!!");
    warmup_done = true;
    for(auto &shard: bclient){
      shard->WarmupDone();
    }
  });
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

  //Note pendingReqs and pendingQueries should be empty. 
  for(auto &[_, pendingFB]: FB_instances){
    delete pendingFB;
  }

  for (auto b : bclient) {
      delete b;
  }
  delete c2client;
  delete endorseClient;
  delete policyParseClient;
  delete verifier;
}

/* Begins a transaction. All subsequent operations before a commit() or
 * abort() are part of this transaction.
 */
void Client::Begin(begin_callback bcb, begin_timeout_callback btcb,
      uint32_t timeout, bool retry, const std::string &txnState) {
  
  // if (exec_time_ms.size() > 0 && exec_time_ms.size() % 2000 == 0) {
  //   double mean_latency = std::accumulate(exec_time_ms.begin(), exec_time_ms.end(), 0.0) / exec_time_ms.size();
  //   std::cerr << "Mean execution latency: " << mean_latency << std::endl;
  //   double mean_endorsement_wait_latency = std::accumulate(endorsement_wait_ms.begin(), endorsement_wait_ms.end(), 0.0) / endorsement_wait_ms.size();
  //   std::cerr << "Mean endorsement wait latency: " << mean_endorsement_wait_latency << std::endl;
  //   double mean_phase1_latency = std::accumulate(phase1_time_ms.begin(), phase1_time_ms.end(), 0.0) / phase1_time_ms.size();
  //   std::cerr << "Mean phase1 latency: " << mean_phase1_latency << std::endl;
  // }

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
    c2client->SetFailureFlag(failureActive);
    if(failureActive) stats.Increment("failure_attempts", 1);
    if(failureEnabled) stats.Increment("total_fresh_tx_byz", 1);
    if(!failureEnabled) stats.Increment("total_fresh_tx_honest", 1);
  }

  transport->Timer(0, [this, bcb, btcb, timeout, txnState]() { 
    if (pingReplicas) {
      if (!first && !startedPings) {
        startedPings = true;
        for (auto sclient : bclient) {
          sclient->StartPings();
        }
        c2client->StartPings();
      }
      first = false;
    }

    Latency_Start(&executeLatency);
    client_seq_num++;

    uint64_t txnStartTime = timeServer.GetTime();
    TxnState protoTxnState;
    protoTxnState.ParseFromString(txnState);
    
    // begin sintr validation
    endorseClient->Reset();
    endorseClient->SetClientSeqNum(client_seq_num);
    // using policy client with default policy set to weight 0 policy
    // TODO: Default should be either ACL or Weight Policy depending on parameter
    PolicyClient *policyClient = new PolicyClient();
    EstimateTxnPolicy(protoTxnState, policyClient);
    c2client->SendBeginValidateTxnMessage(client_seq_num, protoTxnState, txnStartTime, policyClient);
    delete policyClient;
    policyClient = nullptr;
    txn.Clear(); //txn = proto::Transaction();
    txn.set_client_id(client_id);
    txn.set_client_seq_num(client_seq_num);
    // Optimistically choose a read timestamp for all reads in this transaction
    txn.mutable_timestamp()->set_timestamp(txnStartTime);
    txn.mutable_timestamp()->set_id(client_id);

    if (IsPolicyChangeTxn(protoTxnState)) {
      Debug("Begin policy change transaction from client id %lu, seq num %lu", client_id, client_seq_num);
      txn.set_policy_type(proto::Transaction::POLICY_ID_POLICY);
    }

      //std::cerr<< "BEGIN TX with client_seq_num: " << client_seq_num << std::endl;
    Debug("BEGIN [%lu]. TS[%lu:%lu]", client_seq_num, txn.timestamp().timestamp(), txn.timestamp().id());

    sql_interpreter.NewTx(&txn);

    pendingWriteStatements.clear();
    point_read_cache.clear();

    ClearTxnQueries();
    //pendingQueries.clear(); //shouldn't be necessary to call, should be empty anyways

    //FIXME: Just for profiling.
  if(PROFILING_LAT){
     struct timespec ts_start;
    clock_gettime(CLOCK_MONOTONIC, &ts_start);
    exec_start_ms = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;
    // Warning("STARTING NEW TX");
  }

    bcb(client_seq_num);
  });
}

bool Client::IsPolicyChangeTxn(const TxnState &protoTxnState) const {
  return protoTxnState.txn_name().find("policy") != std::string::npos;
}

void Client::EstimateTxnPolicy(const TxnState &protoTxnState, PolicyClient *policyClient) {
  if (IsPolicyChangeTxn(protoTxnState)) {
    // policy change transaction could require separate handling
    const Policy *policy;
    UW_ASSERT(endorseClient->GetPolicyFromCache(0, policy));
    policyClient->AddPolicy(policy);
  } 
  else {
    EstimatePolicy est_policy_obj;
    est_policy_obj.EstimateTxnPolicy(protoTxnState, policyClient, endorseClient);
  }
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
        bool hasDep, bool addReadSet,
        const proto::CommittedProof &proof, const std::string &serializedWrite, 
        const std::string &serializedWriteTypeName, const proto::EndorsementPolicyMessage &policyMsg,
        const proto::Dependency &policyDep, bool hasPolicyDep) {

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
        
        // new policy can only come from server, which must correspond to addReadSet
        if (policyMsg.IsInitialized()) {
          if (Message_DebugEnabled(__FILE__)) {
            Debug("PULL[%lu:%lu] POLICY FOR key %s in GET",client_id, client_seq_num, BytesToHex(key, 16).c_str());
          }
          Policy *policy = policyParseClient->Parse(policyMsg.policy());
          endorseClient->UpdatePolicyCache(policyMsg.policy_id(), policy);
        }
      }
      if (hasDep) {
        *txn.add_deps() = dep;
      }
      if (hasPolicyDep) {
        *txn.add_deps() = policyDep;
      }

      c2client->SendForwardReadResultMessage(
        key, val, ts, proof, serializedWrite, 
        serializedWriteTypeName, dep, hasDep, addReadSet,
        policyDep, hasPolicyDep
      );

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
    Debug("PUT[%lu:%lu] for key %s", client_id, client_seq_num, BytesToHex(key, 16).c_str());

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

    // look in cache for policy
    const Policy *policy;
    bool exists = endorseClient->GetPolicyFromCache(key, policy);
    if (!exists) {
      // if not found, use default policy for now
      uint64_t policyId = policyIdFunction(key, value);
      endorseClient->GetPolicyFromCache(policyId, policy);
    }
    c2client->HandlePolicyUpdate(policy);
    
    if(bclient[i]->GetPolicyShardClient()) {
      // empty callback functions
      // This is a hack, but the downside is that it will add the key to the readset, 
      // which shouldn't happen during a blind write. It may also introduce unnecessary dependencies. 
      // Fortunately, this should occur very rarely.
      if (Message_DebugEnabled(__FILE__)) {
        Debug("PULL[%lu:%lu] POLICY FOR key %s in PUT",client_id, client_seq_num, BytesToHex(key, 16).c_str());
      }
      get_callback gcb = [this](int, const std::string &, const std::string &, Timestamp){
          Debug("get policy callback done");
          get_policy_done -= 1;
      };
      get_timeout_callback tgcb = [](int, const std::string &){
        Panic("TIMEOUT FOR GETTING POLICY VALUE");
      };
      Get(key, gcb, tgcb, timeout);
      get_policy_done += 1;
      Debug("get sent for policy");
    }
    bclient[i]->Put(client_seq_num, key, value, pcb, ptcb, timeout);
  });
}

void Client::SQLRequest(std::string &statement, sql_callback scb,
    sql_timeout_callback stcb, uint32_t timeout){

    size_t pos;
    if((pos = statement.find(select_hook) != string::npos)){  
      Query(statement, std::move(scb), std::move(stcb), timeout);
    }
    else {
      Write(statement, std::move(scb), std::move(stcb), timeout);
    }
}

//primary_key_encoding_support is an encoding_helper function: Specify which columns of a write statement correspond to the primary key; each vector belongs to one insert. 
//In case of nesting or concat --> order = order of reading
void Client::Write(std::string &write_statement, write_callback wcb,
      write_timeout_callback wtcb, uint32_t timeout, bool blind_write){ //blind_write: default false, must be explicit application choice to skip.

    stats.Increment("total_writes");
    //////////////////
    // Write Statement parser/interpreter:   //For now design to supports only individual Insert/Update/Delete statements. No nesting, no concatenation
    //TODO: parse write statement into table, column list, values_list, and read condition
    
    Debug("Processing Write Statement: %s", write_statement.c_str());
    std::string read_statement;
    std::function<void(int, query_result::QueryResult*)>  write_continuation;
    bool skip_query_interpretation = false;
    uint64_t point_target_group = 0;
    std::vector<std::string> *keys_written = new std::vector<std::string>();

    //Write must stay in scope until the TX is done (because the Transformation creates String Views on it that it needs). Discard upon finishing TX
    pendingWriteStatements.push_back(write_statement);

    try{
      sql_interpreter.TransformWriteStatement(pendingWriteStatements.back(), read_statement, write_continuation, wcb, point_target_group, skip_query_interpretation, blind_write, keys_written);
    }
    catch(...){
      Panic("bug in transformer: %s -> %s", write_statement.c_str(), read_statement.c_str());
    }

   Debug("Transformed Write into re-con read_statement: %s", read_statement.c_str());
   
   //Testing/Debug only
  //  Debug("Current read set: Before next write.");
  //  for(auto read: txn.read_set()){
  //     Debug("Read set already contains: %s", read.key().c_str());
  //   }

  // call write_continuation and then update policy accordingly
  auto write_cont_update_policy = [this, write_continuation, keys_written](int status, query_result::QueryResult *result){
    write_continuation(status, result);

    // update policy for current transaction
    for (const auto &key : *keys_written) {
      Debug("keys_written key %s", key.c_str());
      const Policy *policy;
      endorseClient->GetPolicyFromCache(key, policy);
      c2client->HandlePolicyUpdate(policy);
    }

    delete keys_written;
  };

    if(read_statement.empty()){ //Must be point operation (Insert/Delete)
      //Add to writes directly.  //Call write_continuation for Insert ; for Point Delete -- > OR: Call them inside Transform.
      //NOTE: must return a QueryResult... 
      Debug("No read statement, immediately writing");
      sql::QueryResultProtoWrapper *write_result = new sql::QueryResultProtoWrapper(""); //TODO: replace with real result.

      //TODO: Write a real result that we can cache => this will allow for read your own write semantics.
        //     //Cache point read results. This can help optimize common point Select + point Update patterns.
        // if(!result.empty()){ //only cache if we did find a row.
        //   //Only cache if we did a Select *, i.e. we have the full row, and thus it can be used by Update.
        //   if(size_t pos = pendingQuery->queryMsg.query_cmd().find("SELECT *"); pos != std::string::npos) point_read_cache[key] = result;
        // } 

      if (!IsParticipant(point_target_group)) {
        txn.add_involved_groups(point_target_group);
        bclient[point_target_group]->Begin(client_seq_num);
      }  

      write_cont_update_policy(REPLY_OK, write_result);
    }
    else{
      Debug("Issuing re-con Query");
      stats.Increment("total_recon_reads");
      Query(read_statement, std::move(write_cont_update_policy), wtcb, timeout, false, skip_query_interpretation); //cache_result = false
      //Note: don't to cache results of intermediary queries: otherwise we will not be able to read our own updated version //TODO: Eventually add a cache containing own writes (to support read your own writes)
      //TODO: add a field for "is_point" (for Inserts we already know!)
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
    query_timeout_callback qtcb, uint32_t timeout, bool cache_result, bool skip_query_interpretation) {

  stats.Increment("total_reads");
  
  UW_ASSERT(query.length() < ((uint64_t)1<<32)); //Protobuf cannot handle strings longer than 2^32 bytes --> cannot handle "arbitrarily" complex queries: If this is the case, we need to break down the query command.

  transport->Timer(0, [this, query, qcb, qtcb, timeout, cache_result, skip_query_interpretation]() mutable {
    // Latency_Start(&getLatency);

    query_seq_num++;

    if(PROFILING_LAT){
     struct timespec ts_start;
    clock_gettime(CLOCK_MONOTONIC, &ts_start);
    uint64_t query_start_ms = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;
    query_start_times[query_seq_num] = query_start_ms;
   }


    //TEST: Set TS only at the end.
    //txn.mutable_timestamp()->set_timestamp(timeServer.GetTime());

    txn.set_last_query_seq(query_seq_num);
    Debug("Query[%lu:%lu:%lu] (client:tx-seq:query-seq). TS: [%lu:%lu]: %s.", 
            client_id, client_seq_num, query_seq_num, txn.timestamp().timestamp(), txn.timestamp().id(), query.c_str());

 
    // Contact the appropriate shard to execute the query on.
    //TODO: Determine involved groups
    //Requires parsing the Query statement to extract tables touched? Might touch multiple shards...
    //Assume for now only touching one group. (single sharded system)
    PendingQuery *pendingQuery = new PendingQuery(this, query_seq_num, query, qcb, cache_result);
    pendingQueries[query_seq_num] = pendingQuery;
  
    //TODO: Check col conditions. --> Switch between QueryResultCallback and PointQueryResultCallback
    
    pendingQuery->is_point = skip_query_interpretation? false : sql_interpreter.InterpretQueryRange(query, pendingQuery->table_name, pendingQuery->p_col_values, relax_point_cond); 

    Debug("Query [%d] is of type: %s ", query_seq_num, pendingQuery->is_point? "POINT" : "RANGE");
    
    if(pendingQuery->is_point){
      Debug("Encoded key: %s", EncodeTableRow(pendingQuery->table_name, pendingQuery->p_col_values).c_str()); 
      std::string encoded_key = EncodeTableRow(pendingQuery->table_name, pendingQuery->p_col_values);
      auto itr = point_read_cache.find(encoded_key);
      if(itr != point_read_cache.end()){

        //   if(PROFILING_LAT){
        //     struct timespec ts_start;
        //     clock_gettime(CLOCK_MONOTONIC, &ts_start);
        //     uint64_t query_end_ms = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;
            
        //     //Should not take more than 1 ms (already generous) to parse and prepare.
        //     auto duration = query_end_ms - query_start_times[query_seq_num];    //TODO: Store query_start_ms in some map.Look it up via query seq num!.
        //     Warning("PointQuery[%d] Cache latency in ms [%d]. in us [%d]", query_seq_num, duration/1000, duration);
        // }

        // still forward cached point query result but no proofs or dependencies needed
        c2client->SendForwardPointQueryResultMessage(
          encoded_key, itr->second, Timestamp(), pendingQuery->table_name,
          proto::CommittedProof(), std::string(), std::string(),
          proto::Dependency(), false, false
        );

        Debug("Supply point query result from cache! (Query seq: %d)", query_seq_num);
        auto res = new sql::QueryResultProtoWrapper(itr->second);
        qcb(REPLY_OK, res);
        return;
      }
    } 
    //Alternatively: Instead of storing the key, we could also let servers provide the keys and wait for f+1 matching keys. But then we'd have to wait for 2f+1 reads in total... ==> Client stores key
    else{
      auto itr = scan_read_cache.find(query);
      if(itr != scan_read_cache.end()){
        Debug("Supply scan query result from cache! (Query seq: %d). Query: %s", query_seq_num, query.c_str());

        // still forward cached query result but no readset or proofs needed
        c2client->SendForwardQueryResultMessage(
          pendingQuery->query_gen_id, itr->second,
          proto::QueryResultMetaData(),
          std::map<uint64_t, std::vector<proto::SignedMessage>>(), false
        );

        auto res = new sql::QueryResultProtoWrapper(itr->second);
        qcb(REPLY_OK, res);
        return;
      }
    }

    //std::vector<uint64_t> involved_groups = {0};//{0UL, 1UL};
      // Contact the appropriate shard to get the value.
    std::vector<int> txnGroups(txn.involved_groups().begin(), txn.involved_groups().end());

    //Invoke partitioner function to figure out which group/shard we need to request from. 
    // invalid conversion from int to uint64_t?
    int target_group = (*part)(pendingQuery->table_name, query, nshards, -1, txnGroups, false) % ngroups;
    //if(target_group != 0) Panic("Trying to use a Shard other than 0");   //FIXME: Remove: Just for testing single-shard setup currently

    std::vector<uint64_t> involved_groups = {target_group};
    pendingQuery->SetInvolvedGroups(involved_groups);
    Debug("[group %i] designated as Query Execution Manager for query [%lu:%lu]", pendingQuery->queryMsg.query_manager(), client_seq_num, query_seq_num);
    pendingQuery->SetQueryId(this);

   
    // If needed, add this shard to set of participants and send BEGIN.
    for(auto &i: pendingQuery->involved_groups){
       if (!IsParticipant(i)) {
        txn.add_involved_groups(i);
        bclient[i]->Begin(client_seq_num);
      }
    }
    
    //Could send table_name always? Then we know how to lookup table_version (NOTE: Won't work for joins etc though..)

       
    result_callback rcb = nullptr;
    point_result_callback prcb = nullptr;
    
    //For Retry: Override callback to be this one.
    if(pendingQuery->is_point && !params.query_params.eagerPointExec){ //TODO: Create separate param for point eagerExec
      //In callback: If point and query fails (it was using eager exec) -> Retry should issue Point without eager exec.

      pendingQuery->key = EncodeTableRow(pendingQuery->table_name, pendingQuery->p_col_values); //TODO: Pass it down!!! Ptr to table_name and key.
      prcb = std::bind(&Client::PointQueryResultCallback, this, pendingQuery,
                     std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, 
                     std::placeholders::_4, std::placeholders::_5, std::placeholders::_6, std::placeholders::_7,
                     std::placeholders::_8, std::placeholders::_9, std::placeholders::_10, std::placeholders::_11,
                     std::placeholders::_12);
      stats.Increment("PointQueryAttempts", 1);
    }
    else{
      rcb = std::bind(&Client::QueryResultCallback, this, pendingQuery,
                     std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, 
                     std::placeholders::_4, std::placeholders::_5, std::placeholders::_6, std::placeholders::_7);
      stats.Increment("QueryAttempts", 1);
      if(warmup_done) stats.Increment("QueryAttempts_postwarmup", 1);
    }


    //result_callback rcb = qcb;
    //NOTE: result_hash = read_set hash. ==> currently not hashing queryId, version or result contents into it. Appears unecessary.
    //result_callback rcb = [qcb, pendingQuery, this](int status, int group, std::map<std::string, TimestampMessage> &read_set, std::string &result_hash, std::string &result, bool success) mutable { 
    
    result_timeout_callback rtcb = qtcb;

    // Send the Query operation to involved shards & select transaction manager (shard responsible for result reply) 
    for(auto &i: pendingQuery->involved_groups){
      Debug("[group %i] starting Query [%lu:%lu]", i, client_seq_num, query_seq_num);
      bclient[i]->Query(client_seq_num, query_seq_num, pendingQuery->queryMsg, timeout, rtcb, rcb, prcb, pendingQuery->is_point, &pendingQuery->table_name, &pendingQuery->key);
    }
    // Shard Client upcalls only if it is the leader for the query, and if it gets matching result hashes  ..........const std::string &resultHash
       //store QueryID + result hash in transaction.

    //queryBuffer[query_seq_num] = std::move(queryMsg);  //Buffering only after sending, so we can move contents for free.

  });
}


void Client::PointQueryResultCallback(PendingQuery *pendingQuery,  
                                  int status, const std::string &key, const std::string &result, const Timestamp &read_time, const std::string &table_name,
                                  const proto::Dependency &dep, bool hasDep, bool addReadSet,
                                  const proto::CommittedProof &proof, const std::string &serializedWrite, 
                                  const std::string &serializedWriteTypeName, const proto::EndorsementPolicyMessage &policyMsg) 
{ 
  
   if(PROFILING_LAT){
     struct timespec ts_start;
    clock_gettime(CLOCK_MONOTONIC, &ts_start);
    uint64_t query_end_ms = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;
    
    //Should not take more than 1 ms (already generous) to parse and prepare.
    auto duration = query_end_ms - query_start_times[pendingQuery->queryMsg.query_seq_num()];    //TODO: Store query_start_ms in some map.Look it up via query seq num!.
    // Warning("Query[%d] exec latency in ms [%d]. in us [%d]", pendingQuery->queryMsg.query_seq_num(), duration/1000, duration);
    // if(duration > 20000) Warning("PointQuery exec exceeded 20ms");
  }

  if (addReadSet) { 
    //Note: We must add to read set even if result = empty (i.e. there was no write). In that case, mutable_read time will be empty. (default = 0)
    Debug("Adding key %s read set with readtime [%lu:%lu]", key.c_str(), read_time.getTimestamp(), read_time.getID());
    ReadMessage *read = txn.add_read_set();
    read->set_key(key);
    read_time.serialize(read->mutable_readtime());

    // new policy can only come from server, which must correspond to addReadSet
    if (policyMsg.IsInitialized()) {
      Debug("PULL[%lu:%lu] POLICY FOR key %s in GET",client_id, client_seq_num, BytesToHex(key, 16).c_str());
      Policy *policy = policyParseClient->Parse(policyMsg.policy());
      endorseClient->UpdatePolicyCache(policyMsg.policy_id(), policy);
    }
  }
  if (hasDep) {
    *txn.add_deps() = dep;
  }

  // note that the pendingQuery->table_name has been moved out, so is no longer valid
  // instead we use the table_name passed in as an argument
  c2client->SendForwardPointQueryResultMessage(
    key, result, read_time, table_name, proof,
    serializedWrite, serializedWriteTypeName, dep, hasDep, addReadSet
  );
      
  Debug("Upcall with Point Query result");

  //Note: result = empty ==>default case: no replica reported valid result (== all honest replicas send empty)
  // ==> QueryResultWrapper constructor will create empty result.

  query_result::QueryResult *q_result = new sql::QueryResultProtoWrapper(result);
  
  Debug("Result size: %d. Result rows affected: %d", q_result->size(), q_result->rows_affected());

  if(TEST_READ_SET){
    Debug("Print result for query: %s", pendingQuery->queryMsg.query_cmd().c_str());
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
  

  //Cache point read results. This can help optimize common point Select + point Update patterns.
  if(!result.empty()){ //only cache if we did find a row.
    //Only cache if we did a Select *, i.e. we have the full row, and thus it can be used by Update.
    if(size_t pos = pendingQuery->queryMsg.query_cmd().find("SELECT *"); pos != std::string::npos) point_read_cache[key] = result;
  } 

  stats.Increment("PointQuerySuccess", 1);
  pendingQuery->qcb(REPLY_OK, q_result); //callback to application (or write cont)
  
  pendingQueries.erase(pendingQuery->queryMsg.query_seq_num());
  delete pendingQuery;  //For Point Queries can delete immediately;
  //clean pendingQuery and query_seq_num_mapping in all shards. ==> Not necessary here: Already happens in HandlePointQuery
  //ClearQuery(pendingQuery);     

  return;               

}

void Client::QueryResultCallback(PendingQuery *pendingQuery,  
                                  int status, int group, proto::ReadSet *query_read_set, std::string &result_hash, std::string &result, bool success,
                                  const std::vector<proto::SignedMessage> &query_sigs) 
{ 

  if(PROFILING_LAT){
     struct timespec ts_start;
    clock_gettime(CLOCK_MONOTONIC, &ts_start);
    uint64_t query_end_ms = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;
    
    //Should not take more than 1 ms (already generous) to parse and prepare.
    auto duration = query_end_ms - query_start_times[pendingQuery->queryMsg.query_seq_num()]; ;
    //Warning("Query[%d] exec latency in ms [%d]. in us [%d]", pendingQuery->queryMsg.query_seq_num(), duration/1000, duration);
  }
      //FIXME: If success: add readset/result hash to datastructure. If group==query manager, record result. If all shards received ==> upcall. 
      //If failure: re-set datastructure and try again. (any shard can report failure to sync)
      //Note: Ongoing shard clients PendingQuery implicitly maps to current retry_version
    
  //JUST FOR TESTING::
  // if(query_seq_num == 2){
  //   success = false;
  //   std::cerr << "TESTING RETRY FOR POINT" << std::endl;
  // } 

  UW_ASSERT(pendingQuery != nullptr);
  if(!success){ //Retry query
    delete query_read_set;
    Debug("[group %i] Reported failure for QuerySync [seq : ver] [%lu : %lu] \n", group, pendingQuery->queryMsg.query_seq_num(), pendingQuery->version);
    RetryQuery(pendingQuery);
    return;
  }      
  

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

  // add signatures to pendingQuery
  for(auto &sig : query_sigs){
    pendingQuery->group_sigs[group].push_back(sig);
  }

  //wait for all shard read-sets to arrive before reporting result. (Realistically result shard replies last, since it has to coordinate data transfer for computation)
  if(pendingQuery->involved_groups.size() != pendingQuery->group_replies) return;

  
  Debug("Received all required group replies for QuerySync[%lu:%lu] (seq:ver). UPCALLING \n", group, pendingQuery->queryMsg.query_seq_num(), pendingQuery->version);

  //just for testing
  if(TEST_READ_SET) TestReadSet(pendingQuery);

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
  else{ //Move Read Sets into Transaction
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

         for(auto &pred: *query_read_set->mutable_read_predicates()){
            if(!txn.read_predicates().empty() && pred.pred_instances_size() == 1){ //This is just a simple check that sees if there are 2 consecutive preds (that only have 1 instantiation) with the same pred_instance
                if(pred.pred_instances()[0] == txn.read_predicates()[txn.read_predicates_size()-1].pred_instances()[0]) continue;
            }
            proto::ReadPredicate *add_pred = txn.add_read_predicates();
            *add_pred = std::move(pred);
         }

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

  // forward to validating clients
  c2client->SendForwardQueryResultMessage(
    pendingQuery->query_gen_id, pendingQuery->result, *queryRep,
    pendingQuery->group_sigs, true
  );

  Debug("Upcall with Query result");
  sql::QueryResultProtoWrapper *q_result = new sql::QueryResultProtoWrapper(pendingQuery->result);

  stats.Increment("QuerySuccess", 1);
  //if it was a point query
  // if(pendingQuery->is_point && params.query_params.eagerPointExec) stats.Increment("PointQueryEager_successes", 1);
  // //If it was an eager exec.    //Note: Running eager exec whenever version == 0 AND either eagerExec param is set, or it is a pointQuery and eagerPointExec is set
  // else if(pendingQuery->version == 0 && params.query_params.eagerExec) stats.Increment("EagerExec_successes", 1);
  // else stats.Increment("Sync_successes", 1);

  stats.IncrementList("NumRetries", pendingQuery->version);

  Debug("Result size: %d. Result rows affected: %d", q_result->size(), q_result->rows_affected());

  if(TEST_READ_SET){
    int num_rows = std::min((int) q_result->size(), 10);
    Debug("Print result for query: %s", pendingQuery->queryMsg.query_cmd().c_str());
    Debug("Printing first %d rows.", num_rows);
    for(int i = 0; i < num_rows; ++i){
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


  if(!q_result->empty() && (pendingQuery->cache_result || FORCE_SCAN_CACHING)){ //only cache if we did find a row.
     Debug("Caching result for query: %s", pendingQuery->queryMsg.query_cmd().c_str());
        //Only cache if we did a Select *, i.e. we have the full row, and thus it can be used by Update.
      if(size_t pos = pendingQuery->queryMsg.query_cmd().find("SELECT *"); pos != std::string::npos){
            scan_read_cache[pendingQuery->queryMsg.query_cmd()] = pendingQuery->result;  
      }
  }

  //Optional Result Caching... Turn the result into individual point results. 
  //Cache result for future point updates. This can help optimize common point Select + point Update patterns.
  if(false && !q_result->empty() && !pendingQuery->table_name.empty() && (pendingQuery->cache_result || FORCE_SCAN_CACHING)){ //only cache if we did find a row.
      ColRegistry *col_registry;
      try{
        col_registry = &sql_interpreter.GetTableRegistry()->at(pendingQuery->table_name);
        Debug("Try to cache result for Table: %s", pendingQuery->table_name.c_str());
      }
      catch(...){
        Debug("Unsuitable Table string for caching: %s", pendingQuery->table_name.c_str());
        pendingQuery->qcb(REPLY_OK, q_result); //callback to application 
        return;
      }

        //Only cache if we did a Select *, i.e. we have the full row, and thus it can be used by Update.
      if(size_t pos = pendingQuery->queryMsg.query_cmd().find("SELECT *"); pos != std::string::npos){

          bool is_point_res = result.size() == 1; //TODO: In this case technically don't need to build new result.

          for(int i = 0; i < q_result->size(); ++i){
            
            std::vector<std::string> p_col_values;

            sql::QueryResultProtoBuilder queryResultBuilder;
            RowProto *new_row = queryResultBuilder.new_row();

            std::unique_ptr<query_result::Row> row = (*q_result)[i]; 
            for(int j=0; j<row->num_columns(); ++j){
              const std::string &col = row->name(j);
              std::unique_ptr<query_result::Field> field = (*row)[j];
              const std::string &field_val = field->get();

              queryResultBuilder.add_column(col);
              queryResultBuilder.AddToRow_s(new_row, field_val); 

              if(col_registry->primary_key_cols.count(col)){
                p_col_values.push_back(field_val);
              }
            }
            
            std::string key = EncodeTableRow(pendingQuery->table_name, p_col_values);
            Debug("CACHING KEY: %s", key.c_str());
            point_read_cache[key] = queryResultBuilder.get_result()->SerializeAsString(); 
            
          } 
      }
  }

  pendingQuery->qcb(REPLY_OK, q_result); //callback to application 
  //clean pendingQuery and query_seq_num_mapping in all shards.
  //ClearQuery(pendingQuery); ==> now clearing all Queries together only upon Writeback

  return;               
}

void Client::TestReadSet(PendingQuery *pendingQuery){
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

}

void Client::ClearTxnQueries(){
  for(auto &[_, pendingQuery]: pendingQueries){
    ClearQuery(pendingQuery);
  }
  pendingQueries.clear();
}

void Client::ClearQuery(PendingQuery *pendingQuery){
  for(auto &g: pendingQuery->involved_groups){
    bclient[g]->ClearQuery(pendingQuery->queryMsg.query_seq_num()); //-->Remove mapping + pendingRequest.
  }

  //pendingQueries.erase(pendingQuery->queryMsg.query_seq_num()); Don't delete early..
  delete pendingQuery;
}

void Client::RetryQuery(PendingQuery *pendingQuery){

  if(params.query_params.retryLimit > -1 && pendingQuery->version >= params.query_params.retryLimit){
    Warning("Exceeded Retry Limit for Query[%lu:%lu:%lu]. Limit: %d", client_id, pendingQuery->queryMsg.query_seq_num(), pendingQuery->version, params.query_params.retryLimit);
    stats.Increment("Exceeded_Retry_Limit");
    //throw std::runtime_error("Exceeded Retry Limit"); //Application will catch exception and turn it into a SystemAbort

    for (auto group : txn.involved_groups()) {
      bclient[group]->Abort(client_seq_num, txn.timestamp(), txn);
    }

    pendingQuery->qcb(REPLY_FAIL, nullptr);
  }

  stats.Increment("QueryRetries", 1);
  //if it was a point query
  if(pendingQuery->is_point && params.query_params.eagerPointExec) stats.Increment("PointQueryEager_failures", 1);
  //If it was an eager exec.    //Note: Running eager exec whenever version == 0 AND either eagerExec param is set, or it is a pointQuery and eagerPointExec is set
  else if(pendingQuery->version == 0 && params.query_params.eagerExec && !params.query_params.eagerPlusSnapshot) stats.Increment("EagerExec_failures", 1);
  else{ //!eager OR eager ran with snapshot and failed
     if(pendingQuery->version == 0) stats.Increment("First_Sync_fail");
     stats.Increment("Sync_failures", 1);
  } 
  
  pendingQuery->version++;
  pendingQuery->group_replies = 0;
  pendingQuery->queryMsg.set_retry_version(pendingQuery->version);
  pendingQuery->ClearReplySets();

  for(auto &g: pendingQuery->involved_groups){
    if(pendingQuery->is_point){
      stats.Increment("eager_point_fail", 1);
      pendingQuery->key = EncodeTableRow(pendingQuery->table_name, pendingQuery->p_col_values);
      bclient[g]->RetryQuery(pendingQuery->queryMsg.query_seq_num(), pendingQuery->queryMsg, true, std::bind(&Client::PointQueryResultCallback, this, pendingQuery,
                     std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, 
                     std::placeholders::_4, std::placeholders::_5, std::placeholders::_6, std::placeholders::_7,
                     std::placeholders::_8, std::placeholders::_9, std::placeholders::_10, std::placeholders::_11,
                     std::placeholders::_12));
    } 
    else{
      bclient[g]->RetryQuery(pendingQuery->queryMsg.query_seq_num(), pendingQuery->queryMsg); //--> Retry Query, shard clients already have the rcb.
    }
  }

  //If point query && retry --> Issue PointQueryCallback (pass as extra argument/alternate function)
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
void Client::AddWriteSetIdx(proto::Transaction &txn){
  if(!params.query_params.sql_mode) return; //only for sql_mode. NOT correct behavior for non-sql mode (in that mode there are no TableWrites)

  ::sintrstore::AddWriteSetIdx(txn);
}

void Client::Commit(commit_callback ccb, commit_timeout_callback ctcb,
    uint32_t timeout) {
  if (get_policy_done != 0) {
    transport->Timer(0, [this, ccb, ctcb, timeout]() {
      Debug("Retrying commit because policy get on put not finished");
      Commit(ccb, ctcb, timeout);
    });
    return;
  }
  transport->Timer(0, [this, ccb, ctcb, timeout]() {

    if(PROFILING_LAT){
       struct timespec ts_start;
      clock_gettime(CLOCK_MONOTONIC, &ts_start);
      exec_end_ms = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;
      commit_start_ms = exec_end_ms;
      
        //Should not take more than 1 ms (already generous) to parse and prepare.
        auto duration = exec_end_ms - exec_start_ms;
        // exec_time_ms.push_back(duration);
        //Warning("  Transaction total execution latency in us [%d]", duration);

        //Notice("Tx has %d deps", txn.deps().size());
    }
    
    uint64_t ns = Latency_End(&executeLatency);
    Latency_Start(&commitLatency);

    if(!txn.query_set().empty() && !params.query_params.cacheReadSet && params.query_params.mergeActiveAtClient){
      //If has queries, and query deps are meant to be reported by client:
      // Sort and erase all duplicate dependencies. (equality = same txn_id and same involved group.)
      std::sort(txn.mutable_deps()->begin(), txn.mutable_deps()->end(), sortDepSet);
      txn.mutable_deps()->erase(std::unique(txn.mutable_deps()->begin(), txn.mutable_deps()->end(), equalDep), txn.mutable_deps()->end());  //erases all but last appearance
      
    }

    //Add a TableVersion for each TableWrite -- CON: Don't want to do this for updates...
    for(auto &[table_name, table_write] : txn.table_writes()){
      //Only update TableVersion if we inserted/deleted a row
      if(table_write.has_changed_table() && table_write.changed_table()){  //TODO: Set changed_table for insert and delete.
          WriteMessage *table_ver = txn.add_write_set();
          //table_ver->set_key(table_name);
          table_ver->set_key(EncodeTable(table_name));
          table_ver->set_value("");
          table_ver->set_is_table_col_version(true);
          table_ver->mutable_rowupdates()->set_row_idx(-1); 
      }
    }

    if(TEST_READ_SET){
      Debug("Try Commit. PRINT WRITE SET"); //FIXME: REMOVE THIS. JUST FOR TESTING
      for(auto &write: txn.write_set()){
        Debug("key: %s. table_v? %d. deletion? %d", write.key().c_str(), write.is_table_col_version(), write.rowupdates().has_deletion() ? write.rowupdates().deletion() : 0);
      }
      Debug("Try Commit. PRINT READ SET"); //FIXME: REMOVE THIS. JUST FOR TESTING
      for(auto &read: txn.read_set()){
        Debug("key: %s. TS[%lu:%lu], is_table_v? %d", read.key().c_str(), read.readtime().timestamp(), read.readtime().id(), read.is_table_col_version());
      }
    }

    //TODO: Remove duplicate Writes and TableWrites 
    //sort over TableWrite vector (by primary key), and erase duplicates (same primary key) (try to keep the latter.)


    //XXX flag to sort read/write sets for parallel OCC
    //if(params.parallel_CCC || true){ //NOTE: FIXME: Currently always sorting: This way we can detect duplicate table versions early. 
                                        //=> Ignore: TableVersion in ReadSet now always 0 (purely for locking convenience, it has no meaning)
    if(params.parallel_CCC){
      //NOTE: Relying on client to provide sorted inputs is technically not byz robust. 
      //However, this is convenient for the prototype. A truly robust version should sort server side before locking.
      try {
        std::sort(txn.mutable_read_set()->begin(), txn.mutable_read_set()->end(), sortReadSetByKey);
        std::sort(txn.mutable_write_set()->begin(), txn.mutable_write_set()->end(), sortWriteSetByKey);
        if(params.query_params.sql_mode) AddWriteSetIdx(txn);
        //Note: Use stable_sort to guarantee order respects duplicates; Altnernatively: Can try to delete from write sets to save redundant size.

        //If write set can contain duplicates use the following: Reverse + sort --> "latest" put is first. Erase all but first entry. 
        //(Note: probably cheaper to erase all duplicates manually during insert? Or use map?)
        // std::reverse(txn.mutable_write_set()->begin(), txn.mutable_write_set()->end());
        // std::stable_sort(txn.mutable_write_set()->begin(), txn.mutable_write_set()->end(), sortWriteSetByKey);  //sorts while maintaining relative order
        // txn.mutable_write_set()->erase(std::unique(txn.mutable_write_set()->begin(), txn.mutable_write_set()->end(), equalWriteMsg), txn.mutable_write_set()->end());  //erases all but last appearance
      }
      catch(...) {
        Debug("Preemptive Abort: Trying to commit a transaction with 2 different reads for the same key");

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
    else if(params.query_params.sql_mode) {
      // must sort writeset always, because validation client writeset ordering is not guaranteed in query mode
      std::sort(txn.mutable_write_set()->begin(), txn.mutable_write_set()->end(), sortWriteSetByKey);
      AddWriteSetIdx(txn);
    }

    //TEST: Set TS only at the end.
    //txn.mutable_timestamp()->set_timestamp(timeServer.GetTime());

    // also sort involved groups for endorsement comparisons
    std::sort(txn.mutable_involved_groups()->begin(), txn.mutable_involved_groups()->end());

    // set expected endorsement digest
    std::string digest = TransactionDigest(txn, params.hashDigest);
    if (params.sintr_params.debugEndorseCheck) {
      endorseClient->DebugSetExpectedTxnOutput(txn);
    }
    endorseClient->SetExpectedTxnOutput(digest);

    PendingRequest *req = new PendingRequest(client_seq_num, this);
    pendingReqs[client_seq_num] = req;
    req->txn = txn; //Is this a copy or just reference?
    req->ccb = ccb;
    req->ctcb = ctcb;
    req->callbackInvoked = false;
    req->txnDigest = digest;
   
    req->timeout = timeout; //20000UL; //timeout;
    stats.IncrementList("txn_groups", txn.involved_groups().size());

    Debug("TRY COMMIT[%s]", BytesToHex(req->txnDigest, 16).c_str());
   
    //Notice("Try Commit. Txn[%s][%lu:%lu].", BytesToHex(req->txnDigest, 16).c_str(), txn.timestamp().timestamp(), txn.timestamp().id()); //FIXME: REMOVE THIS. JUST FOR TESTING
    if(false){
      Notice("Try Commit. Txn[%s][%lu:%lu]. PRINT WRITE SET", BytesToHex(req->txnDigest, 16).c_str(), txn.timestamp().timestamp(), txn.timestamp().id()); //FIXME: REMOVE THIS. JUST FOR TESTING
      for(auto &write: txn.write_set()){
        Notice("key: %s. table_v? %d. deletion? %d", write.key().c_str(), write.is_table_col_version(), write.rowupdates().has_deletion() ? write.rowupdates().deletion() : 0);
      }

      Notice("Try Commit. PRINT READ SET"); //FIXME: REMOVE THIS. JUST FOR TESTING
      for(auto &read: txn.read_set()){
        Notice("key: %s. TS[%lu:%lu], is_table_v? %d", read.key().c_str(), read.readtime().timestamp(), read.readtime().id(), read.is_table_col_version());
      }
    }

    Phase1(req);
  });
}

void Client::Phase1(PendingRequest *req) {
  // if endorsement is not satisfied yet, add back to event loop
  if (!endorseClient->IsSatisfied()) {
    transport->Timer(0, [this, req]() {
      Phase1(req);
    });
    return;
  }
  // update txn digest with endorsements
  Debug("OLD TXN DIGEST CLIENT: %s", BytesToHex(req->txnDigest, 16).c_str());
  const std::vector<proto::SignedMessage> &endorsements = endorseClient->GetEndorsements();
  if(PROFILING_LAT){
    struct timespec ts_start;
    clock_gettime(CLOCK_MONOTONIC, &ts_start);
    endorsements_received_ms = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;
    auto duration = endorsements_received_ms - commit_start_ms;
    // endorsement_wait_ms.push_back(duration);
    // Warning("  Transaction waiting for endorsements latency in us [%d]", duration);
  }
  
  proto::SignedMessages protoEndorsements;
  for (auto &endorsement : endorsements) {
    *protoEndorsements.add_sig_msgs() = endorsement;
  }

  // TODO: implement compartor for endorsements to use for sorting
  /*
  if(params.parallel_CCC) {
    std::sort(protoEndorsements.mutable_sig_msgs()->begin(), protoEndorsements.mutable_sig_msgs()->end());
  }
  */

  // add endorsement to txn
  *(req->txn.mutable_endorsements()) = protoEndorsements;
  // this does not modify transaction itself, so need to modify below
  *txn.mutable_endorsements() = protoEndorsements;
  if(params.sintr_params.hashEndorsements) {
    req->txnDigest = EndorsedTxnDigest(req->txnDigest, txn, params.hashDigest);
  }

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
        req->timeout,
        protoEndorsements);
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
      Debug("Have %d ABSTAIN replies from group %d. Saving in eqvAbortSigsGrouped", itr_abort->second.sigs_size(), group);
      (*req->eqvAbortSigsGrouped.mutable_grouped_sigs())[group] = itr_abort->second;
      req->slowAbortGroup = group;
      // saving commit sigs in req->p1ReplySigsGrouped
      auto itr_commit = sigs.find(proto::ConcurrencyControl::COMMIT);
      UW_ASSERT(itr_commit != sigs.end());
      Debug("Have %d COMMIT replies from group %d.", itr_commit->second.sigs_size(), group);
      (*req->p1ReplySigsGrouped.mutable_grouped_sigs())[group] = itr_commit->second;

    }
    //non-equivocation processing
    else if(decision == proto::ABORT && fast && !conflict_flag){
      auto itr = sigs.find(proto::ConcurrencyControl::ABSTAIN);
      UW_ASSERT(itr != sigs.end());
      Debug("Have %d ABSTAIN replies from group %d.", itr->second.sigs_size(),  group);
      (*req->p1ReplySigsGrouped.mutable_grouped_sigs())[group] = itr->second;
      req->fastAbortGroup = group;
    } else if (decision == proto::ABORT && !fast) {
      auto itr = sigs.find(proto::ConcurrencyControl::ABSTAIN);
      UW_ASSERT(itr != sigs.end());
      Debug("Have %d ABSTAIN replies from group %d.", itr->second.sigs_size(), group);
      (*req->p1ReplySigsGrouped.mutable_grouped_sigs())[group] = itr->second;
      req->slowAbortGroup = group;
    } else if (decision == proto::COMMIT) {
      auto itr = sigs.find(proto::ConcurrencyControl::COMMIT);
      if(itr == sigs.end()){
        for(auto [res, b]: sigs){
          Notice("Res[%d] has %d sigs", res, b.sigs_size());
        }
        if(fb){
          std::cerr << "abort on fallback path for txn: " << BytesToHex(req->txnDigest, 16) << std::endl;
        }
        if(!fb){
          std::cerr << "abort on normal path for txn: " << BytesToHex(req->txnDigest, 16) << std::endl;
        }
      }
      UW_ASSERT(itr != sigs.end());
      Debug("Have %d COMMIT replies from group %d.", itr->second.sigs_size(), group);
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
  //Panic("P2 timing out for txnId: %lu; honest client: %s. This should not happen for honest clients -- most likely servers crashed", txnId, failureActive ? "False" : "True");
  //Note: This should never happen - it suggests that servers crashed during P2 handling.

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

  if(PROFILING_LAT){
    struct timespec ts_start;
    clock_gettime(CLOCK_MONOTONIC, &ts_start);
    commit_end_ms = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;
    
    //Should not take more than 1 ms (already generous) to parse and prepare.
    // auto duration = commit_end_ms - commit_start_ms;
    //Warning("     Transaction commit latency in us [%d]", duration);

    auto duration = commit_end_ms - endorsements_received_ms;
    // phase1_time_ms.push_back(duration);
  }

  //total_writebacks++;
  Debug("WRITEBACK[%s][%lu:%lu] result %s", BytesToHex(TransactionDigest(req->txn, params.hashDigest), 16).c_str(), client_id, req->id, req->decision ?  "ABORT" : "COMMIT");
  //Notice("WRITEBACK[%s][%lu:%lu] result %s", BytesToHex(TransactionDigest(req->txn, params.hashDigest), 16).c_str(), client_id, req->id, req->decision ?  "ABORT" : "COMMIT");
  req->startedWriteback = true;

  if (failureActive && params.injectFailure.type == InjectFailureType::CLIENT_STALL_AFTER_P1) {
    // TODO: modify debug statements to use transaction digest with endorsements
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

  ClearTxnQueries();
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

    ClearTxnQueries();
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
  
  ClearTxnQueries();
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

bool Client::isDep(const std::string &txnDigest, proto::Transaction &Req_txn, const proto::Transaction* txn){
  //TODO:FIXME: This function is not rigorously protecting against Byz replica ddos. It has some best effort defenses right now.
    //Caching ReadSet/Predicates makes it difficult to assert whether or not TXs actually conflict or cause dependencies

  //Check if received relayTx is in the dep set of current Tx.
  for(auto & dep: Req_txn.deps()){
   if(dep.write().prepared_txn_digest() == txnDigest){ return true;}
  }

  if(!params.query_params.sql_mode) return false; //Deps must be part of dep set.

  //For Txns with Queries:

  //TODO: if we are not caching, but we didn't merge at client. => Check Query deps
  if(!params.query_params.cacheReadSet){
    if(params.query_params.mergeActiveAtClient) return false; //Deps must have been part of dep set.

    for(auto &query_md: Req_txn.query_set()){
      for(auto &[group, group_md]: query_md.group_meta()){
        UW_ASSERT(group_md.has_query_read_set()); //since we are not caching
        for(auto &dep: group_md.query_read_set().deps()){
           if(dep.write().prepared_txn_digest() == txnDigest){ return true;}
        }
      }
    }
  }

        //TODO: if TXN not yet preparing => cannot be a relevant dep.
  //If we are caching: Check whether dependency is part of snapshot; If eager, make an exception and accept dep
  if(params.query_params.cacheReadSet){
 
    //If this is our current Txn: Check for all of it's queries whether or not it could be part of its deps
    if(Req_txn.client_seq_num() == txn->client_seq_num() && Req_txn.client_id() == txn->client_id()){
      for(auto &[query_seq_num, pendingQuery]: pendingQueries){ 
        for(auto &group: pendingQuery->involved_groups){  
          if(bclient[group]->isValidQueryDep(query_seq_num, txnDigest, txn)) return true; 
        }
      }
    }
    else{ //Relay Tx is a "deeper dep", i.e. a dep of a fallback TX => then we are currently unable to check the cached deps.
          //The Tx in question might be a non-local Tx (for which we have no read set/snapshot); or an old Tx of this client (shouldn't happen for honest) for which QueryMD has been deleted already
      Warning("Cannot check cached deps on deeper deps");  //TODO: Would need to get access to deps cached set
      return true;
    }
  }


  if(params.query_params.sql_mode && params.query_params.useSemanticCC) return true; 
  //If dep could be dynamic dep, simply accept for now. //TODO: Be smarter about this. 
  // Check if Tx relevant to predicate of some query. (To do this, must have predicate local, not cached)
  
  Panic("Don't expect to receive non-relevant Relay's in simulation");
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
  if(!isDep(txnDigest, itr->second->txn, &relayP1.p1().txn())){
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
    if(FB_instances.find(p1_pair.first) != FB_instances.end()) continue;
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
  if(!isDep(txnDigest, itrFB->second->txn, &relayP1.p1().txn())){
    Debug("Tx[%s] is not a dependency of ReqId: %d", BytesToHex(txnDigest, 128).c_str(), reqId);
    return;
  }

  proto::Phase1 *p1 = relayP1.release_p1();

  Debug("Starting Phase1FB for deeper depth right away");

  //itr->second->req_FB_instances.insert(txnDigest); //XXX mark connection between reqId and FB instance
  Phase1FB_deeper(reqId, txnDigest, dependent_txnDigest, p1);
}


void Client::Phase1FB(const std::string &txnDigest, uint64_t conflict_id, proto::Phase1 *p1){  //passes callbacks
  if(FB_instances.find(txnDigest) != FB_instances.end()) return;

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
   if(FB_instances.find(txnDigest) != FB_instances.end()) return;

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
      Panic("txnDigs don't match");
      return false;
    } 
  }
  else if(msg.has_txn()){
    std::string temp_digest = TransactionDigest(msg.txn(), params.hashDigest);
    if(params.sintr_params.hashEndorsements) {
      temp_digest = EndorsedTxnDigest(temp_digest, msg.txn(), params.hashDigest);
    }
    if(*txnDigest != temp_digest){
      Panic("txnDig doesnt match Transaction");
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
              Panic("WRITEBACK[%s] Failed to validate P1 replies for fast decision %s.", BytesToHex(*txnDigest, 16).c_str(), (msg.decision() == proto::CommitDecision::COMMIT) ? "commit" : "abort");
              return false;
        }   
    }
    else if (params.signedMessages && msg.has_p2_sigs()) {
        if(!msg.has_p2_view()) return false;
        proto::CommitDecision myDecision;
  
        if (!ValidateP2Replies(msg.decision(), msg.p2_view(), txn, txnDigest, msg.p2_sigs(), keyManager, config, -1, myDecision, verifier)) {
                Panic("WRITEBACK[%s] Failed to validate P2 replies for decision %s.", BytesToHex(*txnDigest, 16).c_str(), (msg.decision() == proto::CommitDecision::COMMIT) ? "commit" : "abort");
                return false;
        }
    } 
    else if (msg.decision() == proto::ABORT && msg.has_conflict()) {
      std::string committedTxnDigest = TransactionDigest(msg.conflict().txn(), params.hashDigest);
      if(params.sintr_params.hashEndorsements) {
        committedTxnDigest = EndorsedTxnDigest(committedTxnDigest, msg.conflict().txn(), params.hashDigest);
      }
      if (!ValidateCommittedConflict(msg.conflict(), &committedTxnDigest, txn, txnDigest, params.signedMessages, keyManager, config, verifier, 
          params.sintr_params.policyFunctionName)) {
            Panic("WRITEBACK[%s] Failed to validate committed conflict for fast abort.", BytesToHex(*txnDigest, 16).c_str());
            return false;
      }
    } 
    else if (params.signedMessages) {
      Panic("WRITEBACK[%s] decision %d without proof, has_p1_sigs %d, has_p2_sigs %d, and has_conflict %d.", BytesToHex(*txnDigest, 16).c_str(), msg.decision(), msg.has_p1_sigs(), msg.has_p2_sigs(), msg.has_conflict());
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
    Debug("Already started Phase2FB/WritebackFB for tx [%s[]. Ignoring Phase1 callback response from group %d.", BytesToHex(txnDigest, 128).c_str(), group);
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

  Debug("PHASE2FB[%lu:%s] callback", client_id, BytesToHex(txnDigest, 16).c_str());

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




} // namespace sintrstore
