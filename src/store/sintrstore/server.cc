// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/sintrstore/server.cc:
 *   Implementation of a single transactional key-value server.
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

#include "store/sintrstore/server.h"

#include <bitset>
#include <chrono>
#include <ctime>
#include <queue>
#include <sys/time.h>

#include "lib/assert.h"
#include "lib/batched_sigs.h"
#include "lib/tcptransport.h"
#include "store/sintrstore/basicverifier.h"
#include "store/sintrstore/common.h"
#include "store/sintrstore/localbatchsigner.h"
#include "store/sintrstore/localbatchverifier.h"
#include "store/sintrstore/phase1validator.h"
#include "store/sintrstore/sharedbatchsigner.h"
#include "store/sintrstore/sharedbatchverifier.h"
#include "store/sintrstore/policy/policy_parse_client.h"
#include <fmt/core.h>
#include <valgrind/memcheck.h>

namespace sintrstore {

Server::Server(const transport::Configuration &config, int groupIdx, int idx,
               int numShards, int numGroups, Transport *transport,
               KeyManager *keyManager, Parameters params,
               std::string &table_registry_path, uint64_t timeDelta,
               OCCType occType, Partitioner *part,
               unsigned int batchTimeoutMicro, bool sql_bench, 
               bool simulate_point_kv, bool simulate_replica_failure, bool simulate_inconsistency, bool disable_prepare_visibility,
               TrueTime timeServer)
    : PingServer(transport), config(config), groupIdx(groupIdx), idx(idx),
      numShards(numShards), numGroups(numGroups), id(groupIdx * config.n + idx),
      transport(transport), occType(occType), part(part), params(params),
      keyManager(keyManager), timeDelta(timeDelta), timeServer(timeServer),
      sql_bench(sql_bench), 
      simulate_point_kv(simulate_point_kv), simulate_replica_failure(simulate_replica_failure), simulate_inconsistency(simulate_inconsistency), disable_prepare_visibility(disable_prepare_visibility) {

  ongoing = ongoingMap(100000);
  p1MetaData = p1MetaDataMap(100000);
  p2MetaDatas = p2MetaDataMap(100000);
  prepared = preparedMap(100000);
  preparedReads = tbb::concurrent_unordered_map<std::string, std::pair<std::shared_mutex, std::set<const proto::Transaction *>>>(100000);
  preparedWrites = tbb::concurrent_unordered_map<std::string, std::pair<std::shared_mutex,std::map<Timestamp, const proto::Transaction *>>>(100000);
  rts = tbb::concurrent_unordered_map<std::string, std::atomic_uint64_t>(100000);
  committed = tbb::concurrent_unordered_map<std::string, proto::CommittedProof *>(100000);
  writebackMessages = tbb::concurrent_unordered_map<std::string, proto::Writeback>(100000);
  dependents = dependentsMap(100000);
  waitingDependencies = std::unordered_map<std::string, WaitingDependency>(100000);

  store.KVStore_Reserve(4200000);
  // Create simulated MACs that are used for Fallback all to all:
  CreateSessionKeys();
  //////

  stats.Increment("total_equiv_received_adopt", 0);

  Notice("Starting Sintr replica. ID: %d, IDX: %d, GROUP: %d\n", id, idx, groupIdx);
  Notice("Sign Client Proposals? %s\n", params.signClientProposals ? "True" : "False");
  Debug("Starting Indicus replica %d.", id);
  transport->Register(this, config, groupIdx, idx);
  _Latency_Init(&committedReadInsertLat, "committed_read_insert_lat");
  _Latency_Init(&verifyLat, "verify_lat");
  _Latency_Init(&signLat, "sign_lat");
  _Latency_Init(&waitingOnLocks, "lock_lat");
  //_Latency_Init(&waitOnProtoLock, "proto_lock_lat");
  //_Latency_Init(&store.storeLockLat, "store_lock_lat");

  //define signer and verifier for replica signatures
  if (params.signatureBatchSize == 1) {
    //verifier = new BasicVerifier(transport);
    verifier = new BasicVerifier(transport, batchTimeoutMicro, params.validateProofs &&
      params.signedMessages && params.adjustBatchSize, params.verificationBatchSize);
    batchSigner = nullptr;
  } else {
    if (params.sharedMemBatches) {
      batchSigner = new SharedBatchSigner(transport, keyManager, GetStats(),
          batchTimeoutMicro, params.signatureBatchSize, id,
          params.validateProofs && params.signedMessages &&
          params.signatureBatchSize > 1 && params.adjustBatchSize,
          params.merkleBranchFactor);
    } else {
      batchSigner = new LocalBatchSigner(transport, keyManager, GetStats(),
          batchTimeoutMicro, params.signatureBatchSize, id,
          params.validateProofs && params.signedMessages &&
          params.signatureBatchSize > 1 && params.adjustBatchSize,
          params.merkleBranchFactor);
    }

    if (params.sharedMemVerify) {
      verifier = new SharedBatchVerifier(params.merkleBranchFactor, stats); //add transport if using multithreading
    } else {
      //verifier = new LocalBatchVerifier(params.merkleBranchFactor, stats, transport);
      verifier = new LocalBatchVerifier(params.merkleBranchFactor, stats, transport,
        batchTimeoutMicro, params.validateProofs && params.signedMessages &&
        params.signatureBatchSize > 1 && params.adjustBatchSize, params.verificationBatchSize);
    }
  }
  //define verifier that handles client signatures -- this can always be a basic verifier.
  client_verifier = new BasicVerifier(transport, batchTimeoutMicro, params.validateProofs &&
      params.signedMessages && params.adjustBatchSize, params.verificationBatchSize);

  // this is needed purely from loading data without executing transactions
  proto::CommittedProof *proof = new proto::CommittedProof();
  proof->mutable_txn()->set_client_id(0);
  proof->mutable_txn()->set_client_seq_num(0);
  proof->mutable_txn()->mutable_timestamp()->set_timestamp(0);
  proof->mutable_txn()->mutable_timestamp()->set_id(0);

  committed.insert(std::make_pair("", proof));   //This is not the genesis digest -- this is just a dummy reference to use for loading.
  // ts_to_tx.insert(std::make_pair(MergeTimestampId(0, 0), ""));
  // materializedMap::accessor mat;
  // materialized.insert(mat, "");
  // mat.release();

  //Add real genesis digest   --  Might be needed when we add TableVersions to snapshot and need to sync on them
  std::string genesis_txn_dig = TransactionDigest(proof->txn(), params.hashDigest);
  Notice("Create Genesis Txn with digest: %s", BytesToHex(genesis_txn_dig, 16).c_str());
  *proof->mutable_txn()->mutable_txndigest() = genesis_txn_dig;
  committed.insert(std::make_pair(genesis_txn_dig, proof));
  ts_to_tx.insert(std::make_pair(MergeTimestampId(0, 0), genesis_txn_dig));
  materializedMap::accessor mat;
  materialized.insert(mat, genesis_txn_dig);
  mat.release();

  //Compute write_monotonicity_grace  //DEPRECATED 
  write_monotonicity_grace = timeServer.MStoTS(params.query_params.monotonicityGrace);
  Notice("write_monotonicity_grace: %d", write_monotonicity_grace);
  //std::cerr << "Reverse: " << timeServer.TStoMS(write_monotonicity_grace) << std::endl;
  UW_ASSERT(timeServer.TStoMS(write_monotonicity_grace) == params.query_params.monotonicityGrace);

  // load policy store
  Notice("Loading Policy Store from config file: %s", params.sintr_params.policyConfigPath.c_str());
  LoadPolicyStore(params.sintr_params.policyConfigPath);

  policyIdFunction = GetPolicyIdFunction(params.sintr_params.policyFunctionName);

  if (sql_bench) {

    // TODO: turn read_prepared into a function, not a lambda
    auto read_prepared_pred = [this](const std::string &txn_digest) {
      if (this->occType != MVTSO || this->params.maxDepDepth == -2) return false; // Only read prepared if parameters allow
      // Check for Depth. Only read prepared if dep depth < maxDepth
      return (this->params.maxDepDepth == -1 || DependencyDepth(txn_digest) <= this->params.maxDepDepth);
    };

    if (TEST_QUERY) {
      table_store = new ToyTableStore(); // Just a black hole
    } else {
      int num_threads = std::thread::hardware_concurrency();
      if(num_threads > 8) Warning("more than 8 threads"); //num_threads = 8;
      //num_threads = num_threads - 2; //technically only need 6 since we use 1 thread for networking and 1 thread as mainThread
      table_store = new PelotonTableStore(&params.query_params, table_registry_path,
          std::bind(&Server::FindTableVersion, this, 
                        std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4, 
                        std::placeholders::_5, std::placeholders::_6, std::placeholders::_7, std::placeholders::_8),
          std::move(read_prepared_pred), num_threads);
      // table_store = new PelotonTableStore();
    }

    // table_store->RegisterTableSchema(table_registry_path);

    table_store->sql_interpreter.RegisterPartitioner(part, numShards, numGroups, groupIdx);

    // TODO: Create lambda/function for setting Table Version
    // Look at store and preparedWrites ==> pick larger (if read_prepared true)
    // Add it to QueryReadSetMgr

    // table_store->SetPreparePredicate(std::move(read_prepared_pred));
    // table_store->SetFindTableVersion(std::bind(&Server::FindTableVersion,
    // this, std::placeholders::_1, std::placeholders::_2,
    // std::placeholders::_3, std::placeholders::_4, std::placeholders::_5, std::placeholders::_6));
  }

  if (TEST_QUERY) {
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
    committed["toy_txn"] = real_proof;
  }


  // std::string short_s("dummy");
  // std::string short_s2("dummy");
  // std::string long_s("extrasuperlongdummy");
  // std::string long_s2("extrasuperlongdummy");
  // std::cerr << "short cap: " << short_s.capacity() << std::endl;
  //  std::cerr << "long cap: " << long_s.capacity() << std::endl;
  //  short_s += "1";
  //  short_s2 += "1";
  // //  long_s += "1";
  // //  long_s2 += "1";
  //  std::cerr << "short cap: " << short_s.capacity() << std::endl;
  //   std::cerr << "long cap: " << long_s.capacity() << std::endl;

  // // std::string test_dig = "digest1";
  // uint64_t data[4];
  // const uint8_t *arr = (const uint8_t *)short_s.c_str();
  // //bit_per_datum = sizeof(uint64_t) * 8.
  // //_len = 256/64 = 4
  // //N=256
  //  arr += 256 / 8;
  // for (uint64_t *ptr = data + 4; ptr > data;)
  // {
  //     uint64_t x = 0;
  //     for (unsigned j = 0; j < sizeof(uint64_t); j++)
  //         x = (x << 8) | *(--arr);
  //     *(--ptr) = x;
  // }
  // for(int i =0; i<4; ++i){
  //    std::cerr << "data : " << data[i] << std::endl;
  // }

  //  uint64_t data2[4];
  // const uint8_t *arr2 = (const uint8_t *)short_s2.c_str();
  // //bit_per_datum = sizeof(uint64_t) * 8.
  // //_len = 256/64 = 4
  // //N=256
  //  arr2 += 256 / 8;
  // for (uint64_t *ptr = data2 + 4; ptr > data2;)
  // {
  //     uint64_t x = 0;
  //     for (unsigned j = 0; j < sizeof(uint64_t); j++)
  //         x = (x << 8) | *(--arr2);
  //     *(--ptr) = x;
  // }
  // for(int i =0; i<4; ++i){
  //    std::cerr << "data2 : " << data2[i] << std::endl;
  // }

  //  uint64_t data3[4];
  // const uint8_t *arr3 = (const uint8_t *)long_s.c_str();
  // //bit_per_datum = sizeof(uint64_t) * 8.
  // //_len = 256/64 = 4
  // //N=256
  //  arr3 += 256 / 8;
  // for (uint64_t *ptr = data3 + 4; ptr > data3;)
  // {
  //     uint64_t x = 0;
  //     for (unsigned j = 0; j < sizeof(uint64_t); j++)
  //         x = (x << 8) | *(--arr3);
  //     *(--ptr) = x;
  // }
  // for(int i =0; i<4; ++i){
  //    std::cerr << "data3 : " << data3[i] << std::endl;
  // }

  // uint64_t data4[4];
  // const uint8_t *arr4 = (const uint8_t *)long_s2.c_str();
  // //bit_per_datum = sizeof(uint64_t) * 8.
  // //_len = 256/64 = 4
  // //N=256
  //  arr4 += 256 / 8;
  // for (uint64_t *ptr = data4 + 4; ptr > data4;)
  // {
  //     uint64_t x = 0;
  //     for (unsigned j = 0; j < sizeof(uint64_t); j++)
  //         x = (x << 8) | *(--arr4);
  //     *(--ptr) = x;
  // }
  // for(int i =0; i<4; ++i){
  //    std::cerr << "data4 : " << data4[i] << std::endl;
  // }

  // Panic("test");



  // struct timeval now;
  // gettimeofday(&now, NULL);
  // uint64_t miliseconds_start2 = now.tv_sec * 1000 * 1000 + now.tv_usec;

  // int x = 0;
  // for(int i = 0; i < 10000; ++i){
  //   x += std::rand(); //rand val so compiler cannot optimize away
  // }
  // //=> this takes roughly 100us

  // gettimeofday(&now, NULL);
  // uint64_t miliseconds_end2 = now.tv_sec * 1000 * 1000 + now.tv_usec;
 
  // //Should not take more than 1 ms (already generous) to parse and prepare.
  // auto duration2 = miliseconds_end2 - miliseconds_start2;
  // if(duration2 > 50){
  //   Warning("TEST EXCECUTE exceeded 50us: %d", duration2);
  // }
  //  std::cerr << "duration: " << duration2 << std::endl;
  // Panic("stop time test");
  std::cerr.sync_with_stdio(true);
}


Server::~Server() {
  std::cerr << "KVStore size: " << store.KVStore_size() << std::endl;
  std::cerr << "ReadStore size: " << store.ReadStore_size() << std::endl;
  std::cerr << "commitGet count: " << commitGet_count << std::endl;

  std::cerr << "Hash count: " << BatchedSigs::hashCount << std::endl;
  std::cerr << "Hash cat count: " << BatchedSigs::hashCatCount << std::endl;
  std::cerr << "Total count: " << BatchedSigs::hashCount + BatchedSigs::hashCatCount << std::endl;

  std::cerr << "Store wait latency (ms): " << store.lock_time << std::endl;
  std::cerr << "parallel OCC lock wait latency (ms): " << total_lock_time_ms << std::endl;
  Latency_Dump(&waitingOnLocks);
  //Latency_Dump(&waitOnProtoLock);
  //Latency_Dump(&batchSigner->waitOnBatchLock);
  //Latency_Dump(&(store.storeLockLat));

  // free policies
  for (const auto &p : policiesToFree) {
    delete p;
  }

  if(sql_bench){
    Notice("Freeing Table Store interface");
    delete table_store;
  }

  Notice("Freeing verifier.");
  delete verifier;
   //if(params.mainThreadDispatching) committedMutex.lock();
  for (const auto &c : committed) {   ///XXX technically not threadsafe
    delete c.second;
  }
   //if(params.mainThreadDispatching) committedMutex.unlock();
   ////if(params.mainThreadDispatching) ongoingMutex.lock();
  for (const auto &o : ongoing) {     ///XXX technically not threadsafe
    //delete o.second;
    delete o.second.txn;
  }
   ////if(params.mainThreadDispatching) ongoingMutex.unlock();
   if(params.mainThreadDispatching) readReplyProtoMutex.lock();
  for (auto r : readReplies) {
    delete r;
  }
   if(params.mainThreadDispatching) readReplyProtoMutex.unlock();
   if(params.mainThreadDispatching) p1ReplyProtoMutex.lock();
  for (auto r : p1Replies) {
    delete r;
  }
   if(params.mainThreadDispatching) p1ReplyProtoMutex.unlock();
   if(params.mainThreadDispatching) p2ReplyProtoMutex.lock();
  for (auto r : p2Replies) {
    delete r;
  }
   if(params.mainThreadDispatching) p2ReplyProtoMutex.unlock();
  Notice("Freeing signer.");
  if (batchSigner != nullptr) {
    delete batchSigner;
  }
  Latency_Dump(&verifyLat);
  Latency_Dump(&signLat);
}

void PrintSendCount() {
  send_count++;
  fprintf(stderr, "send count: %d \n", send_count);
}

void PrintRcvCount() {
  rcv_count++;
  fprintf(stderr, "rcv count: %d\n", rcv_count);
}

void ParseProto(::google::protobuf::Message *msg, std::string &data) {
  msg->ParseFromString(data);
}
// use like this: main dispatches lambda
// auto f = [this, msg, type, data](){
//   ParseProto;
//   auto g = [this, msg, type](){
//     this->HandleType(msg, type);  //HandleType checks which handler to use...
//   };
//   this->transport->DispatchTP_main(g);
//   return (void*) true;
// };
// DispatchTP_noCB(f);
// Main dispatches serialization, and lets serialization thread dispatch to
// main2

// Upcall from Network layer with new message. Called by TCPReadableCallback(..)
void Server::ReceiveMessage(const TransportAddress &remote, const std::string &type, const std::string &data, void *meta_data) {

  //Simulate Failure: Just ignore all messages.  -- Don't crash or else TCP will drop causing panic at client?
  if(simulate_replica_failure) return; //Ignore all messages

  if (params.dispatchMessageReceive) {
    Debug("Dispatching message handling to Support Main Thread");
    // using this path results in an extra copy
    // Can I move the data or release the message to avoid duplicates?
    transport->DispatchTP_main([this, &remote, type, data, meta_data]() {
      this->ReceiveMessageInternal(remote, type, data, meta_data);
      return (void *)true;
    });
  } else {
    ReceiveMessageInternal(remote, type, data, meta_data);
  }
}

// Calls function handlers for respective message types.
//  ManageDsipatch_ Manages respective Multithread assignments
void Server::ReceiveMessageInternal(const TransportAddress &remote, const std::string &type, const std::string &data, void *meta_data) {

  if (type == read.GetTypeName()) {
    ManageDispatchRead(remote, data);

    // Normal Protocol Messages
  } else if (type == phase1.GetTypeName()) {
    ManageDispatchPhase1(remote, data);

  } else if (type == phase2.GetTypeName()) {
    ManageDispatchPhase2(remote, data);

  } else if (type == writeback.GetTypeName()) {
    ManageDispatchWriteback(remote, data);

  } else if (type == abort.GetTypeName()) {
    ManageDispatchAbort(remote, data);

  } else if (type == ping.GetTypeName()) {
    ping.ParseFromString(data);
    HandlePingMessage(this, remote, ping); 

    // PingMessage p;
    // p.ParseFromString(data);
    // transport->DispatchTP_main([this, p, &remote](){
    //   this->HandlePingMessage(this, remote, ping);
    //   return (void*) true;
    // });


    // Fallback Messages
  } else if (type == phase1FB.GetTypeName()) {
    ManageDispatchPhase1FB(remote, data);

  } else if (type == phase2FB.GetTypeName()) {
    ManageDispatchPhase2FB(remote, data);

  } else if (type == invokeFB.GetTypeName()) {
    ManageDispatchInvokeFB(remote, data);

  } else if (type == electFB.GetTypeName()) {
    ManageDispatchElectFB(remote, data);

  } else if (type == decisionFB.GetTypeName()) {
    ManageDispatchDecisionFB(remote, data);

  } else if (type == moveView.GetTypeName()) {
    ManageDispatchMoveView(remote, data);

  }

  // Query Protocol Messages
  else if (type == queryReq.GetTypeName()) {
    ManageDispatchQuery(remote, data);
  } else if (type == syncMsg.GetTypeName()) {
    ManageDispatchSync(remote, data);
  } else if (type == requestTx.GetTypeName()) {
    ManageDispatchRequestTx(remote, data);
  } else if (type == supplyTx.GetTypeName()) {
    ManageDispatchSupplyTx(remote, data);
  }
  // Checkpoint Messages

  ///////

  else {
    Panic("Received unexpected message type: %s", type.c_str());
  }
}



///////////////////////////////////////////////////////////////////////

//DATA LOADING

//////////////////////////////////////////////////////////////////////

// Adds new key-value store entry
// Used for initialization (loading) the key-value store contents at startup.
// Debug("Stuck at line %d", __LINE__);
void Server::Load(const std::string &key, const std::string &value,
                  const Timestamp timestamp) {
  Value val;
  val.val = value;
  auto committedItr = committed.find("");
  UW_ASSERT(committedItr != committed.end());
  val.proof = committedItr->second;
  val.policyProof = committedItr->second;
  // TODO: actually set policy
  uint64_t policyId = policyIdFunction(key, value);
  val.policyId = policyId;
  std::pair<Timestamp, Policy *> tsPolicy;
  bool exists = policyStore.get(policyId, tsPolicy);
  if (!exists) {
    Panic("Policy %lu does not exist", policyId);
  }
  store.put(key, val, timestamp);
  if (key.length() == 5 && key[0] == 0) {
    std::cerr << std::bitset<8>(key[0]) << ' ' << std::bitset<8>(key[1]) << ' '
              << std::bitset<8>(key[2]) << ' ' << std::bitset<8>(key[3]) << ' '
              << std::bitset<8>(key[4]) << ' ' << std::endl;
  }
}

void Server::LoadPolicyStore(const std::string &policyStorePath) {
  PolicyParseClient policyParseClient;

  std::map<uint64_t, Policy *> policies = policyParseClient.ParseConfigFile(policyStorePath);

  for (const auto &p : policies) {
    policyStore.put(p.first, p.second, Timestamp());
    policiesToFree.push_back(p.second);
  }
}

// TODO: For hotstuffPG store --> let proxy call into PG
// TODO: For Crdb --> let server establish a client connection to backend too.
void Server::CreateTable(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, const std::vector<uint32_t> &primary_key_col_idx){
  //Based on Syntax from: https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-create-table/ 

  //NOTE: Assuming here we do not need special descriptors like foreign keys, column condidtions... (If so, it maybe easier to store the SQL statement in JSON directly)
  UW_ASSERT(!column_data_types.empty());
  //UW_ASSERT(!primary_key_col_idx.empty());

  std::string sql_statement("CREATE TABLE");
  sql_statement += " " + table_name;
  
  sql_statement += " (";
  for(auto &[col, type]: column_data_types){
    std::string p_key = (primary_key_col_idx.size() == 1 && col == column_data_types[primary_key_col_idx[0]].first) ? " PRIMARY KEY": "";
    sql_statement += col + " " + type + p_key + ", ";
  }

  sql_statement.resize(sql_statement.size() - 2); //remove trailing ", "

  if(primary_key_col_idx.size() > 1){
    sql_statement += ", PRIMARY KEY ";
    if(primary_key_col_idx.size() > 1) sql_statement += "(";

    for(auto &p_idx: primary_key_col_idx){
      sql_statement += column_data_types[p_idx].first + ", ";
    }
    sql_statement.resize(sql_statement.size() - 2); //remove trailing ", "

    if(primary_key_col_idx.size() > 1) sql_statement += ")";
  }
  
  
  sql_statement +=");";

  std::cerr << "Create Table: " << sql_statement << std::endl;

  //Call into TableStore with this statement.
  table_store->ExecRaw(sql_statement);

  //Create TABLE version  -- just use table_name as key.  This version tracks updates to "table state" (as opposed to row state): I.e. new row insertions; row deletions;
  //Note: It does currently not track table creation/deletion itself -- this is unsupported. If we do want to support it, either we need to make a separate version; 
                                                                 //or we require inserts/updates to include the version in the ReadSet. 
                                                                 //However, we don't want an insert to abort just because another row was inserted.
  Load(EncodeTable(table_name), "", Timestamp());

  //Create TABLE_COL version -- use table_name + delim + col_name as key. This version tracks updates to "column state" (as opposed to table state): I.e. row Updates;
  //Updates to column values change search meta data such as Indexes on a given Table. Scans that search on the column (using Active Reads) should conflict
  for(auto &[col_name, _] : column_data_types){
    Load(EncodeTableCol(table_name, col_name), "", Timestamp());
  }

  TableWriteMap::accessor tw;
  tableWrites.insert(tw, table_name);
  tw->second.clear();
  tw.release();

  HighTblVmap::accessor ht;
  highTableVersions.insert(ht, table_name); 
  ht->second = Timestamp(0, 0);
  ht.release();
}

void Server::CreateIndex(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, const std::string &index_name, const std::vector<uint32_t> &index_col_idx){
  //Based on Syntax from: https://www.postgresqltutorial.com/postgresql-indexes/postgresql-create-index/ and  https://www.postgresqltutorial.com/postgresql-indexes/postgresql-multicolumn-indexes/
  //CREATE INDEX index_name ON table_name(a,b,c,...);

  UW_ASSERT(!column_data_types.empty());
  UW_ASSERT(!index_col_idx.empty());
  UW_ASSERT(column_data_types.size() >= index_col_idx.size());

  std::string sql_statement("CREATE INDEX");
  sql_statement += " " + index_name;
  sql_statement += " ON " + table_name;

  sql_statement += "(";
  for (auto &i_idx : index_col_idx) {
    sql_statement += column_data_types[i_idx].first + ", ";
  }
  sql_statement.resize(sql_statement.size() - 2); // remove trailing ", "

  sql_statement += ");";

  // Call into TableStore with this statement.
  table_store->ExecRaw(sql_statement);

}

void Server::CacheCatalog(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, 
      const std::vector<uint32_t> &primary_key_col_idx){

    UW_ASSERT(!column_data_types.empty());

    //This is just a dummy transaction to trigger Catalog Cache generation in a concurrency free setting
    std::string sql_statement = fmt::format("SELECT * FROM {};", table_name); //Note: this might not invoke index caching. In that case, maybe add primary keys in..
    //TODO: ExecRaw, but turn on cache use..
    
    table_store->ExecRaw(sql_statement, false);
}

void Server::LoadTableData_SQL(const std::string &table_name, const std::string &table_data_path, const std::vector<uint32_t> &primary_key_col_idx){

  //Syntax based of: https://www.postgresqltutorial.com/postgresql-tutorial/import-csv-file-into-posgresql-table/ 
    std::string copy_table_statement = fmt::format("COPY {0} FROM {1} DELIMITER ',' CSV HEADER", table_name, table_data_path);

    //TODO: For CRDB: https://www.cockroachlabs.com/docs/stable/import-into.html
    //std::string copy_table_statement_crdb = fmt::format("IMPORT INTO {0} CSV DATA {1} WITH skip = '1'", table_name, table_data_path); //FIXME: does one need to specify column names? Target columns don't appear to be enforced

    //Call into TableStore with this statement.
    std::cerr << "Load Table: " << copy_table_statement << std::endl;

    //table_store->ExecRaw(copy_table_statement);
    auto committedItr = committed.find("");
    UW_ASSERT(committedItr != committed.end());
    
    Timestamp genesis_ts(0,0);
    proto::CommittedProof *genesis_proof = committedItr->second;
    std::string genesis_txn_dig = TransactionDigest(genesis_proof->txn(), params.hashDigest); //("");
    table_store->LoadTable(copy_table_statement, genesis_txn_dig, genesis_ts, genesis_proof);

    //std::cerr << "Load Table: " << copy_table_statement << std::endl;

    //std::cerr << "read csv data: " << table_data_path << std::endl;
    std::ifstream row_data(table_data_path);

    //Skip header
    std::string columns;
    getline(row_data, columns); 

    // std::cerr << "cols : " << columns << std::endl;

    std::string row_line;
    std::string value;

    //NOTE: CSV Data is UNQUOTED always
    //std::vector<bool> *col_quotes = table_store->GetRegistryColQuotes(table_name);

    while(getline(row_data, row_line)){
     
      //std::cerr << "row_line: " << row_line;
      std::vector<std::string> primary_col_vals;
      uint32_t col_idx = 0;
      uint32_t p_col_idx = 0;
      // used for breaking words
      std::stringstream row(row_line);

      // read every column data of a row and store it in a string variable, 'value'. Extract only the primary_col_values
      while (getline(row, value, ',')) {
        if(col_idx == primary_key_col_idx[p_col_idx]){
          // if((*col_quotes)[col_idx]){ //If value has quotes '' strip them off
          //     primary_col_vals.push_back(value.substr(1, value.length()-2));
          // }
          // else{
              primary_col_vals.push_back(std::move(value));

              p_col_idx++;
              if(p_col_idx >= primary_key_col_idx.size()) break;
         //}
        }
        col_idx++;
      }
      std::string enc_key = EncodeTableRow(table_name, primary_col_vals);
      Load(enc_key, "", Timestamp());

      //std::cerr << "  ==> Enc Key: " << enc_key << std::endl;
    }
}


static bool parallel_load = true; 
static int max_segment_size = 20000;//INT_MAX; //20000 seems to work well for TPC-C 1 warehouse and for Seats

void Server::LoadTableData(const std::string &table_name, const std::string &table_data_path, 
    const std::vector<std::pair<std::string, std::string>> &column_names_and_types, const std::vector<uint32_t> &primary_key_col_idx)
{

    //Call into TableStore with this statement.
   Notice("Load Data for Table %s from: %s", table_name.c_str(), table_data_path.c_str());

    //Get Genesis TX.
    auto committedItr = committed.find("");
    UW_ASSERT(committedItr != committed.end());
    
    Timestamp genesis_ts(0,0);
    proto::CommittedProof *genesis_proof = committedItr->second;
    std::string genesis_txn_dig = TransactionDigest(genesis_proof->txn(), params.hashDigest); //("");

    auto f = [this, genesis_ts, genesis_proof, genesis_txn_dig, table_name, table_data_path, column_names_and_types, primary_key_col_idx](){
      Debug("Parsing Table on core %d", sched_getcpu());
      std::vector<row_segment_t*> table_row_segments = ParseTableDataFromCSV(table_name, table_data_path, column_names_and_types, primary_key_col_idx);

      Debug("Dispatch Table Loading for table: %s. Number of Segments: %d", table_name.c_str(), table_row_segments.size());
      int i = 0;
      for(auto& row_segment: table_row_segments){
        if(row_segment->empty()){
          delete row_segment;
          continue; //Note: This could happen when "popping back" rows for sharding purposes
        }
        LoadTableRows(table_name, column_names_and_types, row_segment, primary_key_col_idx, ++i, false); //Already loaded into CC-store.
      }
      return (void*) true;
    };
    if(parallel_load){
       transport->DispatchTP_noCB(std::move(f)); //Dispatching this seems to add no perf
    }
    else{
      f();
    }
}

std::vector<row_segment_t*> Server::ParseTableDataFromCSV(const std::string &table_name, const std::string &table_data_path, 
    const std::vector<std::pair<std::string, std::string>> &column_names_and_types, const std::vector<uint32_t> &primary_key_col_idx){
    //Read in CSV --> Transform into ApplyTableWrite
    std::ifstream row_data(table_data_path);

    //Skip header
    std::string columns;
    getline(row_data, columns); 

    // std::cerr << "cols : " << columns << std::endl;

    std::string row_line;
    std::string value;

    //NOTE: CSV Data is UNQUOTED always
    //std::vector<bool> *col_quotes = table_store->GetRegistryColQuotes(table_name);

    //Turn CSV into Vector of Rows. Split Table into Segments for parallel loading
    //Each segment is allocated, so that we don't have to copy it when dispatching it to another thread for parallel loading.

    uint64_t total_rows = 0;
    uint64_t local_rows = 0;

    std::vector<row_segment_t*> table_row_segments = {new row_segment_t};

    while(getline(row_data, row_line)){
      total_rows++;
      //std::cerr << "row_line: " << row_line;

      if(table_row_segments.back()->size() >= max_segment_size) table_row_segments.push_back(new row_segment_t);
      auto row_segment = table_row_segments.back();
    
      // used for breaking words
      std::stringstream row(row_line);

      row_segment->push_back({}); //Create new row
      std::vector<std::string> &row_values = row_segment->back();
      
      // read every column data of a row and store it
      while (getline(row, value, ',')) {
        row_values.push_back(std::move(value));
      }

      //Also Load row into CC-store
      std::vector<const std::string*> primary_cols;
      for(auto i: primary_key_col_idx){
        primary_cols.push_back(&(row_values[i]));
      }
      std::string enc_key = EncodeTableRow(table_name, primary_cols);
      //Notice("Encoded key: %s. Table: %s", enc_key.c_str(), table_name.c_str());
    
      //Only load if in local partition. If not => remove last row_values val. 
      auto designated_group = ((*part)("", enc_key, numShards, groupIdx, dummyTxnGroups, true) % numGroups);
      
      //Notice("Encoded key: %s is designated for group %d. Curr group: %d", enc_key.c_str(), designated_group, groupIdx);
      if(designated_group != groupIdx){
        //Panic("shouldnt be getting here when testing without sharding. TODO: Remove this line when testing WITH sharding");
        row_segment->pop_back();
        continue;
      }

      Load(enc_key, "", Timestamp());

      local_rows++;
    }
    
    //avoid empty segments (could happen if we pop back a row from a segment)
    auto last_row_segment = table_row_segments.back();
    Debug("Table[%s]. Last segment size: %d.", table_name.c_str(), last_row_segment->size());
    if(last_row_segment->empty()){
      table_row_segments.pop_back();
      delete last_row_segment;
    }

    Notice("Loading %d / %d rows for table %s", local_rows, total_rows, table_name.c_str());

    return table_row_segments;
}

void Server::LoadTableRows(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, 
                           const row_segment_t *row_segment, const std::vector<uint32_t> &primary_key_col_idx, int segment_no, bool load_cc){
  
    Debug("Load Table: %s [Segment: %d]", table_name.c_str(), segment_no);
    //Call into TableStore with this statement.
    //table_store->ExecRaw(sql_statement);
    
    //GENESIS TX
    auto committedItr = committed.find("");
    UW_ASSERT(committedItr != committed.end());
    Timestamp genesis_ts(0,0);
    proto::CommittedProof *genesis_proof = committedItr->second; 
    std::string genesis_txn_dig = TransactionDigest(genesis_proof->txn(), params.hashDigest); //("");
    //std::string genesis_tx_dig("");

    Debug("Dispatch Table Loading for table: %s. Segment [%d] with %d rows", table_name.c_str(), segment_no, row_segment->size());

    auto f = [this, genesis_proof, genesis_ts, genesis_txn_dig, table_name, segment_no, row_segment, load_cc, primary_key_col_idx](){
      //Load it into CC-Store (Note: Only if we haven't already done it while reading from CSV)
                  //Note: Partitioning has already been checked: For RW-SQL it happens at server.cc level. For all others it happens in ParseFromCSV.
      if(load_cc){
        Debug("Load segment %d of table %s", segment_no, table_name.c_str());
        for(auto &row: *row_segment){
      
          //Load it into CC-store
          std::vector<const std::string*> primary_cols;
          for(auto i: primary_key_col_idx){
            primary_cols.push_back(&(row[i]));
          }
          std::string enc_key = EncodeTableRow(table_name, primary_cols);
        
          if(simulate_point_kv){
            //Debug("Loading key: %s. With value: %s", enc_key.c_str(), row[1].c_str());
            Load(enc_key, row[1], Timestamp());
            continue;
          }
          Load(enc_key, "", Timestamp());
        }
      }

      if(simulate_point_kv) return (void*) true; //Don't need to load into peloton
      
    
    //Put this into dispatch: (pass gensiss proof)

    //TODO: Pass a pointer to row segment instead of it... (TODO: Allocate row segment and then delete)
    // auto f = [this, genesis_proof, genesis_ts, genesis_txn_dig, table_name, segment_no, row_segment](){
      Debug("Loading Table: %s [Segment: %d]. On core %d", table_name.c_str(), segment_no, sched_getcpu());

      table_store->LoadTable(table_store->sql_interpreter.GenerateLoadStatement(table_name, *row_segment, segment_no), genesis_txn_dig, genesis_ts, genesis_proof);
      delete row_segment;
      Notice("Finished loading Table: %s [Segment: %d]. On core %d", table_name.c_str(), segment_no, sched_getcpu());
       return (void*) true;
    };
    // Call into ApplyTableWrites from different threads. On each Thread, it is a synchronous interface.

    if(parallel_load){
       transport->DispatchTP_noCB(std::move(f)); 
    }
    else{
      f();
    }
   
  return;
}

//It should split segments.
//Is it better to read in segments from data right away? or split after?
 //Cleanest seems to be: Directly read into segments. Then for each segment call LoadTableRows.

void Server::LoadTableRows_Old(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, 
                           const std::vector<std::vector<std::string>> &row_values, const std::vector<uint32_t> &primary_key_col_idx, int segment_no){
  
  Debug("Load Table: %s", table_name.c_str());
  //Call into TableStore with this statement.
  //table_store->ExecRaw(sql_statement);
  auto committedItr = committed.find("");
  UW_ASSERT(committedItr != committed.end());
  //std::string genesis_tx_dig("");
  Timestamp genesis_ts(0,0);
  
  int segment_size = row_values.size(); // // single segment for now
  int row_segments = 1; //row_values.size() / segment_size;

  // int segment_size = 20000; // single segment for now
  // int row_segments = row_values.size() / segment_size;


  //1) try splitting into segments.
  //2) try parallelizing loading!
      // Spin of LoadTableRows to a worker thread.  (one per segment)
      // NOTE: Each segment must have it's own copy of genesis_proof. This is because we currently set the TXN data to read in inside Genesis_Proof
      // TODO: Create a more direct interface for loading.
     

  //3) don't need row_values to be const?? allow moving. Avoid as many copies as we can for loading. => Create a special GenerateWrite statement that consumes input (rather than copying)
  for(int i=0; i<row_segments; ++i){
    proto::CommittedProof *genesis_proof = new proto::CommittedProof(*committedItr->second); //NOTE: Different TX might have different pointer references 
                                                                                            //I.e. there can be multiple genesis copies.

    std::string genesis_txn_dig = TransactionDigest(genesis_proof->txn(), params.hashDigest); //("");


    TableWrite &table_write = (*genesis_proof->mutable_txn()->mutable_table_writes())[table_name];
    for(int j=0; j<segment_size; ++j){
      const std::vector<std::string> &row = row_values[i*segment_size + j];

      //Load it into CC-store
      std::vector<const std::string*> primary_cols;
      for(auto i: primary_key_col_idx){
        primary_cols.push_back(&(row[i]));
      }
      std::string enc_key = EncodeTableRow(table_name, primary_cols);
      Load(enc_key, "", Timestamp());

      //Create Table-write
      RowUpdates *new_row = table_write.add_rows();
      for(auto &value: row){
        new_row->add_column_values(std::move(value));
      }
      //*new_row = {row.begin(), row.end()}; //one-liner, but incurs copying
    }
    
    Debug("Dispatch Table Loading for table: %s. Segment [%d/%d]", table_name.c_str(), i+1, row_segments);
    //Put this into dispatch: (pass gensiss proof)
    auto f = [this, genesis_proof, genesis_ts, genesis_txn_dig, table_name, i, row_segments](){
      Debug("Loading Table: %s [Segment: %d/%d]. On core %d", table_name.c_str(), i+1, row_segments, sched_getcpu());

      //TODO: Create a custom loader interface... This is needlessly slow. And no need for data to be inside the proof...
      ApplyTableWrites(genesis_proof->txn(), genesis_ts, genesis_txn_dig, genesis_proof);
      genesis_proof->mutable_txn()->clear_table_writes(); //don't need to keep storing them.   
      Debug("Finished loading Table: %s [Segment: %d/%d]. On core %d", table_name.c_str(), i+1, row_segments, sched_getcpu());
       return (void*) true;
    };
    // Call into ApplyTableWrites from different threads. On each Thread, it is a synchronous interface.
    transport->DispatchTP_noCB(std::move(f));
    }
  return;
}

// void Server::LoadTableRows(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, const std::vector<std::vector<std::string>> &row_values, const std::vector<uint32_t> &primary_key_col_idx ){
  
//   //Call into TableStore with this statement.
//   //table_store->ExecRaw(sql_statement);
//   auto committedItr = committed.find("");
//   UW_ASSERT(committedItr != committed.end());
//   //std::string genesis_tx_dig("");
//   Timestamp genesis_ts(0,0);
//   proto::CommittedProof *genesis_proof = committedItr->second;

//   std::string genesis_txn_dig = TransactionDigest(genesis_proof->txn(), params.hashDigest); //("");

//    //TODO: Instead of using the INSERT SQL statement, could generate a TableWrite and use the TableWrite API.
//   TableWrite &table_write = (*genesis_proof->mutable_txn()->mutable_table_writes())[table_name];
//   for(auto &row: row_values){

//     //Load it into CC-store
//     std::vector<const std::string*> primary_cols;
//     for(auto i: primary_key_col_idx){
//       primary_cols.push_back(&(row[i]));
//     }
//     std::string enc_key = EncodeTableRow(table_name, primary_cols);
//     Load(enc_key, "", Timestamp());

//     //Create Table-write
//     RowUpdates *new_row = table_write.add_rows();
//     for(auto &value: row){
//       new_row->add_column_values(std::move(value));
//     }
//     //*new_row = {row.begin(), row.end()}; //one-liner, but incurs copying
//   }
  
//   ApplyTableWrites(genesis_proof->txn(), genesis_ts, genesis_txn_dig, genesis_proof);

//   genesis_proof->mutable_txn()->clear_table_writes(); //don't need to keep storing them. 
// }

//!!"Deprecated" (Unused)
void Server::LoadTableRow(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, const std::vector<std::string> &values, const std::vector<uint32_t> &primary_key_col_idx ){
  

  //TODO: Instead of using the INSERT SQL statement, could generate a TableWrite and use the TableWrite API.
  std::string sql_statement("INSERT INTO");
  sql_statement += " " + table_name ;

  UW_ASSERT(!values.empty()); //Need to insert some values...
  UW_ASSERT(column_data_types.size() == values.size()); //Need to specify all columns to insert into

  sql_statement += " (";
  for(auto &[col, _]: column_data_types){
    sql_statement += col + ", ";
  }
  sql_statement.resize(sql_statement.size() - 2); //remove trailing ", "
  sql_statement +=")";
  
  sql_statement += " VALUES (";
  
  std::vector<bool> *col_quotes = table_store->GetRegistryColQuotes(table_name); //TODO: Add Quotes if applicable
  for(auto &val: values){
    //Note: Adding quotes indiscriminately for now.
    sql_statement += "\'" + val + "\'" + ", ";
  }
  sql_statement.resize(sql_statement.size() - 2); //remove trailing ", "

  sql_statement += ");" ;
  
  //Call into TableStore with this statement.
  //table_store->ExecRaw(sql_statement);
  auto committedItr = committed.find("");
  UW_ASSERT(committedItr != committed.end());
  //std::string genesis_tx_dig("");
  Timestamp genesis_ts(0,0);
  proto::CommittedProof *genesis_proof = committedItr->second;
   std::string genesis_txn_dig = TransactionDigest(genesis_proof->txn(), params.hashDigest); //("");

  table_store->LoadTable(sql_statement, genesis_txn_dig, genesis_ts, genesis_proof);

  std::vector<const std::string*> primary_cols;
  for(auto i: primary_key_col_idx){
    primary_cols.push_back(&(values[i]));
  }
  std::string enc_key = EncodeTableRow(table_name, primary_cols);
  Load(enc_key, "", Timestamp());
}

///////////////////////////////////////////////////////////////////////

//PROTOCOL REALM

//////////////////////////////////////////////////////////////////////



//Handle Read Message
//Dispatches to reader thread if params.parallel_reads = true, and multithreading enabled
//Returns a signed message including i) the latest committed write (+ cert), and ii) the latest prepared write (both w.r.t to Timestamp of reader)
void Server::HandleRead(const TransportAddress &remote,
     proto::Read &msg) {

  Debug("READ[%lu:%lu] for key %s with ts %lu.%lu.", msg.timestamp().id(),
      msg.req_id(), BytesToHex(msg.key(), 16).c_str(),
      msg.timestamp().timestamp(), msg.timestamp().id());
  Timestamp ts(msg.timestamp());
  if (CheckHighWatermark(ts)) {
    // ignore request if beyond high watermark
    Debug("Read timestamp beyond high watermark.");
    if(params.mainThreadDispatching && (!params.dispatchMessageReceive || params.parallel_reads)) FreeReadmessage(&msg);
    return;
  }

  std::pair<Timestamp, Server::Value> tsVal;
  //find committed write value to read from
  bool committed_exists = store.get(msg.key(), ts, tsVal);

  proto::ReadReply* readReply = GetUnusedReadReply();
  readReply->set_req_id(msg.req_id());
  readReply->set_key(msg.key()); //technically don't need to send back.
  readReply->mutable_write()->set_key(msg.key());
  if (committed_exists) {
    //if(tsVal.first > ts) Panic("Should not read committed value with larger TS than read");
    Debug("READ[%lu:%lu] Committed value of length %lu bytes with ts %lu.%lu.",
        msg.timestamp().id(), msg.req_id(), tsVal.second.val.length(), tsVal.first.getTimestamp(),
        tsVal.first.getID());
    readReply->mutable_write()->set_committed_value(tsVal.second.val);
    tsVal.first.serialize(readReply->mutable_write()->mutable_committed_timestamp());
    if (params.validateProofs) {
      *readReply->mutable_proof() = *tsVal.second.proof;
      *readReply->mutable_policy_proof() = *tsVal.second.policyProof;
    }

    // get policy from policyStore
    std::pair<Timestamp, Policy *> tsPolicy;
    bool policyExists = policyStore.get(tsVal.second.policyId, ts, tsPolicy);
    if (!policyExists) {
      Panic("Cannot find policy %lu in policyStore", tsVal.second.policyId);
    }
    readReply->mutable_write()->mutable_committed_policy()->set_policy_id(tsVal.second.policyId);
    tsPolicy.second->SerializeToProtoMessage(readReply->mutable_write()->mutable_committed_policy());
  }

  TransportAddress *remoteCopy = remote.clone();


  //auto sendCB = [this, remoteCopy, readReply, c_id = msg.timestamp().id(), req_id=msg.req_id()]() {
  //Debug("Sent ReadReply[%lu:%lu]", c_id, req_id);  
  auto sendCB = [this, remoteCopy, readReply]() {
    this->transport->SendMessage(this, *remoteCopy, *readReply);
    delete remoteCopy;
    FreeReadReply(readReply);
  };

  //If MVTSO: Read prepared, Set RTS
  if (occType == MVTSO) {
  
    //Sets RTS timestamp. Favors readers commit chances.
    //Disable if worried about Byzantine Readers DDos, or if one wants to favor writers.
    Debug("Set up RTS for READ[%lu:%lu]", msg.timestamp().id(), msg.req_id());
    SetRTS(ts, msg.key());

  
    //find prepared write to read from
    /* add prepared deps */
    if (params.maxDepDepth > -2) {
      Debug("Look for prepared value to READ[%lu:%lu]", msg.timestamp().id(), msg.req_id());
      CheckPreparedWrites(msg.key(), ts, committed_exists, tsVal, readReply);
    }
    /*
    if (params.maxDepDepth > -2) {
      Debug("Look for prepared value to READ[%lu:%lu]", msg.timestamp().id(), msg.req_id());
      const proto::Transaction *mostRecent = nullptr;

      //std::pair<std::shared_mutex,std::map<Timestamp, const proto::Transaction *>> &x = preparedWrites[write.key()];
      auto itr = preparedWrites.find(msg.key());
      //tbb::concurrent_unordered_map<std::string, std::pair<std::shared_mutex,std::map<Timestamp, const proto::Transaction *>>>::const_iterator itr = preparedWrites.find(msg.key());
      if (itr != preparedWrites.end()){

        //std::pair &x = preparedWrites[write.key()];
        std::shared_lock lock(itr->second.first);
        if(itr->second.second.size() > 0) {

          // //Find biggest prepared write smaller than TS.
          // auto it = itr->second.second.lower_bound(ts); //finds smallest element greater equal TS
          // if(it != itr->second.second.begin()) { //If such an elem exists; go back one == greates element less than TS
          //     --it;
          // }
          // if(it != itr->second.second.begin()){ //if such elem exists: read from it.
          //     mostRecent = it->second;
          //     if(exists && tsVal.first > Timestamp(mostRecent->timestamp())) mostRecent = nullptr; // don't include prepared read if it is smaller than committed.
          // }

          // there is a prepared write for the key being read
          for (const auto &t : itr->second.second) {
            if(t.first > ts) break; //only consider it if it is smaller than TS (Map is ordered, so break should be fine here.)
            if(committed_exists && t.first <= tsVal.first) continue; //only consider it if bigger than committed value. 
            if (mostRecent == nullptr || t.first > Timestamp(mostRecent->timestamp())) { 
              mostRecent = t.second;
            }
          }

          if (mostRecent != nullptr) {
            //if(Timestamp(mostRecent->timestamp()) > ts) Panic("Reading prepared write with TS larger than read ts");
            std::string preparedValue;
            uint64_t preparedPolicyId;
            for (const auto &w : mostRecent->write_set()) {
              if (w.key() == msg.key()) {
                preparedValue = w.value();
                preparedPolicyId = GetPolicyId(w.key(), w.value(), mostRecent->timestamp());
                break;
              }
            }

            Debug("Prepared write with most recent ts %lu.%lu.",
                mostRecent->timestamp().timestamp(), mostRecent->timestamp().id());
            //std::cerr << "Dependency depth: " << (DependencyDepth(mostRecent)) << std::endl;
            if (params.maxDepDepth == -1 || DependencyDepth(mostRecent) <= params.maxDepDepth) {
              readReply->mutable_write()->set_prepared_value(preparedValue);
              *readReply->mutable_write()->mutable_prepared_timestamp() = mostRecent->timestamp();
              *readReply->mutable_write()->mutable_prepared_txn_digest() = TransactionDigest(*mostRecent, params.hashDigest);

              // get policy from policyStore
              std::pair<Timestamp, Policy *> tsPolicy;
              bool policyExists = policyStore.get(preparedPolicyId, mostRecent->timestamp(), tsPolicy);
              if (!policyExists) {
                Panic("Cannot find policy %lu in policyStore", preparedPolicyId);
              }
              readReply->mutable_write()->mutable_prepared_policy()->set_policy_id(preparedPolicyId);
              tsPolicy.second->SerializeToProtoMessage(readReply->mutable_write()->mutable_prepared_policy());
            }
          }
        }
      }
    }
    */
  }

  //Sign and Send Reply
  if (params.validateProofs && params.signedMessages && (readReply->write().has_committed_value() || (params.verifyDeps && readReply->write().has_prepared_value()))) {
    //remove params.verifyDeps requirement to sign prepared. Not sure if it causes a bug so I kept it for now -- realistically never triggered
    //TODO: This code does not sign a message if there is no value at all (or if verifyDeps == false and there is only a prepared, i.e. no committed, value) -- change it so it always signs.  //Note: Need to make compatible in a bunch of places
        proto::Write *write = readReply->release_write();
        SignSendReadReply(write, readReply->mutable_signed_write(), sendCB);
  }
  else{
      sendCB();
  }

  if(params.mainThreadDispatching && (!params.dispatchMessageReceive || params.parallel_reads)) FreeReadmessage(&msg);
}

void Server::CheckPreparedWrites(const std::string &key, const Timestamp &ts, const bool committed_exists, 
    const std::pair<Timestamp, Server::Value> &tsVal, proto::ReadReply* readReply) {
  const proto::Transaction *mostRecent = nullptr;
  const proto::Transaction *mostRecentPolicyChange = nullptr;

  auto itr = preparedWrites.find(key);
  if (itr != preparedWrites.end()){
    std::shared_lock lock(itr->second.first);
    if(itr->second.second.size() > 0) {
      // there is a prepared write for the key being read
      for (const auto &t : itr->second.second) {
        if(t.first > ts) break; //only consider it if it is smaller than TS (Map is ordered, so break should be fine here.)
        if(committed_exists && t.first <= tsVal.first) continue; //only consider it if bigger than committed value. 
        if (mostRecent == nullptr || t.first > Timestamp(mostRecent->timestamp())) { 
          if (t.second->policy_type() == proto::Transaction::NONE) {
            mostRecent = t.second;
          }
        }
        if (mostRecentPolicyChange == nullptr || t.first > Timestamp(mostRecentPolicyChange->timestamp())) {
          if (t.second->policy_type() == proto::Transaction::KEY_POLICY_ID) {
            mostRecentPolicyChange = t.second;
          }
        }
      }

      if (mostRecent != nullptr || mostRecentPolicyChange != nullptr) {
        std::string preparedValue;
        uint64_t preparedPolicyId;
        if (mostRecent != nullptr) {
          for (const auto &w : mostRecent->write_set()) {
            if (w.key() == key) {
              preparedValue = w.value();
              preparedPolicyId = GetPolicyId(w.key(), w.value(), mostRecent->timestamp());
              break;
            }
          }
        }

        // always take prepared policy change since above GetPolicyId will only take from committed values
        // even if mostRecent > mostRecentPolicyChange, mostRecentPolicyChange may not have committed
        if (mostRecentPolicyChange != nullptr) {
          for (const auto &w : mostRecentPolicyChange->write_set()) {
            if (w.key() == key) {
              preparedPolicyId = std::stoull(w.value());
              break;
            }
          }
        }

        bool preparedValueExists = true;
        if (mostRecent == nullptr && mostRecentPolicyChange != nullptr) {
          Debug("only policy change dependency exists for key %s", BytesToHex(key, 16).c_str());
          // in this case, this transaction only has a dependency that is pure policy change
          mostRecent = mostRecentPolicyChange;
          preparedValueExists = false;
        }

        Debug("Prepared write with most recent ts %lu.%lu.",
            mostRecent->timestamp().timestamp(), mostRecent->timestamp().id());
        if (params.maxDepDepth == -1 || DependencyDepth(mostRecent) <= params.maxDepDepth) {
          if (preparedValueExists) {
            readReply->mutable_write()->set_prepared_value(preparedValue);
            *readReply->mutable_write()->mutable_prepared_timestamp() = mostRecent->timestamp();
            *readReply->mutable_write()->mutable_prepared_txn_digest() = TransactionDigest(*mostRecent, params.hashDigest);
          }

          // get policy from policyStore
          std::pair<Timestamp, Policy *> tsPolicy;
          bool policyExists = policyStore.get(preparedPolicyId, mostRecent->timestamp(), tsPolicy);
          if (!policyExists) {
            Panic("Cannot find policy %lu in policyStore", preparedPolicyId);
          }
          readReply->mutable_write()->mutable_prepared_policy()->set_policy_id(preparedPolicyId);
          tsPolicy.second->SerializeToProtoMessage(readReply->mutable_write()->mutable_prepared_policy());
        }
      }
    }
  }
}


//////////////////////

// DEPRECATED
//Optional Code Handler in case one wants to parallelize P1 handling as well. Currently deprecated (possibly not working)
//Skip to HandlePhase1(...)
void Server::HandlePhase1_atomic(const TransportAddress &remote,
    proto::Phase1 &msg) {

  //atomic_testMutex.lock();
  std::string txnDigest = TransactionDigest(msg.txn(), params.hashDigest); //could parallelize it too hypothetically
  Debug("PHASE1[%lu:%lu][%s] with ts %lu.", msg.txn().client_id(),
      msg.txn().client_seq_num(), BytesToHex(txnDigest, 16).c_str(),
      msg.txn().timestamp().timestamp());

  proto::Transaction *txn = msg.release_txn();

  //NOTE: Ongoing *must* be added before p2/wb since the latter dont include it themselves as an optimization
  //TCP guarantees that this happens, but I cannot dispatch parallel P1 before logging ongoing or else it could be ordered after p2/wb.
  AddOngoing(txnDigest, txn);
 
  //NOTE: "Problem": If client comes after writeback, it will try to do a p2/wb itself but fail
  //due to ongoing being removed already.
  //XXX solution: add to ongoing always, and call Clean() for redundant Writebacks as well.
  //XXX alternative solution: Send a Forward Writeback message for normal path clients too.
          // make sure that Commit+Clean is atomic; and that the check for commit and remainder is atomic.
            // use overarching lock on ongoing.

  if(params.parallel_CCC){
    TransportAddress* remoteCopy = remote.clone();
    auto f = [this, remoteCopy, p1 = &msg, txn, txnDigest]() mutable {
        this->ProcessPhase1_atomic(*remoteCopy, *p1, txn, txnDigest);
        delete remoteCopy;
        return (void*) true;
      };
      transport->DispatchTP_noCB(std::move(f));
  }
  else{
    ProcessPhase1_atomic(remote, msg, txn, txnDigest);
  }
}

void Server::ProcessPhase1_atomic(const TransportAddress &remote,
    proto::Phase1 &msg, proto::Transaction *txn, std::string &txnDigest){

  proto::ConcurrencyControl::Result result;
  const proto::CommittedProof *committedProof;
  const proto::Transaction *abstain_conflict = nullptr;
      //NOTE : make sure c and b dont conflict on lock order
  p1MetaDataMap::accessor c;
  bool hasP1 = p1MetaData.find(c, txnDigest);
  // p1MetaData.insert(c, txnDigest);
  // bool hasP1 = c->second.hasP1;
  // no-replays property, i.e. recover existing decision/result from storage
  //Ignore duplicate requests that are already committed, aborted, or ongoing
  if(hasP1){
    result = c->second.result;

    if(result == proto::ConcurrencyControl::WAIT){
        ManageDependencies(txnDigest, *txn, remote, msg.req_id());
    }
    else if (result == proto::ConcurrencyControl::ABORT) {
      committedProof = c->second.conflict;
      UW_ASSERT(committedProof != nullptr);
    }

  } else{

    if (params.validateProofs && params.signedMessages && params.verifyDeps) {
      for (const auto &dep : txn->deps()) {
        if (!dep.has_write_sigs()) {
          Debug("Dep for txn %s missing signatures.",
              BytesToHex(txnDigest, 16).c_str());
          if((params.mainThreadDispatching && (!params.dispatchMessageReceive || params.parallel_CCC)) || (params.multiThreading && params.signClientProposals)) FreePhase1message(&msg);
          return;
        }
        if (!ValidateDependency(dep, &config, params.readDepSize, keyManager,
              verifier)) {
          Debug("VALIDATE Dependency failed for txn %s.",
              BytesToHex(txnDigest, 16).c_str());
          // safe to ignore Byzantine client
          if((params.mainThreadDispatching && (!params.dispatchMessageReceive || params.parallel_CCC)) || (params.multiThreading && params.signClientProposals)) FreePhase1message(&msg);
          return;
        }
      }
    }
    //c.release();
    //current_views[txnDigest] = 0;
    p2MetaDataMap::accessor p;
    p2MetaDatas.insert(p, txnDigest);
    p.release();
    //p2MetaDatas.insert(std::make_pair(txnDigest, P2MetaData()));

    Timestamp retryTs;

    result = DoOCCCheck(msg.req_id(), remote, txnDigest, *txn, retryTs,
          committedProof, abstain_conflict);
    // uint64_t reqId = msg.req_id();
    // BufferP1Result(c, result, committedProof, txnDigest, reqId, 0, &remote, false);

  }
  c.release();
  //atomic_testMutex.unlock();
  HandlePhase1CB(msg.req_id(), result, committedProof, txnDigest, txn, remote, abstain_conflict, false);
  if((params.mainThreadDispatching && (!params.dispatchMessageReceive || params.parallel_CCC)) || (params.multiThreading && params.signClientProposals)) FreePhase1message(&msg);
}


//Helper function that forwards a received P1 message to a neighboring replica. Only does so if param.replica_gossip = true
//Goal: Increases robustness against Byzantine Clients that only send P1 to some replicas. 
//      This can cause concurrent transactions to miss forming dependencies, but later have to abort due to the conflict.
void Server::ForwardPhase1(proto::Phase1 &msg){
  //std::vector<uint64_t> recipients;
  msg.set_replica_gossip(true);
  //Optionally send to f+1 neighboring replicas instead of 1 for increased robustness
  // for(int i = 1; i <= config.f+1; ++i){
  //   uint64_t nextReplica = (idx + i) % config.n;
  //   transport->SendMessageToReplica(this, groupIdx, nextReplica, msg);
  // }

  uint64_t nextReplica = (idx + 1) % config.n;
  transport->SendMessageToReplica(this, groupIdx, nextReplica, msg);
}

//Helper function to elect a replica leader responsible for completing the commit protocol of a tx if no client leader finishes it 
//Actual garbage collection (finishing commit protocol NOT currently implemented)
void Server::Inform_P1_GC_Leader(proto::Phase1Reply &reply, proto::Transaction &txn, std::string &txnDigest, int64_t grpLeader){ //default -1
  int64_t logGrp = GetLogGroup(txn, txnDigest);
  if(grpLeader == -1) grpLeader = txnDigest[0] % config.n; //Default *first* leader,
  transport->SendMessageToReplica(this, logGrp, grpLeader,reply);
}

//Handler for Phase1 message
//Manages ongoing transactions
//Executes Concurrency Control Check
//Replies with signed Phase1Reply voting on whether to Commit/Abort the transaction
//NOTE: Current Client Implementation in the Normal Case is "non-adaptive", i.e. it always expects a P1Reply, and cannot handle a P2Reply or WritebackAck (e.g. if the replica already has committed the tx). 
                                                                           // Optimize this in future iterations.
void Server::HandlePhase1(const TransportAddress &remote, proto::Phase1 &msg) {
  // dummyTx = msg.txn(); //PURELY TESTING PURPOSES!!: NOTE WARNING

  // UW_ASSERT(msg.endorsements().sig_msgs_size() == 1);

   proto::Transaction *txn;
  if(params.signClientProposals){
    txn = new proto::Transaction();
     txn->ParseFromString(msg.signed_txn().data());
      //  Alternatively change proto to contain txn in addition to signed bytes: its easier if it contains both the TX + the signed bytes.
          //      Tradeoff: deserialization cost vs double bytes...
          // In remainder of code: Signature is on serialized message, not on hash. That is easier than defining our own hash function for all messages. Since we compute the hash here its a bit redundant but thats okay.
  }
  else{
     txn = msg.mutable_txn();
  }
  
  std::string txnDigest = TransactionDigest(*txn, params.hashDigest); //could parallelize it too hypothetically
  //Notice("Txn:[%d:%d] has digest: %s", txn->client_id(), txn->client_seq_num(), BytesToHex(txnDigest, 16).c_str());

  Debug("Received Phase1 message for txn id: %s", BytesToHex(txnDigest, 16).c_str());
  //if(params.signClientProposals) *txn->mutable_txndigest() = txnDigest; //Hack to have access to txnDigest inside TXN later (used for abstain conflict)
  *txn->mutable_txndigest() = txnDigest; //Hack to have access to txnDigest inside TXN later (used for abstain conflict, and for FindTableVersion)

  //If have ooMSG. Ignore P1, and just process ooMsg. Note: Typically shouldn't happen with TPCC, but it is possible that moodycamel queue is not Fifo
  ooMap::accessor oo;
  bool has_oo = ooMessages.find(oo, txnDigest);
  if(has_oo){
      auto &ooMsg = oo->second;
      if(!params.signClientProposals) txn = msg.release_txn(); //Only release it here so that we can forward complete P1 message without making any wasteful copies
      AddOngoing(txnDigest, txn);
      if(ooMsg.mode == 1){
        Notice("Handle oo P2");
        HandlePhase2(*ooMsg.remoteCopy, *ooMsg.p2);
        if(!(params.multiThreading || (params.mainThreadDispatching && !params.dispatchMessageReceive))) FreePhase2message(ooMsg.p2);    //If we are not using dispatch. Free msg.
      }
      else if(ooMsg.mode == 2){
        Notice("Handle oo WB");
        HandleWriteback(*ooMsg.remoteCopy, *ooMsg.wb);
        if(!(params.multiThreading || (params.mainThreadDispatching && !params.dispatchMessageReceive))) FreeWBmessage(ooMsg.wb);    //If we are not using dispatch. Free msg.
      }
      else{
        Panic("oo mode must be 1 or 2");
      }
      delete ooMsg.remoteCopy;
      ooMessages.erase(oo);
      return;
  }
  oo.release();

  Debug("PHASE1[%lu:%lu][%s] with ts %lu.", txn->client_id(), txn->client_seq_num(), BytesToHex(txnDigest, 16).c_str(), txn->timestamp().timestamp());
  proto::ConcurrencyControl::Result result;
  const proto::CommittedProof *committedProof = nullptr;
  const proto::Transaction *abstain_conflict = nullptr;

  if(msg.has_crash_failure() && msg.crash_failure()){
    stats.Increment("total_crash_received", 1);
  }
  //KEEP track of interested client //TODO: keep track of original client? 
  //--> Not doing this currently. Instead: We let the original client always do its P1/P2/Writeback processing
  //                                       even if it is possibly redundant. The current code does this for simplicity,
  //                                       but it can be updated in the future
          // interestedClientsMap::accessor i;
          // bool interestedClientsItr = interestedClients.insert(i, txnDigest);
          // i->second.insert(remote.clone());
          // i.release();
          // //interestedClients[txnDigest].insert(remote.clone());

  
  bool isGossip = msg.replica_gossip(); //Check if P1 was forwarded by another replica.

  

// no-replays property, i.e. recover existing decision/result from storage
  //Ignore duplicate requests that are already committed, aborted, or ongoing
  bool process_proposal = false; //Only process_proposal if true
 
  p1MetaDataMap::accessor c; //Note: Only needs to be an accessor to subscribe original.   //p1MetaDataMap::const_accessor c;
      // p1MetaData.insert(c, txnDigest);  //TODO: next: make P1 part of ongoing? same for P2?
  bool hasP1result = p1MetaData.find(c, txnDigest) ? c->second.hasP1 : false; //TODO: instead of "hasP1" could use the insert bool return value

  if(hasP1result && isGossip){  // If P1 has already been received and current message is a gossip one, do nothing. //Do not need to reply to forwarded P1. If adding replica GC -> want to forward it to leader.
    //Inform_P1_GC_Leader(proto::Phase1Reply &reply, proto::Transaction &txn, std::string &txnDigest, int64_t grpLeader);
    Debug("P1 message for txn[%s] received is of type Gossip, and P1 has already been received", BytesToHex(txnDigest, 16).c_str());
    result = proto::ConcurrencyControl::IGNORE;
  } 
  else if(hasP1result){ //&& !isGossip // If P1 has already been received (sent by a fallback) and current message is from original client, then only inform client of blocked dependencies so it can issue fallbacks of its own
    result = c->second.result;
    // need to check if result is WAIT: if so, need to add to waitingDeps original client..
        //(TODO) Instead: use original client list and store pairs <txnDigest, <reqID, remote>>
    if(result == proto::ConcurrencyControl::WAIT){
        c->second.SubscribeOriginal(remote, msg.req_id()); //Subscribe Client in case result is wait (either due to waiting for query, or due to waiting for tx dep) -- subsumes/replaces ManageDependencies subscription
        ManageDependencies(txnDigest, *txn, remote, msg.req_id()); //Request RelayP1 tx in case we are blocking on dependencies
    }
    if (result == proto::ConcurrencyControl::ABORT) {
      committedProof = c->second.conflict;
      UW_ASSERT(committedProof != nullptr);
    }
    Debug("P1 message for txn[%s] received is of type Normal, and P1 has already been received with result %d", BytesToHex(txnDigest, 16).c_str(), result);
  } 
  else if(committed.find(txnDigest) != committed.end()){ //has already committed Txn
      Debug("Already committed txn[%s]. Replying with result %d", BytesToHex(txnDigest, 16).c_str(), 0);
      result = proto::ConcurrencyControl::COMMIT; //TODO: Eventually update to send direct WritebackAck --> Can move this up before p1Meta check --> can delete P1 Meta (currently need to keep it because original expects p1 reply)
  } 
  else if(aborted.find(txnDigest) != aborted.end()){ //has already aborted Txn
      Debug("Already committed txn[%s]. Replying with result %d", BytesToHex(txnDigest, 16).c_str(), 1);
      result = proto::ConcurrencyControl::ABSTAIN;  //TODO: Eventually update to send direct WritebackAck
  } 
  else{ // FIRST P1 request received (i.e. from original client). Gossip if desired and check whether dependencies are valid
    process_proposal = true;
   
  }
  c.release();


  if(process_proposal){
     if(params.replicaGossip) ForwardPhase1(msg); //If params.replicaGossip is enabled then set msg.replica_gossip to true and forward.
    if(!isGossip) msg.set_replica_gossip(false); //unset msg.replica_gossip (which we possibly just set to foward) if the message was received by the client

    //TODO: DispatchTP_noCB(Verify Client Proposals) 
    // Verification calls DispatchTP_main (ProcessProposal.) -- Re-cecheck hasP1 (if used multithread branch)
    
    Debug("P1 message for txn[%s] received is of type Normal, no P1 result exist. Calling ProcessProposal", BytesToHex(txnDigest, 16).c_str());
    ProcessProposal(msg, remote, txn, txnDigest, isGossip); //committedProof, abstain_conflict, result);
  }
  else{ //If we already have result: Send it and free msg/delete txn --- only send result if it is of type != Wait/Ignore (i.e. only send Commit, Abstain, Abort)
      if(result != proto::ConcurrencyControl::WAIT && result != proto::ConcurrencyControl::IGNORE){
        SendPhase1Reply(msg.req_id(), result, committedProof, txnDigest, &remote, abstain_conflict); //TODO: Eventually update to send direct WritebackAck
      } 
      if((params.mainThreadDispatching && (!params.dispatchMessageReceive || params.parallel_CCC)) || (params.multiThreading && params.signClientProposals)) FreePhase1message(&msg);
      if(params.signClientProposals) delete txn;
  }
  return;
  //HandlePhase1CB(&msg, result, committedProof, txnDigest, remote, abstain_conflict, isGossip);
}

//Called after Concurrency Control Check completes
//Sends P1Reply to client. Sends no reply if P1 receives was simply forwarded by another replica.
//TODO: move p1Decision into this function (not sendp1: Then, can unlock here.)
void Server::HandlePhase1CB(uint64_t reqId, proto::ConcurrencyControl::Result result,
  const proto::CommittedProof* &committedProof, std::string &txnDigest, proto::Transaction *txn, const TransportAddress &remote, 
  const proto::Transaction *abstain_conflict, bool isGossip, bool forceMaterialize){



  Debug("Call HandleP1CB for txn[%s][%lu:%lu] with result %d", BytesToHex(txnDigest, 16).c_str(), txn->timestamp().timestamp(), txn->timestamp().id(), result);
  if(result == proto::ConcurrencyControl::IGNORE) return;

  //Note: remote_original might be deleted if P1Meta is erased. In that case, must hold Buffer P1 accessor manually here.  Note: We currently don't delete P1Meta, so it won't happen; but it's still good to have.
  const TransportAddress *remote_original = &remote; 
  uint64_t req_id = reqId;
  bool wake_fallbacks = false;
  p1MetaDataMap::accessor c;
  bool sub_original = BufferP1Result(c, result, committedProof, txnDigest, req_id, remote_original, wake_fallbacks, forceMaterialize, isGossip, 0);
  bool send_reply = (result != proto::ConcurrencyControl::WAIT && !isGossip) || sub_original; //Note: sub_original = true only if originall subbed AND result != wait.
  if(send_reply){ //Send reply to subscribed original client instead.
     Debug("Sending P1Reply for txn [%s] to original client. sub_original=%d, isGossip=%d", BytesToHex(txnDigest, 16).c_str(), sub_original, isGossip);
     SendPhase1Reply(reqId, result, committedProof, txnDigest, remote_original, abstain_conflict);
  }
  c.release();
  //Note: wake_fallbacks only true if result != wait
  if(wake_fallbacks) WakeAllInterestedFallbacks(txnDigest, result, committedProof); //Note: Possibly need to wakeup interested fallbacks here since waking tx from missing query triggers TryPrepare (which returns here). 
 
  //If result is Abstain, and ForceMaterialization was requested => materialize Txn.
  if(forceMaterialize) ForceMaterialization(result, txnDigest, txn);


  // if (result != proto::ConcurrencyControl::WAIT && !isGossip) { //forwarded P1 needs no reply.
  //   //XXX setting client time outs for Fallback
  //   // if(client_starttime.find(txnDigest) == client_starttime.end()){
  //   //   struct timeval tv;
  //   //   gettimeofday(&tv, NULL);
  //   //   uint64_t start_time = (tv.tv_sec*1000000+tv.tv_usec)/1000;  //in miliseconds
  //   //   client_starttime[txnDigest] = start_time;
  //   // }//time(NULL); //TECHNICALLY THIS SHOULD ONLY START FOR THE ORIGINAL CLIENT, i.e. if another client manages to do it first it shouldnt count... Then again, that client must have gotten it somewhere, so the timer technically started.

  //   SendPhase1Reply(reqId, result, committedProof, txnDigest, &remote, abstain_conflict);
  // }
  //if((params.mainThreadDispatching && (!params.dispatchMessageReceive || params.parallel_CCC)) || (params.multiThreading && params.signClientProposals)) FreePhase1message(msg);
}

void Server::SendPhase1Reply(uint64_t reqId, proto::ConcurrencyControl::Result result,
                             const proto::CommittedProof *conflict, const std::string &txnDigest,
                             const TransportAddress *remote, const proto::Transaction *abstain_conflict) {

  Debug("Normal sending P1 result:[%d] for txn: %s", result, BytesToHex(txnDigest, 16).c_str());
  // BufferP1Result(result, conflict, txnDigest);

  proto::Phase1Reply *phase1Reply = GetUnusedPhase1Reply();
  phase1Reply->set_req_id(reqId);
  TransportAddress *remoteCopy = remote->clone();

  // Include Abstain Conflict if present -- Note, does not need to be signed
  // (already has originators client sig) if(result ==
  // proto::ConcurrencyControl::ABSTAIN)
  // *phase1Reply->mutable_abstain_conflict() = dummyTx; //NOTE WARNING: PURELY
  // for testing
  if (abstain_conflict != nullptr) {
    // Panic("setting abstain_conflict");
    // phase1Reply->mutable_abstain_conflict()->set_req_id(0);
    if (params.signClientProposals) {
      p1MetaDataMap::accessor c;
      bool p1MetaExists = p1MetaData.find(c, abstain_conflict->txndigest());
      if (p1MetaExists && c->second.hasSignedP1) { // only send if conflicting tx not yet finished
        phase1Reply->mutable_abstain_conflict()->set_req_id(0);
        *phase1Reply->mutable_abstain_conflict()->mutable_signed_txn() = *c->second.signed_txn;
      }
      c.release();
    } else { // TODO: ideally also check if ongoing still exists, and only send conflict if so.
      phase1Reply->mutable_abstain_conflict()->set_req_id(0);
      *phase1Reply->mutable_abstain_conflict()->mutable_txn() = *abstain_conflict;
    }
    // *phase1Reply->mutable_abstain_conflict() = *abstain_conflict;
  }

  auto sendCB = [remoteCopy, this, phase1Reply]() {
    this->transport->SendMessage(this, *remoteCopy, *phase1Reply);
    FreePhase1Reply(phase1Reply);
    delete remoteCopy;
  };

  phase1Reply->mutable_cc()->set_ccr(result);
  if (params.validateProofs) {
    *phase1Reply->mutable_cc()->mutable_txn_digest() = txnDigest;
    phase1Reply->mutable_cc()->set_involved_group(groupIdx);
    // Set Abort proof or Sign message
    if (result == proto::ConcurrencyControl::ABORT) {
      *phase1Reply->mutable_cc()->mutable_committed_conflict() = *conflict;
    } else if (params.signedMessages) { // Only need to sign reply if voting something else than Abort -- i.e. if there is an Abort Commit Proof there is no need for a replica to sign the
                                        // reply for authentication -- the proof is absolute (it contains a quorum ofsigs).
      proto::ConcurrencyControl *cc = new proto::ConcurrencyControl(phase1Reply->cc());
      // Latency_Start(&signLat);
      Debug("PHASE1[%s] Batching Phase1Reply.", BytesToHex(txnDigest, 16).c_str());

      MessageToSign(cc, phase1Reply->mutable_signed_cc(), [sendCB, cc, txnDigest, this, phase1Reply]() {
            Debug("PHASE1[%s] Sending Phase1Reply with signature %s from priv key %lu.", BytesToHex(txnDigest, 16).c_str(),
                  BytesToHex(phase1Reply->signed_cc().signature(), 100).c_str(),
                  phase1Reply->signed_cc().process_id());

            sendCB();
            delete cc;
          });
      // Latency_End(&signLat);
      return;
    }
  }
  sendCB();
}

////////////////////////// Phase2 Handling

//TODO: ADD AUTHENTICATION IN ORDER TO ENFORCE FB TIMEOUTS. ANYBODY THAT IS NOT THE ORIGINAL CLIENT SHOULD ONLY BE ABLE TO SEND P2FB messages and NOT normal P2!!!!!!
//     if(params.clientAuthenticated){ //TODO: this branch needs to go around the validate Proofs.
//       if(!msg.has_auth_p2dec()){
//         Debug("PHASE2 message not authenticated");
//         return;
//       }
//       proto::Phase2ClientDecision p2cd;
//       p2cd.ParseFromString(msg.auth_p2dec().data());
//       //TODO: the parsing of txn and txn_digest
//
//       if(msg.auth_p2dec().process_id() != txn.client_id()){
//         Debug("PHASE2 message from unauthenticated client");
//         return;
//       }
//       //Todo: Get client key... Currently unimplemented. Clients not authenticated.
//       crypto::Verify(clientPublicKey, &msg.auth_p2dec().data()[0], msg.auth_p2dec().data().length(), &msg.auth_p2dec().signature()[0]);
//
//
//       //TODO: 1) check that message is signed
//       //TODO: 2) check if id matches txn id. Access
//       //TODO: 3) check whether signature validates.
//     }
//     else{
//
//     }

//Handler for Phase2
//Verifies correctness of Phase2 message (quorum signatures of p1 replies) and sends P2 reply
//Dispatches verification to worker threads if multiThreading enabled.
void Server::HandlePhase2(const TransportAddress &remote, proto::Phase2 &msg) {

 //  std::cerr << "Received Phase2 msg with special id: " << msg.req_id() << std::endl;

  const proto::Transaction *txn;
  std::string computedTxnDigest;
  const std::string *txnDigest = &computedTxnDigest;
  if (params.validateProofs) {
    if (!msg.has_txn() && !msg.has_txn_digest()) {
      Debug("PHASE2 message contains neither txn nor txn_digest.");
      return;
    }

    if (msg.has_txn_digest() ) {
      txnDigest = &msg.txn_digest();
    } else {
      txn = &msg.txn();
      computedTxnDigest = TransactionDigest(msg.txn(), params.hashDigest);
      txnDigest = &computedTxnDigest;
      RegisterTxTS(computedTxnDigest, txn);
    }

  }
  else{
    txnDigest = &dummyString;  //just so I can run with validate turned off while using params.mainThreadDispatching
  }

  proto::Phase2Reply* phase2Reply = GetUnusedPhase2Reply();
  TransportAddress *remoteCopy = remote.clone();

  auto sendCB = [this, remoteCopy, phase2Reply, txnDigest]() {
    this->transport->SendMessage(this, *remoteCopy, *phase2Reply);
    Debug("PHASE2[%s] Sent Phase2Reply.", BytesToHex(*txnDigest, 16).c_str());
    //std::cerr << "Send Phase2Reply for special Id: " << phase2Reply->req_id() << std::endl;
    FreePhase2Reply(phase2Reply);
    delete remoteCopy;
  };
  auto cleanCB = [this, remoteCopy, phase2Reply]() {
    FreePhase2Reply(phase2Reply);
    delete remoteCopy;
  };

  phase2Reply->set_req_id(msg.req_id());
  *phase2Reply->mutable_p2_decision()->mutable_txn_digest() = *txnDigest;
  phase2Reply->mutable_p2_decision()->set_involved_group(groupIdx);

// if (!(params.validateProofs && params.signedMessages)){
//         //TransportAddress *remoteCopy2 = remote.clone();
//           phase2Reply->mutable_p2_decision()->set_decision(msg.decision());
//           SendPhase2Reply(&msg, phase2Reply, sendCB);
//           //HandlePhase2CB(remoteCopy2, &msg, txnDigest, sendCB, phase2Reply, cleanCB, (void*) true);
//           return;
//         }

//   // ELSE: i.e. if (params.validateProofs && params.signedMessages)

  // no-replays property, i.e. recover existing decision/result from storage (do this for HandlePhase1 as well.)
  p2MetaDataMap::accessor p;
  p2MetaDatas.insert(p, *txnDigest);
  bool hasP2 = p->second.hasP2;
  //NOTE: If Tx already went P2 or is committed/aborted: Just re-use that decision + view.
  //Since we currently do not garbage collect P2: anything in committed/aborted but not in metaP2 must have gone FastPath,
  //so choosing view=0 is ok, since equivocation/split-decisions cannot be possible.
  if(hasP2){
      //p.release();
      //std::cerr << "Already have P2 for special id: " << msg.req_id() << std::endl;
      phase2Reply->mutable_p2_decision()->set_decision(p->second.p2Decision);
      if (params.validateProofs) {
        phase2Reply->mutable_p2_decision()->set_view(p->second.decision_view);
      }
      //NOTE: Temporary hack fix to subscribe original client to p2 decisions from views > 0;;
      //TODO: Replace with ForwardWriteback eventually
      p->second.has_original = true;
      p->second.original_msg_id = msg.req_id();
      p->second.original_address = remote.clone();
      p.release();
      SendPhase2Reply(&msg, phase2Reply, std::move(sendCB));
      return;
      //TransportAddress *remoteCopy2 = remote.clone();
      //HandlePhase2CB(remoteCopy2, &msg, txnDigest, sendCB, phase2Reply, cleanCB, (void*) true);
  }
  //TODO: Replace both Commit/Abort check with ForwardWriteback at some point. (this should happen before the hasP2 case then.)
  //NOTE: Either approach only works if atomicity of adding to committed/aborted and removing from ongoing is given.
          //Currently, this is that case as HandlePhase2 and WritebackCB are always on the same thread.
  else if(committed.find(*txnDigest) != committed.end()){
      p.release();
      phase2Reply->mutable_p2_decision()->set_decision(proto::COMMIT);
      phase2Reply->mutable_p2_decision()->set_view(0);
      SendPhase2Reply(&msg, phase2Reply, std::move(sendCB));
      return;
  }
  else if(aborted.find(*txnDigest) != aborted.end()){
      p.release();
      phase2Reply->mutable_p2_decision()->set_decision(proto::ABORT);
      phase2Reply->mutable_p2_decision()->set_view(0);
      SendPhase2Reply(&msg, phase2Reply, std::move(sendCB));
      return;
  }
  //first time receiving p2 message:
  else{
      p.release();
      //std::cerr << "First time P2 for special id: " << msg.req_id() << std::endl;
      Debug("PHASE2[%s].", BytesToHex(*txnDigest, 16).c_str());

      int64_t myProcessId;
      proto::ConcurrencyControl::Result myResult;

      if(msg.has_real_equiv() && msg.real_equiv()){
        stats.Increment("total_real_equiv_received_p2", 1);
      }
      if(msg.has_simulated_equiv() && msg.simulated_equiv()){
        //std::cerr<< "phase 2 has simulated equiv with decision: " << msg.decision() << std::endl;
        stats.Increment("total_simul_received_p2", 1);
        myProcessId = -1;
        if(msg.decision() == proto::COMMIT) stats.Increment("total_received_equiv_COMMIT", 1);
        if(msg.decision() == proto::ABORT) stats.Increment("total_received_equiv_ABORT", 1);
      }
      else{
        LookupP1Decision(*txnDigest, myProcessId, myResult);
      }

      if (!(params.validateProofs && params.signedMessages)){
        //TransportAddress *remoteCopy2 = remote.clone();
          phase2Reply->mutable_p2_decision()->set_decision(msg.decision());
          SendPhase2Reply(&msg, phase2Reply, sendCB);
          //HandlePhase2CB(remoteCopy2, &msg, txnDigest, sendCB, phase2Reply, cleanCB, (void*) true);
          return;
        }

  // ELSE: i.e. if (params.validateProofs && params.signedMessages)


       // i.e. if (params.validateProofs && params.signedMessages) {
        if(msg.has_txn_digest()) {
          ongoingMap::const_accessor o;
          auto txnItr = ongoing.find(o, msg.txn_digest());
          if(txnItr){
            txn = o->second.txn;
            o.release();
          }
          else{
         
            o.release();
            if(msg.has_txn()){
              txn = &msg.txn();
              // check that digest and txn match..
               if(*txnDigest !=TransactionDigest(*txn, params.hashDigest)) return;
               RegisterTxTS(*txnDigest, txn);
            }
            else{
              Debug("PHASE2[%s] message does not contain txn, but have not seen txn_digest previously.", BytesToHex(msg.txn_digest(), 16).c_str());

              ooMap::accessor oo;
              ooMessages.insert(oo, msg.txn_digest());
              auto &ooMsg = oo->second;
              if(ooMsg.mode > 0) return; //already have a P2 or WB waiting.
              ooMsg.mode = 1;
              ooMsg.remoteCopy = remoteCopy;
              if(params.multiThreading || (params.mainThreadDispatching && !params.dispatchMessageReceive)){
                ooMsg.p2 = &msg;
              }
              else{
                ooMsg.p2 = new proto::Phase2(msg);
              }
              oo.release();
              return;

              //std::cerr << "Aborting for txn: " << BytesToHex(msg.txn_digest(), 16) << std::endl;
              if(params.multiThreading || (params.mainThreadDispatching && !params.dispatchMessageReceive)){
                  FreePhase2message(&msg); //const_cast<proto::Phase2&>(msg));
              }

              // if(normal.count(msg.txn_digest()) > 0){ std::cerr << "was added on normal P1" << std::endl;}
              // else if(fallback.count(msg.txn_digest()) > 0){ std::cerr << "was added on ExecP1 FB" << std::endl;}
              // else{ std::cerr << "have not seen p1" << std::endl;}
              // waiting.insert(msg.txn_digest());
              auto txnDig = *txnDigest;
              transport->Timer(1000, [this, txnDig](){
                 Warning("Checking whether P1 has arrived since.");
                 ongoingMap::const_accessor o;
                 bool hasOngoing = ongoing.find(o, txnDig);
                 o.release();
                 bool hadOngoing = ongoingErased.count(txnDig);
                 if(!hasOngoing && !hadOngoing) Panic("Received P1 after P2."); //If this doesn't print = we never received.
              });
              transport->Timer(2000, [this](){
                Panic("Had no txn at time of P2.");
              });
              //Panic("Cannot validate p2 because server does not have tx for this reqId");

              bool had_ongoing = ongoingErased.count(txnDig);
              Warning("Cannot validate p2 because server does not have tx for this reqId. Had Ongoing erased? %d", had_ongoing);
              //Panic("This should not be happening with TCP");
              //std::cerr << "Cannot validate p2 because do not have tx for special id: " << msg.req_id() << std::endl;
              return;
            }
          }
        }

        ManagePhase2Validation(remote, msg, txnDigest, txn, sendCB, phase2Reply, cleanCB, myProcessId, myResult);
  }
}

//Sends a signed P2 reply to the client, containing Commit/Abort respectively. 
//Echos the decision carried in P2 if none buffered, and returns previously buffered decision otherwise
void Server::HandlePhase2CB(TransportAddress *remote, proto::Phase2 *msg, const std::string* txnDigest,
                            signedCallback sendCB, proto::Phase2Reply* phase2Reply, cleanCallback cleanCB, void* valid)
{

  Debug("HandlePhase2CB invoked");

  if(msg->has_simulated_equiv() && msg->simulated_equiv()){
    valid = (void*) true; //Code to simulate equivocation even when necessary equiv signatures do not exist.
    if(msg->decision() == proto::COMMIT) stats.Increment("total_equiv_COMMIT", 1);
    if(msg->decision() == proto::ABORT) stats.Increment("total_equiv_ABORT", 1);
  }

  if(!valid){
    stats.Increment("total_p2_invalid", 1);
    Debug("VALIDATE P1Replies for TX %s failed.", BytesToHex(*txnDigest, 16).c_str());
    cleanCB(); //deletes SendCB resources should SendCB not be called.
    if(params.multiThreading || (params.mainThreadDispatching && !params.dispatchMessageReceive)){
      FreePhase2message(msg); //const_cast<proto::Phase2&>(msg));
    }
    delete remote;
    Panic("P2 verification was invalid -- this should never happen?");
    return;
  }

  Debug("Phase2CB: [%s]", BytesToHex(*txnDigest, 16).c_str());

  auto f = [this, remote, msg, txnDigest, sendCB = std::move(sendCB), phase2Reply, cleanCB = std::move(cleanCB), valid ]() mutable {

    p2MetaDataMap::accessor p;
    p2MetaDatas.insert(p, *txnDigest);
    bool hasP2 = p->second.hasP2;
    if(hasP2){
      phase2Reply->mutable_p2_decision()->set_decision(p->second.p2Decision);
      //std::cerr << "Already had P2 from FB. Setting P2 Decision for specialal id: " << phase2Reply->req_id() << std::endl;
    }
    else{
      p->second.p2Decision = msg->decision();
      p->second.hasP2 = true;
      phase2Reply->mutable_p2_decision()->set_decision(msg->decision());
      //std::cerr << "First time: Setting P2 Decision for speical id: " << phase2Reply->req_id() << std::endl;
    }

    if (params.validateProofs) {
      phase2Reply->mutable_p2_decision()->set_view(p->second.decision_view);
    }
    //NOTE: Temporary hack fix to subscribe original client to p2 decisions from views > 0
    p->second.has_original = true;
    p->second.original_msg_id = msg->req_id();
    p->second.original_address = remote;
    p.release();

    //XXX start client timeout for Fallback relay
    // if(client_starttime.find(*txnDigest) == client_starttime.end()){
    //   struct timeval tv;
    //   gettimeofday(&tv, NULL);
    //   uint64_t start_time HandlePhase2CB= (tv.tv_sec*1000000+tv.tv_usec)/1000;  //in miliseconds
    //   client_starttime[*txnDigest] = start_time;
    // }

    SendPhase2Reply(msg, phase2Reply, sendCB);
    //return;
    return (void*) true;
 };

 if(params.mainThreadDispatching && params.dispatchCallbacks){
   transport->DispatchTP_main(std::move(f));
 }
 else{
   f();
 }
}

//Dispatch Message signing and sending to a worker thread
void Server::SendPhase2Reply(proto::Phase2 *msg, proto::Phase2Reply *phase2Reply, signedCallback sendCB){

  if (params.validateProofs && params.signedMessages) {
    proto::Phase2Decision* p2Decision = new proto::Phase2Decision(phase2Reply->p2_decision());

    MessageToSign(p2Decision, phase2Reply->mutable_signed_p2_decision(),
      [sendCB, p2Decision, msg, this]() {
        sendCB();
        delete p2Decision;
        //Free allocated memory
        if(params.multiThreading || params.mainThreadDispatching){
          FreePhase2message(msg); // const_cast<proto::Phase2&>(msg));
        }
      });
    return;
  }
  else{
    sendCB();
    //Free allocated memory
    if(params.multiThreading || params.mainThreadDispatching){
      FreePhase2message(msg); // const_cast<proto::Phase2&>(msg));
    }
    return;
  }
}

/////////////////////////////// Writeback Handling

//Handler for Writeback Message
//Verifies correctness of request (quorum of P1replies or P2 replies depending on Fast/Slow Path)
//Dispatches verification to worker threads if multiThreading enabled
void Server::HandleWriteback(const TransportAddress &remote,
                             proto::Writeback &msg) {

  stats.Increment("total_writeback_received", 1);
  // simulating failures in local experiment
  //  fail_writeback++;
  //  if(fail_writeback %2 == 1){
  //    return;
  //  }

  proto::Transaction *txn;
  const std::string *txnDigest;
  std::string computedTxnDigest;
  if (!msg.has_txn() && !msg.has_txn_digest()) {
    Panic("WRITEBACK message contains neither txn nor txn_digest.");
    return WritebackCallback(&msg, txnDigest, txn, (void *)false);
  }

  if (msg.has_txn_digest()) {
    txnDigest = &msg.txn_digest();
    Debug("WRITEBACK[%s] received with decision %d.", BytesToHex(*txnDigest, 16).c_str(), msg.decision());

    if(committed.find(*txnDigest) != committed.end() || aborted.find(*txnDigest) != aborted.end()){
      if(params.multiThreading || (params.mainThreadDispatching && !params.dispatchMessageReceive)){
        Debug("Calling Re-CLEAN on Writeback for Txn[%s]", BytesToHex(*txnDigest, 16).c_str());
        Clean(*txnDigest); //XXX Clean again since client could have added it back to ongoing...
        FreeWBmessage(&msg);
      }
      return;  //TODO: Forward to all interested clients and empty it?
    }

    ongoingMap::const_accessor o;
    bool hasTxn = ongoing.find(o, msg.txn_digest());
    if(hasTxn){
      txn = o->second.txn;
      o.release();
    }
    else{
      o.release(); //Note, can remove this: o.empty() = true
      if(msg.has_txn()){
        txn = msg.release_txn();
        // check that digest and txn match..
         if(*txnDigest !=TransactionDigest(*txn, params.hashDigest)) return;
        
        RegisterTxTS(*txnDigest, txn);
      }
      else{
        //No longer ongoing => was committed/aborted on a concurrent thread
        Debug("Writeback[%s] message does not contain txn, but have not seen"
            " txn_digest previously.", BytesToHex(msg.txn_digest(), 16).c_str());
        Warning("Cannot process Writeback because ongoing does not contain tx for this request. Should not happen with TCP.... CPU %d", sched_getcpu());

        if(simulate_inconsistency){
          Panic("Cannot process Writeback because ongoing does not contain tx for this request. Should not happen with TCP.... CPU %d", sched_getcpu());
          return; // this mode not supported with inconsistency simulation because we pass dummy remote.
        }

         ooMap::accessor oo;
        ooMessages.insert(oo, msg.txn_digest());
        auto &ooMsg = oo->second;
        if(ooMsg.mode == 2) return; //already have a WB waiting.
        if(ooMsg.mode == 1){
          //don't write new remote. Free old.
            FreePhase2message(ooMsg.p2); 
        }
        else{
           ooMsg.remoteCopy = remote.clone();
        }
        ooMsg.mode = 2;
       
        if(params.multiThreading || (params.mainThreadDispatching && !params.dispatchMessageReceive)){
          ooMsg.wb = &msg;
        }
        else{
          ooMsg.wb = new proto::Writeback(msg);
        }
        oo.release();
        return;


        Panic("When using TCP the tx should always be ongoing before doing WB");
        if(params.multiThreading || (params.mainThreadDispatching && !params.dispatchMessageReceive)){
          FreeWBmessage(&msg);
        }
        return;
        //return WritebackCallback(&msg, txnDigest, txn, (void*) false);
      }
    }
  } else {
    UW_ASSERT(msg.has_txn());
    txn = msg.release_txn();
    computedTxnDigest = TransactionDigest(*txn, params.hashDigest);
    txnDigest = &computedTxnDigest;
    RegisterTxTS(*txnDigest, txn);

    if(committed.find(*txnDigest) != committed.end() || aborted.find(*txnDigest) != aborted.end()){
      if(params.multiThreading || (params.mainThreadDispatching && !params.dispatchMessageReceive)){
        Clean(*txnDigest); //XXX Clean again since client could have added it back to ongoing...
        FreeWBmessage(&msg);
        delete txn;
      }
      return;  //TODO: Forward to all interested clients and empty it?
    }
  }

  if(simulate_inconsistency){
    //Occasionally drop some of the prepare/commit and rely on sync.. 
    uint64_t target_replica = txn->client_id() % config.n; //find "lead replica"
    bool drop_at_this_replica = false;
    for(int i = 0; i < 2; ++i){ //Drop at 2 replicas.
       if((target_replica + i) % config.n == idx) drop_at_this_replica = true;
    }
    // drop_at_this_replica &= msg.decision() == proto::COMMIT;
    // Notice("Target replica: %d. Drop on this replica[%d]? %d.", target_replica, idx, drop_at_this_replica);
    if(drop_at_this_replica){ //only drop commit.

      // auto [itr, first] = dropped_c.insert(*txnDigest); 
      // if(first){ //only drop initial. Allow via sync or fallback
      //   Debug("Dropping Commit of Txn[%s]. From client: %d", BytesToHex(*txnDigest, 16).c_str(), txn->client_id());
      //   stats.Increment("dropped_c_" + std::to_string(idx));
      //   return;
      // }

      //Note: Rather than "dropping" commit, we will delay it until sync has happened
      droppedMap::accessor d;
      bool first = dropped_c.insert(d, *txnDigest);
      if(first){
       
        bool already_simul_sync = supplied_sync.find(*txnDigest) != supplied_sync.end(); //if simul already happened. Then just apply.

        if(!already_simul_sync && msg.decision() == proto::COMMIT){

         proto::Writeback *wb_copy;
          if(params.multiThreading || (params.mainThreadDispatching && !params.dispatchMessageReceive)){
            wb_copy = &msg; //already allocated
          }
          else{
            wb_copy = new proto::Writeback(msg);
          }
          d->second = wb_copy;
          Debug("Dropping Commit of Txn[%s]. From client: %d", BytesToHex(*txnDigest, 16).c_str(), txn->client_id());
          stats.Increment("dropped_c_" + std::to_string(idx));
          return;
        }
        else{ //otherwise, mark Tx as dropped, but continue anyways.
          d->second = nullptr;
           //continue applying.
           d.release();
        }
      }
    }
  }


  Debug("WRITEBACK[%s] with decision %d. Begin Validation", BytesToHex(*txnDigest, 16).c_str(), msg.decision());

  //Verify Writeback Proofs + Call WritebackCallback
  ManageWritebackValidation(msg, txnDigest, txn);
}

// Called after Writeback request has been verified
// Updates committed/aborted datastructures and key-value store accordingly
// Garbage collects ongoing meta-data
void Server::WritebackCallback(proto::Writeback *msg, const std::string *txnDigest, proto::Transaction *txn, void *valid) {

  Debug("Writeback Txn[%s][%lu:%lu]", BytesToHex(*txnDigest, 16).c_str(), txn->timestamp().timestamp(), txn->timestamp().id());

  if(!valid){
    Panic("Writeback Validation should not fail for TX %s ", BytesToHex(*txnDigest, 16).c_str());
    Debug("VALIDATE Writeback for TX %s failed.", BytesToHex(*txnDigest, 16).c_str());
    if(params.multiThreading || (params.mainThreadDispatching && !params.dispatchMessageReceive)){
      FreeWBmessage(msg);
    }
    return;
  }

  auto f = [this, msg, txnDigest, txn, valid]() mutable {
      Debug("WRITEBACK Callback[%s] being called", BytesToHex(*txnDigest, 16).c_str());

      ///////////////////////////// Below: Only executed by MainThread
      // tbb::concurrent_hash_map<std::string, std::mutex>::accessor z;
      // completing.insert(z, *txnDigest);
      // //z->second.lock();
      // z.release();
      if(committed.find(*txnDigest) != committed.end() || aborted.find(*txnDigest) != aborted.end()){
        //duplicate, do nothing. TODO: Forward to all interested clients and empty it?
      }
      else if (msg->decision() == proto::COMMIT) {
        stats.Increment("total_transactions", 1);
        stats.Increment("total_transactions_commit", 1);
        Debug("WRITEBACK[%s] successfully committing.", BytesToHex(*txnDigest, 16).c_str());
        bool p1Sigs = msg->has_p1_sigs();
        uint64_t view = -1;
        if(!p1Sigs){
          if(msg->has_p2_sigs() && msg->has_p2_view()){
            view = msg->p2_view();
          }
          else{
            Debug("Writeback for P2 does not have view or sigs");
            //return; //XXX comment back
            view = 0;
            //return (void*) false;
          }
        }
        Debug("COMMIT ONLY RUN BY MAINTHREAD: %d", sched_getcpu());
        Commit(*txnDigest, txn, p1Sigs ? msg->release_p1_sigs() : msg->release_p2_sigs(), p1Sigs, view);
      } else {
        stats.Increment("total_transactions", 1);
        stats.Increment("total_transactions_abort", 1);
        Debug("WRITEBACK[%s] successfully aborting.", BytesToHex(*txnDigest, 16).c_str());
        //msg->set_allocated_txn(txn); //dont need to set since client will?   
        *msg->mutable_txn() = *txn; //Copy Txn back into msg => This might be necessary in case a replica syncs on an aborted Txn that it hasn't seen before. //TODO: can we use set_allocated? Or is that not threadsafe

        writebackMessages.insert(std::make_pair(*txnDigest, *msg)); //Note: Could move data item and create a copy of txnDigest and Timestamp to call Abort instead. (writebackMessages insert has to happen before Abort insert)
        //writebackMessages[*txnDigest] = *msg;  //Only necessary for fallback... (could avoid storing these, if one just replied with a p2 vote instead - but that is not as responsive)
        ///CAUTION: msg might no longer hold txn; could have been released in HanldeWriteback

        Abort(*txnDigest, txn);
        //delete txn; //See Clean(), currently unsafe to delete txn due to multithreading.
      }

      if(params.multiThreading || params.mainThreadDispatching){
        FreeWBmessage(msg);
      }
      //return;
      return (void*) true;
  };

 if(params.mainThreadDispatching && params.dispatchCallbacks){ 
   transport->DispatchTP_main(std::move(f));
 }
 else{
   f();
 }
}

//Clients may abort their own Tx before Preparing: This removes the RTS and may be used in case we decide to store any in-execution meta data: E.g. Queries.
void Server::HandleAbort(const TransportAddress &remote,
    const proto::Abort &msg) {
  
  Debug("HandleAbort");
  const proto::AbortInternal *abort;
  if (params.validateProofs && params.signedMessages && params.signClientProposals) {
    if (!msg.has_signed_internal()) {
      return;
    }

    Latency_Start(&verifyLat);
    if (!client_verifier->Verify(keyManager->GetPublicKey(keyManager->GetClientKeyId(msg.signed_internal().process_id())),
          msg.signed_internal().data(),
          msg.signed_internal().signature())) {
      //Latency_End(&verifyLat);
      return;
    }
    Latency_End(&verifyLat);

    if (!abortInternal.ParseFromString(msg.signed_internal().data())) {
      return;
    }

    if (abortInternal.ts().id() != msg.signed_internal().process_id()) {
      return;
    }

    abort = &abortInternal;
  } else {
    UW_ASSERT(msg.has_internal());
    abort = &msg.internal();
  }


  ////Garbage collect Read Timestamps

  //RECOMMENT XXX currently displaced by RTS implementation that has no set, but only a single RTS version that keeps getting replaced.
  //  if(params.mainThreadDispatching) rtsMutex.lock();
  // for (const auto &read : abort->read_set()) {
  //   rts[read].erase(abort->ts());
  // }
  //  if(params.mainThreadDispatching) rtsMutex.unlock();
  ClearRTS(abort->read_set(), abort->ts());
  
  //Garbage collect Queries.
  for (const auto &query_id: abort->query_ids()){
    queryMetaDataMap::accessor q;
    if(queryMetaData.find(q, query_id)){
      //erase current retry version from missing (Note: all previous ones must have been deleted via ClearMetaData)

      // auto query_retry_id = QueryRetryId(query_id, q->second->retry_version, (params.query_params.signClientQueries && params.query_params.cacheReadSet && params.hashDigest));
      // queryMissingTxnsMap::accessor qm;
      // if(queryMissingTxns.find(qm, query_retry_id)){
      //    queryMissingTxns.erase(qm);
      // }
      queryMissingTxns.erase(QueryRetryId(query_id, q->second->retry_version, (params.query_params.signClientQueries && params.query_params.cacheReadSet && params.hashDigest)));
     
      QueryMetaData *query_md = q->second;
      ClearRTS(query_md->queryResultReply->result().query_read_set().read_set(), query_md->ts);

      if(query_md != nullptr) delete query_md;
      //erase query_md
      queryMetaData.erase(q); 
    }
    q.release();

    //Delete any possibly subscribed queries.
    // subscribedQueryMap::accessor sq;
    // if(subscribedQuery.find(sq, query_id)){
    //   subscribedQuery.erase(sq);
    // }
    subscribedQuery.erase(query_id);

    Debug("Removed query: %s", BytesToHex(query_id, 16).c_str());
  }

  if(params.multiThreading || (params.mainThreadDispatching && !params.dispatchMessageReceive))  FreeAbortMessage(&msg);

}

 
/////////////////////////////////////// PREPARE, COMMIT AND ABORT LOGIC  + Cleanup

void Server::Prepare(const std::string &txnDigest, const proto::Transaction &txn, const ReadSet &readSet) {

  // return; //Test perf if we don't simulate but we don't prepare.
  if(simulate_inconsistency){
    //Occasionally drop some of the prepare/commit and rely on sync.. Drop at 2 replicas.
    uint64_t target_replica = txn.client_id() % config.n; //find "lead replica"
    bool drop_at_this_replica = false;
    for(int i = 0; i < 2; ++i){ //Drop at 2 replicas.
       if((target_replica + i) % config.n == idx) drop_at_this_replica = true;
    }
    // Notice("Target replica: %d. Drop on this replica[%d]? %d", target_replica, idx, drop_at_this_replica);
    if(drop_at_this_replica){
      auto [_, first] = dropped_p.insert(txnDigest);
      if(first){ //only drop initial. Allow via sync or fallback
        Debug("Dropping Prepare of Txn[%s]", BytesToHex(txnDigest, 16).c_str());
        stats.Increment("dropped_p_" + std::to_string(idx));
        return;
      }
    }
  }


  Debug("PREPARE[%s] agreed to commit with ts %lu.%lu.",BytesToHex(txnDigest, 16).c_str(), txn.timestamp().timestamp(), txn.timestamp().id());
  
  Timestamp ts = Timestamp(txn.timestamp());

  //const ReadSet &readSet = txn.read_set();
  const WriteSet &writeSet = txn.write_set();
  
  ongoingMap::const_accessor o;
  auto ongoingItr = ongoing.find(o, txnDigest);
  if(!ongoingItr){
  //if(ongoingItr == ongoing.end()){
    Debug("Already concurrently Committed/Aborted txn[%s]", BytesToHex(txnDigest, 16).c_str());
    return;
  }
  proto::Transaction *ongoingTxn = o->second.txn; //const
  //const proto::Transaction *ongoingTxn = ongoing.at(txnDigest);

  UW_ASSERT(ongoingTxn->has_txndigest());
  if(!ongoingTxn->has_txndigest()) *ongoingTxn->mutable_txndigest() = txnDigest; //Hack to have access to txnDigest inside TXN later (used for abstain conflict, and for FindTableVersion)

  preparedMap::accessor a;
  bool first_prepare = prepared.insert(a, std::make_pair(txnDigest, std::make_pair(ts, ongoingTxn)));

  if(!first_prepare) return; //Already inserted all Read/Write Sets.

  // Debug("PREPARE: TESTING MERGED READ");
  // for(auto &read : readSet){
  //     Debug("[group Merged] Read key %s with version [%lu:%lu]", read.key().c_str(), read.readtime().timestamp(), read.readtime().id());
  // }


  for (const auto &read : readSet) {
    bool table_v = read.has_is_table_col_version() && read.is_table_col_version(); //don't need to record reads to TableVersion (not checked for normal cc)
    //Notice("prepare reads: %s", read.key().c_str());
    if (!table_v && IsKeyOwned(read.key())) { 
      //preparedReads[read.key()].insert(p.first->second.second);
      //preparedReads[read.key()].insert(a->second.second);

      std::pair<std::shared_mutex, std::set<const proto::Transaction *>> &y = preparedReads[read.key()];
      std::unique_lock lock(y.first);
      y.second.insert(a->second.second);

      // if(params.rtsMode == 2){ //remove RTS now that its prepared? --> Kind of pointless, since prepare fulfills the same thing.
      //   std::pair<std::shared_mutex, std::set<Timestamp>> &rts_set = rts_list[read.key()];
      //   {
      //     std::unique_lock lock(rts_set.first);
      //     rts_set.second.erase(txn.timestamp());
      //   }
      // }
    }
  }

  std::pair<Timestamp, const proto::Transaction *> pWrite = std::make_pair(a->second.first, a->second.second);
  a.release();
    //std::make_pair(p.first->second.first, p.first->second.second);

  std::vector<const std::string*> table_and_col_versions; 

  for (const auto &write : writeSet) {
      //Skip applying TableVersion until after TableWrites have been applied; Same for TableColVersions. Currenty those are both marked as delay.
       //Note: Delay flag set by client is not BFT robust -- server has to infer on it's own. Ok for current prototype. //TODO: turn into parsing at some point
    if(write.has_is_table_col_version() && write.is_table_col_version()){
      //table_and_col_versions.push_back(&write.key());
      continue;
    }   
      //Also need to do this for TableColVersions...  //write them aside in a little map (just keep a pointer ref to the position)
      //Currently rely on unique_delim to figure out it is a TableColVersion. Ideally check that prefix is table_name too.
      // size_t pos;
      // if((pos = write.key().find(unique_delimiter)) != std::string::npos){  //FIXME: This currently filters out ALL keys
      //   table_and_col_versions.push_back(&write.key());
      //   continue;
      // }
      //Better solution: Mark keys as skip inside the write itself? That way client controls what gets skipped. 
      //Not really BFT, but it simplifies. Otherwise have to go and consult the TableRegistry...
      //Attack: Byz client applies version after tablewrite..Causes honest Tx to not respect safety.
     //Notice("prepare write: %s", read.key().c_str());
    if (IsKeyOwned(write.key())) { 

      std::pair<std::shared_mutex,std::map<Timestamp, const proto::Transaction *>> &x = preparedWrites[write.key()];
      std::unique_lock lock(x.first);
      x.second.insert(pWrite);
      // std::unique_lock lock(preparedWrites[write.key()].first);
      // preparedWrites[write.key()].second.insert(pWrite);

      //TODO: Insert into table as well.
    }

    //Notice("Preparing key:[%s]. Ts[%lu:%lu]", write.key().c_str(), txn.timestamp().timestamp(), txn.timestamp().id());
  }

  //TODO: Improve Precision for Multi-shard TXs.
  // 1. In Prepare/Commit: Only write Table version if there is a write to THIS shard.
  // 2. In RegisterWrites (for semanticCC): only register if there is a write to THIS shard. 
  //   => This minimizes unecessary CC work. (CC-checks in which no write is owned)
     
  //Solution:
  // Loop through TableWrites instead of looping through Write set
  // Lookup associated write set key.
  /* 
  for (const auto &[table_name, table_write] : txn.table_writes()){
    bool write_table_version = false; //only write TableVersion if one key is owned. 
    //for each row
      for(auto &row: table_write.rows()){
      
        const std::string &write_key = GetEncodedRow(txn, row, table_name);

        // if write does not apply to this shard, continue.
        if (!IsKeyOwned(write_key)) continue;
        write_table_version = true;

        std::pair<std::shared_mutex,std::map<Timestamp, const proto::Transaction *>> &x = preparedWrites[write_key];
        std::unique_lock lock(x.first);
        x.second.insert(pWrite);

      }
    if(write_table_version) table_and_col_versions.push_back(&table_name);
    else {
      //TODO: Mark Table as not locally relevant. NOT threadsafe if we write to TXN itself.
      //TODO: Could keep a "list" of relevant TableWrites, and pass this to RecordReadPrepdicatesAndWrites
    }
  }
  */



  if(params.query_params.useSemanticCC){
    RecordReadPredicatesAndWrites(*ongoingTxn, ts, false);
  }
  //Note: Now that Writes/Reads and Predicates have been prepared it is safe to release the TableVersion locks. any conflicting TX must observe this one!
  //      Applying the TableWrite state (+TableVersion) can be done asynchronously. 
          //Note: TableVersion should only be set after all writes to a given Table are complete so that it encompasses all the writes
          //note again: this is just the "readable" table version; the table version for conflict checks is already stored in RecordReadPredicatesAndWrites.

  o.release(); //Relase only at the end, so that Prepare and Clean in parallel for the same TX are atomic.

  Debug("Prepare Txn[%s][%lu:%lu]", BytesToHex(txnDigest, 16).c_str(), ts.getTimestamp(), ts.getID());

  if(disable_prepare_visibility) return; //Don't apply Writes.

  if(ASYNC_WRITES){
    auto f = [this, ongoingTxn, ts, txnDigest, pWrite](){   //not very safe: Need to rely on fact that ongoingTxn won't be deleted => maybe make a copy?
      UW_ASSERT(ongoingTxn);
    
      std::vector<std::string> locally_relevant_table_changes = ApplyTableWrites(*ongoingTxn, ts, txnDigest, nullptr, false);
    
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
  }
  else{
    std::vector<std::string> locally_relevant_table_changes = ApplyTableWrites(*ongoingTxn, ts, txnDigest, nullptr, false);

    // for (const auto &[table_name, table_write] : txn.table_writes()){
    //   ApplyTableWrites(table_name, table_write, ts, txnDigest, nullptr, false);
    //   //Apply TableVersion  ==> currently moved below
    //   // std::pair<std::shared_mutex,std::map<Timestamp, const proto::Transaction *>> &x = preparedWrites[table_name];
    //   // std::unique_lock lock(x.first);
    //   // x.second.insert(pWrite);
    // }

    //Apply TableVersion and TableColVersion 
    //TODO: for max efficiency (minimal wait time to update): Set_change table in table_write, and write TableVersion as soon as TableWrite has been applied
                                                              //Do the same for Table_Col_Version. TODO: this requires parsing out the table_name however.
    // for(auto table_or_col_version: table_and_col_versions){   
    //   Debug("Preparing TableVersion or TableColVersion: %s with TS: [%lu:%lu]", (*table_or_col_version).c_str(), ts.getTimestamp(), ts.getID());
    //   std::pair<std::shared_mutex,std::map<Timestamp, const proto::Transaction *>> &x = preparedWrites[*table_or_col_version];
    //   std::unique_lock lock(x.first);
    //   x.second.insert(pWrite);
    // }
    for(auto table: locally_relevant_table_changes){   //TODO: Ideally also only update Writes in RecordReadPredicatesAndWrites for the relevant table changes
      Debug("Preparing TableVersion or TableColVersion: %s with TS: [%lu:%lu]", table.c_str(), ts.getTimestamp(), ts.getID());
      std::pair<std::shared_mutex,std::map<Timestamp, const proto::Transaction *>> &x = preparedWrites[std::move(table)];
      std::unique_lock lock(x.first);
      x.second.insert(pWrite);
    }
  }
}

void Server::Commit(const std::string &txnDigest, proto::Transaction *txn,
      proto::GroupedSignatures *groupedSigs, bool p1Sigs, uint64_t view) {

      //TESTING HOW LONG THIS TAKES: FIXME: REMOVE 
  // struct timespec ts_start;
  // clock_gettime(CLOCK_MONOTONIC, &ts_start);
  // uint64_t microseconds_start = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;
  
  proto::CommittedProof *proof = nullptr;
  if (params.validateProofs) {
    Debug("Access only by CPU: %d", sched_getcpu());
    proof = new proto::CommittedProof();
    //proof = testing_committed_proof.back();
    //testing_committed_proof.pop_back();
  }

  Timestamp ts(txn->timestamp());

  if (params.validateProofs) {
    // CAUTION: we no longer own txn pointer (which we allocated during Phase1  and stored in ongoing)
    proof->set_allocated_txn(txn); //Note: This appears to only be safe because any request that may want to use txn from ongoing (Phase1, Phase2, FB, Writeback) are all on mainThread => will all short-circuit if already committed
    if (params.signedMessages) {
      if (p1Sigs) {
        proof->set_allocated_p1_sigs(groupedSigs);
      } else {
        proof->set_allocated_p2_sigs(groupedSigs);
        proof->set_p2_view(view);
        //view = groupedSigs.begin()->second.sigs().begin()
      }
    }
  }

  Value val;
  if (txn->policy_type() == proto::Transaction::NONE) {
    val.proof = proof;
  }
  else {
    val.policyProof = proof;
  }

  auto [committedItr, first_commit] = committed.insert(std::make_pair(txnDigest, proof));
  Debug("Inserted txn %s into Committed on CPU %d",BytesToHex(txnDigest, 16).c_str(), sched_getcpu());
  //auto committedItr =committed.emplace(txnDigest, proof);
  
  if(!first_commit) return;// already was inserted

  proto::Transaction* txn_ref = params.validateProofs? proof->mutable_txn() : txn;

  CommitToStore(proof, txn_ref, txnDigest, ts, val);

  Debug("Calling CLEAN for committing txn[%s]", BytesToHex(txnDigest, 16).c_str());
  Clean(txnDigest);
  CheckDependents(txnDigest);
  CleanDependencies(txnDigest);

  CleanQueries(txn_ref);
  //CheckWaitingQueries(txnDigest, txn->timestamp().timestamp(), txn->timestamp().id()); //Now waking after applyTablewrite

  // clock_gettime(CLOCK_MONOTONIC, &ts_start);
  // uint64_t microseconds_end = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;
  // if(ts.getID() % 5) Notice("[CPU: %d. Commit time: %d us", sched_getcpu(), microseconds_end - microseconds_start);
}

//Note: This might be called on a different thread than mainthread. Thus insertion into committed + Clean are concurrent. Safe because proof comes with its own owned tx, which is not used by any other thread.
void Server::CommitWithProof(const std::string &txnDigest, proto::CommittedProof *proof){ //Called for Replica To Replica Sync

    proto::Transaction *txn = proof->mutable_txn();
    //std::string txnDigest(TransactionDigest(*txn, params.hashDigest));

    Timestamp ts(txn->timestamp());

    Value val;
    if (txn->policy_type() == proto::Transaction::NONE) {
      val.proof = proof;
    }
    else {
      val.policyProof = proof;
    }

    // committed.insert(std::make_pair(txnDigest, proof)); //Note: This may override an existing commit proof -- that's fine.
    auto [committedItr, first_commit] = committed.insert(std::make_pair(txnDigest, proof)); //Note: This may override an existing commit proof -- that's fine.
    Debug("Inserted txn %s into Committed on CPU %d",BytesToHex(txnDigest, 16).c_str(), sched_getcpu());
    if(!first_commit) return;// already was inserted


    CommitToStore(proof, txn, txnDigest, ts, val);

    Debug("Calling CLEAN for committing txn[%s]", BytesToHex(txnDigest, 16).c_str());
    Clean(txnDigest);
    CheckDependents(txnDigest);
    CleanDependencies(txnDigest);

    CleanQueries(proof->mutable_txn()); //Note: Changing txn is not threadsafe per se, but should not cause any issues..
    //CheckWaitingQueries(txnDigest, txn->timestamp().timestamp(), txn->timestamp().id());  //Now waking after applyTablewrite
}


void Server::UpdateCommittedReads(proto::Transaction *txn, const std::string &txnDigest, Timestamp &ts, proto::CommittedProof *proof) {

  Debug("Update Committed Reads for txn %s", BytesToHex(txnDigest, 16).c_str());
  const ReadSet *readSet = &txn->read_set(); // DEFAULT
  const DepSet *depSet = &txn->deps();       // DEFAULT
  const PredSet *predSet = &txn->read_predicates(); //DEFAULT

  proto::ConcurrencyControl::Result res = mergeTxReadSets(readSet, depSet, predSet, *txn, txnDigest, proof);
  
  Debug("was able to pull read set from cache? res: %d", res);
  // Note: use whatever readSet is returned -- if query read sets are not correct/present just use base (it's safe: another replica would've had all)
  // I.e.: If the TX got enough commit votes (3f+1), then at least 2f+1 correct replicas must have had the correct readSet. Those suffice for safety
  // conflicts. this replica will STILL apply the TableWrites, so visibility isn't impeded.

  // Once subscription on queries wakes up, it will call UpdatecommittedReads again, at which point mergeReadSet will return the full mergedReadSet
  // TODO: For eventually consistent state may want to explicitly sync on the waiting queries -- not necessary for safety though (see above)
  // TODO: FIXME: Currently, we ignore processing queries whose Tx have already committed. Consequently, UpdateCommitted won't be called. This is safe, but
  // might result in this replica unecessarily preparing conflicting tx that are doomed to abort (because committedreads is not set)

  // Debug("COMMIT: TESTING MERGED READ");
  // for(auto &read : *readSet){
  //     Debug("[group Merged] Read key %s with version [%lu:%lu]", read.key().c_str(), read.readtime().timestamp(), read.readtime().id());
  // }

  for (const auto &read : *readSet) {
     bool table_v = read.has_is_table_col_version() && read.is_table_col_version(); //Don't need to record read table versions, not checked by normal cc
     // Notice("committed reads: %s", read.key().c_str());
    if (table_v || !IsKeyOwned(read.key())) {
      continue;
    }
    // store.commitGet(read.key(), read.readtime(), ts);   //SEEMINGLY NEVER
    // USED XXX commitGet_count++;

    // Latency_Start(&committedReadInsertLat);

    std::pair<std::shared_mutex, std::set<committedRead>> &z = committedReads[read.key()];
    std::unique_lock lock(z.first);
    z.second.insert(std::make_tuple(ts, read.readtime(), proof));
    // committedReads[read.key()].insert(std::make_tuple(ts, read.readtime(),
    //       committedItr.first->second));

    // uint64_t ns = Latency_End(&committedReadInsertLat);
    // stats.Add("committed_read_insert_lat_" + BytesToHex(read.key(), 18), ns);
  }
}

void Server::CommitToStore(proto::CommittedProof *proof, proto::Transaction *txn, const std::string &txnDigest, Timestamp &ts, Value &val){
  std::vector<const std::string*> table_and_col_versions; 

  Debug("Commit Tx[%s] Ts[%lu:%lu]", BytesToHex(txnDigest, 16).c_str(), txn->timestamp().timestamp(), txn->timestamp().id());


  UpdateCommittedReads(txn, txnDigest, ts, proof);

  for (const auto &write : txn->write_set()) {

    //Skip applying TableVersion until after TableWrites have been applied; Same for TableColVersions. Currenty those are both marked as delay.
    //Note: Delay flag set by client is not BFT robust -- server has to infer on it's own. Ok for current prototype. //TODO: turn into parsing at some point
    if(write.has_is_table_col_version() && write.is_table_col_version()){
      //table_and_col_versions.push_back(&write.key());
      continue;
    }   
    // //Also need to do this for TableColVersions...  //write them aside in a little map (just keep a pointer ref to the position)
    // //Currently rely on unique_delim to figure out it is a TableColVersion. Ideally check that prefix is table_name too.
    // size_t pos;
    // if((pos = write.key().find(unique_delimiter)) != std::string::npos){
    //   table_and_col_versions.push_back(&write.key());
    //   continue;
    // }

     //Notice("commit write: %s", write.key().c_str());
    if (!IsKeyOwned(write.key())) {
      continue;
    }

    //Notice("Commit key:[%s]. Ts[%lu:%lu]", write.key().c_str(), txn->timestamp().timestamp(), txn->timestamp().id());

    Debug("COMMIT[%lu,%lu] Committing write for key %s.", txn->client_id(), txn->client_seq_num(), write.key().c_str());

    if (txn->policy_type() == proto::Transaction::NONE) {
      if(write.has_value()) val.val = write.value();
      else if(!params.query_params.sql_mode){
        Panic("When running in KV-store mode write should always have a value");
      }
      val.policyId = GetPolicyId(write.key(), write.value(), ts);
      // policy proof remains the same or is the initial if new key
      std::pair<Timestamp, Server::Value> tsVal;
      bool exists = store.get(write.key(), ts, tsVal);
      if (exists) {
        val.policyProof = tsVal.second.policyProof;
      }
      else {
        auto committedItr = committed.find("");
        UW_ASSERT(committedItr != committed.end());
        val.policyProof = committedItr->second;
      }
    }
    else if (txn->policy_type() == proto::Transaction::KEY_POLICY_ID) {
      Debug("Committing for key %s new policy id %s.", BytesToHex(write.key(), 16).c_str(), write.value().c_str());
      // use existing value and proof
      std::pair<Timestamp, Server::Value> tsVal;
      UW_ASSERT(store.get(write.key(), ts, tsVal));
      val.val = tsVal.second.val;
      val.proof = tsVal.second.proof;
      val.policyId = std::stoull(write.value());
    }
    
    UW_ASSERT(txn->has_txndigest());
    if(!txn->has_txndigest()) *txn->mutable_txndigest() = txnDigest;

    store.put(write.key(), val, ts);

    
    if(params.rtsMode == 1){
      //Do nothing
    }
    else if(params.rtsMode == 2){
      //Remove obsolete RTS. TODO: Can be more refined: Only remove
      std::pair<std::shared_mutex, std::set<Timestamp>> &rts_set = rts_list[write.key()];
      {
          std::unique_lock lock(rts_set.first);
          auto itr = rts_set.second.lower_bound(ts); //begin
          auto endItr = rts_set.second.end();        //upper_bound  --> Old version felt wrong, why would we remove the smaller RTS..
          //delete all RTS >= committed TS. Those RTS can no longer claim the space < RTS since a write successfully committed.
          //TODO: Ideally this would be more refined: RTS should include the timestamp of the write read, i.e. an RTS claims range <read-write, timestamp>
          while (itr != endItr) {
            itr = rts_set.second.erase(itr);
          }
      }
    }
    else{
      //No RTS
    }
  }

  if(params.query_params.useSemanticCC){
    RecordReadPredicatesAndWrites(*txn, ts, true);
    //Note on safety: We are not holding a lock on TableVersion while committing. This means the server-local writes are not guaranteed to be observed by concurrent read predicates
    //This is fine globally, because reaching Commit locally, implies that a Quorum of servers have prepared already. 
    //Since that prepare DID hold locks, safety is already enforced. Recording them here simply upgrades them to commit status.
  }

  if(ASYNC_WRITES){
    auto f = [this, val, txn, ts, txnDigest]() mutable {   //not very safe: Need to rely on fact that txn won't be deleted (should never be, since it is part of proof)
      std::vector<std::string> locally_relevant_table_changes = ApplyTableWrites(*txn, ts, txnDigest, val.proof);
    
       //Apply TableVersion 
      for(auto table: locally_relevant_table_changes){   
        Debug("Commit TableVersion or TableColVersion: %s with TS: [%lu:%lu]", table.c_str(), ts.getTimestamp(), ts.getID());
        val.val = ""; 
        store.put(std::move(table), val, ts);  
      }
      return (void*) true;
    };
    transport->DispatchTP_noCB(std::move(f));
  }
  else{
    //Apply TableWrites: //TODO: Apply also for Prepare: Mark TableWrites as prepared. TODO: add interface func to set prepared, and clean also.. commit should upgrade them. //FIXME: How does SQL update handle exising row
                              // alternatively: don't mark prepare/commit inside the table store, only in CC store. But that requires extra lookup for all keys in read set.
                              // + how do we remove prepared rows? Do we treat it as SQL delete (at ts)? row becomes invisible -- fully removed from CC store.
    std::vector<std::string> locally_relevant_table_changes = ApplyTableWrites(*txn, ts, txnDigest, proof);
    // for (const auto &[table_name, table_write] : txn->table_writes()){
    //   ApplyTableWrites(table_name, table_write, ts, txnDigest, proof);
    //   //val.val = "";
    //   //store.put(table_name, val, ts);     //TODO: Confirm that ApplyTableWrite is synchronous -- i.e. only returns after all writes are applied. 
    //                                                         //If not, then must call SetTableVersion as callback from within Peloton once it is done.
    //   //Note: Should be safe to apply TableVersion table by table (i.e. don't need to wait for all TableWrites to finish before applying TableVersions)

      
    
    //   //Note: Does one have to do special handling for Abort? No ==> All prepared versions just produce unecessary conflicts & dependencies, so there is no safety concern.
    // }
    //Apply TableVersion and TableColVersion
    //TODO: for max efficiency (minimal wait time to update): Set_change table in table_write, and write TableVersion as soon as TableWrite has been applied
                                                              //Do the same for Table_Col_Version. TODO: this requires parsing out the table_name however.
    //NOTE: Applying table versions last is necessary for correctness:
            //It guarantees that a reader cannot see a table version without having seen a snapshot that includes *all* the table version txn's writes.
            //Consequently, if we see a snapshot that is incomplete, then the CC check will still check for conflict against this write txn in question.
    // for(auto table_or_col_version: table_and_col_versions){   
    //    Debug("Commit TableVersion or TableColVersion: %s with TS: [%lu:%lu]", (*table_or_col_version).c_str(), ts.getTimestamp(), ts.getID());
    //   val.val = ""; 
    //   store.put(*table_or_col_version, val, ts);  
    // }
    //Only apply TableVersions for writes that are relevant to this shard.
    for(auto table: locally_relevant_table_changes){   
      Debug("Commit TableVersion or TableColVersion: %s with TS: [%lu:%lu]", table.c_str(), ts.getTimestamp(), ts.getID());
      val.val = ""; 
      store.put(std::move(table), val, ts);  
    }
  }

}

void Server::Abort(const std::string &txnDigest, proto::Transaction *txn) {
   //if(params.mainThreadDispatching) abortedMutex.lock();
  aborted.insert(txnDigest);
  Debug("Inserted txn %s into ABORTED on CPU: %d", BytesToHex(txnDigest, 16).c_str(), sched_getcpu());
   //if(params.mainThreadDispatching) abortedMutex.unlock();
  Debug("Calling CLEAN for aborting txn[%s]", BytesToHex(txnDigest, 16).c_str());
  Clean(txnDigest, true);
  CheckDependents(txnDigest);
  CleanDependencies(txnDigest);

  ClearRTS(txn->read_set(), txn->timestamp());

  CleanQueries(txn, false);

   if(params.query_params.useSemanticCC){
    ClearPredicateAndWrites(*txn);
  }

  materializedMap::accessor mat;
  bool first_mat = materialized.insert(mat, txnDigest); //NOTE: Even though abort does not "write anything", we still mark the data structure to indicate it has been materialized
  mat.release();

  CheckWaitingQueries(txnDigest, txn->timestamp().timestamp(), txn->timestamp().id(), true, first_mat? 0 : 1); //is_abort  //NOTE: WARNING: If Clean(abort) deletes txn then must callCheckWaitingQueries before Clean.
        //If not the first materialization: only wake TS.
}

void Server::Clean(const std::string &txnDigest, bool abort, bool hard) {

  //Latency_Start(&waitingOnLocks);
  //auto ongoingMutexScope = params.mainThreadDispatching ? std::unique_lock<std::shared_mutex>(ongoingMutex) : std::unique_lock<std::shared_mutex>();
  //auto preparedMutexScope = params.mainThreadDispatching ? std::unique_lock<std::shared_mutex>(preparedMutex) : std::unique_lock<std::shared_mutex>();
  //auto preparedReadsMutexScope = params.mainThreadDispatching ? std::unique_lock<std::shared_mutex>(preparedReadsMutex) : std::unique_lock<std::shared_mutex>();
  //auto preparedWritesMutexScope = params.mainThreadDispatching ? std::unique_lock<std::shared_mutex>(preparedWritesMutex) : std::unique_lock<std::shared_mutex>();

  //auto interestedClientsMutexScope = params.mainThreadDispatching ? std::unique_lock<std::mutex>(interestedClientsMutex) : std::unique_lock<std::mutex>();
  //auto p1ConflictsMutexScope = params.mainThreadDispatching ? std::unique_lock<std::mutex>(p1ConflictsMutex) : std::unique_lock<std::mutex>();
  //p1Decisions..
  // auto p2DecisionsMutexScope = params.mainThreadDispatching ? std::unique_lock<std::mutex>(p2DecisionsMutex) : std::unique_lock<std::mutex>();
  // auto current_viewsMutexScope = params.mainThreadDispatching ? std::unique_lock<std::mutex>(current_viewsMutex) : std::unique_lock<std::mutex>();
  // auto decision_viewsMutexScope = params.mainThreadDispatching ? std::unique_lock<std::mutex>(decision_viewsMutex) : std::unique_lock<std::mutex>();
  //auto ElectQuorumMutexScope = params.mainThreadDispatching ? std::unique_lock<std::mutex>(ElectQuorumMutex) : std::unique_lock<std::mutex>();
  //Latency_End(&waitingOnLocks);

  ongoingMap::accessor o;
  bool is_ongoing = ongoing.find(o, txnDigest);
  if(is_ongoing && hard){
    //delete o->second.txn; //Not safe to delete txn because another thread could be using it concurrently... //FIXME: Fix this leak at some point --> Threads must either hold ongoing while operating on tx, or manage own object.
    aborted.insert(txnDigest); //No need to call abort -- A hard clean indicates an invalid tx, and no honest replica would have voted for it --> thus there can be no dependents and dependencies.
  } 
  if(is_ongoing){
    //ongoingErased.insert(txnDigest);
    ongoing.erase(o); //Note: Erasing here implicitly releases accessor o. => Note: Don't think holding o matters here, all that matters is that ongoing is erased before (or atomically) with prepared
  } 
  

  preparedMap::accessor a;
  bool is_prepared = prepared.find(a, txnDigest);
  if(is_prepared){
      const proto::Transaction *txn = a->second.second;
      Timestamp &ts = a->second.first;
  //if (itr != prepared.end()) {
    for (const auto &read : txn->read_set()) {
    //for (const auto &read : itr->second.second->read_set()) {
       //Notice("clean prepare reads: %s", read.key().c_str());
      if(read.has_is_table_col_version() && read.is_table_col_version()) continue;
      if (IsKeyOwned(read.key())) {
        //preparedReads[read.key()].erase(a->second.second);
        //preparedReads[read.key()].erase(itr->second.second);
        std::pair<std::shared_mutex, std::set<const proto::Transaction *>> &y = preparedReads[read.key()];
        std::unique_lock lock(y.first);
        y.second.erase(txn);
      }
    }
    for (const auto &write : txn->write_set()) {
    //for (const auto &write : itr->second.second->write_set()) {
       //Notice("clean prepared write: %s", write.key().c_str());
       //Note: Table versions are owned by all -> thus remove them. Not really important, since this has no CC bearing... Would be fine to keep them too (does not affect safety/liveness, only efficiency).
      if ((write.has_is_table_col_version() && write.is_table_col_version())|| IsKeyOwned(write.key())) { 
        //preparedWrites[write.key()].erase(itr->second.first);
        std::pair<std::shared_mutex,std::map<Timestamp, const proto::Transaction *>> &x = preparedWrites[write.key()];
        std::unique_lock lock(x.first);
        x.second.erase(ts);
        //x.second.erase(itr->second.first);
      }
    }
    prepared.erase(a);

    if(abort || hard){ //If abort or invalid TX: Remove any prepared writes; Note: ApplyWrites(commit) already updates all prepared values to committed.
      Debug("Purging txn: [%s]. Num tables: %d", BytesToHex(txnDigest, 16).c_str(), txn->table_writes_size());

        if(ASYNC_WRITES){
          auto f = [this, txn, ts, txnDigest](){   //not very safe: Need to rely on fact that ongoingTxn won't be deleted => maybe make a copy?
            UW_ASSERT(txn);
            for (const auto &[table_name, table_write] : txn->table_writes()){
              table_store->PurgeTableWrite(table_name, table_write, ts, txnDigest);
            }
            return (void*) true;
          };
          transport->DispatchTP_noCB(std::move(f));
        }
        else{
          for (const auto &[table_name, table_write] : txn->table_writes()){
            table_store->PurgeTableWrite(table_name, table_write, ts, txnDigest);
          }
        }

     
    }
  }
  a.release();

  // if(is_ongoing){
  //     std::cerr << "ONGOING ERASE: " << BytesToHex(txnDigest, 16).c_str() << " On CPU: " << sched_getcpu()<< std::endl;
  //     //if(abort) delete b->second.txn; //delete allocated txn.
  //     //ongoing.erase(b);
  // }
  //if(is_ongoing) ongoing.erase(o);
  o.release(); //Release only at the end, so that Prepare and Clean in parallel for the same TX are atomic.  => See note above -> Don't actually need this atomicity.
  //TODO: might want to move release all the way to the end.

  //XXX: Fallback related cleans

  //interestedClients[txnDigest].insert(remote.clone());
  interestedClientsMap::accessor i;
  auto jtr = interestedClients.find(i, txnDigest);
  if(jtr){
  //if (jtr != interestedClients.end()) {
    for (const auto client : i->second) {
      delete client.second;
    }
    interestedClients.erase(i);
  }
  i.release();


  ElectQuorumMap::accessor e;
  auto ktr = ElectQuorums.find(e, txnDigest);
  if (ktr) {
    // ElectFBorganizer &electFBorganizer = e->second;
    //   //delete all sigs allocated in here
    //   auto it=electFBorganizer.view_quorums.begin();
    //   while(it != electFBorganizer.view_quorums.end()){
    //       for(auto decision_sigs : it->second){
    //         for(auto sig : decision_sigs.second.second){  //it->second[decision_sigs].second
    //           delete sig;
    //         }
    //       }
    //       it = electFBorganizer.view_quorums.erase(it);
    //   }

    ElectQuorums.erase(e);
  }
  e.release();

  //TODO: try to merge more/if not all tx local state into ongoing and hold ongoing locks for simpler atomicity.

//FIXME: Fix these leaks at some point.

//Currently commented out so that original client does not re-do work if p1/p2 has already happened
//--> TODO: Make original client dynamic, i.e. it can directly receive a writeback if it exists, instead of HAVING to finish normal case.
  p1MetaDataMap::accessor c;
  bool p1MetaExists = p1MetaData.find(c, txnDigest);
  if(p1MetaExists && !TEST_PREPARE_SYNC){
    if(c->second.hasSignedP1) delete c->second.signed_txn; //Delete signed txn if not needed anymore.
    c->second.hasSignedP1 = false;
  } 
  //if(hasP1) p1MetaData.erase(c);
  if(hard) p1MetaData.erase(c);
  c.release();

  //Note: won't exist if hard clean (invalid tx)
  // p2MetaDataMap::accessor p;
  // if(p2MetaDatas.find(p, txnDigest)){
  //   p2MetaDatas.erase(p);
  // }
  // p.release();

  //TODO: erase all timers if we end up using them again

  // tbb::concurrent_hash_map<std::string, std::mutex>::accessor z;
  // if(completing.find(z, txnDigest)){
  //   //z->second.unlock();
  //   completing.erase(z);
  // }
  // z.release();

   TxnMissingQueriesMap::accessor mq;
   if(TxnMissingQueries.find(mq, txnDigest)){
    if(mq->second != nullptr) delete mq->second;
    TxnMissingQueries.erase(mq);
   }
   mq.release();
}

uint64_t Server::GetPolicyId(const std::string &key, const std::string &value, const Timestamp &ts, const bool checkPrepared) {
  // first try and find in policy store
  std::pair<Timestamp, Server::Value> tsVal;
  bool exists = store.get(key, ts, tsVal);

  const proto::Transaction *mostRecentPrepared = nullptr;
  if (checkPrepared && params.maxDepDepth > -2) {
    // find most recent prepared policy change transaction
    mostRecentPrepared = FindPreparedVersion(key, ts, exists, tsVal, true);
  }

  if (mostRecentPrepared != nullptr) {
    for (const auto &w : mostRecentPrepared->write_set()) {
      if (w.key() == key) {
        // policy change transaction writeset value is a new policy id
        return std::stoull(w.value());
      }
    }
  }
  else if (exists) {
    // if no prepared policy change transaction, use the committed policy id
    Debug("Found policy for key %s in store", BytesToHex(key, 16).c_str());
    return tsVal.second.policyId;
  }

  // if no prepared or committed policy id, use the policy function
  Debug("No policy found for key %s, using policy function", BytesToHex(key, 16).c_str());
  return policyIdFunction(key, value);
}

bool Server::EndorsementCheck(const proto::SignedMessages *endorsements, const std::string &txnDigest, const proto::Transaction *txn) {
  PolicyClient policyClient;
  ExtractPolicy(txn, policyClient);
  return ValidateEndorsements(policyClient, endorsements, txn->client_id(), txnDigest);
}

void Server::ExtractPolicy(const proto::Transaction *txn, PolicyClient &policyClient) {
  Timestamp ts(txn->timestamp());
  // if regular txn, then need to check prepared policy
  // if txn is a change policy txn, checking prepared would result it checking itself
  const bool checkPreparedPolicy = txn->policy_type() == proto::Transaction::NONE;

  for (const auto &write : txn->write_set()) {
    if (!IsKeyOwned(write.key())) {
      continue;
    }

    uint64_t policyId = GetPolicyId(write.key(), write.value(), ts, checkPreparedPolicy);
    Debug("Extracting policy %lu for key %s", policyId, BytesToHex(write.key(), 16).c_str());

    std::pair<Timestamp, Policy *> tsPolicy;
    bool exists = policyStore.get(policyId, ts, tsPolicy);
    if (!exists) {
      Panic("Cannot find policy %lu in policyStore", policyId);
    }

    policyClient.AddPolicy(tsPolicy.second);
  }

  // disallow readset to contain a policy weaker than the write set
  for (const auto &read : txn->read_set()) {
    if (!IsKeyOwned(read.key())) {
      continue;
    }

    uint64_t policyId = GetPolicyId(read.key(), "", read.readtime(), checkPreparedPolicy);
    Debug("Extracting policy %lu for key %s", policyId, BytesToHex(read.key(), 16).c_str());
    std::pair<Timestamp, Policy *> tsPolicy;
    bool exists = policyStore.get(policyId, read.readtime(), tsPolicy);
    if (!exists) {
      Panic("Cannot find policy %lu in policyStore", policyId);
    }

    if (policyClient.IsOtherWeaker(tsPolicy.second)) {
      Panic("Read policy is weaker than write policy");
    }
  }
}

bool Server::ValidateEndorsements(const PolicyClient &policyClient, const proto::SignedMessages *endorsements, 
    uint64_t client_id, const std::string &txnDigest) {

  std::set<uint64_t> endorsers;
  endorsers.insert(client_id);

  if (endorsements != nullptr) {
    for (const auto &endorsement : endorsements->sig_msgs()) {
      // cannot have empty data
      if (endorsement.data().length() == 0) {
        return false;
      }
      // then check that data is all same as well
      if (txnDigest != endorsement.data()) {
        return false;
      }

      // check signature
      if (params.sintr_params.signFinishValidation) {
        if (!client_verifier->Verify(
          keyManager->GetPublicKey(keyManager->GetClientKeyId(endorsement.process_id())), 
          endorsement.data(), 
          endorsement.signature())
        ) {
          return false;
        }
      }

      endorsers.insert(endorsement.process_id());
    }
  }

  // check if endorsers satisfy policy
  return policyClient.IsSatisfied(endorsers);
}

} // namespace sintrstore
