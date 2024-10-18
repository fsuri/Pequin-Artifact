/***********************************************************************
 *
 * Copyright 2021 Florian Suri-Payer <fsp@cs.cornell.edu>
 *                Yunhao Zhang <yz2327@cornell.edu>
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
#include "store/pg_SMRstore/server.h"
#include "store/pg_SMRstore/common.h"
#include "store/common/transaction.h"
#include <iostream>
#include <sys/time.h>
#include <cstdlib>
#include <fmt/core.h>
#include <regex>

namespace pg_SMRstore {

static bool TEST_DUMMY_RESULT = false;
static uint64_t number_of_threads = 8;

using namespace std;

Server::Server(const transport::Configuration& config, KeyManager *keyManager,
  int groupIdx, int idx, int numShards, int numGroups, bool signMessages,
  bool validateProofs, uint64_t timeDelta, Partitioner *part, Transport* tp, bool localConfig,
  int SMR_mode, uint64_t num_clients, TrueTime timeServer) : config(config), keyManager(keyManager),
  groupIdx(groupIdx), idx(idx), id(groupIdx * config.n + idx),
  numShards(numShards), numGroups(numGroups), signMessages(signMessages),
  validateProofs(validateProofs),  timeDelta(timeDelta), part(part), tp(tp), localConfig(localConfig), timeServer(timeServer) {

  //tp->AddIndexedThreads(number_of_threads);

      //thread 0 is doing messages
    //thread 1 is doing parsing.  //TODO: Move these onto same thread
    //TODO: Configure based off SMR mode
    //TODO: add offset param  (make offset = num cores - num_threads)
    //If SMR mode = 0: use 7 threads and offset = 1   => might be better with 6 as well? Maybe just use 6 for even comparison?
    if(SMR_mode == 0) number_of_threads = 7;
    if(SMR_mode == 1) number_of_threads = 6;  
    //If SMR mode = 2 => same I assume. check!
    if(SMR_mode == 2) number_of_threads = 7;

  number_of_threads = num_clients; //give each client it's own thread. //TODO: threads => offset by 64..
  tp->AddIndexedThreads(number_of_threads);

  //separate for local configuration we set up different db name for each servers, otherwise they can share the db name
  std::string db_name = "db1";
  if (localConfig){
    Debug("using local config (pg server");
    db_name = "db" + std::to_string(1 + idx);
  }

  // password should match the one created in Pequin-Artifact/pg_setup/postgres_service.sh script
  // port should match the one that appears when executing "pg_lsclusters -h"
 
  connection_string = "host=/users/fs435/tmp-pgdata/socket/ user=pequin_user password=123 dbname=" + db_name + " port=5432"; //Connect to UNIX socket
  //connection_string = "host=localhost user=pequin_user password=123 dbname=" + db_name + " port=5432"; //Connect via TCP using localhost loopback device
 
  connectionPool = tao::pq::connection_pool::create(connection_string);

  Notice("PostgreSQL serverside client-proxy created! Server Id: %d", idx);
}

Server::~Server() {}


////////////////////////////////////////////
/*         SQL Exec Orchestration         */
////////////////////////////////////////////

std::shared_ptr< tao::pq::transaction> Server::GetClientTransaction(clientConnectionMap::accessor &c, const uint64_t &client_id, const uint64_t &tx_id){
  bool new_connection = clientConnections.insert(c, client_id);
  if(new_connection) c->second.CreateNewConnection(connection_string, client_id);
  return c->second.GetTX(tx_id);
}

::google::protobuf::Message* Server::ParseMsg(const string& type, const string& msg, std::string &client_seq_key, uint64_t &req_id, uint64_t &client_id, uint64_t &tx_id){

  Debug("Start parse");
  if (type == sql_rpc_template.GetTypeName()) {
    proto::SQL_RPC *sql_rpc = new proto::SQL_RPC();
    sql_rpc->ParseFromString(msg);
    client_seq_key = createClientSeqKey(sql_rpc->client_id(), sql_rpc->txn_seq_num());
    req_id = sql_rpc->req_id();
    client_id = sql_rpc->client_id();
    tx_id = sql_rpc->txn_seq_num();
    return sql_rpc;
  } 
  else if (type == try_commit_template.GetTypeName()) {
    proto::TryCommit *try_commit = new proto::TryCommit();
    try_commit->ParseFromString(msg);
    client_seq_key = createClientSeqKey(try_commit->client_id(),try_commit->txn_seq_num());
    req_id = try_commit->req_id();
    client_id = try_commit->client_id();
    tx_id = try_commit->txn_seq_num();
    return try_commit;
  } 
  else if (type == user_abort_template.GetTypeName()) {
    proto::UserAbort *user_abort = new proto::UserAbort();
    user_abort->ParseFromString(msg);
    client_seq_key = createClientSeqKey(user_abort->client_id(), user_abort->txn_seq_num());
    client_id = user_abort->client_id();
    tx_id = user_abort->txn_seq_num();
    return user_abort;
  }
  else{
    Panic("invalid message type");
  }
}

static uint64_t counter = 0;
//Synchronous Execution Interface -> Exec synchronously and return result (no threads)
std::vector<::google::protobuf::Message*> Server::Execute(const string& type, const string& msg) {
  Debug("Execute: %s", type.c_str());
  
  std::vector<::google::protobuf::Message*> results;
  std::string client_seq_key; //TODO: GET RID OF THIS
  uint64_t req_id;

  uint64_t client_id;
  uint64_t tx_id;

  ::google::protobuf::Message *req = ParseMsg(type, msg, client_seq_key, req_id, client_id, tx_id);

  Debug("Received Request from client: %lu. Tx: %lu", client_id, tx_id);

  results.push_back(ProcessReq(req_id, client_id, tx_id, type, req)); //TODO: NEED TO PUSH BACK.
  
  delete req;
  return results;
}

//Asynchronous Execution Interface -> Dispatch execution to a thread, and let it call callback when done
void Server::Execute_Callback(const string& type, const string& msg, std::function<void(std::vector<google::protobuf::Message*>& )> ecb) {
  Debug("Execute with callback: %s", type.c_str());

  std::string client_seq_key; //TODO: GET RID OF THIS
  uint64_t req_id;

  uint64_t client_id;
  uint64_t tx_id;
  
  ::google::protobuf::Message *req = ParseMsg(type, msg, client_seq_key, req_id, client_id, tx_id);

  auto f = [this, type, client_id, tx_id, req, req_id, cb = std::move(ecb)]() mutable{
    std::vector<::google::protobuf::Message*> results;
    results.push_back(ProcessReq(req_id, client_id, tx_id, type, req));
    delete req;

    // Issue Callback back on mainthread (That way don't need to worry about concurrency when building EBatch)
    tp->Timer(0, std::bind(cb, results));
  
    return (void*) true;
  };

  auto thread_id = getThreadID(client_id);
  Debug("Thread id for key %d is: %d", client_id, thread_id);
 
  //Dispatch the exec job to the Indexed Threadpool. Make sure that all operations from the same TX go onto the same Thread (and thus are sequential)
  tp->DispatchIndexedTP_noCB(thread_id,f);
}

uint64_t Server::getThreadID(const uint64_t &client_id){
  return client_id % number_of_threads;
}



//TODO: Call this from ExecuteCallback too.
::google::protobuf::Message* Server::ProcessReq(uint64_t req_id, uint64_t client_id, uint64_t tx_id, const string& type, ::google::protobuf::Message *req){

  stats.Increment("ProcessReq, 1");
  Debug("ProcessReq. client: %lu. txn: %lu. req: %lu", client_id, tx_id, req_id);

  clientConnectionMap::accessor c;
   //auto tx = GetClientTransaction(c, client_id, tx_id);
  bool new_connection = clientConnections.insert(c, client_id);
  if(!c->second.ValidTX(tx_id)) {
    Debug("Tx[%lu:%lu] is no longer active!", client_id, tx_id);
    return nullptr;
  }
  if(new_connection) c->second.CreateNewConnection(connection_string, client_id);
  auto tx = c->second.GetTX(tx_id);
  
// ///////////////  Dummy testing
if(TEST_DUMMY_RESULT){
    // counter++;
    // if(counter % 2 == 1){
    if(type == sql_rpc_template.GetTypeName()){

      // struct timespec ts_start;
      // clock_gettime(CLOCK_MONOTONIC, &ts_start);
      // uint64_t exec_start_us = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;
    
      auto transaction = c->second.connection->transaction(); //NEXT: Try the difference between setting it here and setting to nullptr, vs keeping it open.
      transaction = nullptr;

      // clock_gettime(CLOCK_MONOTONIC, &ts_start);
      // uint64_t exec_end_us = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;
      // Notice("Postgres latency: %lu us", exec_end_us - exec_start_us);
      
      //if(req_id % 1000 == 1) Notice("Finished Postgres: %lu", req_id);

      
      proto::SQL_RPCReply *reply  = new proto::SQL_RPCReply();
      reply->set_req_id(req_id);
      reply->set_status(0);
      
      sql::QueryResultProtoBuilder res;
      res.set_rows_affected(1);
      reply->set_sql_res(res.get_result()->SerializeAsString());
      //transport->SendMessage(this, remote, reply);
      return reply;
    }
    else{
      //tx->commit();
      //c->second.TerminateTX();
      proto::TryCommitReply *reply = new proto::TryCommitReply();
      reply->set_req_id(req_id);
      reply->set_status(0);
      //transport->SendMessage(this, remote, reply);
      return reply;
    }
  
  Panic("getting here");
 
}

//  if(type == sql_rpc_template.GetTypeName()){
//   c->second.transaction = nullptr;
//   c->second.transaction = c->second.connection->transaction();
//  }
//  auto tx = c->second.transaction;
 //auto tx = c->second.GetTX(tx_id);
  ///////////////
  

  if (tx){ // if TX is still active
    UW_ASSERT(tx != nullptr); //sanity debug
    Debug("ProcessReq Continue Tx. client: %lu. txn: %lu. req: %lu", client_id, tx_id, req_id);

    if (type == sql_rpc_template.GetTypeName()) {
      proto::SQL_RPC *sql_rpc = (proto::SQL_RPC*) req;
      return HandleSQL_RPC(c, tx, req_id, sql_rpc->query());
    } 
    else if (type == try_commit_template.GetTypeName()) {
      return HandleTryCommit(c, tx, req_id);
    } 
    else if (type == user_abort_template.GetTypeName()) {
      //Panic("This case should not be triggered in simple RW-SQL test with single client");
      HandleUserAbort(c, tx); //no reply needed.
      return nullptr;
    }

  }
  else{ //if TX has been aborted
     Debug("ProcessReq Has Been terminated already. client: %lu. txn: %lu. req: %lu", client_id, tx_id, req_id);
    if (type == sql_rpc_template.GetTypeName()) {
      //Panic("Should never get triggered in simple test with single client running sequentially");
      //UW_ASSERT(is_aborted);
      proto::SQL_RPCReply* reply = new proto::SQL_RPCReply();
      reply->set_req_id(req_id);
      reply->set_status(REPLY_FAIL);
      return reply;
    } else if (type == try_commit_template.GetTypeName()) {
      //Panic("Should never get triggered in simple test with single client running sequentially");
      Debug("tr is null - trycommit ");
      //UW_ASSERT(is_aborted);
      proto::TryCommitReply* reply = new proto::TryCommitReply();
      reply->set_req_id(req_id);
      reply->set_status(REPLY_FAIL); 
      return reply;
    }
    else{
      //If already deleted, ignore Abort
       Debug("Redundant Abort Tx. client: %lu. txn: %lu. req: %lu", client_id, tx_id, req_id);
      UW_ASSERT(type == user_abort_template.GetTypeName());
      return nullptr;
    }
  }
}


//Synchronous Execution Interface -> Exec synchronously and return result (no threads)
std::vector<::google::protobuf::Message*> Server::Execute_OLD(const string& type, const string& msg) {
  Debug("Execute: %s", type.c_str());
  
  std::vector<::google::protobuf::Message*> results;
  std::string client_seq_key;
  uint64_t req_id;

  uint64_t client_id = 0;
  uint64_t tx_id = 0;
  
  ::google::protobuf::Message *req = ParseMsg(type, msg, client_seq_key, req_id, client_id, tx_id);

  txnStatusMap::accessor t;
  auto [tx, is_aborted] = getPgTransaction(t, client_seq_key);
  if (tx){ // if TX is still active
    if (type == sql_rpc_template.GetTypeName()) {
      proto::SQL_RPC *sql_rpc = (proto::SQL_RPC*) req;
      results.push_back(HandleSQL_RPC_OLD(t, tx, req_id, sql_rpc->query()));
    } 
    else if (type == try_commit_template.GetTypeName()) {
      results.push_back(HandleTryCommit_OLD(t, tx, req_id));
    } 
    else if (type == user_abort_template.GetTypeName()) {
      HandleUserAbort_OLD(t, tx); //no reply needed.
    }

  }
  else{ //if TX has been aborted
    if (type == sql_rpc_template.GetTypeName()) {
      //UW_ASSERT(is_aborted);
      proto::SQL_RPCReply* reply = new proto::SQL_RPCReply();
      reply->set_req_id(req_id);
      reply->set_status(REPLY_FAIL);
      results.push_back(reply);
    } else if (type == try_commit_template.GetTypeName()) {
      Debug("tr is null - trycommit ");
      //UW_ASSERT(is_aborted);
      proto::TryCommitReply* reply = new proto::TryCommitReply();
      reply->set_req_id(req_id);
      reply->set_status(REPLY_FAIL); // OR should it be reply_ok?
      results.push_back(reply);
    }
  }

  delete req;
  return results;
}



//Asynchronous Execution Interface -> Dispatch execution to a thread, and let it call callback when done
void Server::Execute_Callback_OLD(const string& type, const string& msg, std::function<void(std::vector<google::protobuf::Message*>& )> ecb) {
  Debug("Execute with callback: %s", type.c_str());

  std::string client_seq_key;
  uint64_t req_id;

   uint64_t client_id = 0;
  uint64_t tx_id = 0;
  
  ::google::protobuf::Message *req = ParseMsg(type, msg, client_seq_key, req_id, client_id, tx_id);

  //TODO: Change this: 
  auto thread_id = getThreadID(client_seq_key);
  Debug("Thread id for key %s is: %d",client_seq_key.c_str(), thread_id);
  // std::cerr << "Shir print:    " << "Thread id for key " << client_seq_key.c_str()<<" is: "<<thread_id << std::endl;

  
  auto f = [this, type, client_seq_key, req, req_id, ecb](){
    std::vector<::google::protobuf::Message*> results;

    //TODO: Replace all this with ProcessReq();

    txnStatusMap::accessor t;
    auto [tr, is_aborted] = getPgTransaction(t, client_seq_key);
    // auto [tr, is_aborted] = std::make_pair(nullptr,true);
    if (tr){
      // this means tr is not a null pointer. it would be a null pointer if this txn was alerady aborted. 
      if (type == sql_rpc_template.GetTypeName()) {
        proto::SQL_RPC *sql_rpc = (proto::SQL_RPC*) req;
        results.push_back(HandleSQL_RPC_OLD(t, tr, req_id, sql_rpc->query()));
      } 
      else if (type == try_commit_template.GetTypeName()) {
        results.push_back(HandleTryCommit_OLD(t, tr, req_id));
      } 
      else if (type == user_abort_template.GetTypeName()) {
        HandleUserAbort_OLD(t,tr);
      }
    }
    else{
      if (type == sql_rpc_template.GetTypeName()) {
        UW_ASSERT(is_aborted);
        proto::SQL_RPCReply* reply = new proto::SQL_RPCReply();
        reply->set_req_id(req_id);
        reply->set_status(REPLY_FAIL);
        results.push_back(reply);
      } 
      else if (type == try_commit_template.GetTypeName()) {
        Debug("tr is null - trycommit ");
        UW_ASSERT(is_aborted);
        proto::TryCommitReply* reply = new proto::TryCommitReply();
        reply->set_req_id(req_id);
        reply->set_status(REPLY_OK); // OR should it be reply_ok?
        results.push_back(reply);
      }
    }
    delete req;

    // Issue Callback back on mainthread (That way don't need to worry about concurrency when building EBatch)
    tp->Timer(0, std::bind(ecb, results));
  
    return (void*) true;
  };

  //Dispatch the exec job to the Indexed Threadpool. Make sure that all operations from the same TX go onto the same Thread (and thus are sequential)
  tp->DispatchIndexedTP_noCB(thread_id,f);
}



////////////////////////////////////////////
/*          SQL PROCESSING LOGIC          */
////////////////////////////////////////////

::google::protobuf::Message* Server::HandleSQL_RPC(clientConnectionMap::accessor &c, std::shared_ptr<tao::pq::transaction> tx, uint64_t req_id, std::string query) {
  Debug("Handling SQL_RPC");
  
  proto::SQL_RPCReply* reply = new proto::SQL_RPCReply();
  reply->set_req_id(req_id);

  try {
    Debug("Attempt query %s", query.c_str());
    
    if(TEST_DUMMY_RESULT){
      sql::QueryResultProtoBuilder res;
      res.set_rows_affected(1);
      reply->set_sql_res(res.get_result()->SerializeAsString());
      reply->set_status(0);
    }
    else{

      Debug("Try exec");
      const tao::pq::result sql_res = tx->execute(query);
    
      Debug("Finished Exec");
  
      reply->set_status(REPLY_OK);
      reply->set_sql_res(createResult(sql_res));
    }
    
  } 
  catch(tao::pq::sql_error e) {
    //Panic("No exceptions should be thrown for simple rw sql test with single client");
    Debug("An exception caught while using postgres.");
   
    if (std::regex_match(e.sqlstate, std::regex("40...")) || std::regex_match(e.sqlstate, std::regex("55P03"))){ // Concurrency errors
      Debug("A concurrency exception caught while using postgres.: %s", e.what());
      //tr->rollback();
      c->second.TerminateTX(); //tx = nullptr; //alternatively: Then must pass tx by reference
      
      reply->set_status(REPLY_FAIL);
      reply->set_sql_res("");
    } 
    else if (std::regex_match(e.sqlstate, std::regex("23..."))){ // Application errors
    Notice("An integrity exception caught while using postgres.: %s", e.what());
      reply->set_status(REPLY_OK); 
      reply->set_sql_res("");
    }
    else if (std::regex_match(e.sqlstate, std::regex("25P04"))){ // Concurrency errors
      Notice("A lock-timeout exception caught while using postgres.: %s", e.what());
      //tr->rollback();
      c->second.TerminateTX(); //tx = nullptr; //alternatively: Then must pass tx by reference
      
      reply->set_status(REPLY_FAIL);
      reply->set_sql_res("");
    } 
    else{
      Panic("Unexpected postgres exception: %s. %s", e.sqlstate.c_str() , e.what());  //FIXME: pivot probably means we aborted due to lock timeout -> need to abort next tx too.
    }
  }
  catch (const std::exception &e) {
    Panic("Tx write failed with uncovered exception: %s", e.what());
  }

  return reply;
}

::google::protobuf::Message* Server::HandleTryCommit(clientConnectionMap::accessor &c, std::shared_ptr<tao::pq::transaction> tx, uint64_t req_id) {
  Debug("Trying to commit a txn %d", req_id);
 
  proto::TryCommitReply* reply = new proto::TryCommitReply();
  reply->set_req_id(req_id);

  
  if(TEST_DUMMY_RESULT){
    reply->set_status(0); 
  }
  else{

    try {
      tx->commit();
      Debug("TryCommit went through successfully.");
      reply->set_status(REPLY_OK);
    } 
    catch(tao::pq::sql_error e) {
      Debug("A exception caugth while using postgres: %s", e.what()); 
      reply->set_status(REPLY_FAIL);
    }
  }
 
  c->second.TerminateTX();
  return reply;

}

::google::protobuf::Message* Server::HandleUserAbort(clientConnectionMap::accessor &c, std::shared_ptr<tao::pq::transaction> tx) {

  Debug("UserAbort: Rollback transaction");

  //TODO: Only do this if it's the current tx_id;

  tx->rollback();
  c->second.TerminateTX();

  Debug("Return no reply for Abort");
  return nullptr;
}



::google::protobuf::Message* Server::HandleSQL_RPC_OLD(txnStatusMap::accessor &t, std::shared_ptr<tao::pq::transaction> tr, uint64_t req_id, std::string query) {
  Debug("Handling SQL_RPC");
  
  proto::SQL_RPCReply* reply = new proto::SQL_RPCReply();
  reply->set_req_id(req_id);

  try {
    Debug("Attempt query %s", query.c_str());
    
    const tao::pq::result sql_res = tr->execute(query);
   
    Debug("Query executed");
   
    reply->set_status(REPLY_OK);
    reply->set_sql_res(createResult(sql_res));
  } 
  catch(tao::pq::sql_error e) {
    Debug("An exception caugth while using postgres.");
   
    if (std::regex_match(e.sqlstate, std::regex("40...")) || std::regex_match(e.sqlstate, std::regex("55P03"))){ // Concurrency errors
      Notice("A concurrency exception caugth while using postgres.: %s", e.what());
      //tr->rollback();
      markTxnTerminated(t);
      t.release();
      reply->set_status(REPLY_FAIL);
      reply->set_sql_res("");
    } 
    else if (std::regex_match(e.sqlstate, std::regex("23..."))){ // Application errors
    Notice("An integrity exception caugth while using postgres.: %s", e.what());
      reply->set_status(REPLY_OK); 
      reply->set_sql_res("");
    }
    else{
      Panic("Unexpected postgres exception: %s", e.what());
    }
  }

  return reply;
}

::google::protobuf::Message* Server::HandleTryCommit_OLD(txnStatusMap::accessor &t, std::shared_ptr<tao::pq::transaction> tr, uint64_t req_id) {
  Debug("Trying to commit a txn %d",req_id);
 
  Debug("the tr pointer for commit is :    %s ",tr);
  proto::TryCommitReply* reply = new proto::TryCommitReply();
  reply->set_req_id(req_id);

  try {
    tr->commit();
    Debug("TryCommit went through successfully.");
    reply->set_status(REPLY_OK);
  } 
  catch(tao::pq::sql_error e) {
    Debug("A exception caugth while using postgres: %s", e.what()); 
    reply->set_status(REPLY_FAIL);
    //tr->rollback(); //probably unecessary
  }
  markTxnTerminated(t);
  return reply;
}

::google::protobuf::Message* Server::HandleUserAbort_OLD(txnStatusMap::accessor &t, std::shared_ptr<tao::pq::transaction> tr) {

  Debug("UserAbort: Rollback transaction");
  tr->rollback();
  markTxnTerminated(t);

  Debug("Return no reply for Abort");
  return nullptr;
}

////////////////////////////////////////////
/*          HELPER FUNCTIONS         */
////////////////////////////////////////////

 std::string Server:: createClientSeqKey(uint64_t cid, uint64_t tid){
    std::string client_seq_key;
    client_seq_key.append(std::to_string(cid));
    client_seq_key.append("|");
    client_seq_key.append(std::to_string(tid));
    return client_seq_key;
}


std::pair<std::shared_ptr<tao::pq::transaction>, bool> Server::getPgTransaction(txnStatusMap::accessor &t, const std::string &key){

  std::shared_ptr<tao::pq::transaction> tr;
  Debug("Client transaction key: %s", key.c_str());
 
  bool is_aborted=false;

  if(txnMap.insert(t,key)) {
      Debug("Key was not found, creating a new connection");
     
      auto connection = connectionPool->connection();
      tr = connection->transaction();
    
      auto txn_status =std::make_tuple(connection, tr, false);
      t->second=txn_status;
    } else {
      Debug("Key already exists. Returning open Tx connection");
      tr = get<1>(t->second);
      is_aborted=get<2>(t->second);
  }
    return std::make_pair(tr,is_aborted);
}


uint64_t Server::getThreadID(const std::string &key){
  std::hash<std::string> hasher;  
  uint64_t id = hasher(key) % number_of_threads;
  return id;
}

// fields are < connection, transaction tr, bool was_aborted>
void Server::markTxnTerminated(txnStatusMap::accessor &t){
  get<1>(t->second) = nullptr; //reset transaction
  get<2>(t->second) = true;
   get<0>(t->second) = nullptr; //reset connection -> goes back to pool
}


std::string Server::createResult(const tao::pq::result &sql_res){
  // Should extrapolate out into builder method
  // Start by looping over columns and adding column names

  sql::QueryResultProtoBuilder res_builder;  
  res_builder.set_rows_affected(sql_res.rows_affected());
  if(sql_res.columns() == 0) {
    res_builder.add_empty_row();
  } else {
    for(int i = 0; i < sql_res.columns(); i++) {
      res_builder.add_column(sql_res.name(i));
      //std::cout << sql_res.name(i) << std::endl;
    }
    // After loop over rows and add them using add_row method
    // for(const auto& row : sql_res) {
    //   res_builder->add_row(std::begin(row), std::end(row));
    // }
    // for(auto it = std::begin(sql_res); it != std::end(sql_res); ++it) {
      
    // }
    Debug("res size: %d", sql_res.size());
    for( const auto& row : sql_res ) {
      RowProto *new_row = res_builder.new_row();
      for( const auto& field : row ) {
        std::string field_str; //This is a HACK. If the result is Null, we should propagate the "null-ness" to frontend. It will work fine here because only string values will be null/empty
        if(!field.is_null()){
           field_str = field.as<std::string>();
        }
        res_builder.AddToRow(new_row,field_str);
        //std::cout << field_str << std::endl;
      }
    }
  }
  return res_builder.get_result()->SerializeAsString();
}

////////////////////////////////////////////
/*          TABLE GENERATION LOGIC        */
////////////////////////////////////////////

void Server::exec_statement(const std::string &sql_statement) {
  auto connection = connectionPool->connection();
  connection->execute(sql_statement);
}


void Server::CreateTable(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, const std::vector<uint32_t> &primary_key_col_idx){
  // //Based on Syntax from: https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-create-table/ 

  Debug("Creating table! %s",table_name.c_str());

  //NOTE: Assuming here we do not need special descriptors like foreign keys, column condidtions... (If so, it maybe easier to store the SQL statement in JSON directly)
  UW_ASSERT(!column_data_types.empty());
  // UW_ASSERT(!primary_key_col_idx.empty());

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

  // std::cerr << "Create Table: " << sql_statement << std::endl;
  Notice("Create Table: %s", sql_statement.c_str());
  this->exec_statement(sql_statement);
}

void Server::CreateIndex(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, const std::string &index_name, const std::vector<uint32_t> &index_col_idx){
  // Based on Syntax from: https://www.postgresqltutorial.com/postgresql-indexes/postgresql-create-index/ and  https://www.postgresqltutorial.com/postgresql-indexes/postgresql-multicolumn-indexes/
  // CREATE INDEX index_name ON table_name(a,b,c,...);
  Debug("Creating index: %s on table: %s", index_name.c_str(), table_name.c_str());

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

  this->exec_statement(sql_statement);
}

void Server::LoadTableData(const std::string &table_name, const std::string &table_data_path, 
    const std::vector<std::pair<std::string, std::string>> &column_names_and_types, const std::vector<uint32_t> &primary_key_col_idx){
  Debug("Load Table data: %s", table_name.c_str());
  // std::cerr<<"Shir: Load Table data\n";
  std::string copy_table_statement = fmt::format("COPY {0} FROM '{1}' DELIMITER ',' CSV HEADER", table_name, table_data_path);
  std::thread t1([this, copy_table_statement]() { this->exec_statement(copy_table_statement); });
  t1.detach();
}

void Server::LoadTableRows(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, const row_segment_t *row_segment, const std::vector<uint32_t> &primary_key_col_idx, int segment_no, bool load_cc){
  Debug("Load %lu Table rows for: %s", row_segment->size(), table_name.c_str());
  // std::cerr<< "Shir: Load Table rows!\n";
  std::string sql_statement = this->GenerateLoadStatement(table_name,*row_segment,0);
  std::thread t1([this, sql_statement]() { this->exec_statement(sql_statement); });
  t1.detach();
  // Shir();
}

//!!"Deprecated" (Unused)
void Server::LoadTableRow(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, const std::vector<std::string> &values, const std::vector<uint32_t> &primary_key_col_idx ){
    Panic("Deprecated");
}


std::string Server::GenerateLoadStatement(const std::string &table_name, const std::vector<std::vector<std::string>> &row_segment, int segment_no){
  std::string load_statement = fmt::format("INSERT INTO {0} VALUES ", table_name);
  for(auto &row: row_segment){
        load_statement += "(";
            for(int i = 0; i < row.size(); ++i){
               load_statement += row[i] + ", ";
            }
        load_statement.resize(load_statement.length()-2); //remove trailing ", "
        load_statement += "), ";
    }
    load_statement.resize(load_statement.length()-2); //remove trailing ", "
    load_statement += ";";
    Debug("Generate Load Statement for Table %s. Segment %d. Statement: %s", table_name.c_str(), segment_no, load_statement.substr(0, 1000).c_str());
    // std::cerr<< "Shir: Generate Load Statement for Tab!\n";

    return load_statement;
}


Stats &Server::GetStats() {
  return stats;
}

Stats* Server::mutableStats() {
  return &stats;
}

}
