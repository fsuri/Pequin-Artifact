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
#include "store/pelotonstore/server.h"
#include "store/pelotonstore/common.h"
#include "store/common/transaction.h"
#include <iostream>
#include <sys/time.h>
#include <cstdlib>
#include <fmt/core.h>
#include <regex>

namespace pelotonstore {

static bool TEST_DUMMY_RESULT = false;
const uint64_t number_of_threads = 8;

using namespace std;

Server::Server(const transport::Configuration& config, KeyManager *keyManager, std::string &table_registry_path,
  int groupIdx, int idx, int numShards, int numGroups, bool signMessages,
  bool validateProofs, uint64_t timeDelta, Partitioner *part, Transport* tp, bool localConfig,
  TrueTime timeServer) : 
  config(config), keyManager(keyManager),
  groupIdx(groupIdx), idx(idx), id(groupIdx * config.n + idx),
  numShards(numShards), numGroups(numGroups), signMessages(signMessages),
  validateProofs(validateProofs),  timeDelta(timeDelta), part(part), tp(tp), localConfig(localConfig), timeServer(timeServer) {

  tp->AddIndexedThreads(number_of_threads);

  int num_threads = std::thread::hardware_concurrency();
  if(num_threads > 8) {
    Warning("more than 8 threads"); 
    //num_threads = 8;
  } 
  table_store = new pelotonstore::TableStore(num_threads); //Register num_threads many traffic cops to be used for Data Loading

  RegisterTables(table_registry_path);

  Notice("Peloton Server Id: %d", idx);
}

Server::~Server() {
  delete table_store;
}


////////////////////////////////////////////
/*         SQL Exec Orchestration         */
////////////////////////////////////////////

::google::protobuf::Message* Server::ParseMsg(const string& type, const string& msg, uint64_t &req_id, uint64_t &client_id, uint64_t &tx_id){

  Debug("Start parse");
  if (type == sql_rpc_template.GetTypeName()) {
    proto::SQL_RPC *sql_rpc = new proto::SQL_RPC();
    sql_rpc->ParseFromString(msg);
   
    req_id = sql_rpc->req_id();
    client_id = sql_rpc->client_id();
    tx_id = sql_rpc->txn_seq_num();
    return sql_rpc;
  } 
  else if (type == try_commit_template.GetTypeName()) {
    proto::TryCommit *try_commit = new proto::TryCommit();
    try_commit->ParseFromString(msg);
   
    req_id = try_commit->req_id();
    client_id = try_commit->client_id();
    tx_id = try_commit->txn_seq_num();
    return try_commit;
  } 
  else if (type == user_abort_template.GetTypeName()) {
    proto::UserAbort *user_abort = new proto::UserAbort();
    user_abort->ParseFromString(msg);
    req_id = 0; //Abort needs no reply
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
 
  uint64_t req_id;
  uint64_t client_id;
  uint64_t tx_id;

  ::google::protobuf::Message *req = ParseMsg(type, msg, req_id, client_id, tx_id);

  Debug("Received Request from client: %lu. Tx: %lu", client_id, tx_id);

  results.push_back(ProcessReq(req_id, client_id, tx_id, type, req)); //TODO: NEED TO PUSH BACK.
  
  delete req;
  return results;
}

//Asynchronous Execution Interface -> Dispatch execution to a thread, and let it call callback when done
void Server::Execute_Callback(const string& type, const string& msg, std::function<void(std::vector<google::protobuf::Message*>& )> ecb) {
  Debug("Execute with callback: %s", type.c_str());


  uint64_t req_id;
  uint64_t client_id;
  uint64_t tx_id;
  
  ::google::protobuf::Message *req = ParseMsg(type, msg, req_id, client_id, tx_id);
  Debug("Received msg. Type[%s]. Txn [%lu:%lu:%lu]", type.c_str(), client_id, tx_id, req_id);

  auto f = [this, type, client_id, tx_id, req, req_id, ecb](){
    std::vector<::google::protobuf::Message*> results;
    results.push_back(ProcessReq(req_id, client_id, tx_id, type, req));
    delete req;

    Debug("Issue Replica Callback [%d:%d] (client, tx_id)", client_id, tx_id);
    // Issue Callback back on mainthread (That way don't need to worry about concurrency when building EBatch)
    //tp->Timer(0, std::bind(ecb, results));
    tp->IssueCB(std::bind(ecb, results), (void*) true);
  
    return (void*) true;
  };

  auto thread_id = getThreadID(client_id);
  Debug("Thread id for client %d is: %d", client_id, thread_id);
 
  //Dispatch the exec job to the Indexed Threadpool. Make sure that all operations from the same TX go onto the same Thread (and thus are sequential)
  tp->DispatchIndexedTP_noCB(thread_id,f);
}

uint64_t Server::getThreadID(const uint64_t &client_id){
  return client_id % number_of_threads;
}


::google::protobuf::Message* Server::ProcessReq(uint64_t req_id, uint64_t client_id, uint64_t tx_id, const string& type, ::google::protobuf::Message *req){

  stats.Increment("ProcessReq, 1");
  Debug("ProcessReq. client: %lu. txn: %lu. req: %lu", client_id, tx_id, req_id);

  ClientStateMap::accessor c;
  clientState.insert(c, client_id);
  if(!c->second.ValidTX(tx_id)) {
    Panic("Tx[%lu:%lu] is no longer active!", client_id, tx_id);
    return nullptr;
  }
  bool terminate_last = false;
  bool begin = false;
  bool active = c->second.GetTxStatus(tx_id, begin, terminate_last); //Checks if there is an ongoing Txn (and if it is active); if not, starts a new Tx.
  if(terminate_last) table_store->Abort(client_id, tx_id-1); //NOTE: This is only to help with FakeSMR mode. It's not technically necessary.
  if(begin) table_store->Begin(client_id, tx_id);
  
  if (active){ // if TX is still active
    Debug("ProcessReq Continue Tx. client: %lu. txn: %lu. req: %lu", client_id, tx_id, req_id);

    if (type == sql_rpc_template.GetTypeName()) {
      proto::SQL_RPC *sql_rpc = (proto::SQL_RPC*) req;
      return HandleSQL_RPC(c, req_id, client_id, tx_id, sql_rpc->query());
    } 
    else if (type == try_commit_template.GetTypeName()) {
      return HandleTryCommit(c, req_id, client_id, tx_id);
    } 
    else if (type == user_abort_template.GetTypeName()) {
      //Panic("This case should not be triggered in simple RW-SQL test with single client");
      HandleUserAbort(c, client_id, tx_id); //no reply needed.
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
      Debug("tr already aborted ");
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


////////////////////////////////////////////
/*          SQL PROCESSING LOGIC          */
////////////////////////////////////////////


::google::protobuf::Message* Server::HandleSQL_RPC(ClientStateMap::accessor &c, uint64_t req_id, uint64_t client_id, uint64_t tx_id, const std::string &query) {
  Debug("Handling SQL_RPC [%d:%d:%d] (client, tx_id, req)", client_id, tx_id, req_id);
  
  proto::SQL_RPCReply* reply = new proto::SQL_RPCReply();
  reply->set_req_id(req_id);

  Debug("Try exec");
  peloton_peloton::ResultType result_status; 
  std::string error_msg; //TODO: It seems like Peloton error msg is always empty?

  //result == serialized ProtoWrapper result
  std::string result = table_store->ExecTransactional(query, client_id, tx_id, result_status, error_msg);

  bool terminate = true;

  if (result_status == peloton_peloton::ResultType::SUCCESS) {
    Debug("Success! [%d:%d:%d]. Query: %s", client_id, tx_id, req_id, query.c_str());
    terminate = false;
    reply->set_status(REPLY_OK);
    reply->set_sql_res(result);
  } 
  else if(result_status == peloton_peloton::ResultType::FAILURE) {
    Panic("Peloton failed");
    // Notice("Peloton Failure [%d:%d:%d] An integrity exception caught while using peloton.: %s", client_id, tx_id, req_id, error_msg.c_str());
    //   reply->set_status(REPLY_OK); 
    //   reply->set_sql_res("");
  }
  else if (result_status == peloton_peloton::ResultType::ABORTED) {
    Notice("Peloton Aborted [%d:%d:%d] : Q:%s. Error: %s", client_id, tx_id, req_id, query.c_str(), error_msg.c_str());
    reply->set_status(REPLY_FAIL);
    reply->set_sql_res("");
    table_store->Abort(client_id, tx_id); //Explicitly abort Tx  
    //(If we don't do this, then the tx hasn't been "removed". When we start the next TX, the aborted txn state is still present and the next TX aborts falsely)
  }
  else if (result_status == peloton_peloton::ResultType::TO_ABORT) {
    Panic("This case should not be getting triggered");
    Notice("Peloton Aborted (TO_ABORT) [%d:%d:%d]: %s", client_id, tx_id, req_id, error_msg.c_str());
    reply->set_status(REPLY_FAIL);
    reply->set_sql_res("");
    //Call Abort.
    table_store->Abort(client_id, tx_id);
  }
  else{
      Panic("Unexpected Peloton result type: %d. Error msg: %s", result_status, error_msg.c_str());
  }
  

  //mark current tx as inactive
  if(terminate) c->second.TerminateTX();

  Debug("Finished SQL RPC [%d:%d:%d] (client, tx_id, req_id)", client_id, tx_id, req_id);
  return reply;
}

::google::protobuf::Message* Server::HandleTryCommit(ClientStateMap::accessor &c, uint64_t req_id, uint64_t client_id, uint64_t tx_id) {
  Debug("Trying to commit a txn %d", req_id);
 
  proto::TryCommitReply* reply = new proto::TryCommitReply();
  reply->set_req_id(req_id);
   
  auto result_type = table_store->Commit(client_id, tx_id);

  if (result_type == peloton_peloton::ResultType::SUCCESS) {
      Debug("TryCommit went through successfully.");
    reply->set_status(REPLY_OK);
  } else {
    Panic("Commit failed. Peloton Commit should always succeed, unless Txn failed earlier already... (in which case we shouldve already aborted TX)"); 
    reply->set_status(REPLY_FAIL);
  }
  
  c->second.TerminateTX();
  return reply;

}

::google::protobuf::Message* Server::HandleUserAbort(ClientStateMap::accessor &c, uint64_t client_id, uint64_t tx_id) {

  Debug("UserAbort: Rollback transaction");

  table_store->Abort(client_id, tx_id);

  c->second.TerminateTX();

  Debug("Return no reply for Abort");
  return nullptr;
}


////////////////////////////////////////////
/*          TABLE GENERATION LOGIC        */
////////////////////////////////////////////

//Register any table meta data we need. Specifically, we need to know the "type" of each column so we can generate an appropriate load statement for Peloton
void Server::RegisterTables(std::string &table_registry){ //TODO: This table registry file does not need to include the rows.

  Debug("Register tables from registry: %s", table_registry.c_str());
  if(table_registry.empty()) Panic("Did not provide a table_registry for a sql_bench");

  std::ifstream generated_tables(table_registry);
  json tables_to_load;
    try {
      tables_to_load = json::parse(generated_tables);
  }
  catch (const std::exception &e) {
    Panic("Failed to load Table JSON Schema");
  }
      
  //Load all tables. 
  for(auto &[table_name, table_args]: tables_to_load.items()){ 
    const std::vector<std::pair<std::string, std::string>> &column_names_and_types = table_args["column_names_and_types"];
  
    ColRegistry &col_registry = TableRegistry[table_name];
    Debug("Registering Table: %s", table_name.c_str());
    
    //register column types
    uint32_t i = 0;
    for(auto &[col_name, col_type]: column_names_and_types){
      (col_type == "TEXT" || col_type == "VARCHAR") ? col_registry.col_quotes.push_back(true) : col_registry.col_quotes.push_back(false);
    }
  }
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
  table_store->ExecSingle(sql_statement);
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

  table_store->ExecSingle(sql_statement);
}

//!!"Deprecated" (Unused)
void Server::LoadTableRow(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, const std::vector<std::string> &values, const std::vector<uint32_t> &primary_key_col_idx ){
    Panic("Deprecated");
}


static bool fine_grained_quotes = true;

std::string Server::GenerateLoadStatement(const std::string &table_name, const std::vector<std::vector<std::string>> &row_segment, int segment_no){
  const ColRegistry &col_registry = TableRegistry.at(table_name);
  
  std::string load_statement = fmt::format("INSERT INTO {0} VALUES ", table_name);

  for(auto &row: row_segment){
        load_statement += "(";
        if(fine_grained_quotes){ // Use this to add fine grained quotes:
            for(int i = 0; i < row.size(); ++i){
                if(col_registry.col_quotes[i])  load_statement += "\'" + row[i]  + "\'" + ", ";
                else load_statement += row[i] + ", ";
            }
        }
        else{ //Otherwise, just add quotes to everything
            for(auto &col_val: row){
                load_statement += "\'" + col_val  + "\'" + ", ";
            }
        }
    
        load_statement.resize(load_statement.length()-2); //remove trailing ", "
        load_statement += "), ";
    }
    load_statement.resize(load_statement.length()-2); //remove trailing ", "
    load_statement += ";";

    //Debug("Generate Load Statement for Table %s. Segment %d. Statement: %s", table_name.c_str(), segment_no, load_statement.substr(0, 1000).c_str());

    return load_statement;
}

static bool parallel_load = true; 
static int max_segment_size = 20000;//INT_MAX; //20000 seems to work well for TPC-C 1 warehouse and for Seats

void Server::LoadTableData(const std::string &table_name, const std::string &table_data_path, 
    const std::vector<std::pair<std::string, std::string>> &column_names_and_types, const std::vector<uint32_t> &primary_key_col_idx)
{

  //Call into TableStore with this statement.
  Notice("Load Data for Table %s from: %s", table_name.c_str(), table_data_path.c_str());

  auto f = [this, table_name, table_data_path, column_names_and_types, primary_key_col_idx](){
    Notice("Parsing Table on core %d", sched_getcpu());
    std::vector<row_segment_t*> table_row_segments = ParseTableDataFromCSV(table_name, table_data_path, column_names_and_types, primary_key_col_idx);

    Notice("Dispatch Table Loading for table: %s. Number of Segments: %d", table_name.c_str(), table_row_segments.size());
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
  f();
  // if(parallel_load){
  //     tp->DispatchTP_noCB(std::move(f)); //Dispatching this seems to add no perf
  //     tp->DispatchIndexedTP_noCB(thread_id,std::move(f));
  // }
  // else{
  //   f();
  // }
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
    }
    
    //avoid empty segments (could happen if we pop back a row from a segment)
    auto last_row_segment = table_row_segments.back();
    Debug("Table[%s]. Last segment size: %d.", table_name.c_str(), last_row_segment->size());
    if(last_row_segment->empty()){
      table_row_segments.pop_back();
      delete last_row_segment;
    }

    Notice("Loading %d rows for table %s", total_rows, table_name.c_str());

    return table_row_segments;
}

void Server::LoadTableRows(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types,
                           const row_segment_t *row_segment, const std::vector<uint32_t> &primary_key_col_idx, int segment_no, bool load_cc){

  Notice("Load %lu Table rows for: %s", row_segment->size(), table_name.c_str());

  auto f = [this, table_name, segment_no, row_segment](){
      Notice("Loading Table: %s [Segment: %d]. On core %d", table_name.c_str(), segment_no, sched_getcpu());

      table_store->ExecSingle(this->GenerateLoadStatement(table_name, *row_segment, segment_no));
      delete row_segment;
      Notice("Finished loading Table: %s [Segment: %d]. On core %d", table_name.c_str(), segment_no, sched_getcpu());
       return (void*) true;
    };
    // Call into ApplyTableWrites from different threads. On each Thread, it is a synchronous interface.

    if(parallel_load){
      //tp->DispatchTP_noCB(std::move(f)); 
      tp->DispatchIndexedTP_noCB(segment_no,f); //Use indexed threadpool because pelotonstore has no worker threads.
    }
    else{
      f();
    }
}






}
