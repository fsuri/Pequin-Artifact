/***********************************************************************
 *
 * store/pequinstore/table_store_interface_peloton.cc: 
 *      Implementation of a execution shim to pelton based backend.
 *
 * Copyright 2023 Florian Suri-Payer <fsp@cs.cornell.edu>
 *                Neil Giridharan <giridhn@berkeley.edu>
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

#include "store/pelotonstore/table_store.h"
#include "lib/assert.h"
#include "lib/message.h"
#include <algorithm>
#include <atomic>
#include <sched.h>
#include <utility>

#include "store/common/query_result/query_result.h"
#include "store/pequinstore/sql_interpreter.h"

namespace pelotonstore {

std::string
GetResultValueAsString(const std::vector<peloton_peloton::ResultValue> &result, size_t index) {
  std::string value(result[index].begin(), result[index].end());
  return value;
}

void UtilTestTaskCallback(void *arg) {
  std::atomic_int *count = static_cast<std::atomic_int *>(arg);
  count->store(0);
}

void ContinueAfterComplete(std::atomic_int &counter_) {
  while (counter_.load() == 1) {
    Panic("should never get triggered now that TrafficCop executions are no longer asynchronous");
    usleep(2); // TODO: Instead of busy looping turn this into a callback. Note:
               // in that case it's tricky to manage that the tpool will
               // schedule the CB to the right core
               //  => Use c++ async programming? promise.wait?
  }
}

///////////////////// CLASS FUNCTIONS ///////////////////////////

//TODO: Need num_threads traffic cops for ExecRaw.
//TODO: Need traffic cop per client.

TableStore::TableStore(int num_threads) : unnamed_statement("unnamed"), unnamed_variable(false) {
  // Init Peloton default DB
  Init(num_threads);
}

TableStore::~TableStore() {

  for(auto &[tcop, counter]: traffic_cops_){
    delete tcop;
    delete counter;
  }
  for(auto &[_, cop_pair]: client_cop){
    delete cop_pair.first;
    delete cop_pair.second;
  }

  Latency_t sum_read;
  _Latency_Init(&sum_read, "total_read");
  for (unsigned int i = 0; i < readLats.size(); i++) {
    Latency_Sum(&sum_read, &readLats[i]);
  }
  Latency_t sum_write;
  _Latency_Init(&sum_write, "total_write");
  for (unsigned int i = 0; i < writeLats.size(); i++) {
    Latency_Sum(&sum_write, &writeLats[i]);
  }
  Latency_t sum_snapshot;
  _Latency_Init(&sum_snapshot, "total_snapshot");
  for (unsigned int i = 0; i < snapshotLats.size(); i++) {
    Latency_Sum(&sum_snapshot, &snapshotLats[i]);
  }
   //Note: If using eagerPlusSnapshot mode: total_readLat - total_snapshotLat = materializedReadLat

  Latency_Dump(&sum_read);
  Latency_Dump(&sum_write);
  Latency_Dump(&sum_snapshot);
}

void TableStore::Init(int num_threads) {
  // Init Peloton default DB
  auto &txn_manager = peloton_peloton::concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  peloton_peloton::catalog::Catalog::GetInstance()->CreateDatabase(txn, DEFAULT_DB_NAME);
  txn_manager.CommitTransaction(txn);
  // traffic_cop_ = peloton::tcop::TrafficCop(UtilTestTaskCallback, &counter_);

  //peloton_peloton::catalog::Catalog::GetInstance()->SetQueryParams(query_params); //Bootstrap Catalog

  peloton_peloton::optimizer::StatsStorage::GetInstance(); //Force early creation of pg_column_stats

  if (num_threads > 0) {
    for (int i = 0; i < num_threads; i++) {
      std::atomic_int *counter = new std::atomic_int();
      peloton_peloton::tcop::TrafficCop *new_cop = new peloton_peloton::tcop::TrafficCop(UtilTestTaskCallback, counter);
      traffic_cops_.push_back({new_cop, counter});
    }
  }

 Notice("Instantiated %d traffic cop(s)", traffic_cops_.size()); 
  for (int i = 0; i < num_threads; ++i) {
    Latency_t readLat;
    Latency_t writeLat;
    Latency_t snapshotLat;
    readLats.push_back(readLat);
    writeLats.push_back(writeLat);
    snapshotLats.push_back(snapshotLat);
    _Latency_Init(&readLats.back(), "read");
    _Latency_Init(&writeLats.back(), "write");
    _Latency_Init(&snapshotLats.back(), "snapshotting");
  }
}

//Get the traffic cop designated for this thread. //NOTE: Only use this for Single statement Txns.
std::pair<peloton_peloton::tcop::TrafficCop *, std::atomic_int *> TableStore::GetCop() {
  int t_id = sched_getcpu();
  if (t_id >= traffic_cops_.size()) {
      Panic("Not enough traffic cops allocated for the number of cores: %d, requested: %d", traffic_cops_.size(), t_id);
  }
  Debug("Using Traffic Cop: %d", t_id);
  return traffic_cops_.at(t_id);
}

//Get the traffic cop designated for this client.
std::pair<peloton_peloton::tcop::TrafficCop *, std::atomic_int *> TableStore::GetClientCop(uint64_t client_id, uint64_t tx_id) {
  
  auto itr = client_cop.find(client_id);
  if(itr == client_cop.end()){
    //Create new cop
    std::atomic_int *counter = new std::atomic_int();
    peloton_peloton::tcop::TrafficCop *new_cop = new peloton_peloton::tcop::TrafficCop(UtilTestTaskCallback, counter);
    std::pair<peloton_peloton::tcop::TrafficCop *, std::atomic_int *>cop_pair = {new_cop, counter};
    client_cop.insert({client_id, cop_pair});
    return cop_pair;
  }
  return itr->second;
}

////////////////  Helper Functions //////////////////////////
std::shared_ptr<peloton_peloton::Statement> TableStore::ParseAndPrepare(const std::string &query_statement, peloton_peloton::tcop::TrafficCop *tcop, bool skip_cache) {

  std::shared_ptr<peloton_peloton::Statement> statement;

  UW_ASSERT(!query_statement.empty());
  //Debug("Beginning of parse and prepare: %s", query_statement.substr(0, 1000).c_str());
  //Warning("Beginning of parse and prepare: %s", query_statement.substr(0, 1000).c_str());
  // prepareStatement
  auto &peloton_parser = peloton_peloton::parser::PostgresParser::GetInstance();
  try{
    auto sql_stmt_list = peloton_parser.BuildParseTree(query_statement);
    UW_ASSERT(sql_stmt_list);
    // PELOTON_ASSERT(sql_stmt_list);
    if (!sql_stmt_list->is_valid) {
      Panic("SQL command not valid: %s", query_statement.substr(0, 1000).c_str()); // return peloton::ResultType::FAILURE;
    }
    //Debug("Parsed statement successfully, beginning prepare. [%s]", query_statement.substr(0, 1000).c_str());
    statement = tcop->PrepareStatement(unnamed_statement, query_statement, std::move(sql_stmt_list), skip_cache);
    if (statement.get() == nullptr) {
      tcop->setRowsAffected(0);
      Panic("SQL command not valid: %s", query_statement.size() < 1000? query_statement.c_str() : 
          (query_statement.substr(0, 500) + " ... " + query_statement.substr(query_statement.size()-500)).c_str()); // return peloton::ResultType::FAILURE;
    }
  }
  catch(...){
    Panic("Exception parse/preparing query: %s", query_statement.substr(0, 1000).c_str());
  }

  //Debug("Finished preparing statement: %s", query_statement.substr(0, 1000).c_str());
  return statement;
}

//TODO: FIXME: Now that we sidestep the Peloton WorkerPool all of this isn't really necessary anymore => it's not a synchronous blocking execution, so we know that when we get here it is done.
void TableStore::GetResult(peloton_peloton::ResultType &status, uint64_t &rows_affected, peloton_peloton::tcop::TrafficCop *tcop, std::atomic_int *c) {
  if (tcop->GetQueuing()) {
    ContinueAfterComplete(*c);  // busy loop until result is ready. 
    tcop->ExecuteStatementPlanGetResult();
    status = tcop->ExecuteStatementGetResult();
    rows_affected = tcop->getRowsAffected();
    tcop->SetQueuing(false);
  }
  //If Not Queuing: status mustve been != Success already
}

//Transform PelotonResult into a ProtoWrapper
std::string TableStore::TransformResult(peloton_peloton::ResultType &status, std::shared_ptr<peloton_peloton::Statement> statement, std::vector<peloton_peloton::ResultValue> &result, uint64_t rows_affected) {

  std::vector<peloton_peloton::FieldInfo> tuple_descriptor;
  if (status == peloton_peloton::ResultType::SUCCESS) {
    tuple_descriptor = statement->GetTupleDescriptor();
    Debug("Query Read successful. ");
  } else {
    Debug("Query read failure");
    return ""; 
  }

  sql::QueryResultProtoBuilder queryResultBuilder;

  Debug("Rows affected: %d", rows_affected);
  queryResultBuilder.set_rows_affected(rows_affected);

  //If there are no rows read (e.g. for Write queries) return immediately
  if(tuple_descriptor.empty()){
    Debug("Serialize QueryResult. Tuple descriptor empty (must be a Write query)");
   return queryResultBuilder.get_result(false)->SerializeAsString();
  }

  Debug("Tuple descriptor size: %d", tuple_descriptor.size());
  // Add columns
  for (unsigned int i = 0; i < tuple_descriptor.size(); i++) {
    std::string column_name = std::get<0>(tuple_descriptor[i]);
    queryResultBuilder.add_column(column_name);
  }
  // Add rows
  unsigned int rows = result.size() / tuple_descriptor.size();
  Debug("Num rows: %d", rows);
  for (unsigned int i = 0; i < rows; i++) {
  
    Debug("Row[%d]", i);
  
    RowProto *row = queryResultBuilder.new_row();
    for (unsigned int j = 0; j < tuple_descriptor.size(); j++) {
      // Use interface addtorow, and pass in field to that row
      Debug("   Col: %s. Value: %s", std::get<0>(tuple_descriptor[j]).c_str(), result[i * tuple_descriptor.size() + j].c_str());
      queryResultBuilder.AddToRow(row, result[i * tuple_descriptor.size() + j]);
    }
  }

  bool already_sorted = false;
  //Check if Statement contains OrderBY, if so, don't sort!! Result already has a sorted order
  if(statement->GetQueryString().find("ORDER BY")!= std::string::npos) already_sorted = true;

  Debug("Serialize QueryResult");
  return queryResultBuilder.get_result(!already_sorted)->SerializeAsString();
}

/////////////////// INTERFACE FUNCTIONS //////////////////////////////

// Execute a statement (as single transaction) directly on the Table backend, no output -- This is used for Table Creation and Data loading
void TableStore::ExecSingle(const std::string &sql_statement, bool skip_cache) {
 
  //Debug("Beginning of exec raw. Statement: %s", sql_statement.substr(0, 1000).c_str());
  std::pair<peloton_peloton::tcop::TrafficCop *, std::atomic_int *> cop_pair = GetCop(); //Get the Cop for this thread.
 
  std::atomic_int *counter = cop_pair.second;
  peloton_peloton::tcop::TrafficCop *tcop = cop_pair.first;
  bool unamed;

  // prepareStatement
  auto statement = ParseAndPrepare(sql_statement, tcop, skip_cache);

  // ExecuteStatment
  std::vector<peloton_peloton::type::Value> param_values;
  std::vector<peloton_peloton::ResultValue> result;
  std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);
  uint64_t rows_affected;

  counter->store(1);
  auto status = tcop->ExecuteStatement(statement, param_values, unamed, result_format, result);

  Debug("Made it after status");
  // GetResult(status);
  GetResult(status, rows_affected, tcop, counter);

  if (status == peloton_peloton::ResultType::SUCCESS)
    Debug("RawExec success");
  else
    Panic("RawExec failure. Table Loading should always succeed!");
}


//Execute SQL Statement on backend. Use the traffic cop associated with the client (and its ongoing transaction)
std::string TableStore::ExecTransactional(const std::string &sql_statement, uint64_t client_id, uint64_t tx_id, peloton_peloton::ResultType &result_status, std::string &error_msg, bool skip_cache) {
 
  Debug("Beginning of transactional Statement: %s", sql_statement.c_str());
  std::pair<peloton_peloton::tcop::TrafficCop *, std::atomic_int *> cop_pair = GetClientCop(client_id, tx_id);
 
  std::atomic_int *counter = cop_pair.second;
  peloton_peloton::tcop::TrafficCop *tcop = cop_pair.first;
  bool unamed;

  // prepareStatement
  //std::cerr << "Before parse and prepare" << std::endl;
  auto statement = ParseAndPrepare(sql_statement, tcop, skip_cache);

  // ExecuteStatment
  std::vector<peloton_peloton::type::Value> param_values;
  std::vector<peloton_peloton::ResultValue> result;
  std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);
  uint64_t rows_affected;

  counter->store(1);
  auto status = tcop->ExecuteStatement(statement, param_values, unamed, result_format, result); //status will be == Queuing (unless Txn already failed, then it might be Aborting)

  Debug("Made it after status");
  // GetResult(status);
  GetResult(status, rows_affected, tcop, counter);  //If result was queuing => fetch final result. If result was not queuing, result was already != sucess, so nothing needs to be done.
  result_status = status;

  if (status == peloton_peloton::ResultType::SUCCESS){
    Debug("RawExecResult success");
    std::string &&res(TransformResult(status, statement, result, rows_affected));
    return std::move(res);
  }
  else {
    error_msg = tcop->GetErrorMessage();
    return "";
  }
}

void TableStore::Begin(uint64_t client_id, uint64_t tx_id){
  Debug("Begin Transaction [%d:%d] (client, tx_id)", client_id, tx_id);
  std::pair<peloton_peloton::tcop::TrafficCop *, std::atomic_int *> cop_pair = GetClientCop(client_id, tx_id);
  peloton_peloton::tcop::TrafficCop *tcop = cop_pair.first;
  tcop->BeginQueryHelper(0); //TODO: Pass a thread id?
}

peloton_peloton::ResultType TableStore::Commit(uint64_t client_id, uint64_t tx_id){
  Debug("Try to Commit Transaction[%d:%d] (client, tx_id)", client_id, tx_id);
  std::pair<peloton_peloton::tcop::TrafficCop *, std::atomic_int *> cop_pair = GetClientCop(client_id, tx_id);
  peloton_peloton::tcop::TrafficCop *tcop = cop_pair.first;
  return tcop->CommitQueryHelper(); 
}

void TableStore::Abort(uint64_t client_id, uint64_t tx_id){
   Debug("Try to Abort Transaction[%d:%d] (client, tx_id)", client_id, tx_id);
  std::pair<peloton_peloton::tcop::TrafficCop *, std::atomic_int *> cop_pair = GetClientCop(client_id, tx_id);
  peloton_peloton::tcop::TrafficCop *tcop = cop_pair.first;
  tcop->AbortQueryHelper();
}

} // namespace pequinstore
