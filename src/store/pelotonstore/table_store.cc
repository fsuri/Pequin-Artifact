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
#include "store/pelotonstore/query-engine/traffic_cop/traffic_cop.h"
#include <algorithm>
#include <atomic>
#include <sched.h>
#include <utility>

#include "store/common/query_result/query_result.h"

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

PelotonTableStore::PelotonTableStore(int num_threads) : unnamed_statement("unnamed"), unnamed_variable(false) {
  // Init Peloton default DB
  Init(num_threads);
}

/*PelotonTableStore::PelotonTableStore(const QueryParameters *query_params, std::string &table_registry_path,
                                     find_table_version &&find_table_version,
                                     read_prepared_pred &&read_prepared_pred,
                                     int num_threads)
    : TableStore(query_params, table_registry_path, std::move(find_table_version), std::move(read_prepared_pred)), unnamed_statement("unnamed"), unnamed_variable(false) {
  // Init Peloton default DB
  Init(num_threads);
}*/

PelotonTableStore::~PelotonTableStore() {

  // Release all allocated cops
  size_t cop_count = traffic_cops.size_approx();
  while (cop_count) {
    cop_count--;

    std::pair<peloton_peloton::tcop::TrafficCop *, std::atomic_int *> cop_pair;
    bool found = traffic_cops.try_dequeue(cop_pair);

    if (found) {
      delete cop_pair.first;
      delete cop_pair.second;
    }
  }

  size_t cops_left = traffic_cops_.size();
  while (cops_left > 0) {
    std::pair<peloton_peloton::tcop::TrafficCop *, std::atomic_int *> cop_pair;
    cop_pair = traffic_cops_.back();
    traffic_cops_.pop_back();

    delete cop_pair.first;
    delete cop_pair.second;
    cops_left--;
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

void PelotonTableStore::Init(int num_threads) {
  // Init Peloton default DB
  auto &txn_manager =  peloton_peloton::concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  //peloton::catalog::Catalog::GetInstance()->CreateDatabase(txn, DEFAULT_DB_NAME);
  txn_manager.CommitTransaction(txn);
  // traffic_cop_ = peloton::tcop::TrafficCop(UtilTestTaskCallback, &counter_);

  peloton_peloton::catalog::Catalog::GetInstance()->SetQueryParams(query_params); //Bootstrap Catalog

  peloton_peloton::optimizer::StatsStorage::GetInstance(); //Force early creation of pg_column_stats


  if (num_threads > 0) {
    is_recycled_version_ = false;
    for (int i = 0; i < num_threads; i++) {
      std::atomic_int *counter = new std::atomic_int();
      //std::cerr << "create tcop: " << i << std::endl;
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

////////////////  Helper Functions //////////////////////////
std::shared_ptr<peloton_peloton::Statement>
PelotonTableStore::ParseAndPrepare(const std::string &query_statement, peloton_peloton::tcop::TrafficCop *tcop, bool skip_cache) {

  //TESTING HOW LONG THIS TAKES: FIXME: REMOVE 
  // struct timespec ts_start;
  // clock_gettime(CLOCK_MONOTONIC, &ts_start);
  // uint64_t microseconds_start = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;

  ///////////

  std::shared_ptr<peloton::Statement> statement;

  UW_ASSERT(!query_statement.empty());
  Debug("Beginning of parse and prepare: %s", query_statement.substr(0, 1000).c_str());
  //Warning("Beginning of parse and prepare: %s", query_statement.substr(0, 1000).c_str());
  // prepareStatement
  auto &peloton_parser = peloton::parser::PostgresParser::GetInstance();
  try{
    auto sql_stmt_list = peloton_parser.BuildParseTree(query_statement);
    UW_ASSERT(sql_stmt_list);
    // PELOTON_ASSERT(sql_stmt_list);
    if (!sql_stmt_list->is_valid) {
      Panic("SQL command not valid: %s", query_statement.substr(0, 1000).c_str()); // return peloton::ResultType::FAILURE;
    }
    Debug("Parsed statement successfully, beginning prepare. [%s]", query_statement.substr(0, 1000).c_str());
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


  Debug("Finished preparing statement: %s", query_statement.substr(0, 1000).c_str());

  //TESTING HOW LONG THIS TAKES: FIXME: REMOVE 
  // clock_gettime(CLOCK_MONOTONIC, &ts_start);
  // uint64_t microseconds_end = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;
 
  // //Should not take more than 1 ms (already generous) to parse and prepare.
  // auto duration = microseconds_end - microseconds_start;
  // Warning("ParseAndPrepare duration: %d. Q[%s]", duration, query_statement.substr(0, 1000).c_str());
  // if(duration > 1000){
  //   if(size_t insert_pos = query_statement.find("INSERT"); insert_pos != std::string::npos) Warning("ParseAndPrepare[%s] exceeded 1000us (INSERT): %d", query_statement.substr(0, 1000).c_str(), duration);
  //   else Warning("ParseAndPrepare exceeded 1000us (SELECT): %d. Q[%s]", duration, query_statement.substr(0, 1000).c_str());
  // }
  /////////////

  return statement;
}

//TODO: FIXME: Now that we sidestep the Peloton WorkerPool all of this isn't really necessary anymore => it's not a synchronous blocking execution, so we know that when we get here it is done.
void PelotonTableStore::GetResult(peloton_peloton::ResultType &status, peloton_peloton::tcop::TrafficCop *tcop, std::atomic_int *c) {
  // busy loop until result is ready. TODO: Change into callback style to avoid
  // busy loop.
  if (tcop->GetQueuing()) {
    ContinueAfterComplete(*c);
    tcop->ExecuteStatementPlanGetResult(); //This line does not seem to be necessary. Our Queries are not transactional inside Peloton, so no need to "Commit" them.
    status = tcop->ExecuteStatementGetResult();
    tcop->SetQueuing(false);
  }
}

// std::string
// PelotonTableStore::TransformResult(std::vector<peloton::FieldInfo>
// &tuple_descriptor, std::vector<peloton::ResultValue> &result){
std::string PelotonTableStore::TransformResult(peloton_peloton::ResultType &status, std::shared_ptr<peloton_peloton::Statement> statement, std::vector<peloton_peloton::ResultValue> &result) {

  std::vector<peloton_peloton::FieldInfo> tuple_descriptor;
  if (status == peloton_peloton::ResultType::SUCCESS) {
    tuple_descriptor = statement->GetTupleDescriptor();
    Debug("Query Read successful. ");
  } else {
    Panic("Query read failure");
    return ""; //return empty string. (empty result)
    //Panic("Query read failure");
  }

  //UW_ASSERT(result.size());

  // bool is_limit = statement->GetQueryString().find(limit_hook) != std::string::npos;
  // if(is_limit) Notice("Transforming Limit statement: %s", statement->GetQueryString().c_str());

  sql::QueryResultProtoBuilder queryResultBuilder;
  // Add columns
  for (unsigned int i = 0; i < tuple_descriptor.size(); i++) {
    std::string column_name = std::get<0>(tuple_descriptor[i]);
    queryResultBuilder.add_column(column_name);
  }
  // Add rows
  unsigned int rows = result.size() / tuple_descriptor.size();
  for (unsigned int i = 0; i < rows; i++) {
    //std::cerr << "Row[" << i << "]" << std::endl;
    Debug("Row[%d]", i);
    //if(is_limit) Notice("Row[%d]", i);

    RowProto *row = queryResultBuilder.new_row();
    for (unsigned int j = 0; j < tuple_descriptor.size(); j++) {
      // Use interface addtorow, and pass in field to that row
      //std::string r = result[i * tuple_descriptor.size() + j];
       Debug("   Col: %s. Value: %s", std::get<0>(tuple_descriptor[j]).c_str(), result[i * tuple_descriptor.size() + j].c_str());
      //if(is_limit) Notice("   Col: %s. Value: %s", std::get<0>(tuple_descriptor[j]).c_str(), result[i * tuple_descriptor.size() + j].c_str());
      queryResultBuilder.AddToRow(row, result[i * tuple_descriptor.size() + j]);
      //std::cerr << "  Col: " << (std::get<0>(tuple_descriptor[j]))<< ". Value: " << (result[i * tuple_descriptor.size() + j]) << std::endl;
    }
  }

  //std::cerr << statement->GetQueryString() << std::endl;
  bool already_sorted = false;
  //Check if Statement contains OrderBY, if so, don't sort!! Result already has a sorted order
  if(statement->GetQueryString().find(order_hook)!= std::string::npos) already_sorted = true;
  return queryResultBuilder.get_result(!already_sorted)->SerializeAsString();
}

std::pair<peloton_peloton::tcop::TrafficCop *, std::atomic_int *> PelotonTableStore::GetCop() {
  if (!is_recycled_version_) {
    int t_id = sched_getcpu();
    // std::cerr << "Thread id is " << t_id << std::endl;
    if (t_id >= traffic_cops_.size()) {
      Panic("Not enough traffic cops allocated for the number of cores: %d, requested: %d", traffic_cops_.size(), t_id);
    }
    Debug("Using Traffic Cop: %d", t_id);
    return traffic_cops_.at(t_id);
  } else {
    Debug("Using un-used Traffic Cop");
    return GetUnusedTrafficCop();
  }
}

std::pair<peloton_peloton::tcop::TrafficCop *, std::atomic_int *> PelotonTableStore::GetUnusedTrafficCop() {
  std::pair<peloton_peloton::tcop::TrafficCop *, std::atomic_int *> cop_pair;
  bool found = traffic_cops.try_dequeue(cop_pair);

  if (found) {
    cop_pair.first->Reset(); // Reset traffic cop
    return cop_pair;
  }

  std::atomic_int *counter = new std::atomic_int();
  peloton_peloton::tcop::TrafficCop *new_cop = new peloton_peloton::tcop::TrafficCop(UtilTestTaskCallback, counter);
  return {new_cop, counter};
}

void PelotonTableStore::ReleaseTrafficCop(std::pair<peloton_peloton::tcop::TrafficCop *, std::atomic_int *> cop_pair) {
  traffic_cops.enqueue(cop_pair);
}

/////////////////// INTERFACE FUNCTIONS //////////////////////////////

// Execute a statement directly on the Table backend, no questions asked, no
// output
void PelotonTableStore::ExecRaw(const std::string &sql_statement, bool skip_cache) {
  // Execute on Peloton  //Note -- this should be a synchronous call. I.e.
  // ExecRaw should not return before the call is done.

  Debug("Beginning of exec raw. Statement: %s", sql_statement.c_str());
  std::pair<peloton_peloton::tcop::TrafficCop *, std::atomic_int *> cop_pair = GetCop();
  //std::cerr << "Got the cop" << std::endl;

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

  // counter_.store(1); // SetTrafficCopCounter();
  /*auto status = traffic_cop_.ExecuteStatement(statement, param_values, unnamed, result_format, result);*/

  counter->store(1);
  auto status = tcop->ExecuteStatement(statement, param_values, unamed, result_format, result);

  Debug("Made it after status");
  // GetResult(status);
  GetResult(status, tcop, counter);

  if (status == peloton_peloton::ResultType::SUCCESS)
    Debug("RawExec success");
  else
    Panic("RawExec failure");
}

// Execute a statement directly on the Table backend, no questions asked, no
// output
std::string PelotonTableStore::ExecRawResult(const std::string &sql_statement, peloton_peloton::ResultType &result_status, bool skip_cache) {
  // Execute on Peloton  //Note -- this should be a synchronous call. I.e.
  // ExecRaw should not return before the call is done.

  Debug("Beginning of exec raw. Statement: %s", sql_statement.c_str());
  std::pair<peloton_peloton::tcop::TrafficCop *, std::atomic_int *> cop_pair = GetCop();
  //std::cerr << "Got the cop" << std::endl;

  std::atomic_int *counter = cop_pair.second;
  peloton_peloton::tcop::TrafficCop *tcop = cop_pair.first;
  bool unamed;

  // prepareStatement
  //std::cerr << "Before parse and prepare" << std::endl;
  auto statement = ParseAndPrepare(sql_statement, tcop, skip_cache);

  // ExecuteStatment
  std::vector<peloton::type::Value> param_values;
  std::vector<peloton::ResultValue> result;
  std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);

  // counter_.store(1); // SetTrafficCopCounter();
  /*auto status = traffic_cop_.ExecuteStatement(statement, param_values, unnamed, result_format, result);*/

  counter->store(1);
  auto status = tcop->ExecuteStatement(statement, param_values, unamed, result_format, result);

  Debug("Made it after status");
  // GetResult(status);
  GetResult(status, tcop, counter);
  result_status = status;

  if (status == peloton_peloton::ResultType::SUCCESS)
    Debug("RawExecResult success");
    std::string &&res(TransformResult(status, statement, result));
    return std::move(res);
  else
    return "";
}

} // namespace pequinstore
