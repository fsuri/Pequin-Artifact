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

#include "store/pequinstore/table_store_interface_peloton.h"
#include "lib/assert.h"
#include "lib/message.h"
#include "store/pequinstore/query-engine/traffic_cop/traffic_cop.h"
#include <algorithm>
#include <atomic>
#include <sched.h>
#include <utility>

#include "store/common/query_result/query_result.h"

namespace pequinstore {

std::string
GetResultValueAsString(const std::vector<peloton::ResultValue> &result, size_t index) {
  std::string value(result[index].begin(), result[index].end());
  return value;
}

void UtilTestTaskCallback(void *arg) {
  std::atomic_int *count = static_cast<std::atomic_int *>(arg);
  count->store(0);
}

void ContinueAfterComplete(std::atomic_int &counter_) {
  while (counter_.load() == 1) {
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

PelotonTableStore::PelotonTableStore(std::string &table_registry_path,
                                     find_table_version &&find_table_version,
                                     read_prepared_pred &&read_prepared_pred,
                                     int num_threads)
    : TableStore(table_registry_path, std::move(find_table_version), std::move(read_prepared_pred)), unnamed_statement("unnamed"), unnamed_variable(false) {
  // Init Peloton default DB
  Init(num_threads);
}

PelotonTableStore::~PelotonTableStore() {

  // Release all allocated cops
  size_t cop_count = traffic_cops.size_approx();
  while (cop_count) {
    cop_count--;

    std::pair<peloton::tcop::TrafficCop *, std::atomic_int *> cop_pair;
    bool found = traffic_cops.try_dequeue(cop_pair);

    if (found) {
      delete cop_pair.first;
      delete cop_pair.second;
    }
  }

  size_t cops_left = traffic_cops_.size();
  while (cops_left > 0) {
    std::pair<peloton::tcop::TrafficCop *, std::atomic_int *> cop_pair;
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
  auto &txn_manager =  peloton::concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  peloton::catalog::Catalog::GetInstance()->CreateDatabase(txn, DEFAULT_DB_NAME);
  txn_manager.CommitTransaction(txn);
  // traffic_cop_ = peloton::tcop::TrafficCop(UtilTestTaskCallback, &counter_);

  if (num_threads > 0) {
    is_recycled_version_ = false;
    for (int i = 0; i < num_threads; i++) {
      std::atomic_int *counter = new std::atomic_int();
      peloton::tcop::TrafficCop *new_cop = new peloton::tcop::TrafficCop(UtilTestTaskCallback, counter);
      traffic_cops_.push_back({new_cop, counter});
    }
  }

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
std::shared_ptr<peloton::Statement>
PelotonTableStore::ParseAndPrepare(const std::string &query_statement, peloton::tcop::TrafficCop *tcop) {

  UW_ASSERT(!query_statement.empty());
  Debug("Beginning of parse and prepare: %s", query_statement.substr(0, 1000).c_str());
  // prepareStatement
  auto &peloton_parser = peloton::parser::PostgresParser::GetInstance();
  auto sql_stmt_list = peloton_parser.BuildParseTree(query_statement);
  UW_ASSERT(sql_stmt_list);
  // PELOTON_ASSERT(sql_stmt_list);
  if (!sql_stmt_list->is_valid) {
    Panic("SQL command not valid: %s", query_statement.substr(0, 1000).c_str()); // return peloton::ResultType::FAILURE;
  }
  Debug("Parses successfully, beginning prepare");
  auto statement = tcop->PrepareStatement(unnamed_statement, query_statement, std::move(sql_stmt_list));
  if (statement.get() == nullptr) {
    tcop->setRowsAffected(0);
    Panic("SQL command not valid: %s", query_statement.size() < 1000? query_statement.c_str() : 
        (query_statement.substr(0, 500) + " ... " + query_statement.substr(query_statement.size()-500)).c_str()); // return peloton::ResultType::FAILURE;
  }
  Debug("Finished preparing statement: %s", query_statement.substr(0, 1000).c_str());
  return statement;
}

void PelotonTableStore::GetResult(peloton::ResultType &status, peloton::tcop::TrafficCop *tcop, std::atomic_int *c) {
  // busy loop until result is ready. TODO: Change into callback style to avoid
  // busy loop.
  if (tcop->GetQueuing()) {
    ContinueAfterComplete(*c);
    tcop->ExecuteStatementPlanGetResult();
    status = tcop->ExecuteStatementGetResult();
    tcop->SetQueuing(false);
  }
}

// std::string
// PelotonTableStore::TransformResult(std::vector<peloton::FieldInfo>
// &tuple_descriptor, std::vector<peloton::ResultValue> &result){
std::string PelotonTableStore::TransformResult(peloton::ResultType &status, std::shared_ptr<peloton::Statement> statement, std::vector<peloton::ResultValue> &result) {

  std::vector<peloton::FieldInfo> tuple_descriptor;
  if (status == peloton::ResultType::SUCCESS) {
    tuple_descriptor = statement->GetTupleDescriptor();
    Debug("Query Read successful. ");
  } else {
    return ""; //return empty string. (empty result)
    //Panic("Query read failure");
  }

  //UW_ASSERT(result.size());

  sql::QueryResultProtoBuilder queryResultBuilder;
  // Add columns
  for (unsigned int i = 0; i < tuple_descriptor.size(); i++) {
    std::string column_name = std::get<0>(tuple_descriptor[i]);
    queryResultBuilder.add_column(column_name);
  }
  // Add rows
  unsigned int rows = result.size() / tuple_descriptor.size();
  for (unsigned int i = 0; i < rows; i++) {
    std::cerr << "Row[" << i << "]" << std::endl;
    RowProto *row = queryResultBuilder.new_row();
    for (unsigned int j = 0; j < tuple_descriptor.size(); j++) {
      // Use interface addtorow, and pass in field to that row
      //std::string r = result[i * tuple_descriptor.size() + j];
      queryResultBuilder.AddToRow(row, result[i * tuple_descriptor.size() + j]);
      std::cerr << "  Col: " << (std::get<0>(tuple_descriptor[j]))<< ". Value: " << (result[i * tuple_descriptor.size() + j]) << std::endl;
    }
  }

  //std::cerr << statement->GetQueryString() << std::endl;
  bool already_sorted = false;
  //Check if Statement contains OrderBY, if so, don't sort!! Result already has a sorted order
  if(statement->GetQueryString().find(order_hook)!= std::string::npos) already_sorted = true;
  return queryResultBuilder.get_result(!already_sorted)->SerializeAsString();
}

std::pair<peloton::tcop::TrafficCop *, std::atomic_int *> PelotonTableStore::GetCop() {
  if (!is_recycled_version_) {
    int t_id = sched_getcpu();
    // std::cout << "Thread id is " << t_id << std::endl;
    if (t_id >= traffic_cops_.size()) {
      Panic("Not enough traffic cops allocated for the number of cores");
    }
    Debug("Using Traffic Cop: %d", t_id);
    return traffic_cops_.at(t_id);
  } else {
    Debug("Using un-used Traffic Cop");
    return GetUnusedTrafficCop();
  }
}

std::pair<peloton::tcop::TrafficCop *, std::atomic_int *> PelotonTableStore::GetUnusedTrafficCop() {
  std::pair<peloton::tcop::TrafficCop *, std::atomic_int *> cop_pair;
  bool found = traffic_cops.try_dequeue(cop_pair);

  if (found) {
    cop_pair.first->Reset(); // Reset traffic cop
    return cop_pair;
  }

  std::atomic_int *counter = new std::atomic_int();
  peloton::tcop::TrafficCop *new_cop = new peloton::tcop::TrafficCop(UtilTestTaskCallback, counter);
  return {new_cop, counter};
}

void PelotonTableStore::ReleaseTrafficCop(std::pair<peloton::tcop::TrafficCop *, std::atomic_int *> cop_pair) {
  traffic_cops.enqueue(cop_pair);
}

/////////////////// INTERFACE FUNCTIONS //////////////////////////////

// Execute a statement directly on the Table backend, no questions asked, no
// output
void PelotonTableStore::ExecRaw(const std::string &sql_statement) {
  // Execute on Peloton  //Note -- this should be a synchronous call. I.e.
  // ExecRaw should not return before the call is done.

  std::cout << "Beginning of exec raw. Statement: " << sql_statement << std::endl;
  std::pair<peloton::tcop::TrafficCop *, std::atomic_int *> cop_pair = GetCop();
  std::cout << "Got the cop" << std::endl;

  std::atomic_int *counter = cop_pair.second;
  peloton::tcop::TrafficCop *tcop = cop_pair.first;
  bool unamed;

  // prepareStatement
  std::cout << "Before parse and prepare" << std::endl;
  auto statement = ParseAndPrepare(sql_statement, tcop);

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

  if (status == peloton::ResultType::SUCCESS)
    Debug("RawExec success");
  else
    Debug("RawExec failure");
}

// void PelotonTableStore::LoadTable(const std::string &load_statement, const std::string &txn_digest, const Timestamp &ts, const proto::CommittedProof *committedProof) {
//   // When calling the LoadStatement: We'll want to initialize all rows to be committed and have genesis proof (see server) Call statement (of type Copy
//   // or Insert) and set meta data accordingly (bool commit = true, committedProof, txn_digest, ts)

//   std::pair<peloton::tcop::TrafficCop *, std::atomic_int *> cop_pair = GetCop();

//   std::atomic_int *counter = cop_pair.second;
//   peloton::tcop::TrafficCop *tcop = cop_pair.first;
//   bool unamed;

//   std::shared_ptr<std::string> txn_dig(std::make_shared<std::string>( txn_digest)); // Turn txn_digest into a shared_ptr, write everywhere it is
//                     // needed.

//   // execute the query using tcop
//   // prepareStatement
//   auto statement = ParseAndPrepare(load_statement, tcop);

//   // ExecuteStatment
//   std::vector<peloton::type::Value> param_values;
//   std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);
//   std::vector<peloton::ResultValue> result;

//   counter->store(1); // SetTrafficCopCounter();
//   auto status = tcop->ExecuteStatement(statement, param_values, unamed, result_format, result);

//   GetResult(status, tcop, counter);

//   if (status == peloton::ResultType::SUCCESS)
//     Debug("Load success");
//   else
//     Debug("RawExec failure");
// }

void PelotonTableStore::LoadTable(const std::string &load_statement, const std::string &txn_digest, const Timestamp &ts, const proto::CommittedProof *committedProof) {
  // When calling the LoadStatement: We'll want to initialize all rows to be committed and have genesis proof (see server) Call statement (of type Copy
  // or Insert) and set meta data accordingly (bool commit = true, committedProof, txn_digest, ts)

   int core = sched_getcpu();
  Debug("Begin writeLat on core: %d", core); //TODO: Change to Load Latency
  Latency_Start(&writeLats[core]);

  UW_ASSERT(ts == Timestamp(committedProof->txn().timestamp()));
  UW_ASSERT(ts.getTimestamp() == 0 && ts.getID() == 0); //loading genesis TS

  Debug("Load Table with genesis txn %s. TS [%lu:%lu]", BytesToHex(txn_digest, 16).c_str(), ts.getTimestamp(), ts.getID());

  std::pair<peloton::tcop::TrafficCop *, std::atomic_int *> cop_pair = GetCop();

  std::atomic_int *counter = cop_pair.second;
  peloton::tcop::TrafficCop *tcop = cop_pair.first;
  bool unamed;

  // Turn txn_digest into a shared_ptr, write everywhere it is needed.
  std::shared_ptr<std::string> txn_dig(std::make_shared<std::string>(txn_digest));

  // Execute Writes on Peloton
  std::vector<peloton::ResultValue> result;

 
  //Notice("Write statement: %s", write_statement.substr(0, 1000).c_str());

  Debug("Load statement: %s", load_statement.substr(0, 1000).c_str());
  
  // prepareStatement
  auto statement = ParseAndPrepare(load_statement, tcop);

  // ExecuteStatment
  std::vector<peloton::type::Value> param_values;
  std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);

  counter->store(1);
  auto status = tcop->ExecuteWriteStatement(statement, param_values, unamed, result_format, result, ts, txn_dig, committedProof, true, false); //commit = true, materialize

  // GetResult(status);
  GetResult(status, tcop, counter);

  if (status == peloton::ResultType::SUCCESS)
    Debug("Write successful");
  else
    Panic("Write failure"); //Our writes are "no questions asked". They do not respect Insert/Update/Delete semantics -- those are enforced by our CC layer.
                            //Thus our writes should always succeed.
  

  Debug("End writeLat on core: %d", core);
  // Debug("getCPU says on core: %d", sched_getcpu());
  // UW_ASSERT(core == sched_getcpu());
  Latency_End(&writeLats[core]);

}

// Execute a read query statement on the Table backend and return a query_result/proto (in serialized form) as well as a read set (managed by readSetMgr)
std::string PelotonTableStore::ExecReadQuery(const std::string &query_statement, const Timestamp &ts, QueryReadSetMgr &readSetMgr) {

  // UW_ASSERT(ts.getTimestamp() >= 0 && ts.getID() >= 0);

  // std::string stmt = "set enable_hashjoin = OFF";
  // ExecRaw(stmt);

  Debug("Execute ReadQuery: %s. TS: [%lu:%lu]", query_statement.c_str(), ts.getTimestamp(), ts.getID());

  int core = sched_getcpu();
  Debug("Begin readLat on core: %d", core);
  Latency_Start(&readLats[core]);

  // Execute on Peloton (args: query, Ts, readSetMgr, this->can_read_prepared, this->set_table_version) --> returns peloton result --> transform into protoResult

  // TRY TO CREATE SEPARATE TRAFFIC COP
  // auto [traffic_cop, counter] = GetUnusedTrafficCop();
  //  std::atomic_int counter;
  //  peloton::tcop::TrafficCop traffic_cop(UtilTestTaskCallback, &counter);
  // traffic_cop_.Reset();
  std::pair<peloton::tcop::TrafficCop *, std::atomic_int *> cop_pair = GetCop();

  std::atomic_int *counter = cop_pair.second;
  peloton::tcop::TrafficCop *tcop = cop_pair.first;
  bool unamed;

  // TRY TO SET A THREAD ID
  // size_t t_id = std::hash<std::thread::id>{}(std::this_thread::get_id()); //
  // % 4; std::cout << "################################# STARTING ExecReadQuery
  // ############################## on Thread: " < t_id << std::endl;

  // std::cout << query_statement << std::endl;

  // prepareStatement
  auto statement = ParseAndPrepare(query_statement, tcop);

  // ExecuteStatment
  std::vector<peloton::type::Value> param_values;
  std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);
  std::vector<peloton::ResultValue> result;

  // SetTrafficCopCounter();
  // counter_.store(1);
  counter->store(1);

  // execute the query using tcop
  auto status = tcop->ExecuteReadStatement(statement, param_values, unamed, result_format, result, &sql_interpreter, //sql_interpreter.GetTableRegistry(),
                                            ts, &this->record_table_version, &this->can_read_prepared, peloton::PequinMode::eagerRead, &readSetMgr);
  // auto status = tcop->ExecuteReadStatement(statement, param_values, unamed, result_format, result, ts, readSetMgr,
  //     this->record_table_version, this->can_read_prepared);

  // GetResult(status);
  GetResult(status, tcop, counter);

  // std::string result_string = TransformResult(status, statement, result);
  // //Testing:
  // //TODO: turn result into protowrapper and read it out.
  // query_result::QueryResult *res = new
  // sql::QueryResultProtoWrapper(result_string);
  // //TODO: what type are the result values? does the transformation shomehow
  // need to mention this.

  //   std::cerr << "IS empty?: " << (res->empty()) << std::endl;
  //   std::cerr << "num cols:" <<  (res->num_columns()) << std::endl;
  //   std::cerr << "num rows written:" <<  (res->rows_affected()) << std::endl;
  //   std::cerr << "num rows read:" << (res->size()) << std::endl;

  //  size_t nbytes;
  //  const char* out;
  // std::string output_row;

  // for(int j = 0; j < res->size(); ++j){
  //    std::stringstream p_ss(std::ios::in | std::ios::out | std::ios::binary);
  //   for(int i = 0; i<res->num_columns(); ++i){
  //     out = res->get(j, i, &nbytes);
  //     std::string p_output(out, nbytes);
  //     p_ss << p_output;
  //     output_row;
  //     {
  //       cereal::BinaryInputArchive iarchive(p_ss); // Create an input archive
  //       iarchive(output_row); // Read the data from the archive
  //     }
  //     std::cerr << "Query Result. Col " << i << ": " << output_row <<
  //     std::endl;
  //   }
  // }

  // delete res;

  // Transform PelotonResult into ProtoResult
  std::string &&res(TransformResult(status, statement, result));

  Debug("End readLat on core: %d", core);
  Latency_End(&readLats[core]);
  return std::move(res); // return TransformResult(status, statement, result)
}

// Execute a point read on the Table backend and return a query_result/proto (in
// serialized form) as well as a commitProof (note, the read set is implicit)
void PelotonTableStore::ExecPointRead(const std::string &query_statement, std::string &enc_primary_key, const Timestamp &ts,
      proto::Write *write, const proto::CommittedProof* &committedProof) {
  
  // Client sends query statement, and expects a Query Result for the given key, a timestamp, and a proof (if it was a committed value it read) Note:
  // Sending a query statement (even though it is a point request) allows us to handle complex Select operators (like Count, Max, or just some subset of
  // rows, etc) without additional parsing Since the CC-store holds no data, the server would have to generate a statement anyways --> so it's easiest to
  // just send it from the client as is (rather than assembling it from the encoded key )

  // Read prepared predicate will evaluate to true if a) prepared reads are enabled, AND b) the dependency depth of a prepared value is within the
  // threshold. Pass down a Lambda function that takes in txn_digest and checks whether is readable (Like passing an electrical probe down the ocean)

  // TODO: If no write/read exists (result == empty) -> send empty result (i.e. no fields in write are set), read_time = 0 by default
  //  WARNING: Don't set prepared or committed -- let client side default handling take care of it. (optional TODO:) For optimal CC we'd ideally
  //  send the time of last delete (to minimize conflict window) rather than default to 0 - but then we have to send it as committed (with proof) or as prepared (with
  // value = empty result) Client will have to check proof txn ==> lookup that  key exists in Writeset was marked as delete. Note: For Query reads that
  // would technically be the best too --> the coarse lock of the Table Version  helps simulate it.

  // Note: Don't read TableVersion for PointReads -- they do not care about what other rows exist

  Debug("ExecPointRead for query statement: %s with Timestamp[%lu:%lu]", query_statement.c_str(), ts.getTimestamp(), ts.getID());

  int core = sched_getcpu();
  Debug("Begin readLat on core: %d", core);
  Latency_Start(&readLats[core]);
  std::pair<peloton::tcop::TrafficCop *, std::atomic_int *> cop_pair = GetCop();

  std::atomic_int *counter = cop_pair.second;
  peloton::tcop::TrafficCop *tcop = cop_pair.first;
  bool unamed;

  // prepareStatement
  auto statement = ParseAndPrepare(query_statement, tcop);

  // ExecuteStatment
  std::vector<peloton::type::Value> param_values;
  std::vector<peloton::ResultValue> result;
  std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);

  counter->store(1); // SetTrafficCopCounter();

  Timestamp committed_timestamp;
  Timestamp prepared_timestamp;         // TODO: Change into write subparts.
  std::shared_ptr<std::string> txn_dig(nullptr); // prepared dependency

  // Execute PointQueryStatement on Peloton using traffic_cop args: query, Ts, this->can_read_prepared ; commit: (result1, timestamp1, proof), prepared: (result2, timestamp2, txn_digest), key (optional) 
  //Read latest committed (return committedProof) + Read latest prepared (if > committed)
  auto status = tcop->ExecutePointReadStatement(statement, param_values, unamed, result_format, result, ts,
      this->can_read_prepared, &committed_timestamp, &committedProof, &prepared_timestamp, &txn_dig, write);

  // GetResult(status);
  GetResult(status, tcop, counter);

  if (committedProof == nullptr) {
    Debug("The commit proof after executing point read is null");
  } else {
    Debug("The commit proof is not null");

    /*auto proof_ts = Timestamp(committedProof->txn().timestamp());
    Debug("ExecPointRead Proof ts is %lu, %lu", proof_ts.getTimestamp(),
          proof_ts.getID());*/

    Debug("ExecPointRead committed ts is %lu, %lu",
          committed_timestamp.getTimestamp(), committed_timestamp.getID());
  }

  TransformPointResult(write, committed_timestamp, prepared_timestamp, txn_dig, status, statement, result);

  Debug("End readLat on core: %d", core);
  Latency_End(&readLats[core]);

  return;
}
// Note: Could execute PointRead via ExecReadQuery (Eagerly) as well.
//  ExecPointRead should translate enc_primary_key into a query_statement to be exec by ExecReadQuery.
//(Alternatively: Could already send a Sql command from the client) ==> Should do it at the client, so that we can keep whatever Select specification, e.g. * or specific cols...
void PelotonTableStore::TransformPointResult(proto::Write *write, Timestamp &committed_timestamp, Timestamp &prepared_timestamp, std::shared_ptr<std::string> txn_dig,
    peloton::ResultType &status, std::shared_ptr<peloton::Statement> statement, std::vector<peloton::ResultValue> &result) {

  std::vector<peloton::FieldInfo> tuple_descriptor;
  if (status == peloton::ResultType::SUCCESS) {
    tuple_descriptor = statement->GetTupleDescriptor();
  }
  else{
    Panic("no result for point read. We always expect a result for current workload");
    write->Clear(); //wipe everything -- "has_committed"/has_prepared may be set, even if there is not actually a result (because we call _mutable in tcop)
    Debug("Read Committed? %d. read Prepared? %d", write->has_committed_value(), write->has_prepared_value());
    return; //empty read
  }

  Debug("Transform PointResult");
  // Change Peloton result into query proto.

  unsigned int rows = result.empty()? 0 : result.size() / tuple_descriptor.size();
  UW_ASSERT(rows <= 2); // There should be at most 2 rows: One committed, and one prepared. The committed one always comes last. (if it exists)

  //Notes on expected behavior.
  //If we did read a committed/prepared value, but it doesn't hit the predicate, the result should be empty, but we should still be sending the proof/version 
 
  

  //FIXME: REMOVE THIS ONCE SECONDARY INDEX BUG IS FIXED. THIS JUST A SHORT-TERM FIX TO IGNORE INVALID PREP READS
  // value should only be "f" if it does hit primary key, but the predicate is stronger than that... But secondary index scan violates this.
  // if(write->prepared_value() == "f"){
  //   Notice("Reading a prepared value that doesn't hit predicate. False Positive.");
  //   write->clear_prepared_value();
  // } 
  // if(write->committed_value() == "f"){
  //   Notice("Reading a committed value that doesn't hit predicate. False Positive.");
  //   write->clear_committed_value();
  // } 

  sql::QueryResultProtoBuilder queryResultBuilder;
  RowProto *row;
 
  //Invariant: If we read a value, then write->has_XX_value() is true (it contains "e") -- where XX = committed/prepared
  
  //UW_ASSERT(!write->has_committed_value() || write->has_committed_timestamp()); //if have a committed val, then committedTS MUST be set.
  //UW_ASSERT(!write->has_prepared_value() || write->has_prepared_timestamp());
   // => This is not true currently, since we are writing to "Timestamp committed_timestamp" instead of "write.committed_timestamp()" 
     //TODO: Instead of passing TS, pass in Write.TimestampedMessage?  ==> Optimize this anyways.

  // Committed
  if(!write->committed_value().empty()){ //Indicate that the committed version produced a result. If not, just return empty result
    //NOTE: value == "r" for readable, "d" for delete, "f" for failed pred
    //if tuple_descriptor.empty() => could write value = "" (empty) => frontend will handle it
    if(write->committed_value() == "r"){ //only consume a row if the row exists for this version (i.e. write nothing to result if version was delete)
      row = queryResultBuilder.new_row();
      Debug("Committed row has %lu cols", tuple_descriptor.size());
      for (unsigned int i = 0; i < tuple_descriptor.size(); i++) {
        std::string &column_name = std::get<0>(tuple_descriptor[i]);
        queryResultBuilder.add_column(column_name);
        size_t index = (rows - 1) * tuple_descriptor.size() + i;
        //Debug("Index in result array is %lu", index);
        std::cerr << "Commit. Col: " << i << ". Val: " << (result[index]) << std::endl;
        queryResultBuilder.AddToRow(row, result[index]); // Note: rows-1 == last row == Committed
      }
    }

    //std::cerr << "Committed val (pre): " << write->committed_value() << std::endl;
    write->set_committed_value(queryResultBuilder.get_result()->SerializeAsString()); // Note: This "clears" the builder
    UW_ASSERT(write->has_committed_value()); // should be true EVEN if we write empty value.
    //std::cerr << "Committed val: " << write->committed_value() << ". size: " << write->committed_value().size() << std::endl;
    
    committed_timestamp.serialize(write->mutable_committed_timestamp());
    //NOTE: Committed proof is already set

    Debug("PointRead Committed Val: %s. Version:[%lu:%lu]", BytesToHex(write->committed_value(), 20).c_str(), committed_timestamp.getTimestamp(), committed_timestamp.getID());
  }
  else{
    write->clear_committed_value();
    //WARNING: Accessing mutable_committed_value in traffic topturns "has_committed/prepared_value" to true, even if they are empty!! Clear them if empty!.
  }
  
  // Prepared
  //if (rows < 2) return; // no prepared //FIX: Remove this line. We might read only a prepared row, and no committed one.

  if(!write->prepared_value().empty()){ //Indicate that the prepared version produced a result. If not, just return empty result

    if(write->prepared_value() == "r"){ //only consume a row if the row exists for this version (i.e. write nothing to result if version was delete)
         
        // if(rows != 2) Panic("current test should always see committed. Statement: %s", statement->GetQueryString().c_str()); 
        // if(write->committed_value().empty()) Panic("In current test there should always be a committed value to read");
      
      row = queryResultBuilder.new_row();
      for (unsigned int i = 0; i < tuple_descriptor.size(); i++) {
        std::string &column_name = std::get<0>(tuple_descriptor[i]);
        queryResultBuilder.add_column(column_name);
        size_t index = 0 * tuple_descriptor.size() + i;
        std::cerr << "Prep. Col: " << i << ". Val: " << (result[index]) << std::endl;
        queryResultBuilder.AddToRow(row, result[index]); // Note: first row == Prepared (if present)
      }
    }
    else{

    }
    write->set_prepared_value(queryResultBuilder.get_result()->SerializeAsString()); // Note: This "clears" the builder
    UW_ASSERT(write->has_prepared_value()); // should be true EVEN if we write empty value.

    prepared_timestamp.serialize(write->mutable_prepared_timestamp());
    write->set_prepared_txn_digest(*txn_dig);
    UW_ASSERT(!write->prepared_txn_digest().empty());

    Debug("PointRead Prepared Val: %s. Version:[%lu:%lu]. Dependency: %s", BytesToHex(write->prepared_value(), 20).c_str(), 
          prepared_timestamp.getTimestamp(), prepared_timestamp.getID(), BytesToHex(write->prepared_txn_digest(), 16).c_str());
  }
  else{
    write->clear_prepared_value();
    //WARNING: Accessing mutable_prepared_value turns "has_committed/prepared_value" to true, even if they are empty!! Clear them if empty!.
  }
   
  Debug("Read Committed? %d. read Prepared? %d", write->has_committed_value(), write->has_prepared_value());
  return;
}


//////////////////////// WRITE STATEMENTS

// void ExecWrite(std::string &write_statement, const Timestamp &ts, const
// std::string &txn_digest,
//     const proto::CommittedProof *commit_proof, bool commit_or_prepare)

// Apply a set of Table Writes (versioned row creations) to the Table backend
void PelotonTableStore::ApplyTableWrite(const std::string &table_name, const TableWrite &table_write, const Timestamp &ts, const std::string &txn_digest,
    const proto::CommittedProof *commit_proof, bool commit_or_prepare, bool forceMaterialize) {
  // Note: These are "no questions asked writes", i.e. they should always succeed/be applied, because they don't care about any semantics
  //  TODO: can add boolean to allow a use of this function that DOES respect Insert/Update/Delete semantics -- however those can just go through ExecRaw (and wait for the result)

  //if(forceMaterialize) Panic("shouldn't try to force mat in testing");

  // Ensure that ApplyTableWrite is synchronous -- i.e. only returns after all writes are applied. => currently this is achieved by waiting for the Write Result 
  // If we don't want to wait for the Write Result, then must call SetTableVersion as callback from within Peloton once it is done to set the TableVersion 
  //(Currently, it is being set right after ApplyTableWrite() returns)
  int core = sched_getcpu();
  Debug("Begin writeLat on core: %d", core);
  Latency_Start(&writeLats[core]);

  if (commit_or_prepare) {
    UW_ASSERT(commit_proof);
    Debug("Before timestamp asserts for apply table write");
    UW_ASSERT(ts == Timestamp(commit_proof->txn().timestamp()));
  }

  // UW_ASSERT(ts.getTimestamp() >= 0 && ts.getID() >= 0);
  Debug("Apply TableWrite[%s] for txn %s. TS [%lu:%lu]. Commit? %d. ForceMat? %d", table_name.c_str(), BytesToHex(txn_digest, 16).c_str(), ts.getTimestamp(), ts.getID(), commit_or_prepare, forceMaterialize);

  if (table_write.rows().empty()) return;

  std::pair<peloton::tcop::TrafficCop *, std::atomic_int *> cop_pair = GetCop();

  std::atomic_int *counter = cop_pair.second;
  peloton::tcop::TrafficCop *tcop = cop_pair.first;
  bool unamed;

  // Turn txn_digest into a shared_ptr, write everywhere it is needed.
  std::shared_ptr<std::string> txn_dig(std::make_shared<std::string>(txn_digest));

  std::string write_statement; // empty if no writes
  //  std::string delete_statement; //empty if no deletes
  std::vector<std::string> delete_statements;
  sql_interpreter.GenerateTableWriteStatement(write_statement, delete_statements, table_name, table_write);

  // Execute Writes and Deletes on Peloton
  std::vector<peloton::ResultValue> result;

  // Debug("Delete statements: %s", fmt::join(delete_statements, "|"));


  // Execute Write Statement
  if (!write_statement.empty()) {

    //Notice("Write statement: %s", write_statement.substr(0, 1000).c_str());

    Debug("Write statement: %s", write_statement.substr(0, 1000).c_str());
    Debug("Commit or prepare is %d", commit_or_prepare);

    // Notice("Txn %s is trying to %s with TS[%lu:%lu]", BytesToHex(txn_digest, 16).c_str(), commit_or_prepare? "commit" : "prepare" , ts.getTimestamp(), ts.getID());
    // Notice("Commit or prepare is %d", commit_or_prepare);
    
    // prepareStatement
    auto statement = ParseAndPrepare(write_statement, tcop);

    // ExecuteStatment
    std::vector<peloton::type::Value> param_values;
    //size_t t_id = 0; // std::hash<std::thread::id>{}(std::this_thread::get_id());

    std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);

    // counter_.store(1); // SetTrafficCopCounter();
    /*auto status = traffic_cop_.ExecuteWriteStatement(statement, param_values, unnamed, result_format, result, ts, txn_dig,commit_proof, commit_or_prepare, t_id);*/


    counter->store(1);
    auto status = tcop->ExecuteWriteStatement(statement, param_values, unamed, result_format, result, ts, txn_dig, commit_proof, commit_or_prepare, forceMaterialize);

    // GetResult(status);
    GetResult(status, tcop, counter);

    if (status == peloton::ResultType::SUCCESS)
      Debug("Write successful");
    else
      Panic("Write failure");
  }

  // Execute Delete Statement
  // Note: Peloton does not support WHERE IN syntax in Delete statements. Thus
  // we have to represent multi deletes as indidivdual statements -- which is
  // quite inefficient

  // if (!delete_statement.empty()) {
  for (auto &delete_statement : delete_statements) { // TODO: Find a way to parallelize these statement calls (they don't conflict)
    Notice("Delete statement: %s", delete_statement.c_str());
    Debug("Delete statement: %s", delete_statement.c_str());
    
    // prepare Statement
    auto statement = ParseAndPrepare(delete_statement, tcop);
    // ExecuteStatment
    std::vector<peloton::type::Value> param_values; // param_values.clear();

    std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);

    counter->store(1);
    // counter_.store(1); // SetTrafficCopCounter();
    /*auto status = traffic_cop_.ExecuteWriteStatement( statement, param_values, unnamed, result_format, result, ts, txn_dig, commit_proof, commit_or_prepare);*/
    auto status = tcop->ExecuteWriteStatement(statement, param_values, unamed, result_format, result, ts, txn_dig, commit_proof, commit_or_prepare, forceMaterialize, true); //is_delete

    // GetResult(status);
    GetResult(status, tcop, counter);

    if (status == peloton::ResultType::SUCCESS)
      Debug("Delete successful");
    else
      Panic("Delete failure");
  }

  Debug("End writeLat on core: %d", core);
  // Debug("getCPU says on core: %d", sched_getcpu());
  // UW_ASSERT(core == sched_getcpu());
  Latency_End(&writeLats[core]);
}

void PelotonTableStore::PurgeTableWrite(const std::string &table_name, const TableWrite &table_write, const Timestamp &ts, const std::string &txn_digest) {

  Debug("Purge Txn[%s]", BytesToHex(txn_digest, 16).c_str());
  if (table_write.rows().empty())
    return;

  int core = sched_getcpu();
  Debug("Begin writeLat on core: %d", core);
  Latency_Start(&writeLats[core]);
  // Purge statement is a "special" delete statement: it deletes existing row insertions for the timestamp, but it also undoes existing deletes for the timestamp

  // Simple implementation: Check Versioned linked list and delete row with Timestamp ts. Return if ts > current WARNING: ONLY Purge Rows/Tuples that are prepared. 
  // Do NOT purge committed ones. Note: Since Delete does not impact Indexes no other changes are necessary. MUST delete even if not in index 
  //Note: Purging Prepared Inserts will not clean up Index updates, i.e. the aborted transaction may leave behind a false positive index entry. 
  //Removing this false positive would require modifying the Peloton internals, so we will ignore this issue since it only affects efficiency and not results.
  //  I.e. a hit to the false positive will just result in a wasted lookup.

  //==> Effectively it is "aborting" all previously suggested (prepared) table writes.

  std::pair<peloton::tcop::TrafficCop *, std::atomic_int *> cop_pair = GetCop();

  std::atomic_int *counter = cop_pair.second;
  peloton::tcop::TrafficCop *tcop = cop_pair.first;
  bool unamed;

  std::shared_ptr<std::string> txn_dig(
      std::make_shared<std::string>(txn_digest));

  std::string
      purge_statement; // empty if no writes/deletes (i.e. nothing to abort)
  sql_interpreter.GenerateTablePurgeStatement(purge_statement, table_name,
                                              table_write);
  // std::vector<std::string> purge_statements;
  // sql_interpreter.GenerateTablePurgeStatement(purge_statements, table_name, table_write);

  if (purge_statement.empty())
    return; // Nothing to undo.

  Notice("Purge statement: %s", purge_statement.c_str());
  Debug("Purge statement: %s", purge_statement.c_str());
  // Debug("Purge statements: %s", fmt::join(purge_statements, "|"));

  std::vector<peloton::ResultValue> result;
  std::vector<peloton::FieldInfo> tuple_descriptor;

  // for (auto &purge_statement : purge_statements) {
  //  prepareStatement
  auto statement = ParseAndPrepare(purge_statement, tcop);
  std::cout << purge_statement << std::endl;

  std::vector<peloton::type::Value> param_values; // param_values.clear();
  std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);

  counter->store(1); // SetTrafficCopCounter();
  auto status = tcop->ExecutePurgeStatement(statement, param_values, unamed, result_format, result, ts, txn_dig, true); //! purge_statements.empty());

  // GetResult(status);
  GetResult(status, tcop, counter);

  if (status == peloton::ResultType::SUCCESS)
    Debug("Purge successful");
  else
    Debug("Purge failure/Nothing to purge");
    //Panic("Purge failure");
  //}

  Debug("End writeLat on core: %d", core);
  // UW_ASSERT(core == sched_getcpu());
  Latency_End(&writeLats[core]);
}

///////////////////// Snapshot Protocol Support

// Partially execute a read query statement (reconnaissance execution) and return the snapshot state (managed by ssMgr)
void PelotonTableStore::FindSnapshot(const std::string &query_statement, const Timestamp &ts, SnapshotManager &ssMgr, size_t snapshot_prepared_k) {


  Debug("Execute FindSnapshot: %s. TS: [%lu:%lu]", query_statement.c_str(), ts.getTimestamp(), ts.getID());

  int core = sched_getcpu();
  Debug("Begin snapshotLat on core: %d", core);
  Latency_Start(&snapshotLats[core]);

  std::pair<peloton::tcop::TrafficCop *, std::atomic_int *> cop_pair = GetCop();

  std::atomic_int *counter = cop_pair.second;
  peloton::tcop::TrafficCop *tcop = cop_pair.first;
  bool unamed;

  // prepareStatement
  auto statement = ParseAndPrepare(query_statement, tcop);

  // ExecuteStatment
  std::vector<peloton::type::Value> param_values;
  std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);
  std::vector<peloton::ResultValue> result;

  counter->store(1);
  
  // execute the query using tcop
  auto status = tcop->ExecuteReadStatement(statement, param_values, unamed, result_format, result, &sql_interpreter, //sql_interpreter.GetTableRegistry(),
                                            ts, &this->record_table_version, &this->can_read_prepared, peloton::PequinMode::findSnapshot, nullptr, &ssMgr, snapshot_prepared_k); //Read k latest prepared.
  // auto status = tcop->ExecuteFindSnapshotStatement(statement, param_values, unamed, result_format, result, ts, &ssMgr, k_prepared_versions,
  //     this->record_table_version, this->can_read_prepared);
  
  // Note: Don't need to return a result
  // Note: Ideally execution is "partial" and only executes the leaf scan operations.

  GetResult(status, tcop, counter);


  Debug("End snapshotLat on core: %d", core);
  Latency_End(&snapshotLats[core]);
}

std::string PelotonTableStore::EagerExecAndSnapshot(const std::string &query_statement, const Timestamp &ts, SnapshotManager &ssMgr, QueryReadSetMgr &readSetMgr, size_t snapshot_prepared_k) {
  //Perform EagerRead + FindSnapshot in one go
  Debug("Execute EagerExecAndSnapshot: %s. TS: [%lu:%lu]", query_statement.c_str(), ts.getTimestamp(), ts.getID());

  int core = sched_getcpu();
  Debug("Begin readLat and snapshotLat on core: %d", core);  //Note: If using this mode: total_readLat - total_snapshotLat = materializedReadLat
  Latency_Start(&readLats[core]);
  Latency_Start(&snapshotLats[core]);

  std::pair<peloton::tcop::TrafficCop *, std::atomic_int *> cop_pair = GetCop();

  std::atomic_int *counter = cop_pair.second;
  peloton::tcop::TrafficCop *tcop = cop_pair.first;
  bool unamed;

  // prepareStatement
  auto statement = ParseAndPrepare(query_statement, tcop);

  // ExecuteStatment
  std::vector<peloton::type::Value> param_values;
  std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);
  std::vector<peloton::ResultValue> result;

  counter->store(1);

  // execute the query using tcop
  auto status = tcop->ExecuteReadStatement(statement, param_values, unamed, result_format, result, &sql_interpreter, //sql_interpreter.GetTableRegistry(),
                                            ts, &this->record_table_version, &this->can_read_prepared, peloton::PequinMode::eagerPlusSnapshot, &readSetMgr, &ssMgr, snapshot_prepared_k);
  // auto status = tcop->ExecuteEagerExecAndSnapshotStatement(statement, param_values, unamed, result_format, result, ts, readSetMgr, &ssMgr, k_prepared_versions,
  //     this->record_table_version, this->can_read_prepared);

  GetResult(status, tcop, counter);

  // Transform PelotonResult into ProtoResult
  std::string &&res(TransformResult(status, statement, result));

  Debug("End readLat and snapshotLat on core: %d", core);
  Latency_End(&readLats[core]);
  Latency_End(&snapshotLats[core]);

  return std::move(res);
}



std::string PelotonTableStore::ExecReadQueryOnMaterializedSnapshot(const std::string &query_statement, const Timestamp &ts, QueryReadSetMgr &readSetMgr,
            const snapshot &ss_txns) //::google::protobuf::Map<std::string, proto::ReplicaList> == snapshot
{
  //Perform a read on a materialized snapshot
  
  Debug("Execute ReadQuery: %s. TS: [%lu:%lu]", query_statement.c_str(), ts.getTimestamp(), ts.getID());

  int core = sched_getcpu();
  Debug("Begin readLat on core: %d", core);
  Latency_Start(&readLats[core]);

  std::pair<peloton::tcop::TrafficCop *, std::atomic_int *> cop_pair = GetCop();

  std::atomic_int *counter = cop_pair.second;
  peloton::tcop::TrafficCop *tcop = cop_pair.first;
  bool unamed;

  // prepareStatement
  auto statement = ParseAndPrepare(query_statement, tcop);

  // ExecuteStatment
  std::vector<peloton::type::Value> param_values;
  std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);
  std::vector<peloton::ResultValue> result;

  counter->store(1);

  // execute the query using tcop
  auto status = tcop->ExecuteReadStatement(statement, param_values, unamed, result_format, result, &sql_interpreter, //sql_interpreter.GetTableRegistry(),
                                          ts, &this->record_table_version, &this->can_read_prepared, peloton::PequinMode::readMaterialized, &readSetMgr, nullptr, 1, &ss_txns);
  // auto status = tcop->ExecuteSnapshotReadStatement(statement, param_values, unamed, result_format, result, ts, readSetMgr, &ss_txns,
  //     this->record_table_version, this->can_read_prepared);

  GetResult(status, tcop, counter);

  // Transform PelotonResult into ProtoResult
  std::string &&res(TransformResult(status, statement, result));

  Debug("End readLat on core: %d", core);
  Latency_End(&readLats[core]);
  return std::move(res); // return TransformResult(status, statement, result)

}


/////////////// DEPRECATED:

// Materialize a snapshot on the Table backend and execute on said snapshot.
//TODO: Deprecate this interface. Instead: Add bool to ApplyTableWrite that let's you write even if not prepared (special gray visibility)

// void PelotonTableStore::MaterializeSnapshot(
//     const std::string &query_retry_id, const proto::MergedSnapshot &merged_ss,
//     const std::set<proto::Transaction *> &ss_txns) {
//   // Note: Not sure whether we should materialize full snapshot on demand, or
//   // continuously as we sync on Tx

//   // TODO: Apply all txn in snapshot to Table backend as a "view" that is only visible to query_id
//   // FIXME: The merged_ss argument only holds the txn_ids. --> instead, call Materialize Snapshot on a set of transactions... ==> if doing it
//   // continuously might need to call this function often.
// }

// std::string PelotonTableStore::ExecReadOnSnapshot(
//     const std::string &query_retry_id, std::string &query_statement,
//     const Timestamp &ts, QueryReadSetMgr &readSetMgr, bool abort_early) {
//   // TODO: Execute on the snapshot for query_id/retry_version

//   //--> returns peloton result
//   // TODO: Change peloton result into query proto:

//   sql::QueryResultProtoBuilder queryResultBuilder;
//   // queryResultBuilder.add_column("result");
//   // queryResultBuilder.add_row(result_row.begin(), result_row.end());

//   return queryResultBuilder.get_result()->SerializeAsString();
// }

} // namespace pequinstore
