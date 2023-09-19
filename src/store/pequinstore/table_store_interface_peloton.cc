#include "store/pequinstore/table_store_interface_peloton.h"
#include "query-engine/traffic_cop/traffic_cop.h"
#include <atomic>
#include <sched.h>
#include <utility>

#include "store/common/query_result/query_result.h"


namespace pequinstore {

std::string
GetResultValueAsString(const std::vector<peloton::ResultValue> &result,
                       size_t index) {
  std::string value(result[index].begin(), result[index].end());
  return value;
}

void UtilTestTaskCallback(void *arg) {
  std::atomic_int *count = static_cast<std::atomic_int *>(arg);
  count->store(0);
}

void ContinueAfterComplete(std::atomic_int &counter_) {
  while (counter_.load() == 1) {
    usleep(10);
  }
}

///////////////////// CLASS FUNCTIONS ///////////////////////////

PelotonTableStore::PelotonTableStore(int num_threads)
    : unnamed_statement("unnamed"), unnamed_variable(false) {
  // Init Peloton default DB
  Init(num_threads);
}

PelotonTableStore::PelotonTableStore(std::string &table_registry_path,
                                     find_table_version &&find_table_version,
                                     read_prepared_pred &&read_prepared_pred,
                                     int num_threads)
    : TableStore(table_registry_path, std::move(find_table_version),
                 std::move(read_prepared_pred)),
      unnamed_statement("unnamed"), unnamed_variable(false) {
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
}

void PelotonTableStore::Init(int num_threads) {
  // Init Peloton default DB
  auto &txn_manager =
      peloton::concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  peloton::catalog::Catalog::GetInstance()->CreateDatabase(txn,
                                                           DEFAULT_DB_NAME);
  txn_manager.CommitTransaction(txn);
  // traffic_cop_ = peloton::tcop::TrafficCop(UtilTestTaskCallback, &counter_);

  if (num_threads > 0) {
    is_recycled_version_ = false;
    for (int i = 0; i < num_threads; i++) {
      std::atomic_int *counter = new std::atomic_int();
      peloton::tcop::TrafficCop *new_cop =
          new peloton::tcop::TrafficCop(UtilTestTaskCallback, counter);
      traffic_cops_.push_back({new_cop, counter});
    }
  }
}

////////////////  Helper Functions //////////////////////////
std::shared_ptr<peloton::Statement>
PelotonTableStore::ParseAndPrepare(const std::string &query_statement,
                                   peloton::tcop::TrafficCop *tcop) {
  // prepareStatement
  auto &peloton_parser = peloton::parser::PostgresParser::GetInstance();
  auto sql_stmt_list = peloton_parser.BuildParseTree(query_statement);
  // PELOTON_ASSERT(sql_stmt_list);
  if (!sql_stmt_list->is_valid) {
    Panic("SQL command not valid"); // return peloton::ResultType::FAILURE;
  }

  auto statement = tcop->PrepareStatement(unnamed_statement, query_statement, std::move(sql_stmt_list));
  if (statement.get() == nullptr) {
    tcop->setRowsAffected(0);
    Panic("SQL command not valid"); // return peloton::ResultType::FAILURE;
  }
  return statement;
}

void PelotonTableStore::GetResult(peloton::ResultType &status,
                                  peloton::tcop::TrafficCop *tcop,
                                  std::atomic_int *c) {
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
std::string PelotonTableStore::TransformResult(
    peloton::ResultType &status, std::shared_ptr<peloton::Statement> statement,
    std::vector<peloton::ResultValue> &result) {

  std::vector<peloton::FieldInfo> tuple_descriptor;
  if (status == peloton::ResultType::SUCCESS) {
    tuple_descriptor = statement->GetTupleDescriptor();
  }

  sql::QueryResultProtoBuilder queryResultBuilder;
  // Add columns
  for (unsigned int i = 0; i < tuple_descriptor.size(); i++) {
    std::string column_name = std::get<0>(tuple_descriptor[i]);
    queryResultBuilder.add_column(column_name);
  }
  // Add rows
  unsigned int rows = result.size() / tuple_descriptor.size();
  for (unsigned int i = 0; i < rows; i++) {
    RowProto *row = queryResultBuilder.new_row();
    for (unsigned int j = 0; j < tuple_descriptor.size(); j++) {
      // TODO: Use interface addtorow, and pass in field to that row
      queryResultBuilder.AddToRow(row, result[i * tuple_descriptor.size() + j]);
      // auto r = result[i * tuple_descriptor.size() + j];
      // std::cerr << "Col/Row" << j << " : " << (result[i * tuple_descriptor.size() + j]) << std::endl;
    }
  }

  return queryResultBuilder.get_result()->SerializeAsString();
}

std::pair<peloton::tcop::TrafficCop *, std::atomic_int *>
PelotonTableStore::GetCop() {
  if (!is_recycled_version_) {
    int t_id = sched_getcpu();
    // std::cout << "Thread id is " << t_id << std::endl;
    return traffic_cops_.at(t_id);
  } else {
    return GetUnusedTrafficCop();
  }
}

std::pair<peloton::tcop::TrafficCop *, std::atomic_int *>
PelotonTableStore::GetUnusedTrafficCop() {
  std::pair<peloton::tcop::TrafficCop *, std::atomic_int *> cop_pair;
  bool found = traffic_cops.try_dequeue(cop_pair);

  if (found) {
    cop_pair.first->Reset(); // Reset traffic cop
    return cop_pair;
  }

  std::atomic_int *counter = new std::atomic_int();
  peloton::tcop::TrafficCop *new_cop =
      new peloton::tcop::TrafficCop(UtilTestTaskCallback, counter);
  return {new_cop, counter};
}

void PelotonTableStore::ReleaseTrafficCop(
    std::pair<peloton::tcop::TrafficCop *, std::atomic_int *> cop_pair) {
  traffic_cops.enqueue(cop_pair);
}

/////////////////// INTERFACE FUNCTIONS //////////////////////////////

// Execute a statement directly on the Table backend, no questions asked, no
// output
void PelotonTableStore::ExecRaw(const std::string &sql_statement) {
  // Execute on Peloton  //Note -- this should be a synchronous call. I.e.
  // ExecRaw should not return before the call is done.

  std::pair<peloton::tcop::TrafficCop *, std::atomic_int *> cop_pair = GetCop();

  std::atomic_int *counter = cop_pair.second;
  peloton::tcop::TrafficCop *tcop = cop_pair.first;
  bool unamed;

  // prepareStatement
  auto statement = ParseAndPrepare(sql_statement, tcop);

  // ExecuteStatment
  std::vector<peloton::type::Value> param_values;
  std::vector<peloton::ResultValue> result;
  std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);

  // counter_.store(1); // SetTrafficCopCounter();
  /*auto status = traffic_cop_.ExecuteStatement(statement, param_values,
     unnamed, result_format, result);*/

  counter->store(1);
  auto status = tcop->ExecuteStatement(statement, param_values, unamed,
                                       result_format, result);

  Debug("Made it after status");
  // GetResult(status);
  GetResult(status, tcop, counter);

  if (status == peloton::ResultType::SUCCESS)
    Debug("RawExec success");
  else
    Debug("RawExec failure");
}

void PelotonTableStore::LoadTable(const std::string &load_statement,
                                  const std::string &txn_digest,
                                  const Timestamp &ts,
                                  const proto::CommittedProof *committedProof) {
  // When calling the LoadStatement: We'll want to initialize all rows to be
  // committed and have genesis proof (see server) Call statement (of type Copy
  // or Insert) and set meta data accordingly (bool commit = true,
  // committedProof, txn_digest, ts)

  std::pair<peloton::tcop::TrafficCop *, std::atomic_int *> cop_pair = GetCop();

  std::atomic_int *counter = cop_pair.second;
  peloton::tcop::TrafficCop *tcop = cop_pair.first;
  bool unamed;

  std::shared_ptr<std::string> txn_dig(std::make_shared<std::string>(
      txn_digest)); // Turn txn_digest into a shared_ptr, write everywhere it is
                    // needed.

  // execute the query using tcop
  // prepareStatement
  auto statement = ParseAndPrepare(load_statement, tcop);

  // ExecuteStatment
  std::vector<peloton::type::Value> param_values;
  std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);
  std::vector<peloton::ResultValue> result;

  counter->store(1); // SetTrafficCopCounter();
  auto status = tcop->ExecuteStatement(statement, param_values, unamed,
                                       result_format, result);

  GetResult(status, tcop, counter);

  if (status == peloton::ResultType::SUCCESS)
    Debug("Load success");
  else
    Debug("RawExec failure");
}

// Execute a read query statement on the Table backend and return a
// query_result/proto (in serialized form) as well as a read set (managed by
// readSetMgr)
std::string PelotonTableStore::ExecReadQuery(const std::string &query_statement,
                                             const Timestamp &ts,
                                             QueryReadSetMgr &readSetMgr) {

  
  //UW_ASSERT(ts.getTimestamp() >= 0 && ts.getID() >= 0);
  
  Debug("Execute ReadQuery: %s. TS: [%lu:%lu]", query_statement.c_str(), ts.getTimestamp(), ts.getID());
 
  // Execute on Peloton (args: query, Ts, readSetMgr, this->can_read_prepared,
  // this->set_table_version) --> returns peloton result --> transform into
  // protoResult

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
  // size_t t_id = std::hash<std::thread::id>{}(std::this_thread::get_id()); // % 4;
  // std::cout << "################################# STARTING  ExecReadQuery  ############################## on Thread: " < t_id << std::endl;
  
  //std::cout << query_statement << std::endl;

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
  auto status = tcop->ExecuteReadStatement(
      statement, param_values, unamed, result_format, result, ts, readSetMgr,
      this->record_table_version, this->can_read_prepared);

  // GetResult(status);
  GetResult(status, tcop, counter);

  // std::string result_string = TransformResult(status, statement, result);
  // //Testing:
  // //TODO: turn result into protowrapper and read it out.
  // query_result::QueryResult *res = new sql::QueryResultProtoWrapper(result_string);
  // //TODO: what type are the result values? does the transformation shomehow need to mention this.

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
  //     std::cerr << "Query Result. Col " << i << ": " << output_row << std::endl;
  //   }
  // }
   
  // delete res;               
            

  // Transform PelotonResult into ProtoResult
  return TransformResult(status, statement, result);
}

// Execute a point read on the Table backend and return a query_result/proto (in
// serialized form) as well as a commitProof (note, the read set is implicit)
void PelotonTableStore::ExecPointRead(
    const std::string &query_statement, std::string &enc_primary_key,
    const Timestamp &ts, proto::Write *write,
    const proto::CommittedProof *committedProof) {

  // Client sends query statement, and expects a Query Result for the given key,
  // a timestamp, and a proof (if it was a committed value it read) Note:
  // Sending a query statement (even though it is a point request) allows us to
  // handle complex Select operators (like Count, Max, or just some subset of
  // rows, etc) without additional parsing Since the CC-store holds no data, the
  // server would have to generate a statement anyways --> so it's easiest to
  // just send it from the client as is (rather than assembling it from the
  // encoded key )

  // Read prepared predicate will evaluate to true if a) prepared reads are
  // enabled, AND b) the dependency depth of a prepared value is within the
  // threshold. Pass down a Lambda function that takes in txn_digest and checks
  // whether is readable (Like passing an electrical probe down the ocean)

  // TODO: If no write/read exists (result == empty) -> send empty result (i.e.
  // no fields in write are set), read_time = 0 by default
  //  WARNING: Don't set prepared or committed -- let client side default
  //  handling take care of it. (optional TODO:) For optimal CC we'd ideally
  //  send the time of last delete (to minimize conflict window) rather than
  //  default to 0
  //- but then we have to send it as committed (with proof) or as prepared (with
  // value = empty result) Client will have to check proof txn ==> lookup that
  // key exists in Writeset was marked as delete. Note: For Query reads that
  // would technically be the best too --> the coarse lock of the Table Version
  // helps simulate it.

  // Note: Don't read TableVersion for PointReads -- they do not care about what
  // other rows exist
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
  std::shared_ptr<std::string> txn_dig; // prepared dependency

  // Execute PointQueryStatement on Peloton using traffic_cop
  // args: query, Ts, this->can_read_prepared ; commit: (result1, timestamp1,
  // proof), prepared: (result2, timestamp2, txn_digest), key (optional) Read
  // latest committed (return committedProof) + Read latest prepared (if >
  // committed)
  auto status = tcop->ExecutePointReadStatement(
      statement, param_values, unamed, result_format, result, ts,
      this->can_read_prepared, &committed_timestamp, committedProof,
      &prepared_timestamp, txn_dig, write);

  // GetResult(status);
  GetResult(status, tcop, counter);

  TransformPointResult(write, committed_timestamp, prepared_timestamp, txn_dig,
                       status, statement, result);

  return;
}
// Note: Could execute PointRead via ExecReadQuery (Eagerly) as well.
//  ExecPointRead should translate enc_primary_key into a query_statement to be
//  exec by ExecReadQuery.
//(Alternatively: Could already send a Sql command from the client) ==> Should
// do it at the client, so that we can keep whatever Select specification, e.g.
// * or specific cols...

void PelotonTableStore::TransformPointResult(
    proto::Write *write, Timestamp &committed_timestamp,
    Timestamp &prepared_timestamp, std::shared_ptr<std::string> txn_dig,
    peloton::ResultType &status, std::shared_ptr<peloton::Statement> statement,
    std::vector<peloton::ResultValue> &result) {

  std::vector<peloton::FieldInfo> tuple_descriptor;
  if (status == peloton::ResultType::SUCCESS) {
    tuple_descriptor = statement->GetTupleDescriptor();
  }

  // write->set_committed_value()
  /*std::cout << "Commit proof client id: "
            << traffic_cop_.commit_proof_->txn().client_id()
            << " : sequence number: "
            << traffic_cop_.commit_proof_->txn().client_seq_num() <<
     std::endl;*/

  // Change Peloton result into query proto.

  unsigned int rows = result.size() / tuple_descriptor.size();
  UW_ASSERT(rows <= 2); // There should be at most 2 rows: One committed, and
                        // one prepared. The committed one always comes last.

  if (rows == 0)
    return; // Empty result: No tuple exists for the supplied Row-key

  // Committed
  sql::QueryResultProtoBuilder queryResultBuilder;
  RowProto *row = queryResultBuilder.new_row();

  for (unsigned int i = 0; i < tuple_descriptor.size(); i++) {
    std::string &column_name = std::get<0>(tuple_descriptor[i]);
    queryResultBuilder.add_column(column_name);
    queryResultBuilder.AddToRow(
        row, result[rows - 1 * tuple_descriptor.size() +
                    i]); // Note: rows-1 == last row == Committed
  }

  write->set_committed_value(
      queryResultBuilder.get_result()
          ->SerializeAsString()); // Note: This "clears" the builder
  committed_timestamp.serialize(write->mutable_committed_timestamp());

  // Prepared
  if (rows < 2)
    return; // no prepared

  row = queryResultBuilder.new_row();
  for (unsigned int i = 0; i < tuple_descriptor.size(); i++) {
    std::string &column_name = std::get<0>(tuple_descriptor[i]);
    queryResultBuilder.add_column(column_name);
    queryResultBuilder.AddToRow(
        row, result[0 * tuple_descriptor.size() +
                    i]); // Note: first row == Prepared (if present)
  }

  write->set_prepared_value(
      queryResultBuilder.get_result()
          ->SerializeAsString()); // Note: This "clears" the builder
  prepared_timestamp.serialize(write->mutable_prepared_timestamp());
  write->set_prepared_txn_digest(*txn_dig);

  return;
}

//////////////////////// WRITE STATEMENTS

// void ExecWrite(std::string &write_statement, const Timestamp &ts, const
// std::string &txn_digest,
//     const proto::CommittedProof *commit_proof, bool commit_or_prepare)

// Apply a set of Table Writes (versioned row creations) to the Table backend
void PelotonTableStore::ApplyTableWrite(
    const std::string &table_name, const TableWrite &table_write,
    const Timestamp &ts, const std::string &txn_digest,
    const proto::CommittedProof *commit_proof, bool commit_or_prepare) {
  // Note: These are "no questions asked writes", i.e. they should always
  // succeed/be applied, because they don't care about any semantics
  //  TODO: can add boolean to allow a use of this function that DOES respect
  //  Insert/Update/Delete semantics -- however those can just go through
  //  ExecRaw (and wait for the result)

  // Ensure that ApplyTableWrite is synchronous -- i.e. only returns after all
  // writes are applied. => currently this is achieved by waiting for the Write
  // Result If we don't want to wait for the Write Result, then must call
  // SetTableVersion as callback from within Peloton once it is done to set the
  // TableVersion (Currently, it is being set right after ApplyTableWrite()
  // returns)

  //UW_ASSERT(ts.getTimestamp() >= 0 && ts.getID() >= 0);
  Debug("Apply TableWrite for txn %s. TS [%lu:%lu]", BytesToHex(txn_digest, 16).c_str(), ts.getTimestamp(), ts.getID()); 

  if (table_write.rows().empty())
    return;

  std::pair<peloton::tcop::TrafficCop *, std::atomic_int *> cop_pair = GetCop();

  std::atomic_int *counter = cop_pair.second;
  peloton::tcop::TrafficCop *tcop = cop_pair.first;
  bool unamed;

  // Turn txn_digest into a shared_ptr, write everywhere it is needed.
  std::shared_ptr<std::string> txn_dig(
      std::make_shared<std::string>(txn_digest));

  std::string write_statement; // empty if no writes
  //  std::string delete_statement; //empty if no deletes
  std::vector<std::string> delete_statements;
  sql_interpreter.GenerateTableWriteStatement(
      write_statement, delete_statements, table_name, table_write);

  std::cout << write_statement << std::endl;

  // Execute Writes and Deletes on Peloton
  std::vector<peloton::ResultValue> result;

  // Debug("Delete statements: %s", fmt::join(delete_statements, "|"));

  // Execute Write Statement
  if (!write_statement.empty()) {

    Debug("Write statement: %s", write_statement.c_str());
    // prepareStatement
    auto statement = ParseAndPrepare(write_statement, tcop);

    // ExecuteStatment
    std::vector<peloton::type::Value> param_values;
    size_t t_id =
        0; // std::hash<std::thread::id>{}(std::this_thread::get_id());

    std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);

    // counter_.store(1); // SetTrafficCopCounter();
    /*auto status = traffic_cop_.ExecuteWriteStatement(
        statement, param_values, unnamed, result_format, result, ts, txn_dig,
        commit_proof, commit_or_prepare, t_id);*/

    counter->store(1);
    auto status = tcop->ExecuteWriteStatement(
        statement, param_values, unamed, result_format, result, ts, txn_dig,
        commit_proof, commit_or_prepare, t_id);

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
  for (auto &delete_statement :
       delete_statements) { // TODO: Find a way to parallelize these statement
                            // calls (they don't conflict)
    Debug("Delete statement: %s", delete_statement.c_str());
    // prepare Statement
    auto statement = ParseAndPrepare(delete_statement, tcop);
    // ExecuteStatment
    std::vector<peloton::type::Value> param_values; // param_values.clear();

    std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);

    // counter_.store(1); // SetTrafficCopCounter();
    /*auto status = traffic_cop_.ExecuteWriteStatement(
        statement, param_values, unnamed, result_format, result, ts, txn_dig,
        commit_proof, commit_or_prepare);*/
    auto status = tcop->ExecuteWriteStatement(
        statement, param_values, unamed, result_format, result, ts, txn_dig,
        commit_proof, commit_or_prepare);

    // GetResult(status);
    GetResult(status, tcop, counter);

    if (status == peloton::ResultType::SUCCESS)
      Debug("Delete successful");
    else
      Panic("Delete failure");
  }
}

void PelotonTableStore::PurgeTableWrite(const std::string &table_name,
                                        const TableWrite &table_write,
                                        const Timestamp &ts,
                                        const std::string &txn_digest) {
  if (table_write.rows().empty())
    return;

  // Purge statement is a "special" delete statement:
  //  it deletes existing row insertions for the timestamp, but it also undoes
  //  existing deletes for the timestamp

  // Simple implementation: Check Versioned linked list and delete row with
  // Timestamp ts. Return if ts > current WARNING: ONLY Purge Rows/Tuples that
  // are prepared. Do NOT purge committed ones. Note: Since Delete does not
  // impact Indexes no other changes are necessary. MUST delete even if not in
  // index Note: Purging Prepared Inserts will not clean up Index updates,
  // i.e. the aborted transaction may leave behind a false positive index
  // entry. Removing this false positive would require modifying the Peloton
  // internals, so we will ignore this issue since it only affects efficiency
  // and not results.
  //  I.e. a hit to the false positive will just result in a wasted lookup.

  //==> Effectively it is "aborting" all previously suggested (prepared) table
  // writes.

  std::pair<peloton::tcop::TrafficCop *, std::atomic_int *> cop_pair = GetCop();

  std::atomic_int *counter = cop_pair.second;
  peloton::tcop::TrafficCop *tcop = cop_pair.first;
  bool unamed;

  std::shared_ptr<std::string> txn_dig(
      std::make_shared<std::string>(txn_digest));

  // std::string purge_statement; //empty if no writes/deletes (i.e. nothing
  // to abort)
  std::vector<std::string> purge_statements;
  sql_interpreter.GenerateTablePurgeStatement(purge_statements, table_name,
                                              table_write);

  if (purge_statements.empty())
    return; // Nothing to undo.

  Debug("Purge statements: %s", fmt::join(purge_statements, "|"));

  std::vector<peloton::ResultValue> result;
  std::vector<peloton::FieldInfo> tuple_descriptor;

  for (auto &purge_statement : purge_statements) {
    // prepareStatement
    auto statement = ParseAndPrepare(purge_statement, tcop);

    std::vector<peloton::type::Value> param_values; // param_values.clear();
    std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);

    counter->store(1); // SetTrafficCopCounter();
    auto status = tcop->ExecutePurgeStatement(
        statement, param_values, unamed, result_format, result, ts, txn_dig,
        !purge_statements.empty());

    // GetResult(status);
    GetResult(status, tcop, counter);

    if (status == peloton::ResultType::SUCCESS)
      Debug("Delete successful");
    else
      Panic("Delete failure");
  }
}

///////////////////// Snapshot Protocol Support

// Partially execute a read query statement (reconnaissance execution) and
// return the snapshot state (managed by ssMgr)
void PelotonTableStore::FindSnapshot(std::string &query_statement,
                                     const Timestamp &ts,
                                     SnapshotManager &ssMgr) {

  // Generalize the PointRead interface:
  // Read k latest prepared.
  // Use the same prepare predicate to determine whether to read or ignore a
  // prepared version

  // TODO: Execute on Peloton
  // Note: Don't need to return a result
  // Note: Ideally execution is "partial" and only executes the leaf scan
  // operations.
}

// Materialize a snapshot on the Table backend and execute on said snapshot.
void PelotonTableStore::MaterializeSnapshot(
    const std::string &query_retry_id, const proto::MergedSnapshot &merged_ss,
    const std::set<proto::Transaction *> &ss_txns) {
  // Note: Not sure whether we should materialize full snapshot on demand, or
  // continuously as we sync on Tx

  // TODO: Apply all txn in snapshot to Table backend as a "view" that is only
  // visible to query_id
  // FIXME: The merged_ss argument only holds the txn_ids. --> instead, call
  // Materialize Snapshot on a set of transactions... ==> if doing it
  // continuously might need to call this function often.
}

std::string PelotonTableStore::ExecReadOnSnapshot(
    const std::string &query_retry_id, std::string &query_statement,
    const Timestamp &ts, QueryReadSetMgr &readSetMgr, bool abort_early) {
  // TODO: Execute on the snapshot for query_id/retry_version

  //--> returns peloton result
  // TODO: Change peloton result into query proto:

  sql::QueryResultProtoBuilder queryResultBuilder;
  // queryResultBuilder.add_column("result");
  // queryResultBuilder.add_row(result_row.begin(), result_row.end());

  return queryResultBuilder.get_result()->SerializeAsString();
}

} // namespace pequinstore
