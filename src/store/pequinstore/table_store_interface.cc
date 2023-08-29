#include "store/pequinstore/table_store_interface.h"

// TODO: Include whatever Peloton Deps
#include "../../query-engine/common/logger.h"
#include "../../query-engine/common/macros.h"
#include "../../query-engine/parser/drop_statement.h"
#include "../../query-engine/parser/postgresparser.h"

#include "../../query-engine/catalog/catalog.h"
#include "../../query-engine/catalog/proc_catalog.h"
#include "../../query-engine/catalog/system_catalogs.h"

#include "../../query-engine/concurrency/transaction_manager_factory.h"

#include "../../query-engine/executor/create_executor.h"
#include "../../query-engine/executor/create_function_executor.h"
#include "../../query-engine/executor/executor_context.h"

#include "../../query-engine/planner/create_function_plan.h"
#include "../../query-engine/planner/create_plan.h"
#include "../../query-engine/storage/data_table.h"

#include "../../query-engine/executor/insert_executor.h"
#include "../../query-engine/expression/constant_value_expression.h"
#include "../../query-engine/parser/insert_statement.h"
#include "../../query-engine/planner/insert_plan.h"
#include "../../query-engine/traffic_cop/traffic_cop.h"
#include "../../query-engine/type/type.h"
#include "../../query-engine/type/value_factory.h"
#include "store/common/query_result/query_result_proto_builder.h"
#include <ostream>
#include <string>
#include <tuple>

namespace pequinstore {

static std::string
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

TableStore::TableStore() : traffic_cop_(UtilTestTaskCallback, &counter_) {
  // init Peloton
  // traffic_cop_ = peloton::tcop::TrafficCop(UtilTestTaskCallback, &counter_);
}

TableStore::~TableStore() {}

void TableStore::SetFindTableVersion(find_table_version _set_table_version) {
  set_table_version = std::move(_set_table_version);
}
void TableStore::SetPreparePredicate(read_prepared_pred read_prepared_pred) {
  can_read_prepared = std::move(read_prepared_pred);
}

void TableStore::RegisterTableSchema(std::string &table_registry_path) {
  sql_interpreter.RegisterTables(table_registry_path);
}

std::vector<bool> *
TableStore::GetRegistryColQuotes(const std::string &table_name) {
  return &(sql_interpreter.GetColRegistry(table_name)->col_quotes);
}
std::vector<bool> *
TableStore::GetRegistryPColQuotes(const std::string &table_name) {
  return &(sql_interpreter.GetColRegistry(table_name)->p_col_quotes);
}

// Execute a statement directly on the Table backend, no questions asked, no
// output
void TableStore::ExecRaw(const std::string &sql_statement) {
  // TODO: Execute on Peloton  //Note -- this should be a synchronous call. I.e.
  // ExecRaw should not return before the call is done.
  std::vector<peloton::ResultValue> result;
  std::vector<peloton::FieldInfo> tuple_descriptor;

  // execute the query using tcop
  // prepareStatement
  // LOG_TRACE("Query: %s", query.c_str());
  std::string unnamed_statement = "unnamed";
  auto &peloton_parser = peloton::parser::PostgresParser::GetInstance();
  auto sql_stmt_list = peloton_parser.BuildParseTree(sql_statement);
  // PELOTON_ASSERT(sql_stmt_list);
  if (!sql_stmt_list->is_valid) {
    // return peloton::ResultType::FAILURE;
  }
  auto statement = traffic_cop_.PrepareStatement(
      unnamed_statement, sql_statement, std::move(sql_stmt_list));
  if (statement.get() == nullptr) {
    traffic_cop_.setRowsAffected(0);
    // return peloton::ResultType::FAILURE;
  }
  // ExecuteStatment
  std::vector<peloton::type::Value> param_values;
  bool unnamed = false;
  std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);

  // SetTrafficCopCounter();
  counter_.store(1);
  auto status = traffic_cop_.ExecuteStatement(statement, param_values, unnamed,
                                              result_format, result);
  std::cout << "Made it after status" << std::endl;
  if (traffic_cop_.GetQueuing()) {
    ContinueAfterComplete(counter_);
    traffic_cop_.ExecuteStatementPlanGetResult();
    status = traffic_cop_.ExecuteStatementGetResult();
    traffic_cop_.SetQueuing(false);
  }
  std::cout << "Check if status is success" << std::endl;
  if (status == peloton::ResultType::SUCCESS) {
    tuple_descriptor = statement->GetTupleDescriptor();
  }

  std::cout << "End of exec raw" << std::endl;

  // TODO: When calling the LoadStatement: We'll want to initialize all rows to
  // be committed and have genesis proof (see server)
}

void TableStore::LoadTable(const std::string &load_statement,
                           const std::string &txn_digest, const Timestamp &ts,
                           const proto::CommittedProof *committedProof) {
  // TODO: When calling the LoadStatement: We'll want to initialize all rows to
  // be committed and have genesis proof (see server)

  // Turn txn_digest into a shared_ptr, write everywhere it is needed.
  std::shared_ptr<std::string> txn_dig(
      std::make_shared<std::string>(txn_digest));

  // Call statement (of type Copy or Insert) and set meta data accordingly (bool
  // commit = true, committedProof, txn_digest, ts)
  std::vector<peloton::ResultValue> result;
  std::vector<peloton::FieldInfo> tuple_descriptor;

  // execute the query using tcop
  // prepareStatement
  // LOG_TRACE("Query: %s", query.c_str());
  std::string unnamed_statement = "unnamed";
  auto &peloton_parser = peloton::parser::PostgresParser::GetInstance();
  auto sql_stmt_list = peloton_parser.BuildParseTree(load_statement);
  // PELOTON_ASSERT(sql_stmt_list);
  if (!sql_stmt_list->is_valid) {
    // return peloton::ResultType::FAILURE;
  }
  auto statement = traffic_cop_.PrepareStatement(
      unnamed_statement, load_statement, std::move(sql_stmt_list));
  if (statement.get() == nullptr) {
    traffic_cop_.setRowsAffected(0);
    // return peloton::ResultType::FAILURE;
  }
  // ExecuteStatment
  std::vector<peloton::type::Value> param_values;
  bool unnamed = false;
  std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);
  // SetTrafficCopCounter();
  counter_.store(1);
  // pequinstore::proto::ReadSet read_set_one;

  // pequinstore::QueryReadSetMgr query_read_set_mgr_one(&read_set_one, 1,
  // false);
  auto status = traffic_cop_.ExecuteStatement(statement, param_values, unnamed,
                                              result_format, result);

  if (traffic_cop_.GetQueuing()) {
    ContinueAfterComplete(counter_);
    traffic_cop_.ExecuteStatementPlanGetResult();
    status = traffic_cop_.ExecuteStatementGetResult();
    traffic_cop_.SetQueuing(false);
  }
  if (status == peloton::ResultType::SUCCESS) {
    tuple_descriptor = statement->GetTupleDescriptor();
  }
}

// Execute a read query statement on the Table backend and return a
// query_result/proto (in serialized form) as well as a read set (managed by
// readSetMgr)
std::string TableStore::ExecReadQuery(const std::string &query_statement,
                                      Timestamp &ts,
                                      QueryReadSetMgr &readSetMgr) {
  std::cout << "Inside ExecReadQuery" << std::endl;
  // args: query, Ts, readSetMgr, this->can_read_prepared,
  // this->set_table_version
  // TODO: Execute on Peloton --> returns peloton result
  std::vector<peloton::ResultValue> result;
  std::vector<peloton::FieldInfo> tuple_descriptor;

  // execute the query using tcop
  // prepareStatement
  // LOG_TRACE("Query: %s", query.c_str());
  std::string unnamed_statement = "unnamed";
  auto &peloton_parser = peloton::parser::PostgresParser::GetInstance();
  auto sql_stmt_list = peloton_parser.BuildParseTree(query_statement);
  // PELOTON_ASSERT(sql_stmt_list);
  if (!sql_stmt_list->is_valid) {
    // return peloton::ResultType::FAILURE;
  }

  auto statement = traffic_cop_.PrepareStatement(
      unnamed_statement, query_statement, std::move(sql_stmt_list));
  if (statement.get() == nullptr) {
    traffic_cop_.setRowsAffected(0);
    // return peloton::ResultType::FAILURE;
  }
  // ExecuteStatment
  std::vector<peloton::type::Value> param_values;
  bool unnamed = false;
  std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);
  // SetTrafficCopCounter();
  counter_.store(1);
  auto status = traffic_cop_.ExecuteReadStatement(
      statement, param_values, unnamed, result_format, result, ts, readSetMgr,
      this->set_table_version, this->can_read_prepared);
  if (traffic_cop_.GetQueuing()) {
    ContinueAfterComplete(counter_);
    traffic_cop_.ExecuteStatementPlanGetResult();
    status = traffic_cop_.ExecuteStatementGetResult();
    traffic_cop_.SetQueuing(false);
  }

  if (status == peloton::ResultType::SUCCESS) {
    tuple_descriptor = statement->GetTupleDescriptor();
  }

  // TODO: Pass in predicate here as well, table_version not for point read only
  // readquery, done before touching indexes

  // TODO: Change Peloton result into query proto.
  sql::QueryResultProtoBuilder queryResultBuilder;
  // queryResultBuilder.add_column("result");
  // queryResultBuilder.add_row(result_row.begin(), result_row.end());
  // std::cout << "Before adding columns" << std::endl;
  // Add columns
  for (unsigned int i = 0; i < tuple_descriptor.size(); i++) {
    std::string column_name = std::get<0>(tuple_descriptor[i]);
    queryResultBuilder.add_column(column_name);
  }

  // std::cout << "Before adding rows" << std::endl;
  // std::cout << "Tuple descriptor size is " << tuple_descriptor.size()
  //<< std::endl;

  // Add rows
  unsigned int rows = result.size() / tuple_descriptor.size();
  for (unsigned int i = 0; i < rows; i++) {
    // std::string row_string = "Row " + std::to_string(i) + ": ";
    // std::cout << "Row index is " << i << std::endl;
    // queryResultBuilder.add_empty_row();

    RowProto *row = queryResultBuilder.new_row();
    // queryResultBuilder.add_empty_row();
    for (unsigned int j = 0; j < tuple_descriptor.size(); j++) {
      // TODO: Use interface addtorow, and pass in field to that row

      queryResultBuilder.AddToRow(row, result[i * tuple_descriptor.size() + j]);
      // std::cout << "Get field value" << std::endl;
      //  FieldProto *field = row->add_fields();
      //  std::string field_value = GetResultValueAsString(result, i *
      //  tuple_descriptor.size() + j);
      //  field->set_data(queryResultBuilder.serialize(field_value));
      //  field->set_data(result[i*tuple_descriptor.size()+j]);
      // std::cout << "After" << std::endl;
      //  queryResultBuilder.update_field_in_row(i, j, field_value);
      //  row_string += GetResultValueAsString(result, i *
      //  tuple_descriptor.size() + j);

      // std::cout << "Inside j loop" << std::endl;
      // std::cout << GetResultValueAsString(result, i * tuple_descriptor.size()
      // + j) << std::endl;
    }
  }

  std::cout << "Result from query result builder is " << std::endl;
  // std::cout << queryResultBuilder.get_result()->SerializeAsString() <<
  // std::endl;
  std::string query_result =
      queryResultBuilder.get_result()->SerializeAsString();
  // std::string* copy = new std::string(query_result);
  std::cout << query_result << std::endl;

  return query_result;
}

// Execute a point read on the Table backend and return a query_result/proto (in
// serialized form) as well as a commitProof (note, the read set is implicit)
void TableStore::ExecPointRead(const std::string &query_statement,
                               std::string &enc_primary_key, Timestamp &ts,
                               proto::Write *write,
                               const proto::CommittedProof *committedProof) {
  // TODO: If read_prepared = true read both committed/prepared read
  // if true --> After execution check txn_digest of prepared_value (if exist).
  // Check dependency depth. for txn_digest. If too deep, remove it.
  // FIXME: to have access to this: need server (pass as this in constructor?)
  // ==> No, do this stuff inside the ProcessPointQuery level.
  // TODO: If no write/read exists (result == empty) -> send empty result (i.e.
  // no fields in write are set), read_time = 0 by default
  // WARNING: Don't set prepared or committed -- let client side default
  // handling take care of it.

  // (optional TODO:) For optimal CC we'd ideally send the time of last delete
  // (to minimize conflict window)
  //- but then we have to send it as committed (with proof) or as prepared (with
  // value = empty result) Client will have to check proof txn ==> lookup that
  // key exists in Writeset was marked as delete. Note: For Query reads that
  // would technically be the best too --> the coarse lock of the Table Version
  // helps simulate it.

  // Alternatively:
  // Since we also need to avoid reading prepared for the normal queries:
  // Pass down a Lambda function that takes in txn_digest and checks whether is
  // readable (Like passing an electrical probe down the ocean)
  // Don't read TableVersion (quetion: how do we read table version for normal
  // query? --> let it return table name and then look up?)

  // args: query, Ts, this->can_read_prepared ; commit: (result, timestamp,
  // proof), prepared: (result, timestamp, txn_digest), key (optional)
  // TODO: Execute QueryStatement on Peloton. -> returns peloton result
  // TODO: Read latest committed (return committedProof) + Read latest prepared
  // (if > committed)

  // TODO: Change Peloton result into query proto. //TODO: For both the
  // prepared/committed value sql::QueryResultProtoBuilder queryResultBuilder;
  //  queryResultBuilder.add_column("result");
  //  queryResultBuilder.add_row(result_row.begin(), result_row.end());
  // queryResultBuilder.get_result()->SerializeAsString(); //TODO: store into
  // prepared/committed value

  // TODO: Extract proof/version from CC-store. --> return ReadReply + value =
  // serialized proto result.

  // TODO: use add row interface for serialization //
  // DecodeTableRow(enc_primary_key, table_name, primary_key_column_values);
  std::vector<peloton::ResultValue> result;
  std::vector<peloton::FieldInfo> tuple_descriptor;

  // execute the query using tcop
  // prepareStatement
  // LOG_TRACE("Query: %s", query.c_str());
  std::string unnamed_statement = "unnamed";
  auto &peloton_parser = peloton::parser::PostgresParser::GetInstance();
  auto sql_stmt_list = peloton_parser.BuildParseTree(query_statement);
  // PELOTON_ASSERT(sql_stmt_list);
  if (!sql_stmt_list->is_valid) {
    // return peloton::ResultType::FAILURE;
  }
  auto statement = traffic_cop_.PrepareStatement(
      unnamed_statement, query_statement, std::move(sql_stmt_list));
  if (statement.get() == nullptr) {
    traffic_cop_.setRowsAffected(0);
    // return peloton::ResultType::FAILURE;
  }
  // ExecuteStatment
  std::vector<peloton::type::Value> param_values;
  bool unnamed = false;
  std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);
  // SetTrafficCopCounter();
  counter_.store(1);

  Timestamp committed_timestamp;
  Timestamp prepared_timestamp;
  std::shared_ptr<std::string> txn_dig;

  auto status = traffic_cop_.ExecutePointReadStatement(
      statement, param_values, unnamed, result_format, result, ts,
      this->can_read_prepared, &committed_timestamp, committedProof,
      &prepared_timestamp, txn_dig, write);
  if (traffic_cop_.GetQueuing()) {
    ContinueAfterComplete(counter_);
    traffic_cop_.ExecuteStatementPlanGetResult();
    status = traffic_cop_.ExecuteStatementGetResult();
    traffic_cop_.SetQueuing(false);
  }
  if (status == peloton::ResultType::SUCCESS) {
    tuple_descriptor = statement->GetTupleDescriptor();
  }

  // write->set_committed_value()
  std::cout << "Commit proof client id: "
            << traffic_cop_.commit_proof_->mutable_txn()->client_id()
            << " : sequence number: "
            << traffic_cop_.commit_proof_->mutable_txn()->client_seq_num()
            << std::endl;

  // TODO: Change Peloton result into query proto.
  sql::QueryResultProtoBuilder queryResultBuilder;
  // queryResultBuilder.add_column("result");
  // queryResultBuilder.add_row(result_row.begin(), result_row.end());
  std::cout << "Before adding columns" << std::endl;
  // Add columns
  for (unsigned int i = 0; i < tuple_descriptor.size(); i++) {
    std::string column_name = std::get<0>(tuple_descriptor[i]);
    queryResultBuilder.add_column(column_name);
  }

  // std::cout << "Before adding rows" << std::endl;
  // std::cout << "Tuple descriptor size is " << tuple_descriptor.size()
  //<< std::endl;
  bool read_prepared = false;
  bool already_read_prepared = false;

  // Add rows
  unsigned int rows = result.size() / tuple_descriptor.size();
  for (unsigned int i = 0; i < rows; i++) {
    // std::string row_string = "Row " + std::to_string(i) + ": ";
    // std::cout << "Row index is " << i << std::endl;
    // queryResultBuilder.add_empty_row();
    RowProto *row = queryResultBuilder.new_row();
    std::string row_string = "";

    // queryResultBuilder.add_empty_row();
    for (unsigned int j = 0; j < tuple_descriptor.size(); j++) {
      // queryResultBuilder.AddToRow(row, result[i*tuple_descriptor.size()+j]);
      // std::cout << "Get field value" << std::endl;
      // FieldProto *field = row->add_fields();
      // std::string field_value = GetResultValueAsString(result, i *
      // tuple_descriptor.size() + j);
      queryResultBuilder.AddToRow(row, result[i * tuple_descriptor.size() + j]);
      // field->set_data(queryResultBuilder.serialize(field_value));
      // field->set_data(result[i*tuple_descriptor.size()+j]);
      // std::cout << "After" << std::endl;
      // row_string += field_value + " ";

      // queryResultBuilder.update_field_in_row(i, j, field_value);
      // row_string += GetResultValueAsString(result, i *
      // tuple_descriptor.size() + j);

      // std::cout << "Inside j loop" << std::endl;
      // std::cout << GetResultValueAsString(result, i * tuple_descriptor.size()
      // + j) << std::endl;
    }
    if (read_prepared && !already_read_prepared) {
      write->set_prepared_value(row_string);
      std::cout << "Prepared value is " << row_string << std::endl;
      write->set_prepared_txn_digest(*txn_dig.get());
      std::cout << "Prepared txn digest is " << *txn_dig.get() << std::endl;
      // write->set_allocated_prepared_timestamp(TimestampMessage{prepared_timestamp.getID(),
      // prepared_timestamp.getTimestamp()});
      std::cout << "Prepared timestamp is " << prepared_timestamp.getTimestamp()
                << ", " << prepared_timestamp.getID() << std::endl;

      already_read_prepared = true;
    }

    write->set_committed_value(row_string);
    std::cout << "Committed value is " << row_string << std::endl;
    //  write->set_allocated_committed_timestamp(TimestampMessage(committed_timestamp));
    std::cout << "Commit timestamp is " << committed_timestamp.getTimestamp()
              << ", " << committed_timestamp.getID() << std::endl;
  }
  // write->set_allocated_proof(traffic_cop_.commit_proof_->SerializeAsString());

  std::cout << "Result from query result builder is " << std::endl;
  std::cout << queryResultBuilder.get_result()->SerializeAsString()
            << std::endl;

  // return queryResultBuilder.get_result()->SerializeAsString();

  return;
}
// Note: Could execute PointRead via ExecReadQuery (Eagerly) as well.
//  ExecPointRead should translate enc_primary_key into a query_statement to be
//  exec by ExecReadQuery.
//(Alternatively: Could already send a Sql command from the client) ==> Should
// do it at the client, so that we can keep whatever Select specification, e.g.
// * or specific cols...

// Apply a set of Table Writes (versioned row creations) to the Table backend
void TableStore::ApplyTableWrite(const std::string &table_name,
                                 const TableWrite &table_write, Timestamp &ts,
                                 const std::string &txn_digest,
                                 const proto::CommittedProof *commit_proof,
                                 bool commit_or_prepare) {
  std::cout << "In apply table write" << std::endl;
  // Turn txn_digest into a shared_ptr, write everywhere it is needed.
  std::shared_ptr<std::string> txn_dig(
      std::make_shared<std::string>(txn_digest));

  std::string write_statement;
  std::string delete_statement;
  bool has_delete = sql_interpreter.GenerateTableWriteStatement(
      write_statement, delete_statement, table_name, table_write);
  // TODO: Check whether there is a more efficient way than creating SQL
  // commands for each.

  // TODO: Execute on Peloton
  // Exec write
  std::vector<peloton::ResultValue> result;
  std::vector<peloton::FieldInfo> tuple_descriptor;

  std::cout << "The write statement is: " << write_statement << std::endl;
  std::cout << "The delete statement is: " << delete_statement << std::endl;

  if (!has_delete) {
    // execute the query using tcop
    // prepareStatement
    // LOG_TRACE("Query: %s", query.c_str());
    std::string unnamed_statement = "unnamed";
    auto &peloton_parser = peloton::parser::PostgresParser::GetInstance();
    auto sql_stmt_list = peloton_parser.BuildParseTree(write_statement);
    // PELOTON_ASSERT(sql_stmt_list);
    if (!sql_stmt_list->is_valid) {
      // return peloton::ResultType::FAILURE;
    }
    auto statement = traffic_cop_.PrepareStatement(
        unnamed_statement, write_statement, std::move(sql_stmt_list));
    if (statement.get() == nullptr) {
      traffic_cop_.setRowsAffected(0);
      // return peloton::ResultType::FAILURE;
    }
    // ExecuteStatment
    std::vector<peloton::type::Value> param_values;
    bool unnamed = false;
    std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);
    // SetTrafficCopCounter();
    counter_.store(1);
    auto status = traffic_cop_.ExecuteWriteStatement(
        statement, param_values, unnamed, result_format, result, ts, txn_dig,
        commit_proof, commit_or_prepare);
    if (traffic_cop_.GetQueuing()) {
      ContinueAfterComplete(counter_);
      traffic_cop_.ExecuteStatementPlanGetResult();
      status = traffic_cop_.ExecuteStatementGetResult();
      traffic_cop_.SetQueuing(false);
    }
    if (status == peloton::ResultType::SUCCESS) {
      tuple_descriptor = statement->GetTupleDescriptor();
    }

    // TODO: Change Peloton result into query proto.
    sql::QueryResultProtoBuilder queryResultBuilder;
    // queryResultBuilder.add_column("result");
    // queryResultBuilder.add_row(result_row.begin(), result_row.end());
    std::cout << "Before adding columns" << std::endl;
    // Add columns
    for (unsigned int i = 0; i < tuple_descriptor.size(); i++) {
      std::string column_name = std::get<0>(tuple_descriptor[i]);
      queryResultBuilder.add_column(column_name);
    }
  }

  // execute the query using tcop
  // prepareStatement
  // LOG_TRACE("Query: %s", query.c_str());
  /*std::string unnamed_statement = "unnamed";
  auto &peloton_parser = peloton::parser::PostgresParser::GetInstance();
  auto sql_stmt_list = peloton_parser.BuildParseTree(write_statement);
  //PELOTON_ASSERT(sql_stmt_list);
  if (!sql_stmt_list->is_valid) {
  //return peloton::ResultType::FAILURE;
  }
  auto statement = traffic_cop_.PrepareStatement(unnamed_statement,
write_statement, std::move(sql_stmt_list)); if (statement.get() == nullptr) {
          traffic_cop_.setRowsAffected(0);
          //return peloton::ResultType::FAILURE;
  }
  // ExecuteStatment
  std::vector<peloton::type::Value> param_values;
  bool unnamed = false;
  std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);
  // SetTrafficCopCounter();
  counter_.store(1);
  auto status = traffic_cop_.ExecuteWriteStatement(statement, param_values,
unnamed, result_format, result, ts, txn_dig, commit_proof, commit_or_prepare);
  if (traffic_cop_.GetQueuing()) {
          ContinueAfterComplete(counter_);
          traffic_cop_.ExecuteStatementPlanGetResult();
          status = traffic_cop_.ExecuteStatementGetResult();
          traffic_cop_.SetQueuing(false);
  }
  if (status == peloton::ResultType::SUCCESS) {
          tuple_descriptor = statement->GetTupleDescriptor();
  }

//TODO: Change Peloton result into query proto.
sql::QueryResultProtoBuilder queryResultBuilder;
// queryResultBuilder.add_column("result");
// queryResultBuilder.add_row(result_row.begin(), result_row.end());
  std::cout << "Before adding columns" << std::endl;
  // Add columns
  for (unsigned int i = 0; i < tuple_descriptor.size(); i++) {
          std::string column_name = std::get<0>(tuple_descriptor[i]);
          queryResultBuilder.add_column(column_name);
  }*/

  // TODO: Replace has_delete with !delete_statement.empty()
  // if(has_delete) Exec delete
  if (has_delete) {
    std::string unnamed_statement = "unnamed";
    auto &peloton_parser = peloton::parser::PostgresParser::GetInstance();
    auto sql_stmt_list = peloton_parser.BuildParseTree(delete_statement);

    // PELOTON_ASSERT(sql_stmt_list);
    if (!sql_stmt_list->is_valid) {
      // return peloton::ResultType::FAILURE;
    }
    auto statement = traffic_cop_.PrepareStatement(
        unnamed_statement, delete_statement, std::move(sql_stmt_list));
    if (statement.get() == nullptr) {
      traffic_cop_.setRowsAffected(0);
      // return peloton::ResultType::FAILURE;
    }
    // ExecuteStatment
    std::vector<peloton::type::Value> param_values;
    param_values.clear();
    bool unnamed = false;
    std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);
    // SetTrafficCopCounter();
    counter_.store(1);
    auto status = traffic_cop_.ExecuteWriteStatement(
        statement, param_values, unnamed, result_format, result, ts, txn_dig,
        commit_proof, commit_or_prepare);
    if (traffic_cop_.GetQueuing()) {
      ContinueAfterComplete(counter_);
      traffic_cop_.ExecuteStatementPlanGetResult();
      status = traffic_cop_.ExecuteStatementGetResult();
      traffic_cop_.SetQueuing(false);
    }
    if (status == peloton::ResultType::SUCCESS) {
      tuple_descriptor = statement->GetTupleDescriptor();
    }

    // TODO: add boolean to sometimes not force writes and deletes
    // Don't need to return anything

    // TODO: Change Peloton result into query proto.
    /*sql::QueryResultProtoBuilder queryResultBuilder1;
    // queryResultBuilder.add_column("result");
    // queryResultBuilder.add_row(result_row.begin(), result_row.end());
    std::cout << "Before adding columns" << std::endl;
    // Add columns
    for (unsigned int i = 0; i < tuple_descriptor.size(); i++) {
            std::string column_name = std::get<0>(tuple_descriptor[i]);
            queryResultBuilder1.add_column(column_name);
    }*/
  }

  // TODO: Confirm that ApplyTableWrite is synchronous -- i.e. only returns
  // after all writes are applied. If not, then must call SetTableVersion as
  // callback from within Peloton once it is done to set the TableVersion
  // (Currently, it is being set right after ApplyTableWrite() returns)
}

void TableStore::PurgeTableWrite(const std::string &table_name,
                                 const TableWrite &table_write, Timestamp &ts,
                                 const std::string &txn_digest) {

  std::shared_ptr<std::string> txn_dig(
      std::make_shared<std::string>(txn_digest));

  std::string purge_statement;
  bool has_purge = sql_interpreter.GenerateTablePurgeStatement(
      purge_statement, table_name, table_write);
  has_purge = true;

  std::cout << "Has purge value is " << has_purge << std::endl;
  std::cout << "Purge statement is " << purge_statement << std::endl;

  std::vector<peloton::ResultValue> result;
  std::vector<peloton::FieldInfo> tuple_descriptor;

  // execute the query using tcop
  // prepareStatement
  // LOG_TRACE("Query: %s", query.c_str());
  std::string unnamed_statement = "unnamed";
  auto &peloton_parser = peloton::parser::PostgresParser::GetInstance();
  auto sql_stmt_list = peloton_parser.BuildParseTree(purge_statement);
  // PELOTON_ASSERT(sql_stmt_list);

  auto statement = traffic_cop_.PrepareStatement(
      unnamed_statement, purge_statement, std::move(sql_stmt_list));

  if (statement.get() == nullptr) {
    traffic_cop_.setRowsAffected(0);
  }
  std::vector<peloton::type::Value> param_values;
  bool unnamed = false;
  param_values.clear();
  std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);

  // SetTrafficCopCounter();
  counter_.store(1);
  pequinstore::proto::CommittedProof commit_proof;
  auto status = traffic_cop_.ExecutePurgeStatement(
      statement, param_values, unnamed, result_format, result, ts, txn_dig,
      has_purge);
  if (traffic_cop_.GetQueuing()) {
    ContinueAfterComplete(counter_);
    traffic_cop_.ExecuteStatementPlanGetResult();
    status = traffic_cop_.ExecuteStatementGetResult();
    traffic_cop_.SetQueuing(false);
  }
  if (status == peloton::ResultType::SUCCESS) {
    tuple_descriptor = statement->GetTupleDescriptor();
  }

  // TODO: Purge statement is a "special" delete statement:
  //  it deletes existing row insertions for the timestamp
  //  but it also undoes existing deletes for the timestamp
  //  Check the verison linked list and delete any row past timestamp (don't
  //  need to distinguish between deletes and inserts)

  // TODO: For both normal deletes and purge (special deletes) need to not use
  // the index, and insert an invisible row, check linked list from back and hit
  // timestamp that is smaller then return May have false positives but okay
  // Sidestep index always go to data structure linked list
  //==> Effectively it is "aborting" all suggested table writes.

  // TODO: Execute on Peloton
}

///////////////////// Snapshot Protocol Support

// Partially execute a read query statement (reconnaissance execution) and
// return the snapshot state (managed by ssMgr)
void TableStore::FindSnapshot(std::string &query_statement, const Timestamp &ts,
                              SnapshotManager &ssMgr) {

  // TODO: Execute on Peloton
  // Note: Don't need to return a result
  // Note: Ideally execution is "partial" and only executes the leaf scan
  // operations.
}

// Materialize a snapshot on the Table backend and execute on said snapshot.
void TableStore::MaterializeSnapshot(
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

std::string TableStore::ExecReadOnSnapshot(const std::string &query_retry_id,
                                           std::string &query_statement,
                                           const Timestamp &ts,
                                           QueryReadSetMgr &readSetMgr,
                                           bool abort_early) {
  // TODO: Execute on the snapshot for query_id/retry_version

  //--> returns peloton result
  // TODO: Change peloton result into query proto:

  sql::QueryResultProtoBuilder queryResultBuilder;
  // queryResultBuilder.add_column("result");
  // queryResultBuilder.add_row(result_row.begin(), result_row.end());

  return queryResultBuilder.get_result()->SerializeAsString();
}

} // namespace pequinstore
