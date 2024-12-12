#include <iostream>
#include <ostream>
//#include "common/harness.h"
#include "common/logger.h"
#include "common/macros.h"
#include "parser/drop_statement.h"
#include "parser/postgresparser.h"

#include "catalog/catalog.h"
#include "catalog/proc_catalog.h"
#include "catalog/system_catalogs.h"

#include "concurrency/transaction_manager_factory.h"

#include "executor/create_executor.h"
#include "executor/create_function_executor.h"
#include "executor/executor_context.h"

#include "planner/create_function_plan.h"
#include "planner/create_plan.h"
#include "storage/data_table.h"

#include "executor/insert_executor.h"
#include "expression/constant_value_expression.h"
#include "parser/insert_statement.h"
#include "planner/insert_plan.h"
#include "type/value_factory.h"
#include "traffic_cop/traffic_cop.h"
#include "type/type.h"

#include "store/common/timestamp.h"
#include "store/sintrstore/common.h"

#include "store/sintrstore/table_store_interface.h"
#include "store/common/query_result/query_result_proto_builder.h"
#include "store/common/query_result/query_result.h"
#include "store/common/query_result/query_result_proto_wrapper.h"

#include "store/sintrstore/sql_interpreter.h"
#include "store/benchmark/async/json_table_writer.h"

static std::string GetResultValueAsString(
      const std::vector<peloton_sintr::ResultValue> &result, size_t index) {
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

// Should additionally take a timestamp and a txn id
peloton_sintr::ResultType ExecuteSQLQuery(const std::string query, peloton_sintr::tcop::TrafficCop &traffic_cop, std::atomic_int &counter_, std::vector<peloton_sintr::ResultValue> &result, std::vector<peloton_sintr::FieldInfo> &tuple_descriptor, Timestamp &basil_timestamp, sintrstore::QueryReadSetMgr &query_read_set_mgr) {
  //std::vector<peloton_sintr::ResultValue> result;
  //std::vector<peloton_sintr::FieldInfo> tuple_descriptor;

  // execute the query using tcop
  // prepareStatement
  //LOG_TRACE("Query: %s", query.c_str());
  std::string unnamed_statement = "unnamed";
  auto &peloton_parser = peloton_sintr::parser::PostgresParser::GetInstance();
  auto sql_stmt_list = peloton_parser.BuildParseTree(query);
  //PELOTON_ASSERT(sql_stmt_list);
  if (!sql_stmt_list->is_valid) {
    return peloton_sintr::ResultType::FAILURE;
  }
  auto statement = traffic_cop.PrepareStatement(unnamed_statement, query,
                                                 std::move(sql_stmt_list));
  if (statement.get() == nullptr) {
    traffic_cop.setRowsAffected(0);
    return peloton_sintr::ResultType::FAILURE;
  }
  // ExecuteStatment
  std::vector<peloton_sintr::type::Value> param_values;
  bool unnamed = false;
  std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);
  // SetTrafficCopCounter();
  counter_.store(1);
  auto status = traffic_cop.ExecuteStatement(statement, param_values, unnamed,
                                              result_format, result);
  if (traffic_cop.GetQueuing()) {
    ContinueAfterComplete(counter_);
    traffic_cop.ExecuteStatementPlanGetResult();
    status = traffic_cop.ExecuteStatementGetResult();
    traffic_cop.SetQueuing(false);
  }
  if (status == peloton_sintr::ResultType::SUCCESS) {
    tuple_descriptor = statement->GetTupleDescriptor();
  }

  //std::cerr << "Result 1st value is " << GetResultValueAsString(result, 0) << std::endl;
  /*LOG_TRACE("Statement executed. Result: %s",
           ResultTypeToString(status).c_str());*/
  return status;
}

std::string ConvertTableWriteToUpdate(std::string encoded_key, std::vector<std::vector<std::string>> values) {
  std::string update_statement = "UPDATE test SET ";
  for (unsigned int i = 1; i < values.size(); i++) {
    for (unsigned int j = 0; j < values[i].size(); j++) {
      //update_statement = update_statement + values[0][j] + "=" values[i][j] + " ";
    }
  }
  return update_statement;
}

int main(int argc, char *argv[]) {
  std::cerr << "Beginning of query exec test" << std::endl;
  auto &txn_manager = peloton_sintr::concurrency::TransactionManagerFactory::GetInstance();
  std::cerr << "query exec test second" << std::endl;
  auto txn = txn_manager.BeginTransaction();
  std::cerr << "query exec test third" << std::endl;
  peloton_sintr::catalog::Catalog::GetInstance()->CreateDatabase(txn, DEFAULT_DB_NAME);
  std::cerr << "query exec test fourth" << std::endl;
  txn_manager.CommitTransaction(txn);
  std::cerr << "query exec test fifth" << std::endl;

	Timestamp pesto_timestamp(4, 6);
	sintrstore::proto::ReadSet read_set_one;
	sintrstore::QueryReadSetMgr query_read_set_mgr_one(&read_set_one, 1, false);

  /*std::atomic_int counter_;
  std::vector<peloton_sintr::ResultValue> result;
  std::vector<peloton_sintr::FieldInfo> tuple_descriptor;
  peloton_sintr::tcop::TrafficCop traffic_cop(UtilTestTaskCallback, &counter_);
  Timestamp pesto_timestamp(4, 6);
  Timestamp basil_timestamp(3, 5);
  Timestamp read_timestamp(2, 4);
  Timestamp five(5,7);
  Timestamp six(6,8);

  sintrstore::proto::ReadSet read_set_one;
  sintrstore::proto::ReadSet read_set_two;
  sintrstore::proto::ReadSet read_set_three;
  sintrstore::proto::ReadSet read_set_four;
  sintrstore::proto::ReadSet read_set_five;
  sintrstore::proto::ReadSet read_set_six;
  sintrstore::proto::ReadSet read_set_seven;

  sintrstore::QueryReadSetMgr query_read_set_mgr_one(&read_set_one, 1, false);
  sintrstore::QueryReadSetMgr query_read_set_mgr_two(&read_set_two, 2, false);
  sintrstore::QueryReadSetMgr query_read_set_mgr_three(&read_set_three, 3, false);
  sintrstore::QueryReadSetMgr query_read_set_mgr_four(&read_set_four, 4, false);
  sintrstore::QueryReadSetMgr query_read_set_mgr_five(&read_set_five, 5, false);
  sintrstore::QueryReadSetMgr query_read_set_mgr_six(&read_set_six, 6, false);
  sintrstore::QueryReadSetMgr query_read_set_mgr_seven(&read_set_seven, 7, false);

  std::cerr << "Before first query" << std::endl;
  ExecuteSQLQuery("CREATE TABLE test(a INT, b INT, PRIMARY KEY(a));", traffic_cop, counter_, result, tuple_descriptor, pesto_timestamp, query_read_set_mgr_one);
  std::cerr << "After first query" << std::endl;
  ExecuteSQLQuery("INSERT INTO test VALUES (99, 999);", traffic_cop, counter_, result, tuple_descriptor, pesto_timestamp, query_read_set_mgr_two);
  std::cerr << "After second query" << std::endl;
  ExecuteSQLQuery("INSERT INTO test VALUES (1001, 10001);", traffic_cop, counter_, result, tuple_descriptor, pesto_timestamp, query_read_set_mgr_three);
  std::cerr << "After third query" << std::endl;
  ExecuteSQLQuery("UPDATE test SET b=72 WHERE a=99;", traffic_cop, counter_, result, tuple_descriptor, basil_timestamp, query_read_set_mgr_four);
  std::cerr << "After fourth query" << std::endl;
  ExecuteSQLQuery("UPDATE test SET b=855 WHERE a=99;", traffic_cop, counter_, result, tuple_descriptor, five, query_read_set_mgr_five);
  std::cerr << "After fifth query" << std::endl;
  ExecuteSQLQuery("UPDATE test SET b=16 WHERE a=99;", traffic_cop, counter_, result, tuple_descriptor, read_timestamp, query_read_set_mgr_six);
  std::cerr << "After sixth query" << std::endl;
  ExecuteSQLQuery("INSERT INTO test VALUES (1001, 542);", traffic_cop, counter_, result, tuple_descriptor, pesto_timestamp, query_read_set_mgr_three);
  ExecuteSQLQuery("SELECT * FROM test;", traffic_cop, counter_, result, tuple_descriptor, pesto_timestamp, query_read_set_mgr_seven);

  std::cerr << "Result is" << std::endl;
  std::cerr << "---------------------------------------------------------" << std::endl;

  unsigned int rows = result.size() / tuple_descriptor.size();
  for (unsigned int i = 0; i < rows; i++) {
    std::string row_string = "Row " + std::to_string(i) + ": ";
    for (unsigned int j = 0; j < tuple_descriptor.size(); j++) {
      row_string += GetResultValueAsString(result, i * tuple_descriptor.size() + j);
      if (j < tuple_descriptor.size() - 1) {
        row_string += "|";
      }
    }

    std::cerr << row_string << std::endl;
  }

  std::cerr << "---------------------------------------------------------" << std::endl;
  std::cerr << "Read Set is not" << std::endl;

  if (query_read_set_mgr_seven.read_set->mutable_read_set() != nullptr) {
    std::cerr << "Query read set is not null" << std::endl;
  }

  for(auto &read_msg : *(query_read_set_mgr_seven.read_set->mutable_read_set())){
      std::cerr << "Encoded key: " << read_msg.key() << ". Timestamp: (" << read_msg.readtime().timestamp() << ", " << read_msg.readtime().id() << ")" << std::endl;
  }*/

  Timestamp toy_ts_c(10, 12);
  sintrstore::proto::CommittedProof *real_proof = new sintrstore::proto::CommittedProof();
  real_proof->mutable_txn()->set_client_id(10);
  real_proof->mutable_txn()->set_client_seq_num(12);
  toy_ts_c.serialize(real_proof->mutable_txn()->mutable_timestamp());
  TableWrite &table_write = (*real_proof->mutable_txn()->mutable_table_writes())["test"];
  
  RowUpdates *row1 = table_write.add_rows();
  row1->add_column_values("42");
  row1->add_column_values("54");

  RowUpdates *row2 = table_write.add_rows();
  row2->add_column_values("24");
  row2->add_column_values("225");

  RowUpdates *row3 = table_write.add_rows();
  row3->add_column_values("34");
  row3->add_column_values("315");
  row3->set_deletion(true);

  /*TableWrite &table_write_1 = (*real_proof->mutable_txn()->mutable_table_writes())["test"];
  RowUpdates *row_1 = table_write.add_rows();
  row_1->add_column_values("42");
  row_1->add_column_values("54");
  row_1->set_deletion(true);*/


  static std::string file_name = "sql_interpreter_test_registry";
  //Create desired registry via table writer.
  std::string table_name = "test";
  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  std::vector<uint32_t> primary_key_col_idx;

  TableWriter table_writer(file_name);

  //Table1:
  table_name = "test";
  column_names_and_types.push_back(std::make_pair("a", "INT"));
  column_names_and_types.push_back(std::make_pair("b", "INT"));
  primary_key_col_idx.push_back(0);
  table_writer.add_table(table_name, column_names_and_types, primary_key_col_idx);
  //Write Tables to JSON
  table_writer.flush();

	sintrstore::TableStore table_store;
  sintrstore::proto::Write write;
  sintrstore::proto::CommittedProof committed_proof;
  std::string table_registry = file_name + "-tables-schema.json";
  table_store.RegisterTableSchema(table_registry);
	table_store.ExecRaw("CREATE TABLE test(a INT, b INT, PRIMARY KEY(a));");
	//table_store.ExecRaw("INSERT INTO test VALUES (42, 54);");
	//table_store.ExecRaw("INSERT INTO test VALUES (35, 26);");
	//table_store.ExecRaw("INSERT INTO test VALUES (190, 999);");
  table_store.ApplyTableWrite("test", table_write, toy_ts_c, "random", real_proof, true);
  std::cerr << "New change 10" << std::endl;
  //table_store.ApplyTableWrite("test", table_write_1, toy_ts_c, "random", real_proof, true);
  std::string enc_primary_key = "test//24";
  //table_store.ExecRaw("DELETE FROM test WHERE a=24;");
  std::cerr << "End of queryexec test" << std::endl;
  //table_store.ExecPointRead("SELECT * FROM test WHERE a=34;", enc_primary_key, toy_ts_c, &write, &committed_proof);

	table_store.ExecReadQuery("SELECT * FROM test;", toy_ts_c, query_read_set_mgr_one);

  return 0;
}
