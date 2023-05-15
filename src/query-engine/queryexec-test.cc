#include <iostream>
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
#include "store/pequinstore/common.h"

static std::string GetResultValueAsString(
      const std::vector<peloton::ResultValue> &result, size_t index) {
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
peloton::ResultType ExecuteSQLQuery(const std::string query, peloton::tcop::TrafficCop &traffic_cop, std::atomic_int &counter_, std::vector<peloton::ResultValue> &result, std::vector<peloton::FieldInfo> &tuple_descriptor, Timestamp &basil_timestamp, pequinstore::QueryReadSetMgr &query_read_set_mgr) {
  //std::vector<peloton::ResultValue> result;
  //std::vector<peloton::FieldInfo> tuple_descriptor;

  // execute the query using tcop
  // prepareStatement
  //LOG_TRACE("Query: %s", query.c_str());
  std::string unnamed_statement = "unnamed";
  auto &peloton_parser = peloton::parser::PostgresParser::GetInstance();
  auto sql_stmt_list = peloton_parser.BuildParseTree(query);
  //PELOTON_ASSERT(sql_stmt_list);
  if (!sql_stmt_list->is_valid) {
    return peloton::ResultType::FAILURE;
  }
  auto statement = traffic_cop.PrepareStatement(unnamed_statement, query,
                                                 std::move(sql_stmt_list));
  if (statement.get() == nullptr) {
    traffic_cop.setRowsAffected(0);
    return peloton::ResultType::FAILURE;
  }
  // ExecuteStatment
  std::vector<peloton::type::Value> param_values;
  bool unnamed = false;
  std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);
  // SetTrafficCopCounter();
  counter_.store(1);
  auto status = traffic_cop.ExecuteStatement(statement, param_values, unnamed,
                                              result_format, result, basil_timestamp, query_read_set_mgr);
  if (traffic_cop.GetQueuing()) {
    ContinueAfterComplete(counter_);
    traffic_cop.ExecuteStatementPlanGetResult();
    status = traffic_cop.ExecuteStatementGetResult();
    traffic_cop.SetQueuing(false);
  }
  if (status == peloton::ResultType::SUCCESS) {
    tuple_descriptor = statement->GetTupleDescriptor();
  }

  //std::cout << "Result 1st value is " << GetResultValueAsString(result, 0) << std::endl;
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
  std::cout << "Beginning of query exec test" << std::endl;
  auto &txn_manager = peloton::concurrency::TransactionManagerFactory::GetInstance();
  std::cout << "query exec test second" << std::endl;
  auto txn = txn_manager.BeginTransaction();
  std::cout << "query exec test third" << std::endl;
  peloton::catalog::Catalog::GetInstance()->CreateDatabase(txn, DEFAULT_DB_NAME);
  std::cout << "query exec test fourth" << std::endl;
  txn_manager.CommitTransaction(txn);
  std::cout << "query exec test fifth" << std::endl;

  std::atomic_int counter_;
  std::vector<peloton::ResultValue> result;
  std::vector<peloton::FieldInfo> tuple_descriptor;
  peloton::tcop::TrafficCop traffic_cop(UtilTestTaskCallback, &counter_);
  Timestamp pesto_timestamp(4, 6);
  Timestamp basil_timestamp(3, 5);
  Timestamp read_timestamp(2, 4);
  Timestamp five(5,7);
  Timestamp six(6,8);

  /*std::unique_ptr<pequinstore::QueryReadSetMgr> query_read_set_mgr_one(new pequinstore::QueryReadSetMgr);
  std::unique_ptr<pequinstore::QueryReadSetMgr> query_read_set_mgr_two(new pequinstore::QueryReadSetMgr);
  std::unique_ptr<pequinstore::QueryReadSetMgr> query_read_set_mgr_three(new pequinstore::QueryReadSetMgr);
  std::unique_ptr<pequinstore::QueryReadSetMgr> query_read_set_mgr_four(new pequinstore::QueryReadSetMgr);
  std::unique_ptr<pequinstore::QueryReadSetMgr> query_read_set_mgr_five(new pequinstore::QueryReadSetMgr);
  std::unique_ptr<pequinstore::QueryReadSetMgr> query_read_set_mgr_six(new pequinstore::QueryReadSetMgr);
  std::unique_ptr<pequinstore::QueryReadSetMgr> query_read_set_mgr_seven(new pequinstore::QueryReadSetMgr);*/

  pequinstore::proto::ReadSet read_set_one;
  pequinstore::proto::ReadSet read_set_two;
  pequinstore::proto::ReadSet read_set_three;
  pequinstore::proto::ReadSet read_set_four;
  pequinstore::proto::ReadSet read_set_five;
  pequinstore::proto::ReadSet read_set_six;
  pequinstore::proto::ReadSet read_set_seven;

  pequinstore::QueryReadSetMgr query_read_set_mgr_one(&read_set_one, 1, false);
  pequinstore::QueryReadSetMgr query_read_set_mgr_two(&read_set_two, 2, false);
  pequinstore::QueryReadSetMgr query_read_set_mgr_three(&read_set_three, 3, false);
  pequinstore::QueryReadSetMgr query_read_set_mgr_four(&read_set_four, 4, false);
  pequinstore::QueryReadSetMgr query_read_set_mgr_five(&read_set_five, 5, false);
  pequinstore::QueryReadSetMgr query_read_set_mgr_six(&read_set_six, 6, false);
  pequinstore::QueryReadSetMgr query_read_set_mgr_seven(&read_set_seven, 7, false);

  /*pequinstore::QueryReadSetMgr* query_read_set_mgr_one = new pequinstore::QueryReadSetMgr;
  pequinstore::QueryReadSetMgr* query_read_set_mgr_two = new pequinstore::QueryReadSetMgr;
  pequinstore::QueryReadSetMgr* query_read_set_mgr_three = new pequinstore::QueryReadSetMgr;
  pequinstore::QueryReadSetMgr* query_read_set_mgr_four = new pequinstore::QueryReadSetMgr;
  pequinstore::QueryReadSetMgr* query_read_set_mgr_five = new pequinstore::QueryReadSetMgr;
  pequinstore::QueryReadSetMgr* query_read_set_mgr_six = new pequinstore::QueryReadSetMgr;
  pequinstore::QueryReadSetMgr* query_read_set_mgr_seven = new pequinstore::QueryReadSetMgr;*/

  /** Commented out queries */
  std::cout << "Before first query" << std::endl;
  ExecuteSQLQuery("CREATE TABLE test(a INT, b INT, PRIMARY KEY(a));", traffic_cop, counter_, result, tuple_descriptor, pesto_timestamp, query_read_set_mgr_one);
  std::cout << "After first query" << std::endl;
  ExecuteSQLQuery("INSERT INTO test VALUES (99, 999);", traffic_cop, counter_, result, tuple_descriptor, pesto_timestamp, query_read_set_mgr_two);
  std::cout << "After second query" << std::endl;
  ExecuteSQLQuery("INSERT INTO test VALUES (1001, 10001);", traffic_cop, counter_, result, tuple_descriptor, pesto_timestamp, query_read_set_mgr_three);
  std::cout << "After third query" << std::endl;
  ExecuteSQLQuery("UPDATE test SET b=72 WHERE a=99;", traffic_cop, counter_, result, tuple_descriptor, basil_timestamp, query_read_set_mgr_four);
  std::cout << "After fourth query" << std::endl;
  ExecuteSQLQuery("UPDATE test SET b=855 WHERE a=99;", traffic_cop, counter_, result, tuple_descriptor, five, query_read_set_mgr_five);
  std::cout << "After fifth query" << std::endl;
  ExecuteSQLQuery("UPDATE test SET b=16 WHERE a=99;", traffic_cop, counter_, result, tuple_descriptor, read_timestamp, query_read_set_mgr_six);
  std::cout << "After sixth query" << std::endl;
  ExecuteSQLQuery("INSERT INTO test VALUES (1001, 542);", traffic_cop, counter_, result, tuple_descriptor, pesto_timestamp, query_read_set_mgr_three);
  //ExecuteSQLQuery("INSERT INTO test VALUES (99, 24) ON CONFLICT (a) DO UPDATE SET b=EXCLUDED.b;", traffic_cop, counter_, result, tuple_descriptor, read_timestamp, query_read_set_mgr_six);
  ExecuteSQLQuery("SELECT * FROM test;", traffic_cop, counter_, result, tuple_descriptor, pesto_timestamp, query_read_set_mgr_seven);
  //ExecuteSQLQuery("CREATE TABLE string(a TEXT, b TEXT, PRIMARY KEY(a));", traffic_cop, counter_, result, tuple_descriptor, pesto_timestamp, query_read_set_mgr_one);
  //ExecuteSQLQuery("INSERT INTO string VALUES ('apple', 'pear');", traffic_cop, counter_, result, tuple_descriptor, pesto_timestamp, query_read_set_mgr_two);
  //ExecuteSQLQuery("SELECT * FROM string;", traffic_cop, counter_, result, tuple_descriptor, pesto_timestamp, query_read_set_mgr_seven);
  //std::cout << "After seventh query" << std::endl;
  
  // function args: timestamp, snapshot manager, read set manager, boolean flag finding snapshot/executing (read set manager)
  //ExecuteSQLQuery("CREATE TABLE test1(c INT, b INT);", traffic_cop, counter_, result, basil_timestamp);
  //ExecuteSQLQuery("CREATE TABLE test2(b INT, d INT, e INT)", traffic_cop, counter_, result, basil_timestamp);
  //ExecuteSQLQuery("INSERT INTO test1 VALUES (99, 200);", traffic_cop, counter_, result, basil_timestamp);
  //ExecuteSQLQuery("INSERT INTO test1 VALUES (1001, 2202);", traffic_cop, counter_, result, basil_timestamp);
  //ExecuteSQLQuery("INSERT INTO test2 VALUES (2202, 3303, 4404);", traffic_cop, counter_, result, basil_timestamp);
  //ExecuteSQLQuery("INSERT INTO test2 VALUES (3303);", traffic_cop, counter_, result);
  //ExecuteSQLQuery("INSERT INTO test2 VALUES (4404);", traffic_cop, counter_, result);
  //ExecuteSQLQuery("SELECT * FROM (SELECT test1.b FROM test1 JOIN test ON test1.c = test.a WHERE test1.b > 100) AS X JOIN test2 ON test2.b=X.b;", traffic_cop, counter_, result, basil_timestamp);
  
  //std::cout << "Statement executed!! Result: " << peloton::ResultTypeToString(status).c_str() << std::endl;
  //std::cout << "Result size: " << result.size() /*<< " First row " << GetResultValueAsString(result, 0) << " Second row " << GetResultValueAsString(result, 1)*/ << std::endl;

  std::cout << "Result is" << std::endl;
  std::cout << "---------------------------------------------------------" << std::endl;

  unsigned int rows = result.size() / tuple_descriptor.size();
  for (unsigned int i = 0; i < rows; i++) {
    std::string row_string = "Row " + std::to_string(i) + ": ";
    for (unsigned int j = 0; j < tuple_descriptor.size(); j++) {
      row_string += GetResultValueAsString(result, i * tuple_descriptor.size() + j);
      if (j < tuple_descriptor.size() - 1) {
        row_string += "|";
      }
    }

    std::cout << row_string << std::endl;
  }

  std::cout << "---------------------------------------------------------" << std::endl;
  std::cout << "Read Set is not" << std::endl;

  if (query_read_set_mgr_seven.read_set->mutable_read_set() != nullptr) {
    std::cout << "Query read set is not null" << std::endl;
  }

  for(auto &read_msg : *(query_read_set_mgr_seven.read_set->mutable_read_set())){
      std::cout << "Encoded key: " << read_msg.key() << ". Timestamp: (" << read_msg.readtime().timestamp() << ", " << read_msg.readtime().id() << ")" << std::endl;
  }

  
  /*for (unsigned int i = 0; i < traffic_cop.p_status_.read_set.size(); i++) {
    std::string encoded_key;
    Timestamp key_ts;

    std::tie(encoded_key, key_ts) = traffic_cop.p_status_.read_set[i];
    std::cout << "Encoded key: " << encoded_key << ". Timestamp: (" << key_ts.getTimestamp() << ", " << key_ts.getID() << ")" << std::endl;
  }*/

  /*delete query_read_set_mgr_one;
  delete query_read_set_mgr_two;
  delete query_read_set_mgr_three;
  delete query_read_set_mgr_four;
  delete query_read_set_mgr_five;
  delete query_read_set_mgr_six;
  delete query_read_set_mgr_seven;*/

  return 0;
}
