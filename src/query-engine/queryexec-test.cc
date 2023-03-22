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

peloton::ResultType ExecuteSQLQuery(const std::string query, peloton::tcop::TrafficCop &traffic_cop, std::atomic_int &counter_, std::vector<peloton::ResultValue> &result) {
  //std::vector<peloton::ResultValue> result;
  std::vector<peloton::FieldInfo> tuple_descriptor;

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
  std::cout << "After prepare statement" << std::endl;
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
                                              result_format, result);
  std::cout << "After execute statement" << std::endl;
  if (traffic_cop.GetQueuing()) {
    ContinueAfterComplete(counter_);
    traffic_cop.ExecuteStatementPlanGetResult();
    std::cout << "After execute statement plan get result" << std::endl;
    status = traffic_cop.ExecuteStatementGetResult();
    std::cout << "After execute statement get result" << std::endl;
    traffic_cop.SetQueuing(false);
  }
  /*if (status == peloton::ResultType::SUCCESS) {
    tuple_descriptor = statement->GetTupleDescriptor();
  }*/

  //std::cout << "Result 1st value is " << GetResultValueAsString(result, 0) << std::endl;
  /*LOG_TRACE("Statement executed. Result: %s",
           ResultTypeToString(status).c_str());*/
  return status;
}

int main(int argc, char *argv[]) {
  auto &txn_manager = peloton::concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  peloton::catalog::Catalog::GetInstance()->CreateDatabase(txn, DEFAULT_DB_NAME);
  txn_manager.CommitTransaction(txn);

  std::atomic_int counter_;
  std::vector<peloton::ResultValue> result;
  peloton::tcop::TrafficCop traffic_cop(UtilTestTaskCallback, &counter_);
  //auto &traffic_cop = peloton::tcop::TrafficCop::GetInstance();

  /*std::string unnamed_statement = "unnamed";
  std::string query = "CREATE TABLE test(salary INT); INSERT INTO test VALUES (100); SELECT * FROM test;";  
  auto &peloton_parser = peloton::parser::PostgresParser::GetInstance();
  auto sql_stmt_list = peloton_parser.BuildParseTree(query);

  auto statement = traffic_cop.PrepareStatement(unnamed_statement, query,
                                                 std::move(sql_stmt_list));
  
  std::vector<peloton::type::Value> param_values;
  bool unnamed = false;
  std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);
  std::vector<peloton::ResultValue> result;
  auto status = traffic_cop.ExecuteStatement(statement, param_values, unnamed,
                                              result_format, result);

  if (traffic_cop.GetQueuing()) {
    //ContinueAfterComplete();
    usleep(10);
    traffic_cop.ExecuteStatementPlanGetResult();
    status = traffic_cop.ExecuteStatementGetResult();
    traffic_cop.SetQueuing(false);
  }*/

  ExecuteSQLQuery("CREATE TABLE test(a INT);", traffic_cop, counter_, result);
  ExecuteSQLQuery("INSERT INTO test VALUES (99);", traffic_cop, counter_, result);
  ExecuteSQLQuery("INSERT INTO test VALUES (1001);", traffic_cop, counter_, result);
  // function args: timestamp, snapshot manager, read set manager, boolean flag finding snapshot/executing (read set manager)
  ExecuteSQLQuery("CREATE TABLE test1(c INT, b INT);", traffic_cop, counter_, result);
  ExecuteSQLQuery("CREATE TABLE test2(b INT, d INT, e INT)", traffic_cop, counter_, result);
  ExecuteSQLQuery("INSERT INTO test1 VALUES (99, 200);", traffic_cop, counter_, result);
  ExecuteSQLQuery("INSERT INTO test1 VALUES (1001, 2202);", traffic_cop, counter_, result);
  ExecuteSQLQuery("INSERT INTO test2 VALUES (2202, 3303, 4404);", traffic_cop, counter_, result);
  //ExecuteSQLQuery("INSERT INTO test2 VALUES (3303);", traffic_cop, counter_, result);
  //ExecuteSQLQuery("INSERT INTO test2 VALUES (4404);", traffic_cop, counter_, result);
  ExecuteSQLQuery("SELECT * FROM (SELECT test1.b FROM test1 JOIN test ON test1.c = test.a WHERE test1.b > 100) AS X JOIN test2 ON test2.b=X.b;", traffic_cop, counter_, result);
  
  //std::cout << "Statement executed!! Result: " << peloton::ResultTypeToString(status).c_str() << std::endl;
  std::cout << "Result size: " << result.size() /*<< " First row " << GetResultValueAsString(result, 0) << " Second row " << GetResultValueAsString(result, 1)*/ << std::endl;
  for (int i = 0; i < result.size(); i++) {
    std::cout << "Row " << i << ": Value: " << GetResultValueAsString(result, i) << std::endl;
  }
  
  /*peloton::catalog::Catalog::GetInstance();

  auto &txn_manager = peloton::concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  // Insert a table first
  auto id_column = peloton::catalog::Column(
      peloton::type::TypeId::INTEGER, peloton::type::Type::GetTypeSize(peloton::type::TypeId::INTEGER),
      "dept_id", true);
  auto name_column =
      peloton::catalog::Column(peloton::type::TypeId::VARCHAR, 32, "dept_name", false);

  std::unique_ptr<peloton::catalog::Schema> table_schema(
      new peloton::catalog::Schema({id_column, name_column}));

  peloton::catalog::Catalog::GetInstance()->CreateDatabase(txn, DEFAULT_DB_NAME);
  txn_manager.CommitTransaction(txn);

  txn = txn_manager.BeginTransaction();
  peloton::catalog::Catalog::GetInstance()->CreateTable(txn,
                                               DEFAULT_DB_NAME,
                                               DEFAULT_SCHEMA_NAME,
                                               std::move(table_schema),
                                               "TEST_TABLE",
                                               false);

  auto table = peloton::catalog::Catalog::GetInstance()->GetTableWithName(txn,
                                                                 DEFAULT_DB_NAME,
                                                                 DEFAULT_SCHEMA_NAME,
                                                                 "TEST_TABLE");
  txn_manager.CommitTransaction(txn);

  txn = txn_manager.BeginTransaction();

  std::unique_ptr<peloton::executor::ExecutorContext> context(
      new peloton::executor::ExecutorContext(txn));

  std::unique_ptr<peloton::parser::InsertStatement> insert_node(
      new peloton::parser::InsertStatement(peloton::InsertType::VALUES));

  std::string name = "TEST_TABLE";
  auto table_ref = new peloton::parser::TableRef(peloton::TableReferenceType::NAME);
  peloton::parser::TableInfo *table_info = new peloton::parser::TableInfo();
  table_info->table_name = name;

  std::string col_1 = "dept_id";
  std::string col_2 = "dept_name";

  table_ref->table_info_.reset(table_info);
  insert_node->table_ref_.reset(table_ref);

  insert_node->columns.push_back(col_1);
  insert_node->columns.push_back(col_2);

  insert_node->insert_values.push_back(
      std::vector<std::unique_ptr<peloton::expression::AbstractExpression>>());
  auto &values_ptr = insert_node->insert_values[0];

  values_ptr.push_back(std::unique_ptr<peloton::expression::AbstractExpression>(
      new peloton::expression::ConstantValueExpression(
          peloton::type::ValueFactory::GetIntegerValue(70))));

  values_ptr.push_back(std::unique_ptr<peloton::expression::AbstractExpression>(
      new peloton::expression::ConstantValueExpression(
          peloton::type::ValueFactory::GetVarcharValue("Hello"))));

  insert_node->select.reset(new peloton::parser::SelectStatement());

  peloton::planner::InsertPlan node(table, &insert_node->columns,
                           &insert_node->insert_values);
  peloton::executor::InsertExecutor executor(&node, context.get());

  executor.Init();
  executor.Execute();

  std::cout << "number of tuples is " << table->GetTupleCount() << std::endl;*/

    /*auto &txn_manager = peloton::concurrency::TransactionManagerFactory::GetInstance();
    std::cout << "1" << std::endl;
    auto txn = txn_manager.BeginTransaction();
    std::cout << "2" << std::endl;
    peloton::catalog::Catalog::GetInstance()->CreateDatabase(txn, DEFAULT_DB_NAME);
    std::cout << "3" << std::endl;

  // Insert a table first
  auto id_column = peloton::catalog::Column(
      peloton::type::TypeId::INTEGER, peloton::type::Type::GetTypeSize(peloton::type::TypeId::INTEGER),
      "dept_id", true);
    std::cout << "4" << std::endl;
  auto name_column =
      peloton::catalog::Column(peloton::type::TypeId::VARCHAR, 32, "dept_name", false);
  std::cout << "5" << std::endl;    

  // Schema
  std::unique_ptr<peloton::catalog::Schema> table_schema(
      new peloton::catalog::Schema({id_column, name_column}));
  std::cout << "6" << std::endl;    

  std::unique_ptr<peloton::executor::ExecutorContext> context(
      new peloton::executor::ExecutorContext(txn));
  std::cout << "7" << std::endl;    

  // Create plans
  peloton::planner::CreatePlan node("department_table", DEFAULT_SCHEMA_NAME,
                           DEFAULT_DB_NAME, std::move(table_schema),
                           peloton::CreateType::TABLE);
  std::cout << "8" << std::endl;                         

  // Create executer
  peloton::executor::CreateExecutor executor(&node, context.get());
  std::cout << "9" << std::endl;

  executor.Init();
  std::cout << "10" << std::endl;
  executor.Execute();
  std::cout << "Executed create table" << std::endl;
    
    std::cout << "begin" << std::endl;
    auto parser = peloton::parser::PostgresParser::GetInstance();
    std::cout << "after first" << std::endl;
    std::string query = "CREATE TABLE test(a INT PRIMARY KEY, b INT, c INT);";
    std::cout << "after second" << std::endl;
    std::unique_ptr<peloton::parser::SQLStatementList> stmt_list(parser.BuildParseTree(query).release());
    std::cout << "after third" << std::endl;

    std::cout << "Testing 123" << std::endl;*/
    return 0;
}