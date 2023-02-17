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

int main(int argc, char *argv[]) {
  peloton::catalog::Catalog::GetInstance();

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

  std::cout << "number of tuples is " << table->GetTupleCount() << std::endl;

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