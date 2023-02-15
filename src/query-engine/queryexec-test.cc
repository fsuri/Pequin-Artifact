#include <iostream>
//#include "common/harness.h"
/*#include "common/logger.h"
#include "common/macros.h"
#include "parser/drop_statement.h"
#include "parser/postgresparser.h"*/

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

int main(int argc, char *argv[]) {
    auto &txn_manager = peloton::concurrency::TransactionManagerFactory::GetInstance();
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
    
    /*std::cout << "begin" << std::endl;
    auto parser = peloton::parser::PostgresParser::GetInstance();
    std::cout << "after first" << std::endl;
    std::string query = "CREATE TABLE test(a INT PRIMARY KEY, b INT, c INT);";
    std::cout << "after second" << std::endl;
    std::unique_ptr<peloton::parser::SQLStatementList> stmt_list(parser.BuildParseTree(query).release());
    std::cout << "after third" << std::endl;

    std::cout << "Testing 123" << std::endl;*/
    return 0;
}