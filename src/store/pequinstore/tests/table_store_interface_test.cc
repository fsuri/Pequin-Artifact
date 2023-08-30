#include <cstddef>
#include <iostream>
#include <ostream>
#include <string>
// #include "common/harness.h"
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

#include "lib/assert.h"
#include "store/common/timestamp.h"
#include "store/pequinstore/common.h"

#include "store/common/query_result/query_result.h"
#include "store/common/query_result/query_result_proto_builder.h"
#include "store/common/query_result/query_result_proto_wrapper.h"
#include "store/pequinstore/table_store_interface.h"
#include "store/pequinstore/table_store_interface_peloton.h"

#include "store/benchmark/async/json_table_writer.h"
#include "store/pequinstore/sql_interpreter.h"

using namespace pequinstore;

void test_read_query() {

  auto &txn_manager = peloton::concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  peloton::catalog::Catalog::GetInstance()->CreateDatabase(txn, DEFAULT_DB_NAME);
  txn_manager.CommitTransaction(txn);

  Timestamp pesto_timestamp(4, 6);
  pequinstore::proto::ReadSet read_set_one;
  pequinstore::QueryReadSetMgr query_read_set_mgr_one(&read_set_one, 1, false);

  Timestamp toy_ts_c(10, 12);
  pequinstore::proto::CommittedProof *real_proof = new pequinstore::proto::CommittedProof();
  real_proof->mutable_txn()->set_client_id(10);
  real_proof->mutable_txn()->set_client_seq_num(12);
  toy_ts_c.serialize(real_proof->mutable_txn()->mutable_timestamp());
  TableWrite &table_write = (*real_proof->mutable_txn()->mutable_table_writes())["test"];

  RowUpdates *row1 = table_write.add_rows();
  row1->add_column_values("42");
  row1->add_column_values("54");

  pequinstore::proto::CommittedProof *real_proof2 = new pequinstore::proto::CommittedProof();
  real_proof2->mutable_txn()->set_client_id(10);
  real_proof2->mutable_txn()->set_client_seq_num(12);

  TableWrite &table_write2 = (*real_proof2->mutable_txn()->mutable_table_writes())["test"];

  RowUpdates *row2 = table_write2.add_rows();
  row2->add_column_values("24");
  row2->add_column_values("225");

  pequinstore::proto::CommittedProof *real_proof3 = new pequinstore::proto::CommittedProof();
  real_proof3->mutable_txn()->set_client_id(10);
  real_proof3->mutable_txn()->set_client_seq_num(12);

  TableWrite &table_write3 = (*real_proof3->mutable_txn()->mutable_table_writes())["test"];

  RowUpdates *row3 = table_write3.add_rows();
  row3->add_column_values("34");
  row3->add_column_values("315");

  pequinstore::proto::CommittedProof *real_proof4 = new pequinstore::proto::CommittedProof();
  real_proof4->mutable_txn()->set_client_id(10);
  real_proof4->mutable_txn()->set_client_seq_num(12);

  TableWrite &table_write4 = (*real_proof4->mutable_txn()->mutable_table_writes())["test"];

  RowUpdates *row4 = table_write4.add_rows();
  row4->add_column_values("24");
  row4->add_column_values("225");
  row4->set_deletion(true);

  static std::string file_name = "sql_interpreter_test_registry";
  // Create desired registry via table writer.
  std::string table_name = "test";
  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  std::vector<uint32_t> primary_key_col_idx;

  TableWriter table_writer(file_name);

  // Table1:
  table_name = "test";
  column_names_and_types.push_back(std::make_pair("a", "INT"));
  column_names_and_types.push_back(std::make_pair("b", "INT"));
  primary_key_col_idx.push_back(0);
  table_writer.add_table(table_name, column_names_and_types,
                         primary_key_col_idx);
  // Write Tables to JSON
  table_writer.flush();

  pequinstore::TableStore *table_store = new pequinstore::PelotonTableStore();
  pequinstore::proto::Write write;
  pequinstore::proto::CommittedProof committed_proof;
  std::string table_registry = file_name + "-tables-schema.json";
  table_store->RegisterTableSchema(table_registry);
  table_store->ExecRaw("CREATE TABLE test(a INT, b INT, PRIMARY KEY(a));");
  // table_store->ExecRaw("INSERT INTO test VALUES (42, 54);");
  // table_store->ExecRaw("INSERT INTO test VALUES (35, 26);");
  // table_store->ExecRaw("INSERT INTO test VALUES (190, 999);");
  table_store->ApplyTableWrite("test", table_write, toy_ts_c, "random",
                              real_proof, true);
  table_store->ApplyTableWrite("test", table_write2, toy_ts_c, "random",
                              real_proof2, true);
  table_store->ApplyTableWrite("test", table_write3, toy_ts_c, "random",
                              real_proof3, true);
  table_store->ApplyTableWrite("test", table_write4, toy_ts_c, "random",
                              real_proof4, true);

  std::cout << "New change 10" << std::endl;
  // table_store->ApplyTableWrite("test", table_write_1, toy_ts_c, "random",
  // real_proof, true);
  std::string enc_primary_key = "test//24";
  // table_store->ExecRaw("INSERT INTO test VALUES (24, 256)");
  // table_store->ExecRaw("INSERT INTO test VALUES (26, 870)");
  // table_store->ExecRaw("DELETE FROM test WHERE a=24;");
  std::cout << "End of queryexec test" << std::endl;
  // table_store->ExecPointRead("SELECT * FROM test WHERE a=34;",
  // enc_primary_key, toy_ts_c, &write, &committed_proof);

  // table_store->ExecReadQuery("SELECT * FROM test;", toy_ts_c,
  // query_read_set_mgr_one);
  table_store->PurgeTableWrite("test", table_write4, toy_ts_c, "random");
  table_store->ExecReadQuery("SELECT * FROM test;", toy_ts_c,
                            query_read_set_mgr_one);

  delete table_store;
}

void test_committed_table_write() {
  auto &txn_manager =
      peloton::concurrency::TransactionManagerFactory::GetInstance();

  auto txn = txn_manager.BeginTransaction();

  peloton::catalog::Catalog::GetInstance()->CreateDatabase(txn,
                                                           DEFAULT_DB_NAME);

  txn_manager.CommitTransaction(txn);

  Timestamp pesto_timestamp(4, 6);
  pequinstore::proto::ReadSet read_set_one;
  pequinstore::QueryReadSetMgr query_read_set_mgr_one(&read_set_one, 1, false);

  static std::string file_name =
      "store/pequinstore/tests/sql_interpreter_test_registry";
  // Create desired registry via table writer.
  std::string table_name = "test";
  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  std::vector<uint32_t> primary_key_col_idx;

  TableWriter table_writer(file_name);

  // Table1:
  table_name = "test";
  column_names_and_types.push_back(std::make_pair("a", "INT"));
  column_names_and_types.push_back(std::make_pair("b", "INT"));
  primary_key_col_idx.push_back(0);
  table_writer.add_table(table_name, column_names_and_types,
                         primary_key_col_idx);
  // Write Tables to JSON
  table_writer.flush();

  pequinstore::TableStore *table_store = new pequinstore::PelotonTableStore();
  pequinstore::proto::Write write;
  pequinstore::proto::CommittedProof committed_proof;
  std::string table_registry = file_name + "-tables-schema.json";
  std::cout << "Pre register" << std::endl;
  table_store->RegisterTableSchema(table_registry);
  std::cout << "Post register" << std::endl;
  table_store->ExecRaw("CREATE TABLE test(a INT, b INT, PRIMARY KEY(a));");

  Timestamp toy_ts_c(10, 12);
  Timestamp toy_ts_c_1(20, 20);
  size_t num_writes = 100;
  size_t num_overwrites = 100;

  for (size_t i = 0; i < num_writes; i++) {
    pequinstore::proto::CommittedProof *real_proof =
        new pequinstore::proto::CommittedProof();
    real_proof->mutable_txn()->set_client_id(toy_ts_c.getID());
    real_proof->mutable_txn()->set_client_seq_num(toy_ts_c_1.getTimestamp());
    toy_ts_c.serialize(real_proof->mutable_txn()->mutable_timestamp());
    TableWrite &table_write =
        (*real_proof->mutable_txn()->mutable_table_writes())["test"];

    RowUpdates *row1 = table_write.add_rows();
    row1->add_column_values(std::to_string(i));
    row1->add_column_values(std::to_string(i));

    table_store->ApplyTableWrite("test", table_write, toy_ts_c, "random",
                                real_proof, true);
  }

  for (size_t i = 0; i < num_overwrites; i++) {
    pequinstore::proto::CommittedProof *real_proof =
        new pequinstore::proto::CommittedProof();
    real_proof->mutable_txn()->set_client_id(toy_ts_c_1.getID());
    real_proof->mutable_txn()->set_client_seq_num(toy_ts_c_1.getTimestamp());
    toy_ts_c.serialize(real_proof->mutable_txn()->mutable_timestamp());
    TableWrite &table_write =
        (*real_proof->mutable_txn()->mutable_table_writes())["test"];

    RowUpdates *row1 = table_write.add_rows();
    row1->add_column_values(std::to_string(i));
    row1->add_column_values(std::to_string(i + 100));

    table_store->ApplyTableWrite("test", table_write, toy_ts_c_1, "random",
                                real_proof, true);
  }

  std::string result = table_store->ExecReadQuery(
      "SELECT * FROM test;", toy_ts_c_1, query_read_set_mgr_one);

  sql::QueryResultProtoBuilder queryResultBuilder;
  queryResultBuilder.add_column("a");
  queryResultBuilder.add_column("b");
  for (unsigned int i = 0; i < num_writes; i++) {
    RowProto *row = queryResultBuilder.new_row();

    for (unsigned int j = 0; j < 2; j++) {
      std::string val = "";
      if (j == 0) {
        val = std::to_string(i);
      } else {
        val = std::to_string(i + 100);
      }
      // std::string val = std::to_string(i);
      queryResultBuilder.AddToRow(row, val);
    }
  }
  std::string expected = queryResultBuilder.get_result()->SerializeAsString();
  UW_ASSERT_EQ(expected, result);

  delete table_store;
}

void test_read_predicate() {
  auto &txn_manager =
      peloton::concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  peloton::catalog::Catalog::GetInstance()->CreateDatabase(txn,
                                                           DEFAULT_DB_NAME);
  txn_manager.CommitTransaction(txn);

  Timestamp pesto_timestamp(4, 6);
  pequinstore::proto::ReadSet read_set_one;
  pequinstore::QueryReadSetMgr query_read_set_mgr_one(&read_set_one, 1, false);

  Timestamp toy_ts_c(10, 12);
  pequinstore::proto::CommittedProof *real_proof =
      new pequinstore::proto::CommittedProof();
  real_proof->mutable_txn()->set_client_id(toy_ts_c.getID());
  real_proof->mutable_txn()->set_client_seq_num(toy_ts_c.getTimestamp());
  toy_ts_c.serialize(real_proof->mutable_txn()->mutable_timestamp());
  TableWrite &table_write =
      (*real_proof->mutable_txn()->mutable_table_writes())["test"];

  RowUpdates *row1 = table_write.add_rows();
  row1->add_column_values("42");
  row1->add_column_values("54");

  pequinstore::proto::CommittedProof *real_proof2 =
      new pequinstore::proto::CommittedProof();
  real_proof2->mutable_txn()->set_client_id(toy_ts_c.getID());
  real_proof2->mutable_txn()->set_client_seq_num(toy_ts_c.getTimestamp());

  TableWrite &table_write2 =
      (*real_proof2->mutable_txn()->mutable_table_writes())["test"];

  RowUpdates *row2 = table_write2.add_rows();
  row2->add_column_values("24");
  row2->add_column_values("225");

  static std::string file_name =
      "store/pequinstore/tests/sql_interpreter_test_registry";
  // Create desired registry via table writer.
  std::string table_name = "test";
  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  std::vector<uint32_t> primary_key_col_idx;

  TableWriter table_writer(file_name);

  // Table1:
  table_name = "test";
  column_names_and_types.push_back(std::make_pair("a", "INT"));
  column_names_and_types.push_back(std::make_pair("b", "INT"));
  primary_key_col_idx.push_back(0);
  table_writer.add_table(table_name, column_names_and_types,
                         primary_key_col_idx);
  // Write Tables to JSON
  table_writer.flush();

  pequinstore::TableStore *table_store = new pequinstore::PelotonTableStore();
  pequinstore::proto::Write write;
  pequinstore::proto::CommittedProof committed_proof;
  std::string table_registry = file_name + "-tables-schema.json";
  table_store->RegisterTableSchema(table_registry);
  table_store->ExecRaw("CREATE TABLE test(a INT, b INT, PRIMARY KEY(a));");
  // table_store->ExecRaw("INSERT INTO test VALUES (42, 54);");
  // table_store->ExecRaw("INSERT INTO test VALUES (35, 26);");
  // table_store->ExecRaw("INSERT INTO test VALUES (190, 999);");
  table_store->ApplyTableWrite("test", table_write, toy_ts_c, "random",
                              real_proof, false);
  table_store->ApplyTableWrite("test", table_write2, toy_ts_c, "random",
                              real_proof2, true);

  // table_store->ApplyTableWrite("test", table_write_1, toy_ts_c, "random",
  // real_proof, true);
  std::string enc_primary_key = "test//24";
  // table_store->ExecRaw("INSERT INTO test VALUES (24, 256)");
  // table_store->ExecRaw("INSERT INTO test VALUES (26, 870)");
  // table_store->ExecRaw("DELETE FROM test WHERE a=24;");
  std::cout << "End of queryexec test" << std::endl;
  // table_store->ExecPointRead("SELECT * FROM test WHERE a=34;",
  // enc_primary_key, toy_ts_c, &write, &committed_proof);

  // table_store->ExecReadQuery("SELECT * FROM test;", toy_ts_c,
  // query_read_set_mgr_one);
  table_store->ExecReadQuery("SELECT * FROM test WHERE a=42;", toy_ts_c,
                            query_read_set_mgr_one);
}

int main() {
  test_read_query();
  test_committed_table_write();
  // test_read_predicate();
  return 0;
}
