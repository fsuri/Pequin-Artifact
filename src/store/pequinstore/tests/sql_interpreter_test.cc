/***********************************************************************
 *
 * Copyright 2021 Florian Suri-Payer <fsp@cs.cornell.edu>
 *                Matthew Burke <matthelb@cs.cornell.edu>
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/

#include <iostream>
#include <fstream>
#include <vector>
#include <string>

#include "store/pequinstore/common.h"
#include "store/pequinstore/sql_interpreter.h"
#include "store/benchmark/async/json_table_writer.h"


using namespace pequinstore;

static std::string file_name = "sql_interpreter_test_registry";


static uint64_t config_f = 0;
static QueryParameters query_params(true,
                                  2*config_f + 1, //syncQuorumSize,
                                  2*config_f + 1,    //  queryMessages,
                                  1*config_f + 1,    //  mergeThreshold,
                                  1*config_f + 1,    //  syncMessages,
                                  1*config_f + 1,    //  resultQuorum,
                                  1, 
                                  false, // FLAGS_pequin_query_eager_exec,
                                  false, 
                                  false,
                                  true,    //  FLAGS_pequin_query_read_prepared,
                                  false,    //  FLAGS_pequin_query_cache_read_set,
                                  true,    //  FLAGS_pequin_query_optimistic_txid,
                                  compress,    //  FLAGS_pequin_query_compress_optimistic_txid, 
                                  false,    //  FLAGS_pequin_query_merge_active_at_client,
                                  false,    //  FLAGS_pequin_sign_client_queries,
                                  false, // FLAGS_pequin_sign_replica_to_replica
                                   false,    //  FLAGS_pequin_parallel_queries
                                  false,    //  FLAGS_pequin_use_semantic_cc
                                   false,    // FLAGS_pequin_use_active_read_set
                                  0UL,       // FLAGS_pequin_monotonicity_grace
                                  0UL        // FLAGS_pequin_non_monotonicity_grace
                                  );

static DefaultSQLPartitioner dummy_part;

void test_registry(){
  std::cerr << std::endl << "Test Registry" << std::endl;

  //Create desired registry via table writer.

  std::string table_name = "table";
  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  std::vector<uint32_t> primary_key_col_idx;

  TableWriter table_writer(file_name);

  //Table1:
  table_name = "user";
  column_names_and_types.push_back(std::make_pair("col1", "INT"));
  column_names_and_types.push_back(std::make_pair("col2", "TEXT"));
  column_names_and_types.push_back(std::make_pair("col3", "TEXT"));
  primary_key_col_idx.push_back(0);
  primary_key_col_idx.push_back(2);
  table_writer.add_table(table_name, column_names_and_types, primary_key_col_idx);
  //Write Tables to JSON
  table_writer.flush();

  SQLTransformer sql_interpreter(&query_params);
  std::string table_registry = file_name + "-tables-schema.json";
  sql_interpreter.RegisterTables(table_registry);
}

void test_insert(){

  std::cerr << std::endl << "Test Insert" << std::endl;
  
  SQLTransformer sql_interpreter(&query_params);
  std::string table_registry = file_name + "-tables-schema.json";
  sql_interpreter.RegisterTables(table_registry);
  sql_interpreter.RegisterPartitioner(&dummy_part, 1, 1, 0);
  proto::Transaction txn;
  sql_interpreter.NewTx(&txn);



  std::string write_statement = "INSERT INTO user(col1, col2, col3) VALUES (val1, 'val2', 'val3');";  
  std::string read_statement;
  std::function<void(int, query_result::QueryResult*)>  write_continuation = [](int status, const query_result::QueryResult* res){
    std::cerr << "Issued write_continuation" << std::endl;
  };
  std::function<void(int, query_result::QueryResult*)> wcb = [](int status, const query_result::QueryResult* res){
    std::cerr << "Completed WCB" << std::endl;
  };

  std::cerr << write_statement << std::endl;

  uint64_t dummy_target_table;
  bool skip_query_interpretation = false;
  sql_interpreter.TransformWriteStatement(write_statement, read_statement, write_continuation, wcb, dummy_target_table, skip_query_interpretation);


  query_result::QueryResult *res = new sql::QueryResultProtoWrapper("");
  
  std::cerr << "Read Statement: " << read_statement << std::endl;

  write_continuation(0, res);
  ///TODO: check 
  std::cerr << "Write Set: "  << std::endl;
  for(auto write: txn.write_set()){
      std::cerr << "Key: " << write.key() << std::endl;
      // for(auto [col, val]: write.rowupdates().attribute_writes()){
      //      std::cerr << "Col: " << col << " -- Val: " << val << std::endl;
      // }
  }


  for(auto &[table, table_write]: txn.table_writes()){
    ColRegistry *col_registry = sql_interpreter.GetColRegistry(table);
    std::cerr << "Write to Table: " << table << std::endl;
    for(auto &row: table_write.rows()){
      std::cerr << " Write row: " << std::endl;
      // for(auto &[col_name, idx]: col_registry->col_name_index){
      //      std::cerr << "  Col: " << col_name << " -- Val: " << (row.column_values()[idx]) << "; ";
      // }
      for(int i=0; i<row.column_values().size(); ++i){
           std::cerr << "  Col: " << (col_registry->col_names[i]) << " -- Val: " << (row.column_values()[i]) << "; ";
      }
       std::cerr << "is deletion: " << row.deletion() << std::endl;
    }
  }

  std::cerr << std::endl;
} 

void test_update(){
  std::cerr << std::endl << "Test Update" << std::endl;

  SQLTransformer sql_interpreter(&query_params);
  std::string table_registry = file_name + "-tables-schema.json";
  sql_interpreter.RegisterTables(table_registry);
  sql_interpreter.RegisterPartitioner(&dummy_part, 1, 1, 0);
  proto::Transaction txn;
  sql_interpreter.NewTx(&txn);

  std::string write_statement = "UPDATE user SET col1 = col1 + 1, col2 = 'monkey', col3 = 'pear' WHERE col2 = 'giraffe' AND col3 = 'apple';";
  std::string read_statement;
  std::function<void(int, query_result::QueryResult*)>  write_continuation = [](int status, const query_result::QueryResult* res){
    std::cerr << "Issued write_continuation" << std::endl;
  };
  std::function<void(int, query_result::QueryResult*)> wcb = [](int status, const query_result::QueryResult* res){
    std::cerr << "Completed WCB" << std::endl;
  };

  std::cerr << write_statement << std::endl;

  uint64_t dummy_target_table;
  bool skip_query_interpretation = false;
  sql_interpreter.TransformWriteStatement(write_statement, read_statement, write_continuation, wcb, dummy_target_table, skip_query_interpretation);


  // std::vector<std::string> result_row;
  // result_row.push_back("5");
  // result_row.push_back("giraffe");
  // result_row.push_back("apple");
  sql::QueryResultProtoBuilder queryResultBuilder;
  queryResultBuilder.add_column("col1");
  queryResultBuilder.add_column("col2");
  queryResultBuilder.add_column("col3");
  //queryResultBuilder.add_row(result_row.begin(), result_row.end());  //FIXME: This encodes only 

  RowProto *row = queryResultBuilder.new_row();
  int32_t v1 = 5;
  std::string v2("giraffe");
  std::string v3("apple");
  queryResultBuilder.AddToRow(row, v1);
  queryResultBuilder.AddToRow(row, v2);
  queryResultBuilder.AddToRow(row, v3);

  // std::vector<void*> res_row;
  // res_row.push_back(&v1);
  // res_row.push_back(&v2);
  // res_row.push_back(&v3);
  // queryResultBuilder.add_row(res_row.begin(), res_row.end());


  std::string result = queryResultBuilder.get_result()->SerializeAsString();
  
  query_result::QueryResult *res = new sql::QueryResultProtoWrapper(result);
  
  std::cerr << "Read Statement: " << read_statement << std::endl;

  write_continuation(0, res);
  ///TODO: check 
  std::cerr << "Write Set: "  << std::endl;
  for(auto write: txn.write_set()){
      std::cerr << "Key: " << write.key() << " Value: " << write.value() << std::endl;
      for(auto [col, val]: write.rowupdates().attribute_writes()){
           std::cerr << "Col: " << col << " -- Val: " << val << std::endl;
      }
  }

   for(auto &[table, table_write]: txn.table_writes()){
    ColRegistry *col_registry = sql_interpreter.GetColRegistry(table);
    std::cerr << "Write to Table: " << table << std::endl;
    for(auto &row: table_write.rows()){
      std::cerr << " Write row: " << std::endl;
      // for(auto &[col_name, idx]: col_registry->col_name_index){
      //      std::cerr << "  Col: " << col_name << " -- Val: " << (row.column_values()[idx]) << "; ";
      // }
      for(int i=0; i<row.column_values().size(); ++i){
           std::cerr << "  Col: " << (col_registry->col_names[i]) << " -- Val: " << (row.column_values()[i]) << "; ";
      }
       std::cerr << "is deletion: " << row.deletion() << std::endl;
    }
  }

  std::cerr << std::endl;
}

void test_delete(){
  std::cerr << std::endl << "Test Delete" << std::endl;
  SQLTransformer sql_interpreter(&query_params);
  std::string table_registry = file_name + "-tables-schema.json";
  sql_interpreter.RegisterTables(table_registry);
  sql_interpreter.RegisterPartitioner(&dummy_part, 1, 1, 0);
  proto::Transaction txn;
  sql_interpreter.NewTx(&txn);

  uint64_t dummy_target_table;

  //DELETE FROM <table_name> WHERE <condition>
  std::string write_statement = "DELETE FROM user WHERE col2 = 'giraffe' AND col3 = 'apple';";
 
  std::string read_statement;
  std::function<void(int, query_result::QueryResult*)>  write_continuation = [](int status, const query_result::QueryResult* res){
    std::cerr << "Issued write_continuation" << std::endl;
  };
  std::function<void(int, query_result::QueryResult*)> wcb = [](int status, const query_result::QueryResult* res){
    std::cerr << "Completed WCB" << std::endl;
  };

  std::cerr << write_statement << std::endl;

  bool skip_query_interpretation = false;
  sql_interpreter.TransformWriteStatement(write_statement, read_statement, write_continuation, wcb, dummy_target_table, skip_query_interpretation);


  // std::vector<std::string> result_row;
  // result_row.push_back("5");
  // result_row.push_back("giraffe");
  // result_row.push_back("apple");
  sql::QueryResultProtoBuilder queryResultBuilder;
  queryResultBuilder.add_column("col1");
  //queryResultBuilder.add_column("col2"); //Only add primary columns.
  queryResultBuilder.add_column("col3");
  //queryResultBuilder.add_row(result_row.begin(), result_row.end());

  RowProto *row = queryResultBuilder.new_row();
  int32_t v1 = 5;
  std::string v2("giraffe");
  std::string v3("apple");
  queryResultBuilder.AddToRow(row, v1);
  //queryResultBuilder.AddToRow(row, v2);
  queryResultBuilder.AddToRow(row, v3);

  std::string result = queryResultBuilder.get_result()->SerializeAsString();
  
  query_result::QueryResult *res = new sql::QueryResultProtoWrapper(result);
  
  std::cerr << "Read Statement: " << read_statement << std::endl;

  write_continuation(0, res);
 
  std::cerr << "Write Set: "  << std::endl;
  for(auto write: txn.write_set()){
      std::cerr << "Key: " << write.key() << " Value: " << write.value() << std::endl;
      std::cerr << "Is Deletion: " << write.rowupdates().deletion() << std::endl;
  }

   for(auto &[table, table_write]: txn.table_writes()){
    ColRegistry *col_registry = sql_interpreter.GetColRegistry(table);
    std::cerr << "Write to Table: " << table << std::endl;
    for(auto &row: table_write.rows()){
      std::cerr << " Write row: " << std::endl;
      // for(auto &[col_name, idx]: col_registry->col_name_index){
      //      std::cerr << "  Col: " << col_name << " -- Val: " << (row.column_values()[idx]) << "; ";
      // }
      for(int i=0; i<row.column_values().size(); ++i){
           std::cerr << "  Col: " << (col_registry->col_names[i]) << " -- Val: " << (row.column_values()[i]) << "; ";
      }
       std::cerr << "is deletion: " << row.deletion() << std::endl;
    }
  }

  ////////////////////// Test point delete
  std::cerr << "\n Test point delete: " << std::endl;

  txn.Clear();
  sql_interpreter.NewTx(&txn);

  write_statement = "DELETE FROM user WHERE col1 = 5 AND col3 = 'apple';";
  read_statement = "";
 
  std::cerr << write_statement << std::endl;

  sql_interpreter.TransformWriteStatement(write_statement, read_statement, write_continuation, wcb, dummy_target_table, skip_query_interpretation);

  std::cerr << "Read Statement: " << read_statement << std::endl;

  write_continuation(0, res);

  std::cerr << "Write Set: "  << std::endl;
  for(auto write: txn.write_set()){
      std::cerr << "Key: " << write.key() << " Value: " << write.value() << std::endl;
      std::cerr << "Is Deletion: " << write.rowupdates().deletion() << std::endl;
  }

   for(auto &[table, table_write]: txn.table_writes()){
    ColRegistry *col_registry = sql_interpreter.GetColRegistry(table);
    std::cerr << "Write to Table: " << table << std::endl;
    for(auto &row: table_write.rows()){
      std::cerr << " Write row: " << std::endl;
      // for(auto &[col_name, idx]: col_registry->col_name_index){
      //      std::cerr << "  Col: " << col_name << " -- Val: " << (row.column_values()[idx]) << "; ";
      // }
      for(int i=0; i<row.column_values().size(); ++i){
           std::cerr << "  Col: " << (col_registry->col_names[i]) << " -- Val: " << (row.column_values()[i]) << "; ";
      }
       std::cerr << "is deletion: " << row.deletion() << std::endl;
    }
  }

  std::cerr << std::endl;
}

void test_cond(){
  std::cerr << std::endl << "Test Cond" << std::endl;
  SQLTransformer sql_interpreter(&query_params);
  std::string table_registry = file_name + "-tables-schema.json";
  sql_interpreter.RegisterTables(table_registry);
  sql_interpreter.RegisterPartitioner(&dummy_part, 1, 1, 0);
  proto::Transaction txn;
  sql_interpreter.NewTx(&txn);


  //DELETE FROM <table_name> WHERE <condition>
  //Test 1:
  std::string query_statement = "SELECT * FROM user WHERE col2 = 'apple' AND col3 = 'giraffe';";
  
  std::cerr << "Test1: " << query_statement << std::endl;

  std::string table_name;
  std::vector<std::string> p_col_value;
  bool skip_interpretation = false; //In Query pass this arg; if true, don't interpret and just treat as range.
  bool is_point = sql_interpreter.InterpretQueryRange(query_statement, table_name, p_col_value);

  std::cerr << is_point << std::endl;
  UW_ASSERT(is_point == false);
  
  //Test 2
  query_statement = "SELECT * FROM user WHERE col1 = 5 AND col3 = 'giraffe';";
  std::cerr << "Test2: " << query_statement << std::endl;
  p_col_value.clear();
  is_point = sql_interpreter.InterpretQueryRange(query_statement, table_name, p_col_value);

  std::cerr << is_point << std::endl;
  UW_ASSERT(is_point == true);
 
  //Test 3
  query_statement = "SELECT * FROM user WHERE col1 = 5;";
  
  std::cerr << "Test3: " << query_statement << std::endl;
  p_col_value.clear();
  is_point = sql_interpreter.InterpretQueryRange(query_statement, table_name, p_col_value);

  std::cerr << is_point << std::endl;
  for(auto &val: p_col_value){
    std::cerr << "primary col with value: " << val << std::endl;
  }
  std::cerr << is_point << std::endl;
  UW_ASSERT(is_point == false);

  //Test 4
  query_statement = "SELECT * FROM user WHERE col2 = 'apple' OR col1 = 5 AND col3 = 'giraffe';";
  
  std::cerr << "Test4: " << query_statement << std::endl;
  p_col_value.clear();
  is_point = sql_interpreter.InterpretQueryRange(query_statement, table_name, p_col_value);

  std::cerr << is_point << std::endl;
  UW_ASSERT(is_point == false);

  //Test 5
  query_statement = "SELECT * FROM user WHERE (col2 = 'apple' OR col1 = 5) AND (col3 = 'giraffe' AND col1 = 5);";
  
  std::cerr << "Test5: " << query_statement << std::endl;
  p_col_value.clear();
  is_point = sql_interpreter.InterpretQueryRange(query_statement, table_name, p_col_value);

  std::cerr << is_point << std::endl << std::endl;
  UW_ASSERT(is_point == true);

  //Test 6
  query_statement = "SELECT * FROM user WHERE (col2 = 'apple' OR col1 = 5) AND (col3 = 'giraffe' AND col1 = 5) AND col2 = 'apple';";
  
  std::cerr << "Test6: " << query_statement << std::endl;
  p_col_value.clear();
  is_point = sql_interpreter.InterpretQueryRange(query_statement, table_name, p_col_value);

  std::cerr << is_point << std::endl << std::endl;
  UW_ASSERT(is_point == false);


  //Test 7
  query_statement = "SELECT * FROM user WHERE col2 = 'apple' AND (col2 = 'apple' OR col1 = 5) AND (col3 = 'giraffe' AND col1 = 5);";
  
  std::cerr << "Test7: " << query_statement << std::endl;
  p_col_value.clear();
  is_point = sql_interpreter.InterpretQueryRange(query_statement, table_name, p_col_value);

  std::cerr << is_point << std::endl << std::endl;
  UW_ASSERT(is_point == false);

   //Test 8
  query_statement = "SELECT * FROM user WHERE col1 = 5 AND (col3 = 'giraffe');";
  
  std::cerr << "Test8: " << query_statement << std::endl;
  p_col_value.clear();
  is_point = sql_interpreter.InterpretQueryRange(query_statement, table_name, p_col_value);

  std::cerr << is_point << std::endl;
  UW_ASSERT(is_point == true);

  std::cerr << std::endl;


  //Test 9: Relaxed

  query_statement = "SELECT * FROM user WHERE col1 = 5 AND col2 = 'apple' AND col3 = 'giraffe';";
  
  std::cerr << "Test9(relaxed): " << query_statement << std::endl;
  p_col_value.clear();
  is_point = sql_interpreter.InterpretQueryRange(query_statement, table_name, p_col_value, true);

  std::cerr << is_point << std::endl;
  UW_ASSERT(is_point == true);

  std::cerr << std::endl;
  
  query_statement = "SELECT * FROM user WHERE (col2 = 'apple' OR col1 = 5) AND (col3 = 'giraffe' AND col1 = 5) AND col2 = 'apple';";
  std::cerr << "Test10(relaxed): " << query_statement << std::endl;
  p_col_value.clear();
  is_point = sql_interpreter.InterpretQueryRange(query_statement, table_name, p_col_value, true);

  std::cerr << is_point << std::endl << std::endl;
  UW_ASSERT(is_point == true);

  query_statement = "SELECT * FROM user WHERE col2 = 'apple' AND (col2 = 'apple' OR col1 = 5) AND (col3 = 'giraffe' AND col1 = 5);";
  
  std::cerr << "Test11(relaxed): " << query_statement << std::endl;
  p_col_value.clear();
  is_point = sql_interpreter.InterpretQueryRange(query_statement, table_name, p_col_value, true);

  std::cerr << is_point << std::endl << std::endl;
  UW_ASSERT(is_point == true);
}

void test_write(){

   std::cerr << "Test Write Generation:" << std::endl;

   SQLTransformer sql_interpreter(&query_params);
  std::string table_registry = file_name + "-tables-schema.json";
  sql_interpreter.RegisterTables(table_registry);
  sql_interpreter.RegisterPartitioner(&dummy_part, 1, 1, 0);
  proto::Transaction txn;
  sql_interpreter.NewTx(&txn);

  std::string table_name = "user";
  std::string write_statement;
  std::string delete_statement;
  TableWrite table_write;

  std::vector<std::string> col_names = {"age", "name", "color"};
  std::vector<uint32_t> p_col_idx = {0, 2};
  *table_write.mutable_column_names() = {col_names.begin(), col_names.end()};
  *table_write.mutable_col_primary_idx() = {p_col_idx.begin(), p_col_idx.end()};

  std::vector<std::string> val1 = {"26", "flo", "brown"};
  std::vector<std::string> val2 = {"24", "neil", "black"};
  RowUpdates *row1 = table_write.add_rows();
  *row1->mutable_column_values() = {val1.begin(), val1.end()};
   RowUpdates *row2 = table_write.add_rows();
  *row2->mutable_column_values() = {val2.begin(), val2.end()};

  //deletions:
  std::vector<std::string> val3 = {"62", "lor", "brown"};
  std::vector<std::string> val4 = {"31", "nat", "blonde"};
   RowUpdates *row3 = table_write.add_rows();
  *row3->mutable_column_values() = {val3.begin(), val3.end()};
  row3->set_deletion(true);
   RowUpdates *row4 = table_write.add_rows();
  *row4->mutable_column_values() = {val4.begin(), val4.end()};
  row4->set_deletion(true);
 

  //
  sql_interpreter.GenerateTableWriteStatement(write_statement, delete_statement, table_name, table_write);
  std::cerr << "write: " << write_statement << std::endl;
   std::cerr << "delete: " << delete_statement << std::endl;

}

void test_predicates(){

   std::cerr << "Test Predicte eval:" << std::endl;

   SQLTransformer sql_interpreter(&query_params);
  std::string table_registry = file_name + "-tables-schema.json";
  sql_interpreter.RegisterTables(table_registry);
  sql_interpreter.RegisterPartitioner(&dummy_part, 1, 1, 0);
  proto::Transaction txn;
  sql_interpreter.NewTx(&txn);

  std::string table_name = "user";
  
  TableWrite table_write;

  std::vector<std::string> col_names = {"age", "name", "color"};
  std::vector<uint32_t> p_col_idx = {0, 2};
  *table_write.mutable_column_names() = {col_names.begin(), col_names.end()};
  *table_write.mutable_col_primary_idx() = {p_col_idx.begin(), p_col_idx.end()};

  std::vector<std::string> val1 = {"5", "flo", "brown"};
  std::vector<std::string> val2 = {"6", "neil", "black"};
  RowUpdates *row1 = table_write.add_rows();
  *row1->mutable_column_values() = {val1.begin(), val1.end()};
   RowUpdates *row2 = table_write.add_rows();
  *row2->mutable_column_values() = {val2.begin(), val2.end()};

  //
  std::string pred = "col1 = 5";
  std::cerr << std::endl << "TESTING PRED: " << pred << std::endl;
  
   std::cerr << std::endl << "row1: " << std::endl;
  bool r1 = sql_interpreter.EvalPred(pred, table_name, *row1);
  UW_ASSERT(r1);
    std::cerr << std::endl << "row2: " << std::endl;
  bool r2 = sql_interpreter.EvalPred(pred, table_name, *row2);
  UW_ASSERT(!r2);

  pred = "col1 >= 5 AND col2 = 'neil'";
  std::cerr << std::endl << "TESTING PRED: " << pred << std::endl;

    std::cerr << std::endl << "row1: " << std::endl;
  r1 = sql_interpreter.EvalPred(pred, table_name, *row1);
  UW_ASSERT(!r1);

    std::cerr << std::endl << "row2: " << std::endl;
  r2 = sql_interpreter.EvalPred(pred, table_name, *row2);
  UW_ASSERT(r2);

  pred = "col1 >= 5 AND (col2 = 'neil' OR col3 = 'brown')";
  std::cerr << std::endl << "TESTING PRED: " << pred << std::endl;
    std::cerr << std::endl << "row1: " << std::endl;
  r1 = sql_interpreter.EvalPred(pred, table_name, *row1);
  UW_ASSERT(r1);
    std::cerr << std::endl << "row2: " << std::endl;
  r2 = sql_interpreter.EvalPred(pred, table_name, *row2);
  UW_ASSERT(r2);


   pred = "col1 >= 5 AND col2 = 'neil' AND col3 = 'brown'";
  std::cerr << std::endl << "TESTING PRED: " << pred << std::endl;
    std::cerr << std::endl << "row1: " << std::endl;
  r1 = sql_interpreter.EvalPred(pred, table_name, *row1);
  UW_ASSERT(!r1);
 
}

int main() {
  
  std::cerr<< "Testing Write Parser (DEPRECATED TESTS -- PROBABLY WON'T WORK FOR ALL OLD TESTS)" << std::endl;

  test_registry();
  test_predicates();
  return 0;

  test_insert();

  test_update();

  test_delete();

  test_cond();

  test_write();

  std::cerr << "test string view scope 1" << std::endl;

  std::vector<std::string_view> vec;
 

  std::string t("test");
  {
    std::string_view s(t);
    s.remove_prefix(2);
    vec.push_back(s); //this creates a copy
  }
  std::cerr << (vec[0]) << std::endl;

   std::function<void()> f;
  
     std::string_view test {"hello"};
    std::cerr << test << std::endl;

    f = [test]() mutable {
      test.remove_prefix(1);
      std::cerr << test << std::endl;
    };
   

  f();
  std::cerr << test << std::endl;

  std::map<std::string_view, int> q;
  q[t] = 1;

  // std::map<std::string, int> r;
  // std::string_view s(t);
  // r[s] = 1;
  //Can look up string_view with string, but not in reverse --> string is superset of string view

  // ::google::protobuf::Message test_proto_m = proto::PointQueryResultReply;
  // proto::PointQueryResultReply test_proto = dynamic_cast<proto::PointQueryResultReply&>(test_proto_m);
  // test_proto.mutable_write()->set_prepared_txn_digest("hello");
  // proto::SignedMessage *sm = test_proto.mutable_signed_write();

  // std::cerr << "has write? " << (test_proto.has_write()) << std::endl;
  // std::cerr << "has signed write? " << (test_proto.has_signed_write()) << std::endl;
  // std::cerr << "write " << (test_proto.write().prepared_txn_digest()) << std::endl;

  return 0;
}

