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


using namespace pequinstore;

void test_registry(){
  std::cerr << std::endl << "Test Registry" << std::endl;
  SQLTransformer sql_interpreter;
  std::string table_registry = "table_load_test-client.json";
  sql_interpreter.RegisterTables(table_registry);
}

void test_insert(){

  std::cerr << std::endl << "Test Insert" << std::endl;
  proto::Transaction txn;
  SQLTransformer sql_interpreter;
  sql_interpreter.NewTx(&txn);



  
  std::string write_statement = "INSERT INTO user(col1, col2, col3) VALUES (val1, val2, val3);";  
  std::vector<std::vector<uint32_t>> primary_key_encoding_support = {{0, 2}};
  std::string read_statement;
  std::function<void(int, query_result::QueryResult*)>  write_continuation = [](int status, const query_result::QueryResult* res){
    std::cerr << "Issued write_continuation" << std::endl;
  };
  std::function<void(int, const query_result::QueryResult*)> wcb = [](int status, const query_result::QueryResult* res){
    std::cerr << "Completed WCB" << std::endl;
  };

  std::cerr << write_statement << std::endl;

  sql_interpreter.TransformWriteStatement(write_statement, primary_key_encoding_support, read_statement, write_continuation, wcb);


  query_result::QueryResult *res = new sql::QueryResultProtoWrapper("");
  
  std::cerr << "Read Statement: " << read_statement << std::endl;

  write_continuation(0, res);
  ///TODO: check 
  std::cerr << "Write Set: "  << std::endl;
  for(auto write: txn.write_set()){
      std::cerr << "Key: " << write.key() << std::endl;
      for(auto [col, val]: write.rowupdates().attribute_writes()){
           std::cerr << "Col: " << col << ". Val: " << val << std::endl;
      }
  }

  std::cerr << std::endl;
} 

void test_update(){
  std::cerr << std::endl << "Test Update" << std::endl;
  proto::Transaction txn;
  SQLTransformer sql_interpreter;
  sql_interpreter.NewTx(&txn);



  
  std::string write_statement = "UPDATE user SET col1 = col1 + 1, col2 = monkey WHERE col2 = apple AND col3 = giraffe;";
  std::vector<std::vector<uint32_t>> primary_key_encoding_support = {{0}};
  std::string read_statement;
  std::function<void(int, query_result::QueryResult*)>  write_continuation = [](int status, const query_result::QueryResult* res){
    std::cerr << "Issued write_continuation" << std::endl;
  };
  std::function<void(int, const query_result::QueryResult*)> wcb = [](int status, const query_result::QueryResult* res){
    std::cerr << "Completed WCB" << std::endl;
  };

  std::cerr << write_statement << std::endl;

  sql_interpreter.TransformWriteStatement(write_statement, primary_key_encoding_support, read_statement, write_continuation, wcb);


  std::vector<std::string> result_row;
  result_row.push_back("5");
  result_row.push_back("giraffe");
  result_row.push_back("apple");
  sql::QueryResultProtoBuilder queryResultBuilder;
  queryResultBuilder.add_column("col1");
  queryResultBuilder.add_column("col2");
  queryResultBuilder.add_column("col3");
  queryResultBuilder.add_row(result_row.begin(), result_row.end());

  std::string result = queryResultBuilder.get_result()->SerializeAsString();
  
  query_result::QueryResult *res = new sql::QueryResultProtoWrapper(result);
  
  std::cerr << "Read Statement: " << read_statement << std::endl;

  write_continuation(0, res);
  ///TODO: check 
  std::cerr << "Write Set: "  << std::endl;
  for(auto write: txn.write_set()){
      std::cerr << "Key: " << write.key() << " Value: " << write.value() << std::endl;
      for(auto [col, val]: write.rowupdates().attribute_writes()){
           std::cerr << "Col: " << col << ". Val: " << val << std::endl;
      }
  }

  std::cerr << std::endl;
}

int main() {
  
  std::cerr<< "Testing Write Parser" << std::endl;

  test_registry();

  test_insert();

  test_update();

 
  return 0;
}

