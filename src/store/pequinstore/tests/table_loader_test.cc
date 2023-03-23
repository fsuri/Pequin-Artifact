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
#include <nlohmann/json.hpp>
using json = nlohmann::json;

#include "store/benchmark/async/json_table_writer.h"



int main() {
  
  std::cerr<< "Testing Table Writer and Loader" << std::endl;
  std::string file_name = "table_load_test.json";
 
  std::string table_name = "table";
  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  std::vector<uint32_t> primary_key_col_idx;
  std::vector<std::string> values;

  TableWriter table_writer;

  //Table1:
  table_name = "table1";
  column_names_and_types.push_back(std::make_pair("col1", "INT"));
  column_names_and_types.push_back(std::make_pair("col2", "VARCHAR"));
  values.push_back("10");
  values.push_back("val");
  primary_key_col_idx.push_back(0);

  table_writer.add_table(table_name, column_names_and_types, primary_key_col_idx);
  table_writer.add_row(table_name, values);

  //Table2:
  column_names_and_types.clear();
  values.clear();
  primary_key_col_idx.clear();

  table_name = "table2";
  column_names_and_types.push_back(std::make_pair("col1", "INT"));
  column_names_and_types.push_back(std::make_pair("col2", "INT"));
  column_names_and_types.push_back(std::make_pair("col3", "VARCHAR"));
  values.push_back("20");
  values.push_back("12");
  values.push_back("val2");
  primary_key_col_idx.push_back(0);
  primary_key_col_idx.push_back(2);

  table_writer.add_table(table_name, column_names_and_types, primary_key_col_idx);
  table_writer.add_row(table_name, values);

  //Write Tables to JSON
  table_writer.flush(file_name);

  std::cerr << "Loading Tables" << std::endl;

  //Load Tables:

  std::ifstream generated_tables(file_name);
  json tables_to_load = json::parse(generated_tables);
       
  //std::cerr << (tables_to_load["tables"].size()) << std::endl;
  std::cerr << tables_to_load << std::endl;

  //Load all tables. 
  for(auto &[table_name, table]: tables_to_load.items()){
    //std::string table_name = name; //["table_name"]; 
    const std::vector<std::pair<std::string, std::string>> column_names_and_types = table["column_names_and_types"];
    const std::vector<uint32_t> primary_key_col_idx = table["primary_key_col_idx"];
    std::cerr << table_name << std::endl;
    for(auto &[col, type]: column_names_and_types){
      std::cerr << col << " : " << type << std::endl;
    }
   
    std::cerr << "(";
    for(auto &pidx: primary_key_col_idx){
      std::cerr << (column_names_and_types[pidx].first) << ", ";
    }
    std::cerr << ")" << std::endl;

    for(auto &row: table["rows"]){
     // server->LoadTableRow(table_name, column_names_and_types, row["values"], primary_key_col_idx);
      const std::vector<std::string> &vals = row;
      for(auto &val: vals){
        std::cerr << val << std::endl;
      }
    }
    std::cerr << std::endl;
  }

  return 0;
}
