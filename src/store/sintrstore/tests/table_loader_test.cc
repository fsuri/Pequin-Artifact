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
#include <memory>
#include <fstream>
#include <vector>
#include <nlohmann/json.hpp>
using json = nlohmann::json;

#include "store/benchmark/async/json_table_writer.h"



int main() {
  
  std::cerr<< "Testing Table Writer and Loader" << std::endl;
  std::string file_name = "test";
 
  std::string table_name = "table";
  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  std::vector<uint32_t> primary_key_col_idx;
  std::vector<std::string> values;
  std::vector<std::uint32_t> index_cols_idx;

  TableWriter table_writer(file_name);

  //Table1:
  table_name = "table1";
  column_names_and_types.push_back(std::make_pair("col1", "INT"));
  column_names_and_types.push_back(std::make_pair("col2", "VARCHAR"));
  values.push_back("10");
  values.push_back("val");
  primary_key_col_idx.push_back(0);

  table_writer.add_table(table_name, column_names_and_types, primary_key_col_idx);
  table_writer.add_row(table_name, values);

  values.clear();
   values.push_back("20");
  values.push_back("val2");
   table_writer.add_row(table_name, values);

  //Table2:
  column_names_and_types.clear();
  values.clear();
  primary_key_col_idx.clear();

  table_name = "table2";
  std::string index_name = "indexA";
  column_names_and_types.push_back(std::make_pair("col1", "INT"));
  column_names_and_types.push_back(std::make_pair("col2", "INT"));
  column_names_and_types.push_back(std::make_pair("col3", "TEXT"));
  values.push_back("20");
  values.push_back("12");
  values.push_back("val2");
  primary_key_col_idx.push_back(0);
  primary_key_col_idx.push_back(2);
  index_cols_idx.push_back(1);
  index_cols_idx.push_back(2);

  table_writer.add_table(table_name, column_names_and_types, primary_key_col_idx);
  table_writer.add_index(table_name, index_name, index_cols_idx);
  table_writer.add_row(table_name, values);

  //Write Tables to JSON
  table_writer.flush();

  std::cerr << "Loading Tables" << std::endl;

  //Load Tables:

  std::string load_name = file_name + "-tables-schema.json";

  std::ifstream generated_tables(load_name);
  json tables_to_load = json::parse(generated_tables);
       
  //std::cerr << (tables_to_load["tables"].size()) << std::endl;
  std::cerr << tables_to_load << std::endl;

  //Load all tables. 
  for(auto &[table_name, table]: tables_to_load.items()){
    //std::string table_name = name; //["table_name"]; 
    const std::vector<std::pair<std::string, std::string>> &column_names_and_types = table["column_names_and_types"];
    const std::vector<uint32_t> &primary_key_col_idx = table["primary_key_col_idx"];
    std::cerr << table_name << std::endl;
    for(auto &[col, type]: column_names_and_types){
      std::cerr << col << " : " << type << std::endl;
    }
   
    std::cerr << "primary cols: ";
    std::cerr << "(";
    for(auto &pidx: primary_key_col_idx){
      std::cerr << (column_names_and_types[pidx].first) << ", ";
    }
    std::cerr << ")" << std::endl;

    for(auto &[index_name, index_col_idx]: table["indexes"].items()){
        std::cerr << "Index name: " << index_name << ":(";
        for(auto &i_idx: index_col_idx){
          std::cerr << (column_names_and_types[i_idx].first) << ", ";
        }
        std::cerr << ")" << std::endl;
    }

    // for(auto &row: table["rows"]){
    //  // server->LoadTableRow(table_name, column_names_and_types, row["values"], primary_key_col_idx);
    //   const std::vector<std::string> &vals = row;
    //   for(auto &val: vals){
    //     std::cerr << val << std::endl;
    //   }
    // }

    std::string FLAGS_data_file_path = "/Research/Projects/Sintr/Sintr-Artifact/src/sintrstore/testing/test-data-tables-schema.json";
    //std::string FLAGS_data_file_path = "test-data-tables-schema.json";
    // std::cerr << "PARSE PATH ROOT NAME: " << std::filesystem::path(FLAGS_data_file_path).root_name() << std::endl;
    // std::cerr << "PARSE PATH ROOT DIR: " << std::filesystem::path(FLAGS_data_file_path).root_directory() << std::endl;
    // std::cerr << "PARSE PATH ROOT PATH: " << std::filesystem::path(FLAGS_data_file_path).root_path() << std::endl;
    // std::cerr << "PARSE PATH RELATIVE: " << std::filesystem::path(FLAGS_data_file_path).relative_path() << std::endl;
    // std::cerr << "PARSE PATH PARENT: " << std::filesystem::path(FLAGS_data_file_path).parent_path() << std::endl;

    std::string row_data_path = std::filesystem::path(FLAGS_data_file_path).replace_filename(table["row_data_path"]);
    std::cerr << "TEST DATA PATH: " << row_data_path << std::endl;

    //Read in CSV:
    const std::string &row_file_name = table["row_data_path"];  //TODO: pass this to Import function
    //Additionally: Read The primary column values  
        //Read full column value, and then find primary.
        //OR: Read Header --> find index, then only extract that index
    
    std::cerr << "Reading Table Data from: " << row_file_name << std::endl;

    std::ifstream row_data(row_file_name);

    //Skip header
    std::string columns;
    getline(row_data, columns); 
    std::cerr << "columns: " << columns << std::endl;


    std::string row_line;
    std::string value;

    while(getline(row_data, row_line)){
      std::cerr << "next row: " << row_line << std::endl;
        std::vector<std::string> primary_col_vals;
        uint32_t col_idx = 0;
        uint32_t p_col_idx = 0;
       // used for breaking words
        std::stringstream row(row_line);
  
        // read every column data of a row and store it in a string variable, 'value'. Extract only the primary_col_values
         while (getline(row, value, ',')) {
          if(col_idx == primary_key_col_idx[p_col_idx]){
            p_col_idx++;
      
             std::cerr << "p_col_value: " << value << std::endl;
            primary_col_vals.push_back(std::move(value));
          }
          col_idx++;
        
        }
        
    }
  
    std::cerr << std::endl;
  }

  // CRDB Example:

  // std::cerr << "Testing CRDB Generation" << std::endl;

  // file_name = "crdb_example/crdb-kv";
   
  //  // TableWriter test_writer(file_name);

  // //  column_names_and_types.clear();
 
  // // primary_key_col_idx.clear();
 
  // // std::string test_name = "datastore";
  
  // // column_names_and_types.push_back(std::make_pair("key_", "TEXT"));
  // // column_names_and_types.push_back(std::make_pair("val_", "TEXT"));

  // // primary_key_col_idx.push_back(0);
  // // test_writer.add_table(test_name, column_names_and_types, primary_key_col_idx);

  // // //Write Tables to JSON
  // // test_writer.flush();


  // //Read CSV

  // load_name = file_name + "-tables-schema.json";
  //  std::ifstream crdb_tables(load_name);
  //  json crdb_tables_to_load = json::parse(crdb_tables);

  // //Load all tables. 
  // for(auto &[table_name, table]: crdb_tables_to_load.items()){
  //   //std::string table_name = name; //["table_name"]; 
  //   const std::vector<std::pair<std::string, std::string>> &column_names_and_types = table["column_names_and_types"];
  //   const std::vector<uint32_t> &primary_key_col_idx = table["primary_key_col_idx"];
  //   std::cerr << table_name << std::endl;
  //   for(auto &[col, type]: column_names_and_types){
  //     std::cerr << col << " : " << type << std::endl;
  //   }
   
  //   std::cerr << "primary cols: ";
  //   std::cerr << "(";
  //   for(auto &pidx: primary_key_col_idx){
  //     std::cerr << (column_names_and_types[pidx].first) << ", ";
  //   }
  //   std::cerr << ")" << std::endl;

  //   for(auto &[index_name, index_col_idx]: table["indexes"].items()){
  //       std::cerr << "Index name: " << index_name << ":(";
  //       for(auto &i_idx: index_col_idx){
  //         std::cerr << (column_names_and_types[i_idx].first) << ", ";
  //       }
  //       std::cerr << ")" << std::endl;
  //   }

  //   // for(auto &row: table["rows"]){
  //   //  // server->LoadTableRow(table_name, column_names_and_types, row["values"], primary_key_col_idx);
  //   //   const std::vector<std::string> &vals = row;
  //   //   for(auto &val: vals){
  //   //     std::cerr << val << std::endl;
  //   //   }
  //   // }


  //   //Read in CSV:
  //   const std::string &row_file_name = table["row_data_path"];  //TODO: pass this to Import function
  //   //Additionally: Read The primary column values  
  //       //Read full column value, and then find primary.
  //       //OR: Read Header --> find index, then only extract that index
    
  //   std::cerr << "Reading Table Data from: " << row_file_name << std::endl;

  //   std::ifstream row_data(row_file_name);

  //   //Skip header
  //   std::string columns;
  //   getline(row_data, columns); 
  //   std::cerr << "columns: " << columns << std::endl;


  //   std::string row_line;
  //   std::string value;

  //   while(getline(row_data, row_line)){
  //     std::cerr << "next row: " << row_line << std::endl;
  //       std::vector<std::string> primary_col_vals;
  //       uint32_t col_idx = 0;
  //       uint32_t p_col_idx = 0;
  //      // used for breaking words
  //       std::stringstream row(row_line);
  
  //       // read every column data of a row and store it in a string variable, 'value'. Extract only the primary_col_values
  //        while (getline(row, value, ',')) {
  //         if(col_idx == primary_key_col_idx[p_col_idx]){
  //           p_col_idx++;
      
  //            std::cerr << "p_col_value: " << value << std::endl;
  //           primary_col_vals.push_back(std::move(value));
  //         }
  //         col_idx++;
        
  //       }
        
  //   }
  //   std::cerr << std::endl;
  // }


  


  return 0;
}
