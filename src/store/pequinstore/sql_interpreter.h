// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/pequinstore/client.cc:
 *   Client to INDICUS transactional storage system.
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

#include "store/common/frontend/client.h"
#include "store/common/transaction.h"
#include "store/pequinstore/pequin-proto.pb.h"
#include "store/pequinstore/common.h"
#include <sys/time.h>
#include <algorithm>
#include <variant>
#include <iostream>
#include <sstream>
#include <cstdint>

#include "store/common/query_result/query_result_proto_wrapper.h"
#include "store/common/query_result/query_result_proto_builder.h"

#include <nlohmann/json.hpp>
using json = nlohmann::json;

namespace pequinstore {

using namespace std;



static std::string insert_hook("INSERT INTO ");
static std::string values_hook(" VALUES ");
static std::string update_hook("UPDATE ");
static std::string set_hook(" SET ");
static std::string where_hook(" WHERE ");
static std::string delete_hook("DELETE ");
//static std::string lbracket("(");


class SQLTransformer {
    public:
        SQLTransformer(){}
        ~SQLTransformer(){}
        void NewTx(proto::Transaction *_txn);
        void TransformWriteStatement(std::string &write_statement, std::vector<std::vector<uint32_t>> primary_key_encoding_support,
             std::string &read_statement, std::function<void(int, query_result::QueryResult*)>  &write_continuation, write_callback &wcb);

        void RegisterTables(std::string &table_registry);
    private:
        proto::Transaction *txn;
        typedef struct Col_Update {
            std::string l_value;
            bool has_operand;
            std::string operand;
            std::string r_value;
        
            //TODO: cast all values to uint64 to perform operand
        } Col_Update;
        void ParseColUpdate(std::string col_update, std::map<std::string, Col_Update> &col_updates);

        void TransformInsert(size_t pos, std::string &write_statement, std::vector<std::vector<uint32_t>> primary_key_encoding_support, 
        std::string &read_statement, std::function<void(int, query_result::QueryResult*)>  &write_continuation, write_callback &wcb);
        void TransformUpdate(size_t pos, std::string &write_statement, std::vector<std::vector<uint32_t>> primary_key_encoding_support, 
        std::string &read_statement, std::function<void(int, query_result::QueryResult*)>  &write_continuation, write_callback &wcb);
        void TransformDelete(size_t pos, std::string &write_statement, std::vector<std::vector<uint32_t>> primary_key_encoding_support, 
        std::string &read_statement, std::function<void(int, query_result::QueryResult*)>  &write_continuation, write_callback &wcb);

        typedef struct ColRegistry {
            std::map<std::string, std::string> col_name_type; //map from column name to SQL data type (e.g. INT, VARCHAR, TIMESTAMP) --> Needs to be matched to real types for deser
            std::vector<std::string> primary_key_cols;
            std::map<std::string, std::vector<std::string>> secondary_key_cols;
        } ColRegistry;
        std::map<std::string, ColRegistry> TableRegistry;
        //TODO: Can remove primary_key_encoding_support.
        //TODO: Load Table Registry from File Name:

};

RegisterTables(std::string &table_registry){ //TODO: This table registry file does not need to include the rows.

    std::ifstream generated_tables(FLAGS_data_file_path);
    json tables_to_load = json::parse(generated_tables);
       
       //Load all tables. 
       for(auto &[table_name, table_args]: tables_to_load.items()){ 
          const std::vector<std::pair<std::string, std::string>> &column_names_and_types = table_args["column_names_and_types"];
          const std::vector<uint32_t> &primary_key_col_idx = table_args["primary_key_col_idx"];

          ColRegistry &col_registry = TableRegistry[table_name];
          //register column types
          for(auto &[col_name, col_type]: column_names_and_types){
            col_registry.col_name_type[col_name] = col_type;
          }
          //register primary key
          for(auto &p_idx: primary_key_col_idx){
            col_registry.primary_key_cols.push_back(column_names_and_data_types[p_idx].first);
          }
          //register secondary indexes
          for(auto &[index_name, index_col_idx]: table_args["indexes"].items()){
             std::vector<std::string> &index_cols = col_registry.secondary_key_cols[index_name];
             for(auto &i_idx: index_col_idx){
                index_cols.push_back(column_names_and_data_types[i_idx].first)
             }
          }
       }
}

// class ReadSQLTransformer {
//     public:
//        QuerySQLTransformer(){}
//        ~QuerySQLTransformer(){}
// };


};