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
static std::string delete_hook("DELETE FROM ");
//static std::string lbracket("(");
static std::string and_hook(" AND ");
static std::string or_hook(" OR ");

class SQLTransformer {
    public:
        SQLTransformer(){}
        ~SQLTransformer(){}
        void RegisterTables(std::string &table_registry);

        inline void NewTx(proto::Transaction *_txn){
            txn = _txn;
        }
        void TransformWriteStatement(std::string &write_statement, //std::vector<std::vector<uint32_t>> primary_key_encoding_support,
             std::string &read_statement, std::function<void(int, query_result::QueryResult*)>  &write_continuation, write_callback &wcb);

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

        void TransformInsert(size_t pos, std::string &write_statement, 
        std::string &read_statement, std::function<void(int, query_result::QueryResult*)>  &write_continuation, write_callback &wcb);
        void TransformUpdate(size_t pos, std::string &write_statement, 
        std::string &read_statement, std::function<void(int, query_result::QueryResult*)>  &write_continuation, write_callback &wcb);
        void TransformDelete(size_t pos, std::string &write_statement, 
        std::string &read_statement, std::function<void(int, query_result::QueryResult*)>  &write_continuation, write_callback &wcb);

        typedef struct ColRegistry {
            std::map<std::string, std::string> col_name_type; //map from column name to SQL data type (e.g. INT, VARCHAR, TIMESTAMP) --> Needs to be matched to real types for deser
            std::map<std::string, uint32_t> primary_key_cols; //ordered set.  //map from col name to index
            std::map<std::string, std::vector<std::string>> secondary_key_cols;
        } ColRegistry;
        std::map<std::string, ColRegistry> TableRegistry;
    
        bool CheckColConditions(std::string &cond_statement, std::string &table_name, std::map<std::string, std::string> &p_col_value);
        bool CheckColConditions(std::string &cond_statement, ColRegistry &col_registry, std::map<std::string, std::string> &p_col_value);
};


// class ReadSQLTransformer {
//     public:
//        QuerySQLTransformer(){}
//        ~QuerySQLTransformer(){}
// };


//define real types as variables in interpreter --> set respective one..  -->> return a void* that points to that value. Deref and do operations on it?
//Can use Any type?

//1) decode, 2) turn into a string, 3) if arithmetic, turn uint64_, 4 -> turn back to string, 5) let Neil figure out 


std::string DecodeType(std::unique_ptr<query_result::Field> &field, std::string &col_type);

std::string DecodeType(std::string &enc_value, std::string &col_type);

};