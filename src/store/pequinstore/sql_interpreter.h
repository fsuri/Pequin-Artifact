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
#ifndef SQL_INTERPRETER_H
#define SQL_INTERPRETER_H

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
#include <string_view>

#include "store/common/query_result/query_result_proto_wrapper.h"
#include "store/common/query_result/query_result_proto_builder.h"

#include <nlohmann/json.hpp>
using json = nlohmann::json;

namespace pequinstore {


//Query
static std::string select_hook("SELECT ");
static std::string from_hook(" FROM ");
static std::string where_hook(" WHERE ");
static std::string order_hook(" ORDER BY");
static std::string group_hook(" GROUP BY");
static std::string join_hook("JOIN");

//Insert
static std::string insert_hook("INSERT INTO ");
static std::string values_hook(" VALUES ");

//Update
static std::string update_hook("UPDATE ");
static std::string set_hook(" SET ");

//Delete
static std::string delete_hook("DELETE FROM ");

//Condition operators
static std::string and_hook(" AND ");
static std::string or_hook(" OR ");
static std::string in_hook("IN");
static std::string between_hook("BETWEEN");

enum col_t { //Could store col_t instead of String data type.
    BOOL,
    VARCHAR,
    TEXT,
    INT,
    BIGINT,
    SMALLINT
};

// const std::string col_type_args[] = {
// };


enum op_t {
    SQL_START,
    SQL_NONE,
    SQL_AND,
    SQL_OR,
    SQL_SPECIAL //e.g. IN or BETWEEN
};

struct StringVisitor {
    std::string operator()(int64_t &i) const { 
        return std::to_string(i);
    }
    std::string operator()(bool &b) const { 
        return std::to_string(b);
    }
    std::string operator()(std::string& s) const { 
        return s;
    }
};

// auto pred = [] (auto a, auto b)
//                    { return a.first == b; };


inline bool map_set_key_equality(const std::pair<std::string, std::string> &a, const std::string &b){
    return a.first == b; 
}


bool set_in_map(std::map<std::string, std::string> const &lhs, std::set<std::string> const &rhs);

//Returns true if computed active col set matches primary cols. (I.e. not more cols, and no less) 
inline bool primary_key_compare(std::map<std::string, std::string> const &lhs, std::set<std::string> const &rhs, bool relax = false) {
    if(relax){
        //Panic("Relax not yet enabled");
        //return true;
        return lhs.size() >= rhs.size() && set_in_map(lhs, rhs);
    }
    else{
        return lhs.size() == rhs.size() && std::equal(lhs.begin(), lhs.end(), rhs.begin(), map_set_key_equality);
    }
} 


typedef struct ColRegistry {
            std::map<std::string, std::string> col_name_type; //map from column name to SQL data type (e.g. INT, VARCHAR, TIMESTAMP) --> Needs to be matched to real types for deser
            std::map<std::string, uint32_t> col_name_index; //map from column name to index (order of cols/values in statements) 
            std::vector<std::string> col_names; //col_names in order
            std::vector<bool> col_quotes; //col_quotes in order

            std::vector<std::pair<std::string, uint32_t>> primary_key_cols_idx; //ordered (by index) from primary col name to index   //Could alternatively store a map from index to col name.
            std::set<std::string> primary_key_cols; 
            std::vector<uint32_t> primary_col_idx; 
            std::vector<bool> p_col_quotes; //col_quotes in order


            std::map<std::string, std::vector<std::string>> secondary_key_cols; //index name -> composite key vector

            std::set<std::string_view> indexed_cols; //all column names that are part of some index (be it primary or secondary)
} ColRegistry;


class SQLTransformer {
    public:
        SQLTransformer(){}
        ~SQLTransformer(){}
        void RegisterTables(std::string &table_registry);
        inline ColRegistry* GetColRegistry(const std::string &table_name){
            return &TableRegistry.at(table_name);
        }

        inline void NewTx(proto::Transaction *_txn){
            txn = _txn;
        }
        void TransformWriteStatement(std::string &_write_statement, //std::vector<std::vector<uint32_t>> primary_key_encoding_support,
             std::string &read_statement, std::function<void(int, query_result::QueryResult*)>  &write_continuation, write_callback &wcb, bool skip_query_interpretation = false);

        bool InterpretQueryRange(const std::string &_query, std::string &table_name, std::vector<std::string> &p_col_values, bool relax = false);

        std::string GenerateLoadStatement(const std::string &table_name, const std::vector<std::vector<std::string>> &row_segment)
;        void GenerateTableWriteStatement(std::string &write_statement, std::string &delete_statement, const std::string &table_name, const TableWrite &table_write);
            void GenerateTableWriteStatement(std::string &write_statement, std::vector<std::string> &delete_statements, const std::string &table_name, const TableWrite &table_write);
        void GenerateTablePurgeStatement(std::string &purge_statement, const std::string &table_name, const TableWrite &table_write); 
            void GenerateTablePurgeStatement_DEPRECATED(std::string &purge_statement, const std::string &table_name, const TableWrite &table_write);
            void GenerateTablePurgeStatement_DEPRECATED(std::vector<std::string> &purge_statements, const std::string &table_name, const TableWrite &table_write);

        

    private:
        proto::Transaction *txn;

        //Table Schema
        std::map<std::string, ColRegistry> TableRegistry;
       
        void TransformInsert(size_t pos, std::string_view &write_statement, 
            std::string &read_statement, std::function<void(int, query_result::QueryResult*)>  &write_continuation, write_callback &wcb);
        void TransformUpdate(size_t pos, std::string_view &write_statement, 
             std::string &read_statement, std::function<void(int, query_result::QueryResult*)>  &write_continuation, write_callback &wcb);
        void TransformDelete(size_t pos, std::string_view &write_statement, 
            std::string &read_statement, std::function<void(int, query_result::QueryResult*)>  &write_continuation, write_callback &wcb, bool skip_query_interpretation = false);

        //Helper Functions
        typedef struct Col_Update {
            std::string_view l_value;
            bool has_operand;
            std::string_view operand;
            std::string_view r_value;
        } Col_Update;

        void ParseColUpdate(std::string_view col_update, std::map<std::string_view, Col_Update> &col_updates);
        std::string GetUpdateValue(const std::string &col, std::variant<bool, int64_t, std::string> &field_val, std::unique_ptr<query_result::Field> &field, 
            std::map<std::string_view, Col_Update> &col_updates, const std::string &col_type, bool &change_val);
        TableWrite* AddTableWrite(const std::string &table_name, const ColRegistry &col_registry);
        RowUpdates* AddTableWriteRow(TableWrite *table_write, const ColRegistry &col_registry);


        //Read Parser
        bool CheckColConditions(std::string_view &cond_statement, std::string &table_name, std::vector<std::string> &p_col_values, bool relax = false);
        bool CheckColConditions(std::string_view &cond_statement, const ColRegistry &col_registry, std::vector<std::string> &p_col_values, bool relax = false);

        bool CheckColConditions(size_t &end, std::string_view cond_statement, const ColRegistry &col_registry, std::map<std::string, std::string> &p_col_value, bool &terminate_early, bool relax = false);
            void ExtractColCondition(std::string_view cond_statement, const ColRegistry &col_registry, std::map<std::string, std::string> &p_col_value);
            void GetNextOperator(std::string_view &cond_statement, size_t &op_pos, size_t &op_pos_post, op_t &op_type);
            bool MergeColConditions(op_t &op_type, std::map<std::string, std::string> &l_p_col_value, std::map<std::string, std::string> &r_p_col_value);
        bool CheckColConditionsDumb(std::string_view &cond_statement, const ColRegistry &col_registry, std::map<std::string, std::string> &p_col_value);
        
};

inline std::string_view TrimValue(std::string_view &val, bool trim = false){
    if(trim){
        val.remove_prefix(1);
        val.remove_suffix(1);
    }
    return val;
}
inline std::string_view TrimValueByType(std::string_view &val, const std::string &type){
    return TrimValue(val, (type == "TEXT" || type == "VARCHAR"));
}




// class ReadSQLTransformer {
//     public:
//        QuerySQLTransformer(){}
//        ~QuerySQLTransformer(){}
// };


//define real types as variables in interpreter --> set respective one..  -->> return a void* that points to that value. Deref and do operations on it?
//Can use Any type?

//1) decode, 2) turn into a string, 3) if arithmetic, turn uint64_, 4 -> turn back to string, 5) let Neil figure out 

template<typename T>
void DeCerealize(std::string &enc_value, T &dec_value){
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary); 
    ss << enc_value;
    {
        cereal::BinaryInputArchive iarchive(ss); // Create an input archive
        iarchive(dec_value); // Read the data from the archive
    }
}

std::variant<bool, int64_t, std::string> DecodeType(std::unique_ptr<query_result::Field> &field, const std::string &col_type);

std::variant<bool, int64_t, std::string> DecodeType(const std::string &enc_value, const std::string &col_type);
std::variant<bool, int64_t, std::string> DecodeType(std::string &enc_value, const std::string &col_type);

};


#endif /* SQL_INTERPRETER_H */