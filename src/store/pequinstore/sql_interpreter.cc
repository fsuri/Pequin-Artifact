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

#include "store/pequinstore/common.h"
#include <sys/time.h>
#include <algorithm>
#include <variant>
#include <iostream>
#include <sstream>
#include <cstdint>

#include "store/common/query_result/query_result_proto_wrapper.h"
#include "store/common/query_result/query_result_proto_builder.h"

#include "store/pequinstore/sql_interpreter.h"

namespace pequinstore {

using namespace std;



//TODO: Table Write byte encoding  --- Should be doable server side
    // Need to encode the column values as generic bytes. Try to use cereal library
    // At server need to decode the column value. Can one decode this without extra information? Or does one have to pass the type too
    // Maybe the server "knows" what type the bytes need to be?
    //Maybe just storing as string is actually fine? Since its part of a SQL statement usually... But now we want to use our own manual table write.


 void WriteSQLTransformer::NewTx(proto::Transaction *_txn){
    txn = _txn;
 }

void WriteSQLTransformer::TransformWriteStatement(std::string &write_statement, std::vector<std::vector<uint32_t>> primary_key_encoding_support, 
    std::string &read_statement, std::function<void(int, query_result::QueryResult*)>  &write_continuation, write_callback &wcb){

    //match on write type:
    size_t pos = 0;

    //Case 1) INSERT INTO <table_name> (<column_list>) VALUES (<value_list>)
    if( (pos = write_statement.find(insert_hook) != string::npos)){   //  if(write_statement.rfind("INSERT", 0) == 0){
        TransformInsert(pos, write_statement, primary_key_encoding_support, read_statement, write_continuation, wcb);
    }
    //Case 2) UPDATE <table_name> SET {(column = value)} WHERE <condition>
    else if( (pos = write_statement.find(update_hook) != string::npos)){  //  else if(write_statement.rfind("UPDATE", 0) == 0){
        TransformUpdate(pos, write_statement, primary_key_encoding_support, read_statement, write_continuation, wcb);
    }
    //Case 3) DELETE FROM <table_name> WHERE <condition>
    else if( (pos = write_statement.find(delete_hook) != string::npos)){  //   else if(write_statement.rfind("DELETE", 0) == 0){
        TransformDelete(pos, write_statement, primary_key_encoding_support, read_statement, write_continuation, wcb);
    }
    else{
        Panic("Currently only support the following Write statement operations: INSERT, DELETE, UPDATE");
    }
    //Case 4) REPLACE INTO:  Probably don't want to support either -- could turn into a Delete + Insert. Or just make it a blind write for efficiency
    //Case 4) SELECT INTO : Not supported, write statement as Select followed by Insert Into (new table)? Or parse into INSERT INTO statement with nested SELECT (same logic)

    
    
    
    // write_continuation = [this](query_result::QueryResult *result){

    //   sql::QueryResultProtoWrapper *write_result = new sql::QueryResultProtoWrapper(""); //TODO: replace with real result. Create proto builder and set rows affected somehow..
    //   uint32_t n_rows_affected = 0;
    //   write_result->set_rows_affected(n_rows_affected);
    //   return write_result;
    // }; // = //Some function that takes ResultObject as input and issue the write statements.
        //  for result-row in result{
        //     EncodeTableRow (use primary_key_encoding to derive it from the table and column_list)
        //     Find Ts in ReadSet 
        //     CreateTable Write entry with Timestamp and the rows to be updated. -- Note: Timestamp identifies the row from which no copy from -> i.e. the one thats updated.
                      //ReadSet is already cached as part of txn-> -- Right version can be found by just looking up the latest query seq in the txn->. TODO: if we want parallel writes (async) then we might need to identify
                      //Result ReadSet key can be inferred from Result Primary key.
                       //Version can be looked up by checking read set for this key (currently would have to loop -- but may want to turn into a map)
                              //If these two methods are not possible after all, then must modify sync to parameterize the fact that it is part of a "read-modify-write" 
                                                          //--> should explicitly label all entries in Read Set that belong to result rows... or must include full row here.
            // WriteMessage *write = txn->add_write_set();
            // write->set_key(key); //TODO: key = EncodeTableRow(table_name, primary_key)
            // *write->mutable_rowupdates(); //TODO: Set these.
            // *write->mutable_readtime()...//TODO Set this.
        //   }
    /////////////////

}

void WriteSQLTransformer::TransformInsert(size_t pos, std::string &write_statement, std::vector<std::vector<uint32_t>> primary_key_encoding_support, 
    std::string &read_statement, std::function<void(int, query_result::QueryResult*)>  &write_continuation, write_callback &wcb){

     //Case 1) INSERT INTO <table_name> (<column_list>) VALUES (<value_list>)
              //Note: Value list may be the output of a nested SELECT statement. In that case, embed the nested select statement as part of the read_statement
        //-> Turn into read_statement: Result(column, column_value) SELECT <primary_columns> FROM <table_name> WHERE <col = value>  // Nested Select Statement.
        //             write_cont: if(Result.empty()) create TableWrite with primary column encoded key, column_list, value_list
        //     TODO: Need to add to read set the time stamp of read "empty" version: I.e. for no existing version (result = empty) -> 0 (genesis TS); for deleted version --> version that deleted row.
                                                                                        // I think it's always fine to just set version to 0 here.
                                                                                        // During CC, should ignore conflicts of genesis against delete versions (i.e. they are equivalent)
        // TODO: Also need to write new "Table version" (in write set) -- to indicate set of rows changes     

    std::string table_name;
    std::vector<std::string> column_list;
    std::vector<std::string> value_list;

    //1 Remove insert hook
    write_statement.erase(0, pos + insert_hook.length()-1);  //TODO: Maybe do it without deletion for efficiency?
    
    //2 Split on values
    pos = write_statement.find(values_hook);
    UW_ASSERT(pos != std::string::npos);
    //Everything from 0 - pos is "<table_name>(columns)". Everything from pos + values_hook.length() --> end is "(values)"
    size_t val_pos = pos + values_hook.length();
    
    //3) Extract table
    //std::string table_col = write_statement.substr(0, pos);
    // Look for "(" (before end)
    pos = write_statement.find("(", 0); //Look only until start of values_hook   // Might be easier if we just create substring.
    if(pos == std::string::npos || pos > val_pos){ //if > val_pos then we found the "(" for Values
    // If "(" doesn't exist --> whole string is table_name.. Throw error -> can't compute Select Statement
        Panic("Codebase requires INSERT statement to contain (at least primary) column names for translation into SELECT statement");
    }
    
    //Extract table name
    table_name = write_statement.substr(0, pos);
    //Skip ahead past "("
    size_t col_pos = pos+1;  //FIXME: is "(".length() = 1

    // split on ", "
    // add item inbetween to cols vector   -- only search until 
    size_t next_col;
    while((next_col = write_statement.find(", ", col_pos)) != string::npos && next_col < val_pos){
        column_list.push_back(write_statement.substr(col_pos, next_col-col_pos));
        pos = next_col;
        col_pos = pos + 2;
    }

    // if no more ", " --> look for ")" and skip. Then insert last col value
    pos = write_statement.find(")", col_pos); //val_pos - values_hook.length() - col_pos
    UW_ASSERT(pos != std::string::npos && pos < val_pos);
    if(pos < val_pos){
     column_list.push_back(write_statement.substr(col_pos, pos-col_pos));
    }
    // Done.

    //4) Extract values
    // Look for "(" (before end)
    pos = write_statement.find("(", val_pos); //Look only from after values_hook   // Might be easier if we just create substring.
    // UW_ASSERT(pos != std::string::npos);

    //Skip ahead past "("
    val_pos = pos+1;  //FIXME: is "(".length() = 1

    // split on ", "
    // add item inbetween to values vector                                     //TODO: Value might be a nested Select + arithmetic. Extract Select statement and add to a map<col_name, select>. 
                                                                               // Then execute all the select statements to find all relevant values.
                                                                               // Then Union all the selects and perform as one query --> produces one query result
                                                                               // Loop through results and apply update with arithmetic.
                                                                        
    while((pos = write_statement.find(", ", val_pos)) != string::npos){
        value_list.push_back(write_statement.substr(val_pos, pos-val_pos));
        val_pos = pos + 2;
    }
    // if no more ", " --> look for ")" and skip. Then insert last value
    pos = write_statement.find(")", val_pos);
    // UW_ASSERT(pos != std::string::npos);
    value_list.push_back(write_statement.substr(val_pos, pos-val_pos));
    // Done.
            
    UW_ASSERT(value_list.size() == column_list.size()); //Require to pass all columns currently.
    UW_ASSERT(column_list.size() >= 1); // At least one column specified (e.g. single column primary key)
        

    ///////// //Create Read statement:  ==> Ideally for Inserts we'd just use a point get on the primary keys. (instead of a sql select statement that's a bit overkill)
    //TODO: What about nested statements.
    std::vector<const std::string*> primary_key_column_values;

    if(false){  //TODO: NOTE: FIXME: DO NOT NEED TO CREATE ANY READ STATEMENT FOR SINGLE ROW INSERTS. ==> Just set read version = 0 (TODO: Confirm OCC check will check vs latest version = delete)
                    //THIS WAY WILL SAVE QUERY ROUNDTRIP + WONT HAVE TO REMOVE TABLE VERSION POSSIBLY ADDED BY SCAN
            read_statement = "SELECT ";  
        //insert primary columns --> Can already concat them with delimiter:   col1  || '###' || col2 ==> but then how do we look up column?  
        for(auto p_idx: primary_key_encoding_support[0]){
            read_statement += column_list[p_idx] + ", ";   //TODO: Can just do Select *...
        }
        read_statement.resize(read_statement.size() - 2); //remove trailing ", "


        read_statement += " FROM " + table_name;

        read_statement += " WHERE ";
        for(auto p_idx: primary_key_encoding_support[0]){
            std::string &val = value_list[p_idx];
            read_statement += column_list[p_idx] + " = " + val + ", ";
            primary_key_column_values.push_back(&val);
        }
        //insert primary col conditions.
        read_statement.resize(read_statement.size() - 2); //remove trailing ", "

        read_statement += ";";
    }
    else{
        for(auto p_idx: primary_key_encoding_support[0]){
            std::string &val = value_list[p_idx];
            primary_key_column_values.push_back(&val);
        }
    }

    
    std::string enc_key = EncodeTableRow(table_name, primary_key_column_values);

    //////// Create Write continuation:  
    write_continuation = [this, wcb, enc_key, table_name, column_list, value_list](int status, query_result::QueryResult* result){
        //TODO: Does one need to use status? --> Query should not fail?
        if(result->empty()){
            

            //Read genesis timestamp (0) ==> FIXME: THIS CURRENTLY DOES NOT WORK WITH EXISTING OCC CHECK.
            ReadMessage *read = txn->add_read_set();
            read->set_key(enc_key);
            read->mutable_readtime()->set_id(0);
            read->mutable_readtime()->set_timestamp(0);

            //Create Table Write. Note: Enc_key encodes table_name + primary key column values.
            WriteMessage *write = txn->add_write_set();
            write->set_key(enc_key);
            for(int i=0; i<column_list.size(); ++i){
                (*write->mutable_rowupdates()->mutable_attribute_writes())[column_list[i]] = value_list[i];
            }

            //Write Table Version itself. //Only for kv-store.
            WriteMessage *table_ver = txn->add_write_set();
            table_ver->set_key(table_name);
            table_ver->set_value("");



            //Create result object with rows affected = 1.
            result->set_rows_affected(1);
            wcb(REPLY_OK, result);
        }
        else{
            //Create result object with rows affected = 0.
            result->set_rows_affected(0);
            wcb(REPLY_OK, result);
        }
    };


    return;

}



static std::string eq_hook = " = ";
void WriteSQLTransformer::ParseColUpdate(std::string col_update, std::map<std::string, Col_Update> &col_updates){

    //split on "=" into col and update
        size_t pos = col_update.find(eq_hook);
        UW_ASSERT(pos != std::string::npos);

        //Then parse Value based on operands.
        Col_Update &val = col_updates[col_update.substr(0, pos)];
        col_update.erase(0, pos + eq_hook.length());

        // find val.  //TODO: Add support for nesting if necessary.
        pos = col_update.find(" ");
        if(pos == std::string::npos){  //is string statement
            val.l_value = col_update;
            val.has_operand = false;
        }
        else{
            val.has_operand = true;
            //Parse operand; //Assuming here it is of simple form:  x <operand> y  and operand = {+, -, *, /}   Note: Assuming all values are Integers. For "/" will cast to float.
            val.l_value = col_update.substr(0, pos);
            val.operand = col_update.substr(pos+1, 1);
            val.r_value = col_update.substr(pos+3);
        }
}

void WriteSQLTransformer::TransformUpdate(size_t pos, std::string &write_statement, std::vector<std::vector<uint32_t>> primary_key_encoding_support, 
    std::string &read_statement, std::function<void(int, query_result::QueryResult*)>  &write_continuation, write_callback &wcb){

    //Case 2) UPDATE <table_name> SET {(column = value)} WHERE <col_name = condition>
    //-> Turn into read_statement: Result(column, column_value = rows(attributes))
    //             SELECT * FROM <table_name> (value_columns) WHERE <condition>         //Note "*" returns all columns for the rows. Use the primary key encoding to find primary columns
                                                                                    //Alternatively, would select only the columns required by the Values; and pass the primary columns in separately.
    //             write_cont: for (column = value) statement, create TableWrite with primary column encoded key, column_list, and column_values (or direct inputs)

    std::string table_name;
    std::map<std::string, Col_Update> col_updates;
        std::vector<std::string> column_list;
        std::vector<std::string> value_list;  //Note: This may either be a direct update, e.g. "= 5" or with an operator, e.g. "= col_val + 5"; // TODO: Parse this in the continuation.
    std::string where_cond;

    //1 Remove insert hook
    write_statement.erase(0, pos + update_hook.length()-1);  //TODO: Maybe do it without deletion for efficiency?
    
    //2 Split on values
    pos = write_statement.find(set_hook);
    UW_ASSERT(pos != std::string::npos);
    
    //3) Extract table name
    table_name = write_statement.substr(0, pos);
    
    //Skip ahead past "SET" hook
    //Everything from 0 - pos is "<table_name>". Everything from set_pos - where_pos is the content between Set and Where --> "SET <CONTENT> WHERE"
    size_t set_pos = pos + set_hook.length();
    size_t where_pos = write_statement.find(where_hook);  
    UW_ASSERT(where_pos != std::string::npos); //TODO: Assuming here it has a Where hook. If not (i.e. update all rows), then ignore parsing it. (i.e. set where_pos to length of string, and skip where clause)

    // split on ", " to identify the updates.
    // for each string split again on "=" and insert into column and value lists
    size_t next_up;
    while(next_up < where_pos && (next_up = write_statement.find(", ", set_pos)) != string::npos){ //Find next ", ", look only up to where hook.
        ParseColUpdate(write_statement.substr(set_pos, next_up-set_pos), col_updates);
       
        set_pos = next_up + 2; //skip past ", "
        next_up = set_pos;
    }
    //Note: After loop is done next_up == npos
    //Insert last item (between next_up and where_pos)
    ParseColUpdate(write_statement.substr(set_pos, where_pos-set_pos), col_updates);
    
   
    // isolate the Whole where condition and just re-use in the SELECT statement? -- can keep the Where hook.
    //Skip past "WHERE" hook
    //where_pos += where_hook.length();
    where_cond = write_statement.substr(where_pos);
        //If we want to isolate the where conditions:
        //split conditions on "AND" or "OR"
        //Within cond, split string on "=" --> extract cond column and cond value
   
    UW_ASSERT(value_list.size() == column_list.size()); //Require to pass all columns currently.
    UW_ASSERT(col_updates.size() >= 1); // At least one column specified to be updated
        
    ///////// //Create Read statement:  Just Select * with Where condition
            //==> Ideally for Updates we'd just select on the primary key columns. (instead of a sql select * statement that's a bit overkill)
            //std::vector<const std::string*> primary_key_column_values;

    read_statement = "SELECT * FROM ";
    read_statement += table_name;
    read_statement += where_cond;  //Note: Where cond starts with a " "
       
       
    //////// Create Write continuation:  
    write_continuation = [this, wcb, table_name, col_updates, primary_key_encoding_support](int status, query_result::QueryResult* result){

         //Write Table Version itself. //Only for kv-store.
        WriteMessage *table_ver = txn->add_write_set();
        table_ver->set_key(table_name);
        table_ver->set_value("");


       


        //For each row in query result
        for(int i = 0; i < result->size(); ++i){
            std::unique_ptr<query_result::Row> row = (*result)[i];

            std::vector<const std::string*> primary_key_column_values;
            for(auto idx: primary_key_encoding_support[0]){
                const std::string &p_col_name = row->name(idx);
                primary_key_column_values.push_back(&p_col_name);
            }
            std::string enc_key = EncodeTableRow(table_name, primary_key_column_values);

            WriteMessage *write = txn->add_write_set();
            write->set_key(enc_key);
            // For col in col_updates update the columns specified by update_cols. Set value to update_values
            for(int j=0; j<row->columns(); ++j){
                const std::string &col = row->name(j);
                std::unique_ptr<query_result::Field> field = (*row)[j];
                size_t nbytes;
                const char* field_val_char = field->get(&nbytes);
                std::string field_val(field_val_char, nbytes);

                //Deserialize encoding to be a string. //FIXME: What if serialized value was an int? //TODO: This seems to be a pointless encoding/decoding. 
                std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
                ss << field_val;
                {
                    cereal::BinaryInputArchive iarchive(ss); // Create an input archive
                    iarchive(field_val); // Read the data from the archive
                }

                std::cerr << "Checking column " << col << " with field " << field_val << std::endl;
                

                //Replace value with col value if applicable. Then operate arithmetic by casting ops to uint64_t and then turning back to string.
                auto itr = col_updates.find(col);
                if(itr != col_updates.end()){
                    std::cerr << "replacing col: " << col << std::endl;
                    //Update value.
                    const Col_Update &col_update = itr->second;
                    if(col_update.has_operand){
                        std::cerr << "update has operand: " << col_update.operand << std::endl;
                         std::cerr << "lvalue = " << col_update.l_value << std::endl;
                        uint64_t l_value;
                        uint64_t r_value;
                        //TODO: Check if l_value needs to be replaced
                        //Search Col by name... 
                        //FIXME: For now just use current col -- I assume that's always the case tbh..
                        if(col_update.l_value == field->name()){
                            std::istringstream iss(field_val);
                            iss.exceptions(std::ios::failbit | std::ios::badbit);
                            iss >> l_value;  
                             std::cerr << "lvalue = " << l_value << std::endl;
                        }
                        else{ //Otherwise l_value is already the number...
                            std::istringstream iss(col_update.l_value);
                            iss >> l_value;  
                        }

                         if(col_update.r_value == field->name()){
                            std::istringstream iss(field_val);
                            iss >> r_value;  
                        }
                        else{ //Otherwise l_value is already the number...
                            std::istringstream iss(col_update.r_value);
                            iss >> r_value;  
                        }

                        uint64_t output;
                        if(col_update.operand == "+"){
                            output = l_value + r_value;
                            std::cerr << "output = " << output << std::endl;
                        }
                        else if(col_update.operand == "-"){
                            output = l_value - r_value;
                        }
                        else if(col_update.operand == "*"){
                            output = l_value * r_value;
                        }
                        else if(col_update.operand == "/"){
                            output = l_value / r_value;  //Note: this will round instead of producing a float.
                        }
                        else{
                            Panic("Unsupported operand %s", col_update.operand);
                        }
                       
                        
                        (*write->mutable_rowupdates()->mutable_attribute_writes())[col] = ""+ std::to_string(output);
                    }
                    else{
                        //Replace col value if it's a placeholder:  ///FIXME: Currently this just takes the same col name, i.e. it keeps the value the same... NOTE: Should never be triggered
                        if(col_update.l_value == field->name()){
                            Panic("Placeholder should not be existing column value");
                            (*write->mutable_rowupdates()->mutable_attribute_writes())[col] = std::move(field_val);
                        }
                        else{
                            (*write->mutable_rowupdates()->mutable_attribute_writes())[col] = std::move(col_update.l_value);
                        }
                    }
                }
                else{
                      (*write->mutable_rowupdates()->mutable_attribute_writes())[col] = std::move(field_val);
                }
            }    

        }

            // //Isolate primary keys ==> create encoding and table write
          
            // //Copy all column values (unless in col_updates)
            // //For col in col_updates update the columns specified by update_cols. Set value to update_values
            //         //Replace value with col value if applicable. Then operate arithmetic by casting ops to uint64_t and then turning back to string.

    
            // //Create Table Write. Note: Enc_key encodes table_name + primary key column values.
            // WriteMessage *write = txn->add_write_set();
            // write->set_key(enc_key);
            // for(int i=0; i<column_list.size(); ++i){
            //     (*write->mutable_rowupdates()->mutable_attribute_writes())[column_list[i]] = value_list[i];
            // }
        
     
        result->set_rows_affected(0); //TODO: Fill in with number of rows.
        wcb(REPLY_OK, result);
        
    };



}

void WriteSQLTransformer::TransformDelete(size_t pos, std::string &write_statement, std::vector<std::vector<uint32_t>> primary_key_encoding_support, 
    std::string &read_statement, std::function<void(int, query_result::QueryResult*)>  &write_continuation, write_callback &wcb){
    
     //Case 3) DELETE FROM <table_name> WHERE <condition>
         //-> Turn into read_statement: Result(column, column_value) SELECT FROM <table_name>(primary_columns) 
         //             write_cont: for(row in result) create TableWrite with primary column encoded key, bool = delete (create new version with empty values/some meta data indicating delete)
         // TODO: Handle deleted versions: Create new version with special delete marker. NOTE: Read Sets of queries should include the empty version; but result computation should ignore it.
         // But how can one distinguish deleted versions from rows not yet created? Maybe one MUST have semantic CC to support new row inserts/row deletions.


}

};