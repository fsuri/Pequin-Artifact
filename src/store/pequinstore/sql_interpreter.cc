// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/pequinstore/client.cc:
 *   Client to INDICUS transactional storage system.
 *
 * Copyright 2023 Florian Suri-Payer <fsp@cs.cornell.edu>
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
#include <algorithm>
#include <variant>
#include <iostream>
#include <sstream>
#include <cstdint>  
#include <regex>
#include <string_view>
#include <fmt/core.h>
#include <fmt/ranges.h>

#include "store/pequinstore/sql_interpreter.h"

namespace pequinstore {

using namespace std;


//Note on Value quoting:
    //We expect the following input syntax: https://www.prisma.io/dataguide/postgresql/short-guides/quoting-rules 
        // Insert: Only strings are single quoted (i.e. TEXT or VARCHAR)
        // Update SET: Only strings are single quoted ''  -- put differently, you can write "col1 = col1 + 5", or "col1 = 5" without quotes around 5.
        // WHERE: Only strings single quoted. E.g write WHERE col1 = 5 or col = true or col = 'monkey'.
    //The client side Transformer functions remove all quotes and produce a) a quote free primary key encoding, and b) a quote free TableWrite set
    //The serverside Generator function adds quotes to all values indiscriminately
    //Note: The Peloton result set (and consequently the QueryResult set) contains no quotes.

//Table Write byte encoding:
    //"Stringify" all values
    // Peloton Engine is able to convert to appropriate Table type using Schema information.
    // Currently we just turn Values into a joint Insert statement

    //Alternatively:
        // Could encode the column values as generic bytes using the cereal library
        // At server would then need to decode the column value. This requires knowing the type and passing it to the decoder function

    //Alternatively:
        //Could use Protobuf as Variant of types (oneof) for TableWrite
        //Peloton would have to pick the desired type to retrieve.

//Current full ser/deser lifcycle:
    // 1. SQL statement (string) --> Peloton: Outputs serialized Peloton result
    // 2. Serialized Peloton Result --> Proto Wrapper: Deserialize Peloton + Cerialize into Proto
    // 3. Proto Wrapper -> WriteCont: deCerialize Proto (perform arithmetic) + Stringify TableWrite     //Note: Can only stringify basic types; if we want to support Array, then we need to serialize somehow
    // 4. TableWrite -> Peloton: De-stringify into correct type.

void SQLTransformer::RegisterTables(std::string &table_registry){ //TODO: This table registry file does not need to include the rows.

    Debug("Register tables from registry: %s", table_registry.c_str());
    if(table_registry.empty()) Panic("Did not provide a table_registry for a sql_bench");

    std::ifstream generated_tables(table_registry);
    json tables_to_load;
     try {
        tables_to_load = json::parse(generated_tables);
    }
    catch (const std::exception &e) {
        Panic("Failed to load Table JSON Schema");
    }
       
    //Load all tables. 
    for(auto &[table_name, table_args]: tables_to_load.items()){ 
        const std::vector<std::pair<std::string, std::string>> &column_names_and_types = table_args["column_names_and_types"];
        const std::vector<uint32_t> &primary_key_col_idx = table_args["primary_key_col_idx"];

        ColRegistry &col_registry = TableRegistry[table_name];
        Debug("Registering Table: %s", table_name.c_str());
        
        //register column types
        uint32_t i = 0;
        for(auto &[col_name, col_type]: column_names_and_types){
            col_registry.col_name_type[col_name] = col_type;
            //col_registry.col_name_index.emplace_back(col_name, i++);
            col_registry.col_name_index[col_name] = i++;
            col_registry.col_names.push_back(col_name);
            (col_type == "TEXT" || col_type == "VARCHAR") ? col_registry.col_quotes.push_back(true) : col_registry.col_quotes.push_back(false);
        
        //std::cerr << "   Register column " << col_name << " : " << col_type << std::endl;
        }
        Debug(fmt::format("Column Names: [{}]", fmt::join(col_registry.col_names,"|")).c_str());
        Debug(fmt::format("Column Names/Types: [{}]", fmt::join(col_registry.col_name_type,"|")).c_str());

        //register primary key
        for(auto &p_idx: primary_key_col_idx){
            //col_registry.primary_key_cols_idx[column_names_and_types[p_idx].first] = p_idx;
            col_registry.primary_key_cols_idx.emplace_back(column_names_and_types[p_idx].first, p_idx);
            col_registry.primary_key_cols.insert(column_names_and_types[p_idx].first);
            (col_registry.col_quotes[p_idx]) ? col_registry.p_col_quotes.push_back(true) : col_registry.p_col_quotes.push_back(false);
            //std::cerr << "Primary key col " << column_names_and_types[p_idx].first << std::endl;

            col_registry.indexed_cols.emplace(col_registry.col_names[p_idx]);
        }
        col_registry.primary_col_idx = primary_key_col_idx;

        Debug(fmt::format("Primary key cols: [{}]", fmt::join(col_registry.primary_key_cols,"|")).c_str());

        //register secondary indexes
        for(auto &[index_name, index_col_idx]: table_args["indexes"].items()){
            std::vector<std::string> &index_cols = col_registry.secondary_key_cols[index_name];
            for(auto &i_idx: index_col_idx){
                index_cols.push_back(column_names_and_types[i_idx].first);
                //Record all cols that are part of any index
                 col_registry.indexed_cols.insert(col_registry.col_names[i_idx]);
            }
            Debug(fmt::format("Secondary Index: {} with key cols: [{}]", index_name, fmt::join(index_cols,"|")).c_str());
        }
    }
}


bool SQLTransformer::InterpretQueryRange(const std::string &_query, std::string &table_name, std::vector<std::string> &p_col_values, bool relax){
    //Using Syntax from: https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-select/

    std::string_view query_statement(_query);
    //Parse Table name.
    size_t from_pos = query_statement.find(from_hook);
    if(from_pos == std::string::npos) return false; //Reading full DB.
    
    query_statement.remove_prefix(from_pos + from_hook.length());

    //If query contains a JOIN statement --> Cannot be point read.
    if(size_t join_pos = query_statement.find(join_hook); join_pos != std::string::npos) return false; 
    
    size_t where_pos = query_statement.find(where_hook);

    //Parse where cond (if none, then it's automatically a range)
    if(where_pos == std::string::npos) return false;
    
    table_name = std::move(static_cast<std::string>(query_statement.substr(0, where_pos)));
    //If query tries to read from multiple tables --> Cannot be point read. It is an implicit join. E.g. "Select * FROM table1, table2 WHERE"
    if(size_t pos = table_name.find(","); pos != std::string::npos){
        //TODO: Automatically add reflexive conditions.
        return false;
    } 
   
    std::string_view cond_statement = query_statement.substr(where_pos + where_hook.length()); // I.e. everything after WHERE

    //Ensure that there is no LIMIT or ORDER BY or GROUP BY or other stuff after the where. If there is, then it's safe to assume that it's not a point query.
    if(size_t order_pos = cond_statement.find(order_hook); order_pos != std::string::npos) return false;
    if(size_t grp_pos = cond_statement.find(group_hook); grp_pos != std::string::npos) return false;

    //std::cerr << "checking col conds: " << cond_statement << std::endl;
    return CheckColConditions(cond_statement, table_name, p_col_values, relax); //TODO: Enable Relax
    //true == point, false == range read --> use table_name + p_col_value to do a point read.
}

void SQLTransformer::TransformWriteStatement(std::string &_write_statement, 
    std::string &read_statement, std::function<void(int, query_result::QueryResult*)>  &write_continuation, write_callback &wcb, bool skip_query_interpretation){

    //match on write type:
    size_t pos = 0;

    //Note: This creates a reference to the _write_statement; as long as Write is ongoing, _write_statement must stay in scope!
    std::string_view write_statement(_write_statement);

    // std::cerr << "TEST: " << std::endl;
    //  std::cerr << "string write: " << _write_statement << std::endl;
    //  std::cerr << "string view write: " << write_statement << std::endl;
    //  _write_statement = "TEST";
    //   std::cerr << "string view write2: " << write_statement << std::endl;

    //Case 1) INSERT INTO <table_name> (<column_list>) VALUES (<value_list>)
    if( (pos = write_statement.find(insert_hook) != string::npos)){   //  if(write_statement.rfind("INSERT", 0) == 0){
        TransformInsert(pos, write_statement, read_statement, write_continuation, wcb);
    }
    //Case 2) UPDATE <table_name> SET {(column = value)} WHERE <condition>
    else if( (pos = write_statement.find(update_hook) != string::npos)){  //  else if(write_statement.rfind("UPDATE", 0) == 0){
        TransformUpdate(pos, write_statement, read_statement, write_continuation, wcb);
    }
    //Case 3) DELETE FROM <table_name> WHERE <condition>
    else if( (pos = write_statement.find(delete_hook) != string::npos)){  //   else if(write_statement.rfind("DELETE", 0) == 0){
        TransformDelete(pos, write_statement, read_statement, write_continuation, wcb, skip_query_interpretation);
    }
    else{
        Panic("Currently only support the following Write statement operations: INSERT, DELETE, UPDATE");
    }
    //Case 4) REPLACE INTO:  Probably don't want to support either -- could turn into a Delete + Insert. Or just make it a blind write for efficiency
    //Case 4) SELECT INTO : Not supported, write statement as Select followed by Insert Into (new table)? Or parse into INSERT INTO statement with nested SELECT (same logic)
}

//TODO: Modify to support multi-row insert -> create row in TableWrite for each parsed result.
void SQLTransformer::TransformInsert(size_t pos, std::string_view &write_statement,
    std::string &read_statement, std::function<void(int, query_result::QueryResult*)>  &write_continuation, write_callback &wcb){
    //Based on Syntax from: https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-insert/ 
    // https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-insert-multiple-rows/ https://www.digitalocean.com/community/tutorials/sql-insert-multiple-rows (TODO: Not yet implemented)

     //Case 1) INSERT INTO <table_name> (<column_list>) VALUES (<value_list>)
              //Note: Value list may be the output of a nested SELECT statement. In that case, embed the nested select statement as part of the read_statement
        //-> Turn into read_statement: Result(column, column_value) SELECT <primary_columns> FROM <table_name> WHERE <col = value>  // Nested Select Statement. //Optimization: Don't send a Read.
        //             write_cont: if(Result.empty()) create TableWrite with primary column encoded key, column_list, value_list
        //     TODO: Need to add to read set the time stamp of read "empty" version: I.e. for no existing version (result = empty) -> 0 (genesis TS); for deleted version --> version that deleted row.
                                                                                        // I think it's always fine to just set version to 0 here.
                                                                                        // During CC, should ignore conflicts of genesis against delete versions (i.e. they are equivalent)
        // TODO: Also need to write new "Table version" (in write set) -- to indicate set of rows changes     

    std::string table_name;
    std::vector<std::string> column_list;
    std::vector<std::string_view> value_list;

    //1 Remove insert hook
    write_statement = write_statement.substr(pos + insert_hook.length()-1);
  
    //2 Split on values
    pos = write_statement.find(values_hook);
    UW_ASSERT(pos != std::string::npos);
    //Everything from 0 - pos is "<table_name>(columns)". Everything from pos + values_hook.length() --> end is "(values)"
    std::string_view table_col_statement = write_statement.substr(0, pos);
    std::string_view values_statement = write_statement.substr(pos + values_hook.length());

    
    //3) Extract table
    //std::string table_col = write_statement.substr(0, pos);
    // Look for "(" (before end)
    pos = table_col_statement.find("("); //Look only until start of values_hook   // Might be easier if we just create substring.
    //NOTE: With TableRegistry extractic columsn is obsolete.
    if(pos == std::string::npos){ //if > val_pos then we found the "(" for Values
        // If "(" doesn't exist --> whole string is table_name.. Throw error -> can't compute Select Statement
        table_name = std::move(static_cast<std::string>(table_col_statement));
        //Panic("Codebase requires INSERT statement to contain (at least primary) column names for translation into SELECT statement");
    }
    else{
        //Extract table name
        table_name = std::move(static_cast<std::string>(table_col_statement.substr(0, pos-1)));

        //Remove "()"
        //NOTE: with col registry there is no longer a need to parse cols.
        if(false){
             std::string_view col_statement = table_col_statement.substr(pos);
            col_statement.remove_prefix(1); //remove "("
            pos = col_statement.find(")"); 
            UW_ASSERT(pos != std::string::npos);
            col_statement.remove_suffix(1); //remove ")"

            // split on ", "
            // add item inbetween to cols vector   -- only search until 
            if(false){
                size_t next_col;
                while((next_col = col_statement.find(", ")) != string::npos){
                    column_list.push_back(std::move(static_cast<std::string>(col_statement.substr(0, next_col))));
                    col_statement = col_statement.substr(next_col + 2);
                }
                column_list.push_back(std::move(static_cast<std::string>(col_statement))); //push back last col (only remaining).
                }
        }
        // Done.
    }

    Debug("Find TABLE: [%s] in Registry", table_name.c_str());
    auto itr = TableRegistry.find(table_name);
    UW_ASSERT(itr != TableRegistry.end()); //Panic if ColRegistry does not exist.
    const ColRegistry &col_registry = itr->second;//TableRegistry[table_name];
    
    //4) Extract values
    // Remove "()"
    pos = values_statement.find("("); //Look only from after values_hook  
    UW_ASSERT(pos != std::string::npos);
    values_statement.remove_prefix(1); //remove "("
    pos = values_statement.find_last_of(")"); //Look only from after values_hook  
    UW_ASSERT(pos != std::string::npos);
    values_statement.remove_suffix(values_statement.length()-pos); //remove ")"

    //Check that there are no nested Selects
    pos = values_statement.find("SELECT");
    if(pos != std::string::npos) Panic("Pequinstore does not support Write Parsing for Inserts with nested Select statements. Please write the statements sequentially");
    //Note: If we wanted to support nested Inserts: We'd have to parse all present Selects, execute them in parallel ideally, and pass the write_cont as callback that is exec once all select results ready...
         //E.g. Value might be a nested Select + arithmetic. Extract Select statement and create a callback that adds result to a map<col_name, select>. 
                                                         // Then execute all the select statements to find all relevant values. 
                                                         // Once callback is notified that alls selects are done --> call write_cont with the result map
                                                         // Alternatively: Extract all selects and store any arithmetic (cont statements) in a map
                                                                            //Then Union all the selects and perform as one query --> produces one query result
                                                                            //Callback Write_cont ==> Loop through results and apply update with arithmetic.

    // split on ", "
    // add item inbetween to values vector    
    int i = 0;        //remove quotes if applicable                        
    size_t next_val;                                                                
    while((next_val = values_statement.find(", ")) != string::npos){
        std::string_view curr_val = values_statement.substr(0, next_val);
        value_list.push_back(std::move(TrimValue(curr_val, col_registry.col_quotes[i++]))); //value_list.push_back(std::move(static_cast<std::string>(values_statement.substr(0, next_val))));
        values_statement = values_statement.substr(next_val+2);
    }
    value_list.push_back(std::move(TrimValue(values_statement, col_registry.col_quotes[i++]))); //value_list.push_back(std::move(static_cast<std::string>(values_statement))); //push back last value (only remaining).

    // Done.
            
    //UW_ASSERT(value_list.size() == column_list.size()); //Require to pass all columns currently.
    //UW_ASSERT(column_list.size() >= 1); // At least one column specified (e.g. single column primary key)
     //UW_ASSERT(column_list.size() >= 1); // At least one column specified (e.g. single column primary key)
        

    ///////// //Create Read statement:  ==> Ideally for Inserts we'd just use a point get on the primary keys. (instead of a sql select statement that's a bit overkill)
    
    std::vector<const std::string_view*> primary_key_column_values;


    UW_ASSERT(value_list.size() == col_registry.col_name_type.size());

    if(false){  //NOTE: DO NOT NEED TO CREATE ANY READ STATEMENT FOR SINGLE ROW INSERTS. ==> Just set read version = 0 (TODO: Confirm OCC check will check vs latest version = delete)
                    //THIS WAY WILL SAVE QUERY ROUNDTRIP + WONT HAVE TO REMOVE TABLE VERSION POSSIBLY ADDED BY SCAN
           
        //read_statement = "SELECT ";  
        std::string col_statement;
        //insert primary columns --> Can already concat them with delimiter:   col1  || '###' || col2 ==> but then how do we look up column?  
        for(auto [col_name, p_idx]: col_registry.primary_key_cols_idx){
            col_statement += col_name +  ", ";   //TODO: Can just do Select *...
            //read_statement += column_list[p_idx] +  ", ";   //TODO: Can just do Select *...
        }
        col_statement.resize(read_statement.size() - 2); //remove trailing ", "


        // read_statement += " FROM " + table_name;

        // read_statement += " WHERE ";
        std::string cond_statement;
        for(auto [col_name, p_idx]: col_registry.primary_key_cols_idx){
            const std::string_view &val = value_list[p_idx];
            //read_statement += column_list[p_idx] + " = " + val + ", ";
            cond_statement += col_name + " = ";
            cond_statement += val;
            cond_statement += ", ";
            primary_key_column_values.push_back(&val);
        }
        //insert primary col conditions.
        cond_statement.resize(read_statement.size() - 2); //remove trailing ", "

        //read_statement += ";";

        //use fmt::format to create more readable read_statement generation.
        //read_statement = fmt::format("SELECT {0} FROM {1} WHERE {2};", std::move(col_statement), table_name, std::move(cond_statement));
    }
    else{
        for(auto [col_name, p_idx]: col_registry.primary_key_cols_idx){
            const std::string_view &val = value_list[p_idx];
            primary_key_column_values.push_back(&val);
        }
    }

    
    std::string enc_key = EncodeTableRow(table_name, primary_key_column_values);
    bool is_blind_write = col_registry.primary_col_idx.empty(); //If there is no primary key, then uniqueness doesn't matter. Treat write as blind! (E.g. history table in TPC-C)

    //////// Create Write continuation:  

     write_continuation = [this, wcb](int status, query_result::QueryResult* result){
        //TODO: Does one need to use status? --> Query should not fail?
        
        //Create result object with rows affected = 1.
        result->set_rows_affected(1);
        wcb(REPLY_OK, result); 
    };

    //Write Table Version itself. //Only for kv-store.   
    //-- Note: If a TX issues many Inserts => we'll write table_version redundantly to write set -- However, these are all ignored by Prepare/Commit & filtered out in LockTxnKeys_scoped
    //Safe but wasteful (message bigger than need be + CC checks keys redundantly) // TODO: Improve: Either filter out during client sorting; or better: only when submitting a TXN for commit, add key per table write
        // WriteMessage *table_ver = txn->add_write_set();
        // table_ver->set_key(table_name);
        // table_ver->set_value("");


        
        //Read genesis timestamp (0) for key (if not a blind write)
        //==> FIXME: THIS IS CURRENTLY NOT HANDLED FULLY CORRECTLY IN EXISTING OCC CHECK.
            //If genesis EXISTS, then we should abort. But we won't currently because we read 0,0 as well.
            //This is a rare semantic corner case (if we try to insert something that was loaded)
            //FIX: In addition to giving this read genesis time, give it a bool "try_insert". 
                        //If set => abort if the last Write is Genesis.
                        //Ideally could set read time to -1, but it's an unsigned int...
        if(!is_blind_write){
            ReadMessage *read = txn->add_read_set();
            read->set_key(enc_key);
            read->mutable_readtime()->set_id(0);
            read->mutable_readtime()->set_timestamp(0);
        }
    
        //Create Table Write for key. Note: Enc_key encodes table_name + primary key column values.
        WriteMessage *write = txn->add_write_set();
        write->set_key(enc_key);
        // // for(int i=0; i<column_list.size(); ++i){
        // //     (*write->mutable_rowupdates()->mutable_attribute_writes())[column_list[i]] = value_list[i];
        // // }
        // for(auto &[col_name, col_idx]: col_registry_ptr->col_name_index){
        //     (*write->mutable_rowupdates()->mutable_attribute_writes())[col_name] = value_list[col_idx];
        // }

        //New version: 
        TableWrite *table_write = AddTableWrite(table_name, col_registry);
        table_write->set_changed_table(true); //Add Table Version.
        write->mutable_rowupdates()->set_row_idx(table_write->rows().size()); //set row_idx for proof reference

        RowUpdates *row_update = table_write->add_rows();
        *row_update->mutable_column_values() = {value_list.begin(), value_list.end()};
        row_update->set_write_set_idx(txn->write_set_size()-1);
        // for(auto &[col_name, col_idx]: col_registry_ptr->col_name_index){
        //     std::string *col_val = row_update->add_column_values();
        //     *col_val = std::move(value_list[col_idx]);
        // }

    //Write cont that takes in a result (from read)
    // write_continuation = [this, wcb, enc_key, table_name, col_registry_ptr = &col_registry, value_list](int status, query_result::QueryResult* result){
    //     //TODO: Does one need to use status? --> Query should not fail?
    //     if(result->empty()){

    //         //Write Table Version itself. //Only for kv-store.
    //         WriteMessage *table_ver = txn->add_write_set();
    //         table_ver->set_key(table_name);
    //         table_ver->set_value("");

            
    //         //Read genesis timestamp (0) for key ==> FIXME: THIS CURRENTLY DOES NOT WORK WITH EXISTING OCC CHECK.
    //         ReadMessage *read = txn->add_read_set();
    //         read->set_key(enc_key);
    //         read->mutable_readtime()->set_id(0);
    //         read->mutable_readtime()->set_timestamp(0);

    //         //Create Table Write for key. Note: Enc_key encodes table_name + primary key column values.
    //         WriteMessage *write = txn->add_write_set();
    //         write->set_key(enc_key);
    //         // // for(int i=0; i<column_list.size(); ++i){
    //         // //     (*write->mutable_rowupdates()->mutable_attribute_writes())[column_list[i]] = value_list[i];
    //         // // }
    //         // for(auto &[col_name, col_idx]: col_registry_ptr->col_name_index){
    //         //     (*write->mutable_rowupdates()->mutable_attribute_writes())[col_name] = value_list[col_idx];
    //         // }

    //         //New version: 
    //         TableWrite *table_write = AddTableWrite(table_name, *col_registry_ptr);
    //         write->mutable_rowupdates()->set_row_idx(table_write->rows().size()); //set row_idx for proof reference
    //         RowUpdates *row_update = table_write->add_rows();
    //         *row_update->mutable_column_values() = {value_list.begin(), value_list.end()};
    //         // for(auto &[col_name, col_idx]: col_registry_ptr->col_name_index){
    //         //     std::string *col_val = row_update->add_column_values();
    //         //     *col_val = std::move(value_list[col_idx]);
    //         // }

            
    //         //Create result object with rows affected = 1.
    //         result->set_rows_affected(1);
    //         wcb(REPLY_OK, result);
    //     }
    //     else{
    //         //Create result object with rows affected = 0.
    //         result->set_rows_affected(0);
    //         wcb(REPLY_OK, result);
    //     }
    // };

    return;

}


void SQLTransformer::TransformUpdate(size_t pos, std::string_view &write_statement,
    std::string &read_statement, std::function<void(int, query_result::QueryResult*)>  &write_continuation, write_callback &wcb){
    //Based on Syntax from: https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-update/ 

    //Case 2) UPDATE <table_name> SET {(column = value)} WHERE <col_name = condition>
    //-> Turn into read_statement: Result(column, column_value = rows(attributes))
    //             SELECT * FROM <table_name> (value_columns) WHERE <condition>         //Note "*" returns all columns for the rows. Use the primary key encoding to find primary columns
                                                                                    //Alternatively, would select only the columns required by the Values; and pass the primary columns in separately.
    //             write_cont: for (column = value) statement, create TableWrite with primary column encoded key, column_list, and column_values (or direct inputs)

    std::string table_name;
    std::map<std::string_view, Col_Update> col_updates;
    std::string_view where_cond;

    //1 Remove insert hook
    write_statement.remove_prefix(pos + update_hook.length()-1);  
    
    //2 Split on values
    pos = write_statement.find(set_hook);
    UW_ASSERT(pos != std::string::npos);
    
    //3) Extract table name
    table_name = std::move(static_cast<std::string>(write_statement.substr(0, pos)));
    
    //Skip ahead past "SET" hook
    //Everything from 0 - pos is "<table_name>". Everything from set_pos - where_pos is the content between Set and Where --> "SET <CONTENT> WHERE"
    size_t set_pos = pos + set_hook.length();
    write_statement.remove_prefix(set_pos);

    size_t where_pos = write_statement.find(where_hook);  
    UW_ASSERT(where_pos != std::string::npos); //TODO: Assuming here it has a Where hook. If not (i.e. update all rows), then ignore parsing it. (i.e. set where_pos to length of string, and skip where clause)

    std::string_view set_statement = write_statement.substr(0, where_pos);

    //std::cerr << "set_statement: " << set_statement << std::endl;
    
    // split on ", " to identify the updates.
    // for each string split again on "=" and insert into column and value lists
    size_t next_up;
    while((next_up = set_statement.find(", ")) != string::npos){ //Find next ", ", look only up to where hook.
        ParseColUpdate(set_statement.substr(0, next_up), col_updates);
        set_statement.remove_prefix(next_up + 2); //skip past ", "
    }
    //Note: After loop is done next_up == npos
    //process last set statement (only one remaining)
    ParseColUpdate(set_statement, col_updates);
    
   
    // isolate the Whole where condition and just re-use in the SELECT statement? -- can keep the Where hook.
    //Skip past "WHERE" hook
    //where_pos += where_hook.length();
    where_cond = write_statement.substr(where_pos);
        //If we want to isolate the where conditions:
        //split conditions on "AND" or "OR"
        //Within cond, split string on "=" --> extract cond column and cond value
   
    UW_ASSERT(col_updates.size() >= 1); // At least one column specified to be updated
        
    ///////// //Create Read statement:  Just Select * with Where condition
            //==> TODO: Ideally for Updates we'd just select on the primary key columns. (instead of a sql select * statement which is a bit overkill): But then we can't copy col values
            //std::vector<const std::string*> primary_key_column_values;
    // read_statement = "SELECT * FROM ";
    // read_statement += table_name;
    // read_statement += where_cond;  //Note: Where cond starts with a " " and ends with ";"

    where_cond = where_cond.substr(where_hook.length());
    read_statement = fmt::format("SELECT * FROM {0} WHERE {1}", table_name, std::move(where_cond));  //Note: Where cond ends with ";"
    //TODO: Currently TransformUpdate always does SELECT * in order to be able to copy the entire row
            //Eventually re-factor to only request the values that are being updated, and only write a delta
            //Store Tx-id/TS of the row that was read, and fetch it on demand serverside. Copy it and apply delta
            //Note: Need to ensure that row reference still exists. This is always the case currently, since we don't GC yet.
       
    Debug("Transformed read statement: %s", read_statement.c_str());
    // for(auto &col_up: col_updates){
    //     std::cerr << "col: " << col_up.first << " val: " << col_up.second.l_value << std::endl;
    // }
       
    //////// Create Write continuation:  
    //Note: Reading and copying all column values ("Select *"")
        //Could optimize to only read select columns, and supply read time TS to copy on demand at write time. (Note: This is tricky if the replica does not own the read version)
        // --> Procedure:
               // CreateTable Write entry with Timestamp and the column values to be updated. Note: Timestamp identifies the row from which no copy from -> i.e. the one thats updated.
               // Include additionally the read version of the row that was read. This is already part of the read set of the txn --> can be found by looking up latest query seq in the txn.
               // For each row in result, map primary key to the primary key in the query read set to identify the read version.
               // Note: if we want to support parallel writes (async) then we might need to identify the query explicitly (rather than just checking the latest query seq)
                                        
    
    write_continuation = [this, wcb, table_name, col_updates](int status, query_result::QueryResult* result) mutable {

        //std::cerr << "TEST WRITE CONT" << std::endl;
        Debug("Performing write_continuation"); //FIXME: Debug doesnt seem to be registered

        if(result->empty()){
            Debug("No rows to update");
            result->set_rows_affected(result->size()); 
            wcb(REPLY_OK, result);
            return;
        }
    
        Debug("Find TABLE: [%s] in Registry", table_name.c_str());
        auto itr = TableRegistry.find(table_name);
        UW_ASSERT(itr != TableRegistry.end());
        ColRegistry &col_registry = itr->second; //TableRegistry[table_name];

         //Write Table Version itself. //Only for kv-store. //FIXME: This is for coarse CC -- Update conflicts with ALL Selects on Table
        //TODO: Ideally UPDATE statement does not need to change TableVersion: It does not create or remove any rows. I.e. the Table "stays the same"
        //However, currently updates do need to change the TableVersion. Why? Because Updates change the Index, and the Index implicitly creates an "Active" Read Set for scans.
            //I.e. due to an update, a scan might miss reading a given row (this is indistinguishable for us as if the row as inserted later/deleted etc)
        // WriteMessage *table_ver = txn->add_write_set();
        // table_ver->set_key(table_name);
        // table_ver->set_value("");
        bool changed_table = false; // false //FOR NOW ALWAYS SETTING TO TRUE due to UPDATE INDEX issue (see above comment) TODO: Implement TableColumnVersion optimization

        
        //Write TableColVersions
        for(auto &[col, _]: col_updates){
            std::cerr << "Trying to set Table Col Version for col: " << col << std::endl;
            if(!col_registry.indexed_cols.count(col)) continue; //only set col version for indexed columns

            WriteMessage *write = txn->add_write_set();   
            write->set_key(table_name + unique_delimiter + std::string(col));  
            write->set_delay(true);
             //If a TX has multiple Queries with the same Col updates there will be duplicates. Does that matter? //Writes are sorted to avoid deadlock.
             //TODO: to avoid duplicates: store the idx of the cols accessed and write Version only later.
        }

        TableWrite *table_write = AddTableWrite(table_name, col_registry);

        Debug("Checking %d rows.", result->size());
        //For each row in query result
        for(int i = 0; i < result->size(); ++i){
            std::unique_ptr<query_result::Row> row = (*result)[i]; 
            Debug("Row: %d. Checking %d columns. ", i, row->num_columns());

            //Note: Enc key is based on pkey values, not col names!!!  -- if we want access to index; can store in primary key map too.
            std::vector<std::string> primary_key_column_values;
            
            WriteMessage *write = txn->add_write_set();
            write->mutable_rowupdates()->set_row_idx(table_write->rows().size()); //set row_idx for proof reference
            
            RowUpdates *row_update = AddTableWriteRow(table_write, col_registry); //row_update->mutable_column_values()->Resize(col_registry.col_names.size(), ""); Resize seems to not work for strings
            row_update->set_write_set_idx(txn->write_set_size()-1);
            //std::cerr << "Row size: " <<  row_update->mutable_column_values()->size() << std::endl;
          
            //TODO: Do this for UPDATE and DELETE too. //TODO: For Delete: Set all columns. Set values only for the columns we care about.
            bool update_primary_key = false;
            std::vector<std::string> backup_primary_key_column_values;

            // For col in col_updates update the columns specified by update_cols. Set value to update_values
            for(int j=0; j<row->num_columns(); ++j){
                const std::string &col = row->name(j);
               
                std::unique_ptr<query_result::Field> field = (*row)[j];
          
                //Deserialize encoding to be a stringified type (e.g. whether it's int/bool/string store all as normal readable string)
                //std::cerr << "Checking column: " << col << std::endl;
                const std::string &col_type = col_registry.col_name_type.at(col);
               
                //Currently we receive everything as plain-text string (as opposed to cereal). 
                //TODO: Thus we should update this code to skip Decoding unless necessary to do computation.
                auto field_val(DecodeType(field, col_type));
                //std::string field_val(DecodeType(field, col_registry.col_name_type[col]));


                std::cerr << "Checking column: " << col << " , with field " << std::visit(StringVisitor(), field_val) << std::endl;
                
               
                //Replace value with col value if applicable. Then operate arithmetic by casting ops to uint64_t and then turning back to string.
                //(*write->mutable_rowupdates()->mutable_attribute_writes())[col] = std::move(GetUpdateValue(col, field_val, field, col_updates));
                bool change_val = false;
                std::string set_val = GetUpdateValue(col, field_val, field, col_updates, col_type, change_val);

                if(change_val){
                     //std::cerr << "Checking column: " << col << " , with field " << std::visit(StringVisitor(), field_val) << std::endl;
                     std::cerr << "Updating col: " << col << " , new val: " << set_val << std::endl;
                } 
                //TODO: return bool if set_val is changed. In that case, record which columsn changed. and add a CC-store write entry per column updated.
               
                if(col_registry.primary_key_cols.count(col)){
                   
                    if(!update_primary_key && change_val){ //If primary key changes: Create TableWrite/WriteMsg for deletion of current row.
                        update_primary_key = true;
                        backup_primary_key_column_values = primary_key_column_values; //Copy map
                    } 

                     //Isolate primary keys ==> create encoding and table write
                    primary_key_column_values.emplace_back(set_val);
                   
                    if(update_primary_key){ //If primary key changes: Record current value too
                        backup_primary_key_column_values.emplace_back(std::visit(StringVisitor(), field_val));
                        
                    } 
                }
    
                (*row_update->mutable_column_values())[col_registry.col_name_index[col]] = std::move(set_val); //Realistically row columns will be in correct order. But in case they are not, must insert at the right position.
               
            }    
        
            std::string enc_key = EncodeTableRow(table_name, primary_key_column_values);
            
            //if(std::find(txn->write_set().begin(), txn->write_set().end(), enc_key) != txn->write_set().end()) Panic("duplicate write");
            // Notice("TODO: REMOVE WRITE SET DUPLICATE CHECK");
            // for(int i = 0; i < txn->write_set_size() - 1; ++i){
            //     //ignore last write -- has no key set yet.
            //     if(txn->write_set()[i].key() == enc_key) Panic("duplicate write: %s", enc_key.c_str());
            // }

            write->set_key(enc_key);

            if(update_primary_key){ //Also set Primary key we replace.
                write = txn->add_write_set();
                enc_key = EncodeTableRow(table_name, backup_primary_key_column_values);
                write->set_key(enc_key);
                write->set_value("d");
                write->mutable_rowupdates()->set_deletion(true);

                 row_update = AddTableWriteRow(table_write, col_registry);

                int i = 0;
                for(auto &[col_name, p_idx]: col_registry.primary_key_cols_idx){
                    (*row_update->mutable_column_values())[p_idx] = std::move(backup_primary_key_column_values[i++]);
                }
                row_update->set_deletion(true);

                
                Warning("Trying to update primary key value");
                bool changed_table = true;
            }
        }

        if(changed_table){ //Update resulted in row insertion + deletion => must update TableVersion
            //Write Table Version itself. //Only for kv-store. //FIXME: This is for coarse CC -- Update conflicts with ALL Selects on Table
            // WriteMessage *table_ver = txn->add_write_set();
            // table_ver->set_key(table_name);
            // table_ver->set_value("");
            table_write->set_changed_table(true);
        }





        Debug("Completed Write with %lu rows written", result->size());
        //std::cerr << "Completed Write with " << result->size() << " row(s) written" << std::endl;
        result->set_rows_affected(result->size()); 
        wcb(REPLY_OK, result);
        
    };
}


void SQLTransformer::TransformDelete(size_t pos, std::string_view &write_statement, 
    std::string &read_statement, std::function<void(int, query_result::QueryResult*)>  &write_continuation, write_callback &wcb, bool skip_query_interpretation){
    //Based on Syntax from: https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-delete/ 
    
     //Case 3) DELETE FROM <table_name> WHERE <condition>
         //-> Turn into read_statement: Result(column, column_value) SELECT * FROM <table_name>(primary_columns) 
         //             write_cont: for(row in result) create TableWrite with primary column encoded key, bool = delete (create new version with empty values/some meta data indicating delete)
         // TODO: Handle deleted versions: Create new version with special delete marker. NOTE: Read Sets of queries should include the empty version; but result computation should ignore it.
         // But how can one distinguish deleted versions from rows not yet created? Maybe one MUST have semantic CC to support new row inserts/row deletions.
                //--> Simple solution: change table version
    std::string table_name;
    
    std::string_view where_cond;

    //1 Remove insert hook
    write_statement.remove_prefix(pos + delete_hook.length()-1);
    
    //2 Split on values
    pos = write_statement.find(where_hook);
    UW_ASSERT(pos != std::string::npos);
    
    //3) Extract table name
    table_name = std::move(static_cast<std::string>(write_statement.substr(0, pos)));
    
    // isolate the Whole where condition and just re-use in the SELECT statement? -- can keep the Where hook.
    //Skip past "WHERE" hook
    size_t where_pos = pos + where_hook.length();
    where_cond = write_statement.substr(where_pos);

    Debug("Find TABLE: [%s] in Registry", table_name.c_str());
    auto itr = TableRegistry.find(table_name);
    UW_ASSERT(itr != TableRegistry.end());
    ColRegistry &col_registry = itr->second; //TableRegistry[table_name]; 


    //Check whether delete is for single key (point) or flexible amount of rows
   
    //std::map<std::string, std::string> p_col_values;  
    std::vector<std::string> p_col_values;  
    bool is_point_delete = CheckColConditions(where_cond, col_registry, p_col_values); 
   
    if(is_point_delete){
        //Add to write set.
        std::cerr << "IS POINT DELETE" << std::endl;

        //Write Table Version itself. //Only for kv-store.
        // WriteMessage *table_ver = txn->add_write_set();
        // table_ver->set_key(table_name);
        // table_ver->set_value("");

        //Note: Do not add to read set. Don't care if deletion doesn't work.

        //Add Delete also to Table Write : 
        TableWrite *table_write = AddTableWrite(table_name, col_registry);
        table_write->set_changed_table(true); //Add Table Version.

        WriteMessage *write = txn->add_write_set();
        write->mutable_rowupdates()->set_row_idx(table_write->rows().size()); //set row_idx for proof reference

        RowUpdates *row_update = AddTableWriteRow(table_write, col_registry);
        row_update->set_write_set_idx(txn->write_set_size()-1);
        row_update->set_deletion(true);

        std::string enc_key = EncodeTableRow(table_name, p_col_values);
        
        write->set_key(enc_key);
        write->set_value("d");
        write->mutable_rowupdates()->set_deletion(true);
    
        std::vector<const std::string*> primary_key_column_values;
        int col_idx = 0;
        for(auto &[col_name, _]: col_registry.primary_key_cols_idx){
            std::string &col_value = (*row_update->mutable_column_values())[col_registry.col_name_index[col_name]];
            col_value = std::move(p_col_values.at(col_idx++));
        }
        
        //Create a QueryResult -- set rows affected to 1.
        write_continuation = [this, wcb](int status, query_result::QueryResult* result){
            result->set_rows_affected(1); 
            wcb(REPLY_OK, result);
        };
        return;
        //Return
    }

    //Else: Is Query Delete
    skip_query_interpretation = true; //We already know we must do a scan. Do not Interpret Query again

    // read_statement = "SELECT * FROM ";  //Ideally select only primary column rows. To support this, need rows to allow access to columns by name (and not just index)

    //read_statement = "SELECT ";
    std::string col_statement;
    for(auto [col_name, idx]: col_registry.primary_key_cols_idx){
        //read_statement += col_name + ", "; 
        col_statement += col_name + ", ";
    }
    col_statement.resize(col_statement.length() - 2); // drop last ", "

    // read_statement.resize(read_statement.length() - 2); // drop last ", "
    // read_statement += " FROM ";  
    // read_statement += table_name;
    // read_statement += " WHERE ";
    // read_statement += where_cond; 
    // read_statement += ";"; // Add back ; --> was truncated by CheckColCond

    //use fmt::format to create more readable read_statement generation.

    //read_statement = fmt::format("SELECT {0} FROM {1} WHERE {2};", fmt::join(col_registry.primary_key_cols, ", "), table_name, std::move(where_cond));
    //Could use this --> but then result might be out of order --> would need to look up pcols by name

    read_statement = fmt::format("SELECT {0} FROM {1} WHERE {2};", std::move(col_statement), table_name, std::move(where_cond));
    
     //////// Create Write continuation:  
    write_continuation = [this, wcb, table_name, col_registry_ptr = &col_registry](int status, query_result::QueryResult* result){

         //Write Table Version itself. //Only for kv-store.
        // WriteMessage *table_ver = txn->add_write_set();
        // table_ver->set_key(table_name);
        // table_ver->set_value("");
        if(result->empty()){
            Debug("No rows to delete");
            result->set_rows_affected(result->size()); 
            wcb(REPLY_OK, result);
        }

         TableWrite *table_write = AddTableWrite(table_name, *col_registry_ptr);
         table_write->set_changed_table(true); //Add Table Version.

        //For each row in query result
        for(int i = 0; i < result->size(); ++i){
            std::unique_ptr<query_result::Row> row = (*result)[i];

            //Create TableWrite for Delete too.
            WriteMessage *write = txn->add_write_set();
            write->mutable_rowupdates()->set_row_idx(table_write->rows().size()); //set row_idx for proof reference
            RowUpdates *row_update = AddTableWriteRow(table_write, *col_registry_ptr);
            row_update->set_write_set_idx(txn->write_set_size()-1);
            row_update->set_deletion(true);

            std::vector<const std::string*> primary_key_column_values;
            for(int idx = 0; idx < row->num_columns(); ++idx){ //Note: Assume here that cols are in correct order of primary key cols.
            //for(auto [col_name, idx]: col_registry_ptr->primary_key_cols_idx){
                
                std::unique_ptr<query_result::Field> field = (*row)[idx];
                //Deserialize encoding to be a stringified type (e.g. whether it's int/bool/string store all as normal readable string)
                const std::string &col_name = field->name();
              
                if(col_registry_ptr->primary_key_cols_idx[idx].first != col_name){ Panic("Primary Columns out of order");}
                if(col_registry_ptr->primary_key_cols.find(col_name) == col_registry_ptr->primary_key_cols.end()){ Panic("Delete Read Result includes column that is not part of primary key");}

                auto field_val(DecodeType(field, col_registry_ptr->col_name_type[col_name]));
                //auto field_val(DecodeType(field, col_registry_ptr->col_name_type[col_name]));
                //primary_key_column_values.emplace_back(std::visit(StringVisitor(), field_val));


                std::string &col_value = (*row_update->mutable_column_values())[col_registry_ptr->col_name_index[col_name]]; //Alternatively: col_registry_ptr->primary_key_cols_idx[idx].first
                col_value = std::visit(StringVisitor(), field_val);

                primary_key_column_values.push_back(&col_value);
            }

            std::string enc_key = EncodeTableRow(table_name, primary_key_column_values);

            
            write->set_key(enc_key);
            write->set_value("d");
            write->mutable_rowupdates()->set_deletion(true);
        }

        result->set_rows_affected(result->size()); 
        wcb(REPLY_OK, result);
        
    };
}


//////////////////// Table Write Generator

static bool fine_grained_quotes = true;  //false == add quotes to everything, true == add quotes only to the fields that need it. (e.g. strings, bool; not int)
//fine_grained_quotes requires use of TableRegistry now. However, it seems to work fine for Peloton to add quotes to everything indiscriminately. 

//server calls: GenerateLoadStatement, into LoadTable (passing the statement)
std::string SQLTransformer::GenerateLoadStatement(const std::string &table_name, const std::vector<std::vector<std::string>> &row_segment, int segment_no){

    //std::cerr << "row_segment, size: " << row_segment.size() << std::endl;

    const ColRegistry &col_registry = TableRegistry.at(table_name);
    std::string load_statement = fmt::format("INSERT INTO {0} VALUES ", table_name);

    for(auto &row: row_segment){
        load_statement += "(";
        if(fine_grained_quotes){ // Use this to add fine grained quotes:
            for(int i = 0; i < row.size(); ++i){
                if(col_registry.col_quotes[i])  load_statement += "\'" + row[i]  + "\'" + ", ";
                else load_statement += row[i] + ", ";
            }
        }
        else{ //Otherwise, just add quotes to everything
            for(auto &col_val: row){
                load_statement += "\'" + col_val  + "\'" + ", ";
            }
        }
    
        load_statement.resize(load_statement.length()-2); //remove trailing ", "
        load_statement += "), ";
    }
    load_statement.resize(load_statement.length()-2); //remove trailing ", "
    load_statement += ";";

    Debug("Generate Load Statement for Table %s. Segment %d. Statement: %s", table_name.c_str(), segment_no, load_statement.substr(0, 1000).c_str());

    return load_statement;
}


//NOTE: Peloton does not support WHERE IN syntax for delete statements. => must use the generator version that creates separate delete statements.
//TODO: Turn into a bunch of (x=4 OR x=5 OR x=7 OR) AND (y=a OR y=b OR) .. statements...

void SQLTransformer::GenerateTableWriteStatement(std::string &write_statement, std::string &delete_statement, const std::string &table_name, const TableWrite &table_write){
   
//Turn Table Writes into Upsert and Delete statement:  ///https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-upsert/ 
    //Multi row Upsert:  https://stackoverflow.com/questions/40647600/postgresql-multi-value-upserts 
    // INSERT INTO table1 VALUES (1, 'foo'), (2,'bar'), (3,'baz')
    // ON CONFLICT (col1)
    // DO UPDATE SET col2 = EXCLUDED.col2;

    //Multi row Delete: https://www.commandprompt.com/education/how-to-delete-multiple-rows-from-a-table-in-postgresql/ 
    // DELETE FROM article_details
    // WHERE article_id IN (2, 4, 7);

    const ColRegistry &col_registry = TableRegistry.at(table_name);

    //NOTE: Inserts must always insert -- even if value exists ==> Insert new row.
    write_statement = fmt::format("INSERT INTO {0} VALUES ", table_name);

    // In Write statement: for rows that are marked as delete: don't insert. --> split into write_statement and delete_statement.
    std::map<std::string, std::vector<std::string>> delete_conds;

    bool has_write = false;
    bool has_delete = false;

    for(auto &row: table_write.rows()){
        //Alternatively: Move row contents to a vector and use: fmt::join(vec, ",")
        if(row.has_deletion() && row.deletion()){

            for(auto &[col_name, p_idx]: col_registry.primary_key_cols_idx){
                if(fine_grained_quotes){
                    if(col_registry.col_quotes[p_idx]) delete_conds[col_name].push_back("\'" + row.column_values()[p_idx] + "\'");
                    else delete_conds[col_name].push_back(row.column_values()[p_idx] );
                }
                else{
                    delete_conds[col_name].push_back("\'" + row.column_values()[p_idx] + "\'");
                }
            }
        }
        else{
            has_write = true;
            write_statement += "(";
            if(fine_grained_quotes){ // Use this to add fine grained quotes:
                for(int i = 0; i < row.column_values_size(); ++i){
                    if(col_registry.col_quotes[i])  write_statement += "\'" + row.column_values()[i]  + "\'" + ", ";
                    else write_statement += row.column_values()[i] + ", ";
                }
            }
            else{
                for(auto &col_val: row.column_values()){
                    write_statement += "\'" + col_val  + "\'" + ", ";
                }
            }
        
            write_statement.resize(write_statement.length()-2); //remove trailing ", "
            write_statement += "), ";
        }
      

        //write_statement += fmt::format("{}, ", fmt::join(row.column_values(), ','));
    }
    write_statement.resize(write_statement.length()-2); //remove trailing ", "

    //TODO: If we are hacking Insert to always insert --> then can remove this block 
    if(false){
         write_statement += " ON CONFLICT (";
        for(auto &[col_name, _]: col_registry.primary_key_cols_idx){
            write_statement += col_name + ", ";
        }
        write_statement.resize(write_statement.length()-2); //remove trailing ", "

        write_statement += ") VALUES DO UPDATE SET "; //TODO: loop, replace with index
        for(auto &col: col_registry.col_names){
            write_statement += fmt::format("{0} = EXCLUDED.{0}, ", col);
        }
        write_statement.resize(write_statement.length()-2); //remove trailing ", "

    }
   
    write_statement += ";";

    if(!has_write) write_statement = "";
    //std::cerr << "delete_conds size: " << delete_conds[0].size() << std::endl;
    if(delete_conds.empty()) return;  //i.e. !has_delete

    //Else: Construct also a delete statement

    //NOTE: Deletes must always delete -- even if no value exists ==> Insert empty row.

    delete_statement = fmt::format("DELETE FROM {0} WHERE ", table_name);
    for(auto &[col_name, p_idx]: col_registry.primary_key_cols_idx){
        delete_statement += fmt::format("{0} in ({1}) AND ", col_name, fmt::join(delete_conds[col_name], ", "));
    }
    delete_statement.resize(delete_statement.length()-5); //Remove trailing " AND "
    delete_statement += ";";

    //FIXME: If no delete clauses --> should not delete anything
    return;  //I.e. there is delete conds.

}

void SQLTransformer::GenerateTableWriteStatement(std::string &write_statement, std::vector<std::string> &delete_statements, const std::string &table_name, const TableWrite &table_write){
    //Create one joint WriteStatement for all insertions
        //NOTE: Inserts must always insert -- even if value exists ==> Insert new row.
    //Create separate Delete statements for each delete

    // write_statement = fmt::format("INSERT INTO {0} VALUES ", table_name);

    const ColRegistry &col_registry = TableRegistry.at(table_name);

    for(auto &row: table_write.rows()){
        
        if(row.has_deletion() && row.deletion()){
            delete_statements.push_back("");
            std::string &delete_statement = delete_statements.back();
            delete_statement = fmt::format("DELETE FROM {0} WHERE ", table_name);

            for(auto &[col_name, p_idx]: col_registry.primary_key_cols_idx){
                //delete_statement += fmt::format("{0}={1} AND ", col_name, row.column_values()[p_idx]);
                //delete_statement += col_name + "=" + row.column_values()[p_idx] + " AND ";

                if(fine_grained_quotes){
                    if(col_registry.col_quotes[p_idx]) delete_statement += col_name + "=" + "\'" + row.column_values()[p_idx] + "\'" + " AND ";
                    else delete_statement += col_name + "=" + row.column_values()[p_idx] + " AND ";
                }
                else{
                    delete_statement += col_name + "=" + "\'" + row.column_values()[p_idx] + "\'" + " AND ";
                }
            }

            delete_statement.resize(delete_statement.length()-5); //Remove trailing " AND "
            delete_statement += ";";
        }
        else{
            Debug("Table[%s]: insert row with pkey: ", table_name.c_str());
           
            write_statement += "(";
            UW_ASSERT(row.column_values_size() == col_registry.col_names.size());
            if(fine_grained_quotes){ // Use this to add fine grained quotes:
                for(int i = 0; i < row.column_values_size(); ++i){
                     if(col_registry.primary_key_cols.count(col_registry.col_names[i])) Debug("Table[%s][%d]:  %s", table_name.c_str(), i, row.column_values()[i].c_str());
                    if(col_registry.col_quotes[i])  write_statement += "\'" + row.column_values()[i]  + "\'" + ", ";
                    else write_statement += row.column_values()[i] + ", ";
                }
            }
            else{
                for(auto &col_val: row.column_values()){
                    write_statement += "\'" + col_val  + "\'" + ", ";
                }
            }
        
            write_statement.resize(write_statement.length()-2); //remove trailing ", "
            write_statement += "), ";
        }
      

        //write_statement += fmt::format("{}, ", fmt::join(row.column_values(), ','));
    }
    if(!write_statement.empty()){
        write_statement.resize(write_statement.length()-2); //remove trailing ", "
        write_statement = fmt::format("INSERT INTO {0} VALUES ", table_name) + write_statement;

        //TODO: If we are hacking Insert to always insert --> then can remove this block 
        if(false){
            write_statement += " ON CONFLICT (";
            for(auto &[col_name, _]: col_registry.primary_key_cols_idx){
                write_statement += col_name + ", ";
            }
            write_statement.resize(write_statement.length()-2); //remove trailing ", "

            write_statement += ") VALUES DO UPDATE SET "; //TODO: loop, replace with index
            for(auto &col: col_registry.col_names){
                write_statement += fmt::format("{0} = EXCLUDED.{0}, ", col);
            }
            write_statement.resize(write_statement.length()-2); //remove trailing ", "

        }
    
        write_statement += ";";
    }
    
    return;  
}

void SQLTransformer::GenerateTablePurgeStatement(std::string &purge_statement, const std::string &table_name, const TableWrite &table_write){
    const ColRegistry &col_registry = TableRegistry.at(table_name);

    if(table_write.rows().empty()){
        purge_statement = "";
        return;
    } 
    //NOTE: Inserts must always insert -- even if value exists ==> Insert new row.
    purge_statement = fmt::format("INSERT INTO {0} VALUES ", table_name);
    
    for(auto &row: table_write.rows()){
        //Alternatively: Move row contents to a vector and use: fmt::join(vec, ",")

        purge_statement += "(";
        if(fine_grained_quotes){ // Use this to add fine grained quotes:
            for(int i = 0; i < row.column_values_size(); ++i){
                if(col_registry.col_quotes[i])  purge_statement += "\'" + row.column_values()[i]  + "\'" + ", ";
                else purge_statement += row.column_values()[i] + ", ";
            }
        }
        else{
            for(auto &col_val: row.column_values()){
                purge_statement += "\'" + col_val  + "\'" + ", ";
            }
        }
    
        purge_statement.resize(purge_statement.length()-2); //remove trailing ", "
        purge_statement += "), ";
      

        //purge_statement += fmt::format("{}, ", fmt::join(row.column_values(), ','));
    }
    purge_statement.resize(purge_statement.length()-2); //remove trailing ", "
   
    purge_statement += ";";
}

//Deprecated
void SQLTransformer::GenerateTablePurgeStatement_DEPRECATED(std::string &purge_statement, const std::string &table_name, const TableWrite &table_write){
    //Abort all TableWrites: Previous writes must be deleted; Previous deletes must be un-done
    //Puts all write and deletes into one purge

    const ColRegistry &col_registry = TableRegistry.at(table_name);
    std::map<std::string, std::vector<std::string>> purge_conds;

    for(auto &row: table_write.rows()){
        //Alternatively: Move row contents to a vector and use: fmt::join(vec, ",")
        for(auto &[col_name, p_idx]: col_registry.primary_key_cols_idx){
            if(fine_grained_quotes){
                if(col_registry.col_quotes[p_idx]) purge_conds[col_name].push_back("\'" + row.column_values()[p_idx] + "\'");
                else purge_conds[col_name].push_back(row.column_values()[p_idx] );
            }
            else{
                purge_conds[col_name].push_back("\'" + row.column_values()[p_idx] + "\'");
            }
        }
        //write_statement += fmt::format("{}, ", fmt::join(row.column_values(), ','));
    }

    if(purge_conds.empty()) return;

    purge_statement = fmt::format("DELETE FROM {0} WHERE ", table_name);
    for(auto &[col_name, p_idx]: col_registry.primary_key_cols_idx){
        purge_statement += fmt::format("{0} in ({1}) AND ", col_name, fmt::join(purge_conds[col_name], ", "));
    }
    purge_statement.resize(purge_statement.length()-5); //Remove trailing " AND "
    purge_statement += ";";

    return; 
}


//Deprecated
void SQLTransformer::GenerateTablePurgeStatement_DEPRECATED(std::vector<std::string> &purge_statements, const std::string &table_name, const TableWrite &table_write){

    const ColRegistry &col_registry = TableRegistry.at(table_name);

    for(auto &row: table_write.rows()){
         //generate a purge statement per row.
        purge_statements.push_back("");
        std::string &purge_statement = purge_statements.back();
        purge_statement = fmt::format("DELETE FROM {0} WHERE ", table_name);

        for(auto &[col_name, p_idx]: col_registry.primary_key_cols_idx){
            //purge_statement += fmt::format("{0}={1} AND ", col_name, row.column_values()[p_idx]);
            //purge_statement += col_name + "=" + row.column_values()[p_idx] + " AND ";

            if(fine_grained_quotes){
                if(col_registry.col_quotes[p_idx]) purge_statement += col_name + "=" + "\'" + row.column_values()[p_idx] + "\'" + " AND ";
                else purge_statement += col_name + "=" + row.column_values()[p_idx] + " AND ";
            }
            else{
                purge_statement += col_name + "=" + "\'" + row.column_values()[p_idx] + "\'" + " AND ";
            }
        }

        purge_statement.resize(purge_statement.length()-5); //Remove trailing " AND "
        purge_statement += ";";
    }

    return;
    //TODO: update GenerateDelete Statements too.
    //TODO: update interface to use this function. Loop over deletes and purges.
        
}



//////////////////// OLD: Without TableRegistry


// //Note: If we turn table writes into a sql statement, then don't actually need to decode type into anything else than a string... 
//             //could just send statement (with replaced columnvalue) and skip performing arithmetic ourselves. 
//             //(Note: this makes it harder to perform proofs for point reads, because the reader would need to perform the arithmetic ad hoc)
// bool SQLTransformer::GenerateTableWriteStatement(std::string &write_statement, std::string &delete_statement, const std::string &table_name, const TableWrite &table_write){


// //Turn Table Writes into Upsert and Delete statement:  ///https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-upsert/ 
//     //Multi row Upsert:  https://stackoverflow.com/questions/40647600/postgresql-multi-value-upserts 
//     // INSERT INTO table1 VALUES (1, 'foo'), (2,'bar'), (3,'baz')
//     // ON CONFLICT (col1)
//     // DO UPDATE SET col2 = EXCLUDED.col2;

//     //Multi row Delete: https://www.commandprompt.com/education/how-to-delete-multiple-rows-from-a-table-in-postgresql/ 
//     // DELETE FROM article_details
//     // WHERE article_id IN (2, 4, 7);

//     write_statement = fmt::format("INSERT INTO {0} VALUES ", table_name);

//     std::vector<std::vector<std::string>> delete_conds(table_write.col_primary_idx_size());

//     for(auto &row: table_write.rows()){
//         //Alternatively: Move row contents to a vector and use: fmt::join(vec, ",")
//         if(row.deletion()){
//             for(int i = 0; i<table_write.col_primary_idx_size(); ++i){
//                 delete_conds[i].push_back("\'" + row.column_values()[table_write.col_primary_idx()[i]] + "\'");
//             }
//         }
//         else{
//             write_statement += "(";
//             for(auto &col_val: row.column_values()){
//                 write_statement += "\'" + col_val  + "\'" + ", ";
//             }
//             write_statement.resize(write_statement.length()-2); //remove trailing ", "
//             write_statement += "), ";
//         }
      

//         //write_statement += fmt::format("{}, ", fmt::join(row.column_values(), ','));
//     }
//     write_statement.resize(write_statement.length()-2); //remove trailing ", "

//     //TODO: If we are hacking Insert to always insert --> then can remove this block 
//     if(true){
//          write_statement += " ON CONFLICT (";
//         for(auto &p_idx: table_write.col_primary_idx()){
//             write_statement += table_write.column_names()[p_idx] + ", ";
//         }
//         write_statement.resize(write_statement.length()-2); //remove trailing ", "

//         write_statement += ") VALUES DO UPDATE SET "; //TODO: loop, replace with index
//         for(auto &col: table_write.column_names()){
//             write_statement += fmt::format("{0} = EXCLUDED.{0}, ", col);
//         }
//         write_statement.resize(write_statement.length()-2); //remove trailing ", "

//     }
   
//     write_statement += ";";

//     //std::cerr << "delete_conds size: " << delete_conds[0].size() << std::endl;
//     if(delete_conds[0].empty()) return false;

//     //Else: Construct also a delete statement

//     delete_statement = fmt::format("DELETE FROM {0} WHERE ", table_name);
//     for(int i = 0; i<table_write.col_primary_idx_size(); ++i){
//         delete_statement += fmt::format("{0} in ({1}) AND ", table_write.column_names()[table_write.col_primary_idx()[i]], fmt::join(delete_conds[i], ", "));
//     }
//     delete_statement.resize(delete_statement.length()-5); //Remove trailing " AND "
//     delete_statement += ";";

   
//     return true;  //I.e. there is delete conds.

// }

// bool SQLTransformer::GenerateTablePurgeStatement(std::string &purge_statement, const std::string &table_name, const TableWrite &table_write){
   
//     std::vector<std::vector<std::string>> delete_conds(table_write.col_primary_idx_size());

//     for(auto &row: table_write.rows()){
//             //Alternatively: Move row contents to a vector and use: fmt::join(vec, ",")
//             if(!row.deletion()){
//                 for(int i = 0; i<table_write.col_primary_idx_size(); ++i){
//                     delete_conds[i].push_back("\'" + row.column_values()[table_write.col_primary_idx()[i]] + "\'");
//                 }
//             }
//     }
    
//     if(delete_conds[0].empty()) return false;
    
//     purge_statement = fmt::format("DELETE FROM {0} WHERE ", table_name);
//     for(int i = 0; i<table_write.col_primary_idx_size(); ++i){
//         purge_statement += fmt::format("{0} in ({1}) AND ", table_write.column_names()[table_write.col_primary_idx()[i]], fmt::join(delete_conds[i], ", "));
//     }
//     purge_statement.resize(purge_statement.length()-5); //Remove trailing " AND "
//     purge_statement += ";";

//     return true; //I.e. there is delete conds.
   
// }




//////////////////// Helper functions:


static std::string eq_hook = " = ";
void SQLTransformer::ParseColUpdate(std::string_view col_update, std::map<std::string_view, Col_Update> &col_updates){

    //split on "=" into col and update
        size_t pos = col_update.find(eq_hook);
        UW_ASSERT(pos != std::string::npos);

        //Then parse Value based on operands.
        Col_Update &val = col_updates[col_update.substr(0, pos)]; // col_updates[std::move(static_cast<std::string>(col_update.substr(0, pos)))];
        col_update.remove_prefix(pos + eq_hook.length());

        // find val.  //TODO: Add support for nesting if necessary.
        pos = col_update.find_first_of("+-*/");
        //pos = col_update.find(" ");
        if(pos == std::string::npos){  //is string statement
            val.l_value = col_update; //std::move(static_cast<std::string>(col_update));
            val.has_operand = false;
        }
        else{
            val.has_operand = true;
            //Parse operand; //Assuming here it is of simple form:  x <operand> y  and operand = {+, -, *, /}   Note: Assuming all values are Integers. For "/" will cast to float.
            val.l_value = col_update.substr(0, pos-1);//std::move(static_cast<std::string>(col_update.substr(0, pos-1)));
            val.operand = col_update.substr(pos, 1);//std::move(static_cast<std::string>(col_update.substr(pos, 1)));
            val.r_value = col_update.substr(pos+2);//std::move(static_cast<std::string>(col_update.substr(pos+2)));
        }
}

//TODO: Support double too.
std::string SQLTransformer::GetUpdateValue(const std::string &col, std::variant<bool, int64_t, double, std::string> &field_val, std::unique_ptr<query_result::Field> &field, 
    std::map<std::string_view, Col_Update> &col_updates, const std::string &col_type, bool &change_val){

     //Copy all column values (unless in col_updates)
            //Replace update value with read col value if applicable. 
            //Then operate arithmetic by casting ops to uint64_t and then turning back to string.

    auto itr = col_updates.find(col);
    if(itr == col_updates.end()){ //No update for the column --> just copy
        return std::visit(StringVisitor(), field_val); 
    }
    else{ //There is update for the column -> assign new value: possibly replace col:
        change_val = true;
        //std::cerr << "replacing col: " << col << std::endl;
        //Update value.
        Col_Update &col_update = itr->second;
        if(col_update.has_operand){
            //std::cerr << "update has operand: " << col_update.operand << std::endl;
                //std::cerr << "lvalue = " << col_update.l_value << std::endl;
            int64_t l_value;
            int64_t r_value;
            // Check if l_value needs to be replaced
            //For now just check current col name (field->name()) -- I assume that's always the case tbh.. //FIXME: Ideally search arbitrary Col by name...
            if(col_update.l_value == field->name()){
                try {
                    l_value = std::get<int64_t>(field_val);
                }
                catch (std::bad_variant_access&) {
                    Panic("Could not extract numeric type (int64_t) from field_val variant");
                }

                // std::istringstream iss(field_val);
                // iss.exceptions(std::ios::failbit | std::ios::badbit);
                // iss >> l_value;  
                //std::cerr << "lvalue = " << l_value << std::endl;
            }
            else{ //Otherwise l_value is already the number...
                std::istringstream iss(static_cast<std::string>(col_update.l_value));
                iss >> l_value;  
            }

            if(col_update.r_value == field->name()){
                try {
                    r_value = std::get<int64_t>(field_val);
                }
                catch (std::bad_variant_access&) {
                    Panic("Could not extract numeric type (int64_t) from field_val variant");
                }
                // std::istringstream iss(field_val);
                // iss.exceptions(std::ios::failbit | std::ios::badbit);
                // iss >> r_value;  
            }
            else{ //Otherwise l_value is already the number...
                std::istringstream iss(static_cast<std::string>(col_update.r_value));
                iss >> r_value;  
            }

            int64_t output;
            if(col_update.operand == "+"){
                output = l_value + r_value;
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
            
            //std::cerr << "output = " << output << std::endl;
           return std::to_string(output);
        }
        else{
            //Replace col value if it's a placeholder:  ///FIXME: Currently this just takes the same col name, i.e. it keeps the value the same... NOTE: Should never be triggered
            if(col_update.l_value == field->name()){
                Panic("Placeholder should not be existing column value");
                return std::visit(StringVisitor(), field_val);
            }
            else{
                return static_cast<std::string>(std::move(TrimValueByType(col_update.l_value, col_type)));
                // if(field_val.index() == 2){ //For now assuming numerics and bools are not quoted in Update Set Statements, but only strings are. Only trim if string..
                //     std::cerr << "Trimming: " << col_update.l_value << std::endl; 
                //     return static_cast<std::string>(std::move(TrimValue(col_update.l_value, true)));
                // }
                // else{
                //     return static_cast<std::string>(std::move(col_update.l_value));
                // }
            }
        }
    }
    
}

TableWrite* SQLTransformer::AddTableWrite(const std::string &table_name, const ColRegistry &col_registry){
    TableWrite &table_write = (*txn->mutable_table_writes())[table_name];

    // if(table_write.column_names().empty()){
    //     *table_write.mutable_column_names() = {col_registry.col_names.begin(), col_registry.col_names.end()}; //set columns if first TableWrite
    //     *table_write.mutable_col_primary_idx() = {col_registry.primary_col_idx.begin(), col_registry.primary_col_idx.end()}; //set columns if first TableWrite
    // } 
   
   return &table_write;
}

RowUpdates* SQLTransformer::AddTableWriteRow(TableWrite *table_write, const ColRegistry &col_registry){
   
    
    RowUpdates *row_update = table_write->add_rows();
   
    //std::vector<std::string *> col_vals;
    for(int q=0; q<col_registry.col_names.size(); ++q){
        row_update->add_column_values();
        //col_vals.push_back(row_update->add_column_values());
    }
    //std::cerr << "Row size: " <<  row_update->mutable_column_values()->size() << std::endl;
    
    return row_update;
}

std::variant<bool, int64_t, double, std::string> DecodeType(std::unique_ptr<query_result::Field> &field, const std::string &col_type){
    const std::string &field_val = field->get();
    return DecodeType(field_val, col_type);
}

// std::variant<bool, int64_t, std::string> DecodeType(std::unique_ptr<query_result::Field> &field, const std::string &col_type){
//     size_t nbytes;
//     const char* field_val_char = field->get(&nbytes);
//     std::string field_val(field_val_char, nbytes);
//     return DecodeType(field_val, col_type);
// }

 //  out = field->get(&nbytes);
                // std::string p_output(out, nbytes);
                //  std::stringstream p_ss(std::ios::in | std::ios::out | std::ios::binary);
                // p_ss << p_output;
                // output_row;
                // {
                // cereal::BinaryInputArchive iarchive(p_ss); // Create an input archive
                // iarchive(output_row); // Read the data from the archive
                // }
                // std::cerr << "Query Result. Col " << i << ": " << output_row << std::endl;


std::variant<bool, int64_t, double, std::string> DecodeType(const std::string &enc_value, const std::string &col_type){
    //Based on Syntax from: https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-data-types/ && https://www.postgresql.org/docs/current/datatype-numeric.html 
    //Resource for std::variant: https://www.cppstories.com/2018/06/variant/ 
   
    //Note: currently the generated types are PostGresSQL types. We could however also input "normal types" and transform them into SQL types only for Peloton.

    std::variant<bool, int64_t, double, std::string> type_variant;   //TODO: can pass variant to cereal? Then don't need all the redundant code

    //match on col_type
    if(col_type == "VARCHAR" || col_type == "TEXT"){ //FIXME: VARCHAR might actually look like "VARCHAR (n)"
        type_variant = enc_value;
    }
    // else if(col_type == "TEXT []"){
    //     std::vector<std::string> dec_value;
    //     DeCerealize(enc_value, dec_value);
    //     type_variant = std::move(dec_value);
    // }
    else if(col_type == "INTEGER" || col_type == "INT" || col_type == "BIGINT" || col_type == "SMALLINT"){ // SMALLINT (2byteS) INT (4), BIGINT (8), DOUBLE () 
        //int64_t dec_value;  //FIXME: Peloton encodes everything as string currently. So must DeCerialize as string and only then convert.
       
        int64_t dec = std::stol(enc_value); 
        type_variant = std::move(dec);

    }
    else if (col_type == "FLOAT"){
        int64_t dec = std::stod(enc_value); 
        type_variant = std::move(dec);
    }
    // else if(col_type == "INT []" || col_type == "BIGINT []" || col_type == "SMALLINT []"){
    //     std::vector<int64_t> dec_value;
    //     DeCerealize(enc_value, dec_value);
    //     type_variant = std::move(dec_value);
    // }
    else if(col_type == "BOOL" || col_type == "BOOLEAN"){

        bool dec; //FIXME: Peloton encodes everything as string currently. So must DeCerialize as string and only then convert.
        istringstream(enc_value) >> dec;
        type_variant = std::move(dec);
    }
    else{
        Panic("Unsupported type: %s. Currently only supporting {VARCHAR, TEXT, INT/INTEGER, BIGINT, SMALLINT, BOOL/BOOLEAN}", col_type.c_str());
        //e.g. // TIMESTAMP (time and date) or Array []

        //Note: Array will never be primary key, so realistically any update to array won't need to be translated to string, but could be encoded as cereal.
        //Note: we don't seem to need Array type... --> Auctionmark only uses it for arguments.
    }

    //std::cerr << "Decoded type" << std::endl;
    return type_variant;  //Use decoding..
}

//DEPRECATED: No longer using DeCerialize
std::variant<bool, int64_t, double, std::string> DecodeType(std::string &enc_value, const std::string &col_type){
    //Based on Syntax from: https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-data-types/ && https://www.postgresql.org/docs/current/datatype-numeric.html 
    //Resource for std::variant: https://www.cppstories.com/2018/06/variant/ 
   
    //Note: currently the generated types are PostGresSQL types. We could however also input "normal types" and transform them into SQL types only for Peloton.

    std::variant<bool, int64_t, double, std::string> type_variant;   //TODO: can pass variant to cereal? Then don't need all the redundant code

    //match on col_type
    if(col_type == "VARCHAR" || col_type == "TEXT"){ //FIXME: VARCHAR might actually look like "VARCHAR (n)"
        std::string dec_value;
        DeCerealize(enc_value, dec_value);
        type_variant = std::move(dec_value);
    }
    // else if(col_type == "TEXT []"){
    //     std::vector<std::string> dec_value;
    //     DeCerealize(enc_value, dec_value);
    //     type_variant = std::move(dec_value);
    // }
    else if(col_type == "INT" || col_type == "BIGINT" || col_type == "SMALLINT"){ // SMALLINT (2byteS) INT (4), BIGINT (8), DOUBLE () 
        //int64_t dec_value;  //FIXME: Peloton encodes everything as string currently. So must DeCerialize as string and only then convert.
        std::string dec_value;
        DeCerealize(enc_value, dec_value);
        //std::cerr << "DEC VALUE: " << dec_value << std::endl; 

        int64_t dec = std::stoi(dec_value); 
        type_variant = std::move(dec);

    }
    else if (col_type == "FLOAT"){
        std::string dec_value;
        DeCerealize(enc_value, dec_value);
        //std::cerr << "DEC VALUE: " << dec_value << std::endl; 

        double dec = std::stod(dec_value); 
        type_variant = std::move(dec);
    }
    // else if(col_type == "INT []" || col_type == "BIGINT []" || col_type == "SMALLINT []"){
    //     std::vector<int64_t> dec_value;
    //     DeCerealize(enc_value, dec_value);
    //     type_variant = std::move(dec_value);
    // }
    else if(col_type == "BOOL"){

       
         std::string dec_value;
        bool dec; //FIXME: Peloton encodes everything as string currently. So must DeCerialize as string and only then convert.
        DeCerealize(enc_value, dec_value);
        
        istringstream(dec_value) >> dec;
        type_variant = std::move(dec);
    }
    else{
        Panic("Types other than {VARCHAR, INT, BIGINT, SMALLINT, BOOL} Currently not supported");
        //e.g. // TIMESTAMP (time and date) or Array []

        //Note: Array will never be primary key, so realistically any update to array won't need to be translated to string, but could be encoded as cereal.
        //Note: we don't seem to need Array type... --> Auctionmark only uses it for arguments.
    }

    return type_variant;  //Use decoding..
}

  
/////////////////////////////// Read parser:

bool set_in_map(std::map<std::string, std::string> const &lhs, std::set<std::string> const &rhs){
    //for(auto)
    auto first1 = lhs.begin();
    auto last1 = lhs.end();
    auto first2 = rhs.begin();
    auto last2 = rhs.end();
    for (; first2 != last2; ++first1)
    {
        if (first1 == last1 || *first2 < first1->first)
            return false;
        if ( !(first1->first < *first2) )
            ++first2;
    }
    return true;
}


//Note: input (cond_statement) contains everything following "WHERE" keyword
//Returns true if cond_statement can be issued as Point Operation. P_col_value contains all extracted primary key col values (without quotes)
bool SQLTransformer::CheckColConditions(std::string_view &cond_statement, std::string &table_name, std::vector<std::string> &p_col_values, bool relax){
    //Using Syntax from: https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-where/ ; https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-order-by/ 
    
    Debug("Find TABLE: [%s] in Registry", table_name.c_str());
    auto itr = TableRegistry.find(table_name);
    UW_ASSERT(itr != TableRegistry.end());
    const ColRegistry &col_registry = itr->second; //TableRegistry[table_name]; 

    return CheckColConditions(cond_statement, col_registry, p_col_values, relax);
}


bool SQLTransformer::CheckColConditions(std::string_view &cond_statement, const ColRegistry &col_registry, std::vector<std::string> &p_col_values, bool relax){

    size_t pos;

    //Check for Nested queries --> Cannot be point read
    if((pos = cond_statement.find(select_hook)) != std::string::npos) return false; 

    //Remove Order By condition for parsing
    pos = cond_statement.find(order_hook);
    if(pos != std::string::npos){
        cond_statement.remove_suffix(cond_statement.length()-pos);
    }
    else {  //Remove last ";" (if not already removed)
        pos = cond_statement.find(";");
        if(pos != std::string::npos){
            cond_statement.remove_suffix(cond_statement.length()-pos);
        }
    }

    bool terminate_early = false;
    size_t end = 0;

    std::map<std::string, std::string> p_col_value;
    if(!CheckColConditions(end, cond_statement, col_registry, p_col_value, terminate_early, relax)) return false;

    // for(auto &[key, val]: p_col_value){
    //     std::cerr << "extracted cols: " << key << ". val: " << val << std::endl;
    // }

    //Else: Transform map into p_col_values only (in correct order); extract just the primary key cols.
    for(auto &[col_name, idx]: col_registry.primary_key_cols_idx){
        std::cerr << "extracted p_ cols: " << col_name << std::endl;
        p_col_values.push_back(std::move(p_col_value.at(col_name)));
    }
   

    return true; // is_point

     //Return true if can definitely do a point key lookup
    //E.g. if pkey = col1
    //  Where col1 = 5 OR col2 = 6 ==> False
    // Where col1 = 5 AND col2 = 6 ==> True   // but even more restrictive -- value of point read might want to be empty ;; but read set must hold the key
    // Where col1 = 5 ==> True

    //Harder cases:
    // e.g. pkey = (col1, col2)
    // Where (col1 = 5 AND col2 = 5) OR col3 = 5 ==> False
    // Where (col1 = 5 AND col2 = 5) OR (col1 = 8 AND col2 = 9) ==> False  //TODO: Could do point read for both sides... // For now treat as query.

    //Condition most basic: All bools need to be pcol and be bound by AND
        // any more --> false
        // anything more fancy --> false
        
     //Condition smart:
    //Split on weakest binding recursively.
    // Try to join in order of strongest binding: for AND --> merge both subsets; for OR, propagate up both subsets
    // Return true at highest layer if both subsets bubbled up have all primary key cols (i.e are of size primary key cols)
}


//TODO: check that column type is primitive: If its array or Timestamp --> defer it to query engine.

//Note: Don't pass cond_statement by reference inside recursion.
bool SQLTransformer::CheckColConditions(size_t &end, std::string_view cond_statement, const ColRegistry &col_registry, std::map<std::string, std::string> &p_col_value, bool &terminate_early, bool relax){
    //Note: If relax = true ==> primary_key_compare checks for subset, if false ==> checks for equality.

    //std::cerr << "view cond statement: " << cond_statement << std::endl;

    std::map<std::string, std::string> &left_p_col_value = p_col_value;
    std::map<std::string, std::string> right_p_col_value;

    size_t pos = 0;
    pos = cond_statement.find_first_of("()");

    //Base case:  
    if(pos == std::string_view::npos || (pos > 0 && cond_statement.substr(pos, 1) == "(")){   //Either a) no () exist, or b) they are to the right of the first operator
        //split on operators if existent. --> apply col merge left to right. //Note: Multi statements without () are left binding by default.
        op_t op_type(SQL_START);
        op_t next_op_type;
        size_t op_pos; 
        size_t op_pos_post = 0; 

        while(op_type != SQL_NONE){
            cond_statement = cond_statement.substr(op_pos_post);
            pos -= op_pos_post; //adjust pos accordingly;

            GetNextOperator(cond_statement, op_pos, op_pos_post, next_op_type); //returns SQL_NONE && op_pos = length if no more operators.
            if(next_op_type == SQL_SPECIAL){
                terminate_early = true;
                return false;
            } 

            //std::cerr << "pos"
             //Check if operator > pos. if so this is first op inside a >. Ignore setting it (just break while loop). Call recursion for right hand side.
            if(op_pos > pos){
                next_op_type = SQL_NONE; // equivalent to break;
                size_t dummy = 0;
                CheckColConditions(dummy, cond_statement.substr(pos), col_registry, right_p_col_value, terminate_early, relax);
            }
            else{
                ExtractColCondition(cond_statement.substr(0, op_pos), col_registry, right_p_col_value);
            }
          
            terminate_early = !MergeColConditions(op_type, p_col_value, right_p_col_value);
            if(terminate_early) return false;

            op_type = next_op_type;
            
        }

        // std::cerr << "Combined" << std::endl;
        // for(auto &[col, val]: p_col_value){
        //     std::cerr << "col vals: " << col << ":" << val << std::endl;
        // }
        
        return !terminate_early && primary_key_compare(p_col_value, col_registry.primary_key_cols, relax);      //p_col_value.size() == col_registry.primary_key_cols.size();
    }


    // Recurse on opening (
    // Return on closing )  ==> This is left value
    // find operator, then find opening ( (if any) ==> Recurse on it, return closing ). ==> This is right value
    // Apply merge. Return
    if(cond_statement.substr(pos, 1) == "("){ //Start recursion
        cond_statement.remove_prefix(pos+1);

        //Recurse --> Returns result inside ")"  
        CheckColConditions(end, cond_statement, col_registry, left_p_col_value, terminate_early, relax);   //End = pos after ) that matches opening (  (or base case no bracket.)
      
        if(end == std::string::npos || cond_statement.substr(end, 1) == ")"){ //this is the right side of a statement -> return.
            return !terminate_early && primary_key_compare(left_p_col_value, col_registry.primary_key_cols, relax);      //p_col_value.size() == col_registry.primary_key_cols.size();
        }
       
        //else: this is the left side of a statement. ==> there must be at least one operator to the right.

        //FIND AND/OR. (search after end)
        cond_statement = cond_statement.substr(end); //Note, this does not modify the callers view.

        op_t next_op_type;
        size_t op_pos; 
        size_t op_pos_post = 0; 
        
        GetNextOperator(cond_statement, op_pos, op_pos_post, next_op_type); //returns SQL_NONE && op_pos = length if no more operators.
        if(next_op_type == SQL_SPECIAL){
            terminate_early = true;
            return false;
        } 
        if(next_op_type == SQL_NONE){
            return primary_key_compare(p_col_value, col_registry.primary_key_cols, relax);      //p_col_value.size() == col_registry.primary_key_cols.size();
        }

        //After that recurse on right cond.
        CheckColConditions(end, cond_statement.substr(op_pos_post), col_registry, right_p_col_value, terminate_early, relax);

        //Merge left and right side of operator
        terminate_early = !MergeColConditions(next_op_type, left_p_col_value, right_p_col_value);

        return !terminate_early && primary_key_compare(p_col_value, col_registry.primary_key_cols, relax);      //p_col_value.size() == col_registry.primary_key_cols.size();
    }

    //Recursive cases:
    if(cond_statement.substr(pos, 1) == ")"){ //End recursion.
        std::string_view sub_cond = cond_statement.substr(0, pos);
      
        //Note: Anything between 0 and ) must be normal statement.
        // Recurse to use base case and return result.
         size_t dummy;

        CheckColConditions(dummy, sub_cond, col_registry, p_col_value, terminate_early, relax);

        end = pos+1; //Next level receives p_col_values, and end 
        return !terminate_early && primary_key_compare(p_col_value, col_registry.primary_key_cols, relax);      //p_col_value.size() == col_registry.primary_key_cols.size(); 
    }
}

void SQLTransformer::ExtractColCondition(std::string_view cond_statement, const ColRegistry &col_registry, std::map<std::string, std::string> &p_col_value){
    //Note: Expecting all conditions to be of form "col_name = X"
    //Do not currently support boolean conditions, like "col_name";
    //std::cerr << "extract: cond statement: " << cond_statement << std::endl;
    size_t pos = cond_statement.find(" = ");
    if(pos == std::string::npos) return;

    //check left val;
    std::string_view left = cond_statement.substr(0, pos);
    const std::string &left_str = static_cast<std::string>(left);
    //if(col_registry.primary_key_cols.count(left_str)){ //if primary key and equality --> add value to map so we can compute enc_key for point read/write
        std::string_view right = cond_statement.substr(pos + 3);
        const std::string &type = col_registry.col_name_type.at(left_str);
        p_col_value[left_str] = TrimValueByType(right, type);
    //}
    // std::cerr << "right val: " << right << std::endl;
    // std::cerr << "trim val: " << TrimValueByType(right, type) << std::endl;

    return;
}

void SQLTransformer::GetNextOperator(std::string_view &cond_statement, size_t &op_pos, size_t &op_pos_post, op_t &op_type){
    
    if((op_pos = cond_statement.find(and_hook)) != std::string::npos){ //if AND exists.
        op_type = SQL_AND;
        op_pos_post = op_pos + and_hook.length();
    }
    else if((op_pos = cond_statement.find(or_hook)) != std::string::npos){ //if OR exists.
         op_type = SQL_OR;
         op_pos_post = op_pos + or_hook.length();
    }
    else if((op_pos = cond_statement.find(in_hook)) != std::string::npos){ //if IN exists.
         op_type = SQL_SPECIAL;
         op_pos_post = op_pos + in_hook.length();
    }
    else if((op_pos = cond_statement.find(between_hook)) != std::string::npos){ //if BETWEEN exists.
         op_type = SQL_SPECIAL;
         op_pos_post = op_pos + between_hook.length();
    }
    else{
        op_type = SQL_NONE;
        op_pos = cond_statement.length();
    }
    return;
        
}

bool SQLTransformer::MergeColConditions(op_t &op_type, std::map<std::string, std::string> &l_p_col_value, std::map<std::string, std::string> &r_p_col_value)
{
    switch (op_type) {
        case SQL_NONE:
            Panic("Should never be Merging Conditions on None");
            break;
        case SQL_START:
        {
            for(auto &[col, val]: r_p_col_value){
                l_p_col_value[std::move(col)] = std::move(val);
            }
        }   
            break;
        case SQL_AND:  //Union of both subsets. Note: Values must be unique.
        {
            for(auto &[col, val]: r_p_col_value){
                auto [itr, new_col] = l_p_col_value.try_emplace(std::move(col), std::move(val));
                if(!new_col){
                    //exists already, check that both values were the same...
                    if(val != itr->second) return false; //in this case we want to return false and terminate early.
                    // https://stackoverflow.com/questions/4286670/what-is-the-preferred-idiomatic-way-to-insert-into-a-map --> try_emplace does not move val if it fails.
                }
            }
        }
            break;
        case SQL_OR: //Intersection of both subsets. Note: Values must be unique.
        { 
            std::map<std::string, std::string> i_p_col_value; //intersection
            auto it_l = l_p_col_value.begin();
            auto it_r = r_p_col_value.begin();
            while(it_l != l_p_col_value.end() && it_r != r_p_col_value.end()){
                if(it_l->first < it_r->first) ++it_l;
                else if(it_l->first > it_r->first) ++it_r;
                else{
                    if(it_l->second == it_r->second) i_p_col_value[std::move(it_l->first)] = std::move(it_l->second);
                    ++it_l;
                    ++it_r;
                }
            }
            l_p_col_value = std::move(i_p_col_value);
        }    
            break;
        
        default:   
            NOT_REACHABLE();

    }

    r_p_col_value.clear(); //Clear right col keys
    return true;
  
}

///////////////// Old read parser

bool SQLTransformer::CheckColConditionsDumb(std::string_view &cond_statement, const ColRegistry &col_registry, std::map<std::string, std::string> &p_col_value){
    //Returns false if should use Query Protocol (Sync or Eager)
    //Returns true if should use Point Read Protocol

    //Input: boolean conditions, concatenated by AND/OR. E.g. <cond1> AND <cond2> OR <cond3>
   
    //Condition most basic: All bools need to be pcol and be bound by AND
        // any more --> false
        // anything more fancy --> false

    //split WHERE statement on AND/OR
    size_t pos;
  
    if((pos = cond_statement.find(or_hook)) != std::string::npos){ //if OR exists.
        return false;
    }
    else if((pos = cond_statement.find(in_hook)) != std::string::npos){ //if OR exists.
        return false;
    }
    else if((pos = cond_statement.find(between_hook)) != std::string::npos){ //if OR exists.
        return false;
    }

    if( (pos = cond_statement.find(and_hook)) != std::string::npos){ //if AND exists.
        //split on and, return both sides.
        std::string_view left_cond = cond_statement.substr(0, pos);
        std::string_view right_cond = cond_statement.substr(pos + and_hook.length());

         //split on = 
        pos = left_cond.find(" = ");
        if(pos == std::string::npos) return false;

        //check left val;
        std::string_view left = left_cond.substr(0, pos);
        const std::string &left_str = static_cast<std::string>(left);
        if(!col_registry.primary_key_cols.count(left_str)){ //if primary key and equality --> add value to map so we can compute enc_key for point read/write
            return false;
        }
        p_col_value[std::move(left_str)] = left_cond.substr(pos + 3);
        return CheckColConditionsDumb(right_cond, col_registry, p_col_value); //TODO: don't recurse (this re-checks all above statements) --> just while loop  
                                                                            
    }
    else { //No more bindings.
          //split on = 
        pos = cond_statement.find(" = ");
        if(pos == std::string::npos) return false;

        //check left val;
        std::string_view left = cond_statement.substr(0, pos);
        const std::string &left_str = static_cast<std::string>(left);
        if(!col_registry.primary_key_cols.count(left_str)){ //if primary key and equality --> add value to map so we can compute enc_key for point read/write
            return false;
        }
        p_col_value[std::move(left_str)] = cond_statement.substr(pos + 3);
        return p_col_value.size() == col_registry.primary_key_cols.size();
    }

    return false;
}


//FIXME: Wrong: This Read parser assumed no parentheses & simply checked whether all statements were primary col -- disregarding semantics of AND and OR
// bool SQLTransformer::CheckColConditions(std::string_view &cond_statement, ColRegistry &col_registry, std::map<std::string, std::string> &p_col_value){
//     //NOTE: Where condition must entirely be specified as AND or OR
//     // No Between: WHERE column_name BETWEEN value 1 AND value 2;
//     //TODO: Support WHERE name IN ("Lucy","Stella","Max","Tiger"); ==> Between or IN automatically count as  Ranges

//     //TODO: If string has extra statement (more restrictive than primary col) --> Simple point get won't be able to handle (need to add condidtions) --> simpler: Send through Sync...
//             //Or maybe we send the Get without sync but as a Select statement itself.

//     //split WHERE statement on AND/OR
//     size_t pos;
//     if( (pos = cond_statement.find(and_hook)) != std::string::npos){ //if AND exists.
//         //split on and, return both sides.
//         std::string_view left_cond = cond_statement.substr(0, pos);
//         std::string_view right_cond = cond_statement.substr(pos + and_hook.length());
//         return CheckColConditions(left_cond, col_registry, p_col_value) && CheckColConditions(right_cond, col_registry, p_col_value); 
//     }
//     else if((pos = cond_statement.find(or_hook)) != std::string::npos){ //if OR exists.
//         std::string_view left_cond = cond_statement.substr(0, pos);
//         std::string_view right_cond = cond_statement.substr(pos + or_hook.length());
//         return CheckColConditions(left_cond, col_registry, p_col_value) && CheckColConditions(right_cond, col_registry, p_col_value); 
//     }

//     //split on = 
//     pos = cond_statement.find(" = ");
//     if(pos == std::string::npos) return false;

//     //check left val;
//     std::string_view left = cond_statement.substr(0, pos);
//     const std::string &left_str = static_cast<std::string>(left);
//     if(col_registry.primary_key_cols.count(left_str)){ //if primary key and equality --> add value to map so we can compute enc_key for point read/write
//         p_col_value[std::move(left_str)] = cond_statement.substr(pos + 3);
//         return true;
//     }

//     return false;
    
//  //Find = ; Isolate what is to the left and to th

//     //check if ALL conds are primary col AND operand is =
//     // --> if so, parse the values from cond into the write
//     //return true --> skip creation of read statement. (set to "")

//     // if not, use query ;; pass an arg that tells query interpreter to skip interpretation we already know it must use query protocol.
// }



};