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

#include "store/common/query_result/query_result.h"

using std::string;

  static std::string insert_hook("INSERT INTO ");
  static std::string values_hook(" VALUES ");
    //static std::string lbracket("(");

int main() {
  
  std::cerr<< "Testing Write Parser" << std::endl;

   std::string table_name;
   std::vector<std::string> column_list;
   std::vector<std::string> value_list;
  
  std::string write_statement = "INSERT INTO user(col1, col2, col3) VALUES (val1, val2, val3)";
  std::vector<uint32_t> primary_key_encoding_support = {0, 2};

  std::cerr << write_statement << std::endl;

  size_t pos = 0;
 
  if( (pos = write_statement.find(insert_hook) != string::npos)){
    //1 Remove insert hook
    write_statement.erase(0, pos + insert_hook.length()-1);  //TODO: Maybe do it without deletion for efficiency?
    std::cerr << write_statement << std::endl;
    //2 Split on values
    pos = write_statement.find(values_hook);
    // UW_ASSERT(pos != std::string::npos);
    //Everything from 0 - pos is "<table_name>(columns)". Everything from pos + values_hook.length() --> end is "(values)"
    size_t val_pos = pos + values_hook.length();
    std::cerr << write_statement.substr(val_pos) << std::endl;
    //3) Extract table
    //std::string table_col = write_statement.substr(0, pos);
        // Look for "(" (before end)
        std::cerr << pos << std::endl;
         std::cerr << val_pos << std::endl;
        pos = write_statement.find("(", 0); //Look only until start of values_hook   // Might be easier if we just create substring.
        if(pos == std::string::npos || pos > val_pos){ //if > val_pos then we found the "(" for Values
          // If "(" doesn't exist --> whole string is table_name.. Throw error -> can't compute Select Statement
          // Panic("Codebase requires INSERT statement to contain (at least primary) column names for translation into SELECT statement");
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
        // UW_ASSERT(pos != std::string::npos && pos < val_pos);
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
        // add item inbetween to cols vector
        while((pos = write_statement.find(", ", val_pos)) != string::npos){
          value_list.push_back(write_statement.substr(val_pos, pos-val_pos));
          val_pos = pos + 2;
        }
        // if no more ", " --> look for ")" and skip. Then insert last value
        pos = write_statement.find(")", val_pos);
        // UW_ASSERT(pos != std::string::npos);
        value_list.push_back(write_statement.substr(val_pos, pos-val_pos));
        // Done.

        
      //TODO: Construct read_statement and write_cont
      //UW_ASSERT(value_list.size() == column_list.size()); //Require to pass all columns currently.
      //UW_ASSERT(column_list.size() >= 1); // At least one column specified (e.g. single column primary key)

  }

  std::cerr << "Table name: " << table_name << std::endl;
  for(auto col : column_list){
      std::cerr << "col: " << col << std::endl;
  }
  for(auto val : value_list){
      std::cerr << "val: " << val << std::endl;
  }

  //Create Read statement:  ==> Ideally for Inserts we'd just use a point get on the primary keys. (instead of a sql select statement that's a bit overkill)
  std::string read_statement("SELECT ");  
  //insert primary columns --> Can already concat them with delimiter:   col1  || '###' || col2 ==> but then how do we look up column?  
  for(auto p_idx: primary_key_encoding_support){
    read_statement += column_list[p_idx] + ", ";
  }
  read_statement.resize(read_statement.size() - 2); //remove trailing ", "


  read_statement += " FROM " + table_name;

  read_statement += " WHERE ";
  for(auto p_idx: primary_key_encoding_support){
    read_statement += column_list[p_idx] + " = " + value_list[p_idx] + ", ";
  }
  //insert primary col conditions.
  read_statement.resize(read_statement.size() - 2); //remove trailing ", "

  read_statement += ";";

  std::cerr << read_statement << std::endl;

  //TODO: Create Write continuation:   //TODO: include Result object .h
  // std::function<sql::QueryResultProtoWrapper*(const query_result::QueryResult*)>  write_continuation = [](const query_result::QueryResult*){
  //     if()
  // };

    //Check result: If empty. --> Add to TableWrites; If not empty --> return fail to App (rows affected = 0)
    //


  //Case 1) INSERT INTO <table_name> (<column_list>) VALUES (<value_list>)
              //Note: Value list may be the output of a nested SELECT statement. In that case, embed the nested select statement as part of the read_statement
        //-> Turn into read_statement: Result(column, column_value) SELECT <primary columsn> FROM <table_name>(primary_columns) WHERE <col = value>  // Nested Select Statement.
        //             write_cont: if(Result.empty()) create TableWrite with primary column encoded key, column_list, value_list
        //     TODO: Need to add to read set the time stamp of read "empty" version: I.e. for no existing version (result = empty) -> 0 (genesis TS); for deleted version --> version that deleted row.
                                                                                        // I think it's always fine to just set version to 0 here.
                                                                                        // During CC, should ignore conflicts of genesis against delete versions (i.e. they are equivalent)
        // TODO: Also need to write new "Table version" (in write set) -- to indicate set of rows changes 

  return 0;
}
