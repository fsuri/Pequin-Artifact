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
#include "store/common/partitioner.h"

#include "lib/message.h"
#include "store/common/backend/sql_engine/table_kv_encoder.h"
#include <iostream>
#include <string_view>

uint64_t DefaultPartitioner::operator()(const std::string &key, uint64_t nshards,
    int group, const std::vector<int> &txnGroups) {
  // uint64_t hash = 5381;
  // const char* str = key.c_str();
  // for (unsigned int i = 0; i < key.length(); i++) {
  //   hash = ((hash << 5) + hash) + (uint64_t)str[i];
  // }
  // return (hash % nshards);
  return hash(key) % nshards;
};
    
/*partitioner warehouse_partitioner = [](const std::string &key, uint64_t nshards,
    int group, const std::vector<int> &txnGroups) {
  // keys format is defined in tpcc_utils.cc
  // bytes 1 through 4 (0-indexed) contain the warehouse id for each table except
  // for the item table (which has no warehouse ids associated with rows).
  uint32_t w_id = *reinterpret_cast<const uint32_t*>(key.c_str() + 1);
  return w_id % nshards;
};*/


uint64_t WarehouseDistItemsPartitioner::operator()(const std::string &key,
    uint64_t nshards, int group, const std::vector<int> &txnGroups) {
  uint32_t w_id = *reinterpret_cast<const uint32_t*>(key.c_str() + 1);
  uint32_t d_id = 0;
  if (key.length() >= 9) {
    d_id = *reinterpret_cast<const uint32_t*>(key.c_str() + 5);
  }
  return (((w_id - 1) * 10) + d_id) % nshards;
}


uint64_t WarehousePartitioner::operator()(const std::string &key,
    uint64_t nshards, int group, const std::vector<int> &txnGroups) {
  switch (key[0]) {
    case 0:  // WAREHOUSE
    case 8:  // STOCK
    {
      uint32_t w_id = *reinterpret_cast<const uint32_t*>(key.c_str() + 1);
      return w_id % nshards;
    }
    case 7:  // ITEM
    {
      if (group == -1) {
        if (txnGroups.size() > 0) {
          size_t idx = std::uniform_int_distribution<size_t>(0, txnGroups.size() - 1)(rd);
          return static_cast<uint64_t>(txnGroups[idx]);
        } else {
          return std::uniform_int_distribution<uint64_t>(0, nshards)(rd);
        }
      } else {
        return static_cast<uint64_t>(group);
      }
    }
    case 1:  // DISTRICT
    case 2:  // CUSTOMER
    case 3:  // HISTORY
    case 4:  // NEW_ORDER
    case 5:  // ORDER
    case 6:  // ORDER_LINE
    case 9:  // CUSTOMER_BY_NAME
    case 10: // ORDER_BY_CUSTOMER
    case 11: // EARLIEST_NEW_ORDER
    {
      uint32_t w_id = *reinterpret_cast<const uint32_t*>(key.c_str() + 1);
      uint32_t d_id = *reinterpret_cast<const uint32_t*>(key.c_str() + 5);
      return (((w_id - 1) * 10) + (d_id - 1)) % nshards;
    }
    default:
      return 0UL;
  }
}


/* 
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

  */

// SQL PARTITIONERS

uint64_t RWSQLPartitioner::operator()(const std::string &table_name, const std::string &input, uint64_t nshards,
    int group, const std::vector<int> &txnGroups, bool is_key) {
  if(nshards == 1) return 0;

  return hash(table_name) % nshards;
};


uint64_t WarehouseSQLPartitioner::operator()(const std::string &table_name, const std::string &input,  //Note: if is_key: input = encoded key
    uint64_t nshards, int group, const std::vector<int> &txnGroups, bool is_key) {  //bool is_key: Set true if INSERT, Else = everything else. (i.e. has a Where clause)
    
  if(nshards == 1) return 0;

  //TODO: Pass "is_write" argument. If true, then expect input to not be a statement, but rather an encoded key. (find right value based on steps in delimiter)
            //TODO: In ParseTableDataFromCSV: Check whether it is in Partition based of the encoded key. If it is not, then don't add to row_values => this makes sure that it is not written.

  //FIXME: Fix "owns key in Concurrency control"
 

  switch (is_key? input[0] : table_name[0]) { //Note: if `is_key` then input = encoded key. If not, then input = query, and we have to check the table_name
    case 'w':  // WAREHOUSE
    case 's':  // STOCK
    {
      std::cerr << "table name: " << table_name << std::endl;
      std::cerr << "input: " << input << std::endl;
      uint32_t w_id;
      if(is_key){
          size_t w_pos = input.find(unique_delimiter);
          if((is_key? input[0] : table_name[0]) == 's') w_pos = input.find(unique_delimiter, w_pos+unique_delimiter.size()); //Find the SECOND position.
          if(w_pos == std::string::npos) Panic("Cannot find w_id.");
          size_t w_start = w_pos + unique_delimiter.size();
          w_id = std::stoi(input.substr(w_start));
      }
      else{
         //Extract the  'xyz_w_id = 5' portion and get 5.'
        size_t w_pos = input.find("w_id = ");
        if(w_pos == std::string::npos) Panic("Cannot find w_id.");
        size_t w_start = w_pos + 7;  //Start: just search for w_id.
         //End: first space or semicolon after 5.
        size_t w_end = input.find_first_of("; ", w_start);
        if(w_pos == std::string::npos) Panic("Cannot find end point");
        w_id = std::stoi(input.substr(w_start, w_end-w_start));
      }
      std::cerr << "w_id: " << w_id << std::endl;
      return w_id % nshards;
    }
    case 'i':  // ITEM
    {
      //ITEM is replicated to all shards; it is read only.
      if (group == -1) { //Client picks a random shard to read from for load balancing purposes
        if (txnGroups.size() > 0) {
          size_t idx = std::uniform_int_distribution<size_t>(0, txnGroups.size() - 1)(rd);
          return static_cast<uint64_t>(txnGroups[idx]);
        } else {
          return std::uniform_int_distribution<uint64_t>(0, nshards)(rd);
        }
      } else { //If server pick own group -> this way every server loads the Item table
        return static_cast<uint64_t>(group);
      }
      return 0;
    }
    case 'd' :  // DISTRICT        
    case 'c' :  // CUSTOMER        
    case 'n':  // NEW_ORDER        
    case 'o':  // ORDER & ORDER_LINE        
    case 'E': // EARLIEST_NEW_ORDER   
    {
      uint32_t w_id;
      uint32_t d_id;

      if(is_key){  //w_id d_id are first and second entries of the primary key
          //w_id is the first after delim
          size_t w_pos = input.find(unique_delimiter);
          if(w_pos == std::string::npos) Panic("Cannot find w_id.");
          size_t w_start = w_pos + unique_delimiter.size();
          //d_id is the second 
          size_t d_pos = input.find(unique_delimiter, w_start);
          if(d_pos == std::string::npos) Panic("Cannot find d_id.");
         
          w_id = std::stoi(input.substr(w_start, d_pos-w_start));

          //TODO: d_id does not seem useful for sharding across groups...
          // size_t d_start = d_pos + unique_delimiter.size();
          // size_t d_end = input.find(unique_delimiter, d_start);
          // if(d_end == std::string::npos){ //d_id is all the rest
          //   d_id = std::stoi(input.substr(d_start));
          // }
          // else{
          //   d_id = std::stoi(input.substr(d_start, d_end-d_start));
          // }
      }
      else{
         //Extract the  'xyz_w_id = 5' portion and get 5.'
        size_t w_pos = input.find("w_id = ");
        if(w_pos == std::string::npos) Panic("Cannot find w_id.");
        size_t w_start = w_pos + 7;  //Start: just search for w_id.
         //End: first space or semicolon after 5.
        size_t w_end = input.find_first_of("; ", w_start);
        if(w_pos == std::string::npos) Panic("Cannot find end point");
        w_id = std::stoi(input.substr(w_start, w_end-w_start));

        // size_t d_pos = input.find("d_id = ");
        // if(d_pos == std::string::npos) Panic("Cannot find d_id.");
        // size_t d_start = d_pos + 7;  //Start: just search for d_id.
        //  //End: first space or semicolon after 5.
        // size_t d_end = input.find_first_of("; ", d_start);
        // if(d_pos == std::string::npos) Panic("Cannot find end point");
        // d_id = std::stoi(input.substr(d_start, w_end-w_start));
      }

       std::cerr << "w_id: " << w_id << std::endl;
      //  std::cerr << "w_id: " << w_id << ". d_id: " << d_id << std::endl;
      // return (((w_id - 1) * 10) + (d_id - 1)) % nshards; //TODO: Seems unecessary to put different districts from the same warehouse on different shards? (Will only increase number of cross shard TXs)
      return w_id % nshards; //TODO: Remove d_id computation.
    }
    case 'h':  // HISTORY           
    //TODO: FIXME: History has no primary key so we cannot hope to process a key.
   
          //TODO: Either include w_id as part of history primary key. Or allow partitioner function to be called directly on RowUpdate (col_values) and consider Owned only if this shard has that w_id.
    {
      uint32_t w_id;
      uint32_t d_id;

      if(is_key){  //w_id d_id are third and second entries of the primary key.

          //skip the first delim
           size_t skip_pos = input.find(unique_delimiter);
          if(skip_pos == std::string::npos) Panic("Cannot find a delim to skip.");

          //d_id is after the second delim
          size_t d_pos = input.find(unique_delimiter);
          if(d_pos == std::string::npos) Panic("Cannot find d_id.");
          size_t d_start = d_pos + unique_delimiter.size();

          //w_id is after the third delim
          size_t w_pos = input.find(unique_delimiter, d_start);
          if(w_pos == std::string::npos) Panic("Cannot find w_id.");
         
          d_id = std::stoi(input.substr(d_start, w_pos-d_start));

          size_t w_start = w_pos + unique_delimiter.size();
          size_t w_end = input.find(unique_delimiter, w_start);
          if(w_end == std::string::npos){ //w_id is all the rest
            w_id = std::stoi(input.substr(w_start));
          }
          else{
            w_id = std::stoi(input.substr(w_start, w_end-w_start));
          }
      }
      else{
        Panic("TPCC should never be reading from History Table");
      }
      return (((w_id - 1) * 10) + (d_id - 1)) % nshards;
    }
    default:
      {
        Panic("Invalid TPCC-SQL Table");
        return 0UL;
      }


  }
  Panic("shouldn't reach here");

   TPCC_Table table;

  //Allow to call in is_key mode without table_name. This is useful for "IsOwned" checks. 
            //However, it is a bit inefficient to extract the table_name: Ideally try to get TableName by going from read_set/write_set to associated Query/TableWrite
  if(table_name.empty()){   
    UW_Assert(is_key);
    size_t t_end = input.find(unique_delimiter);
    if(t_end == std::string::npos) Panic("Calling Partitioner check on TableVersion."); //Note: Table Version should be partition local.
    std::cerr << "TEST TABLE: [" << (input.substr(0, t_end)) << "]" << std::endl;
    table = tables.at(input.substr(0, t_end));

    //FIXME: Fix TableVersion handling
    // In CC: move skip table version check before isOwned
    // In Prepare/Commit: Only write Table version if there is a write to THIS shard.
    // In applyTableWrites: Only write TableWrite if write is to this shard.  //TODO: FIXME: must support parsing from TableWrite row... (or Insert statement...)
    // In RegisterWrites (for semanticCC): only register if there is a write to THIS shard.
        //In CC-check: only need to check conflicts for writes on the *current* shard. (Technically not necessary, CC check will not throw conflicts. But it's wasted work.)

  }
  else{
    table = tables.at(table_name);
  }


  //auto &table = tables.at(table_name);
  switch (table) {
    case TPCC_Table::WAREHOUSE:  // WAREHOUSE
    case TPCC_Table::STOCK:  // STOCK
    {
      std::cerr << "table name: " << table_name << std::endl;
      std::cerr << "input: " << input << std::endl;
      uint32_t w_id;
      if(is_key){
          size_t w_pos = input.find(unique_delimiter);
          if(table == TPCC_Table::STOCK) w_pos = input.find(unique_delimiter, w_pos+unique_delimiter.size()); //Find the SECOND position.
          if(w_pos == std::string::npos) Panic("Cannot find w_id.");
          size_t w_start = w_pos + unique_delimiter.size();
          w_id = std::stoi(input.substr(w_start));
      }
      else{
         //Extract the  'xyz_w_id = 5' portion and get 5.'
        size_t w_pos = input.find("w_id = ");
        if(w_pos == std::string::npos) Panic("Cannot find w_id.");
        size_t w_start = w_pos + 7;  //Start: just search for w_id.
         //End: first space or semicolon after 5.
        size_t w_end = input.find_first_of("; ", w_start);
        if(w_pos == std::string::npos) Panic("Cannot find end point");
        w_id = std::stoi(input.substr(w_start, w_end-w_start));
      }
      std::cerr << "w_id: " << w_id << std::endl;
      return w_id % nshards;
    }
    case TPCC_Table::ITEM:  // ITEM
    {
      //ITEM is replicated to all shards; it is read only.
      if (group == -1) { //Client picks a random shard to read from for load balancing purposes
        if (txnGroups.size() > 0) {
          size_t idx = std::uniform_int_distribution<size_t>(0, txnGroups.size() - 1)(rd);
          return static_cast<uint64_t>(txnGroups[idx]);
        } else {
          return std::uniform_int_distribution<uint64_t>(0, nshards)(rd);
        }
      } else { //If server pick own group -> this way every server loads the Item table
        return static_cast<uint64_t>(group);
      }
      return 0;
    }
    case TPCC_Table::DISTRICT :  // DISTRICT        
    case TPCC_Table::CUSTOMER:  // CUSTOMER        
    case TPCC_Table::NEW_ORDER:  // NEW_ORDER        
    case TPCC_Table::ORDER:  // ORDER                 
    case TPCC_Table::ORDER_LINE:  // ORDER_LINE       
    case TPCC_Table::EARLIEST_NEW_ORDER: // EARLIEST_NEW_ORDER   
    {
      uint32_t w_id;
      uint32_t d_id;

      if(is_key){  //w_id d_id are first and second entries of the primary key
          //w_id is the first after delim
          size_t w_pos = input.find(unique_delimiter);
          if(w_pos == std::string::npos) Panic("Cannot find w_id.");
          size_t w_start = w_pos + unique_delimiter.size();
          //d_id is the second 
          size_t d_pos = input.find(unique_delimiter, w_start);
          if(d_pos == std::string::npos) Panic("Cannot find d_id.");
         
          w_id = std::stoi(input.substr(w_start, d_pos-w_start));

          //TODO: d_id does not seem useful for sharding across groups...
          // size_t d_start = d_pos + unique_delimiter.size();
          // size_t d_end = input.find(unique_delimiter, d_start);
          // if(d_end == std::string::npos){ //d_id is all the rest
          //   d_id = std::stoi(input.substr(d_start));
          // }
          // else{
          //   d_id = std::stoi(input.substr(d_start, d_end-d_start));
          // }
      }
      else{
         //Extract the  'xyz_w_id = 5' portion and get 5.'
        size_t w_pos = input.find("w_id = ");
        if(w_pos == std::string::npos) Panic("Cannot find w_id.");
        size_t w_start = w_pos + 7;  //Start: just search for w_id.
         //End: first space or semicolon after 5.
        size_t w_end = input.find_first_of("; ", w_start);
        if(w_pos == std::string::npos) Panic("Cannot find end point");
        w_id = std::stoi(input.substr(w_start, w_end-w_start));

        // size_t d_pos = input.find("d_id = ");
        // if(d_pos == std::string::npos) Panic("Cannot find d_id.");
        // size_t d_start = d_pos + 7;  //Start: just search for d_id.
        //  //End: first space or semicolon after 5.
        // size_t d_end = input.find_first_of("; ", d_start);
        // if(d_pos == std::string::npos) Panic("Cannot find end point");
        // d_id = std::stoi(input.substr(d_start, w_end-w_start));
      }

      //  std::cerr << "w_id: " << w_id << ". d_id: " << d_id << std::endl;
      // return (((w_id - 1) * 10) + (d_id - 1)) % nshards; //TODO: Seems unecessary to put different districts from the same warehouse on different shards? (Will only increase number of cross shard TXs)
      return w_id % nshards; //TODO: Remove d_id computation.
    }
    case TPCC_Table::HISTORY:  // HISTORY           
    //TODO: FIXME: History has no primary key so we cannot hope to process a key.
   
          //TODO: Either include w_id as part of history primary key. Or allow partitioner function to be called directly on RowUpdate (col_values) and consider Owned only if this shard has that w_id.
    {
      uint32_t w_id;
      uint32_t d_id;

      if(is_key){  //w_id d_id are third and second entries of the primary key.

          //skip the first delim
           size_t skip_pos = input.find(unique_delimiter);
          if(skip_pos == std::string::npos) Panic("Cannot find a delim to skip.");

          //d_id is after the second delim
          size_t d_pos = input.find(unique_delimiter);
          if(d_pos == std::string::npos) Panic("Cannot find d_id.");
          size_t d_start = d_pos + unique_delimiter.size();

          //w_id is after the third delim
          size_t w_pos = input.find(unique_delimiter, d_start);
          if(w_pos == std::string::npos) Panic("Cannot find w_id.");
         
          d_id = std::stoi(input.substr(d_start, w_pos-d_start));

          size_t w_start = w_pos + unique_delimiter.size();
          size_t w_end = input.find(unique_delimiter, w_start);
          if(w_end == std::string::npos){ //w_id is all the rest
            w_id = std::stoi(input.substr(w_start));
          }
          else{
            w_id = std::stoi(input.substr(w_start, w_end-w_start));
          }
      }
      else{
        Panic("TPCC should never be reading from History Table");
      }
      return (((w_id - 1) * 10) + (d_id - 1)) % nshards;
    }
    default:
      {
        Panic("Invalid TPCC-SQL Table");
        return 0UL;
      }
  }
}