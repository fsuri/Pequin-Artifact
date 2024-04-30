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
#include "store/common/table_kv_encoder.h"
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

  //Shard based off Table numeric; this creates a perfect partition by tables. (Note: If we want, we could store the table names directly as numeric..)
  if(is_key){ 
    size_t w_pos = input.find(unique_delimiter);
    if(w_pos == std::string::npos) Panic("Cannot find table_name");
    return std::stoi(input.substr(0, w_pos)) % nshards;
  }
  else{
    UW_Assert(!table_name.empty());
    return std::stoi(EncodeTable(table_name)) % nshards;
  }

  //Alternative version: turn numeric to name, and hash. Con: Hash not necessarily evenly distributed, and thus not guaranteed that all shards have the same amount of tables.
  /*
  if(is_key){
    //input = encoded, must extract table_v
    // std::string_view key(input);
    // size_t w_pos = key.find(unique_delimiter);
    size_t w_pos = input.find(unique_delimiter);
     if(w_pos == std::string::npos) Panic("Cannot find table_name");

    // std::cerr << "key case: " << hash(key.substr(0, w_pos)) << std::endl;
    // return hash(key.substr(0, w_pos)) % nshards;

    const std::string &extracted_name = *DecodeTable(input.substr(0, w_pos));
    return hash(extracted_name) % nshards;
  }
  else{
    UW_Assert(!table_name.empty());
    // std::cerr << "tbl case: " << hash(std::string_view(table_name)) << std::endl;
    // return hash(std::string_view(table_name)) % nshards;
    return hash(table_name) % nshards;
  }
  */

};

uint64_t RWSQLPartitioner::operator()(const std::string &table_name, const google::protobuf::RepeatedPtrField<std::string> &col_values, uint64_t nshards, int group, const std::vector<int> &txnGroups){
  return hash(std::string_view(table_name)) % nshards;
}


//NOTE: This partitioner is used to 
      //a) allow the client to figure out which shard to contact for a read (input = query). To figure out which to write to, use the other operator below.
      //b) allow the server to figure out which keys are owned or not. (for CC purposes). To figure out which rows should be written to SQL backend use the other operator below.
uint64_t WarehouseSQLPartitioner::operator()(const std::string &table_name, const std::string &input,  //Note: if is_key: input = encoded key
    uint64_t nshards, int group, const std::vector<int> &txnGroups, bool is_key) {  //bool is_key: Set true if INSERT, Else = everything else. (i.e. has a Where clause)
    
  if(nshards == 1) return 0;

  //Note: if `is_key` then input = encoded key. If not, then input = query, and we have to check the table_name
  //const char &t = is_key? input[0] : table_name[0];
  //TODO: Can we get rid of this copy of input[0]?
  const char &t = is_key? (*DecodeTable({input[0]}))[0] : table_name[0];   //Note: the first char of input is the numeric encoding (because TPCC has only 10 tables, i.e. numerics 0-9)
  switch (t) { 
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
    {
      //Note: History has no primary key so the primary key is always empty.
   
      if(is_key){  
        return 0; //Technically nobody needs to be responsible for writing History for CC purposes. Since it has no primary key, we enforce no uniqueness.
      }
      else{
        Panic("TPCC should never be reading from History Table");
      }
      
    }
    default:
      {
        Panic("Invalid TPCC-SQL Table");
        return 0UL;
      }
  }
}

/*
//w_id d_id are third and second entries of the primary key.

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
          return (((w_id - 1) * 10) + (d_id - 1)) % nshards;
*/


//NOTE: This partitioner call is used to 
// a) clientside: figure out which shard to route a Write to
// b) serverside: filter out relevant RowUpdates for the backend.
uint64_t WarehouseSQLPartitioner::operator()(const std::string &table_name, const google::protobuf::RepeatedPtrField<std::string> &col_values, uint64_t nshards, int group, const std::vector<int> &txnGroups){
    
  if(nshards == 1) return 0;

  switch (table_name[0]) { 
    case 'w':  // WAREHOUSE
    {
      uint32_t w_id = std::stoi(col_values[0]);
      return w_id % nshards;
    }
    case 's':  // STOCK
    {
      
      uint32_t w_id = std::stoi(col_values[1]);
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
      uint32_t w_id = std::stoi(col_values[0]);
      //uint32_t d_id = std::stoi(col_values[1]);

       std::cerr << "w_id: " << w_id << std::endl;
      //  std::cerr << "w_id: " << w_id << ". d_id: " << d_id << std::endl;
      // return (((w_id - 1) * 10) + (d_id - 1)) % nshards; //TODO: Seems unecessary to put different districts from the same warehouse on different shards? (Will only increase number of cross shard TXs)
      return w_id % nshards; //TODO: Remove d_id computation.
    }
    case 'h':  // HISTORY           
    //TODO: FIXME: History has no primary key so we cannot hope to process a key.
   
          //TODO: Either include w_id as part of history primary key. Or allow partitioner function to be called directly on RowUpdate (col_values) and consider Owned only if this shard has that w_id.
    {
       //w_id d_id are fourth and fifth entries of the row
      uint32_t w_id = std::stoi(col_values[3]);
      //uint32_t d_id = std::stoi(col_values[4]);
       
      //return (((w_id - 1) * 10) + (d_id - 1)) % nshards;
       return w_id % nshards; //TODO: Remove d_id computation.
    }
    default:
      {
        Panic("Invalid TPCC-SQL Table");
        return 0UL;
      }
  }
}


/* Some testing code */
//////
  /*
  std::mt19937 test_rand;
   auto test_part = WarehouseSQLPartitioner(1, test_rand);
  std::string tbl = "warehouse";
  const std::vector<int> grps;
  std::string test_put = "warehouse#15";
  auto shard = test_part(tbl, test_put, 2, 1, grps, true);
  std::cerr << "TEST: " << shard << std::endl;

  std::string test_query = "SELECT * FROM warehouse WHERE w_id = 15;";
  shard = test_part(tbl, test_query, 2, 1, grps, false);
  std::cerr << "TEST2: " << shard << std::endl;
  std::string test_query2 = "SELECT * FROM warehouse WHERE w_id = 15 AND d_id = 2;";
  shard = test_part(tbl, test_query2, 2, 1, grps, false);
  std::cerr << "TEST3: " << shard << std::endl;


   tbl = "stock";
   std::string test_put2 = "stock#4#15";
  shard = test_part(tbl, test_put2, 2, 1, grps, true);
  std::cerr << "TEST4: " << shard << std::endl;

  std::string test_query3 = "SELECT * FROM stock WHERE s_i_id = 2 AND s_w_id = 15;";
  shard = test_part(tbl, test_query3, 2, 1, grps, false);
  std::cerr << "TEST5: " << shard << std::endl;


  //Test 2 generics: 1 that ends in 2, 1 that has 3 or more

   tbl = "district";
   std::string test_put3 = "district#15#5";
   auto sum_id = test_part(tbl, test_put3, 2, 1, grps, true);
  std::cerr << "TEST6: " << sum_id << std::endl;

  std::string test_query4 = "SELECT * FROM district WHERE d_id = 5 AND d_w_id = 15;";
  shard = test_part(tbl, test_query4, 2, 1, grps, false);
  std::cerr << "TEST7: " << shard << std::endl;

   tbl = "customer";
   std::string test_put4 = "customer#15#5#4";
  shard = test_part(tbl, test_put4, 2, 1, grps, true);
  std::cerr << "TEST8: " << shard << std::endl;

  std::string test_query5 = "SELECT * FROM customer WHERE c_w_id = 15 AND c_d_id = 5 AND c_id = 4;";
  shard = test_part(tbl, test_query5, 2, 1, grps, false);
  std::cerr << "TEST9: " << shard << std::endl;

  //Test History TODO: FIXME:

  //  tbl = "history";
  //  std::string test_put2 = "history";
  // w_id = test_part(tbl, test_put2, 2, 1, grps, true);
  // std::cerr << "TEST4: " << w_id << std::endl;

  // std::string test_query3 = "SELECT * FROM stock WHERE s_i_id = 2 AND s_w_id = 15;";
  // w_id = test_part(tbl, test_query3, 2, 1, grps, false);
  // std::cerr << "TEST5: " << w_id << std::endl;

  UW_ASSERT(FLAGS_sql_bench);



  // uint32_t w_id = *reinterpret_cast<const uint32_t*>(key.c_str() + 1);
  std::string t1 = "1_table";
  std::string t15 = "15_table";

  std::cerr << "t1: " << t1[0] << std::endl; 
   std::cerr << "t15: " << t15[0] << std::endl; 

  enum test {
    a, b, c,d,e,f,g,h,i,j,k,l
  };

  char t[2];
  t[0] = static_cast<char>(test::l);
  t[1] = 't';
  std::string test_t(t, sizeof(t));
  std::cerr << "test_t: " << test_t << std::endl;
  std::cerr << "t: " << test_t[0] << std::endl; 
  UW_ASSERT(test_t[0]== 11);


  auto rw_part = RWSQLPartitioner(10);
  tbl = "15t";
  test_put = "15t#12";
  shard = rw_part(tbl, test_put, 2, 1, grps, true);
  std::cerr << "RW-TEST: " << shard << std::endl;
  shard = rw_part(tbl, test_put, 2, 1, grps, false);
  std::cerr << "RW-TEST2: " << shard << std::endl;


  //Panic("stop test");
  */
  


  /////