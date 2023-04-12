#include <vector>
#include <string>
#include "store/common/backend/sql_engine/table_kv_encoder.h"
#include "lib/assert.h"


static std::string unique_delimiter = "###";
//TODO: input: convert row_name type into byte array. E.g. Int: static_cast<char*>(static_cast<void*>(&x)); String: str.c_str();
std::string EncodeTableRow(const std::string &table_name, const std::vector<std::string> &primary_key_column_values){  //std::string &row_name
  
  //Note: Assuming unique delimiter that is neither part of table_nor string.
  std::string encoding = table_name;
  for(auto primary_column_value: primary_key_column_values){
    encoding += unique_delimiter + primary_column_value;
  }
  return encoding;
  //return table_name + unique_delimiter + row_name;
}

std::string EncodeTableRow(const std::string &table_name, const std::vector<const std::string*> &primary_key_column_values){  //std::string &row_name
  
  //Note: Assuming unique delimiter that is neither part of table_nor string.
  std::string encoding = table_name;
  for(auto primary_column_value: primary_key_column_values){
    encoding += unique_delimiter + (*primary_column_value);
  }
  return encoding;
  //return table_name + unique_delimiter + row_name;
}

std::string EncodeTableRow(const std::string &table_name, const std::vector<const std::string_view*> &primary_key_column_values){  //std::string &row_name
  
  //Note: Assuming unique delimiter that is neither part of table_nor string.
  std::string encoding = table_name;
  for(auto primary_column_value: primary_key_column_values){
    encoding += unique_delimiter;
    encoding += *primary_column_value;
  }
  return encoding;
  //return table_name + unique_delimiter + row_name;
}


//NOTE: Returns row primary keys as strings here... TODO: At application to table, convert as appropriate. E.g. Int: stoi(), String: string()
void DecodeTableRow(const std::string &enc_key, std::string &table_name, std::vector<std::string> &primary_key_column_values) {  //std::string &row_name){
  size_t pos = enc_key.find(unique_delimiter);

  UW_ASSERT(pos != std::string::npos);
  table_name = enc_key.substr(0, pos);
  //row_name = enc_key.substr(pos + unique_delimiter.length()); //For "single row name"  //, enc_key.length());

  // //If looping create a copy in order to use erase.
  // std::string s = enc_key;
  // s.erase(0, pos + delimiter.length());

  // while ((pos = s.find(delimiter)) != std::string::npos) {
  //   primary_key_columns.push_back(s.substr(0, pos));
  //   s.erase(0, pos + delimiter.length());
  // }
  //  primary_key_columns.push_back(s);

  //Alternative, without erasure.
   size_t last = pos + unique_delimiter.length(); 
   size_t next; 

   while ((next = enc_key.find(unique_delimiter, last)) != std::string::npos) {   
    primary_key_column_values.push_back(enc_key.substr(last, next-last));   
    last = next + unique_delimiter.length(); 
   } 
  primary_key_column_values.push_back(enc_key.substr(last));

}