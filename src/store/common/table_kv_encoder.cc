#include <vector>
#include <string>
#include "store/common/table_kv_encoder.h"
#include "lib/assert.h"
#include <iostream>

std::string NameToNumeric(const std::string &table_name){
  auto itr = name_to_numerics.find(table_name);
  if(itr != name_to_numerics.end()) return std::to_string(itr->second);

  size_t new_numeric = name_to_numerics.size();
  name_to_numerics[table_name] = new_numeric;
  //numeric_to_name.push_back(table_name);
  numerics_to_name[std::to_string(new_numeric)] = table_name;

  //TEST
  // for(const auto &[name, num]: name_to_numerics){
  //   std::cerr << "Map: " << name << " -> " << num << std::endl;
  // }
  // for(const auto &[num, name]: numerics_to_name){
  //   std::cerr << "Rev: " << num << " -> " << name << std::endl;
  // }

  return std::to_string(new_numeric);
}

std::string* NumericToName(const std::string &numeric){
   auto itr = numerics_to_name.find(numeric);
  if(itr != numerics_to_name.end()) return &itr->second;

  Panic("Have not stored conversion for numeric: %s", numeric.c_str());
}

//TODO: input: convert row_name type into byte array. E.g. Int: static_cast<char*>(static_cast<void*>(&x)); String: str.c_str();
std::string EncodeTableRow(const std::string &table_name, const std::vector<std::string> &primary_key_column_values){  //std::string &row_name
  
  //Note: Assuming unique delimiter that is neither part of table_nor string.
  //std::string encoding = table_name;
  std::string encoding = NameToNumeric(table_name);
  for(auto primary_column_value: primary_key_column_values){
    encoding += unique_delimiter + std::move(primary_column_value);
  }
  return encoding;
  //return table_name + unique_delimiter + row_name;
}

std::string EncodeTableRow(const std::string &table_name, const std::vector<const std::string*> &primary_key_column_values){  //std::string &row_name
  
  //Note: Assuming unique delimiter that is neither part of table_nor string.
    //std::string encoding = table_name;
  std::string encoding = NameToNumeric(table_name);
  for(auto primary_column_value: primary_key_column_values){
    encoding += unique_delimiter + (*primary_column_value);  
  }
  return encoding;
  //return table_name + unique_delimiter + row_name;
}

//If p_key values come with quotes --> dynamically remove them here
std::string EncodeTableRow(const std::string &table_name, const std::vector<const std::string*> &primary_key_column_values, const std::vector<bool>* p_col_quotes){  //std::string &row_name
  
  //Note: Assuming unique delimiter that is neither part of table_nor string.
    //std::string encoding = table_name;
  std::string encoding = NameToNumeric(table_name);
  for(int i = 0; i < primary_key_column_values.size(); ++i){
    const std::string *p_col_val = primary_key_column_values[i];
    encoding += unique_delimiter + ((*p_col_quotes)[i] ? p_col_val->substr(1, p_col_val->length()-2) : *p_col_val);
  }
  return encoding;
  //return table_name + unique_delimiter + row_name;
}

std::string EncodeTableRow(const std::string &table_name, const std::vector<const std::string_view*> &primary_key_column_values){  //std::string &row_name
  
  //Note: Assuming unique delimiter that is neither part of table_nor string.
   //std::string encoding = table_name;
  std::string encoding = NameToNumeric(table_name);
  for(auto primary_column_value: primary_key_column_values){
    encoding += unique_delimiter;
    encoding += *primary_column_value;
  }
  return encoding;
  //return table_name + unique_delimiter + row_name;
}

std::string EncodeTableCol(const std::string &table_name, const std::string &col_name){
  return table_name + unique_delimiter + col_name;
}


//NOTE: Returns row primary keys as strings here... TODO: At application to table, convert as appropriate. E.g. Int: stoi(), String: string()
void DecodeTableRow(const std::string &enc_key, std::string &table_name, std::vector<std::string> &primary_key_column_values) {  //std::string &row_name){
  size_t pos = enc_key.find(unique_delimiter);

  UW_ASSERT(pos != std::string::npos);
  //table_name = enc_key.substr(0, pos);
  table_name = *NumericToName(enc_key.substr(0, pos));

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