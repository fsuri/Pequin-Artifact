#include<iostream>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include <stdlib.h>

#include <cstring>
#include <vector>
#include <iomanip>

#include <fstream>
#include <nlohmann/json.hpp>
//https://github.com/nlohmann/json
//Overview of other popular JSON c++ libraries: https://medium.com/ml2b/a-guide-to-json-using-c-a48039124f3a 
// Printing might be prettier with a different library

using json = nlohmann::json;

class TableWriter {
    public:
        TableWriter();
        virtual ~TableWriter();
        // Types are SQL type names
        void add_table(std::string &table_name, std::vector<std::pair<std::string, std::string>>& column_names_and_types, const std::vector<uint32_t> primary_key_col_idx);
        void add_index(std::string &table_name, std::string &index_name, const std::vector<uint32_t> &index_col_idx);
        void add_row(std::string &table_name, const std::vector<std::string> &values);
        void flush(std::string &file_name);
    private:
        json out_tables;
        std::map<std::string, json> tables;
};

TableWriter::TableWriter(){
    //out_tables["tables"] = {};
}

TableWriter::~TableWriter(){
}

//TODO: May want to add column constraints. --> change columns to be tuples (name, type, constraint)
//May want to add table constraints
//May want to add foreign key too

//--> How would these be enforced? DB engine would say an operation failed -> app should abort? (app abort, not system abort) 

void TableWriter::add_table(std::string &table_name, std::vector<std::pair<std::string, std::string>>& column_names_and_types, const std::vector<uint32_t> primary_key_col_idx ){
    json &table = tables[table_name];
    table["table_name"] = table_name; //Not needed for parsing, but can make it easier to search for "table_name" if trying to read Json file.
    table["column_names_and_types"] = json(column_names_and_types);  //Note: data type length should be part of type. 
    table["primary_key_col_idx"] = json(primary_key_col_idx);
    table["rows"] = {};
}

void TableWriter::add_index(std::string &table_name, std::string &index_name, const std::vector<uint32_t> &index_col_idx){
    json &table = tables[table_name];
    json &indexes = table["indexes"];
    indexes[index_name] = json(index_col_idx);
}

void TableWriter::add_row(std::string &table_name, const std::vector<std::string> &values) { //const std::vector<(std::string) &columns, const std::vector<uint32_t> primary_key_col_idx){
    // json row;
    // row["values"] = json(values);
    //row["columns"] = json(columns); //don't need to store... already part of table.
    //row["primary_key_col_idx"] = json(primary_key_col_idx); //don't need to store... already part of table.

    json &table = tables[table_name];
    json &rows = table["rows"];
    rows.push_back(json(values));
}

void TableWriter::flush(std::string &file_name){
    for(auto &[name, table]: tables){
        //out_tables["tables"].push_back(table);
        out_tables[name] = table;
    }

    //std::cerr << out_tables.dump(2) << std::endl; //TESTING

    std::ofstream generated_tables;
    generated_tables.open(file_name, std::ios::trunc);
    generated_tables << out_tables.dump(2); //Formatting with dump: https://cppsecrets.com/users/4467115117112114971069711297105100105112971089764103109971051084699111109/C00-Jsondump.php
    generated_tables.close();
}


