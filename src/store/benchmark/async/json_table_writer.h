#include<iostream>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include <stdlib.h>

#include <sys/types.h>
#include <sys/stat.h>

#include <cstring>
#include <vector>
#include <iomanip>
#include <fmt/core.h>
#include <fmt/ranges.h>

#include <fstream>
#include <nlohmann/json.hpp>
//https://github.com/nlohmann/json
//Overview of other popular JSON c++ libraries: https://medium.com/ml2b/a-guide-to-json-using-c-a48039124f3a 
// Printing might be prettier with a different library

using json = nlohmann::json;

class TableWriter {
    public:
        TableWriter(const std::string &file_name);
        virtual ~TableWriter();
        void add_table(const std::string &table_name, const std::vector<std::pair<std::string, std::string>>& column_names_and_types, const std::vector<uint32_t> primary_key_col_idx, bool add_data = true);
        void add_index(const std::string &table_name, const std::string &index_name, const std::vector<uint32_t> &index_col_idx);
        void add_row(const std::string &table_name, const std::vector<std::string> &values);
        void flush();
    private:
        std::string file_name;
        json out_tables;
        std::map<std::string, json> tables;
        std::map<std::string, std::ofstream> table_rows; //map from table_name to csv file name: //TODO: change map to be to a stream.. TODO: Pass file name to TableWriter()
};

TableWriter::TableWriter(const std::string &file_name): file_name(file_name){
    //out_tables["tables"] = {};
    mkdir((file_name + "-data").data(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
}

TableWriter::~TableWriter(){
}

//TODO: May want to add column constraints. --> change columns to be tuples (name, type, constraint)
//May want to add table constraints
//May want to add foreign key too

//--> How would these be enforced? DB engine would say an operation failed -> app should abort? (app abort, not system abort) 

void TableWriter::add_table(const std::string &table_name, const std::vector<std::pair<std::string, std::string>>& column_names_and_types, const std::vector<uint32_t> primary_key_col_idx, bool add_data){
    json &table = tables[table_name];
    table["table_name"] = table_name; //Not needed for parsing, but can make it easier to search for "table_name" if trying to read Json file.
    table["column_names_and_types"] = json(column_names_and_types);  //Note: data type length should be part of type. 
    table["primary_key_col_idx"] = json(primary_key_col_idx);
    //table["rows"] = {};

    if(!add_data) return;
    // Create a new stream to a CSV file. 
    std::string file_path(file_name + "-data"+ "/" + table_name + "-data.csv");
    table_rows[table_name].open(file_path, std::ios::trunc);
    table["row_data_path"] = file_path;
    //Optional: Add header with column names: --> If so, Copy/Insert INTO must account for the header.
    table_rows[table_name] << fmt::format("{}\n", fmt::join(column_names_and_types, ","));
}

void TableWriter::add_index(const std::string &table_name, const std::string &index_name, const std::vector<uint32_t> &index_col_idx){
    json &table = tables[table_name];
    json &indexes = table["indexes"];
    indexes[index_name] = json(index_col_idx);
}

void TableWriter::add_row(const std::string &table_name, const std::vector<std::string> &values) { //const std::vector<(std::string) &columns, const std::vector<uint32_t> primary_key_col_idx){
   
    //Note: No longer write data to json. Write data only to CSV
    // json &table = tables[table_name];
    // json &rows = table["rows"];
    // rows.push_back(json(values));

    //Access the respective table CSV file and write the row.    //After row is done, add a linebreak. FIXME: last row will be empty, check that this works fine..
    std::ofstream &table_row = table_rows[table_name]; 
    std::string row_vals = fmt::format("{}\n", fmt::join(values, ","));
    table_row << row_vals; //std:endl; --> endl flushes output buffer everytime.
}

//NOTE: pass as file_name without the ".json" suffix
void TableWriter::flush(){
    for(auto &[name, table]: tables){
        //out_tables["tables"].push_back(table);
        out_tables[name] = table;

        table_rows[name].close();
    }

    //std::cerr << out_tables.dump(2) << std::endl; //TESTING

    
    std::ofstream generated_tables;
    // generated_tables.open(file_name + ".json", std::ios::trunc);
    // generated_tables << out_tables.dump(2); //Formatting with dump: https://cppsecrets.com/users/4467115117112114971069711297105100105112971089764103109971051084699111109/C00-Jsondump.php
    // generated_tables.close();


    // //Write another file without row data (for client consumption).     //Alternatively, write one file with table meta, and one file for table rows. Let client read meta, let server read both.
    // for(auto &[name, table]: tables){
    //     //out_tables["tables"].push_back(table);
    //     out_tables[name]["rows"].clear();
    // }
    // generated_tables;

    //Note: Only write Schema, no data. (Data now lives in CSV)
    generated_tables.open(file_name + "-tables-schema.json", std::ios::trunc);
    generated_tables << out_tables.dump(2); //Formatting with dump: https://cppsecrets.com/users/4467115117112114971069711297105100105112971089764103109971051084699111109/C00-Jsondump.php
    generated_tables.close();
}


