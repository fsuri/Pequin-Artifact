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

using json = nlohmann::json;

class TableWriter {
    public:
        TableWriter();
        virtual ~TableWriter();
        void add_table(std::string &table_name, std::vector<std::pair<std::string, std::string>>& column_names_and_types, const std::vector<uint32_t> primary_key_col_idx);
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

//TODO: Can remove table_name? Can remove "values"?
void TableWriter::add_table(std::string &table_name, std::vector<std::pair<std::string, std::string>>& column_names_and_types, const std::vector<uint32_t> primary_key_col_idx ){
    json &table = tables[table_name];
    table["table_name"] = table_name;
    table["column_names_and_types"] = json(column_names_and_types);
    table["primary_key_col_idx"] = json(primary_key_col_idx);
    table["rows"] = {};
}

void TableWriter::add_row(std::string &table_name, const std::vector<std::string> &values) { //const std::vector<(std::string) &columns, const std::vector<uint32_t> primary_key_col_idx){
    // json row;
    // row["values"] = json(values);
    //row["columns"] = json(columns); //don't need to store... already part of table.
    //row["primary_key_col_idx"] = json(primary_key_col_idx); //don't need to store... already part of table.

    json &table = tables[table_name];
    json &rows = table["rows"];
    // rows.push_back(row);
    rows.push_back(json(values));
}

void TableWriter::flush(std::string &file_name){
    for(auto &[name, table]: tables){
        //out_tables["tables"].push_back(table);
        out_tables[name] = table;
    }

    std::cerr << out_tables << std::endl;

    std::ofstream generated_tables;
    generated_tables.open(file_name, std::ios::trunc);
    generated_tables << out_tables; // << endl;
    generated_tables.close();
}


