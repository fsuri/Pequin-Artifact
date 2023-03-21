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
#include "json_table_writer.h"

using json = nlohmann::json;


TableWriter::TableWriter(){
    tables["tables"] = {};
}

TableWriter::~TableWriter(){
}

void TableWriter::add_table(std::string &table_name, std::vector<std::pair<std::string&, std::string&>>& column_names__and_types, const std::vector<uint32_t> primary_key_col_idx ){
    json table;
    table["table_name"] = table_name;
    table["column_names_and_types"] = json(column_names_and_types);
    table["primary_key_col_idx"] = json(primary_key_col_idx);

    tables["tables"].push_back(table);
}

void TableWriter::add_row(std::string &table_name, const std::vector<std::string> &values) { //const std::vector<(std::string) &columns, const std::vector<uint32_t> primary_key_col_idx){
    json row;
    row["values"] = json(values);
    //row["columns"] = json(columns); //don't need to store... already part of table.
    //row["primary_key_col_idx"] = json(primary_key_col_idx); //don't need to store... already part of table.

    json &rows = table["table_name"]["rows"];
    rows.push_back(row);
}

void TableWriter::flush(std::string &file_name){
    std::ofstream generated_tables;
    generated_tables.open(file_name, ios::trunc);
    generated_tables << tables; // << endl;
    generated_tables.close();
}




// TABLE WRITER :

//Class TableWriter:
// open file

//void add_table_

//void add_row


// TABLE READER: --> Probably easiest to move this into server?? Or should this generic interface just return a vector of arguments. (Seems suboptimal)

// Class TableReader:
//open file

//Loop through:
// for each row: call Server::add_row

