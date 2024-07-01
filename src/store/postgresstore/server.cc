/***********************************************************************
 *
 * Copyright 2021 Florian Suri-Payer <fsp@cs.cornell.edu>
 *                Yunhao Zhang <yz2327@cornell.edu>
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
#include "store/postgresstore/server.h"
//#include "store/potsgresstore/common.h"
#include "store/common/transaction.h"
#include <iostream>
#include <sys/time.h>
#include <cstdlib>
#include <fmt/core.h>
#include <regex>
#include <chrono>
#include <ctime>

namespace postgresstore {

// Shir: pick the right number
const uint64_t number_of_threads=8;

using namespace std;

Server::Server() {

  tp->AddIndexedThreads(number_of_threads);

  //separate for local configuration we set up different db name for each servers, otherwise they can share the db name
  std::string db_name = "db1";
 
  // password should match the one created in Pequin-Artifact/pg_setup/postgres_service.sh script
  // port should match the one that appears when executing "pg_lsclusters -h"
  std::string connection_str = "host=localhost user=pequin_user password=123 dbname=" + db_name + " port=5432";
  //std::string connection_str = "sudo -u postgres psql -U postgres --host localhost:5432 -d bench -c 'sql'";// + host + ":" + port + " -d bench -c '" + sql + "'"; 
  std::cerr<<"Shir: 33333333333333333333333333333333333333333333333333333333333\n";
  std::cerr << connection_str <<"\n";
  Debug("Shir: 33333333333333333333333333333333333333333333333333333333333");


  connectionPool = tao::pq::connection_pool::create(connection_str);

  Debug("PostgreSQL connection established");
  
}

Server::~Server() {}



void Server::CreateTable(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, const std::vector<uint32_t> &primary_key_col_idx){
  // //Based on Syntax from: https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-create-table/ 

  Debug("Shir: Creating table!");

  //NOTE: Assuming here we do not need special descriptors like foreign keys, column condidtions... (If so, it maybe easier to store the SQL statement in JSON directly)
  //UW_ASSERT(!column_data_types.empty());
  //UW_ASSERT(!primary_key_col_idx.empty());

  std::string sql_statement("CREATE TABLE");
  sql_statement += " " + table_name;
  
  sql_statement += " (";
  for(auto &[col, type]: column_data_types){
    std::string p_key = (primary_key_col_idx.size() == 1 && col == column_data_types[primary_key_col_idx[0]].first) ? " PRIMARY KEY": "";
    sql_statement += col + " " + type + p_key + ", ";
  }

  sql_statement.resize(sql_statement.size() - 2); //remove trailing ", "

  if(primary_key_col_idx.size() > 1){
    sql_statement += ", PRIMARY KEY ";
    if(primary_key_col_idx.size() > 1) sql_statement += "(";

    for(auto &p_idx: primary_key_col_idx){
      sql_statement += column_data_types[p_idx].first + ", ";
    }
    sql_statement.resize(sql_statement.size() - 2); //remove trailing ", "

    if(primary_key_col_idx.size() > 1) sql_statement += ")";
  }
  
  
  sql_statement +=");";

  std::cerr << "Create Table: " << sql_statement << std::endl;

  this->exec_statement(sql_statement);


}

void Server::CreateIndex(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, const std::string &index_name, const std::vector<uint32_t> &index_col_idx){
  // Based on Syntax from: https://www.postgresqltutorial.com/postgresql-indexes/postgresql-create-index/ and  https://www.postgresqltutorial.com/postgresql-indexes/postgresql-multicolumn-indexes/
  // CREATE INDEX index_name ON table_name(a,b,c,...);
  Debug("Shir: Creating index!");

  UW_ASSERT(!column_data_types.empty());
  UW_ASSERT(!index_col_idx.empty());
  UW_ASSERT(column_data_types.size() >= index_col_idx.size());

  std::string sql_statement("CREATE INDEX");
  sql_statement += " " + index_name;
  sql_statement += " ON " + table_name;

  sql_statement += "(";
  for (auto &i_idx : index_col_idx) {
    sql_statement += column_data_types[i_idx].first + ", ";
  }
  sql_statement.resize(sql_statement.size() - 2); // remove trailing ", "

  sql_statement += ");";

  this->exec_statement(sql_statement);
}

void Server::LoadTableData(const std::string &table_name, const std::string &table_data_path, 
    const std::vector<std::pair<std::string, std::string>> &column_names_and_types, const std::vector<uint32_t> &primary_key_col_idx){
  Debug("Shir: Load Table data!");
  std::cerr<<"Shir: Load Table data\n";
  std::cerr << "Shir data path is " << table_data_path << std::endl;
  auto start = std::chrono::system_clock::now();
  std::time_t start_time = std::chrono::system_clock::to_time_t(start);
  std::cerr << "start time is "  << std::ctime(&start_time) << std::endl;

  std::string copy_table_statement = fmt::format("COPY {0} FROM '{1}' DELIMITER ',' CSV HEADER", table_name, table_data_path);
  std::thread t1([this, copy_table_statement]() { this->exec_statement(copy_table_statement); });
  t1.detach();
  //this->exec_statement(copy_table_statement);
  
  // Some computation here
  auto end = std::chrono::system_clock::now();
  std::time_t end_time = std::chrono::system_clock::to_time_t(end);
  std::cerr << "end time is " << std::ctime(&end_time) << std::endl;
}

void Server::LoadTableRows(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, const row_segment_t *row_segment, const std::vector<uint32_t> &primary_key_col_idx, int segment_no, bool load_cc){
  Debug("Shir: Load Table rows!");
  std::cerr<< "Shir: Load Table rows!\n";
  std::string sql_statement = this->GenerateLoadStatement(table_name,*row_segment,0);
  this->exec_statement(sql_statement);
}

//!!"Deprecated" (Unused)
void Server::LoadTableRow(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, const std::vector<std::string> &values, const std::vector<uint32_t> &primary_key_col_idx ){
    Debug("Shir: Load Table row!");
}


void Server::exec_statement(const std::string &sql_statement) {
  auto connection = connectionPool->connection();
  //std::thread t1([connection, sql_statement]() { connection->execute(sql_statement); });
  connection->execute(sql_statement);
}


std::string Server::GenerateLoadStatement(const std::string &table_name, const std::vector<std::vector<std::string>> &row_segment, int segment_no){
  std::string load_statement = fmt::format("INSERT INTO {0} VALUES ", table_name);
  for(auto &row: row_segment){
        load_statement += "(";
            for(int i = 0; i < row.size(); ++i){
               load_statement += row[i] + ", ";
            }
        load_statement.resize(load_statement.length()-2); //remove trailing ", "
        load_statement += "), ";
    }
    load_statement.resize(load_statement.length()-2); //remove trailing ", "
    load_statement += ";";
    Debug("Generate Load Statement for Table %s. Segment %d. Statement: %s", table_name.c_str(), segment_no, load_statement.substr(0, 1000).c_str());
    std::cerr<< "Shir: Generate Load Statement for Tab!\n";

    return load_statement;
}


Stats &Server::GetStats() {
  return stats;
}

Stats* Server::mutableStats() {
  return &stats;
}

}
