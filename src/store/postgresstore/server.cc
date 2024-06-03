// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/postgresstore/server.cc:
 *
 *  @author Liam Arzola <lma77@cornell.edu>
 *
 **********************************************************************/

#include "store/postgresstore/server.h"

#include "lib/assert.h"
#include <fmt/core.h>
#include <unistd.h>
#include <filesystem>

#include <iostream>
#include <string>

namespace postgresstore {

void Server::exec_sql(std::string sql) {
  std::string psql_command = "sudo -u postgres psql -U postgres --host " + host + ":" + port + " -d bench -c '" + sql + "'";
  Notice("Issuing SQL command %s", psql_command.c_str());
  int status = system(psql_command.c_str());
  UW_ASSERT(status == 0);
  stats.Increment("sql_commands_executed", 1);
}

Server::Server(const transport::Configuration &config)
    : config(config) {
  const int groupIdx = 0;
  const int idx = 0;
  Notice("Starting PostgreSQL DB");
  zone = config.replica(groupIdx, idx).host;
  port = config.replica(groupIdx, idx).port;
  Notice("Finished starting replica");
  int status = 0;
  char host_name[HOST_NAME_MAX];
  int result;
  result = gethostname(host_name, HOST_NAME_MAX);
  if (result) {
    Panic("Unable to get host name for CRDB");
  }
  // cout << result << endl;
  //  remove site
  std::string site(host_name);

  try {
    site = site.substr(site.find('.'));
  } catch (std::out_of_range &e) {
    site = "";
  }
  host = std::string(host_name);

  /**
   * --advertise-addr determines which address to tell other nodes to use.
   * --listen-addr determines which address(es) to listen on for connections
   *   from other nodes and clients.
   */
  std::string add_postgres_user_cmd = "sudo useradd -U -s /bin/bash postgres";
  std::string create_cluster_cmd = "sudo pg_createcluster 12 main --start";
  std::string start_cmd = "sudo pg_ctlcluster 12 main start";
  std::string set_port_cmd = fmt::format("sudo sed -i '/^#port =\\|^port =/ s/^#*port =.*/port = {}/' /etc/postgresql/12/main/postgresql.conf", port);
  std::string set_ip_cmd = fmt::format("sudo sed -i '/^#listen_addresses =\\|^listen_addresses =/ s/^#*listen_addresses =.*/listen_addresses = '{}'/' /etc/postgresql/12/main/postgresql.conf", host);
  std::string restart_pg = "sudo systemctl restart postgresql";
  std::string create_db = fmt::format("sudo -u postgres createdb bench", host, port);
  // start server
  Notice("Debug start initalizing");

  status = system(add_postgres_user_cmd.c_str());
  UW_ASSERT(status == 0);
  status = system(create_cluster_cmd.c_str());
  UW_ASSERT(status == 0);
  status = system(start_cmd.c_str());
  UW_ASSERT(status == 0);
  status = system(set_port_cmd.c_str());
  UW_ASSERT(status == 0);
  status = system(set_ip_cmd.c_str());
  UW_ASSERT(status == 0);
  status = system(restart_pg.c_str());
  UW_ASSERT(status == 0);
  status = system(create_db.c_str());
  UW_ASSERT(status == 0);

  Notice("PostgreSQL node started. Listening %s:%s", host.c_str(),
         port.c_str());
}

void Server::Load(const string &key, const string &value,
                  const Timestamp timestamp) {
  std::string sql_statement("INSERT INTO bench(key_, val_) VALUES(");
  sql_statement += ("\'" + key + "\', \'" + value +
                    "\') ON CONFLICT (key_) "
                    "DO UPDATE SET val_ = " +
                    "\'" + value + "\'");
  Server::exec_sql(sql_statement);
  stats.Increment("num_keys_loaded", 1);
}

void Server::CreateTable(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, 
      const std::vector<uint32_t> &primary_key_col_idx) {
  std::string sql_statement("CREATE TABLE IF NOT EXISTS");
  sql_statement += " " + table_name;
  UW_ASSERT(!column_data_types.empty());
  sql_statement += " (";
  for (auto &[col, type] : column_data_types) {
    sql_statement += col + " " + type + ", ";
  }
  if (!primary_key_col_idx.empty()) {
    sql_statement += "PRIMARY KEY ";
    sql_statement += "(";
    for (auto &p_idx : primary_key_col_idx) {
      sql_statement += (column_data_types[p_idx].first + ", ");
    }
    sql_statement.resize(sql_statement.size() - 2);  // remove trailing ", "
    sql_statement += ")";
  } else {
    sql_statement.resize(sql_statement.size() - 2);  // remove trailing ", "
  }
  sql_statement += ");";

  Notice("Create Table: %s", table_name.c_str());
  Server::exec_sql(sql_statement);
}

void Server::LoadTableData(const std::string &table_name, const std::string &table_data_path, 
      const std::vector<std::pair<std::string, std::string>> &column_names_and_types, const std::vector<uint32_t> &primary_key_col_idx) {
  Notice("Loading table from path: %s", table_data_path.c_str());

  std::string copy_table_statement = fmt::format(
    "tail -n +2 {} | sudo -u postgres psql -d bench -c \"\\COPY {} FROM stdin DELIMITER ',' CSV\"",
    table_data_path, table_name);
  int status = system(copy_table_statement.c_str());

  Notice("Load Table Data for table: %s", table_name.c_str());
  stats.Increment("TablesLoaded", 1);
}

void Server::CreateIndex(
    const std::string &table_name,
    const std::vector<std::pair<std::string, std::string>> &column_data_types,
    const std::string &index_name, const std::vector<uint32_t> &index_col_idx) {
  // CREATE INDEX index_name ON table_name(a,b,c,...);

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
  sql_statement.resize(sql_statement.size() - 2);  // remove trailing ", "

  sql_statement += ");";

  exec_sql(sql_statement);
}

void Server::LoadTableRows(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, 
      const std::vector<row_t> *values, const std::vector<uint32_t> &primary_key_col_idx, int segment_no, bool load_cc) {
  std::string sql_statement("INSERT INTO");
  sql_statement += " " + table_name;
  UW_ASSERT(!values->empty());  // Need to insert some values...
  UW_ASSERT(column_data_types.size() ==
            values->size());  // Need to specify all columns to insert into
  sql_statement += " (";
  for (auto &[col, _] : column_data_types) {
    sql_statement += col + ", ";
  }
  sql_statement.resize(sql_statement.size() - 2);  // remove trailing ", "
  sql_statement += ")";
  sql_statement += " VALUES (";
  for (auto &val : *values) {
    for (auto &col : val) {
      sql_statement += "\'" + col + "\', ";
    }
    break;
  }
  sql_statement.resize(sql_statement.size() - 2);  // remove trailing ", "
  sql_statement += ");";
  Notice("Load Table Rows for table: %s", table_name.c_str());
  Notice("SQL Statement: %s", sql_statement.c_str());
  Server::exec_sql(sql_statement);
}

Server::~Server() { 
  Server::exec_sql("DROP DATABASE bench;");
  system("sudo systemctl stop postgresql@12-main"); 
}

}  // namespace postgresstore