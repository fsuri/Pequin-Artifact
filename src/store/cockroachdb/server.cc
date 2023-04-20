// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/cockroachdb/server.cc:
 *
 *  @author Benton Li <cl2597@cornell.edu>
 *
 **********************************************************************/

#include "store/cockroachdb/server.h"

#include <unistd.h>

#include <iostream>
#include <string>
#include <fmt/core.h>

namespace cockroachdb {

using namespace std;

void Server::exec_sql(std::string sql) {
  std::string crdb_command = "cockroach sql --insecure --host=" + host +
                             std::string() + ":" + port + " --execute=\"" +
                             sql + "\"";
  Notice(crdb_command.c_str());
  int status = system(crdb_command.c_str());
}

Server::Server(const transport::Configuration &config, KeyManager *keyManager,
               int groupIdx, int idx, int numShards, int numGroups)
    : config(config),
      keyManager(keyManager),
      groupIdx(groupIdx),
      idx(idx),
      id(groupIdx * config.n + idx),
      numShards(numShards),
      numGroups(numGroups) {
  zone = config.replica(groupIdx, idx).host;
  port = config.replica(groupIdx, idx).port;
  int status = 0;
  char host_name[HOST_NAME_MAX];
  int result;
  result = gethostname(host_name, HOST_NAME_MAX);
  if (result) {
    Panic("Unable to get host name for CRDB");
  }
  cout << result << endl;
  // remove site
  std::string site(host_name);

  site.replace(site.find(zone), zone.length(), "");
  host = std::string(host_name);

  /**
   * --advertise-addr determines which address to tell other nodes to use.
   * --listen-addr determines which address(es) to listen on for connections
   *   from other nodes and clients.
   */
  std::string start_script = "cockroach start";
  std::string security_flag = " --insecure";
  std::string listen_addr_flag = " --listen-addr=" + host + ":" + port;
  std::string sql_addr_flag = " --advertise-sql-addr=" + host + ":" + port;
  std::string advertise_flag = " --advertise-addr=" + host + ":" + port;
  std::string join_flag = " --join=";
  std::string load_flag = " --join=";

  // Naive implementation: join all node
  for (int i = 0; i < numGroups; i++) {
    for (int j = 0; j < config.n; j++) {
      transport::ReplicaAddress join_address = config.replica(i, j);
      join_flag =
          join_flag + join_address.host + site + ":" + join_address.port + ",";
    }
  }
  // Remove last comma
  join_flag.pop_back();

  // TODO: better port number
  std::string http_addr_flag =
      " --http-addr=" + host + ":" + std::to_string(8069 + id);
  std::string store_flag_disk =
      " --store=~/mnt/extra/experiments/store/cockroachdb/crdb_node" +
      std::to_string(id);

  // In memory cluster.
  std::string store_flag_mem = " --store=type=mem,size=90%";

  // std::string log_flag =
  //     " --log=\"sinks: {file-groups: {ops: {channels: [OPS, HEALTH, "
  //     "SQL_SCHEMA], filter: ERROR}}}\"";

  std::string log_flag = " --log-config-file=./store/cockroachdb/logs.yaml";

  // TODO : Add encryption

  // region = shard group number
  // zone = host name
  std::string locality_flag =
      " --locality=region=" + std::to_string(groupIdx) + ",zone=" + zone;

  // TODO: Add load balancer
  std::string other_flags = " --background ";

  std::string script_parts[] = {join_flag, security_flag,
                                listen_addr_flag,  // for nodes and clients
                                // advertise_flag,    // for nodes
                                sql_addr_flag,   // for client's sql
                                http_addr_flag,  // for  DB Console
                                store_flag_mem, log_flag, locality_flag,
                                other_flags};

  for (std::string part : script_parts) {
    start_script = start_script + part;
  }

  // start server
  Notice("Debug start initalizing");
  status = system((start_script + "&").c_str());
  Notice("Cockroach node %d started. Listening %s:%s", id, host.c_str(),
         port.c_str());

  if (idx == numShards - 1) {
    // If node is the last one in the group, serve as load balancer
    std::string proxy_script =
        "cockroach gen haproxy --insecure --host=" + host + ":" + port +
        " --locality=region=" + std::to_string(groupIdx);
    status = system(proxy_script.c_str());
  }
  // If happens to be the last one, init server
  if (id == numGroups * config.n - 1) {
    std::string init_script =
        "cockroach init --insecure --host=" + host + ":" + port;

    status = system(init_script.c_str());
    Notice("Cluster initliazed by node %d. Listening %s:%s", id, host.c_str(),
           port.c_str());

    Server::exec_sql(
        "CREATE TABLE IF NOT EXISTS datastore ( key_ TEXT PRIMARY KEY, val_ "
        "TEXT NOT NULL)");
  }
}

void Server::Load(const string &key, const string &value,
                  const Timestamp timestamp) {
  std::string sql_statement("INSERT INTO datastore(key_, val_) VALUES(");
  sql_statement += ("\'" + key + "\', \'" + value +
                    "\') ON CONFLICT (key_) "
                    "DO UPDATE SET val_ = " +
                    "\'" + value + "\'");
  Server::exec_sql(sql_statement);
}

void Server::CreateTable(
    const std::string &table_name,
    const std::vector<std::pair<std::string, std::string>> &column_data_types,
    const std::vector<uint32_t> primary_key_col_idx) {
  std::string sql_statement("CREATE TABLE IF NOT EXISTS");
  sql_statement += " " + table_name;
  UW_ASSERT(!column_data_types.empty());
  UW_ASSERT(!primary_key_col_idx.empty());
  sql_statement += " (";
  for (auto &[col, type] : column_data_types) {
    std::string p_key = (primary_key_col_idx.size() == 1 &&
                         col == column_data_types[primary_key_col_idx[0]].first)
                            ? " PRIMARY KEY"
                            : "";
    sql_statement += col + " " + type + p_key + ", ";
  }
  sql_statement += "PRIMARY KEY ";
  if (primary_key_col_idx.size() > 1) sql_statement += "(";
  for (auto &p_idx : primary_key_col_idx) {
    sql_statement += (column_data_types[p_idx].first + ", ");
  }
  sql_statement.resize(sql_statement.size() - 2);  // remove trailing ", "
  if (primary_key_col_idx.size() > 1) sql_statement += ")";
  sql_statement += ");";
  Server::exec_sql(sql_statement);
}

void Server::LoadTableData(const std::string &table_name, const std::string &table_data_path, const std::vector<uint32_t> &primary_key_col_idx){
    //Syntax based on: https://www.cockroachlabs.com/docs/stable/import-into.html
    std::string copy_table_statement_crdb = fmt::format("IMPORT INTO {0} CSV DATA {1} WITH skip = '1'", table_name, table_data_path); //FIXME: does one need to specify column names? Target columns don't appear to be enforced

  exec_sql(copy_table_statement_crdb, serverAddress);
  
}

void Server::LoadTableRow(
    const std::string &table_name,
    const std::vector<std::pair<std::string, std::string>> &column_data_types,
    const std::vector<std::string> &values,
    const std::vector<uint32_t> primary_key_col_idx) {
  std::string sql_statement("INSERT INTO");
  sql_statement += " " + table_name;
  UW_ASSERT(!values.empty());  // Need to insert some values...
  UW_ASSERT(column_data_types.size() ==
            values.size());  // Need to specify all columns to insert into
  sql_statement += " (";
  for (auto &[col, _] : column_data_types) {
    sql_statement += col + ", ";
  }
  sql_statement.resize(sql_statement.size() - 2);  // remove trailing ", "
  sql_statement += ")";
  sql_statement += " VALUES (";
  for (auto &val : values) {
    sql_statement += "\'" + val + "\', ";
  }
  sql_statement.resize(sql_statement.size() - 2);  // remove trailing ", "
  sql_statement += ");";
  Server::exec_sql(sql_statement);
}

void Server::LoadTable(
    const std::string &table_name,
    const std::vector<std::pair<std::string, std::string>> &column_data_types,
    const std::vector<std::string> &values,
    const std::vector<uint32_t> primary_key_col_idx,
    const std::string &csv_file_name) {
  // IMPORT INTO employees (c1, c2, c3, ..., cn)
  //  CSV DATA (
  //    'xxx.csv'
  //  );
  std::string sql_statement("IMPORT INTO ");
  sql_statement += table_name;
  sql_statement += " (";
  for (auto &[col, _] : column_data_types) {
    sql_statement += col + ", ";
  }
  sql_statement.resize(sql_statement.size() - 2);  // remove trailing ", "
  sql_statement += ") CSV DATA (\'";
  sql_statement += csv_file_name;
  sql_statement += "'\');";
  Server::exec_sql(sql_statement);
}

Stats &Server::GetStats() { return stats; }

Server::~Server() { system("pkill -9 -f cockroach"); }

}  // namespace cockroachdb