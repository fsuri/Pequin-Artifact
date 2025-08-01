// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/cockroachdb/server.cc:
 *
 *  @author Benton Li <cl2597@cornell.edu>
 *  @author Liam Arzola <lma77@cornell.edu>
 *
 **********************************************************************/

#include "store/cockroachdb/server.h"

#include "lib/assert.h"
#include <fmt/core.h>
#include <unistd.h>
#include <filesystem>

#include <iostream>
#include <string>

namespace cockroachdb {

using namespace std;

void Server::exec_sql(std::string sql) {
  std::string crdb_command = "cockroach sql --insecure --host=" + host +
                             std::string() + ":" + port + " --execute=\"" +
                             sql + "\"";
  Notice("Issuing SQL command %s", crdb_command.c_str());
  int status = system(crdb_command.c_str());
  stats.Increment("sql_commands_executed", 1);
}

Server::Server(const transport::Configuration &config, KeyManager *keyManager,
               int groupIdx, int idx, int numShards, int numGroups, std::string &table_registry_path)
    : config(config),
      keyManager(keyManager),
      groupIdx(groupIdx),
      idx(idx),
      id(groupIdx * config.n + idx),
      numShards(numShards),
      numGroups(numGroups) {

  Notice("Starting Replica: g: %d, idx: %d, id: %d", groupIdx, idx, id);
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
  system("pkill -9 -f \"cockroach start\"");
  system("pkill -9 -f \"python3 -m http.server\"");
  /**
   * --advertise-addr determines which address to tell other nodes to use.
   * --listen-addr determines which address(es) to listen on for connections
   *   from other nodes and clients.
   */
  std::string start_cmd;
  if (config.n > 1) {
    start_cmd = "cockroach start";
  } else {
    start_cmd = "cockroach start-single-node";
  }
  std::string security_flag = " --insecure";
  std::string listen_addr_flag = " --listen-addr=" + host + ":" + port;
  std::string join_flag;
  if (config.n > 1) {
    join_flag = " --join=";
  } else {
    join_flag = "";
  }

  if (config.n > 1) {
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
  }
  // TODO: better port number
  std::string http_addr_flag =
      " --http-addr=" + host + ":" + std::to_string(8069 + id);
  std::string store_flag_disk =
      " --store=~/mnt/extra/experiments/store/cockroachdb/crdb_node" +
      std::to_string(id);

  // In memory cluster.
  std::string store_flag_mem = " --store=type=mem,size=1.0";

  std::string log_flag =
      " --log=\"sinks: {file-groups: {ops: {channels: [OPS, HEALTH, "
      "SQL_SCHEMA], filter: FATAL}}, stderr: {filter: NONE}}\"";
  // std::string log_flag = " --log-config-file=./store/cockroachdb/logs.yaml";

  // TODO : Add encryption

  // region = shard group number
  // zone = host name
  std::string locality_flag;
  if (config.n > 1) {
   locality_flag =
        " --locality=region=" + std::to_string(groupIdx) + ",zone=" + zone;
  }
  // TODO: Add load balancer
  std::string other_flags = " --background ";

  std::string script_parts[] = {join_flag, security_flag,
                                listen_addr_flag,  // for nodes and clients
                                // advertise_flag,    // for nodes
                                http_addr_flag,  // for  DB Console
                                log_flag, store_flag_mem, locality_flag,
                                other_flags};

  for (std::string part : script_parts) {
    start_cmd = start_cmd + part;
  }

  // start server
  Notice("Debug start initalizing");

  Notice("Invoke start script: %s", start_cmd.c_str());

  status = system((start_cmd + "&").c_str());
  Notice("Cockroach node %d started. Listening %s:%s", id, host.c_str(),
         port.c_str());

  // If happens to be the last one, init server
  if (id == numGroups * config.n - 1) {
    std::string init_cmd =
        "cockroach init --insecure --host=" + host + ":" + port;
    Notice("Invoke init script: %s", init_cmd.c_str());
    status = system(init_cmd.c_str());
    Notice("Cluster initliazed by node %d. Listening %s:%s", id, host.c_str(),
           port.c_str());

    // Server::exec_sql(
    //     "CREATE TABLE IF NOT EXISTS datastore ( key_ TEXT PRIMARY KEY, val_ "
    //     "TEXT NOT NULL)");
    std::string lock_timeout_cmd = "ALTER DATABASE defaultdb SET lock_timeout = '50ms';";
    Notice("Issuing SQL command %s", lock_timeout_cmd.c_str());
    exec_sql(lock_timeout_cmd);
    std::string set_num_replicas = "ALTER RANGE default CONFIGURE ZONE USING range_min_bytes = 0, range_max_bytes = 134217728, num_replicas = 1;";
//    std::string set_num_replicas = "ALTER RANGE default CONFIGURE ZONE USING num_replicas = 1;";
    Notice("Issuing SQL command %s", set_num_replicas.c_str());
    exec_sql(set_num_replicas);
    std::string enable_merge_queue = "SET CLUSTER SETTING kv.range_merge.queue_enabled = true;";
    Notice("Issuing SQL command %s", enable_merge_queue.c_str());
    exec_sql(enable_merge_queue);
    // Experimental: reduce Raft batch size
    // std::string set_batch = "SET CLUSTER SETTING kv.transaction.write_pipelining.max_batch_size = 1;";
    // Notice("Issuing SQL command %s", set_batch.c_str());
    // exec_sql(set_batch);
  }
  if (idx == config.n - 1) {
    // If node is the last one in the group, serve as load balancer
    std::string proxy_cmd =
        "cockroach gen haproxy --insecure --host=" + host + ":" + port +
        " --locality=region=" + std::to_string(groupIdx);

    Notice("Invoke proxy script: %s", proxy_cmd.c_str());
    status = system(proxy_cmd.c_str());
  }

  auto registry_path = std::filesystem::path(table_registry_path);
  if (registry_path.has_filename()) {
    registry_path.remove_filename();
  }
  std::string registry_path_string = registry_path;
  std::string file_server_cmd = "cd " + registry_path_string + "; python3 -m http.server 3000 &";
  status = system(file_server_cmd.c_str());
  Notice("File server started on port 3000 at path: %s", registry_path_string.c_str());
}

void Server::Load(const string &key, const string &value,
                  const Timestamp timestamp) {
  std::string sql_statement("INSERT INTO datastore(key_, val_) VALUES(");
  sql_statement += ("\'" + key + "\', \'" + value +
                    "\') ON CONFLICT (key_) "
                    "DO UPDATE SET val_ = " +
                    "\'" + value + "\'");
  Server::exec_sql(sql_statement);
  stats.Increment("num_keys_loaded", 1);
}

void Server::CreateTable(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, 
      const std::vector<uint32_t> &primary_key_col_idx) {
  // Only the last node creates the table
  if (id == numGroups * config.n - 1) {
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
}

void Server::LoadTableData(const std::string &table_name, const std::string &table_data_path, 
      const std::vector<std::pair<std::string, std::string>> &column_names_and_types, const std::vector<uint32_t> &primary_key_col_idx) {
  // Syntax based on: https://www.cockroachlabs.com/docs/stable/import-into.html
  // https://www.cockroachlabs.com/docs/v22.2/use-a-local-file-server
  // data path should be a url
  if (id == numGroups * config.n - 1) {
    std::string table_file_name = std::filesystem::path(table_data_path).filename();
    auto parent_path = std::filesystem::path(table_data_path).parent_path();
    std::string parent_dir = parent_path.filename();
    std::string table_column_name;
    for (auto &[col, type] : column_names_and_types) {
      table_column_name += "" + col + ",";
    }
    table_column_name.pop_back();

    Notice("Loading table from path: %s", table_data_path.c_str());

    std::string copy_table_statement_crdb = fmt::format(
        "IMPORT INTO {0} ({1}) CSV DATA "
        "(\'http://localhost:3000/{2}/{3}\') "
        "WITH skip = \'1\'",
        table_name, table_column_name,
        parent_dir, table_file_name);

    Notice("Load Table Data for table: %s", table_name.c_str());
    // TODO: exec with thread
    exec_sql(copy_table_statement_crdb);
    stats.Increment("TablesLoaded", 1);
  }
}

void Server::CreateIndex(
    const std::string &table_name,
    const std::vector<std::pair<std::string, std::string>> &column_data_types,
    const std::string &index_name, const std::vector<uint32_t> &index_col_idx) {
  if (id == numGroups * config.n - 1) {
    // Based on Syntax from:
    // https://www.postgresqltutorial.com/postgresql-indexes/postgresql-create-index/
    // and
    // https://www.postgresqltutorial.com/postgresql-indexes/postgresql-multicolumn-indexes/
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

    // Call into TableStore with this statement.
    exec_sql(sql_statement);
  }
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

// Stats &Server::GetStats() { return stats; }

Server::~Server() { system("pkill -9 -f cockroach; pkill -f \"python3 -m http.server\""); }

}  // namespace cockroachdb
