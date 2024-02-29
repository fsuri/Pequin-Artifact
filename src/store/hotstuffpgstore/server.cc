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
#include "store/hotstuffpgstore/server.h"
#include "store/hotstuffpgstore/common.h"
#include "store/common/transaction.h"
#include <iostream>
#include <sys/time.h>
#include <cstdlib>
#include <fmt/core.h>

namespace hotstuffpgstore {

using namespace std;

Server::Server(const transport::Configuration& config, KeyManager *keyManager,
  int groupIdx, int idx, int numShards, int numGroups, bool signMessages,
  bool validateProofs, uint64_t timeDelta, Partitioner *part, Transport* tp,
  TrueTime timeServer) : config(config), keyManager(keyManager),
  groupIdx(groupIdx), idx(idx), id(groupIdx * config.n + idx),
  numShards(numShards), numGroups(numGroups), signMessages(signMessages),
  validateProofs(validateProofs),  timeDelta(timeDelta), part(part), tp(tp),
  timeServer(timeServer) {

  // Start the cluster before trying to connect:
  // version and cluster name should match the ones in Pequin-Artifact/pg_setup/postgres_service.sh script
  // const char* scriptName = "../pg_setup/postgres_service.sh -r";
  // std::system(scriptName);
  // const char* command = "sudo pg_ctlcluster 12 pgdata start";
  // system(command);

  // Shir: get back to this at some point
  std::string db_name = "db" + std::to_string(1 + idx);
  // std::string db_name = "db1"; // Use this code if every server is run on a 
  //separate host, otherwise use the above so they all reference a different database

  // password should match the one created in Pequin-Artifact/pg_setup/postgres_service.sh script
  // port should match the one that appears when executing "pg_lsclusters -h"
  std::string connection_str = "host=localhost user=pequin_user password=123 dbname=" + db_name + " port=5432";

  Debug("Shir: 33333333333333333333333333333333333333333333333333333333333");
  Debug("Shir: connection str is:  ");
  // Debug(connection_str);
  std::cerr << connection_str <<"\n";
  Debug("Shir: 33333333333333333333333333333333333333333333333333333333333");


  connectionPool = tao::pq::connection_pool::create(connection_str);

  Debug("PostgreSQL client created!", idx);
}

Server::~Server() {}


::google::protobuf::Message* Server::returnMessage(::google::protobuf::Message* msg) {
  // Send decision to client
  if (false) { //signMessages
    Debug("Returning signed Message");
    proto::SignedMessage *signedMessage = new proto::SignedMessage();
    SignMessage(*msg, keyManager->GetPrivateKey(id), id, *signedMessage);
    delete msg;
    return signedMessage;
  } else {
    Debug("Returning basic Message");
    return msg;
  }
}

std::vector<::google::protobuf::Message*> Server::Execute(const string& type, const string& msg) {
  std::cerr<<"Shir: recieved a message of type:    "<<type.c_str()<<"   and looking for the right handler\n";
  Debug("Execute: %s", type.c_str());
  //std::unique_lock lock(atomicMutex);

  proto::SQL_RPC sql_rpc;
  proto::TryCommit try_commit;
  proto::UserAbort user_abort;
  if (type == sql_rpc.GetTypeName()) {
    Debug("Shir: executing SQL_RPC here");

    sql_rpc.ParseFromString(msg);
    std::vector<::google::protobuf::Message*> results;
    results.push_back(HandleSQL_RPC(sql_rpc));
    return results;
  } else if (type == try_commit.GetTypeName()) {
    Debug("Shir: executing commit (tryCommit) here");

    try_commit.ParseFromString(msg);
    std::vector<::google::protobuf::Message*> results;
    results.push_back(HandleTryCommit(try_commit));
    return results;
  } else if (type == user_abort.GetTypeName()) {
    user_abort.ParseFromString(msg);
    std::vector<::google::protobuf::Message*> results;
    results.push_back(HandleUserAbort(user_abort));
    return results;
  }
  std::vector<::google::protobuf::Message*> results;
  results.push_back(nullptr);
  return results;
}

void Server::Execute_Callback(const string& type, const string& msg, const execute_callback ecb) {
  Debug("Execute with callback: %s", type.c_str());

  proto::SQL_RPC sql_rpc;
  proto::TryCommit try_commit;
  proto::UserAbort user_abort;
  std::vector<::google::protobuf::Message*> results;
  if (type == sql_rpc.GetTypeName()) {
    sql_rpc.ParseFromString(msg);
    results.push_back(HandleSQL_RPC(sql_rpc));
  } else if (type == try_commit.GetTypeName()) {
    try_commit.ParseFromString(msg);
    results.push_back(HandleTryCommit(try_commit));
  } else if (type == user_abort.GetTypeName()) {
    user_abort.ParseFromString(msg);
    results.push_back(HandleUserAbort(user_abort));
  } else{
    results.push_back(nullptr);
    Panic("Only failed grouped decisions should be atomically broadcast");
  }
  ecb(results);
}

::google::protobuf::Message* Server::HandleSQL_RPC(const proto::SQL_RPC& sql_rpc) {
  Debug("Handling SQL_RPC");
  proto::SQL_RPCReply* reply = new proto::SQL_RPCReply();
  reply->set_req_id(sql_rpc.req_id());

  std::string client_seq_key = createClientSeqKey(sql_rpc.client_id(),sql_rpc.txn_seq_num());
  txnStatusMap::accessor t;
  std::shared_ptr<tao::pq::transaction> tr = getPgTransaction(t, client_seq_key);

  try {
    Debug("Attempt query %s", sql_rpc.query());
    std::cout << sql_rpc.query() << std::endl;

    std::cerr<< "Shir: Before executing tr->execute (2)\n";
    const auto sql_res = tr->execute(sql_rpc.query());
    std::cerr<< "Shir: After executing tr->execute (2)\n";

    std::cerr<< "Shir: number of rows affected (according to server):   "<<sql_res.rows_affected() <<"\n";
    std::cerr<< "Shir: this is for txn by client id:   "<<std::to_string(sql_rpc.client_id()) <<"\n";


    Debug("Query executed");
    // Should extrapolate out into builder method
    // Start by looping over columns and adding column names

    sql::QueryResultProtoBuilder* res_builder = new sql::QueryResultProtoBuilder();
    res_builder->set_rows_affected(sql_res.rows_affected());
    if(sql_res.columns() == 0) {
      Debug("Had rows affected");
      res_builder->add_empty_row();
    } else {
      Debug("No rows affected");
      for(int i = 0; i < sql_res.columns(); i++) {
        res_builder->add_column(sql_res.name(i));
        std::cout << sql_res.name(i) << std::endl;
      }
      // After loop over rows and add them using add_row method
      // for(const auto& row : sql_res) {
      //   res_builder->add_row(std::begin(row), std::end(row));
      // }
      // for(auto it = std::begin(sql_res); it != std::end(sql_res); ++it) {
        
      // }
      for( const auto& row : sql_res ) {
        res_builder->add_empty_row();
        for( const auto& field : row ) {
          std::string field_str = field.as<std::string>();
          res_builder->add_field_to_last_row(field_str);
          std::cout << field_str << std::endl;
        }
      }
    }

    reply->set_status(REPLY_OK);
    // std::string* res_string;
    // res_builder->get_result()->SerializeToString(res_string);
    // reply->set_sql_res(*res_string); //&
    reply->set_sql_res(res_builder->get_result()->SerializeAsString());
    t.release();
  } catch(tao::pq::sql_error e) {
    Debug("An exception caugth while using postgres.");
    reply->set_status(REPLY_FAIL);
    t.release();
    CleanTxnMap(client_seq_key);
  }
  return returnMessage(reply);
}

::google::protobuf::Message* Server::HandleTryCommit(const proto::TryCommit& try_commit) {
  Debug("Trying to commit a txn");
  proto::TryCommitReply* reply = new proto::TryCommitReply();
  reply->set_req_id(try_commit.req_id());

  std::string client_seq_key = createClientSeqKey(try_commit.client_id(),try_commit.txn_seq_num());
  txnStatusMap::accessor t;
  std::shared_ptr<tao::pq::transaction> tr = getPgTransaction(t, client_seq_key);

  try {
    tr->commit();
    Debug("TryCommit went through successfully.");
    reply->set_status(REPLY_OK);
  } catch(tao::pq::sql_error e) {
    Debug("An exception caugth while using postgres.");
    reply->set_status(REPLY_FAIL);
  }
  t.release();
  CleanTxnMap(client_seq_key);
  return returnMessage(reply);
}

::google::protobuf::Message* Server::HandleUserAbort(const proto::UserAbort& user_abort) {
  std::shared_ptr<tao::pq::transaction> tr;
  std::string client_seq_key = createClientSeqKey(user_abort.client_id(),user_abort.txn_seq_num());
  txnStatusMap::accessor t;

  if(!txnMap.find(t,client_seq_key)) {
    t.release();
    return nullptr;
  } else {
    tr = get<1>(t->second);
  }
  tr->rollback();
  t.release();
  CleanTxnMap(client_seq_key);
  return nullptr;
}

std::string Server::createClientSeqKey(uint64_t cid, uint64_t tid){
  std::string client_seq_key;
  client_seq_key.append(std::to_string(cid));
  client_seq_key.append("|");
  client_seq_key.append(std::to_string(tid));
  return client_seq_key;
}

std::shared_ptr<tao::pq::transaction> Server::getPgTransaction(txnStatusMap::accessor &t, const std::string &key){
  std::shared_ptr<tao::pq::transaction> tr;
  Debug("Client transaction key: %s", key);
  std::cout << key << std::endl;

  if(!txnMap.find(t,key)) {
      Debug("Key was not found, creating a new connection");
      auto connection = connectionPool->connection();
      tr = connection->transaction();
      auto txn_status =std::make_tuple(connection, tr, false);
      txnMap.insert(t, key);
      t->second=txn_status;
    } else {
      tr = get<1>(t->second);
      Debug("Key already exists");
  }
  return tr;
}

void Server::CleanTxnMap(const std::string &client_seq_key){
  txnMap.erase(client_seq_key);
}

void Server::CreateTable(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, const std::vector<uint32_t> &primary_key_col_idx){
  // //Based on Syntax from: https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-create-table/ 

  Debug("Shir: Creating table!");

  //NOTE: Assuming here we do not need special descriptors like foreign keys, column condidtions... (If so, it maybe easier to store the SQL statement in JSON directly)
  UW_ASSERT(!column_data_types.empty());
  UW_ASSERT(!primary_key_col_idx.empty());

  std::string sql_statement("CREATE TABLE");
  sql_statement += " " + table_name;
  
  sql_statement += " (";
  for(auto &[col, type]: column_data_types){
    std::string p_key = (primary_key_col_idx.size() == 1 && col == column_data_types[primary_key_col_idx[0]].first) ? " PRIMARY KEY": "";
    sql_statement += col + " " + type + p_key + ", ";
  }

  sql_statement.resize(sql_statement.size() - 2); //remove trailing ", "

  if(primary_key_col_idx.size() > 1){
    sql_statement += ", PRIMARY_KEY ";
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


void Server::LoadTableData(const std::string &table_name, const std::string &table_data_path, const std::vector<uint32_t> &primary_key_col_idx){
  Debug("Shir: Load Table data!");
  std::string copy_table_statement = fmt::format("COPY {0} FROM {1} DELIMITER ',' CSV HEADER", table_name, table_data_path);
}

void Server::LoadTableRows(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, const std::vector<std::vector<std::string>> &row_values, const std::vector<uint32_t> &primary_key_col_idx ){
  Debug("Shir: Load Table rows!");
  std::string sql_statement = this->GenerateLoadStatement(table_name,row_values,0);
  this->exec_statement(sql_statement);
}

//!!"Deprecated" (Unused)
void Server::LoadTableRow(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, const std::vector<std::string> &values, const std::vector<uint32_t> &primary_key_col_idx ){
    Debug("Shir: Load Table row!");
}


void Server::exec_statement(std::string sql_statement) {
  // Debug("Shir: executing the following sql statement in postgres: ");
  // std::cerr<< sql_statement << "\n";

  auto connection = connectionPool->connection();
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
    return load_statement;
}


Stats &Server::GetStats() {
  return stats;
}

Stats* Server::mutableStats() {
  return &stats;
}

}
