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

Server::Server(const transport::Configuration& config, KeyManager *keyManager,
  int groupIdx, int idx, int numShards, int numGroups, bool signMessages,
  bool validateProofs, uint64_t timeDelta, Partitioner *part, Transport* tp, bool localConfig,
  TrueTime timeServer) : config(config), keyManager(keyManager),
  groupIdx(groupIdx), idx(idx), id(groupIdx * config.n + idx),
  numShards(numShards), numGroups(numGroups), signMessages(signMessages),
  validateProofs(validateProofs),  timeDelta(timeDelta), part(part), tp(tp), localConfig(localConfig), timeServer(timeServer) {

  tp->AddIndexedThreads(number_of_threads);

  //separate for local configuration we set up different db name for each servers, otherwise they can share the db name
  std::string db_name = "db1";
  if (localConfig){
    std::cerr << "Shir: using local config (pg server)\n";
    db_name = "db" + std::to_string(1 + idx);
  }

  // password should match the one created in Pequin-Artifact/pg_setup/postgres_service.sh script
  // port should match the one that appears when executing "pg_lsclusters -h"
  std::string connection_str = "host=localhost user=pequin_user password=123 dbname=" + db_name + " port=5432";
  //std::string connection_str = "sudo -u postgres psql -U postgres --host localhost:5432 -d bench -c 'sql'";// + host + ":" + port + " -d bench -c '" + sql + "'"; 
  std::cerr<<"Shir: 33333333333333333333333333333333333333333333333333333333333\n";
  std::cerr << connection_str <<"\n";
  Debug("Shir: 33333333333333333333333333333333333333333333333333333333333");


  connectionPool = tao::pq::connection_pool::create(connection_str);

  Debug("PostgreSQL client created! Id: %d", idx);
  std::cerr << "PostgreSQL client created! Id:  "<< idx <<"\n";

}

Server::~Server() {}


::google::protobuf::Message* Server::returnMessage(::google::protobuf::Message* msg) {
  // Send decision to client
  if (false) { //signMessages
    /*Debug("Returning signed Message");
    proto::SignedMessage *signedMessage = new proto::SignedMessage();
    SignMessage(*msg, keyManager->GetPrivateKey(id), id, *signedMessage);
    delete msg;
    return signedMessage;*/
    return msg;
  } else {
    Debug("Returning basic Message");
    return msg;
  }
}

std::vector<::google::protobuf::Message*> Server::Execute(const string& type, const string& msg) {
  std::cerr<<"Shir: recieved a message of type:    "<<type.c_str()<<"   and looking for the right handler\n";
  Debug("Execute: %s", type.c_str());
  /*proto::SQL_RPC sql_rpc;
  proto::TryCommit try_commit;
  proto::UserAbort user_abort;*/
  std::vector<::google::protobuf::Message*> results;
  /*std::string client_seq_key ;
  if (type == sql_rpc.GetTypeName()) {
    sql_rpc.ParseFromString(msg);
    client_seq_key = createClientSeqKey(sql_rpc.client_id(),sql_rpc.txn_seq_num());
  } else if (type == try_commit.GetTypeName()) {
    try_commit.ParseFromString(msg);
    client_seq_key = createClientSeqKey(try_commit.client_id(),try_commit.txn_seq_num());
  } else if (type == user_abort.GetTypeName()) {
    user_abort.ParseFromString(msg);
    client_seq_key = createClientSeqKey(user_abort.client_id(),user_abort.txn_seq_num());
  }

  txnStatusMap::accessor t;
  auto [tr, is_aborted] = getPgTransaction(t, client_seq_key);
  if (tr){
    // this means tr is not a null pointer. it would be a null pointer if this txn was alerady aborted. handle this case later

    if (type == sql_rpc.GetTypeName()) {
      results.push_back(HandleSQL_RPC(t,tr,sql_rpc.req_id(),sql_rpc.query()));
    } else if (type == try_commit.GetTypeName()) {
      results.push_back(HandleTryCommit(t,tr,try_commit.req_id()));
    } else if (type == user_abort.GetTypeName()) {
      results.push_back(HandleUserAbort(t,tr));
    }

  }else{
    if (type == sql_rpc.GetTypeName()) {
      UW_ASSERT(is_aborted);
      proto::SQL_RPCReply* reply = new proto::SQL_RPCReply();
      reply->set_req_id(sql_rpc.req_id());
      reply->set_status(REPLY_FAIL);
      results.push_back(returnMessage(reply));
    } else if (type == try_commit.GetTypeName()) {
      Debug("tr is null - trycommit ");
      UW_ASSERT(is_aborted);
      proto::TryCommitReply* reply = new proto::TryCommitReply();
      reply->set_req_id(try_commit.req_id());
      reply->set_status(REPLY_FAIL); // OR should it be reply_ok?
      results.push_back(returnMessage(reply));
    }else{
      // shir: when should we get here?
      Panic("Should not try to issue parallel operations that aren't sql_query");
    }
  }*/

  return results;
}


void Server::Execute_Callback(const string& type, const string& msg, std::function<void(std::vector<google::protobuf::Message*>& )> ecb) {

  Debug("Execute with callback: %s", type.c_str());

  /*proto::SQL_RPC sql_rpc;
  proto::TryCommit try_commit;
  proto::UserAbort user_abort;
  std::vector<::google::protobuf::Message*> results;
  std::string client_seq_key ;
  if (type == sql_rpc.GetTypeName()) {
    sql_rpc.ParseFromString(msg);
    client_seq_key = createClientSeqKey(sql_rpc.client_id(),sql_rpc.txn_seq_num());
  } else if (type == try_commit.GetTypeName()) {
    try_commit.ParseFromString(msg);
    client_seq_key = createClientSeqKey(try_commit.client_id(),try_commit.txn_seq_num());
  } else if (type == user_abort.GetTypeName()) {
    user_abort.ParseFromString(msg);
    client_seq_key = createClientSeqKey(user_abort.client_id(),user_abort.txn_seq_num());
  }

  auto thread_id = getThreadID(client_seq_key);
  Debug("Thread id for key %s is: %d",client_seq_key.c_str(), thread_id);


  auto f = [this, type, client_seq_key, sql_rpc, try_commit, user_abort, ecb](){
    proto::SQL_RPC sql_rpc_template;
    proto::TryCommit try_commit_template;
    proto::UserAbort user_abort_template;
    std::vector<::google::protobuf::Message*> results;
    txnStatusMap::accessor t;
    auto [tr, is_aborted] = getPgTransaction(t, client_seq_key);
    if (tr){
      // this means tr is not a null pointer. it would be a null pointer if this txn was alerady aborted. 
      if (type == sql_rpc_template.GetTypeName()) {
        std::cerr<<sql_rpc.query()<<"\n";
        results.push_back(HandleSQL_RPC(t,tr,sql_rpc.req_id(),sql_rpc.query()));
      } else if (type == try_commit_template.GetTypeName()) {
        results.push_back(HandleTryCommit(t,tr,try_commit.req_id()));
      } else if (type == user_abort_template.GetTypeName()) {
        results.push_back(HandleUserAbort(t,tr));
      }
    }else{
      if (type == sql_rpc_template.GetTypeName()) {
        UW_ASSERT(is_aborted);
        proto::SQL_RPCReply* reply = new proto::SQL_RPCReply();
        reply->set_req_id(sql_rpc.req_id());
        reply->set_status(REPLY_FAIL);
        results.push_back(returnMessage(reply));
      } else if (type == try_commit.GetTypeName()) {
      Debug("tr is null - trycommit ");
      UW_ASSERT(is_aborted);
      proto::TryCommitReply* reply = new proto::TryCommitReply();
      reply->set_req_id(try_commit.req_id());
      reply->set_status(REPLY_OK); // OR should it be reply_ok?
      results.push_back(returnMessage(reply));
      }else{
        std::cerr<< type<<"\n";
        Panic("Should not try to issue parallel operations that aren't sql_query");
      }
    }

    tp->Timer(0, std::bind(ecb,results));
  
    return (void*) true;
  };

  tp->DispatchIndexedTP_noCB(thread_id,f);*/
}

::google::protobuf::Message* Server::HandleSQL_RPC(txnStatusMap::accessor &t, std::shared_ptr<tao::pq::transaction> tr, uint64_t req_id,std::string query) {
  Debug("Handling SQL_RPC");
  /*proto::SQL_RPCReply* reply = new proto::SQL_RPCReply();
  reply->set_req_id(req_id);

  try {
    Debug("Attempt query %s", query.c_str());
    // std::cerr<< "Shir: Before executing tr->execute with the following tr addr:  "<< tr <<"\n";
    const tao::pq::result sql_res = tr->execute(query);
    // std::cerr<< "Shir: After executing tr->execute \n";
    Debug("Query executed");
    sql::QueryResultProtoBuilder* res_builder = createResult(sql_res);
    reply->set_status(REPLY_OK);
    reply->set_sql_res(res_builder->get_result()->SerializeAsString());
  } catch(tao::pq::sql_error e) {
    Debug("A exception caugth while using postgres.");
 
    if (std::regex_match(e.sqlstate, std::regex("40...")) || std::regex_match(e.sqlstate, std::regex("55P03"))){ // Concurrency errors
      Debug("A concurrency exception caugth while using postgres.");
      markTxnTerminated(t,"sql_rpc, concurrency problem");
      t.release();
      reply->set_status(REPLY_FAIL);
    } else if (std::regex_match(e.sqlstate, std::regex("23..."))){ // Application errors
      reply->set_status(REPLY_OK);
    }else{
      std::cerr<< e.sqlstate << std::endl;
      Panic("Unexpected postgres exception");
    }
  }*/

  //return returnMessage(reply);
  return nullptr;
}

::google::protobuf::Message* Server::HandleTryCommit(txnStatusMap::accessor &t, std::shared_ptr<tao::pq::transaction> tr, uint64_t req_id) {
  Debug("Trying to commit a txn %d",req_id);
  std::cerr<<"the tr pointer for commit is :     "<< tr << "\n";
  /*proto::TryCommitReply* reply = new proto::TryCommitReply();
  reply->set_req_id(req_id);
  try {
    tr->commit();
    Debug("TryCommit went through successfully.");
    reply->set_status(REPLY_OK);
  } catch(tao::pq::sql_error e) {
    std::cerr<< e.sqlstate << std::endl;
    Debug("A exception caugth while using postgres."); 
    reply->set_status(REPLY_FAIL);
    // Shir: do we need diffenet codes here?
    // if (std::regex_match(e.sqlstate, std::regex("40..."))){ // Concurrency errors
    //   Debug("A concurrency exception caugth while using postgres.");
    //   markTxnTerminated(t);
    //   t.release();
    //   reply->set_status(REPLY_FAIL);
    // } else if (std::regex_match(e.sqlstate, std::regex("23..."))){ // Application errors
    //   reply->set_status(REPLY_OK);
    // }else{
    //   std::cerr<< e.sqlstate << std::endl;
    //   Panic("Unexpected postgres exception");
    // }

  }
  markTxnTerminated(t,"trycommit");*/
  //return returnMessage(reply);
  return nullptr;
}

::google::protobuf::Message* Server::HandleUserAbort(txnStatusMap::accessor &t, std::shared_ptr<tao::pq::transaction> tr) {
  tr->rollback();
  markTxnTerminated(t,"");
  return nullptr;
}

std::string Server::createClientSeqKey(uint64_t cid, uint64_t tid){
  std::string client_seq_key;
  client_seq_key.append(std::to_string(cid));
  client_seq_key.append("|");
  client_seq_key.append(std::to_string(tid));
  return client_seq_key;
}

std::pair<std::shared_ptr<tao::pq::transaction>, bool> Server::getPgTransaction(txnStatusMap::accessor &t, const std::string &key){
  std::shared_ptr<tao::pq::transaction> tr;
  Debug("Client transaction key: %s", key.c_str());
  std::cout << key << std::endl;
  bool is_aborted=false;

  if(txnMap.insert(t,key)) {
      Debug("Key was not found, creating a new connection");
      auto connection = connectionPool->connection();
      tr = connection->transaction();
      std::cerr<<"Shir: new tr pointer is :     "<< tr <<"\n";
      auto txn_status =std::make_tuple(connection, tr, false);
      // txnMap.insert(t, key);
      t->second=txn_status;
    } else {
      Debug("Key already exists");
      tr = get<1>(t->second);
      std::cerr<<"Shir: for key:  "<< key.c_str() <<"  existing tr pointer is :     "<< tr <<"\n";
      is_aborted=get<2>(t->second);
  }
    return std::make_pair(tr,is_aborted);
}

uint64_t Server::getThreadID(const std::string &key){
  std::hash<std::string> hasher;  
  uint64_t id = hasher(key) % number_of_threads;
  return id;
}

// fields are < connection, transaction tr, bool was_aborted>
void Server::markTxnTerminated(txnStatusMap::accessor &t,string from){
  std::cerr<<"Shir: terminating txn from  "<< from.c_str() << "with tr pointer:    "<< get<1>(t->second) <<"\n";

  get<0>(t->second) = nullptr;
  get<1>(t->second) = nullptr;
  get<2>(t->second) = true;
}

sql::QueryResultProtoBuilder* Server::createResult(const tao::pq::result &sql_res){
  // Should extrapolate out into builder method
  // Start by looping over columns and adding column names

  sql::QueryResultProtoBuilder* res_builder = new sql::QueryResultProtoBuilder();
  //res_builder->set_rows_affected(sql_res.rows_affected());
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
        res_builder->add_field_to_last_row_serialize(field_str);
        std::cout << field_str << std::endl;
      }
    }
  }
  return res_builder;
}

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
