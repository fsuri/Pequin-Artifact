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
  bool order_commit, bool validate_abort,
  TrueTime timeServer) : config(config), keyManager(keyManager),
  groupIdx(groupIdx), idx(idx), id(groupIdx * config.n + idx),
  numShards(numShards), numGroups(numGroups), signMessages(signMessages),
  validateProofs(validateProofs),  timeDelta(timeDelta), part(part), tp(tp),
  order_commit(order_commit), validate_abort(validate_abort),
  timeServer(timeServer) {
  dummyProof = std::make_shared<proto::CommitProof>();

  dummyProof->mutable_writeback_message()->set_status(REPLY_OK);
  dummyProof->mutable_writeback_message()->set_txn_digest("");
  proto::ShardSignedDecisions dec;
  *dummyProof->mutable_writeback_message()->mutable_signed_decisions() = dec;

  dummyProof->mutable_txn()->mutable_timestamp()->set_timestamp(0);
  dummyProof->mutable_txn()->mutable_timestamp()->set_id(0);


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



  // auto connection = connectionPool->connection();
  // std::shared_ptr<tao::pq::transaction> tr;
  // tr = connection->transaction();
  // try {
  //   auto res = tr->execute("INSERT INTO users (name, age) VALUES ($1, $2)", "Oliver3", 27);
  //   Debug("Has rows affected: %d", res.has_rows_affected());
  //   auto res2 = tr->execute("INSERT INTO users (name, age) VALUES ('Oliver4', 28)");
  //   Debug("Has rows affected: %d", res2.has_rows_affected());
  //   tr->commit();
  //   Debug("test insert done!");
  // } catch(tao::pq::sql_error e) {
  //   Debug("test insert fail!");
  //   Debug(e.what());
  // }

  Debug("PostgreSQL client created!", idx);
}

Server::~Server() {
  // Stopping the postgres cluster
  // version and cluster name should match the ones in Pequin-Artifact/pg_setup/postgres_service.sh script
  // const char* command = "sudo pg_ctlcluster 12 pgdata stop";
  // system(command);
}


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

  proto::Transaction transaction;
  proto::Inquiry inquiry;
  proto::GroupedDecision gdecision;
  proto::Apply apply;
  proto::Rollback rollback;
  if (type == inquiry.GetTypeName()) {
    Debug("Shir: executing inquiry here");

    inquiry.ParseFromString(msg);
    std::vector<::google::protobuf::Message*> results;
    results.push_back(HandleInquiry(inquiry));
    return results;
  } else if (type == apply.GetTypeName()) {
    Debug("Shir: executing commit (apply) here");

    apply.ParseFromString(msg);
    std::vector<::google::protobuf::Message*> results;
    results.push_back(HandleApply(apply));
    return results;
  } else if (type == rollback.GetTypeName()) {
    rollback.ParseFromString(msg);
    std::vector<::google::protobuf::Message*> results;
    results.push_back(HandleRollback(rollback));
    return results;
  }
  std::vector<::google::protobuf::Message*> results;
  results.push_back(nullptr);
  return results;
}

void Server::Execute_Callback(const string& type, const string& msg, const execute_callback ecb) {
  Debug("Execute with callback: %s", type.c_str());

  proto::Inquiry inquiry;
  proto::Apply apply;
  proto::Rollback rollback;
  std::vector<::google::protobuf::Message*> results;
  if (type == inquiry.GetTypeName()) {
    inquiry.ParseFromString(msg);
    results.push_back(HandleInquiry(inquiry));
  } else if (type == apply.GetTypeName()) {
    apply.ParseFromString(msg);
    results.push_back(HandleApply(apply));
  } else if (type == rollback.GetTypeName()) {
    rollback.ParseFromString(msg);
    results.push_back(HandleRollback(rollback));
  } else{
    results.push_back(nullptr);
    Panic("Only failed grouped decisions should be atomically broadcast");
  }
  ecb(results);
}

::google::protobuf::Message* Server::HandleInquiry(const proto::Inquiry& inquiry) {
  Debug("Handling Inquiry");
  proto::InquiryReply* reply = new proto::InquiryReply();
  reply->set_req_id(inquiry.req_id());

  std::shared_ptr<tao::pq::transaction> tr;
  std::string client_seq_key;
  client_seq_key.append(std::to_string(inquiry.client_id()));
  client_seq_key.append("|");
  client_seq_key.append(std::to_string(inquiry.txn_seq_num()));

  std::cerr << "Shir:  "<< client_seq_key << "\n";
  // client_seq_key prints 0|1
  Debug("Shir is now handling Inquiry 1");

  if(txnMap.find(client_seq_key) == txnMap.end()) {
    // Debug("Shir is now handling Inquiry 2");

    auto connection = connectionPool->connection();

    // Debug("Shir is now handling Inquiry 2.1");

    tr = connection->transaction();

    // Debug("Shir is now handling Inquiry 2.2");

    connectionMap[client_seq_key] = connection;
    txnMap[client_seq_key] = tr;
    Debug("Query key: %s", client_seq_key);
    std::cout << client_seq_key << std::endl;
  } else {
    tr = txnMap[client_seq_key];
    Debug("Query key already in: %s", client_seq_key);
    std::cout << client_seq_key << std::endl;
  }

  // Debug("Shir is now handling Inquiry 3");

  try {
    Debug("Attempt query %s", inquiry.query());
    std::cout << inquiry.query() << std::endl;

    const auto sql_res = tr->execute(inquiry.query());
    // tr->commit(); // Shir: do we want to commit here?
    std::cerr<< "Shir: number of rows affected (according to server):   "<<sql_res.rows_affected() <<"\n";


    Debug("Query executed");
    sql::QueryResultProtoBuilder* res_builder = new sql::QueryResultProtoBuilder();
    // Should extrapolate out into builder method
    // Start by looping over columns and adding column names

  // Debug("Shir is now handling Inquiry 4");

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

  // Debug("Shir is now handling Inquiry 5");


    reply->set_status(REPLY_OK);
    // std::string* res_string;
    // res_builder->get_result()->SerializeToString(res_string);
    // reply->set_sql_res(*res_string); //&
    reply->set_sql_res(res_builder->get_result()->SerializeAsString());
  } catch(tao::pq::sql_error e) {
    reply->set_status(REPLY_FAIL);
  }

  return returnMessage(reply);
}

::google::protobuf::Message* Server::HandleApply(const proto::Apply& apply) {
  Debug("Applying(commiting) a txn");
  proto::ApplyReply* reply = new proto::ApplyReply();
  reply->set_req_id(apply.req_id());

  std::shared_ptr<tao::pq::transaction> tr;
  std::string client_seq_key;
  client_seq_key.append(std::to_string(apply.client_id()));
  client_seq_key.append("|");
  client_seq_key.append(std::to_string(apply.txn_seq_num()));

  if(txnMap.find(client_seq_key) == txnMap.end()) {
    auto connection = connectionPool->connection();
    tr = connection->transaction();
    connectionMap[client_seq_key] = connection;
    txnMap[client_seq_key] = tr;
    Debug("Apply key: %s", client_seq_key);
    std::cout << client_seq_key << std::endl;
  } else {
    tr = txnMap[client_seq_key];
    Debug("Apply key already in(should be): %s", client_seq_key);
    std::cout << client_seq_key << std::endl;
  }

  try {
    tr->commit();
    txnMap.erase(client_seq_key);
    connectionMap.erase(client_seq_key);
    reply->set_status(REPLY_OK);
    Debug("Apply went through successfully");
  } catch(tao::pq::sql_error e) {
    reply->set_status(REPLY_FAIL);
  }

  return returnMessage(reply);
}

::google::protobuf::Message* Server::HandleRollback(const proto::Rollback& rollback) {

  std::shared_ptr<tao::pq::transaction> tr;
  std::string client_seq_key;
  client_seq_key.append(std::to_string(rollback.client_id()));
  client_seq_key.append("|");
  client_seq_key.append(std::to_string(rollback.txn_seq_num()));

  if(txnMap.find(client_seq_key) == txnMap.end()) {
    return nullptr;
  } else {
    tr = txnMap[client_seq_key];
  }

  tr->rollback();

  txnMap.erase(client_seq_key);
  connectionMap.erase(client_seq_key);

  return nullptr;
}

void Server::Load(const string &key, const string &value, const Timestamp timestamp) {
  ValueAndProof val;
  val.value = value;
  val.commitProof = dummyProof;
  commitStore.put(key, val, timestamp);
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

  //Create TABLE version  -- just use table_name as key.  This version tracks updates to "table state" (as opposed to row state): I.e. new row insertions; row deletions;
  //Note: It does currently not track table creation/deletion itself -- this is unsupported. If we do want to support it, either we need to make a separate version; 
                                                                 //or we require inserts/updates to include the version in the ReadSet. 
                                                                 //However, we don't want an insert to abort just because another row was inserted.
  Load(table_name, "", Timestamp());

  //Create TABLE_COL version -- use table_name + delim + col_name as key. This version tracks updates to "column state" (as opposed to table state): I.e. row Updates;
  //Updates to column values change search meta data such as Indexes on a given Table. Scans that search on the column (using Active Reads) should conflict
  for(auto &[col_name, _] : column_data_types){
    Load(table_name + unique_delimiter + col_name, "", Timestamp());
  }
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
