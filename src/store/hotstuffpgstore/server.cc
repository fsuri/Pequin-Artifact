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

bool Server::CCC2(const proto::Transaction& txn) {
  Debug("Starting ccc v2 check");
  Timestamp txTs(txn.timestamp());
  for (const auto &read : txn.readset()) {
    if(!IsKeyOwned(read.key())) {
      continue;
    }

    // we want to make sure that our reads don't span any
    // committed/prepared writes

    // check the committed writes
    Timestamp rts(read.readtime());
    // we want to make sure there are no committed writes for this key after
    // the rts and before the txTs
    std::vector<std::pair<Timestamp, Server::ValueAndProof>> committedWrites;
    if (commitStore.getCommittedAfter(read.key(), rts, committedWrites)) {
      for (const auto& committedWrite : committedWrites) {
        if (committedWrite.first < txTs) {
          Debug("found committed conflict with read for key: %s", read.key().c_str());
          return false;
        }
      }
    }

    // check the prepared writes
    const auto preparedWritesItr = preparedWrites.find(read.key());
    if (preparedWritesItr != preparedWrites.end()) {
      for (const auto& writeTs : preparedWritesItr->second) {
        if (rts < writeTs && writeTs < txTs) {
          Debug("found prepared conflict with read for key: %s", read.key().c_str());
          return false;
        }
      }
    }

  }

  Debug("checked all reads");

  for (const auto &write : txn.writeset()) {
    if(!IsKeyOwned(write.key())) {
      continue;
    }

    // we want to make sure that no prepared/committed read spans
    // our writes

    // check commited reads
    // for (const auto& read : committedReads[write.key()]) {
    //   // second is the read ts, first is the txTs that did the read
    //   if (read.second < txTs && txTs < read.first) {
    //       Debug("found committed conflict with write for key: %s", write.key().c_str());
    //       return false;
    //     }
    //   }

    auto committedReadsItr = committedReads.find(write.key());

    if (committedReadsItr != committedReads.end() && committedReadsItr->second.size() > 0) {

      for (auto read = committedReadsItr->second.rbegin(); read != committedReadsItr->second.rend(); ++read) {
      //for (const auto& read : committedReads[write.key()]) {
        // second is the read ts, first is the txTs that did the read
        if (txTs >= read->first){
          break;
        }
        if (read->second < txTs && txTs < read->first) {
            Debug("found committed conflict with write for key: %s", write.key().c_str());
            return false;
        }

      }
    }

    // check prepared reads
    for (const auto& read : preparedReads[write.key()]) {
      // second is the read ts, first is the txTs that did the read
      if (read.second < txTs && txTs < read.first) {
          Debug("found prepared conflict with write for key: %s", write.key().c_str());
          return false;
      }
    }
  }
  return true;
}

bool Server::CCC(const proto::Transaction& txn) {
  Debug("Starting ccc check");
  Timestamp txTs(txn.timestamp());
  for (const auto &read : txn.readset()) {
    if(!IsKeyOwned(read.key())) {
      continue;
    }

    // we want to make sure that our reads don't span any
    // committed/prepared writes

    // check the committed writes
    Timestamp rts(read.readtime());
    Timestamp upper;
    // this is equivalent to checking if there is a write with a timestamp t
    // such that t > rts and t < txTs
    if (commitStore.getUpperBound(read.key(), rts, upper)) {
      if (upper < txTs) {
        Debug("found committed conflict with read for key: %s", read.key().c_str());
        return false;
      }
    }

    // check the prepared writes
    for (const auto& pair : pendingTransactions) {
      for (const auto& write : pair.second.writeset()) {
        if (write.key() == read.key()) {
          Timestamp wts(pair.second.timestamp());
          if (wts > rts && wts < txTs) {
            Debug("found prepared conflict with read for key: %s", read.key().c_str());
            return false;
          }
        }
      }
    }
  }

  Debug("checked all reads");

  for (const auto &write : txn.writeset()) {
    if(!IsKeyOwned(write.key())) {
      continue;
    }

    // we want to make sure that no prepared/committed read spans
    // our writes

    // check commited reads
    // get a pointer to the first read that commits after this tx
    auto it = committedReads[write.key()].lower_bound(txTs);
    if (it != committedReads[write.key()].end()) {
      // if the iterator is at the end, then that means there are no committed reads
      // before this tx
      it++;
      // all iterator pairs committed after txTs (commit ts > txTs)
      // so we just need to check if they returned a version before txTs (read ts < txTs)
      while(it != committedReads[write.key()].end()) {
        if ((*it).second < txTs) {
          Debug("found committed conflict with write for key: %s", write.key().c_str());
          return false;
        }
        it++;
      }
    }

    // next, check the prepared tx's read sets
    for (const auto& pair : pendingTransactions) {
      for (const auto& read : pair.second.readset()) {
        if (read.key() == write.key()) {
          Timestamp pendingTxTs(pair.second.timestamp());
          Timestamp rts(read.readtime());
          if (txTs > rts && txTs < pendingTxTs) {
            Debug("found prepared conflict with write for key: %s", write.key().c_str());
            return false;
          }
        }
      }
    }
  }
  return true;

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
  if (type == transaction.GetTypeName()) {
    transaction.ParseFromString(msg);

    return HandleTransaction(transaction);
  } else if (type == inquiry.GetTypeName()) {
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
  } else if (type == gdecision.GetTypeName()) {
    gdecision.ParseFromString(msg);

    if (gdecision.status() == REPLY_FAIL) {
      std::vector<::google::protobuf::Message*> results;
      results.push_back(HandleGroupedAbortDecision(gdecision));
      return results;
    } else if(order_commit && gdecision.status() == REPLY_OK) {
      std::vector<::google::protobuf::Message*> results;
      results.push_back(HandleGroupedCommitDecision(gdecision));
      return results;
    }
    else{
      Panic("Only failed grouped decisions should be atomically broadcast");
    }
  }
  std::vector<::google::protobuf::Message*> results;
  results.push_back(nullptr);
  return results;
}

void Server::Execute_Callback(const string& type, const string& msg, const execute_callback ecb) {
  Debug("Execute with callback: %s", type.c_str());
  //std::unique_lock lock(atomicMutex);

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
  // return results;
}

std::vector<::google::protobuf::Message*> Server::HandleTransaction(const proto::Transaction& transaction) {
  std::unique_lock lock(atomicMutex); //TODO: might be able to make it finer.

  std::vector<::google::protobuf::Message*> results;
  proto::TransactionDecision* decision = new proto::TransactionDecision();
  //std::cerr << "allocating reply" << std::endl;

  string digest = TransactionDigest(transaction);

  // TEST HOW MANY DUPLICATE TX: 
  auto [_, first] = executed_tx.insert(digest);
  if(first) stats.Increment("tx_unique", 1);
  if(!first) stats.Increment("tx_duplicate", 1);


  Debug("Handling transaction");
  DebugHash(digest);
  stats.Increment("handle_tx",1);
  decision->set_txn_digest(digest);
  decision->set_shard_id(groupIdx);

  pendingTransactions[digest] = transaction;

  if (bufferedGDecs.find(digest) != bufferedGDecs.end()) {
    stats.Increment("used_buffered_gdec",1);
    Debug("found buffered gdecision");
    if(bufferedGDecs[digest].status() == REPLY_OK){
      results.push_back(HandleGroupedCommitDecision(bufferedGDecs[digest]));
    }
    else{
      results.push_back(HandleGroupedAbortDecision(bufferedGDecs[digest]));
    }
    bufferedGDecs.erase(digest);
    return results;
  }

  // OCC check
  if (CCC2(transaction)) {
    stats.Increment("ccc_succeed",1);
    Debug("ccc succeeded");
    decision->set_status(REPLY_OK);
    pendingTransactions[digest] = transaction;

    // update prepared reads and writes
    Timestamp txTs(transaction.timestamp());
    for (const auto& write : transaction.writeset()) {
      if(!IsKeyOwned(write.key())) {
        continue;
      }
      preparedWrites[write.key()].insert(txTs);
    }
    for (const auto& read : transaction.readset()) {
      if(!IsKeyOwned(read.key())) {
        continue;
      }
      preparedReads[read.key()][txTs] = read.readtime();
    }

    // check for buffered gdecision
    // if (bufferedGDecs.find(digest) != bufferedGDecs.end()) {
    //   stats.Increment("used_buffered_gdec",1);
    //   Debug("found buffered gdecision");
    //   results.push_back(HandleGroupedCommitDecision(bufferedGDecs[digest]));
    //   bufferedGDecs.erase(digest);
    // }

    // check if this transaction was already aborted
    if (false & abortedTxs.find(digest) != abortedTxs.end() ) { //this branch of code is not used anymore
      //it was only used for Writeback Acks...
      stats.Increment("gdec_failed_buf",1);
      // abort the tx
      cleanupPendingTx(digest);
      proto::GroupedDecisionAck* groupedDecisionAck = new proto::GroupedDecisionAck();
      groupedDecisionAck->set_status(REPLY_FAIL);
      groupedDecisionAck->set_txn_digest(digest);
      results.push_back(returnMessage(groupedDecisionAck));
    }
  } else {
    Debug("ccc failed");
    stats.Increment("ccc_fail",1);
    decision->set_status(REPLY_FAIL);
    pendingTransactions[digest] = transaction;

    // if (bufferedGDecs.find(digest) != bufferedGDecs.end()) {
    //   stats.Increment("used_buffered_gdec",1);
    //   Debug("found buffered gdecision");
    //   results.push_back(HandleGroupedAbortDecision(bufferedGDecs[digest]));
    //   bufferedGDecs.erase(digest);
    // }
  }


  results.push_back(decision);

  return results;
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


::google::protobuf::Message* Server::HandleMessage(const string& type, const string& msg) {

  std::cerr<< "Shir: Got a message!!!!:   "<< type.c_str() <<"\n";

  Debug("Handle %s", type.c_str());
  //std::shared_lock lock(atomicMutex);
  //std::unique_lock lock(atomicMutex);

  proto::Read read;
  proto::GroupedDecision gdecision;

  if (type == read.GetTypeName()) {
    read.ParseFromString(msg);

    return HandleRead(read);
  } else if (type == gdecision.GetTypeName()) {
    if(order_commit && signMessages){
      Panic("Should be ordering all Writeback messages");
    }
    gdecision.ParseFromString(msg);
    if (gdecision.status() == REPLY_OK) {
      //std::unique_lock lock(atomicMutex);
      return HandleGroupedCommitDecision(gdecision);
    } else {
      Panic("Only commit grouped decisions allowed to be sent directly to server");
    }

  }
  else{
    Panic("Request not of type Read (or Commit Writeback)");
  }

  return nullptr;
}

::google::protobuf::Message* Server::HandleRead(const proto::Read& read) {
  Timestamp ts(read.timestamp());
  //std::shared_lock lock(atomicMutex); //come back to this: probably dont need it at all.

  stats.Increment("total_reads_processed", 1);
  pair<Timestamp, ValueAndProof> result;
  bool exists = commitStore.get(read.key(), ts, result);

  proto::ReadReply* readReply = new proto::ReadReply();
  Debug("Handle read req id %lu", read.req_id());
  readReply->set_req_id(read.req_id());
  readReply->set_key(read.key());
  if (exists) {
    Debug("Read exists f");
    Debug("Read exits for key: %s  value: %s", read.key().c_str(), result.second.value.c_str());
    readReply->set_status(REPLY_OK);
    readReply->set_value(result.second.value);
    result.first.serialize(readReply->mutable_value_timestamp());
    if (validateProofs) {
      *readReply->mutable_commit_proof() = *result.second.commitProof;
    }
  } else {
    stats.Increment("read_dne",1);
    Debug("Read does not exit for key: %s", read.key().c_str());
    readReply->set_status(REPLY_FAIL);
  }
  //lock.unlock_shared();
  return returnMessage(readReply);
}


// Rewrite this to work for queries instead, will have to rewrite end point on client side too
::google::protobuf::Message* Server::HandleGroupedCommitDecision(const proto::GroupedDecision& gdecision) {
  proto::GroupedDecisionAck* groupedDecisionAck = new proto::GroupedDecisionAck();


  Debug("Handling Grouped commit Decision");
  string digest = gdecision.txn_digest();
  DebugHash(digest);

  groupedDecisionAck->set_txn_digest(digest);
  atomicMutex.lock();
  if (pendingTransactions.find(digest) == pendingTransactions.end()) {

    Debug("Buffering gdecision");
    stats.Increment("buff_dec",1);
    // we haven't yet received the tx so buffer this gdecision until we get it
    bufferedGDecs[digest] = gdecision;
    atomicMutex.unlock();
    return nullptr;
  }
  atomicMutex.unlock();
  // verify gdecision

    // struct timeval tp;
    // gettimeofday(&tp, NULL);
    // long int us = tp.tv_sec * 1000 * 1000 + tp.tv_usec;

  if (verifyGDecision_parallel(gdecision, pendingTransactions[digest], keyManager, signMessages, config.f, tp)) {
  //if (verifyGDecision(gdecision, pendingTransactions[digest], keyManager, signMessages, config.f)) {
 //if(true){
    // gettimeofday(&tp, NULL);
    // long int lock_time = ((tp.tv_sec * 1000 * 1000 + tp.tv_usec) -us);
    // std::cerr << "Commit Verification takes " << lock_time << " microseconds" << std::endl;

    std::unique_lock lock(atomicMutex);
    stats.Increment("apply_tx",1);
    proto::Transaction txn = pendingTransactions[digest];
    Timestamp ts(txn.timestamp());
    // apply tx
    Debug("applying tx");
    for (const auto &read : txn.readset()) {
      if(!IsKeyOwned(read.key())) {
        continue;
      }
      Debug("applying read to key %s", read.key().c_str());
      committedReads[read.key()][ts] = read.readtime();
    }

    proto::CommitProof proof;
    *proof.mutable_writeback_message() = gdecision;
    *proof.mutable_txn() = txn;
    shared_ptr<proto::CommitProof> commitProofPtr = make_shared<proto::CommitProof>(move(proof));

    for (const auto &write : txn.writeset()) {
      if(!IsKeyOwned(write.key())) {
        continue;
      }

      ValueAndProof valProof;

      valProof.value = write.value();
      valProof.commitProof = commitProofPtr;
      Debug("applying write to key %s", write.key().c_str());
      commitStore.put(write.key(), valProof, ts);

      // GC stuff?
      // auto rtsItr = rts.find(write.key());
      // if (rtsItr != rts.end()) {
      //   auto itr = rtsItr->second.begin();
      //   auto endItr = rtsItr->second.upper_bound(ts);
      //   while (itr != endItr) {
      //     itr = rtsItr->second.erase(itr);
      //   }
      // }
    }

    // for (const auto &query : txn.queryset) {
    //   // This is just for verification in the above code, I don't think I have to
    //   // do anything with it here

    // }

    std::shared_ptr<tao::pq::transaction> tr;
    std::string client_seq_key;
    client_seq_key.append(std::to_string(gdecision.client_id()));
    client_seq_key.append("|");
    client_seq_key.append(std::to_string(gdecision.txn_seq_num()));

    if(txnMap.find(client_seq_key) == txnMap.end()) {
      auto connection = connectionPool->connection();
      tr = connection->transaction();
      connectionMap[client_seq_key] = connection;
      txnMap[client_seq_key] = tr;
    } else {
      tr = txnMap[client_seq_key];
    }

    try {
      tr->commit();
      groupedDecisionAck->set_status(REPLY_OK);
      txnMap.erase(client_seq_key);
      connectionMap.erase(client_seq_key);
    } catch(tao::pq::sql_error e) {
      groupedDecisionAck->set_status(REPLY_FAIL);
    }
    

    // mark txn as commited
    cleanupPendingTx(digest);
    // groupedDecisionAck->set_status(REPLY_OK);
  } else {
    stats.Increment("gdec_failed_valid",1);
    groupedDecisionAck->set_status(REPLY_FAIL);
  }

  Debug("decision ack status: %d", groupedDecisionAck->status());

  return returnMessage(groupedDecisionAck);
  // return nullptr;
}


::google::protobuf::Message* Server::HandleGroupedAbortDecision(const proto::GroupedDecision& gdecision) {
  proto::GroupedDecisionAck* groupedDecisionAck = new proto::GroupedDecisionAck();


  Debug("Handling Grouped abort Decision");
  string digest = gdecision.txn_digest();
  DebugHash(digest);

  atomicMutex.lock();
  if (pendingTransactions.find(digest) == pendingTransactions.end()) {

    Debug("Buffering gdecision");
    stats.Increment("buff_dec",1);
    // we haven't yet received the tx so buffer this gdecision until we get it
    bufferedGDecs[digest] = gdecision;
    atomicMutex.unlock();
    return nullptr;
  }
  atomicMutex.unlock();

 if(validate_abort){
   // struct timeval tp;
   // gettimeofday(&tp, NULL);
   // long int us = tp.tv_sec * 1000 * 1000 + tp.tv_usec;

   if(!verifyG_Abort_Decision(gdecision, pendingTransactions[digest], keyManager, signMessages, config.f)){
    //if(!verifyGDecision_Abort_parallel(gdecision, pendingTransactions[digest], keyManager, signMessages, config.f, tp)){
     Debug("failed validation for abort decision");
     return nullptr;
   }

   // gettimeofday(&tp, NULL);
   // long int lock_time = ((tp.tv_sec * 1000 * 1000 + tp.tv_usec) -us);
   // std::cerr << "Abort Verification takes " << lock_time << " microseconds" << std::endl;
 }
 std::unique_lock lock(atomicMutex);

  std::shared_ptr<tao::pq::transaction> tr;
  std::string client_seq_key;
  client_seq_key.append(std::to_string(gdecision.client_id()));
  client_seq_key.append("|");
  client_seq_key.append(std::to_string(gdecision.txn_seq_num()));

  if(txnMap.find(client_seq_key) == txnMap.end()) {
    auto connection = connectionPool->connection();
    tr = connection->transaction();
    connectionMap[client_seq_key] = connection;
    txnMap[client_seq_key] = tr;
  } else {
    tr = txnMap[client_seq_key];
  }

  tr->rollback();

  txnMap.erase(client_seq_key);
  connectionMap.erase(client_seq_key);

  groupedDecisionAck->set_txn_digest(digest);

  stats.Increment("gdec_failed",1);
  // abort the tx
  cleanupPendingTx(digest);
  // there is a chance that this abort comes before we see the tx, so save the decision
  abortedTxs.insert(digest);

  groupedDecisionAck->set_status(REPLY_FAIL);
  //
  return returnMessage(groupedDecisionAck);
  // return nullptr;
}

void Server::cleanupPendingTx(std::string digest) {
  if (pendingTransactions.find(digest) != pendingTransactions.end()) {
    proto::Transaction tx = pendingTransactions[digest];
    // remove prepared reads and writes
    Timestamp txTs(tx.timestamp());
    for (const auto& write : tx.writeset()) {
      if(!IsKeyOwned(write.key())) {
        continue;
      }
      preparedWrites[write.key()].erase(txTs);
    }
    for (const auto& read : tx.readset()) {
      if(!IsKeyOwned(read.key())) {
        continue;
      }
      preparedReads[read.key()].erase(txTs);
    }

    pendingTransactions.erase(digest);
  }
}

void Server::Load(const string &key, const string &value,
    const Timestamp timestamp) {
      // if (IsKeyOwned(key)) {
  ValueAndProof val;
  val.value = value;
  val.commitProof = dummyProof;
  commitStore.put(key, val, timestamp);

      // }
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
