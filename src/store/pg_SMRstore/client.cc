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
#include "store/pg_SMRstore/client.h"

#include "store/pg_SMRstore/common.h"
#include "lib/cereal/archives/binary.hpp"
#include "lib/cereal/types/string.hpp"

namespace pg_SMRstore {

using namespace std;

Client::Client(const transport::Configuration& config, uint64_t id, int nShards, int nGroups,
      const std::vector<int> &closestReplicas,
      Transport *transport, Partitioner *part,
      uint64_t readMessages, uint64_t readQuorumSize, bool signMessages,
      bool validateProofs, KeyManager *keyManager,TrueTime timeserver, 
      bool fake_SMR, uint64_t SMR_mode, const std::string &PG_BFTSMART_config_path) : config(config), nshards(nShards),
    ngroups(nGroups), transport(transport), part(part), readMessages(readMessages), readQuorumSize(readQuorumSize),
    signMessages(signMessages), validateProofs(validateProofs), keyManager(keyManager), timeServer(timeserver),
    fake_SMR(fake_SMR), SMR_mode(SMR_mode), PG_BFTSMART_config_path(PG_BFTSMART_config_path) {
  // just an invariant for now for everything to work ok
  assert(nGroups == nShards);

  client_id = id;

  client_seq_num = 0;

  bclient.reserve(ngroups);

  Notice("Initializing HotStuff Postgres client with id [%lu] %lu", client_id, ngroups);

  if(ngroups > 1) Panic("HS PG store does not support sharding");

  /* Start a client for each shard. */
  for (uint64_t i = 0; i < ngroups; i++) {
    bclient[i] = new ShardClient(config, transport, client_id, i, closestReplicas,
        signMessages, validateProofs, keyManager, &stats, fake_SMR, SMR_mode, PG_BFTSMART_config_path);
  }

  Notice("HotStuff Postgres client [%lu] created! %lu %lu", client_id, ngroups, bclient.size());

  //Test connecting directly.
  if(TEST_DIRECT_PG_CONNECTION){
    std::string connection_str = "host=us-east-1-0.pequin.pequin-pg0.utah.cloudlab.us user=pequin_user password=123 dbname=db1 port=5432";
    Notice("Connection string: %s", connection_str.c_str());
    connection = tao::pq::connection::create(connection_str);
    //connectionPool = tao::pq::connection_pool::create(connection_str);
  }
 
}

Client::~Client()
{
    for (auto b : bclient) {
        delete b;
    }

    if(SMR_mode == 2) BftSmartAgent::destroy_java_vm();
}

/* Begins a transaction. All subsequent operations before a commit() or abort() are part of this transaction. */
void Client::Begin(begin_callback bcb, begin_timeout_callback btcb, uint32_t timeout, bool retry) {
  transport->Timer(0, [this, bcb, btcb, timeout]() {
    
    client_seq_num++;
    Debug("BEGIN tx: ", client_seq_num);

    //Test connecting directly
    if(TEST_DIRECT_PG_CONNECTION){
      //connection = connectionPool->connection();
      transaction = connection->transaction();
    }

    bcb(client_seq_num);
  });
}

// TODO: consider invoke SQLRequest with the given parameters and a default db
void Client::Get(const std::string &key, get_callback gcb, get_timeout_callback gtcb, uint32_t timeout) {
  Panic("Client GET is not supported.");
}

// TODO: consider invoke SQLRequest with the given parameters and a default db
void Client::Put(const std::string &key, const std::string &value, put_callback pcb, put_timeout_callback ptcb, uint32_t timeout) {
  Panic("Client PUT is not supported.");
}


void Client::Commit(commit_callback ccb, commit_timeout_callback ctcb, uint32_t timeout) {

  //Test
  if(TEST_DIRECT_PG_CONNECTION){
   try {
    transaction->commit();
    transaction = nullptr; //reset txn
    ccb(COMMITTED);
    } catch (const std::exception &e) {
      //Panic("Commit should not fail while testing RW-SQL");
      const std::string &error_message = e.what();
      Debug("Commit Failed: %s. Aborting!", error_message.c_str());
      if (error_message.find("restart transaction") != std::string::npos) {
        transaction = nullptr;
      }
      ccb(ABORTED_SYSTEM);
    }
    return;
  }



  transport->Timer(0, [this, ccb, ctcb, timeout]() {
    try_commit_callback tccb = [ccb, this](int status) {

        //QUICK TEST
    //    transaction->commit();
    // transaction = nullptr; //reset txn
  
      if(status == REPLY_OK) {
        Debug("COMMIT SUCCESS");
        ccb(COMMITTED);
      } else {
        Debug("COMMIT ABORT");
        ccb(ABORTED_SYSTEM);
      }
    };
    
    Debug("Trying to commit txn: [%lu:%lu]", client_id, client_seq_num);
    bclient[0]->Commit(client_id, client_seq_num, tccb, ctcb, timeout);
  });
}

void Client::Abort(abort_callback acb, abort_timeout_callback atcb, uint32_t timeout) {

  //TEST
  if(TEST_DIRECT_PG_CONNECTION){
    try {
      transaction->rollback();
    } catch (...) {
      Panic("Rolling back Txn failed");
    }
    acb();
    return;
  }


  transport->Timer(0, [this, acb, atcb, timeout]() {
    Debug("Issue Abort (asynchronously)");
    bclient[0]->Abort(client_id, client_seq_num);
    acb();
  });
}

void Client::SQLRequest(std::string &statement, sql_callback scb, sql_timeout_callback stcb, uint32_t timeout){

  //QUICK TEST
  // transaction = connection->transaction();
  // transaction = nullptr;

    // struct timespec ts_start;
    // clock_gettime(CLOCK_MONOTONIC, &ts_start);
    // exec_start_us = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;



  if(TEST_DIRECT_PG_CONNECTION){
   try {
    if (transaction == nullptr) {
      Warning("Transaction has already been terminated. ReplyFail");
      scb(REPLY_FAIL, nullptr);
      return;
    }

    tao::pq::result result = transaction->execute(statement);
    taopq_wrapper::TaoPQQueryResultWrapper *tao_res = new taopq_wrapper::TaoPQQueryResultWrapper(std::make_unique<tao::pq::result>(std::move(result)));
    scb(REPLY_OK, tao_res);
  } catch (const tao::pq::integrity_constraint_violation &e) {
    Notice("Write[%s] exec failed with integrity violation: %s", statement.c_str(), e.what());
    auto result = new taopq_wrapper::TaoPQQueryResultWrapper();
    scb(REPLY_OK, result);
  } catch (const tao::pq::transaction_rollback &e) {
    Notice("Transaction rollback: %s", e.what());
    transaction->rollback();
    transaction = nullptr;
    scb(REPLY_FAIL, nullptr);
  } catch (const tao::pq::in_failed_sql_transaction &e) {
    Notice("In failed sql transaction: %s", e.what());
    transaction = nullptr;
    scb(REPLY_FAIL, nullptr);
  } catch (const std::exception &e) {
    Panic("Tx write failed with uncovered exception: %s", e.what());
  }
  return;
  }


  transport->Timer(0, [this, statement, scb, stcb, timeout](){

    Debug("Invoke SQL Request: %s", statement.c_str());

    sql_rpc_callback srcb = [scb, statement, this](int status, const std::string& sql_res) {
      Debug("Received query response");

      // struct timespec ts_start;
      // clock_gettime(CLOCK_MONOTONIC, &ts_start);
      // exec_end_us = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;
      // Notice("End to end exec latency: %lu us", exec_end_us - exec_start_us);
      
      //Deserialize sql_res and return to application.
      query_result::QueryResult* query_res;
      if(status == REPLY_OK) {
        Debug("Statement execution SUCCESS. Return result");
        query_res = new sql::QueryResultProtoWrapper(sql_res);
      } else {
        Debug("Statement execution FAILURE.");
        //This is simply a hack to force all follower replicas to also abort in order to make them unlock any held locks.
        if(fake_SMR) bclient[0]->Abort(client_id, client_seq_num); //TODO: FIXME: ADD THIS BACK?
        
        query_res = new sql::QueryResultProtoWrapper();
      }
      Debug("Upcalling");
      scb(status, query_res);

    };
    
    bclient[0]->Query(statement, client_id, client_seq_num, srcb, stcb, timeout);

  });
}


void Client::Query(const std::string &query, query_callback qcb, query_timeout_callback qtcb, uint32_t timeout,bool cache_result, bool skip_query_interpretation) {
    Debug("Processing Query Statement: %s", query.c_str());
    this->SQLRequest(const_cast<std::string &>(query), qcb, qtcb, timeout);
}


void Client::Write(std::string &write_statement, write_callback wcb, write_timeout_callback wtcb, uint32_t timeout, bool blind_write){
    Debug("Processing Write Statement: %s", write_statement.c_str());
    this->SQLRequest(write_statement, wcb, wtcb, timeout);
}

}
