// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/indicusstore/client.cc:
 *   Client to PostgreSQL database.
 *
 * Copyright 2022 Florian Suri-Payer <fsp@cs.cornell.edu>
 *                Liam Arzola <lma77@cornell.edu>
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

#include "store/postgresstore/client.h"

namespace postgresstore {

Client::Client(std::string connection_str, std::string experiment_name, bool pg_replicated, std::uint64_t id) : client_id(id), txn_id(0UL) {
  Notice("Initializing PostgreSQL client with id [%lu]", client_id);

  std::string port="5432";
  if (pg_replicated){
    port="5433";
  }
  // worked from terminal (for replication); 
  //  psql -h us-east-1-0.pg-smr-wis.pequin-pg0.wisc.cloudlab.us -U pequin_user -p 5433 -d db1

  //connection_str = "host=us-east-1-0.postgres-test.pequin-pg0.utah.cloudlab.us user=pequin_user password=123 dbname=db1 port=5432";
  //connection_str = "postgres://pequin_user:123@us-east-1-0.postgres-test.pequin-pg0.utah.cloudlab.us:5432/db1";
  
  //connection_str = "host=us-east-1-0.pequin.pequin-pg0.utah.cloudlab.us user=pequin_user password=123 dbname=db1 port=5432";
  //TODO: Parameterize further
  // connection_str = "host=us-east-1-0." + experiment_name + ".pequin-pg0.utah.cloudlab.us dbname=postgres port="+port;
  connection_str = "host=us-east-1-0." + experiment_name + ".pequin-pg0.utah.cloudlab.us user=pequin_user password=123 dbname=db1 port="+port;
  // connection_str = "host=us-east-1-0." + experiment_name + ".pequin-pg0.wisc.cloudlab.us user=pequin_user password=123 dbname=db1 port="+port;

  Notice("Connection string: %s", connection_str.c_str());
  connection = tao::pq::connection::create(connection_str);
 
  Notice("PostgreSQL client [%lu] created!", client_id);
}

Client::~Client()
{
}

/* Begins a transaction. All subsequent operations before a commit() or
 * abort() are part of this transaction.
 */
void Client::Begin(begin_callback bcb, begin_timeout_callback btcb,
    uint32_t timeout, bool retry, const std::string &txnState) {
  Debug("Begin Txn...");

  transaction = connection->transaction();
  txn_id++;
  bcb(txn_id);
}

void Client::Get(const std::string &key, get_callback gcb,
    get_timeout_callback gtcb, uint32_t timeout) {
  auto result = transaction->execute("SELECT $1 FROM kv", key);
  const std::string result_str = result[0][0].as<std::string>();
  gcb(0, key, result_str, Timestamp(0));
}

void Client::Put(const std::string &key, const std::string &value,
    put_callback pcb, put_timeout_callback ptcb, uint32_t timeout) {
  auto result = transaction->execute("INSERT INTO kv VALUES ($1, $2)", key, value);
  pcb(REPLY_OK, key, value);
}

void Client::Commit(commit_callback ccb, commit_timeout_callback ctcb,
    uint32_t timeout) {
  
  Debug("Try Committing txn: %d", txn_id);
  try {
    transaction->commit();
    transaction = nullptr; //reset txn
    stats.Increment("num_commit", 1);
    Debug("Commit success! :)");
    ccb(COMMITTED);
  } catch (const std::exception &e) {
    const std::string &error_message = e.what();
    Debug("Commit Failed: %s. Aborting!", error_message.c_str());
    if (error_message.find("restart transaction") != std::string::npos) {
      transaction = nullptr;
    }
    stats.Increment("num_aborts", 1);
    ccb(ABORTED_SYSTEM);
  }
}

void Client::Abort(abort_callback acb, abort_timeout_callback atcb,
    uint32_t timeout) {
  
  Debug("Abort Txn: %d", txn_id);
  try {
    transaction->rollback();
  } catch (...) {
    Panic("Rolling back Txn failed");
  }
  acb();
}

// Get the value corresponding to key.
inline void Client::Query(const std::string &query_statement, query_callback qcb,
    query_timeout_callback qtcb, uint32_t timeout, bool cache_result, bool skip_query_interpretation) {
  
    Debug("Processing Query Statement: %s", query_statement.c_str());
    stats.Increment("queries_issued", 1);
    this->SQLRequest(const_cast<std::string &>(query_statement), qcb, qtcb, timeout);
}

// Execute the write operation and return the result.
inline void Client::Write(std::string &write_statement, write_callback wcb,
      write_timeout_callback wtcb, uint32_t timeout, bool blind_write) {
  Debug("Processing Write Statement: %s", write_statement.c_str());
   stats.Increment("writes_issued", 1);
  this->SQLRequest(write_statement, wcb, wtcb, timeout);
}


void printResult(const tao::pq::result sql_res){
  std::cerr << "number of cols:   "<< sql_res.columns() <<"\n" ;

  if(sql_res.columns() == 0) {
    std::cerr << "empty result, e.g.:  inserts\n" ;
    return;
  }
  if (sql_res.empty()){
    std::cerr << "empty res\n" ;
  }
    for( const auto& row : sql_res ) {
      for( const auto& field : row ) {
        std::string field_str = field.as<std::string>();
        std::cerr << "Row:  "<< field_str <<"\n";
      }
    }
}



void Client::SQLRequest(std::string &statement, sql_callback scb, sql_timeout_callback stcb, uint32_t timeout){

  if (statement.find("INSERT") != std::string::npos){
    if(statement.back() == ';'){
      statement.pop_back(); // Remove the last character
    }
    statement= statement +" ON CONFLICT DO NOTHING;";
  }

  try {
    if (transaction == nullptr) {
      Debug("Transaction has already been terminated. ReplyFail");
      scb(REPLY_FAIL, nullptr);
      return;
    }

    tao::pq::result result = transaction->execute(statement);
    // printResult(result);
    // Debug("Completed SQLRequest execution: %s", statement.c_str());
    taopq_wrapper::TaoPQQueryResultWrapper *tao_res = new taopq_wrapper::TaoPQQueryResultWrapper(std::make_unique<tao::pq::result>(std::move(result)));
    // Debug("Completed creating result for: %s", statement.c_str());
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
  } catch (const tao::pq::lock_not_available &e) {
    Notice("In lock timeout: %s", e.what());
    transaction->rollback();
    transaction = nullptr;
    scb(REPLY_FAIL, nullptr);
  } 
  catch (const std::exception &e) {
    Panic("Tx write failed with uncovered exception: %s", e.what());
  }
}

} // namespace postgresqlstore
