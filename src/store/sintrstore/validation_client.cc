/***********************************************************************
 *
 * Copyright 2024 Austin Li <atl63@cornell.edu>
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

#include "store/sintrstore/validation_client.h"
#include "store/sintrstore/common.h"
#include "lib/message.h"

#include "store/common/query_result/query_result.h"
#include "store/common/query_result/query_result_proto_wrapper.h"
#include "store/common/query_result/query_result_proto_builder.h"

namespace sintrstore {

ValidationClient::ValidationClient(Transport *transport, uint64_t client_id, uint64_t nshards, uint64_t ngroups, Partitioner *part,
  const QueryParameters* query_params) : 
  transport(transport), client_id(client_id), nshards(nshards), ngroups(ngroups), part(part), query_params(query_params) {}

ValidationClient::~ValidationClient() {
  // TODO: Garbage collection/free memory for allValTxnStates
  for (auto it = clientIDtoSQL.begin(); it != clientIDtoSQL.end(); ++it) {
    delete it->second;
  }
  clientIDtoSQL.clear();
  for (auto it = allValTxnStates.begin(); it != allValTxnStates.end(); ++it) {
    delete it->second;
  }
  allValTxnStates.clear();
}

void ValidationClient::Begin(begin_callback bcb, begin_timeout_callback btcb,
    uint32_t timeout, bool retry, const std::string &txnState) {
  uint64_t txn_client_id, txn_client_seq_num;
  GetThreadValTxnId(&txn_client_id, &txn_client_seq_num);
  std::string txn_id = ToTxnId(txn_client_id, txn_client_seq_num);

  allValTxnStatesMap::accessor a;
  if (!allValTxnStates.find(a, txn_id)) {
    // Begin should always happen after SetTxnTimestamp, which inserts at txn_id
    Panic("cannot find transaction %s in allValTxnStates", txn_id.c_str());
  }
  a.release();
  bcb(txn_client_seq_num);
}

void ValidationClient::Get(const std::string &key, get_callback gcb,
    get_timeout_callback gtcb, uint32_t timeout) {
  // define callback for when get completes
  validation_read_callback vrcb = [gcb, this](int status, uint64_t txn_client_id, uint64_t txn_client_seq_num, 
      const std::string &key, const std::string &value, const Timestamp &ts) {
    
    Debug("validation_read_callback on key %s, value %s", BytesToHex(key, 16).c_str(), BytesToHex(value, 16).c_str());
    gcb(status, key, value, ts);
  };

  uint64_t txn_client_id, txn_client_seq_num;
  GetThreadValTxnId(&txn_client_id, &txn_client_seq_num);
  std::string txn_id = ToTxnId(txn_client_id, txn_client_seq_num);

  allValTxnStatesMap::accessor a;
  if (!allValTxnStates.find(a, txn_id)) {
    // Get should always happen after SetTxnTimestamp, which inserts at txn_id
    Panic("cannot find transaction %s in allValTxnStates", txn_id.c_str());
  }
  proto::Transaction *txn = a->second->txn;
  // edit the involved groups for txn
  std::vector<int> txnGroups(txn->involved_groups().begin(), txn->involved_groups().end());
  int i = (*part)(key, nshards, -1, txnGroups) % ngroups;
  if (!IsTxnParticipant(txn, i)) {
    txn->add_involved_groups(i);
  }

  // read locally in buffer
  if (BufferGet(a->second, key, vrcb)) {
    Debug(
      "ValidationClient::BufferGet for client id %lu, seq num %lu, on key %s", 
      txn_client_id,
      txn_client_seq_num,
      BytesToHex(key, 16).c_str()
    );
    return;
  }
  // check if forward read result already received (if callback exists)
  if(a->second->pendingForwardedReadCB.find(key) != a->second->pendingForwardedReadCB.end()) {
    Debug("Adding queried get to readset for key %s", BytesToHex(key, 16).c_str());
    std::pair<std::string, Timestamp> res = a->second->pendingForwardedReadCB[key](a->second);
    vrcb(REPLY_OK, txn_client_id, txn_client_seq_num, key, res.first, res.second);
    a->second->pendingForwardedReadCB.erase(key);
    return;
  }

  Debug(
    "ValidationClient::Get registering PendingGet for client id %lu, seq num %lu on key %s", 
    txn_client_id, 
    txn_client_seq_num, 
    BytesToHex(key, 16).c_str()
  );

  // otherwise have to wait for read results to get passed over
  PendingValidationGet *pendingGet = new PendingValidationGet(txn_client_id, txn_client_seq_num);
  pendingGet->key = key;
  pendingGet->vrcb = vrcb;
  pendingGet->vrtcb = gtcb;

  a->second->pendingGets.push_back(pendingGet);

  pendingGet->timeout = new Timeout(transport, 2000, [this, txn_id, pendingGet]() {
    allValTxnStatesMap::accessor a;
    if (!allValTxnStates.find(a, txn_id)) {
      // transaction has completed
      return;
    }
    std::vector<PendingValidationGet *> pendingGets = a->second->pendingGets;

    auto reqs_itr = std::find_if(
      pendingGets.begin(), pendingGets.end(), 
      [curr_key=pendingGet->key](const PendingValidationGet *req) { return req->key == curr_key; }
    );
    if (reqs_itr == pendingGets.end()) {
      // pendingGet fulfilled
      return;
    }
    Panic("Timeout triggered for txn_id %s key %s", txn_id.c_str(), BytesToHex(pendingGet->key, 16).c_str());
    // pendingGet->vrtcb(REPLY_TIMEOUT, pendingGet->key);
  });

  pendingGet->timeout->Reset();
}

void ValidationClient::Put(const std::string &key, const std::string &value,
    put_callback pcb, put_timeout_callback ptcb,
    uint32_t timeout) {
  uint64_t txn_client_id, txn_client_seq_num;
  GetThreadValTxnId(&txn_client_id, &txn_client_seq_num);
  std::string txn_id = ToTxnId(txn_client_id, txn_client_seq_num);

  allValTxnStatesMap::accessor a;
  if (!allValTxnStates.find(a, txn_id)) {
    // Put should always happen after SetTxnTimestamp, which inserts at txn_id
    Panic("cannot find transaction %s in allValTxnStates", txn_id.c_str());
  }

  proto::Transaction *txn = a->second->txn;
  WriteMessage *write = txn->add_write_set();
  write->set_key(key);
  write->set_value(value);

  std::vector<int> txnGroups(txn->involved_groups().begin(), txn->involved_groups().end());
  int i = (*part)(key, nshards, -1, txnGroups) % ngroups;
  if (!IsTxnParticipant(txn, i)) {
    txn->add_involved_groups(i);
  }

  a.release();
  pcb(REPLY_OK, key, value);
}

void ValidationClient::SQLRequest(std::string &statement, sql_callback scb,
  sql_timeout_callback stcb, uint32_t timeout){

  size_t pos;
  if((pos = statement.find(select_hook) != string::npos)){  
    Query(statement, std::move(scb), std::move(stcb), timeout);
  }
  else {
    Write(statement, std::move(scb), std::move(stcb), timeout);
  }
}

void ValidationClient::Write(std::string &write_statement, write_callback wcb,
  write_timeout_callback wtcb, uint32_t timeout, bool blind_write){ //blind_write: default false, must be explicit application choice to skip.

  Debug("Processing Write Statement: %s", write_statement.c_str());
  std::string read_statement;
  std::function<void(int, query_result::QueryResult*)>  write_continuation;
  bool skip_query_interpretation = false;
  uint64_t point_target_group = 0;

  uint64_t txn_client_id, txn_client_seq_num;
  std::string txn_id = ToTxnId(txn_client_id, txn_client_seq_num);
  allValTxnStatesMap::accessor a;
  if (!allValTxnStates.find(a, txn_id)) {
    // Write should always happen after SetTxnTimestamp, which inserts at txn_id
    Panic("cannot find transaction %s in allValTxnStates", txn_id.c_str());
  }
    
  proto::Transaction *txn = a->second->txn;
  GetThreadValTxnId(&txn_client_id, &txn_client_seq_num);
  ClientToSQLInterpreterMap::accessor ca;
  if (!clientIDtoSQL.find(ca, txn_client_id)) {
    // Write should always happen after SetTxnTimestamp, which inserts at txn_id
    Panic("cannot find client ID %d in client accessor", txn_client_id);
  }
  SQLTransformer *sql_interpreter = ca->second;

  a->second->pendingWriteStatements.push_back(write_statement);

  try{
    sql_interpreter->TransformWriteStatement(a->second->pendingWriteStatements.back(), read_statement, write_continuation, wcb, point_target_group, skip_query_interpretation, blind_write);
    ca.release();
  }
  catch(...){
    Panic("bug in transformer: %s -> %s", write_statement.c_str(), read_statement.c_str());
  }

  Debug("Transformed Write into re-con read_statement: %s", read_statement.c_str());

  //Testing/Debug only
  //  Debug("Current read set: Before next write.");
  //  for(auto read: txn.read_set()){
  //     Debug("Read set already contains: %s", read.key().c_str());
  //   }

  if(read_statement.empty()){ //Must be point operation (Insert/Delete)
    Debug("No read statement, immediately writing in validation client");  
    sql::QueryResultProtoWrapper *write_result = new sql::QueryResultProtoWrapper("");
    
    if (!IsTxnParticipant(txn, point_target_group)) {
      txn->add_involved_groups(point_target_group);
    }
    a.release();

    write_continuation(REPLY_OK, write_result);
  }
  else{
    Debug("Issuing re-con Query validation");
    a.release();
    Query(read_statement, std::move(write_continuation), wtcb, timeout, false, skip_query_interpretation); //cache_result = false
  }
  return;
}

void ValidationClient::Query(const std::string &query, query_callback qcb,
  query_timeout_callback qtcb, uint32_t timeout, bool cache_result, bool skip_query_interpretation) {

  UW_ASSERT(query.length() < ((uint64_t)1<<32));    
  uint64_t txn_client_id, txn_client_seq_num;
  GetThreadValTxnId(&txn_client_id, &txn_client_seq_num);
  std::string txn_id = ToTxnId(txn_client_id, txn_client_seq_num);  
  allValTxnStatesMap::accessor a;
  if (!allValTxnStates.find(a, txn_id)) {
    Panic("cannot find transaction %s in allValTxnStates for query", txn_id.c_str());
  }
  proto::Transaction *txn = a->second->txn;

  Debug("Query[%lu:%lu] (client:tx-seq). TS: [%lu:%lu]: %s.", 
      client_id, txn_client_seq_num, txn->timestamp().timestamp(), txn->timestamp().id(), query.c_str());

  PendingValidationQuery *pendingQuery = new PendingValidationQuery(Timestamp(txn->timestamp()), query, qcb, cache_result);
  ClientToSQLInterpreterMap::accessor ca;
  if (!clientIDtoSQL.find(ca, txn_client_id)) {
    Panic("cannot find client ID %d in client accessor", txn_client_id);
  }
  SQLTransformer* sql_interpreter = ca->second;
  
  pendingQuery->is_point = skip_query_interpretation? false : sql_interpreter->InterpretQueryRange(query, pendingQuery->table_name, pendingQuery->p_col_values, true); 
  ca.release();
  Debug("Query is of type: %s ", pendingQuery->is_point? "POINT" : "RANGE");
  if(pendingQuery->is_point){
    Debug("Encoded key: %s", EncodeTableRow(pendingQuery->table_name, pendingQuery->p_col_values).c_str()); 
    std::string encoded_key = EncodeTableRow(pendingQuery->table_name, pendingQuery->p_col_values);
    auto itr = a->second->point_read_cache.find(encoded_key);
    if(itr != a->second->point_read_cache.end()){
      Debug("Supply point query result from cache!");
      auto res = new sql::QueryResultProtoWrapper(itr->second);
      qcb(REPLY_OK, res);
      delete pendingQuery;
      pendingQuery = nullptr;
      return;
    }
    // check if forward read result already received (if callback exists)
    if(a->second->pendingForwardedPointQueryCB.find(encoded_key) != a->second->pendingForwardedPointQueryCB.end()) {
      Debug("Adding point query to readset for key %s", BytesToHex(encoded_key, 16).c_str());
      auto res = new sql::QueryResultProtoWrapper(a->second->pendingForwardedPointQueryCB[encoded_key](a->second));
      a->second->pendingForwardedPointQueryCB.erase(encoded_key);
      qcb(REPLY_OK, res);
      delete pendingQuery;
      pendingQuery = nullptr;
      return;
    }
  } 
  else{
    auto itr = a->second->scan_read_cache.find(query);
    if(itr != a->second->scan_read_cache.end()){
      Debug("Supply scan query result from cache! Query: %s", query.c_str());

      auto res = new sql::QueryResultProtoWrapper(itr->second);
      qcb(REPLY_OK, res);
      delete pendingQuery;
      pendingQuery = nullptr;
      return;
    }
    // probably need to check txn read set & query_set of txn
    // to find if query result has already been forwarded
    // check if forward query result already received (if callback exists)
    if(a->second->pendingForwardedQueryCB.find(pendingQuery->query_gen_id) != a->second->pendingForwardedQueryCB.end()) {
      Debug("Adding query %s result to readset", pendingQuery->query_gen_id.c_str());
      auto res = new sql::QueryResultProtoWrapper(
        a->second->pendingForwardedQueryCB[pendingQuery->query_gen_id](a->second, pendingQuery, cache_result));
        a->second->pendingForwardedQueryCB.erase(pendingQuery->query_gen_id);
      qcb(REPLY_OK, res);
      delete pendingQuery;
      pendingQuery = nullptr;
      return;
    }
  }
  a->second->pendingQueries.push_back(pendingQuery);

  // Contact the appropriate shard to get the value.
  std::vector<int> txnGroups(txn->involved_groups().begin(), txn->involved_groups().end());

  int target_group = (*part)(pendingQuery->table_name, query, nshards, -1, txnGroups, false) % ngroups;

  std::vector<uint64_t> involved_groups = {target_group};
  
  for(auto &i: involved_groups){
    if (!IsTxnParticipant(txn, i)) {
      txn->add_involved_groups(i);
    }
  }
  pendingQuery->timeout = new Timeout(transport, 2000, [this, txn_id, pendingQuery]() {
    allValTxnStatesMap::accessor a;
    if (!allValTxnStates.find(a, txn_id)) {
        // transaction has completed
      return;
    }
    std::vector<PendingValidationQuery *> pendingQueries = a->second->pendingQueries;
  
    auto reqs_itr = std::find_if(
      pendingQueries.begin(), pendingQueries.end(), 
      [curr_gen_id=pendingQuery->query_gen_id](const PendingValidationQuery *req) { return req->query_gen_id == curr_gen_id; }
    );
    if (reqs_itr == pendingQueries.end()) {
      // pendingQuery fulfilled
      return;
    }
    Panic("Timeout triggered for txn_id %s key %s", txn_id.c_str(), BytesToHex(pendingQuery->query_gen_id, 16).c_str());
  });
  
  pendingQuery->timeout->Reset();
}
void ValidationClient::Commit(commit_callback ccb, commit_timeout_callback ctcb,
    uint32_t timeout) {
  ccb(COMMITTED);
}

void ValidationClient::Abort(abort_callback acb, abort_timeout_callback atcb,
    uint32_t timeout) {
  // on abort, clean up stored data
  uint64_t txn_client_id, txn_client_seq_num;
  GetThreadValTxnId(&txn_client_id, &txn_client_seq_num);
  std::string txn_id = ToTxnId(txn_client_id, txn_client_seq_num);

  allValTxnStatesMap::accessor a;
  if (!allValTxnStates.find(a, txn_id)) {
    // Abort should always happen after SetTxnTimestamp, which inserts at txn_id
    Panic("cannot find transaction %s in allValTxnStates", txn_id.c_str());
  }
  delete a->second->txn;
  allValTxnStates.erase(a);
  a.release();
  acb();
}

void ValidationClient::SetThreadValTxnId(uint64_t txn_client_id, uint64_t txn_client_seq_num) {
  threadValTxnIdsMap::accessor a;
  threadValTxnIds.insert(a, std::this_thread::get_id());
  a->second = std::make_pair(txn_client_id, txn_client_seq_num);
}

void ValidationClient::SetTxnTimestamp(uint64_t txn_client_id, uint64_t txn_client_seq_num, const Timestamp &ts) {
  std::string txn_id = ToTxnId(txn_client_id, txn_client_seq_num);
  allValTxnStatesMap::accessor a;
  const bool isNewKey = allValTxnStates.insert(a, txn_id);
  proto::Transaction *txn;
  if (isNewKey) {
    txn = new proto::Transaction();
    txn->set_client_id(txn_client_id);
    txn->set_client_seq_num(txn_client_seq_num);
    a->second = new AllValidationTxnState(txn_client_id, txn_client_seq_num, txn);
  } 
  else {
    txn = a->second->txn;
  }
  ClientToSQLInterpreterMap::accessor ac;
  const bool isNewClientID = clientIDtoSQL.insert(ac, txn_client_id);
  if(isNewClientID) {
    // TODO: Add query params object
    SQLTransformer* sql_interpreter = new SQLTransformer(query_params);
    ac->second = sql_interpreter;
  } else {
    ac->second->NewTx(txn);
  }
  ts.serialize(txn->mutable_timestamp());
}

void ValidationClient::ProcessForwardReadResult(uint64_t txn_client_id, uint64_t txn_client_seq_num, 
    const proto::ForwardReadResult &fwdReadResult, const proto::Dependency &dep, bool hasDep, bool addReadset,
    const proto::Dependency &policyDep, bool hasPolicyDep) {
  std::string curr_key = fwdReadResult.key();
  std::string curr_value = fwdReadResult.value();
  Timestamp curr_ts = Timestamp(fwdReadResult.timestamp());
  Debug(
    "ProcessForwardReadResult from client id %lu, seq num %lu for key %s", 
    txn_client_id,
    txn_client_seq_num,
    BytesToHex(curr_key, 16).c_str()
  );

  // lambda for editing txn state
  auto editTxnStateCB = [
    this, curr_key, curr_value, curr_ts, dep, hasDep, addReadset, policyDep, hasPolicyDep
  ](AllValidationTxnState *allValTxnState) {
    if (addReadset) {
      AddReadset(allValTxnState, curr_key, curr_value, curr_ts);
    }
    if (hasDep) {
      AddDep(allValTxnState, dep);
    }
    if (hasPolicyDep) {
      AddDep(allValTxnState, policyDep);
    }
    return std::make_pair(curr_value, curr_ts);
  };

  // find matching pending get by first going off txn client id and sequence number, then key
  // if forwarded read result is for a get that the validation transaction has not yet gotten to,
  // add it to the appropriate transaction readset

  std::string curr_txn_id = ToTxnId(txn_client_id, txn_client_seq_num);

  allValTxnStatesMap::accessor a;
  const bool isNewKey = allValTxnStates.insert(a, curr_txn_id);
  if (isNewKey) {
    Debug(
      "ProcessForwardReadResult from client id %lu, seq num %lu, before txn_id in allValTxnStates registered for key %s", 
      txn_client_id,
      txn_client_seq_num,
      BytesToHex(curr_key, 16).c_str()
    );
    proto::Transaction *txn = new proto::Transaction();
    txn->set_client_id(txn_client_id);
    txn->set_client_seq_num(txn_client_seq_num);
    a->second = new AllValidationTxnState(txn_client_id, txn_client_seq_num, txn);
    a->second->pendingForwardedReadCB[curr_key] = editTxnStateCB;
    ClientToSQLInterpreterMap::accessor ac;
    const bool isNewClientID = clientIDtoSQL.insert(ac, txn_client_id);
    if(isNewClientID) {
      // TODO: Add query params object
      SQLTransformer *sql_interpreter = new SQLTransformer(query_params);
      ac->second = sql_interpreter;
    } else {
      ac->second->NewTx(txn);
    }
    return;
  }

  std::vector<PendingValidationGet *> *reqs = &a->second->pendingGets;
  auto reqs_itr = std::find_if(
    reqs->begin(), reqs->end(), 
    [&curr_key](const PendingValidationGet *req) { return req->key == curr_key; }
  );
  if (reqs_itr == reqs->end()) {
    Debug(
      "ProcessForwardReadResult from client id %lu, seq num %lu, before PendingGet registered for key %s", 
      txn_client_id,
      txn_client_seq_num,
      BytesToHex(curr_key, 16).c_str()
    );
    a->second->pendingForwardedReadCB[curr_key] = editTxnStateCB;
    return;
  }
  // callback
  PendingValidationGet *req = *reqs_itr;

  struct timespec ts_end;
  clock_gettime(CLOCK_MONOTONIC, &ts_end);
  uint64_t end = ts_end.tv_sec * 1000 * 1000 + ts_end.tv_nsec / 1000;
  auto duration = end - req->start_time;
  // Warning("PendingValidationGet took %lu us", duration);

  req->ts = curr_ts;
  editTxnStateCB(a->second);
  req->vrcb(REPLY_OK, txn_client_id, txn_client_seq_num, req->key, curr_value, req->ts);

  // remove from vector
  reqs->erase(reqs_itr);
  // free memory
  delete req;
}

void ValidationClient::ProcessForwardPointQueryResult(uint64_t txn_client_id, uint64_t txn_client_seq_num, 
    const proto::ForwardReadResult &fwdReadResult, const proto::Dependency &dep, bool hasDep, bool addReadset) {
  std::string curr_key = fwdReadResult.key();
  std::string curr_value = fwdReadResult.value();
  Timestamp curr_ts = Timestamp(fwdReadResult.timestamp());
  Debug(
    "ProcessForwardPointQueryResult from client id %lu, seq num %lu for key %s", 
    txn_client_id,
    txn_client_seq_num,
    BytesToHex(curr_key, 16).c_str()
  );

  // lambda for editing txn state
  // TODO: need to know query cmd, but what if forward comes before validation query is registered?
  auto editTxnStateCB = [
    this, curr_key, curr_value, curr_ts, dep, hasDep, addReadset
  ](AllValidationTxnState *allValTxnState) {
    if (addReadset) {
      bool cache_point = !curr_value.empty(); // && pendingQuery->queryMsg.query_cmd().find("SELECT *") != std::string::npos;
      AddReadset(allValTxnState, curr_key, curr_value, curr_ts, false, cache_point);
    }
    if (hasDep) {
      AddDep(allValTxnState, dep);
    }
    return curr_value;
  };

  std::string curr_txn_id = ToTxnId(txn_client_id, txn_client_seq_num);

  allValTxnStatesMap::accessor a;
  const bool isNewKey = allValTxnStates.insert(a, curr_txn_id);
  if (isNewKey) {
    Debug(
      "ProcessForwardPointQueryResult from client id %lu, seq num %lu, before txn_id in allValTxnStates registered for key %s", 
      txn_client_id,
      txn_client_seq_num,
      BytesToHex(curr_key, 16).c_str()
    );
    proto::Transaction *txn = new proto::Transaction();
    txn->set_client_id(txn_client_id);
    txn->set_client_seq_num(txn_client_seq_num);
    a->second = new AllValidationTxnState(txn_client_id, txn_client_seq_num, txn);
    a->second->pendingForwardedPointQueryCB[curr_key] = editTxnStateCB;
    return;
  }

  std::vector<PendingValidationQuery *> *reqs = &a->second->pendingQueries;
  auto reqs_itr = std::find_if(
    reqs->begin(), reqs->end(),
    [&curr_key](const PendingValidationQuery *req) { return req->is_point && req->key == curr_key; }
  );
  if (reqs_itr == reqs->end()) {
    Debug(
      "ProcessForwardPointQueryResult from client id %lu, seq num %lu, before PendingValidationQuery registered for key %s", 
      txn_client_id,
      txn_client_seq_num,
      BytesToHex(curr_key, 16).c_str()
    );
    a->second->pendingForwardedPointQueryCB[curr_key] = editTxnStateCB;
    return;
  }
  // callback
  PendingValidationQuery *req = *reqs_itr;

  req->ts = curr_ts;
  editTxnStateCB(a->second);
  sql::QueryResultProtoWrapper *q_result = new sql::QueryResultProtoWrapper(curr_value);
  req->vqcb(REPLY_OK, q_result);

  // remove from vector
  reqs->erase(reqs_itr);
  // free memory
  delete req;
}

void ValidationClient::ProcessForwardQueryResult(uint64_t txn_client_id, uint64_t txn_client_seq_num, 
    const proto::ForwardQueryResult &fwdQueryResult, const std::map<uint64_t, proto::QueryGroupMeta> &queryGroupMeta,
    bool addReadset) {
  std::string curr_query_gen_id = fwdQueryResult.query_gen_id();
  std::string curr_query_result = fwdQueryResult.query_result();
  Debug(
    "ProcessForwardQueryResult from client id %lu, seq num %lu, query gen id %s, query result %s", 
    txn_client_id, 
    txn_client_seq_num,
    BytesToHex(curr_query_gen_id, 16).c_str(),
    BytesToHex(curr_query_result, 16).c_str()
  );

  // lambda for editing txn state
  auto editTxnStateCB = [
    this, curr_query_gen_id, curr_query_result, queryGroupMeta, addReadset
  ](AllValidationTxnState *allValTxnState, PendingValidationQuery* pendingQuery, bool cache_result) {
    sql::QueryResultProtoWrapper *q_result = new sql::QueryResultProtoWrapper(curr_query_result);
    if (addReadset) {
      AddQueryReadset(allValTxnState, queryGroupMeta);
    }
    if (!q_result->empty() && cache_result) {
      // Only cache if we did a Select *, i.e. we have the full row, and thus it can be used by Update
      // if(size_t pos = pendingQuery->queryMsg.query_cmd().find("SELECT *"); pos != std::string::npos){
      //   allValTxnState->scan_read_cache[pendingQuery->queryMsg.query_cmd()] = curr_query_result;  
      // }
    }
    delete q_result;
    q_result = nullptr;
    return curr_query_result;
  };

  // find matching pending query by first going off txn client id and sequence number, then query_gen_id
  // if forwarded query result is for a query that the validation transaction has not yet gotten to,
  // add it to the appropriate transaction query result cache

  std::string curr_txn_id = ToTxnId(txn_client_id, txn_client_seq_num);

  allValTxnStatesMap::accessor a;
  const bool isNewKey = allValTxnStates.insert(a, curr_txn_id);
  if (isNewKey) {
    Debug(
      "ProcessForwardQueryResult from client id %lu, seq num %lu, before txn_id in allValTxnStates registered for query %s", 
      txn_client_id,
      txn_client_seq_num,
      BytesToHex(curr_query_gen_id, 16).c_str()
    );
    proto::Transaction *txn = new proto::Transaction();
    txn->set_client_id(txn_client_id);
    txn->set_client_seq_num(txn_client_seq_num);
    a->second = new AllValidationTxnState(txn_client_id, txn_client_seq_num, txn);
    a->second->pendingForwardedQueryCB[curr_query_gen_id] = editTxnStateCB;
    return;
  }

  std::vector<PendingValidationQuery *> *reqs = &a->second->pendingQueries;
  auto reqs_itr = std::find_if(
    reqs->begin(), reqs->end(), 
    [&curr_query_gen_id](const PendingValidationQuery *req) { return req->query_gen_id == curr_query_gen_id; }
  );
  if (reqs_itr == reqs->end()) {
    Debug(
      "ProcessForwardQueryResult from client id %lu, seq num %lu, before PendingQuery registered for query %s", 
      txn_client_id,
      txn_client_seq_num,
      BytesToHex(curr_query_gen_id, 16).c_str()
    );
    a->second->pendingForwardedQueryCB[curr_query_gen_id] = editTxnStateCB;
    return;
  }

  // callback
  PendingValidationQuery *req = *reqs_itr;

  editTxnStateCB(a->second, req, req->cache_result);

  sql::QueryResultProtoWrapper *q_result = new sql::QueryResultProtoWrapper(curr_query_result);
  req->vqcb(REPLY_OK, q_result);
  // no need to delete q_result since the query callback will take care of it

  // remove from vector
  reqs->erase(reqs_itr);
  // free memory
  delete req;
}

proto::Transaction *ValidationClient::GetCompletedTxn(uint64_t txn_client_id, uint64_t txn_client_seq_num) {
  std::string txn_id = ToTxnId(txn_client_id, txn_client_seq_num);
  allValTxnStatesMap::accessor a;
  if (!allValTxnStates.find(a, txn_id)) {
    // GetCompletedTxn is called after validation has completed
    // so txn_id must be in allValTxnStates
    Panic("cannot find transaction %s in allValTxnStates", txn_id.c_str());
  }
  proto::Transaction *txn = a->second->txn;

  Debug(
    "ValidationClient::GetCompletedValTxn called for txn client id %lu, seq num %lu",
    txn_client_id,
    txn_client_seq_num
  );

  allValTxnStates.erase(a);
  return txn;
}

bool ValidationClient::BufferGet(const AllValidationTxnState *allValTxnState, const std::string &key, 
    validation_read_callback vrcb) {
  uint64_t txn_client_id = allValTxnState->txn_client_id;
  uint64_t txn_client_seq_num = allValTxnState->txn_client_seq_num;
  proto::Transaction *txn = allValTxnState->txn;
  for (const auto &write : txn->write_set()) {
    if (write.key() == key) {
      vrcb(REPLY_OK, txn_client_id, txn_client_seq_num, key, write.value(), Timestamp());
      return true;
    }
  }

  for (const auto &read : txn->read_set()) {
    if (read.key() == key) {
      vrcb(REPLY_OK, txn_client_id, txn_client_seq_num, key, allValTxnState->readValues.at(key), read.readtime());
      return true;
    }
  }

  return false;
}

void ValidationClient::AddReadset(AllValidationTxnState *allValTxnState,
    const std::string &key, const std::string &value, const Timestamp &ts,
    bool is_get, bool cache_point) {
  // add to readset
  proto::Transaction *txn = allValTxnState->txn;
  ReadMessage *read = txn->add_read_set();
  read->set_key(key);
  ts.serialize(read->mutable_readtime());

  if (is_get) {
    // add to readValues for future BufferGets
    allValTxnState->readValues[key] = value;
  }
  else if (cache_point) {
    // add to point_read_cache for future point queries
    allValTxnState->point_read_cache[key] = value;
  }
}

void ValidationClient::AddQueryReadset(AllValidationTxnState *allValTxnState,
    const std::map<uint64_t, proto::QueryGroupMeta> &queryGroupMeta) {

  proto::Transaction *txn = allValTxnState->txn;

  proto::QueryResultMetaData *queryRep = txn->add_query_set();
  for (const auto &[group, queryMeta] : queryGroupMeta) {
    if(query_params->cacheReadSet) {
      proto::QueryGroupMeta &queryMD = (*queryRep->mutable_group_meta())[group]; 
      queryMD.set_read_set_hash(queryMeta.read_set_hash());
    }
    else {
      if (query_params->mergeActiveAtClient) {
        for(const auto &read : queryMeta.query_read_set().read_set()) {
          ReadMessage* add_read = txn->add_read_set();
          *add_read = std::move(read);
        }
      }
      else {
        proto::QueryGroupMeta &queryMD = (*queryRep->mutable_group_meta())[group]; 
        *queryMD.mutable_query_read_set() = queryMeta.query_read_set();
      }
    }
  }
}

void ValidationClient::AddDep(AllValidationTxnState *allValTxnState, const proto::Dependency &dep) {
  proto::Transaction *txn = allValTxnState->txn;
  *txn->add_deps() = dep;
}

bool ValidationClient::IsTxnParticipant(proto::Transaction *txn, int g) {
  for (const auto &participant : txn->involved_groups()) {
    if (participant == g) {
      return true;
    }
  }
  return false;
}

void ValidationClient::GetThreadValTxnId(uint64_t *txn_client_id, uint64_t *txn_client_seq_num) {
  threadValTxnIdsMap::const_accessor a;
  if (!threadValTxnIds.find(a, std::this_thread::get_id())) {
    Panic("Current thread does not validate transactions");
  }

  *txn_client_id = a->second.first;
  *txn_client_seq_num = a->second.second;
}

std::string ValidationClient::ToTxnId(uint64_t txn_client_id, uint64_t txn_client_seq_num) {
  return std::to_string(txn_client_id) + "_" + std::to_string(txn_client_seq_num);
}

} // namespace sintrstore
