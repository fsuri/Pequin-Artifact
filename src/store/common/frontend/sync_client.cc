/***********************************************************************
 *
 * Copyright 2021 Florian Suri-Payer <fsp@cs.cornell.edu>
 *                Matthew Burke <matthelb@cs.cornell.edu>
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
#include "store/common/frontend/sync_client.h"
#include "store/common/query_result/query_result_proto_wrapper.h"


SyncClient::SyncClient(Client *client) : client(client) {
}

SyncClient::~SyncClient() {
}

void SyncClient::Begin(uint32_t timeout) {
  //Confirm that all promises have been cleared -- i.e. no ongoing operations.
  UW_ASSERT(getPromises.empty());
  UW_ASSERT(queryPromises.empty());
  UW_ASSERT(asyncPromises.empty());

  Promise promise(timeout);
  client->Begin([promisePtr = &promise](uint64_t id){ promisePtr->Reply(0); },
      [](){}, timeout);
  promise.GetReply();
}

void SyncClient::Get(const std::string &key, std::string &value,
      uint32_t timeout) {
  Promise promise(timeout);
  client->Get(key, std::bind(&SyncClient::GetCallback, this, &promise,
        std::placeholders::_1, std::placeholders::_2, std::placeholders::_3,
        std::placeholders::_4), std::bind(&SyncClient::GetTimeoutCallback, this,
        &promise, std::placeholders::_1, std::placeholders::_2), timeout);
  value = promise.GetValue();
}

void SyncClient::Get(const std::string &key, uint32_t timeout) {
  Promise *promise = new Promise(timeout);
  getPromises.push_back(promise);
  client->Get(key, std::bind(&SyncClient::GetCallback, this, promise,
      std::placeholders::_1, std::placeholders::_2, std::placeholders::_3,
      std::placeholders::_4), std::bind(&SyncClient::GetTimeoutCallback, this,
      promise, std::placeholders::_1, std::placeholders::_2), timeout);
}

void SyncClient::Wait(std::vector<std::string> &values) {
  //values.clear(); //TODO: Add this for safekeeping -- not sure if existing code cared about it.
  for (auto promise : getPromises) {
    values.push_back(promise->GetValue());
    delete promise;
  }
  getPromises.clear();
}

void SyncClient::Put(const std::string &key, const std::string &value,
      uint32_t timeout) {
  Promise promise(timeout);

  client->Put(key, value, std::bind(&SyncClient::PutCallback, this, &promise,
        std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
        std::bind(&SyncClient::PutTimeoutCallback, this,
        &promise, std::placeholders::_1, std::placeholders::_2,
        std::placeholders::_3), timeout);

  promise.GetReply();
}

transaction_status_t SyncClient::Commit(uint32_t timeout) {
  if (getPromises.size() > 0) {
    std::vector<std::string> strs;
    Wait(strs);
  }

  if (queryPromises.size() > 0) {
    std::vector<std::unique_ptr<const query_result::QueryResult>> values;
    Wait(values);
  }

  if (asyncPromises.size() > 0) {
    asyncWait();
  }

  Promise promise(timeout);

  client->Commit(std::bind(&SyncClient::CommitCallback, this, &promise,
        std::placeholders::_1),
        std::bind(&SyncClient::CommitTimeoutCallback, this,
        &promise), timeout);

  return static_cast<transaction_status_t>(promise.GetReply());
}
  
void SyncClient::Abort(uint32_t timeout) {
  if (getPromises.size() > 0) {
    std::vector<std::string> strs;
    Wait(strs);
  }

  if (queryPromises.size() > 0) {
    std::vector<std::unique_ptr<const query_result::QueryResult>> values;
    Wait(values);
  }

  if (asyncPromises.size() > 0) {
    asyncWait();
  }

  Promise promise(timeout);

  client->Abort(std::bind(&SyncClient::AbortCallback, this, &promise),
        std::bind(&SyncClient::AbortTimeoutCallback, this, &promise), timeout);

  promise.GetReply();
}

//Ensure that we wait for any possibly outstanding concurrent requests to return before throwing an exception.
std::unique_ptr<const query_result::QueryResult> SyncClient::SafeRelease(Promise &promise){
  try{
    std::unique_ptr<const query_result::QueryResult> result = promise.ReleaseQueryResult();
    return result;
  }
  catch(...){
    Notice("CATCHING ABORT. WILL PROPAGATE ONCE ALL OUTSTANDING ASYNC REQUESTS ARE DONE");
    std::vector<std::unique_ptr<const query_result::QueryResult>> throw_away_values;
    Wait(throw_away_values);
    asyncWait();
    throw std::exception(); //Propagate Abort exception
  }
}


void SyncClient::SQLRequest(std::string &statement, std::unique_ptr<const query_result::QueryResult> &result, uint32_t timeout) {
  Promise promise(timeout);
  
  client->SQLRequest(statement, std::bind(&SyncClient::SQLCallback, this, &promise,
        std::placeholders::_1, std::placeholders::_2), 
        std::bind(&SyncClient::SQLTimeoutCallback, this,
        &promise, std::placeholders::_1), timeout);

  //result = promise.ReleaseQueryResult(); 
  result = SafeRelease(promise);
}

void SyncClient::SQLRequest(std::string &statement, uint32_t timeout) {
  Promise *promise = new Promise(timeout);
  queryPromises.emplace_back(promise);
  
  client->SQLRequest(statement, std::bind(&SyncClient::SQLCallback, this, promise,
        std::placeholders::_1, std::placeholders::_2), 
        std::bind(&SyncClient::SQLTimeoutCallback, this,
        promise, std::placeholders::_1), timeout);
}


void SyncClient::Write(std::string &statement, std::unique_ptr<const query_result::QueryResult> &result, uint32_t timeout, bool blind_write) {
  Promise promise(timeout);
  
  client->Write(statement, std::bind(&SyncClient::WriteCallback, this, &promise,
        std::placeholders::_1, std::placeholders::_2), 
        std::bind(&SyncClient::WriteTimeoutCallback, this,
        &promise, std::placeholders::_1), timeout, blind_write);
  result.reset();
  //result = promise.ReleaseQueryResult(); 
  result = SafeRelease(promise);
}

void SyncClient::Write(std::string &statement, uint32_t timeout, bool async, bool blind_write) {
   Promise *promise = new Promise(timeout);
  if(async){
    asyncPromises.push_back(promise);
  }
  else {
    queryPromises.push_back(promise);
  }
  
  client->Write(statement, std::bind(&SyncClient::WriteCallback, this, promise,
        std::placeholders::_1, std::placeholders::_2), 
        std::bind(&SyncClient::WriteTimeoutCallback, this,
        promise, std::placeholders::_1), timeout, blind_write);
}

void SyncClient::Query(const std::string &query, std::unique_ptr<const query_result::QueryResult> &result, uint32_t timeout, bool cache_result) {
  Promise promise(timeout);
  // std::cerr<< "Shir: performing query transaction 11\n";
  client->Query(query, std::bind(&SyncClient::QueryCallback, this, &promise,
        std::placeholders::_1, std::placeholders::_2), 
        std::bind(&SyncClient::QueryTimeoutCallback, this,
        &promise, std::placeholders::_1), timeout, cache_result);

  result.reset();

  //result = promise.ReleaseQueryResult(); 
  result = SafeRelease(promise);
}

void SyncClient::Query(const std::string &query, uint32_t timeout, bool cache_result) {
  Promise *promise = new Promise(timeout);
  queryPromises.push_back(promise);
  client->Query(query, std::bind(&SyncClient::QueryCallback, this, promise,
        std::placeholders::_1, std::placeholders::_2), 
        std::bind(&SyncClient::QueryTimeoutCallback, this,
        promise, std::placeholders::_1), timeout, cache_result);
}

//NOTE: For parallel TX: If one of the TX throws an Abort exception, wait until we have processed all replies.
        //This assumes that we WILL get a reply for all queries, even if the first one causes an exception serverside.
//Alternativey implementation option: If we catch one exception, immediately delete everything and propagate exception. But then must edit Callback too!
void SyncClient::Wait(std::vector<std::unique_ptr<const query_result::QueryResult>> &values) {
  values.clear();
  bool aborted = false;
  
  for (auto &promise : queryPromises) {
    try{
      values.push_back(promise->ReleaseQueryResult());
    }
    catch(...){
      Notice("CATCHING ABORT. WILL PROPAGATE AFTER ALL PARALLEL ARE DONE");
      aborted = true;
    }
    delete promise;
  }
  queryPromises.clear();

  if(aborted){
    values.clear();
    asyncWait(); //wait for any possibly outstanding requests to return before throwing exception.
    throw std::exception(); //Propagate Abort exception
  }
  
}

void SyncClient::asyncWait() {
  bool aborted = false;

  for (auto promise : asyncPromises) {
    int status = promise->GetReply();
    if(status > 0) aborted = true;
    delete promise;
  }
  asyncPromises.clear();

  if(aborted) {
    std::vector<std::unique_ptr<const query_result::QueryResult>> throw_away_values;
    Wait(throw_away_values); //wait for any possibly outstanding requests to return before throwing exception.
    throw std::exception(); //Propagate Abort exception
  }
}

///////// Callbacks

void SyncClient::GetCallback(Promise *promise, int status,
    const std::string &key, const std::string &value, Timestamp ts){
  promise->Reply(status, ts, value);
}

void SyncClient::GetTimeoutCallback(Promise *promise, int status, const std::string &key) {
  promise->Reply(status);
}

void SyncClient::PutCallback(Promise *promise, int status, const std::string &key,
      const std::string &value) {
  promise->Reply(status);
}

void SyncClient::PutTimeoutCallback(Promise *promise, int status, const std::string &key,
      const std::string &value) {
  promise->Reply(status);
}

void SyncClient::CommitCallback(Promise *promise, transaction_status_t status) {
  std::cerr<< "Shir: Commit callback\n";
  promise->Reply(status);
}

void SyncClient::CommitTimeoutCallback(Promise *promise) {
  std::cerr<< "Shir: Commit Timeout callback\n";
  promise->Reply(REPLY_TIMEOUT);
}

void SyncClient::AbortCallback(Promise *promise) {
  promise->Reply(ABORTED_USER);
}

void SyncClient::AbortTimeoutCallback(Promise *promise) {
  promise->Reply(REPLY_TIMEOUT);
}


void SyncClient::SQLCallback(Promise *promise, int status, query_result::QueryResult* result){
  promise->Reply(status, std::unique_ptr<const query_result::QueryResult>(result)); 
}

void SyncClient::SQLTimeoutCallback(Promise *promise, int status){
  promise->Reply(status);
}

//Deprecating these calls
void SyncClient::WriteCallback(Promise *promise, int status, query_result::QueryResult* result){
  promise->Reply(status, std::unique_ptr<const query_result::QueryResult>(result)); 
}

void SyncClient::WriteTimeoutCallback(Promise *promise, int status){
  promise->Reply(status);
}

void SyncClient::QueryCallback(Promise *promise, int status, query_result::QueryResult* result){
  promise->Reply(status, std::unique_ptr<query_result::QueryResult>(result));
}

void SyncClient::QueryTimeoutCallback(Promise *promise, int status){
  promise->Reply(status);
}

