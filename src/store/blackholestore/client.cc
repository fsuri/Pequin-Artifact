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
#include "store/blackholestore/client.h"

#include "store/common/query_result/query_result.h"
#include "store/common/query_result/query_result_proto_wrapper.h"
#include "store/common/query_result/query_result_proto_builder.h"

#include "store/common/transaction.h"

namespace blackhole {

using namespace std;

Client::Client(): seq_no(0UL) {
  // just an invariant for now for everything to work ok
  
}

Client::~Client()
{
    
}

/* Begins a transaction. All subsequent operations before a commit() or
 * abort() are part of this transaction.
 */
void Client::Begin(begin_callback bcb, begin_timeout_callback btcb, uint32_t timeout, bool retry) {
 
  //TODO: Add Seq no.
    //Print and callback
    seq_no++;
    Debug("Invoked Txn[%d] Begin", seq_no);
    bcb(seq_no);
}

void Client::Get(const std::string &key, get_callback gcb,
    get_timeout_callback gtcb, uint32_t timeout) {
 
    //Print and callback
    Debug("Invoked Txn[%d] Get. Key: %s", seq_no, key.c_str());
    const std::string val = "reply";
    const Timestamp ts = Timestamp();
    gcb(REPLY_OK, key, val, ts);
}

void Client::Put(const std::string &key, const std::string &value,
    put_callback pcb, put_timeout_callback ptcb, uint32_t timeout) {
  
    //Print and callback
    Debug("Invoked Txn[%d] Put. Key: %s. Value %s", seq_no, key.c_str(), value.c_str());
    pcb(REPLY_OK, key, value);
}

void Client::Commit(commit_callback ccb, commit_timeout_callback ctcb,
    uint32_t timeout) {
 
    //Print and callback.
    Debug("Invoked Txn[%d] Commit", seq_no);
    ccb(COMMITTED);
}

// Abort all Get(s) and Put(s) since Begin().
void Client::Abort(abort_callback acb, abort_timeout_callback atcb,
      uint32_t timeout) {

    Debug("Invoked Txn[%d] Abort", seq_no);
      acb();
}

// Get the result for a given query SQL statement
void Client::Query(const std::string &query_statement, query_callback qcb,
    query_timeout_callback qtcb, uint32_t timeout, bool cache_result, bool skip_query_interpretation){

    Debug("Invoked Txn[%d] Query statement %s", seq_no, query_statement.c_str());
     
    //send back dummy Query result
    query_result::QueryResult *query_result = new sql::QueryResultProtoWrapper();
    query_result->set_rows_affected(1);
    qcb(REPLY_OK, query_result);
      //TODO: Callback with result.
}

//inline virtual void Wait(vector of results) { just do nothing unless overriden} ;; Wait will call getResult, which in turn will trigger the Query callbacks

// Get the result (rows affected) for a given write SQL statement
void Client::Write(std::string &write_statement, write_callback wcb, write_timeout_callback wtcb, uint32_t timeout){

  Debug("Invoked Txn[%d] Write statement %s", seq_no, write_statement.c_str());

  //send back dummy Query result
  query_result::QueryResult *query_result = new sql::QueryResultProtoWrapper();
  query_result->set_rows_affected(1);
  wcb(REPLY_OK, query_result);
  
}

} //namespace blackhole