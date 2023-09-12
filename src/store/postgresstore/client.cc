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

Client::Client(std::string connection_str, std::uint64_t id) : client_id(id) {
  Debug("Initializing PostgreSQL client with id [%lu]", client_id);
  connection = tao::pq::connection::create(connection_str);
  txn_id = 0;
  Debug("PostgreSQL client [%lu] created!", client_id);
}

Client::~Client()
{
}

/* Begins a transaction. All subsequent operations before a commit() or
 * abort() are part of this transaction.
 */
void Client::Begin(begin_callback bcb, begin_timeout_callback btcb,
    uint32_t timeout, bool retry) {
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
  try { 
    transaction->commit();
    ccb(transaction_status_t::COMMITTED);
  } catch (...) {
    ccb(transaction_status_t::ABORTED_SYSTEM);
  }
}

void Client::Abort(abort_callback acb, abort_timeout_callback atcb,
    uint32_t timeout) {
  try {
    transaction->rollback();
    acb();
  } catch (...) {
    atcb();
  }
}

// Get the value corresponding to key.
inline void Client::Query(const std::string &query_statement, query_callback qcb,
    query_timeout_callback qtcb, uint32_t timeout, bool skip_query_interpretation) {
  try {
    auto result = transaction->execute(query_statement);
    auto wrapped_result = new taopq_wrapper::TaoPQQueryResultWrapper(std::make_unique<tao::pq::result>(result));
    qcb(0, wrapped_result);
  } catch (...) {
    qtcb(0);
  }
}
} // namespace postgresqlstore
