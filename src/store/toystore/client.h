// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * Copyright 2024 Gaurav Bhatnagar <gbhatnagar@berkeley.edu>
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
#ifndef _POSTGRESQL_CLIENT_H_
#define _POSTGRESQL_CLIENT_H_

#include "store/common/frontend/client.h"
#include <tao/pq.hpp>
#include "store/common/query_result/taopq_query_result_wrapper.h"
#include <sys/time.h>
#include "store/common/transaction.h"

namespace toystore {

class Client : public ::Client {
 public:
  Client() {};
//   Client(std::string connection_str, std::uint64_t id);
  virtual ~Client();

// // Begin a transaction.
//   virtual void Begin(begin_callback bcb, begin_timeout_callback btcb,
//       uint32_t timeout, bool retry = false) override;

//   // Get the value corresponding to key.
//   virtual void Get(const std::string &key, get_callback gcb,
//       get_timeout_callback gtcb, uint32_t timeout) override;

//   // Set the value for the given key.
//   virtual void Put(const std::string &key, const std::string &value,
//       put_callback pcb, put_timeout_callback ptcb, uint32_t timeout) override;

//   // Commit all Get(s) and Put(s) since Begin().
//   virtual void Commit(commit_callback ccb, commit_timeout_callback ctcb,
//       uint32_t timeout) override;

//   // Abort all Get(s) and Put(s) since Begin().
//   virtual void Abort(abort_callback acb, abort_timeout_callback atcb,
//       uint32_t timeout) override;

  // Get the result for a given query SQL statement
  inline virtual void Query(const std::string &query_statement, query_callback qcb,
      query_timeout_callback qtcb, uint32_t timeout, bool skip_query_interpretation = false) override;)


  // Get the result (rows affected) for a given write SQL statement
//   inline virtual void Write(std::string &write_statement, write_callback wcb,
//       write_timeout_callback wtcb, uint32_t timeout) override;

 private:
//   std::shared_ptr<tao::pq::connection> connection;
//   std::shared_ptr<tao::pq::transaction> transaction;
//   std::uint64_t txn_id;
//   std::uint64_t client_id;
  void* hi;
};

}

#endif // _POSTGRESQL_CLIENT_H_
