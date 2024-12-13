// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/cockroachdb/client.h:
 *    cockroachdb client interface.
 *
 * @author Benton Li <cl2597@cornell.edu>
 *
 **********************************************************************/

#ifndef _CRDB_CLIENT_H_
#define _CRDB_CLIENT_H_

#include <google/protobuf/message.h>
#include <sys/time.h>
#include <unistd.h>

#include <functional>
#include <string>
#include <tao/pq.hpp>
#include <vector>

#include "lib/assert.h"
#include "lib/configuration.h"
#include "lib/message.h"
#include "lib/udptransport.h"
#include "store/common/frontend/client.h"
#include "store/common/timestamp.h"
#include "store/common/truetime.h"

typedef std::function<void()> abort_callback;
typedef std::function<void()> abort_timeout_callback;

// typedef std::function<void(int, const tao::pq::result &)> query_callback;
// typedef std::function<void(int)> query_timeout_callback;

using namespace std;

namespace cockroachdb {

class Client : public ::Client {
 public:
  Client(const transport::Configuration &config, uint64_t id, int nShards,
         int nGroups, Transport *transport, TrueTime timeserver = TrueTime(0, 0));
  ~Client();

  // Begin a transaction.
  virtual void Begin(begin_callback bcb, begin_timeout_callback btcb,
                     uint32_t timeout, bool retry = false, const std::string &txnState = std::string()) override;

  // Get the value corresponding to key.
  virtual void Get(const std::string &key, get_callback gcb,
                   get_timeout_callback gtcb, uint32_t timeout) override;

  // Set the value for the given key.
  virtual void Put(const std::string &key, const std::string &value,
                   put_callback pcb, put_timeout_callback ptcb,
                   uint32_t timeout) override;

  // Commit all Get(s) and Put(s) since Begin().
  virtual void Commit(commit_callback ccb, commit_timeout_callback ctcb,
                      uint32_t timeout) override;

  // Abort all Get(s) and Put(s) since Begin().
  virtual void Abort(abort_callback acb, abort_timeout_callback atcb,
                     uint32_t timeout) override;

//   virtual void SQLRequest(std::string &statement, sql_callback scb,
//     sql_timeout_callback stcb, uint32_t timeout) override;

  virtual void Query(const std::string &query_statement, query_callback qcb,
      query_timeout_callback qtcb, uint32_t timeout, bool cache_result = false, bool skip_query_interpretation = false) override;

  virtual void Write(std::string &write_statement, write_callback wcb,
      write_timeout_callback wtcb, uint32_t timeout, bool blind_write=false) override;

 protected:
  Stats stats;

  std::shared_ptr<tao::pq::connection> conn;
  std::shared_ptr<tao::pq::transaction> tr;

 private:
  Client *client;
  uint64_t id;
  uint32_t timeout;
  /* Configuration State */
  transport::Configuration config;
  // Unique ID for this client.
  uint64_t client_id;
  // Number of shards.
  uint64_t nshards;
  // Number of replica groups.
  uint64_t ngroups;
  // Transport used by shard clients.
  Transport *transport;
  // TrueTime server.
  TrueTime timeServer;
};
}  // namespace cockroachdb

#endif /* _CRDB_CLIENT_H_ */
