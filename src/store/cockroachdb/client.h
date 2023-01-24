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

#include <functional>
#include <string>
#include <tao/pq.hpp>
#include <vector>

#include "lib/assert.h"
#include "lib/message.h"
#include "store/common/frontend/client.h"

typedef std::function<void()> abort_callback;
typedef std::function<void()> abort_timeout_callback;

typedef std::function<void(int, const tao::pq::result &)> query_callback;
typedef std::function<void(int)> query_timeout_callback;

// TODO Stats feature reserved
// class Stats;
using namespace std;

namespace cockroachdb {

class Client : public ::Client {
 public:
  Client();
  Client(string config);
  ~Client();

  // Begin a transaction.
  virtual void Begin(begin_callback bcb, begin_timeout_callback btcb,
                     uint32_t timeout, bool retry = false) override;

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

  // inline const Stats &GetStats() const { return stats; }
  virtual void Query(std::string &query, query_callback qcb,
                     query_timeout_callback qtcb, uint32_t timeout);

  //  protected:
  //   Stats stats;

  std::shared_ptr<tao::pq::connection> conn;
  std::shared_ptr<tao::pq::transaction> tr;
};
}  // namespace cockroachdb

#endif /* _CRDB_CLIENT_H_ */
