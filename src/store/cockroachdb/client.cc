// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/cockroachdb/client.cc:
 *
 *  @author Benton Li <cl2597@cornell.edu>
 *
 **********************************************************************/

#include "store/cockroachdb/client.h"

#include <iostream>
#include <typeinfo>

// Reply types
#define REPLY_OK 0
#define REPLY_FAIL 1
#define REPLY_RETRY 2
#define REPLY_ABSTAIN 3
#define REPLY_TIMEOUT 4
#define REPLY_NETWORK_FAILURE 5
#define REPLY_MAX 6

namespace cockroachdb {
using namespace std;

// TODO replace TrueTime with others
Client::Client(const transport::Configuration &config, uint64_t id, int nShards,
               int nGroups, Transport *transport, uint64_t default_timeout,
               TrueTime timeServer)
    : config(config),
      client_id(id),
      nshards(nShards),
      ngroups(nGroups),
      transport(transport),
      timeServer(timeServer) {
  try {
    // TODO add encryption layer
    // Takes the last server as gateway
    transport::ReplicaAddress gateway =
        config.replica(nGroups - 1, nShards - 1);
    string addr = gateway.host + ":" + gateway.port;
    string url = "postgresql://root@" + addr + "/defaultdb?sslmode=disable";

    // Establish connection
    conn = tao::pq::connection::create(url);

    // Create a table for get and put Key on key_; value on val_
    conn->execute(
        "CREATE TABLE IF NOT EXISTS datastore ( key_ TEXT PRIMARY KEY, val_ "
        "TEXT NOT NULL)");

    // Prepare put function. Use PostgreSQL's upsert feature (i.e. if exists
    // update else insert)
    conn->prepare(
        "put",
        "INSERT INTO datastore(key_, val_) VALUES($1, $2) ON CONFLICT (key_) "
        "DO UPDATE SET val_ = $2;");

    // Prepare get function. Use point query
    conn->prepare("get", "SELECT val_ FROM datastore WHERE key_ = \'$1\'");
    cout << "Initialization confirmed" << endl;

  } catch (const std::exception &e) {
    std::cerr << e.what() << '\n';
  }
}

Client::~Client() {}

// Begin a transaction.
void Client::Begin(begin_callback bcb, begin_timeout_callback btcb,
                   uint32_t timeout, bool retry) {
  try {
    // Create a new Tx
    tr = conn->transaction(tao::pq::isolation_level::serializable,
                           tao::pq::access_mode::default_access_mode);
    // TODO replace with some Tx ID
    bcb(420);
  } catch (const std::exception &e) {
    std::cerr << "Tx begin Failed" << '\n';
    std::cerr << e.what() << '\n';
  }
}

// Get the value corresponding to key.
void Client::Get(const std::string &key, get_callback gcb,
                 get_timeout_callback gtcb, uint32_t timeout) {
  try {
    // TODO: transport->Timer?

    // Hardwire a SQL command
    string point_query =
        "SELECT val_ FROM datastore WHERE key_ = \'" + key + "\'";

    auto const result = [this, &point_query]() {
      // If part of a Tx, use Tx->exec, else use connection->exec
      if (tr != nullptr)
        return tr->execute(point_query);
      else
        return conn->execute(point_query);
    }();

    // read result
    string value;
    for (const auto &row : result) {
      value = row["val_"].as<std::string>();
    }

    // TODO replace Timestamp that makes sense
    gcb(REPLY_OK, key, value, Timestamp(0));
  } catch (const std::exception &e) {
    gcb(REPLY_FAIL, key, "NOTHING BRO", Timestamp(0));
    std::cerr << "Get Failed" << '\n';
    std::cerr << e.what() << '\n';
  }
}

// Set the value for the given key.
void Client::Put(const std::string &key, const std::string &value,
                 put_callback pcb, put_timeout_callback ptcb,
                 uint32_t timeout) {
  try {
    tr->execute("put", key, value);
    // pcb(REPLY_OK, key, value);
    pcb(REPLY_OK, key, value);
  } catch (const std::exception &e) {
    pcb(REPLY_FAIL, key, value);
    std::cerr << "Tx put failed" << '\n';
    std::cerr << e.what() << '\n';
  }
}

// Execute query.
void Client::Query(std::string &query, query_callback qcb,
                   query_timeout_callback qtcb, uint32_t timeout) {
  try {
    auto const result = [this, &query]() {
      // If part of a Tx, use Tx->exec, else use connection->exec
      if (tr != nullptr)
        return tr->execute(query);
      else
        return conn->execute(query);
    }();
    // TODO handle qcb
    // qcb(REPLY_OK, result);
  } catch (const std::exception &e) {
    std::cerr << "Tx query failed" << '\n';
    std::cerr << e.what() << '\n';
  }
}

// Commit all Get(s) and Put(s) since Begin().
void Client::Commit(commit_callback ccb, commit_timeout_callback ctcb,
                    uint32_t timeout) {
  try {
    tr->commit();
    tr = nullptr;
    ccb(transaction_status_t::COMMITTED);
  } catch (const std::exception &e) {
    std::cerr << e.what() << '\n';
  }
}

// Abort all Get(s) and Put(s) since Begin().
void Client::Abort(abort_callback acb, abort_timeout_callback atcb,
                   uint32_t timeout) {
  tr->rollback();
  tr = nullptr;
  acb();
}

// protected:
// Stats stats;

}  // namespace cockroachdb