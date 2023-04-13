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

#include "store/common/query_result.h"
#include "store/common/taopq_query_result_wrapper.h"

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

std::string ReplaceAll(std::string str, const std::string &from,
                       const std::string &to) {
  size_t start_pos = 0;
  while ((start_pos = str.find(from, start_pos)) != std::string::npos) {
    str.replace(start_pos, from.length(), to);
    start_pos +=
        to.length();  // Handles case where 'to' is a substring of 'from'
  }
  return str;
}

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
    std::vector<transport::ReplicaAddress> gateways;
    // Last shard group serves as gateways. Can tolerate f failure
    // A gate way serves as a shard master
    for (int i = 0; i < nShards; i++) {
      gateways.push_back(config.replica(nGroups - 1, i));
    }
    // Takes the last server as gateway
    transport::ReplicaAddress gateway0 = gateways.back();

    char host_name[HOST_NAME_MAX];
    int result;
    result = gethostname(host_name, HOST_NAME_MAX);
    if (result) {
      Panic("Unable to get host name for CRDB");
    }
    cout << result << endl;
    // remove site
    std::string site(host_name);
    site.replace(site.find(gateway0.host), gateway0.host.length(), "");
    string addr = gateway0.host + site + ":" + gateway0.port;
    string url = "postgresql://root@" + addr + "/defaultdb?sslmode=disable";

    Notice("Connecting to gateway %s", url.c_str());
    // Establish connection
    conn = tao::pq::connection::create(url);

    // Prepare put function. Use PostgreSQL's upsert feature (i.e. if exists
    // update else insert)
    conn->prepare("put",
                  "INSERT INTO datastore(key_, val_) VALUES(\'$1\', \'$2\') ON "
                  "CONFLICT (key_) "
                  "DO UPDATE SET val_ = \'$2\';");

    // Prepare get function. Use point query
    conn->prepare("get", "SELECT val_ FROM datastore WHERE key_ = \'$1\'");
    // cout << "Initialization confirmed" << endl;

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
                           tao::pq::access_mode::read_write);
    // TODO replace with some Tx ID
    // std::cout << "begin " << '\n';
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
    std::replace_all(key, "\\", "\\\\");
    std::replace_all(value, "\\", "\\\\");
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
    std::cerr << "get " << key << '\n';

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
    std::replace_all(key, "\\", "\\\\");
    std::replace_all(value, "\\", "\\\\");
    std::string put_command("INSERT INTO datastore(key_, val_) VALUES(\'" +
                            key + "\', \'" + value +
                            "\') ON "
                            "CONFLICT (key_) "
                            "DO UPDATE SET val_ = \'" +
                            value + "\';");
    auto const result = [this, &put_command]() {
      // If part of a Tx, use Tx->exec, else use connection->exec
      if (tr != nullptr)
        return tr->execute(put_command);
      else
        return conn->execute(put_command);
    }();
    // pcb(REPLY_OK, key, value);
    std::cerr << "put (" << key << ", " << value << ")" << '\n';
    pcb(REPLY_OK, key, value);
  } catch (const std::exception &e) {
    pcb(REPLY_FAIL, key, value);
    std::cerr << "Tx put failed" << '\n';
    std::cerr << e.what() << '\n';
  }
}

// Execute query.
void Client::Query(const std::string &query, query_callback qcb,
                   query_timeout_callback qtcb, uint32_t timeout) {
  try {
    tao::pq::result result = [this, &query]() {
      // If part of a Tx, use Tx->exec, else use connection->exec
      if (tr != nullptr)
        return tr->execute(query);
      else
        return conn->execute(query);
    }();
    // TODO handle qcb
    taopq_wrapper::TaoPQQueryResultWrapper *tao_res =
        new taopq_wrapper::TaoPQQueryResultWrapper(&result);
    qcb(REPLY_OK, tao_res);
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
    std::cout << "commit " << '\n';

    ccb(COMMITTED);
  } catch (const std::exception &e) {
    std::cerr << e.what() << '\n';
    ccb(ABORTED_SYSTEM);
  }
}

// Abort all Get(s) and Put(s) since Begin().
void Client::Abort(abort_callback acb, abort_timeout_callback atcb,
                   uint32_t timeout) {
  tr->rollback();
  tr = nullptr;
  acb();
}

}  // namespace cockroachdb