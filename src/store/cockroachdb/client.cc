// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/cockroachdb/client.cc:
 *
 *  @author Benton Li <cl2597@cornell.edu>
 *
 **********************************************************************/

#include "store/cockroachdb/client.h"

#include <algorithm>
#include <iostream>
#include <typeinfo>
#include <memory>

#include "store/common/query_result/query_result.h"
#include "store/common/query_result/taopq_query_result_wrapper.h"

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
               int nGroups, Transport *transport, TrueTime timeServer)
    : config(config),
      client_id(id),
      nshards(nShards),
      ngroups(nGroups),
      transport(transport),
      timeServer(timeServer) {
  
    // TODO add encryption layer
    std::vector<transport::ReplicaAddress> gateways;
    // Last shard group serves as gateways. Can tolerate f failure
    // A gate way serves as a shard master
    for (int i = 0; i < nGroups; i++) {
      gateways.push_back(config.replica(nGroups - 1, i));
    }
    // Takes the last server as gateway
    transport::ReplicaAddress gateway0 = gateways.back();

    char host_name[HOST_NAME_MAX];
    int result;

    try {
      result = gethostname(host_name, HOST_NAME_MAX);
      if (result == -1) {
        Panic("Unable to get host name for CRDB");
      }
    } catch (const std::exception &e) {
      Panic("Failed Hostname Discovery: %s", e.what());
    }
    //cout << result << endl;
   
    // remove site
    std::string site(host_name);
    try {
      site = site.substr(site.find("."));
    } catch (std::out_of_range &e) {
      site = "";
    }
    Notice("Site %s \n", site.c_str());
    string addr = gateway0.host + site + ":" + gateway0.port;
    string url = "postgresql://root@" + addr + "/defaultdb?sslmode=disable";

    Notice("Connecting to gateway %s", url.c_str());
    // Establish connection


    try{
      conn = tao::pq::connection::create(url);
    } catch (const std::exception &e) {
      Panic("Failed TaoConnect: %s", e.what()); 
    }

    int con_tries = 1;
    // bool connected = false;
    // while(!connected){ //Keep connecting
    //   if(con_tries == 20) Panic("stop trying to connect");
    //   try{
    //     conn = tao::pq::connection::create(url);
    //     connected = true;
    //   } catch (const std::exception &e) {
    //     con_tries++;
    //     Notice("Failed TaoConnect");
    //     //Panic("Failed TaoConnect: %s", e.what());
    //   }
    // }
   
    Notice("Connected successfully after %d tries", con_tries);

    if(conn->is_open()){
      Notice("Established TaoPQ connection to backend");
    }
    else{
      Panic("TaoPQ connection to backend could not be established");
    }

    Notice("Client successfully started");
 
}

Client::~Client() {}

// Begin a transaction.
void Client::Begin(begin_callback bcb, begin_timeout_callback btcb,
                   uint32_t timeout, bool retry) {
  Notice("Begin Transaction");
  try {
    if (tr != nullptr) {
      Panic("why is tr not nullptr???");
    }
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
    //ReplaceAll(key, "\\0", "\\\\0");
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
    //std::cerr << key << ": " << value << '\n';

    // TODO replace Timestamp that makes sense
    gcb(REPLY_OK, key, value, Timestamp(0));
  } catch (const std::exception &e) {
    gcb(REPLY_FAIL, key, "NOTHING BRO", Timestamp(0));
    //std::cerr << "Get Failed" << '\n';
    //std::cerr << e.what() << '\n';
  }
}

// Set the value for the given key.
void Client::Put(const std::string &key, const std::string &value,
                 put_callback pcb, put_timeout_callback ptcb,
                 uint32_t timeout) {
  try {
    //ReplaceAll(key, "\\0", "\\\\0");
    //ReplaceAll(value, "\\0", "\\\\0");
    //std::cerr << "val: " << value << endl;
    std::string put_command("INSERT INTO datastore(key_, val_) VALUES(\'" +
                            key + "\', \'" + value +
                            "\') ON "
                            "CONFLICT (key_) "
                            "DO UPDATE SET val_ = \'" +
                            value + "\';");
    //std::cerr << put_command << endl;
    auto const result = [this, &put_command]() {
      // If part of a Tx, use Tx->exec, else use connection->exec
      if (tr != nullptr)
        return tr->execute(put_command);
      else
        return conn->execute(put_command);
    }();
    pcb(REPLY_OK, key, value);
  } catch (const std::exception &e) {
    pcb(REPLY_FAIL, key, value);
    //std::cerr << "Tx put failed" << '\n';
    //std::cerr << e.what() << '\n';
  }
}

// Execute query.
void Client::Query(const std::string &query_statement, query_callback qcb,
      query_timeout_callback qtcb, uint32_t timeout, bool cache_result, bool skip_query_interpretation) {
  std::cerr << "In Query" << std::endl;
  try {
    if (tr == nullptr) {
      qcb(REPLY_FAIL, nullptr);
      return;
    }
    tao::pq::result result = [this, &query_statement]() {
      return tr->execute(query_statement);
    }();
    stats.Increment("queries_issued", 1);
    taopq_wrapper::TaoPQQueryResultWrapper *tao_res =
        new taopq_wrapper::TaoPQQueryResultWrapper(std::make_unique<tao::pq::result>(std::move(result)));
    qcb(REPLY_OK, tao_res);
  } catch (const std::exception &e) {
    std::cerr << "Tx query failed" << '\n';
    std::cerr << e.what() << '\n';
    tr->rollback();
    tr = nullptr;
    qcb(REPLY_FAIL, nullptr);
  }
}

void Client::Write(std::string &write_statement, write_callback wcb, 
  write_timeout_callback wtcb, uint32_t timeout, bool blind_write) {
  try {
    if (tr == nullptr) {
      wcb(REPLY_FAIL, nullptr);
      return;
    }

    tao::pq::result result = tr->execute(write_statement);
    stats.Increment("writes_issued", 1);
    taopq_wrapper::TaoPQQueryResultWrapper *tao_res =
        new taopq_wrapper::TaoPQQueryResultWrapper(std::make_unique<tao::pq::result>(std::move(result)));
    wcb(REPLY_OK, tao_res);
  } catch (const tao::pq::integrity_constraint_violation &e) {
    std::cerr << "Tx write integrity constraint violation" << '\n';
    std::cerr << e.what() << '\n';
    auto result = new taopq_wrapper::TaoPQQueryResultWrapper();
    wcb(REPLY_OK, result);
  } catch (const tao::pq::transaction_rollback &e) {
    std::cerr << "Tx write transaction rollback" << std::endl;
    std::cerr << e.what() << std::endl;
    tr->rollback();
    tr = nullptr;
    wcb(REPLY_FAIL, nullptr);
  } catch (const tao::pq::in_failed_sql_transaction &e) {
    std::cerr << "Tx write failed" << std::endl;
    std::cerr << e.what() << std::endl;
    tr = nullptr;
    wcb(REPLY_FAIL, nullptr);
  } catch (const std::exception &e) {
    std::cerr << "Tx write failed" << '\n';
    std::cerr << e.what() << '\n';
    Panic("Tx write failed");
  }
}

// Commit all Get(s) and Put(s) since Begin().
void Client::Commit(commit_callback ccb, commit_timeout_callback ctcb,
                    uint32_t timeout) {
  try {
    tr->commit();
    tr = nullptr;
    //std::cout << "commit " << '\n';
    stats.Increment("num_commit", 1);
    ccb(COMMITTED);
  } catch (const std::exception &e) {
    std::cerr << "Tx commit failed" << std::endl;
    std::string error_message = e.what();
    std::cerr << error_message << std::endl;
    if (error_message.find("restart transaction") != std::string::npos) {
      tr = nullptr;
    }
    stats.Increment("num_aborts", 1);
    ccb(ABORTED_SYSTEM);
  }
}

// Abort all Get(s) and Put(s) since Begin().
void Client::Abort(abort_callback acb, abort_timeout_callback atcb,
                   uint32_t timeout) {
  try {
    if (tr != nullptr) {
      tr->rollback();
      tr = nullptr;
    }
    acb();
  } catch (const std::exception &e) {
    std::cerr << "Tx abort failed" << std::endl;
    std::cerr << e.what() << std::endl;
    Panic("Tx abort failed");
  }
}

}  // namespace cockroachdb