// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/cockroachdb/server.cc:
 *
 *  @author Benton Li <cl2597@cornell.edu>
 *
 **********************************************************************/

#include "store/cockroachdb/server.h"

#include <iostream>
#include <string>

namespace cockroachdb {

using namespace std;

Server::Server(const transport::Configuration &config, KeyManager *keyManager,
               int groupIdx, int idx, int numShards, int numGroups)
    : config(config),
      keyManager(keyManager),
      groupIdx(groupIdx),
      idx(idx),
      id(groupIdx * config.n + idx),
      numShards(numShards),
      numGroups(numGroups) {
  int status = 0;
  transport::ReplicaAddress server_address = config.replica(groupIdx, idx);
  std::string host = server_address.host;
  std::string port = server_address.port;

  /**
   * --advertise-addr determines which address to tell other nodes to use.
   * --listen-addr determines which address(es) to listen on for connections
   *   from other nodes and clients.
   */
  std::string start_script = "cockroach start";
  std::string security_flag = " --insecure";
  std::string listen_addr_flag = " --listen-addr=" + host + ":" + port;
  std::string sql_addr_flag = " --sql-addr=" + host + ":" + port;
  std::string advertise_flag = " --advertise-addr=" + host + ":" + port;
  std::string join_flag = " --join=";

  // Naive implementation: join all node
  for (int i = 0; i < numGroups; i++) {
    for (int j = 0; j < config.n; j++) {
      transport::ReplicaAddress join_address = config.replica(i, j);
      join_flag = join_flag + join_address.host + ":" + join_address.port + ",";
    }
  }
  // Remove last comma
  join_flag.pop_back();

  // TODO: better port number
  std::string http_addr_flag =
      " --http-addr=" + host + ":" + std::to_string(8069 + id);
  std::string store_flag =
      " --store=store/cockroachdb/crdb_node" + std::to_string(id);

  // TODO : Add encryption
  // TODO: set zones
  // TODO: Add load balancer
  std::string other_flags = " --background ";

  std::string script_parts[] = {join_flag, security_flag,
                                listen_addr_flag,  // for nodes and clients
                                // advertise_flag,    // for nodes
                                // sql_addr_flag,     // for client's sql
                                http_addr_flag,  // for  DB Console
                                store_flag, other_flags};

  for (std::string part : script_parts) {
    start_script = start_script + part;
  }

  // start server
  Notice("Debug start initalizing");
  status = system((start_script + "&").c_str());
  Notice("Cockroch node %d started. Listening %s:%s", id, host.c_str(),
         port.c_str());
  // If happens to be the last one, init server
  if (id == numGroups * config.n - 1) {
    std::string init_script =
        "cockroach init --insecure --host=" + host + ":" + port;
    status = system(init_script.c_str());

    Notice("Cluster initliazed by node %d. Listening %s:%s", id, host.c_str(),
           port.c_str());
  }
}

void Server::Load(const string &key, const string &value,
                  const Timestamp timestamp) {
  // ValueAndProof val;
  // val.value = value;
  // val.commitProof = dummyProof;
  // commitStore.put(key, val, timestamp);
}

Stats &Server::GetStats() { return stats; }

Server::~Server() {}

}  // namespace cockroachdb