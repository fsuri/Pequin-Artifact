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

Server::Server(const transport::Configuration& config, KeyManager* keyManager, int groupIdx, int idx, int numShards, int numGroups)
    : config(config),
      keyManager(keyManager),
      groupIdx(groupIdx),
      idx(idx),
      id(groupIdx * config.n + idx),
      numShards(numShards),
      numGroups(numGroups) {
  int status = 0;
  transport::ReplicaAddress server_address = config.replica(groupIdx, idx);

  /**
  * --advertise-addr determines which address to tell other nodes to use.
  * --listen-addr determines which address(es) to listen on for connections 
  *   from other nodes and clients.
  */
  std::string start_script =
      "cockroach start --insecure --listen-addr=" + server_address.host +
      ":" + server_address.port;


  std::string join_flag = " --join=";
  // Naive implementation: join all node
  for (int i = 0; i < numGroups; i++) {
    for (int j = 0; j < config.n; j++) {
      join_flag =
          join_flag + server_address.host + ":" + server_address.port + ",";
    }
  }
  // Remove last comma
  join_flag.pop_back();

  // TODO: better port number
  std::string listen_addr_flag = " --http-addr="+server_address.host+":" + std::to_string(8069 + id);
  std::string store_flag = " --store=node" + std::to_string(id);

  // TODO : Add encryption
  // TODO: set zones
  // TODO: Add load balancer
  std::string other_flags = " --background ";
  start_script = start_script + join_flag + store_flag + listen_addr_flag + other_flags;
  // start server
  status = system(start_script.c_str());

  // If happens to be the last one, init server
  if (id == numGroups * config.n - 1) {
    std::string init_script =
        "cockroach init --insecure --host=" + server_address.host + ":" +
        server_address.port;
    status = system(init_script.c_str());
  }
}

void Server::Load(const string &key, const string &value,
    const Timestamp timestamp) {
  // ValueAndProof val;
  // val.value = value;
  // val.commitProof = dummyProof;
  // commitStore.put(key, val, timestamp);
}

Stats &Server::GetStats() {
  return stats;
}

Server::~Server() {}

}  // namespace cockroachdb