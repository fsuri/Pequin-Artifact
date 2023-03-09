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

Server::Server(const transport::Configuration& config, KeyManager* keyManager,
               int groupIdx, int idx, int numShards, int numGroups)
    : config(config),
      keyManager(keyManager),
      groupIdx(groupIdx),
      idx(idx),
      id(groupIdx * config.n + idx),
      numShards(numShards),
      numGroups(numGroups) {
  transport::ReplicaAddress server_address = config.replica(groupIdx, idx);

  // TODO : Add encryption
  std::string start_script =
      "cockroach start --insecure --advertise-addr=" + server_address.host +
      ":" + server_address.port;

  // TODO: set zones
  // TODO: Add load balancer
  std::string join_flag = "--join=";
  // Naive implementation: join all node
  for (int i = 0; i < numGroups; i++) {
    for (int j = 0; j < config.n; j++) {
      join_flag =
          join_flag + server_address.host + ":" + server_address.port + ",";
    }
  }
  // Remove last comma
  join_flag.pop_back();
  std::string other_flags = "--cache=.25 --max -sql -memory=.25 --background ";

  start_script = start_script + join_flag + other_flags;

  // If happens to be the last one, start
  if (id == numGroups * config.n - 1) {
    std::string init_script =
        "cockroach init --insecure --host=" + server_address.host + ":" +
        server_address.port;
  }
}

Server::~Server() {}

}  // namespace cockroachdb