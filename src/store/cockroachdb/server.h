// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/cockroachdb/server.cc:
 *
 *  @author Benton Li <cl2597@cornell.edu>
 *
 **********************************************************************/

#ifndef _CRDB_SERVER_H_
#define _CRDB_SERVER_H_

#include "lib/configuration.h"
#include "lib/keymanager.h"
#include "store/server.h"

namespace cockroachdb {

class Server ::Server {
 public:
  Server(const transport::Configuration& config, KeyManager* keyManager,
         int groupIdx, int idx, int numShards, int numGroups);
  ~Server();

 private:
  //   Stats stats;
  transport::Configuration config;
  KeyManager* keyManager;
  int groupIdx;
  int idx;
  int id;
  int numShards;
  int numGroups;
};

}  // namespace cockroachdb

#endif /* _CRDB_SERVER_H_ */
