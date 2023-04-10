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

class Server : public ::Server {
 public:
  Server(const transport::Configuration &config, KeyManager *keyManager,
         int groupIdx, int idx, int numShards, int numGroups);
  ~Server();
  void Load(const string &key, const string &value, const Timestamp timestamp);

  void CreateTable(
      const std::string &table_name,
      const std::vector<std::pair<std::string, std::string>> &column_data_types,
      const std::vector<uint32_t> primary_key_col_idx) override;

  void LoadTableRow(
      const std::string &table_name,
      const std::vector<std::pair<std::string, std::string>> &column_data_types,
      const std::vector<std::string> &values,
      const std::vector<uint32_t> primary_key_col_idx) override;

  Stats &GetStats();


 private:
  Stats stats;
  transport::Configuration config;
  KeyManager *keyManager;
  transport::ReplicaAddress serverAddress;
  int groupIdx;
  int idx;
  int id;
  int numShards;
  int numGroups;
};

}  // namespace cockroachdb

#endif /* _CRDB_SERVER_H_ */
