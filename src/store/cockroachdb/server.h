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
         int groupIdx, int idx, int numShards, int numGroups, std::string &table_registry_path);
  ~Server();
  void Load(const string &key, const string &value, const Timestamp timestamp);

  void CreateTable(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, 
      const std::vector<uint32_t> &primary_key_col_idx) override;

  void CreateIndex(
      const std::string &table_name,
      const std::vector<std::pair<std::string, std::string>> &column_data_types,
      const std::string &index_name,
      const std::vector<uint32_t> &index_col_idx) override;

  void LoadTableData(const std::string &table_name, const std::string &table_data_path, 
      const std::vector<std::pair<std::string, std::string>> &column_names_and_types, const std::vector<uint32_t> &primary_key_col_idx)
      override;

  void LoadTableRows(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, 
      const std::vector<row_t> *values, const std::vector<uint32_t> &primary_key_col_idx, int segment_no = 1, bool load_cc = true) override;

  void LoadTable(
      const std::string &table_name,
      const std::vector<std::pair<std::string, std::string>> &column_data_types,
      const std::vector<std::string> &values,
      const std::vector<uint32_t> primary_key_col_idx,
      const std::string &csv_file_name);
  void exec_sql(std::string sql);

  inline Stats &GetStats() override {
    std::cout << "Yeet the stats" << std::endl;
    return stats;
  }

 private:
  Stats stats;
  transport::Configuration config;
  KeyManager *keyManager;
  std::string domain;
  std::string host;
  std::string zone;
  std::string port;
  std::string site;

  int groupIdx;
  int idx;
  int id;
  int numShards;
  int numGroups;
};

}  // namespace cockroachdb

#endif /* _CRDB_SERVER_H_ */
