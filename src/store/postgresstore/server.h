/***********************************************************************
 *
 * Copyright 2021 Florian Suri-Payer <fsp@cs.cornell.edu>
 *                Yunhao Zhang <yz2327@cornell.edu>
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/
#ifndef _POSTGRES_SERVER_H_
#define _POSTGRES_SERVER_H_

#include <memory>
#include <map>
#include <unordered_map>
#include <mutex>
#include <shared_mutex>

//#include "store/pg_SMRstore/server-proto.pb.h"
#include "store/server.h"
#include "lib/keymanager.h"
#include "lib/configuration.h"
#include "store/common/backend/versionstore.h"
#include "store/common/partitioner.h"
#include "store/common/truetime.h"
#include "lib/transport.h"
#include <tao/pq.hpp>
#include "store/common/query_result/query_result_proto_builder.h"
#include "tbb/concurrent_hash_map.h"

namespace postgresstore {

typedef std::function<void(std::vector<google::protobuf::Message*>&)> execute_callback;
// typedef std::function<void()> execute_timeout_callback;

class Server : public ::Server {
public:
  Server(Transport* tp);
  ~Server();

  void Load(const std::string &key, const std::string &value,
      const Timestamp timestamp){};

  virtual void CreateTable(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, 
      const std::vector<uint32_t> &primary_key_col_idx) override;
  
  virtual void CreateIndex(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, 
      const std::string &index_name, const std::vector<uint32_t> &index_col_idx) override;

  virtual void LoadTableData(const std::string &table_name, const std::string &table_data_path, 
      const std::vector<std::pair<std::string, std::string>> &column_names_and_types, const std::vector<uint32_t> &primary_key_col_idx) override;

  virtual void LoadTableRows(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, 
      const row_segment_t *row_segment, const std::vector<uint32_t> &primary_key_col_idx, int segment_no = 1, bool load_cc = true) override;

  virtual void LoadTableRow(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, 
      const std::vector<std::string> &values, const std::vector<uint32_t> &primary_key_col_idx) override;


  Stats &GetStats();

  Stats* mutableStats();

private:
  std::shared_ptr<tao::pq::connection_pool> connectionPool;

  Transport * tp;

  Stats stats;

  void exec_statement(const std::string &sql_statement);

  std::string GenerateLoadStatement(const std::string &table_name, const std::vector<std::vector<std::string>> &row_segment, int segment_no);

};

}

#endif
