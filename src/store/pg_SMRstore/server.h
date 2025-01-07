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
#ifndef _HOTSTUFF_PG_SERVER_H_
#define _HOTSTUFF_PG_SERVER_H_

#include <memory>
#include <map>
#include <unordered_map>
#include <mutex>
#include <shared_mutex>

#include "store/pg_SMRstore/app.h"
#include "store/pg_SMRstore/server-proto.pb.h"
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

namespace pg_SMRstore {

typedef std::function<void(std::vector<google::protobuf::Message*>&)> execute_callback;
// typedef std::function<void()> execute_timeout_callback;

class Server : public App, public ::Server {
public:
  Server(const transport::Configuration& config, KeyManager *keyManager, int groupIdx, int idx, int numShards,
    int numGroups, bool signMessages, bool validateProofs, uint64_t timeDelta, Partitioner *part, Transport* tp,
    bool localConfig, int SMR_mode, uint64_t num_clients, TrueTime timeServer = TrueTime(0, 0));
  ~Server();

  std::vector<::google::protobuf::Message*> Execute(const std::string& type, const std::string& msg);
    std::vector<::google::protobuf::Message*> Execute_OLD(const string& type, const string& msg);
    
  void Execute_Callback(const std::string& type, const std::string& msg, execute_callback ecb);
    void Execute_Callback_OLD(const std::string& type, const std::string& msg, const execute_callback ecb);

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
  // map from txn id (client_seq_key= client_id | txn_seq_num) to a pair of <postgres connection, postgres txn, terminated>
  typedef tbb::concurrent_hash_map<std::string,std::tuple<std::shared_ptr< tao::pq::connection >, std::shared_ptr< tao::pq::transaction >, bool>> txnStatusMap; 
  txnStatusMap txnMap;

  std::string connection_string;

  std::shared_ptr<tao::pq::connection> test_connection; //PG connection
  std::shared_ptr< tao::pq::transaction> test_transaction;

  //TODO: FIXME: CHANGE THIS TO HANDLE CONCURRENT TXNS -> What if failed.
  struct ClientConnection {
    ClientConnection(): tx_id(0), connection(nullptr), transaction(nullptr), active(false), test_counter(0) {}
    
    void CreateNewConnection(const std::string &connection_str, uint64_t client_id){
      if(active) Panic("Calling Create twice");
      Notice("Opening connection for client: %lu. Connection string: %s", client_id, connection_str.c_str());
      connection = tao::pq::connection::create(connection_str);
      active = true;
    }
    
    std::shared_ptr< tao::pq::transaction> GetTX(uint64_t tx_id_){ // if tx exists / tx_id is same => return current TX. If Tx_id is greater => Start new tx}
      // test_counter++;
      // if(test_counter % 2 == 1){
      //   transaction = connection->transaction();
      // }
      // return transaction;
      if(tx_id_ < tx_id) return nullptr; //outdated req

      if(tx_id_ > tx_id){
        //Start new transaction
        UW_ASSERT(transaction == nullptr);
        Debug("Start new Transaction. id: %lu", tx_id_);
        tx_id = tx_id_;
        transaction = connection->transaction();
      }
      Debug("Continue existing TX");
      return transaction; //returns nullptr if current tx is dead.
    }
    bool ValidTX(uint64_t tx_id_){
      return true; //TESTING
      return tx_id_ >= tx_id;
    }
    void TerminateTX(){ // close current txn
      Debug("Terminate Tnxn %lu", tx_id);
      transaction = nullptr;
    }
    bool active;
    std::shared_ptr<tao::pq::connection> connection; //PG connection
    uint64_t tx_id; //current tx_id;
    std::shared_ptr< tao::pq::transaction> transaction; //Current Transaction

    uint64_t test_counter;
  };
  typedef tbb::concurrent_hash_map<uint64_t, ClientConnection> clientConnectionMap; 
  clientConnectionMap clientConnections;

  Transport* tp;
  Stats stats;
  transport::Configuration config;
  KeyManager* keyManager;
  int groupIdx;
  int idx;
  int id;
  int numShards;
  int numGroups;
  bool signMessages;
  bool validateProofs;
  uint64_t timeDelta;
  Partitioner *part;
  bool localConfig;
  TrueTime timeServer;


  proto::SQL_RPC sql_rpc_template;
  proto::TryCommit try_commit_template;
  proto::UserAbort user_abort_template;

  std::shared_mutex atomicMutex;

     /////////////////// HELPER FUNCTIONS ///////////////////
  ::google::protobuf::Message* ParseMsg(const string& type, const string& msg, std::string &client_seq_key, uint64_t &req_id, uint64_t &client_id, uint64_t &tx_id);
  ::google::protobuf::Message* ProcessReq(uint64_t req_id, uint64_t client_id, uint64_t tx_id, const string& type, ::google::protobuf::Message *req);
  uint64_t getThreadID(const uint64_t &client_id);
  std::shared_ptr< tao::pq::transaction> GetClientTransaction(clientConnectionMap::accessor &c, const uint64_t &client_id, const uint64_t &tx_id);
  std::string createResult(const tao::pq::result &sql_res);

  ::google::protobuf::Message* HandleSQL_RPC(clientConnectionMap::accessor &c, std::shared_ptr<tao::pq::transaction> tx, uint64_t req_id,std::string query);
  ::google::protobuf::Message* HandleTryCommit(clientConnectionMap::accessor &c, std::shared_ptr<tao::pq::transaction> tx, uint64_t req_id);
  ::google::protobuf::Message* HandleUserAbort(clientConnectionMap::accessor &c, std::shared_ptr<tao::pq::transaction> tx);
  
  void exec_statement(const std::string &sql_statement);
  std::string GenerateLoadStatement(const std::string &table_name, const std::vector<std::vector<std::string>> &row_segment, int segment_no);  

  // return true if this key is owned by this shard
  inline bool IsKeyOwned(const std::string &key) const {
    std::vector<int> txnGroups;
    return static_cast<int>((*part)(key, numShards, groupIdx, txnGroups) % numGroups) == groupIdx;
  }

  //Testing:
  std::unordered_set<std::string> executed_tx;

  //////////////////// OLD

  ::google::protobuf::Message* HandleSQL_RPC_OLD(txnStatusMap::accessor &t, std::shared_ptr<tao::pq::transaction> tr, uint64_t req_id,std::string query);
  ::google::protobuf::Message* HandleTryCommit_OLD(txnStatusMap::accessor &t, std::shared_ptr<tao::pq::transaction> tr, uint64_t req_id);
  ::google::protobuf::Message* HandleUserAbort_OLD(txnStatusMap::accessor &t, std::shared_ptr<tao::pq::transaction> tr);
  std::string createClientSeqKey(uint64_t cid, uint64_t tid);
  std::pair<std::shared_ptr<tao::pq::transaction>, bool> getPgTransaction(txnStatusMap::accessor &t, const std::string &key);
  uint64_t getThreadID(const std::string &key);
  void markTxnTerminated(txnStatusMap::accessor &t);
};

// proto::SQL_RPC* GetUnusedSQLRPC();
// void FreeSQLRPC(proto::SQL_RPC *m);
// proto::TryCommit* GetUnusedCommit();
// void FreeTryCommit(proto::TryCommit *m);
// proto::UserAbort GetUnusedUserAbort();
// void FreeUserAbort(proto::UserAbort *m);

}

#endif
