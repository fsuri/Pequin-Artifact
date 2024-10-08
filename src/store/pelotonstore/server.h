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
#ifndef _PELOTON_SERVER_H_
#define _PELOTON_SERVER_H_

#include <memory>
#include <map>
#include <unordered_map>
#include <mutex>
#include <shared_mutex>

#include "store/pelotonstore/app.h"
#include "store/pelotonstore/server-proto.pb.h"
#include "store/server.h"
#include "table_store.h"
#include "lib/keymanager.h"
#include "lib/configuration.h"
#include "store/common/backend/versionstore.h"
#include "store/common/partitioner.h"
#include "store/common/truetime.h"
#include "lib/transport.h"
#include "store/common/query_result/query_result_proto_builder.h"
#include "tbb/concurrent_hash_map.h"

#include <nlohmann/json.hpp>
using json = nlohmann::json;

namespace pelotonstore {

typedef std::function<void(std::vector<google::protobuf::Message*>&)> execute_callback;
// typedef std::function<void()> execute_timeout_callback;

typedef struct ColRegistry {
    std::vector<bool> col_quotes; //col_quotes in order
} ColRegistry;
typedef std::map<std::string, ColRegistry> TableRegistry_t;

class Server : public App, public ::Server {
public:
  Server(const transport::Configuration& config, KeyManager *keyManager, std::string &table_registry_path, 
    int groupIdx, int idx, int numShards, int numGroups, bool signMessages, bool validateProofs, uint64_t timeDelta, Partitioner *part, Transport* tp,
    bool localConfig, TrueTime timeServer = TrueTime(0, 0));
  ~Server();

  void RegisterTables(std::string &table_registry);

  std::vector<::google::protobuf::Message*> Execute(const std::string& type, const std::string& msg);
    std::vector<::google::protobuf::Message*> Execute_OLD(const string& type, const string& msg);
    
  void Execute_Callback(const std::string& type, const std::string& msg, const execute_callback ecb);
    void Execute_Callback_OLD(const std::string& type, const std::string& msg, const execute_callback ecb);

  void Load(const std::string &key, const std::string &value,
      const Timestamp timestamp){};

  virtual void CreateTable(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, 
      const std::vector<uint32_t> &primary_key_col_idx) override;
  
  virtual void CreateIndex(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, 
      const std::string &index_name, const std::vector<uint32_t> &index_col_idx) override;

  virtual void LoadTableData(const std::string &table_name, const std::string &table_data_path, 
      const std::vector<std::pair<std::string, std::string>> &column_names_and_types, const std::vector<uint32_t> &primary_key_col_idx) override;

    //Helper function:
    std::vector<row_segment_t*> ParseTableDataFromCSV(const std::string &table_name, const std::string &table_data_path, 
    const std::vector<std::pair<std::string, std::string>> &column_names_and_types, const std::vector<uint32_t> &primary_key_col_idx);

  virtual void LoadTableRows(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, 
      const row_segment_t *row_segment, const std::vector<uint32_t> &primary_key_col_idx, int segment_no = 1, bool load_cc = true) override;

  //DEPRECATED
  virtual void LoadTableRow(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, 
      const std::vector<std::string> &values, const std::vector<uint32_t> &primary_key_col_idx) override;


  inline Stats &GetStats() {return stats;};

  inline Stats* mutableStats() {return &stats;};

private:  
  struct ClientState {
     ClientState(): tx_id(0), active(false) {}
    
    bool ValidTX(uint64_t tx_id_){
      //if(tx_id_ > tx_id && active) Panic("Processing new tx_id: %d before curr tx_id: %d finished", tx_id_, tx_id); //TODO: REmove this line when testing with 4 replicas/fakeSMR
      return tx_id_ >= tx_id;
    }
    
    bool GetTxStatus(uint64_t tx_id_, bool &begin, bool &terminate_last){ // if tx exists / tx_id is same => return current TX. If Tx_id is greater => Start new tx}
      
      if(tx_id_ < tx_id) Panic("Receiving outdated request");

      if(tx_id_ > tx_id){
        //Start new transaction
        if(active){
            Warning("Previous Txn has not yet terminated"); //This should never trigger when running against only a single replica.
            //Panic("Processing new tx_id: %d before curr tx_id: %d finished", tx_id_, tx_id);
            terminate_last = true; //Terminate current tx so we can start a new one.
        }
      
        Debug("Start new Transaction. id: %lu", tx_id_);
        tx_id = tx_id_;
        active = true;
        begin = true; 
      }
      else{
        Debug("Continue existing Transaction. id: %lu", tx_id);
      }
      return active; //if current txn not active anymore (i.e. was aborted) => can return immediately (don't need to parse etc)
    }
   
    void TerminateTX(){ // close current txn
    Debug("Mark Tx[%d] as inactive", tx_id);
      active = false;
    }
    bool active; //curr txn still active
    uint64_t tx_id; //current tx_id;
  };

  typedef tbb::concurrent_hash_map<uint64_t, ClientState> ClientStateMap; 
  ClientStateMap clientState;

  TableRegistry_t TableRegistry;

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
  pelotonstore::TableStore *table_store;

     /////////////////// HELPER FUNCTIONS ///////////////////
  ::google::protobuf::Message* ParseMsg(const string& type, const string& msg, uint64_t &req_id, uint64_t &client_id, uint64_t &tx_id);
  ::google::protobuf::Message* ProcessReq(uint64_t req_id, uint64_t client_id, uint64_t tx_id, const string& type, ::google::protobuf::Message *req);
  uint64_t getThreadID(const uint64_t &client_id);
  
  /*::google::protobuf::Message* HandleSQL_RPC(clientConnectionMap::accessor &c, std::shared_ptr<tao::pq::transaction> tx, uint64_t req_id,std::string query);
  ::google::protobuf::Message* HandleTryCommit(clientConnectionMap::accessor &c, std::shared_ptr<tao::pq::transaction> tx, uint64_t req_id);
  ::google::protobuf::Message* HandleUserAbort(clientConnectionMap::accessor &c, std::shared_ptr<tao::pq::transaction> tx);*/

  ::google::protobuf::Message* HandleSQL_RPC(ClientStateMap::accessor &c, uint64_t req_id, uint64_t client_id, uint64_t tx_id, const std::string &query);
  ::google::protobuf::Message* HandleTryCommit(ClientStateMap::accessor &c, uint64_t req_id, uint64_t client_id, uint64_t tx_id);
  ::google::protobuf::Message* HandleUserAbort(ClientStateMap::accessor &c, uint64_t client_id, uint64_t tx_id);
  
  std::string GenerateLoadStatement(const std::string &table_name, const std::vector<std::vector<std::string>> &row_segment, int segment_no);  

  // return true if this key is owned by this shard
  inline bool IsKeyOwned(const std::string &key) const {
    std::vector<int> txnGroups;
    return static_cast<int>((*part)(key, numShards, groupIdx, txnGroups) % numGroups) == groupIdx;
  }

};

}

#endif
