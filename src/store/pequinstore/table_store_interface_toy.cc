#include "store/pequinstore/table_store_interface_toy.h"

// TODO: Ideally move all Toy Testing code currently in querysync-server.cc to
// here.
// TODO: To support that the ToyTableStore will need access to Server Data
// structures.

namespace pequinstore {

ToyTableStore::ToyTableStore() {
  std::cerr << "TOY INTERFACE IS A BLACK HOLE" << std::endl;

  // init Peloton
}

ToyTableStore::~ToyTableStore() {}

// Execute a statement directly on the Table backend, no questions asked, no
// output
void ToyTableStore::ExecRaw(const std::string &sql_statement, bool skip_cache) {
  std::cerr << "EXECUTING RAW ON TOY INTERFACE" << std::endl;
}

void ToyTableStore::LoadTable(const std::string &load_statement,
                              const std::string &txn_digest,
                              const Timestamp &ts,
                              const proto::CommittedProof *committedProof) {
  std::cerr << "LOADING TABLE ON TOY INTERFACE" << std::endl;
}

// Execute a read query statement on the Table backend and return a
// query_result/proto (in serialized form) as well as a read set (managed by
// readSetMgr)
std::string ToyTableStore::ExecReadQuery(const std::string &query_statement,
                                         const Timestamp &ts,
                                         QueryReadSetMgr &readSetMgr) {

  std::cerr << "EXECUTING READ QUERY ON TOY INTERFACE" << std::endl;
  // TODO: Toy Table Store should be constructed with pointers to Server data
  // structures.

  // if(TEST_READ_SET){

  //     for(auto const&[tx_id, proof] : committed){
  //         if(tx_id == "toy_txn") continue;
  //         const proto::Transaction *txn = &proof->txn();
  //         for(auto &write: txn->write_set()){
  //             queryReadSetMgr.AddToReadSet(write.key(), txn->timestamp());
  //             Debug("Added read key %s and ts to read set",
  //             write.key().c_str());
  //         }
  //         //queryReadSetMgr.AddToDepSet(tx_id, query_md->useOptimisticTxId,
  //         txn->timestamp());
  //     }

  //     //Creating Dummy keys for testing
  //     for(int i=5;i > 0; --i){
  //         TimestampMessage ts;
  //         ts.set_id(query_md->ts.getID());
  //         ts.set_timestamp(query_md->ts.getTimestamp());
  //         std::string dummy_key = groupIdx == 0 ? "dummy_key_g1_" +
  //         std::to_string(i) : "dummy_key_g2_" + std::to_string(i);

  //         queryReadSetMgr.AddToReadSet(dummy_key, ts);
  //         //query_md->read_set[dummy_key] = ts; //query_md->ts;
  //         //replaced with repeated field -> directly in result object.

  //         // ReadMessage *read =
  //         query_md->queryResultReply->mutable_result()->mutable_query_read_set()->add_read_set();
  //         // //ReadMessage *read =
  //         query_md->queryResult->mutable_query_read_set()->add_read_set();
  //         // read->set_key(dummy_key);
  //         // *read->mutable_readtime() = ts;
  //     }
  //     //Creating Dummy deps for testing

  //         //Write to Query Result; Release/Re-allocate temporarily if not
  //         sending;
  //         //For caching:
  //             // Cache the deps --> During CC: look through the data
  //             structure.
  //         //For non-caching:
  //             // Add the deps to SyncReply --> Let client choose whether to
  //             include them (only if proposed them in merge; marked as prep)
  //             --> During CC: Look through the included deps.

  //     //During execution only read prepared if depth allowed.
  //     //  i.e. if (params.maxDepDepth == -1 || DependencyDepth(txn) <=
  //     params.maxDepDepth)  (maxdepth = -1 means no limit) if
  //     (params.query_params.readPrepared && params.maxDepDepth > -2) {

  //         //JUST FOR TESTING.
  //         for(preparedMap::const_iterator i=prepared.begin();
  //         i!=prepared.end(); ++i ) {
  //             const std::string &tx_id = i->first;
  //             const proto::Transaction *txn = i->second.second;

  //             queryReadSetMgr.AddToDepSet(tx_id, txn->timestamp());

  //             // proto::Dependency *add_dep =
  //             query_md->queryResultReply->mutable_result()->mutable_query_read_set()->add_deps();
  //             // add_dep->set_involved_group(groupIdx);
  //             // add_dep->mutable_write()->set_prepared_txn_digest(tx_id);
  //             // Debug("Adding Dep: %s",
  //             BytesToHex(add_dep->write().prepared_txn_digest(),
  //             16).c_str());
  //             // //Note: Send merged TS.
  //             // if(query_md->useOptimisticTxId){
  //             //     //MergeTimestampId(txn->timestamp().timestamp(),
  //             txn->timestamp().id()
  //             //
  //             add_dep->mutable_write()->mutable_prepared_timestamp()->set_timestamp(txn->timestamp().timestamp());
  //             //
  //             add_dep->mutable_write()->mutable_prepared_timestamp()->set_id(txn->timestamp().id());
  //             // }
  //         }
  //     }
  // }

  // std::string test_result_string = "success" +
  // std::to_string(query_md->query_seq_num); std::vector<std::string>
  // result_row; result_row.push_back(test_result_string);
  // sql::QueryResultProtoBuilder queryResultBuilder;
  // queryResultBuilder.add_column("result");
  // queryResultBuilder.add_row(result_row.begin(), result_row.end());

  // std::string dummy_result =
  // queryResultBuilder.get_result()->SerializeAsString();

  // return dummy_result;

  return "";
}

// Execute a point read on the Table backend and return a query_result/proto (in
// serialized form) as well as a commitProof (note, the read set is implicit)
void ToyTableStore::ExecPointRead(
    const std::string &query_statement, std::string &enc_primary_key,
    const Timestamp &ts, proto::Write *write,
    const proto::CommittedProof *&committedProof) {

  std::cerr << "EXECUTING POINT READ ON TOY INTERFACE" << std::endl;
  // Toy Transaction
  //  std::string toy_txn("toy_txn");

  // //Create Prepared Value
  // Timestamp toy_ts(0, 2); //set to genesis time.

  // sql::QueryResultProtoBuilder queryResultBuilder;
  // queryResultBuilder.add_columns({"key_", "val_"});
  // std::vector<std::string> result_row = {"alice", "blonde"};
  // queryResultBuilder.add_row(result_row.begin(), result_row.end());
  // std::string toy_result =
  // queryResultBuilder.get_result()->SerializeAsString();

  // //if(id ==0){  //TODO: ID is not an argument currently.     // Control how
  // many replicas sent prepared values -> if > f+1 -> result should be
  // prepared.
  //         write->set_prepared_value(toy_result);
  //         std::cerr << "SENT PREPARED RESULT: " << write->prepared_value() <<
  //         std::endl; write->set_prepared_txn_digest(toy_txn);
  //         toy_ts.serialize(write->mutable_prepared_timestamp());
  // //}

  // //Create Committed Value + Proof
  // sql::QueryResultProtoBuilder queryResultBuilder2;
  // queryResultBuilder2.add_columns({"key_", "val_"});
  // result_row = {"alice", "black"};
  // queryResultBuilder2.add_row(result_row.begin(), result_row.end());
  // std::string toy_result2 =
  // queryResultBuilder2.get_result()->SerializeAsString();

  // Timestamp toy_ts_c(0, 1);

  // proto::CommittedProof *real_proof = new proto::CommittedProof();
  // proto::Transaction *txn = real_proof->mutable_txn();
  // real_proof->mutable_txn()->set_client_id(0);
  // real_proof->mutable_txn()->set_client_seq_num(1);
  // toy_ts_c.serialize(real_proof->mutable_txn()->mutable_timestamp());
  // TableWrite &table_write =
  // (*real_proof->mutable_txn()->mutable_table_writes())["datastore"];
  // RowUpdates *row = table_write.add_rows();
  // row->add_column_values("alice");
  // row->add_column_values("black");
  // WriteMessage *write_msg = real_proof->mutable_txn()->add_write_set();
  // write_msg->set_key("datastore#alice");
  // write_msg->mutable_rowupdates()->set_row_idx(0);

  // //Add relal qC:
  // // proto::Signatures &sigs = (*real_proof->mutable_p1_sigs())[id];
  // // sigs.add_sigs();
  // // SignMessage()

  // //committed[toy_txn]  = real_proof;  //TODO: Need to move this elsewhere so
  // all servers have it.

  // write->set_committed_value(toy_result2);

  // toy_ts_c.serialize(write->mutable_committed_timestamp());
  // committedProof = real_proof;

  return;
}
// Note: Could execute PointRead via ExecReadQuery (Eagerly) as well.
//  ExecPointRead should translate enc_primary_key into a query_statement to be
//  exec by ExecReadQuery.
//(Alternatively: Could already send a Sql command from the client) ==> Should
// do it at the client, so that we can keep whatever Select specification, e.g.
// * or specific cols...

// Apply a set of Table Writes (versioned row creations) to the Table backend
bool ToyTableStore::ApplyTableWrite(const std::string &table_name,
                                    const TableWrite &table_write,
                                    const Timestamp &ts,
                                    const std::string &txn_digest,
                                    const proto::CommittedProof *commit_proof,
                                    bool commit_or_prepare,
                                    bool forcedMaterialize) {
  std::cerr << "APPLY TABLE WRITE ON TOY INTERFACE" << std::endl;
  return true;
}

void ToyTableStore::PurgeTableWrite(const std::string &table_name,
                                    const TableWrite &table_write,
                                    const Timestamp &ts,
                                    const std::string &txn_digest) {

  std::cerr << "PURGE TABLE WRITE ON TOY INTERFACE" << std::endl;
}

///////////////////// Snapshot Protocol Support

// Partially execute a read query statement (reconnaissance execution) and
// return the snapshot state (managed by ssMgr)
void ToyTableStore::FindSnapshot(const std::string &query_statement,
                                 const Timestamp &ts, SnapshotManager &ssMgr, size_t snapshot_prepared_k) {

  std::cerr << "FIND SNAPSHOT ON TOY INTERFACE" << std::endl;
}

std::string ToyTableStore::EagerExecAndSnapshot(const std::string &query_statement, const Timestamp &ts, SnapshotManager &ssMgr, QueryReadSetMgr &readSetMgr, size_t snapshot_prepared_k){
  return "";
}

std::string ToyTableStore::ExecReadQueryOnMaterializedSnapshot(const std::string &query_statement, const Timestamp &ts, QueryReadSetMgr &readSetMgr,
           const ::google::protobuf::Map<std::string, proto::ReplicaList> &ss_txns)
{
  return "";
}

//DEPRECATED: 

// // Materialize a snapshot on the Table backend and execute on said snapshot.
// void ToyTableStore::MaterializeSnapshot(
//     const std::string &query_retry_id, const proto::MergedSnapshot &merged_ss,
//     const std::set<proto::Transaction *> &ss_txns) {}

// std::string ToyTableStore::ExecReadOnSnapshot(const std::string &query_retry_id,
//                                               std::string &query_statement,
//                                               const Timestamp &ts,
//                                               QueryReadSetMgr &readSetMgr,
//                                               bool abort_early) {

//   sql::QueryResultProtoBuilder queryResultBuilder;
//   // queryResultBuilder.add_column("result");
//   // queryResultBuilder.add_row(result_row.begin(), result_row.end());

//   return queryResultBuilder.get_result()->SerializeAsString();
// }

} // namespace pequinstore
