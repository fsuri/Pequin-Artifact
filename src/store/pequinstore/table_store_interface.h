#ifndef _PEQUIN_TABLESTORE_H_
#define _PEQUIN_TABLESTORE_H_

#include "store/common/query_result/query_result.h"
#include "store/common/query_result/query_result_proto_builder.h"
#include "store/common/query_result/query_result_proto_wrapper.h"
#include "store/pequinstore/common.h" //for ReadSetMgr

#include "store/pequinstore/sql_interpreter.h"

// TODO: Include whatever Peloton Deps
#include "../../query-engine/traffic_cop/traffic_cop.h"

namespace pequinstore {

typedef std::function<void(const std::string &, const Timestamp &, bool,
                           QueryReadSetMgr *, SnapshotManager *)>
    find_table_version;
typedef std::function<bool(const std::string &)>
    read_prepared_pred; // This is a function that, given a txnDigest of a
                        // prepared tx, evals to true if it is readable, and
                        // false if not.

class TableStore {
public:
  TableStore();
  virtual ~TableStore();

  void SetFindTableVersion(find_table_version _set_table_version);
  void SetPreparePredicate(read_prepared_pred read_prepared_pred);

  void RegisterTableSchema(std::string &table_registry_path);
  std::vector<bool> *GetRegistryColQuotes(const std::string &table_name);
  std::vector<bool> *GetRegistryPColQuotes(const std::string &table_name);

  // Execute a statement directly on the Table backend, no questions asked, no
  // output
  void ExecRaw(const std::string &sql_statement);

  void LoadTable(const std::string &load_statement,
                 const std::string &txn_digest, const Timestamp &ts,
                 const proto::CommittedProof *committedProof);

  // Execute a read query statement on the Table backend and return a
  // query_result/proto (in serialized form) as well as a read set (managed by
  // readSetMgr)
  std::string ExecReadQuery(const std::string &query_statement, Timestamp &ts,
                            QueryReadSetMgr &readSetMgr);

  // Execute a point read on the Table backend and return a query_result/proto
  // (in serialized form) as well as a commitProof (note, the read set is
  // implicit)
  void ExecPointRead(const std::string &query_statement,
                     std::string &enc_primary_key, Timestamp &ts,
                     proto::Write *write,
                     const proto::CommittedProof *committedProof);
  // Note: Could execute PointRead via ExecReadQuery (Eagerly) as well.
  //  ExecPointRead should translate enc_primary_key into a query_statement to
  //  be exec by ExecReadQuery. (Alternatively: Could already send a Sql command
  //  from the client)

  // Apply a set of Table Writes (versioned row creations) to the Table backend
  void ApplyTableWrite(const std::string &table_name,
                       const TableWrite &table_write, Timestamp &ts,
                       const std::string &txn_digest,
                       const proto::CommittedProof *commit_proof = nullptr,
                       bool commit_or_prepare = true);
  /// https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-upsert/
  void PurgeTableWrite(const std::string &table_name,
                       const TableWrite &table_write, Timestamp &ts,
                       const std::string &txn_digest);

  // Partially execute a read query statement (reconnaissance execution) and
  // return the snapshot state (managed by ssMgr)
  void FindSnapshot(std::string &query_statement, const Timestamp &ts,
                    SnapshotManager &ssMgr);

  // Materialize a snapshot on the Table backend and execute on said snapshot.
  void MaterializeSnapshot(
      const std::string &query_id, const proto::MergedSnapshot &merged_ss,
      const std::set<proto::Transaction *>
          &ss_txns); // Note: Not sure whether we should materialize full
                     // snapshot on demand, or continuously as we sync on Tx
  std::string ExecReadOnSnapshot(const std::string &query_id,
                                 std::string &query_statement,
                                 const Timestamp &ts,
                                 QueryReadSetMgr &readSetMgr,
                                 bool abort_early = false);

private:
  find_table_version
      set_table_version; // void function that finds current table version  ==>
                         // set bool accordingly whether using for read set or
                         // snapshot. Set un-used manager to nullptr
  read_prepared_pred can_read_prepared; // bool function to determine whether or
                                        // not to read prepared row
  SQLTransformer sql_interpreter;
  // TODO: Peloton DB singleton "table_backend"
  peloton::tcop::TrafficCop traffic_cop_;
  std::atomic_int counter_;
};

} // namespace pequinstore

#endif //_PEQUIN_TABLESTORE_H
