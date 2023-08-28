#ifndef _PEQUIN_TABLESTORE_H_
#define _PEQUIN_TABLESTORE_H_

#include "store/pequinstore/common.h" //for ReadSetMgr
#include "store/common/query_result/query_result_proto_builder.h"
#include "store/common/query_result/query_result.h"
#include "store/common/query_result/query_result_proto_wrapper.h"

#include "store/pequinstore/sql_interpreter.h"

//TODO: Include whatever Peloton Deps

namespace pequinstore {

typedef std::function<void(const std::string &, const Timestamp &, bool, QueryReadSetMgr *, SnapshotManager *)> find_table_version;
typedef std::function<bool(const std::string &)> read_prepared_pred; //This is a function that, given a txnDigest of a prepared tx, evals to true if it is readable, and false if not.

class TableStore {
    public:
        TableStore();
        virtual ~TableStore();

        void SetFindTableVersion(find_table_version &&find_table_version); 
        void SetPreparePredicate(read_prepared_pred &&read_prepared_pred); 

        void RegisterTableSchema(std::string &table_registry_path);
            std::vector<bool>* GetRegistryColQuotes(const std::string &table_name);
            std::vector<bool>* GetRegistryPColQuotes(const std::string &table_name);

        //Execute a statement directly on the Table backend, no questions asked, no output
        void ExecRaw(const std::string &sql_statement);

        void LoadTable(const std::string &load_statement, const std::string &txn_digest, const Timestamp &ts, const proto::CommittedProof *committedProof);

        //Execute a read query statement on the Table backend and return a query_result/proto (in serialized form) as well as a read set (managed by readSetMgr)
        std::string ExecReadQuery(const std::string &query_statement, const Timestamp &ts, QueryReadSetMgr &readSetMgr);

        //Execute a point read on the Table backend and return a query_result/proto (in serialized form) as well as a commitProof (note, the read set is implicit)
        void ExecPointRead(const std::string &query_statement, std::string &enc_primary_key, const Timestamp &ts, proto::Write *write, const proto::CommittedProof *committedProof);  
                //Note: Could execute PointRead via ExecReadQuery (Eagerly) as well.
                // ExecPointRead should translate enc_primary_key into a query_statement to be exec by ExecReadQuery. (Alternatively: Could already send a Sql command from the client)

        //Apply a set of Table Writes (versioned row creations) to the Table backend
        void ApplyTableWrite(const std::string &table_name, const TableWrite &table_write, const Timestamp &ts, const std::string &txn_digest, 
            const proto::CommittedProof *commit_proof = nullptr, bool commit_or_prepare = true); 
         ///https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-upsert/ 
        void PurgeTableWrite(const std::string &table_name, const TableWrite &table_write, const Timestamp &ts, const std::string &txn_digest); 

        

        //Partially execute a read query statement (reconnaissance execution) and return the snapshot state (managed by ssMgr)
        void FindSnapshot(std::string &query_statement, const Timestamp &ts, SnapshotManager &ssMgr);

        //Materialize a snapshot on the Table backend and execute on said snapshot.
        void MaterializeSnapshot(const std::string &query_id, const proto::MergedSnapshot &merged_ss, const std::set<proto::Transaction*> &ss_txns); //Note: Not sure whether we should materialize full snapshot on demand, or continuously as we sync on Tx
        std::string ExecReadOnSnapshot(const std::string &query_id, std::string &query_statement, const Timestamp &ts, QueryReadSetMgr &readSetMgr, bool abort_early = false);


    private:
        find_table_version record_table_version;  //void function that finds current table version  ==> set bool accordingly whether using for read set or snapshot. Set un-used manager to nullptr
        read_prepared_pred can_read_prepared; //bool function to determine whether or not to read prepared row
        SQLTransformer sql_interpreter;
        //TODO: Peloton DB singleton "table_backend"
};


} // namespace pequinstore

#endif //_PEQUIN_TABLESTORE_H


class Client {
 public:
  Client() {};
  virtual ~Client() {};

  // Begin a transaction.
  virtual void Begin(begin_callback bcb, begin_timeout_callback btcb,
      uint32_t timeout, bool retry = false) = 0;

  // Get the value corresponding to key.
  virtual void Get(const std::string &key, get_callback gcb,
      get_timeout_callback gtcb, uint32_t timeout) = 0;

  // Set the value for the given key.
  virtual void Put(const std::string &key, const std::string &value,
      put_callback pcb, put_timeout_callback ptcb, uint32_t timeout) = 0;

  // Commit all Get(s) and Put(s) since Begin().
  virtual void Commit(commit_callback ccb, commit_timeout_callback ctcb,
      uint32_t timeout) = 0;

  // Abort all Get(s) and Put(s) since Begin().
  virtual void Abort(abort_callback acb, abort_timeout_callback atcb,
      uint32_t timeout) = 0;

  // Get the result for a given query SQL statement
  inline virtual void Query(const std::string &query_statement, query_callback qcb,
      query_timeout_callback qtcb, uint32_t timeout, bool skip_query_interpretation = false){Panic("This protocol store does not implement support for Query Statements"); };   

  //inline virtual void Wait(vector of results) { just do nothing unless overriden} ;; Wait will call getResult, which in turn will trigger the Query callbacks

  // Get the result (rows affected) for a given write SQL statement
  inline virtual void Write(std::string &write_statement, write_callback wcb,
      write_timeout_callback wtcb, uint32_t timeout){Panic("This protocol store does not implement support for Write Statements"); };   //TODO: Can probably avoid using Callbacks at all. Just void write-through.

  inline const Stats &GetStats() const { return stats; }

 protected:
  Stats stats;
};

#endif /* _CLIENT_API_H_ */