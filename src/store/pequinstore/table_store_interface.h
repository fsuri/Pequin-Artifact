#ifndef _PEQUIN_TABLESTORE_H_
#define _PEQUIN_TABLESTORE_H_

#include "store/pequinstore/common.h" //for ReadSetMgr
#include "store/common/query_result/query_result_proto_builder.h"
#include "store/common/query_result/query_result.h"
#include "store/common/query_result/query_result_proto_wrapper.h"

//TODO: Include whatever Peloton Deps

namespace pequinstore {

class TableStore {
    public:
        TableStore();
        virtual ~TableStore();

        //Execute a statement directly on the Table backend, no questions asked, no output
        void ExecRaw(std::string &sql_statement);

        //Execute a read query statement on the Table backend and return a query_result/proto (in serialized form) as well as a read set (managed by readSetMgr)
        std::string ExecReadQuery(std::string &query_statement, const Timestamp &ts, QueryReadSetMgr &readSetMgr);

        //Execute a point read on the Table backend and return a query_result/proto (in serialized form) as well as a commitProof (note, the read set is implicit)
        std::string ExecPointRead(std::string &query_statement, std::string &enc_primary_key, const Timestamp &ts, Timestamp &read_version, proto::CommittedProof *committedProof);  
                //Note: Could execute PointRead via ExecReadQuery (Eagerly) as well.
                // ExecPointRead should translate enc_primary_key into a query_statement to be exec by ExecReadQuery. (Alternatively: Could already send a Sql command from the client)

        //Apply a set of Table Writes (versioned row creations) to the Table backend
        void ApplyTableWrite(const std::string &table_name, const TableWrite &table_write, const Timestamp &ts, const std::string &txn_digest, bool commit_or_prepare = true); 
         ///https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-upsert/ 
        void PurgeTableWrite(const std::string &table_name, const TableWrite &table_write, const Timestamp &ts, const std::string &txn_digest); 

        

        //Partially execute a read query statement (reconnaissance execution) and return the snapshot state (managed by ssMgr)
        void FindSnapshot(std::string &query_statement, const Timestamp &ts, SnapshotManager &ssMgr);

        //Materialize a snapshot on the Table backend and execute on said snapshot.
        void MaterializeSnapshot(const std::string &query_id, const proto::MergedSnapshot &merged_ss, const std::set<proto::Transaction*> &ss_txns); //Note: Not sure whether we should materialize full snapshot on demand, or continuously as we sync on Tx
        std::string ExecReadOnSnapshot(const std::string &query_id, std::string &query_statement, const Timestamp &ts, QueryReadSetMgr &readSetMgr, bool abort_early = false);


    private:

        //TODO: Peloton DB singleton "table_backend"
};


} // namespace pequinstore

#endif //_PEQUIN_TABLESTORE_H