#ifndef _PELOTON_TABLESTORE_H_
#define _PELOTON_TABLESTORE_H_

#include "store/pequinstore/table_store_interface.h"

//Include whatever Peloton Deps
#include "../../query-engine/traffic_cop/traffic_cop.h"
#include "../../query-engine/common/logger.h"
#include "../../query-engine/common/macros.h"
#include "../../query-engine/parser/drop_statement.h"
#include "../../query-engine/parser/postgresparser.h"

#include "../../query-engine/catalog/catalog.h"
#include "../../query-engine/catalog/proc_catalog.h"
#include "../../query-engine/catalog/system_catalogs.h"

#include "../../query-engine/concurrency/transaction_manager_factory.h"

#include "../../query-engine/executor/create_executor.h"
#include "../../query-engine/executor/create_function_executor.h"
#include "../../query-engine/executor/executor_context.h"

#include "../../query-engine/planner/create_function_plan.h"
#include "../../query-engine/planner/create_plan.h"
#include "../../query-engine/storage/data_table.h"

#include "../../query-engine/executor/insert_executor.h"
#include "../../query-engine/expression/constant_value_expression.h"
#include "../../query-engine/parser/insert_statement.h"
#include "../../query-engine/planner/insert_plan.h"
#include "../../query-engine/traffic_cop/traffic_cop.h"
#include "../../query-engine/type/type.h"
#include "../../query-engine/type/value_factory.h"
#include "store/common/query_result/query_result_proto_builder.h"
#include <ostream>
#include <string>
#include <tuple>

namespace pequinstore {

class PelotonTableStore : public TableStore {
    public:
        PelotonTableStore();
        virtual ~PelotonTableStore();

        //Execute a statement directly on the Table backend, no questions asked, no output
        void ExecRaw(const std::string &sql_statement) override;

        void LoadTable(const std::string &load_statement, const std::string &txn_digest, const Timestamp &ts, const proto::CommittedProof *committedProof) override;

        //Execute a read query statement on the Table backend and return a query_result/proto (in serialized form) as well as a read set (managed by readSetMgr)
        std::string ExecReadQuery(const std::string &query_statement, const Timestamp &ts, QueryReadSetMgr &readSetMgr) override;

        //Execute a point read on the Table backend and return a query_result/proto (in serialized form) as well as a commitProof (note, the read set is implicit)
        void ExecPointRead(const std::string &query_statement, std::string &enc_primary_key, const Timestamp &ts, proto::Write *write, const proto::CommittedProof *committedProof) override;  
                //Note: Could execute PointRead via ExecReadQuery (Eagerly) as well.
                // ExecPointRead should translate enc_primary_key into a query_statement to be exec by ExecReadQuery. (Alternatively: Could already send a Sql command from the client)

        //Apply a set of Table Writes (versioned row creations) to the Table backend
        void ApplyTableWrite(const std::string &table_name, const TableWrite &table_write, const Timestamp &ts, const std::string &txn_digest, 
            const proto::CommittedProof *commit_proof = nullptr, bool commit_or_prepare = true) override; 
         ///https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-upsert/ 
        void PurgeTableWrite(const std::string &table_name, const TableWrite &table_write, const Timestamp &ts, const std::string &txn_digest) override; 

        

        //Partially execute a read query statement (reconnaissance execution) and return the snapshot state (managed by ssMgr)
        void FindSnapshot(std::string &query_statement, const Timestamp &ts, SnapshotManager &ssMgr) override;

        //Materialize a snapshot on the Table backend and execute on said snapshot.
        void MaterializeSnapshot(const std::string &query_id, const proto::MergedSnapshot &merged_ss, const std::set<proto::Transaction*> &ss_txns) override; //Note: Not sure whether we should materialize full snapshot on demand, or continuously as we sync on Tx
        std::string ExecReadOnSnapshot(const std::string &query_id, std::string &query_statement, const Timestamp &ts, QueryReadSetMgr &readSetMgr, bool abort_early = false) override;

        

    private:
        //Peloton DB singleton "table_backend"
		peloton::tcop::TrafficCop traffic_cop_;
		std::atomic_int counter_;
        std::string unnamed_statement;
        bool unnamed;

};


} // namespace pequinstore

#endif //_PELOTON_TABLESTORE_H

