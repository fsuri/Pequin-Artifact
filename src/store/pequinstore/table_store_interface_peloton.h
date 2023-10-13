#ifndef _PELOTON_TABLESTORE_H_
#define _PELOTON_TABLESTORE_H_

#include "store/pequinstore/table_store_interface.h"

#include "lib/latency.h"
#include "lib/message.h"

// Include whatever Peloton Deps
// #include "../../query-engine/common/logger.h"
// #include "../../query-engine/common/macros.h"
// #include "../../query-engine/parser/drop_statement.h"
#include "../../query-engine/parser/postgresparser.h"
// #include "../../query-engine/traffic_cop/traffic_cop.h"

#include "../../query-engine/catalog/catalog.h"
// #include "../../query-engine/catalog/proc_catalog.h"
// #include "../../query-engine/catalog/system_catalogs.h"

#include "../../query-engine/concurrency/transaction_manager_factory.h"

// #include "../../query-engine/executor/create_executor.h"
// #include "../../query-engine/executor/create_function_executor.h"
// #include "../../query-engine/executor/executor_context.h"

// #include "../../query-engine/planner/create_function_plan.h"
// #include "../../query-engine/planner/create_plan.h"
// #include "../../query-engine/storage/data_table.h"

// #include "../../query-engine/executor/insert_executor.h"
// #include "../../query-engine/expression/constant_value_expression.h"
// #include "../../query-engine/parser/insert_statement.h"
// #include "../../query-engine/planner/insert_plan.h"
#include "../../query-engine/traffic_cop/traffic_cop.h"
// #include "../../query-engine/type/type.h"
// #include "../../query-engine/type/value_factory.h"
#include "store/common/query_result/query_result_proto_builder.h"
#include <ostream>
#include <string>
#include <tuple>

#include "lib/concurrentqueue/concurrentqueue.h"
#include <fmt/core.h>
#include <fmt/ranges.h>

namespace pequinstore {

class PelotonTableStore : public TableStore {
    public:
        PelotonTableStore(int num_threads = 0);
        PelotonTableStore(std::string &table_registry_path, find_table_version &&find_table_version, read_prepared_pred &&read_prepared_pred, int num_threads = 0);
        virtual ~PelotonTableStore();

        // Execute a statement directly on the Table backend, no questions asked, no
        // output
        void ExecRaw(const std::string &sql_statement) override;

        void LoadTable(const std::string &load_statement, const std::string &txn_digest, const Timestamp &ts, const proto::CommittedProof *committedProof) override;

        //Execute a read query statement on the Table backend and return a query_result/proto (in serialized form) as well as a read set (managed by readSetMgr)
        std::string ExecReadQuery(const std::string &query_statement, const Timestamp &ts, QueryReadSetMgr &readSetMgr) override;
                            
        //Execute a point read on the Table backend and return a query_result/proto (in serialized form) as well as a commitProof (note, the read set is implicit)
        void ExecPointRead(const std::string &query_statement, std::string &enc_primary_key, const Timestamp &ts, proto::Write *write, const proto::CommittedProof* &committedProof) override;  
                //Note: Could execute PointRead via ExecReadQuery (Eagerly) as well.
                // ExecPointRead should translate enc_primary_key into a query_statement to be exec by ExecReadQuery. (Alternatively: Could already send a Sql command from the client)

        //Apply a set of Table Writes (versioned row creations) to the Table backend
        void ApplyTableWrite(const std::string &table_name, const TableWrite &table_write, const Timestamp &ts,
                const std::string &txn_digest, const proto::CommittedProof *commit_proof = nullptr, bool commit_or_prepare = true, bool forceMaterialize = false) override;

            ///https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-upsert/ 
        void PurgeTableWrite(const std::string &table_name, const TableWrite &table_write, const Timestamp &ts, const std::string &txn_digest) override; 

        

        //Partially execute a read query statement (reconnaissance execution) and return the snapshot state (managed by ssMgr)
        void FindSnapshot(const std::string &query_statement, const Timestamp &ts, SnapshotManager &ssMgr) override;

        std::string EagerExecAndSnapshot(const std::string &query_statement, const Timestamp &ts, SnapshotManager &ssMgr, QueryReadSetMgr &readSetMgr) override;

        std::string ExecReadQueryOnMaterializedSnapshot(const std::string &query_statement, const Timestamp &ts, QueryReadSetMgr &readSetMgr,
            const google::protobuf::Map<std::string, proto::ReplicaList> &ss_txns) override;
        //TODO: in this read; only read if txn-id of tuple in snapshot. Allow to read "materialized" visibility

        //DEPRECATED:

        // //Materialize a snapshot on the Table backend and execute on said snapshot.
        // void MaterializeSnapshot(const std::string &query_id, const proto::MergedSnapshot &merged_ss, const std::set<proto::Transaction*> &ss_txns) override; 
        //         //Note: Not sure whether we should materialize full snapshot on demand, or continuously as we sync on Tx
        // std::string ExecReadOnSnapshot(const std::string &query_id, std::string &query_statement, const Timestamp &ts, QueryReadSetMgr &readSetMgr, bool abort_early = false) override;

    
    private:
        void Init(int num_threads);

        std::vector<Latency_t> readLats;
        std::vector<Latency_t> writeLats;

        std::string unnamed_statement;
        bool unnamed_variable;

        //Peloton DB singleton "table_backend"
		peloton::tcop::TrafficCop traffic_cop_;
		std::atomic_int counter_;
        bool is_recycled_version_;

        //std::vector<peloton::tcop::TrafficCop*> traffic_cops;
        moodycamel::ConcurrentQueue<std::pair<peloton::tcop::TrafficCop*, std::atomic_int*>> traffic_cops; //https://github.com/cameron314/concurrentqueue

        std::pair<peloton::tcop::TrafficCop*, std::atomic_int*> GetUnusedTrafficCop();
        void ReleaseTrafficCop(std::pair<peloton::tcop::TrafficCop*, std::atomic_int*> cop_pair);

        int num_threads;
        std::vector<std::pair<peloton::tcop::TrafficCop *, std::atomic_int *>> traffic_cops_;
        std::pair<peloton::tcop::TrafficCop *, std::atomic_int *> GetCop();

        std::shared_ptr<peloton::Statement> ParseAndPrepare(const std::string &query_statement, peloton::tcop::TrafficCop *tcop);

        void GetResult(peloton::ResultType &status, peloton::tcop::TrafficCop *tcop, std::atomic_int *c);

        //std::string TransformResult(std::vector<peloton::FieldInfo> &tuple_descriptor, std::vector<peloton::ResultValue> &result);
        std::string TransformResult(peloton::ResultType &status, std::shared_ptr<peloton::Statement> statement, std::vector<peloton::ResultValue> &result);
        void TransformPointResult(proto::Write *write, Timestamp &committed_timestamp, Timestamp &prepared_timestamp, std::shared_ptr<std::string> txn_dig, 
                                    peloton::ResultType &status, std::shared_ptr<peloton::Statement> statement, std::vector<peloton::ResultValue> &result);

};

} // namespace pequinstore

#endif //_PELOTON_TABLESTORE_H

//// OLD POINT QUERY CODE:
// std::vector<peloton::FieldInfo> tuple_descriptor;
//     if (status == peloton::ResultType::SUCCESS) {
//         tuple_descriptor = statement->GetTupleDescriptor();
//     }

//     // write->set_committed_value()
//     std::cout << "Commit proof client id: "
//                 << traffic_cop_.commit_proof_->txn().client_id()
//                 << " : sequence number: "
//                 << traffic_cop_.commit_proof_->txn().client_seq_num()
//                 << std::endl;

//     // TODO: Change Peloton result into query proto.
//     sql::QueryResultProtoBuilder queryResultBuilder;
//     // queryResultBuilder.add_column("result");
//     // queryResultBuilder.add_row(result_row.begin(), result_row.end());
//     std::cout << "Before adding columns" << std::endl;
//     // Add columns
//     for (unsigned int i = 0; i < tuple_descriptor.size(); i++) {
//         std::string column_name = std::get<0>(tuple_descriptor[i]);
//         queryResultBuilder.add_column(column_name);
//     }

//     // std::cout << "Before adding rows" << std::endl;
//     // std::cout << "Tuple descriptor size is " << tuple_descriptor.size()
//     //<< std::endl;
//     bool read_prepared = false;
//     bool already_read_prepared = false;

//     // Add rows
//     unsigned int rows = result.size() / tuple_descriptor.size();
//     for (unsigned int i = 0; i < rows; i++) {
//         // std::string row_string = "Row " + std::to_string(i) + ": ";
//         // std::cout << "Row index is " << i << std::endl;
//         // queryResultBuilder.add_empty_row();
//         RowProto *row = queryResultBuilder.new_row();
//         std::string row_string = "";

//         // queryResultBuilder.add_empty_row();
//         for (unsigned int j = 0; j < tuple_descriptor.size(); j++) {
//         // queryResultBuilder.AddToRow(row,
//         result[i*tuple_descriptor.size()+j]);
//         // std::cout << "Get field value" << std::endl;
//         // FieldProto *field = row->add_fields();
//         // std::string field_value = GetResultValueAsString(result, i *
//         // tuple_descriptor.size() + j);
//         queryResultBuilder.AddToRow(row, result[i * tuple_descriptor.size() +
//         j]);
//         // field->set_data(queryResultBuilder.serialize(field_value));
//         // field->set_data(result[i*tuple_descriptor.size()+j]);
//         // std::cout << "After" << std::endl;
//         // row_string += field_value + " ";

//         // queryResultBuilder.update_field_in_row(i, j, field_value);
//         // row_string += GetResultValueAsString(result, i *
//         // tuple_descriptor.size() + j);

//         // std::cout << "Inside j loop" << std::endl;
//         // std::cout << GetResultValueAsString(result, i *
//         tuple_descriptor.size()
//         // + j) << std::endl;
//         }
//         if (read_prepared && !already_read_prepared) {
//         write->set_prepared_value(row_string);
//         std::cout << "Prepared value is " << row_string << std::endl;
//         write->set_prepared_txn_digest(*txn_dig.get());
//         std::cout << "Prepared txn digest is " << *txn_dig.get() <<
//         std::endl;
//         //
//         write->set_allocated_prepared_timestamp(TimestampMessage{prepared_timestamp.getID(),
//         // prepared_timestamp.getTimestamp()});
//         std::cout << "Prepared timestamp is " <<
//         prepared_timestamp.getTimestamp()
//                     << ", " << prepared_timestamp.getID() << std::endl;

//         already_read_prepared = true;
//         }

//         write->set_committed_value(row_string);
//         std::cout << "Committed value is " << row_string << std::endl;
//         //
//         write->set_allocated_committed_timestamp(TimestampMessage(committed_timestamp));
//         std::cout << "Commit timestamp is " <<
//         committed_timestamp.getTimestamp()
//                 << ", " << committed_timestamp.getID() << std::endl;
//     }
//     //
//     write->set_allocated_proof(traffic_cop_.commit_proof_->SerializeAsString());

//     std::cout << "Result from query result builder is " << std::endl;
//     std::cout << queryResultBuilder.get_result()->SerializeAsString()
//                 << std::endl;

//     // return queryResultBuilder.get_result()->SerializeAsString();
