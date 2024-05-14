/***********************************************************************
 *
 * store/pequinstore/table_store_interface.h: 
 *      Implementation of Pesto to DataStore interface
 *
 * Copyright 2023 Florian Suri-Payer <fsp@cs.cornell.edu>
 *            
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), 
 * to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, 
 * and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. 
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, 
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 **********************************************************************/

#ifndef _PEQUIN_TABLESTORE_H_
#define _PEQUIN_TABLESTORE_H_

#include "store/common/query_result/query_result.h"
#include "store/common/query_result/query_result_proto_builder.h"
#include "store/common/query_result/query_result_proto_wrapper.h"
#include "store/pequinstore/common.h" //for ReadSetMgr

#include "store/pequinstore/sql_interpreter.h"

namespace pequinstore {

// typedef std::function<void(const std::string &, const Timestamp &, bool,
//                            QueryReadSetMgr *, SnapshotManager *)>
//     find_table_version;
// typedef std::function<bool(const std::string &)>
//     read_prepared_pred; // This is a function that, given a txnDigest of a
//                         // prepared tx, evals to true if it is readable, and
//                         // false if not.

class TableStore {
    public:
        TableStore(): sql_interpreter(nullptr) {};
        TableStore(const QueryParameters *query_params, std::string &table_registry_path, find_table_version &&find_table_version, read_prepared_pred &&read_prepared_pred):
            query_params(query_params), sql_interpreter(query_params) {
            sql_interpreter.RegisterTables(table_registry_path);
            record_table_version = std::move(find_table_version);
            can_read_prepared = std::move(read_prepared_pred);
        };
        virtual ~TableStore() {};

        //Generic helper functions
        void SetFindTableVersion(find_table_version &&find_table_version){
            record_table_version = std::move(find_table_version);
        }
        void SetPreparePredicate(read_prepared_pred &&read_prepared_pred){
            can_read_prepared = std::move(read_prepared_pred);
        } 

        void RegisterTableSchema(std::string &table_registry_path){
            sql_interpreter.RegisterTables(table_registry_path);
        }

        std::vector<bool>* GetRegistryColQuotes(const std::string &table_name){
            return &(sql_interpreter.GetColRegistry(table_name)->col_quotes);
        }

        std::vector<bool>* GetRegistryPColQuotes(const std::string &table_name){
            return &(sql_interpreter.GetColRegistry(table_name)->p_col_quotes);
        }

        //Backend specific implementations

        //Execute a statement directly on the Table backend, no questions asked, no output
        virtual void ExecRaw(const std::string &sql_statement, bool skip_cache = true) = 0;

        virtual void LoadTable(const std::string &load_statement, const std::string &txn_digest, const Timestamp &ts, const proto::CommittedProof *committedProof) = 0;

        //Execute a read query statement on the Table backend and return a query_result/proto (in serialized form) as well as a read set (managed by readSetMgr)
        virtual std::string ExecReadQuery(const std::string &query_statement, const Timestamp &ts, QueryReadSetMgr &readSetMgr) = 0;

        //Execute a point read on the Table backend and return a query_result/proto (in serialized form) as well as a commitProof (note, the read set is implicit)
        virtual void ExecPointRead(const std::string &query_statement, std::string &enc_primary_key, const Timestamp &ts, proto::Write *write, const proto::CommittedProof *&committedProof) = 0;  
                //Note: Could execute PointRead via ExecReadQuery (Eagerly) as well.
                // ExecPointRead should translate enc_primary_key into a query_statement to be exec by ExecReadQuery. (Alternatively: Could already send a Sql command from the client)

        //Apply a set of Table Writes (versioned row creations) to the Table backend
        virtual void ApplyTableWrite(const std::string &table_name, const TableWrite &table_write, const Timestamp &ts, const std::string &txn_digest, 
            const proto::CommittedProof *commit_proof = nullptr, bool commit_or_prepare = true, bool forcedMaterialize = false) = 0; 

         ///https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-upsert/ 
        virtual void PurgeTableWrite(const std::string &table_name, const TableWrite &table_write, const Timestamp &ts, const std::string &txn_digest) = 0; 

        

        //Partially execute a read query statement (reconnaissance execution) and return the snapshot state (managed by ssMgr)
        virtual void FindSnapshot(const std::string &query_statement, const Timestamp &ts, SnapshotManager &ssMgr, size_t snapshot_prepared_k = 1) = 0;

        virtual std::string EagerExecAndSnapshot(const std::string &query_statement, const Timestamp &ts, SnapshotManager &ssMgr, QueryReadSetMgr &readSetMgr, size_t snapshot_prepared_k = 1) = 0;

        virtual std::string ExecReadQueryOnMaterializedSnapshot(const std::string &query_statement, const Timestamp &ts, QueryReadSetMgr &readSetMgr,
            const ::google::protobuf::Map<std::string, proto::ReplicaList> &ss_txns) = 0;


        // //Materialize a snapshot on the Table backend and execute on said snapshot.
        // virtual void MaterializeSnapshot(const std::string &query_id, const proto::MergedSnapshot &merged_ss, const std::set<proto::Transaction*> &ss_txns) = 0; 
        // //Note: Not sure whether we should materialize full snapshot on demand, or continuously as we sync on Tx
        // virtual std::string ExecReadOnSnapshot(const std::string &query_id, std::string &query_statement, const Timestamp &ts, QueryReadSetMgr &readSetMgr, bool abort_early = false) = 0; 


        const QueryParameters *query_params;
        find_table_version record_table_version;  //void function that finds current table version  ==> set bool accordingly whether using for read set or snapshot. Set un-used manager to nullptr
        read_prepared_pred can_read_prepared; //bool function to determine whether or not to read prepared row
        SQLTransformer sql_interpreter;
    private:
        
};

} // namespace pequinstore

#endif //_PEQUIN_TABLESTORE_H
