#include "store/pequinstore/table_store_interface.h"
#include "store/pequinstore/sql_interpreter.h"

//TODO: Include whatever Peloton Deps

namespace pequinstore {


TableStore::TableStore(){
    //init Peloton
}

TableStore::~TableStore(){

}

//Execute a statement directly on the Table backend, no questions asked, no output
void TableStore::ExecRaw(std::string &sql_statement){

    //TODO: Execute on Peloton  //Note -- this should be a synchronous call. I.e. ExecRaw should not return before the call is done.
}

//Execute a read query statement on the Table backend and return a query_result/proto (in serialized form) as well as a read set (managed by readSetMgr)
std::string TableStore::ExecReadQuery(std::string &query_statement, const Timestamp &ts, QueryReadSetMgr &readSetMgr){

    //TODO: Execute on Peloton --> returns peloton result

    //TODO: Change Peloton result into query proto.
    sql::QueryResultProtoBuilder queryResultBuilder;
    // queryResultBuilder.add_column("result");
    // queryResultBuilder.add_row(result_row.begin(), result_row.end());

    return queryResultBuilder.get_result()->SerializeAsString();
}

//Execute a point read on the Table backend and return a query_result/proto (in serialized form) as well as a commitProof (note, the read set is implicit)
std::string TableStore::ExecPointRead(std::string &query_statement, std::string &enc_primary_key, const Timestamp &ts, Timestamp &read_version, proto::CommittedProof *committedProof){

    // std::string table_name;
    // std::vector<std::string> primary_key_column_values;
    // DecodeTableRow(enc_primary_key, table_name, primary_key_column_values);

    //TODO: Execute QueryStatement on Peloton. -> returns peloton result

    //TODO: Change Peloton result into query proto.
    sql::QueryResultProtoBuilder queryResultBuilder;
    // queryResultBuilder.add_column("result");
    // queryResultBuilder.add_row(result_row.begin(), result_row.end());

    //TODO: Extract proof/version from CC-store. --> return ReadReply + value = serialized proto result.

    return queryResultBuilder.get_result()->SerializeAsString();

}
        //Note: Could execute PointRead via ExecReadQuery (Eagerly) as well.
        // ExecPointRead should translate enc_primary_key into a query_statement to be exec by ExecReadQuery. 
        //(Alternatively: Could already send a Sql command from the client) ==> Should do it at the client, so that we can keep whatever Select specification, e.g. * or specific cols...

//Apply a set of Table Writes (versioned row creations) to the Table backend
void TableStore::ApplyTableWrite(const std::string &table_name, const TableWrite &table_write, const Timestamp &ts, const std::string &txn_digest, bool commit_or_prepare){
    //Turn txn_digest into a shared_ptr, write everywhere it is needed.
    std::shared_ptr<std::string> txn_dig(std::make_shared<std::string>(txn_digest));

   std::string write_statement;
   std::string delete_statement;
   bool has_delete = GenerateTableWriteStatement(write_statement, delete_statement, table_name, table_write);
    //TODO: Check whether there is a more efficient way than creating SQL commands for each.

    //TODO: Execute on Peloton
    //Exec write
    //if(has_delete) Exec delete
}

void TableStore::PurgeTableWrite(const std::string &table_name, const TableWrite &table_write, const Timestamp &ts, const std::string &txn_digest){

    std::shared_ptr<std::string> txn_dig(std::make_shared<std::string>(txn_digest));

    std::string purge_statement;
    GenerateTablePurgeStatement(purge_statement, table_name, table_write);   

    //TODO: Execute on Peloton
}
   

///////////////////// Snapshot Protocol Support

//Partially execute a read query statement (reconnaissance execution) and return the snapshot state (managed by ssMgr)
void TableStore::FindSnapshot(std::string &query_statement, const Timestamp &ts, SnapshotManager &ssMgr){

    //TODO: Execute on Peloton
    //Note: Don't need to return a result
    //Note: Ideally execution is "partial" and only executes the leaf scan operations.
}

//Materialize a snapshot on the Table backend and execute on said snapshot.
void TableStore::MaterializeSnapshot(const std::string &query_retry_id, const proto::MergedSnapshot &merged_ss, const std::set<proto::Transaction*> &ss_txns){
    //Note: Not sure whether we should materialize full snapshot on demand, or continuously as we sync on Tx

    //TODO: Apply all txn in snapshot to Table backend as a "view" that is only visible to query_id
    //FIXME: The merged_ss argument only holds the txn_ids. --> instead, call Materialize Snapshot on a set of transactions... ==> if doing it continuously might need to call this function often.
} 

std::string TableStore::ExecReadOnSnapshot(const std::string &query_retry_id, std::string &query_statement, const Timestamp &ts, QueryReadSetMgr &readSetMgr, bool abort_early ){
    //TODO: Execute on the snapshot for query_id/retry_version

    //--> returns peloton result
    //TODO: Change peloton result into query proto:

     sql::QueryResultProtoBuilder queryResultBuilder;
    // queryResultBuilder.add_column("result");
    // queryResultBuilder.add_row(result_row.begin(), result_row.end());

    return queryResultBuilder.get_result()->SerializeAsString();

}



} // namespace pequinstore
