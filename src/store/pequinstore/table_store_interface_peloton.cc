#include "store/pequinstore/table_store_interface_peloton.h"

//TODO: Include whatever Peloton Deps

namespace pequinstore {


std::string GetResultValueAsString(const std::vector<peloton::ResultValue> &result, size_t index) {
    std::string value(result[index].begin(), result[index].end());
    return value;
}

void UtilTestTaskCallback(void *arg) {
    std::atomic_int *count = static_cast<std::atomic_int *>(arg);
    count->store(0);
}

void ContinueAfterComplete(std::atomic_int &counter_) {
    while (counter_.load() == 1) {
        usleep(10);
    }
}



PelotonTableStore::PelotonTableStore(): traffic_cop_(UtilTestTaskCallback, &counter_), unnamed_statement("unnamed"), unnamed(false) {
  // init Peloton
  auto &txn_manager = peloton::concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  peloton::catalog::Catalog::GetInstance()->CreateDatabase(txn, DEFAULT_DB_NAME);
  txn_manager.CommitTransaction(txn);
  // traffic_cop_ = peloton::tcop::TrafficCop(UtilTestTaskCallback, &counter_);
}

PelotonTableStore::~PelotonTableStore(){

}


//Execute a statement directly on the Table backend, no questions asked, no output
void PelotonTableStore::ExecRaw(const std::string &sql_statement){

    //TODO: Execute on Peloton  //Note -- this should be a synchronous call. I.e. ExecRaw should not return before the call is done.

    //TODO: When calling the LoadStatement: We'll want to initialize all rows to be committed and have genesis proof (see server)

    std::vector<peloton::ResultValue> result;
    std::vector<peloton::FieldInfo> tuple_descriptor;

    // execute the query using tcop
    // prepareStatement
    // LOG_TRACE("Query: %s", query.c_str());
    auto &peloton_parser = peloton::parser::PostgresParser::GetInstance();
    auto sql_stmt_list = peloton_parser.BuildParseTree(sql_statement);
    // PELOTON_ASSERT(sql_stmt_list);
    if (!sql_stmt_list->is_valid) {
        // return peloton::ResultType::FAILURE;
    }
    auto statement = traffic_cop_.PrepareStatement(
        unnamed_statement, sql_statement, std::move(sql_stmt_list));
    if (statement.get() == nullptr) {
        traffic_cop_.setRowsAffected(0);
        // return peloton::ResultType::FAILURE;
    }
    // ExecuteStatment
    std::vector<peloton::type::Value> param_values;
    
    std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);

    // SetTrafficCopCounter();
    counter_.store(1);
    auto status = traffic_cop_.ExecuteStatement(statement, param_values, unnamed,
                                                result_format, result);
    std::cout << "Made it after status" << std::endl;
    if (traffic_cop_.GetQueuing()) {
        ContinueAfterComplete(counter_);
        traffic_cop_.ExecuteStatementPlanGetResult();
        status = traffic_cop_.ExecuteStatementGetResult();
        traffic_cop_.SetQueuing(false);
    }
    std::cout << "Check if status is success" << std::endl;
    if (status == peloton::ResultType::SUCCESS) {
        tuple_descriptor = statement->GetTupleDescriptor();
    }

    std::cout << "End of exec raw" << std::endl;

}

void PelotonTableStore::LoadTable(const std::string &load_statement, const std::string &txn_digest, const Timestamp &ts, const proto::CommittedProof *committedProof){
         //Turn txn_digest into a shared_ptr, write everywhere it is needed.
    std::shared_ptr<std::string> txn_dig(std::make_shared<std::string>(txn_digest));

    //Call statement (of type Copy or Insert) and set meta data accordingly (bool commit = true, committedProof, txn_digest, ts)
    std::vector<peloton::ResultValue> result;
    std::vector<peloton::FieldInfo> tuple_descriptor;

    // execute the query using tcop
    // prepareStatement
    // LOG_TRACE("Query: %s", query.c_str());
    auto &peloton_parser = peloton::parser::PostgresParser::GetInstance();
    auto sql_stmt_list = peloton_parser.BuildParseTree(load_statement);
    // PELOTON_ASSERT(sql_stmt_list);
    if (!sql_stmt_list->is_valid) {
        // return peloton::ResultType::FAILURE;
    }
    auto statement = traffic_cop_.PrepareStatement(
        unnamed_statement, load_statement, std::move(sql_stmt_list));
    if (statement.get() == nullptr) {
        traffic_cop_.setRowsAffected(0);
        // return peloton::ResultType::FAILURE;
    }
    // ExecuteStatment
    std::vector<peloton::type::Value> param_values;
    
    std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);
    // SetTrafficCopCounter();
    counter_.store(1);
    // pequinstore::proto::ReadSet read_set_one;

    // pequinstore::QueryReadSetMgr query_read_set_mgr_one(&read_set_one, 1,
    // false);
    auto status = traffic_cop_.ExecuteStatement(statement, param_values, unnamed,
                                                result_format, result);

    if (traffic_cop_.GetQueuing()) {
        ContinueAfterComplete(counter_);
        traffic_cop_.ExecuteStatementPlanGetResult();
        status = traffic_cop_.ExecuteStatementGetResult();
        traffic_cop_.SetQueuing(false);
    }
    if (status == peloton::ResultType::SUCCESS) {
        tuple_descriptor = statement->GetTupleDescriptor();
    }
}

//Execute a read query statement on the Table backend and return a query_result/proto (in serialized form) as well as a read set (managed by readSetMgr)
std::string PelotonTableStore::ExecReadQuery(const std::string &query_statement, const Timestamp &ts, QueryReadSetMgr &readSetMgr){

            //args: query, Ts, readSetMgr, this->can_read_prepared, this->set_table_version
    //TODO: Execute on Peloton --> returns peloton result

    std::cout << "Inside ExecReadQuery" << std::endl;
    // args: query, Ts, readSetMgr, this->can_read_prepared,
    // this->set_table_version
    // TODO: Execute on Peloton --> returns peloton result
    std::vector<peloton::ResultValue> result;
    std::vector<peloton::FieldInfo> tuple_descriptor;

    // execute the query using tcop
    // prepareStatement
    // LOG_TRACE("Query: %s", query.c_str());
    auto &peloton_parser = peloton::parser::PostgresParser::GetInstance();
    auto sql_stmt_list = peloton_parser.BuildParseTree(query_statement);
    // PELOTON_ASSERT(sql_stmt_list);
    if (!sql_stmt_list->is_valid) {
        // return peloton::ResultType::FAILURE;
    }

    auto statement = traffic_cop_.PrepareStatement(
        unnamed_statement, query_statement, std::move(sql_stmt_list));
    if (statement.get() == nullptr) {
        traffic_cop_.setRowsAffected(0);
        // return peloton::ResultType::FAILURE;
    }
    // ExecuteStatment
    std::vector<peloton::type::Value> param_values;
    
    // TODO: Pass in predicate here as well, table_version not for point read
    //only readquery, done before touching indexes
    std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);
    // SetTrafficCopCounter();
    counter_.store(1);
    auto status = traffic_cop_.ExecuteReadStatement(
        statement, param_values, unnamed, result_format, result, ts, readSetMgr,
        this->record_table_version, this->can_read_prepared);
    if (traffic_cop_.GetQueuing()) {
        ContinueAfterComplete(counter_);
        traffic_cop_.ExecuteStatementPlanGetResult();
        status = traffic_cop_.ExecuteStatementGetResult();
        traffic_cop_.SetQueuing(false);
    }

    if (status == peloton::ResultType::SUCCESS) {
        tuple_descriptor = statement->GetTupleDescriptor();
    }

    

    // Change Peloton result into query proto.
     // std::cout << "Tuple descriptor size is " << tuple_descriptor.size() << std::endl;
    sql::QueryResultProtoBuilder queryResultBuilder;
    // Add columns
    for (unsigned int i = 0; i < tuple_descriptor.size(); i++) {
        std::string column_name = std::get<0>(tuple_descriptor[i]);
        queryResultBuilder.add_column(column_name);
    }
   
    // Add rows
    unsigned int rows = result.size() / tuple_descriptor.size();
    for (unsigned int i = 0; i < rows; i++) {
        RowProto *row = queryResultBuilder.new_row();
        for (unsigned int j = 0; j < tuple_descriptor.size(); j++) {
        // TODO: Use interface addtorow, and pass in field to that row
            queryResultBuilder.AddToRow(row, result[i * tuple_descriptor.size() + j]);
        }
    }

    return queryResultBuilder.get_result()->SerializeAsString();
}

//Execute a point read on the Table backend and return a query_result/proto (in serialized form) as well as a commitProof (note, the read set is implicit)
void PelotonTableStore::ExecPointRead(const std::string &query_statement, std::string &enc_primary_key, const Timestamp &ts, proto::Write *write, const proto::CommittedProof *committedProof){

    //Client sends query statement, and expects a Query Result for the given key, a timestamp, and a proof (if it was a committed value it read)
        //Sending a query statement (even though it is a point request) allows us to handle complex Select operators (like Count, Max, or just some subset of rows, etc) without additional parsing
        //Since the CC-store holds no data, we have to generate a statement otherwise anyways --> so it's easiest to just send it from the client as is (rather than assembling it from the encoded key )
                                                                                                                                    // std::string table_name;
                                                                                                                                    // std::vector<std::string> primary_key_column_values;
                                                                                                                                    // DecodeTableRow(enc_primary_key, table_name, primary_key_column_values);

    //TODO: If read_prepared = true read both committed/prepared read
    // if true --> After execution check txn_digest of prepared_value (if exist). Check dependency depth. for txn_digest. If too deep, remove it. 
        //FIXME: to have access to this: need server (pass as this in constructor?) ==> No, do this stuff inside the ProcessPointQuery level.
        //TODO: If no write/read exists (result == empty) -> send empty result (i.e. no fields in write are set), read_time = 0 by default
                 // WARNING: Don't set prepared or committed -- let client side default handling take care of it.
                                
                // (optional TODO:) For optimal CC we'd ideally send the time of last delete (to minimize conflict window) 
                        //- but then we have to send it as committed (with proof) or as prepared (with value = empty result)
                        //Client will have to check proof txn ==> lookup that key exists in Writeset was marked as delete.
                              //Note: For Query reads that would technically be the best too --> the coarse lock of the Table Version helps simulate it.
           
                              

        //Alternatively: 
            //Since we also need to avoid reading prepared for the normal queries:
            //Pass down a Lambda function that takes in txn_digest and checks whether is readable (Like passing an electrical probe down the ocean)
    //Don't read TableVersion (quetion: how do we read table version for normal query? --> let it return table name and then look up?)
    

         //args: query, Ts, this->can_read_prepared ; commit: (result, timestamp, proof), prepared: (result, timestamp, txn_digest), key (optional)
    //TODO: Execute QueryStatement on Peloton. -> returns peloton result
            //TODO: Read latest committed (return committedProof) + Read latest prepared (if > committed)

  

    //TODO: Extract proof/version from CC-store. --> return ReadReply + value = serialized proto result.

    // TODO: use add row interface for serialization //
    // DecodeTableRow(enc_primary_key, table_name, primary_key_column_values);
    std::vector<peloton::ResultValue> result;
    std::vector<peloton::FieldInfo> tuple_descriptor;

    // execute the query using tcop
    // prepareStatement
    // LOG_TRACE("Query: %s", query.c_str());
    auto &peloton_parser = peloton::parser::PostgresParser::GetInstance();
    auto sql_stmt_list = peloton_parser.BuildParseTree(query_statement);
    // PELOTON_ASSERT(sql_stmt_list);
    if (!sql_stmt_list->is_valid) {
        // return peloton::ResultType::FAILURE;
    }
    auto statement = traffic_cop_.PrepareStatement(
        unnamed_statement, query_statement, std::move(sql_stmt_list));
    if (statement.get() == nullptr) {
        traffic_cop_.setRowsAffected(0);
        // return peloton::ResultType::FAILURE;
    }
    // ExecuteStatment
    std::vector<peloton::type::Value> param_values;
    
    std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);
    // SetTrafficCopCounter();
    counter_.store(1);

    Timestamp committed_timestamp;
    Timestamp prepared_timestamp;
    std::shared_ptr<std::string> txn_dig;

    auto status = traffic_cop_.ExecutePointReadStatement(
        statement, param_values, unnamed, result_format, result, ts,
        this->can_read_prepared, &committed_timestamp, committedProof,
        &prepared_timestamp, txn_dig, write);
    if (traffic_cop_.GetQueuing()) {
        ContinueAfterComplete(counter_);
        traffic_cop_.ExecuteStatementPlanGetResult();
        status = traffic_cop_.ExecuteStatementGetResult();
        traffic_cop_.SetQueuing(false);
    }
    if (status == peloton::ResultType::SUCCESS) {
        tuple_descriptor = statement->GetTupleDescriptor();
    }

    // write->set_committed_value()
    std::cout << "Commit proof client id: "
                << traffic_cop_.commit_proof_->txn().client_id()
                << " : sequence number: "
                << traffic_cop_.commit_proof_->txn().client_seq_num()
                << std::endl;

    // TODO: Change Peloton result into query proto.
    sql::QueryResultProtoBuilder queryResultBuilder;
    // queryResultBuilder.add_column("result");
    // queryResultBuilder.add_row(result_row.begin(), result_row.end());
    std::cout << "Before adding columns" << std::endl;
    // Add columns
    for (unsigned int i = 0; i < tuple_descriptor.size(); i++) {
        std::string column_name = std::get<0>(tuple_descriptor[i]);
        queryResultBuilder.add_column(column_name);
    }

    // std::cout << "Before adding rows" << std::endl;
    // std::cout << "Tuple descriptor size is " << tuple_descriptor.size()
    //<< std::endl;
    bool read_prepared = false;
    bool already_read_prepared = false;

    // Add rows
    unsigned int rows = result.size() / tuple_descriptor.size();
    for (unsigned int i = 0; i < rows; i++) {
        // std::string row_string = "Row " + std::to_string(i) + ": ";
        // std::cout << "Row index is " << i << std::endl;
        // queryResultBuilder.add_empty_row();
        RowProto *row = queryResultBuilder.new_row();
        std::string row_string = "";

        // queryResultBuilder.add_empty_row();
        for (unsigned int j = 0; j < tuple_descriptor.size(); j++) {
        // queryResultBuilder.AddToRow(row, result[i*tuple_descriptor.size()+j]);
        // std::cout << "Get field value" << std::endl;
        // FieldProto *field = row->add_fields();
        // std::string field_value = GetResultValueAsString(result, i *
        // tuple_descriptor.size() + j);
        queryResultBuilder.AddToRow(row, result[i * tuple_descriptor.size() + j]);
        // field->set_data(queryResultBuilder.serialize(field_value));
        // field->set_data(result[i*tuple_descriptor.size()+j]);
        // std::cout << "After" << std::endl;
        // row_string += field_value + " ";

        // queryResultBuilder.update_field_in_row(i, j, field_value);
        // row_string += GetResultValueAsString(result, i *
        // tuple_descriptor.size() + j);

        // std::cout << "Inside j loop" << std::endl;
        // std::cout << GetResultValueAsString(result, i * tuple_descriptor.size()
        // + j) << std::endl;
        }
        if (read_prepared && !already_read_prepared) {
        write->set_prepared_value(row_string);
        std::cout << "Prepared value is " << row_string << std::endl;
        write->set_prepared_txn_digest(*txn_dig.get());
        std::cout << "Prepared txn digest is " << *txn_dig.get() << std::endl;
        // write->set_allocated_prepared_timestamp(TimestampMessage{prepared_timestamp.getID(),
        // prepared_timestamp.getTimestamp()});
        std::cout << "Prepared timestamp is " << prepared_timestamp.getTimestamp()
                    << ", " << prepared_timestamp.getID() << std::endl;

        already_read_prepared = true;
        }

        write->set_committed_value(row_string);
        std::cout << "Committed value is " << row_string << std::endl;
        //  write->set_allocated_committed_timestamp(TimestampMessage(committed_timestamp));
        std::cout << "Commit timestamp is " << committed_timestamp.getTimestamp()
                << ", " << committed_timestamp.getID() << std::endl;
    }
    // write->set_allocated_proof(traffic_cop_.commit_proof_->SerializeAsString());

    std::cout << "Result from query result builder is " << std::endl;
    std::cout << queryResultBuilder.get_result()->SerializeAsString()
                << std::endl;

    // return queryResultBuilder.get_result()->SerializeAsString();

    return;

}
        //Note: Could execute PointRead via ExecReadQuery (Eagerly) as well.
        // ExecPointRead should translate enc_primary_key into a query_statement to be exec by ExecReadQuery. 
        //(Alternatively: Could already send a Sql command from the client) ==> Should do it at the client, so that we can keep whatever Select specification, e.g. * or specific cols...

//Apply a set of Table Writes (versioned row creations) to the Table backend
void PelotonTableStore::ApplyTableWrite(const std::string &table_name, const TableWrite &table_write, const Timestamp &ts, const std::string &txn_digest, 
    const proto::CommittedProof *commit_proof, bool commit_or_prepare)
{

    std::cout << "In apply table write" << std::endl;

    if(table_write.rows().empty()) return;

    //Turn txn_digest into a shared_ptr, write everywhere it is needed.
    std::shared_ptr<std::string> txn_dig(std::make_shared<std::string>(txn_digest));

   std::string write_statement; //empty if no writes
   //std::string delete_statement; //empty if no deletes
   std::vector<std::string> delete_statements;
   sql_interpreter.GenerateTableWriteStatement(write_statement, delete_statements, table_name, table_write);
    //TODO: Check whether there is a more efficient way than creating SQL commands for each.

    //TODO: Execute on Peloton
    //Exec write
    //if(has_delete) Exec delete
    std::vector<peloton::ResultValue> result;
    std::vector<peloton::FieldInfo> tuple_descriptor;

    std::cout << "The write statement is: " << write_statement << std::endl;
    std::cout << "The delete statements are: " << std::endl;
    for(auto &delete_statement: delete_statements){
        std::cout << delete_statement << std::endl;
    }
    

    if (!write_statement.empty()) {
        // execute the query using tcop
        // prepareStatement
        // LOG_TRACE("Query: %s", query.c_str());
        auto &peloton_parser = peloton::parser::PostgresParser::GetInstance();
        auto sql_stmt_list = peloton_parser.BuildParseTree(write_statement);
        // PELOTON_ASSERT(sql_stmt_list);
        if (!sql_stmt_list->is_valid) {
        // return peloton::ResultType::FAILURE;
        }
        auto statement = traffic_cop_.PrepareStatement(
            unnamed_statement, write_statement, std::move(sql_stmt_list));
        if (statement.get() == nullptr) {
        traffic_cop_.setRowsAffected(0);
        // return peloton::ResultType::FAILURE;
        }
        // ExecuteStatment
        std::vector<peloton::type::Value> param_values;
        
        std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);
        // SetTrafficCopCounter();
        counter_.store(1);
        auto status = traffic_cop_.ExecuteWriteStatement(
            statement, param_values, unnamed, result_format, result, ts, txn_dig,
            commit_proof, commit_or_prepare);
        if (traffic_cop_.GetQueuing()) {
        ContinueAfterComplete(counter_);
        traffic_cop_.ExecuteStatementPlanGetResult();
        status = traffic_cop_.ExecuteStatementGetResult();
        traffic_cop_.SetQueuing(false);
        }
        if (status == peloton::ResultType::SUCCESS) {
        tuple_descriptor = statement->GetTupleDescriptor();
        }

        // TODO: Change Peloton result into query proto.
        sql::QueryResultProtoBuilder queryResultBuilder;
        // queryResultBuilder.add_column("result");
        // queryResultBuilder.add_row(result_row.begin(), result_row.end());
        std::cout << "Before adding columns" << std::endl;
        // Add columns
        for (unsigned int i = 0; i < tuple_descriptor.size(); i++) {
        std::string column_name = std::get<0>(tuple_descriptor[i]);
        queryResultBuilder.add_column(column_name);
        }
    }

    // execute the query using tcop
    // prepareStatement
    // LOG_TRACE("Query: %s", query.c_str());
    /*std::string unnamed_statement = "unnamed";
    auto &peloton_parser = peloton::parser::PostgresParser::GetInstance();
    auto sql_stmt_list = peloton_parser.BuildParseTree(write_statement);
    //PELOTON_ASSERT(sql_stmt_list);
    if (!sql_stmt_list->is_valid) {
    //return peloton::ResultType::FAILURE;
    }
    auto statement = traffic_cop_.PrepareStatement(unnamed_statement,
    write_statement, std::move(sql_stmt_list)); if (statement.get() == nullptr) {
            traffic_cop_.setRowsAffected(0);
            //return peloton::ResultType::FAILURE;
    }
    // ExecuteStatment
    std::vector<peloton::type::Value> param_values;
    
    std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);
    // SetTrafficCopCounter();
    counter_.store(1);
    auto status = traffic_cop_.ExecuteWriteStatement(statement, param_values,
    unnamed, result_format, result, ts, txn_dig, commit_proof, commit_or_prepare);
    if (traffic_cop_.GetQueuing()) {
            ContinueAfterComplete(counter_);
            traffic_cop_.ExecuteStatementPlanGetResult();
            status = traffic_cop_.ExecuteStatementGetResult();
            traffic_cop_.SetQueuing(false);
    }
    if (status == peloton::ResultType::SUCCESS) {
            tuple_descriptor = statement->GetTupleDescriptor();
    }

    //TODO: Change Peloton result into query proto.
    sql::QueryResultProtoBuilder queryResultBuilder;
    // queryResultBuilder.add_column("result");
    // queryResultBuilder.add_row(result_row.begin(), result_row.end());
    std::cout << "Before adding columns" << std::endl;
    // Add columns
    for (unsigned int i = 0; i < tuple_descriptor.size(); i++) {
            std::string column_name = std::get<0>(tuple_descriptor[i]);
            queryResultBuilder.add_column(column_name);
    }*/

    // TODO: Replace has_delete with !delete_statement.empty()
    // if(has_delete) Exec delete

    //if (!delete_statement.empty()) {
    for(auto &delete_statement: delete_statements){
        auto &peloton_parser = peloton::parser::PostgresParser::GetInstance();
        auto sql_stmt_list = peloton_parser.BuildParseTree(delete_statement);

        // PELOTON_ASSERT(sql_stmt_list);
        if (!sql_stmt_list->is_valid) {
        // return peloton::ResultType::FAILURE;
        }
        auto statement = traffic_cop_.PrepareStatement(
            unnamed_statement, delete_statement, std::move(sql_stmt_list));
        if (statement.get() == nullptr) {
        traffic_cop_.setRowsAffected(0);
        // return peloton::ResultType::FAILURE;
        }
        // ExecuteStatment
        std::vector<peloton::type::Value> param_values;
        param_values.clear();
        
        std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);
        // SetTrafficCopCounter();
        counter_.store(1);
        auto status = traffic_cop_.ExecuteWriteStatement(
            statement, param_values, unnamed, result_format, result, ts, txn_dig,
            commit_proof, commit_or_prepare);
        if (traffic_cop_.GetQueuing()) {
        ContinueAfterComplete(counter_);
        traffic_cop_.ExecuteStatementPlanGetResult();
        status = traffic_cop_.ExecuteStatementGetResult();
        traffic_cop_.SetQueuing(false);
        }
        if (status == peloton::ResultType::SUCCESS) {
        tuple_descriptor = statement->GetTupleDescriptor();
        }

        // TODO: add boolean to sometimes not force writes and deletes
        // Don't need to return anything

        // TODO: Change Peloton result into query proto.
        /*sql::QueryResultProtoBuilder queryResultBuilder1;
        // queryResultBuilder.add_column("result");
        // queryResultBuilder.add_row(result_row.begin(), result_row.end());
        std::cout << "Before adding columns" << std::endl;
        // Add columns
        for (unsigned int i = 0; i < tuple_descriptor.size(); i++) {
                std::string column_name = std::get<0>(tuple_descriptor[i]);
                queryResultBuilder1.add_column(column_name);
        }*/
    }


    //TODO: Confirm that ApplyTableWrite is synchronous -- i.e. only returns after all writes are applied. 
     //If not, then must call SetTableVersion as callback from within Peloton once it is done to set the TableVersion (Currently, it is being set right after ApplyTableWrite() returns)
}

void PelotonTableStore::PurgeTableWrite(const std::string &table_name, const TableWrite &table_write, const Timestamp &ts, const std::string &txn_digest){

    if(table_write.rows().empty()) return;

    std::shared_ptr<std::string> txn_dig(std::make_shared<std::string>(txn_digest));

    //std::string purge_statement; //empty if no writes/deletes (i.e. nothing to abort)
    std::vector<std::string> purge_statements;
    sql_interpreter.GenerateTablePurgeStatement(purge_statements, table_name, table_write);   

    //TODO: Purge statement is a "special" delete statement:
            // it deletes existing row insertions for the timestamp
            // but it also undoes existing deletes for the timestamp
        //Simple implementation: Check Versioned linked list and delete row with Timestamp ts. Return if ts > current
        //WARNING: ONLY Purge Rows/Tuples that are prepared. Do NOT purge committed ones.
        //Note: Since Delete does not impact Indexes no other changes are necessary.
                //Note: Purging Prepared Inserts will not clean up Index updates, i.e. the aborted transaction may leave behind a false positive index entry.
                        //Removing this false positive would require modifying the Peloton internals, so we will ignore this issue since it only affects efficiency and not results.
                        // I.e. a hit to the false positive will just result in a wasted lookup.

        //TODO: MUST delete even if not in index.  -- TODO: MUST ALSO DO THIS FOR NORMAL ABORT

    //==> Effectively it is "aborting" all suggested table writes.

    //TODO: Execute on Peloton
    bool has_purge = !purge_statements.empty();

    std::cout << "Has purge value is " << has_purge << std::endl;
    for(auto &purge_statement : purge_statements){
        std::cout << "Purge statement:" << purge_statement << std::endl;
    }
    

    if(!has_purge) return; //Nothing to undo.   //TODO: CONFIRM WITH NEIL THAT PURGE ALSO UNDOES DELETES

    std::vector<peloton::ResultValue> result;
    std::vector<peloton::FieldInfo> tuple_descriptor;

    for(auto &purge_statement: purge_statements){
        // execute the query using tcop
        // prepareStatement
        // LOG_TRACE("Query: %s", query.c_str());
        std::string unnamed_statement = "unnamed";
        auto &peloton_parser = peloton::parser::PostgresParser::GetInstance();
        auto sql_stmt_list = peloton_parser.BuildParseTree(purge_statement);
        // PELOTON_ASSERT(sql_stmt_list);

        auto statement = traffic_cop_.PrepareStatement(
            unnamed_statement, purge_statement, std::move(sql_stmt_list));

        if (statement.get() == nullptr) {
            traffic_cop_.setRowsAffected(0);
        }
        std::vector<peloton::type::Value> param_values;
        
        param_values.clear();
        std::vector<int> result_format(statement->GetTupleDescriptor().size(), 0);

        // SetTrafficCopCounter();
        counter_.store(1);
        pequinstore::proto::CommittedProof commit_proof;
        auto status = traffic_cop_.ExecutePurgeStatement(
            statement, param_values, unnamed, result_format, result, ts, txn_dig,
            has_purge);
        if (traffic_cop_.GetQueuing()) {
            ContinueAfterComplete(counter_);
            traffic_cop_.ExecuteStatementPlanGetResult();
            status = traffic_cop_.ExecuteStatementGetResult();
            traffic_cop_.SetQueuing(false);
        }
        if (status == peloton::ResultType::SUCCESS) {
            tuple_descriptor = statement->GetTupleDescriptor();
        }
    }
}
   

///////////////////// Snapshot Protocol Support

//Partially execute a read query statement (reconnaissance execution) and return the snapshot state (managed by ssMgr)
void PelotonTableStore::FindSnapshot(std::string &query_statement, const Timestamp &ts, SnapshotManager &ssMgr){

    //Generalize the PointRead interface:  
        //Read k latest prepared.
    //Use the same prepare predicate to determine whether to read or ignore a prepared version


    //TODO: Execute on Peloton
    //Note: Don't need to return a result
    //Note: Ideally execution is "partial" and only executes the leaf scan operations.
}

//Materialize a snapshot on the Table backend and execute on said snapshot.
void PelotonTableStore::MaterializeSnapshot(const std::string &query_retry_id, const proto::MergedSnapshot &merged_ss, const std::set<proto::Transaction*> &ss_txns){
    //Note: Not sure whether we should materialize full snapshot on demand, or continuously as we sync on Tx

    //TODO: Apply all txn in snapshot to Table backend as a "view" that is only visible to query_id
    //FIXME: The merged_ss argument only holds the txn_ids. --> instead, call Materialize Snapshot on a set of transactions... ==> if doing it continuously might need to call this function often.
} 

std::string PelotonTableStore::ExecReadOnSnapshot(const std::string &query_retry_id, std::string &query_statement, const Timestamp &ts, QueryReadSetMgr &readSetMgr, bool abort_early ){
    //TODO: Execute on the snapshot for query_id/retry_version

    //--> returns peloton result
    //TODO: Change peloton result into query proto:

     sql::QueryResultProtoBuilder queryResultBuilder;
    // queryResultBuilder.add_column("result");
    // queryResultBuilder.add_row(result_row.begin(), result_row.end());

    return queryResultBuilder.get_result()->SerializeAsString();

}



} // namespace pequinstore
