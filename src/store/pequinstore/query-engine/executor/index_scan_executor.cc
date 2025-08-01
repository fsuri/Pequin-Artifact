//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_scan_executor.cpp
//
// Identification: src/executor/index_scan_executor.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <string_view>
#include "../executor/index_scan_executor.h"

#include "../catalog/catalog.h"
#include "../catalog/manager.h"
#include "../common/container_tuple.h"
#include "../common/internal_types.h"
#include "../common/logger.h"
#include "../concurrency/transaction_manager_factory.h"
#include "../executor/executor_context.h"
#include "../executor/logical_tile.h"
#include "../executor/logical_tile_factory.h"
#include "../expression/abstract_expression.h"

#include "../expression/comparison_expression.h"
#include "../expression/conjunction_expression.h"
#include "../expression/constant_value_expression.h"

#include "../expression/tuple_value_expression.h"
#include "../index/index.h"
#include "../planner/index_scan_plan.h"
#include "../storage/data_table.h"
#include "../storage/masked_tuple.h"
#include "../storage/storage_manager.h"
#include "../storage/tile_group.h"
#include "../storage/tile_group_header.h"
#include "../type/value.h"
#include "lib/message.h"
#include "store/pequinstore/query-engine/common/item_pointer.h"
#include "store/common/table_kv_encoder.h"
#include "store/pequinstore/pequin-proto.pb.h"


namespace peloton {
namespace executor {

/**
 * @brief Constructor for indexscan executor.
 * @param node Indexscan node corresponding to this executor.
 */
IndexScanExecutor::IndexScanExecutor(const planner::AbstractPlan *node,
                                     ExecutorContext *executor_context)
    : AbstractScanExecutor(node, executor_context) {}

IndexScanExecutor::~IndexScanExecutor() {
  // Nothing to do here
}

/**
 * @brief Let base class Dinit() first, then do my job.
 * @return true on success, false otherwise.
 */
bool IndexScanExecutor::DInit() {
  auto status = AbstractScanExecutor::DInit();

  if (!status)
    return false;

  PELOTON_ASSERT(children_.size() == 0);

  // Grab info from plan node and check it
  const planner::IndexScanPlan &node = GetPlanNode<planner::IndexScanPlan>();

  result_itr_ = START_OID;
  result_.clear();
  done_ = false;
  key_ready_ = false;

  column_ids_ = node.GetColumnIds();
  key_column_ids_ = node.GetKeyColumnIds();
  expr_types_ = node.GetExprTypes();
  values_ = node.GetValues();
  runtime_keys_ = node.GetRunTimeKeys();
  predicate_ = node.GetPredicate();
  old_predicate_ = predicate_;

  updated_column_ids.clear();

  is_primary_index = false;
  already_added_table_col_versions = false;
  first_execution = true;
  is_implicit_point_read_ = false;

  left_open_ = node.GetLeftOpen();
  right_open_ = node.GetRightOpen();

  // This is for limit operation accelerate
  limit_ = node.GetLimit();
  limit_number_ = node.GetLimitNumber();
  limit_offset_ = node.GetLimitOffset();
  descend_ = node.GetDescend();

  if (runtime_keys_.size() != 0) {
    PELOTON_ASSERT(runtime_keys_.size() == values_.size());

    if (!key_ready_) {
      values_.clear();

      for (auto expr : runtime_keys_) {
        auto value = expr->Evaluate(nullptr, nullptr, executor_context_);
        LOG_TRACE("Evaluated runtime scan key: %s", value.GetInfo().c_str());
        values_.push_back(value.Copy());
      }

      key_ready_ = true;
    }
  }

  table_ = node.GetTable();

  is_metadata_table_ = table_->GetName().substr(0,3) == "pg_";


  lowest_snapshot_frontier = Timestamp(UINT64_MAX); //smallest prepared version that we skip during snapshot materialization.  //Update continously during read. At the end of exec, update TblVersion
  // Compute upper limit: So that in case the TblV is 0 (because we materialized nothing in snapshot) we pick something reasonable.
  //Timestamp upper_bound; //current highTS? we know no newer writes can be admitted. So the only thing we need to care about is the "danger zone" of prepared Tx that we skipped over
  //change how TblV is materialized from snapshot. Remove attempt to read from snapshot. Just pick min(lower_bound, upper_bound)

  // PAVLO (2017-01-05): This seems unnecessary and a waste of time
  if (table_ != nullptr) {
    full_column_ids_.resize(table_->GetSchema()->GetColumnCount());
    std::iota(full_column_ids_.begin(), full_column_ids_.end(), 0);
  }

  oid_t index_id = node.GetIndexId();
  index_ = table_->GetIndexWithOid(index_id);
  PELOTON_ASSERT(index_ != nullptr);

  // Then add the only conjunction predicate into the index predicate list
  // (at least for now we only supports single conjunction)
  //
  // Values that are left blank will be recorded for future binding
  // and their offset inside the value array will be remembered
  index_predicate_.AddConjunctionScanPredicate(index_.get(), values_,
                                               key_column_ids_, expr_types_);

  return true;
}


/**
 * @brief Creates logical tile(s) after scanning index.
 * @return true on success, false otherwise.
 */
bool IndexScanExecutor::DExecute() {
  LOG_TRACE("Index Scan executor :: 0 child");

  if (!done_) {
    //bool is_metadata_table_ = table_->GetName().substr(0,3) == "pg_"; //don't do any of the Pequin features for meta data tables..
    if(is_metadata_table_){
      Debug("Doing a META DATA secondary index scan for %s", table_->GetName().c_str());
      //auto status = ExecSecondaryIndexLookup_OLD();

      // struct timespec ts_start;
      // clock_gettime(CLOCK_MONOTONIC, &ts_start);
      // uint64_t microseconds_start = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;

      auto status = ExecSecondaryIndexLookup();

      //  struct timespec ts_end;
      // clock_gettime(CLOCK_MONOTONIC, &ts_end);
      // uint64_t microseconds_end = ts_end.tv_sec * 1000 * 1000 + ts_end.tv_nsec / 1000;
      // auto duration = microseconds_end - microseconds_start;
      // Warning("IndexExecutor Latency: %dus", duration);


      if (status == false)
        return false;
    }
    else if (index_->GetIndexType() == IndexConstraintType::PRIMARY_KEY) {
      Debug("Doing a primary index scan for table: %s", table_->GetName().c_str());
      is_primary_index = true;
      auto status = ExecPrimaryIndexLookup();
      if (status == false)
        return false;
    } else {
     
      Debug("Doing a secondary index scan for table: %s", table_->GetName().c_str());
     
      auto status = ExecSecondaryIndexLookup();
      if (status == false)
        return false;
    }
  }
  // Already performed the index lookup
  PELOTON_ASSERT(done_);

  while (result_itr_ < result_.size()) { // Avoid returning empty tiles
    if (result_[result_itr_]->GetTupleCount() == 0) {
      result_itr_++;
      continue;
    } else {
      LOG_TRACE("Information %s", result_[result_itr_]->GetInfo().c_str());
      SetOutput(result_[result_itr_]);
      result_itr_++;
      return true;
    }

  } // end while

  return false;
}



void IndexScanExecutor::GetColNames(const expression::AbstractExpression * child_expr, std::unordered_set<std::string> &column_names, bool use_updated) {
  //Get all Column Names in the WHERE clause. (both the already visible ones, and the replaced ones..)
  for (size_t i = 0; i < child_expr->GetChildrenSize(); i++) {
    auto child = child_expr->GetChild(i);
    if (dynamic_cast<const expression::TupleValueExpression*>(child) != nullptr) {
      auto tv_expr = dynamic_cast<const expression::TupleValueExpression*>(child);
      column_names.insert(tv_expr->GetColumnName());
      //std::cerr << "getting col info: " << tv_expr->GetInfo() << std::endl;
    }
    GetColNames(child, column_names, false);
  }

  if(use_updated){
    Debug("get N updated cols: %d", updated_column_ids.size());
    for(auto &col_id : updated_column_ids){
      Debug("adding updated colID: %d", col_id);
      column_names.insert(table_->GetSchema()->GetColumn(col_id).GetName());
    }
  }
}


void IndexScanExecutor::SetTableColVersions(concurrency::TransactionContext *current_txn, pequinstore::QueryReadSetMgr *query_read_set_mgr, const Timestamp &current_txn_timestamp){

  if(is_implicit_point_read_) return;
  if(already_added_table_col_versions) return;

  
    //bool is_metadata_table_ = table_->GetName().substr(0,3) == "pg_"; //don't do any of the Pequin features for meta data tables..
    //UW_ASSERT(!is_metadata_table_);
    if (!current_txn->IsPointRead() && current_txn->CheckPredicatesInitialized() && !is_metadata_table_) {
     Debug("Read Table/Col versions for TS:[%lu:%lu]", current_txn_timestamp.getTimestamp(), current_txn_timestamp.getID());

      //shorthands
      bool get_read_set = current_txn->GetHasReadSetMgr();
      bool find_snapshot = current_txn->GetHasSnapshotMgr();
      auto ss_mgr = current_txn->GetSnapshotMgr();
      bool perform_read_on_snapshot = current_txn->GetSnapshotRead();
      auto snapshot_set = current_txn->GetSnapshotSet();

      // Read table version and table col versions
      current_txn->GetTableVersion()(table_->GetName(), current_txn_timestamp, get_read_set, query_read_set_mgr, find_snapshot, ss_mgr, perform_read_on_snapshot, snapshot_set);
      
      //if(use_col_versions){
      if(catalog::Catalog::GetInstance()->GetQueryParams()->useColVersions){
        // Table column version. Note: Read version per Col, not composite key
        std::unordered_set<std::string> column_names;
        //std::vector<std::string> col_names;
        GetColNames(predicate_, column_names);
        if(predicate_ != nullptr) std::cerr << "pred: " << predicate_->GetInfo() << std::endl;

        for (auto &col : column_names) {
          Debug("Col name is: %s", col.c_str());
          current_txn->GetTableVersion()(EncodeTableCol(table_->GetName(), col), current_txn_timestamp, get_read_set, query_read_set_mgr, find_snapshot, ss_mgr, perform_read_on_snapshot, snapshot_set);
          //col_names.push_back(col);
        }
      }
    }
  
  already_added_table_col_versions = true;
}

void IndexScanExecutor::RefineTableColVersions(concurrency::TransactionContext *current_txn, pequinstore::QueryReadSetMgr *query_read_set_mgr){
  //return;
    //If we are in perform_read_on_snapshot mode: update TableVersion.
    if(!current_txn->GetSnapshotRead()) return;

    UW_ASSERT(current_txn->GetSqlInterpreter()); //should be set for Snapshot read
    auto query_params = current_txn->GetSqlInterpreter()->GetQueryParams();
   
    //if(query_params->useSemanticCC) 
    query_read_set_mgr->RefinePredicateTableVersion(lowest_snapshot_frontier, query_params->monotonicityGrace);
    //TODO: Profile how badly we update this
}

//TODO: Only call this in primary index key..

bool IndexScanExecutor::IsImplicitPointRead(concurrency::TransactionContext *current_txn){

  // Figure out on demand whether the predicate is essentially a point read (i.e. we are using primary index scan + fully subsumed)
      // In this case: don't add predicate, but add to read set (regardless of active or not)
      // Note: Must add to read set also if index scan finds nothing (.e.g. empty read)
      // Note: If index scan does find something, avoid duplicate inserts into read set.
      // Simply do the following: Set a bool to indicate read set must be recorded. 
      //         If index scan finds no row => insert into read set with TS 0
      //         If it does, but eval goes to false => insert into read set anyways (implicitly active)

  if(!is_primary_index) return false;

  if(current_txn->IsPointRead()) return false; //Don't need to do this for explicit point reads.

  auto sql_interpreter = current_txn->GetSqlInterpreter();

  

  is_implicit_point_read_ = sql_interpreter->IsPoint(predicate_->expr_name_, table_->GetName(), true);
  Debug("check Pred: %s. IsPoint: %d ", predicate_->expr_name_.c_str(), is_implicit_point_read_);
  //Note: We are currently using the relaxed = true argument
  //In this mode, we treat the predicate as a point read even if it is stronger than just the primary keys. E.g. say x is pkey, but predicate is x=5 AND name='xyz'
  //This means that we are giving up on incorporating the additional semantics of name=`xyz` for CC. The ActiveReadSet will throw a conflict if *anything* about the row changes
  //If instead we used the non-relaxed mode, then we would not treat the above stricter predicate as a point read, and would instead add it to the predicate set (for semanticCC)



  //std::string_view cond_statement(predicate_->expr_name_);
  // std::vector<std::string> p_col_values; //TODO: Sanity check that this is the same as values_
  // is_implicit_point_read_ = sql_interpreter->CheckColConditions(cond_statement, table_, p_col_values, true);

  // bool terminate_early = false;
  // size_t end = 0;
  // bool relax = true;

  // UW_ASSERT(sql_interpreter->GetTableRegistry_const()->count(table_->GetName()));
  // const auto &col_registry = sql_interpreter->GetTableRegistry_const()->at(table_->GetName());

  // std::map<std::string, std::string> p_col_value; //TODO: Sanity check that this is the same as values_
  // is_implicit_point_read_ = sql_interpreter->CheckColConditions(end, cond_statement, col_registry, p_col_value, terminate_early, relax);
  //Note: CheckColConditions is adjusted to handle/ignore reflexive preds 
  //TODO: Test that this works fine...
 
    //Alternatively: Manual check
    //a) are all primary columns present
        // std::unordered_set<std::string> column_names;
        // GetColNames(predicate_, column_names);

        // auto tableReg = current_txn->GetTableRegistry()->at(table_->GetName());

        // //Check if current predicate is fully subsumed by primary key
        // for(auto &pcol : tableReg.primary_key_cols){
        //   if(!column_names.count(pcol)){
        //     return;
        //   }
        // }

    //b) TODO: Check if all are conjuctors... //Check if ==..


  //FIXME: Only testing code
  // for(auto &val: values_){ //Note: values_ holds all values of the index_predicate (i.e. subset primary key ones)
  //   UW_ASSERT(val.GetTypeId() != type::TypeId::DECIMAL);
  //   std::cerr << "VALUE POINT: " << val.ToString() << std::endl;
  // }
  
  return is_implicit_point_read_;
}

void IndexScanExecutor::TryForceReadSetAddition(concurrency::TransactionContext *current_txn, pequinstore::QueryReadSetMgr *query_read_set_mgr, const Timestamp &time){
  //timestamp = 0 if not in index
  //timestamp = whatever curr if eval false.

  if(!is_implicit_point_read_) return;

  //Construct primary key encoding on demand. //Note: This function will only be called if predicate is an implicit point read.
                                              //E.g. a join causes us to call a primary index scan on a single row.
  std::vector<std::string> primary_key_cols;
  for(auto &val: values_){ //Note: values_ holds all values of the index_predicate (i.e. subset primary key ones)
    UW_ASSERT(val.GetTypeId() != type::TypeId::DECIMAL);
    primary_key_cols.push_back(val.ToString());
  }
  std::string &&encoded = EncodeTableRow(table_->GetName(), primary_key_cols);
  Debug("Force Read Set addition for encoded read set key: %s. Version: [%lu: %lu]", encoded.c_str(), time.getTimestamp(), time.getID());
  query_read_set_mgr->AddToReadSet(std::move(encoded), time);
}

void IndexScanExecutor::SetPredicate(concurrency::TransactionContext *current_txn, pequinstore::QueryReadSetMgr *query_read_set_mgr){
  
  if(!current_txn->GetHasReadSetMgr()) return;

  //bool is_metadata_table_ = table_->GetName().substr(0,3) == "pg_"; //don't do any of the Pequin features for meta data tables..
   
  if (current_txn->IsPointRead() || !current_txn->CheckPredicatesInitialized() || is_metadata_table_) return;

  //Don't set predicates if it is an implicit point read. In that case, try to add force read set addition
  if(is_implicit_point_read_) return;

  const_cast<peloton::expression::AbstractExpression *>(predicate_)->DeduceExpressionName(); //Note: must deduce before checking IsImplicitPoint

  if(first_execution){
     if(IsImplicitPointRead(current_txn)) return;

     /* Reserve a new predicate in the readset manager */
    query_read_set_mgr->AddPredicate(table_->GetName());
    first_execution = false;
  }

  //FIXME: Must copy because const... //Instead of deduce expression name here: do it much earlier?
    //  auto pred_copy = predicate_->Copy();
    // pred_copy->DeduceExpressionName();
  //const_cast<peloton::expression::AbstractExpression *>(predicate_)->DeduceExpressionName();
  auto &pred = predicate_->expr_name_;
  query_read_set_mgr->ExtendPredicate(pred);
  Debug("Adding new read set predicate instance: %s", pred.c_str());

  Debug("Num preds: %d", query_read_set_mgr->read_set->read_predicates_size());

  // // // Index predicate in string form
  //     /*std::string index_pred = "";
  //     for (int i = 0; i < values_.size(); i++) {
  //       std::string col_name = table_->GetSchema()->GetColumn(key_column_ids_[i]).GetName();
  //       std::string op = ExpressionTypeToString(expr_types_[i], true);
  //       std::string val = values_[i].ToString();

  //       index_pred = col_name + op + val + " AND ";
  //     }*/
  //   auto pred_copy = predicate_->Copy();
  //   pred_copy->DeduceExpressionName();
    
  //   std::string full_pred = "SELECT * FROM " + table_->GetName() + " WHERE " + pred_copy->expr_name_;
  //   // Truncate the last AND
  //   /*if (pred_copy->expr_name_.length() == 0) {
  //     full_pred = full_pred.substr(0, full_pred.length()-5);
  //   }*/
    
  //   query_read_set_mgr->ExtendPredicate(full_pred);
  //     std::cerr << "The readset predicate is " << full_pred << std::endl;
}

bool IndexScanExecutor::ExecPrimaryIndexLookup() {    
  PELOTON_ASSERT(!done_);
  Debug("Inside Index Scan Executor"); // std::cerr << "Inside index scan executor" << std::endl;
  //std::cerr << "Index predicate: " << index_predicate_.GetConjunctionList()[0].GetLowKey()->GetInfo() << std::endl;

  auto current_txn = executor_context_->GetTransaction();

  //if(current_txn->is_limit) Notice("Starting Scan");

  //////////////// Get TableVersion and TableColVersions    //These should be taken before the index lookup //
  auto query_read_set_mgr = current_txn->GetQueryReadSetMgr();
  auto const &current_txn_timestamp = current_txn->GetBasilTimestamp();

  //On FIRST executor iteration: Set TableVersion and Set Predicate
  SetPredicate(current_txn, query_read_set_mgr); // NOTE: MUST add predicate before index scan component; in case index scan doesn't find anything, we still need pred.
  SetTableColVersions(current_txn, query_read_set_mgr, current_txn_timestamp);
  //in case of read_from_snapshot:
    //TODO: Change SetTableColVersion: don't loop to genesis, pick min(materialized in snapshot, currentHighTS) as starting point
    //TODO: In case of snapshot. Also keep track of min prepared version we skipped over. (ignoring force mats)
    //TODO: At the end of exec: adjust table version to account for ignored prepares

  ///////////////////////////////////////////////////////////////////////////////////////////////////////////////

  std::vector<ItemPointer *> tuple_location_ptrs;

  //Notice("Curr pred: %s", predicate_->GetInfo().c_str());

  // Grab info from plan node
  bool acquire_owner = GetPlanNode<planner::AbstractScan>().IsForUpdate();

  PELOTON_ASSERT(index_->GetIndexType() == IndexConstraintType::PRIMARY_KEY);

  if (0 == key_column_ids_.size()) {
    // std::cerr << "Index executor scan all keys" << std::endl;
    index_->ScanAllKeys(tuple_location_ptrs);
  } else {
    // Limit clause accelerate
    if (limit_) {
      // invoke index scan limit
      if (!descend_) {
        LOG_TRACE("ASCENDING SCAN LIMIT in Primary Index");
        // std::cerr << "Index executor scan limit ascending" << std::endl;
        index_->ScanLimit(values_, key_column_ids_, expr_types_, ScanDirectionType::FORWARD, tuple_location_ptrs, &index_predicate_.GetConjunctionList()[0], limit_number_, limit_offset_);
      } else {
        LOG_TRACE("DESCENDING SCAN LIMIT in Primary Index");
        // std::cerr << "Index executor scan limit descending" << std::endl;
        index_->ScanLimit(values_, key_column_ids_, expr_types_, ScanDirectionType::BACKWARD, tuple_location_ptrs, &index_predicate_.GetConjunctionList()[0], limit_number_, limit_offset_);
        LOG_TRACE("1-Result size is %lu", result_.size());
      }
    }
    // Normal SQL (without limit)
    else {
      LOG_TRACE("Index Scan in Primary Index");
      // std::cerr << "Index executor scan all" << std::endl;
      index_->Scan(values_, key_column_ids_, expr_types_, ScanDirectionType::FORWARD, tuple_location_ptrs, &index_predicate_.GetConjunctionList()[0]);
    }
    LOG_TRACE("tuple_location_ptrs:%lu", tuple_location_ptrs.size());
  }

  if (tuple_location_ptrs.size() == 0) {
    // std::cerr << "No tuples retrieved in the index" << std::endl;
    LOG_TRACE("no tuple is retrieved from index.");
    Debug(" Found no matching rows in table: %s", table_->GetName().c_str());

    if(is_implicit_point_read_) TryForceReadSetAddition(current_txn, query_read_set_mgr, Timestamp(0));
    return false;
  }

  auto &transaction_manager = concurrency::TransactionManagerFactory::GetInstance();


  auto storage_manager = storage::StorageManager::GetInstance();
  std::vector<ItemPointer> visible_tuple_locations;
  // Add set for visible and prepared visible tuple locations to prevent duplicates
  std::set<ItemPointer> visible_tuple_set;
  
  // NEW: Commit proofs
  // std::vector<const pequinstore::proto::CommittedProof *> proofs;


  #ifdef LOG_TRACE_ENABLED
    int num_tuples_examined = 0;
  #endif

  Debug("Query[%lu:%lu]Primary Index Scan on Table [%s]. Number of rows to check: %d", current_txn->GetBasilTimestamp().getTimestamp(), current_txn->GetBasilTimestamp().getID(), table_->GetName().c_str(), tuple_location_ptrs.size());
  
  // if(tuple_location_ptrs.size() > 10) Warning("Potentially inefficient PRIMARY scan on table %s. Rows to check %d. Sanity check!", table_->GetName().c_str(), tuple_location_ptrs.size());
 
  if(current_txn->IsPointRead()) UW_ASSERT(tuple_location_ptrs.size() == 1);
  // for every tuple that is found in the index.
  //int cnt= 0;
  for (auto tuple_location_ptr : tuple_location_ptrs) {
    //std::cerr << "check row: " << cnt++ << std::endl;
    CheckRow(*tuple_location_ptr, transaction_manager, current_txn, storage_manager, visible_tuple_locations, visible_tuple_set);
  }

  RefineTableColVersions(current_txn, query_read_set_mgr); //Refine TableVersion if reading from materialized snapshot

  LOG_TRACE("Examined %d tuples from index %s", num_tuples_examined, index_->GetName().c_str());

  LOG_TRACE("%ld tuples before pruning boundaries", visible_tuple_locations.size());

  // Check whether the boundaries satisfy the required condition
  CheckOpenRangeWithReturnedTuples(visible_tuple_locations);

  LOG_TRACE("%ld tuples after pruning boundaries", visible_tuple_locations.size());

  // Add the tuple locations to the result vector in the order returned by the index scan. We might end up reading the same tile group multiple times. However, this is necessary to adhere to the ORDER BY clause
  oid_t current_tile_group_oid = INVALID_OID;
  std::vector<oid_t> tuples;


  for (auto &visible_tuple_location : visible_tuple_locations) {
    //Debug("Result contains location [%lu:%lu]", visible_tuple_location.block, visible_tuple_location.offset);
    if (current_tile_group_oid == INVALID_OID) {
      current_tile_group_oid = visible_tuple_location.block;
    }
    if (current_tile_group_oid == visible_tuple_location.block) {
      tuples.push_back(visible_tuple_location.offset);
    } else {
      // Since the tile_group_oids differ, fill in the current tile group into the result vector
      auto storage_manager = storage::StorageManager::GetInstance();
      auto tile_group = storage_manager->GetTileGroup(current_tile_group_oid);
      PrepareResult(tuples, tile_group);

      // Change the current_tile_group_oid and add the current tuple
      tuples.clear();
      current_tile_group_oid = visible_tuple_location.block;
      tuples.push_back(visible_tuple_location.offset);
    }
    //ManageReadSet(visible_tuple_location, current_txn, storage_manager);
  }

  // Add the remaining tuples to the result vector
  if ((current_tile_group_oid != INVALID_OID) && (!tuples.empty())) {
    auto tile_group = storage_manager->GetTileGroup(current_tile_group_oid);
    PrepareResult(tuples, tile_group);
  }

  done_ = true;

  LOG_TRACE("Result tiles : %lu", result_.size());

  return true;
}

void IndexScanExecutor::PrepareResult(std::vector<oid_t> &tuples, std::shared_ptr<storage::TileGroup> tile_group) {

    std::unique_ptr<LogicalTile> logical_tile(LogicalTileFactory::GetTile());
    // Add relevant columns to logical tile
    logical_tile->AddColumns(tile_group, full_column_ids_);
    logical_tile->AddPositionList(std::move(tuples));
    // Add prepared values and commit proofs to logical tile
    // logical_tile->AddPreparedValues(prepared_values[0]);
    // logical_tile->SetCommitProofs(proofs[0]);
    if (column_ids_.size() != 0) {
      logical_tile->ProjectColumns(full_column_ids_, column_ids_);
    }
    result_.push_back(logical_tile.release());
}


void IndexScanExecutor::ManageReadSet(ItemPointer &visible_tuple_location, concurrency::TransactionContext *current_txn,
    pequinstore::QueryReadSetMgr *query_read_set_mgr, storage::StorageManager *storage_manager) {
  
  //Don't create a read set if it's a point query, or the query is executed in snapshot only mode
  // Or if it is on a metadata table
   //bool is_metadata_table_ = table_->GetName().substr(0,3) == "pg_"; //don't do any of the Pequin features for meta data tables..
   //UW_ASSERT(!is_metadata_table_);
  if (current_txn->GetHasReadSetMgr() && !is_metadata_table_) {
    auto const &primary_index_columns_ = index_->GetMetadata()->GetKeyAttrs();
    auto query_read_set_mgr = current_txn->GetQueryReadSetMgr();

    auto tile_group = storage_manager->GetTileGroup(visible_tuple_location.block);
    auto tile_group_header = tile_group->GetHeader();

    ManageReadSet(visible_tuple_location, tile_group, tile_group_header, current_txn);
  }
}

void IndexScanExecutor::ManageReadSet(ItemPointer &tuple_location, std::shared_ptr<storage::TileGroup> tile_group, storage::TileGroupHeader *tile_group_header, 
    concurrency::TransactionContext *current_txn) {
  
  //bool is_metadata_table_ = table_->GetName().substr(0,3) == "pg_";
  //Don't create a read set if it's a point query, or the query is executed in snapshot only mode
  if (is_metadata_table_) return;

  //Don't create a read set if it's a point query, or the query is executed in snapshot only mode
  if (current_txn->GetHasReadSetMgr()) {

    auto const &primary_index_columns_ = index_->GetMetadata()->GetKeyAttrs();
    auto query_read_set_mgr = current_txn->GetQueryReadSetMgr();

    ContainerTuple<storage::TileGroup> row(tile_group.get(), tuple_location.offset);

    std::vector<std::string> primary_key_cols;
    for (auto const &col_idx : current_txn->GetTableRegistry()->at(table_->GetName()).primary_col_idx) {
    //for (auto const &col_idx : primary_index_columns_) { //These are not the right primary key cols. They may be secondary index cols...
      try{
      auto const &val = row.GetValue(col_idx);
     
      UW_ASSERT(val.GetTypeId() != type::TypeId::DECIMAL); //FIXME: We need to get a non-decimal encoding. Results like 1.3e+12 are not useable by us. (or client needs to do this as well)
      primary_key_cols.push_back(val.ToString());
      Debug("Read set. Primary col %d has value: %s", col_idx, val.ToString().c_str());
      //std::cerr << "primary col: " << col_idx << std::endl;
       }
      catch(...){Panic("Fail in Read");}
    }


    const Timestamp &time = tile_group_header->GetBasilTimestamp(tuple_location.offset);
    std::string &&encoded = EncodeTableRow(table_->GetName(), primary_key_cols);
    Debug("Tuple location[%lu:%lu] encoded read set key is: %s. Version: [%lu: %lu]",tuple_location.block, tuple_location.offset, encoded.c_str(), time.getTimestamp(), time.getID());

    //if(current_txn->is_limit) Notice("Next key that is read for limit txn: %s", encoded.c_str());

    //If prepared: Additionally set Dependency
    if (!tile_group_header->GetCommitOrPrepare(tuple_location.offset)) {
      //Notice("Adding Dep: %s. Tuple location[%lu:%lu] encoded read set key is: %s. Version: [%lu: %lu]", pequinstore::BytesToHex(*tile_group_header->GetTxnDig(tuple_location.offset), 16).c_str(), tuple_location.block, tuple_location.offset, encoded.c_str(), time.getTimestamp(), time.getID());
      if (tile_group_header->GetTxnDig(tuple_location.offset) == nullptr) Panic("Dep Digest is null");
      query_read_set_mgr->AddToDepSet(*tile_group_header->GetTxnDig(tuple_location.offset), time);
    }

    query_read_set_mgr->AddToReadSet(std::move(encoded), time);
  }
}


void IndexScanExecutor::CheckRow(ItemPointer tuple_location, concurrency::TransactionManager &transaction_manager, concurrency::TransactionContext *current_txn, storage::StorageManager *storage_manager, 
  std::vector<ItemPointer> &visible_tuple_locations, std::set<ItemPointer> &visible_tuple_set, 
  bool use_secondary_index)
{

#ifdef LOG_TRACE_ENABLED
    num_tuples_examined++;
#endif

  // std::cerr << "Index executor inside for loop" << std::endl;
    auto tile_group = storage_manager->GetTileGroup(tuple_location.block);
    auto tile_group_header = tile_group.get()->GetHeader();
   
    // the following code traverses the version chain until a certain visible version is found. we should always find a visible version from a version chain.
    // NOTE: Similar read logic as seq_scan_executor
    auto const &timestamp = current_txn->GetBasilTimestamp();
    Debug("Check next row. Current Txn TS: [%lu, %lu]", timestamp.getTimestamp(), timestamp.getID());
    //if(timestamp.getID() == 0) Notice("Check next row. Current Txn TS: [%lu, %lu]", timestamp.getTimestamp(), timestamp.getID());
   
    // Get the head of the version chain (latest version)
    // tile_group_header->GetSpinLatch(tuple_location.offset).Lock(); //TODO//FIXME: This latch is useless. 
    ItemPointer *head = tile_group_header->GetIndirection(tuple_location.offset);

    //Note for Primary index scan: Since we are not holding a lock during reads, the tuple_location might not be the head even for primary index scan. We are simply "correcting" to the header if possible.
    //For Secondary index scan it is natural that the tuple location is not necessarily the head.

    if (head == nullptr) {
      std::cerr << "Head is null and location of curr tuple is (" << tuple_location.block << ", " << tuple_location.offset << ")" << std::endl;
      if(!(*head == tuple_location)) Panic("Head of linked list == nullptr; but current tuple location isnt. How is this possible");
      Panic("Head of linked list == nullptr...");
      return;
    }

    auto head_tile_group = storage_manager->GetTileGroup(head->block);
    auto head_tile_group_header = head_tile_group.get()->GetHeader();

    auto tuple_timestamp = head_tile_group_header->GetBasilTimestamp(head->offset);
    tuple_location = *head;
    tile_group_header = head_tile_group_header;

    // tile_group_header->GetSpinLatch(tuple_location.offset).Unlock();
    // auto curr_tuple_id = location.offset;

    //std::cerr << "Head timestamp is " << tuple_timestamp.getTimestamp() << ", " << tuple_timestamp.getID() << std::endl;

    //Find the Right Row Version to read
    bool done = false;

    bool found_committed = false;
    bool found_prepared = false;

    //If in snapshot only mode don't need to produce a result. Note: if doing pointQuery DO want the result => FOR NESTED LOOP JOIN WE NEED RESULT
    //bool snapshot_only_mode = false; 
    bool snapshot_only_mode = !current_txn->GetHasReadSetMgr() && current_txn->GetHasSnapshotMgr() && !current_txn->IsNLJoin(); 
  
       
    //Iterate through linked list, from newest to oldest version   
    size_t chain_length = 0;
    size_t num_iters = 0;

    int max_num_reads = current_txn->IsPointRead()? 2 : 1; 
    int num_reads = 0;

    //fprintf(stderr, "First tuple in row: Looking at Tuple at location [%lu:%lu] with TS: [%lu:%lu] \n", tuple_location.block, tuple_location.offset, tuple_timestamp.getTimestamp(), tuple_timestamp.getID());
      
    //bool is_metadata_table_ = table_->GetName().substr(0,3) == "pg_"; //don't do any of the Pequin features for meta data tables..

    while(!done){
      ++chain_length;

      auto tuple_timestamp = tile_group_header->GetBasilTimestamp(tuple_location.offset);
      Debug("Looking at Tuple at location [%lu:%lu] with TS: [%lu:%lu]", tuple_location.block, tuple_location.offset, tuple_timestamp.getTimestamp(), tuple_timestamp.getID());

      bool readable_version = !tile_group_header->IsPurged(tuple_location.offset); //skip over purged versions.

      if (readable_version && (timestamp >= tuple_timestamp || is_metadata_table_)) {
        // Within range of timestamp
        bool read_curr_version = false;
        done = FindRightRowVersion(timestamp, tile_group, tile_group_header, tuple_location, num_iters, current_txn, read_curr_version, found_committed, found_prepared); 

        UW_ASSERT(!is_metadata_table_ || read_curr_version);
        //Eval should be called on the latest readable version. Note: For point reads we will call this up to twice (for prepared & committed)
        if((read_curr_version && ++num_reads <= max_num_reads ) && (!snapshot_only_mode || is_metadata_table_)){
          //std::cerr << "num_reads: " << num_reads << std::endl;
          UW_ASSERT(num_reads <= max_num_reads); //Assert we are not reading more than 1 for scans, and no more than 2 for point
          EvalRead(tile_group, tile_group_header, tuple_location, visible_tuple_locations, current_txn, visible_tuple_set, use_secondary_index);  //TODO: might be more elegant to move this into FindRightRowVersion
        }
      
        //If we are in read_from_snapshot mode, and we skip a read (i.e. done = false), update the min_snapshot_frontier. Note: Ignore tuples that a force materialized (they are effectively invisible)
        if(!done && current_txn->GetSnapshotRead()){ //bool perform_read_on_snapshot = true
          UW_ASSERT(!read_curr_version);
          UW_ASSERT(!found_committed && !found_prepared); // in read_on_snapshot mode done should be true as soon as found_prepared or found_committed becomes true.
          if(!tile_group_header->GetMaterialize(tuple_location.offset)){
              Debug("Skipping [txn: %s]. [%lu:%lu]", pequinstore::BytesToHex(*tile_group_header->GetTxnDig(tuple_location.offset), 16).c_str(), tuple_location.block, tuple_location.offset);
              Timestamp const &skipped_timestamp = tile_group_header->GetBasilTimestamp(tuple_location.offset);
              lowest_snapshot_frontier = std::min(lowest_snapshot_frontier, skipped_timestamp);
          }
        }
      }

      if(done) break;

      ItemPointer old_item = tuple_location;
      // std::cerr << "Offset is " << old_item.offset << std::endl;
      //fprintf(stderr, "Curr Tuple location [%lu:%lu]\n", tuple_location.block, tuple_location.offset);
      tuple_location = tile_group_header->GetNextItemPointer(old_item.offset);
      //fprintf(stderr, "Next Tuple location [%lu:%lu]\n", tuple_location.block, tuple_location.offset);
      if (tuple_location.IsNull()) {
        //std::cerr << "Tuple location is null" << std::endl;
        done = true;
        break;
      }
      tile_group = storage_manager->GetTileGroup(tuple_location.block);
      tile_group_header = tile_group->GetHeader();

      
    }

    if(current_txn->is_customer_read){
        if(!found_committed){
          Warning("Point read to customer didn't find committed. Looped through %d tuples. Print out all tuples in linked list. Query TS: [%lu:%lu]", chain_length, timestamp.getTimestamp(), timestamp.getID());
          //Loop again from start loc and print more. 
          //TODO: Fix start point.
          
            tuple_location = *head;
            tile_group_header = head_tile_group_header;
           
            while(true){
              ++chain_length;

              auto tuple_timestamp = tile_group_header->GetBasilTimestamp(tuple_location.offset);
             
              bool readable_version = !tile_group_header->IsPurged(tuple_location.offset); //skip over purged versions.

              Warning("Looking at Tuple at location [%lu:%lu] with TS: [%lu:%lu]. Readable? %d. Committed: %d", tuple_location.block, tuple_location.offset, tuple_timestamp.getTimestamp(), tuple_timestamp.getID(), 
                      readable_version, tile_group_header->GetCommitOrPrepare(tuple_location.offset));
             
              ItemPointer old_item = tuple_location;
              tuple_location = tile_group_header->GetNextItemPointer(old_item.offset);
              if (tuple_location.IsNull()) {
                break;
              }
              tile_group = storage_manager->GetTileGroup(tuple_location.block);
              tile_group_header = tile_group->GetHeader();

            }
          Panic("Point read to customer failed");
        }

        UW_ASSERT(found_committed); // point reads to customer table MUST find a committed value.. genesis.
    } 

    LOG_TRACE("Traverse length: %d\n", (int)chain_length);
}

//static bool USE_ACTIVE_READ_SET = true; //If true, then Must use Table_Col_Version
static bool USE_ACTIVE_SNAPSHOT_SET = false; //Currently, our Snapshots are always Complete (non-Active) "as they can be". Note: Index scan already makes the readable set somewhat Active

bool IndexScanExecutor::FindRightRowVersion(const Timestamp &timestamp, std::shared_ptr<storage::TileGroup> tile_group, storage::TileGroupHeader *tile_group_header, ItemPointer tuple_location,
    size_t &num_iters, concurrency::TransactionContext *current_txn, 
    bool &read_curr_version, bool &found_committed, bool &found_prepared)
{
    /////////////////////////////// PESTO MODIFIERS  -- these are just aliases for convenience  ///////////////////////////////////
    bool perform_find_snapshot = current_txn->GetHasSnapshotMgr();
    auto snapshot_mgr = current_txn->GetSnapshotMgr();
    //size_t k_prepared_versions = current_txn->GetKPreparedVersions();

    //Note: For find snapshot only mode we also want to read and produce a result (necessary for nested joins), but we will not record a readset
    bool perform_read = current_txn->GetHasReadSetMgr() || (perform_find_snapshot && !current_txn->IsNLJoin()); 
    bool perform_point_read = current_txn->IsPointRead();
  
    bool perform_read_on_snapshot = current_txn->GetSnapshotRead();
    //Force Read to only check from snapshot. UNSAFE: No way to know when to "stop"; TODO: Add bounding code: Skip over freshest commit for up to i iterations; if find nothing in snapshot, use freshest commit.
    //if(perform_read_on_snapshot && current_txn->GetSqlInterpreter()->GetQueryParams()->forceReadFromSnapshot) perform_read = false; 
    auto snapshot_set = current_txn->GetSnapshotSet();
    UW_ASSERT(!perform_read_on_snapshot || perform_read); //if read on snapshot, must be performing read.
    UW_ASSERT(!perform_find_snapshot || !current_txn->IsPointRead()); //if finding snapshot, cant be a point read.
    UW_ASSERT(!(perform_find_snapshot && perform_read_on_snapshot));
  
   bool write_mode = !perform_read && !perform_find_snapshot && !perform_read_on_snapshot; //If scanning as part of a write

  Debug("Perform read? %d. Point read? %d. Perform find snapshot? %d. Perform read_on_snapshot? %d. Write mode? %d", 
          perform_read, perform_point_read, perform_find_snapshot, perform_read_on_snapshot, write_mode);
   ///////////////////////////////////////////////////////////////////////////////////
  UW_ASSERT(!(perform_find_snapshot && perform_read_on_snapshot)); //shouldn't do both simultaneously currently. Though we could support it in theory.

  auto txn_digest = tile_group_header->GetTxnDig(tuple_location.offset);
  bool done = false;

  Debug("Tuple at location [%lu:%lu] with commit state: %d.", tuple_location.block, tuple_location.offset, tile_group_header->GetCommitOrPrepare(tuple_location.offset));

  // If reading.
  if (perform_read || perform_point_read || write_mode ) {
    // Three cases: Tuple is committed, or prepared. If prepared, it might have been forceMaterialized
    
    // If the tuple is committed read the version.    Try to read committed always -- whether it is eager or snapshot_read
    if (tile_group_header->GetCommitOrPrepare(tuple_location.offset)) {
      found_committed = true;
      read_curr_version = true;
      done = true;

      Timestamp const &committed_timestamp = tile_group_header->GetBasilTimestamp(tuple_location.offset);
      Debug("Read Committed Timestamp: [%lu:%lu]", committed_timestamp.getTimestamp(), committed_timestamp.getID());
      if (perform_point_read) SetPointRead(current_txn, tile_group_header, tuple_location, committed_timestamp);


      //Just for snapshot debugging purposes
      if(false && perform_read_on_snapshot && snapshot_set->find(*txn_digest.get()) == snapshot_set->end()){
          ContainerTuple<storage::TileGroup> row(tile_group.get(), tuple_location.offset);

          std::vector<std::string> primary_key_cols;
          for (auto const &col_idx : current_txn->GetTableRegistry()->at(table_->GetName()).primary_col_idx) {
          //for (auto const &col_idx : primary_index_columns_) { //These are not the right primary key cols. They may be secondary index cols...
            try{
            auto const &val = row.GetValue(col_idx);
          
            UW_ASSERT(val.GetTypeId() != type::TypeId::DECIMAL); //FIXME: We need to get a non-decimal encoding. Results like 1.3e+12 are not useable by us. (or client needs to do this as well)
            primary_key_cols.push_back(val.ToString());
            Debug("Read set. Primary col %d has value: %s", col_idx, val.ToString().c_str());
            //std::cerr << "primary col: " << col_idx << std::endl;
            }
            catch(...){Panic("Fail in Read");}
          }

          std::string &&encoded = EncodeTableRow(table_->GetName(), primary_key_cols);
          Notice("Read Committed Timestamp instead of reading from Snapshot: key: %s [%lu:%lu]. txn_id: %s", encoded.c_str(), committed_timestamp.getTimestamp(), committed_timestamp.getID(), 
            pequinstore::BytesToHex(*txn_digest, 16).c_str());

          for(const auto &[tx_id, replica_list]: *snapshot_set){
            Notice("Snapshot contains: %s",pequinstore::BytesToHex(tx_id, 16).c_str());
          }
      }

    } 
    else if(!found_prepared) { //tuple is prepared (and we waven't read a prepared one yet)     Note: Technically in write mode always read the prepare. In practice, loader will never come here?
  
      if(write_mode && current_txn->IsDeletion()){ //Special handling to allow deletes to upgrade from prepare to commit; and to observe other delete rows.
       //TODO: Just re-factor delete to also use Insert interface... (just write dummy values...) Then no scan is necessary.
        Timestamp const &prepared_timestamp = tile_group_header->GetBasilTimestamp(tuple_location.offset);
        bool tuple_is_delete = tile_group_header->IsDeleted(tuple_location.offset);
        if(tuple_is_delete && prepared_timestamp == current_txn->GetBasilTimestamp() && current_txn->GetCommitOrPrepare()){  //upgrade case
            //fprintf(stderr, "Upgrade tuple[%lu:%lu], TS[%lu:%lu] from prepare to commit", tuple_location.block, tuple_location.offset, current_txn->GetBasilTimestamp().getTimestamp(), current_txn->GetBasilTimestamp().getID());
            Debug("Upgrade tuple[%lu:%lu], TS[%lu:%lu] from prepare to commit", tuple_location.block, tuple_location.offset, current_txn->GetBasilTimestamp().getTimestamp(), current_txn->GetBasilTimestamp().getID());
            //this upgrades prepared version (with same TS) to commit, 
            const pequinstore::proto::CommittedProof *proof =  current_txn->GetCommittedProof();
            UW_ASSERT(proof);
            tile_group_header->SetCommittedProof(tuple_location.offset, proof);
            tile_group_header->SetCommitOrPrepare(tuple_location.offset, true);
            tile_group_header->SetMaterialize(tuple_location.offset, false);
             read_curr_version = false; //No need to read this. Won't pass Eval check anyways!
        }
        else{ //return as part of result (delete_executor will create new tuple version)
          read_curr_version = true;
        }
        found_prepared = true;
        //Only "read" the first prepared version as part of result
        done = true;
        return done;
      }
      else if (!perform_read_on_snapshot){   //if doing eager read
         // Don't read materialized 
        if(tile_group_header->GetMaterialize(tuple_location.offset)) {
          Debug("Don't read force materialized, continue reading");
          //Panic("Nothing should be force Materialized in current test");
          read_curr_version = false;
          done = false;
          return done;
        }

        auto const &read_prepared_pred = current_txn->GetReadPreparedPred();
        Timestamp const &prepared_timestamp = tile_group_header->GetBasilTimestamp(tuple_location.offset);

        // Only read prepared tuples if it satisfies the prepared predicate
        if (num_iters == 0 && read_prepared_pred && read_prepared_pred(*txn_digest)) {
          found_prepared = true;
          //Only "read" the first prepared version as part of result
          read_curr_version = true;
          Debug("Read Prepared Timestamp: [%lu:%lu]. Dep: %s", prepared_timestamp.getTimestamp(), prepared_timestamp.getID(), pequinstore::BytesToHex(*txn_digest, 16).c_str());
          if (perform_point_read) SetPointRead(current_txn, tile_group_header, tuple_location, prepared_timestamp);
        }
        //if we are performing snapshot, continue scanning up to k versions. Otherwise, for normal read, stop reading. For PointRead, keep going until commit. (but read at most one prepared)
        if(!perform_find_snapshot && !perform_point_read) done = true;
      }
    }
  }

  // If calling read from snapshot 
  if (perform_read_on_snapshot) {
    Debug("Performing read on snapshot. Trying to read prepared Txn");
    //Note: Read committed already handled by normal read case above.
    // Read regardless if it's prepared or force materialized as long as it's in the snapshot
    bool should_read_from_snapshot = !found_committed && snapshot_set->find(*txn_digest.get()) != snapshot_set->end();

    if (should_read_from_snapshot) {
      Debug("TxnDig[%s] is part of snapshot. Read prepared tuple", pequinstore::BytesToHex(*txn_digest, 16).c_str());
      found_prepared = true;
      read_curr_version = true;
      done = true;
    }
  }

    // If calling find snapshot in table store interface
  if (perform_find_snapshot) {
    // Two cases: tuple is either prepared or committed
    if (tile_group_header->GetCommitOrPrepare(tuple_location.offset)) {
      found_committed = true;
      //read_curr_version = true; should not be reading for result if in snapshot only mode (let read mode account for reading)

      Timestamp const &committed_timestamp = tile_group_header->GetBasilTimestamp(tuple_location.offset);
      ManageSnapshot(current_txn, tile_group_header, tuple_location, committed_timestamp, num_iters, true);
      Debug("Add committed to Snapshot");
      done = true;
    } else {
      //Note: this check is technically redundant: FindSnapshot currently also always causes PerformRead to be triggered.
      if(tile_group_header->GetMaterialize(tuple_location.offset)) { //Ignore
          Debug("Don't add force materialized to snapshot, continue reading"); //Panic("Nothing should be force Materialized in current test");
          return done;
      }

      auto const &read_prepared_pred = current_txn->GetReadPreparedPred();
      Timestamp const &prepared_timestamp = tile_group_header->GetBasilTimestamp(tuple_location.offset);
      // Add to snapshot if tuple satisfies read prepared predicate and haven't read more than k versions
      bool should_add_to_snapshot = !found_committed && num_iters < current_txn->GetKPreparedVersions() && read_prepared_pred && read_prepared_pred(*txn_digest);

      if (should_add_to_snapshot) {
        //currently only count "readable" (meets predicate) versions towards snapshotK. This is so that we read at lest one committed if there is >= k unreadable prepared
         ManageSnapshot(current_txn, tile_group_header, tuple_location, prepared_timestamp, num_iters, false);
        //ManageSnapshot(tile_group_header, tuple_location, prepared_timestamp, snapshot_mgr);
        //num_iters++;
        Debug("add prepared to snapshot. Num_iters: %d", num_iters);
      }
      else{
        Debug("skip adding prepared to snapshot");
      }

      // We are done if we read k prepared versions
      done = num_iters >= current_txn->GetKPreparedVersions();
    }
    // std::cerr << "Num_iters: " << num_iters << std::endl;
    // std::cerr << "Max_Prepared_versions: " << current_txn->GetKPreparedVersions() << std::endl;
  }

  //Current tuple was not readable.
    Debug("Found committed: %d. Found prepared: %d, Can Read Prepared: %d, GetCommitOrPrepare: %d, GetMaterialize: %d", found_committed, found_prepared, current_txn->CanReadPrepared(), 
          tile_group_header->GetCommitOrPrepare(tuple_location.offset), tile_group_header->GetMaterialize(tuple_location.offset));

  // if(found_committed){
  //   std::cerr << "found committed SPECIAL PRINT" << std::endl;
  //     //Code to print result: TESTCODE
  //  auto storage_manager = storage::StorageManager::GetInstance();
  //   auto curr_tile_group = storage_manager->GetTileGroup(tuple_location.block);
  //  const auto *schema = curr_tile_group->GetAbstractTable()->GetSchema();
  //   oid_t current_tile_group_oid = tuple_location.block;
    
  //   auto tile_group = storage_manager->GetTileGroup(current_tile_group_oid);
  //   ContainerTuple<storage::TileGroup> tuple(tile_group.get(), tuple_location.offset);     
  //   for (uint32_t col_idx = 0; col_idx < schema->GetColumnCount(); col_idx++) {
  //       auto val = tuple.GetValue(col_idx);
  //       Debug("Read col %d. Value: %s", col_idx, val.ToString().c_str());  
  //   }
  // }

  return done;

}

//TODO: Re-factor so this is nicely "per mode" for better readability. Like in SeqScanExecutor.
bool IndexScanExecutor::FindRightRowVersion_Old(const Timestamp &timestamp, std::shared_ptr<storage::TileGroup> tile_group, storage::TileGroupHeader *tile_group_header, ItemPointer tuple_location,
    size_t &num_iters, concurrency::TransactionContext *current_txn, 
    bool &read_curr_version, bool &found_committed, bool &found_prepared)
{
    /////////////////////////////// PESTO MODIFIERS  -- these are just aliases for convenience  ///////////////////////////////////
    bool perform_read = current_txn->GetHasReadSetMgr();
    
    bool perform_find_snapshot = current_txn->GetHasSnapshotMgr();
    auto snapshot_mgr = current_txn->GetSnapshotMgr();
    //size_t k_prepared_versions = current_txn->GetKPreparedVersions();
  
    bool perform_read_on_snapshot = current_txn->GetSnapshotRead();
    auto snapshot_set = current_txn->GetSnapshotSet();

  Debug("Perform read? %d. Perform find snapshot? %d. Perform read_on_snapshot? %d", perform_read, perform_find_snapshot, perform_read_on_snapshot);
   ///////////////////////////////////////////////////////////////////////////////////

  UW_ASSERT(!(perform_find_snapshot && perform_read_on_snapshot)); //shouldn't do both simultaneously currently. Though we could support it in theory.

  Debug("Tuple commit state: %d.", tile_group_header->GetCommitOrPrepare(tuple_location.offset));
    //Debug("Tuple commit state: %d. Is tuple in visibility set (already processed)? %d", tile_group_header->GetCommitOrPrepare(tuple_location.offset), (visible_tuple_set.find(tuple_location) == visible_tuple_set.end()));

    //CASE 1:  The tuple is committed
    if (tile_group_header->GetCommitOrPrepare(tuple_location.offset)) { // && !visible_tuple_set.count(tuple_location)) {

      // Set boolean flag found_committed to true
      found_committed = true;
      Debug("Found committed tuple");

      read_curr_version = true; //Try reading from this value

      // Set the committed timestmp
      Timestamp const &committed_timestamp = tile_group_header->GetBasilTimestamp(tuple_location.offset);
      Debug("Committed Timestamp: [%lu:%lu]", committed_timestamp.getTimestamp(), committed_timestamp.getID());

      if (perform_find_snapshot) ManageSnapshot(current_txn, tile_group_header, tuple_location, committed_timestamp, num_iters, true);
      
      if (current_txn->IsPointRead()) SetPointRead(current_txn, tile_group_header, tuple_location, committed_timestamp);
  
      return true; // Since tuple is committed we can stop looking at the version chain
    }

    //CASE 2: Tuple is prepared (or forceMaterialized)

    //CASE 2a: For reads on snapshot: Read regardless of whether it is prepared or force_materialized
    else if (perform_read_on_snapshot && !found_committed) {
      Debug("Performing read on snapshot. Trying to read prepared Txn");
      auto txn_dig = tile_group_header->GetTxnDig(tuple_location.offset);
      bool should_read = snapshot_set->find(*txn_dig.get()) != snapshot_set->end();

      if (should_read) {
        Debug("TxnDig[%s] is part of snapshot. Read prepared tuple", pequinstore::BytesToHex(*txn_dig, 16).c_str());
        found_prepared = true;
        read_curr_version = true;
        return true;
      }
    }


    //CASE 2b: Tuple is prepared, Finding Snapshot
    // If finding a snapshot then read up to k prepared values (but don't add them to the result) -- Note, the first prepared value is handled by normal prepare process below.
    else if (perform_find_snapshot && num_iters < current_txn->GetKPreparedVersions() && !found_committed && found_prepared && !tile_group_header->GetCommitOrPrepare(tuple_location.offset)) {
        
        auto const &read_prepared_pred = current_txn->GetReadPreparedPred();
        if (read_prepared_pred) {
          auto txn_digest = tile_group_header->GetTxnDig(tuple_location.offset);
          UW_ASSERT(txn_digest);
          if (read_prepared_pred(*txn_digest)){
            Timestamp const &prepared_timestamp = tile_group_header->GetBasilTimestamp(tuple_location.offset);
            ManageSnapshot(current_txn, tile_group_header, tuple_location, prepared_timestamp, num_iters, false);
          }
        }
        
        return false;
    }

    //CASE 2c: Read first prepared tuple (either for normal exec, or first of snapshot)
    else if (!found_committed && !found_prepared && /*current_txn->CanReadPrepared() &&*/ !tile_group_header->GetCommitOrPrepare( tuple_location.offset)) {
     
      //do not read forcefully materialized values
      if(tile_group_header->GetMaterialize(tuple_location.offset)){
        Debug("dont read force materialized versions for queries that are not reading from snapshot. TxnDig[%s] is forceMaterialized. Continue reading", 
                pequinstore::BytesToHex(*tile_group_header->GetTxnDig(tuple_location.offset), 16).c_str());
        return false;
      } 

      //check to see if tuple satisfies predicate
      auto const &read_prepared_pred = current_txn->GetReadPreparedPred();
      if (read_prepared_pred) {
        auto txn_digest = tile_group_header->GetTxnDig(tuple_location.offset);
         Debug("Checking prepared tuple with txnDig[%s].", pequinstore::BytesToHex(*txn_digest, 16).c_str());

        //if predicate satisfied then add to prepared visible tuple vector (avoid duplicates)
        if (read_prepared_pred(*txn_digest)) { // && !visible_tuple_set.count(tuple_location) ) {
         
          Debug("ReadPreparedPredicate is satisfied");
          // After finding latest prepare we can stop looking at the version chain
          found_prepared = true;

          Timestamp const &prepared_timestamp = tile_group_header->GetBasilTimestamp(tuple_location.offset);
      
          Debug("Tuple is prepared and predicate is satisfied");
          if(num_iters == 0) read_curr_version = true; //Only add the first read to the result

          // If it's not a point read query then don't need to read committed, unless it's find snapshot
          if(num_iters == 0 && current_txn->IsPointRead()){
            SetPointRead(current_txn, tile_group_header, tuple_location, prepared_timestamp);
            Debug("PointQuery read prepared. Still try to read committed");
            return false; // Need to still try to read committed
          }

          if (perform_find_snapshot) {
            ManageSnapshot(current_txn, tile_group_header, tuple_location, prepared_timestamp, num_iters, false);
            if(num_iters < current_txn->GetKPreparedVersions()){
              Debug("Perform snapshot has read %d prepared. Continue reading until have %d", num_iters, current_txn->GetKPreparedVersions());
              return false;
            }
          }
          return true;
        }
      }
    }

    //Current tuple was not readable.
    Debug("Found committed: %d. Found prepared: %d, Can Read Prepared: %d, GetCommitOrPrepare: %d, GetMaterialize: %d", found_committed, found_prepared, current_txn->CanReadPrepared(), 
          tile_group_header->GetCommitOrPrepare(tuple_location.offset), tile_group_header->GetMaterialize(tuple_location.offset));

    return false;   
}

void IndexScanExecutor::EvalRead(std::shared_ptr<storage::TileGroup> tile_group, storage::TileGroupHeader *tile_group_header, ItemPointer tuple_location,
    std::vector<ItemPointer> &visible_tuple_locations, concurrency::TransactionContext *current_txn, std::set<ItemPointer> &visible_tuple_set, bool use_secondary_index){
    //Eval should be called on the latest readable version. Note: For point reads we will call this up to twice (for prepared & committed)
  
  //UW_ASSERT(!tile_group_header->IsPurged(tuple_location.offset)); //purged versions should already be skipped
  if (tile_group_header->IsPurged(tuple_location.offset)) {
    //It is possible that between CheckedRow and Eval Read the version was purged. In that case, its better to ignore it (since reading it guarantees abort)
    Warning("Should have skipped purged version at tuple[%lu:%lu]. From txn:[%s]", tuple_location.block, tuple_location.offset, pequinstore::BytesToHex(*tile_group_header->GetTxnDig(tuple_location.offset), 16).c_str());
    return;
  }

  if (visible_tuple_set.count(tuple_location)) {
    Warning("Duplicate detected, skipping");
    return;
  }

  bool eval = true;

  if (tile_group_header->IsDeleted(tuple_location.offset)) {
      Debug("Tuple is deleted so will not include in result");
      if (current_txn->IsDeletion()) eval = true;  //Allow Deletions to read deleted rows. This is just for visibility... Technically, deletion should always work even if nothing exists.
      else{ eval = false;}
  }
 
  else if (predicate_ != nullptr) { // if having predicate (WHERE clause), then perform evaluation.
      LOG_TRACE("perform predicate evaluate");
      ContainerTuple<storage::TileGroup> tuple(tile_group.get(), tuple_location.offset);


      // if(use_secondary_index){  //FIXME: This code seems unecessary... Compare just checks for a subset of Evaluate as far as I can tell
      //   //Check whether current version is in secondary index(?)
      //   LOG_TRACE("candidate_tuple size: %s", tuple.GetInfo().c_str());
      //   // Construct the key tuple
      //   auto &indexed_columns = index_->GetKeySchema()->GetIndexedColumns();  
      //   storage::MaskedTuple key_tuple(&tuple, indexed_columns);

      //   // Compare the key tuple and the key
      //   if (index_->Compare(key_tuple, key_column_ids_, expr_types_, values_) == false) {
      //     LOG_TRACE("Secondary key mismatch: %u, %u\n", tuple_location.block, tuple_location.offset);
      //     //break; //simply do nothing => don't evaluate
      //     eval = false;
      //   }
      //   else{
      //     eval = predicate_->Evaluate(&tuple, nullptr, executor_context_).IsTrue();
      //   }
      // }
      // else{
        eval = predicate_->Evaluate(&tuple, nullptr, executor_context_).IsTrue();
      //}
      //Note: Eval is false by default for Deleted Rows
  }
             
  // Add the tuple to the visible tuple vector
  if(eval) {  
    visible_tuple_locations.push_back(tuple_location);
    visible_tuple_set.insert(tuple_location);

   
   if(catalog::Catalog::GetInstance()->GetQueryParams()->useActiveReadSet || is_implicit_point_read_) ManageReadSet(tuple_location, tile_group, tile_group_header, current_txn);
   //if(USE_ACTIVE_READ_SET || is_implicit_point_read_) ManageReadSet(tuple_location, tile_group, tile_group_header, current_txn);
  }
  //for point reads, mark as invalid.
  if(current_txn->IsPointRead()) RefinePointRead(current_txn, tile_group_header, tuple_location, eval);
  

  UW_ASSERT(catalog::Catalog::GetInstance()->GetQueryParams()->useActiveReadSet);
  //FOR NOW ONLY PICK ACTIVE READ SET. USE THIS LINE FOR COMPLETE RS: 
  if(!catalog::Catalog::GetInstance()->GetQueryParams()->useActiveReadSet) ManageReadSet(tuple_location, tile_group, tile_group_header, current_txn); //Note: For primary index they'll always be the same.
  //if(!USE_ACTIVE_READ_SET) ManageReadSet(tuple_location, tile_group, tile_group_header, current_txn); //Note: For primary index they'll always be the same.

  //SetPointRead();
}

//Mark the PointRead value as having failed the predicate. Distinguish between it being a deletion and predicate failure.
void IndexScanExecutor::RefinePointRead(concurrency::TransactionContext *current_txn, storage::TileGroupHeader *tile_group_header, ItemPointer tuple_location, bool eval){
  
  // bool is_metadata_table_ = table_->GetName().substr(0,3) == "pg_"; //don't do any of the Pequin features for meta data tables..
   if(is_metadata_table_) return;

  //if we called SetPointRead while the tuple was prepared, but by the time we call RefinePointRead the tuple is marked as Committed, then still treat it as prepare here.
        //otherwise we could have a weird non-atomic state, in which we set commit_val, even though it shouldn't be; and we don't set prepared_val, even though it should be.
  //To figure out whether the current tuple *was* prepared, we check whether a) the tuple is currently commit, b) whether or not we've previously set a txn_digest
  //and c) whether we've previously set a prepared timestamp that matches this tuple (in SetPointRead) => in which case this tuple must be the prepared one.
  bool has_prep_txn_digest = *current_txn->GetPreparedTxnDigest() != nullptr; //I.e. true if txn_digest was set to something.
  bool concurrent_upgrade = tile_group_header->GetCommitOrPrepare(tuple_location.offset) && has_prep_txn_digest 
                            && tile_group_header->GetBasilTimestamp(tuple_location.offset) == *current_txn->GetPreparedTimestamp(); 
  
  //if tuple is type commit (and it wasn't a conc upgrade) => pick commit val; otherwise, pick prepared val type.
  std::string *res_val = tile_group_header->GetCommitOrPrepare(tuple_location.offset) && !concurrent_upgrade ? current_txn->GetCommittedValue() : current_txn->GetPreparedValue();
  if(eval || *res_val == "r"){ //FIXME: The second cond is a hack to not change this value once it's set (for the bugged version that reads via SecondaryIndex) 
    *res_val = "r"; //passed predicate, is "readable"
    //if(eval) std::cerr << "hitting point read. Tuple: " << tuple_location.block << "," << tuple_location.offset << std::endl;
  }
  else{
    *res_val = tile_group_header->IsDeleted(tuple_location.offset) ? "d" : "f"; 
  }
  //It is set to "d" if the row is deleted, and "f" if it exists, but failed the predicate

  Debug("Set %s point read value to type: %s", tile_group_header->GetCommitOrPrepare(tuple_location.offset)? "commit" : "prepare", res_val->c_str());
  Debug("PointRead prepare value: %s. PointRead commit value: %s", current_txn->GetPreparedValue()->c_str(), current_txn->GetCommittedValue()->c_str());


   //Debug("PointRead CommittedTS:[%lu:%lu]", current_txn->GetCommitTimestamp()->getTimestamp(), current_txn->GetCommitTimestamp()->getID());
  //commit_val being non_empty indicates that we found a committed version of the row. If the version does not meet the predicate, we must refine the value type.

  //Note: This code accounts for the fact that a point read may fail in case the row version does not pass a predicate condition that is STRICTER than just the primary keys.
  //In this case we also want to write an empty result (just like in the delete case). The different names (d/f) are just for debugging purposes.
  
  //Note that even though we should return an empty result row, we should still return the latest version and proof etc.
}


void IndexScanExecutor::SetPointRead(concurrency::TransactionContext *current_txn, storage::TileGroupHeader *tile_group_header, ItemPointer tuple_location, Timestamp const &write_timestamp){
  //Manage PointRead Result
  // bool is_metadata_table_ = table_->GetName().substr(0,3) == "pg_"; //don't do any of the Pequin features for meta data tables..
  //UW_ASSERT(!is_metadata_table_);
  if(is_metadata_table_) return;

  UW_ASSERT(is_primary_index);
  
  if(tile_group_header->GetCommitOrPrepare(tuple_location.offset)){
    //mark commit result as existent  ==> New refactor: Only mark it after evaluating predicate.
    // auto commit_val = current_txn->GetCommittedValue();
    // std::cerr << "Trying to set PointRead committed. Current committed value: " << *commit_val << std::endl;
    // *commit_val = "e";  //e for "exists"
    //commit_val being non_empty indicates that we found a committed version of the row. If the version does not meet the predicate, we must refine the value type.
    
    //HACK to avoid setting proof and TS again if we already hit. 
    //TODO: THIS IS ONLY A HACK WHILE WE HAVE THE SECONDARY INDEX BUG. It is not technically correct!!!
    if(*current_txn->GetCommittedValue() == "r"){
      //Only set once!
      return;
    }

    auto commit_ts = current_txn->GetCommitTimestamp(); 
    *commit_ts = write_timestamp; //Use either this line to copy TS, OR the SetCommitTs func below to set ref. Don't need both...  (can also get TS via: commit_proof->txn().timestamp())
  
    Debug("PointRead CommittedTS:[%lu:%lu]", current_txn->GetCommitTimestamp()->getTimestamp(), current_txn->GetCommitTimestamp()->getID());
    //fprintf(stderr,"PointRead CommittedTS:[%lu:%lu] \n", current_txn->GetCommitTimestamp()->getTimestamp(), current_txn->GetCommitTimestamp()->getID());
    //current_txn->SetCommitTimestamp(&committed_timestamp); 

    Debug("Setting the commit proof");
    // Get the commit proof
    auto write_commit_proof = tile_group_header->GetCommittedProof(tuple_location.offset);
    Debug("Getting Commit proof from tuple: [%lu:%lu]", tuple_location.block, tuple_location.offset);
    UW_ASSERT(write_commit_proof); //Every committed tuple must have a commit proof.

    // Set the commit proof reference.
    const pequinstore::proto::CommittedProof **commit_proof_ref = current_txn->GetCommittedProofRef();
    *commit_proof_ref = write_commit_proof;
   

    


  }
  else{
    //HACK to avoid setting proof and TS again if we already hit. 
    //TODO: THIS IS ONLY A HACK WHILE WE HAVE THE SECONDARY INDEX BUG. It is not technically correct!!!
    if(*current_txn->GetPreparedValue() == "r"){
      //Only set once!
      return;
    }
    //mark prepare result as existent ==> New refactor: Only mark it after evaluating predicate.
    // auto prepare_val = current_txn->GetPreparedValue();
    // *prepare_val = "e";
    //prepare_val being non_empty indicates that we found a committed version of the row. If the version does not meet the predicate, we must refine the value type.

    // Set the prepared timestamp
    auto prepared_ts = current_txn->GetPreparedTimestamp(); 
    *prepared_ts = write_timestamp; //Use either this line to copy TS, OR the SetCommitTs func below to set ref. Don't need both...  Copy seems safer: prepare might get purged...
    //current_txn->SetPreparedTimestamp(&prepared_timestamp); 
    
    //Set the prepared digest
    auto prepared_txn_digest = current_txn->GetPreparedTxnDigest();
    *prepared_txn_digest = tile_group_header->GetTxnDig(tuple_location.offset);
  
    Debug("PointRead PreparedTS:[%lu:%lu], dependency: %s", current_txn->GetPreparedTimestamp()->getTimestamp(), current_txn->GetPreparedTimestamp()->getID(), pequinstore::BytesToHex(**prepared_txn_digest, 16).c_str());
      
  
    
          //TODO: Is it possible that here we mark it as prepared, but by the time Refine is called it's already committed?
          //TODO: Setting point read must be atomic...?
                  //HOw to enforce this?

          //NOTE: this does not seem to be the bug however.
  }

  //Code to print result: TESTCODE
  //  auto storage_manager = storage::StorageManager::GetInstance();
  //   auto curr_tile_group = storage_manager->GetTileGroup(tuple_location.block);
  //  const auto *schema = curr_tile_group->GetAbstractTable()->GetSchema();
  //   oid_t current_tile_group_oid = tuple_location.block;
    
  //   auto tile_group = storage_manager->GetTileGroup(current_tile_group_oid);
  //   ContainerTuple<storage::TileGroup> tuple(tile_group.get(), tuple_location.offset);     
  //   for (uint32_t col_idx = 0; col_idx < schema->GetColumnCount(); col_idx++) {
  //       auto val = tuple.GetValue(col_idx);
  //       Debug("Read col %d. Value: %s", col_idx, val.ToString().c_str());  
  //   }
}         


void IndexScanExecutor::ManageSnapshot(concurrency::TransactionContext *current_txn, storage::TileGroupHeader *tile_group_header, ItemPointer tuple_location, const Timestamp &write_timestamp, size_t &num_iters, bool commit_or_prepare){

   //bool is_metadata_table_ = table_->GetName().substr(0,3) == "pg_"; //don't do any of the Pequin features for meta data tables..
   //UW_ASSERT(!is_metadata_table_);
  if(is_metadata_table_) return;

  if(tile_group_header->IsPurged(tuple_location.offset)) return; //if row got concurrently purged ignore it. (won't make it to eval either)

  UW_ASSERT(!tile_group_header->GetMaterialize(tuple_location.offset)); // not force mat

  Debug("Manage Snapshot. Add TS [%lu:%lu]", write_timestamp.getTimestamp(), write_timestamp.getID());
  auto txn_digest = tile_group_header->GetTxnDig(tuple_location.offset);

  if(!txn_digest){
    Warning("Tuple location[%lu:%lu] of TS [%lu:%lu] has no txn_digest", tuple_location.block, tuple_location.offset, write_timestamp.getTimestamp(), write_timestamp.getID());
    if(write_timestamp.getTimestamp() == 0) return; //doesn't really matter if it is genesis
    Panic("Tuple location[%lu:%lu] of TS [%lu:%lu] has no txn_digest", tuple_location.block, tuple_location.offset, write_timestamp.getTimestamp(), write_timestamp.getID());
  }
  UW_ASSERT(txn_digest);

  auto snapshot_mgr = current_txn->GetSnapshotMgr();

  snapshot_mgr->AddToLocalSnapshot(*txn_digest.get(), write_timestamp.getTimestamp(), write_timestamp.getID(), commit_or_prepare);
  num_iters++; //currently only count "readable" versions towards snapshotK. This is so that we read at lest one committed if there is >= k unreadable prepared

  // if(num_iters > 0) Notice("Added more than 1 row key to snapshot!");
}



bool IndexScanExecutor::ExecPrimaryIndexLookup_OLD() {
  PELOTON_ASSERT(!done_);
  Debug("Inside Index Scan Executor"); // std::cerr << "Inside index scan executor" << std::endl;

  std::vector<ItemPointer *> tuple_location_ptrs;

  // Grab info from plan node
  bool acquire_owner = GetPlanNode<planner::AbstractScan>().IsForUpdate();

  PELOTON_ASSERT(index_->GetIndexType() == IndexConstraintType::PRIMARY_KEY);

  if (0 == key_column_ids_.size()) {
    // std::cerr << "Index executor scan all keys" << std::endl;
    index_->ScanAllKeys(tuple_location_ptrs);
  } else {
    // Limit clause accelerate
    if (limit_) {
      // invoke index scan limit
      if (!descend_) {
        LOG_TRACE("ASCENDING SCAN LIMIT in Primary Index");
        // std::cerr << "Index executor scan limit ascending" << std::endl;
        index_->ScanLimit(values_, key_column_ids_, expr_types_, ScanDirectionType::FORWARD, tuple_location_ptrs, &index_predicate_.GetConjunctionList()[0], limit_number_, limit_offset_);
      } else {
        LOG_TRACE("DESCENDING SCAN LIMIT in Primary Index");
        // std::cerr << "Index executor scan limit descending" << std::endl;
        index_->ScanLimit(values_, key_column_ids_, expr_types_, ScanDirectionType::BACKWARD, tuple_location_ptrs, &index_predicate_.GetConjunctionList()[0], limit_number_, limit_offset_);
        LOG_TRACE("1-Result size is %lu", result_.size());
      }
    }
    // Normal SQL (without limit)
    else {
      LOG_TRACE("Index Scan in Primary Index");
      // std::cerr << "Index executor scan all" << std::endl;
      index_->Scan(values_, key_column_ids_, expr_types_, ScanDirectionType::FORWARD, tuple_location_ptrs, &index_predicate_.GetConjunctionList()[0]);
    }
    LOG_TRACE("tuple_location_ptrs:%lu", tuple_location_ptrs.size());
  }

  if (tuple_location_ptrs.size() == 0) {
    // std::cerr << "No tuples retrieved in the index" << std::endl;
    LOG_TRACE("no tuple is retrieved from index.");
    return false;
  }

  auto &transaction_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto current_txn = executor_context_->GetTransaction();
  auto storage_manager = storage::StorageManager::GetInstance();
  std::vector<ItemPointer> visible_tuple_locations;
  // Add set for visible and prepared visible tuple locations to prevent duplicates
  std::set<ItemPointer> visible_tuple_set;
  // NEW: prepared_visible_tuple_locations;
  std::vector<ItemPointer> prepared_visible_tuple_locations;
  std::set<ItemPointer> prepared_tuple_set;

  /////////////////////////////// PESTO MODIFIERS ///////////////////////////////////
  bool has_snapshot_mgr = current_txn->GetHasSnapshotMgr();
  auto snapshot_mgr = current_txn->GetSnapshotMgr();

  //std::cerr << "HAS SNAPSHOT MGR?" << has_snapshot_mgr << std::endl;

  size_t k_prepared_versions = current_txn->GetKPreparedVersions();
  std::vector<ItemPointer> snapshot_tuple_locations;

  bool is_snapshot_read = current_txn->GetSnapshotRead();
  auto snapshot_set = current_txn->GetSnapshotSet();
  ///////////////////////////////////////////////////////////////////////////////////


  // NEW: Commit proofs
  // std::vector<const pequinstore::proto::CommittedProof *> proofs;

#ifdef LOG_TRACE_ENABLED
  int num_tuples_examined = 0;
#endif

  // for every tuple that is found in the index.
  for (auto tuple_location_ptr : tuple_location_ptrs) {
    // std::cerr << "Index executor inside for loop" << std::endl;
    ItemPointer tuple_location = *tuple_location_ptr;
    auto tile_group = storage_manager->GetTileGroup(tuple_location.block);
    auto tile_group_header = tile_group.get()->GetHeader();
    size_t chain_length = 0;
    bool found_committed = false;
    bool found_prepared = false;
    size_t num_iters = 0;

    auto visibility = transaction_manager.IsVisible(current_txn, tile_group_header, tuple_location.offset);

    Debug("Index executor visibility: %d. Undo delete: %d", visibility, current_txn->GetUndoDelete());
   
#ifdef LOG_TRACE_ENABLED
    num_tuples_examined++;
#endif
    // the following code traverses the version chain until a certain visible version is found. we should always find a visible version from a version chain.
    // NOTE: Similar read logic as seq_scan_executor
    auto const &timestamp = current_txn->GetBasilTimestamp();

    Debug(" Txn TS: [%lu, %lu]", timestamp.getTimestamp(), timestamp.getID());
   
    // Get the head of the version chain (latest version)
    ItemPointer *head = tile_group_header->GetIndirection(tuple_location.offset);

    if (head == nullptr) {
      // std::cerr << "Head is null and location of curr tuple is (" << tuple_location.block << ", " << tuple_location.offset << ")" << std::endl;
    }

    auto head_tile_group_header = storage_manager->GetTileGroup(head->block)->GetHeader();

    auto tuple_timestamp = head_tile_group_header->GetBasilTimestamp(head->offset);
    tuple_location = *head;
    tile_group_header = head_tile_group_header;
    // auto curr_tuple_id = location.offset;


    ContainerTuple<storage::TileGroup> row_(tile_group.get(), tuple_location.offset);

    //FIXME: REMOVE? This looks like its just for testing
    auto index_columns_ = index_->GetMetadata()->GetKeyAttrs();
    for (auto col : index_columns_) {
      auto val = row_.GetValue(col);
      // encoded_key = encoded_key + "///" + val.ToString();
      // primary_key_cols.push_back(val.GetAs<const char*>());
      Debug("Primary key value: %s", val.ToString().c_str());
      // std::cerr << "read set value is " << val.ToString() << std::endl;
    }

    //Find latest write appropriate for the current Txn TS.
    while (true) {
      ++chain_length;
      auto tuple_timestamp = tile_group_header->GetBasilTimestamp(tuple_location.offset);
      Debug("Tuple TS: [%lu:%lu]", tuple_timestamp.getTimestamp(), tuple_timestamp.getID());

      if (timestamp >= tuple_timestamp) {
        // Within range of timestamp
        bool eval = true;
        // if having predicate, then perform evaluation.
        if (predicate_ != nullptr) {
          LOG_TRACE("perform predicate evaluate");
          ContainerTuple<storage::TileGroup> tuple(tile_group.get(),  tuple_location.offset);
          eval = predicate_->Evaluate(&tuple, nullptr, executor_context_).IsTrue();
        }

        std::cerr << "Before delete check" << std::endl;

        /** NEW: Force eval to be true if deleted */
        if (tile_group_header->IsDeleted(tuple_location.offset)) {
          eval = true;
        }

        // if passed evaluation, then perform write.
        if (eval == true) {
          LOG_TRACE("perform read operation");
          auto res = transaction_manager.PerformRead(current_txn, tuple_location, tile_group_header, acquire_owner);
          /*if (!res) {
            LOG_TRACE("read nothing");
            transaction_manager.SetTransactionResult(current_txn,
                                                     ResultType::FAILURE);
            return res;
          }*/

          Debug("Tuple commit state: %d. Is tuple in visibility set? %d", tile_group_header->GetCommitOrPrepare(tuple_location.offset), (visible_tuple_set.find(tuple_location) == visible_tuple_set.end()));

          std::cerr << "Tuple check if committed is " << tile_group_header->GetCommitOrPrepare(tuple_location.offset) << ". Already processed is " << (visible_tuple_set.find(tuple_location) == visible_tuple_set.end()) << std::endl;

          // The tuple is committed
          if (tile_group_header->GetCommitOrPrepare(tuple_location.offset) && visible_tuple_set.find(tuple_location) == visible_tuple_set.end()) {

            // Set boolean flag found_committed to true
            found_committed = true;
            std::cerr << "Found committed tuple" << std::endl;

            if (tile_group_header->IsDeleted(tuple_location.offset)) {
              std::cerr << "Tuple is deleted so will break" << std::endl;
              break;
            }

            // Get the commit proof
            auto commit_proof = tile_group_header->GetCommittedProof(tuple_location.offset);
            // Add the commit proof to the vector
            // proofs.push_back(commit_proof);
            // Add the tuple to the visible tuple vector
            visible_tuple_locations.push_back(tuple_location);
            visible_tuple_set.insert(tuple_location);
            // Set the committed timestmp
            Timestamp committed_timestamp = tile_group_header->GetBasilTimestamp(tuple_location.offset);
            Debug("Committed Timestamp: [%lu:%lu]", committed_timestamp.getTimestamp(), committed_timestamp.getID());


            if (has_snapshot_mgr) {
              auto txn_digest = tile_group_header->GetTxnDig(tuple_location.offset);
              auto commit_or_prepare = tile_group_header->GetCommitOrPrepare(tuple_location.offset);

              if (txn_digest != nullptr) {
                snapshot_mgr->AddToLocalSnapshot(*txn_digest.get(), committed_timestamp.getTimestamp(), committed_timestamp.getID(), commit_or_prepare);
                num_iters++;
              }
            }


            if (current_txn->IsPointRead()) {
              Debug("Setting the commit proof");
              const pequinstore::proto::CommittedProof **commit_proof_ref = current_txn->GetCommittedProofRef();
              *commit_proof_ref = commit_proof;
              auto commit_ts = current_txn->GetCommitTimestamp();
              *commit_ts = committed_timestamp;
              current_txn->SetCommitTimestamp(&committed_timestamp);
              //*commit_proof_ref = *commit_proof;
              // current_txn->SetCommittedProofRef(commit_proof);
              //  std::cerr << (*commit_proof)->DebugString() << std::endl;
              if (commit_proof == nullptr) {
                Debug("Commit proof is null");
                std::cerr << "Commit proof is null" << std::endl;
              } else {
                Debug("Inside index scan proof not null");
                std::cerr << "Inside index scan proof not null" << std::endl;

                auto proof_ts = Timestamp(commit_proof->txn().timestamp());
                Debug("Proof ts is %lu, %lu", proof_ts.getTimestamp(), proof_ts.getID());

                std::cerr << "Proof ts is " << proof_ts.getTimestamp() << ", " << proof_ts.getID() << std::endl;
              }

              if (commit_proof_ref == nullptr) {
                Debug("Commit proof ref is null");
                std::cerr << "Commit proof ref is null" << std::endl;
              } else {
                if (*commit_proof_ref == nullptr) {
                  Debug("* commit proof ref is null");
                  std::cerr << "* commit proof ref is null" << std::endl;
                } else {
                  Debug("Neither pointer is null");
                  std::cerr << "Neither pointer is null" << std::endl;
                }
              }
            }
            // current_txn->SetCommittedProof(
            //     tile_group_header->GetCommittedProof(tuple_location.offset));
            //  Since tuple is committed we can stop looking at the version
            //  chain
            break;
          }


          // If finding a snapshot then read prepared values
          else if (has_snapshot_mgr && num_iters < k_prepared_versions && !found_committed && found_prepared && !tile_group_header->GetCommitOrPrepare(tuple_location.offset)) {

            auto const &read_prepared_pred = current_txn->GetReadPreparedPred();

            if (read_prepared_pred) {
              auto txn_digest = tile_group_header->GetTxnDig(tuple_location.offset);
              auto commit_or_prepare = tile_group_header->GetCommitOrPrepare(tuple_location.offset);

              Timestamp const &prepared_timestamp = tile_group_header->GetBasilTimestamp(tuple_location.offset);

              if (txn_digest != nullptr && read_prepared_pred(*txn_digest)) {
                snapshot_mgr->AddToLocalSnapshot(*txn_digest.get(), prepared_timestamp.getTimestamp(), prepared_timestamp.getID(), commit_or_prepare);
                num_iters++;
              }
            }
             
          }

          // For snapshot read
          else if (is_snapshot_read && !found_committed) {
            auto txn_dig = tile_group_header->GetTxnDig(tuple_location.offset);
            bool should_read = snapshot_set->find(*txn_dig.get()) != snapshot_set->end();

            if (should_read) {
              if (tile_group_header->IsDeleted(tuple_location.offset)) {
                  break;
              }

              Debug("Tuple is prepared and read_prepared_pred is satisfied");
              visible_tuple_locations.push_back(tuple_location);
              visible_tuple_set.insert(tuple_location);
              found_prepared = true;
              break;
            }
          }

          // NEW: if we can read prepared values, check to see if prepared
          // tuple satisfies predicate
          else if (!found_committed && !found_prepared &&
                   /*current_txn->CanReadPrepared() &&*/
                   !tile_group_header->GetCommitOrPrepare( tuple_location.offset)) {
            // NEW: check to see if tuple satisfies predicate
            auto const &read_prepared_pred = current_txn->GetReadPreparedPred();

            if (read_prepared_pred) {
              auto txn_digest = tile_group_header->GetTxnDig(tuple_location.offset);
              if (read_prepared_pred(*txn_digest) &&
                  visible_tuple_set.find(tuple_location) == visible_tuple_set.end()) {
                // NEW: if predicate satisfied then add to prepared visible
                // tuple vector

                std::cerr << "ReadPreparedPredicate is satisfied" << std::endl;
                // Set the prepared timestamp
                Timestamp prepared_timestamp = tile_group_header->GetBasilTimestamp(tuple_location.offset);
                current_txn->SetPreparedTimestamp(&prepared_timestamp);
                if (current_txn->IsPointRead()) {
                  auto prepared_txn_digest = current_txn->GetPreparedTxnDigest();
                  *prepared_txn_digest = tile_group_header->GetTxnDig(tuple_location.offset);
                }
                // Set the prepared txn digest
                /*current_txn->SetPreparedTxnDigest(
                    tile_group_header->GetTxnDig(tuple_location.offset));*/
                // After finding latest prepare we can stop looking at the
                // version chain
                found_prepared = true;


                if (has_snapshot_mgr) {
                  auto commit_or_prepare = tile_group_header->GetCommitOrPrepare(tuple_location.offset);

                  if (txn_digest != nullptr) {
                    snapshot_mgr->AddToLocalSnapshot(*txn_digest.get(), prepared_timestamp.getTimestamp(), prepared_timestamp.getID(), commit_or_prepare);
                    num_iters++;
                  }
                }


                if (tile_group_header->IsDeleted(tuple_location.offset)) {
                  break;
                }

                Debug("Tuple is prepared and predicate is satisfied");
                visible_tuple_locations.push_back(tuple_location);
                visible_tuple_set.insert(tuple_location);

                // If it's not a point read query then don't need to read
                // committed, unless it's find snapshot
                if (!current_txn->IsPointRead() && !has_snapshot_mgr) {
                  Debug("Not a point read query");
                  break;
                }
              }
            }
          }

          Debug("Found committed: %d. Found prepared: %d, Can Read Prepared: "
                "%d, GetCommitOrPrepare: %d",
                found_committed, found_prepared, current_txn->CanReadPrepared(),
                tile_group_header->GetCommitOrPrepare(tuple_location.offset));
        }
      }

      ItemPointer old_item = tuple_location;
      // std::cerr << "Offset is " << old_item.offset << std::endl;
      tuple_location = tile_group_header->GetNextItemPointer(old_item.offset);
      tile_group = storage_manager->GetTileGroup(tuple_location.block);
      tile_group_header = tile_group->GetHeader();

      if (tuple_location.IsNull()) {
        // std::cerr << "Tuple location is null" << std::endl;
        break;
      }
    }
    // std::cerr << "Outside while loop" << std::endl;
    LOG_TRACE("Traverse length: %d\n", (int)chain_length);
    // std::cerr << "For loop iteration" << std::endl;
  }
  // std::cerr << "Outside for loop" << std::endl;
  LOG_TRACE("Examined %d tuples from index %s", num_tuples_examined, index_->GetName().c_str());

  LOG_TRACE("%ld tuples before pruning boundaries", visible_tuple_locations.size());

  // Check whether the boundaries satisfy the required condition
  CheckOpenRangeWithReturnedTuples(visible_tuple_locations);

  LOG_TRACE("%ld tuples after pruning boundaries",
            visible_tuple_locations.size());

  // Add the tuple locations to the result vector in the order returned by the index scan. We might end up reading the same tile group multipletimes. However, this is necessary to adhere to the ORDER BY clause
  oid_t current_tile_group_oid = INVALID_OID;
  std::vector<oid_t> tuples;

  auto const &primary_index_columns_ = index_->GetMetadata()->GetKeyAttrs();
  auto query_read_set_mgr = current_txn->GetQueryReadSetMgr();
  auto const &current_txn_timestamp = current_txn->GetBasilTimestamp();


  if (!current_txn->IsPointRead()) {
    Debug("Get Table and Col Versions");
    // Read table version and table col versions bool get_read_set = current_txn->GetHasReadSetMgr();
    bool get_read_set = current_txn->GetHasReadSetMgr();
    bool find_snapshot = current_txn->GetHasSnapshotMgr();
    auto ss_mgr = current_txn->GetSnapshotMgr();
    bool perform_read_on_snapshot = current_txn->GetSnapshotRead();
    auto snapshot_set = current_txn->GetSnapshotSet();

    // Read table version and table col versions
    current_txn->GetTableVersion()(table_->GetName(), current_txn_timestamp, get_read_set, query_read_set_mgr, find_snapshot, ss_mgr, perform_read_on_snapshot, snapshot_set);
    
    //if(use_col_versions){
    if(catalog::Catalog::GetInstance()->GetQueryParams()->useColVersions){
      std::unordered_set<std::string> column_names;
      //std::vector<std::string> col_names;
      GetColNames(predicate_, column_names); //Note: These are *all* columns in the where clause, not just the ones covered by the predicate

      for (auto &col : column_names) {
        std::cerr << "Col name is " << col << std::endl;
        current_txn->GetTableVersion()(EncodeTableCol(table_->GetName(), col),  current_txn_timestamp, get_read_set, query_read_set_mgr, find_snapshot, ss_mgr, perform_read_on_snapshot, snapshot_set);
        //col_names.push_back(col);
      }

      // std::string encoded_key = EncodeTableRow(table_->GetName(), col_names);
      // std::cerr << "Encoded key is " << encoded_key << std::endl;
      // current_txn->GetTableVersion()(encoded_key, current_txn_timestamp, true, query_read_set_mgr, nullptr);
    }
  }

  for (auto &visible_tuple_location : visible_tuple_locations) {
    if (current_tile_group_oid == INVALID_OID) {
      current_tile_group_oid = visible_tuple_location.block;
    }
    if (current_tile_group_oid == visible_tuple_location.block) {
      tuples.push_back(visible_tuple_location.offset);

      if (current_txn->GetHasReadSetMgr()) {
        auto tile_group = storage_manager->GetTileGroup(visible_tuple_location.block);
        auto tile_group_header = tile_group->GetHeader();

        ContainerTuple<storage::TileGroup> row(tile_group.get(), visible_tuple_location.offset);

        std::vector<std::string> primary_key_cols;
        for (auto col : primary_index_columns_) {
          auto val = row.GetValue(col);
          primary_key_cols.push_back(val.ToString());
          Debug("Read set value: %s", val.ToString().c_str());
        }

        const Timestamp &time = tile_group_header->GetBasilTimestamp(visible_tuple_location.offset);
        std::string &&encoded = EncodeTableRow(table_->GetName(), primary_key_cols);
        Debug("encoded read set key is: %s. Version: [%lu: %lu]", encoded.c_str(), time.getTimestamp(), time.getID());
        query_read_set_mgr->AddToReadSet(std::move(encoded), time);

        //If prepared: Additionally set Dependency
        if (!tile_group_header->GetCommitOrPrepare(visible_tuple_location.offset)) {
          if (tile_group_header->GetTxnDig(visible_tuple_location.offset) == nullptr) Panic("Dep Digest is null");
          query_read_set_mgr->AddToDepSet(*tile_group_header->GetTxnDig(visible_tuple_location.offset), time);
          
        }
      }

    } else {
      // Since the tile_group_oids differ, fill in the current tile group
      // into the result vector
      auto storage_manager = storage::StorageManager::GetInstance();
      auto tile_group = storage_manager->GetTileGroup(current_tile_group_oid);
      std::unique_ptr<LogicalTile> logical_tile(LogicalTileFactory::GetTile());
      // Add relevant columns to logical tile
      logical_tile->AddColumns(tile_group, full_column_ids_);
      logical_tile->AddPositionList(std::move(tuples));
      // Add prepared values and commit proofs to logical tile
      // logical_tile->AddPreparedValues(prepared_values[0]);
      // logical_tile->SetCommitProofs(proofs[0]);
      if (column_ids_.size() != 0) {
        logical_tile->ProjectColumns(full_column_ids_, column_ids_);
      }
      result_.push_back(logical_tile.release());

      // Change the current_tile_group_oid and add the current tuple
      tuples.clear();
      current_tile_group_oid = visible_tuple_location.block;
      tuples.push_back(visible_tuple_location.offset);

      if (current_txn->GetHasReadSetMgr()) {
        auto tile_group = storage_manager->GetTileGroup(visible_tuple_location.block);
        auto tile_group_header = tile_group->GetHeader();

        ContainerTuple<storage::TileGroup> row(tile_group.get(), visible_tuple_location.offset);

        std::vector<std::string> primary_key_cols;
        for (auto col : primary_index_columns_) {
          auto val = row.GetValue(col);
          primary_key_cols.push_back(val.ToString());
          Debug("Read set value: %s", val.ToString().c_str());
        }

        const Timestamp &time = tile_group_header->GetBasilTimestamp(visible_tuple_location.offset);
        std::string &&encoded = EncodeTableRow(table_->GetName(), primary_key_cols);
        Debug("encoded read set key is: %s. Version: [%lu: %lu]", encoded.c_str(), time.getTimestamp(), time.getID());
        query_read_set_mgr->AddToReadSet(std::move(encoded), time);

        //If prepared: Additionally set Dependency
        if (!tile_group_header->GetCommitOrPrepare(visible_tuple_location.offset)) {
          if (tile_group_header->GetTxnDig(visible_tuple_location.offset) == nullptr) Panic("Dep Digest is null");
          query_read_set_mgr->AddToDepSet(*tile_group_header->GetTxnDig(visible_tuple_location.offset), time);
          
        }
      }
    }
  }

  // Add the remaining tuples to the result vector
  if ((current_tile_group_oid != INVALID_OID) && (!tuples.empty())) {
    auto tile_group = storage_manager->GetTileGroup(current_tile_group_oid);
    std::unique_ptr<LogicalTile> logical_tile(LogicalTileFactory::GetTile());
    // Add relevant columns to logical tile
    logical_tile->AddColumns(tile_group, full_column_ids_);
    logical_tile->AddPositionList(std::move(tuples));
    // Add prepared values and commit proofs to logical tile
    // logical_tile->AddPreparedValues(prepared_values[0]);
    // logical_tile->SetCommitProofs(proofs[0]);
    if (column_ids_.size() != 0) {
      logical_tile->ProjectColumns(full_column_ids_, column_ids_);
    }
    result_.push_back(logical_tile.release());
  }

  done_ = true;

  LOG_TRACE("Result tiles : %lu", result_.size());

  return true;
}

bool IndexScanExecutor::ExecSecondaryIndexLookup_OLD() {
  LOG_TRACE("ExecSecondaryIndexLookup");
  PELOTON_ASSERT(!done_);
  PELOTON_ASSERT(index_->GetIndexType() != IndexConstraintType::PRIMARY_KEY);
  Debug("Inside Secondary Scan");

  std::vector<ItemPointer *> tuple_location_ptrs;

  // Grab info from plan node
  bool acquire_owner = GetPlanNode<planner::AbstractScan>().IsForUpdate();

  if (0 == key_column_ids_.size()) {
    index_->ScanAllKeys(tuple_location_ptrs);
  } else {
    // Limit clause accelerate
    if (limit_) {
      // invoke index scan limit
      if (!descend_) {
        LOG_TRACE("ASCENDING SCAN LIMIT in Secondary Index");
        index_->ScanLimit(values_, key_column_ids_, expr_types_, ScanDirectionType::FORWARD, tuple_location_ptrs, &index_predicate_.GetConjunctionList()[0],limit_number_, limit_offset_);
      } else {
        LOG_TRACE("DESCENDING SCAN LIMIT in Secondary Index");
        index_->ScanLimit(values_, key_column_ids_, expr_types_, ScanDirectionType::BACKWARD, tuple_location_ptrs, &index_predicate_.GetConjunctionList()[0], limit_number_, limit_offset_);

        if (tuple_location_ptrs.size() == 0) {
          LOG_TRACE("2-Result size is %lu", tuple_location_ptrs.size());
        }
      }
    }
    // Normal SQL (without limit)
    else {
      LOG_TRACE("Index Scan in Primary Index");
      index_->Scan(values_, key_column_ids_, expr_types_, ScanDirectionType::FORWARD, tuple_location_ptrs, &index_predicate_.GetConjunctionList()[0]);
    }
  }

  if (tuple_location_ptrs.size() == 0) {
    LOG_TRACE("no tuple is retrieved from index.");
    return false;
  }

  auto &transaction_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto current_txn = executor_context_->GetTransaction();

  std::vector<ItemPointer> visible_tuple_locations;

  // Quickie Hack
  // Sometimes we can get the tuples we need in the same block if they
  // were inserted at the same time. So we'll record the last block that
  // we got for each tuple and check whether its the same to avoid having
  // to go back to the catalog each time.
  oid_t last_block = INVALID_OID;
  std::shared_ptr<storage::TileGroup> tile_group;
  storage::TileGroupHeader *tile_group_header = nullptr;

#ifdef LOG_TRACE_ENABLED
  int num_tuples_examined = 0;
  int num_blocks_reused = 0;
#endif
  auto storage_manager = storage::StorageManager::GetInstance();
  for (auto tuple_location_ptr : tuple_location_ptrs) {
    ItemPointer tuple_location = *tuple_location_ptr;
    if (tuple_location.block != last_block) {
      tile_group = storage_manager->GetTileGroup(tuple_location.block);
      tile_group_header = tile_group.get()->GetHeader();
    }
#ifdef LOG_TRACE_ENABLED
    else
      num_blocks_reused++;
    num_tuples_examined++;
#endif

    // the following code traverses the version chain until a certain visible
    // version is found.
    // we should always find a visible version from a version chain.
    // different from primary key index lookup, we have to compare the
    // secondary
    // key to guarantee the correctness of the result.
    size_t chain_length = 0;
    while (true) {
      ++chain_length;

      auto visibility = transaction_manager.IsVisible(current_txn, tile_group_header, tuple_location.offset);

      // if the tuple is deleted
      if (visibility == VisibilityType::DELETED) {
        LOG_TRACE("encounter deleted tuple: %u, %u", tuple_location.block, tuple_location.offset);
        break;
      }
      // if the tuple is visible.
      else if (visibility == VisibilityType::OK) {
        LOG_TRACE("perform read: %u, %u", tuple_location.block, tuple_location.offset);

        // Further check if the version has the secondary key
        ContainerTuple<storage::TileGroup> candidate_tuple(tile_group.get(), tuple_location.offset);

        LOG_TRACE("candidate_tuple size: %s", candidate_tuple.GetInfo().c_str());
        // Construct the key tuple
        auto &indexed_columns = index_->GetKeySchema()->GetIndexedColumns();
        storage::MaskedTuple key_tuple(&candidate_tuple, indexed_columns);

        // Compare the key tuple and the key
        if (index_->Compare(key_tuple, key_column_ids_, expr_types_, values_) == false) {
          LOG_TRACE("Secondary key mismatch: %u, %u\n", tuple_location.block, tuple_location.offset);
          break;
        }

        bool eval = true;
        // if having predicate, then perform evaluation.
        if (predicate_ != nullptr) {
          eval = predicate_->Evaluate(&candidate_tuple, nullptr, executor_context_).IsTrue();
        }
        // if passed evaluation, then perform write.
        if (eval == true) {
          auto res = transaction_manager.PerformRead(current_txn, tuple_location, tile_group_header, acquire_owner);
          if (!res) {
            transaction_manager.SetTransactionResult(current_txn, ResultType::FAILURE);
            LOG_TRACE("passed evaluation, but txn read fails");
            return res;
          }
          // if perform read is successful, then add to visible tuple vector.
          visible_tuple_locations.push_back(tuple_location);
          LOG_TRACE("passed evaluation, visible_tuple_locations size: %lu", visible_tuple_locations.size());
        } else {
          LOG_TRACE("predicate evaluate fails");
        }

        break;
      }
      // if the tuple is not visible.
      else {
        PELOTON_ASSERT(visibility == VisibilityType::INVISIBLE);

        LOG_TRACE("Invisible read: %u, %u", tuple_location.block,
                  tuple_location.offset);

        bool is_acquired = (tile_group_header->GetTransactionId(
                                tuple_location.offset) == INITIAL_TXN_ID);
        bool is_alive =
            (tile_group_header->GetEndCommitId(tuple_location.offset) <=
             current_txn->GetReadId());
        if (is_acquired && is_alive) {
          // See an invisible version that does not belong to any one in the
          // version chain.
          // this means that some other transactions have modified the version
          // chain.
          // Wire back because the current version is expired. have to search
          // from scratch.
          tuple_location =
              *(tile_group_header->GetIndirection(tuple_location.offset));
          tile_group = storage_manager->GetTileGroup(tuple_location.block);
          tile_group_header = tile_group.get()->GetHeader();
          chain_length = 0;
          continue;
        }

        ItemPointer old_item = tuple_location;
        tuple_location = tile_group_header->GetNextItemPointer(old_item.offset);

        if (tuple_location.IsNull()) {
          // For an index scan on a version chain, the result should be one of
          // the following:
          //    (1) find a visible version
          //    (2) find a deleted version
          //    (3) find an aborted version with chain length equal to one
          if (chain_length == 1) {
            break;
          }

          // in most cases, there should exist a visible version.
          // if we have traversed through the chain and still can not fulfill
          // one of the above conditions,
          // then return result_failure.
          transaction_manager.SetTransactionResult(current_txn,
                                                   ResultType::FAILURE);
          return false;
        }

        // search for next version.
        tile_group = storage_manager->GetTileGroup(tuple_location.block);
        tile_group_header = tile_group.get()->GetHeader();
      }
    }
    LOG_TRACE("Traverse length: %d\n", (int)chain_length);
  }
  LOG_TRACE("Examined %d tuples from index %s [num_blocks_reused=%d]",
            num_tuples_examined, index_->GetName().c_str(), num_blocks_reused);

  // Check whether the boundaries satisfy the required condition
  CheckOpenRangeWithReturnedTuples(visible_tuple_locations);

  // Add the tuple locations to the result vector in the order returned by
  // the index scan. We might end up reading the same tile group multiple
  // times. However, this is necessary to adhere to the ORDER BY clause
  oid_t current_tile_group_oid = INVALID_OID;
  std::vector<oid_t> tuples;

  for (auto &visible_tuple_location : visible_tuple_locations) {
    if (current_tile_group_oid == INVALID_OID) {
      current_tile_group_oid = visible_tuple_location.block;
    }
    if (current_tile_group_oid == visible_tuple_location.block) {
      tuples.push_back(visible_tuple_location.offset);
    } else {
      // Since the tile_group_oids differ, fill in the current tile group
      // into the result vector
      auto tile_group = storage_manager->GetTileGroup(current_tile_group_oid);
      std::unique_ptr<LogicalTile> logical_tile(LogicalTileFactory::GetTile());
      // Add relevant columns to logical tile
      logical_tile->AddColumns(tile_group, full_column_ids_);
      logical_tile->AddPositionList(std::move(tuples));
      if (column_ids_.size() != 0) {
        logical_tile->ProjectColumns(full_column_ids_, column_ids_);
      }
      result_.push_back(logical_tile.release());

      // Change the current_tile_group_oid and add the current tuple
      tuples.clear();
      current_tile_group_oid = visible_tuple_location.block;
      tuples.push_back(visible_tuple_location.offset);
    }
  }

  // Add the remaining tuples (if any) to the result vector
  if ((current_tile_group_oid != INVALID_OID) && (!tuples.empty())) {
    auto tile_group = storage_manager->GetTileGroup(current_tile_group_oid);
    std::unique_ptr<LogicalTile> logical_tile(LogicalTileFactory::GetTile());
    // Add relevant columns to logical tile
    logical_tile->AddColumns(tile_group, full_column_ids_);
    logical_tile->AddPositionList(std::move(tuples));
    if (column_ids_.size() != 0) {
      logical_tile->ProjectColumns(full_column_ids_, column_ids_);
    }
    result_.push_back(logical_tile.release());
  }

  done_ = true;

  LOG_TRACE("Result tiles : %lu", result_.size());

  return true;
}


bool IndexScanExecutor::ExecSecondaryIndexLookup() {
  LOG_TRACE("ExecSecondaryIndexLookup");
  PELOTON_ASSERT(!done_);
  //PELOTON_ASSERT(index_->GetIndexType() != IndexConstraintType::PRIMARY_KEY); //FIXME: Should be able to use this?
  Debug("Inside Secondary Scan");

   auto current_txn = executor_context_->GetTransaction();

   UW_ASSERT(!current_txn->is_customer_read);// Should only be true for point read to customer.

   //if(current_txn->is_limit) Notice("Starting Scan (Secondary)");

  //////////////// Get TableVersion and TableColVersions    //These should be taken before the index lookup //   //TODO: Confirm that this works the same way for secondary index
  auto query_read_set_mgr = current_txn->GetQueryReadSetMgr();
  auto const &current_txn_timestamp = current_txn->GetBasilTimestamp();

  SetPredicate(current_txn, query_read_set_mgr);
  SetTableColVersions(current_txn, query_read_set_mgr, current_txn_timestamp);
  ///////////////////////////////////////////////////////////////////////////////////////////////////////////////


  std::vector<ItemPointer *> tuple_location_ptrs;

  // Grab info from plan node
  bool acquire_owner = GetPlanNode<planner::AbstractScan>().IsForUpdate();

  if (0 == key_column_ids_.size()) {
    index_->ScanAllKeys(tuple_location_ptrs);
  } else {
    // Limit clause accelerate
    if (limit_) {
      // invoke index scan limit
      if (!descend_) {
        LOG_TRACE("ASCENDING SCAN LIMIT in Secondary Index");
        index_->ScanLimit(values_, key_column_ids_, expr_types_, ScanDirectionType::FORWARD, tuple_location_ptrs, &index_predicate_.GetConjunctionList()[0], limit_number_, limit_offset_);
      } else {
        LOG_TRACE("DESCENDING SCAN LIMIT in Secondary Index");
        index_->ScanLimit(values_, key_column_ids_, expr_types_, ScanDirectionType::BACKWARD, tuple_location_ptrs, &index_predicate_.GetConjunctionList()[0], limit_number_, limit_offset_);

        if (tuple_location_ptrs.size() == 0) {
          LOG_TRACE("2-Result size is %lu", tuple_location_ptrs.size());
        }
      }
    }
    // Normal SQL (without limit)
    else {
      LOG_TRACE("Index Scan in Primary Index");
      index_->Scan(values_, key_column_ids_, expr_types_, ScanDirectionType::FORWARD, tuple_location_ptrs, &index_predicate_.GetConjunctionList()[0]);
    }
  }

  if (tuple_location_ptrs.size() == 0) {
    LOG_TRACE("no tuple is retrieved from index.");
    Debug(" Found no matching rows in table: %s", table_->GetName().c_str());
    return false;
  }

  auto &transaction_manager = concurrency::TransactionManagerFactory::GetInstance();


  std::vector<ItemPointer> visible_tuple_locations;
  // Add set for visible and prepared visible tuple locations to prevent duplicates
  std::set<ItemPointer> visible_tuple_set;

  //TODO: FS: I don't understand this comment (It's a hack only in secondary indexes). Is this something that we can use to optimize PrimaryIndex too?

  // Quickie Hack: Sometimes we can get the tuples we need in the same block if they were inserted at the same time. 
  //               So we'll record the last block that we got for each tuple and check whether its the same to avoid having to go back to the catalog each time.
  oid_t last_block = INVALID_OID;
  std::shared_ptr<storage::TileGroup> tile_group;
  storage::TileGroupHeader *tile_group_header = nullptr;

#ifdef LOG_TRACE_ENABLED
  int num_tuples_examined = 0;
  int num_blocks_reused = 0;
#endif
  auto storage_manager = storage::StorageManager::GetInstance();

  // std::cerr << "Secondary Number of rows to check " << tuple_location_ptrs.size() << std::endl;
  // int max_size = std::min((int)tuple_location_ptrs.size(), INT_MAX);
  // //if(current_txn->IsPointRead()) max_size = 1; //UW_ASSERT(tuple_location_ptrs.size() == 1);
  // tuple_location_ptrs.resize(max_size);
  // tuple_location_ptrs.shrink_to_fit();
 
  Debug("Query[%lu:%lu]Secondary Index Scan on Table [%s]. Number of rows to check: %d", current_txn->GetBasilTimestamp().getTimestamp(), current_txn->GetBasilTimestamp().getID(), table_->GetName().c_str(), tuple_location_ptrs.size());
  // if(tuple_location_ptrs.size() > 200) Warning("Potentially inefficient SECONDARY scan on table %s. Rows to check %d. Sanity check!", table_->GetName().c_str(), tuple_location_ptrs.size());
  
  //if(tuple_location_ptrs.size() > 150) Panic("doing full scan");
  

  for (auto tuple_location_ptr : tuple_location_ptrs) {
    // ItemPointer tuple_location = *tuple_location_ptr;
    // if (tuple_location.block != last_block) {
    //   tile_group = storage_manager->GetTileGroup(tuple_location.block);
    //   tile_group_header = tile_group.get()->GetHeader();
    // }
    // #ifdef LOG_TRACE_ENABLED
    //     else
    //       num_blocks_reused++;
    //     num_tuples_examined++;
    // #endif

    CheckRow(*tuple_location_ptr, transaction_manager, current_txn, storage_manager, visible_tuple_locations, visible_tuple_set, true);
  }

  RefineTableColVersions(current_txn, query_read_set_mgr); //Refine TableVersion if reading from materialized snapshot

  // // std::cerr << "Outside for loop" << std::endl;
  LOG_TRACE("Examined %d tuples from index %s", num_tuples_examined, index_->GetName().c_str());

  LOG_TRACE("%ld tuples before pruning boundaries", visible_tuple_locations.size());

  // Check whether the boundaries satisfy the required condition
  CheckOpenRangeWithReturnedTuples(visible_tuple_locations);

  LOG_TRACE("%ld tuples after pruning boundaries", visible_tuple_locations.size());

  // Add the tuple locations to the result vector in the order returned by the index scan. We might end up reading the same tile group multiple times. However, this is necessary to adhere to the ORDER BY clause
  oid_t current_tile_group_oid = INVALID_OID;
  std::vector<oid_t> tuples;

  for (auto &visible_tuple_location : visible_tuple_locations) {
    if (current_tile_group_oid == INVALID_OID) {
      current_tile_group_oid = visible_tuple_location.block;
    }
    if (current_tile_group_oid == visible_tuple_location.block) {
      tuples.push_back(visible_tuple_location.offset);
    } else {
      // Since the tile_group_oids differ, fill in the current tile group into the result vector
      auto storage_manager = storage::StorageManager::GetInstance();
      auto tile_group = storage_manager->GetTileGroup(current_tile_group_oid);
      PrepareResult(tuples, tile_group);

      // Change the current_tile_group_oid and add the current tuple
      tuples.clear();
      current_tile_group_oid = visible_tuple_location.block;
      tuples.push_back(visible_tuple_location.offset);
    }
    //ManageReadSet(visible_tuple_location, current_txn, storage_manager);
  }

  // Add the remaining tuples to the result vector
  if ((current_tile_group_oid != INVALID_OID) && (!tuples.empty())) {
    auto tile_group = storage_manager->GetTileGroup(current_tile_group_oid);
    PrepareResult(tuples, tile_group);
  }

  done_ = true;

  LOG_TRACE("Result tiles : %lu", result_.size());

  return true;
   
}

void IndexScanExecutor::CheckOpenRangeWithReturnedTuples(
    std::vector<ItemPointer> &tuple_locations) {
  while (left_open_) {
    LOG_TRACE("Range left open!");
    auto tuple_location_itr = tuple_locations.begin();

    if (tuple_location_itr == tuple_locations.end() ||
        CheckKeyConditions(*tuple_location_itr) == true)
      left_open_ = false;
    else
      tuple_locations.erase(tuple_location_itr);
  }

  while (right_open_) {
    LOG_TRACE("Range right open!");
    auto tuple_location_itr = tuple_locations.rbegin();

    if (tuple_location_itr == tuple_locations.rend() ||
        CheckKeyConditions(*tuple_location_itr) == true)
      right_open_ = false;
    else
      tuple_locations.pop_back();
  }
}

bool IndexScanExecutor::CheckKeyConditions(const ItemPointer &tuple_location) {
  // The size of these three arrays must be the same
  PELOTON_ASSERT(key_column_ids_.size() == expr_types_.size());
  PELOTON_ASSERT(expr_types_.size() == values_.size());

  LOG_TRACE("Examining key conditions for the returned tuple.");

  auto storage_manager = storage::StorageManager::GetInstance();
  auto tile_group = storage_manager->GetTileGroup(tuple_location.block);
  ContainerTuple<storage::TileGroup> tuple(tile_group.get(),
                                           tuple_location.offset);

  // This is the end of loop
  oid_t cond_num = key_column_ids_.size();

  // Go over each attribute in the list of comparison columns
  // The key_columns_ids, as the name shows, saves the key column ids that
  // have values and expression needs to be compared.

  for (oid_t i = 0; i < cond_num; i++) {
    // First retrieve the tuple column ID from the map, and then map
    // it to the column ID of index key
    oid_t tuple_key_column_id = key_column_ids_[i];

    // This the comparison right hand side operand
    const type::Value &rhs = values_[i];

    // Also retrieve left hand side operand using index key column ID
    type::Value val = (tuple.GetValue(tuple_key_column_id));
    const type::Value &lhs = val;

    // Expression type. We use this to interpret comparison result
    //
    // Possible results of comparison are: EQ, >, <
    const ExpressionType expr_type = expr_types_[i];

    // If the operation is IN, then use the boolean values comparator
    // that determines whether a value is in a list
    //
    // To make the procedure more uniform, we interpret IN as EQUAL
    // and NOT IN as NOT EQUAL, and react based on expression type below
    // accordingly
    if (lhs.CompareEquals(rhs) == CmpBool::CmpTrue) {
      switch (expr_type) {
      case ExpressionType::COMPARE_EQUAL:
      case ExpressionType::COMPARE_LESSTHANOREQUALTO:
      case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
      case ExpressionType::COMPARE_IN:
        continue;

      case ExpressionType::COMPARE_NOTEQUAL:
      case ExpressionType::COMPARE_LESSTHAN:
      case ExpressionType::COMPARE_GREATERTHAN:
        return false;

      default:
        throw IndexException("Unsupported expression type : " +
                             ExpressionTypeToString(expr_type));
      }
    } else {
      if (lhs.CompareLessThan(rhs) == CmpBool::CmpTrue) {
        switch (expr_type) {
        case ExpressionType::COMPARE_NOTEQUAL:
        case ExpressionType::COMPARE_LESSTHAN:
        case ExpressionType::COMPARE_LESSTHANOREQUALTO:
          continue;

        case ExpressionType::COMPARE_EQUAL:
        case ExpressionType::COMPARE_GREATERTHAN:
        case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
        case ExpressionType::COMPARE_IN:
          return false;

        default:
          throw IndexException("Unsupported expression type : " +
                               ExpressionTypeToString(expr_type));
        }
      } else {
        if (lhs.CompareGreaterThan(rhs) == CmpBool::CmpTrue) {
          switch (expr_type) {
          case ExpressionType::COMPARE_NOTEQUAL:
          case ExpressionType::COMPARE_GREATERTHAN:
          case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
            continue;

          case ExpressionType::COMPARE_EQUAL:
          case ExpressionType::COMPARE_LESSTHAN:
          case ExpressionType::COMPARE_LESSTHANOREQUALTO:
          case ExpressionType::COMPARE_IN:
            return false;

          default:
            throw IndexException("Unsupported expression type : " +
                                 ExpressionTypeToString(expr_type));
          }
        } else {
          // Since it is an AND predicate, we could directly return false
          return false;
        }
      }
    }
  }

  LOG_TRACE("Examination returning true.");

  return true;
}

// column_ids is the right predicate column id. For example, i_id = s_id, then s_id is column_ids
// But the passing value is the result (output) id. We need to transform it to physical id
void IndexScanExecutor::UpdatePredicate(
    const std::vector<oid_t> &column_ids,
    const std::vector<type::Value> &values) {
  // Update index predicate
  LOG_TRACE("values_ size %lu", values_.size());

  std::vector<oid_t> key_column_ids;
  // The non index column ids
  std::vector<oid_t> non_index_column_ids;
  // The values of indexed columns
  std::vector<type::Value> indexed_values;
  // The values of non-indexed columns
  std::vector<type::Value> non_indexed_values;
  int value_idx = 0;

  Debug("UPDATING INDEX SCAN PREDICATE");
  // Get the column ids which are in the index
  auto indexed_cols = index_->GetKeySchema()->GetIndexedColumns();
  std::set<oid_t> index_col_set(indexed_cols.begin(), indexed_cols.end());
  
  PELOTON_ASSERT(column_ids.size() <= column_ids_.size());
  // Get the real physical ids
  for (auto column_id : column_ids) {
    // If the index contains column id then add to key column id list
    if (index_col_set.find(column_ids_[column_id]) != index_col_set.end()) {
      key_column_ids.push_back(column_ids_[column_id]);
      // Add corresponding value of column to indexed values
      indexed_values.push_back(values[value_idx]);
    } else {
      // Column not in index remove from indexed, add to non-indexed values
      std::cerr << "The col id is " << column_ids_[column_id] << std::endl;
      non_index_column_ids.push_back(column_ids_[column_id]);
      non_indexed_values.push_back(values[value_idx]);
    }
    value_idx++;
    
    LOG_TRACE("Output id is %d---physical column id is %d", column_id, column_ids_[column_id]);
  }

  updated_column_ids = key_column_ids;

  // Update values in index plan node
  PELOTON_ASSERT(key_column_ids.size() == indexed_values.size());
  PELOTON_ASSERT(key_column_ids_.size() == values_.size());

  // Find out the position (offset) where is key_column_id
  for (oid_t new_idx = 0; new_idx < key_column_ids.size(); new_idx++) {
    unsigned int current_idx = 0;
    for (; current_idx < values_.size(); current_idx++) {
      if (key_column_ids[new_idx] == key_column_ids_[current_idx]) {
      
        // fprintf(stderr, "Orignial is %d:%s", key_column_ids[new_idx], values_[current_idx].GetInfo().c_str());
        // fprintf(stderr, "Changed to %d:%s\n", key_column_ids[new_idx], indexed_values[new_idx].GetInfo().c_str());
        LOG_TRACE("Orignial is %d:%s", key_column_ids[new_idx], values_[current_idx].GetInfo().c_str());
        LOG_TRACE("Changed to %d:%s", key_column_ids[new_idx], values[new_idx].GetInfo().c_str());
        values_[current_idx] = indexed_values[new_idx];

        // There should not be two same columns. So when we find a column, we should break the loop
        break;
      }
    }

    // If new value doesn't exist in current value list, add it.
    // For the current simple optimizer, since all the key column ids must be initiated when creating index_scan_plan, 
    // we don't need to examine whether the passing column and value exist or not (they definitely exist). 
    //But for the future optimizer, we probably change the logic. So we still keep the examine code here.
    if (current_idx == values_.size()) {
      LOG_TRACE("Add new column for index predicate:%u-%s", key_column_ids[new_idx], values[new_idx].GetInfo().c_str());

      // Add value
      values_.push_back(indexed_values[new_idx]);

      // Add column id
      key_column_ids_.push_back(key_column_ids[new_idx]);

      // Add column type.
      // TODO: We should add other types in the future
      expr_types_.push_back(ExpressionType::COMPARE_EQUAL);
    }
  }

  // for(int i = 0; i<key_column_ids.size(); ++i){
  //    std::cerr << "col name: " << table_->GetSchema()->GetColumn(key_column_ids[i]).GetName()  << ". val: " << indexed_values[i] << std::endl;
  // }

  //index_predicate_.GetConjunctionListToSetup()[0].
  // Update the new value
  index_predicate_.GetConjunctionListToSetup()[0].SetTupleColumnValue(index_.get(), key_column_ids, indexed_values);

  //Update predicate to fill in all values
  //Note: This is necessary to have the full predicate for SemanticCC
  //The executor itself technically only needs to fill in the non_indexed_column_ids.
  std::move(non_index_column_ids.begin(), non_index_column_ids.end(), std::back_inserter(key_column_ids));
  std::move(non_indexed_values.begin(), non_indexed_values.end(), std::back_inserter(indexed_values));
  expression::AbstractExpression *new_predicate = ColumnsValuesToExpr(key_column_ids, indexed_values, 0);
  //expression::AbstractExpression *new_predicate = non_indexed_values.size() != 0 ? ColumnsValuesToExpr(non_index_column_ids, non_indexed_values, 0) : nullptr;

  if (new_predicate == nullptr) return;

     // combine with original predicate
  if (old_predicate_ != nullptr) {
     expression::AbstractExpression *lexpr = new_predicate, *rexpr = old_predicate_->Copy();

     new_predicate = new expression::ConjunctionExpression(ExpressionType::CONJUNCTION_AND, lexpr, rexpr);
  }

  // Currently a hack that prevent memory leak we should eventually make prediate_ a unique_ptr
  new_predicate_.reset(new_predicate);
  predicate_ = new_predicate;
  
  //std::cerr << "new pred: " << new_predicate->GetInfo() << std::endl;
  Debug("New pred: %s", new_predicate->GetInfo().c_str());

  // auto current_txn = executor_context_->GetTransaction();
  // if (current_txn->GetHasReadSetMgr()) {
  //   pequinstore::QueryReadSetMgr *query_read_set_mgr = current_txn->GetQueryReadSetMgr();
  //   auto pred_copy = predicate_->Copy();
  //   pred_copy->DeduceExpressionName();
        
  //   std::string full_pred = "SELECT * FROM " + table_->GetName() + " WHERE " + pred_copy->expr_name_;
  //   query_read_set_mgr->ExtendPredicate(full_pred);
  // }
}


// Transfer a list of equality predicate to a expression tree
expression::AbstractExpression *IndexScanExecutor::ColumnsValuesToExpr(
    const std::vector<oid_t> &predicate_column_ids,
    const std::vector<type::Value> &values, size_t idx) 
{
  if (idx + 1 == predicate_column_ids.size())
    return ColumnValueToCmpExpr(predicate_column_ids[idx], values[idx]);

  // recursively build the expression tree
  expression::AbstractExpression *lexpr = ColumnValueToCmpExpr(predicate_column_ids[idx], values[idx]),
                                 *rexpr = ColumnsValuesToExpr(predicate_column_ids, values, idx + 1);

  expression::AbstractExpression *root_expr = new expression::ConjunctionExpression(ExpressionType::CONJUNCTION_AND, lexpr, rexpr);

  root_expr->DeduceExpressionType();
  return root_expr;
}

expression::AbstractExpression * IndexScanExecutor::ColumnValueToCmpExpr(const oid_t column_id, const type::Value &value) {
  expression::AbstractExpression *lexpr = new expression::TupleValueExpression("");
  reinterpret_cast<expression::TupleValueExpression *>(lexpr)->SetValueType(table_->GetSchema()->GetColumn(column_id).GetType());
  reinterpret_cast<expression::TupleValueExpression *>(lexpr)->SetValueIdx(column_id);
  reinterpret_cast<expression::TupleValueExpression *>(lexpr)->SetColName(table_->GetSchema()->GetColumn(column_id).GetName());

  expression::AbstractExpression *rexpr = new expression::ConstantValueExpression(value);

  expression::AbstractExpression *root_expr = new expression::ComparisonExpression(ExpressionType::COMPARE_EQUAL, lexpr, rexpr);

  root_expr->DeduceExpressionType();
  return root_expr;
}

void IndexScanExecutor::ResetState() {
  result_.clear();

  result_itr_ = START_OID;

  done_ = false;

  const planner::IndexScanPlan &node = GetPlanNode<planner::IndexScanPlan>();

  left_open_ = node.GetLeftOpen();

  right_open_ = node.GetRightOpen();
}

} // namespace executor
} // namespace peloton
