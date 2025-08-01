//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// seq_scan_executor.cpp
//
// Identification: src/executor/seq_scan_executor.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "../executor/seq_scan_executor.h"

#include "../catalog/catalog.h"
#include "../../store/common/table_kv_encoder.h"
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
#include "../planner/create_plan.h"
#include "../storage/data_table.h"
#include "../storage/storage_manager.h"
#include "../storage/tile.h"
#include "../storage/tile_group_header.h"
#include "../type/value_factory.h"
#include "lib/message.h"
#include "store/pequinstore/common.h"
#include "store/pequinstore/query-engine/common/item_pointer.h"
#include "store/pequinstore/query-engine/concurrency/transaction_context.h"
#include "store/pequinstore/query-engine/concurrency/transaction_manager.h"
#include "store/pequinstore/query-engine/planner/attribute_info.h"
#include "store/pequinstore/query-engine/storage/tile_group.h"
#include <memory>
#include <unordered_set>

namespace peloton {
namespace executor {

/**
 * @brief Constructor for seqscan executor.
 * @param node Seqscan node corresponding to this executor.
 */
SeqScanExecutor::SeqScanExecutor(const planner::AbstractPlan *node, ExecutorContext *executor_context)
    : AbstractScanExecutor(node, executor_context) {}

/**
 * @brief Let base class DInit() first, then do mine.
 * @return true on success, false otherwise.
 */
bool SeqScanExecutor::DInit() {
  Debug("Inside seq scan executor");
  auto status = AbstractScanExecutor::DInit();

  if (!status)
    return false;

  // Grab data from plan node.
  const planner::SeqScanPlan &node = GetPlanNode<planner::SeqScanPlan>();

  target_table_ = node.GetTable();

  current_tile_group_offset_ = START_OID;

  old_predicate_ = predicate_;

  already_added_table_col_versions = false;
  first_execution = true;

  lowest_snapshot_frontier = Timestamp(UINT64_MAX); //smallest prepared version that we skip during snapshot materialization.  //Update continously during read. At the end of exec, update TblVersion

  bool is_metadata_table_ = target_table_->GetName().substr(0,3) == "pg_"; //Note: seq_scan never used for meta data tables.
  UW_ASSERT(!is_metadata_table_);

  if (target_table_ != nullptr) {
    table_tile_group_count_ = target_table_->GetTileGroupCount();

    if (column_ids_.empty()) {
      column_ids_.resize(target_table_->GetSchema()->GetColumnCount());
      std::iota(column_ids_.begin(), column_ids_.end(), 0);
    }
  }

  return true;
}

//static bool USE_ACTIVE_READ_SET = true; //If true, then Must use Table_Col_Version
static bool USE_ACTIVE_SNAPSHOT_SET = false; //Currently, our Snapshots are always Complete (non-Active)

void SeqScanExecutor::GetColNames(const expression::AbstractExpression * child_expr, std::unordered_set<std::string> &column_names) {
  if(child_expr == nullptr) return;

  for (size_t i = 0; i < child_expr->GetChildrenSize(); i++) {
    auto child = child_expr->GetChild(i);
    if (dynamic_cast<const expression::TupleValueExpression*>(child) != nullptr) {
      auto tv_expr = dynamic_cast<const expression::TupleValueExpression*>(child);
      column_names.insert(tv_expr->GetColumnName());
    }
    GetColNames(child, column_names);
  }
}

void SeqScanExecutor::CheckRow(ItemPointer head_tuple_location, concurrency::TransactionManager &transaction_manager, concurrency::TransactionContext *current_txn, 
    storage::StorageManager *storage_manager, std::unordered_map<oid_t, std::vector<oid_t>> &position_map) {
  //Check versions of the current row, starting from the freshest version.

  auto tile_group = storage_manager->GetTileGroup(head_tuple_location.block);
  auto tile_group_header = tile_group.get()->GetHeader();
  
  auto const &txn_timestamp = current_txn->GetBasilTimestamp();

  // Pointer to current version in linked list
  ItemPointer tuple_location = head_tuple_location;

   size_t num_iters = 0;

  // Find the right version to read
  bool done = false;

  bool found_committed = false;
  bool found_prepared = false;

 
  //If in snapshot only mode don't need to produce a result. Note: if doing pointQuery DO want the result => FOR NESTED LOOP JOIN WE NEED RESULT.
  //bool snapshot_only_mode = false; 
  bool snapshot_only_mode = !current_txn->GetHasReadSetMgr() && current_txn->GetHasSnapshotMgr() && !current_txn->IsNLJoin(); 
 
  int max_num_reads = 1; 
  int num_reads = 0;

  while(!done) {
    auto tuple_timestamp = tile_group_header->GetBasilTimestamp(tuple_location.offset);

    if (txn_timestamp >= tuple_timestamp) {
      // Within the range of the txn timestamp
      bool read_curr_version = false;
      done = FindRightRowVersion(txn_timestamp, tile_group, tile_group_header, tuple_location, num_iters, current_txn, read_curr_version, found_committed, found_prepared);

      if (read_curr_version && ++num_reads <= max_num_reads && !snapshot_only_mode) { //if the current version is readable, and we are in read mode: Evaluate Read
        EvalRead(tile_group, tile_group_header, tuple_location, current_txn, position_map);
      }

      //If we are in read_from_snapshot mode, and we skip a read (i.e. done = false), update the min_snapshot_frontier. Note: Ignore tuples that a force materialized (they are effectively invisible)
      if(!done && current_txn->GetSnapshotRead()){ //bool perform_read_on_snapshot = true
        UW_ASSERT(!found_committed && !found_prepared); // in read_on_snapshot mode done should be true as soon as found_prepared or found_committed becomes true.
        if(!tile_group_header->GetMaterialize(tuple_location.offset)){
            Timestamp const &skipped_timestamp = tile_group_header->GetBasilTimestamp(tuple_location.offset);
            lowest_snapshot_frontier = std::min(lowest_snapshot_frontier, skipped_timestamp);
        }
      }

    }

    if (done) break;

    //Move on to next oldest version.
    ItemPointer old_location = tuple_location;
    tuple_location = tile_group_header->GetNextItemPointer(old_location.offset);
    if (tuple_location.IsNull()){
      done = true;
      break;
    }

    tile_group = storage_manager->GetTileGroup(tuple_location.block);
    tile_group_header = tile_group->GetHeader();
  }
}

void SeqScanExecutor::EvalRead(std::shared_ptr<storage::TileGroup> tile_group, storage::TileGroupHeader *tile_group_header, ItemPointer tuple_location, 
    concurrency::TransactionContext *current_txn, std::unordered_map<oid_t, std::vector<oid_t>> &position_map) {
  bool eval = true; //True if Row version fulfills ReadPredicate, False otherwise.

  if (tile_group_header->IsDeleted(tuple_location.offset)) {
      Debug("Tuple is deleted so will not include in result");
      eval = false;
  }
  else if (predicate_ != nullptr) {
    ContainerTuple<storage::TileGroup> tuple(tile_group.get(), tuple_location.offset);
    eval = predicate_->Evaluate(&tuple, nullptr, executor_context_).IsTrue();
  }

  if (eval) { //If pass read predicate: Add to set of rows to be included in the result
    // Add to position map
    if (position_map.find(tuple_location.block) == position_map.end()) {
      position_map[tuple_location.block] = std::vector<oid_t>();
    }
    position_map[tuple_location.block].push_back(tuple_location.offset); 

  
    // If active read set then add to read
    if(catalog::Catalog::GetInstance()->GetQueryParams()->useActiveReadSet) ManageReadSet(current_txn, tile_group, tile_group_header, tuple_location, current_txn->GetQueryReadSetMgr());
    //if (USE_ACTIVE_READ_SET) ManageReadSet(current_txn, tile_group, tile_group_header, tuple_location, current_txn->GetQueryReadSetMgr());
  }
  // If not using active read set, always add to read set
  if(!catalog::Catalog::GetInstance()->GetQueryParams()->useActiveReadSet) ManageReadSet(current_txn, tile_group, tile_group_header, tuple_location, current_txn->GetQueryReadSetMgr());
  //if (!USE_ACTIVE_READ_SET) ManageReadSet(current_txn, tile_group, tile_group_header, tuple_location, current_txn->GetQueryReadSetMgr());
}

void SeqScanExecutor::ManageSnapshot(storage::TileGroupHeader *tile_group_header, ItemPointer tuple_location, Timestamp const &timestamp, 
    pequinstore::SnapshotManager *snapshot_mgr) {

  auto txn_digest = tile_group_header->GetTxnDig(tuple_location.offset);
  bool commit_or_prepare = tile_group_header->GetCommitOrPrepare(tuple_location.offset);
  if (txn_digest != nullptr) {
    snapshot_mgr->AddToLocalSnapshot(*txn_digest.get(), timestamp.getTimestamp(), timestamp.getID(), commit_or_prepare);
  }
}

void SeqScanExecutor::ManageReadSet(concurrency::TransactionContext *current_txn, std::shared_ptr<storage::TileGroup> tile_group, storage::TileGroupHeader *tile_group_header, 
    ItemPointer location, pequinstore::QueryReadSetMgr *query_read_set_mgr) {

  // Don't create read set if query is executed in snapshot only mode
  // Or if metadata table
  bool is_metadata = target_table_->GetName().substr(0,3) == "pg_";
  if (current_txn->GetHasReadSetMgr() && !is_metadata) {
    auto &primary_index_columns = target_table_->GetIndex(0)->GetMetadata()->GetKeyAttrs();  //TODO- MAY WANT TO TAKE FROM TABLE REGISTRY INSTEAD
    auto query_read_set_mgr = current_txn->GetQueryReadSetMgr();
   
    ContainerTuple<storage::TileGroup> row(tile_group.get(), location.offset);
    std::vector<std::string> primary_key_cols;

    for (auto col : primary_index_columns) {
      auto val = row.GetValue(col);
      primary_key_cols.push_back(val.ToString());
    }

    const Timestamp &time = tile_group_header->GetBasilTimestamp(location.offset); 

    std::string &&encoded = EncodeTableRow(target_table_->GetName(), primary_key_cols);
    Debug("encoded read set key is: %s. Version: [%lu: %lu]", encoded.c_str(), time.getTimestamp(), time.getID());

    query_read_set_mgr->AddToReadSet(std::move(encoded), time);

    //If prepared: Additionally set Dependency
    if (!tile_group_header->GetCommitOrPrepare(location.offset)) {
      if (tile_group_header->GetTxnDig(location.offset) == nullptr) Panic("Dep Digest is null");
      query_read_set_mgr->AddToDepSet(*tile_group_header->GetTxnDig(location.offset), time);
    }
  }
}

bool SeqScanExecutor::FindRightRowVersion(const Timestamp &txn_timestamp, std::shared_ptr<storage::TileGroup> tile_group, storage::TileGroupHeader *tile_group_header,
    ItemPointer tuple_location, size_t &num_iters, concurrency::TransactionContext *current_txn, bool &read_curr_version, bool &found_committed, bool &found_prepared) {

  // Shorthand for table store interface functions

  bool perform_find_snapshot = current_txn->GetHasSnapshotMgr();
  auto snapshot_mgr = current_txn->GetSnapshotMgr();

  //Note: For find snapshot only mode we also want to read and produce a result (necessary for nested joins), but we will not record a readset
  bool perform_read = current_txn->GetHasReadSetMgr() || (perform_find_snapshot && !current_txn->IsNLJoin()); 
 
  bool perform_read_on_snapshot = current_txn->GetSnapshotRead();
  auto snapshot_set = current_txn->GetSnapshotSet();
  UW_ASSERT(!perform_read_on_snapshot || perform_read); //if read on snapshot, must be performing read.

 
  UW_ASSERT(!(perform_read_on_snapshot && perform_find_snapshot)); //shouldn't do both simultaneously currently

  auto txn_digest = tile_group_header->GetTxnDig(tuple_location.offset);
  bool done = false;


  // If reading.
  if (perform_read) {
    // Three cases: Tuple is committed, or prepared. If prepared, it might have been forceMaterialized
    
    // If the tuple is committed read the version.    Try to read committed always -- whether it is eager or snapshot_read
    if (tile_group_header->GetCommitOrPrepare(tuple_location.offset)) {
      found_committed = true;
      read_curr_version = true;
      done = true;
    } 
    else if(!found_prepared)  { //tuple is prepared, and we haven't read a prepared one yet.
      if (!perform_read_on_snapshot){   //if doing eager read
         // Don't read materialized 
        if(tile_group_header->GetMaterialize(tuple_location.offset)) {
          Debug("Don't read force materialized, continue reading");
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
        }
        //if we are performing snapshot, continue scanning up to k versions. Otherwise, return on first read.
        if(!perform_find_snapshot) done = true;
      }
    }
  }

  // If calling read from snapshot 
  if (perform_read_on_snapshot) {
    //Note: Read committed already handled by normal read case above.
    // Read regardless if it's prepared or force materialized as long as it's in the snapshot
    bool should_read_from_snapshot = !found_committed && snapshot_set->find(*txn_digest.get()) != snapshot_set->end();

    if (should_read_from_snapshot) {
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
      ManageSnapshot(tile_group_header, tuple_location, committed_timestamp, snapshot_mgr);
      done = true;
    } else {

      //Note: this check is technically redundant: FindSnapshot currently also always causes PerformRead to be triggered.
      if(tile_group_header->GetMaterialize(tuple_location.offset)) {
          Debug("Don't add force materialized to snapshot, continue reading"); //Panic("Nothing should be force Materialized in current test");
          return done;
      }

      auto const &read_prepared_pred = current_txn->GetReadPreparedPred();
      Timestamp const &prepared_timestamp = tile_group_header->GetBasilTimestamp(tuple_location.offset);
      // Add to snapshot if tuple satisfies read prepared predicate and haven't read more than k versions
      bool should_add_to_snapshot = !found_committed && num_iters < current_txn->GetKPreparedVersions() && read_prepared_pred && read_prepared_pred(*txn_digest);

      if (should_add_to_snapshot) {
        //currently only count "readable" (meets predicate) versions towards snapshotK. This is so that we read at lest one committed if there is >= k unreadable prepared
        ManageSnapshot(tile_group_header, tuple_location, prepared_timestamp, snapshot_mgr);
        num_iters++;
      }

      // We are done if we read k prepared versions
      done = num_iters >= current_txn->GetKPreparedVersions();
    }
  }

  return done;
}

void SeqScanExecutor::PrepareResult(std::unordered_map<oid_t, std::vector<oid_t>> &position_map) {
  if (position_map.size() > 0) {
    for (auto &pair : position_map) {
      current_tile_group_offset_ = pair.first;
      Debug("The block is %d", pair.first);
      for (size_t i = 0; i < pair.second.size(); i++) {
        Debug("The oid_t is %d", pair.second[i]);
      }
      std::unique_ptr<LogicalTile> logical_tile(LogicalTileFactory::GetTile());
      auto tile_group = this->target_table_->GetTileGroupById(pair.first);
      logical_tile->AddColumns(tile_group, column_ids_);
      logical_tile->AddPositionList(std::move(pair.second));
      // SetOutput(logical_tile.release());
      result_.push_back(logical_tile.release());
    }
  }
}


void SeqScanExecutor::SetTableColVersions(concurrency::TransactionContext *current_txn, pequinstore::QueryReadSetMgr *query_read_set_mgr, const Timestamp &current_txn_timestamp){
  if(!already_added_table_col_versions){
     
    //UW_ASSERT(!is_metadata_table_);
    if (current_txn->CheckPredicatesInitialized()) {
      Debug("Set Read Table/Col versions");

      //shorthands
      bool get_read_set = current_txn->GetHasReadSetMgr();
      bool find_snapshot = current_txn->GetHasSnapshotMgr();
      auto ss_mgr = current_txn->GetSnapshotMgr();
      bool perform_read_on_snapshot = current_txn->GetSnapshotRead();
      auto snapshot_set = current_txn->GetSnapshotSet();

      // Read table version and table col versions
      current_txn->GetTableVersion()(target_table_->GetName(), current_txn_timestamp, get_read_set, query_read_set_mgr, find_snapshot, ss_mgr, perform_read_on_snapshot, snapshot_set);
      
       //If Scanning (Non_active read set), then don't need to include ColVersions in ActiveReadSet. Changes to index could not be affecting the observed read set.
      //However, if we use Active Read set, then the read_set is only the keys that hit the predicate. 
      //Thus we need the ColVersion to detect changes to col values that might be relevant to ActiveRS (i.e. include ColVersion for all Col in search predicate)
      
      if(catalog::Catalog::GetInstance()->GetQueryParams()->useColVersions && catalog::Catalog::GetInstance()->GetQueryParams()->useActiveReadSet){
        // Table column version : FIXME: Read version per Col, not composite key
        std::unordered_set<std::string> column_names;
        //std::vector<std::string> col_names;
        GetColNames(predicate_, column_names);
        if(predicate_ != nullptr) std::cerr << "pred: " << predicate_->GetInfo() << std::endl;

        for (auto &col : column_names) {
          Debug("Col name is: %s", col.c_str());
          current_txn->GetTableVersion()(EncodeTableCol(target_table_->GetName(), col), current_txn_timestamp, get_read_set, query_read_set_mgr, find_snapshot, ss_mgr, perform_read_on_snapshot, snapshot_set);
          //col_names.push_back(col);
        }
      }
    }
  }
  already_added_table_col_versions = true;
}

void SeqScanExecutor::RefineTableColVersions(concurrency::TransactionContext *current_txn, pequinstore::QueryReadSetMgr *query_read_set_mgr){
    //If we are in perform_read_on_snapshot mode: update TableVersion.
    if(!current_txn->GetSnapshotRead()) return;

    UW_ASSERT(current_txn->GetSqlInterpreter()); //should be set for Snapshot read
    auto query_params = current_txn->GetSqlInterpreter()->GetQueryParams();
   
    //if(query_params->useSemanticCC) 
    query_read_set_mgr->RefinePredicateTableVersion(lowest_snapshot_frontier, query_params->monotonicityGrace);
}

void SeqScanExecutor::SetPredicate(concurrency::TransactionContext *current_txn, pequinstore::QueryReadSetMgr *query_read_set_mgr){

  if(!current_txn->GetHasReadSetMgr()) return;

  if (!current_txn->CheckPredicatesInitialized()) return;

  if(first_execution){
     /* Reserve a new predicate in the readset manager */
    query_read_set_mgr->AddPredicate(target_table_->GetName());
    first_execution = false;
  }

  //FIXME: must copy?  //   auto pred_copy = predicate_->Copy();
    //  auto pred_copy = predicate_->Copy();
    // pred_copy->DeduceExpressionName();
  const_cast<peloton::expression::AbstractExpression *>(predicate_)->DeduceExpressionName();
  auto &pred = predicate_->expr_name_;
  query_read_set_mgr->ExtendPredicate(pred);
  Debug("Adding new read set predicate instance: %s ", pred);

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

void SeqScanExecutor::Scan() {
  concurrency::TransactionManager &transaction_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto current_txn = executor_context_->GetTransaction();
  if (current_txn->IsPointRead()) {
     if(predicate_ != nullptr) std::cerr << "pred: " << predicate_->GetInfo() << std::endl;
    Panic("Point Reads should always go through Index Scan! Table: %s", target_table_->GetName().c_str());
  }
  UW_ASSERT(!current_txn->IsPointRead());//NOTE: PointReads should always go through IndexScan

  auto storage_manager = storage::StorageManager::GetInstance();

  auto const &current_txn_timestamp = current_txn->GetBasilTimestamp();
  auto query_read_set_mgr = current_txn->GetQueryReadSetMgr();

  //Warning("Txn [%lu:%lu] going through SequentialScan", current_txn_timestamp.getTimestamp(), current_txn_timestamp.getID());

  std::unordered_map<oid_t, std::vector<oid_t>> position_map;

  SetPredicate(current_txn, query_read_set_mgr);
  SetTableColVersions(current_txn, query_read_set_mgr, current_txn_timestamp);


  // Iterate through each linked list per row
  for (auto indirection_array : target_table_->active_indirection_arrays_) {
    int indirection_counter = indirection_array->indirection_counter_;
    for (int offset = 0; offset < indirection_counter; offset++) {
      //std::cerr << "Offset is " << offset << std::endl;
      ItemPointer *head = indirection_array->GetIndirectionByOffset(offset);
      if (head == nullptr) {
        // return false;
      }
      auto location = *head;
      CheckRow(location, transaction_manager, current_txn, storage_manager, position_map);
    }
  }

  RefineTableColVersions(current_txn, query_read_set_mgr); //Refine TableVersion if reading from materialized snapshot

  PrepareResult(position_map);
  done_ = true;

}

void SeqScanExecutor::OldScan() {
  concurrency::TransactionManager &transaction_manager = concurrency::TransactionManagerFactory::GetInstance();

  bool acquire_owner = GetPlanNode<planner::AbstractScan>().IsForUpdate();
  auto current_txn = executor_context_->GetTransaction();

  // NEW: calculate encoded key for the read set
  auto primary_index_columns_ = target_table_->GetIndex(0)->GetMetadata()->GetKeyAttrs();
  bool has_read_set_mgr = current_txn->GetHasReadSetMgr();
  auto query_read_set_mgr = current_txn->GetQueryReadSetMgr();

  bool perform_find_snapshot = current_txn->GetHasSnapshotMgr();
  auto snapshot_mgr = current_txn->GetSnapshotMgr();
  size_t k_prepared_versions = current_txn->GetKPreparedVersions();

  bool perform_read_on_snapshot = current_txn->GetSnapshotRead();
  auto snapshot_set = current_txn->GetSnapshotSet();

  std::vector<oid_t> position_list;
  std::set<oid_t> position_set;
  std::unordered_map<oid_t, std::vector<oid_t>> position_map;

  /*std::vector<ItemPointer> position_list;
  std::set<ItemPointer> position_set;*/

  auto storage_manager = storage::StorageManager::GetInstance();
  auto timestamp = current_txn->GetBasilTimestamp();

  ////////////////////////////////////  TAKE TABLE VERSION / TABLE-COL VERSION/////////////////////////////////////////////////////////////////

  // Read table version and table col versions
 
  current_txn->GetTableVersion()(target_table_->GetName(), timestamp, has_read_set_mgr, query_read_set_mgr, perform_find_snapshot, snapshot_mgr, perform_read_on_snapshot, snapshot_set);

  //NOTE: Don't need TableColVersion when doing seq_scan  //TODO: Need them to safe guar

  //NOTE: Don't need TableColVersion when doing seq_scan  //TODO: Need them to safe guard Active Reads if we don't have SemanticCC
    // // Table column version 
    // std::unordered_set<std::string> column_names;
    // //std::vector<std::string> col_names;
    // GetColNames(predicate_, column_names);

    // for (auto &col : column_names) {
    //   std::cerr << "Col name is " << col << std::endl;
    //   current_txn->GetTableVersion()(EncodeTableCol(target_table_->GetName(), col), timestamp, current_txn->GetHasReadSetMgr(), query_read_set_mgr, current_txn->GetHasSnapshotMgr(), current_txn->GetSnapshotMgr());
    //   //col_names.push_back(col);
    // }

  /////////////////////////////////////////////////////////////////////////////////////////////////////////

  ItemPointer location_copy;
  for (auto indirection_array : target_table_->active_indirection_arrays_) {
    int indirection_counter = indirection_array->indirection_counter_;
    for (int offset = 0; offset < indirection_counter; offset++) {
      //std::cerr << "Offset is " << offset << std::endl;
      ItemPointer *head = indirection_array->GetIndirectionByOffset(offset);
      if (head == nullptr) {
        // return false;
      }

      auto head_tile_group_header = storage_manager->GetTileGroup(head->block)->GetHeader();

      auto tuple_timestamp = head_tile_group_header->GetBasilTimestamp(head->offset);
      auto location = *head;
      auto tile_group_header = head_tile_group_header;
      auto curr_tuple_id = location.offset;
      location_copy = location;
      bool materialize = tile_group_header->GetMaterialize(curr_tuple_id);

      Debug("Head timestamp: [%lu: %lu]", tuple_timestamp.getTimestamp(), tuple_timestamp.getID());

      // Now we find the appropriate version to read that's less than the timestamp by traversing the next pointers (I.e. find the last version with writeTS < curr.readTS)
              //Note: If the row is only "materialized" (not prepared/committed), and we are not performing a snapshot read, then don't read it.
      while (tuple_timestamp > timestamp || (!perform_read_on_snapshot && materialize)) {
        // Get the previous version in the linked list
        ItemPointer new_location = tile_group_header->GetNextItemPointer(curr_tuple_id);

        // Get the associated tile group header so we can find the timestamp
        if (new_location.IsNull()) {
          // std::cerr << "New location is null" << std::endl;
          break;
        }

        auto new_tile_group_header = storage_manager->GetTileGroup(new_location.block)->GetHeader();
        // Update the timestamp
        tuple_timestamp = new_tile_group_header->GetBasilTimestamp(new_location.offset);
        location = new_location;
        tile_group_header = new_tile_group_header;
        curr_tuple_id = new_location.offset;
        materialize = new_tile_group_header->GetMaterialize(curr_tuple_id);
      }

      Debug( "location timestamp is: [%lu:%lu]", tile_group_header->GetBasilTimestamp(location.offset).getTimestamp(),  tile_group_header->GetBasilTimestamp(location.offset).getID());
      // std::cerr << "Location timestamp is " <<
      // tile_group_header->GetBasilTimestamp(location.offset).getTimestamp()
      // << std::endl; std::cerr << "Current tuple id is " << curr_tuple_id <<
      // std::endl;
      //

      bool is_deleted = tile_group_header->IsDeleted(curr_tuple_id);
      auto tile_group = target_table_->GetTileGroupById(location.block);
      bool found_committed = false;
      size_t num_iters = 0;
      bool read_completed = false;


      /*visibility = transaction_manager.IsVisible(
          current_txn, tile_group_header, curr_tuple_id);*/
      // check transaction visibility
      if (!is_deleted) {
        // if the tuple is visible, then perform predicate evaluation.
        if (predicate_ == nullptr) {
          // if (position_set.find(curr_tuple_id) == position_set.end()) {
          position_list.push_back(curr_tuple_id);
          position_set.insert(curr_tuple_id);

          auto committed = tile_group_header->GetCommitOrPrepare(curr_tuple_id);
          auto txn_dig = tile_group_header->GetTxnDig(curr_tuple_id);
          //Read if: a) this is not a read from snapshot, or b) it is, but it is not in the snapshot set, cor ommitted (and thus should be read anyways)
          bool should_read = !perform_read_on_snapshot || committed || snapshot_set->find(*txn_dig.get()) != snapshot_set->end();

          if (should_read) {
            if (position_map.find(location.block) == position_map.end()) {
              position_map[location.block] = std::vector<oid_t>();
            }

            position_map[location.block].push_back(location.offset);
            read_completed = true;
          }
          //TODO: if !should_read, return early.

          ContainerTuple<storage::TileGroup> row(tile_group.get(), curr_tuple_id);

          // std::string encoded_key = target_table_->GetName();
          std::vector<std::string> primary_key_cols;
          for (auto col : primary_index_columns_) {
            auto val = row.GetValue(col);
            // encoded_key = encoded_key + "///" + val.ToString();
            primary_key_cols.push_back(val.ToString());
            // primary_key_cols.push_back(val.GetAs<const char*>());
            // std::cerr << "read set value is " << val.ToString() << std::endl;
          }

          const Timestamp &time = tile_group_header->GetBasilTimestamp(location.offset); 

          std::string &&encoded = EncodeTableRow(target_table_->GetName(), primary_key_cols);
          Debug("encoded read set key is: %s. Version: [%lu: %lu]", encoded.c_str(), time.getTimestamp(), time.getID());

          if (has_read_set_mgr && should_read) {
            query_read_set_mgr->AddToReadSet(std::move(encoded), time);
          }

          if (perform_find_snapshot) {
            auto txn_digest = tile_group_header->GetTxnDig(curr_tuple_id);
            auto commit_or_prepare = tile_group_header->GetCommitOrPrepare(curr_tuple_id);
            found_committed = commit_or_prepare;


            if (!commit_or_prepare) {
              auto const &read_prepared_pred = current_txn->GetReadPreparedPred();

              if (read_prepared_pred) {
                if (txn_digest != nullptr && read_prepared_pred(*txn_digest)) {
                  snapshot_mgr->AddToLocalSnapshot(*txn_digest.get(), timestamp.getTimestamp(), timestamp.getID(), commit_or_prepare);
                  num_iters++;
                }
              }
            } else {
              // If committed don't need to check the read_prepared_pred
              if (txn_digest != nullptr) {
                snapshot_mgr->AddToLocalSnapshot(*txn_digest.get(), time.getTimestamp(), time.getID(), commit_or_prepare);
                num_iters++;
              }
            }

          }

          if (!tile_group_header->GetCommitOrPrepare(curr_tuple_id)) {
            if (tile_group_header->GetTxnDig(curr_tuple_id) != nullptr) {
              query_read_set_mgr->AddToDepSet(*tile_group_header->GetTxnDig(curr_tuple_id), time);
            } else {
              Panic("Txn Dig null");
            }
          }

          // logical_tile->AddEntryReadSet(encoded, time);
          //}
          // position_list.push_back(curr_tuple_id);
          auto res = transaction_manager.PerformRead(current_txn, location, tile_group_header, acquire_owner);
          // Since CC is done at Basil level res should always be true
          res = true;
          if (!res) {
            transaction_manager.SetTransactionResult(current_txn, ResultType::FAILURE);
            // return res;
          }
        } else {
          ContainerTuple<storage::TileGroup> tuple(tile_group.get(), curr_tuple_id);
          LOG_TRACE("Evaluate predicate for a tuple");
          auto eval = predicate_->Evaluate(&tuple, nullptr, executor_context_);
          LOG_TRACE("Evaluation result: %s", eval.GetInfo().c_str());
          if (eval.IsTrue()) {
            // if (position_set.find(curr_tuple_id) == position_set.end()) {
            position_list.push_back(curr_tuple_id);
            position_set.insert(curr_tuple_id);


            auto committed = tile_group_header->GetCommitOrPrepare(curr_tuple_id);
            auto txn_dig = tile_group_header->GetTxnDig(curr_tuple_id);
            bool should_read = !perform_read_on_snapshot || committed || snapshot_set->find(*txn_dig.get()) != snapshot_set->end();

            if (should_read) {
              if (position_map.find(location.block) == position_map.end()) {
                Debug("Created new vector for new block in tile group");
                position_map[location.block] = std::vector<oid_t>();
              }

              position_map[location.block].push_back(location.offset);
              read_completed = true;
              Debug("Location is %d, %d", location.block, location.offset);
            }
            ContainerTuple<storage::TileGroup> row(tile_group.get(),
                                                   curr_tuple_id);
            std::vector<std::string> primary_key_cols;
            // std::string encoded_key = target_table_->GetName();
            for (auto col : primary_index_columns_) {
              auto val = row.GetValue(col);
              // encoded_key = encoded_key + "///" + val.ToString();
              primary_key_cols.push_back(val.ToString());
              // Debug("Read set value: %s", val.ToString().c_str());
              //  std::cerr << "read set value is " << val.ToString() << std::endl;
            }
            const Timestamp &time = tile_group_header->GetBasilTimestamp(location.offset); 
            // logical_tile->AddToReadSet(std::tie(encoded_key, time));

            // for (unsigned int i = 0; i < primary_key_cols.size(); i++) {
            //   std::cerr << "Primary key columns are " <<
            //   primary_key_cols[i] << std::endl;
            // }
            std::string &&encoded = EncodeTableRow(target_table_->GetName(), primary_key_cols);
            Debug("encoded read set key is: %s. Version: [%lu: %lu]",  encoded.c_str(), time.getTimestamp(), time.getID());
            // std::cerr << "Encoded key from read set is " << encoded << std::endl;
            // TimestampMessage ts_message = TimestampMessage();
            // ts_message.set_id(time.getID());
            // ts_message.set_timestamp(time.getTimestamp());
            //query_read_set_mgr->AddToReadSet(std::move(encoded), time);
            
            if (has_read_set_mgr && should_read) {
              query_read_set_mgr->AddToReadSet(std::move(encoded), time);
            }

            if (perform_find_snapshot) {
              auto txn_digest = tile_group_header->GetTxnDig(curr_tuple_id);
              auto commit_or_prepare = tile_group_header->GetCommitOrPrepare(curr_tuple_id);
              found_committed = commit_or_prepare;


              if (!commit_or_prepare) {
                auto const &read_prepared_pred = current_txn->GetReadPreparedPred();

                if (read_prepared_pred) {
                  if (txn_digest != nullptr && read_prepared_pred(*txn_digest)) {
                    snapshot_mgr->AddToLocalSnapshot(*txn_digest.get(), timestamp.getTimestamp(), timestamp.getID(), commit_or_prepare);
                    num_iters++;
                  }
                }
              } else {
                // If committed don't need to check the read_prepared_pred
                if (txn_digest != nullptr) {
                  snapshot_mgr->AddToLocalSnapshot(*txn_digest.get(), time.getTimestamp(), time.getID(), commit_or_prepare);
                  num_iters++;
                }
              }
            }

            if (!tile_group_header->GetCommitOrPrepare(curr_tuple_id)) {
              if (tile_group_header->GetTxnDig(curr_tuple_id) != nullptr) {
                query_read_set_mgr->AddToDepSet(
                    *tile_group_header->GetTxnDig(curr_tuple_id), time);
              } else {
                Panic("Txn Dig null");
              }
            }

            // logical_tile->AddEntryReadSet(encoded_key, time);
            //}
            // position_list.push_back(curr_tuple_id);
            auto res = transaction_manager.PerformRead(
                current_txn, location, tile_group_header, acquire_owner);
            // Since CC is done at Basil level res shoud always be true
            res = true;
            if (!res) {
              transaction_manager.SetTransactionResult(current_txn,
                                                       ResultType::FAILURE);
              // return res;
            } else {
              LOG_TRACE("Sequential Scan Predicate Satisfied");
            }
          }
        }
      }

      while (perform_read_on_snapshot && !read_completed) {
        location = tile_group_header->GetNextItemPointer(curr_tuple_id);

        if (location.IsNull()) {
          break;
        }

        tile_group = target_table_->GetTileGroupById(location.block);
        tile_group_header = tile_group->GetHeader();
        curr_tuple_id = location.offset;


        is_deleted = tile_group_header->IsDeleted(curr_tuple_id);

        // if the tuple is visible, then perform predicate evaluation.
        if (predicate_ == nullptr && !is_deleted) {
          // if (position_set.find(curr_tuple_id) == position_set.end()) {
          position_list.push_back(curr_tuple_id);
          position_set.insert(curr_tuple_id);

          auto committed = tile_group_header->GetCommitOrPrepare(curr_tuple_id);
          auto txn_dig = tile_group_header->GetTxnDig(curr_tuple_id);
          bool should_read = !perform_read_on_snapshot || committed || snapshot_set->find(*txn_dig.get()) != snapshot_set->end();

          if (should_read) {
            if (position_map.find(location.block) == position_map.end()) {
              position_map[location.block] = std::vector<oid_t>();
            }

            position_map[location.block].push_back(location.offset);
            read_completed = true;
          }

          ContainerTuple<storage::TileGroup> row(tile_group.get(),
                                                 curr_tuple_id);
          // std::string encoded_key = target_table_->GetName();
          std::vector<std::string> primary_key_cols;
          for (auto col : primary_index_columns_) {
            auto val = row.GetValue(col);
            // encoded_key = encoded_key + "///" + val.ToString();
            primary_key_cols.push_back(val.ToString());
            // primary_key_cols.push_back(val.GetAs<const char*>());
            // std::cerr << "read set value is " << val.ToString()
            //           << std::endl;
          }

          const Timestamp &time = tile_group_header->GetBasilTimestamp(
              location.offset); // TODO: remove copy

          std::string &&encoded =
              EncodeTableRow(target_table_->GetName(), primary_key_cols);
          Debug("encoded read set key is: %s. Version: [%lu: %lu]",
                encoded.c_str(), time.getTimestamp(), time.getID());

          if (has_read_set_mgr && should_read) {
            query_read_set_mgr->AddToReadSet(std::move(encoded), time);
          }

          if (!tile_group_header->GetCommitOrPrepare(curr_tuple_id)) {
            if (tile_group_header->GetTxnDig(curr_tuple_id) != nullptr) {
              query_read_set_mgr->AddToDepSet(
                  *tile_group_header->GetTxnDig(curr_tuple_id), time);
            } else {
              Panic("Txn Dig null");
            }
          }

          auto res = transaction_manager.PerformRead(
              current_txn, location, tile_group_header, acquire_owner);
          // Since CC is done at Basil level res should always be true
          res = true;
          if (!res) {
            transaction_manager.SetTransactionResult(current_txn,
                                                     ResultType::FAILURE);
          }
        } else if (!is_deleted) {
          ContainerTuple<storage::TileGroup> tuple(tile_group.get(),
                                                   curr_tuple_id);
          LOG_TRACE("Evaluate predicate for a tuple");
          auto eval = predicate_->Evaluate(&tuple, nullptr, executor_context_);
          LOG_TRACE("Evaluation result: %s", eval.GetInfo().c_str());
          if (eval.IsTrue()) {
            // if (position_set.find(curr_tuple_id) == position_set.end()) {
            position_list.push_back(curr_tuple_id);
            position_set.insert(curr_tuple_id);


            auto committed = tile_group_header->GetCommitOrPrepare(curr_tuple_id);
            auto txn_dig = tile_group_header->GetTxnDig(curr_tuple_id);
            bool should_read = !perform_read_on_snapshot || committed || snapshot_set->find(*txn_dig.get()) != snapshot_set->end();

            if (should_read) {
              if (position_map.find(location.block) == position_map.end()) {
                Debug("Created new vector for new block in tile group");
                position_map[location.block] = std::vector<oid_t>();
              }

              position_map[location.block].push_back(location.offset);
              read_completed = true;
              Debug("Location is %d, %d", location.block, location.offset);
            }
            ContainerTuple<storage::TileGroup> row(tile_group.get(),
                                                   curr_tuple_id);
            std::vector<std::string> primary_key_cols;
            // std::string encoded_key = target_table_->GetName();
            for (auto col : primary_index_columns_) {
              auto val = row.GetValue(col);
              // encoded_key = encoded_key + "///" + val.ToString();
              primary_key_cols.push_back(val.ToString());
              // Debug("Read set value: %s", val.ToString().c_str());
              //  std::cerr << "read set value is " << val.ToString() <<
              //  std::endl;
            }
            const Timestamp &time = tile_group_header->GetBasilTimestamp(
                location.offset); // TODO: remove copy
            // logical_tile->AddToReadSet(std::tie(encoded_key, time));

            // for (unsigned int i = 0; i < primary_key_cols.size(); i++) {
            //   std::cerr << "Primary key columns are " <<
            //   primary_key_cols[i] << std::endl;
            // }
            std::string &&encoded =
                EncodeTableRow(target_table_->GetName(), primary_key_cols);
            Debug("encoded read set key is: %s. Version: [%lu: %lu]",
                  encoded.c_str(), time.getTimestamp(), time.getID());
            // std::cerr << "Encoded key from read set is " << encoded <<
            // std::endl;
            // TimestampMessage ts_message = TimestampMessage();
            // ts_message.set_id(time.getID());
            // ts_message.set_timestamp(time.getTimestamp());
            //query_read_set_mgr->AddToReadSet(std::move(encoded), time);
            
            if (has_read_set_mgr && should_read) {
              query_read_set_mgr->AddToReadSet(std::move(encoded), time);
            }

            if (!tile_group_header->GetCommitOrPrepare(curr_tuple_id)) {
              if (tile_group_header->GetTxnDig(curr_tuple_id) != nullptr) {
                query_read_set_mgr->AddToDepSet(
                    *tile_group_header->GetTxnDig(curr_tuple_id), time);
              } else {
                Panic("Txn Dig null");
              }
            }

            // logical_tile->AddEntryReadSet(encoded_key, time);
            //}
            // position_list.push_back(curr_tuple_id);
            auto res = transaction_manager.PerformRead(
                current_txn, location, tile_group_header, acquire_owner);
            // Since CC is done at Basil level res shoud always be true
            res = true;
            if (!res) {
              transaction_manager.SetTransactionResult(current_txn,
                                                       ResultType::FAILURE);
              // return res;
            } else {
              LOG_TRACE("Sequential Scan Predicate Satisfied");
            }
          }
        }
      }

      // Read the last k prepared or until we reach a committed
      while (perform_find_snapshot && !found_committed && num_iters < k_prepared_versions) {
        location = tile_group_header->GetNextItemPointer(curr_tuple_id);

        if (location.IsNull()) {
          break;
        }

        tile_group = target_table_->GetTileGroupById(location.block);
        tile_group_header = tile_group->GetHeader();
        curr_tuple_id = location.offset;


        is_deleted = tile_group_header->IsDeleted(curr_tuple_id);

        if (!is_deleted) {
          // if the tuple is visible, then perform predicate evaluation.
          if (predicate_ == nullptr) {
            const Timestamp &time = tile_group_header->GetBasilTimestamp(
                location.offset); // TODO: remove copy

            auto txn_digest = tile_group_header->GetTxnDig(curr_tuple_id);
            auto commit_or_prepare = tile_group_header->GetCommitOrPrepare(curr_tuple_id);
            found_committed = commit_or_prepare;

            if (!commit_or_prepare) {
              auto const &read_prepared_pred = current_txn->GetReadPreparedPred();

              if (read_prepared_pred) {
                if (txn_digest != nullptr && read_prepared_pred(*txn_digest)) {
                  snapshot_mgr->AddToLocalSnapshot(*txn_digest.get(), timestamp.getTimestamp(), timestamp.getID(), commit_or_prepare);
                  num_iters++;
                }
              }
            } else {
              if (txn_digest != nullptr) {
                snapshot_mgr->AddToLocalSnapshot(*txn_digest.get(), time.getTimestamp(), time.getID(), commit_or_prepare);
                num_iters++;
              }
            }

          } else {
            ContainerTuple<storage::TileGroup> tuple(tile_group.get(),
                                                    curr_tuple_id);
            LOG_TRACE("Evaluate predicate for a tuple");
            auto eval = predicate_->Evaluate(&tuple, nullptr, executor_context_);
            LOG_TRACE("Evaluation result: %s", eval.GetInfo().c_str());
            if (eval.IsTrue()) {
              const Timestamp &time = tile_group_header->GetBasilTimestamp(
                  location.offset); // TODO: remove copy

              auto txn_digest = tile_group_header->GetTxnDig(curr_tuple_id);
              auto commit_or_prepare = tile_group_header->GetCommitOrPrepare(curr_tuple_id);
              found_committed = commit_or_prepare;

              // If prepared check the read_prepared_pred
              if (!commit_or_prepare) {
                auto const &read_prepared_pred = current_txn->GetReadPreparedPred();

                if (read_prepared_pred) {
                  if (txn_digest != nullptr && read_prepared_pred(*txn_digest)) {
                    snapshot_mgr->AddToLocalSnapshot(*txn_digest.get(), timestamp.getTimestamp(), timestamp.getID(), commit_or_prepare);
                    num_iters++;
                  }
                }
              } else {
                // If committed don't need to check the read_prepared_pred
                if (txn_digest != nullptr) {
                  snapshot_mgr->AddToLocalSnapshot(*txn_digest.get(), time.getTimestamp(), time.getID(), commit_or_prepare);
                  num_iters++;
                }
              }
            } else {
                LOG_TRACE("Sequential Scan Predicate Satisfied");
            }
          }
        }
      }

      // Reset counters
      num_iters = 0;
      found_committed = false;

      /*if (position_list.size() > 0) {
        std::cerr << "Adding to position list" << std::endl;
        std::unique_ptr<LogicalTile> logical_tile(
            LogicalTileFactory::GetTile());
        logical_tile->AddColumns(tile_group, column_ids_);
        logical_tile->AddPositionList(std::move(position_list));
        LOG_TRACE("Information %s", logical_tile->GetInfo().c_str());
        std::cerr << "Before release" << std::endl;
        SetOutput(logical_tile.release());
        std::cerr << "After release" << std::endl;
        // return true;
      }*/

      // LOG_TRACE("Information %s", logical_tile->GetInfo().c_str());
      //  SetOutput(logical_tile.release());
      //  return true;

      // Construct logical tile.
      // std::unique_ptr<LogicalTile>
      // logical_tile(LogicalTileFactory::GetTile());
      // logical_tile->AddColumns(tile_group, column_ids_);
      // logical_tile->AddPositionList(std::move(position_list));
    }
  }

  if (position_map.size() > 0) {
    for (auto &pair : position_map) {
      current_tile_group_offset_ = pair.first;
      Debug("The block is %d", pair.first);
      for (size_t i = 0; i < pair.second.size(); i++) {
        Debug("The oid_t is %d", pair.second[i]);
      }
      std::unique_ptr<LogicalTile> logical_tile(LogicalTileFactory::GetTile());
      auto tile_group = this->target_table_->GetTileGroupById(pair.first);
      logical_tile->AddColumns(tile_group, column_ids_);
      logical_tile->AddPositionList(std::move(pair.second));
      // SetOutput(logical_tile.release());
      result_.push_back(logical_tile.release());
    }
    // return true;
  }
  done_ = true;
}

/**
 * @brief Creates logical tile from tile group and applies scan predicate.
 * @return true on success, false otherwise.
 */
bool SeqScanExecutor::DExecute() {
  // Scanning over a logical tile.
  std::unique_ptr<LogicalTile> logical_tile(LogicalTileFactory::GetTile());
  Debug("Executing seq scan for table: %s", target_table_->GetName().c_str());
  if (children_.size() == 1 &&
      // There will be a child node on the create index scenario,
      // but we don't want to use this execution flow
      !(GetRawNode()->GetChildren().size() > 0 &&
        GetRawNode()->GetChildren()[0].get()->GetPlanNodeType() ==
            PlanNodeType::CREATE &&
        ((planner::CreatePlan *)GetRawNode()->GetChildren()[0].get())
                ->GetCreateType() == CreateType::INDEX)) {
    // FIXME Check all requirements for children_.size() == 0 case.
    LOG_TRACE("Seq Scan executor :: 1 child ");

    PELOTON_ASSERT(target_table_ == nullptr);
    PELOTON_ASSERT(column_ids_.size() == 0);

    while (children_[0]->Execute()) {
      std::unique_ptr<LogicalTile> tile(children_[0]->GetOutput());

      if (predicate_ != nullptr) {
        // Invalidate tuples that don't satisfy the predicate.
        for (oid_t tuple_id : *tile) {
          ContainerTuple<LogicalTile> tuple(tile.get(), tuple_id);
          auto eval = predicate_->Evaluate(&tuple, nullptr, executor_context_);
          if (eval.IsFalse()) {
            // if (predicate_->Evaluate(&tuple, nullptr, executor_context_)
            //        .IsFalse()) {
            tile->RemoveVisibility(tuple_id);
          }
        }
      }

      if (0 == tile->GetTupleCount()) { // Avoid returning empty tiles
        continue;
      }

      /* Hopefully we needn't do projections here */
      SetOutput(tile.release());
      return true;
    }
    return false;
  }
  // Scanning a table
  else if (children_.size() == 0 ||
           // If we are creating an index, there will be a child
           (children_.size() == 1 &&
            // This check is only needed to pass seq_scan_test
            // unless it is possible to add a executor child
            // without a corresponding plan.
            GetRawNode()->GetChildren().size() > 0 &&
            // Check if the plan is what we actually expect.
            GetRawNode()->GetChildren()[0].get()->GetPlanNodeType() ==
                PlanNodeType::CREATE &&
            // If it is, confirm it is for indexes
            ((planner::CreatePlan *)GetRawNode()->GetChildren()[0].get())
                    ->GetCreateType() == CreateType::INDEX)) {
    LOG_TRACE("Seq Scan executor :: 0 child ");

    PELOTON_ASSERT(target_table_ != nullptr);
    PELOTON_ASSERT(column_ids_.size() > 0);
    if (children_.size() > 0 && !index_done_) {
      children_[0]->Execute();
      // This stops continuous executions due to
      // a parent and avoids multiple creations
      // of the same index.
      index_done_ = true;
    }

    if (!done_) {
      Scan();
    }

    while (result_itr_ < result_.size()) { // Avoid returning empty tiles
      if (result_[result_itr_]->GetTupleCount() == 0) {
        //std::cerr << "No tuples in tile" << std::endl;
        result_itr_++;
        continue;
      } else {
        //std::cerr << "Output here for tile " << result_itr_ << std::endl;
        LOG_TRACE("Information %s", result_[result_itr_]->GetInfo().c_str());
        SetOutput(result_[result_itr_]);
        result_itr_++;
        return true;
      }

    } // end while
  }
  return false;
}

// Update Predicate expression this is used in the NLJoin executor
void SeqScanExecutor::UpdatePredicate(const std::vector<oid_t> &column_ids,
                                      const std::vector<type::Value> &values) {
  std::vector<oid_t> predicate_column_ids;

  PELOTON_ASSERT(column_ids.size() <= column_ids_.size());

  // columns_ids is the column id in the join executor, should convert to the column id in the seq scan executor
  for (auto column_id : column_ids) {
    predicate_column_ids.push_back(column_ids_[column_id]);
  }

  expression::AbstractExpression *new_predicate = values.size() != 0 ? ColumnsValuesToExpr(predicate_column_ids, values, 0) : nullptr;

  // combine with original predicate
  if (old_predicate_ != nullptr) {
    expression::AbstractExpression *lexpr = new_predicate, *rexpr = old_predicate_->Copy();

    new_predicate = new expression::ConjunctionExpression(ExpressionType::CONJUNCTION_AND, lexpr, rexpr);
  }

  // Currently a hack that prevent memory leak we should eventually make prediate_ a unique_ptr
  new_predicate_.reset(new_predicate);
  predicate_ = new_predicate;

  // Set the readset manager predicate
  auto current_txn = executor_context_->GetTransaction();
  if (current_txn->GetHasReadSetMgr()) {
    pequinstore::QueryReadSetMgr *query_read_set_mgr = current_txn->GetQueryReadSetMgr();
    auto pred_copy = predicate_->Copy();
    pred_copy->DeduceExpressionName();
        
    std::string full_pred = "SELECT * FROM " + target_table_->GetName() + " WHERE " + pred_copy->expr_name_;
    query_read_set_mgr->ExtendPredicate(full_pred);
  }
}

// Transfer a list of equality predicate to a expression tree
expression::AbstractExpression *SeqScanExecutor::ColumnsValuesToExpr(
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

expression::AbstractExpression * SeqScanExecutor::ColumnValueToCmpExpr(const oid_t column_id, const type::Value &value) {
  expression::AbstractExpression *lexpr = new expression::TupleValueExpression("");
  reinterpret_cast<expression::TupleValueExpression *>(lexpr)->SetValueType(target_table_->GetSchema()->GetColumn(column_id).GetType());
  reinterpret_cast<expression::TupleValueExpression *>(lexpr)->SetValueIdx(column_id);
  reinterpret_cast<expression::TupleValueExpression *>(lexpr)->SetColName(target_table_->GetSchema()->GetColumn(column_id).GetName());

  expression::AbstractExpression *rexpr = new expression::ConstantValueExpression(value);

  expression::AbstractExpression *root_expr = new expression::ComparisonExpression(ExpressionType::COMPARE_EQUAL, lexpr, rexpr);

  root_expr->DeduceExpressionType();
  return root_expr;
}
} // namespace executor
} // namespace peloton
