//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// seq_scan_executor.h
//
// Identification: src/include/executor/seq_scan_executor.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "../executor/abstract_scan_executor.h"
#include "../planner/seq_scan_plan.h"
#include "../concurrency/transaction_manager.h"
#include "../storage/storage_manager.h"
#include "../storage/tile_group_header.h"

namespace peloton_sintr {
namespace executor {

/**
 * 2018-01-07: This is <b>deprecated</b>. Do not modify these classes.
 * The old interpreted engine will be removed.
 * @deprecated
 */
class SeqScanExecutor : public AbstractScanExecutor {
public:
  SeqScanExecutor(const SeqScanExecutor &) = delete;
  SeqScanExecutor &operator=(const SeqScanExecutor &) = delete;
  SeqScanExecutor(SeqScanExecutor &&) = delete;
  SeqScanExecutor &operator=(SeqScanExecutor &&) = delete;

  explicit SeqScanExecutor(const planner::AbstractPlan *node,
                           ExecutorContext *executor_context);

  void UpdatePredicate(const std::vector<oid_t> &column_ids,
                       const std::vector<type::Value> &values) override;

  void ResetState() override {
    current_tile_group_offset_ = START_OID;
    result_itr_ = 0;
    result_.clear();
    done_ = false;
  }

protected:
  bool DInit() override;

  bool DExecute() override;

  void Scan();

  void OldScan();

  void GetColNames(const expression::AbstractExpression * child_expr, std::unordered_set<std::string> &column_names);
  void SetPredicate(concurrency::TransactionContext *current_txn, sintrstore::QueryReadSetMgr *query_read_set_mgr);
  void SetTableColVersions(concurrency::TransactionContext *current_txn, sintrstore::QueryReadSetMgr *query_read_set_mgr, const Timestamp &current_txn_timestamp);
  void RefineTableColVersions(concurrency::TransactionContext *current_txn, sintrstore::QueryReadSetMgr *query_read_set_mgr);

  void CheckRow(ItemPointer head_tuple_location, concurrency::TransactionManager &transaction_manager, concurrency::TransactionContext *current_txn, 
    storage::StorageManager *storage_manager, std::unordered_map<oid_t, std::vector<oid_t>> &position_map);

  void EvalRead(std::shared_ptr<storage::TileGroup> tile_group, storage::TileGroupHeader *tile_group_header, ItemPointer tuple_location, 
    concurrency::TransactionContext *current_txn, std::unordered_map<oid_t, std::vector<oid_t>> &position_map); 

  void ManageSnapshot(storage::TileGroupHeader *tile_group_header, ItemPointer tuple_location, Timestamp const &timestamp, 
    sintrstore::SnapshotManager *snapshot_mgr);

  void ManageReadSet(concurrency::TransactionContext *current_txn, std::shared_ptr<storage::TileGroup> tile_group, storage::TileGroupHeader *tile_group_header, 
    ItemPointer location, sintrstore::QueryReadSetMgr *query_read_set_mgr);

  bool FindRightRowVersion(const Timestamp &txn_timestamp, std::shared_ptr<storage::TileGroup> tile_group, storage::TileGroupHeader *tile_group_header,
    ItemPointer tuple_location, size_t &num_iters, concurrency::TransactionContext *current_txn, bool &read_curr_version, bool &found_committed, bool &found_prepared);

  void PrepareResult(std::unordered_map<oid_t, std::vector<oid_t>> &position_map);

//  inline const std::string* GetTableName() override {
//    return target_table_->GetName();
//   }

private:
  //===--------------------------------------------------------------------===//
  // Helper Functions
  //===--------------------------------------------------------------------===//

  expression::AbstractExpression *
  ColumnsValuesToExpr(const std::vector<oid_t> &predicate_column_ids,
                      const std::vector<type::Value> &values, size_t idx);

  expression::AbstractExpression *
  ColumnValueToCmpExpr(const oid_t column_id, const type::Value &value);

  std::vector<LogicalTile *> result_;
  /** @brief Result itr */
  oid_t result_itr_ = 0;

  /** @brief Computed the result */
  bool done_ = false;

  //===--------------------------------------------------------------------===//
  // Executor State
  //===--------------------------------------------------------------------===//

  /** @brief Keeps track of current tile group id being scanned. */
  oid_t current_tile_group_offset_ = INVALID_OID;

  /** @brief Keeps track of the number of tile groups to scan. */
  oid_t table_tile_group_count_ = INVALID_OID;

  //===--------------------------------------------------------------------===//
  // Plan Info
  //===--------------------------------------------------------------------===//

  bool index_done_ = false;

  /** @brief Pointer to table to scan from. */
  storage::DataTable *target_table_ = nullptr;

  // TODO make predicate_ a unique_ptr
  // this is a hack that prevents memory leak
  std::unique_ptr<expression::AbstractExpression> new_predicate_ = nullptr;

  // The original predicate, if it's not nullptr
  // we need to combine it with the undated predicate
  const expression::AbstractExpression *old_predicate_;

  bool already_added_table_col_versions;
  bool first_execution;
  Timestamp lowest_snapshot_frontier;
};

} // namespace executor
} // namespace peloton_sintr
