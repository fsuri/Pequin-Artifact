//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_scan_executor.h
//
// Identification: src/include/executor/index_scan_executor.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "../executor/abstract_scan_executor.h"
#include "../index/scan_optimizer.h"
#include "../storage/tile_group_header.h"
#include "../storage/storage_manager.h"
#include "../concurrency/transaction_manager.h"

namespace peloton {

namespace index {
class Index;
}

namespace storage {
class AbstractTable;
}

namespace executor {

/**
 * 2018-01-07: This is <b>deprecated</b>. Do not modify these classes.
 * The old interpreted engine will be removed.
 * @deprecated
 */
class IndexScanExecutor : public AbstractScanExecutor {
  IndexScanExecutor(const IndexScanExecutor &) = delete;
  IndexScanExecutor &operator=(const IndexScanExecutor &) = delete;

 public:
  explicit IndexScanExecutor(const planner::AbstractPlan *node,
                             ExecutorContext *executor_context);

  ~IndexScanExecutor();

  void UpdatePredicate(const std::vector<oid_t> &column_ids UNUSED_ATTRIBUTE,
                       const std::vector<type::Value> &values UNUSED_ATTRIBUTE);

  void ResetState();

 protected:
  bool DInit();

  bool DExecute();


  void SetPredicate(concurrency::TransactionContext *current_txn, pequinstore::QueryReadSetMgr *query_read_set_mgr);
    bool IsImplicitPointRead(concurrency::TransactionContext *current_txn);
    void TryForceReadSetAddition(concurrency::TransactionContext *current_txn, pequinstore::QueryReadSetMgr *query_read_set_mgr, const Timestamp &timestamp);
  void SetTableColVersions(concurrency::TransactionContext *current_txn, pequinstore::QueryReadSetMgr *query_read_set_mgr, const Timestamp &current_txn_timestamp);
  void GetColNames(const expression::AbstractExpression * child_expr, std::unordered_set<std::string> &column_names, bool use_updated = true);


 private:
    //===--------------------------------------------------------------------===//
  // Helper Functions
  //===--------------------------------------------------------------------===//

  expression::AbstractExpression *
  ColumnsValuesToExpr(const std::vector<oid_t> &predicate_column_ids,
                      const std::vector<type::Value> &values, size_t idx);

  expression::AbstractExpression *
  ColumnValueToCmpExpr(const oid_t column_id, const type::Value &value);
    
  //===--------------------------------------------------------------------===//
  // Helper
  //===--------------------------------------------------------------------===//
  bool ExecPrimaryIndexLookup();
  bool ExecPrimaryIndexLookup_OLD(); //__REFACTOR__IN__PROGRESS();
    void CheckRow(ItemPointer tuple_location, concurrency::TransactionManager &transaction_manager, concurrency::TransactionContext *current_txn, storage::StorageManager *storage_manager, 
          std::vector<ItemPointer> &visible_tuple_locations, std::set<ItemPointer> &visible_tuple_set, 
          bool use_secondary_index = false);
    bool FindRightRowVersion(const Timestamp &timestamp, std::shared_ptr<storage::TileGroup> tile_group, storage::TileGroupHeader *tile_group_header, ItemPointer tuple_location,
        size_t &num_iters, concurrency::TransactionContext *current_txn, bool &read_curr_version, bool &found_committed, bool &found_prepared);
    bool FindRightRowVersion_Old(const Timestamp &timestamp, std::shared_ptr<storage::TileGroup> tile_group, storage::TileGroupHeader *tile_group_header, ItemPointer tuple_location,
        size_t &num_iters, concurrency::TransactionContext *current_txn, bool &read_curr_version, bool &found_committed, bool &found_prepared);
    void EvalRead(std::shared_ptr<storage::TileGroup> tile_group, storage::TileGroupHeader *tile_group_header, ItemPointer tuple_location,
        std::vector<ItemPointer> &visible_tuple_locations, concurrency::TransactionContext *current_txn, bool use_secondary_index = false);
    void SetPointRead(concurrency::TransactionContext *current_txn, storage::TileGroupHeader *tile_group_header, ItemPointer tuple_location, Timestamp const &write_timestamp);
    void RefinePointRead(concurrency::TransactionContext *current_txn, storage::TileGroupHeader *tile_group_header, ItemPointer tuple_location, bool eval);
    void ManageSnapshot(concurrency::TransactionContext *current_txn, storage::TileGroupHeader *tile_group_header, ItemPointer tuple_location, const Timestamp &write_timestamp, 
            size_t &num_iters, bool commit_or_prepare);
    void PrepareResult(std::vector<oid_t> &tuples, std::shared_ptr<storage::TileGroup> tile_group);
    void ManageReadSet(ItemPointer &visible_tuple_location, concurrency::TransactionContext *current_txn,
        pequinstore::QueryReadSetMgr *query_read_set_mgr, storage::StorageManager *storage_manager);
    void ManageReadSet(ItemPointer &tuple_location, std::shared_ptr<storage::TileGroup> tile_group, storage::TileGroupHeader *tile_group_header, 
        concurrency::TransactionContext *current_txn);
  bool ExecSecondaryIndexLookup();
  bool ExecSecondaryIndexLookup_OLD(); 

  // When the required scan range has open boundaries, the tuples found by the
  // index might not be exact since the index can only give back tuples in a
  // close range. This function prune the head and the tail of the returned
  // tuple list to get the correct result.
  void CheckOpenRangeWithReturnedTuples(
      std::vector<ItemPointer> &tuple_locations);

  // Check whether the tuple at a given location satisfies the required
  // conditions on key columns
  bool CheckKeyConditions(const ItemPointer &tuple_location);

  //===--------------------------------------------------------------------===//
  // Executor State
  //===--------------------------------------------------------------------===//

  /** @brief Result of index scan. */
  std::vector<LogicalTile *> result_;

  /** @brief Result itr */
  oid_t result_itr_ = INVALID_OID;

  /** @brief Computed the result */
  bool done_ = false;

  //===--------------------------------------------------------------------===//
  // Plan Info
  //===--------------------------------------------------------------------===//

  /** @brief index associated with index scan. */
  std::shared_ptr<index::Index> index_;

  // the underlying table that the index is for
  storage::DataTable *table_ = nullptr;

  std::vector<oid_t> updated_column_ids;

  bool is_metadata_table_;
  bool is_primary_index;
  bool already_added_table_col_versions;
  bool first_execution;
  bool is_implicit_point_read_;

  // TODO make predicate_ a unique_ptr
  // this is a hack that prevents memory leak
  std::unique_ptr<expression::AbstractExpression> new_predicate_ = nullptr;

   // The original predicate, if it's not nullptr
  // we need to combine it with the undated predicate
  const expression::AbstractExpression *old_predicate_;

  // columns to be returned as results
  std::vector<oid_t> column_ids_;

  // columns for key accesses.
  std::vector<oid_t> key_column_ids_;

  // all the columns in a table.
  std::vector<oid_t> full_column_ids_;

  // expression types ( >, <, =, ...)
  std::vector<ExpressionType> expr_types_;

  // values for evaluation.
  std::vector<type::Value> values_;

  std::vector<expression::AbstractExpression *> runtime_keys_;

  bool key_ready_ = false;

  // whether the index scan range is left open
  bool left_open_ = false;

  // whether the index scan range is right open
  bool right_open_ = false;

  // copy from underlying plan
  index::IndexScanPredicate index_predicate_;

  // whether it is an order by + limit plan
  bool limit_ = false;

  // how many tuples should be returned
  int64_t limit_number_ = 0;

  // offset means from which point
  int64_t limit_offset_ = 0;

  // whether order by is descending
  bool descend_ = false;
};

}  // namespace executor
}  // namespace peloton
