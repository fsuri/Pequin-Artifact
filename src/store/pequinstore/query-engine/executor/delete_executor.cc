//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// delete_executor.cpp
//
// Identification: src/executor/delete_executor.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "../executor/delete_executor.h"
#include "../executor/executor_context.h"
#include "../storage/storage_manager.h"
#include <cinttypes>
#include <iostream>

#include "../catalog/manager.h"
#include "../catalog/trigger_catalog.h"
#include "../common/container_tuple.h"
#include "../common/logger.h"
#include "../concurrency/transaction_manager_factory.h"
#include "../executor/logical_tile.h"
#include "../planner/delete_plan.h"
#include "../storage/data_table.h"
#include "../storage/tile.h"
#include "../storage/tile_group.h"
#include "../storage/tile_group_header.h"
#include "../storage/tuple.h"
#include "../type/value.h"
// #include "../trigger/trigger.h"
#include "../catalog/table_catalog.h"
#include "../parser/pg_trigger.h"
#include "store/pequinstore/query-engine/common/internal_types.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor for delete executor.
 * @param node Delete node corresponding to this executor.
 */
DeleteExecutor::DeleteExecutor(const planner::AbstractPlan *node,
                               ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context) {}

/**
 * @brief Nothing to init at the moment.
 * @return true on success, false otherwise.
 */
bool DeleteExecutor::DInit() {
  PELOTON_ASSERT(children_.size() == 1);
  PELOTON_ASSERT(executor_context_);

  PELOTON_ASSERT(target_table_ == nullptr);

  // Delete tuples in logical tile
  LOG_TRACE("Delete executor :: 1 child ");

  // Grab data from plan node.
  const planner::DeletePlan &node = GetPlanNode<planner::DeletePlan>();
  target_table_ = node.GetTable();
  PELOTON_ASSERT(target_table_);

  return true;
}

/**
 * @brief Delete the table tuples using the position list in the logical tile.
 *
 * If truncate is on, then it will truncate the table itself.
 * @return true on success, false otherwise.
 */
// bool DeleteExecutor::DExecute() {
//   Debug("INSIDE DELETE EXECUTOR");
//   //std::cerr << "Inside delete executor" << std::endl;
//   //NOTE: Pequinstore functionality: Delete should essentially just be adding a dummy row marked as delete. Nothing more. //TODO: Should re-factor delete to use INSERT EXECUTOR interface

//   PELOTON_ASSERT(target_table_);
//   // Retrieve next tile.
//   if (!children_[0]->Execute()) {   //Perform the Scan to Find the Rows to delete
//     return false;
//   }

//   // std::cerr << "After child execution" << std::endl;
//   std::unique_ptr<LogicalTile> source_tile(children_[0]->GetOutput());  //This is the tuples that Delete scanned => these are to be deleted.

//   // std::cerr << "After delete child output" << std::endl;
//   auto &pos_lists = source_tile.get()->GetPositionLists();

//   auto &transaction_manager = concurrency::TransactionManagerFactory::GetInstance();

//   auto current_txn = executor_context_->GetTransaction();

//   LOG_TRACE("Source tile : %p Tuples : %lu ", source_tile.get(), source_tile->GetTupleCount());
//   LOG_TRACE("Source tile info: %s", source_tile->GetInfo().c_str());
//   LOG_TRACE("Transaction ID: %" PRId64, executor_context_->GetTransaction()->GetTransactionId());

//   auto executor_pool = executor_context_->GetPool();
//   auto target_table_schema = target_table_->GetSchema();
//   auto column_count = target_table_schema->GetColumnCount();

//               /*trigger::TriggerList *trigger_list = target_table_->GetTriggerList();
//               if (trigger_list != nullptr) {
//                 LOG_TRACE("size of trigger list in target table: %d",
//                           trigger_list->GetTriggerListSize());
//                 if (trigger_list->HasTriggerType(TriggerType::BEFORE_DELETE_STATEMENT)) {
//                   LOG_TRACE("target table has per-statement-before-delete triggers!");
//                   trigger_list->ExecTriggers(TriggerType::BEFORE_DELETE_STATEMENT,
//                                             current_txn);
//                 }
//               }*/

//   //  Delete each row => Insert a new tuple version that is marked as "deleted"
//   for (oid_t visible_tuple_id : *source_tile) {
//     storage::TileGroup *tile_group = source_tile->GetBaseTile(0)->GetTileGroup();
//     storage::TileGroupHeader *tile_group_header = tile_group->GetHeader();

//     oid_t physical_tuple_id = pos_lists[0][visible_tuple_id];

//     ItemPointer old_location(tile_group->GetTileGroupId(), physical_tuple_id);

    
//     Debug("Deleted tuple location [%d:%d]", old_location.block, old_location.offset);
//     //std::cerr << "Deleted tuple id is block " << old_location.block << " and offset " << old_location.offset << std::endl;
//     LOG_TRACE("Visible Tuple id : %u, Physical Tuple id : %u ", visible_tuple_id, physical_tuple_id);

//     ContainerTuple<storage::TileGroup> old_tuple(tile_group, physical_tuple_id);
//     storage::Tuple prev_tuple(target_table_->GetSchema(), true);

//     // Get a copy of the old tuple. TODO: What exactly is this necessary for? Can it be removed?
//     for (oid_t column_itr = 0; column_itr < target_table_schema->GetColumnCount(); column_itr++) {
//       type::Value val = (old_tuple.GetValue(column_itr));
//       prev_tuple.SetValue(column_itr, val, executor_context_->GetPool());
//     }

//     // Check the foreign key source table
//     if (target_table_->CheckForeignKeySrcAndCascade(&prev_tuple, nullptr, current_txn, executor_context_, false) == false) {
//       transaction_manager.SetTransactionResult(current_txn, peloton::ResultType::FAILURE);
//       return false;
//     }



//     bool is_owner = transaction_manager.IsOwner(current_txn, tile_group_header, physical_tuple_id);
//     bool is_written = transaction_manager.IsWritten(current_txn, tile_group_header, physical_tuple_id);
//     // if the current transaction is the creator of this version.
//     // which means the current transaction has already updated the version.

//               //std::unique_ptr<storage::Tuple> real_tuple(new storage::Tuple(target_table_schema, true));
//               //bool tuple_is_materialzed = false;

//               // check whether there are per-row-before-delete triggers on this table using trigger catalog
//               /*if (trigger_list != nullptr) {
//                 LOG_TRACE("size of trigger list in target table: %d", trigger_list->GetTriggerListSize());
//                 if (trigger_list->HasTriggerType(TriggerType::BEFORE_DELETE_ROW)) {
//                   ContainerTuple<LogicalTile> logical_tile_tuple(source_tile.get(), visible_tuple_id);
//                   // Materialize the logical tile tuple
//                   for (oid_t column_itr = 0; column_itr < column_count; column_itr++) {
//                     type::Value val = (logical_tile_tuple.GetValue(column_itr));
//                     real_tuple->SetValue(column_itr, val, executor_pool);
//                   }
//                   tuple_is_materialzed = true;
//                   LOG_TRACE("target table has per-row-before-delete triggers!");
//                   trigger_list->ExecTriggers(TriggerType::BEFORE_DELETE_ROW, current_txn, real_tuple.get(), executor_context_);
//                 }
//               }*/

//     Debug("Is owner: %d, Is written: %d", is_owner, is_written);
//     // std::cerr << "Is owner is " << is_owner << ". Is written is " << is_written << std::endl;

//     if (is_owner == true && is_written == true) {
//       // if the transaction is the owner of the tuple, then directly update in place.
//       LOG_TRACE("The current transaction is the owner of the tuple");
//       //std::cerr << "In the if case delete executor" << std::endl;
//       Panic("This should never be getting triggered in our current setup. TX scope are single statements");
//       transaction_manager.PerformDelete(current_txn, old_location);
//     } else {
//       bool is_ownable = is_owner || transaction_manager.IsOwnable(current_txn, tile_group_header, physical_tuple_id);

//       //std::cerr << "Delete executor in the else case" << std::endl;
//       is_ownable = true;  //NOTE: All the ownership biz seems like it can be removed for Pequin //TODO: Remove what is not needed
//       if (is_ownable == true) {
//         // if the tuple is not owned by any transaction and is visible to current transaction.
//         LOG_TRACE("Thread is not the owner of the tuple, but still visible");

//         bool acquire_ownership_success = is_owner || transaction_manager.AcquireOwnership(current_txn, tile_group_header, physical_tuple_id);

//         acquire_ownership_success = true;
//         if (acquire_ownership_success == false) {
//           transaction_manager.SetTransactionResult(current_txn, ResultType::FAILURE);
//           return false;
//         }

//         //THIS IF-BLOCK IS DEPRECATED. PURGE (UNDO-DELETE) IS NOW HANDLED THROUGH insert_executor.cc
//         if(current_txn->GetUndoDelete()) Panic("UndoDelete in delete_executor.cc is deprecated");
//               //std::cerr << "Undo delete is " << current_txn->GetUndoDelete() << std::endl;
//               /*
//               // Before getting new location
//               if (current_txn->GetUndoDelete()) {
//                 // If undoing a delete then reset the begin and commit ids
//                 // std::cerr << "Made it to undoing deletes" << std::endl;
//                 tile_group_header->SetBeginCommitId(old_location.offset, current_txn->GetTransactionId());
//                 tile_group_header->SetEndCommitId(old_location.offset, MAX_CID);
//                 tile_group_header->SetTransactionId(old_location.offset, current_txn->GetTransactionId());
//                 tile_group_header->SetLastReaderCommitId(old_location.offset, current_txn->GetCommitId());

//                 ItemPointer *index_entry_ptr = tile_group_header->GetIndirection(old_location.offset);

//                 // if there's no primary index on a table, then index_entry_ptr == nullptr.
//                 if (index_entry_ptr != nullptr) {
//                   // std::cerr << "Undo delete inside if statement" << std::endl;
//                   tile_group_header->SetIndirection(old_location.offset, index_entry_ptr);

//                   // Set the index header in an atomic way.
//                   // We do it atomically because we don't want any one to see a half-down pointer 
//                   // In case of contention, no one can update this pointer when we are updating it because we are holding the write lock. 
//                   //This update should success in its first trial.
//                   UNUSED_ATTRIBUTE auto res = AtomicUpdateItemPointer(index_entry_ptr, old_location);
//                   PELOTON_ASSERT(res == true);
//                 }

//                 // tile_group_header->SetEndCommitId(old_location.offset, INVALID_CID);
//                 Debug("Trying to undo deleted tuple id. block %d, offset %d",  old_location.block, old_location.offset);
//                 // std::cerr << "Trying to undo deleted tuple id is block " << old_location.block << " and offset " << old_location.offset << std::endl;
//                 Debug("Undo delete visibility type is: %d", transaction_manager.IsVisible(current_txn, tile_group_header, old_location.offset));
//                 // std::cerr << "Undo delete visibility type is " << transaction_manager.IsVisible(current_txn, tile_group_header, old_location.offset) << std::endl;
//                 return true;
//               }
//               */

//         // if it is the latest version and not locked by other threads, then  insert an empty version.
//         ItemPointer new_location = target_table_->InsertEmptyVersion(current_txn);

//         Debug("Delete executor New tuple location[%d:%d]", new_location.block, new_location.offset);
//         //std::cerr << "Delete executor New location tuple id is block " << new_location.block << " and offset " << new_location.offset << std::endl;

//         // PerformDelete() will not be executed if the insertion failed.
//         // There is a write lock acquired, but since it is not in the write set, because we haven't yet put them into the write set.
//         // the acquired lock can't be released when the txn is aborted. the YieldOwnership() function helps us release the acquired write lock.
//         if (new_location.IsNull() == true) {
//           Panic("New location is null");
//           LOG_TRACE("Fail to insert new tuple. Set txn failure.");
//           if (is_owner == false) {
//             // If the ownership is acquired inside this update executor, we release it here
//             transaction_manager.YieldOwnership(current_txn, tile_group_header, physical_tuple_id);
//           }
//           transaction_manager.SetTransactionResult(current_txn, ResultType::FAILURE);
//           return false;
//         }
//         // std::cerr << "Got to perform delete" << std::endl;
//         /** NOTE: Old logic */
//         /**transaction_manager.PerformDelete(current_txn, old_location,new_location);*/

//         // Create special "Delete Tuple". Note: Technically only needs the primary key values. The rest can be dummy/empty
//         std::cerr << "Delete executor performing delete. Commit: " << current_txn->GetCommitOrPrepare() << ". ForceMat? " << current_txn->GetForceMaterialize() << std::endl;

       
//         auto new_tile_group_header = target_table_->GetTileGroupById(new_location.block)->GetHeader();
//         new_tile_group_header->SetIsDeleted(new_location.offset, true);
//          //TODO: These are all unecessary to set. Will be set anyways inside PerformUpdate()
//         // auto ts = current_txn->GetBasilTimestamp();
//         // new_tile_group_header->SetBasilTimestamp(new_location.offset, ts);
//         // new_tile_group_header->SetCommitOrPrepare(new_location.offset, current_txn->GetCommitOrPrepare());
//         // new_tile_group_header->SetMaterialize(new_location.offset, current_txn->GetForceMaterialize());
//         transaction_manager.PerformUpdate(current_txn, old_location, new_location);

//         executor_context_->num_processed += 1; // deleted one
//       } else {
//         // transaction should be aborted as we cannot update the latest version.
//         LOG_TRACE("Fail to update tuple. Set txn failure.");
//         transaction_manager.SetTransactionResult(current_txn, ResultType::FAILURE);
//         return false;
//       }
//     }
//           // execute after-delete-row triggers and
//           // record on-commit-delete-row triggers into current transaction
//           /*if (trigger_list != nullptr) {
//             LOG_TRACE("size of trigger list in target table: %d", trigger_list->GetTriggerListSize());
//             if (trigger_list->HasTriggerType(TriggerType::AFTER_DELETE_ROW) ||
//                 trigger_list->HasTriggerType(TriggerType::ON_COMMIT_DELETE_ROW)) {
//               if (!tuple_is_materialzed) {
//                 ContainerTuple<LogicalTile> logical_tile_tuple(source_tile.get(), visible_tuple_id);
//                 // Materialize the logical tile tuple
//                 for (oid_t column_itr = 0; column_itr < column_count; column_itr++) {
//                   type::Value val = (logical_tile_tuple.GetValue(column_itr));
//                   real_tuple->SetValue(column_itr, val, executor_pool);
//                 }
//               }
//               if (trigger_list->HasTriggerType(TriggerType::AFTER_DELETE_ROW)) {
//                 LOG_TRACE("target table has per-row-after-delete triggers!");
//                 trigger_list->ExecTriggers(TriggerType::AFTER_DELETE_ROW, current_txn, real_tuple.get(), executor_context_);
//               }
//               if (trigger_list->HasTriggerType(TriggerType::ON_COMMIT_DELETE_ROW)) {
//                 LOG_TRACE("target table has per-row-on-commit-delete triggers!");
//                 trigger_list->ExecTriggers(TriggerType::ON_COMMIT_DELETE_ROW, current_txn, real_tuple.get(), executor_context_);
//               }
//             }
//           }*/
//   }
//           // execute after-delete-statement triggers and
//           // record on-commit-delete-statement triggers into current transaction
//           /*if (trigger_list != nullptr) {
//             LOG_TRACE("size of trigger list in target table: %d", trigger_list->GetTriggerListSize());
//             if (trigger_list->HasTriggerType(TriggerType::AFTER_DELETE_STATEMENT)) {
//               LOG_TRACE("target table has per-statement-after-delete triggers!");
//               trigger_list->ExecTriggers(TriggerType::AFTER_DELETE_STATEMENT, current_txn);
//             }
//             if (trigger_list->HasTriggerType(TriggerType::ON_COMMIT_DELETE_STATEMENT)) {
//               LOG_TRACE("target table has per-statement-on-commit-delete triggers!");
//               trigger_list->ExecTriggers(TriggerType::ON_COMMIT_DELETE_STATEMENT, current_txn);
//             }
//           }*/
//   return true;
// }

bool DeleteExecutor::DExecute() {
  Debug("INSIDE DELETE EXECUTOR");
  //std::cerr << "Inside delete executor" << std::endl;
  //NOTE: Pequinstore functionality: Delete should essentially just be adding a dummy row marked as delete. Nothing more. //TODO: Should re-factor delete to use INSERT EXECUTOR interface

  PELOTON_ASSERT(target_table_);
  // Retrieve next tile.
  if (!children_[0]->Execute()) {   //Perform the Scan to Find the Rows to delete
    return false;
  }

  // std::cerr << "After child execution" << std::endl;
  std::unique_ptr<LogicalTile> source_tile(children_[0]->GetOutput());  //This is the tuples that Delete scanned => these are to be deleted.

  // std::cerr << "After delete child output" << std::endl;
  auto &pos_lists = source_tile.get()->GetPositionLists();

  auto &transaction_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto current_txn = executor_context_->GetTransaction();

  LOG_TRACE("Source tile : %p Tuples : %lu ", source_tile.get(), source_tile->GetTupleCount());
  LOG_TRACE("Source tile info: %s", source_tile->GetInfo().c_str());
  LOG_TRACE("Transaction ID: %" PRId64, executor_context_->GetTransaction()->GetTransactionId());

  auto target_table_schema = target_table_->GetSchema();
  auto column_count = target_table_schema->GetColumnCount();


  //  Delete each row => Insert a new tuple version that is marked as "deleted"
  for (oid_t visible_tuple_id : *source_tile) {
    storage::TileGroup *tile_group = source_tile->GetBaseTile(0)->GetTileGroup();
    storage::TileGroupHeader *tile_group_header = tile_group->GetHeader();

    oid_t physical_tuple_id = pos_lists[0][visible_tuple_id];

    ItemPointer old_location(tile_group->GetTileGroupId(), physical_tuple_id);

    
    Debug("Deleted tuple location [%d:%d]", old_location.block, old_location.offset);
    //std::cerr << "Deleted tuple id is block " << old_location.block << " and offset " << old_location.offset << std::endl;
    LOG_TRACE("Visible Tuple id : %u, Physical Tuple id : %u ", visible_tuple_id, physical_tuple_id);

    ContainerTuple<storage::TileGroup> old_tuple(tile_group, physical_tuple_id);
    storage::Tuple prev_tuple(target_table_->GetSchema(), true);

    // Create special "Delete Tuple" by copying old tuple. Note: Technically only needs the primary key values. The rest can be dummy/empty  
    for (oid_t column_itr = 0; column_itr < target_table_schema->GetColumnCount(); column_itr++) {
      type::Value val = (old_tuple.GetValue(column_itr));
      prev_tuple.SetValue(column_itr, val, executor_context_->GetPool());
    }



    //transaction_manager.AcquireOwnership(current_txn, tile_group_header, physical_tuple_id);


    //THIS IF-BLOCK IS DEPRECATED. PURGE (UNDO-DELETE) IS NOW HANDLED THROUGH insert_executor.cc
    if(current_txn->GetUndoDelete()) Panic("UndoDelete in delete_executor.cc is deprecated");
    
    // if it is the latest version and not locked by other threads, then  insert an empty version.
    ItemPointer new_location = target_table_->InsertEmptyVersion(current_txn);

    Debug("Delete executor New tuple location[%d:%d]", new_location.block, new_location.offset);
    //std::cerr << "Delete executor New location tuple id is block " << new_location.block << " and offset " << new_location.offset << std::endl;
    
    auto new_tile_group_header = target_table_->GetTileGroupById(new_location.block)->GetHeader();
    new_tile_group_header->SetIsDeleted(new_location.offset, true);
  
    size_t indirection_offset = tile_group_header->GetIndirectionOffset(old_location.offset);
    new_tile_group_header->SetIndirectionOffset(new_location.offset, indirection_offset);

    transaction_manager.PerformUpdate(current_txn, old_location, new_location);

    executor_context_->num_processed += 1; // deleted one
    
  }
  return true;
}

} // namespace executor
} // namespace peloton
