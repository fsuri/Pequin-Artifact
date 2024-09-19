//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// insert_executor.cpp
//
// Identification: src/executor/insert_executor.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "../executor/insert_executor.h"

#include "../catalog/manager.h"
#include "../common/container_tuple.h"
#include "../common/logger.h"
#include "../concurrency/transaction_manager_factory.h"
#include "../executor/executor_context.h"
#include "../executor/logical_tile.h"
#include "../planner/insert_plan.h"
#include "../storage/data_table.h"
#include "../storage/tuple_iterator.h"
// #include "../trigger/trigger.h"
#include "../catalog/catalog.h"
#include "../catalog/trigger_catalog.h"
#include "../storage/storage_manager.h"
#include "../storage/tuple.h"
#include "store/pequinstore/query-engine/common/item_pointer.h"

namespace peloton {
namespace executor {

/**
 * @brief Constructor for insert executor.
 * @param node Insert node corresponding to this executor.
 */
InsertExecutor::InsertExecutor(const planner::AbstractPlan *node,
                               ExecutorContext *executor_context)
    : AbstractExecutor(node, executor_context) {}

/**
 * @brief Nothing to init at the moment.
 * @return true on success, false otherwise.
 */
bool InsertExecutor::DInit() {
  PELOTON_ASSERT(children_.size() == 0 || children_.size() == 1);
  PELOTON_ASSERT(executor_context_);

  done_ = false;
  return true;
}


/**
 * @brief Adds a column to the logical tile, using the position lists.
 * @return true on success, false otherwise.
 */
bool InsertExecutor::DExecute() {
  //std::cerr << "Inside insert executor" << std::endl;
  if (done_)
    return false;

  PELOTON_ASSERT(!done_);
  PELOTON_ASSERT(executor_context_ != nullptr);

  const planner::InsertPlan &node = GetPlanNode<planner::InsertPlan>();
  // std::cerr << "Inside insert executor 1" << std::endl;
  storage::DataTable *target_table = node.GetTable();
  oid_t bulk_insert_count = node.GetBulkInsertCount();

  auto &transaction_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto current_txn = executor_context_->GetTransaction();

  if (!target_table) {
    transaction_manager.SetTransactionResult(current_txn, peloton::ResultType::FAILURE);
    return false;
  }

  LOG_TRACE("Number of tuples in table before insert: %lu", target_table->GetTupleCount());
  auto executor_pool = executor_context_->GetPool();

      /*trigger::TriggerList *trigger_list = target_table->GetTriggerList();
      if (trigger_list != nullptr) {
        LOG_TRACE("size of trigger list in target table: %d", trigger_list->GetTriggerListSize());
        if (trigger_list->HasTriggerType(TriggerType::BEFORE_INSERT_STATEMENT)) {
          LOG_TRACE("target table has per-statement-before-insert triggers!");
          trigger_list->ExecTriggers(TriggerType::BEFORE_INSERT_STATEMENT, current_txn);
        }
      }*/

  // Inserting a logical tile.
  if (children_.size() == 1) {
    Debug("Inside insert executor, One child");
    if (!children_[0]->Execute()) {
      return false;
    }

    std::unique_ptr<LogicalTile> logical_tile(children_[0]->GetOutput());
    // NEW: Get the tile group header so we can add the timestamp to the tuple
    storage::TileGroup *tile_group = logical_tile->GetBaseTile(0)->GetTileGroup();
    storage::TileGroupHeader *tile_group_header = tile_group->GetHeader();

    // FIXME: Wrong? What if the result of select is nothing? Michael
    PELOTON_ASSERT(logical_tile.get() != nullptr);

    auto target_table_schema = target_table->GetSchema();
    auto column_count = target_table_schema->GetColumnCount();

    std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(target_table_schema, true));

   
    int cnt = 0;
    // Go over the logical tile
    for (oid_t tuple_id : *logical_tile) {
      cnt++;
      ContainerTuple<LogicalTile> cur_tuple(logical_tile.get(), tuple_id);

      // if(current_txn->GetBasilTimestamp().getTimestamp() > 0) Notice("Next Tuple");

      // Materialize the logical tile tuple
      for (oid_t column_itr = 0; column_itr < column_count; column_itr++) {
        type::Value val = (cur_tuple.GetValue(column_itr));
        //if(current_txn->GetBasilTimestamp().getTimestamp() > 0) Notice("  Val: %s", val.ToString().c_str());
        tuple->SetValue(column_itr, val, executor_pool);
      }

      // insert tuple into the table.
      ItemPointer *index_entry_ptr = nullptr;
      Debug("Checking insertion for id %d", tuple_id);
      peloton::ItemPointer location = target_table->InsertTuple(tuple.get(), current_txn, &index_entry_ptr);

      // it is possible that some concurrent transactions have inserted the same tuple.
      // in this case, abort the transaction.
      // Commented out this check since concurrency control happens at Basil layer
      if (location.block == INVALID_OID) {
        Panic("InsertTuple failed");
       transaction_manager.SetTransactionResult(current_txn, peloton::ResultType::FAILURE);
       return false;
      }

      transaction_manager.PerformInsert(current_txn, location, index_entry_ptr);

      executor_context_->num_processed += 1; // insert one
    }
    //if(current_txn->GetBasilTimestamp().getTimestamp() > 0) Notice("logical tile size: %d", cnt);

    // execute after-insert-statement triggers and
    // record on-commit-insert-statement triggers into current transaction
    /*if (trigger_list != nullptr) {
      LOG_TRACE("size of trigger list in target table: %d",trigger_list->GetTriggerListSize());
      if (trigger_list->HasTriggerType(TriggerType::AFTER_INSERT_STATEMENT)) {
        LOG_TRACE("target table has per-statement-after-insert triggers!");
        trigger_list->ExecTriggers(TriggerType::AFTER_INSERT_STATEMENT, current_txn);
      }
      if (trigger_list->HasTriggerType( TriggerType::ON_COMMIT_INSERT_STATEMENT)) {
        LOG_TRACE("target table has per-statement-on-commit-insert triggers!");
        trigger_list->ExecTriggers(TriggerType::ON_COMMIT_INSERT_STATEMENT,current_txn);
      }
    }*/
    return true;
  }
  // Inserting a collection of tuples from plan node
  else if (children_.size() == 0) {
    // Extract expressions from plan node and construct the tuple.
    // For now we just handle a single tuple

    //std::cerr << "Inside insert executor No Children" << std::endl;
    auto schema = target_table->GetSchema();
    auto project_info = node.GetProjectInfo();
    auto tuple = node.GetTuple(0);
    std::unique_ptr<storage::Tuple> storage_tuple;

    // Check if this is not a raw tuple
    if (project_info) {
      Panic("using project info branch");
      //std::cerr << "from project info" << std::endl;  //TODO: Add Panic? Don't think we ever use this branch
      // Otherwise, there must exist a project info
      PELOTON_ASSERT(project_info);
      // There should be no direct maps
      PELOTON_ASSERT(project_info->GetDirectMapList().size() == 0);

      storage_tuple.reset(new storage::Tuple(schema, true));

      for (auto target : project_info->GetTargetList()) {
        auto value = target.second.expr->Evaluate(nullptr, nullptr, executor_context_);
        storage_tuple->SetValue(target.first, value, executor_pool);
      }
      // Set tuple to point to temporary project tuple
      tuple = storage_tuple.get();
    }

    // if(current_txn->GetBasilTimestamp().getTimestamp() > 0) Notice("Bulk Insert count: %d", bulk_insert_count);
    // Bulk Insert Mode
    for (oid_t insert_itr = 0; insert_itr < bulk_insert_count; insert_itr++) {
      // if we are doing a bulk insert from values not project_info

      UW_ASSERT(!project_info);
      if (!project_info) {
        tuple = node.GetTuple(insert_itr);

        if (tuple == nullptr) {
          storage_tuple.reset(new storage::Tuple(schema, true));


         // if(current_txn->GetBasilTimestamp().getTimestamp() > 0) Notice("Next Tuple");

          // read from values
          uint32_t num_columns = schema->GetColumnCount();
          for (uint32_t col_id = 0; col_id < num_columns; col_id++) {
            try{
              auto value = node.GetValue(col_id + insert_itr * num_columns);
              //if(current_txn->GetBasilTimestamp().getTimestamp() > 0) Notice("   Val: %s", value.ToString().c_str());
              storage_tuple->SetValue(col_id, value, executor_pool);
            }
            catch(...){
              Panic("Failing in GetValue in Insert");
            }
          }

          // Set tuple to point to temporary project tuple
          tuple = storage_tuple.get();
        }
      }

      auto new_tuple = tuple;
         
      // Carry out insertion
      ItemPointer *index_entry_ptr = nullptr;
      // std::cerr << "Insert executor before insertion in else if statement" << std::endl;
      bool result = true;
      bool is_duplicate = false;
 
      /*ItemPointer location = target_table->InsertTuple(new_tuple, current_txn, &index_entry_ptr);*/

      // if(current_txn->GetBasilTimestamp().getTimestamp() > 0) Notice("Calling InsertTuple. Cnt: %d. Txn status: %d (prepare/commit)", insert_itr, current_txn->GetCommitOrPrepare());
      ItemPointer old_location = ItemPointer(0, 0);
      ItemPointer location = target_table->InsertTuple(new_tuple, current_txn, result, is_duplicate, old_location, &index_entry_ptr); //TODO:FIXME: In InsertTuple we should be setting tuple txn meta data.
    

      // if (new_tuple->GetColumnCount() > 2) {
      //   type::Value val = (new_tuple->GetValue(2));
      //   LOG_TRACE("value: %s", val.GetInfo().c_str());
      // }

      // Txn should never fail since CC is done at Basil level
      if (location.block == INVALID_OID) {
      //  LOG_TRACE("Failed to Insert. Set txn failure.");
        //transaction_manager.SetTransactionResult(current_txn, ResultType::FAILURE);
        Panic("Insert failed in insert executor");
        return false;
      }

      bool is_purge = current_txn->GetUndoDelete();

      //If inserting a NEW row version. (if there is already a row version exists yet)
      if (!result && !is_duplicate && !is_purge) {
      
        InsertNewVersion(project_info, target_table, transaction_manager, current_txn, location, old_location, index_entry_ptr);
      }
      //  //If inserting a NEW row version, and it is the first version of the row
      else if (!is_duplicate && !is_purge) {
          InsertFirstVersion(transaction_manager, current_txn, location, index_entry_ptr);
      }

      // TODO: This is what was here before
      // transaction_manager.PerformInsert(current_txn, location, index_entry_ptr);

      LOG_TRACE("Number of tuples in table after insert: %lu", target_table->GetTupleCount());

      executor_context_->num_processed += 1; // insert one

      new_tuple = tuple;
    }
    
    done_ = true;
    return true;
  }
  //return true;
}

bool InsertExecutor::InsertFirstVersion(concurrency::TransactionManager &transaction_manager, concurrency::TransactionContext *current_txn, ItemPointer &location, ItemPointer *index_entry_ptr){
   //If inserting a NEW row version, and it is the first version of the row
  //Debug("Insert was performed");
    transaction_manager.PerformInsert(current_txn, location, index_entry_ptr);

    return true;
}

bool InsertExecutor::InsertNewVersion(const planner::ProjectInfo *project_info, storage::DataTable *target_table, concurrency::TransactionManager &transaction_manager, concurrency::TransactionContext *current_txn, 
                                      ItemPointer &location, ItemPointer &old_location, ItemPointer *index_entry_ptr){
  //If inserting a NEW row version. (if there is already a row version exists yet)
 
      Debug("Trying to insert with existing primary key -- doing update instead");
      // ItemPointer new_location = target_table->AcquireVersion();
      ItemPointer new_location = location;
      // std::cerr << "New location is (" << new_location.block << ", " << new_location.offset << ")" << std::endl;
      

      if (old_location.IsNull()) {
        Panic("Old location is null");
      } 
      auto storage_manager = storage::StorageManager::GetInstance();
      auto tile_group = storage_manager->GetTileGroup(old_location.block);
      auto tile_group_header = tile_group->GetHeader();

      auto new_tile_group = storage_manager->GetTileGroup(new_location.block);

      ContainerTuple<storage::TileGroup> new_tuple_one(new_tile_group.get(), new_location.offset);

      ContainerTuple<storage::TileGroup> old_tuple_one(tile_group.get(), old_location.offset);

 
      // get indirection.
      // std::cerr << "Before getting indirection" << std::endl;
      ItemPointer *indirection = tile_group_header->GetIndirection(old_location.offset);
      // std::cerr << "After getting indirection" << std::endl;
      if (indirection == nullptr) {
        // std::cerr << "Indirection pointer is null" << std::endl;
      }

      new_tile_group->GetHeader()->SetIndirection(new_location.offset, indirection);
    

      transaction_manager.PerformUpdate(current_txn, old_location, new_location);

       bool install_res = target_table->InstallVersion(&new_tuple_one, &(project_info->GetTargetList()), current_txn, indirection);

      /** NEW: Yield ownership if necessary */
      if (install_res == false) {
        Panic("Fail to install new tuple. Set txn failure.");
      }

      return true;
}

bool InsertExecutor::InsertNewVersionOLD(const planner::ProjectInfo *project_info, storage::DataTable *target_table, concurrency::TransactionManager &transaction_manager, concurrency::TransactionContext *current_txn, 
                                      ItemPointer &location, ItemPointer &old_location, ItemPointer *index_entry_ptr){
  //If inserting a NEW row version. (if there is already a row version exists yet)
  // std::cerr << "Tried to insert row with same primary key, so will do an update"  << std::endl;
      Debug("Trying to insert with existing primary key -- doing update instead");
      // ItemPointer new_location = target_table->AcquireVersion();
      ItemPointer new_location = location;
      // std::cerr << "New location is (" << new_location.block << ", " << new_location.offset << ")" << std::endl;
      auto storage_manager = storage::StorageManager::GetInstance();

      if (old_location.IsNull()) {
        // std::cerr << "Old location is null" << std::endl;
        Panic("Old location is null");
      } else {
        // std::cerr << "old location is (" << old_location.block << ", "  << old_location.offset << ")" << std::endl;
      }

      auto tile_group = storage_manager->GetTileGroup(old_location.block);
      auto tile_group_header = tile_group->GetHeader();


      /*tile_group_header->SetCommitOrPrepare(location.offset, current_txn->GetCommitOrPrepare());*/

      auto new_tile_group = storage_manager->GetTileGroup(new_location.block);

      ContainerTuple<storage::TileGroup> new_tuple_one(new_tile_group.get(), new_location.offset);

      ContainerTuple<storage::TileGroup> old_tuple_one(tile_group.get(), old_location.offset);

      if (current_txn->GetTxnDig() != nullptr) {
        new_tile_group->GetHeader()->SetTxnDig(new_location.offset, current_txn->GetTxnDig());
      }


      // bool same_columns = true;
      /*bool should_upgrade = !tile_group_header->GetCommitOrPrepare(old_location.offset) && new_tile_group->GetHeader()->GetCommitOrPrepare(new_location.offset);
      // NOTE: Check if we can upgrade a prepared tuple to committed
      if (should_upgrade) {
        std::cerr << "trying to upgrade from prepared to committed" << std::endl;
        // std::string encoded_key = target_table_->GetName();
        const auto *schema = tile_group->GetAbstractTable()->GetSchema();
        for (uint32_t col_idx = 0; col_idx < schema->GetColumnCount(); col_idx++) {
          auto val1 = tile_group->GetValue(old_location.offset, col_idx);
          auto val2 = new_tile_group->GetValue(new_location.offset, col_idx);
          //std::cerr << "Val 1 is " << val1.ToString() << std::endl;
          //std::cerr << "Val 2 is " << val2.ToString() << std::endl;
          if (val1.ToString() != val2.ToString()) {
            same_columns = false;
            break;
          }
        }

        if (same_columns) {
          //std::cerr << "Upgrading from prepared to committed" << std::endl;
          tile_group_header->SetCommitOrPrepare(old_location.offset, true);
          ItemPointer *indirection = tile_group_header->GetIndirection(old_location.offset);

          new_tile_group->GetHeader()->SetIndirection(new_location.offset, indirection);
          // transaction_manager.PerformDelete(current_txn, new_location);
        }
      }

      bool same_txn = tile_group_header->GetBasilTimestamp(old_location.offset) == new_tile_group->GetHeader()->GetBasilTimestamp(new_location.offset);
      if (same_txn) {
        std::cerr << "Same txn so going to change the value" << std::endl;
        const auto *schema = tile_group->GetAbstractTable()->GetSchema();
        for (uint32_t col_idx = 0; col_idx < schema->GetColumnCount(); col_idx++) {
          auto val1 = tile_group->GetValue(old_location.offset, col_idx);
          auto val2 = new_tile_group->GetValue(new_location.offset, col_idx);
          tile_group->SetValue(val2, old_location.offset, col_idx);

          // std::cerr << "Val 1 is " << val1.ToString() << std::endl;
          // std::cerr << "Val 2 is " << val2.ToString() << std::endl;
        }
        ItemPointer *indirection = tile_group_header->GetIndirection(old_location.offset);

        new_tile_group->GetHeader()->SetIndirection(new_location.offset, indirection);
      }*/

      // perform projection from old version to new version.
      // this triggers in-place update, and we do not need to allocate another version.
      // project_info->Evaluate(&new_tuple_one, &old_tuple_one, nullptr, executor_context_);

      // get indirection.
      // std::cerr << "Before getting indirection" << std::endl;
      ItemPointer *indirection = tile_group_header->GetIndirection(old_location.offset);
      // std::cerr << "After getting indirection" << std::endl;
      if (indirection == nullptr) {
        // std::cerr << "Indirection pointer is null" << std::endl;
      }

      bool install_res = target_table->InstallVersion(&new_tuple_one, &(project_info->GetTargetList()), current_txn, indirection);

      /** NEW: Yield ownership if necessary */
      if (install_res == false) {
        Panic("Fail to install new tuple. Set txn failure.");
        // if (is_owner == false) {
        //   // If the ownership is acquire inside this update executor, we
        //   // release it here
        //   transaction_manager.YieldOwnership(current_txn, tile_group_header, old_location.offset);
        // }

        // transaction_manager.SetTransactionResult(current_txn,  ResultType::FAILURE);
        // return false;
      }

      // finally install new version into the table
      //target_table->InstallVersion(&new_tuple_one, &(project_info->GetTargetList()), current_txn, indirection);
      
      new_tile_group->GetHeader()->GetSpinLatch(new_location.offset).Lock();
      new_tile_group->GetHeader()->SetIndirection(new_location.offset, indirection);
      new_tile_group->GetHeader()->GetSpinLatch(new_location.offset).Unlock();

      Timestamp time = current_txn->GetBasilTimestamp();
      new_tile_group->GetHeader()->SetBasilTimestamp(new_location.offset, time);

      // std::cerr << "After installing version" << std::endl;

      // PerformUpdate() will not be executed if the insertion failed.
      // There is a write lock acquired, but since it is not in the write set,
      // because we haven't yet put them into the write set. the acquired lock can't be released when the txn is aborted.
      // the YieldOwnership() function helps us release the acquired write lock.
      /*if (ret == false) {
        LOG_TRACE("Fail to insert new tuple. Set txn failure.");
        if (is_owner == false) {
          // If the ownership is acquire inside this update executor, we
          // release it here
          transaction_manager.YieldOwnership(current_txn, tile_group_header, physical_tuple_id);
        }
        transaction_manager.SetTransactionResult(current_txn, ResultType::FAILURE);
        std::cerr << "Fourth false" << std::endl;
        return false;
      }*/

      transaction_manager.PerformUpdate(current_txn, old_location, new_location);
      
      new_tile_group->GetHeader()->GetSpinLatch(new_location.offset).Lock();
      new_tile_group->GetHeader()->SetIndirection(new_location.offset, indirection);
      new_tile_group->GetHeader()->GetSpinLatch(new_location.offset).Unlock();

      //TODO: Obsolete to set here? Will be done in PerformUpdate?
      new_tile_group->GetHeader()->SetCommitOrPrepare(new_location.offset, current_txn->GetCommitOrPrepare());
      new_tile_group->GetHeader()->SetMaterialize(new_location.offset, current_txn->GetForceMaterialize());

      return true;
}


} // namespace executor
} // namespace peloton
