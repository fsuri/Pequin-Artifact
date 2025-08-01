//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// data_table.cpp
//
// Identification: src/storage/data_table.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <mutex>
#include <utility>

#include "../catalog/catalog.h"
#include "../catalog/layout_catalog.h"
#include "../catalog/system_catalogs.h"
#include "../catalog/table_catalog.h"
#include "../common/container_tuple.h"
#include "../common/exception.h"
#include "../common/logger.h"
#include "../common/platform.h"
#include "../concurrency/transaction_context.h"
#include "../concurrency/transaction_manager_factory.h"
#include "../executor/executor_context.h"
#include "../gc/gc_manager_factory.h"
#include "../index/index.h"
// #include "../logging/log_manager.h"
#include "../storage/abstract_table.h"
#include "../storage/data_table.h"
#include "../storage/database.h"
#include "../storage/storage_manager.h"
#include "../storage/tile.h"
#include "../storage/tile_group.h"
#include "../storage/tile_group_factory.h"
#include "../storage/tile_group_header.h"
#include "../storage/tuple.h"
#include "lib/message.h"
#include "store/pequinstore/query-engine/common/internal_types.h"
#include "store/pequinstore/query-engine/common/item_pointer.h"
// #include "../tuning/clusterer.h"
// #include "../tuning/sample.h"

//===--------------------------------------------------------------------===//
// Configuration Variables
//===--------------------------------------------------------------------===//

std::vector<peloton::oid_t> sdbench_column_ids;

double peloton_projectivity;

int peloton_num_groups;

namespace peloton {
namespace storage {

oid_t DataTable::invalid_tile_group_id = -1;

size_t DataTable::default_active_tilegroup_count_ = 1;
size_t DataTable::default_active_indirection_array_count_ = 1;

DataTable::DataTable(catalog::Schema *schema, const std::string &table_name,
                     const oid_t &database_oid, const oid_t &table_oid,
                     const size_t &tuples_per_tilegroup, const bool own_schema,
                     const bool adapt_table, const bool is_catalog,
                     const peloton::LayoutType layout_type)
    : AbstractTable(table_oid, schema, own_schema, layout_type),
      database_oid(database_oid), table_name(table_name),
      tuples_per_tilegroup_(tuples_per_tilegroup),
      current_layout_oid_(ATOMIC_VAR_INIT(COLUMN_STORE_LAYOUT_OID)),
      adapt_table_(adapt_table) /*,
       trigger_list_(new trigger::TriggerList())*/
{
  if (is_catalog == true) {
    active_tilegroup_count_ = 1;
    active_indirection_array_count_ = 1;
  } else {
    active_tilegroup_count_ = default_active_tilegroup_count_;
    active_indirection_array_count_ = default_active_indirection_array_count_;
  }

  active_tile_groups_.resize(active_tilegroup_count_);

  active_indirection_arrays_.resize(active_indirection_array_count_);
  // Create tile groups.
  for (size_t i = 0; i < active_tilegroup_count_; ++i) {
    AddDefaultTileGroup(i);
  }

  // Create indirection layers.
  for (size_t i = 0; i < active_indirection_array_count_; ++i) {
    AddDefaultIndirectionArray(i);
  }
}

DataTable::~DataTable() {
  // clean up tile groups by dropping the references in the catalog
  auto &catalog_manager = catalog::Manager::GetInstance();
  auto storage_manager = storage::StorageManager::GetInstance();
  auto tile_groups_size = tile_groups_.GetSize();
  std::size_t tile_groups_itr;

  for (tile_groups_itr = 0; tile_groups_itr < tile_groups_size;
       tile_groups_itr++) {
    auto tile_group_id = tile_groups_.Find(tile_groups_itr);

    if (tile_group_id != invalid_tile_group_id) {
      LOG_TRACE("Dropping tile group : %u ", tile_group_id);
      // drop tile group in catalog
      storage_manager->DropTileGroup(tile_group_id);
    }
  }

  // drop all indirection arrays
  for (auto indirection_array : active_indirection_arrays_) {
    auto oid = indirection_array->GetOid();
    catalog_manager.DropIndirectionArray(oid);
  }
  // AbstractTable cleans up the schema
}

//===--------------------------------------------------------------------===//
// TUPLE HELPER OPERATIONS
//===--------------------------------------------------------------------===//

bool DataTable::CheckNotNulls(const AbstractTuple *tuple,
                              oid_t column_idx) const {
  if (tuple->GetValue(column_idx).IsNull()) {
    LOG_TRACE("%u th attribute in the tuple was NULL. It is non-nullable "
              "attribute.",
              column_idx);
    return false;
  }
  return true;
}

bool DataTable::CheckConstraints(const AbstractTuple *tuple) const {
  // make sure that the given tuple does not violate constraints.

  // NOT NULL constraint
  for (oid_t column_id : schema->GetNotNullColumns()) {
    if (schema->AllowNull(column_id) == false &&
        CheckNotNulls(tuple, column_id) == false) {
      std::string error =
          StringUtil::Format("NOT NULL constraint violated on column '%s' : %s",
                             schema->GetColumn(column_id).GetName().c_str(),
                             tuple->GetInfo().c_str());
      throw ConstraintException(error);
    }
  }

  // DEFAULT constraint should not be handled here
  // Handled in higher hierarchy

  // multi-column constraints
  for (auto cons_pair : schema->GetConstraints()) {
    auto cons = cons_pair.second;
    ConstraintType type = cons->GetType();
    switch (type) {
    case ConstraintType::CHECK: {
      //          std::pair<ExpressionType, type::Value> exp =
      //          cons.GetCheckExpression();
      //          if (CheckExp(tuple, column_itr, exp) == false) {
      //            LOG_TRACE("CHECK EXPRESSION constraint violated");
      //            throw ConstraintException(
      //                "CHECK EXPRESSION constraint violated : " +
      //                std::string(tuple->GetInfo()));
      //          }
      break;
    }
    case ConstraintType::UNIQUE: {
      break;
    }
    case ConstraintType::PRIMARY: {
      break;
    }
    case ConstraintType::FOREIGN: {
      break;
    }
    case ConstraintType::EXCLUSION: {
      break;
    }
    default: {
      std::string error =
          StringUtil::Format("ConstraintType '%s' is not supported",
                             ConstraintTypeToString(type).c_str());
      LOG_TRACE("%s", error.c_str());
      throw ConstraintException(error);
    }
    } // SWITCH
  }   // FOR (constraints)

  return true;
}

// this function is called when update/delete/insert is performed.
// this function first checks whether there's available slot.
// if yes, then directly return the available slot.
// in particular, if this is the last slot, a new tile group is created.
// if there's no available slot, then some other threads must be allocating a
// new tile group.
// we just wait until a new tuple slot in the newly allocated tile group is
// available.
// when updating a tuple, we will invoke this function with the argument set to
// nullptr.
// this is because we want to minimize data copy overhead by performing
// in-place update at executor level.
// however, when performing insert, we have to copy data immediately,
// and the argument cannot be set to nullptr.
ItemPointer DataTable::GetEmptyTupleSlot(const storage::Tuple *tuple) {
  //=============== garbage collection==================
  // check if there are recycled tuple slots
  auto &gc_manager = gc::GCManagerFactory::GetInstance();
  auto free_item_pointer = gc_manager.ReturnFreeSlot(this->table_oid);
  if (free_item_pointer.IsNull() == false) {
    // when inserting a tuple
    if (tuple != nullptr) {
      auto tile_group = storage::StorageManager::GetInstance()->GetTileGroup(
          free_item_pointer.block);
      tile_group->CopyTuple(tuple, free_item_pointer.offset);
    }
    return free_item_pointer;
  }
  //====================================================

  size_t active_tile_group_id = number_of_tuples_ % active_tilegroup_count_;
  std::shared_ptr<storage::TileGroup> tile_group;
  oid_t tuple_slot = INVALID_OID;
  oid_t tile_group_id = INVALID_OID;

  // get valid tuple.
  while (true) {
    // get the last tile group.
    tile_group = active_tile_groups_[active_tile_group_id];

    tuple_slot = tile_group->InsertTuple(tuple);

    // now we have already obtained a new tuple slot.
    if (tuple_slot != INVALID_OID) {
      //std::cerr << "Breaking out of get empty tuple slot loop" << std::endl;
      tile_group_id = tile_group->GetTileGroupId();
      break;
    }
  }

  // if this is the last tuple slot we can get
  // then create a new tile group
  if (tuple_slot == tile_group->GetAllocatedTupleCount() - 1) {
    //std::cerr << "Creating new tile group" << std::endl;
    AddDefaultTileGroup(active_tile_group_id);
  }

  LOG_TRACE("tile group count: %lu, tile group id: %u, address: %p",
            tile_group_count_.load(), tile_group->GetTileGroupId(),
            tile_group.get());

  // Set tuple location
  ItemPointer location(tile_group_id, tuple_slot);

  return location;
}

//===--------------------------------------------------------------------===//
// INSERT
//===--------------------------------------------------------------------===//
ItemPointer DataTable::InsertEmptyVersion(concurrency::TransactionContext *current_txn) {
  // First, claim a slot
  ItemPointer location = GetEmptyTupleSlot(nullptr);
  if (location.block == INVALID_OID) {
    LOG_TRACE("Failed to get tuple slot.");
    return INVALID_ITEMPOINTER;
  }

  SetPequinMetaData(location, current_txn);

  LOG_TRACE("Location: %u, %u", location.block, location.offset);

  IncreaseTupleCount(1);
  return location;
}

ItemPointer DataTable::AcquireVersion() {
  // First, claim a slot
  ItemPointer location = GetEmptyTupleSlot(nullptr);
  if (location.block == INVALID_OID) {
    LOG_TRACE("Failed to get tuple slot.");
    return INVALID_ITEMPOINTER;
  }

  LOG_TRACE("Location: %u, %u", location.block, location.offset);

  IncreaseTupleCount(1);
  return location;
}

bool DataTable::InstallVersion(const AbstractTuple *tuple,
                               const TargetList *targets_ptr,
                               concurrency::TransactionContext *transaction,
                               ItemPointer *index_entry_ptr) {
  if (CheckConstraints(tuple) == false) {
    Debug("Check constraints false");
    // std::cerr << "Check constraints false" << std::endl;
    LOG_TRACE("InsertVersion(): Constraint violated");
    return false;
  }
  // std::cerr << "Past check constraints" << std::endl;
  //  Index checks and updates
  if (InsertInSecondaryIndexes(tuple, targets_ptr, transaction, index_entry_ptr) == false) {
    // std::cerr << "Inside if insertinsecondaryindexes" << std::endl;
    LOG_TRACE("Index constraint violated");
    return false;
  }
  Debug("Past insert and into secondary indexes");
  // std::cerr << "Past insert into secondary indexes" << std::endl;
  return true;
}

ItemPointer DataTable::InsertTuple(const storage::Tuple *tuple,
                                   concurrency::TransactionContext *transaction,
                                   ItemPointer **index_entry_ptr,
                                   bool check_fk) {
  ItemPointer location = GetEmptyTupleSlot(tuple);
  if (location.block == INVALID_OID) {
    LOG_TRACE("Failed to get tuple slot.");
    return INVALID_ITEMPOINTER;
  }

  ItemPointer old_location = ItemPointer(0, 0);

  auto result = InsertTuple(tuple, location, transaction, old_location, index_entry_ptr, check_fk);
  if (result == false) {
    // check_fk = false;
    return INVALID_ITEMPOINTER;
  }
  return location;
}

void DataTable::SetPequinMetaData(ItemPointer &location, concurrency::TransactionContext *current_txn){

  oid_t tile_group_id = location.block;
  oid_t tuple_id = location.offset;

  auto tile_group = this->GetTileGroupById(tile_group_id);
  auto tile_group_header = tile_group->GetHeader();

  //Set meta data for the new tuple (at location tuple_id)
 
  auto ts = current_txn->GetBasilTimestamp();
  tile_group_header->SetBasilTimestamp(tuple_id, ts);

  if(ts.getTimestamp()>0) UW_ASSERT(current_txn->GetTxnDig());
  tile_group_header->SetTxnDig(tuple_id, current_txn->GetTxnDig());

  if (current_txn->GetUndoDelete()){ //if Txn is Aborted: Add a row but mark it as purged (unreadable). This is necessary in case a prepare of the row arrives "later"
    tile_group_header->SetPurged(tuple_id, true);
    tile_group_header->SetCommitOrPrepare(tuple_id, false); //mark purged as "prepared" (it's "aborted", but its final)
    return;
  }

  const pequinstore::proto::CommittedProof *proof = current_txn->GetCommittedProof();
  if (ts.getTimestamp() > 0 && current_txn->GetCommitOrPrepare()) {
    UW_ASSERT(proof);
    /*auto ts1 = Timestamp(proof->txn().timestamp());
    Debug("The commit proof timestamp is %lu, %lu", ts1.getTimestamp(), ts1.getID());*/
  }
  tile_group_header->SetCommittedProof(tuple_id, proof);

  tile_group_header->SetCommitOrPrepare(tuple_id, current_txn->GetCommitOrPrepare());

  tile_group_header->SetMaterialize(tuple_id, current_txn->GetForceMaterialize());
}

bool DataTable::CheckRowVersionUpdate(const storage::Tuple *tuple, std::shared_ptr<peloton::storage::TileGroup> tile_group, TileGroupHeader *tile_group_header, ItemPointer *index_entry_ptr, ItemPointer &check, concurrency::TransactionContext *transaction){

  bool is_duplicate = false;

  //Take lock on linked list while checking it. 

  //Lock version 1
  size_t indirection_offset = tile_group_header->GetIndirectionOffset(check.offset);
  std::mutex &m = active_indirection_mutexes_[indirection_offset % 32];
  m.lock();

  //Lock version 2
      
  ////// Find the current linked list header (atomically)
  // ItemPointer head_pointer;
  // peloton::storage::TileGroupHeader *head_tile_group_header; 
  // while(true){
  //   head_pointer = *index_entry_ptr;
  //   head_tile_group_header = this->GetTileGroupById(head_pointer.block)->GetHeader();
  
  //   head_tile_group_header->GetSpinLatch(head_pointer.offset).Lock(); //TODO: change code to TryLock
  
  //   //check if we are holding the lock for the header. If not (i.e. the header changed in the meantime, need to re-read the right header)
  //   if(!(head_pointer == *index_entry_ptr)){
  //       // Notice("Core[%d] Not equal: Head pointer [%p: %lu %lu]. Index_entry_ptr [%p: %lu %lu]. Equal? %d", 
  //       //   sched_getcpu(), &head_pointer, head_pointer.block, head_pointer.offset, index_entry_ptr, index_entry_ptr->block, index_entry_ptr->offset, (head_pointer == *index_entry_ptr));
  //     head_tile_group_header->GetSpinLatch(head_pointer.offset).Unlock();
      
  //     continue;
  //   }
    
  //   break;
  // }

  //Check whether linked list already contains a tuple with the same TS (i.e. from the same TX)
  auto ts = tile_group_header->GetBasilTimestamp(check.offset);
  auto curr_pointer = check;
  auto curr_tile_group_header = tile_group_header;
  auto curr_tile_group = tile_group;

  while (ts > transaction->GetBasilTimestamp()) {
    if (curr_tile_group_header->GetNextItemPointer(curr_pointer.offset).IsNull()) {
      break;
    }
    curr_pointer = curr_tile_group_header->GetNextItemPointer(curr_pointer.offset);
    curr_tile_group = this->GetTileGroupById(curr_pointer.block);
    curr_tile_group_header = curr_tile_group->GetHeader();
    ts = curr_tile_group_header->GetBasilTimestamp(curr_pointer.offset);
  }
  //Debug("Trying to write to tile-group-header:offset [%lu:%lu]", curr_pointer.block, curr_pointer.offset);

  //If TX tries to write to a tuple that it previously wrote itself (i.e. prepared)
  //Distinguish whether we are trying to rollback a prepare, or upgrade it to commit.
  if (ts == transaction->GetBasilTimestamp()) {
    Debug("In INSERT TUPLE. txn: %s. Commit (or prepare) ? %d", pequinstore::BytesToHex(*transaction->GetTxnDig(), 16).c_str(), transaction->GetCommitOrPrepare());
    is_duplicate = true;
    bool is_prepared = !curr_tile_group_header->GetCommitOrPrepare(curr_pointer.offset);

    //If current version is prepared => purge it
    if (transaction->GetUndoDelete() && is_prepared) {

      // auto key = curr_tile_group->GetValue(curr_pointer.offset, 0);
      // Notice("txn[%lu:%lu] Row-key[%s]. Purge_Location[%d:%d]. Indirection offset: %d",
      //         transaction->GetBasilTimestamp().getTimestamp(), transaction->GetBasilTimestamp().getID(), key.ToString().c_str(),
      //         curr_pointer.block, curr_pointer.offset, indirection_offset);

      PurgeRowVersion(index_entry_ptr, curr_tile_group_header, curr_pointer, transaction);
      // Notice("txn[%lu:%lu] Row-key[%s]. FINISHED Purge_Location[%d:%d]. Indirection offset: %d",
      //         transaction->GetBasilTimestamp().getTimestamp(), transaction->GetBasilTimestamp().getID(), key.ToString().c_str(),
      //         curr_pointer.block, curr_pointer.offset, indirection_offset);
    }
    //Writing again.  (either current version is prepared => and we upgrade to committed. Or current version is committed => and nothing should happen)
    else {
      UpgradeRowVersionCommitStatus(tuple, curr_tile_group, curr_tile_group_header, curr_pointer, transaction, ts);
    }

  }
    
  //Lock version 1
  m.unlock();

  //Lock version 2
  //head_tile_group_header->GetSpinLatch(head_pointer.offset).Unlock();

  return is_duplicate;

}
void DataTable::PurgeRowVersion(ItemPointer *index_entry_ptr, TileGroupHeader *curr_tile_group_header, ItemPointer &curr_pointer, concurrency::TransactionContext *transaction){

    

      // //FIXME: REMOVE: Print the linked list for visualization.
      // auto n_pointer = *index_entry_ptr;
      // auto n_tile_group_h = this->GetTileGroupById(n_pointer.block)->GetHeader();
      // auto n_ts = n_tile_group_h->GetBasilTimestamp(n_pointer.offset);
      // Notice("txn[%lu:%lu] Purge_Location[%d:%d]. Ind_Location[%d:%d], HeadTS[%lu:%lu]",
      //       transaction->GetBasilTimestamp().getTimestamp(), transaction->GetBasilTimestamp().getID(), 
      //       curr_pointer.block, curr_pointer.offset, n_pointer.block, n_pointer.offset, n_ts.getTimestamp(), n_ts.getID());

      // int max = 5;
      // //Print the linked list. TS and locations.
      // while(!n_tile_group_h->GetNextItemPointer(n_pointer.offset).IsNull() && max-- > 0){
      //   //Print 
      //   Notice("txn[%lu:%lu] NextTS[%lu:%lu]. NextLoc[%d:%d]",
      //       transaction->GetBasilTimestamp().getTimestamp(), transaction->GetBasilTimestamp().getID(), 
      //       n_ts.getTimestamp(), n_ts.getID(), n_pointer.block, n_pointer.offset);

      //   n_pointer = n_tile_group_h->GetNextItemPointer(n_pointer.offset);
      //   n_tile_group_h = this->GetTileGroupById(n_pointer.block)->GetHeader();
      //   n_ts = n_tile_group_h->GetBasilTimestamp(n_pointer.offset);
      // }
  




        Debug("In UndoDelete for Purge [txn: %s]", pequinstore::BytesToHex(*transaction->GetTxnDig(), 16));
        // Purge this tuple

        // Set Purge flag. Note: In theory don't need to adjust any of the linked lists to remove purged versions, but we do it for read efficiency
        Debug("Table[%s]. Purging [txn: %s]. [%lu:%lu]", table_name.c_str(), pequinstore::BytesToHex(*transaction->GetTxnDig(), 16).c_str(), curr_pointer.block, curr_pointer.offset);
        curr_tile_group_header->SetPurged(curr_pointer.offset, true);

        // return;
        //Note: we are currently holding the mutex in CheckRowVersionUpdate.
    
        // Set the linked list pointers
        auto prev_loc = curr_tile_group_header->GetPrevItemPointer(curr_pointer.offset);
        auto next_loc = curr_tile_group_header->GetNextItemPointer(curr_pointer.offset);
        
        // NEW: For purge set the tile group header locks

        if (!prev_loc.IsNull() && !next_loc.IsNull()) { //If curr position is a middle item => remove and re-link next/prev to point to each other.
          Debug("Purging middle item: Updating both pointers [txn: %s]", pequinstore::BytesToHex(*transaction->GetTxnDig(), 16).c_str());
          auto prev_tgh = this->GetTileGroupById(prev_loc.block)->GetHeader();
          auto next_tgh = this->GetTileGroupById(next_loc.block)->GetHeader();
          next_tgh->SetPrevItemPointer(next_loc.offset, prev_loc);
          prev_tgh->SetNextItemPointer(prev_loc.offset, next_loc);
        } 
        else if (prev_loc.IsNull() && !next_loc.IsNull()) { //If curr position is the current head, and the list is not empty. Elevate next to head.
          //std::cerr << "Updating head pointer" << std::endl;
          Debug("Updating head pointer (purge latest) [txn: %s]", pequinstore::BytesToHex(*transaction->GetTxnDig(), 16).c_str());
          auto next_tgh = this->GetTileGroupById(next_loc.block)->GetHeader();
         
          next_tgh->SetPrevItemPointer(next_loc.offset, INVALID_ITEMPOINTER); //ItemPointer(INVALID_OID, INVALID_OID));
          ItemPointer *index_entry_ptr = next_tgh->GetIndirection(next_loc.offset);
          
          // COMPILER_MEMORY_FENCE;

          // Set the index header in an atomic way.
          // We do it atomically because we don't want any one to see a half-done pointer. In case of contention, no one can update this
          // pointer when we are updating it because we are holding the write lock. This update should success in its first trial.
          UNUSED_ATTRIBUTE auto res = AtomicUpdateItemPointer(index_entry_ptr, next_loc);
          PELOTON_ASSERT(res == true);
        } 
        else if (next_loc.IsNull() && !prev_loc.IsNull()) { //if curr position is the tail and its not the only item in LL => remove it.
          Debug("Purging tail [txn: %s]", pequinstore::BytesToHex(*transaction->GetTxnDig(), 16).c_str());
          auto prev_tgh = this->GetTileGroupById(prev_loc.block)->GetHeader();
          prev_tgh->SetNextItemPointer(prev_loc.offset, INVALID_ITEMPOINTER);
        }
        else{ //if curr position is the ONLY item in the linked list: Do nothing. We wan't to keep it in the index.
          Notice("Not updating anything: [txn: %s]", pequinstore::BytesToHex(*transaction->GetTxnDig(), 16).c_str());
          UW_ASSERT(next_loc.IsNull() && prev_loc.IsNull());  //Current tuple = head and tail of list
          //Note: If the only item in the linked list is purged, then we just keep it as part of the linked list. Note: versions *are* marked purged.

          // ItemPointer *index_entry_ptr = curr_tile_group_header->GetIndirection(curr_pointer.offset);
          // COMPILER_MEMORY_FENCE;

          // // Set the index header in an atomic way.
          // // We do it atomically because we don't want any one to see a half-done pointer. In case of contention, no one can update this
          // // pointer when we are updating it because we are holding the write lock. This update should success in its first trial.
          // UNUSED_ATTRIBUTE auto res = AtomicUpdateItemPointer(index_entry_ptr, ItemPointer(INVALID_OID, INVALID_OID));
          // PELOTON_ASSERT(res == true);
         
          //Panic("No case");
        }
}

void DataTable::UpgradeRowVersionCommitStatus(const storage::Tuple *tuple, std::shared_ptr<peloton::storage::TileGroup> curr_tile_group, TileGroupHeader *curr_tile_group_header, ItemPointer &curr_pointer, concurrency::TransactionContext *transaction, const Timestamp &ts){
    bool same_columns = true;
        bool should_upgrade = !curr_tile_group_header->GetCommitOrPrepare(curr_pointer.offset) && transaction->GetCommitOrPrepare(); //i.e. prev = prepare, curr tx = commit
        // NOTE: Check if we can upgrade a prepared tuple to committed
    
        /*for (int i = 0; i < active_tilegroup_count_; i++) {
          std::cerr << "tile group name: " << table_name << " and id is " << active_tile_groups_[i]->GetTileGroupId() << ". next tuple slot is " << active_tile_groups_[i]->GetNextTupleSlot() << std::endl;
        }*/


        // std::string encoded_key = target_table_->GetName();
        const auto *schema = curr_tile_group->GetAbstractTable()->GetSchema();
        for (uint32_t col_idx = 0; col_idx < schema->GetColumnCount(); col_idx++) {

          auto val1 = curr_tile_group->GetValue(curr_pointer.offset, col_idx);
          auto val2 = tuple->GetValue(col_idx);

          if (val1.ToString() != val2.ToString()) {
            // tile_group->SetValue(val2, curr_pointer.offset, col_idx);
            Notice("Different column[%d]: [%s] -> [%s]", col_idx, val1.ToString().c_str(), val2.ToString().c_str());
            same_columns = false;
          }
        }
        if(!same_columns){
          Notice("Column values of existing tuple (TS[%lu:%lu])", curr_tile_group_header->GetBasilTimestamp(curr_pointer.offset).getTimestamp(), curr_tile_group_header->GetBasilTimestamp(curr_pointer.offset).getID());
          for (uint32_t col_idx = 0; col_idx < schema->GetColumnCount(); col_idx++) {
            auto val1 = curr_tile_group->GetValue(curr_pointer.offset, col_idx);
            Notice("column[%d]: [%s]", col_idx, val1.ToString().c_str());
          }
          Notice("Column values of new tuple (TS[%lu:%lu])", ts.getTimestamp(), ts.getID());
          for (uint32_t col_idx = 0; col_idx < schema->GetColumnCount(); col_idx++) {
            auto val2 = tuple->GetValue(col_idx);
            Notice("column[%d]: [%s]", col_idx, val2.ToString().c_str());
          }

          Panic("Columns of TX[%s] (TS:[%lu:%lu]) don't match. Is upgrade? %d. Curr tuple status: %d. New tuple status: %d", 
              pequinstore::BytesToHex(*transaction->GetTxnDig(), 16).c_str(), ts.getTimestamp(), ts.getID(), should_upgrade,
              curr_tile_group_header->GetCommitOrPrepare(curr_pointer.offset), transaction->GetCommitOrPrepare());
        } 
        UW_ASSERT(same_columns); //TODO: remove same_columns check? Should always be the case..

        // For snapshotting upgrade from materialize to commit
        if (curr_tile_group_header->GetMaterialize(curr_pointer.offset) && !transaction->GetForceMaterialize()) {
          curr_tile_group_header->SetMaterialize(curr_pointer.offset, false);
          curr_tile_group_header->SetCommitOrPrepare(curr_pointer.offset, transaction->GetCommitOrPrepare());
        }

        if (should_upgrade && same_columns) {
          Debug("Upgrading tuple[%d:%d] from prepared to committed", curr_pointer.block, curr_pointer.offset);
          const pequinstore::proto::CommittedProof *proof =  transaction->GetCommittedProof();
          UW_ASSERT(proof);
          auto proof_ts = Timestamp(proof->txn().timestamp());
          Debug("Proof ts is %lu, %lu", proof_ts.getTimestamp(), proof_ts.getID());
          Debug("Current ts is %lu, %lu", ts.getTimestamp(), ts.getID());
          curr_tile_group_header->SetCommittedProof(curr_pointer.offset, proof);
          curr_tile_group_header->SetCommitOrPrepare(curr_pointer.offset, true);
          
          Debug("Wrote commit proof for tuple: [%lu:%lu]", curr_pointer.block, curr_pointer.offset);
        } else { // if prepare comes after commit; or if we prepare twice/commit twice  => Do nothing.
   
          if(curr_tile_group_header->GetCommitOrPrepare(curr_pointer.offset) == transaction->GetCommitOrPrepare() ) {
            Notice("Tried to prepare twice or commit twice for same TX. Try Upgrade [txn: %s]. Should upgrade? %d. Current commit/prepare state: %d. Txn state: %d. Curr ForceMat? %d. Tx ForceMat? %d. Same columns? %d. TS[%lu:%lu]. TXN-TS[%lu:%lu]. Dig[%s]. TXN-Dig[%s]. tile-group-header:offset [%lu:%lu]", 
                  pequinstore::BytesToHex(*transaction->GetTxnDig(), 16).c_str(), should_upgrade, curr_tile_group_header->GetCommitOrPrepare(curr_pointer.offset), transaction->GetCommitOrPrepare(),
                  curr_tile_group_header->GetMaterialize(curr_pointer.offset), transaction->GetForceMaterialize(),
                  same_columns, ts.getTimestamp(), ts.getID(), transaction->GetBasilTimestamp().getTimestamp(), transaction->GetBasilTimestamp().getID(),
                  pequinstore::BytesToHex(*curr_tile_group_header->GetTxnDig(curr_pointer.offset),16).c_str(), pequinstore::BytesToHex(*transaction->GetTxnDig(),16).c_str(),
                  curr_pointer.block, curr_pointer.offset);
            //Panic("Tried to prepare twice or commit twice for same TX: %s", );
          }
          else{
            Debug("[CPU:%d] Try Upgrade [txn: %s]. Should upgrade? %d. Current commit/prepare state: %d. Txn state: %d. Same columns? %d. TS[%lu:%lu]. TXN-TS[%lu:%lu]. Dig[%s]. TXN-Dig[%s]. tile-group-header:offset [%lu:%lu]", 
                 sched_getcpu(),  pequinstore::BytesToHex(*transaction->GetTxnDig(), 16).c_str(), should_upgrade, curr_tile_group_header->GetCommitOrPrepare(curr_pointer.offset), transaction->GetCommitOrPrepare(),
                  same_columns, ts.getTimestamp(), ts.getID(), transaction->GetBasilTimestamp().getTimestamp(), transaction->GetBasilTimestamp().getID(),
                  pequinstore::BytesToHex(*curr_tile_group_header->GetTxnDig(curr_pointer.offset),16).c_str(), pequinstore::BytesToHex(*transaction->GetTxnDig(),16).c_str(),
                  curr_pointer.block, curr_pointer.offset);
          }
        }
}

ItemPointer DataTable::InsertTuple(const storage::Tuple *tuple,
                                   concurrency::TransactionContext *transaction,
                                   bool &exists, bool &is_duplicate,
                                   ItemPointer &old_location,
                                   ItemPointer **index_entry_ptr,
                                   bool check_fk) {

  //if(transaction->GetCommitOrPrepare()) UW_ASSERT(transaction->GetCommittedProof()); //This doesn't play nice for CreateTable

  //Brutal impact on write speed. Disable setting this during loading?
  //std::lock_guard<std::mutex> guard(atomic_index); 
  if(transaction->GetTxnDig() && transaction->GetBasilTimestamp().getTimestamp() > 0) atomic_index.lock(); //disable for loading

  // auto lockScope = (transaction->GetTxnDig() == nullptr) ? 
  //                          std::unique_lock<std::mutex>() 
  //                        : std::unique_lock<std::mutex>(atomic_index);

  //take read lock around check if in index
  //while holding lock still. take a different write lock if true. take a different read lock if false.

  ItemPointer check = CheckIfInIndex(tuple, transaction);

  //If there is no version linked list for this row yet: Create a new one.
  if (check.block == 0 && check.offset == 0) {

    is_duplicate = false;
    ItemPointer location = GetEmptyTupleSlot(tuple);
    if (location.block == INVALID_OID) {
      Panic("invalid location?");
      LOG_TRACE("Failed to get tuple slot.");
      Debug("Invalid write to tile-group-header:offset [%lu:%lu]", location.block, location.offset);
      return INVALID_ITEMPOINTER;
    }
    SetPequinMetaData(location, transaction);

    // ItemPointer *old_location;
    auto result = InsertTuple(tuple, location, transaction, old_location, index_entry_ptr, check_fk);
    if (result == false) {
      Panic("InsertTuple result false");
      //  check_fk = false;
      exists = false;  //TODO: Always set false.
      // return INVALID_ITEMPOINTER;
    }

    //if(transaction->GetBasilTimestamp().getTimestamp() > 0) Notice("Inserted tuple[%s] to tile-group-header:offset [%lu:%lu]", tuple->GetInfo().c_str(), location.block, location.offset); //don't print during load time

    // if(transaction->GetTxnDig() && transaction->GetBasilTimestamp().getTimestamp() > 0) Notice("Data Table txn[%lu:%lu]. Insert new. Commit (or prepare) ? %d", transaction->GetBasilTimestamp().getTimestamp(), transaction->GetBasilTimestamp().getID(), transaction->GetCommitOrPrepare());


    if(transaction->GetTxnDig() && transaction->GetBasilTimestamp().getTimestamp() > 0)  atomic_index.unlock();

    return location; //*old_location;

  }
  //If there is a version linked list: Find right position to insert.
  else {
    exists = false;
  // if(transaction->GetTxnDig() && transaction->GetBasilTimestamp().getTimestamp() > 0) atomic_index.unlock();

    auto tile_group = this->GetTileGroupById(check.block);
    auto tile_group_header = tile_group->GetHeader();

    ////// Find the current linked list header (atomically)

    ItemPointer *index_entry_ptr = tile_group_header->GetIndirection(check.offset);
    if(!index_entry_ptr){
      index_entry_ptr = &check;
    }
    UW_ASSERT(index_entry_ptr);

    ///Check whether tuple already exists, and whether it state just needs to be updated
    is_duplicate = CheckRowVersionUpdate(tuple, tile_group, tile_group_header, index_entry_ptr, check, transaction);

     //If this is a new tuple => create new meta data and insert to respective indexes.
    if(!is_duplicate){
      Debug("Data table performing update");
     
      ItemPointer location = GetEmptyTupleSlot(tuple);
      if (location.block == INVALID_OID) {
        Panic("Invalid location");
        LOG_TRACE("Failed to get tuple slot.");
        return INVALID_ITEMPOINTER;
      }

      // //Check whether prev is Null by default.
      // auto new_header = this->GetTileGroupById(location.block)->GetHeader();
      // UW_ASSERT(new_header->GetPrevItemPointer(location.offset).IsNull());
  

      SetPequinMetaData(location, transaction);

        //     //TODO: Do this only in Perform Update... There is no point adding to the index before this new tuple is added yet.
      /** Insert this tuple into secondary index. Note that since this is an "update", the primary index will already have a tuple with the same primary index cols */
      int index_count = GetIndexCount();
      for (int index_itr = index_count - 1; index_itr >= 0; --index_itr) {
        auto index = GetIndex(index_itr);
        if (index == nullptr)
          continue;
        auto index_schema = index->GetKeySchema();
        auto indexed_columns = index_schema->GetIndexedColumns();
        std::unique_ptr<storage::Tuple> key(new storage::Tuple(index_schema, true));
        key->SetFromTuple(tuple, indexed_columns, index->GetPool());

        switch (index->GetIndexType()) {
        case IndexConstraintType::DEFAULT:
        default:
          //index->InsertEntry(key.get(), curr_tile_group_header->GetIndirection(curr_pointer.offset)); //FIXME: This line seems wrong.
          index->InsertEntry(key.get(), index_entry_ptr);
          break;
        }
      }
    
      bool result = false;
      old_location = check;
      IncreaseTupleCount(1);

      if (result == false) {
        Debug("InsertTuple result false");
        //  check_fk = false;
        exists = false;
        // return INVALID_ITEMPOINTER;
      }
    
      if(transaction->GetTxnDig() && transaction->GetBasilTimestamp().getTimestamp() > 0) atomic_index.unlock();
      return location;
    }
  
    if(transaction->GetTxnDig() && transaction->GetBasilTimestamp().getTimestamp() > 0) atomic_index.unlock();
  }

  return ItemPointer(0, 0);
}

bool DataTable::InsertTuple(const AbstractTuple *tuple, ItemPointer location,
                            concurrency::TransactionContext *transaction,
                            ItemPointer &old_location,
                            ItemPointer **index_entry_ptr, bool check_fk) {
  if (CheckConstraints(tuple) == false) {
    Debug("Index constraint violated");
    // std::cerr << "Index Constraint violated" << std::endl;
    LOG_TRACE("InsertTuple(): Constraint violated");
    return false;
  }

  // the upper layer may not pass a index_entry_ptr (default value: nullptr)
  // into the function.
  // in this case, we have to create a temp_ptr to hold the content.
  ItemPointer *temp_ptr = nullptr;
  if (index_entry_ptr == nullptr) {
    index_entry_ptr = &temp_ptr;
  }

  LOG_TRACE("Location: %u, %u", location.block, location.offset);

  auto index_count = GetIndexCount();
  if (index_count == 0) {
    if (check_fk && CheckForeignKeyConstraints(tuple, transaction) == false) {
      LOG_TRACE("ForeignKey constraint violated");
      return false;
    }
    IncreaseTupleCount(1);
    return true;
  }
  // Index checks and updates
  if (InsertInIndexes(tuple, location, transaction, index_entry_ptr,
                      old_location) == false) {
    Debug("Index Constraint violated old location");
    // std::cerr << "Index Constraint violated old location" << std::endl;
    LOG_TRACE("Index constraint violated");
    return false;
  }

  // ForeignKey checks
  if (check_fk && CheckForeignKeyConstraints(tuple, transaction) == false) {
    LOG_TRACE("ForeignKey constraint violated");
    return false;
  }

  PELOTON_ASSERT((*index_entry_ptr)->block == location.block &&
                 (*index_entry_ptr)->offset == location.offset);

  // Increase the table's number of tuples by 1
  IncreaseTupleCount(1);
  return true;
}

// insert tuple into a table that is without index.
ItemPointer DataTable::InsertTuple(const storage::Tuple *tuple) {
  ItemPointer location = GetEmptyTupleSlot(tuple);
  if (location.block == INVALID_OID) {
    LOG_TRACE("Failed to get tuple slot.");
    return INVALID_ITEMPOINTER;
  }

  LOG_TRACE("Location: %u, %u", location.block, location.offset);

  UNUSED_ATTRIBUTE auto index_count = GetIndexCount();
  PELOTON_ASSERT(index_count == 0);
  // Increase the table's number of tuples by 1
  IncreaseTupleCount(1);
  return location;
}

ItemPointer
DataTable::CheckIfInIndex(const storage::Tuple *tuple,
                          concurrency::TransactionContext *transaction) {

  int index_count = GetIndexCount();

  auto &transaction_manager =
      concurrency::TransactionManagerFactory::GetInstance();

  std::function<bool(const void *)> fn =
      std::bind(&concurrency::TransactionManager::IsOccupied,
                &transaction_manager, transaction, std::placeholders::_1);

  std::vector<ItemPointer *> old_locations;

  for (int index_itr = index_count - 1; index_itr >= 0; --index_itr) {
    auto index = GetIndex(index_itr);
    if (index == nullptr)
      continue;
    auto index_schema = index->GetKeySchema();
    auto indexed_columns = index_schema->GetIndexedColumns();
    std::unique_ptr<storage::Tuple> key(new storage::Tuple(index_schema, true));
    key->SetFromTuple(tuple, indexed_columns, index->GetPool());

    if (index->GetIndexType() != IndexConstraintType::PRIMARY_KEY) {
      continue;
    }

    index->ScanKey(key.get(), old_locations);
    for (int i = 0; i < old_locations.size(); i++) {
      // NOTE: For testing
      /*if (fn(old_locations[i])) {
        return *old_locations[i];
      }*/
      return *old_locations[i];
    }
    return ItemPointer(0, 0);
    /*ItemPointer *first_element = old_locations[0];
    if (first_element->IsNull()) {
      Debug("First element is null"); // std::cerr << "First element is null"
                                      // << std::endl;
    }*/

    // auto res = index->CondInsertEntry(key.get(), index_entry_ptr, fn);
    // return res;

    // Handle failure
    // If some of the indexes have been inserted,
    // the pointer has a chance to be dereferenced by readers and it cannot be
    // deleted
    //*index_entry_ptr = nullptr;
  }

  return ItemPointer(0, 0);
}

/**
 * @brief Insert a tuple into all indexes. If index is primary/unique,
 * check visibility of existing
 * index entries.
 * @warning This still doesn't guarantee serializability.
 *
 * @returns True on success, false if a visible entry exists (in case of
 *primary/unique).
 */
bool DataTable::InsertInIndexes(const AbstractTuple *tuple,
                                ItemPointer location,
                                concurrency::TransactionContext *transaction,
                                ItemPointer **index_entry_ptr,
                                ItemPointer &old_location) {
  int index_count = GetIndexCount();

  size_t active_indirection_array_id = number_of_tuples_ % active_indirection_array_count_;

  size_t indirection_offset = INVALID_INDIRECTION_OFFSET;

  while (true) {
    auto active_indirection_array = active_indirection_arrays_[active_indirection_array_id];
    indirection_offset = active_indirection_array->AllocateIndirection();

    if (indirection_offset != INVALID_INDIRECTION_OFFSET) {
      *index_entry_ptr = active_indirection_array->GetIndirectionByOffset(indirection_offset);
      break;
    }
  }

    // Set the indirection offset
  auto tile_group_header = this->GetTileGroupById(location.block)->GetHeader();
  tile_group_header->SetIndirectionOffset(location.offset, indirection_offset);

  (*index_entry_ptr)->block = location.block;
  (*index_entry_ptr)->offset = location.offset;

  if (indirection_offset == INDIRECTION_ARRAY_MAX_SIZE - 1) {
    AddDefaultIndirectionArray(active_indirection_array_id);
  }

  auto &transaction_manager = concurrency::TransactionManagerFactory::GetInstance();

  std::function<bool(const void *)> fn = std::bind(&concurrency::TransactionManager::IsOccupied, &transaction_manager, transaction, std::placeholders::_1);

  // Since this is NOT protected by a lock, concurrent insert may happen.
  bool res = true;
  std::vector<ItemPointer *> old_locations;
  int success_count = 0;

  for (int index_itr = index_count - 1; index_itr >= 0; --index_itr) {
    auto index = GetIndex(index_itr);
    if (index == nullptr)
      continue;
    auto index_schema = index->GetKeySchema();
    auto indexed_columns = index_schema->GetIndexedColumns();
    std::unique_ptr<storage::Tuple> key(new storage::Tuple(index_schema, true));
    key->SetFromTuple(tuple, indexed_columns, index->GetPool());

    switch (index->GetIndexType()) {
    case IndexConstraintType::PRIMARY_KEY:
    case IndexConstraintType::UNIQUE: {
      // get unique tuple from primary/unique index.
      // if in this index there has been a visible or uncommitted
      // <key, location> pair, this constraint is violated
      res = index->CondInsertEntry(key.get(), *index_entry_ptr, fn);
      // index->ScanKey(key.get(), old_locations);
      // NOTE: Commented this out to support linked list version updates
      // index->InsertEntry(key.get(), *index_entry_ptr);
    } break;

    case IndexConstraintType::DEFAULT:
    default:
      index->InsertEntry(key.get(), *index_entry_ptr);
      break;
    }

    // Handle failure
    if (res == false) {
      // If some of the indexes have been inserted,
      // the pointer has a chance to be dereferenced by readers and it cannot be
      // deleted
      //*index_entry_ptr = nullptr;
      index->ScanKey(key.get(), old_locations);
      ItemPointer *first_element = old_locations[0];
      if (first_element->IsNull()) {
        Debug("First element is null"); // std::cerr << "First element is null"
                                        // << std::endl;
      }
      /*if (old_location->IsNull()) {
        std::cerr << "Old location is null" << std::endl;
      }*/
      old_location.block = first_element->block;
      old_location.offset = first_element->offset;
      // std::cerr << "Old location block is " << old_location.block << " and
      // offset is " << old_location.offset << std::endl;

      return false;
    } else {
      success_count += 1;
    }
    LOG_TRACE("Index constraint check on %s passed.", index->GetName().c_str());
  }

  return true;
}

bool DataTable::InsertInSecondaryIndexes(
    const AbstractTuple *tuple, const TargetList *targets_ptr,
    concurrency::TransactionContext *transaction,
    ItemPointer *index_entry_ptr) {
  int index_count = GetIndexCount();
  // Transform the target list into a hash set
  // when attempting to perform insertion to a secondary index,
  // we must check whether the updated column is a secondary index column.
  // insertion happens only if the updated column is a secondary index column.
  if (targets_ptr == nullptr) {
    Debug("Targets pointer == null"); // std::cerr << "Targets pointer is null"
                                      // << std::endl;
  }
  std::unordered_set<oid_t> targets_set;
  if (targets_ptr != nullptr) {
    for (auto target : *targets_ptr) {
      targets_set.insert(target.first);
    }
  }

  // std::cerr << "After iterating through targets_ptr" << std::endl;

  bool res = true;

  auto &transaction_manager =
      concurrency::TransactionManagerFactory::GetInstance();

  std::function<bool(const void *)> fn =
      std::bind(&concurrency::TransactionManager::IsOccupied,
                &transaction_manager, transaction, std::placeholders::_1);

  // Check existence for primary/unique indexes
  // Since this is NOT protected by a lock, concurrent insert may happen.
  for (int index_itr = index_count - 1; index_itr >= 0; --index_itr) {
    auto index = GetIndex(index_itr);
    if (index == nullptr)
      continue;
    auto index_schema = index->GetKeySchema();
    auto indexed_columns = index_schema->GetIndexedColumns();

    if (index->GetIndexType() == IndexConstraintType::PRIMARY_KEY) {
      continue;
    }

    // Check if we need to update the secondary index
    bool updated = false;
    for (auto col : indexed_columns) {
      if (targets_set.find(col) != targets_set.end()) {
        updated = true;
        break;
      }
    }

    // If attributes on key are not updated, skip the index update
    if (updated == false) {
      continue;
    }

    // Key attributes are updated, insert a new entry in all secondary index
    std::unique_ptr<storage::Tuple> key(new storage::Tuple(index_schema, true));

    key->SetFromTuple(tuple, indexed_columns, index->GetPool());
    // std::cerr << "After setting key from tuple" << std::endl;

    switch (index->GetIndexType()) {
    case IndexConstraintType::PRIMARY_KEY:
    case IndexConstraintType::UNIQUE: {
      res = index->CondInsertEntry(key.get(), index_entry_ptr, fn);
    } break;
    case IndexConstraintType::DEFAULT:
    default:
      index->InsertEntry(key.get(), index_entry_ptr);
      break;
    }
    LOG_TRACE("Index constraint check on %s passed.", index->GetName().c_str());
  }
  return res;
}

/**
 * @brief This function checks any other table which has a foreign key
 *constraint
 * referencing the current table, where a tuple is updated/deleted. The final
 * result depends on the type of cascade action.
 *
 * @param prev_tuple: The tuple which will be updated/deleted in the current
 * table
 * @param new_tuple: The new tuple after update. This parameter is ignored
 * if is_update is false.
 * @param current_txn: The current transaction context
 * @param context: The executor context passed from upper level
 * @param is_update: whether this is a update action (false means delete)
 *
 * @return True if the check is successful (nothing happens) or the cascade
 *operation
 * is done properly. Otherwise returns false. Note that the transaction result
 * is not set in this function.
 */
bool DataTable::CheckForeignKeySrcAndCascade(
    storage::Tuple *prev_tuple, storage::Tuple *new_tuple,
    concurrency::TransactionContext *current_txn,
    executor::ExecutorContext *context, bool is_update) {
  if (!schema->HasForeignKeySources())
    return true;

  auto &transaction_manager =
      concurrency::TransactionManagerFactory::GetInstance();

  for (auto cons : schema->GetForeignKeySources()) {
    // Check if any row in the source table references the current tuple
    oid_t source_table_id = cons->GetTableOid();
    storage::DataTable *src_table = nullptr;
    try {
      src_table = (storage::DataTable *)storage::StorageManager::GetInstance()
                      ->GetTableWithOid(GetDatabaseOid(), source_table_id);
    } catch (CatalogException &e) {
      LOG_TRACE("Can't find table %d! Return false", source_table_id);
      return false;
    }

    int src_table_index_count = src_table->GetIndexCount();
    for (int iter = 0; iter < src_table_index_count; iter++) {
      auto index = src_table->GetIndex(iter);
      if (index == nullptr)
        continue;

      // Make sure this is the right index to search in
      if (index->GetOid() == cons->GetIndexOid() &&
          index->GetMetadata()->GetKeyAttrs() == cons->GetColumnIds()) {
        LOG_DEBUG("Searching in source tables's fk index...\n");

        std::vector<oid_t> key_attrs = cons->GetColumnIds();
        std::unique_ptr<catalog::Schema> fk_schema(
            catalog::Schema::CopySchema(src_table->GetSchema(), key_attrs));
        std::unique_ptr<storage::Tuple> key(
            new storage::Tuple(fk_schema.get(), true));

        key->SetFromTuple(prev_tuple, cons->GetFKSinkColumnIds(),
                          index->GetPool());

        std::vector<ItemPointer *> location_ptrs;
        index->ScanKey(key.get(), location_ptrs);

        if (location_ptrs.size() > 0) {
          LOG_DEBUG("Something found in the source table!\n");

          for (ItemPointer *ptr : location_ptrs) {
            auto src_tile_group = src_table->GetTileGroupById(ptr->block);
            auto src_tile_group_header = src_tile_group->GetHeader();

            auto visibility = transaction_manager.IsVisible(
                current_txn, src_tile_group_header, ptr->offset,
                VisibilityIdType::COMMIT_ID);

            if (visibility != VisibilityType::OK)
              continue;

            switch (cons->GetFKUpdateAction()) {
            // Currently NOACTION is the same as RESTRICT
            case FKConstrActionType::NOACTION:
            case FKConstrActionType::RESTRICT: {
              return false;
            }
            case FKConstrActionType::CASCADE:
            default: {
              // Update
              bool src_is_owner = transaction_manager.IsOwner(
                  current_txn, src_tile_group_header, ptr->offset);

              // Read the referencing tuple, update the read timestamp so that
              // we can
              // delete it later
              bool ret = transaction_manager.PerformRead(
                  current_txn, *ptr, src_tile_group_header, true);

              if (ret == false) {
                if (src_is_owner) {
                  transaction_manager.YieldOwnership(
                      current_txn, src_tile_group_header, ptr->offset);
                }
                return false;
              }

              ContainerTuple<storage::TileGroup> src_old_tuple(
                  src_tile_group.get(), ptr->offset);
              storage::Tuple src_new_tuple(src_table->GetSchema(), true);

              if (is_update) {
                for (oid_t col_itr = 0;
                     col_itr < src_table->GetSchema()->GetColumnCount();
                     col_itr++) {
                  type::Value val = src_old_tuple.GetValue(col_itr);
                  src_new_tuple.SetValue(col_itr, val, context->GetPool());
                }

                // Set the primary key fields
                for (oid_t k = 0; k < key_attrs.size(); k++) {
                  auto src_col_index = key_attrs[k];
                  auto sink_col_index = cons->GetFKSinkColumnIds()[k];
                  src_new_tuple.SetValue(src_col_index,
                                         new_tuple->GetValue(sink_col_index),
                                         context->GetPool());
                }
              }

              ItemPointer new_loc = src_table->InsertEmptyVersion(current_txn);

              if (new_loc.IsNull()) {
                if (src_is_owner == false) {
                  transaction_manager.YieldOwnership(
                      current_txn, src_tile_group_header, ptr->offset);
                }
                return false;
              }

              transaction_manager.PerformDelete(current_txn, *ptr, new_loc);

              // For delete cascade, just stop here
              if (is_update == false) {
                break;
              }

              ItemPointer *index_entry_ptr = nullptr;
              peloton::ItemPointer location = src_table->InsertTuple(
                  &src_new_tuple, current_txn, &index_entry_ptr, false);

              if (location.block == INVALID_OID) {
                return false;
              }

              transaction_manager.PerformInsert(current_txn, location,
                                                index_entry_ptr);

              break;
            }
            }
          }
        }

        break;
      }
    }
  }

  return true;
}

// PA - looks like the FIXME has been done. We check to see if the key
// is visible
/**
 * @brief Check if all the foreign key constraints on this table
 * is satisfied by checking whether the key exist in the referred table
 *
 * FIXME: this still does not guarantee correctness under concurrent transaction
 *   because it only check if the key exists the referred table's index
 *   -- however this key might be a uncommitted key that is not visible to
 *   and it might be deleted if that txn abort.
 *   We should modify this function and add logic to check
 *   if the result of the ScanKey is visible.
 *
 * @returns True on success, false if any foreign key constraints fail
 */
bool DataTable::CheckForeignKeyConstraints(
    const AbstractTuple *tuple, concurrency::TransactionContext *transaction) {
  for (auto foreign_key : schema->GetForeignKeyConstraints()) {
    oid_t sink_table_id = foreign_key->GetFKSinkTableOid();
    storage::DataTable *ref_table = nullptr;
    try {
      ref_table = (storage::DataTable *)storage::StorageManager::GetInstance()
                      ->GetTableWithOid(database_oid, sink_table_id);
    } catch (CatalogException &e) {
      LOG_ERROR("Can't find table %d! Return false", sink_table_id);
      return false;
    }
    int ref_table_index_count = ref_table->GetIndexCount();

    for (int index_itr = ref_table_index_count - 1; index_itr >= 0;
         --index_itr) {
      auto index = ref_table->GetIndex(index_itr);
      if (index == nullptr)
        continue;

      // The foreign key constraints only refer to the primary key
      if (index->GetIndexType() == IndexConstraintType::PRIMARY_KEY) {
        std::vector<oid_t> key_attrs = foreign_key->GetFKSinkColumnIds();
        std::unique_ptr<catalog::Schema> foreign_key_schema(
            catalog::Schema::CopySchema(ref_table->schema, key_attrs));
        std::unique_ptr<storage::Tuple> key(
            new storage::Tuple(foreign_key_schema.get(), true));
        key->SetFromTuple(tuple, foreign_key->GetColumnIds(), index->GetPool());

        LOG_TRACE("check key: %s", key->GetInfo().c_str());
        std::vector<ItemPointer *> location_ptrs;
        index->ScanKey(key.get(), location_ptrs);

        // if this key doesn't exist in the referred column
        if (location_ptrs.size() == 0) {
          LOG_DEBUG("The key: %s does not exist in table %s\n",
                    key->GetInfo().c_str(), ref_table->GetName().c_str());
          return false;
        }

        // Check the visibility of the result
        auto tile_group = ref_table->GetTileGroupById(location_ptrs[0]->block);
        auto tile_group_header = tile_group->GetHeader();

        auto &transaction_manager =
            concurrency::TransactionManagerFactory::GetInstance();
        auto visibility = transaction_manager.IsVisible(
            transaction, tile_group_header, location_ptrs[0]->offset,
            VisibilityIdType::READ_ID);

        if (visibility != VisibilityType::OK) {
          LOG_DEBUG("The key: %s is not yet visible in table %s, visibility "
                    "type: %s.\n",
                    key->GetInfo().c_str(), ref_table->GetName().c_str(),
                    VisibilityTypeToString(visibility).c_str());
          return false;
        }

        break;
      }
    }
  }

  return true;
}

//===--------------------------------------------------------------------===//
// STATS
//===--------------------------------------------------------------------===//

/**
 * @brief Increase the number of tuples in this table
 * @param amount amount to increase
 */
void DataTable::IncreaseTupleCount(const size_t &amount) {
  number_of_tuples_ += amount;
  dirty_ = true;
}

/**
 * @brief Decrease the number of tuples in this table
 * @param amount amount to decrease
 */
void DataTable::DecreaseTupleCount(const size_t &amount) {
  number_of_tuples_ -= amount;
  dirty_ = true;
}

/**
 * @brief Set the number of tuples in this table
 * @param num_tuples number of tuples
 */
void DataTable::SetTupleCount(const size_t &num_tuples) {
  number_of_tuples_ = num_tuples;
  dirty_ = true;
}

/**
 * @brief Get the number of tuples in this table
 * @return number of tuples
 */
size_t DataTable::GetTupleCount() const { return number_of_tuples_; }

/**
 * @brief return dirty flag
 * @return dirty flag
 */
bool DataTable::IsDirty() const { return dirty_; }

/**
 * @brief Reset dirty flag
 */
void DataTable::ResetDirty() { dirty_ = false; }

//===--------------------------------------------------------------------===//
// TILE GROUP
//===--------------------------------------------------------------------===//

TileGroup *
DataTable::GetTileGroupWithLayout(std::shared_ptr<const Layout> layout) {
  oid_t tile_group_id =
      storage::StorageManager::GetInstance()->GetNextTileGroupId();
  return (AbstractTable::GetTileGroupWithLayout(database_oid, tile_group_id,
                                                layout, tuples_per_tilegroup_));
}

oid_t DataTable::AddDefaultIndirectionArray(
    const size_t &active_indirection_array_id) {
  auto &manager = catalog::Manager::GetInstance();
  oid_t indirection_array_id = manager.GetNextIndirectionArrayId();

  std::shared_ptr<IndirectionArray> indirection_array(
      new IndirectionArray(indirection_array_id));
  manager.AddIndirectionArray(indirection_array_id, indirection_array);

  COMPILER_MEMORY_FENCE;

  active_indirection_arrays_[active_indirection_array_id] = indirection_array;

  return indirection_array_id;
}

oid_t DataTable::AddDefaultTileGroup() {
  size_t active_tile_group_id = number_of_tuples_ % active_tilegroup_count_;
  return AddDefaultTileGroup(active_tile_group_id);
}

oid_t DataTable::AddDefaultTileGroup(const size_t &active_tile_group_id) {
  oid_t tile_group_id = INVALID_OID;

  // Create a tile group with that partitioning
  std::shared_ptr<TileGroup> tile_group(
      GetTileGroupWithLayout(default_layout_));
  PELOTON_ASSERT(tile_group.get());

  tile_group_id = tile_group->GetTileGroupId();

  LOG_TRACE("Added a tile group ");
  tile_groups_.Append(tile_group_id);

  // add tile group metadata in locator
  storage::StorageManager::GetInstance()->AddTileGroup(tile_group_id,
                                                       tile_group);

  COMPILER_MEMORY_FENCE;

  active_tile_groups_[active_tile_group_id] = tile_group;

  // we must guarantee that the compiler always add tile group before adding
  // tile_group_count_.
  COMPILER_MEMORY_FENCE;

  tile_group_count_++;

  LOG_TRACE("Recording tile group : %u ", tile_group_id);

  return tile_group_id;
}

void DataTable::AddTileGroupWithOidForRecovery(const oid_t &tile_group_id) {
  PELOTON_ASSERT(tile_group_id);

  std::vector<catalog::Schema> schemas;
  schemas.push_back(*schema);
  std::shared_ptr<const Layout> layout = nullptr;

  // The TileGroup for recovery is always added in ROW layout,
  // This was a part of the previous design. If you are planning
  // to change this, make sure the layout is added to the catalog
  if (default_layout_->IsRowStore()) {
    layout = default_layout_;
  } else {
    layout = std::shared_ptr<const Layout>(
        new const Layout(schema->GetColumnCount()));
  }

  std::shared_ptr<TileGroup> tile_group(TileGroupFactory::GetTileGroup(
      database_oid, table_oid, tile_group_id, this, schemas, layout,
      tuples_per_tilegroup_));

  auto tile_groups_exists = tile_groups_.Contains(tile_group_id);

  if (tile_groups_exists == false) {
    tile_groups_.Append(tile_group_id);

    LOG_TRACE("Added a tile group ");

    // add tile group metadata in locator
    storage::StorageManager::GetInstance()->AddTileGroup(tile_group_id,
                                                         tile_group);

    // we must guarantee that the compiler always add tile group before adding
    // tile_group_count_.
    COMPILER_MEMORY_FENCE;

    tile_group_count_++;

    LOG_TRACE("Recording tile group : %u ", tile_group_id);
  }
}

// NOTE: This function is only used in test cases.
void DataTable::AddTileGroup(const std::shared_ptr<TileGroup> &tile_group) {
  size_t active_tile_group_id = number_of_tuples_ % active_tilegroup_count_;

  active_tile_groups_[active_tile_group_id] = tile_group;

  oid_t tile_group_id = tile_group->GetTileGroupId();

  tile_groups_.Append(tile_group_id);

  // add tile group in catalog
  storage::StorageManager::GetInstance()->AddTileGroup(tile_group_id,
                                                       tile_group);

  // we must guarantee that the compiler always add tile group before adding
  // tile_group_count_.
  COMPILER_MEMORY_FENCE;

  tile_group_count_++;

  LOG_TRACE("Recording tile group : %u ", tile_group_id);
}

size_t DataTable::GetTileGroupCount() const { return tile_group_count_; }

std::shared_ptr<storage::TileGroup>
DataTable::GetTileGroup(const std::size_t &tile_group_offset) const {
  PELOTON_ASSERT(tile_group_offset < GetTileGroupCount());

  auto tile_group_id =
      tile_groups_.FindValid(tile_group_offset, invalid_tile_group_id);

  return GetTileGroupById(tile_group_id);
}

std::shared_ptr<storage::TileGroup>
DataTable::GetTileGroupById(const oid_t &tile_group_id) const {
  auto storage_manager = storage::StorageManager::GetInstance();
  return storage_manager->GetTileGroup(tile_group_id);
}

void DataTable::DropTileGroups() {
  auto storage_manager = storage::StorageManager::GetInstance();
  auto tile_groups_size = tile_groups_.GetSize();
  std::size_t tile_groups_itr;

  for (tile_groups_itr = 0; tile_groups_itr < tile_groups_size;
       tile_groups_itr++) {
    auto tile_group_id = tile_groups_.Find(tile_groups_itr);

    if (tile_group_id != invalid_tile_group_id) {
      // drop tile group in catalog
      storage_manager->DropTileGroup(tile_group_id);
    }
  }

  // Clear array
  tile_groups_.Clear();

  tile_group_count_ = 0;
}

//===--------------------------------------------------------------------===//
// INDEX
//===--------------------------------------------------------------------===//

void DataTable::AddIndex(std::shared_ptr<index::Index> index) {
  // Add index
  indexes_.Append(index);

  // Add index column info
  auto index_columns_ = index->GetMetadata()->GetKeyAttrs();
  std::set<oid_t> index_columns_set(index_columns_.begin(),
                                    index_columns_.end());

  indexes_columns_.push_back(index_columns_set);
}

std::shared_ptr<index::Index>
DataTable::GetIndexWithOid(const oid_t &index_oid) {
  std::shared_ptr<index::Index> ret_index;
  auto index_count = indexes_.GetSize();

  for (std::size_t index_itr = 0; index_itr < index_count; index_itr++) {
    ret_index = indexes_.Find(index_itr);
    if (ret_index != nullptr && ret_index->GetOid() == index_oid) {
      break;
    }
  }
  if (ret_index == nullptr) {
    throw CatalogException("No index with oid = " + std::to_string(index_oid) +
                           " is found");
  }
  return ret_index;
}

void DataTable::DropIndexWithOid(const oid_t &index_oid) {
  oid_t index_offset = 0;
  std::shared_ptr<index::Index> index;
  auto index_count = indexes_.GetSize();

  for (std::size_t index_itr = 0; index_itr < index_count; index_itr++) {
    index = indexes_.Find(index_itr);
    if (index != nullptr && index->GetOid() == index_oid) {
      break;
    }
  }

  PELOTON_ASSERT(index_offset < indexes_.GetSize());

  // Drop the index
  indexes_.Update(index_offset, nullptr);

  // Drop index column info
  indexes_columns_[index_offset].clear();
}

void DataTable::DropIndexes() {
  // TODO: iterate over all indexes, and actually drop them

  indexes_.Clear();

  indexes_columns_.clear();
}

// This is a dangerous function, use GetIndexWithOid() instead. Note
// that the returned index could be a nullptr once we can drop index
// with oid (due to a limitation of LockFreeArray).
std::shared_ptr<index::Index> DataTable::GetIndex(const oid_t &index_offset) {
  PELOTON_ASSERT(index_offset < indexes_.GetSize());
  auto ret_index = indexes_.Find(index_offset);

  return ret_index;
}

//
std::set<oid_t> DataTable::GetIndexAttrs(const oid_t &index_offset) const {
  PELOTON_ASSERT(index_offset < GetIndexCount());

  auto index_attrs = indexes_columns_.at(index_offset);

  return index_attrs;
}

oid_t DataTable::GetIndexCount() const {
  size_t index_count = indexes_.GetSize();

  return index_count;
}

oid_t DataTable::GetValidIndexCount() const {
  std::shared_ptr<index::Index> index;
  auto index_count = indexes_.GetSize();
  oid_t valid_index_count = 0;

  for (std::size_t index_itr = 0; index_itr < index_count; index_itr++) {
    index = indexes_.Find(index_itr);
    if (index == nullptr) {
      continue;
    }

    valid_index_count++;
  }

  return valid_index_count;
}

// Get the schema for the new transformed tile group
std::vector<catalog::Schema>
TransformTileGroupSchema(storage::TileGroup *tile_group, const Layout &layout) {
  std::vector<catalog::Schema> new_schema;
  oid_t orig_tile_offset, orig_tile_column_offset;
  oid_t new_tile_offset, new_tile_column_offset;
  auto tile_group_layout = tile_group->GetLayout();

  // First, get info from the original tile group's schema
  std::map<oid_t, std::map<oid_t, catalog::Column>> schemas;

  uint32_t column_count = layout.GetColumnCount();
  for (oid_t col_id = 0; col_id < column_count; col_id++) {
    // Get TileGroup layout's tile and offset for col_id.
    tile_group_layout.LocateTileAndColumn(col_id, orig_tile_offset,
                                          orig_tile_column_offset);
    // Get new layout's tile and offset for col_id.
    layout.LocateTileAndColumn(col_id, new_tile_offset, new_tile_column_offset);

    // Get the column info from original tile
    auto tile = tile_group->GetTile(orig_tile_offset);
    PELOTON_ASSERT(tile != nullptr);
    auto orig_schema = tile->GetSchema();
    auto column_info = orig_schema->GetColumn(orig_tile_column_offset);
    schemas[new_tile_offset][new_tile_column_offset] = column_info;
  }

  // Then, build the new schema
  for (auto schemas_tile_entry : schemas) {
    std::vector<catalog::Column> columns;
    for (auto schemas_column_entry : schemas_tile_entry.second)
      columns.push_back(schemas_column_entry.second);

    catalog::Schema tile_schema(columns);
    new_schema.push_back(tile_schema);
  }

  return new_schema;
}

// Set the transformed tile group column-at-a-time
void SetTransformedTileGroup(storage::TileGroup *orig_tile_group,
                             storage::TileGroup *new_tile_group) {
  auto new_layout = new_tile_group->GetLayout();
  auto orig_layout = orig_tile_group->GetLayout();

  // Check that both tile groups have the same schema
  // Currently done by checking that the number of columns are equal
  // TODO Pooja: Handle schema equality for multiple schema versions.
  UNUSED_ATTRIBUTE auto new_column_count = new_layout.GetColumnCount();
  UNUSED_ATTRIBUTE auto orig_column_count = orig_layout.GetColumnCount();
  PELOTON_ASSERT(new_column_count == orig_column_count);

  oid_t orig_tile_offset, orig_tile_column_offset;
  oid_t new_tile_offset, new_tile_column_offset;

  auto column_count = new_column_count;
  auto tuple_count = orig_tile_group->GetAllocatedTupleCount();
  // Go over each column copying onto the new tile group
  for (oid_t column_itr = 0; column_itr < column_count; column_itr++) {
    // Locate the original base tile and tile column offset
    orig_layout.LocateTileAndColumn(column_itr, orig_tile_offset,
                                    orig_tile_column_offset);

    new_layout.LocateTileAndColumn(column_itr, new_tile_offset,
                                   new_tile_column_offset);

    auto orig_tile = orig_tile_group->GetTile(orig_tile_offset);
    auto new_tile = new_tile_group->GetTile(new_tile_offset);

    // Copy the column over to the new tile group
    for (oid_t tuple_itr = 0; tuple_itr < tuple_count; tuple_itr++) {
      type::Value val =
          (orig_tile->GetValue(tuple_itr, orig_tile_column_offset));
      new_tile->SetValue(val, tuple_itr, new_tile_column_offset);
    }
  }

  // Finally, copy over the tile header
  auto header = orig_tile_group->GetHeader();
  auto new_header = new_tile_group->GetHeader();
  *new_header = *header;
}

storage::TileGroup *
DataTable::TransformTileGroup(const oid_t &tile_group_offset,
                              const double &theta) {
  // First, check if the tile group is in this table
  if (tile_group_offset >= tile_groups_.GetSize()) {
    LOG_ERROR("Tile group offset not found in table : %u ", tile_group_offset);
    return nullptr;
  }

  auto tile_group_id =
      tile_groups_.FindValid(tile_group_offset, invalid_tile_group_id);

  // Get orig tile group from catalog
  auto storage_tilegroup = storage::StorageManager::GetInstance();
  auto tile_group = storage_tilegroup->GetTileGroup(tile_group_id);
  auto diff = tile_group->GetLayout().GetLayoutDifference(*default_layout_);

  // Check threshold for transformation
  if (diff < theta) {
    return nullptr;
  }

  LOG_TRACE("Transforming tile group : %u", tile_group_offset);

  // Get the schema for the new transformed tile group
  auto new_schema =
      TransformTileGroupSchema(tile_group.get(), *default_layout_);

  // Allocate space for the transformed tile group
  std::shared_ptr<storage::TileGroup> new_tile_group(
      TileGroupFactory::GetTileGroup(
          tile_group->GetDatabaseId(), tile_group->GetTableId(),
          tile_group->GetTileGroupId(), tile_group->GetAbstractTable(),
          new_schema, default_layout_, tile_group->GetAllocatedTupleCount()));

  // Set the transformed tile group column-at-a-time
  SetTransformedTileGroup(tile_group.get(), new_tile_group.get());

  // Set the location of the new tile group
  // and clean up the orig tile group
  storage_tilegroup->AddTileGroup(tile_group_id, new_tile_group);

  return new_tile_group.get();
}

/*void DataTable::RecordLayoutSample(const tuning::Sample &sample) {
  // Add layout sample
  {
    std::lock_guard<std::mutex> lock(layout_samples_mutex_);
    layout_samples_.push_back(sample);
  }
}*/

/*std::vector<tuning::Sample> DataTable::GetLayoutSamples() {
  {
    std::lock_guard<std::mutex> lock(layout_samples_mutex_);
    return layout_samples_;
  }
}*/

/*void DataTable::ClearLayoutSamples() {
  // Clear layout samples list
  {
    std::lock_guard<std::mutex> lock(layout_samples_mutex_);
    layout_samples_.clear();
  }
}*/

/*void DataTable::RecordIndexSample(const tuning::Sample &sample) {
  // Add index sample
  {
    std::lock_guard<std::mutex> lock(index_samples_mutex_);
    index_samples_.push_back(sample);
  }
}*/

/*std::vector<tuning::Sample> DataTable::GetIndexSamples() {
  {
    std::lock_guard<std::mutex> lock(index_samples_mutex_);
    return index_samples_;
  }
}*/

/*void DataTable::ClearIndexSamples() {
  // Clear index samples list
  {
    std::lock_guard<std::mutex> lock(index_samples_mutex_);
    index_samples_.clear();
  }
}*/

/*void DataTable::AddTrigger(trigger::Trigger new_trigger) {
  trigger_list_->AddTrigger(new_trigger);
}

int DataTable::GetTriggerNumber() {
  return trigger_list_->GetTriggerListSize();
}

trigger::Trigger *DataTable::GetTriggerByIndex(int n) {
  if (trigger_list_->GetTriggerListSize() <= n) return nullptr;
  return trigger_list_->Get(n);
}

trigger::TriggerList *DataTable::GetTriggerList() {
  if (trigger_list_->GetTriggerListSize() <= 0) return nullptr;
  return trigger_list_.get();
}

void DataTable::UpdateTriggerListFromCatalog(
    concurrency::TransactionContext *txn) {
  trigger_list_ = catalog::Catalog::GetInstance()
      ->GetSystemCatalogs(database_oid)
      ->GetTriggerCatalog()
      ->GetTriggers(txn, table_oid);
}*/

hash_t DataTable::Hash() const {
  auto oid = GetOid();
  hash_t hash = HashUtil::Hash(&oid);
  hash = HashUtil::CombineHashes(
      hash, HashUtil::HashBytes(GetName().c_str(), GetName().length()));
  auto db_oid = GetOid();
  hash = HashUtil::CombineHashes(hash, HashUtil::Hash(&db_oid));
  return hash;
}

bool DataTable::Equals(const storage::DataTable &other) const {
  return (*this == other);
}

bool DataTable::operator==(const DataTable &rhs) const {
  if (GetName() != rhs.GetName())
    return false;
  if (GetDatabaseOid() != rhs.GetDatabaseOid())
    return false;
  if (GetOid() != rhs.GetOid())
    return false;
  return true;
}

bool DataTable::SetCurrentLayoutOid(oid_t new_layout_oid) {
  oid_t old_oid = current_layout_oid_;
  while (old_oid <= new_layout_oid) {
    if (current_layout_oid_.compare_exchange_strong(old_oid, new_layout_oid)) {
      return true;
    }
    old_oid = current_layout_oid_;
  }
  return false;
}

} // namespace storage
} // namespace peloton
