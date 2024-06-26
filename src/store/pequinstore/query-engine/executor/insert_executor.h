//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// insert_executor.h
//
// Identification: src/include/executor/insert_executor.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "../executor/abstract_executor.h"
#include "../concurrency/transaction_manager_factory.h"

namespace peloton {
namespace executor {

/**
 * 2018-01-07: This is <b>deprecated</b>. Do not modify these classes.
 * The old interpreted engine will be removed.
 * @deprecated
 */
class InsertExecutor : public AbstractExecutor {
 public:
  InsertExecutor(const InsertExecutor &) = delete;
  InsertExecutor &operator=(const InsertExecutor &) = delete;
  InsertExecutor(InsertExecutor &&) = delete;
  InsertExecutor &operator=(InsertExecutor &&) = delete;

  explicit InsertExecutor(const planner::AbstractPlan *node,
                          ExecutorContext *executor_context);

 protected:
  bool DInit();

  bool DExecute();

  bool InsertFirstVersion(concurrency::TransactionManager &transaction_manager, concurrency::TransactionContext *current_txn, ItemPointer &location, ItemPointer *index_entry_ptr);
  bool InsertNewVersion(const planner::ProjectInfo *project_info, storage::DataTable *target_table, concurrency::TransactionManager &transaction_manager, concurrency::TransactionContext *current_txn, 
                        ItemPointer &location, ItemPointer &old_location, ItemPointer *index_entry_ptr);

 private:
  bool done_ = false;
};

}  // namespace executor
}  // namespace peloton
