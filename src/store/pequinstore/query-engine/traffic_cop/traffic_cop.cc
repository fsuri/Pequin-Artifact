//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// traffic_cop.cpp
//
// Identification: src/traffic_cop/traffic_cop.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "../traffic_cop/traffic_cop.h"

#include <utility>

#include "../../store/common/timestamp.h"
#include "../binder/bind_node_visitor.h"
#include "../common/internal_types.h"
#include "../concurrency/transaction_context.h"
#include "../concurrency/transaction_manager_factory.h"
#include "../expression/expression_util.h"
#include "../optimizer/optimizer.h"
#include "../planner/plan_util.h"
#include "../settings/settings_manager.h"
#include "../threadpool/mono_queue_pool.h"

namespace peloton {
namespace tcop {

TrafficCop::TrafficCop()
    : is_queuing_(false), rows_affected_(0),
      optimizer_(new optimizer::Optimizer(optimizer::CostModels::TRIVIAL)),
      single_statement_txn_(true) {}

TrafficCop::TrafficCop(void (*task_callback)(void *), void *task_callback_arg)
    : optimizer_(new optimizer::Optimizer(optimizer::CostModels::TRIVIAL)),
      single_statement_txn_(true), task_callback_(task_callback),
      task_callback_arg_(task_callback_arg) {}

void TrafficCop::Reset() {
  //std::cerr << "reset tcop" << std::endl;
  std::stack<TcopTxnState> new_tcop_txn_state;
  // clear out the stack
  swap(tcop_txn_state_, new_tcop_txn_state);
  optimizer_->Reset();
  results_.clear();
  param_values_.clear();
  setRowsAffected(0);
}

TrafficCop::~TrafficCop() {
  // Abort all running transactions
  Warning("Destroying Traffic Cop. Aborting all Txn");
  while (!tcop_txn_state_.empty()) {
    AbortQueryHelper();
  }
}

/* Singleton accessor
 * NOTE: Used by in unit tests ONLY
 */
TrafficCop &TrafficCop::GetInstance() {
  static TrafficCop tcop;
  tcop.Reset();
  return tcop;
}

TrafficCop::TcopTxnState &TrafficCop::GetDefaultTxnState() {
  static TcopTxnState default_state;
  default_state = std::make_pair(nullptr, ResultType::INVALID);
  return default_state;
}

TrafficCop::TcopTxnState &TrafficCop::GetCurrentTxnState() {
  if (tcop_txn_state_.empty()) {
    return GetDefaultTxnState();
  }
  return tcop_txn_state_.top();
}

ResultType TrafficCop::BeginQueryHelper(size_t thread_id) {
  if (tcop_txn_state_.empty()) {
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction(thread_id);
    // this shouldn't happen
    if (txn == nullptr) {
      LOG_DEBUG("Begin txn failed");
      return ResultType::FAILURE;
    }
    // initialize the current result as success
    tcop_txn_state_.emplace(txn, ResultType::SUCCESS);
  }
  return ResultType::SUCCESS;
}

ResultType TrafficCop::CommitQueryHelper() {


  // do nothing if we have no active txns
  if (tcop_txn_state_.empty())
    return ResultType::NOOP;
  auto &curr_state = tcop_txn_state_.top();
  tcop_txn_state_.pop();
  auto txn = curr_state.first;
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  // I catch the exception (ex. table not found) explicitly,
  // If this exception is caused by a query in a transaction, 
  // I will block following queries in that transaction until 'COMMIT' or  'ROLLBACK' After receive 'COMMIT', see if it is rollback or really commit.
  if (curr_state.second != ResultType::ABORTED) {
    // txn committed
    return txn_manager.CommitTransaction(txn);
  } else {
    // otherwise, rollback
    Panic("Abort should never happen when using Pequinstore Peloton interface");
    return txn_manager.AbortTransaction(txn);
  }
}

ResultType TrafficCop::AbortQueryHelper() {
  // do nothing if we have no active txns
  if (tcop_txn_state_.empty())
    return ResultType::NOOP;
  auto &curr_state = tcop_txn_state_.top();
  tcop_txn_state_.pop();
  // explicitly abort the txn only if it has not aborted already
  if (curr_state.second != ResultType::ABORTED) {
    auto txn = curr_state.first;
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto result = txn_manager.AbortTransaction(txn);
    return result;
  } else {
    delete curr_state.first;
    // otherwise, the txn has already been aborted
    return ResultType::ABORTED;
  }
}

ResultType TrafficCop::ExecuteStatementGetResult() {
  LOG_TRACE("Statement executed. Result: %s",
            ResultTypeToString(p_status_.m_result).c_str());
  setRowsAffected(p_status_.m_processed);
  LOG_TRACE("rows_changed %d", p_status_.m_processed);
  is_queuing_ = false;
  return p_status_.m_result;
}

/*
 * Execute a statement that needs a plan(so, BEGIN, COMMIT, ROLLBACK does not
 * come here).
 * Begin a new transaction if necessary.
 * If the current transaction is already broken(for example due to previous
 * invalid
 * queries), directly return
 * Otherwise, call ExecutePlan()
 */
executor::ExecutionResult TrafficCop::ExecuteHelper(
    std::shared_ptr<planner::AbstractPlan> plan,
    const std::vector<type::Value> &params, std::vector<ResultValue> &result,
    const std::vector<int> &result_format, size_t thread_id) {
  auto &curr_state = GetCurrentTxnState();

  concurrency::TransactionContext *txn;
  if (!tcop_txn_state_.empty()) {
    txn = curr_state.first;
  } else {
    // No active txn, single-statement txn
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    // new txn, reset result status
    curr_state.second = ResultType::SUCCESS;
    single_statement_txn_ = true;
    txn = txn_manager.BeginTransaction(thread_id);
    tcop_txn_state_.emplace(txn, ResultType::SUCCESS);
  }

  // skip if already aborted
  if (curr_state.second == ResultType::ABORTED) {
    // If the transaction state is ABORTED, the transaction should be aborted
    // but Peloton didn't explicitly abort it yet since it didn't receive a
    // COMMIT/ROLLBACK.
    // Here, it receive queries other than COMMIT/ROLLBACK in an broken
    // transaction,
    // it should tell the client that these queries will not be executed.
    p_status_.m_result = ResultType::TO_ABORT;
    return p_status_;
  }

  auto on_complete = [&result, this](executor::ExecutionResult p_status, std::vector<ResultValue> &&values) {
    // std::cerr << "Made it to on complete execute helper" << std::endl;
    this->p_status_ = p_status;
    // std::cerr << "The status is " << p_status.m_error_message << std::endl;
    //  TODO (Tianyi) I would make a decision on keeping one of p_status or
    //  error_message in my next PR
    this->error_message_ = std::move(p_status.m_error_message);
    result = std::move(values);
    task_callback_(task_callback_arg_);
    // std::cerr << "After task callback execute helper" << std::endl;
    Debug("Completed Task Callback Execute helper");
  };

  //std::cerr << "Setting skip cache in execute helper" << std::endl;
  //txn->skip_cache = true; //For Table Loading skip cache..
  
  // auto &pool = threadpool::MonoQueuePool::GetInstance();
  // pool.SubmitTask([plan, txn, &params, &result_format, on_complete] {
  //   executor::PlanExecutor::ExecutePlan(plan, txn, params, result_format, on_complete);
  // });
  executor::PlanExecutor::ExecutePlan(plan, txn, params, result_format, on_complete);

  is_queuing_ = true;

  LOG_TRACE("Check Tcop_txn_state Size After ExecuteHelper %lu",tcop_txn_state_.size());
  return p_status_;
}


/*
 * Execute a statement that needs a plan (so, BEGIN, COMMIT, ROLLBACK does not come here).
 * Begin a new transaction if necessary.
 * If the current transaction is already broken(for example due to previous invalid queries), directly return
 * Otherwise, call ExecutePlan()
 */
executor::ExecutionResult TrafficCop::ExecuteReadHelper(
    std::shared_ptr<planner::AbstractPlan> plan, const std::vector<type::Value> &params, std::vector<ResultValue> &result, const std::vector<int> &result_format, 
    //////////////////////// PEQUIN ARGS ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    const pequinstore::SQLTransformer *sql_interpreter,
    //const pequinstore::TableRegistry_t *table_reg,
    const Timestamp &basil_timestamp,
    pequinstore::find_table_version *find_table_version,
    pequinstore::read_prepared_pred *read_prepared_pred,

    PequinMode mode, //mode = 1: eagerRead, mode = 2: readMaterialized, mode = 3: eagerPlusSnapshot, mode = 4: findSnapshot
    pequinstore::QueryReadSetMgr *query_read_set_mgr, 
    pequinstore::SnapshotManager *snapshot_mgr,
    size_t k_prepared_versions,
    const pequinstore::snapshot *ss_txns,
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    size_t thread_id,
    bool is_limit) {

  Debug("ExecuteReadHelper with mode: %d", mode);

  auto &curr_state = GetCurrentTxnState();

  concurrency::TransactionContext *txn;
  if (!tcop_txn_state_.empty()) {
    // std::cerr << "Read helper use existing txn" << std::endl;
    txn = curr_state.first;
  } else {
    // std::cerr << "Read helper create txn" << std::endl;
    //  No active txn, single-statement txn
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    // new txn, reset result status
    curr_state.second = ResultType::SUCCESS;
    single_statement_txn_ = true;
    txn = txn_manager.BeginTransaction(thread_id);
    tcop_txn_state_.emplace(txn, ResultType::SUCCESS);
  }

  /////////////////////// SET PEQUIN TXN ARGS //////////////////////////////////////

  //txn->is_limit = is_limit;

  txn->SetReadOnly(); //THIS IS A READ QUERY

  txn->SetSqlInterpreter(sql_interpreter);
  // Set TableRegistry pointer
  //txn->SetTableRegistry(table_reg);

  // Set the Basil timestamp
  txn->SetBasilTimestamp(basil_timestamp);

  // Set the function to check if a table is prepared
  txn->SetReadPreparedPred(read_prepared_pred);

  // Set the function to find the table version
  txn->SetTableVersion(find_table_version);
  
  //Set mode specific arguments
  if(mode <= 3){
    // Set the readset manager
    txn->SetQueryReadSetMgr(query_read_set_mgr);
    txn->SetHasReadSetMgr(true);
    if(mode == 2){
      txn->SetSnapshotSet(ss_txns);
      txn->SetSnapshotRead(true);
    }
  }
  if(mode >= 3){
    txn->SetSnapshotMgr(snapshot_mgr);
    txn->SetHasSnapshotMgr(true);
    // Set the value of k to read
    txn->SetKPreparedVersions(k_prepared_versions);
  }
  
  // Not undoing deletes
  txn->SetUndoDelete(false);
  txn->SetIsPointRead(false);

  ////////////////////////////////////////////////////////////////////////////////////

  // skip if already aborted
  if (curr_state.second == ResultType::ABORTED) {
    // If the transaction state is ABORTED, the transaction should be aborted but Peloton didn't explicitly abort it yet since it didn't receive a COMMIT/ROLLBACK.
    // Here, it receive queries other than COMMIT/ROLLBACK in an broken transaction, it should tell the client that these queries will not be executed.
    p_status_.m_result = ResultType::TO_ABORT;
    return p_status_;
  }

  auto on_complete = [&result, this](executor::ExecutionResult p_status, std::vector<ResultValue> &&values) {
    this->p_status_ = p_status;
    //  TODO (Tianyi) I would make a decision on keeping one of p_status or error_message in my next PR
    this->error_message_ = std::move(p_status.m_error_message);
    result = std::move(values);
    Debug("Calling task callback");
    task_callback_(task_callback_arg_);
  };

 
  Debug("submit read query with TS [%lu:%lu]", basil_timestamp.getTimestamp(), basil_timestamp.getID());
 
  executor::PlanExecutor::ExecutePlan(plan, txn, params, result_format, on_complete);
  // auto &pool = threadpool::MonoQueuePool::GetInstance();
  // pool.SubmitTask([plan, txn, &params, &result_format, on_complete] {
  //   executor::PlanExecutor::ExecutePlan(plan, txn, params, result_format, on_complete);
  // });

  is_queuing_ = true;

  LOG_TRACE("Check Tcop_txn_state Size After ExecuteHelper %lu", tcop_txn_state_.size());
  return p_status_;
}



/*
 * Execute a statement that needs a plan (so, BEGIN, COMMIT, ROLLBACK does not come here).
 * Begin a new transaction if necessary.
 * If the current transaction is already broken(for example due to previous invalid queries), directly return
 * Otherwise, call ExecutePlan()
 */
executor::ExecutionResult TrafficCop::ExecuteReadHelper(
    std::shared_ptr<planner::AbstractPlan> plan,
    const std::vector<type::Value> &params, std::vector<ResultValue> &result,
    const std::vector<int> &result_format, const Timestamp &basil_timestamp,
    pequinstore::QueryReadSetMgr &query_read_set_mgr,
    pequinstore::find_table_version &find_table_version,
    pequinstore::read_prepared_pred &read_prepared_pred,
    size_t thread_id) {
  auto &curr_state = GetCurrentTxnState();

  concurrency::TransactionContext *txn;
  if (!tcop_txn_state_.empty()) {
    // std::cerr << "Read helper use existing txn" << std::endl;
    txn = curr_state.first;
  } else {
    // std::cerr << "Read helper create txn" << std::endl;
    //  No active txn, single-statement txn
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    // new txn, reset result status
    curr_state.second = ResultType::SUCCESS;
    single_statement_txn_ = true;
    txn = txn_manager.BeginTransaction(thread_id);
    tcop_txn_state_.emplace(txn, ResultType::SUCCESS);
  }

  // Set the Basil timestamp
  txn->SetBasilTimestamp(basil_timestamp);
  // Set the readset manager
  txn->SetQueryReadSetMgr(&query_read_set_mgr);
  txn->SetHasReadSetMgr(true);
  // Set the function to check if a table is prepared
  txn->SetReadPreparedPred(&read_prepared_pred);
  // Set the function to find the table version
  txn->SetTableVersion(&find_table_version);
  // Not undoing deletes
  txn->SetUndoDelete(false);
  txn->SetIsPointRead(false);

  // skip if already aborted
  if (curr_state.second == ResultType::ABORTED) {
    // If the transaction state is ABORTED, the transaction should be aborted
    // but Peloton didn't explicitly abort it yet since it didn't receive a
    // COMMIT/ROLLBACK.
    // Here, it receive queries other than COMMIT/ROLLBACK in an broken
    // transaction,
    // it should tell the client that these queries will not be executed.
    p_status_.m_result = ResultType::TO_ABORT;
    return p_status_;
  }

  auto on_complete = [&result, this](executor::ExecutionResult p_status,
                                     std::vector<ResultValue> &&values) {
    // std::cerr << "Made it to on complete" << std::endl;
    this->p_status_ = p_status;
    // std::cerr << "The status is " << p_status.m_error_message << std::endl;
    //  TODO (Tianyi) I would make a decision on keeping one of p_status or
    //  error_message in my next PR
    this->error_message_ = std::move(p_status.m_error_message);
    result = std::move(values);
    Debug("Calling task callback");
    task_callback_(task_callback_arg_);
  };


  Debug("submit read query with TS [%lu:%lu]", basil_timestamp.getTimestamp(), basil_timestamp.getID());

  // auto &pool = threadpool::MonoQueuePool::GetInstance();
  // pool.SubmitTask([plan, txn, &params, &result_format, on_complete] {
  //   executor::PlanExecutor::ExecutePlan(plan, txn, params, result_format, on_complete);
  // });
  executor::PlanExecutor::ExecutePlan(plan, txn, params, result_format, on_complete);

  is_queuing_ = true;

  LOG_TRACE("Check Tcop_txn_state Size After ExecuteHelper %lu",
            tcop_txn_state_.size());
  return p_status_;
}

/*
 * Execute a statement that needs a plan(so, BEGIN, COMMIT, ROLLBACK does not
 * come here).
 * Begin a new transaction if necessary.
 * If the current transaction is already broken(for example due to previous
 * invalid
 * queries), directly return
 * Otherwise, call ExecutePlan()
 */
executor::ExecutionResult TrafficCop::ExecuteSnapshotReadHelper(
    std::shared_ptr<planner::AbstractPlan> plan,
    const std::vector<type::Value> &params, std::vector<ResultValue> &result,
    const std::vector<int> &result_format, const Timestamp &basil_timestamp,
    pequinstore::QueryReadSetMgr &query_read_set_mgr,
    const ::google::protobuf::Map<std::string, pequinstore::proto::ReplicaList> *ss_txns,
    pequinstore::find_table_version &find_table_version,
    pequinstore::read_prepared_pred &read_prepared_pred,
    size_t thread_id) {
  auto &curr_state = GetCurrentTxnState();

  concurrency::TransactionContext *txn;
  if (!tcop_txn_state_.empty()) {
    // std::cerr << "Read helper use existing txn" << std::endl;
    txn = curr_state.first;
  } else {
    // std::cerr << "Read helper create txn" << std::endl;
    //  No active txn, single-statement txn
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    // new txn, reset result status
    curr_state.second = ResultType::SUCCESS;
    single_statement_txn_ = true;
    txn = txn_manager.BeginTransaction(thread_id);
    tcop_txn_state_.emplace(txn, ResultType::SUCCESS);
  }

  // Set the Basil timestamp
  txn->SetBasilTimestamp(basil_timestamp);
  // Set the readset manager
  txn->SetQueryReadSetMgr(&query_read_set_mgr);
  txn->SetHasReadSetMgr(true);
  txn->SetSnapshotRead(true);
  txn->SetSnapshotSet(ss_txns);
  txn->SetHasSnapshotMgr(false);
  // Set the function to check if a table is prepared
  txn->SetReadPreparedPred(&read_prepared_pred);
  // Set the function to find the table version
  txn->SetTableVersion(&find_table_version);
  // Not undoing deletes
  txn->SetUndoDelete(false);
  txn->SetIsPointRead(false);

  // skip if already aborted
  if (curr_state.second == ResultType::ABORTED) {
    // If the transaction state is ABORTED, the transaction should be aborted
    // but Peloton didn't explicitly abort it yet since it didn't receive a
    // COMMIT/ROLLBACK.
    // Here, it receive queries other than COMMIT/ROLLBACK in an broken
    // transaction,
    // it should tell the client that these queries will not be executed.
    p_status_.m_result = ResultType::TO_ABORT;
    return p_status_;
  }

  auto on_complete = [&result, this](executor::ExecutionResult p_status, std::vector<ResultValue> &&values) {
    // std::cerr << "Made it to on complete" << std::endl;
    this->p_status_ = p_status;
    // std::cerr << "The status is " << p_status.m_error_message << std::endl;
    //  TODO (Tianyi) I would make a decision on keeping one of p_status or
    //  error_message in my next PR
    this->error_message_ = std::move(p_status.m_error_message);
    result = std::move(values);
    Debug("Calling task callback");
    task_callback_(task_callback_arg_);
  };

  Debug("submit read query with TS [%lu:%lu]", basil_timestamp.getTimestamp(), basil_timestamp.getID());
  // auto &pool = threadpool::MonoQueuePool::GetInstance();
  // pool.SubmitTask([plan, txn, &params, &result_format, on_complete] {
  //   executor::PlanExecutor::ExecutePlan(plan, txn, params, result_format, on_complete);
  // });
  executor::PlanExecutor::ExecutePlan(plan, txn, params, result_format, on_complete);

  is_queuing_ = true;

  LOG_TRACE("Check Tcop_txn_state Size After ExecuteHelper %lu",
            tcop_txn_state_.size());
  return p_status_;
}



/*
 * Execute a statement that needs a plan(so, BEGIN, COMMIT, ROLLBACK does not
 * come here).
 * Begin a new transaction if necessary.
 * If the current transaction is already broken(for example due to previous
 * invalid
 * queries), directly return
 * Otherwise, call ExecutePlan()
 */
executor::ExecutionResult TrafficCop::ExecuteFindSnapshotHelper(
    std::shared_ptr<planner::AbstractPlan> plan,
    const std::vector<type::Value> &params, std::vector<ResultValue> &result,
    const std::vector<int> &result_format, const Timestamp &basil_timestamp,
    pequinstore::SnapshotManager *snapshot_mgr,
    size_t k_prepared_versions,
    pequinstore::find_table_version &find_table_version,
    pequinstore::read_prepared_pred &read_prepared_pred,
    size_t thread_id) {
  auto &curr_state = GetCurrentTxnState();

  concurrency::TransactionContext *txn;
  if (!tcop_txn_state_.empty()) {
    // std::cerr << "Read helper use existing txn" << std::endl;
    txn = curr_state.first;
  } else {
    // std::cerr << "Read helper create txn" << std::endl;
    //  No active txn, single-statement txn
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    // new txn, reset result status
    curr_state.second = ResultType::SUCCESS;
    single_statement_txn_ = true;
    txn = txn_manager.BeginTransaction(thread_id);
    tcop_txn_state_.emplace(txn, ResultType::SUCCESS);
  }

  // Set the Basil timestamp
  txn->SetBasilTimestamp(basil_timestamp);
  // Set the snapshot manager
  txn->SetSnapshotMgr(snapshot_mgr);
  txn->SetHasSnapshotMgr(true);
  txn->SetHasReadSetMgr(false);
  // Set the value of k to read
  txn->SetKPreparedVersions(k_prepared_versions);
  // Set the function to check if a table is prepared
  txn->SetReadPreparedPred(&read_prepared_pred);
  // Set the function to find the table version
  txn->SetTableVersion(&find_table_version);
  // Not undoing deletes
  txn->SetUndoDelete(false);
  txn->SetIsPointRead(false);

  // skip if already aborted
  if (curr_state.second == ResultType::ABORTED) {
    // If the transaction state is ABORTED, the transaction should be aborted
    // but Peloton didn't explicitly abort it yet since it didn't receive a
    // COMMIT/ROLLBACK.
    // Here, it receive queries other than COMMIT/ROLLBACK in an broken
    // transaction,
    // it should tell the client that these queries will not be executed.
    p_status_.m_result = ResultType::TO_ABORT;
    return p_status_;
  }

  auto on_complete = [&result, this](executor::ExecutionResult p_status,
                                     std::vector<ResultValue> &&values) {
    // std::cerr << "Made it to on complete" << std::endl;
    this->p_status_ = p_status;
    // std::cerr << "The status is " << p_status.m_error_message << std::endl;
    //  TODO (Tianyi) I would make a decision on keeping one of p_status or
    //  error_message in my next PR
    this->error_message_ = std::move(p_status.m_error_message);
    result = std::move(values);
    Debug("Calling task callback");
    task_callback_(task_callback_arg_);
  };

  

  Debug("submit read query with TS [%lu:%lu]", basil_timestamp.getTimestamp(), basil_timestamp.getID());
  auto &pool = threadpool::MonoQueuePool::GetInstance();
  // pool.SubmitTask([plan, txn, &params, &result_format, on_complete] {
  //   executor::PlanExecutor::ExecutePlan(plan, txn, params, result_format, on_complete);
  // });
  executor::PlanExecutor::ExecutePlan(plan, txn, params, result_format, on_complete);

  is_queuing_ = true;

  LOG_TRACE("Check Tcop_txn_state Size After ExecuteHelper %lu",
            tcop_txn_state_.size());
  return p_status_;
}

executor::ExecutionResult TrafficCop::ExecuteEagerExecAndSnapshotHelper(
    std::shared_ptr<planner::AbstractPlan> plan,
    const std::vector<type::Value> &params, std::vector<ResultValue> &result,
    const std::vector<int> &result_format, const Timestamp &basil_timestamp,
    pequinstore::QueryReadSetMgr &query_read_set_mgr,
    pequinstore::SnapshotManager *snapshot_mgr,
    size_t k_prepared_versions,
    pequinstore::find_table_version &find_table_version,
    pequinstore::read_prepared_pred &read_prepared_pred,
    size_t thread_id) {
  auto &curr_state = GetCurrentTxnState();

  concurrency::TransactionContext *txn;
  if (!tcop_txn_state_.empty()) {
    // std::cerr << "Read helper use existing txn" << std::endl;
    txn = curr_state.first;
  } else {
    // std::cerr << "Read helper create txn" << std::endl;
    //  No active txn, single-statement txn
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    // new txn, reset result status
    curr_state.second = ResultType::SUCCESS;
    single_statement_txn_ = true;
    txn = txn_manager.BeginTransaction(thread_id);
    tcop_txn_state_.emplace(txn, ResultType::SUCCESS);
  }

  // Set the Basil timestamp
  txn->SetBasilTimestamp(basil_timestamp);
  // Set the snapshot manager
  txn->SetSnapshotMgr(snapshot_mgr);
  txn->SetHasSnapshotMgr(true);
  // Set the readset manager
  txn->SetQueryReadSetMgr(&query_read_set_mgr);
  txn->SetHasReadSetMgr(true);
  // Set the value of k to read
  txn->SetKPreparedVersions(k_prepared_versions);
  // Set the function to check if a table is prepared
  txn->SetReadPreparedPred(&read_prepared_pred);
  // Set the function to find the table version
  txn->SetTableVersion(&find_table_version);
  // Not undoing deletes
  txn->SetUndoDelete(false);
  txn->SetIsPointRead(false);

  // skip if already aborted
  if (curr_state.second == ResultType::ABORTED) {
    // If the transaction state is ABORTED, the transaction should be aborted
    // but Peloton didn't explicitly abort it yet since it didn't receive a
    // COMMIT/ROLLBACK.
    // Here, it receive queries other than COMMIT/ROLLBACK in an broken
    // transaction,
    // it should tell the client that these queries will not be executed.
    p_status_.m_result = ResultType::TO_ABORT;
    return p_status_;
  }

  auto on_complete = [&result, this](executor::ExecutionResult p_status,
                                     std::vector<ResultValue> &&values) {
    // std::cerr << "Made it to on complete" << std::endl;
    this->p_status_ = p_status;
    //std::cerr << "The status is " << p_status.m_error_message << std::endl;
    //  TODO (Tianyi) I would make a decision on keeping one of p_status or
    //  error_message in my next PR
    this->error_message_ = std::move(p_status.m_error_message);
    result = std::move(values);
    Debug("Calling task callback");
    task_callback_(task_callback_arg_);
  };

 

  Debug("submit read query with TS [%lu:%lu]", basil_timestamp.getTimestamp(), basil_timestamp.getID());
  // auto &pool = threadpool::MonoQueuePool::GetInstance();
  // pool.SubmitTask([plan, txn, &params, &result_format, on_complete] {
  //   executor::PlanExecutor::ExecutePlan(plan, txn, params, result_format, on_complete);
  // });
  executor::PlanExecutor::ExecutePlan(plan, txn, params, result_format, on_complete);

  is_queuing_ = true;

  LOG_TRACE("Check Tcop_txn_state Size After ExecuteHelper %lu",
            tcop_txn_state_.size());
  return p_status_;
}




/*
 * Execute a statement that needs a plan(so, BEGIN, COMMIT, ROLLBACK does not
 * come here).
 * Begin a new transaction if necessary.
 * If the current transaction is already broken(for example due to previous
 * invalid
 * queries), directly return
 * Otherwise, call ExecutePlan()
 */
executor::ExecutionResult TrafficCop::ExecuteWriteHelper(
    std::shared_ptr<planner::AbstractPlan> plan,
    const std::vector<type::Value> &params, std::vector<ResultValue> &result,
    const std::vector<int> &result_format, const Timestamp &basil_timestamp,
    std::shared_ptr<std::string> txn_dig,
    const pequinstore::proto::CommittedProof *commit_proof,
    bool commit_or_prepare, bool forceMaterialize, bool is_delete, size_t thread_id) {
  //auto &curr_state = GetCurrentTxnState();

  concurrency::TransactionContext *txn;
  // if (!tcop_txn_state_.empty()) {
  //   // std::cerr << "Write helper use of existing txn" << std::endl;
  //   Panic("Write helper uses existing txn");
  //   txn = curr_state.first;
  // } else {
  //   // std::cerr << "Write helper create new txn" << std::endl;
  //   //  No active txn, single-statement txn
  //   auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  //   // new txn, reset result status
  //   curr_state.second = ResultType::SUCCESS;
  //   single_statement_txn_ = true;
  //   txn = txn_manager.BeginTransaction(thread_id);
  //   tcop_txn_state_.emplace(txn, ResultType::SUCCESS);
  // }

   auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

   auto &curr_state = GetDefaultTxnState();
    // new txn, reset result status
    curr_state.second = ResultType::SUCCESS;
    single_statement_txn_ = true;
    txn = txn_manager.BeginTransaction(thread_id);

  // Set the Basil timestamp
  txn->SetBasilTimestamp(basil_timestamp);
  // Set the txn_digeset
  txn->SetTxnDig(txn_dig);
  Debug("Txn digest is %s", pequinstore::BytesToHex(*txn_dig, 16).c_str());
  // Set the commit proof
  txn->SetCommittedProof(commit_proof);
  // txn->SetCommittedProofRef(commit_proof);
  //   Set commit or prepare
  txn->SetCommitOrPrepare(commit_or_prepare);

  txn->SetDeletion(is_delete);
  // Set undo delete false
  txn->SetUndoDelete(false);
  // Set has read set mgr to false
  txn->SetHasReadSetMgr(false);
  txn->SetIsPointRead(false);
  txn->SetHasSnapshotMgr(false);
  // Set whether to force materialize
  txn->SetForceMaterialize(forceMaterialize);

  Debug("IN WRITE HELPER: Txn %s is trying to %s", pequinstore::BytesToHex(*txn_dig, 16).c_str(), commit_or_prepare? "commit" : "prepare");
  // Notice("Commit or prepare is %d", txn->GetCommitOrPrepare());
  // Notice("Commit or prepare is %d", commit_or_prepare);

  // std::cerr << "Commit Or Prepare:" << commit_or_prepare << std::endl;
  // std::cerr << "Txn: Commit Or Prepare:" << txn->GetCommitOrPrepare() << std::endl;
  // skip if already aborted
  if (curr_state.second == ResultType::ABORTED) {
    // If the transaction state is ABORTED, the transaction should be aborted
    // but Peloton didn't explicitly abort it yet since it didn't receive a
    // COMMIT/ROLLBACK.
    // Here, it receive queries other than COMMIT/ROLLBACK in an broken
    // transaction,
    // it should tell the client that these queries will not be executed.
    p_status_.m_result = ResultType::TO_ABORT;
    return p_status_;
  }

  auto on_complete = [&result, this](executor::ExecutionResult p_status, std::vector<ResultValue> &&values) {
    // std::cerr << "Made it to on complete" << std::endl;
    this->p_status_ = p_status;
    //std::cerr << "The status is " << p_status.m_error_message << std::endl;
    //  TODO (Tianyi) I would make a decision on keeping one of p_status or
    //  error_message in my next PR
    this->error_message_ = std::move(p_status.m_error_message);
    if(!this->error_message_.empty()) Panic("got error: %s", this->error_message_.c_str());
    result = std::move(values);
    Debug("Calling Task callback");
    task_callback_(task_callback_arg_);
  };

  executor::PlanExecutor::ExecutePlan(plan, txn, params, result_format,on_complete);
  // auto &pool = threadpool::MonoQueuePool::GetInstance();
  // pool.SubmitTask([plan, txn, &params, &result_format, on_complete] {
  //   executor::PlanExecutor::ExecutePlan(plan, txn, params, result_format,on_complete);
  // });

  is_queuing_ = true;

  LOG_TRACE("Check Tcop_txn_state Size After ExecuteHelper %lu", tcop_txn_state_.size());
  return p_status_;
}

/*
 * Execute a statement that needs a plan(so, BEGIN, COMMIT, ROLLBACK does not
 * come here).
 * Begin a new transaction if necessary.
 * If the current transaction is already broken(for example due to previous
 * invalid
 * queries), directly return
 * Otherwise, call ExecutePlan()
 */
executor::ExecutionResult TrafficCop::ExecutePurgeHelper(
    std::shared_ptr<planner::AbstractPlan> plan,
    const std::vector<type::Value> &params, std::vector<ResultValue> &result,
    const std::vector<int> &result_format, const Timestamp &basil_timestamp,
    std::shared_ptr<std::string> txn_dig, bool undo_delete, size_t thread_id) {
  //auto &curr_state = GetCurrentTxnState();

  concurrency::TransactionContext *txn;
  // if (!tcop_txn_state_.empty()) {
  //   txn = curr_state.first;
  // } else {
  //   // No active txn, single-statement txn
  //   auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  //   // new txn, reset result status
  //   curr_state.second = ResultType::SUCCESS;
  //   single_statement_txn_ = true;
  //   txn = txn_manager.BeginTransaction(thread_id);
  //   tcop_txn_state_.emplace(txn, ResultType::SUCCESS);
  // }

   auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

   auto &curr_state = GetDefaultTxnState();
    // new txn, reset result status
    curr_state.second = ResultType::SUCCESS;
    single_statement_txn_ = true;
    txn = txn_manager.BeginTransaction(thread_id);

  // Set the Basil timestamp
  txn->SetBasilTimestamp(basil_timestamp);
  // Set the txn_digeset
  txn->SetTxnDig(txn_dig);
  // Set undo delete
  txn->SetUndoDelete(undo_delete);

  Debug("Purge helper: undo_delete %d, txn->GetUndoDelete() %d", undo_delete,
  txn->GetUndoDelete());
  // std::cerr << "Undo delete in execute purge helper is " << undo_delete <<
  // std::endl; std::cerr << "Txn get undo delete in execute purge helper is "
  // << txn->GetUndoDelete() << std::endl; //No read set manager for purge
  txn->SetHasReadSetMgr(false);
  txn->SetIsPointRead(false);
  txn->SetHasSnapshotMgr(false);
  txn->SetCommitOrPrepare(false);

  // skip if already aborted
  if (curr_state.second == ResultType::ABORTED) {
    // If the transaction state is ABORTED, the transaction should be aborted
    // but Peloton didn't explicitly abort it yet since it didn't receive a
    // COMMIT/ROLLBACK.
    // Here, it receive queries other than COMMIT/ROLLBACK in an broken
    // transaction,
    // it should tell the client that these queries will not be executed.
    p_status_.m_result = ResultType::TO_ABORT;
    return p_status_;
  }

  auto on_complete = [&result, this](executor::ExecutionResult p_status,
                                     std::vector<ResultValue> &&values) {
    // std::cerr << "Made it to on complete" << std::endl;
    this->p_status_ = p_status;
    // std::cerr << "The status is " << p_status.m_error_message << std::endl;
    //  TODO (Tianyi) I would make a decision on keeping one of p_status or
    //  error_message in my next PR
    this->error_message_ = std::move(p_status.m_error_message);
    result = std::move(values);
    Debug("Calling task callback");
    task_callback_(task_callback_arg_);
  };

  // auto &pool = threadpool::MonoQueuePool::GetInstance();
  // pool.SubmitTask([plan, txn, &params, &result_format, on_complete] {
  //   executor::PlanExecutor::ExecutePlan(plan, txn, params, result_format, on_complete);
  // });
  executor::PlanExecutor::ExecutePlan(plan, txn, params, result_format, on_complete);

  is_queuing_ = true;

  LOG_TRACE("Check Tcop_txn_state Size After ExecuteHelper %lu", tcop_txn_state_.size());
  return p_status_;
}

/* void TrafficCop::ExecuteStatementPlanGetResult() {
  if (p_status_.m_result == ResultType::FAILURE) return;

  auto txn_result = GetCurrentTxnState().first->GetResult();
  if (single_statement_txn_ || txn_result == ResultType::FAILURE) {
    LOG_TRACE("About to commit/abort: single stmt: %d,txn_result: %s",
              single_statement_txn_, ResultTypeToString(txn_result).c_str());
    switch (txn_result) {
      case ResultType::SUCCESS:
        // Commit single statement
        LOG_TRACE("Commit Transaction");
        p_status_.m_result = CommitQueryHelper();
        break;

      case ResultType::FAILURE:
      default:
        // Abort
        LOG_TRACE("Abort Transaction");
        if (single_statement_txn_) {
          LOG_TRACE("Tcop_txn_state size: %lu", tcop_txn_state_.size());
          p_status_.m_result = AbortQueryHelper();
        } else {
          tcop_txn_state_.top().second = ResultType::ABORTED;
          p_status_.m_result = ResultType::ABORTED;
        }
    }
  }
} */

/*
 * Execute a statement that needs a plan(so, BEGIN, COMMIT, ROLLBACK does not
 * come here).
 * Begin a new transaction if necessary.
 * If the current transaction is already broken(for example due to previous
 * invalid
 * queries), directly return
 * Otherwise, call ExecutePlan()
 */
executor::ExecutionResult TrafficCop::ExecutePointReadHelper(
    std::shared_ptr<planner::AbstractPlan> plan,
    const std::vector<type::Value> &params, std::vector<ResultValue> &result,
    const std::vector<int> &result_format, const Timestamp &basil_timestamp,
    std::function<bool(const std::string &)> &predicate,
    Timestamp *committed_timestamp,
    const pequinstore::proto::CommittedProof **commit_proof,
    Timestamp *prepared_timestamp, std::shared_ptr<std::string> *txn_dig,
    pequinstore::proto::Write *write, size_t thread_id, bool is_customer_read) {
  auto &curr_state = GetCurrentTxnState();

  concurrency::TransactionContext *txn;
  if (!tcop_txn_state_.empty()) {
    txn = curr_state.first;
  } else {
    // No active txn, single-statement txn
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    // new txn, reset result status
    curr_state.second = ResultType::SUCCESS;
    single_statement_txn_ = true;
    txn = txn_manager.BeginTransaction(thread_id);
    tcop_txn_state_.emplace(txn, ResultType::SUCCESS);
  }

  txn->is_customer_read = is_customer_read;

  txn->SetReadOnly(); //THIS IS A READ QUERY

  // Set the Basil timestamp
  txn->SetBasilTimestamp(basil_timestamp);
  // Set the predicate
  txn->SetReadPreparedPred(&predicate);
  // Set the txn_digeset
  txn->SetCommitTimestamp(committed_timestamp);
  // auto time = txn->GetCommitTimestamp();
  // time.setTimestamp(1024);

  // Set the commit proof pointer
  txn->SetCommittedProofRef(commit_proof);
  // commit_proof = txn->GetCommittedProof();
  //  Set the prepared timestamp
  txn->SetPreparedTimestamp(prepared_timestamp);
  // Set the prepared txn_dig
  txn->SetPreparedTxnDigest(txn_dig);

  // Set the value references.
  txn->SetCommittedValue(write->mutable_committed_value());
  txn->SetPreparedValue(write->mutable_prepared_value());
  //WARNING: Accessing mutable turns "has_committed/prepared_value" to true, even if they are empty!! Clear them if empty!.
  


  // Not undoing deletes
  txn->SetUndoDelete(false);
  // No read set manager
  txn->SetHasReadSetMgr(false);
  // No snapshot manager
  txn->SetHasSnapshotMgr(false);

  // Is a point read query
  txn->SetIsPointRead(true);


   Debug("PointRead for: Basil Timestamp to [%lu:%lu]. IsPoint: %d", txn->GetBasilTimestamp().getTimestamp(), txn->GetBasilTimestamp().getID(), txn->IsPointRead());

  // skip if already aborted
  if (curr_state.second == ResultType::ABORTED) {
    Panic("I don't think a point read should ever abort");
    // If the transaction state is ABORTED, the transaction should be aborted but Peloton didn't explicitly abort it yet since it didn't receive a COMMIT/ROLLBACK.
    // Here, it receive queries other than COMMIT/ROLLBACK in an broken transaction, it should tell the client that these queries will not be executed.
    p_status_.m_result = ResultType::TO_ABORT;
    return p_status_;
  }


  auto dummy = [](){};
 

  auto on_complete = [&result, this](executor::ExecutionResult p_status,
                                     std::vector<ResultValue> &&values) {
    // std::cerr << "Made it to on complete" << std::endl;
    this->p_status_ = p_status;
    // std::cerr << "The status is for point read " << p_status.m_error_message
    // << std::endl;
    //  TODO (Tianyi) I would make a decision on keeping one of p_status or
    //  error_message in my next PR
    this->error_message_ = std::move(p_status.m_error_message);
    result = std::move(values);
    task_callback_(task_callback_arg_);
    Debug("calling task callback");
    // std::cerr << "End of on complete" << std::endl;
  };

  executor::PlanExecutor::ExecutePlan(plan, txn, params, result_format, on_complete);
  //TODO: On complete/ ContinueAfterComplete in GetResult is not necessary?

  // auto &pool = threadpool::MonoQueuePool::GetInstance();
  // pool.SubmitTask([plan, txn, &params, &result_format, on_complete] {
  //   executor::PlanExecutor::ExecutePlan(plan, txn, params, result_format, on_complete);
  // });

  is_queuing_ = true;

  LOG_TRACE("Check Tcop_txn_state Size After ExecuteHelper %lu",
            tcop_txn_state_.size());
  return p_status_;
}

void TrafficCop::ExecuteStatementPlanGetResult() {
  if (p_status_.m_result == ResultType::FAILURE)
    return;

  auto txn_result = GetCurrentTxnState().first->GetResult();
  if (single_statement_txn_ || txn_result == ResultType::FAILURE) {
    LOG_TRACE("About to commit/abort: single stmt: %d,txn_result: %s",
              single_statement_txn_, ResultTypeToString(txn_result).c_str());
    switch (txn_result) {
    case ResultType::SUCCESS:
      // Commit single statement
      LOG_TRACE("Commit Transaction");
      p_status_.m_result = CommitQueryHelper();
      break;

    case ResultType::FAILURE:
    default:
      // Abort
      LOG_TRACE("Abort Transaction");
      if (single_statement_txn_) {
        LOG_TRACE("Tcop_txn_state size: %lu", tcop_txn_state_.size());
        p_status_.m_result = AbortQueryHelper();
      } else {
        tcop_txn_state_.top().second = ResultType::ABORTED;
        p_status_.m_result = ResultType::ABORTED;
      }
    }
  }
}

/*
 * Prepare a statement based on parse tree. Begin a transaction if necessary.
 * If the query is not issued in a transaction (if txn_stack is empty and it's not BEGIN query), Peloton will create a new transation for it. single_stmt transaction.
 * Otherwise, it's a multi_stmt transaction.
 * TODO(Yuchen): We do not need a query string to prepare a statement and the query string may contain the information of multiple statements rather than the single one.
 * Hack here. We store the query string inside Statement objects for printing infomation.
 */
std::shared_ptr<Statement> TrafficCop::PrepareStatement(
    const std::string &stmt_name, const std::string &query_string,
    std::unique_ptr<parser::SQLStatementList> sql_stmt_list, bool skip_cache,
    const size_t thread_id UNUSED_ATTRIBUTE) {
  LOG_TRACE("Prepare Statement query: %s", query_string.c_str());

  // Empty statement
  // TODO (Tianyi) Read through the parser code to see if this is appropriate
  if (sql_stmt_list.get() == nullptr || sql_stmt_list->GetNumStatements() == 0) {
    // TODO (Tianyi) Do we need another query type called QUERY_EMPTY?
    std::shared_ptr<Statement> statement = std::make_shared<Statement>(stmt_name, QueryType::QUERY_INVALID, query_string, std::move(sql_stmt_list));
    return statement;
  }

  StatementType stmt_type = sql_stmt_list->GetStatement(0)->GetType();
  QueryType query_type = StatementTypeToQueryType(stmt_type, sql_stmt_list->GetStatement(0));

  std::shared_ptr<Statement> statement = std::make_shared<Statement>(stmt_name, query_type, query_string, std::move(sql_stmt_list));

  // We can learn transaction's states, BEGIN, COMMIT, ABORT, or ROLLBACK from
  // member variables, tcop_txn_state_. We can also get single-statement txn or
  // multi-statement txn from member variable single_statement_txn_
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  // --multi-statements except BEGIN in a transaction
  if (!tcop_txn_state_.empty()) {
    single_statement_txn_ = false;
    // multi-statment txn has been aborted, just skip this query,
    // and do not need to parse or execute this query anymore.
    // Do not return nullptr in case that 'COMMIT' cannot be execute,
    // because nullptr will directly return ResultType::FAILURE to
    // packet_manager
    if (tcop_txn_state_.top().second == ResultType::ABORTED) {
      return statement;
    }
  } else {
    // Begin new transaction when received single-statement query or "BEGIN"
    // from multi-statement query
    if (statement->GetQueryType() == QueryType::QUERY_BEGIN) { // only begin a new transaction
      // note this transaction is not single-statement transaction
      LOG_TRACE("BEGIN");
      single_statement_txn_ = false;
      Debug("Begin statement)");
    } else {
      // single statement
      LOG_TRACE("SINGLE TXN");
      single_statement_txn_ = true;
      Debug("Single statement TXN. All queries should be this (since we don't use TX semantics inside Peloton)");
    }
    auto txn = txn_manager.BeginTransaction(thread_id);

    // this shouldn't happen
    if (txn == nullptr) {
      LOG_TRACE("Begin txn failed");
    }
    if(skip_cache) txn->skip_cache = true;
    
    // initialize the current result as success
    tcop_txn_state_.emplace(txn, ResultType::SUCCESS);
  }

  if (settings::SettingsManager::GetBool(settings::SettingId::brain)) {
    tcop_txn_state_.top().first->AddQueryString(query_string.c_str());
  }


  // TODO(Tianyi) Move Statement Planing into Statement's method to increase coherence
  try {
    // Run binder 
    auto bind_node_visitor = binder::BindNodeVisitor(tcop_txn_state_.top().first, default_database_name_);  //FIXME: TODO: FS: This seems to be expensive. Can we change this?
    bind_node_visitor.BindNameToNode(statement->GetStmtParseTreeList()->GetStatement(0));
    //Notice("finished binding; try to optimize next");
    auto plan = optimizer_->BuildPelotonPlanTree(statement->GetStmtParseTreeList(), tcop_txn_state_.top().first);  //FIXME: TODO: FS: This seems to be expensive. Can we change this?

    // Notice("Finished Optimizer; setting plane tree etc next");
    statement->SetPlanTree(plan);
    // Get the tables that our plan references so that we know how to
    // invalidate it at a later point when the catalog changes
    const std::set<oid_t> table_oids = planner::PlanUtil::GetTablesReferenced(plan.get());
    statement->SetReferencedTables(table_oids);

    if (query_type == QueryType::QUERY_SELECT) {
      auto tuple_descriptor = GenerateTupleDescriptor(statement->GetStmtParseTreeList()->GetStatement(0));

      statement->SetTupleDescriptor(tuple_descriptor);
      LOG_TRACE("select query, finish setting");
    }
  } catch (Exception &e) {
    Panic("Fail PrepareStatement");
    error_message_ = e.what();
    ProcessInvalidStatement();
    return nullptr;
  }

  // std::cerr << "QueryType Select? " << QueryTypeToString(query_type)  <<
  // std::endl; std::cerr << "Prepare Statement: " <<
  // statement->GetPlanTree().get()->GetInfo() << std::endl;

#ifdef LOG_DEBUG_ENABLED
  if (statement->GetPlanTree().get() != nullptr) {
    LOG_TRACE("Statement Prepared: %s", statement->GetInfo().c_str());
    LOG_TRACE("%s", statement->GetPlanTree().get()->GetInfo().c_str());
  }
#endif
  return statement;
}

/*
 * Do nothing if there is no active transaction;
 * If single-stmt transaction, abort it;
 * If multi-stmt transaction, just set transaction state to 'ABORTED'.
 * The multi-stmt txn will be explicitly aborted when receiving 'Commit' or
 * 'Rollback'.
 */
void TrafficCop::ProcessInvalidStatement() {
  if (single_statement_txn_) {
    LOG_TRACE("SINGLE ABORT!");
    AbortQueryHelper();
  } else { // multi-statment txn
    if (tcop_txn_state_.top().second != ResultType::ABORTED) {
      tcop_txn_state_.top().second = ResultType::ABORTED;
    }
  }
}

bool TrafficCop::BindParamsForCachePlan(
    const std::vector<std::unique_ptr<expression::AbstractExpression>>
        &parameters,
    const size_t thread_id UNUSED_ATTRIBUTE) {
  if (tcop_txn_state_.empty()) {
    single_statement_txn_ = true;
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction(thread_id);
    // this shouldn't happen
    if (txn == nullptr) {
      LOG_ERROR("Begin txn failed");
    }
    // initialize the current result as success
    tcop_txn_state_.emplace(txn, ResultType::SUCCESS);
  }
  // Run binder
  auto bind_node_visitor = binder::BindNodeVisitor(tcop_txn_state_.top().first,
                                                   default_database_name_);

  std::vector<type::Value> param_values;
  for (const std::unique_ptr<expression::AbstractExpression> &param :
       parameters) {
    if (!expression::ExpressionUtil::IsValidStaticExpression(param.get())) {
      error_message_ = "Invalid Expression Type";
      return false;
    }
    param->Accept(&bind_node_visitor);
    // TODO(Yuchen): need better check for nullptr argument
    param_values.push_back(param->Evaluate(nullptr, nullptr, nullptr));
  }
  if (param_values.size() > 0) {
    statement_->GetPlanTree()->SetParameterValues(&param_values);
  }
  SetParamVal(param_values);
  return true;
}

void TrafficCop::GetTableColumns(parser::TableRef *from_table,
                                 std::vector<catalog::Column> &target_columns) {
  if (from_table == nullptr)
    return;

  // Query derived table
  if (from_table->select != NULL) {
    for (auto &expr : from_table->select->select_list) {
      if (expr->GetExpressionType() == ExpressionType::STAR)
        GetTableColumns(from_table->select->from_table.get(), target_columns);
      else
        target_columns.push_back(catalog::Column(expr->GetValueType(), 0,
                                                 expr->GetExpressionName()));
    }
  } else if (from_table->list.empty()) {
    if (from_table->join == NULL) {
      auto columns =
          static_cast<storage::DataTable *>(
              catalog::Catalog::GetInstance()->GetTableWithName(
                  GetCurrentTxnState().first, from_table->GetDatabaseName(),
                  from_table->GetSchemaName(), from_table->GetTableName()))
              ->GetSchema()
              ->GetColumns();
      target_columns.insert(target_columns.end(), columns.begin(),
                            columns.end());
    } else {
      GetTableColumns(from_table->join->left.get(), target_columns);
      GetTableColumns(from_table->join->right.get(), target_columns);
    }
  }
  // Query has multiple tables. Recursively add all tables
  else {
    for (auto &table : from_table->list) {
      GetTableColumns(table.get(), target_columns);
    }
  }
}

std::vector<FieldInfo>
TrafficCop::GenerateTupleDescriptor(parser::SQLStatement *sql_stmt) {
  std::vector<FieldInfo> tuple_descriptor;
  if (sql_stmt->GetType() != StatementType::SELECT)
    return tuple_descriptor;
  auto select_stmt = (parser::SelectStatement *)sql_stmt;

  // TODO: this is a hack which I don't have time to fix now
  // but it replaces a worse hack that was here before
  // What should happen here is that plan nodes should store
  // the schema of their expected results and here we should just read
  // it and put it in the tuple descriptor

  // Get the columns information and set up
  // the columns description for the returned results
  // Set up the table
  std::vector<catalog::Column> all_columns;

  // Check if query only has one Table
  // Example : SELECT * FROM A;
  GetTableColumns(select_stmt->from_table.get(), all_columns);

  int count = 0;
  for (auto &expr : select_stmt->select_list) {
    count++;
    if (expr->GetExpressionType() == ExpressionType::STAR) {
      for (auto column : all_columns) {
        tuple_descriptor.push_back(
            GetColumnFieldForValueType(column.GetName(), column.GetType()));
      }
    } else {
      std::string col_name;
      if (expr->alias.empty()) {
        col_name = expr->expr_name_.empty()
                       ? std::string("expr") + std::to_string(count)
                       : expr->expr_name_;
      } else {
        col_name = expr->alias;
      }
      tuple_descriptor.push_back(
          GetColumnFieldForValueType(col_name, expr->GetValueType()));
    }
  }

  return tuple_descriptor;
}

// TODO: move it to postgres_protocal_handler.cpp
FieldInfo TrafficCop::GetColumnFieldForValueType(std::string column_name,
                                                 type::TypeId column_type) {
  PostgresValueType field_type;
  size_t field_size;
  switch (column_type) {
  case type::TypeId::BOOLEAN:
  case type::TypeId::TINYINT: {
    field_type = PostgresValueType::BOOLEAN;
    field_size = 1;
    break;
  }
  case type::TypeId::SMALLINT: {
    field_type = PostgresValueType::SMALLINT;
    field_size = 2;
    break;
  }
  case type::TypeId::INTEGER: {
    field_type = PostgresValueType::INTEGER;
    field_size = 4;
    break;
  }
  case type::TypeId::BIGINT: {
    field_type = PostgresValueType::BIGINT;
    field_size = 8;
    break;
  }
  case type::TypeId::DECIMAL: {
    field_type = PostgresValueType::DOUBLE;
    field_size = 8;
    break;
  }
  case type::TypeId::VARCHAR:
  case type::TypeId::VARBINARY: {
    field_type = PostgresValueType::TEXT;
    field_size = 255;
    break;
  }
  case type::TypeId::DATE: {
    field_type = PostgresValueType::DATE;
    field_size = 4;
    break;
  }
  case type::TypeId::TIMESTAMP: {
    field_type = PostgresValueType::TIMESTAMPS;
    field_size = 64; // FIXME: Bytes???
    break;
  }
  default: {
    // Type not Identified
    LOG_ERROR("Unrecognized field type '%s' for field '%s'",
              TypeIdToString(column_type).c_str(), column_name.c_str());
    field_type = PostgresValueType::TEXT;
    field_size = 255;
    break;
  }
  }
  // HACK: Convert the type into a oid_t
  // This ugly and I don't like it one bit...
  return std::make_tuple(column_name, static_cast<oid_t>(field_type),
                         field_size);
}

ResultType TrafficCop::ExecuteStatement(
    const std::shared_ptr<Statement> &statement,
    const std::vector<type::Value> &params, UNUSED_ATTRIBUTE bool unnamed,
    /*std::shared_ptr<stats::QueryMetric::QueryParams> param_stats,*/
    const std::vector<int> &result_format, std::vector<ResultValue> &result,
    size_t thread_id) {
  // TODO(Tianyi) Further simplify this API
  /*if (static_cast<StatsType>(settings::SettingsManager::GetInt(
          settings::SettingId::stats_mode)) != StatsType::INVALID) {
    stats::BackendStatsContext::GetInstance()->InitQueryMetric(
        statement, std::move(param_stats));
  }*/

  LOG_TRACE("Execute Statement of name: %s",
            statement->GetStatementName().c_str());
  LOG_TRACE("Execute Statement of query: %s",
            statement->GetQueryString().c_str());
  LOG_TRACE("Execute Statement Plan:\n%s",
            planner::PlanUtil::GetInfo(statement->GetPlanTree().get()).c_str());
  LOG_TRACE("Execute Statement Query Type: %s",
            statement->GetQueryTypeString().c_str());
  LOG_TRACE("----QueryType: %d--------",
            static_cast<int>(statement->GetQueryType()));

  try {
    switch (statement->GetQueryType()) {
    case QueryType::QUERY_BEGIN: {
      return BeginQueryHelper(thread_id);
    }
    case QueryType::QUERY_COMMIT: {
      return CommitQueryHelper();
    }
    case QueryType::QUERY_ROLLBACK: {
      return AbortQueryHelper();
    }
    default:
      // The statement may be out of date
      // It needs to be replan
      if (statement->GetNeedsReplan()) {
        // TODO(Tianyi) Move Statement Replan into Statement's method
        // to increase coherence
        auto bind_node_visitor = binder::BindNodeVisitor(
            tcop_txn_state_.top().first, default_database_name_);
        bind_node_visitor.BindNameToNode(
            statement->GetStmtParseTreeList()->GetStatement(0));
        auto plan = optimizer_->BuildPelotonPlanTree(
            statement->GetStmtParseTreeList(), tcop_txn_state_.top().first);
        statement->SetPlanTree(plan);
        statement->SetNeedsReplan(true);
      }

      ExecuteHelper(statement->GetPlanTree(), params, result, result_format, thread_id);
      if (GetQueuing()) {
        return ResultType::QUEUING;
      } else {
        return ExecuteStatementGetResult();
      }
    }

  } catch (Exception &e) {
     Panic("Fail ExecuteStatement");
    error_message_ = e.what();
    return ResultType::FAILURE;
  }
}

ResultType TrafficCop::ExecuteReadStatement(
    const std::shared_ptr<Statement> &statement,
    const std::vector<type::Value> &params, UNUSED_ATTRIBUTE bool unnamed,
    const std::vector<int> &result_format, std::vector<ResultValue> &result,
    //////////////////////// PEQUIN ARGS //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    const pequinstore::SQLTransformer *sql_interpreter,
    //const pequinstore::TableRegistry_t *table_reg,
    const Timestamp &basil_timestamp,
    pequinstore::find_table_version *find_table_version,
    pequinstore::read_prepared_pred *read_prepared_pred,

    PequinMode mode, //mode = 1: eagerRead, mode = 2: readMaterialized, mode = 3: eagerPlusSnapshot, mode = 4: findSnapshot
    pequinstore::QueryReadSetMgr *query_read_set_mgr, //TODO: change to ptr
    pequinstore::SnapshotManager *snapshot_mgr,
    size_t k_prepared_versions,
    const pequinstore::snapshot *ss_txns,
     ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    size_t thread_id)   
{
  UW_ASSERT(1 <= mode <= 4); //Assert we are using a valid Pequin mode

  Debug("ExecuteReadStatement with mode: %d", mode);

  LOG_TRACE("Execute Statement of name: %s", statement->GetStatementName().c_str());
  LOG_TRACE("Execute Statement of query: %s", statement->GetQueryString().c_str());
  LOG_TRACE("Execute Statement Plan:\n%s", planner::PlanUtil::GetInfo(statement->GetPlanTree().get()).c_str());
  LOG_TRACE("Execute Statement Query Type: %s", statement->GetQueryTypeString().c_str());
  LOG_TRACE("----QueryType: %d--------", static_cast<int>(statement->GetQueryType()));

  // std::cerr << "Exec Read: " << statement->GetPlanTree().get()->GetInfo() << std::endl; 
  //std::cerr << "Plan Node type: " << statement->GetPlanTree()->GetPlanNodeType() << std::endl;

  try {
    switch (statement->GetQueryType()) {
    case QueryType::QUERY_BEGIN: {
      return BeginQueryHelper(thread_id);
    }
    case QueryType::QUERY_COMMIT: {
      return CommitQueryHelper();
    }
    case QueryType::QUERY_ROLLBACK: {
      return AbortQueryHelper();
    }
    default:
      // The statement may be out of date
      // It needs to be replan
      if (statement->GetNeedsReplan()) {
        // TODO(Tianyi) Move Statement Replan into Statement's method to increase coherence
        auto bind_node_visitor = binder::BindNodeVisitor(tcop_txn_state_.top().first, default_database_name_);
        bind_node_visitor.BindNameToNode(statement->GetStmtParseTreeList()->GetStatement(0));
        auto plan = optimizer_->BuildPelotonPlanTree(statement->GetStmtParseTreeList(), tcop_txn_state_.top().first);
        statement->SetPlanTree(plan);
        statement->SetNeedsReplan(true);
      }


      //bool is_limit = statement->GetQueryString().find("LIMIT") != std::string::npos;
      

      ExecuteReadHelper(statement->GetPlanTree(), params, result, result_format, sql_interpreter, //table_reg,
                        basil_timestamp, find_table_version, read_prepared_pred, 
                        mode,
                        query_read_set_mgr,
                        snapshot_mgr,
                        k_prepared_versions,
                        ss_txns,
                        thread_id);
      if (GetQueuing()) {
        return ResultType::QUEUING;
      } else {
        return ExecuteStatementGetResult();
      }
    }

  } catch (Exception &e) {
     Panic("Fail ExecReadStatement");
    error_message_ = e.what();
    return ResultType::FAILURE;
  }
}

ResultType TrafficCop::ExecuteReadStatement(
    const std::shared_ptr<Statement> &statement,
    const std::vector<type::Value> &params, UNUSED_ATTRIBUTE bool unnamed,
    /*std::shared_ptr<stats::QueryMetric::QueryParams> param_stats,*/
    const std::vector<int> &result_format, std::vector<ResultValue> &result,
    const Timestamp &basil_timestamp,
    pequinstore::QueryReadSetMgr &query_read_set_mgr,
    pequinstore::find_table_version &find_table_version,
    pequinstore::read_prepared_pred &read_prepared_pred,
    size_t thread_id) {
  // TODO(Tianyi) Further simplify this API
  /*if (static_cast<StatsType>(settings::SettingsManager::GetInt(
          settings::SettingId::stats_mode)) != StatsType::INVALID) {
    stats::BackendStatsContext::GetInstance()->InitQueryMetric(
        statement, std::move(param_stats));
  }*/

  LOG_TRACE("Execute Statement of name: %s",
            statement->GetStatementName().c_str());
  LOG_TRACE("Execute Statement of query: %s",
            statement->GetQueryString().c_str());
  LOG_TRACE("Execute Statement Plan:\n%s",
            planner::PlanUtil::GetInfo(statement->GetPlanTree().get()).c_str());
  LOG_TRACE("Execute Statement Query Type: %s",
            statement->GetQueryTypeString().c_str());
  LOG_TRACE("----QueryType: %d--------",
            static_cast<int>(statement->GetQueryType()));

  // std::cerr << "Exec Read: " << statement->GetPlanTree().get()->GetInfo() <<
  // std::endl; std::cerr << "Plan Node type: " <<
  // statement->GetPlanTree()->GetPlanNodeType() << std::endl;

  try {
    switch (statement->GetQueryType()) {
    case QueryType::QUERY_BEGIN: {
      return BeginQueryHelper(thread_id);
    }
    case QueryType::QUERY_COMMIT: {
      return CommitQueryHelper();
    }
    case QueryType::QUERY_ROLLBACK: {
      return AbortQueryHelper();
    }
    default:
      // The statement may be out of date
      // It needs to be replan
      if (statement->GetNeedsReplan()) {
        // TODO(Tianyi) Move Statement Replan into Statement's method
        // to increase coherence
        auto bind_node_visitor = binder::BindNodeVisitor(
            tcop_txn_state_.top().first, default_database_name_);
        bind_node_visitor.BindNameToNode(
            statement->GetStmtParseTreeList()->GetStatement(0));
        auto plan = optimizer_->BuildPelotonPlanTree(
            statement->GetStmtParseTreeList(), tcop_txn_state_.top().first);
        statement->SetPlanTree(plan);
        statement->SetNeedsReplan(true);
      }

      ExecuteReadHelper(statement->GetPlanTree(), params, result, result_format,
                        basil_timestamp, query_read_set_mgr, find_table_version,
                        read_prepared_pred, thread_id);
      if (GetQueuing()) {
        return ResultType::QUEUING;
      } else {
        return ExecuteStatementGetResult();
      }
    }

  } catch (Exception &e) {
     Panic("Fail ExecReadStatement");
    error_message_ = e.what();
    return ResultType::FAILURE;
  }
}

ResultType TrafficCop::ExecuteSnapshotReadStatement(
    const std::shared_ptr<Statement> &statement,
    const std::vector<type::Value> &params, UNUSED_ATTRIBUTE bool unnamed,
    /*std::shared_ptr<stats::QueryMetric::QueryParams> param_stats,*/
    const std::vector<int> &result_format, std::vector<ResultValue> &result,
    const Timestamp &basil_timestamp,
    pequinstore::QueryReadSetMgr &query_read_set_mgr,
    const ::google::protobuf::Map<std::string, pequinstore::proto::ReplicaList> *ss_txns,
    pequinstore::find_table_version &find_table_version,
    pequinstore::read_prepared_pred &read_prepared_pred,
    size_t thread_id) {
  // TODO(Tianyi) Further simplify this API
  /*if (static_cast<StatsType>(settings::SettingsManager::GetInt(
          settings::SettingId::stats_mode)) != StatsType::INVALID) {
    stats::BackendStatsContext::GetInstance()->InitQueryMetric(
        statement, std::move(param_stats));
  }*/

  LOG_TRACE("Execute Statement of name: %s",
            statement->GetStatementName().c_str());
  LOG_TRACE("Execute Statement of query: %s",
            statement->GetQueryString().c_str());
  LOG_TRACE("Execute Statement Plan:\n%s",
            planner::PlanUtil::GetInfo(statement->GetPlanTree().get()).c_str());
  LOG_TRACE("Execute Statement Query Type: %s",
            statement->GetQueryTypeString().c_str());
  LOG_TRACE("----QueryType: %d--------",
            static_cast<int>(statement->GetQueryType()));

  // std::cerr << "Exec Read: " << statement->GetPlanTree().get()->GetInfo() <<
  // std::endl; std::cerr << "Plan Node type: " <<
  // statement->GetPlanTree()->GetPlanNodeType() << std::endl;

  try {
    switch (statement->GetQueryType()) {
    case QueryType::QUERY_BEGIN: {
      return BeginQueryHelper(thread_id);
    }
    case QueryType::QUERY_COMMIT: {
      return CommitQueryHelper();
    }
    case QueryType::QUERY_ROLLBACK: {
      return AbortQueryHelper();
    }
    default:
      // The statement may be out of date
      // It needs to be replan
      if (statement->GetNeedsReplan()) {
        // TODO(Tianyi) Move Statement Replan into Statement's method
        // to increase coherence
        auto bind_node_visitor = binder::BindNodeVisitor(
            tcop_txn_state_.top().first, default_database_name_);
        bind_node_visitor.BindNameToNode(
            statement->GetStmtParseTreeList()->GetStatement(0));
        auto plan = optimizer_->BuildPelotonPlanTree(
            statement->GetStmtParseTreeList(), tcop_txn_state_.top().first);
        statement->SetPlanTree(plan);
        statement->SetNeedsReplan(true);
      }

      ExecuteSnapshotReadHelper(statement->GetPlanTree(), params, result, result_format,
                        basil_timestamp, query_read_set_mgr, ss_txns, find_table_version,
                        read_prepared_pred, thread_id);
      if (GetQueuing()) {
        return ResultType::QUEUING;
      } else {
        return ExecuteStatementGetResult();
      }
    }

  } catch (Exception &e) {
     Panic("Fail ExecSnapshotStatement");
    error_message_ = e.what();
    return ResultType::FAILURE;
  }
}



ResultType TrafficCop::ExecuteFindSnapshotStatement(
    const std::shared_ptr<Statement> &statement,
    const std::vector<type::Value> &params, UNUSED_ATTRIBUTE bool unnamed,
    /*std::shared_ptr<stats::QueryMetric::QueryParams> param_stats,*/
    const std::vector<int> &result_format, std::vector<ResultValue> &result,
    const Timestamp &basil_timestamp,
    pequinstore::SnapshotManager *snapshot_mgr,
    size_t k_prepared_versions,
    pequinstore::find_table_version &find_table_version,
    pequinstore::read_prepared_pred &read_prepared_pred,
    size_t thread_id) {
  // TODO(Tianyi) Further simplify this API
  /*if (static_cast<StatsType>(settings::SettingsManager::GetInt(
          settings::SettingId::stats_mode)) != StatsType::INVALID) {
    stats::BackendStatsContext::GetInstance()->InitQueryMetric(
        statement, std::move(param_stats));
  }*/

  LOG_TRACE("Execute Statement of name: %s",
            statement->GetStatementName().c_str());
  LOG_TRACE("Execute Statement of query: %s",
            statement->GetQueryString().c_str());
  LOG_TRACE("Execute Statement Plan:\n%s",
            planner::PlanUtil::GetInfo(statement->GetPlanTree().get()).c_str());
  LOG_TRACE("Execute Statement Query Type: %s",
            statement->GetQueryTypeString().c_str());
  LOG_TRACE("----QueryType: %d--------",
            static_cast<int>(statement->GetQueryType()));

  // std::cerr << "Exec Read: " << statement->GetPlanTree().get()->GetInfo() <<
  // std::endl; std::cerr << "Plan Node type: " <<
  // statement->GetPlanTree()->GetPlanNodeType() << std::endl;

  try {
    switch (statement->GetQueryType()) {
    case QueryType::QUERY_BEGIN: {
      return BeginQueryHelper(thread_id);
    }
    case QueryType::QUERY_COMMIT: {
      return CommitQueryHelper();
    }
    case QueryType::QUERY_ROLLBACK: {
      return AbortQueryHelper();
    }
    default:
      // The statement may be out of date
      // It needs to be replan
      if (statement->GetNeedsReplan()) {
        // TODO(Tianyi) Move Statement Replan into Statement's method
        // to increase coherence
        auto bind_node_visitor = binder::BindNodeVisitor(
            tcop_txn_state_.top().first, default_database_name_);
        bind_node_visitor.BindNameToNode(
            statement->GetStmtParseTreeList()->GetStatement(0));
        auto plan = optimizer_->BuildPelotonPlanTree(
            statement->GetStmtParseTreeList(), tcop_txn_state_.top().first);
        statement->SetPlanTree(plan);
        statement->SetNeedsReplan(true);
      }

      ExecuteFindSnapshotHelper(statement->GetPlanTree(), params, result, result_format,
                        basil_timestamp, snapshot_mgr, k_prepared_versions, find_table_version,
                        read_prepared_pred, thread_id);
      if (GetQueuing()) {
        return ResultType::QUEUING;
      } else {
        return ExecuteStatementGetResult();
      }
    }

  } catch (Exception &e) {
     Panic("Fail FindSnapshotStatement");
    error_message_ = e.what();
    return ResultType::FAILURE;
  }
}

ResultType TrafficCop::ExecuteEagerExecAndSnapshotStatement(
    const std::shared_ptr<Statement> &statement,
    const std::vector<type::Value> &params, UNUSED_ATTRIBUTE bool unnamed,
    /*std::shared_ptr<stats::QueryMetric::QueryParams> param_stats,*/
    const std::vector<int> &result_format, std::vector<ResultValue> &result,
    const Timestamp &basil_timestamp,
    pequinstore::QueryReadSetMgr &query_read_set_mgr,
    pequinstore::SnapshotManager *snapshot_mgr,
    size_t k_prepared_versions,
    pequinstore::find_table_version &find_table_version,
    pequinstore::read_prepared_pred &read_prepared_pred,
    size_t thread_id) {
  // TODO(Tianyi) Further simplify this API
  /*if (static_cast<StatsType>(settings::SettingsManager::GetInt(
          settings::SettingId::stats_mode)) != StatsType::INVALID) {
    stats::BackendStatsContext::GetInstance()->InitQueryMetric(
        statement, std::move(param_stats));
  }*/

  LOG_TRACE("Execute Statement of name: %s",
            statement->GetStatementName().c_str());
  LOG_TRACE("Execute Statement of query: %s",
            statement->GetQueryString().c_str());
  LOG_TRACE("Execute Statement Plan:\n%s",
            planner::PlanUtil::GetInfo(statement->GetPlanTree().get()).c_str());
  LOG_TRACE("Execute Statement Query Type: %s",
            statement->GetQueryTypeString().c_str());
  LOG_TRACE("----QueryType: %d--------",
            static_cast<int>(statement->GetQueryType()));

  // std::cerr << "Exec Read: " << statement->GetPlanTree().get()->GetInfo() <<
  // std::endl; std::cerr << "Plan Node type: " <<
  // statement->GetPlanTree()->GetPlanNodeType() << std::endl;

  try {
    switch (statement->GetQueryType()) {
    case QueryType::QUERY_BEGIN: {
      return BeginQueryHelper(thread_id);
    }
    case QueryType::QUERY_COMMIT: {
      return CommitQueryHelper();
    }
    case QueryType::QUERY_ROLLBACK: {
      return AbortQueryHelper();
    }
    default:
      // The statement may be out of date
      // It needs to be replan
      if (statement->GetNeedsReplan()) {
        // TODO(Tianyi) Move Statement Replan into Statement's method
        // to increase coherence
        auto bind_node_visitor = binder::BindNodeVisitor(
            tcop_txn_state_.top().first, default_database_name_);
        bind_node_visitor.BindNameToNode(
            statement->GetStmtParseTreeList()->GetStatement(0));
        auto plan = optimizer_->BuildPelotonPlanTree(
            statement->GetStmtParseTreeList(), tcop_txn_state_.top().first);
        statement->SetPlanTree(plan);
        statement->SetNeedsReplan(true);
      }

      ExecuteEagerExecAndSnapshotHelper(statement->GetPlanTree(), params, result, result_format,
                        basil_timestamp, query_read_set_mgr, snapshot_mgr, k_prepared_versions, find_table_version,
                        read_prepared_pred, thread_id);
      if (GetQueuing()) {
        return ResultType::QUEUING;
      } else {
        return ExecuteStatementGetResult();
      }
    }

  } catch (Exception &e) {
     Panic("Fail EagerExecStatement");
    error_message_ = e.what();
    return ResultType::FAILURE;
  }
}




ResultType TrafficCop::ExecutePurgeStatement(
    const std::shared_ptr<Statement> &statement,
    const std::vector<type::Value> &params, UNUSED_ATTRIBUTE bool unnamed,
    /*std::shared_ptr<stats::QueryMetric::QueryParams> param_stats,*/
    const std::vector<int> &result_format, std::vector<ResultValue> &result,
    const Timestamp &basil_timestamp, std::shared_ptr<std::string> txn_dig,
    bool undo_delete, size_t thread_id) {
  // TODO(Tianyi) Further simplify this API
  /*if (static_cast<StatsType>(settings::SettingsManager::GetInt(
          settings::SettingId::stats_mode)) != StatsType::INVALID) {
    stats::BackendStatsContext::GetInstance()->InitQueryMetric(
        statement, std::move(param_stats));
  }*/

  LOG_TRACE("Execute Statement of name: %s",
            statement->GetStatementName().c_str());
  LOG_TRACE("Execute Statement of query: %s",
            statement->GetQueryString().c_str());
  LOG_TRACE("Execute Statement Plan:\n%s",
            planner::PlanUtil::GetInfo(statement->GetPlanTree().get()).c_str());
  LOG_TRACE("Execute Statement Query Type: %s",
            statement->GetQueryTypeString().c_str());
  LOG_TRACE("----QueryType: %d--------",
            static_cast<int>(statement->GetQueryType()));

  try {
    switch (statement->GetQueryType()) {
    case QueryType::QUERY_BEGIN: {
      return BeginQueryHelper(thread_id);
    }
    case QueryType::QUERY_COMMIT: {
      return CommitQueryHelper();
    }
    case QueryType::QUERY_ROLLBACK: {
      return AbortQueryHelper();
    }
    default:
      // The statement may be out of date
      // It needs to be replan
      if (statement->GetNeedsReplan()) {
        // TODO(Tianyi) Move Statement Replan into Statement's method
        // to increase coherence
        auto bind_node_visitor = binder::BindNodeVisitor(
            tcop_txn_state_.top().first, default_database_name_);
        bind_node_visitor.BindNameToNode(
            statement->GetStmtParseTreeList()->GetStatement(0));
        auto plan = optimizer_->BuildPelotonPlanTree(
            statement->GetStmtParseTreeList(), tcop_txn_state_.top().first);
        statement->SetPlanTree(plan);
        statement->SetNeedsReplan(true);
      }
      Debug("Purge statement. Txn[%s] undo_delete: %d", pequinstore::BytesToHex(*txn_dig, 16).c_str(), undo_delete);
      // std::cerr << "Undo delete in execute purge statement is " <<
      // undo_delete << std::endl;
      ExecutePurgeHelper(statement->GetPlanTree(), params, result,
                         result_format, basil_timestamp, txn_dig, undo_delete,
                         thread_id);
      if (GetQueuing()) {
        return ResultType::QUEUING;
      } else {
        return ExecuteStatementGetResult();
      }
    }

  } catch (Exception &e) {
     Panic("Fail PurgeStatement");
    error_message_ = e.what();
    return ResultType::FAILURE;
  }
}

ResultType TrafficCop::ExecuteWriteStatement(
    const std::shared_ptr<Statement> &statement,
    const std::vector<type::Value> &params, UNUSED_ATTRIBUTE bool unnamed,
    /*std::shared_ptr<stats::QueryMetric::QueryParams> param_stats,*/
    const std::vector<int> &result_format, std::vector<ResultValue> &result,
    const Timestamp &basil_timestamp, std::shared_ptr<std::string> txn_dig,
    const pequinstore::proto::CommittedProof *commit_proof,
    bool commit_or_prepare, bool forceMaterialize, bool is_delete, size_t thread_id) {
  // TODO(Tianyi) Further simplify this API
  /*if (static_cast<StatsType>(settings::SettingsManager::GetInt(
          settings::SettingId::stats_mode)) != StatsType::INVALID) {
    stats::BackendStatsContext::GetInstance()->InitQueryMetric(
        statement, std::move(param_stats));
  }*/

  LOG_TRACE("Execute Statement of name: %s", statement->GetStatementName().c_str());
  LOG_TRACE("Execute Statement of query: %s", statement->GetQueryString().c_str());
  LOG_TRACE("Execute Statement Plan:\n%s", planner::PlanUtil::GetInfo(statement->GetPlanTree().get()).c_str());
  LOG_TRACE("Execute Statement Query Type: %s", statement->GetQueryTypeString().c_str());
  LOG_TRACE("----QueryType: %d--------", static_cast<int>(statement->GetQueryType()));

  try {
    switch (statement->GetQueryType()) {
    case QueryType::QUERY_BEGIN: {
      return BeginQueryHelper(thread_id);
    }
    case QueryType::QUERY_COMMIT: {
      return CommitQueryHelper();
    }
    case QueryType::QUERY_ROLLBACK: {
      return AbortQueryHelper();
    }
    default:
      // The statement may be out of date
      // It needs to be replan
      if (statement->GetNeedsReplan()) {
        Panic("Need replan");
        // TODO(Tianyi) Move Statement Replan into Statement's method
        // to increase coherence
        auto bind_node_visitor = binder::BindNodeVisitor(tcop_txn_state_.top().first, default_database_name_);
        bind_node_visitor.BindNameToNode(statement->GetStmtParseTreeList()->GetStatement(0));
        auto plan = optimizer_->BuildPelotonPlanTree(statement->GetStmtParseTreeList(), tcop_txn_state_.top().first);
        statement->SetPlanTree(plan);
        statement->SetNeedsReplan(true);
      }

      ExecuteWriteHelper(statement->GetPlanTree(), params, result,
                         result_format, basil_timestamp, txn_dig, commit_proof,
                         commit_or_prepare, forceMaterialize, is_delete, thread_id);
      if (GetQueuing()) {
        return ResultType::QUEUING;
      } else {
        return ExecuteStatementGetResult();
      }
    }

  } catch (Exception &e) {
     Panic("Fail WriteStatement");
    error_message_ = e.what();
    return ResultType::FAILURE;
  }
}

ResultType TrafficCop::ExecutePointReadStatement(
    const std::shared_ptr<Statement> &statement,
    const std::vector<type::Value> &params, UNUSED_ATTRIBUTE bool unnamed,
    /*std::shared_ptr<stats::QueryMetric::QueryParams> param_stats,*/
    const std::vector<int> &result_format, std::vector<ResultValue> &result,
    const Timestamp &basil_timestamp,
    std::function<bool(const std::string &)> &predicate,
    Timestamp *committed_timestamp,
    const pequinstore::proto::CommittedProof **commit_proof,
    Timestamp *prepared_timestamp, std::shared_ptr<std::string> *txn_dig,
    pequinstore::proto::Write *write, bool is_customer_read, size_t thread_id) {
  // TODO(Tianyi) Further simplify this API
  /*if (static_cast<StatsType>(settings::SettingsManager::GetInt(
          settings::SettingId::stats_mode)) != StatsType::INVALID) {
    stats::BackendStatsContext::GetInstance()->InitQueryMetric(
        statement, std::move(param_stats));
  }*/

  LOG_TRACE("Execute Statement of name: %s",
            statement->GetStatementName().c_str());
  LOG_TRACE("Execute Statement of query: %s",
            statement->GetQueryString().c_str());
  LOG_TRACE("Execute Statement Plan:\n%s",
            planner::PlanUtil::GetInfo(statement->GetPlanTree().get()).c_str());
  LOG_TRACE("Execute Statement Query Type: %s",
            statement->GetQueryTypeString().c_str());
  LOG_TRACE("----QueryType: %d--------",
            static_cast<int>(statement->GetQueryType()));

  try {
    switch (statement->GetQueryType()) {
    case QueryType::QUERY_BEGIN: {
      return BeginQueryHelper(thread_id);
    }
    case QueryType::QUERY_COMMIT: {
      return CommitQueryHelper();
    }
    case QueryType::QUERY_ROLLBACK: {
      return AbortQueryHelper();
    }
    default:
      // The statement may be out of date
      // It needs to be replan
      if (statement->GetNeedsReplan()) {
        // TODO(Tianyi) Move Statement Replan into Statement's method
        // to increase coherence
        auto bind_node_visitor = binder::BindNodeVisitor(
            tcop_txn_state_.top().first, default_database_name_);
        bind_node_visitor.BindNameToNode(
            statement->GetStmtParseTreeList()->GetStatement(0));
        auto plan = optimizer_->BuildPelotonPlanTree(
            statement->GetStmtParseTreeList(), tcop_txn_state_.top().first);
        statement->SetPlanTree(plan);
        statement->SetNeedsReplan(true);
      }

      ExecutePointReadHelper(statement->GetPlanTree(), params, result,
                             result_format, basil_timestamp, predicate,
                             committed_timestamp, commit_proof,
                             prepared_timestamp, txn_dig, write, thread_id, is_customer_read);
      if (GetQueuing()) {
        return ResultType::QUEUING;
      } else {
        return ExecuteStatementGetResult();
      }
    }

  } catch (Exception &e) {
     Panic("Fail PointReadStatement");
    error_message_ = e.what();
    return ResultType::FAILURE;
  }
}

} // namespace tcop
} // namespace peloton
