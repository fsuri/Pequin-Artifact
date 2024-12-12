//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_context.h
//
// Identification: src/include/concurrency/transaction_context.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "../../store/common/timestamp.h"
#include "../../store/sintrstore/common.h"
// #include "../../store/sintrstore/table_store_interface.h"
#include "../catalog/catalog_cache.h"
#include "../common/exception.h"
#include "../common/internal_types.h"
#include "../common/item_pointer.h"
#include "../common/printable.h"
#include "store/sintrstore/sintr-proto.pb.h"
#include "../../store/sintrstore/sql_interpreter.h"

namespace peloton {

/*namespace trigger {
class TriggerSet;
class TriggerData;
}  // namespace trigger*/

static sintrstore::read_prepared_pred default_read_prepared_pred = [](const std::string &s){Debug("Initializer call, do nothing"); return true;};
static sintrstore::find_table_version default_find_table_version = [](const std::string &s, const Timestamp &t, bool b, sintrstore::QueryReadSetMgr *q, bool b2, sintrstore::SnapshotManager *sm, bool b3, const sintrstore::snapshot *ss){Debug("Initializer call, do nothing");};


namespace concurrency {

//===--------------------------------------------------------------------===//
// TransactionContext
//===--------------------------------------------------------------------===//

/**
 * @brief      Class for transaction context.
 */
class TransactionContext : public Printable {
  TransactionContext(TransactionContext const &) = delete;

public:
  TransactionContext(const size_t thread_id, const IsolationLevelType isolation,
                     const cid_t &read_id);

  TransactionContext(const size_t thread_id, const IsolationLevelType isolation,
                     const cid_t &read_id, const cid_t &commit_id);

  // TransactionContext(const size_t thread_id, const IsolationLevelType isolation,
  //                    const cid_t &read_id, const cid_t &commit_id,
  //                    const sintrstore::QueryReadSetMgr *query_read_set_mgr);

  /**
   * @brief      Destroys the object.
   */
  ~TransactionContext() = default;

  bool is_customer_read = false;

private:
  void Init(const size_t thread_id, const IsolationLevelType isolation,
            const cid_t &read_id) {
    Init(thread_id, isolation, read_id, read_id);
  }

  void Init(const size_t thread_id, const IsolationLevelType isolation,
            const cid_t &read_id, const cid_t &commit_id);

public:
    bool is_limit = false;
  //===--------------------------------------------------------------------===//
  // Mutators and Accessors
  //===--------------------------------------------------------------------===//

  /**
   * @brief      Gets the thread identifier.
   *
   * @return     The thread identifier.
   */
  inline size_t GetThreadId() const { return thread_id_; }

  /**
   * @brief      Gets the transaction identifier.
   *
   * @return     The transaction identifier.
   */
  inline txn_id_t GetTransactionId() const { return txn_id_; }

  /**
   * @brief      Gets the read identifier.
   *
   * @return     The read identifier.
   */
  inline cid_t GetReadId() const { return read_id_; }

  /**
   * @brief      Gets the commit identifier.
   *
   * @return     The commit identifier.
   */
  inline cid_t GetCommitId() const { return commit_id_; }

  /**
   * @brief      Gets the epoch identifier.
   *
   * @return     The epoch identifier.
   */
  inline eid_t GetEpochId() const { return epoch_id_; }

  /**
   * @brief      Gets the timestamp.
   *
   * @return     The timestamp.
   */
  inline uint64_t GetTimestamp() const { return timestamp_; }

  /**
   * @brief      Gets the query strings.
   *
   * @return     The query strings.
   */
  inline const std::vector<std::string> &GetQueryStrings() const {
    return query_strings_;
  }

  /**
   * @brief      Sets the commit identifier.
   *
   * @param[in]  commit_id  The commit identifier
   */
  inline void SetCommitId(const cid_t commit_id) { commit_id_ = commit_id; }

  /**
   * @brief      Sets the epoch identifier.
   *
   * @param[in]  epoch_id  The epoch identifier
   */
  inline void SetEpochId(const eid_t epoch_id) { epoch_id_ = epoch_id; }

  /**
   * @brief      Sets the timestamp.
   *
   * @param[in]  timestamp  The timestamp
   */
  inline void SetTimestamp(const uint64_t timestamp) { timestamp_ = timestamp; }

  /**
   * @brief      Adds a query string.
   *
   * @param[in]  query_string  The query string
   */
  inline void AddQueryString(const char *query_string) {
    query_strings_.push_back(std::string(query_string));
  }

  void RecordCreate(oid_t database_oid, oid_t table_oid, oid_t index_oid) {
    rw_object_set_.push_back(
        std::make_tuple(database_oid, table_oid, index_oid, DDLType::CREATE));
  }

  void RecordDrop(oid_t database_oid, oid_t table_oid, oid_t index_oid) {
    rw_object_set_.push_back(
        std::make_tuple(database_oid, table_oid, index_oid, DDLType::DROP));
  }

  void RecordReadOwn(const ItemPointer &);

  void RecordUpdate(const ItemPointer &);

  void RecordInsert(const ItemPointer &);

  /**
   * @brief      Delete the record.
   *
   * @param[in]  <unnamed>  The logical physical location of the record
   * @return    true if INS_DEL, false if DELETE
   */
  bool RecordDelete(const ItemPointer &);

  RWType GetRWType(const ItemPointer &);

  /**
   * @brief      Adds on commit trigger.
   *
   * @param      trigger_data  The trigger data
   */
  // void AddOnCommitTrigger(trigger::TriggerData &trigger_data);

  // void ExecOnCommitTriggers();

  /**
   * @brief      Determines if in rw set.
   *
   * @param[in]  location  The location
   *
   * @return     True if in rw set, False otherwise.
   */
  bool IsInRWSet(const ItemPointer &location) {
    return (rw_set_.find(location) != rw_set_.end());
  }

  /**
   * @brief      Gets the read write set.
   *
   * @return     The read write set.
   */
  inline const ReadWriteSet &GetReadWriteSet() const { return rw_set_; }
  inline const CreateDropSet &GetCreateDropSet() { return rw_object_set_; }

  /**
   * @brief      Gets the gc set pointer.
   *
   * @return     The gc set pointer.
   */
  inline std::shared_ptr<GCSet> GetGCSetPtr() { return gc_set_; }

  /**
   * @brief      Gets the gc object set pointer.
   *
   * @return     The gc object set pointer.
   */
  inline std::shared_ptr<GCObjectSet> GetGCObjectSetPtr() {
    return gc_object_set_;
  }

  /**
   * @brief      Determines if gc set empty.
   *
   * @return     True if gc set empty, False otherwise.
   */
  inline bool IsGCSetEmpty() { return gc_set_->size() == 0; }

  /**
   * @brief      Determines if gc object set empty.
   *
   * @return     True if gc object set empty, False otherwise.
   */
  inline bool IsGCObjectSetEmpty() { return gc_object_set_->size() == 0; }

  /**
   * @brief      Get a string representation for debugging.
   *
   * @return     The information.
   */
  const std::string GetInfo() const;

  /**
   * Set result and status.
   *
   * @param[in]  result  The result
   */
  inline void SetResult(ResultType result) { result_ = result; }

  /**
   * Get result and status.
   *
   * @return     The result.
   */
  inline ResultType GetResult() const { return result_; }

  /**
   * @brief      Determines if read only.
   *
   * @return     True if read only, False otherwise.
   */
  bool IsReadOnly() const { return read_only_; }

  /**
   * @brief      mark this context as read only
   *
   */
  void SetReadOnly() { read_only_ = true; }

  const sintrstore::TableRegistry_t* GetTableRegistry() { 
    //return table_reg_;
    UW_ASSERT(sql_interpreter_);
    return sql_interpreter_->GetTableRegistry_const();
  }
   
  // void SetTableRegistry(const sintrstore::TableRegistry_t *table_reg){
  //   table_reg_ = table_reg;
  // }

  void SetSqlInterpreter(const sintrstore::SQLTransformer *sql_interpreter){
    sql_interpreter_ = sql_interpreter;
  }
  const sintrstore::SQLTransformer * GetSqlInterpreter(){
    return sql_interpreter_;
  }

  Timestamp GetBasilTimestamp() { return basil_timestamp_; }

  void SetBasilTimestamp(const Timestamp &basil_timestamp) {
    basil_timestamp_ = basil_timestamp;
  }

  bool IsDeletion() { return is_deletion_;}
  void SetDeletion(bool is_delete) {is_deletion_ = is_delete; }

  sintrstore::QueryReadSetMgr* GetQueryReadSetMgr() {
    return query_read_set_mgr_;
  }

  void SetQueryReadSetMgr(sintrstore::QueryReadSetMgr *query_read_set_mgr) {
    query_read_set_mgr_ = query_read_set_mgr;
  }

  sintrstore::SnapshotManager* GetSnapshotMgr() {
    return snapshot_mgr_;
  }

  void SetSnapshotMgr(sintrstore::SnapshotManager *snapshot_mgr) {
    snapshot_mgr_ = snapshot_mgr;
  }

  size_t GetKPreparedVersions() {
    return k_prepared_versions_;
  }

  void SetKPreparedVersions(size_t k_prepared_versions) {
    k_prepared_versions_ = k_prepared_versions;
  }

  sintrstore::read_prepared_pred GetReadPreparedPred() { return *read_prepared_pred_; }

  void SetReadPreparedPred(sintrstore::read_prepared_pred *read_prepared) {
    read_prepared_pred_ = read_prepared;
  }

  sintrstore::find_table_version GetTableVersion() { return *table_version_; }

  void SetTableVersion(sintrstore::find_table_version *table_version) {
    if(read_prepared_pred_) predicates_initialized = true;
    table_version_ = table_version;
  }

  bool CheckPredicatesInitialized(){
    return predicates_initialized;
  }

  std::shared_ptr<std::string> GetTxnDig() { return txn_dig_; }

  void SetTxnDig(std::shared_ptr<std::string> txn_dig) { txn_dig_ = txn_dig; }

  const sintrstore::proto::CommittedProof *GetCommittedProof() {
    return committed_proof_;
  }

  void
  SetCommittedProof(const sintrstore::proto::CommittedProof *commit_proof) {
    committed_proof_ = commit_proof;
  }

  const sintrstore::proto::CommittedProof **GetCommittedProofRef() {
    return committed_proof_ref_;
  }

  void SetCommittedProofRef(
      const sintrstore::proto::CommittedProof **commit_proof) {
    committed_proof_ref_ = commit_proof;
  }

  bool GetCommitOrPrepare() { return commit_or_prepare_; }

  void SetCommitOrPrepare(bool commit_or_prepare) {
    commit_or_prepare_ = commit_or_prepare;
  }

  bool CanReadPrepared() { return can_read_prepared_; }

  void SetCanReadPrepared(bool can_read_prepared) {
    can_read_prepared_ = can_read_prepared;
  }

  Timestamp *GetCommitTimestamp() { return committed_timestamp_; }

  void SetCommitTimestamp(Timestamp *commit_timestamp) {
    committed_timestamp_ = commit_timestamp;
  }

  std::shared_ptr<std::string> *GetPreparedTxnDigest() {
    return prepared_txn_dig_;
  }

  void SetPreparedTxnDigest(std::shared_ptr<std::string> *prepared_txn_digest) {
    prepared_txn_dig_ = prepared_txn_digest;
  }

  Timestamp *GetPreparedTimestamp() { return prepared_timestamp_; }

  void SetPreparedTimestamp(Timestamp *prepared_timestamp) {
    prepared_timestamp_ = prepared_timestamp;
  }

  std::string* GetCommittedValue() {
    return committed_value_;
  }
  std::string* GetPreparedValue() {
    return prepared_value_;
  }

  void SetCommittedValue(std::string *committed_value) {
    committed_value_ = committed_value;
  }
  void SetPreparedValue(std::string *prepared_value) {
    prepared_value_ = prepared_value;
  }
  


  bool GetForceMaterialize() {
    return force_materialize_;
  }

  void SetForceMaterialize(bool force_materialize) {
    force_materialize_ = force_materialize;
  }

  bool GetSnapshotRead() {
    return snapshot_read_;
  }

  void SetSnapshotRead(bool snapshot_read) {
    snapshot_read_ = snapshot_read;
  }

  const sintrstore::snapshot *GetSnapshotSet() {
    return snapshot_set_;
  }

  void SetSnapshotSet(const sintrstore::snapshot *snapshot_set) {
    snapshot_set_ = snapshot_set;
  }

  bool GetUndoDelete() { return undo_delete_; }

  void SetUndoDelete(bool undo_delete) { undo_delete_ = undo_delete; }

  bool GetHasReadSetMgr() { return has_read_set_mgr_; }

  void SetHasReadSetMgr(bool has_read_set_mgr) {
    has_read_set_mgr_ = has_read_set_mgr;
  }

  bool GetHasSnapshotMgr() { return has_snapshot_mgr_; }

  void SetHasSnapshotMgr(bool has_snapshot_mgr) {
    has_snapshot_mgr_ = has_snapshot_mgr;
  }

  bool IsPointRead() { return is_point_read_; }

  void SetIsPointRead(bool is_point_read) { is_point_read_ = is_point_read; }

  bool IsNLJoin() {return is_nl_join_;}
  void SetIsNLJoin(bool is_nl_join){ is_nl_join_ = is_nl_join;}

  /**
   * @brief      Gets the isolation level.
   *
   * @return     The isolation level.
   */
  inline IsolationLevelType GetIsolationLevel() const {
    return isolation_level_;
  }

  /** cache for table catalog objects */
  catalog::CatalogCache catalog_cache;
  bool skip_cache = false;

private:
  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  /** transaction id */
  txn_id_t txn_id_;

  /** id of thread creating this transaction */
  size_t thread_id_;

  /**
   * read id
   * this id determines which tuple versions the transaction can access.
   */
  cid_t read_id_;

  /**
   * commit id
   * this id determines the id attached to the tuple version written by the
   * transaction.
   */
  cid_t commit_id_;

  /**
   * epoch id can be extracted from read id.
   * GC manager uses this id to check whether a version is still visible.
   */
  eid_t epoch_id_;

  /**
   * vector of strings to log at the end of the transaction
   * populated only if the indextuner is running
   */
  std::vector<std::string> query_strings_;

  /** timestamp when the transaction began */
  uint64_t timestamp_;

  const sintrstore::SQLTransformer *sql_interpreter_;
  // Reference to the TableRegistry
  const sintrstore::TableRegistry_t *table_reg_;

  /** Basil timestamp */
  Timestamp basil_timestamp_;

  bool is_deletion_ = false; //whether or not this statement is a delete.

  /** Query read set manager */
  sintrstore::QueryReadSetMgr *query_read_set_mgr_ = nullptr;

  /** Transaction digest */
  std::shared_ptr<std::string> txn_dig_;

  /** Commit proof */
  const sintrstore::proto::CommittedProof *committed_proof_;

  /* Commit proof ref */
  const sintrstore::proto::CommittedProof **committed_proof_ref_;

  /** Timestamp of committed value */
  Timestamp *committed_timestamp_;

  /** Timestamp of the prepared value */
  Timestamp *prepared_timestamp_;

  /** Prepared value transaction digest */
  std::shared_ptr<std::string> *prepared_txn_dig_;

  /** Committed and Prepared Result dummy values */
  std::string* committed_value_;
  std::string* prepared_value_;
  

  /** Commit or prepare */
  bool commit_or_prepare_ = true;

  /** Force materialize for ApplyTableWrite */
  bool force_materialize_ = false;

  /** Snapshot read */
  bool snapshot_read_ = false;

  /** Snapshot set */
  const sintrstore::snapshot *snapshot_set_ = nullptr;

  bool predicates_initialized = false;
  /** Read prepared predicate */
  sintrstore::read_prepared_pred *read_prepared_pred_ = &default_read_prepared_pred;

  /** Find table version predicate */
  sintrstore::find_table_version *table_version_ = &default_find_table_version;

  /** Can read prepared */
  bool can_read_prepared_ = false;

  /** Whether purge is undoing a delete */
  bool undo_delete_ = false;

  /** Whether read set manager was passed in */
  bool has_read_set_mgr_ = false;

  /** Whether snapshot manager was passed in */
  bool has_snapshot_mgr_ = false;

  /** Snapshot manager */
  sintrstore::SnapshotManager *snapshot_mgr_ = nullptr;

  /** K prepared versions */
  size_t k_prepared_versions_ = 1;

  /** Whether this is a point read query */
  bool is_point_read_ = false;

  bool is_nl_join_ = false;

  ReadWriteSet rw_set_;
  CreateDropSet rw_object_set_;

  /**
   * this set contains data location that needs to be gc'd in the transaction.
   */
  std::shared_ptr<GCSet> gc_set_;
  std::shared_ptr<GCObjectSet> gc_object_set_;

  /** result of the transaction */
  ResultType result_ = ResultType::SUCCESS;

  IsolationLevelType isolation_level_;

  bool is_written_;

  // std::unique_ptr<trigger::TriggerSet> on_commit_triggers_;

  /** one default transaction is NOT 'read only' unless it is marked 'read only'
   * explicitly*/
  bool read_only_ = false;
};

} // namespace concurrency
} // namespace peloton
