//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_metrics_catalog.h
//
// Identification: src/include/../catalog/trigger_catalog.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// pg_trigger
//
// Schema: (column offset: column_name)
// 0: oid (pkey)
// 1: tgrelid   : table_oid
// 2: tgname    : trigger_name
// 3: tgfoid    : function_oid
// 4: tgtype    : trigger_type
// 5: tgargs    : function_arguemnts
// 6: tgqual    : fire_condition
// 7: timestamp : time_stamp
//
// Indexes: (index offset: indexed columns)
// 0: oid (primary key)
// 1: tgrelid & tgtype (secondary key 0)
// 2: tgrelid (secondary key 1)
// 3: tgname & tgrelid (secondary key 2)
//===----------------------------------------------------------------------===//

#pragma once

#include "../catalog/abstract_catalog.h"
#include "../catalog/catalog_defaults.h"

#define TRIGGER_CATALOG_NAME "pg_trigger"

namespace peloton_peloton {

namespace trigger {
class TriggerList;
}  // namespace trigger

namespace catalog {

class TriggerCatalog : public AbstractCatalog {
 public:
  TriggerCatalog(concurrency::TransactionContext *txn,
                 const std::string &database_name);
  ~TriggerCatalog();

  oid_t GetNextOid() { return oid_++ | TRIGGER_OID_MASK; }

  void UpdateOid(oid_t add_value) { oid_ += add_value; }

  //===--------------------------------------------------------------------===//
  // write Related API
  //===--------------------------------------------------------------------===//
  bool InsertTrigger(concurrency::TransactionContext *txn,
                     oid_t table_oid,
                     std::string trigger_name,
                     int16_t trigger_type,
                     std::string proc_oid,
                     std::string function_arguments,
                     type::Value fire_condition,
                     type::Value timestamp,
                     type::AbstractPool *pool);

  ResultType DropTrigger(concurrency::TransactionContext *txn,
                         const oid_t database_oid,
                         const oid_t table_oid,
                         const std::string &trigger_name);

  bool DeleteTriggerByName(concurrency::TransactionContext *txn,
                           oid_t table_oid,
                           const std::string &trigger_name);

  //===--------------------------------------------------------------------===//
  // get triggers for a specific table; one table may have multiple triggers
  // of the same type
  //===--------------------------------------------------------------------===//
  std::unique_ptr<trigger::TriggerList> GetTriggersByType(concurrency::TransactionContext *txn,
                                                          oid_t table_oid,
                                                          int16_t trigger_type);

  //===--------------------------------------------------------------------===//
  // get all types of triggers for a specific table
  //===--------------------------------------------------------------------===//
  std::unique_ptr<trigger::TriggerList> GetTriggers(concurrency::TransactionContext *txn,
                                                    oid_t table_oid);

  oid_t GetTriggerOid(concurrency::TransactionContext *txn,
                      oid_t table_oid,
                      std::string trigger_name);

  enum ColumnId {
    TRIGGER_OID = 0,
    TABLE_OID = 1,
    TRIGGER_NAME = 2,
    FUNCTION_OID = 3,
    TRIGGER_TYPE = 4,
    FUNCTION_ARGS = 5,
    FIRE_CONDITION = 6,
    TIMESTAMP = 7
  };

 private:
  enum IndexId {
    PRIMARY_KEY = 0,
    TABLE_TYPE_KEY_0 = 1,
    TABLE_KEY_1 = 2,
    NAME_TABLE_KEY_2 = 3,
  };
};

}  // namespace catalog
}  // namespace peloton
