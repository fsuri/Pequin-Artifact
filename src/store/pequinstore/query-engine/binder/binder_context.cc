//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// binder_context.cpp
//
// Identification: src/binder/binder_context.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "../binder/binder_context.h"

#include "../catalog/catalog.h"
#include "../catalog/column_catalog.h"
#include "../catalog/database_catalog.h"
#include "../catalog/table_catalog.h"
#include "../expression/tuple_value_expression.h"
#include "../parser/table_ref.h"
#include "../storage/storage_manager.h"

namespace peloton {
namespace binder {

void BinderContext::AddRegularTable(parser::TableRef *table_ref,
                                    const std::string &default_database_name,
                                    concurrency::TransactionContext *txn) {
  table_ref->TryBindDatabaseName(default_database_name);
  auto table_alias = table_ref->GetTableAlias();
  AddRegularTable(table_ref->GetDatabaseName(), table_ref->GetSchemaName(), table_ref->GetTableName(), table_alias, txn);
}

void BinderContext::AddRegularTable(const std::string &db_name,
                                    const std::string &schema_name,
                                    const std::string &table_name,
                                    const std::string &table_alias,
                                    concurrency::TransactionContext *txn) {

                                      //TODO: FIXME: THIS SEEMS TO BE EXPENSIVE
  // using catalog object to retrieve meta-data
    //TESTING HOW LONG THIS TAKES: FIXME: REMOVE 
  // struct timespec ts_start;
  // clock_gettime(CLOCK_MONOTONIC, &ts_start);
  // uint64_t microseconds_start2 = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;

   //std::cerr << "table_name: " << table_name << ". Alias: " << table_alias << std::endl;

  //TODO: Somehow bind to txn context without looking up?

  //std::cerr << "AddRegularTable -> GetTableCatalogEntry" << std::endl;
  auto table_object = catalog::Catalog::GetInstance()->GetTableCatalogEntry(txn, db_name, schema_name, table_name);


  // clock_gettime(CLOCK_MONOTONIC, &ts_start);
  // uint64_t microseconds_end2 = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;
 
  // //Should not take more than 1 ms (already generous) to parse and prepare.
  //  auto duration2 = microseconds_end2 - microseconds_start2;
  // // if(duration2 > 2000){
  // //   Panic("GetTablecatalogEntry exceeded 2ms: %d", duration2);
  // // }

  if (regular_table_alias_map_.find(table_alias) != regular_table_alias_map_.end() || nested_table_alias_map_.find(table_alias) != nested_table_alias_map_.end()) {
    Panic("duplicate");
    throw Exception("Duplicate alias " + table_alias);
  }
  regular_table_alias_map_[table_alias] = table_object;
}

void BinderContext::AddNestedTable(
    const std::string table_alias,
    std::vector<std::unique_ptr<expression::AbstractExpression>> &select_list) {
  if (regular_table_alias_map_.find(table_alias) !=
          regular_table_alias_map_.end() ||
      nested_table_alias_map_.find(table_alias) !=
          nested_table_alias_map_.end()) {
    throw Exception("Duplicate alias " + table_alias);
  }

  std::unordered_map<std::string, type::TypeId> column_alias_map;
  for (auto &expr : select_list) {
    std::string alias;
    if (!expr->alias.empty()) {
      alias = expr->alias;
    } else if (expr->GetExpressionType() == ExpressionType::VALUE_TUPLE) {
      auto tv_expr =
          reinterpret_cast<expression::TupleValueExpression *>(expr.get());
      alias = tv_expr->GetColumnName();
    } else
      continue;
    std::transform(alias.begin(), alias.end(), alias.begin(), ::tolower);
    column_alias_map[alias] = expr->GetValueType();
  }
  nested_table_alias_map_[table_alias] = column_alias_map;
}

bool BinderContext::GetColumnPosTuple(
    const std::string &col_name,
    std::shared_ptr<catalog::TableCatalogEntry> table_obj,
    std::tuple<oid_t, oid_t, oid_t> &col_pos_tuple, type::TypeId &value_type) {
  auto column_object = table_obj->GetColumnCatalogEntry(col_name);
  if (column_object == nullptr) {
    return false;
  }

  oid_t col_pos = column_object->GetColumnId();
  col_pos_tuple = std::make_tuple(table_obj->GetDatabaseOid(),
                                  table_obj->GetTableOid(), col_pos);
  value_type = column_object->GetColumnType();
  return true;
}

bool BinderContext::GetColumnPosTuple(
    std::shared_ptr<BinderContext> current_context, const std::string &col_name,
    std::tuple<oid_t, oid_t, oid_t> &col_pos_tuple, std::string &table_alias,
    type::TypeId &value_type, int &depth) {
  bool find_matched = false;
  while (current_context != nullptr) {
    // Check regular table
    for (auto entry : current_context->regular_table_alias_map_) {
      bool get_matched =
          GetColumnPosTuple(col_name, entry.second, col_pos_tuple, value_type);
      if (get_matched) {
        if (!find_matched) {
          // First match
          find_matched = true;
          table_alias = entry.first;
        } else {
          throw Exception("Ambiguous column name " + col_name);
        }
      }
    }
    // Check nested table
    for (auto entry : current_context->nested_table_alias_map_) {
      bool get_match = entry.second.find(col_name) != entry.second.end();
      if (get_match) {
        if (!find_matched) {
          // First match
          find_matched = true;
          table_alias = entry.first;
          value_type = entry.second[col_name];
        } else {
          throw Exception("Ambiguous column name " + col_name);
        }
      }
    }
    if (find_matched) {
      depth = current_context->depth_;
      return true;
    }
    current_context = current_context->GetUpperContext();
  }
  return false;
}

bool BinderContext::GetRegularTableObj(
    std::shared_ptr<BinderContext> current_context, std::string &alias,
    std::shared_ptr<catalog::TableCatalogEntry> &table_obj, int &depth) {
  while (current_context != nullptr) {
    auto iter = current_context->regular_table_alias_map_.find(alias);
    if (iter != current_context->regular_table_alias_map_.end()) {
      table_obj = iter->second;
      depth = current_context->depth_;
      return true;
    }
    current_context = current_context->GetUpperContext();
  }
  return false;
}

bool BinderContext::CheckNestedTableColumn(
    std::shared_ptr<BinderContext> current_context, std::string &alias,
    std::string &col_name, type::TypeId &value_type, int &depth) {
  while (current_context != nullptr) {
    auto iter = current_context->nested_table_alias_map_.find(alias);
    if (iter != current_context->nested_table_alias_map_.end()) {
      auto col_iter = iter->second.find(col_name);
      if (col_iter == iter->second.end()) {
        throw Exception("Cannot find column " + col_name);
      }
      value_type = col_iter->second;
      depth = current_context->depth_;
      return true;
    }
    current_context = current_context->GetUpperContext();
  }
  return false;
}

void BinderContext::GenerateAllColumnExpressions(
    std::vector<std::unique_ptr<expression::AbstractExpression>> &exprs) {

      //TESTING HOW LONG THIS TAKES: FIXME: REMOVE 
  // struct timespec ts_start;
  // clock_gettime(CLOCK_MONOTONIC, &ts_start);
  // uint64_t microseconds_start = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;
   

  for (auto &entry : regular_table_alias_map_) {
    auto &table_obj = entry.second;
    auto col_cnt = table_obj->GetColumnCatalogEntries().size();
    for (size_t i = 0; i < col_cnt; i++) {
      auto col_obj = table_obj->GetColumnCatalogEntry(i);
      auto tv_expr = new expression::TupleValueExpression(
          std::string(col_obj->GetColumnName()), std::string(entry.first));
      tv_expr->SetValueType(col_obj->GetColumnType());
      tv_expr->DeduceExpressionName();
      tv_expr->SetBoundOid(table_obj->GetDatabaseOid(),
                           table_obj->GetTableOid(), col_obj->GetColumnId());
      exprs.emplace_back(tv_expr);
    }
  }

  for (auto &entry : nested_table_alias_map_) {
    auto &table_alias = entry.first;
    auto &cols = entry.second;
    for (auto &col_entry : cols) {
      auto tv_expr = new expression::TupleValueExpression(
          std::string(col_entry.first), std::string(table_alias));
      tv_expr->SetValueType(col_entry.second);
      tv_expr->DeduceExpressionName();
      // All derived columns do not have bound column id. We need to set them to
      // all zero to get rid of garbage value and make comparison work
      tv_expr->SetBoundOid(0, 0, 0);
      exprs.emplace_back(tv_expr);
    }
  }


  // clock_gettime(CLOCK_MONOTONIC, &ts_start);
  // uint64_t microseconds_end = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;
 
  // //Should not take more than 1 ms (already generous) to parse and prepare.
  //  auto duration = microseconds_end - microseconds_start;
  // if(duration > 50){
  //   Warning("GenerateAllColumnExpressions exceeded 50us: %d", duration);
  // }

}

}  // namespace binder
}  // namespace peloton
