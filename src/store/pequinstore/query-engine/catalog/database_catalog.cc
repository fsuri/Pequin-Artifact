//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// database_catalog.cpp
//
// Identification: src/catalog/database_catalog.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "../catalog/database_catalog.h"

#include <memory>

#include "../catalog/catalog.h"
#include "../catalog/system_catalogs.h"
#include "../concurrency/transaction_context.h"
#include "../executor/logical_tile.h"
#include "../storage/data_table.h"
#include "../storage/tuple.h"
#include "../type/value_factory.h"
#include <shared_mutex>

namespace peloton {
namespace catalog {

DatabaseCatalogEntry::DatabaseCatalogEntry(concurrency::TransactionContext *txn,
                                           executor::LogicalTile *tile)
    : database_oid_(tile->GetValue(0, DatabaseCatalog::ColumnId::DATABASE_OID).GetAs<oid_t>()),
      database_name_(tile->GetValue(0, DatabaseCatalog::ColumnId::DATABASE_NAME).ToString()),
      table_catalog_entries_cache_(),
      table_catalog_entries_cache_by_name(),
      valid_table_catalog_entries(false),
      txn_(txn) {}

/* @brief   insert table catalog object into cache
 * @param   table_object
 * @return  false if table_name already exists in cache
 */
bool DatabaseCatalogEntry::InsertTableCatalogEntry(
    std::shared_ptr<TableCatalogEntry> table_catalog_entry) {
  if (!table_catalog_entry || table_catalog_entry->GetTableOid() == INVALID_OID) {
    Panic("invalid");
    return false;  // invalid object
  }

  //TODO: Why are multiple threads concurrently adding the same. Isn't this per TX context??
  // check if already in cache
  if (table_catalog_entries_cache_.find(table_catalog_entry->GetTableOid()) != table_catalog_entries_cache_.end()) {
    LOG_DEBUG("Table %u already exists in cache!", table_catalog_entry->GetTableOid());
     //Panic("oid already in cache");
    return false;
  }

  std::string key = table_catalog_entry->GetSchemaName() + "." + table_catalog_entry->GetTableName();
  if (table_catalog_entries_cache_by_name.find(key) != table_catalog_entries_cache_by_name.end()) {
    LOG_DEBUG("Table %s already exists in cache!", table_catalog_entry->GetTableName().c_str());
     //Panic("name already in cache");
    return false;
  }

  table_catalog_entries_cache_.insert(std::make_pair(table_catalog_entry->GetTableOid(), table_catalog_entry));
  table_catalog_entries_cache_by_name.insert(std::make_pair(key, table_catalog_entry));
  return true;
}

/* @brief   evict table catalog object from cache
 * @param   table_oid
 * @return  true if table_oid is found and evicted; false if not found
 */
bool DatabaseCatalogEntry::EvictTableCatalogEntry(oid_t table_oid) {
  // find table name from table name cache
  auto it = table_catalog_entries_cache_.find(table_oid);
  if (it == table_catalog_entries_cache_.end()) {
    return false;  // table oid not found in cache
  }

  auto table_object = it->second;
  PELOTON_ASSERT(table_object);
  table_catalog_entries_cache_.erase(it);
  // erase from table name cache
  std::string key =
      table_object->GetSchemaName() + "." + table_object->GetTableName();
  table_catalog_entries_cache_by_name.erase(key);
  return true;
}

/* @brief   evict table catalog object from cache
 * @param   table_name
 * @return  true if table_name is found and evicted; false if not found
 */
bool DatabaseCatalogEntry::EvictTableCatalogEntry(const std::string &table_name,
                                                  const std::string &schema_name) {
  std::string key = schema_name + "." + table_name;
  // find table name from table name cache
  auto it = table_catalog_entries_cache_by_name.find(key);
  if (it == table_catalog_entries_cache_by_name.end()) {
    return false;  // table name not found in cache
  }

  auto table_object = it->second;
  PELOTON_ASSERT(table_object);
  table_catalog_entries_cache_by_name.erase(it);
  table_catalog_entries_cache_.erase(table_object->GetTableOid());
  return true;
}

/*@brief   evict all table catalog objects in this database from cache
 */
void DatabaseCatalogEntry::EvictAllTableCatalogEntries() {
  table_catalog_entries_cache_.clear();
  table_catalog_entries_cache_by_name.clear();
}

/* @brief   Get table catalog object from cache or all the way from storage
 * @param   table_oid     table oid of the requested table catalog object
 * @param   cached_only   if cached only, return nullptr on a cache miss
 * @return  Shared pointer to the requested table catalog object
 */
std::shared_ptr<TableCatalogEntry> DatabaseCatalogEntry::GetTableCatalogEntry(
    oid_t table_oid, bool cached_only) {
  auto it = table_catalog_entries_cache_.find(table_oid);
  if (it != table_catalog_entries_cache_.end()) return it->second;

  if (cached_only) {
    // cache miss return empty object
    return nullptr;
  } else {
    // cache miss get from pg_table
    auto pg_table = Catalog::GetInstance()->GetSystemCatalogs(database_oid_)->GetTableCatalog();
     std::cerr << "Using txn_ 4" << std::endl;
    return pg_table->GetTableCatalogEntry(txn_, table_oid);
  }
}

/* @brief   Get table catalog object from cache (cached_only == true),
            or all the way from storage (cached_only == false)
 * @param   table_name     table name of the requested table catalog object
 * @param   schema_name    schema name of the requested table catalog object
 * @param   cached_only   if cached only, return nullptr on a cache miss
 * @return  Shared pointer to the requested table catalog object
 */
std::shared_ptr<TableCatalogEntry> DatabaseCatalogEntry::GetTableCatalogEntry(
    const std::string &table_name, const std::string &schema_name,
    bool cached_only) {

  // struct timespec ts_end;
  // clock_gettime(CLOCK_MONOTONIC, &ts_end);
  // uint64_t microseconds_start0 = ts_end.tv_sec * 1000 * 1000 + ts_end.tv_nsec / 1000;

    
    //FIXME: NOT FINDING ANYTHING IN CACHE?
  std::string key = schema_name + "." + table_name;
  auto it = table_catalog_entries_cache_by_name.find(key);  //TODO: IS this by Txn Context only? Try to generalize it.
  if (it != table_catalog_entries_cache_by_name.end()) {
    //std::cerr << "CACHE HIT TABLE\n";

    //  clock_gettime(CLOCK_MONOTONIC, &ts_end);
    // uint64_t microseconds_end0 = ts_end.tv_sec * 1000 * 1000 + ts_end.tv_nsec / 1000;
    //  Warning("CACHE HIT: %d us", microseconds_end0 - microseconds_start0);
    
    return it->second;
  }

  if (cached_only) {
    // cache miss return empty object
    return nullptr;
  } else {
     //std::cerr << "CACHE MISS TABLE\n";
    // cache miss get from pg_table
    

    auto pg_table = Catalog::GetInstance()->GetSystemCatalogs(database_oid_)->GetTableCatalog(); //TODO: Look into this.
    
    // clock_gettime(CLOCK_MONOTONIC, &ts_end);
    // uint64_t microseconds_end = ts_end.tv_sec * 1000 * 1000 + ts_end.tv_nsec / 1000;
    // Warning("PG_TABLE LAT: %d us", microseconds_end - microseconds_start0);
    
    // std::cerr << "Using txn_ 1" << std::endl;
    return pg_table->GetTableCatalogEntry(txn_, schema_name, table_name);
  }
}

/*@brief    Get table catalog object from cache (cached_only == true),
            or all the way from storage (cached_only == false)
 * @param   schema_name
 * @return  table catalog objects
 */
std::vector<std::shared_ptr<TableCatalogEntry>>
DatabaseCatalogEntry::GetTableCatalogEntries(const std::string &schema_name) {
  // read directly from pg_table
  if (!valid_table_catalog_entries) {
    auto pg_table = Catalog::GetInstance()
                        ->GetSystemCatalogs(database_oid_)
                        ->GetTableCatalog();
    // insert every table object into cache
     //std::cerr << "Using txn_ 2" << std::endl;
    pg_table->GetTableCatalogEntries(txn_);
  }
  // make sure to check IsValidTableObjects() before getting table objects
  PELOTON_ASSERT(valid_table_catalog_entries);
  std::vector<std::shared_ptr<TableCatalogEntry>> result;
  for (auto it : table_catalog_entries_cache_) {
    if (it.second->GetSchemaName() == schema_name) {
      result.push_back(it.second);
    }
  }

  return result;
}

/* @brief   Get table catalog object from cache (cached_only == true),
            or all the way from storage (cached_only == false)
 * @param   cached_only   if cached only, return nullptr on a cache miss
 * @return  Shared pointer to the requested table catalog object
 */
std::unordered_map<oid_t, std::shared_ptr<TableCatalogEntry>>
DatabaseCatalogEntry::GetTableCatalogEntries(bool cached_only) {
  if (!cached_only && !valid_table_catalog_entries) {
    // cache miss get from pg_table
    auto pg_table = Catalog::GetInstance()
                        ->GetSystemCatalogs(database_oid_)
                        ->GetTableCatalog();
    //std::cerr << "Using txn_ 3" << std::endl;
    return pg_table->GetTableCatalogEntries(txn_);
  }
  // make sure to check IsValidTableObjects() before getting table objects
  PELOTON_ASSERT(valid_table_catalog_entries);
  return table_catalog_entries_cache_;
}

/*@brief   search index catalog object from all cached database objects
 * @param   index_oid
 * @return  index catalog object; if not found return null
 */
std::shared_ptr<IndexCatalogEntry> DatabaseCatalogEntry::GetCachedIndexCatalogEntry(
    oid_t index_oid) {
  for (auto it = table_catalog_entries_cache_.begin(); it != table_catalog_entries_cache_.end();
       ++it) {
    auto table_object = it->second;
    auto index_object = table_object->GetIndexCatalogEntries(index_oid, true);
    if (index_object) return index_object;
  }
  return nullptr;
}

/*@brief   search index catalog object from all cached database objects
 * @param   index_name
 * @return  index catalog object; if not found return null
 */
std::shared_ptr<IndexCatalogEntry> DatabaseCatalogEntry::GetCachedIndexCatalogEntry(
    const std::string &index_name, const std::string &schema_name) {
  for (auto it = table_catalog_entries_cache_.begin(); it != table_catalog_entries_cache_.end();
       ++it) {
    auto table_object = it->second;
    if (table_object != nullptr &&
        table_object->GetSchemaName() == schema_name) {
      auto index_object = table_object->GetIndexCatalogEntry(index_name, true);
      if (index_object) return index_object;
    }
  }
  return nullptr;
}

DatabaseCatalog *DatabaseCatalog::GetInstance(concurrency::TransactionContext *txn,
                                             storage::Database *pg_catalog,
                                             type::AbstractPool *pool) {
  static DatabaseCatalog database_catalog{txn, pg_catalog, pool};
  return &database_catalog;
}

DatabaseCatalog::DatabaseCatalog(concurrency::TransactionContext *,
                                 storage::Database *pg_catalog,
                                 type::AbstractPool *)
    : AbstractCatalog(pg_catalog,
                      InitializeSchema().release(),
                      DATABASE_CATALOG_OID,
                      DATABASE_CATALOG_NAME) {
  // Add indexes for pg_database
  AddIndex(DATABASE_CATALOG_NAME "_pkey",
           DATABASE_CATALOG_PKEY_OID,
           {ColumnId::DATABASE_OID},
           IndexConstraintType::PRIMARY_KEY);
  AddIndex(DATABASE_CATALOG_NAME "_skey0",
           DATABASE_CATALOG_SKEY0_OID,
           {ColumnId::DATABASE_NAME},
           IndexConstraintType::UNIQUE);
}

DatabaseCatalog::~DatabaseCatalog() {}

/*@brief   private function for initialize schema of pg_database
 * @return  unqiue pointer to schema
 */
std::unique_ptr<catalog::Schema> DatabaseCatalog::InitializeSchema() {
  auto database_id_column = catalog::Column(
      type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
      "database_oid", true);
  database_id_column.SetNotNull();

  auto database_name_column = catalog::Column(
      type::TypeId::VARCHAR, max_name_size_, "database_name", false);
  database_name_column.SetNotNull();

  std::unique_ptr<catalog::Schema> database_catalog_schema(
      new catalog::Schema({database_id_column, database_name_column}));

  database_catalog_schema->AddConstraint(std::make_shared<Constraint>(
      DATABASE_CATALOG_CON_PKEY_OID, ConstraintType::PRIMARY, "con_primary",
      DATABASE_CATALOG_OID, std::vector<oid_t>{ColumnId::DATABASE_OID},
      DATABASE_CATALOG_PKEY_OID));

  database_catalog_schema->AddConstraint(std::make_shared<Constraint>(
      DATABASE_CATALOG_CON_UNI0_OID, ConstraintType::UNIQUE, "con_unique",
      DATABASE_CATALOG_OID, std::vector<oid_t>{ColumnId::DATABASE_NAME},
      DATABASE_CATALOG_SKEY0_OID));

  return database_catalog_schema;
}

bool DatabaseCatalog::InsertDatabase(concurrency::TransactionContext *txn,
                                     oid_t database_oid,
                                     const std::string &database_name,
                                     type::AbstractPool *pool) {
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  auto val0 = type::ValueFactory::GetIntegerValue(database_oid);
  auto val1 = type::ValueFactory::GetVarcharValue(database_name, nullptr);

  tuple->SetValue(ColumnId::DATABASE_OID, val0, pool);
  tuple->SetValue(ColumnId::DATABASE_NAME, val1, pool);

  // Insert the tuple
  return InsertTuple(txn, std::move(tuple));
}

bool DatabaseCatalog::DeleteDatabase(concurrency::TransactionContext *txn, oid_t database_oid) {
  oid_t index_offset = IndexId::PRIMARY_KEY;  // Index of database_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(database_oid).Copy());

  // evict cache
  txn->catalog_cache.EvictDatabaseObject(database_oid);

  return DeleteWithIndexScan(txn, index_offset, values);
}

//////
static std::map<oid_t, std::shared_ptr<peloton::catalog::DatabaseCatalogEntry>> testCache2;
static std::map<std::string, std::shared_ptr<peloton::catalog::DatabaseCatalogEntry>> testCache;
static std::shared_mutex cache_m;
static std::shared_mutex cache_m2;

////////

std::shared_ptr<DatabaseCatalogEntry> DatabaseCatalog::GetDatabaseCatalogEntry(
    concurrency::TransactionContext *txn,
    oid_t database_oid) {
  if (txn == nullptr) {
    throw CatalogException("Transaction is invalid!");
  }




  // try get from cache
  auto database_object = txn->catalog_cache.GetDatabaseObject(database_oid);
  if (database_object) return database_object;


   //Test code: //TODO: Turn off if during loading. (also don't add to cache)
               //If it works, also add rw mutex for concurrency..
  
    //std::cerr << "Skip cache2: " << txn->skip_cache << std::endl;
  if(!txn->skip_cache){
     std::shared_lock lock(cache_m2);
  
    auto itr = testCache2.find(database_oid);
    if(itr != testCache2.end()){
      //std::cerr << "using cache2" << std::endl;
      auto database_object = itr->second;

      DatabaseCatalogEntry database_object_copy = *database_object;
      database_object_copy.txn_ = txn;
      auto database_object_ptr = std::make_shared<DatabaseCatalogEntry>(database_object_copy);

      bool success = txn->catalog_cache.InsertDatabaseObject(database_object_ptr);
      PELOTON_ASSERT(success == true);
        (void)success;

      return database_object_ptr;
      //return database_object;
    }
  }



  // cache miss, get from pg_database
  std::vector<oid_t> column_ids(all_column_ids_);
  oid_t index_offset = IndexId::PRIMARY_KEY;  // Index of database_oid
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(database_oid).Copy());

  auto result_tiles = GetResultWithIndexScan(txn, column_ids, index_offset, values);

  if (result_tiles->size() == 1 && (*result_tiles)[0]->GetTupleCount() == 1) {
    auto database_object = std::make_shared<DatabaseCatalogEntry>(txn, (*result_tiles)[0].get());
    // insert into cache
    if(!txn->skip_cache){
      std::unique_lock lock(cache_m2);
    
      testCache2.insert(std::make_pair(database_oid, database_object));
      //testCache2[database_oid] = database_object;
      //std::cerr << "adding to cache (via Oid): " << database_object->GetDatabaseName() << std::endl;
    }

    bool success = txn->catalog_cache.InsertDatabaseObject(database_object);
    PELOTON_ASSERT(success == true);
    (void)success;
    return database_object;
  } else {
    LOG_DEBUG("Found %lu database tiles with oid %u", result_tiles->size(),
              database_oid);
  }

  // return empty object if not found
  return nullptr;
}




/* @brief   First try get cached object from transaction cache. If cache miss,
 *          construct database object from pg_database, and insert into the
 *          cache.
 */
std::shared_ptr<DatabaseCatalogEntry> DatabaseCatalog::GetDatabaseCatalogEntry(
    concurrency::TransactionContext *txn,
    const std::string &database_name) {
  if (txn == nullptr) {
    throw CatalogException("Transaction is invalid!");
  }


  //FIXME: SEEMINGLY ALWAYS MISSING CACHE? 
  //TODO: Make the cache global instead of per TX.
  // try get from cache
  //auto database_object = catalog_cache_.GetDatabaseObject(database_name);
 
  auto database_object = txn->catalog_cache.GetDatabaseObject(database_name);
  if (database_object){
    //std::cerr << "CACHE HIT DB\n";
    return database_object;
  } 
  //std::cerr << "CACHE MISS DB\n";

    //Test code: //TODO: Turn off if during loading. (also don't add to cache)
               //If it works, also add rw mutex for concurrency..
  
  //std::cerr << "Skip cache: " << txn->skip_cache << std::endl;
  if(!txn->skip_cache){
    std::shared_lock lock(cache_m);
  
    auto itr = testCache.find(database_name);
    if(itr != testCache.end()){
      //std::cerr << "using cache" << std::endl;
      auto database_object = itr->second;

      DatabaseCatalogEntry database_object_copy = *database_object;
      database_object_copy.txn_ = txn;
      auto database_object_ptr = std::make_shared<DatabaseCatalogEntry>(database_object_copy);

      //TODO: Do we even need to insert this?
      bool success = txn->catalog_cache.InsertDatabaseObject(database_object_ptr);
      PELOTON_ASSERT(success == true);
        (void)success;
      return database_object_ptr;
      //return database_object;
    }
  }
  //END Test code

  // cache miss, get from pg_database
  std::vector<oid_t> column_ids(all_column_ids_);
  oid_t index_offset = IndexId::SKEY_DATABASE_NAME;  // Index of database_name
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetVarcharValue(database_name, nullptr).Copy());

  //     struct timespec ts_start;
  // clock_gettime(CLOCK_MONOTONIC, &ts_start);
  // uint64_t microseconds_start = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;

  // std::cerr << "database_name: " << database_name << std::endl;
  //  std::cerr << "index_offset: " << index_offset << std::endl;

  // for(auto &col: column_ids){
  //   std::cerr << "col_id: " << col << std::endl;
  // }

  auto result_tiles = GetResultWithIndexScan(txn, column_ids, index_offset, values);

  //  clock_gettime(CLOCK_MONOTONIC, &ts_start);
  //   uint64_t microseconds_end = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;
  //   Warning("DB Catalog get: %d us", microseconds_end-microseconds_start);

  if (result_tiles->size() == 1 && (*result_tiles)[0]->GetTupleCount() == 1) {
    auto database_object = std::make_shared<DatabaseCatalogEntry>(txn, (*result_tiles)[0].get()); //FIXME: what Txn to pick here? pick a random txn?
    if (database_object) {
      // insert into cache
      if(!txn->skip_cache){
        std::unique_lock lock(cache_m);
        // std::cerr << "adding to cache: " << database_name << std::endl;
        testCache.insert(std::make_pair(database_name, database_object));
        //testCache[database_name] = database_object;    //TODO: Currently the database object is associated with a single txn context. => need to remove or change it?
      }
      //TODO: Why do we need to insert this to the cache? If we have global cache anyways??
      bool success = txn->catalog_cache.InsertDatabaseObject(database_object);
      //bool success = catalog_cache_.InsertDatabaseObject(database_object); //FIXME: doesn't work
      PELOTON_ASSERT(success == true);
      (void)success;
    }

    //  clock_gettime(CLOCK_MONOTONIC, &ts_start);
    // uint64_t microseconds_end2 = ts_start.tv_sec * 1000 * 1000 + ts_start.tv_nsec / 1000;
    // Warning("DB Catalog cache: %d us", microseconds_end2-microseconds_end);
    return database_object;
  }

  Panic("should always find DB");
  // return empty object if not found
  return nullptr;
}

}  // namespace catalog
}  // namespace peloton
