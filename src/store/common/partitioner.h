/***********************************************************************
 *
 * Copyright 2021 Florian Suri-Payer <fsp@cs.cornell.edu>
 *                Matthew Burke <matthelb@cs.cornell.edu>
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/
#ifndef PARTITIONER_H
#define PARTITIONER_H

#include <functional>
#include <set>
#include <string>
#include <random>
#include <vector>
#include <map>

#include "lib/message.h"

enum partitioner_t {
  DEFAULT = 0,
  WAREHOUSE_DIST_ITEMS,
  WAREHOUSE,
  RW_SQL,
};

class Partitioner {
 public:
  Partitioner() {}
  virtual ~Partitioner() {}
  //Use this for KV-mode
  virtual uint64_t operator()(const std::string &key, uint64_t numShards, int group, const std::vector<int> &txnGroups) = 0;
  //Use this for SQL-mode. If is_key = true: input is of type encoded key. If false: input is of type query statement
  virtual uint64_t operator()(const std::string &table_name, const std::string &input, uint64_t numShards, int group, const std::vector<int> &txnGroups, bool is_key = false) = 0;
};

class DefaultPartitioner : public Partitioner {
 public:
  DefaultPartitioner() {}
  virtual ~DefaultPartitioner() {}

  virtual uint64_t operator()(const std::string &key, uint64_t numShards,
      int group, const std::vector<int> &txnGroups);

  inline uint64_t operator()(const std::string &table_name, const std::string &input, uint64_t nshards,
    int group, const std::vector<int> &txnGroups, bool is_key) {Panic("Invoking SQL partitioner in KV-mode.");}
 private:
  std::hash<std::string> hash;
};

class WarehouseDistItemsPartitioner : public Partitioner {
 public:
  WarehouseDistItemsPartitioner(uint64_t numWarehouses) : numWarehouses(numWarehouses) {}
  virtual ~WarehouseDistItemsPartitioner() {}
  virtual uint64_t operator()(const std::string &key, uint64_t numShards,
      int group, const std::vector<int> &txnGroups);

  inline uint64_t operator()(const std::string &table_name, const std::string &input, uint64_t nshards,
    int group, const std::vector<int> &txnGroups, bool is_key) {Panic("Invoking SQL partitioner in KV-mode.");}

 private:
  const uint64_t numWarehouses;
};

class WarehousePartitioner : public Partitioner {
 public:
  WarehousePartitioner(uint64_t numWarehouses, std::mt19937 &rd) :
      numWarehouses(numWarehouses), rd(rd) {}
  virtual ~WarehousePartitioner() {}

  virtual uint64_t operator()(const std::string &key, uint64_t numShards,
      int group, const std::vector<int> &txnGroups);

  inline uint64_t operator()(const std::string &table_name, const std::string &input, uint64_t nshards,
    int group, const std::vector<int> &txnGroups, bool is_key) {Panic("Invoking SQL partitioner in KV-mode.");}

 private:
  const uint64_t numWarehouses;
  std::mt19937 &rd;
};



typedef std::function<uint64_t(const std::string &, uint64_t, int, const std::vector<int> &)> partitioner;

extern partitioner default_partitioner;
extern partitioner warehouse_partitioner;

partitioner warehouse_district_partitioner_dist_items(uint64_t num_warehouses);
partitioner warehouse_district_partitioner(uint64_t num_warehouses, std::mt19937 &rd);


////// SQL PARTITIONERS

class DefaultSQLPartitioner : public Partitioner {
 public:
  DefaultSQLPartitioner() {}
  virtual ~DefaultSQLPartitioner() {}

  inline uint64_t operator()(const std::string &key, uint64_t nshards,
    int group, const std::vector<int> &txnGroups) {Panic("Invoking KV partitioner in SQL-mode.");}

  //No sharding by default. (If more than 1 group is supplied, everything is stored and routed to the first group regardless)
  inline uint64_t operator()(const std::string &table_name, const std::string &input, uint64_t numShards,
      int group, const std::vector<int> &txnGroups, bool is_key){
        return 0;
      }
};

class RWSQLPartitioner : public Partitioner {
 public:
  RWSQLPartitioner(uint64_t numTables): numTables(numTables) {}
  virtual ~RWSQLPartitioner() {}

  inline uint64_t operator()(const std::string &key, uint64_t nshards,
    int group, const std::vector<int> &txnGroups) {Panic("Invoking KV partitioner in SQL-mode.");}

  //Partition by Table.
  virtual uint64_t operator()(const std::string &table_name, const std::string &input, uint64_t numShards,
      int group, const std::vector<int> &txnGroups, bool is_key = false);
 private:
  const uint64_t numTables;
  std::hash<std::string> hash;
};



class WarehouseSQLPartitioner : public Partitioner {
  enum TPCC_Table {
    WAREHOUSE, 
    DISTRICT, 
    ITEM,
    CUSTOMER,
    STOCK, 
    HISTORY,
    NEW_ORDER,
    ORDER,
    ORDER_LINE,
    EARLIEST_NEW_ORDER
  };
 public:
  WarehouseSQLPartitioner(uint64_t numWarehouses, std::mt19937 &rd) :
      numWarehouses(numWarehouses), rd(rd) {
        tables["warehouse"] = TPCC_Table::WAREHOUSE;
        tables["district"] = TPCC_Table::DISTRICT;
        tables["item"] = TPCC_Table::ITEM;
        tables["customer"] = TPCC_Table::CUSTOMER;
        tables["stock"] = TPCC_Table::STOCK;
        tables["history"] = TPCC_Table::HISTORY;
        tables["new_order"] = TPCC_Table::NEW_ORDER;
        tables["order"] = TPCC_Table::ORDER;
        tables["order_line"] = TPCC_Table::ORDER_LINE;
        tables["EarliestNewOrder"] = TPCC_Table::EARLIEST_NEW_ORDER;
      }
  virtual ~WarehouseSQLPartitioner() {}

  inline uint64_t operator()(const std::string &key, uint64_t nshards,
    int group, const std::vector<int> &txnGroups) {Panic("Invoking KV partitioner in SQL-mode.");}

  //Partition by Warehouse
  virtual uint64_t operator()(const std::string &table_name, const std::string &input, uint64_t numShards,
      int group, const std::vector<int> &txnGroups, bool is_key = false);

 private:
  std::map<std::string, TPCC_Table> tables;
  const uint64_t numWarehouses;
  std::mt19937 &rd;
};




#endif /* PARTITIONER_H */
