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
#include <condition_variable>
#include <mutex>
#include <queue>
#include <random>
#include <string>
#include <utility>
#include <numeric>
#include <algorithm>

#include <gflags/gflags.h>

#include "lib/io_utils.h"
#include "store/benchmark/async/tpcc/tpcc_utils.h"
#include "store/benchmark/async/tpcc/tpcc-proto.pb.h"

#include "store/benchmark/async/json_table_writer.h"

const char ALPHA_NUMERIC[] = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

std::string RandomAString(size_t x, size_t y, std::mt19937 &gen) {
  std::string s;
  size_t length = std::uniform_int_distribution<size_t>(x,  y)(gen);
  for (size_t i = 0; i < length; ++i) {
    int j = std::uniform_int_distribution<size_t>(0, sizeof(ALPHA_NUMERIC))(gen);
    s += ALPHA_NUMERIC[j];
  }
  return s;
}

std::string RandomNString(size_t x, size_t y, std::mt19937 &gen) {
  std::string s;
  size_t length = std::uniform_int_distribution<size_t>(x,  y)(gen);
  for (size_t i = 0; i < length; ++i) {
    int j = std::uniform_int_distribution<size_t>(0, 9)(gen);
    s += ALPHA_NUMERIC[j];
  }
  return s;
}

std::string RandomZip(std::mt19937 &gen) {
  return RandomNString(4, 4, gen) + "11111";
}

const char ORIGINAL_CHARS[] = "ORIGINAL";

void GenerateItemTable(TableWriter writer) {
  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("id", "integer"));
  column_names_and_types.push_back(std::make_pair("im_id", "integer"));
  column_names_and_types.push_back(std::make_pair("name", "varchar(24)"));
  column_names_and_types.push_back(std::make_pair("price", "integer"));
  column_names_and_types.push_back(std::make_pair("data", "varchar(50)"));
  const std::vector<uint32_t> primary_key_col_idx {0};

  std::string table_name = "Item";
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);
  std::mt19937 gen;
  for (uint32_t i_id = 1; i_id <= 100000; ++i_id) {
    std::vector<std::string> values;
    values.push_back(std::to_string(i_id));
    values.push_back(std::to_string(std::uniform_int_distribution<uint32_t>(1, 10000)(gen)));
    values.push_back(RandomAString(14, 24, gen));
    values.push_back(std::to_string(std::uniform_int_distribution<uint32_t>(100, 10000)(gen)));
    std::string data = RandomAString(26, 50, gen);
    int original = std::uniform_int_distribution<uint32_t>(1, 10)(gen);
    if (original == 1) {
      int startIdx = std::uniform_int_distribution<int>(0, data.length() - sizeof(ORIGINAL_CHARS))(gen);
      data.replace(startIdx, sizeof(ORIGINAL_CHARS), ORIGINAL_CHARS);
    }
    values.push_back(data);
    writer.add_row(table_name, values);
  }
}

void GenerateWarehouseTable(uint32_t num_warehouses, TableWriter &writer) {
  std::mt19937 gen;
  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("id", "integer"));
  column_names_and_types.push_back(std::make_pair("name", "varchar(10)"));
  column_names_and_types.push_back(std::make_pair("street_1", "varchar(20)"));
  column_names_and_types.push_back(std::make_pair("street_2", "varchar(20)"));
  column_names_and_types.push_back(std::make_pair("city", "varchar(20)"));
  column_names_and_types.push_back(std::make_pair("state", "varchar(2)"));
  column_names_and_types.push_back(std::make_pair("zip", "varchar(9)"));
  column_names_and_types.push_back(std::make_pair("tax", "integer"));
  column_names_and_types.push_back(std::make_pair("ytd", "integer"));
  const std::vector<uint32_t> primary_key_col_idx {0};

  std::string table_name = "Warehouse";
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);
  for (uint32_t w_id = 1; w_id <= num_warehouses; ++w_id) {
    std::vector<std::string> values;
    values.push_back(std::to_string(w_id));
    values.push_back(RandomAString(6, 10, gen));
    values.push_back(RandomAString(10, 20, gen));
    values.push_back(RandomAString(10, 20, gen));
    values.push_back(RandomAString(10, 20, gen));
    values.push_back(RandomAString(2, 2, gen));
    values.push_back(RandomZip(gen));
    values.push_back(std::to_string(std::uniform_int_distribution<uint32_t>(0, 2000)(gen)));
    values.push_back(std::to_string(30000000));
    
    writer.add_row(table_name, values);
  }
}

void GenerateStockTableForWarehouse(uint32_t w_id, TableWriter &writer) {
  std::mt19937 gen;
  std::string table_name = "Stock";

  for (uint32_t s_i_id = 1; s_i_id <= 100000; ++s_i_id) {
    std::vector<std::string> values;
    values.push_back(std::to_string(s_i_id));
    values.push_back(std::to_string(w_id));
    values.push_back(std::to_string(std::uniform_int_distribution<uint32_t>(10, 100)(gen)));
    for(int i = 0; i < 10; i++) {
      values.push_back(RandomAString(24, 24, gen));
    }
    for(int i = 0; i < 3; i++) {
      values.push_back(std::to_string(0));
    }
    
    std::string data = RandomAString(26, 50, gen);
    int original = std::uniform_int_distribution<uint32_t>(1, 10)(gen);
    if (original == 1) {
      int startIdx = std::uniform_int_distribution<int>(0, data.length() - sizeof(ORIGINAL_CHARS))(gen);
      data.replace(startIdx, sizeof(ORIGINAL_CHARS), ORIGINAL_CHARS);
    }
    values.push_back(data);
    writer.add_row(table_name, values);
  }
}

void GenerateStockTable(uint32_t num_warehouses, TableWriter &writer) {
  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("i_id", "integer"));
  column_names_and_types.push_back(std::make_pair("w_id", "integer"));
  column_names_and_types.push_back(std::make_pair("quantity", "integer"));
  column_names_and_types.push_back(std::make_pair("dist_01", "varchar(24)"));
  column_names_and_types.push_back(std::make_pair("dist_02", "varchar(24)"));
  column_names_and_types.push_back(std::make_pair("dist_03", "varchar(24)"));
  column_names_and_types.push_back(std::make_pair("dist_04", "varchar(24)"));
  column_names_and_types.push_back(std::make_pair("dist_05", "varchar(24)"));
  column_names_and_types.push_back(std::make_pair("dist_06", "varchar(24)"));
  column_names_and_types.push_back(std::make_pair("dist_07", "varchar(24)"));
  column_names_and_types.push_back(std::make_pair("dist_08", "varchar(24)"));
  column_names_and_types.push_back(std::make_pair("dist_09", "varchar(24)"));
  column_names_and_types.push_back(std::make_pair("dist_10", "varchar(24)"));
  column_names_and_types.push_back(std::make_pair("ytd", "integer"));
  column_names_and_types.push_back(std::make_pair("order_cnt", "integer"));
  column_names_and_types.push_back(std::make_pair("remote_cnt", "integer"));
  column_names_and_types.push_back(std::make_pair("data", "varchar(50)"));
  const std::vector<uint32_t> primary_key_col_idx {0, 1};
  writer.add_table("Stock", column_names_and_types, primary_key_col_idx);

  for (uint32_t w_id = 1; w_id <= num_warehouses; ++w_id) {
    GenerateStockTableForWarehouse(w_id, writer);
  }
}

void GenerateDistrictTableForWarehouse(uint32_t w_id, TableWriter writer) {
  std::mt19937 gen;
  std::string table_name = "District";
  
  for (uint32_t d_id = 1; d_id <= 10; ++d_id) {
    std::vector<std::string> values;
    values.push_back(std::to_string(d_id));
    values.push_back(std::to_string(w_id));
    values.push_back(RandomAString(6, 10, gen));
    for(int i = 0; i < 3; i++) {
      values.push_back(RandomAString(10, 20, gen));
    }
    values.push_back(RandomAString(2, 2, gen));
    values.push_back(RandomZip(gen));
    values.push_back(std::to_string(std::uniform_int_distribution<uint32_t>(0, 2000)(gen)));
    values.push_back(std::to_string(3000000));
    values.push_back(std::to_string(3001));
    writer.add_row(table_name, values);
  }
}

void GenerateDistrictTable(uint32_t num_warehouses, TableWriter writer) {
  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("id", "integer"));
  column_names_and_types.push_back(std::make_pair("w_id", "integer"));
  column_names_and_types.push_back(std::make_pair("name", "varchar(10)"));
  column_names_and_types.push_back(std::make_pair("street_1", "varchar(20)"));
  column_names_and_types.push_back(std::make_pair("street_2", "varchar(20)"));
  column_names_and_types.push_back(std::make_pair("city", "varchar(20)"));
  column_names_and_types.push_back(std::make_pair("state", "varchar(2)"));
  column_names_and_types.push_back(std::make_pair("zip", "varchar(9)"));
  column_names_and_types.push_back(std::make_pair("tax", "integer"));
  column_names_and_types.push_back(std::make_pair("ytd", "integer"));
  column_names_and_types.push_back(std::make_pair("next_o_id", "integer"));
  const std::vector<uint32_t> primary_key_col_idx {0, 1};
  writer.add_table("District", column_names_and_types, primary_key_col_idx);

  for (uint32_t w_id = 1; w_id <= num_warehouses; ++w_id) {
    GenerateDistrictTableForWarehouse(w_id, writer);
  }
}

void GenerateCustomerTableForWarehouseDistrict(uint32_t w_id, uint32_t d_id,
    uint32_t time, uint32_t c_last, TableWriter writer) {
  std::mt19937 gen;
  std::string table_name = "Customer";

  std::map<std::string, std::set<std::pair<std::string, uint32_t>>> custWithLast;
  for (uint32_t c_id = 1; c_id <= 3000; ++c_id) {
    std::vector<std::string> values;
    values.push_back(std::to_string(c_id));
    values.push_back(std::to_string(d_id));
    values.push_back(std::to_string(w_id));

    int last;
    if (c_id <= 1000) {
      last = c_id - 1;
    } else {
      last = tpcc::NURand(255, 0, 999, static_cast<int>(c_last), gen);
    }
    std::string first_name = RandomAString(8, 16, gen);
    values.push_back(first_name);
    values.push_back("OE");
    std::string last_name = tpcc::GenerateCustomerLastName(last);
    values.push_back(last_name);
    custWithLast[last_name].insert(std::make_pair(first_name, c_id));
    for(int i = 0; i < 3; i++) {
      values.push_back(RandomAString(10, 20, gen));
    }
    values.push_back(RandomAString(2, 2, gen));
    values.push_back(RandomZip(gen));
    values.push_back(RandomNString(16, 16, gen));
    values.push_back(std::to_string(time));

    int credit = std::uniform_int_distribution<int>(1, 10)(gen);
    if (credit == 1) {
      values.push_back("BC");
    } else {
      values.push_back("GC");
    }
    values.push_back(std::to_string(5000000));
    values.push_back(std::to_string(std::uniform_int_distribution<int>(0, 5000)(gen)));
    values.push_back(std::to_string(-1000));
    values.push_back(std::to_string(1000));
    values.push_back(std::to_string(1));
    values.push_back(std::to_string(0));
    values.push_back(std::to_string(RandomAString(300, 500, gen)));
    writer.add_row(table_name, values);
  }

  std::string table_name = "CustomerByName";
  for (auto cwl : custWithLast) {
    std::vector<std::string> values;
    values.push_back(std::string(w_id));
    values.push_back(std::string(d_id));
    values.push_back(cwl.first);
    for (auto id : cwl.second) {
      cbn_row.add_ids(id.second);
    }
    cbn_row.SerializeToString(&cbn_row_out);
    std::string cbn_key = tpcc::CustomerByNameRowKey(w_id, d_id, cwl.first);
    q.Push(std::make_pair(cbn_key, cbn_row_out));
  }
}

void GenerateCustomerTable(uint32_t num_warehouses, uint32_t c_load_c_last,
    uint32_t time, Queue<std::pair<std::string, std::string>> &q) {
  std::mt19937 gen;
  for (uint32_t w_id = 1; w_id <= num_warehouses; ++w_id) {
    for (uint32_t d_id = 1; d_id <= 10; ++d_id) {
      GenerateCustomerTableForWarehouseDistrict(w_id, d_id, time, c_load_c_last,
          q);
    }
  }
}

void GenerateHistoryTable(uint32_t num_warehouses,
    Queue<std::pair<std::string, std::string>> &q) {
  std::mt19937 gen;
  tpcc::HistoryRow h_row;
  std::string h_row_out;
  for (uint32_t w_id = 1; w_id <= num_warehouses; ++w_id) {
    for (uint32_t d_id = 1; d_id <= 10; ++d_id) {
      for (uint32_t c_id = 1; c_id <= 3000; ++c_id) {
        h_row.set_c_id(c_id);
        h_row.set_d_id(d_id);
        h_row.set_w_id(w_id);
        h_row.set_date(std::time(0));
        h_row.set_amount(1000);
        h_row.set_data(RandomAString(12, 24, gen));
        h_row.SerializeToString(&h_row_out);
        std::string h_key = tpcc::HistoryRowKey(w_id, d_id, c_id);
        q.Push(std::make_pair(h_key, h_row_out));
      }
    }
  }
}

void GenerateOrderTableForWarehouseDistrict(uint32_t w_id, uint32_t d_id,
    uint32_t c_load_ol_i_id, Queue<std::pair<std::string, std::string>> &q) {
  std::mt19937 gen;
  tpcc::OrderRow o_row;
  std::string o_row_out;
  tpcc::OrderLineRow ol_row;
  std::string ol_row_out;
  tpcc::OrderByCustomerRow obc_row;
  std::string obc_row_out;
  std::vector<uint32_t> c_ids(3000);
  std::iota(c_ids.begin(), c_ids.end(), 1);
  std::shuffle(c_ids.begin(), c_ids.end(), gen);
  for (uint32_t i = 0; i < 3000; ++i) {
    uint32_t c_id = c_ids[i];
    uint32_t o_id = i + 1;
    o_row.set_id(o_id);
    o_row.set_c_id(c_id);
    o_row.set_d_id(d_id);
    o_row.set_w_id(w_id);
    o_row.set_entry_d(std::time(0));
    if (o_id < 2101) {
      o_row.set_carrier_id(std::uniform_int_distribution<uint32_t>(1, 10)(gen));
    } else {
      o_row.set_carrier_id(0);
    }
    o_row.set_ol_cnt(std::uniform_int_distribution<uint32_t>(5, 15)(gen));
    o_row.set_all_local(true);
    o_row.SerializeToString(&o_row_out);
    std::string o_key = tpcc::OrderRowKey(w_id, d_id, o_id);
    q.Push(std::make_pair(o_key, o_row_out));    
    
    // initially, there is exactly one order per customer, so we do not need to
    // worry about writing multiple OrderByCustomerRow with the same key.
    obc_row.set_w_id(w_id);
    obc_row.set_d_id(d_id);
    obc_row.set_c_id(c_id);
    obc_row.set_o_id(o_id);
    obc_row.SerializeToString(&obc_row_out);
    std::string obc_key = tpcc::OrderByCustomerRowKey(w_id, d_id, c_id);
    q.Push(std::make_pair(obc_key, obc_row_out));
    for (uint32_t ol_number = 0; ol_number < o_row.ol_cnt(); ++ol_number) {
      ol_row.set_o_id(o_id);
      ol_row.set_d_id(d_id);
      ol_row.set_w_id(w_id);
      ol_row.set_number(ol_number);
      ol_row.set_i_id(tpcc::NURand(8191, 1, 100000,
          static_cast<int>(c_load_ol_i_id), gen));
      ol_row.set_supply_w_id(w_id);
      if (o_id < 2101) {
        ol_row.set_delivery_d(o_row.entry_d());
      } else {
        ol_row.set_delivery_d(0);
      }
      ol_row.set_quantity(5);
      if (o_id < 2101) {
        ol_row.set_amount(0);
      } else {
        ol_row.set_amount(std::uniform_int_distribution<uint32_t>(1, 999999)(gen));
      }
      ol_row.set_dist_info(RandomAString(24, 24, gen));
      ol_row.SerializeToString(&ol_row_out);
      std::string ol_key = tpcc::OrderLineRowKey(w_id, d_id, o_id, ol_number);
      q.Push(std::make_pair(ol_key, ol_row_out));
    }
  }
}

void GenerateOrderTable(uint32_t num_warehouses, uint32_t c_load_ol_i_id,
    Queue<std::pair<std::string, std::string>> &q) {
  for (uint32_t w_id = 1; w_id <= num_warehouses; ++w_id) {
    for (uint32_t d_id = 1; d_id <= 10; ++d_id) {
      GenerateOrderTableForWarehouseDistrict(w_id, d_id, c_load_ol_i_id, q);
    }
  }
}

void GenerateNewOrderTableForWarehouseDistrict(uint32_t w_id, uint32_t d_id,
    Queue<std::pair<std::string, std::string>> &q) {
  std::mt19937 gen;
  tpcc::NewOrderRow no_row;
  std::string no_row_out;
  for (uint32_t o_id = 2101; o_id <= 3000; ++o_id) {
    no_row.set_o_id(o_id);
    no_row.set_d_id(d_id);
    no_row.set_w_id(w_id);
    no_row.SerializeToString(&no_row_out);
    std::string no_key = tpcc::NewOrderRowKey(w_id, d_id, o_id);
    q.Push(std::make_pair(no_key, no_row_out));
  }

  tpcc::EarliestNewOrderRow eno_row;
  eno_row.set_w_id(w_id);
  eno_row.set_d_id(d_id);
  eno_row.set_o_id(2101UL);
  std::string eno_row_out;
  eno_row.SerializeToString(&eno_row_out);
  q.Push(std::make_pair(tpcc::EarliestNewOrderRowKey(w_id, d_id), eno_row_out));
}

void GenerateNewOrderTable(uint32_t num_warehouses,
    Queue<std::pair<std::string, std::string>> &q) {
  for (uint32_t w_id = 1; w_id <= num_warehouses; ++w_id) {
    for (uint32_t d_id = 1; d_id <= 10; ++d_id) {
      GenerateNewOrderTableForWarehouseDistrict(w_id, d_id, q);
    }
  }
}


DEFINE_int32(c_load_c_last, 0, "Run-time constant C used for generating C_LAST.");
//DEFINE_int32(c_load_c_id, 0, "Run-time constant C used for generating C_ID.");
DEFINE_int32(c_load_ol_i_id, 0, "Run-time constant C used for generating OL_I_ID.");
DEFINE_int32(num_warehouses, 1, "number of warehouses");
int main(int argc, char *argv[]) {
  gflags::SetUsageMessage(
           "generates a json file containing sql tables for TPC-C data\n");
	gflags::ParseCommandLineFlags(&argc, &argv, true);

  TableWriter writer = TableWriter();
  Queue<std::pair<std::string, std::string>> q(2e9);
  uint32_t time = std::time(0);
  std::cerr << "Generating " << FLAGS_num_warehouses << " warehouses." << std::endl;
  GenerateItemTable(writer);
  GenerateWarehouseTable(FLAGS_num_warehouses, q);
  GenerateStockTable(FLAGS_num_warehouses, q);
  GenerateDistrictTable(FLAGS_num_warehouses, q);
  GenerateCustomerTable(FLAGS_num_warehouses, FLAGS_c_load_c_last, time, q);
  GenerateHistoryTable(FLAGS_num_warehouses, q);
  GenerateOrderTable(FLAGS_num_warehouses, FLAGS_c_load_ol_i_id, q);
  GenerateNewOrderTable(FLAGS_num_warehouses, q);

  writer.flush();
  std::cerr << "Wrote tables." << std::endl;
  return 0;
}
