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
#include "store/benchmark/async/sql/tpcc/tpcc_utils.h"
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

void GenerateItemTable(TableWriter &writer) {
  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("id", "INT"));
  column_names_and_types.push_back(std::make_pair("im_id", "INT"));
  column_names_and_types.push_back(std::make_pair("name", "TEXT"));
  column_names_and_types.push_back(std::make_pair("price", "INT"));
  column_names_and_types.push_back(std::make_pair("data", "TEXT"));
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
  column_names_and_types.push_back(std::make_pair("id", "INT"));
  column_names_and_types.push_back(std::make_pair("name", "TEXT"));
  column_names_and_types.push_back(std::make_pair("street_1", "TEXT"));
  column_names_and_types.push_back(std::make_pair("street_2", "TEXT"));
  column_names_and_types.push_back(std::make_pair("city", "TEXT"));
  column_names_and_types.push_back(std::make_pair("state", "TEXT"));
  column_names_and_types.push_back(std::make_pair("zip", "TEXT"));
  column_names_and_types.push_back(std::make_pair("tax", "INT"));
  column_names_and_types.push_back(std::make_pair("ytd", "INT"));
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
  std::string table_name = "Stock";
  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("i_id", "INT"));
  column_names_and_types.push_back(std::make_pair("w_id", "INT"));
  column_names_and_types.push_back(std::make_pair("quantity", "INT"));
  column_names_and_types.push_back(std::make_pair("dist_01", "TEXT"));
  column_names_and_types.push_back(std::make_pair("dist_02", "TEXT"));
  column_names_and_types.push_back(std::make_pair("dist_03", "TEXT"));
  column_names_and_types.push_back(std::make_pair("dist_04", "TEXT"));
  column_names_and_types.push_back(std::make_pair("dist_05", "TEXT"));
  column_names_and_types.push_back(std::make_pair("dist_06", "TEXT"));
  column_names_and_types.push_back(std::make_pair("dist_07", "TEXT"));
  column_names_and_types.push_back(std::make_pair("dist_08", "TEXT"));
  column_names_and_types.push_back(std::make_pair("dist_09", "TEXT"));
  column_names_and_types.push_back(std::make_pair("dist_10", "TEXT"));
  column_names_and_types.push_back(std::make_pair("ytd", "INT"));
  column_names_and_types.push_back(std::make_pair("order_cnt", "INT"));
  column_names_and_types.push_back(std::make_pair("remote_cnt", "INT"));
  column_names_and_types.push_back(std::make_pair("data", "TEXT"));
  const std::vector<uint32_t> primary_key_col_idx {0, 1};
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);

  for (uint32_t w_id = 1; w_id <= num_warehouses; ++w_id) {
    GenerateStockTableForWarehouse(w_id, writer);
  }
}

void GenerateDistrictTableForWarehouse(uint32_t w_id, TableWriter &writer) {
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

void GenerateDistrictTable(uint32_t num_warehouses, TableWriter &writer) {
  std::string table_name = "District";
  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("id", "INT"));
  column_names_and_types.push_back(std::make_pair("w_id", "INT"));
  column_names_and_types.push_back(std::make_pair("name", "TEXT"));
  column_names_and_types.push_back(std::make_pair("street_1", "TEXT"));
  column_names_and_types.push_back(std::make_pair("street_2", "TEXT"));
  column_names_and_types.push_back(std::make_pair("city", "TEXT"));
  column_names_and_types.push_back(std::make_pair("state", "TEXT"));
  column_names_and_types.push_back(std::make_pair("zip", "TEXT"));
  column_names_and_types.push_back(std::make_pair("tax", "INT"));
  column_names_and_types.push_back(std::make_pair("ytd", "INT"));
  column_names_and_types.push_back(std::make_pair("next_o_id", "INT"));
  const std::vector<uint32_t> primary_key_col_idx {0, 1};
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);

  for (uint32_t w_id = 1; w_id <= num_warehouses; ++w_id) {
    GenerateDistrictTableForWarehouse(w_id, writer);
  }
}

void GenerateCustomerTableForWarehouseDistrict(uint32_t w_id, uint32_t d_id,
    uint32_t time, uint32_t c_last, TableWriter &writer) {
  std::mt19937 gen;
  std::string table_name = "Customer";

  for (uint32_t c_id = 1; c_id <= 3000; ++c_id) {
    std::vector<std::string> values;
    values.push_back(std::to_string(c_id));
    values.push_back(std::to_string(d_id));
    values.push_back(std::to_string(w_id));

    int last;
    if (c_id <= 1000) {
      last = c_id - 1;
    } else {
      last = tpcc_sql::NURand(255, 0, 999, static_cast<int>(c_last), gen);
    }
    std::string first_name = RandomAString(8, 16, gen);
    values.push_back(first_name);
    values.push_back("OE");
    std::string last_name = tpcc_sql::GenerateCustomerLastName(last);
    values.push_back(last_name);
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
    values.push_back(RandomAString(300, 500, gen));
    writer.add_row(table_name, values);
  }
}

void GenerateCustomerTable(uint32_t num_warehouses, uint32_t c_load_c_last,
    uint32_t time, TableWriter &writer) {
  std::mt19937 gen;
  for (uint32_t w_id = 1; w_id <= num_warehouses; ++w_id) {
    for (uint32_t d_id = 1; d_id <= 10; ++d_id) {
      GenerateCustomerTableForWarehouseDistrict(w_id, d_id, time, c_load_c_last,
          writer);
    }
  }
}

void GenerateHistoryTable(uint32_t num_warehouses,
    TableWriter &writer) {
  std::mt19937 gen;
  std::string table_name = "History";
  std::vector<std::pair<std::string, std::string>> column_names_and_types;

  column_names_and_types.push_back(std::make_pair("c_id", "INT"));
  column_names_and_types.push_back(std::make_pair("c_d_id", "INT"));
  column_names_and_types.push_back(std::make_pair("c_w_id", "INT"));
  column_names_and_types.push_back(std::make_pair("d_id", "INT"));
  column_names_and_types.push_back(std::make_pair("w_id", "INT"));
  column_names_and_types.push_back(std::make_pair("date", "INT"));
  column_names_and_types.push_back(std::make_pair("amount", "INT"));
  column_names_and_types.push_back(std::make_pair("data", "TEXT"));

  const std::vector<uint32_t> primary_key_col_idx {0, 1, 2};
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);

  for (uint32_t w_id = 1; w_id <= num_warehouses; ++w_id) {
    for (uint32_t d_id = 1; d_id <= 10; ++d_id) {
      for (uint32_t c_id = 1; c_id <= 3000; ++c_id) {
        std::vector<std::string> values;
        values.push_back(std::to_string(c_id));
        values.push_back(std::to_string(0));
        values.push_back(std::to_string(0));
        values.push_back(std::to_string(d_id));
        values.push_back(std::to_string(w_id));
        values.push_back(std::to_string(std::time(0)));
        values.push_back(std::to_string(1000));
        values.push_back(RandomAString(12, 24, gen));
        
        writer.add_row(table_name, values);
      }
    }
  }
}

void GenerateOrderTableForWarehouseDistrict(uint32_t w_id, uint32_t d_id,
    uint32_t c_load_ol_i_id, TableWriter &writer) {
  std::mt19937 gen;
  std::vector<uint32_t> c_ids(3000);
  std::iota(c_ids.begin(), c_ids.end(), 1);
  std::shuffle(c_ids.begin(), c_ids.end(), gen);
  std::string table_name;
  for (uint32_t i = 0; i < 3000; ++i) {
    table_name = "Order";
    uint32_t c_id = c_ids[i];
    uint32_t o_id = i + 1;
    std::vector<std::string> values;
    values.push_back(std::to_string(o_id));
    values.push_back(std::to_string(d_id));
    values.push_back(std::to_string(w_id));
    values.push_back(std::to_string(c_id));
    values.push_back(std::to_string(std::time(0)));
    if (o_id < 2101) {
      values.push_back(std::to_string(std::uniform_int_distribution<uint32_t>(1, 10)(gen)));
    } else {
      values.push_back(std::to_string(0));
    }
    uint32_t ol_cnt = std::uniform_int_distribution<uint32_t>(5, 15)(gen);
    values.push_back(std::to_string(ol_cnt));
    values.push_back(std::to_string(true));
    writer.add_row(table_name, values);
    values.clear();
    
    table_name = "OrderLine";
    for (uint32_t ol_number = 0; ol_number < ol_cnt; ++ol_number) {
      values.push_back(std::to_string(o_id));
      values.push_back(std::to_string(d_id));
      values.push_back(std::to_string(w_id));
      values.push_back(std::to_string(ol_number));
      values.push_back(std::to_string(tpcc_sql::NURand(8191, 1, 100000,
          static_cast<int>(c_load_ol_i_id), gen)));
      values.push_back(std::to_string(w_id));
      values.push_back(std::to_string(std::time(0)));
      values.push_back(std::to_string(5));
      if (o_id < 2101) {
        values.push_back(std::to_string(0));
      } else {
        values.push_back(std::to_string(std::uniform_int_distribution<uint32_t>(1, 999999)(gen)));
      }
      values.push_back(RandomAString(24, 24, gen));
      writer.add_row(table_name, values);
      values.clear();
    }
  }
}

void GenerateOrderTable(uint32_t num_warehouses, uint32_t c_load_ol_i_id,
    TableWriter &writer) {
  std::string table_name = "Order";
  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("id", "INTEGER"));
  column_names_and_types.push_back(std::make_pair("d_id", "INTEGER"));
  column_names_and_types.push_back(std::make_pair("w_id", "INTEGER"));
  column_names_and_types.push_back(std::make_pair("c_id", "INTEGER"));
  column_names_and_types.push_back(std::make_pair("entry_d", "INTEGER"));
  column_names_and_types.push_back(std::make_pair("carrier_id", "INTEGER"));
  column_names_and_types.push_back(std::make_pair("ol_cnt", "INTEGER"));
  column_names_and_types.push_back(std::make_pair("all_local", "BOOLEAN"));
  std::vector<uint32_t> primary_key_col_idx {0, 1, 2};
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);

  table_name = "OrderLine";
  column_names_and_types.clear();
  column_names_and_types.push_back(std::make_pair("o_id", "INTEGER"));
  column_names_and_types.push_back(std::make_pair("d_id", "INTEGER"));
  column_names_and_types.push_back(std::make_pair("w_id", "INTEGER"));
  column_names_and_types.push_back(std::make_pair("number", "INTEGER"));
  column_names_and_types.push_back(std::make_pair("i_id", "INTEGER"));
  column_names_and_types.push_back(std::make_pair("supply_w_id", "INTEGER"));
  column_names_and_types.push_back(std::make_pair("delivery_d", "INTEGER"));
  column_names_and_types.push_back(std::make_pair("quantity", "INTEGER"));
  column_names_and_types.push_back(std::make_pair("amount", "INTEGER"));
  column_names_and_types.push_back(std::make_pair("dist_info", "VARCHAR(24)"));
  std::vector<uint32_t> primary_key_col_idx_order_line {0, 1, 2, 3};
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx_order_line);

  for (uint32_t w_id = 1; w_id <= num_warehouses; ++w_id) {
    for (uint32_t d_id = 1; d_id <= 10; ++d_id) {
      GenerateOrderTableForWarehouseDistrict(w_id, d_id, c_load_ol_i_id, writer);
    }
  }
}

void GenerateNewOrderTableForWarehouseDistrict(uint32_t w_id, uint32_t d_id,
    TableWriter &writer) {
  std::mt19937 gen;
  std::string table_name = "NewOrder";
  std::vector<std::string> values;

  for (uint32_t o_id = 2101; o_id <= 3000; ++o_id) {
    values.push_back(std::to_string(o_id));
    values.push_back(std::to_string(d_id));
    values.push_back(std::to_string(w_id));
    writer.add_row(table_name, values);
    values.clear();
  }
}

void GenerateNewOrderTable(uint32_t num_warehouses,
    TableWriter &writer) {
  std::string table_name = "NewOrder";
  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("o_id", "INTEGER"));
  column_names_and_types.push_back(std::make_pair("d_id", "INTEGER"));
  column_names_and_types.push_back(std::make_pair("w_id", "INTEGER"));
  const std::vector<uint32_t> primary_key_col_idx {0, 1, 2};
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);

  for (uint32_t w_id = 1; w_id <= num_warehouses; ++w_id) {
    for (uint32_t d_id = 1; d_id <= 10; ++d_id) {
      GenerateNewOrderTableForWarehouseDistrict(w_id, d_id, writer);
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
  std::string file_name = "tpcc-data";
  TableWriter writer = TableWriter(file_name);
  uint32_t time = std::time(0);
  std::cerr << "Generating " << FLAGS_num_warehouses << " warehouses." << std::endl;
  GenerateItemTable(writer);
  GenerateWarehouseTable(FLAGS_num_warehouses, writer);
  GenerateStockTable(FLAGS_num_warehouses, writer);
  GenerateDistrictTable(FLAGS_num_warehouses, writer);
  GenerateCustomerTable(FLAGS_num_warehouses, FLAGS_c_load_c_last, time, writer);
  GenerateHistoryTable(FLAGS_num_warehouses, writer);
  GenerateOrderTable(FLAGS_num_warehouses, FLAGS_c_load_ol_i_id, writer);
  GenerateNewOrderTable(FLAGS_num_warehouses, writer);

  writer.flush();
  std::cerr << "Wrote tables." << std::endl;
  return 0;
}
