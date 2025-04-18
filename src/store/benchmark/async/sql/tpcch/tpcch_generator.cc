#include "store/benchmark/async/json_table_writer.h"
#include "store/benchmark/async/sql/tpcch/tpcch_constants.h"
#include <gflags/gflags.h>
#include <random>
#include <sstream>
#include <iomanip>
#include <fstream>
#include <algorithm>


/** Note: Run sql_tpcc_generator as well to get all tables*/

const char* NATION_TABLE = "./store/benchmark/async/sql/tpcch/resources/nation_gen.tbl";
const char* REGION_TABLE = "./store/benchmark/async/sql/tpcch/resources/region_gen.tbl";

std::vector<std::string> readTBLRow(std::ifstream& iostr) {
  std::vector<std::string> res;

  std::string line; 
  std::getline(iostr, line);
  std::stringstream lineStream(line);
  std::string cell; 

  while (std::getline(lineStream, cell, '|')) {
    res.push_back(cell);
  }

  return res;
}

std::vector<int> GenerateNationTable(TableWriter &writer) {
  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("n_nationkey", "INT"));
  column_names_and_types.push_back(std::make_pair("n_name", "CHAR(25)"));
  column_names_and_types.push_back(std::make_pair("n_regionkey", "INT"));
  column_names_and_types.push_back(std::make_pair("n_comment", "CHAR(152)"));

  const std::vector<uint32_t> primary_key_col_idx {0};
  std::string table_name = "nation";
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);

  std::ifstream file (NATION_TABLE);
  std::vector<std::string> tbl;
  std::ostringstream ss;              // for adjusting size of tbl strings
  std::vector<int> nation_keys;
  for (int i = 0; i < 62; i++) {
    std::vector<std::string> values;
    tbl = readTBLRow(file);
    values.push_back(tbl[0]);
    nation_keys.push_back(stoi(tbl[0]));
    ss << std::setw(25) << tbl[1] << std::setfill(' ');
    values.push_back(ss.str());
    ss.str(std::string()); 
    ss.clear();
    values.push_back(tbl[2]);
    ss << std::setw(25) << tbl[3] << std::setfill(' ');
    values.push_back(ss.str());
    ss.str(std::string());
    ss.clear();

    writer.add_row(table_name, values);
  }
  file.close();
  return nation_keys;
}

void GenerateRegionTable(TableWriter &writer) {
  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("r_regionkey", "INT"));
  column_names_and_types.push_back(std::make_pair("r_name", "CHAR(55)"));
  column_names_and_types.push_back(std::make_pair("r_comment", "CHAR(152)"));
  const std::vector<uint32_t> primary_key_col_idx {0};
  std::string table_name = "region";
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);

  std::ifstream file (REGION_TABLE);
  std::vector<std::string> tbl;
  std::ostringstream ss;              // for adjusting size of tbl strings


  for (int i = 0; i < 5; i++) {
    std::vector<std::string> values;
    tbl = readTBLRow(file);
    values.push_back(tbl[0]);
    ss << std::setw(55) << tbl[1] << std::setfill(' ');
    values.push_back(ss.str());
    ss.str(std::string()); 
    ss.clear();
    ss << std::setw(152) << tbl[2] << std::setfill(' ');
    values.push_back(ss.str());
    ss.str(std::string()); 
    ss.clear();

    writer.add_row(table_name, values);
  }
}

const char ALPHA_NUMERIC[] = "0123456789abcdefghjijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

/** Generate random alpha-numeric string of length [min_len, max_len] */
std::string RandomANString(size_t min_len, size_t max_len, std::mt19937 &gen) {
  std::string s;
  size_t length = std::uniform_int_distribution<size_t>(min_len,  max_len)(gen);
  for (size_t i = 0; i < length; ++i) {
    int j = std::uniform_int_distribution<size_t>(0, sizeof(ALPHA_NUMERIC) - 2)(gen);
    s.push_back(ALPHA_NUMERIC[j]);
  }
  return s;
}

/** Generate random alphabetical string of length [min_len, max_len]*/
std::string RandomAString(size_t min_len, size_t max_len, std::mt19937 &gen) {
  std::string s;
  size_t length = std::uniform_int_distribution<size_t>(min_len,  max_len)(gen);
  for (size_t i = 0; i < length; ++i) {
    int j = std::uniform_int_distribution<size_t>(10, sizeof(ALPHA_NUMERIC) - 2)(gen);
    s += ALPHA_NUMERIC[j];
  }
  return s;
}


/** Generate random numerical string of length [min_len, max_len] */
std::string RandomNString(size_t min_len, size_t max_len, std::mt19937 &gen) {
  std::string s;
  size_t length = std::uniform_int_distribution<size_t>(min_len,  max_len)(gen);
  for (size_t i = 0; i < length; ++i) {
    int j = std::uniform_int_distribution<size_t>(0, 9)(gen);
    s += ALPHA_NUMERIC[j];
  }
  return s;
}

void GenerateSupplierTable(TableWriter &writer, std::vector<int> &nation_keys, int num_suppliers) {
  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("su_suppkey", "INT"));
  column_names_and_types.push_back(std::make_pair("su_name", "CHAR(25)"));
  column_names_and_types.push_back(std::make_pair("su_address", "VARCHAR(40)"));
  column_names_and_types.push_back(std::make_pair("su_nationkey", "INT"));
  column_names_and_types.push_back(std::make_pair("su_phone", "CHAR(15)"));
  column_names_and_types.push_back(std::make_pair("su_acctbal", "NUMERIC(12, 2)"));
  column_names_and_types.push_back(std::make_pair("su_comment", "CHAR(101)"));

  const std::vector<uint32_t> primary_key_col_idx {0};
  std::string table_name = "supplier";
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);

  std::mt19937 gen;
  std::ostringstream os;
  for (int i = 1; i <= num_suppliers; i++) { 
    std::vector<std::string> values; 
    values.push_back(std::to_string(i));
    values.push_back(RandomAString(25, 25, gen));
    values.push_back(RandomANString(20, 40, gen));
    values.push_back(std::to_string(nation_keys[std::uniform_int_distribution<int>(0, nation_keys.size() - 1)(gen)]));
    values.push_back(RandomNString(15, 15, gen));
    os << std::uniform_real_distribution<float>(10000, 1000000000)(gen) << std::setprecision(2);
    values.push_back(os.str());
    os.str(std::string());
    os.clear();
    values.push_back(RandomANString(101, 101, gen));

    writer.add_row(table_name, values);
  }
}


DEFINE_int32(num_suppliers, tpcch_sql::NUM_SUPPLIERS, "number of suppliers");

int main(int argc, char *argv[]) {
    gflags::SetUsageMessage("generates json file containing SQL tables for TPCCH data\n");   
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    std::string file_name = "sql-tpcc";
    TableWriter writer = TableWriter(file_name);
    Notice("Generating SEATS Tables");
    // generate all tables
    GenerateRegionTable(writer);
    Notice("Generated Regions");
    std::vector<int> nation_keys = GenerateNationTable(writer);
    Notice("Generated Nations");
    GenerateSupplierTable(writer, nation_keys, FLAGS_num_suppliers);
    Notice("Generated Supplier");
    writer.flush();
    Notice("Finished SEATS Table Generation");
    return 0;
}