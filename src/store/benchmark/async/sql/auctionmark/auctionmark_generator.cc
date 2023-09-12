/***********************************************************************
 *
 * Copyright 2023 Florian Suri-Payer <fsp@cs.cornell.edu>
 *                Liam Arzola <lma77@cornell.edu>
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

#include <random>
#include <gflags/gflags.h>
#include <set>

#include "store/benchmark/async/json_table_writer.h"
#include "store/benchmark/async/sql/auctionmark/category_parser.h"
#include <boost/histogram.hpp>

const char ALPHA_NUMERIC[] = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

std::string RandomAString(size_t x, size_t y, std::mt19937 &gen)
{
  std::string s;
  size_t length = std::uniform_int_distribution<size_t>(x, y)(gen);
  for (size_t i = 0; i < length; ++i)
  {
    int j = std::uniform_int_distribution<size_t>(0, sizeof(ALPHA_NUMERIC))(gen);
    s += ALPHA_NUMERIC[j];
  }
  return s;
}

void GenerateRegionTable(TableWriter &writer, const uint32_t n_regions)
{
  const size_t min_region_name_length = 6;
  const size_t max_region_name_length = 32;

  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("r_id", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("r_name", "TEXT"));
  const std::vector<uint32_t> primary_key_col_idx{0};

  std::string table_name = "Region";
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);
  std::mt19937 gen;
  for (uint32_t r_id = 1; r_id <= n_regions; ++r_id)
  {
    std::vector<std::string> values;
    values.push_back(std::to_string(r_id));
    values.push_back(RandomAString(min_region_name_length, max_region_name_length, gen));
    writer.add_row(table_name, values);
  }
}

int GenerateCategoryTable(TableWriter &writer)
{
  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("c_id", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("c_name", "TEXT"));
  column_names_and_types.push_back(std::make_pair("c_parent_id", "BIGINT"));
  const std::vector<uint32_t> primary_key_col_idx{0};

  std::string table_name = "Category";
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);
  auto category_parser = auctionmark::CategoryParser();
  auto categories = category_parser.get_categories();
  for (auto &[_, value] : categories)
  {
    std::vector<std::string> values;
    values.push_back(std::to_string(value.get_category_id()));
    values.push_back(value.get_name());
    if (value.get_parent_category_id().has_value())
    {
      values.push_back(std::to_string(value.get_parent_category_id().value()));
    }
    else
    {
      values.push_back("NULL");
    }
    writer.add_row(table_name, values);
  }

  return categories.size();
}

std::set<uint32_t> GenerateGlobalAttributeGroupTable(TableWriter &writer, int n_categories, int n_gags, int gav_per_group)
{
  using namespace boost::histogram;
  auto h = make_histogram(axis::integer<>(0, n_categories - 1));
  std::mt19937 gen;
  std::set<uint32_t> gag_ids;

  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("gag_id", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("gag_c_id", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("gag_name", "TEXT"));
  const std::vector<uint32_t> primary_key_col_idx{0};

  std::string table_name = "GlobalAttributeGroup";
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);
  for (int i = 0; i < n_gags; i++)
  {
    std::vector<std::string> values;
    uint32_t category_id = std::uniform_int_distribution<uint32_t>(0, n_categories - 1)(gen);
    h(category_id);
    uint32_t id = h.at(category_id);
    uint32_t gag_id = ((category_id << 22) >> 22) | ((id << 22) >> 12) | (gav_per_group << 22) >> 2;
    gag_ids.insert(gag_id);
    values.push_back(std::to_string(gag_id));
    values.push_back(std::to_string(category_id));
    values.push_back(RandomAString(6, 32, gen));
    writer.add_row(table_name, values);
  }

  return gag_ids;
}

void GenerateGlobalAttributeValueTable(TableWriter &writer, int gav_per_group, std::set<uint32_t> gag_ids)
{
  std::mt19937 gen;

  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("gav_id", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("gav_gag_id", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("gav_name", "TEXT"));
  const std::vector<uint32_t> primary_key_col_idx{0};

  std::string table_name = "GlobalAttributeValue";
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);

  for (uint32_t gag_id : gag_ids)
  {
    for (int j = 0; j < gav_per_group; j++)
    {
      std::vector<std::string> values;
      uint64_t gav_id = (gag_id << 34) >> 34 | j << 30;
      values.push_back(std::to_string(gav_id));
      values.push_back(std::to_string(gag_id));
      values.push_back(RandomAString(6, 32, gen));
      writer.add_row(table_name, values);
    }
  }
}

void GenerateUserTable(TableWriter &writer, uint32_t n_regions, uint32_t n_users)
{
  std::mt19937 gen;
  const uint32_t user_min_rating = 0;
  const uint32_t user_max_rating = 10000;
  const uint32_t user_min_balance = 1000;
  const uint32_t user_max_balance = 100000;

  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("u_id", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("u_rating", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("u_balance", "DOUBLE"));
  column_names_and_types.push_back(std::make_pair("u_created", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("u_r_id", "BIGINT"));
  const std::vector<uint32_t> primary_key_col_idx{0};

  std::string table_name = "User";
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);

  for (int i = 0; i < n_users; i++)
  {
    std::vector<std::string> values;
    values.push_back(std::to_string(i));
    values.push_back(std::to_string(std::zipf_distribution<uint32_t>(user_min_rating, user_max_rating)(gen)));
  }
}

int main(int argc, char *argv[])
{
  gflags::SetUsageMessage(
      "generates a json file containing sql tables for AuctionMark data\n");
  std::string file_name = "auctionmark";
  TableWriter writer = TableWriter(file_name);

  const uint32_t n_regions = 75;
  const uint32_t n_gags = 100;
  const uint32_t gav_per_group = 10;
  const uint32_t n_users = 1000000;

  GenerateRegionTable(writer, n_regions);
  int n_categories = GenerateCategoryTable(writer);
  std::set<uint32_t> gag_ids = GenerateGlobalAttributeGroupTable(writer, n_categories, n_gags, gav_per_group);
  GenerateGlobalAttributeValueTable(writer, gav_per_group, gag_ids);
  GenerateUserTable(writer, n_regions, n_users);
  writer.flush();
  std::cerr << "Wrote tables." << std::endl;
  return 0;
}
