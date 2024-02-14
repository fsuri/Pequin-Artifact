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
#include <boost/histogram.hpp>

#include "store/benchmark/async/json_table_writer.h"
#include "store/benchmark/async/sql/auctionmark/auctionmark_params.h"
#include "store/benchmark/async/sql/auctionmark/utils/category_parser.h"
#include "store/benchmark/async/sql/auctionmark/utils/auctionmark_utils.h"

namespace auctionmark {
//Read only tables

void GenerateRegionTable(TableWriter &writer, const uint32_t N_REGIONS)
{
  const size_t min_region_name_length = 6;
  const size_t max_region_name_length = 32;

  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("r_id", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("r_name", "TEXT"));
  const std::vector<uint32_t> primary_key_col_idx{0};


  //FIXME: Is this still current?
  std::string table_name = TABLE_REGION;
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);
  std::mt19937_64 gen;
  for (uint32_t r_id = 1; r_id <= N_REGIONS; ++r_id)
  {
    std::vector<std::string> values;
    values.push_back(std::to_string(r_id));
    // values.push_back(auctionmark::RandomAString(min_region_name_length, max_region_name_length, gen));
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

  std::string table_name = TABLE_CATEGORY;
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);

  
  const std::vector<uint32_t> index {2};
  writer.add_index(table_name, "idx_category_parent", index);

  return 5;
  // auto category_parser = auctionmark::CategoryParser();
  // auto categories = category_parser.get_categories();
  // for (auto &[_, value] : categories)
  // {
  //   std::vector<std::string> values;
  //   values.push_back(std::to_string(value.get_category_id()));
  //   values.push_back(value.get_name());
  //   if (value.get_parent_category_id().has_value())
  //   {
  //     values.push_back(std::to_string(value.get_parent_category_id().value()));
  //   }
  //   else
  //   {
  //     values.push_back("NULL");
  //   }
  //   writer.add_row(table_name, values);
  // }

  // return categories.size();
}

std::set<uint32_t> GenerateGlobalAttributeGroupTable(TableWriter &writer, int n_categories, int N_GAGS, int GAV_PER_GROUP)
{
  using namespace boost::histogram;
  auto h = make_histogram(axis::integer<>(0, n_categories - 1));
  std::mt19937_64 gen;
  std::set<uint32_t> gag_ids;

  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("gag_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("gag_c_id", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("gag_name", "TEXT"));
  const std::vector<uint32_t> primary_key_col_idx{0};

  std::string table_name = TABLE_GLOBAL_ATTR_GROUP;
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);

  //FIXME: Is this still current?
  for (int i = 0; i < N_GAGS; i++)
  {
    std::vector<std::string> values;
    uint32_t category_id = std::uniform_int_distribution<uint32_t>(0, n_categories - 1)(gen);
    h(category_id);
    uint32_t id = h.at(category_id);
    uint32_t gag_id = ((category_id << 22) >> 22) | ((id << 22) >> 12) | (GAV_PER_GROUP << 22) >> 2;
    gag_ids.insert(gag_id);
    values.push_back(std::to_string(gag_id));
    values.push_back(std::to_string(category_id));
    // values.push_back(auctionmark::RandomAString(6, 32, gen));
    writer.add_row(table_name, values);
  }

  return gag_ids;
}

void GenerateGlobalAttributeValueTable(TableWriter &writer, int GAV_PER_GROUP, std::set<uint32_t> gag_ids)
{
  std::mt19937_64 gen;

  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("gav_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("gav_gag_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("gav_name", "TEXT"));
  const std::vector<uint32_t> primary_key_col_idx{0, 1};

  std::string table_name = TABLE_GLOBAL_ATTR_VALUE;
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);

  //FIXME: Is this still current?
  for (uint32_t gag_id : gag_ids)
  {
    for (int j = 0; j < GAV_PER_GROUP; j++)
    {
      std::vector<std::string> values;
      uint64_t gav_id = (gag_id << 34) >> 34 | j << 30;
      values.push_back(std::to_string(gav_id));
      values.push_back(std::to_string(gag_id));
      // values.push_back(auctionmark::RandomAString(6, 32, gen));
      writer.add_row(table_name, values);
    }
  }
}


//Read/Write Tables

void GenerateUserAcctTable(TableWriter &writer, uint32_t N_REGIONS, uint32_t N_USERS)
{
  std::mt19937_64 gen;
  const uint32_t user_min_rating = 0;
  const uint32_t user_max_rating = 10000;
  const uint32_t user_min_balance = 1000;
  const uint32_t user_max_balance = 100000;

  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("u_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("u_rating", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("u_balance", "FLOAT"));
  column_names_and_types.push_back(std::make_pair("u_comments", "INT"));
  column_names_and_types.push_back(std::make_pair("u_r_id", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("u_created", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("u_updated", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("u_sattr0", "TEXT"));
  column_names_and_types.push_back(std::make_pair("u_sattr1", "TEXT"));
  column_names_and_types.push_back(std::make_pair("u_sattr2", "TEXT"));
  column_names_and_types.push_back(std::make_pair("u_sattr3", "TEXT"));
  column_names_and_types.push_back(std::make_pair("u_sattr4", "TEXT"));
  column_names_and_types.push_back(std::make_pair("u_sattr5", "TEXT"));
  column_names_and_types.push_back(std::make_pair("u_sattr6", "TEXT"));
  column_names_and_types.push_back(std::make_pair("u_sattr7", "TEXT"));
  column_names_and_types.push_back(std::make_pair("u_iattr0", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("u_iattr1", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("u_iattr2", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("u_iattr3", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("u_iattr4", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("u_iattr5", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("u_iattr6", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("u_iattr7", "BIGINT"));
  
  const std::vector<uint32_t> primary_key_col_idx{0};
  std::string table_name = TABLE_USERACCT;
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);

 
  const std::vector<uint32_t> index {0, 4};
  writer.add_index(table_name, "idx_useracct_region", index);

  for (int i = 0; i < N_USERS; i++)
  {
    std::vector<std::string> values;
    values.push_back(std::to_string(i));
    // values.push_back(std::to_string(std::zipf_distribution<uint32_t>(user_min_rating, user_max_rating)(gen)));
  }
}

// void GenerateUserAcctAttributes(){
//    std::vector<std::pair<std::string, std::string>> column_names_and_types;
//   column_names_and_types.push_back(std::make_pair("ua_id", "BIGINT"));
//   column_names_and_types.push_back(std::make_pair("ua_u_id", "TEXT"));
//   column_names_and_types.push_back(std::make_pair("ua_name", "TEXT"));
//   column_names_and_types.push_back(std::make_pair("ua_value", "TEXT"));
//   column_names_and_types.push_back(std::make_pair("u_created", "BIGINT"));
  
//   const std::vector<uint32_t> primary_key_col_idx{0, 1};

//   std::string table_name = TABLE_USERACCT;
//   writer.add_table(table_name, column_names_and_types, primary_key_col_idx);
// }

void GenerateUserAcctWatch(TableWriter &writer){
   std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("uw_u_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("uw_i_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("uw_i_u_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("uw_created", "BIGINT"));

  const std::vector<uint32_t> primary_key_col_idx{0, 1, 2};

  std::string table_name = TABLE_USERACCT_WATCH;
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);
}

void GenerateUserAcctFeedback(TableWriter &writer){
   std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("uf_u_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("uf_i_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("uf_i_u_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("uf_from_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("uf_rating", "INT"));
  column_names_and_types.push_back(std::make_pair("uf_date", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("uf_sattr0", "TEXT"));
 
  const std::vector<uint32_t> primary_key_col_idx{0, 1, 2, 3};

  std::string table_name = TABLE_USERACCT_FEEDBACK;
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);
}

void GenerateUserAcctItem(TableWriter &writer){
   std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("ui_u_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("ui_i_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("ui_i_u_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("ui_ip_id", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("ui_ip_ib_id", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("ui_ip_ib_i_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("ui_ip_ib_u_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("ui_created", "BIGINT"));
 
  const std::vector<uint32_t> primary_key_col_idx{0, 1, 2};

  std::string table_name = TABLE_USERACCT_ITEM;
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);

  //Optional Index
   const std::vector<uint32_t> index {1};
  writer.add_index(table_name, "idx_useracct_item_id", index);
}

void GenerateItem(TableWriter &writer){
  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("i_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("i_u_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("i_c_id", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("i_name", "TEXT"));
  column_names_and_types.push_back(std::make_pair("i_description", "TEXT"));
  column_names_and_types.push_back(std::make_pair("i_user_attributes", "TEXT"));
  column_names_and_types.push_back(std::make_pair("i_initial_price", "FLOAT"));
  column_names_and_types.push_back(std::make_pair("i_current_price", "FLOAT"));

  column_names_and_types.push_back(std::make_pair("i_num_bids", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("i_num_images", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("i_num_global_attrs", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("i_num_comments", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("i_start_date", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("i_end_date", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("i_status", "INT"));
  column_names_and_types.push_back(std::make_pair("i_created", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("i_updated", "BIGINT"));

  column_names_and_types.push_back(std::make_pair("i_iattr0", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("i_iattr1", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("i_iattr2", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("i_iattr3", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("i_iattr4", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("i_iattr5", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("i_iattr6", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("i_iattr7", "BIGINT"));
  
  const std::vector<uint32_t> primary_key_col_idx{0, 1};

  std::string table_name = TABLE_ITEM;
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);

  const std::vector<uint32_t> index {1};
  writer.add_index(table_name, "idx_item_seller", index);
}

void GenerateItemAttribute(TableWriter &writer){
  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("ia_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("ia_i_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("ia_u_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("ia_gav_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("ia_gag_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("ia_sattr0", "TEXT"));
 
  const std::vector<uint32_t> primary_key_col_idx{0, 1, 2};

  std::string table_name = TABLE_ITEM_ATTR;
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);
}

void GenerateItemImage(TableWriter &writer){
  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("ii_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("ii_i_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("ii_u_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("ia_sattr0", "TEXT"));
 
  const std::vector<uint32_t> primary_key_col_idx{0, 1, 2};

  std::string table_name = TABLE_ITEM_IMAGE;
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);
}

void GenerateItemComment(TableWriter &writer){
  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("ic_id", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("ic_i_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("ic_u_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("ic_buyer_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("ic_question", "TEXT"));
  column_names_and_types.push_back(std::make_pair("ic_response", "TEXT"));
  column_names_and_types.push_back(std::make_pair("ic_created", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("ic_updated", "BIGINT"));
 
  const std::vector<uint32_t> primary_key_col_idx{0, 1, 2};

  std::string table_name = TABLE_ITEM_COMMENT;
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);

   //Optional Index:
  const std::vector<uint32_t> index {1, 2};
  writer.add_index(table_name, "idx_item_comment", index);
}

void GenerateItemBid(TableWriter &writer){
  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("ib_id", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("ib_i_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("ib_u_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("ib_buyer_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("ib_bid", "FLOAT"));
  column_names_and_types.push_back(std::make_pair("ib_max_bid", "FLOAT"));
  column_names_and_types.push_back(std::make_pair("ib_created", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("ib_updated", "BIGINT"));
 
  const std::vector<uint32_t> primary_key_col_idx{0, 1, 2};

  std::string table_name = TABLE_ITEM_BID;
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);
}

void GenerateItemMaxBid(TableWriter &writer){
   std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("imb_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("imb_u_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("imb_ib_id", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("imb_ib_i_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("imb_ib_u_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("imb_created", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("imb_updated", "BIGINT"));
 
  const std::vector<uint32_t> primary_key_col_idx{0, 1};

  std::string table_name = TABLE_ITEM_MAX_BID;
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);
}

void GenerateItemPurchase(TableWriter &writer){
   std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("ip_id", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("ip_ib_id", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("ip_ib_i_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("ip_ib_u_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("ip_date", "BIGINT"));

  const std::vector<uint32_t> primary_key_col_idx{0, 1, 2, 3};

  std::string table_name = TABLE_ITEM_MAX_BID;
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);
}
 
 

int main(int argc, char *argv[]) {
  // gflags::SetUsageMessage(
  //     "generates a json file containing sql tables for AuctionMark data\n");
  // std::string file_name = "auctionmark";
  // TableWriter writer = TableWriter(file_name);

  // GenerateRegionTable(writer, auctionmark::N_REGIONS);
  // int n_categories = GenerateCategoryTable(writer);
  // std::set<uint32_t> gag_ids = GenerateGlobalAttributeGroupTable(writer, n_categories, auctionmark::N_GAGS, auctionmark::GAV_PER_GROUP);
  // GenerateGlobalAttributeValueTable(writer, auctionmark::GAV_PER_GROUP, gag_ids);
  // GenerateUserAcctTable(writer, auctionmark::N_REGIONS, auctionmark::N_USERS);
  // writer.flush();
  // std::cerr << "Wrote tables." << std::endl;
  return 0;
}

}