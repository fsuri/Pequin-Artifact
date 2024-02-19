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
#include <sys/time.h>
// #include <boost/histogram.hpp>

#include "store/benchmark/async/json_table_writer.h"
#include "store/benchmark/async/sql/auctionmark/auctionmark_generator.h"


DEFINE_int32(client_total, 100, "number of clients");
 //FIXME: THIS IS A PROBLEM. NOT REALLY COMPATIBLE WITH OUR EXPERIMENTAL FRAMEWORK since num_clients isn't known at Data generation Time
                          //HOW DOES client id impact contention? => Could we just generate a larger amount of clients, and then not use them?
//TODO: Instead of static loading, write a script that calls the generator on demand
//TODO: Could multithread the generation...
DEFINE_double(scale_factor, 1.0, "scaling factor");


namespace auctionmark {


///////////////////////////////////////////       TABLE SCHEMAS
void RegionTableSchema(TableWriter &writer){
  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("r_id", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("r_name", "TEXT"));
  const std::vector<uint32_t> primary_key_col_idx{0};

  std::string table_name = TABLE_REGION;
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);
}

void CategoryTableSchema(TableWriter &writer){
  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("c_id", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("c_name", "TEXT"));
  column_names_and_types.push_back(std::make_pair("c_parent_id", "BIGINT"));
  const std::vector<uint32_t> primary_key_col_idx{0};

  std::string table_name = TABLE_CATEGORY;
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);

  
  const std::vector<uint32_t> index {2};
  writer.add_index(table_name, "idx_category_parent", index);
}

void GlobalAttrGroupTableSchema(TableWriter &writer){
  //SCHEMA

  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("gag_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("gag_c_id", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("gag_name", "TEXT"));
  const std::vector<uint32_t> primary_key_col_idx{0};

  std::string table_name = TABLE_GLOBAL_ATTR_GROUP;
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);
}

void GlobalAttrGroupValueTableSchema(TableWriter &writer){
    //SCHEMA
  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("gav_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("gav_gag_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("gav_name", "TEXT"));
  const std::vector<uint32_t> primary_key_col_idx{0, 1};

  std::string table_name = TABLE_GLOBAL_ATTR_VALUE;
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);
}

void UserAcctTableSchema(TableWriter &writer){
   std::string table_name = TABLE_USERACCT;
  //SCHEMA
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

  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);

  const std::vector<uint32_t> index {0, 4};
  writer.add_index(table_name, "idx_useracct_region", index);
}

void ItemTableSchema(TableWriter &writer){
 
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

void ItemImageTableSchema(TableWriter &writer){
   std::string table_name = TABLE_ITEM_IMAGE;
  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("ii_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("ii_i_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("ii_u_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("ia_sattr0", "TEXT"));
 
  const std::vector<uint32_t> primary_key_col_idx{0, 1, 2};

  
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);
}

void ItemAttributeTableSchema(TableWriter &writer){
   std::string table_name = TABLE_ITEM_ATTR;
  //SCHEMA
  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("ia_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("ia_i_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("ia_u_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("ia_gav_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("ia_gag_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("ia_sattr0", "TEXT"));
 
  const std::vector<uint32_t> primary_key_col_idx{0, 1, 2};

  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);
}

void ItemCommentTableSchema(TableWriter &writer){
  std::string table_name = TABLE_ITEM_COMMENT;
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

  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);

   //Optional Index:
  const std::vector<uint32_t> index {1, 2};
  writer.add_index(table_name, "idx_item_comment", index);
}

void ItemBidTableSchema(TableWriter &writer){
   std::string table_name = TABLE_ITEM_BID;
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

  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);
}

void ItemMaxBidTableSchema(TableWriter &writer){
  std::string table_name = TABLE_ITEM_MAX_BID;
   std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("imb_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("imb_u_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("imb_ib_id", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("imb_ib_i_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("imb_ib_u_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("imb_created", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("imb_updated", "BIGINT"));
 
  const std::vector<uint32_t> primary_key_col_idx{0, 1};

  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);
}

void ItemPurchaseTableSchema(TableWriter &writer){
  std::string table_name = TABLE_ITEM_PURCHASE;
   std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("ip_id", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("ip_ib_id", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("ip_ib_i_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("ip_ib_u_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("ip_date", "BIGINT"));

  const std::vector<uint32_t> primary_key_col_idx{0, 1, 2, 3};

  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);
}

void UserFeedbackTableSchema(TableWriter &writer){
  std::string table_name = TABLE_USERACCT_FEEDBACK;
  //SCHEMA
   std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("uf_u_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("uf_i_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("uf_i_u_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("uf_from_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("uf_rating", "INT"));
  column_names_and_types.push_back(std::make_pair("uf_date", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("uf_sattr0", "TEXT"));
 
  const std::vector<uint32_t> primary_key_col_idx{0, 1, 2, 3};

  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);
}

void UserItemTableSchema(TableWriter &writer){
   std::string table_name = TABLE_USERACCT_ITEM;
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

  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);

  //Optional Index
   const std::vector<uint32_t> index {1};
  writer.add_index(table_name, "idx_useracct_item_id", index);

}

void UserWatchTableSchema(TableWriter &writer){
  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("uw_u_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("uw_i_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("uw_i_u_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("uw_created", "BIGINT"));

  const std::vector<uint32_t> primary_key_col_idx{0, 1, 2};

  std::string table_name = TABLE_USERACCT_WATCH;
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);
}




//////////////////////////////////////////        TABLE GENERATION

//Read only tables

void GenerateRegionTable(TableWriter &writer)
{
  std::string table_name = TABLE_REGION;

  const size_t min_region_name_length = 6;
  const size_t max_region_name_length = 32;

  std::mt19937_64 gen;
  for (uint32_t r_id = 1; r_id <= TABLESIZE_REGION; ++r_id)
  {
    std::vector<std::string> values;
    values.push_back(std::to_string(r_id));
    values.push_back(RandomAString(min_region_name_length, max_region_name_length, gen));
    writer.add_row(table_name, values);
  }
}


int GenerateCategoryTable(TableWriter &writer)
{
  std::string table_name = TABLE_CATEGORY;

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

int GenerateGlobalAttributeGroupTable(TableWriter &writer, int n_categories, AuctionMarkProfile &profile)
{

  //DATA GEN
  std::string table_name = TABLE_GLOBAL_ATTR_GROUP;

  //using namespace boost::histogram;
  //auto h = make_histogram(axis::integer<>(0, n_categories - 1));
  std::vector<int> category_groups(n_categories);
  std::mt19937_64 gen;
  //std::vector<> gag_ids;

  int total_count;

  for (int i = 0; i < TABLESIZE_GLOBAL_ATTRIBUTE_GROUP; i++){
    uint32_t category_id = std::uniform_int_distribution<uint32_t>(0, n_categories - 1)(gen);
    int id = ++category_groups[category_id];
    int count = std::uniform_int_distribution<int>(1, TABLESIZE_GLOBAL_ATTRIBUTE_VALUE_PER_GROUP)(gen);

    total_count += count;
    GlobalAttributeGroupId gag_id(category_id, id, count);

    profile.gag_ids.push_back(gag_id);

  }

  for (auto &gag_id: profile.gag_ids){
    std::vector<std::string> values;
    values.push_back(gag_id.encode());
    values.push_back(std::to_string(gag_id.get_category_id()));
    values.push_back(RandomAString(6, 32, gen));
    writer.add_row(table_name, values);
  }

  return total_count;
}

void GenerateGlobalAttributeValueTable(TableWriter &writer, AuctionMarkProfile &profile, int GAV_size) //GAV_size == total_count of gag
{
   
  //DATA GEN
  std::mt19937_64 gen;

  int tableSize = GAV_size;

  for (auto & gag_id: profile.gag_ids){
    for(int gav_counter = 0; gav_counter < gag_id.get_count(); ++gav_counter){
      GlobalAttributeValueId gav_id(gag_id.encode(), gav_counter);

      std::vector<std::string> values;
      values.push_back(gav_id.encode());
      values.push_back(gag_id.encode());
      values.push_back(RandomAString(6, 32, gen));
      writer.add_row(TABLE_GLOBAL_ATTR_VALUE, values);
    }
  }
}




std::vector<UserId> GenerateUserAcctTable(TableWriter &writer, AuctionMarkProfile &profile) 
{
 
  //DATA GEN

   std::mt19937_64 gen;
  
  Zipf randomRating(gen, USER_MIN_RATING, USER_MAX_RATING, 1.0001);
  Zipf randomBalance(gen, USER_MIN_BALANCE, USER_MAX_BALANCE, 1.001);

  int max_items = std::max(1, (int) ceil(ITEM_ITEMS_PER_SELLER_MAX * SCALE_FACTOR));
  Zipf randomNumItems(gen, ITEM_ITEMS_PER_SELLER_MIN, max_items, ITEM_ITEMS_PER_SELLER_SIGMA);

   //A histogram for the number of users that have the number of items listed ItemCount -> # of Users
  //profile.users_per_item_count = std::vector<int>(max_items+1); //In vector form, there may be many itemCounts that have 0 users. => Easier to use map.

  for (int i = 0; i < TABLESIZE_USERACCT; i++){
    int num_items = (int) randomNumItems.next_long();
    profile.users_per_item_count[num_items]++;   //increment value freq by 1 count.
  }

  // for(auto &[item, users]: profile.users_per_item_count){
  //   std::cerr << "item_cnt: " << item << " --> " << users << std::endl;
  // }

  UserIdGenerator idGenerator(profile.users_per_item_count, FLAGS_client_total);

  std::vector<UserId> user_ids;

  for (int i = 0; i < TABLESIZE_USERACCT; i++)
  {
   
    UserId u_id = idGenerator.next();
    user_ids.push_back(u_id);

    //if(i==0) std::cerr << "first user: " << u_id.to_string() << std::endl;
    
    std::vector<std::string> values;
    values.push_back(u_id.encode()); //u_id
    Zipf zipf;
    values.push_back(std::to_string(randomRating.next_long())); //u_rating
    double bal = randomBalance.next_long()/10.0;

    values.push_back(std::to_string(bal)); //u_balance
    values.push_back(std::to_string(0)); //u_comments
    values.push_back(std::to_string(std::uniform_int_distribution<int>(0, TABLESIZE_REGION)(gen))); //u_r_id
    struct timeval time;
    gettimeofday(&time, NULL);
    uint64_t curr_time = get_ts(time);
    values.push_back(std::to_string(curr_time)); //u_created
    values.push_back(std::to_string(curr_time)); //u_updated


    
    /** Any column with the name XX_SATTR## will automatically be filled with a random string with length between 0 (empty) and ?)  */
    int max_size = 128; //sattr is VARCHAR(128)
    for(int i = 0; i<8; ++i){
      int min_size = std::uniform_int_distribution<int>(0, max_size-1)(gen);
      values.push_back(RandomAString(min_size, max_size, gen));
    }
    /** Any column with the name XX_IATTR## will automatically be filled with a random integer between (0, 1 << 30)*/
    for(int i = 0; i<8; ++i){
      values.push_back(std::to_string(std::uniform_int_distribution<int>(0, 1 <<30)(gen)));
    }


     writer.add_row(TABLE_USERACCT, values);
    // values.push_back(std::to_string(std::zipf_distribution<uint32_t>(user_min_rating, user_max_rating)(gen)));

    //TODO: Generate all sub-tables (Probably easier to just store the UserId in a vector and call it separately)
  }
  return user_ids;
}
//THIS TABLE SEEMS UNECESSARY: NO TX USES IT.
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


static int num_open_items = 0;
static int num_ending_soon_items = 0;
static int num_waiting_for_purchase_items = 0;
static int num_closed_items = 0;

std::vector<LoaderItemInfo> GenerateItemTableData(TableWriter &writer, AuctionMarkProfile &profile, std::vector<UserId> &user_ids){
 

  std::vector<LoaderItemInfo> items;

  int tableSize = 0;
  // for(int i = 0; i < profile.users_per_item_count.size(); ++i){
  //   tableSize += i * profile.users_per_item_count[i];
  // }
  for(auto &[item_cnt, users]: profile.users_per_item_count){
    tableSize += item_cnt * users;
  }

  
  std::cerr << "Item Table Size: " << tableSize << std::endl;
  std::cerr << "num users: " << user_ids.size() << std::endl;


  std::mt19937_64 gen;

  int user_idx = 0;
  for(auto [itemCount, num_users] : profile.users_per_item_count){
    //for itemCount, there are num_users many users. For each of those user, generate itemCount many items
    for(int i = 0; i < num_users; ++i){
      assert(user_idx <= user_ids.size());
      //pick next user.
      UserId seller_id = user_ids[user_idx];
      user_idx++;

      //for this user, create #itemCount many items
      int remaining = itemCount;
      while(remaining-- > 0){
        //Create item.
        items.push_back(GenerateItemTableRow(writer, profile, gen, seller_id, remaining));
        GenerateSubTableRows(writer, profile, gen, items.back());
      }
    }
  }

//FIXME: This is not the correct item generation procedure style...
//int remaining = tableSize; 
  // for(auto &seller_id: user_ids){
  //   LoaderItemInfo GenerateItemTableRow(writer, seller_id, remaining);

  //   items.push_back(itemInfo);
  // }

  std::cerr << "Generated: " << num_open_items << " open items, " << num_ending_soon_items << " ending soon items, "
          << num_waiting_for_purchase_items << " waiting_for_purchase items, and " << num_closed_items << " closed items." << std::endl;
  return items;
}

static bool once = true;
LoaderItemInfo GenerateItemTableRow(TableWriter &writer, AuctionMarkProfile &profile, std::mt19937_64 &gen, const UserId &seller_id, int remaining){

  ItemId itemId(seller_id, remaining);
  //std::cerr << "first item user: " << itemId.get_seller_id().to_string() << std::endl;
    //if(--remaining == 0) break;

    
  uint64_t endDate = getRandomEndTimestamp(profile);
  uint64_t startDate = getRandomStartTimestamp(endDate, profile);

  uint64_t bidDurationDay = (endDate - startDate) / MILLISECONDS_IN_A_DAY;

  std::pair<Zipf, Zipf> p;
  try {
    p = item_bid_watch_zipfs.at(bidDurationDay);
  }
  catch(...){
    Zipf randomNumBids(gen, ITEM_BIDS_PER_DAY_MIN * bidDurationDay, ITEM_BIDS_PER_DAY_MAX * bidDurationDay, ITEM_BIDS_PER_DAY_SIGMA);
    Zipf randomNumWatches(gen, ITEM_WATCHES_PER_DAY_MIN * bidDurationDay, ITEM_WATCHES_PER_DAY_MAX * bidDurationDay, ITEM_WATCHES_PER_DAY_SIGMA);
    p = std::make_pair(randomNumBids, randomNumWatches);
    item_bid_watch_zipfs[bidDurationDay] = p;
  }
    // Calculate the number of bids and watches for this item
  uint64_t numBids = p.first.next_long();
  uint64_t numWatches = p.first.next_long();

  // Create the ItemInfo object that we will use to cache the local data

  // tables are done with it.
  LoaderItemInfo itemInfo(itemId, endDate, numBids);
  itemInfo.startDate = startDate;
  itemInfo.initialPrice = profile.random_initial_price.next_long();
  itemInfo.numImages = profile.random_num_images.next_long();
  itemInfo.numAttributes = profile.random_num_attributes.next_long();
  //itemInfo.numBids = numBids;
  itemInfo.numWatches = numWatches;
  itemInfo.set_status(ItemStatus::OPEN);

  //num_open_items++;
    // The auction for this item has already closed
  if(itemInfo.get_end_date() <= profile.get_loader_start_time()){
    // Somebody won a bid and bought the item
    if (itemInfo.get_num_bids() > 0) {
      auto sellerId = itemInfo.get_seller_id();
      itemInfo.lastBidderId = profile.get_random_buyer_id(sellerId);
      itemInfo.purchaseDate = getRandomPurchaseTimestamp(endDate, profile);
      itemInfo.numComments = profile.random_num_comments.next_long();
    }
    // num_closed_items++;
    // num_open_items--;
    itemInfo.set_status(ItemStatus::CLOSED);
  }
  // Item is still available
  else if (itemInfo.get_num_bids() > 0) {
    auto sellerId = itemInfo.get_seller_id();
    itemInfo.lastBidderId = profile.get_random_buyer_id(sellerId);
  }
  
  assert(itemInfo.get_status() == ItemStatus::OPEN || itemInfo.get_status() == ItemStatus::CLOSED);
  profile.add_item_to_proper_queue(itemInfo, true);

   if(itemInfo.get_status() == ItemStatus::OPEN) num_open_items++;
   if(itemInfo.get_status() == ItemStatus::ENDING_SOON) num_ending_soon_items++;
    if(itemInfo.get_status() == ItemStatus::WAITING_FOR_PURCHASE) num_waiting_for_purchase_items++;
  if(itemInfo.get_status() == ItemStatus::CLOSED) num_closed_items++;
  //assert(itemInfo.get_status() == ItemStatus::OPEN || itemInfo.get_status() == ItemStatus::CLOSED);
  

  //CREATE ROW
  std::vector<std::string> values;
  values.push_back(itemInfo.get_item_id().encode());// I_ID
  values.push_back(itemInfo.get_seller_id().encode());// I_U_ID
  values.push_back(std::to_string(profile.get_random_category_id()));// I_C_ID
  values.push_back(RandomAString(ITEM_NAME_LENGTH_MIN, ITEM_NAME_LENGTH_MAX, gen)); // I_NAME
  values.push_back(RandomAString(ITEM_DESCRIPTION_LENGTH_MIN, ITEM_DESCRIPTION_LENGTH_MAX, gen));// I_DESCRIPTION
  values.push_back(RandomAString(ITEM_USER_ATTRIBUTES_LENGTH_MIN, ITEM_USER_ATTRIBUTES_LENGTH_MAX, gen));  // I_USER_ATTRIBUTES
  values.push_back(std::to_string(itemInfo.initialPrice));// I_INITIAL_PRICE
  // I_CURRENT_PRICE
  if (itemInfo.get_num_bids() > 0) {
    itemInfo.set_current_price(itemInfo.initialPrice + (itemInfo.get_num_bids() * itemInfo.initialPrice * ITEM_BID_PERCENT_STEP));
    values.push_back(std::to_string(itemInfo.get_current_price()));
  } else {
    values.push_back(std::to_string(itemInfo.initialPrice));
  }
  
  values.push_back(std::to_string(itemInfo.get_num_bids()));// I_NUM_BIDS
  values.push_back(std::to_string(itemInfo.numImages));// I_NUM_IMAGES
  values.push_back(std::to_string(itemInfo.numAttributes));// I_NUM_GLOBAL_ATTRS
  values.push_back(std::to_string(itemInfo.numAttributes));// I_NUM_COMMENTS

  values.push_back(std::to_string(itemInfo.startDate));// I_START_DATE

  values.push_back(std::to_string(endDate));// I_END_DATE

  //int status = itemInfo.get_status() == ItemStatus::OPEN? 0 : 3;
  int status = (int) itemInfo.get_status();
  values.push_back(std::to_string(status));// I_STATUS

  uint64_t start_time = profile.get_loader_start_time();
  values.push_back(std::to_string(start_time));// I_CREATED

  values.push_back(std::to_string(itemInfo.startDate));// I_UPDATED

  writer.add_row(TABLE_ITEM, values);

  return itemInfo;
}

void GenerateSubTableRows(TableWriter &writer, AuctionMarkProfile &profile, std::mt19937_64 &gen, LoaderItemInfo &itemInfo){

  GenerateItemImageRow(writer, itemInfo);
  
  GenerateItemAttributeRow(writer, profile, itemInfo);
 
  GenerateItemCommentRow(writer, itemInfo, gen);

  GenerateItemBidRow(writer, profile, itemInfo);
 
  GenerateItemMaxBidRow(writer, itemInfo);
 
  GenerateItemPurchaseRow(writer, itemInfo, gen);
 
  GenerateUserFeedbackRow(writer, profile, itemInfo);
 
  GenerateUserItemRow(writer, itemInfo);
 
  GenerateUserWatchRow(writer, profile, itemInfo, gen);
  
}

//ITEM SUB ROW GENERATION

void GenerateItemImageRow(TableWriter &writer, LoaderItemInfo &itemInfo){
  //DATA GEN
  for(int count = 0; count < itemInfo.numImages; ++count){
    std::vector<std::string> values;
    values.push_back(std::to_string(count)); //ii_id   //FIXME: Unclear if this is the correct use of count
    values.push_back(itemInfo.get_item_id().encode()); //ii_i_id
    values.push_back(itemInfo.get_seller_id().encode()); //ii_u_id
    writer.add_row(TABLE_ITEM_IMAGE, values);
  }
}

void GenerateItemAttributeRow(TableWriter &writer, AuctionMarkProfile &profile,  LoaderItemInfo &itemInfo){
  //DATA GENERATION
  for(int count = 0; count < itemInfo.numAttributes; ++count){
    std::vector<std::string> values;
    values.push_back(std::to_string(count)); //ia_id  //FIXME: Unclear if this is the correct use of count
    values.push_back(itemInfo.get_item_id().encode()); //ia_i_id
    values.push_back(itemInfo.get_seller_id().encode()); //ia_u_id
  
    GlobalAttributeValueId gav_id = profile.get_random_global_attribute_value();
    values.push_back(gav_id.encode()); //ia_gav_id
    values.push_back(gav_id.get_global_attribute_group().encode()); //ia_gag_id
    writer.add_row(TABLE_ITEM_ATTR, values);
  }
}


void GenerateItemCommentRow(TableWriter &writer, LoaderItemInfo &itemInfo, std::mt19937_64 &gen){
  
  //DATA GENERATION
  
  int total = itemInfo.purchaseDate > 0 ? itemInfo.numComments : 0;
  for(int count = 0; count < total; ++count){
    std::vector<std::string> values;
    values.push_back(std::to_string(count)); //ic_id     //FIXME: Unclear if this is the correct use of count
    values.push_back(itemInfo.get_item_id().encode()); //ic_i_id
    values.push_back(itemInfo.get_seller_id().encode()); //ic_u_id
    values.push_back(itemInfo.lastBidderId.encode()); //ic_buyer_id

    values.push_back(RandomAString(ITEM_COMMENT_LENGTH_MIN, ITEM_COMMENT_LENGTH_MAX, gen));//ic_question

    values.push_back(RandomAString(ITEM_COMMENT_LENGTH_MIN, ITEM_COMMENT_LENGTH_MAX, gen));//ic_response
    
    uint64_t t = getRandomCommentDate(itemInfo.startDate, itemInfo.get_end_date(), gen);
    values.push_back(std::to_string(t));//ic_created
    values.push_back(std::to_string(t));//ic_updated

    writer.add_row(TABLE_ITEM_COMMENT, values);
  }
  
}

void GenerateItemBidRow(TableWriter &writer, AuctionMarkProfile &profile, LoaderItemInfo &itemInfo){
 
   //DATA GENERATION
  
  Bid bid;
  bool new_item = true;
  //TODO: Should these three not reset between items?
  float currentPrice;
  float currentBidPriceAdvanceStep;
  uint64_t currentCreateDateAdvanceStep;

  int total = itemInfo.get_num_bids();
  //std::cerr << "total bids per item: " << total << std::endl;
  assert(total <= 100);
  for(int count = 0; count < total; ++count){
    int remaining = total - count - 1;

    UserId bidderId;
    // Figure out the UserId for the person bidding on this item now

    if(new_item) {
      // If this is a new item and there is more than one bid, then  we'll choose the bidder's UserId at random.
      // If there is only one bid, then it will have to be the last bidder
      bidderId = itemInfo.get_num_bids() == 1 ? itemInfo.lastBidderId : profile.get_random_buyer_id(itemInfo.get_seller_id());
      uint64_t endDate;
      if(itemInfo.get_status() == ItemStatus::OPEN){
        endDate = profile.get_loader_start_time();
      }
      else{
        endDate = itemInfo.get_end_date();
      }
      currentCreateDateAdvanceStep = (endDate - itemInfo.startDate) / (remaining + 1);
      currentBidPriceAdvanceStep = itemInfo.initialPrice * ITEM_BID_PERCENT_STEP;
      currentPrice = itemInfo.initialPrice;
    }
    else if(count == total){
        // The last bid must always be the item's lastBidderId
        bidderId = itemInfo.lastBidderId;
        currentPrice = itemInfo.get_current_price();
    }
    else if(total == 2){
        // The first bid for a two-bid item must always be different than the lastBidderId
      bidderId = profile.get_random_buyer_id({itemInfo.lastBidderId, itemInfo.get_seller_id()});
    }
    else{
        // Since there are multiple bids, we want randomly select one based on the previous bidders
      // We will get the histogram of bidders so that we are more likely to select an existing bidder rather than a completely random one
      auto &bidderHistogram = itemInfo.bidderHistogram;
      bidderId = profile.get_random_buyer_id(bidderHistogram, {bid.bidderId, itemInfo.get_seller_id()});
      currentPrice += currentBidPriceAdvanceStep;
    }

    //Update bid info
    float last_bid = new_item? itemInfo.initialPrice : bid.maxBid;
    bid = itemInfo.getNextBid(count, bidderId);
    bid.createDate = itemInfo.startDate + currentCreateDateAdvanceStep;
    bid.updateDate = bid.createDate;

    if(remaining == 0){
      bid.maxBid = itemInfo.get_current_price();
    }
    else{
      bid.maxBid = last_bid + currentBidPriceAdvanceStep;
    }


    //ROW generation
    std::vector<std::string> values;

    values.push_back(std::to_string(bid.id)); //ib_id
    values.push_back(itemInfo.get_item_id().encode()); //ib_i_id
    values.push_back(itemInfo.get_seller_id().encode()); //ib_u_id
    values.push_back(bid.bidderId.encode()); //ib_buyer_id
    float price = bid.maxBid - (remaining > 0 ? currentBidPriceAdvanceStep / 2.0 : 0);
    values.push_back(std::to_string(price)); //ib_bid
    values.push_back(std::to_string(bid.maxBid)); //ib_max_bid
    values.push_back(std::to_string(bid.createDate)); //ib_created
    values.push_back(std::to_string(bid.updateDate)); //ib_updated
  
    writer.add_row(TABLE_ITEM_BID, values);
  }
  
}

void GenerateItemMaxBidRow(TableWriter &writer, LoaderItemInfo &itemInfo){
  
   //DATA GENERATION
  bool has_max_bid = itemInfo.bids.size() > 0 ? 1 : 0;
  if(has_max_bid){
    Bid const &bid = itemInfo.getLastBid();

    std::vector<std::string> values;
    
      // IMB_I_ID
    values.push_back(itemInfo.get_item_id().encode());
    
    // IMB_U_ID
    values.push_back(itemInfo.get_seller_id().encode());
  
    // IMB_IB_ID
    values.push_back(std::to_string(bid.id));
    
    // IMB_IB_I_ID
    values.push_back(itemInfo.get_item_id().encode());
    
    // IMB_IB_U_ID
      values.push_back(itemInfo.get_seller_id().encode());

    // IMB_CREATED
    values.push_back(std::to_string(bid.createDate));
    
    // IMB_UPDATED
    values.push_back(std::to_string(bid.updateDate));
    
    writer.add_row(TABLE_ITEM_MAX_BID, values);
  }
}

 void GenerateItemPurchaseRow(TableWriter &writer, LoaderItemInfo &itemInfo, std::mt19937_64 &gen){

   //DATA GENERATION
  
  bool has_purchase = itemInfo.bids.size() > 0 && itemInfo.purchaseDate > 0 ? 1 : 0;
  if(has_purchase){
    Bid &bid = itemInfo.getLastBid();

    std::vector<std::string> values;
    
      // IP_ID
    values.push_back(std::to_string(0)); // //FIXME: Unclear if this is the correct use of count
    
  
    // IP_IB_ID
    values.push_back(std::to_string(bid.id));
    
    // IP_IB_I_ID
    values.push_back(itemInfo.get_item_id().encode());
    
    // IP_IB_U_ID
      values.push_back(itemInfo.get_seller_id().encode());

    // IP_DATE
    values.push_back(std::to_string(itemInfo.purchaseDate));
    
    // IMB_UPDATED
    values.push_back(std::to_string(bid.updateDate));
    
    writer.add_row(TABLE_ITEM_PURCHASE, values);

    if(std::uniform_int_distribution<int>(1, 100)(gen) <= PROB_PURCHASE_BUYER_LEAVES_FEEDBACK){
      bid.buyer_feedback = true;
    }
    if(std::uniform_int_distribution<int>(1, 100)(gen) <= PROB_PURCHASE_SELLER_LEAVES_FEEDBACK){
      bid.seller_feedback = true;
    }
  }
}

//////////////////


void GenerateUserFeedbackRow(TableWriter &writer, AuctionMarkProfile &profile, LoaderItemInfo &itemInfo){
  
  //DATA GEN

  bool was_sold = itemInfo.bids.size() > 0 && itemInfo.purchaseDate > 0 ? 1 : 0;
  if(!was_sold) return;

  Bid const &bid = itemInfo.getLastBid();

  if(bid.buyer_feedback){
    std::vector<std::string> values;
    
    values.push_back(bid.bidderId.encode()); // uf_u_id
    values.push_back(itemInfo.get_item_id().encode()); //uf_i_id
    values.push_back(itemInfo.get_seller_id().encode()); //uf_i_u_id
    values.push_back(itemInfo.get_seller_id().encode()); //uf_from_id
    values.push_back(std::to_string(1)); //uf_rating
    values.push_back(std::to_string(profile.get_loader_start_time())); //uf_date
    
    writer.add_row(TABLE_USERACCT_FEEDBACK, values);
  }

  if(bid.seller_feedback){
    std::vector<std::string> values;
    
    values.push_back(itemInfo.get_seller_id().encode()); // uf_u_id
    values.push_back(itemInfo.get_item_id().encode()); //uf_i_id
    values.push_back(itemInfo.get_seller_id().encode()); //uf_i_u_id
    values.push_back(bid.bidderId.encode()); //uf_from_id
    values.push_back(std::to_string(1)); //uf_rating
    values.push_back(std::to_string(profile.get_loader_start_time())); //uf_date
    
    writer.add_row(TABLE_USERACCT_FEEDBACK, values);
  }
}

void GenerateUserItemRow(TableWriter &writer,  LoaderItemInfo &itemInfo){
  
  //DATA GEN
 
  bool was_sold = itemInfo.bids.size() > 0 && itemInfo.purchaseDate > 0 ? 1 : 0;
  if(!was_sold) return;

  Bid const &bid = itemInfo.getLastBid();

  std::vector<std::string> values;
  
  values.push_back(bid.bidderId.encode()); // ui_u_id
  values.push_back(itemInfo.get_item_id().encode()); //ui_i_id
  values.push_back(itemInfo.get_seller_id().encode()); //ui_i_u_id
  //TODO: Technically these are all "null"
  values.push_back(std::to_string(-1)); //ui_ip_id
  values.push_back(std::to_string(-1)); //ui_ip_ib_id
  values.push_back("\"\""); //ui_ip_ib_i_id
  values.push_back("\"\"");//ui_ip_ib_u_id
  values.push_back(std::to_string(itemInfo.get_end_date()));//ui_created
  
  writer.add_row(TABLE_USERACCT_ITEM, values);
}

void GenerateUserWatchRow(TableWriter &writer, AuctionMarkProfile &profile, LoaderItemInfo &itemInfo, std::mt19937_64 &gen){
  

  //DATA GEN
  std::set<UserId> watchers; 

  for(int i = 0; i < itemInfo.numWatches; ++i){
    auto &bidderHistogram = itemInfo.bidderHistogram;
    UserId buyerId;
    bool use_random = itemInfo.numWatches == bidderHistogram.size();
    uint64_t num_watchers = watchers.size();
    uint64_t num_users = TABLESIZE_USERACCT;

    int tries = 1000; //find new watcher
    while(num_watchers < num_users && tries-- > 0){
      if(use_random){
        buyerId = profile.get_random_buyer_id();
      }
      else{
        buyerId = profile.get_random_buyer_id(bidderHistogram, {itemInfo.get_seller_id()});
      }
      if(watchers.insert(buyerId).second) break;
      buyerId = UserId();

        // If for some reason we unable to find a buyer from our bidderHistogram, then just give up and get a random one
      if(!use_random && tries == 0){
        use_random = true;
        tries = 500;
      }
    }

    //Generate row
    std::vector<std::string> values;
  
    values.push_back(buyerId.encode()); // uw_u_id
    values.push_back(itemInfo.get_item_id().encode()); //uw_i_id
    values.push_back(itemInfo.get_seller_id().encode()); //uw_i_u_id
    values.push_back(std::to_string(getRandomDate(itemInfo.startDate, itemInfo.get_end_date(), gen)));//uw_created
    
    writer.add_row(TABLE_USERACCT_WATCH, values);
  }
}
 
} //namespace auctionmark



int main(int argc, char *argv[]) {

  auto start_time = std::time(0);
  
  gflags::SetUsageMessage("generates a json file containing sql tables for AuctionMark data\n");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  std::string file_name = "sql-auctionmark";
  TableWriter writer = TableWriter(file_name);

  std::cerr << "Starting AUCTIONMARK Table Generation. Num Clients: " << FLAGS_client_total << ". Scale Factor: " << FLAGS_scale_factor << std::endl;

  std::mt19937_64 gen;
 
  auctionmark::AuctionMarkProfile profile(-1, FLAGS_client_total, FLAGS_scale_factor);
  struct timeval time;
  gettimeofday(&time, NULL);
  profile.set_loader_start_time(auctionmark::get_ts(time));

  std::cerr << "loader_start_time: " << profile.get_loader_start_time() << std::endl;

  std::cerr << "Load Schemas" << std::endl;

  auctionmark::RegionTableSchema(writer);
  auctionmark::CategoryTableSchema(writer);
  auctionmark::GlobalAttrGroupTableSchema(writer);
  auctionmark::GlobalAttrGroupValueTableSchema(writer);
    
  auctionmark::UserAcctTableSchema(writer);
  auctionmark::ItemTableSchema(writer);

  auctionmark::ItemImageTableSchema(writer);
  auctionmark::ItemAttributeTableSchema(writer);
  auctionmark::ItemCommentTableSchema(writer);
  auctionmark::ItemBidTableSchema(writer);
  auctionmark::ItemMaxBidTableSchema(writer);
  auctionmark::ItemPurchaseTableSchema(writer);

  auctionmark::UserFeedbackTableSchema(writer);
  auctionmark::UserItemTableSchema(writer);
  auctionmark::UserWatchTableSchema(writer);

  std::cerr << "Finished Schemas" << std::endl;

  std::cerr << "Start Generating Data" << std::endl;
  auctionmark::GenerateRegionTable(writer);
  int n_categories = auctionmark::GenerateCategoryTable(writer);
  int n_gags = auctionmark::GenerateGlobalAttributeGroupTable(writer, n_categories, profile);
  auctionmark::GenerateGlobalAttributeValueTable(writer, profile, n_gags);

  std::cerr << "Finished General Tables" << std::endl;

  //Generate UserTables
  std::vector<auctionmark::UserId> users = auctionmark::GenerateUserAcctTable(writer, profile);
  std::cerr << "Finished UserAcct Table" << std::endl;


  std::vector<auctionmark::LoaderItemInfo> items = auctionmark::GenerateItemTableData(writer, profile, users);
  std::cerr << "Finished Item Table" << std::endl;

  // auctionmark::GenerateItemImage(writer, items);
  // std::cerr << "Finished ItemImage Table" << std::endl;
  // auctionmark::GenerateItemAttribute(writer, profile, items);
  // std::cerr << "Finished ItemAttribute Table" << std::endl;
  // auctionmark::GenerateItemComment(writer, items);
  // std::cerr << "Finished ItemComment Table" << std::endl;
  // auctionmark::GenerateItemBid(writer, profile, items);
  // std::cerr << "Finished ItemBid Table" << std::endl;
  // auctionmark::GenerateItemMaxBid(writer, items);
  // std::cerr << "Finished ItemMaxBid Table" << std::endl;
  // auctionmark::GenerateItemPurchase(writer, items);
  // std::cerr << "Finished ItemPurchase Table" << std::endl;

   std::cerr << "Finished all Item* Tables" << std::endl;

  // auctionmark::GenerateUserFeedback(writer, profile, items);
  // std::cerr << "Finished UserFeedback Table" << std::endl;
  // auctionmark::GenerateUserItem(writer, items);
  // std::cerr << "Finished UserItem Table" << std::endl;
  // auctionmark::GenerateUserWatch(writer, profile, items);
  // std::cerr << "Finished UserWatch Table" << std::endl;

   std::cerr << "Finished all User* Tables" << std::endl;

  writer.flush();

  //Serialize profile.
  gettimeofday(&time, NULL);
  profile.set_loader_stop_time(auctionmark::get_ts(time));
  profile.save_profile();
  std::cerr << "Saved profile." << std::endl;

   auto end_time = std::time(0);
    std::cerr << "Finished AUCTIONMARK Table Generation. Took " << (end_time - start_time) << "seconds" << std::endl;
  return 0;
}



//OLD DIRECT GENERATORS

/*
void GenerateItemImage(TableWriter &writer, std::vector<LoaderItemInfo> &items){
  std::string table_name = TABLE_ITEM_IMAGE;
  
  //DATA GEN
  for(auto &item: items){
    for(int count = 0; count < item.numImages; ++count){
      std::vector<std::string> values;
      values.push_back(std::to_string(count)); //ii_id   //FIXME: Unclear if this is the correct use of count
      values.push_back(item.get_item_id().encode()); //ii_i_id
      values.push_back(item.get_seller_id().encode()); //ii_u_id
      writer.add_row(table_name, values);
    }
  }
}

void GenerateItemAttribute(TableWriter &writer, AuctionMarkProfile &profile, std::vector<LoaderItemInfo> &items){
  std::string table_name = TABLE_ITEM_ATTR;
  
  //DATA GENERATION
  for(auto &item: items){
    for(int count = 0; count < item.numAttributes; ++count){
      std::vector<std::string> values;
      values.push_back(std::to_string(count)); //ia_id  //FIXME: Unclear if this is the correct use of count
      values.push_back(item.get_item_id().encode()); //ia_i_id
      values.push_back(item.get_seller_id().encode()); //ia_u_id
    
      GlobalAttributeValueId gav_id = profile.get_random_global_attribute_value();
      values.push_back(gav_id.encode()); //ia_gav_id
      values.push_back(gav_id.get_global_attribute_group().encode()); //ia_gag_id
      writer.add_row(table_name, values);
    }
  }
}



void GenerateItemComment(TableWriter &writer, std::vector<LoaderItemInfo> &items){
  std::string table_name = TABLE_ITEM_COMMENT;
  
  //DATA GENERATION
  std::mt19937_64 gen;

  for(auto &itemInfo: items){
    int total = itemInfo.purchaseDate > 0 ? itemInfo.numComments : 0;
    for(int count = 0; count < total; ++count){
      std::vector<std::string> values;
      values.push_back(std::to_string(count)); //ic_id     //FIXME: Unclear if this is the correct use of count
      values.push_back(itemInfo.get_item_id().encode()); //ic_i_id
      values.push_back(itemInfo.get_seller_id().encode()); //ic_u_id
      values.push_back(itemInfo.lastBidderId.encode()); //ic_buyer_id

      values.push_back(RandomAString(ITEM_COMMENT_LENGTH_MIN, ITEM_COMMENT_LENGTH_MAX, gen));//ic_question

      values.push_back(RandomAString(ITEM_COMMENT_LENGTH_MIN, ITEM_COMMENT_LENGTH_MAX, gen));//ic_response
      
      uint64_t t = getRandomCommentDate(itemInfo.startDate, itemInfo.get_end_date(), gen);
      values.push_back(std::to_string(t));//ic_created
      values.push_back(std::to_string(t));//ic_updated

      writer.add_row(table_name, values);
    }
  }
}

void GenerateItemBid(TableWriter &writer, AuctionMarkProfile &profile, std::vector<LoaderItemInfo> &items){
   std::string table_name = TABLE_ITEM_BID;
 
   //DATA GENERATION
  for(auto &itemInfo: items){
    Bid bid;
    bool new_item = true;
    //TODO: Should these three not reset between items?
    float currentPrice;
    float currentBidPriceAdvanceStep;
    uint64_t currentCreateDateAdvanceStep;

    int total = itemInfo.get_num_bids();
    for(int count = 0; count < total; ++count){
      int remaining = total - count - 1;

      UserId bidderId;
      // Figure out the UserId for the person bidding on this item now

      if(new_item) {
        // If this is a new item and there is more than one bid, then  we'll choose the bidder's UserId at random.
        // If there is only one bid, then it will have to be the last bidder
        bidderId = itemInfo.get_num_bids() == 1 ? itemInfo.lastBidderId : profile.get_random_buyer_id(itemInfo.get_seller_id());
        uint64_t endDate;
        if(itemInfo.get_status() == ItemStatus::OPEN){
          endDate = profile.get_loader_start_time();
        }
        else{
          endDate = itemInfo.get_end_date();
        }
        currentCreateDateAdvanceStep = (endDate - itemInfo.startDate) / (remaining + 1);
        currentBidPriceAdvanceStep = itemInfo.initialPrice * ITEM_BID_PERCENT_STEP;
        currentPrice = itemInfo.initialPrice;
      }
      else if(count == total){
         // The last bid must always be the item's lastBidderId
         bidderId = itemInfo.lastBidderId;
         currentPrice = itemInfo.get_current_price();
      }
      else if(total == 2){
         // The first bid for a two-bid item must always be different than the lastBidderId
        bidderId = profile.get_random_buyer_id({itemInfo.lastBidderId, itemInfo.get_seller_id()});
      }
      else{
         // Since there are multiple bids, we want randomly select one based on the previous bidders
        // We will get the histogram of bidders so that we are more likely to select an existing bidder rather than a completely random one
        auto &bidderHistogram = itemInfo.bidderHistogram;
        bidderId = profile.get_random_buyer_id(bidderHistogram, {bid.bidderId, itemInfo.get_seller_id()});
        currentPrice += currentBidPriceAdvanceStep;
      }

      //Update bid info
      float last_bid = new_item? itemInfo.initialPrice : bid.maxBid;
      bid = itemInfo.getNextBid(count, bidderId);
      bid.createDate = itemInfo.startDate + currentCreateDateAdvanceStep;
      bid.updateDate = bid.createDate;

      if(remaining == 0){
        bid.maxBid = itemInfo.get_current_price();
      }
      else{
        bid.maxBid = last_bid + currentBidPriceAdvanceStep;
      }


      //ROW generation
      std::vector<std::string> values;

      values.push_back(std::to_string(bid.id)); //ib_id
      values.push_back(itemInfo.get_item_id().encode()); //ib_i_id
      values.push_back(itemInfo.get_seller_id().encode()); //ib_u_id
      values.push_back(bid.bidderId.encode()); //ib_buyer_id
      float price = bid.maxBid - (remaining > 0 ? currentBidPriceAdvanceStep / 2.0 : 0);
      values.push_back(std::to_string(price)); //ib_bid
      values.push_back(std::to_string(bid.maxBid)); //ib_max_bid
      values.push_back(std::to_string(bid.createDate)); //ib_created
      values.push_back(std::to_string(bid.updateDate)); //ib_updated
    
      writer.add_row(table_name, values);
    }
  }
}

void GenerateItemMaxBid(TableWriter &writer, std::vector<LoaderItemInfo> &items){
  std::string table_name = TABLE_ITEM_MAX_BID;
  
   //DATA GENERATION
  for(auto &itemInfo: items){
    bool has_max_bid = itemInfo.bids.size() > 0 ? 1 : 0;
    if(has_max_bid){
      Bid const &bid = itemInfo.getLastBid();

      std::vector<std::string> values;
     
       // IMB_I_ID
      values.push_back(itemInfo.get_item_id().encode());
      
      // IMB_U_ID
      values.push_back(itemInfo.get_seller_id().encode());
    
      // IMB_IB_ID
      values.push_back(std::to_string(bid.id));
      
      // IMB_IB_I_ID
      values.push_back(itemInfo.get_item_id().encode());
     
      // IMB_IB_U_ID
       values.push_back(itemInfo.get_seller_id().encode());

      // IMB_CREATED
      values.push_back(std::to_string(bid.createDate));
     
      // IMB_UPDATED
      values.push_back(std::to_string(bid.updateDate));
     
      writer.add_row(table_name, values);
    }
  }
}

 void GenerateItemPurchase(TableWriter &writer, std::vector<LoaderItemInfo> &items){
   std::string table_name = TABLE_ITEM_PURCHASE;

   //DATA GENERATION
  std::mt19937_64 gen;

  for(auto &itemInfo: items){  
    bool has_purchase = itemInfo.bids.size() > 0 && itemInfo.purchaseDate > 0 ? 1 : 0;
    if(has_purchase){
      Bid &bid = itemInfo.getLastBid();

      std::vector<std::string> values;
     
       // IP_ID
      values.push_back(std::to_string(0)); // //FIXME: Unclear if this is the correct use of count
      
    
      // IP_IB_ID
      values.push_back(std::to_string(bid.id));
      
      // IP_IB_I_ID
      values.push_back(itemInfo.get_item_id().encode());
     
      // IP_IB_U_ID
       values.push_back(itemInfo.get_seller_id().encode());

      // IP_DATE
      values.push_back(std::to_string(itemInfo.purchaseDate));
     
      // IMB_UPDATED
      values.push_back(std::to_string(bid.updateDate));
     
      writer.add_row(table_name, values);

      if(std::uniform_int_distribution<int>(1, 100)(gen) <= PROB_PURCHASE_BUYER_LEAVES_FEEDBACK){
        bid.buyer_feedback = true;
      }
      if(std::uniform_int_distribution<int>(1, 100)(gen) <= PROB_PURCHASE_SELLER_LEAVES_FEEDBACK){
        bid.seller_feedback = true;
      }

    }
  }
}

//////////////////


void GenerateUserFeedback(TableWriter &writer, AuctionMarkProfile &profile, std::vector<LoaderItemInfo> &items){
  std::string table_name = TABLE_USERACCT_FEEDBACK;
  
  //DATA GEN
  for(auto &itemInfo: items){  
    bool was_sold = itemInfo.bids.size() > 0 && itemInfo.purchaseDate > 0 ? 1 : 0;
    if(!was_sold) continue;

    Bid const &bid = itemInfo.getLastBid();

    if(bid.buyer_feedback){
      std::vector<std::string> values;
     
      values.push_back(bid.bidderId.encode()); // uf_u_id
      values.push_back(itemInfo.get_item_id().encode()); //uf_i_id
      values.push_back(itemInfo.get_seller_id().encode()); //uf_i_u_id
      values.push_back(itemInfo.get_seller_id().encode()); //uf_from_id
      values.push_back(std::to_string(1)); //uf_rating
      values.push_back(std::to_string(profile.get_loader_start_time())); //uf_date
      
      writer.add_row(table_name, values);
    }

    if(bid.seller_feedback){
      std::vector<std::string> values;
     
      values.push_back(itemInfo.get_seller_id().encode()); // uf_u_id
      values.push_back(itemInfo.get_item_id().encode()); //uf_i_id
      values.push_back(itemInfo.get_seller_id().encode()); //uf_i_u_id
      values.push_back(bid.bidderId.encode()); //uf_from_id
      values.push_back(std::to_string(1)); //uf_rating
      values.push_back(std::to_string(profile.get_loader_start_time())); //uf_date
      
      writer.add_row(table_name, values);
    }
  }
}

void GenerateUserItem(TableWriter &writer, std::vector<LoaderItemInfo> &items){
  std::string table_name = TABLE_USERACCT_ITEM;
  
  //DATA GEN
  for(auto &itemInfo: items){  
    bool was_sold = itemInfo.bids.size() > 0 && itemInfo.purchaseDate > 0 ? 1 : 0;
    if(!was_sold) continue;

    Bid const &bid = itemInfo.getLastBid();

    std::vector<std::string> values;
    
    values.push_back(bid.bidderId.encode()); // ui_u_id
    values.push_back(itemInfo.get_item_id().encode()); //ui_i_id
    values.push_back(itemInfo.get_seller_id().encode()); //ui_i_u_id
    //TODO: Technically these are all "null"
    values.push_back(std::to_string(-1)); //ui_ip_id
    values.push_back(std::to_string(-1)); //ui_ip_ib_id
    values.push_back("\"\""); //ui_ip_ib_i_id
    values.push_back("\"\"");//ui_ip_ib_u_id
    values.push_back(std::to_string(itemInfo.get_end_date()));//ui_created
    
    writer.add_row(table_name, values);
  }
}

void GenerateUserWatch(TableWriter &writer, AuctionMarkProfile &profile, std::vector<LoaderItemInfo> &items){
   
  std::string table_name = TABLE_USERACCT_WATCH;

  //DATA GEN
  std::mt19937_64 gen;

  for(auto &itemInfo: items){  
    
    std::set<UserId> watchers; 

    for(int i = 0; i < itemInfo.numWatches; ++i){
      auto &bidderHistogram = itemInfo.bidderHistogram;
      UserId buyerId;
      bool use_random = itemInfo.numWatches == bidderHistogram.size();
      uint64_t num_watchers = watchers.size();
      uint64_t num_users = TABLESIZE_USERACCT;

      int tries = 1000; //find new watcher
      while(num_watchers < num_users && tries-- > 0){
        if(use_random){
          buyerId = profile.get_random_buyer_id();
        }
        else{
          buyerId = profile.get_random_buyer_id(bidderHistogram, {itemInfo.get_seller_id()});
        }
        if(watchers.insert(buyerId).second) break;
        buyerId = UserId();

         // If for some reason we unable to find a buyer from our bidderHistogram, then just give up and get a random one
        if(!use_random && tries == 0){
          use_random = true;
          tries = 500;
        }
      }

      //Generate row
      std::vector<std::string> values;
    
      values.push_back(buyerId.encode()); // uw_u_id
      values.push_back(itemInfo.get_item_id().encode()); //uw_i_id
      values.push_back(itemInfo.get_seller_id().encode()); //uw_i_u_id
      values.push_back(std::to_string(getRandomDate(itemInfo.startDate, itemInfo.get_end_date(), gen)));//uw_created
      
      writer.add_row(table_name, values);
    }
  }
}
 
} //namespace auctionmark



int main(int argc, char *argv[]) {

  auto start_time = std::time(0);
  
  gflags::SetUsageMessage("generates a json file containing sql tables for AuctionMark data\n");
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  std::string file_name = "sql-auctionmark";
  TableWriter writer = TableWriter(file_name);

  std::cerr << "Starting AUCTIONMARK Table Generation. Num Clients: " << FLAGS_client_total << ". Scale Factor: " << FLAGS_scale_factor << std::endl;

  std::mt19937_64 gen;
 
  auctionmark::AuctionMarkProfile profile(-1, FLAGS_client_total, FLAGS_scale_factor);
  struct timeval time;
  gettimeofday(&time, NULL);
  profile.set_loader_start_time(auctionmark::get_ts(time));

  std::cerr << "loader_start_time: " << profile.get_loader_start_time() << std::endl;

  
  auctionmark::GenerateRegionTable(writer);
  int n_categories = auctionmark::GenerateCategoryTable(writer);
  int n_gags = auctionmark::GenerateGlobalAttributeGroupTable(writer, n_categories, profile);
  auctionmark::GenerateGlobalAttributeValueTable(writer, profile, n_gags);

  std::cerr << "Finished General Tables" << std::endl;

  //Generate UserTables
  std::vector<auctionmark::UserId> users = auctionmark::GenerateUserAcctTable(writer, profile);
  std::cerr << "Finished UserAcct Table" << std::endl;

  std::vector<auctionmark::LoaderItemInfo> items = auctionmark::GenerateItemTable(writer, profile, users);
  std::cerr << "Finished Item Table" << std::endl;

  // auctionmark::GenerateItemImage(writer, items);
  // auctionmark::GenerateItemAttribute(writer, profile, items);
  // auctionmark::GenerateItemComment(writer, items);
  // auctionmark::GenerateItemBid(writer, profile, items);
  // auctionmark::GenerateItemMaxBid(writer, items);
  // auctionmark::GenerateItemPurchase(writer, items);

  // std::cerr << "Finished Item* Tables" << std::endl;

  // auctionmark::GenerateUserFeedback(writer, profile, items);
  // auctionmark::GenerateUserItem(writer, items);
  // auctionmark::GenerateUserWatch(writer, profile, items);
  // std::cerr << "Finished User* Tables" << std::endl;

  // //TODO: Serialize profile.
  //  profile.set_loader_stop_time(std::chrono::system_clock::now());

  writer.flush();
  // std::cerr << "Wrote tables." << std::endl;

   auto end_time = std::time(0);
    std::cerr << "Finished AUCTIONMARK Table Generation. Took " << (end_time - start_time) << "seconds" << std::endl;
  return 0;
}
*/
