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
// #include <boost/histogram.hpp>

#include "store/benchmark/async/json_table_writer.h"
#include "store/benchmark/async/sql/auctionmark/auctionmark_params.h"
#include "store/benchmark/async/sql/auctionmark/auctionmark_profile.h"

#include "store/benchmark/async/sql/auctionmark/utils/category_parser.h"
#include "store/benchmark/async/sql/auctionmark/utils/auctionmark_utils.h"
#include "store/benchmark/async/sql/auctionmark/utils/flat_histogram.h"

#include "store/benchmark/async/sql/auctionmark/utils/global_attribute_group_id.h"
#include "store/benchmark/async/sql/auctionmark/utils/global_attribute_value_id.h"
#include "store/benchmark/async/sql/auctionmark/auctionmark_generator.h"


DEFINE_int32(client_total, 100, "number of clients");
 //FIXME: THIS IS A PROBLEM. NOT REALLY COMPATIBLE WITH OUR EXPERIMENTAL FRAMEWORK
                          //HOW DOES client id impact contention?
//TODO: Instead of static loading, write a script that calls the generator on demand
//TODO: Could multithread the generation...


namespace auctionmark {
//Read only tables

void GenerateRegionTable(TableWriter &writer)
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
  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("c_id", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("c_name", "TEXT"));
  column_names_and_types.push_back(std::make_pair("c_parent_id", "BIGINT"));
  const std::vector<uint32_t> primary_key_col_idx{0};

  std::string table_name = TABLE_CATEGORY;
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);

  
  const std::vector<uint32_t> index {2};
  writer.add_index(table_name, "idx_category_parent", index);

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

  //SCHEMA

  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("gag_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("gag_c_id", "BIGINT"));
  column_names_and_types.push_back(std::make_pair("gag_name", "TEXT"));
  const std::vector<uint32_t> primary_key_col_idx{0};

  std::string table_name = TABLE_GLOBAL_ATTR_GROUP;
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);

  //DATA GEN

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
    values.push_back(gag_id.get_category_id());
    values.push_back(RandomAString(6, 32, gen));
    writer.add_row(table_name, values);
  }

  return total_count;
}

void GenerateGlobalAttributeValueTable(TableWriter &writer, AuctionMarkProfile &profile, int GAV_size) //GAV_size == total_count of gag
{
  //SCHEMA
  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("gav_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("gav_gag_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("gav_name", "TEXT"));
  const std::vector<uint32_t> primary_key_col_idx{0, 1};

  std::string table_name = TABLE_GLOBAL_ATTR_VALUE;
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);

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
      writer.add_row(table_name, values);
    }
  }
}




std::vector<UserId> GenerateUserAcctTable(TableWriter &writer, AuctionMarkProfile &profile,) 
{
 
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
  std::string table_name = TABLE_USERACCT;
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);

  const std::vector<uint32_t> index {0, 4};
  writer.add_index(table_name, "idx_useracct_region", index);

  //DATA GEN

   std::mt19937_64 gen;
  
  Zipf randomRating(gen, USER_MIN_RATING, USER_MAX_RATING, 1.0001);
  Zipf randomBalance(gen, USER_MIN_BALANCE, USER_MAX_BALANCE, 1.001);

  int max_items = max(1, ceil(ITEM_ITEMS_PER_SELLER_MAX * SCALE_FACTOR));
  Zipf randomNumItems(gen, ITEM_ITEMS_PER_SELLER_MIN, max_items, ITEM_ITEMS_PER_SELLER_SIGMA);

   //A histogram for the number of users that have the number of items listed ItemCount -> # of Users
  profile.users_per_item_count = std::vector<int>(max_items);

  for (int i = 0; i < TABLESIZE_USERACCT; i++){
    int num_items = (int) randomNumItems.next_long();
    profile.users_per_item_count[num_items]++;   //increment value freq by 1 count.
  }

  UserIdGenerator idGenerator(profile.users_per_item_count, FLAGS_client_total);

  std::vector<UserId> user_ids;

  for (int i = 0; i < TABLESIZE_USERACCT; i++)
  {
    UserId u_id = idGenerator.next();
    user_ids.push_back(u_id);

    std::vector<std::string> values;
    values.push_back(u_id.encode()); //u_id
    Zipf zipf;
    values.push_back(std::to_string(randomRating.next_long())); //u_rating
    double bal = randomBalance.next_long()/10.0;
    values.push_back(std::to_string(bal)); //u_balance
    values.push_back(std::to_string(0)); //u_comments
    values.push_back(std::to_string(std::uniform_int_distribution<int>(0, TABLESIZE_REGION)(gen))); //u_r_id
    uint64_t curr_time = get_ts(timestamp_t::clock().now());
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
      values.push_back(std::uniform_int_distribution<int>(0, 1 <<30)(gen));
    }


     writer.add_row(table_name, values);
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


void ItemTable(TableWriter &writer){
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

  //DATA GEN
}


std::vector<LoaderItemInfo> GenerateItemTable(TableWriter &writer, AuctionMarkProfile &profile, std::vector<UserId> user_ids){

  std::vector<LoaderItemInfo> items;

  int tableSize = 0;
  for(int i = 0; i < profile.users_per_item_count.size(); ++i){
    tableSize += i * profile.users_per_item_count[i];
  }
  
  int remaining = tableSize; 

  for(auto &seller_id: user_ids){
    ItemId itemId(seller_id, remaining);
    if(--remaining == 0) break;

    
    timestamp_t endDate = 0; //TODO:
    timestamp_t startDate = 0; //TODO:

    uint64_t bidDurationDay = get_ts(endDate - startDate) / MILLISECONDS_IN_A_DAY;

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
    int numBids = p.first.next_long();
    int numWatches = p.first.next_long();

    // Create the ItemInfo object that we will use to cache the local data

    // tables are done with it.
    LoaderItemInfo itemInfo(itemId, endDate, numBids);
    itemInfo.startDate = get_ts(startDate);
    itemInfo.initialPrice = profile.random_initial_price.next_long();
    itemInfo.numImages = profile.random_num_images.next_long();
    itemInfo.numAttributes = profile.random_num_attributes.next_long();
    itemInfo.numBids = numBids;
    itemInfo.numWatches = numWatches;

     // The auction for this item has already closed
    if(itemInfo.endDate <= profile.get_loader_start_time()){
      // Somebody won a bid and bought the item
      if (itemInfo.get_num_bids() > 0) {
        itemInfo.lastBidderId = profile.get_random_buyer_id(itemInfo.get_seller_id());
        itemInfo.purchaseDate = getRandomPurchaseTimestamp(get_ts(endDate), profile);
        itemInfo.numComments = profile.random_num_comments.next_long();
      }
      itemInfo.set_status(ItemStatus::CLOSED);
    }
    // Item is still available
    else if (itemInfo.get_num_bids > 0) {
      itemInfo.lastBidderId = profile.get_random_buyer_id(itemInfo.get_seller_id());
    }
    profile.add_item_to_proper_queue(itemInfo, true);

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
     values.push_back(std::to_string(*itemInfo.get_current_price()));
    } else {
      values.push_back(std::to_string(itemInfo.initialPrice));
    }
   
    values.push_back(std::to_string(itemInfo.get_num_bids()));// I_NUM_BIDS
    values.push_back(std::to_string(itemInfo.numImages));// I_NUM_IMAGES
    values.push_back(std::to_string(itemInfo.numAttributes));// I_NUM_GLOBAL_ATTRS
    values.push_back(std::to_string(itemInfo.numAttributes));// I_NUM_COMMENTS
 
    values.push_back(std::to_string(itemInfo.startDate));// I_START_DATE
  
    values.push_back(std::to_string(get_ts(endDate)));// I_END_DATE
  
    values.push_back(std::to_string(*itemInfo.get_status()));// I_STATUS
  
    uint64_t start_time = get_ts(profile.get_loader_start_time());
    values.push_back(std::to_string(start_time));// I_CREATED
  
    values.push_back(std::to_string(itemInfo.startDate));// I_UPDATED
  
    writer.add_row(TABLE_ITEM, values);

    items.push_back(itemInfo);
  }
  return items;
}

void GenerateItemImage(TableWriter &writer, std::vector<LoaderItemInfo> &items){
  std::vector<std::pair<std::string, std::string>> column_names_and_types;
  column_names_and_types.push_back(std::make_pair("ii_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("ii_i_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("ii_u_id", "TEXT"));
  column_names_and_types.push_back(std::make_pair("ia_sattr0", "TEXT"));
 
  const std::vector<uint32_t> primary_key_col_idx{0, 1, 2};

  std::string table_name = TABLE_ITEM_IMAGE;
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);

  //DATA GEN
  for(auto &item: items){
    for(int count = 0; count < item.numImages; ++count){
      std::vector<std::string> values;
      values.push_back(std::to_string(count)); //ii_id
      values.push_back(item.get_item_id().encode()); //ii_i_id
      values.push_back(item.get_seller_id().encode()); //ii_u_id
      writer.add_row(table_name, values);
    }
  }
}

void GenerateItemAttribute(TableWriter &writer, std::vector<LoaderItemInfo> &items){
  //SCHEMA
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

  //DATA GENERATION
  for(auto &item: items){
    for(int count = 0; count < item.numAttributes; ++count){
      std::vector<std::string> values;
      values.push_back(std::to_string(count)); //ia_id
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

  //DATA GENERATION
  for(auto &item: items){
    int total = itemInfo.purchaseDate > 0 ? itemInfo.numComments : 0;
    for(int count = 0; count < totals; ++count){
      std::vector<std::string> values;
      values.push_back(std::to_string(count)); //ic_id
      values.push_back(item.get_item_id().encode()); //ic_i_id
      values.push_back(item.get_seller_id().encode()); //ic_u_id
      values.push_back(item.lastBidderId.encode()); //ic_buyer_id

      values.push_back(RandomAString(ITEM_COMMENT_LENGTH_MIN, ITEM_COMMENT_LENGTH_MAX, gen));//ic_question

      values.push_back(RandomAString(ITEM_COMMENT_LENGTH_MIN, ITEM_COMMENT_LENGTH_MAX, gen));//ic_response
      
      uint64_t t = getRandomCommentDate(itemInfo.startDate, get_ts(*itemInfo.get_end_date()));
      values.push_back(std::to_string(t));//ic_created
      values.push_back(std::to_string(t));//ic_updated

      writer.add_row(table_name, values);
    }
  }
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

void GenerateItemMaxBid(TableWriter &writer, std::vector<LoaderItemInfo> &items){
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

void GenerateItemPurchase(TableWriter &writer, std::vector<LoaderItemInfo> &items){
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

//////////////////

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

  std::string table_name = TABLE_USERACCT_FEEDBACK;
  writer.add_table(table_name, column_names_and_types, primary_key_col_idx);

  //DATA
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
 
} //namespace auctionmark



int main(int argc, char *argv[]) {

  AuctionMarkProfile profile;
  profile.set_loader_time();
  int x = 4;
  // gflags::SetUsageMessage(
  //     "generates a json file containing sql tables for AuctionMark data\n");
  // std::string file_name = "auctionmark";
  // TableWriter writer = TableWriter(file_name);

  // GenerateRegionTable(writer, auctionmark::N_REGIONS);
  // int n_categories = GenerateCategoryTable(writer);
  // std::set<uint32_t> gag_ids = GenerateGlobalAttributeGroupTable(writer, n_categories, auctionmark::N_GAGS, auctionmark::GAV_PER_GROUP);
  // GenerateGlobalAttributeValueTable(writer, auctionmark::GAV_PER_GROUP, gag_ids);
  // GenerateUserAcctTable(writer, auctionmark::N_REGIONS, auctionmark::N_USERS);
  writer.flush();
  // std::cerr << "Wrote tables." << std::endl;
  return 0;
}

