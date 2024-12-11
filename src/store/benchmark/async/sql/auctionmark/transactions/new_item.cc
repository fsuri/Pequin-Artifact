/***********************************************************************
 *
 * Copyright 2021 Florian Suri-Payer <fsp@cs.cornell.edu>
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
#include "store/benchmark/async/sql/auctionmark/transactions/new_item.h"
#include "store/benchmark/async/sql/auctionmark/utils/auctionmark_utils.h"
#include <fmt/core.h>

namespace auctionmark {


NewItem::NewItem(uint32_t timeout, AuctionMarkProfile &profile, std::mt19937_64 &gen) : AuctionMarkTransaction(timeout), profile(profile), gen(gen) {
  

  UserId sellerId = profile.get_random_seller_id(profile.get_client_id());
  ItemId itemId = profile.get_next_item_id(sellerId); 
 
  item_id = itemId.encode();
  seller_id = sellerId.encode();
//  std::cerr << "NEW ITEM: " << item_id << ", seller: " << seller_id << std::endl;
//  std::cerr << "client id: " << profile.get_client_id() << std::endl;


  name = RandomAString(6, 32, gen);
  description = RandomAString(50, 255, gen);
  category_id = profile.get_random_category_id();

  initial_price = profile.random_initial_price.next_long();
  attributes = RandomAString(50, 255, gen);

  int numAttributes = profile.random_num_attributes.next_long();
  std::set<GlobalAttributeValueId> gavList = {}; 
  UW_ASSERT(numAttributes > 0);
  for (int i = 0; i < numAttributes; i++) {
    GlobalAttributeValueId gav_id = profile.get_random_global_attribute_value();
    gavList.insert(gav_id);
  }

  for(auto &gav_id: gavList){
    gag_ids.push_back(gav_id.get_global_attribute_group().encode());
    gav_ids.push_back(gav_id.encode());
  }

  int numImages = profile.random_num_images.next_long();
  for (int i = 0; i < numImages; i++) {
    images.push_back(RandomAString(20, 100, gen));
  }
  UW_ASSERT(!gag_ids.empty());
  UW_ASSERT(!gav_ids.empty());
  UW_ASSERT(!images.empty());

  duration = profile.get_random_duration();
  //std::binomial_distribution<uint64_t>(ITEM_DURATION_DAYS_MAX-1, 0.5)(gen) + 1;  //gives a val between 1 (DAYS_MIN) and 10 (DAYS_MAX) with normal distribution


}

NewItem::~NewItem(){
}

transaction_status_t NewItem::Execute(SyncClient &client) {
  std::unique_ptr<const query_result::QueryResult> queryResult;
  std::string statement;
  std::vector<std::unique_ptr<const query_result::QueryResult>> results = {};

  //Insert a new ITEM record for user.
  Debug("NEW ITEM");
  Debug("ItemID: %s", item_id.c_str());

  client.Begin(timeout);

  uint64_t current_time = GetProcTimestamp({profile.get_loader_start_time(), profile.get_client_start_time()});
  uint64_t end_date = current_time + (duration * MILLISECONDS_IN_A_DAY);

  std::string updateUserBalance = fmt::format("UPDATE {} SET u_balance = u_balance -1, u_updated = {} WHERE u_id = '{}'", TABLE_USERACCT, current_time, seller_id);
  client.Write(updateUserBalance, timeout, true);

  //Get attribute names and category path and append them to the item description

  //ATTRIBUTES
  description += "\nATTRIBUTES: ";
  //  std::string getGlobablAttribute = fmt::format("SELECT gag_name, gav_name, gag_c_id FROM {}, {}"
  //                                   " WHERE gav_id = '{}' AND gav_gag_id = '{}' AND gav_gag_id = gag_id", TABLE_GLOBAL_ATTR_GROUP, TABLE_GLOBAL_ATTR_VALUE); //TODO: add redundant input?
   std::string getGlobablAttribute = "SELECT gag_name, gav_name, gag_c_id FROM " + std::string(TABLE_GLOBAL_ATTR_GROUP) + ", " + std::string(TABLE_GLOBAL_ATTR_VALUE) +
                                     " WHERE gav_id = '{}' AND gav_gag_id = '{}' AND gav_gag_id = gag_id AND gag_id = '{}'"; //TODO: add redundant input?


  for(int i = 0; i < gag_ids.size(); ++i){
    std::string stmt = fmt::format(getGlobablAttribute, gav_ids[i], gag_ids[i], gag_ids[i]);
    client.Query(stmt, timeout);
  }                             
                                
  //CATEGORY 
  std::string getCategory = fmt::format("SELECT * FROM {} WHERE c_id = {}", TABLE_CATEGORY, category_id);
  client.Query(getCategory, timeout);
 

  //CATEGORY PARENT
  std::string getCategoryParent = fmt::format("SELECT * FROM {} WHERE c_parent_id = {}", TABLE_CATEGORY, category_id);
  client.Query(getCategoryParent, timeout);
 
  //ITEM SELLER COUNT
  int sellerItemCount = 0;
  //std::string getSellerItemCount = fmt::format("SELECT COUNT(*) FROM {} WHERE i_u_id = '{}'", TABLE_ITEM, seller_id);
  std::string getSellerItemCount = fmt::format("SELECT i_id FROM {} WHERE i_u_id = '{}'", TABLE_ITEM, seller_id);
  client.Query(getSellerItemCount, timeout);
  
  client.Wait(results);

  //DESERIALIZE ALL RESULTS
  int offset = 0;



  //ATTRIBUTES
  for(int i = 0; i < gag_ids.size(); ++i){
    if(results[i]->empty()) continue;
    std::string gag_name;
    std::string gav_name;
    queryResult = std::move(results[i]);
    // std::cerr << "res size: " << res->size() << std::endl;
    deserialize(gag_name, queryResult, 0, 0);
    //std::cerr <<"gagname: " << gag_name << std::endl;
    deserialize(gav_name, queryResult, 0, 1);
    // std::cerr <<"gavname: " << gav_name << std::endl;
    description += fmt::format(" * {} -> {}\n", gag_name, gav_name);
  }      
  offset += gag_ids.size();

  //CATEGORY
  queryResult = std::move(results[offset]);
  if (queryResult->empty()) {
//    std::cerr << "category not found: " << category_id << std::endl;
    client.Abort(timeout);
    return ABORTED_USER;
  }

  uint64_t category_p_id;
  uint64_t category_c_id;
  deserialize(category_c_id, queryResult, 0, 0);
  deserialize(category_p_id, queryResult, 0, 2);
  std::string category_name = fmt::format("{}[{}]", category_p_id, category_c_id);

  offset++; 

  //CATEGORY PARENT       
  queryResult = std::move(results[offset]);
  std::string category_parent = "<ROOT>";
 
  if(!queryResult->empty()){
    deserialize(category_c_id, queryResult, 0, 0);
    deserialize(category_p_id, queryResult, 0, 2);
    category_parent = fmt::format("{}[{}]", category_p_id, category_c_id);
  }
  offset++;
  description += fmt::format("\nCATEGORY: {} >> {}", category_parent, category_name);

  //ITEM SELLER COUNT
  queryResult = std::move(results[offset]);
  //deserialize(sellerItemCount, queryResult);

  // NOTE: The chosen item_id might cause a duplicate Insert because client's internal count of the number of items that this seller already has is wrong. 
  // We'll just catch up the cache state and abort the TX.
  for(int i=0; i<queryResult->size(); ++i){
    std::string i_id;
    deserialize(i_id, queryResult, i);
    if(i_id == item_id){
//      std::cerr << "ITEM ALREADY EXISTS" << std::endl;
      //Update Cache
      ItemRecord item_rec(item_id, seller_id, name, initial_price, 0, end_date, ItemStatus::OPEN);
      ItemId itemId = profile.processItemRecord(item_rec);
      //Abort TX
      try {
        client.asyncWait();
      } catch (...) {}
      client.Abort(timeout);
      return ABORTED_USER;
    }
  }
  sellerItemCount = queryResult->size();

  ////////////////// INSERT NEW ROWS/UPDATE

  //Insert a new ITEM tuple
  std::string insertItem = fmt::format("INSERT INTO {} (i_id, i_u_id, i_c_id, i_name, i_description, i_user_attributes, i_initial_price, i_current_price, "
                                                      "i_num_bids, i_num_images, i_num_global_attrs, i_num_comments, i_start_date, i_end_date, i_status, i_created, i_updated, "
                                                      "i_iattr0, i_iattr1, i_iattr2, i_iattr3, i_iattr4, i_iattr5, i_iattr6, i_iattr7) "
                                        "VALUES ('{}', '{}', {}, '{}', '{}', '{}', {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, 0, 0, 0, 0, 0, 0, 0, 0)", 
                                        TABLE_ITEM,
                                        item_id, seller_id, category_id, name, description, attributes, initial_price, initial_price, 0, 
                                        images.size(), gav_ids.size(), 0, current_time, end_date, ItemStatus::OPEN, current_time, current_time);
  client.Write(insertItem, timeout, true, true);    //blind-write: Select i_id should already catch duplicates         
                     
   //Insert ITEM_ATTRIBUTE tuples
  std::string insertItemAttribute = "INSERT INTO " + std::string(TABLE_ITEM_ATTR) + " (ia_id, ia_i_id, ia_u_id, ia_gav_id, ia_gag_id, ia_sattr0) VALUES ('{}', '{}', '{}', '{}', '{}', '')";
  //std::string insertItemAttribute = fmt::format("INSERT INTO {} (ia_id, ia_i_id, ia_u_id, ia_gav_id, ia_gag_id, ia_sattr0) VALUES('{}', '{}', '{}', '{}', '{}', '')", TABLE_ITEM_ATTR);
  for(int i = 0; i< gav_ids.size(); ++i){
    std::string unique_elem_id = GetUniqueElementId(item_id, i);
    std::string stmt = fmt::format(insertItemAttribute, unique_elem_id, item_id, seller_id, gav_ids[i], gag_ids[i]);
    client.Write(stmt, timeout, true); 
  }

  //Insert ITEM_IMAGE tuples         
  std::string insertImage = "INSERT INTO " + std::string(TABLE_ITEM_IMAGE) + " (ii_id, ii_i_id, ii_u_id, ii_sattr0) VALUES ('{}', '{}', '{}', '')";       
  //std::string insertImage = fmt::format("INSERT INTO {} (ii_id, ii_i_id, ii_u_id, ii_sattr0) VALUES ('{}', '{}', '{}', '')", TABLE_ITEM_IMAGE);                     
  for(int i = 0; i<images.size(); ++i){
    std::string unique_elem_id = GetUniqueElementId(item_id, i);
    std::string stmt = fmt::format(insertImage, unique_elem_id, item_id, seller_id, images[i]);
    client.Write(stmt, timeout, true);
  }

  try {
    client.asyncWait();
  } catch (...) {
    client.Abort(timeout);
    return ABORTED_USER;
  }

  Debug("COMMIT");
  auto tx_result = client.Commit(timeout);
  if(tx_result != transaction_status_t::COMMITTED) return tx_result;
   
  //////////////// UPDATE PROFILE /////////////////////
  ItemRecord item_rec(item_id, seller_id, name, initial_price, 0, end_date, ItemStatus::OPEN);
  ItemId itemId = profile.processItemRecord(item_rec);


  return tx_result;
}

} // namespace auctionmark
