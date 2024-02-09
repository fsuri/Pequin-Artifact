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
#include "store/benchmark/async/sql/auctionmark/new_item.h"
#include <fmt/core.h>

namespace auctionmark {

// NewItem i_id not implemented according to spec. 
// Implemented here as a monotonically increasing id, so there may be contention when reading the max i_id from the database. 
// The spec says that i_id should be a composite key where the lower 48-bits of the number is the u_id and the upper 16-bits is the auction count for that user.
// A monotonically increasing i_id allows for easier item selection for other transactions.
NewItem::NewItem(uint32_t timeout, uint64_t &i_id, uint64_t u_id,
      std::string name, std::string description, double initial_price,
      double reserve_price, double buy_now, const std::string attributes, 
      const std::vector<uint64_t> gag_ids, const std::vector<uint64_t> gav_ids, 
      const std::vector<std::string> images, uint64_t start_date, uint64_t end_date,
      std::mt19937_64 &gen) : AuctionMarkTransaction(timeout), i_id(i_id), u_id(u_id),
  name(name), description(description), initial_price(initial_price),
  reserve_price(reserve_price), buy_now(buy_now), attributes(attributes),
  gag_ids(gag_ids), gav_ids(gav_ids), images(images), start_date(start_date),
  end_date(end_date), gen(gen) {
}

NewItem::~NewItem(){
}

transaction_status_t NewItem::Execute(SyncClient &client) {
  std::unique_ptr<const query_result::QueryResult> queryResult;
  std::string statement;
  std::vector<std::unique_ptr<const query_result::QueryResult>> results;

  //Insert a new ITEM record for user.
  Debug("NEW ITEM");
  Debug("User ID: %lu", u_id);

  //TODO: parameterize:
  std::string item_id;
  std::string seller_id;
  uint64_t category_id;
  std::string name;
  std::string description;
  uint64_t duration,
  double initial_price;
  std::string attributes;
  std::vector<std::string> gag_ids;
  std::vector<std::string> gav_ids;
  std::vector<std::string> imaged;

  client.Begin(timeout);

  std::uint64_t current_time = std::time(0);
  std::uint64_t end_date = current_time + (duration * MILLISECONDS_IN_A_DAY);

  //Get attribute names and category path and append them to the item description

  //ATTRIBUTES
  description += "\nATTRIBUTES: ";
  std::string getGlobablAttribute = fmt::format("SELECT gag_name, gav_name, gag_c_id FROM {}, {} WHERE gav_id = {} AND gav_gag_id = {} AND gav_gag_id = gag_id",
                                                                                    TABLE_GLOBAL_ATTR_GROUP, TABLE_GLOBAL_ATTR_VALUE);    //TODO: add redundant input?

  for(int i = 0; i < gag_ids.size(); ++i){
    std::string stmt = fmt::format(getGlobablAttribute, gav_ids[i], gag_ids[i]);
    client.Query(stmt, timeout);
  }                             
  client.Wait(results);

  for(auto res: results){
    //TODO: deserialize
    description += fmt::format(" * {} -> {}\n", res.gag_name, res. gav_name);
  }                                             

  //CATEGORY 
  std::string getCategory = fmt::format("SELECT * FROM {} WHERE c_id = {}", TABLE_CATEGORY, category_id);
  client.Query(getCategory, queryResult, timeout);
  //TODO: deserialize.
   std::string category_name = fmt::format("{}[{}]", field2, field1);

  //CATEGORY PARENT
  std::string getCategoryParent = fmt::format("SELECT * FROM {} WHERE c_parent_id = {}", TABLE_CATEGORY, category_id);
  client.Query(getCategoryParent, queryResult, timeout);
  std::string category_parent = "<ROOT>";
  if(!queryResult.empty()){
    //TODO: deserialize
    category_parent = fmt::format("{}[{}]", field2, field1);
  }
  description += fmt::format("\nCATEGORY: {} >> {}", category_parent, category_name);

  int sellerItemCount = 0;
  std::string getSellerItemCount = fmt::format("SELECT COUNT(*) FROM {} WHERE i_u_id = {}", TABLE_ITEM, seller_id);
  client.Query(getSellerItemCount, queryResult, timeout);
  deserialize(sellerItemCount, queryResult);

  //Insert a new ITEM tuple
  std::string insertItem = fmt::format("INSERT INTO {} (i_id, i_u_id, i_c_id, i_name, i_description, i_user_attributes, i_initial_price, i_current_price, "
                                                      "i_num_bids, i_num_images, i_num_global_attrs, i_start_date, i_end_date, i_status, i_created, i_updated, i_attr0) "
                                        "VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})", TABLE_ITEM,
                                       item_id, seller_id, category_id, name, description, attributes, initial_price, initial_price, 0, 
                                       images.size(), gav_ids.size(), current_time, end_date, ItemStatus::OPEN, current_time, current_time);
  client.Write(insertItem, queryResult, timeout);             
  if(!queryResult->has_rows_affected()){
    return client.Abort(timeout);
  }                              

   //Insert ITEM_ATTRIBUTE tuples
  std::string insertItemAttribute = fmt::format("INSERT INTO {} (ia_id, ia_i_id, ia_u_id, ia_gav_id, ia_gag_id) VALUES({}, {}, {}, {}, {})", TABLE_ITEM_ATTR);
  for(int i = 0; i< gav_ids.size(); ++i){
    std::string unique_elem_id = item_id + "" + std::stoi(i); //TODO: Revisit this  Original code breaks down item_id into seller_id and item_ctr; and then concats seller_id and i 
    std::string stmt = fmt::format(insertItemAttribute, unique_elem_id, item_id, seller_id, gav_ids[i], gag_ids[i]);
    client.Write(stmt, queryResult, timeout); //TODO: parallelize
  }

  //Insert ITEM_IMAGE tuples         
  std::string insertImage = fmt::format("INSERT INTO {} (ii_id, ii_i_id, ii_u_id, ii_sattr0) VALUES ({}, {}, {}, {})", TABLE_ITEM_IMAGE);                    
  for(int i = 0; i<images.length; ++i){
    std::string unique_elem_id = item_id + "" + std::stoi(i); //TODO: Revisit this  Original code breaks down item_id into seller_id and item_ctr; and then concats seller_id and i 
    std::string stmt = fmt::format(insertImage, unique_elem_id, item_id, seller_id, images[i]);
    client.Write(stmt, queryResult, timeout); //TODO: parallelize
  }

  std::string updateUserBalance = fmt::format("UPDATE {} SET u_balance = u_balance -1, u_updated = {} WHERE u_id = {}", TABLE_USER_ACCT, current_time, seller_id);
  client.Write(updateUserBalance, queryResult, timeout);

  Debug("COMMIT");
  return client.Commit(timeout);

}

} // namespace auctionmark
