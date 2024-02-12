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
#include "store/benchmark/async/sql/auctionmark/update_item.h"
#include <fmt/core.h>

namespace auctionmark {

UpdateItem::UpdateItem(uint32_t timeout, string description, std::mt19937_64 &gen) : 
  AuctionMarkTransaction(timeout), description(description), gen(gen) {
}

UpdateItem::~UpdateItem(){
};

transaction_status_t UpdateItem::Execute(SyncClient &client) {
  std::unique_ptr<const query_result::QueryResult> queryResult;
  std::string statement;
  std::vector<std::unique_ptr<const query_result::QueryResult>> results;

  Debug("UPDATE ITEM");


  
   //TODO: parameterize
  std::string item_id;
  std::string seller_id;
  std::string description;
  bool delete_attribute;
  std::vector<std::string> add_attribute;

  client.Begin(timeout);

  uint64_t current_time = std::time(0); //TODO: Revisit how original code handles

  std::string updateItem = fmt::format("UPDATE {} SET i_description = {}, i_updated = {} WHERE i_id = {} AND i_u_id = {}", TABLE_ITEM, description, current_time, item_id, seller_id);
  client.Write(updateItem, queryResult, timeout);
  if(queryResult->has_rows_affected()){
    Debug("Unable to update closed auction");
    return client.Abort(timeout);
  }

  //DELETE ITEM_ATTRIBUTE
  if(delete_attribute){
    std::string ia_id = item_id + "0"; //TODO: Original code extracts seller_id from item_id and concats that with 0.
    std::string deleteItemAttribute = fmt::format("DELETE FROM {} WHERE ia_id = {} AND ia_i_id = {} AND ia_u_id = {}", TABLE_ITEM_ATTR, ia_id, item_id, seller_id);
    client.Write(deleteItemAttribute, queryResult, timeout);
  }

  //ADD ITEM_ATTRIBUTE
  if (add_attribute.size() > 0 && add_attribute[0] != "-1") {
    std::string gag_id = add_attribute[0];
    std::string gav_id = add_attribute[1];
    std::string ia_id = "-1";

    std::string getMaxItemAttributeId = fmt::format("SELECT MAX(ia_id) FROM {} WHERE ia_i_id = {} AND ia_u_id = {}", TABLE_ITEM_ATTR, item_id, seller_id);
    client.Query(getMaxItemAttributeId, queryResult, timeout);
    if(queryResult->empty()){
      ia_id = item_id + "0"; //TODO: Original code extracts seller_id from item_id and concats that with 0.
    }
    else{
      deserialize(ia_id, queryResult);
    }

    std::string insertItemAttribute = fmt::format("INSERT INTO {} (ia_id, ia_i_id, ia_u_id, ia_gav_id, ia_gag_id) "
                                                  "VALUES ({}, {}, {}, {}, {})", TABLE_ITEM_ATTR, ia_id, item_id, seller_id, gag_id, gav_id);
    client.Write(insertItemAttribute, queryResult, timeout);
  }
  
  Debug("COMMIT");
  return client.Commit(timeout);
}

} // namespace auctionmark
