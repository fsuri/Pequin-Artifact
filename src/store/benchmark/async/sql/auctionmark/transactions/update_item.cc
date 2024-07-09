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
#include "store/benchmark/async/sql/auctionmark/transactions/update_item.h"
#include <fmt/core.h>

namespace auctionmark {

UpdateItem::UpdateItem(uint32_t timeout,  AuctionMarkProfile &profile, std::mt19937_64 &gen) : AuctionMarkTransaction(timeout), profile(profile), gen(gen) {
  
  std::cerr << std::endl << "UPDATE ITEM" << std::endl;
  ItemInfo itemInfo = *profile.get_random_available_item();
  UserId sellerId = itemInfo.get_seller_id();
  description = RandomAString(50, 255, gen);

  delete_attribute = false;
  add_attribute = {"-1", "-1"};

  // Delete ITEM_ATTRIBUTE
  if (std::uniform_int_distribution<int>(1, 100)(gen) <= PROB_UPDATEITEM_DELETE_ATTRIBUTE) {
    delete_attribute = true;
  }
  // Add ITEM_ATTRIBUTE
  else if (std::uniform_int_distribution<int>(1, 100)(gen) <= PROB_UPDATEITEM_ADD_ATTRIBUTE) {
    GlobalAttributeValueId gav_id = profile.get_random_global_attribute_value();
    add_attribute[0] = gav_id.get_global_attribute_group().encode();
    add_attribute[1] = gav_id.encode();
  }

  item_id = itemInfo.get_item_id().encode();
  seller_id = sellerId.encode();
}

UpdateItem::~UpdateItem(){
};

transaction_status_t UpdateItem::Execute(SyncClient &client) {
  std::unique_ptr<const query_result::QueryResult> queryResult;
  std::string statement;
  std::vector<std::unique_ptr<const query_result::QueryResult>> results;

  Debug("UPDATE ITEM");

  client.Begin(timeout);

  uint64_t current_time = GetProcTimestamp({profile.get_loader_start_time(), profile.get_client_start_time()});

  std::string updateItem = fmt::format("UPDATE {} SET i_description = '{}', i_updated = {} WHERE i_id = '{}' AND i_u_id = '{}'", TABLE_ITEM, description, current_time, item_id, seller_id);
  client.Write(updateItem, queryResult, timeout);

  if(!queryResult->has_rows_affected()){
    Debug("Unable to update closed auction");
    client.Abort(timeout);
    return ABORTED_USER;
  }

  //DELETE ITEM_ATTRIBUTE
  bool deleted_first_attribute = false;
  if(delete_attribute){
    std::string ia_id = GetUniqueElementId(item_id, 0);
    std::string deleteItemAttribute = fmt::format("DELETE FROM {} WHERE ia_id = '{}' AND ia_i_id = '{}' AND ia_u_id = '{}'", TABLE_ITEM_ATTR, ia_id, item_id, seller_id);
    client.Write(deleteItemAttribute, queryResult, timeout);
    if(queryResult->rows_affected() == 1) deleted_first_attribute = true;
  }
 

  //ADD ITEM_ATTRIBUTE
  if (add_attribute.size() > 0 && add_attribute[0] != "-1") {
    std::string gag_id = add_attribute[0];
    std::string gav_id = add_attribute[1];
    std::string ia_id = "-1";

    std::string getMaxItemAttributeId = fmt::format("SELECT MAX(ia_id) FROM {} WHERE ia_i_id = '{}' AND ia_u_id = '{}'", TABLE_ITEM_ATTR, item_id, seller_id);
    client.Query(getMaxItemAttributeId, queryResult, timeout);
    if(queryResult->empty()){
      //HACK to avoid re-using attribute ID (just to be consistent with the else case)
      if(deleted_first_attribute) ia_id = GetUniqueElementId(item_id, 1);
      else ia_id = GetUniqueElementId(item_id, 0);
    }
    else {
      deserialize(ia_id, queryResult);
      //Note: Pequinstore does not yet implement read your own write semantics. (Note that in an ideal implementation, the Select staement should see the preceeding Delete)
      //      To hack around this, we just enforce here that item attribute IDs always increase. I.e. if we had ia_id=0, but deleted it, then the new row will still be ia_id=1 
      ia_id = GetNextElementId(ia_id); 
    }

    std::string insertItemAttribute = fmt::format("INSERT INTO {} (ia_id, ia_i_id, ia_u_id, ia_gav_id, ia_gag_id, ia_sattr0) "
                                                  "VALUES ('{}', '{}', '{}', '{}', '{}', '')", TABLE_ITEM_ATTR, ia_id, item_id, seller_id, gag_id, gav_id);
    client.Write(insertItemAttribute, queryResult, timeout, true); //blind-write: because it is to a unique position (since we read the Max); if it is not unique, then Max will abort.
  }
  
  Debug("COMMIT");
  return client.Commit(timeout);
}

} // namespace auctionmark
