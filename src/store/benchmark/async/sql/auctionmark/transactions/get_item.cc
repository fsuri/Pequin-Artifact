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
#include "store/benchmark/async/sql/auctionmark/transactions/get_item.h"
#include <fmt/core.h>

namespace auctionmark {

GetItem::GetItem(uint32_t timeout, AuctionMarkProfile &profile, std::mt19937_64 &gen) : 
    AuctionMarkTransaction(timeout), profile(profile) {
   
    std::cerr << std::endl << "GET ITEM" << std::endl;
    //std::cerr << "ItemInfo: " << ItemInfo().get_item_id().to_string() << std::endl;
    // auto itemInfo_opt = profile.get_random_available_item();
    // UW_ASSERT(itemInfo_opt.has_value());

    ItemInfo itemInfo = *profile.get_random_available_item();
    std::cerr << "ItemInfo: " << itemInfo.get_item_id().to_string() << std::endl;
    item_id = itemInfo.get_item_id().encode();
    seller_id = itemInfo.get_seller_id().encode();

}

GetItem::~GetItem(){
}

transaction_status_t GetItem::Execute(SyncClient &client) {
  std::unique_ptr<const query_result::QueryResult> queryResult;
  std::string statement;
  std::vector<std::unique_ptr<const query_result::QueryResult>> results;

  Debug("GET ITEM");
  Debug("Item ID: %s", item_id.c_str());

  client.Begin(timeout);

  statement = fmt::format("SELECT i_id, i_u_id, i_name, i_current_price, i_num_bids, i_end_date, i_status FROM "
                          "{} WHERE i_id = '{}' AND i_u_id = '{}'", TABLE_ITEM, item_id, seller_id);
  client.Query(statement, timeout);

  statement = fmt::format("SELECT u_id, u_rating, u_created, u_sattr0, u_sattr1, u_sattr2, u_sattr3, u_sattr4, r_name "
                         "FROM {}, {} WHERE u_id = '{}' AND u_r_id = r_id "
                         "AND r_id = r_id", //ADDED REFLEXIVE ARG FOR PELOTON PARSING. TODO: AUTOMATE THIS IN SQL_INTERPRETER 
                         TABLE_USERACCT, TABLE_REGION, seller_id);

  // statement = fmt::format("SELECT u_id, u_rating, u_created, u_sattr0, u_sattr1, u_sattr2, u_sattr3, u_sattr4, r_name "
  //                         "FROM {} INNER JOIN {} ON u_r_id = r_id WHERE u_id = '{}' AND r_id = r_id",  TABLE_USERACCT, TABLE_REGION, seller_id);                
  client.Query(statement, timeout);

  client.Wait(results);

  if(results[0]->empty() || results[1]->empty()){
    Debug("Query result empty, aborting GET ITEM");
    client.Abort(timeout);
    return ABORTED_USER;
  } 

  ItemRow ir;
  deserialize(ir, results[0]);

  Debug("COMMIT");
  auto tx_result = client.Commit(timeout);
  if(tx_result != transaction_status_t::COMMITTED) return tx_result;

  //////////////// UPDATE PROFILE /////////////////////
  ItemRecord item_rec(ir.itemId, ir.sellerId, ir.i_name, ir.currentPrice, ir.numBids, ir.endDate, ir.itemStatus);
  ItemId itemId = profile.processItemRecord(item_rec);

  return tx_result;
}

} // namespace auctionmark
