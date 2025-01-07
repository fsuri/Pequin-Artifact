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
#include "store/benchmark/async/sql/auctionmark/transactions/get_user_info.h"

#include <fmt/core.h>

namespace auctionmark {

GetUserInfo::GetUserInfo(uint32_t timeout, AuctionMarkProfile &profile, std::mt19937_64 &gen) : 
    AuctionMarkTransaction(timeout), profile(profile), gen(gen) {

    std::cerr << std::endl << "GET USER INFO" << std::endl;

    UserId userId = profile.get_random_buyer_id();
    user_id = userId.encode();

    uint32_t rand;
    rand = std::uniform_int_distribution<uint32_t>(0, 100)(gen);
    get_feedback = (rand <= PROB_GETUSERINFO_INCLUDE_FEEDBACK);

    rand = std::uniform_int_distribution<uint32_t>(0, 100)(gen);
    get_comments = (rand <= PROB_GETUSERINFO_INCLUDE_COMMENTS);

    rand = std::uniform_int_distribution<uint32_t>(0, 100)(gen);
    get_seller_items= (rand <= PROB_GETUSERINFO_INCLUDE_SELLER_ITEMS);

    rand = std::uniform_int_distribution<uint32_t>(0, 100)(gen);
    get_buyer_items = (rand <= PROB_GETUSERINFO_INCLUDE_BUYER_ITEMS);

    rand = std::uniform_int_distribution<uint32_t>(0, 100)(gen);
    get_watched_items = (rand <= PROB_GETUSERINFO_INCLUDE_WATCHED_ITEMS);


    
}

GetUserInfo::~GetUserInfo(){
}

transaction_status_t GetUserInfo::Execute(SyncClient &client) {
  std::unique_ptr<const query_result::QueryResult> queryResult;
  std::string statement;
  std::vector<std::unique_ptr<const query_result::QueryResult>> results;

  Debug("GET USER INFO");
  Debug("Get Seller Items: %lu", get_seller_items);
  Debug("Get Buyer Items: %lu", get_buyer_items);
  Debug("Get Feedback: %lu", get_feedback);

  std::vector<ItemCommentResponse> item_comment_responses;
  std::vector<ItemRecord> item_records;


  client.Begin(timeout);

  //TODO: Could make these static/global strings of the txn. Just fmt on demand with input.

  //TODO: parallelize tx that can be concurrent.

  //getUser 
  statement = fmt::format("SELECT u_id, u_rating, u_created, u_balance, u_sattr0, u_sattr1, u_sattr2, u_sattr3, u_sattr4, r_name "
                          "FROM {}, {} "
                          "WHERE u_id = '{}' AND u_r_id = r_id "
                          "AND r_id = r_id", //ADDED REFLEXIVE ARG FOR PELOTON PARSING. TODO: AUTOMATE THIS IN SQL_INTERPRETER 
                          TABLE_USERACCT, TABLE_REGION, user_id);
  client.Query(statement, queryResult, timeout);

  if(get_feedback){
     //getUserFeedback
    std::cerr << "getUserFeedback" << std::endl;
    statement = fmt::format("SELECT u_id, u_rating, u_sattr0, u_sattr1, uf_rating, uf_date, uf_sattr0  FROM {}, {} "
                            "WHERE u_id = '{}' AND uf_u_id = '{}' AND uf_u_id = u_id "
                            "ORDER BY uf_date DESC LIMIT 25", TABLE_USERACCT, TABLE_USERACCT_FEEDBACK, user_id, user_id); 
                                                        //redundant input for better Peloton plan...
    client.Query(statement, queryResult, timeout);
  }
 
  if(get_comments){
    //getItemComments    //ITEM_COL_STR: "i_id, i_u_id, i_name, i_current_price, i_num_bids, i_end_date, i_status";
    std::cerr << "getItemComments" << std::endl;
    statement = fmt::format("SELECT {}, ic_id, ic_i_id, ic_u_id, ic_buyer_id, ic_question, ic_created FROM {}, {} "
                            "WHERE i_u_id = '{}' AND i_status = {} "
                            "AND i_id = ic_i_id AND i_u_id = ic_u_id AND ic_response = '' " 
                            "AND ic_i_id = ic_i_id AND ic_u_id = ic_u_id " //ADDED REFLEXIVE ARG FOR PELOTON PARSING. TODO: AUTOMATE THIS IN SQL_INTERPRETER 
                            "ORDER BY ic_created DESC LIMIT 25", 
                            ITEM_COLUMNS_STR, TABLE_ITEM, TABLE_ITEM_COMMENT, user_id, ItemStatus::OPEN);
    client.Query(statement, queryResult, timeout);
   
    for(int i = 0; i < queryResult->size(); ++i){
      std::string itemId;
      std::string sellerId;
      uint64_t commentId;
      deserialize(itemId, queryResult, i, 0);
      deserialize(sellerId, queryResult, i, 1);
      deserialize(commentId, queryResult, i, 7);

      item_comment_responses.push_back(ItemCommentResponse(commentId, itemId, sellerId));
    }
   //Panic("stop after comment");
  }

  if(get_seller_items){
    //getSellerItems
    std::cerr << "getSellerItems" << std::endl;
    statement = fmt::format("SELECT {} FROM {} WHERE i_u_id = '{}' ORDER BY i_end_date DESC LIMIT 25", ITEM_COLUMNS_STR, TABLE_ITEM, user_id);
    client.Query(statement, queryResult, timeout);
    for(int i=0; i < queryResult->size(); ++i){
       ItemRow ir;
      deserialize(ir, queryResult, i);
      
      item_records.push_back(ItemRecord(ir.itemId, ir.sellerId, ir.i_name, ir.currentPrice, ir.numBids, ir.endDate, ir.itemStatus));
    }
   
  }
              
  if(get_buyer_items){
     //getBuyerItems
    std::cerr << "getBuyerItems" << std::endl;
    statement = fmt::format("SELECT {} FROM {}, {} "
                            "WHERE ui_u_id = '{}' AND ui_i_id = i_id AND ui_i_u_id = i_u_id " 
                            "AND i_id = i_id AND i_u_id = i_u_id " //ADDED REFLEXIVE ARG FOR PELOTON PARSING. TODO: AUTOMATE THIS IN SQL_INTERPRETER 
                            "ORDER BY i_end_date DESC LIMIT 25", 
                             ITEM_COLUMNS_STR, TABLE_USERACCT_ITEM, TABLE_ITEM, user_id); //user_id == bidder
    client.Query(statement, queryResult, timeout);
  
     for(int i=0; i < queryResult->size(); ++i){
       ItemRow ir;
      deserialize(ir, queryResult, i);
      
      item_records.push_back(ItemRecord(ir.itemId, ir.sellerId, ir.i_name, ir.currentPrice, ir.numBids, ir.endDate, ir.itemStatus));
    }

  }

  if(get_watched_items){
    //getWatchedItems
    std::cerr << "getWatchedItems" << std::endl;
    statement = fmt::format("SELECT {}, uw_u_id, uw_created FROM {}, {} "
                            "WHERE uw_u_id = '{}' AND uw_i_id = i_id AND uw_i_u_id = i_u_id " 
                            "AND i_id = i_id AND i_u_id = i_u_id " //ADDED REFLEXIVE ARG FOR PELOTON PARSING. TODO: AUTOMATE THIS IN SQL_INTERPRETER 
                            "ORDER BY i_end_date DESC LIMIT 25", 
                            ITEM_COLUMNS_STR, TABLE_USERACCT_WATCH, TABLE_ITEM, user_id); //user_id = buyer; i_u_id = seller
    client.Query(statement, queryResult, timeout);
     for(int i=0; i < queryResult->size(); ++i){
       ItemRow ir;
      deserialize(ir, queryResult, i);
      
      item_records.push_back(ItemRecord(ir.itemId, ir.sellerId, ir.i_name, ir.currentPrice, ir.numBids, ir.endDate, ir.itemStatus));
    }
  }

   
  Debug("COMMIT");
  auto tx_result = client.Commit(timeout);
  if(tx_result != transaction_status_t::COMMITTED) return tx_result;
   
   //////////////// UPDATE PROFILE /////////////////////
  for(auto &item_cr: item_comment_responses){
     profile.add_pending_item_comment_response(item_cr);
  }
  for(auto &item_rec: item_records){
    ItemId itemId = profile.processItemRecord(item_rec);
  }


 return tx_result;
}

} // namespace auctionmark
