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
#include "store/benchmark/async/sql/auctionmark/transactions/new_comment.h"
#include <fmt/core.h>

namespace auctionmark {

NewComment::NewComment(uint32_t timeout, AuctionMarkProfile &profile, std::mt19937_64 &gen) : AuctionMarkTransaction(timeout), profile(profile), gen(gen) {

  std::cerr << std::endl << "NEW COMMENT" << std::endl;
  
  std::optional<ItemInfo> maybeItemInfo = profile.get_random_completed_item();
  ItemInfo itemInfo;
  if (maybeItemInfo.has_value()) {
    itemInfo = maybeItemInfo.value();
  } else {
    throw std::runtime_error("new_comment construction: failed to get random completed item");
  }
  UserId sellerId = itemInfo.get_seller_id();
  UserId buyerId = profile.get_random_buyer_id(sellerId);
  question = RandomAString(ITEM_COMMENT_LENGTH_MIN, ITEM_COMMENT_LENGTH_MAX, gen); 

  item_id = itemInfo.get_item_id().encode();
  seller_id = sellerId.encode();
  buyer_id = buyerId.encode();
}



NewComment::~NewComment(){
}

transaction_status_t NewComment::Execute(SyncClient &client) {
  std::unique_ptr<const query_result::QueryResult> queryResult;
  std::string statement;
  std::vector<std::unique_ptr<const query_result::QueryResult>> results;

  Notice("NEW COMMENT EXEC");

  client.Begin(timeout);

  //Set comment_id;
  uint64_t ic_id = 0;
  //getItemComments
  statement = fmt::format("SELECT i_num_comments FROM {} WHERE i_id = '{}' AND i_u_id = '{}'", TABLE_ITEM, item_id, seller_id);
  client.Query(statement, queryResult, timeout);
  if(queryResult->empty()){
    Panic("item does not exist?");
  }

   try{
    deserialize(ic_id, queryResult);
  }
  catch(...){
    Panic("deserialize num_item comments failed");
  }
  
  Debug("Num comments: %d. query: %s", ic_id, statement.c_str());
  ++ic_id;

  uint64_t current_time = GetProcTimestamp({profile.get_loader_start_time(), profile.get_client_start_time()});

  //insertItemComment
  statement = fmt::format("INSERT INTO {} (ic_id, ic_i_id, ic_u_id, ic_buyer_id, ic_question, ic_response, ic_created, ic_updated) "
                        "VALUES ({}, '{}', '{}', '{}', '{}', '', {}, {})", TABLE_ITEM_COMMENT, ic_id, item_id, seller_id, buyer_id, question, current_time, current_time);
  client.Write(statement, queryResult, timeout, true); //blind write; we read from TableITEM to get num_comments => thus our id should be unique. 

  if(queryResult->rows_affected() == 0){ //Note: Original benchbase code does not use a blind write, and throws a UserAbort if its duplicated. I've changed it here: To me this makes more sense.
    //Note: this case should not be possible unless there was a concurrency error. And if there was a concurrency error, then semantically speaking we should be retrying TX and not aborting!!
    Warning("this case should not trigger for Pequin (It can trigger for other systems!). stmt: %s", statement.c_str());
    Debug("Item comment id %d already exists for item %s and seller %s", ic_id, item_id.c_str(), seller_id.c_str());

   client.Abort(timeout); //make sure Tx is marked as aborted (if not already)
    throw std::exception(); //Trigger TX system abort & retry
    // return ABORTED_USER;
  }

  //updateItemComments
  statement = fmt::format("UPDATE {} SET i_num_comments = i_num_comments + 1 WHERE i_id = '{}' AND i_u_id = '{}'", TABLE_ITEM, item_id, seller_id);
  client.Write(statement, timeout, true);

  //updateUser
  statement = fmt::format("UPDATE {} SET u_comments = u_comments + 1, u_updated = {} WHERE u_id = '{}'", TABLE_USERACCT, current_time, seller_id);
  client.Write(statement, timeout, true);

  client.asyncWait();
  
  Debug("COMMIT");
  auto tx_result = client.Commit(timeout);
  if(tx_result != transaction_status_t::COMMITTED) return tx_result;
   
   //////////////// UPDATE PROFILE /////////////////////
  ItemCommentResponse icr(ic_id, item_id, seller_id);
  profile.add_pending_item_comment_response(icr);

  return tx_result;
}

} // namespace auctionmark
