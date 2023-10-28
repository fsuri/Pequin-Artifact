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
#include "store/benchmark/async/sql/auctionmark/get_user_info.h"
#include <fmt/core.h>

namespace auctionmark {

GetUserInfo::GetUserInfo(uint32_t timeout, uint64_t u_id, uint64_t get_seller_items, 
    uint64_t get_buyer_items, uint64_t get_feedback, std::mt19937 &gen) : 
    AuctionMarkTransaction(timeout), u_id(u_id), get_seller_items(get_seller_items),
    get_buyer_items(get_buyer_items), get_feedback(get_feedback) {
}

GetUserInfo::~GetUserInfo(){
}

transaction_status_t GetUserInfo::Execute(SyncClient &client) {
  std::unique_ptr<const query_result::QueryResult> queryResult;
  std::string statement;
  std::vector<std::unique_ptr<const query_result::QueryResult>> results;

  Debug("GET USER INFO");
  Debug("User ID: %lu", u_id);
  Debug("Get Seller Items: %lu", get_seller_items);
  Debug("Get Buyer Items: %lu", get_buyer_items);
  Debug("Get Feedback: %lu", get_feedback);


  client.Begin(timeout);

  statement = fmt::format("SELECT u_id, u_rating, u_balance, u_created, u_sattr0, "
                          "u_sattr1, u_sattr2, u_sattr3, u_sattr4, r_name FROM USER, "
                          "REGION WHERE u_id = {} AND u_r_id = r_id;", u_id);
  client.Query(statement, queryResult, timeout);
  assert(queryResult->size() == 1);
  if (get_seller_items || get_buyer_items) {
    if (get_seller_items) {
      statement = fmt::format("SELECT i_id, i_u_id, i_name, i_current_price, i_end_date, "
                              "i_status FROM ITEM WHERE i_u_id = {} ORDER BY "
                              "i_end_date ASC LIMIT 20;", u_id);
      client.Query(statement, queryResult, timeout);
      Debug("Seller Items: %lu", queryResult->size());
    } else if (get_buyer_items) {
      statement = fmt::format("SELECT i_id, i_u_id, i_name, i_current_price, i_end_date, "
                              "i_status FROM USER_ITEM, ITEM WHERE ui_u_id = {} AND "
                              "ui_i_id = i_id AND ui_i_u_id = i_u_id ORDER BY i_end_date "
                              "ASC LIMIT 10;", u_id);
      client.Query(statement, queryResult, timeout);
      Debug("Buyer Items: %lu", queryResult->size());
    }

    if (get_feedback) {
      statement = fmt::format("SELECT if_rating, if_comment, if_date, i_id, i_u_id, "
                              "i_name, i_end_date, i_status, u_id, u_rating, u_sattr0, "
                              "u_sattr1 FROM ITEM_FEEDBACK, ITEM, USER WHERE if_buyer_id "
                              "= {} AND if_i_id = i_id AND if_u_id = i_u_id AND "
                              "if_u_id = u_id ” + ORDER BY if_date DESC LIMIT 10;", u_id);
      client.Query(statement, queryResult, timeout);
      Debug("Feedback: %lu", queryResult->size());
    }
  }
  
  Debug("COMMIT");
  return client.Commit(timeout);
}

} // namespace auctionmark
