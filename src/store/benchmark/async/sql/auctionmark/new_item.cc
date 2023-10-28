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

NewItem::NewItem(uint32_t timeout, uint64_t i_id, uint64_t u_id, uint64_t c_id,
      std::string name, std::string description, double initial_price,
      double reserve_price, double buy_now, const std::string attributes, 
      const std::vector<uint64_t> gag_ids, const std::vector<uint64_t> gav_ids, 
      const std::vector<std::string> images, uint64_t start_date, uint64_t end_date,
      std::mt19937 &gen) : AuctionMarkTransaction(timeout), i_id(i_id), u_id(u_id), c_id(c_id),
  name(name), description(description), initial_price(initial_price),
  reserve_price(reserve_price), buy_now(buy_now), attributes(attributes),
  gag_ids(gag_ids), gav_ids(gav_ids), images(images), start_date(start_date),
  end_date(end_date) {
}

NewItem::~NewItem(){
}

transaction_status_t NewItem::Execute(SyncClient &client) {
  std::unique_ptr<const query_result::QueryResult> queryResult;
  std::string statement;
  std::vector<std::unique_ptr<const query_result::QueryResult>> results;

  Debug("NEW ITEM");
  Debug("Item ID: %lu", i_id);
  Debug("User ID: %lu", u_id);
  Debug("Category ID: %lu", c_id);

  client.Begin(timeout);

  std::string description = "";
  std::string gag_name;
  std::string gav_name;
  for(int i = 0; i < (int) gag_ids.size(); i++) {
    statement = fmt::format("SELECT gag_name FROM GLOBAL_ATTRIBUTE_GROUP "
                            "WHERE gag_id = {};",
                            gag_ids[i]);
    client.Query(statement, timeout);
    statement = fmt::format("SELECT gav_name FROM GLOBAL_ATTRIBUTE_VALUE "
                            "WHERE gav_id = {} AND gav_gag_id = {};",
                            gav_ids[i], gag_ids[i]);
    client.Query(statement, timeout);

    client.Wait(results);
    deserialize(gag_name, results[0]);
    deserialize(gav_name, results[1]);
    description += gag_name + " " + gav_name + " ";
  }

  std::string query_values = fmt::format("VALUES ({}, {}, {}, {}, {}, "
                                        "{}, {}, {}, {}, {}, {}, {});",
                                        i_id, u_id, c_id, name, description,
                                        attributes, initial_price, 0, images.size(),
                                        gav_ids.size(), start_date, end_date);
  statement = "INSERT INTO ITEM (i_id, i_u_id, i_c_id, i_name, i_description, "
    "i_user_attributes, i_initial_price, i_num_bids, i_num_images, "
    "i_num_global_attrs, i_start_date, i_end_date) " +
    query_values;
  client.Write(statement, queryResult, timeout);
  assert(queryResult->has_rows_affected());

  for(uint32_t i = 0; i < (uint32_t) images.size(); i++) {
    uint64_t ii_id = (i << 60) | (i_id & 0x0FFFFFFFFFFFFFFF);
    statement = fmt::format("INSERT INTO ITEM_IMAGE (ii_id, ii_i_id, ii_u_id, ii_path) "
                "VALUES ({}, {}, {}, {});",
                ii_id, i_id, u_id, images[i]);
    client.Write(statement, timeout);
  }

  client.Wait(results);
  assert(results.size() == images.size());
  for(int i = 0; i < (int) images.size(); i++) {
    assert(results[i]->has_rows_affected());
  }

  statement = fmt::format("UPDATE USER SET u_balance = u_balance - 1 WHERE u_id = {}", u_id);
  client.Query(statement, queryResult, timeout);
  assert(queryResult->has_rows_affected());
  Debug("COMMIT");
  return client.Commit(timeout);

}

} // namespace auctionmark
