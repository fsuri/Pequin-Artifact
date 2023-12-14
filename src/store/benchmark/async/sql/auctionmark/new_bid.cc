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
#include "store/benchmark/async/sql/auctionmark/new_bid.h"
#include <fmt/core.h>

namespace auctionmark {

NewBid::NewBid(uint32_t timeout, uint64_t i_id, uint64_t i_buyer_id,
      double bid, double max_bid, std::mt19937_64 &gen) : AuctionMarkTransaction(timeout), i_id(i_id),
      i_buyer_id(i_buyer_id), bid(bid), max_bid(max_bid) {
}

NewBid::~NewBid(){
}

transaction_status_t NewBid::Execute(SyncClient &client) {
  std::unique_ptr<const query_result::QueryResult> queryResult;
  std::string statement;
  std::vector<std::unique_ptr<const query_result::QueryResult>> results;

  Debug("NEW BID");
  Debug("Item ID: %lu", i_id);
  Debug("Bid: %f", bid);

  client.Begin(timeout);

  statement = fmt::format("SELECT i_u_id FROM ITEM WHERE i_id = {};", i_id);
  client.Query(statement, queryResult, timeout);
  uint64_t u_id;
  deserialize(u_id, queryResult);
  Debug("User ID: %lu", u_id);

  statement = fmt::format("UPDATE ITEM SET i_num_bids = i_num_bids + 1 "
                          "WHERE i_id = {} AND i_u_id = {} AND i_status = 0;",
                          i_id, u_id);
  client.Write(statement, queryResult, timeout);
  assert(queryResult->has_rows_affected());

  statement = fmt::format("SELECT MAX(ib_id) + 1 FROM ITEM_BID WHERE "
                          "ib_i_id = {} AND ib_u_id = {};",
                          i_id, u_id);
  client.Query(statement, queryResult, timeout);
  uint64_t ib_id;
  deserialize(ib_id, queryResult);

  statement = fmt::format("SELECT imb_ib_id FROM ITEM_MAX_BID WHERE "
                        "imb_i_id = {} AND imb_u_id = {};", i_id, u_id);
  client.Query(statement, queryResult, timeout);
  if (!queryResult->empty()) {
    uint64_t imb_ib_id;
    deserialize(imb_ib_id, queryResult);
    statement = fmt::format("SELECT ib_bid, ib_max_bid FROM ITEM_BID WHERE "
                            "ib_id = {} AND ib_i_id = {} AND ib_u_id = {};",
                            ib_id, i_id, u_id);
    client.Query(statement, queryResult, timeout);

    uint64_t current_bid, current_max_bid; 
    queryResult->at(0)->get(0, &current_bid);
    queryResult->at(0)->get(1, &current_max_bid);

    bool new_bid_win = false;

    if (max_bid > current_max_bid) {
      new_bid_win = true;
      if (bid < current_max_bid) {
        bid = current_max_bid;
      }
    } else {
      if (bid > current_bid) {
        statement = fmt::format("UPDATE ITEM_BID SET ib_bid = {} WHERE "
                                "ib_id = {} AND ib_i_id = {} AND ib_u_id = {};",
                                bid, imb_ib_id, i_id, u_id);
        client.Write(statement, queryResult, timeout);
        assert(queryResult->has_rows_affected());
      }
    }

    statement = fmt::format("INSERT INTO ITEM_BID (ib_id, ib_i_id, ib_u_id, "
                            "ib_buyer_id, ib_bid, ib_max_bid, ib_created, ib_updated) "
                            "VALUES ({}, {}, {}, {}, {}, {}, {}, {});",
                            ib_id, i_id, u_id, i_buyer_id, bid, max_bid, 0, 0);
    client.Write(statement, queryResult, timeout);
    assert(queryResult->has_rows_affected());

    if (new_bid_win) {
      statement = fmt::format("UPDATE ITEM_MAX_BID SET imb_ib_id = {}, "
                              "imb_ib_i_id = {}, imb_ib_u_id = {}, "
                              "imb_updated = {} WHERE imb_i_id = {} "
                              "AND imb_u_id = {};",
                              imb_ib_id, i_id, u_id, 0, i_id, u_id);
      client.Write(statement, queryResult, timeout);
      assert(queryResult->has_rows_affected());
    }
  } else {
    statement = fmt::format("INSERT INTO ITEM_BID "
                            "(ib_id, ib_i_id, ib_u_id, ib_buyer_id, ib_bid, ib_max_bid, "
                            "ib_created, ib_updated) "
                            "VALUES ({}, {}, {}, {}, {}, {}, {}, {});",
                            ib_id, i_id, u_id, i_buyer_id, bid, max_bid, 0, 0);
    client.Write(statement, timeout);
    statement = fmt::format("INSERT INTO ITEM_MAX_BID "
                            "(imb_i_id, imb_u_id, imb_ib_id, imb_ib_i_id, imb_ib_u_id, "
                            "imb_created, imb_updated)"
                            "VALUES ({}, {}, {}, {}, {}, {}, {});",
                            i_id, u_id, ib_id, i_id, u_id, 0, 0);
    client.Write(statement, timeout);

    client.Wait(results);
    assert(results[0]->has_rows_affected());
    assert(results[1]->has_rows_affected());
  }
  
  Debug("COMMIT");
  return client.Commit(timeout);
}

} // namespace auctionmark
