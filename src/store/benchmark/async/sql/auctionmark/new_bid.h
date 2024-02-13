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
#ifndef AUCTION_MARK_NEW_BID_H
#define AUCTION_MARK_NEW_BID_H

#include "store/benchmark/async/sql/auctionmark/auctionmark_transaction.h"
#include "store/benchmark/async/sql/auctionmark/auctionmark_profile.h"

namespace auctionmark {

class NewBid : public AuctionMarkTransaction {
 public:
  NewBid(uint32_t timeout, AuctionMarkProfile &profile, std::mt19937_64 &gen);
  virtual ~NewBid();
  virtual transaction_status_t Execute(SyncClient &client);

 private:
  std::string item_id;
  std::string seller_id;
  std::string buyer_id;
  double newBid;
  timestamp_t estimatedEndDate;
  timestamp_t benchmark_times[2];
};

//Row
class getItemRow {
public:
    getItemRow() {}
    double i_initial_price;
    double i_current_price;
    uint64_t i_num_bids;
    uint64_t i_end_date;
    ItemStatus i_status;
};

//load
inline void load_row(getItemRow& r, std::unique_ptr<query_result::Row> row)
{
  row->get(0, &r.i_initial_price);
  row->get(1, &r.i_current_price);
  row->get(2, &r.i_num_bids);
  row->get(3, &r.i_end_date);
  row->get(4, &r.i_status);
}

class getItemMaxBidRow {
public:
    getItemMaxBidRow() {}
    uint64_t currentBidId;
    double currentBidAmount;
    double currentBidMax;
    std::string currentBuyerId;
};

//load
inline void load_row(getItemMaxBidRow& r, std::unique_ptr<query_result::Row> row)
{
  row->get(0, &r.currentBidId);
  row->get(1, &r.currentBidAmount);
  row->get(2, &r.currentBidMax);
  row->get(3, &r.currentBuyerId);
}


} // namespace auctionmark

#endif /* AUCTION_MARK_NEW_BID_H */
