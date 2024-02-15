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
#ifndef AUCTION_MARK_CLOSE_AUCTIONS_H
#define AUCTION_MARK_CLOSE_AUCTIONS_H

#include "store/benchmark/async/sql/auctionmark/auctionmark_transaction.h"
#include "store/benchmark/async/sql/auctionmark/auctionmark_profile.h"

namespace auctionmark {

class CloseAuctions : public AuctionMarkTransaction {
 public:
  CloseAuctions(uint32_t timeout, AuctionMarkProfile &profile, std::mt19937_64 &gen);
  virtual ~CloseAuctions();
  virtual transaction_status_t Execute(SyncClient &client);
  void UpdateProfile();

 private:
  uint64_t start_time;
  uint64_t end_time;
  std::vector<uint64_t> benchmark_times;

  AuctionMarkProfile &profile;
  std::vector<ItemRecord> item_records;
};

class getDueItemRow {
  public:
    getDueItemRow(){}
    std::string itemId;
    std::string sellerId;
    std::string i_name;
    double currentPrice;
    double numBids;
    uint64_t endDate;
    int i_status;
    ItemStatus itemStatus;
};

inline void load_row(getDueItemRow& r, std::unique_ptr<query_result::Row> row)
{
  row->get(0, &r.itemId);
  row->get(1, &r.sellerId);
  row->get(2, &r.i_name);
  row->get(3, &r.currentPrice);
  row->get(4, &r.numBids);
  row->get(5, &r.endDate);
  row->get(6, &r.i_status);
  r.itemStatus = static_cast<ItemStatus>(r.i_status);
}

class getMaxBidRow {
  public:
    getMaxBidRow(): bidId(0), buyerId(""){}
    uint64_t bidId;
    std::string buyerId;
};

inline void load_row(getMaxBidRow& r, std::unique_ptr<query_result::Row> row)
{
  row->get(0, &r.bidId);
  row->get(1, &r.buyerId);
}


} // namespace auctionmark

#endif /* AUCTION_MARK_CLOSE_AUCTIONS_H */
