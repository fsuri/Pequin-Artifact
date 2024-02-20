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
#ifndef AUCTION_MARK_NEW_PURCHASE_H
#define AUCTION_MARK_NEW_PURCHASE_H

#include "store/benchmark/async/sql/auctionmark/transactions/auctionmark_transaction.h"
#include "store/benchmark/async/sql/auctionmark/auctionmark_profile.h"

namespace auctionmark {

class NewPurchase : public AuctionMarkTransaction {
 public:
  NewPurchase(uint32_t timeout, AuctionMarkProfile &profile, std::mt19937_64 &gen);
  virtual ~NewPurchase();
  virtual transaction_status_t Execute(SyncClient &client);

 private:
  std::string item_id;
  std::string seller_id;
  std::string ip_id;
  float buyer_credit;

  std::mt19937_64 &gen;
  AuctionMarkProfile &profile;
};

class getItemInfoRow {
  public:
    getItemInfoRow() {}
     uint64_t i_num_bids;
    double i_current_price;
    uint64_t i_end_date;
    uint64_t i_status;
    uint64_t ib_id;
    uint64_t ib_buyer_id;
    double u_balance;
    
};

inline void load_row(getItemInfoRow& r, std::unique_ptr<query_result::Row> row)
{
  row->get(0, &r.i_num_bids);
  row->get(1, &r.i_current_price);
  row->get(2, &r.i_end_date);
  row->get(3, &r.i_status);
  row->get(4, &r.ib_id);
  row->get(5, &r.ib_buyer_id);
  row->get(6, &r.u_balance);
}

} // namespace auctionmark

#endif /* AUCTION_MARK_NEW_PURCHASE_H */
