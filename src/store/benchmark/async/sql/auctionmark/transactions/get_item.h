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
#ifndef AUCTION_MARK_GET_ITEM_H
#define AUCTION_MARK_GET_ITEM_H

#include "store/benchmark/async/sql/auctionmark/transactions/auctionmark_transaction.h"
#include "store/benchmark/async/sql/auctionmark/auctionmark_profile.h"

namespace auctionmark {

class GetItem : public AuctionMarkTransaction {
 public:
  GetItem(uint32_t timeout, AuctionMarkProfile &profile, std::mt19937_64 &gen);
  virtual ~GetItem();
  virtual transaction_status_t Execute(SyncClient &client);

 private:
  std::string item_id;
  std::string seller_id;
  AuctionMarkProfile &profile;
};


} // namespace auctionmark

#endif /* AUCTION_MARK_GET_ITEM_H */