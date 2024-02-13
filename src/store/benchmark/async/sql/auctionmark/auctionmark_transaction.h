/***********************************************************************
 *
 * Copyright 2023 Florian Suri-Payer <fsp@cs.cornell.edu>
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
#ifndef AUCTIONMARK_TRANSACTION_H
#define AUCTIONMARK_TRANSACTION_H

//#include "store/benchmark/async/sql/auctionmark/auctionmark_schema.h"
#include "store/benchmark/async/sql/auctionmark/auctionmark_params.h"
#include "store/benchmark/async/sql/auctionmark/auctionmark_utils.h"
#include "store/benchmark/async/sql/auctionmark/utils/item_status.h"
#include "store/common/frontend/sync_transaction.h"

namespace auctionmark {

class AuctionMarkTransaction : public SyncTransaction {
 public:
  AuctionMarkTransaction(uint32_t timeout);
  virtual ~AuctionMarkTransaction();
};
//////////////

//Generic loaders

template <class T>
void load_row(T& t, std::unique_ptr<query_result::Row> row) {
  row->get(0, &t);
}

template <class T>
void load_row(T& t, std::unique_ptr<query_result::Row> row, const std::size_t col) {
  row->get(col, &t);
}

template<class T>
void deserialize(T& t, std::unique_ptr<const query_result::QueryResult>& queryResult, const std::size_t row) {
  load_row(t, queryResult->at(row));
}

template<class T>
void deserialize(T& t, std::unique_ptr<const query_result::QueryResult>& queryResult) {
  deserialize(t, queryResult, 0);
}

template<class T>
void deserialize(T& t, std::unique_ptr<const query_result::QueryResult>& queryResult, const std::size_t row, const std::size_t col) {
  load_row(t, queryResult->at(row), col);
}

class ItemRow {
  public:
    ItemRow(){}
    std::string itemId;
    std::string sellerId;
    std::string i_name;
    double currentPrice;
    double numBids;
    uint64_t endDate;
    int i_status;
    ItemStatus itemStatus;
};

inline void load_row(ItemRow& r, std::unique_ptr<query_result::Row> row)
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



}

#endif /* AUCTIONMARK_TRANSACTION_H */

