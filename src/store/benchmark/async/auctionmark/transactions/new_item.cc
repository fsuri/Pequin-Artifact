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
#include "store/benchmark/async/auctionmark/transactions/new_item.h"

namespace auctionmark {

NewItem::NewItem(uint32_t timeout, uint64_t i_id, uint64_t u_id, uint64_t c_id,
  string_view name, string_view description, double initial_price,
  double reserve_price, double buy_now, const vector<string>& attributes, 
  const vector<uint64_t>& gag_ids, const vector<uint64_t>& gav_ids, 
  const vector<string_view>& images, uint64_t start_date, uint64_t end_date,
  std::mt19937 &gen) : SyncTransaction(timeout), i_id(i_id), u_id(u_id), c_id(c_id),
  name(name), description(description), initial_price(initial_price),
  reserve_price(reserve_price), buy_now(buy_now), attributes(attributes),
  gag_ids(gag_ids), gav_ids(gav_ids), images(images), start_date(start_date),
  end_date(end_date) {
  }
NewItem::~NewItem(){
}

transaction_status_t Execute(SyncClient &client) {
  string description = "";
  for(auto &gag_id : gag_ids) {
    
  }
}

} // namespace auctionmark
