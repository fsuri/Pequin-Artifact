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
#ifndef AUCTION_MARK_NEW_COMMENT_RESPONSE_H
#define AUCTION_MARK_NEW_COMMENT_RESPONSE_H

#include "store/benchmark/async/sql/auctionmark/auctionmark_transaction.h"

namespace auctionmark {

class NewCommentResponse : public AuctionMarkTransaction {
 public:
  NewCommentResponse(uint32_t timeout, uint64_t i_id, uint64_t i_c_id, uint64_t seller_id,
      std::string response, std::mt19937 &gen);
  virtual ~NewCommentResponse();
  virtual transaction_status_t Execute(SyncClient &client);

 private:
  uint64_t i_id;
  uint64_t i_c_id;
  uint64_t seller_id;
  std::string response;
};

} // namespace auctionmark

#endif /* AUCTION_MARK_NEW_COMMENT_RESPONSE_H */
