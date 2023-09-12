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

#include "store/benchmark/async/sql/auctionmark/auctionmark.pb.h"
#include "store/common/frontend/sync_client.h"
#include "store/common/frontend/sync_transaction.h"
#include "lib/cereal/archives/binary.hpp"
#include "lib/cereal/types/string.hpp"

namespace auctionmark {

template<class T>
void deserialize(T& t, std::unique_ptr<const query_result::QueryResult>& queryResult, const std::size_t row) {
  std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
  for(std::size_t i = 0; i < queryResult->columns(); i++) {
    std::size_t n_bytes;
    const char* r_chars = queryResult->get(row, i, &n_bytes);
    std::string r = std::string(r_chars, n_bytes);
    ss << r;
  }
  {
    cereal::BinaryInputArchive iarchive(ss); // Create an input archive
    iarchive(t); // Read the data from the archive
  }
}

template<class T>
void deserialize(T& t, std::unique_ptr<const query_result::QueryResult>& queryResult) {
  deserialize(t, queryResult, 0);
}

class AuctionMarkTransaction : public SyncTransaction {
 public:
  AuctionMarkTransaction(uint32_t timeout);
  virtual ~AuctionMarkTransaction();
};

}

#endif /* AUCTIONMARK_TRANSACTION_H */
