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
#include "store/benchmark/async/sql/auctionmark/utils/auctionmark_utils.h"
#include "store/benchmark/async/sql/auctionmark/utils/item_id.h"
#include "store/benchmark/async/sql/auctionmark/utils/user_id.h"
#include <iostream>

namespace auctionmark {

const char ALPHA_NUMERIC[] = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

std::string RandomAString(size_t x, size_t y, std::mt19937_64 &gen)
{
  std::string s;
  size_t length = std::uniform_int_distribution<size_t>(x, y)(gen);
  for (size_t i = 0; i < length; ++i)
  {
    int j = std::uniform_int_distribution<size_t>(10, sizeof(ALPHA_NUMERIC) -2)(gen);
    s += ALPHA_NUMERIC[j];
  }
  return s;
}


uint64_t GetScaledTimestamp(uint64_t benchmark_start, uint64_t client_start, uint64_t current) {
    auto offset = current - (client_start - benchmark_start);
    auto elapsed = (offset - benchmark_start) * TIME_SCALE_FACTOR;
    return benchmark_start + elapsed;
}

std::string GetUniqueElementId(std::string item_id_, int idx) {
    ItemId item_id = ItemId(item_id_);
    const UserId &seller_id = item_id.get_seller_id();
    return ItemId(seller_id, idx).encode();
}

std::string GetNextElementId(std::string item_id_){
  ItemId item_id = ItemId(item_id_);
  const UserId &seller_id = item_id.get_seller_id();
  int next_idx = item_id.get_item_ctr() + 1;
  return ItemId(seller_id, next_idx).encode();
}

uint64_t GetProcTimestamp(std::vector<uint64_t> benchmark_times) {
  struct timeval time;
  gettimeofday(&time, NULL);
  uint64_t tmp = get_ts(time);
  uint64_t timestamp = GetScaledTimestamp(benchmark_times[0], benchmark_times[1], tmp);
  
  return timestamp;
}

}
