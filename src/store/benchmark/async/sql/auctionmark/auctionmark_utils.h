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
#ifndef AUCTIONMARK_UTILS_H
#define AUCTIONMARK_UTILS_H

#include <random>
#include <chrono>
#include "store/benchmark/async/sql/auctionmark/auctionmark_params.h"

namespace auctionmark {
using timestamp_t = std::chrono::system_clock::time_point;

std::string RandomAString(size_t x, size_t y, std::mt19937_64 &gen);

long GetScaledTimestamp(timestamp_t benchmark_start, timestamp_t client_start, timestamp_t current);

std::string GetUniqueElementId(std::string item_id_, int idx);

timestamp_t GetProcTimestamp(timestamp_t benchmark_times[2]);

} // namespace auctionmark

#endif /* AUCTIONMARK_UTILS_H */
