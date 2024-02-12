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

std::string RandomAString(size_t x, size_t y, std::mt19937_64 &gen);

long GetScaledTimestamp(std::chrono::system_clock::time_point benchmark_start, std::chrono::system_clock::time_point client_start, std::chrono::system_clock::time_point current) {
    auto offset = std::chrono::time_point_cast<std::chrono::milliseconds>(current) - (std::chrono::time_point_cast<std::chrono::milliseconds>(client_start) - std::chrono::time_point_cast<std::chrono::milliseconds>(benchmark_start));
    auto elapsed = (offset - benchmark_start).count() * TIME_SCALE_FACTOR;
    return std::chrono::time_point_cast<std::chrono::milliseconds>(benchmark_start).time_since_epoch().count() + elapsed;
}

} // namespace auctionmark

#endif /* AUCTIONMARK_UTILS_H */
