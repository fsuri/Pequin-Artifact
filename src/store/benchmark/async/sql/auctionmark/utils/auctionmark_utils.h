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
#include <iostream>
#include <sys/time.h>
#include "store/benchmark/async/sql/auctionmark/auctionmark_params.h"

namespace auctionmark
{
//   class GaussGenerator {
//     std::mt19937_64 &gen;
//     std::default_random_engine rand; //seed
//     std::normal_distribution<double> distribution;
//     int min;
//     int max;
// public:
//     GaussGenerator(std::mt19937_64 &gen, double mean, double stddev, int min, int max):
//         gen(gen), distribution(mean, stddev), min(min), max(max), rand(time(0))
//     {}

//     GaussGenerator(std::mt19937_64 &gen, int min, int max):
//         gen(gen), distribution((max + min + 1) / 2, (max - min + 1) / 6), min(min), max(max), rand(time(0))
//     {
//       std::cerr << "gauss: Min " << min << std::endl;
//       std::cerr << "gauss: Max " << max << std::endl;
//       std::cerr << "gauss: Mean " << (min+max)/2 << std::endl;
//       std::cerr << "gauss: stddev " << (max-min)/6 << std::endl;
//     }

//     int next_val() {
//         while (true) {
//             int number = (int) this->distribution(rand);
//             if (number >= this->min && number <= this->max)
//                 return number;
//         }
//     }
//   };

  class GaussGenerator {
    std::mt19937_64 &gen;
    std::default_random_engine rand; //seed
    std::normal_distribution<double> distribution;
    int min;
    int max;
    int range_size;
    double mean;
    bool cached;
    double cached_res;
public:
   
    GaussGenerator(std::mt19937_64 &gen, int min, int max):
        gen(gen), min(min), max(max), cached(false)
    {
    
      range_size = (max - min) + 1;
      mean = range_size /2.0;
    }

    double nextGaussian(){
      if(cached){
        cached = false;
        return cached_res;
      }

      double v1;
      double v2;
      double s;
      do {
        do {
          v1 = 2.0 * std::uniform_real_distribution<double>(0.0, 1.0)(rand) - 1.0;
          v2 = 2.0 * std::uniform_real_distribution<double>(0.0, 1.0)(rand) - 1.0;
          s = v1 * v2 + v2 * v2;
        } while(s >= 1.0);
      } while(s == 0.0);

      double multiplier = sqrt(-2.0 * log(s) / s);
      cached_res = v2 * multiplier;
      cached = true;
      return v1 * multiplier;

    }

    int next_val() {
        int value = -1;
        while(value < 0 || value >= this->range_size){
          double gaussian = (nextGaussian() + 2.0) / 4.0;
          value = (int) round(gaussian * this->range_size);
        }
        return (value + this->min);
    }
  };

  std::string RandomAString(size_t x, size_t y, std::mt19937_64 &gen);

  uint64_t GetScaledTimestamp(uint64_t benchmark_start, uint64_t client_start, uint64_t current);

  std::string GetUniqueElementId(std::string item_id_, int idx);
  std::string GetNextElementId(std::string item_id_);

  uint64_t GetProcTimestamp(std::vector<uint64_t> benchmark_times);

  inline uint64_t get_ts(struct timeval& time)
  {
    // return std::chrono::duration_cast<milliseconds>(time.time_since_epoch()).count();
    return (time.tv_sec * 1000) + (time.tv_usec / 1000);
  }

} // namespace auctionmark

namespace boost
{
  namespace serialization
  {
    template <class Archive, class T>
    void serialize(Archive &ar, std::optional<T> &opt, const unsigned int version)
    {
      bool has_value = opt.has_value();
      ar & has_value;
      if (has_value)
      {
        T value = opt.value();
        ar & value;
        opt.emplace(value);
      }
    }
  } // namespace serialization
} // namespace boost

#endif /* AUCTIONMARK_UTILS_H */
