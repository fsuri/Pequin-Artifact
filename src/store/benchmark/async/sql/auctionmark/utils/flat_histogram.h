#ifndef AUCTIONMARK_FLAT_HISTOGRAM_H
#define AUCTIONMARK_FLAT_HISTOGRAM_H

#include <random>
#include <map>
#include <boost/histogram.hpp>

namespace auctionmark
{

  //TODO: Make template.
  using histogram_str = std::map<int, std::string>; //cumulative freq => bucket
  using histogram_int = std::map<int, int>;


  class FlatHistogram_Int {
  public:
    FlatHistogram_Int(std::mt19937_64 &gen, std::map<int, int> value_rle) : gen(gen)  {
      cumulative_frequency = 0;
      for (auto& [val, freq] : value_rle)
      {
          if(freq == 0) continue;
          cumulative_frequency += freq;
          hist[cumulative_frequency] = val;
      }
    }
    FlatHistogram_Int(std::mt19937_64 &gen, std::vector<int> freq) : gen(gen)  {
      cumulative_frequency = 0;
      for (int i = 0; i < freq.size(); ++i)
      {
          if(freq[i] == 0) continue;
          cumulative_frequency += freq[i];
          hist[cumulative_frequency] = i;
      }
    }
    //FlatHistogram(const FlatHistogram &other) : value_rle(other.value_rle), inner(other.inner), gen(other.gen) {}
    ~FlatHistogram_Int() = default;

    int next_value(){
        int rand = std::uniform_int_distribution<int>(1, cumulative_frequency)(gen) - 1;
        int ret = hist.upper_bound(rand)->second; 
        return ret;
    }

  private:
    histogram_int hist;
    int cumulative_frequency;
    std::mt19937_64 &gen;
  };

  class FlatHistogram_Str {
  public:
    FlatHistogram_Str(std::mt19937_64 &gen, std::map<std::string, int> value_rle) : gen(gen)  {
      cumulative_frequency = 0;
      for (auto& [val, freq] : value_rle)
      {
          if(freq == 0) continue;
          cumulative_frequency += freq;
          hist[cumulative_frequency] = val;
      }
    }
    //FlatHistogram(const FlatHistogram &other) : value_rle(other.value_rle), inner(other.inner), gen(other.gen) {}
    ~FlatHistogram_Str() = default;

    std::string next_value(){
        int rand = std::uniform_int_distribution<int>(1, cumulative_frequency)(gen) - 1;
        std::string ret = hist.upper_bound(rand)->second; 
        return ret;
    }

  private:
    histogram_str hist;
    int cumulative_frequency;
    std::mt19937_64 &gen;
  };

  template <typename T> class FlatHistogram {
  public:
    FlatHistogram(std::mt19937_64 &gen, std::map<T, uint64_t> value_rle) : gen(gen)  {
      cumulative_frequency = 0;
      for (auto& [val, freq] : value_rle)
      {
          if(freq == 0) continue;
          cumulative_frequency += freq;
          hist[cumulative_frequency] = val;
      }
    }
    //FlatHistogram(const FlatHistogram &other) : value_rle(other.value_rle), inner(other.inner), gen(other.gen) {}
    ~FlatHistogram() = default;

    T next_value(){
        int rand = std::uniform_int_distribution<int>(1, cumulative_frequency)(gen) - 1;
        T ret = hist.upper_bound(rand)->second; 
        return ret;
    }

  private:

    std::map<uint64_t, T> hist;
    int cumulative_frequency;
    std::mt19937_64 &gen;
  };

  // template <class Storage = boost::histogram::default_storage, typename T = int, template <typename, typename...> class Axes = boost::histogram::axis::integer, typename... Args>
  // class FlatHistogram
  // {
  // public:
  //   FlatHistogram(std::mt19937_64 &gen, boost::histogram::histogram<std::tuple<Axes<T, Args...>>, Storage> &hist);
  //   FlatHistogram(std::mt19937_64 &gen, std::map<int, T> value_rle);
  //   FlatHistogram(const FlatHistogram &other)
  //       : value_rle(other.value_rle), inner(other.inner), gen(other.gen) {}
  //   ~FlatHistogram() = default;

  //   T next_value();

  // private:
  //   std::map<int, T> value_rle;
  //   std::uniform_int_distribution<> inner;
  //   std::mt19937_64 &gen;
  // };
  // template class FlatHistogram<boost::histogram::default_storage, std::string, boost::histogram::axis::category>;
  // template class FlatHistogram<boost::histogram::unlimited_storage<std::allocator<char>>, int, boost::histogram::axis::integer>;

} // namespace auctionmark

#endif // AUCTIONMARK_FLAT_HISTOGRAM_H