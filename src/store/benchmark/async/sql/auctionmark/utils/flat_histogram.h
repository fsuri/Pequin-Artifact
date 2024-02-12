#ifndef AUCTIONMARK_FLAT_HISTOGRAM_H
#define AUCTIONMARK_FLAT_HISTOGRAM_H

#include <random>
#include <map>
#include <boost/histogram.hpp>

namespace auctionmark
{

  template <class Storage = boost::histogram::default_storage, typename T = int, template <typename, typename...> class Axes = boost::histogram::axis::integer, typename... Args>
  class FlatHistogram
  {
  public:
    FlatHistogram(std::mt19937_64 &gen, boost::histogram::histogram<std::tuple<Axes<T, Args...>>, Storage> &hist);
    FlatHistogram(std::mt19937_64 &gen, std::map<int, T> value_rle);
    FlatHistogram(const FlatHistogram &other)
        : value_rle(other.value_rle), inner(other.inner), gen(other.gen) {}
    ~FlatHistogram() = default;

    T next_value();

  private:
    std::map<int, T> value_rle;
    std::uniform_int_distribution<> inner;
    std::mt19937_64 &gen;
  };

} // namespace auctionmark

#endif // AUCTIONMARK_FLAT_HISTOGRAM_H