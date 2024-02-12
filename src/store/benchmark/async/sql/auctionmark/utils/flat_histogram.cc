#include "store/benchmark/async/sql/auctionmark/utils/flat_histogram.h"

namespace auctionmark {
    template <class Storage, typename T, template <typename, typename...> class Axes, typename... Args>
    FlatHistogram<Storage, T, Axes, Args...>::FlatHistogram(std::mt19937_64& gen, boost::histogram::histogram<std::tuple<Axes<T, Args...>>, Storage>& hist): gen(gen)
    {
        // Assuming that the histogram is already filled, we will generate a map where the key is the cumulative frequency and the value is the corresponding histogram bin.
        int cumulative_frequency = 0;
        for (auto&& x : indexed(hist))
        {
            if (*x > 0) {
                cumulative_frequency += *x;
                value_rle[cumulative_frequency] = *x;
            }
        }
        inner = std::uniform_int_distribution(0, cumulative_frequency);
    }

    template <class Storage, typename T, template <typename, typename...> class Axes, typename... Args>
    FlatHistogram<Storage, T, Axes, Args...>::FlatHistogram(std::mt19937_64 &gen, std::map<int, T> value_rle) : gen(gen), value_rle(value_rle)
    {
        // Generate a random number between 0 and the maximum cumulative frequency.
        int max_cumulative_frequency = value_rle.rbegin()->first;
        inner = std::uniform_int_distribution<>(0, max_cumulative_frequency);
    }

    template <class Storage, typename T, template <typename, typename...> class Axes, typename... Args>
    T FlatHistogram<Storage, T, Axes, Args...>::next_value()
    {
        // Generate a random number between 0 and the maximum cumulative frequency.
        int random_number = inner(gen);

        // Find the first element in the map that has a key greater than or equal to the random number.
        auto it = value_rle.lower_bound(random_number);

        // If such an element is found, return its value. Otherwise, return the value of the last element in the map.
        if (it != value_rle.end())
        {
            return it->second;
        }
        else
        {
            throw std::runtime_error("FlatHistogram: next_value: no value found");
        }
    }
}

// #include <iostream>
// int main() {
//     // Create a random number generator
//     std::mt19937_64 gen(std::random_device{}());

//     // Create a histogram
//     boost::histogram::histogram<std::tuple<boost::histogram::axis::integer<>>> hist = boost::histogram::make_histogram(boost::histogram::axis::integer<>(0, 10, "x"));

//     // Fill the histogram with some data
//     for(int i = 0; i < 10; ++i) {
//         for (int j = 0; j < i; j++)
//             hist(i);
//     }

//     // Create a FlatHistogram from the histogram
//     auctionmark::FlatHistogram<> flatHistogram(gen, hist);
//     auto flat_2 = flatHistogram;
//     // Generate a random value from the FlatHistogram
//     double value = flat_2.next_value();

//     // Print the value
//     std::cout << "Random value: " << value << std::endl;

//     auto c_str = boost::histogram::axis::category<std::string>{"red", "blue"};
//     auto hist2 = boost::histogram::make_histogram(c_str);
//     for (auto&& x : indexed(hist2)) {
//         std::cout << "Wtf is this: " << x.bin() << std::endl;
//     }

//     auto h3 = boost::histogram::algorithm::reduce(hist2, boost::histogram::algorithm::slice(0, hist2.axis(0).index("red"), hist2.axis(0).index("red")+1));
//     for (auto&& x : indexed(h3)) {
//         std::cout << "hist3: Wtf is this: " << x.bin() << std::endl;
//     }
//     return 0;
// }