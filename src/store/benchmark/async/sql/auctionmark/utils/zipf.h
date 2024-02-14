#ifndef AUCTIONMARK_ZIPF_H
#define AUCTIONMARK_ZIPF_H

#include <vector>
#include <random>
#include <algorithm>
#include <cmath>
#include <stdexcept>

namespace auctionmark {

class Zipf {
public:
    Zipf(std::mt19937_64 r, long min, long max, double sigma);
    Zipf(std::mt19937_64 r, long min, long max, double sigma, double epsilon);
    Zipf();

    long next_long();

private:
    static constexpr double DEFAULT_EPSILON = 0.001;
    std::vector<long> k;
    std::vector<double> v;
    std::mt19937_64 random;
    long min;
    long max;
};

}

#endif //AUCTIONMARK_ZIPF_H