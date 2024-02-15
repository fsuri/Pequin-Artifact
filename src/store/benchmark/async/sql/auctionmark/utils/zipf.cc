#include <iostream>
#include "store/benchmark/async/sql/auctionmark/utils/zipf.h"

namespace auctionmark {

Zipf::Zipf(std::mt19937_64 r, long min, long max, double sigma) : Zipf(r, min, max, sigma, DEFAULT_EPSILON) {
}

Zipf::Zipf(std::mt19937_64 r, long min, long max, double sigma, double epsilon) 
        : random(r), min(min), max(max) {
    if ((max <= min) || (sigma <= 1) || (epsilon <= 0) || (epsilon >= 0.5)) {
        std::cerr << "Max: " << max << " Min: " << min << " Sigma: " << sigma << " Epsilon: " << epsilon << std::endl;
        throw std::invalid_argument("Zipf: Invalid arguments");
    }

    double sum = 0;
    long last = -1;
    for (long i = min; i < max; ++i) {
        sum += std::exp(-sigma * std::log(i - min + 1));
        if ((last == -1) || i * (1 - epsilon) > last) {
            k.push_back(i);
            v.push_back(sum);
            last = i;
        }
    }

    if (last != max - 1) {
        k.push_back(max - 1);
        v.push_back(sum);
    }

    v[v.size() - 1] = 1.0;

    for (int i = v.size() - 2; i >= 0; --i) {
        v[i] = v[i] / sum;
    }
}

Zipf::Zipf() : Zipf(std::mt19937_64(0), 0, 1, 1.1) {
}

long Zipf::next_long() {
    std::uniform_real_distribution<> dis(0.0, 1.0);
    double d = dis(random);
    auto idx = std::lower_bound(v.begin(), v.end(), d) - v.begin();

    if (idx >= v.size()) {
        idx = v.size() - 1;
    }

    if (idx == 0) {
        return k[0];
    }

    long ceiling = k[idx];
    long lower = k[idx - 1];

    std::uniform_int_distribution<long> dis2(0, ceiling - lower);
    return ceiling - dis2(random);
}

} // namespace auctionmark