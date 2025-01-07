#ifndef SEATS_SQL_UTILS_H
#define SEATS_SQL_UTILS_H

#include "store/benchmark/async/sql/seats/seats_constants.h"

#include <string>
#include <gflags/gflags.h>
#include <random>
#include <sstream>
#include <iomanip>
#include <cmath>
#include <fstream>
#include <algorithm>
#include <set>
#include <queue>
#include <unordered_set>
#include <map>

namespace seats_sql {

static constexpr char* FLIGHTS_AIRPORT_HISTO_FN = "./resources/histogram_flights_per_airport.json";
static constexpr char* FLIGHTS_TIME_HISTO_FN = "./resources/histogram_flights_per_time.json";
static constexpr char* AIRLINE_SEATS_TABLE = "./resources/table_airline_seats.csv";
static constexpr char* AIRPORT_SEATS_TABLE = "./resources/table_airport_seats.csv";
static constexpr char* COUNTRY_SEATS_TABLE = "./resources/table_country_seats.csv";

class GaussGenerator {
    std::default_random_engine generator;
    std::normal_distribution<double> distribution;
    uint64_t min;
    uint64_t max;
public:
    GaussGenerator(double mean, double stddev, uint64_t min, uint64_t max):
        distribution(mean, stddev), min(min), max(max)
    {}

    GaussGenerator(uint64_t min, uint64_t max):
        distribution((min + max) / 2, (max - min) / 6), min(min), max(max)
    {}

    uint64_t operator ()() {
        while (true) {
            uint64_t number = (uint64_t) this->distribution(generator);
            if (number >= this->min && number <= this->max)
                return (uint64_t) number;
        }
    }
};

class ZipfianGenerator {
public: 
    ZipfianGenerator(int min, int max, double sigma) {
      zetan = zetastatic(0, max - min + 1, sigma, 0);
      items = max - min + 1;
      base = min;
      zipfianconstant = sigma; 
      theta = sigma;
      zeta2theta = zetastatic(0, 2, theta, 0);
      alpha = 1.0 / (1.0 - theta);
      countforzeta = items; 
      eta = compETA(items, theta, zeta2theta, zetan);
    }
    ~ZipfianGenerator() {};
    int nextValue(std::mt19937 &gen) {
      if (items != countforzeta) {
        if (items > countforzeta) {
            zetan = zeta(countforzeta, items, theta, zetan);
            eta = compETA(items, theta, zeta2theta, zetan);
        } 
      }
      double u = std::uniform_real_distribution<double>(0, 1)(gen);
      double uz = u * zetan;
      if (uz < 1.0) return base;
      if (uz < 1.0 + std::pow(0.5, theta)) return base + 1;
      return base + static_cast<int>(items * std::pow(eta * u - eta + 1, alpha));
    }
private: 
    double zetastatic(int st, int n, double theta, double initialsum) {
      for (int i = st; i < n; i++) 
        initialsum += 1 / std::pow(i+1, theta);
      return initialsum;
    }
    double zeta(int st, int n, double theta, double initialsum) {
      countforzeta = n;
      return zetastatic(st, n, theta, initialsum);
    }

    double compETA(int items, double theta, double zeta2theta, double zetan) {
      return (1 - std::pow(2.0 / items, 1 - theta)) / ( 1- zeta2theta / zetan);
    } 
    
    int items;
    int base;
    double zipfianconstant;

    double alpha, zetan, eta, theta, zeta2theta;

    int countforzeta;
};


void skipCSVHeader(std::ifstream& iostr);

std::vector<std::string> readCSVRow(std::ifstream& iostr);

std::pair<std::string, std::string> convertAPConnToAirports(std::string apconn);

using histogram = std::vector<std::pair<std::string, int>>;

std::map<std::string, histogram> createFPAHistograms(std::ifstream& iostr);

histogram createFPAHistogram(std::ifstream& iostr);

histogram createFPTHistogram(std::ifstream& iostr);

bool inline compHist(const std::pair<std::string, int> &e1, const std::pair<std::string, int> &e2) {
  return e1.second < e2.second;
}

std::string getRandValFromHistogram(const histogram &hist, std::mt19937 gen);

void FillColumnNamesWithGenericAttr( std::vector<std::pair<std::string, std::string>> &column_names_and_types,
    std::string generic_col_name, std::string generic_col_type, int num_cols);

const char ALPHA_NUMERIC[] = "0123456789abcdefghjijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

/** Generate random alpha-numeric string of length [min_len, max_len] */
std::string RandomANString(size_t min_len, size_t max_len, std::mt19937 &gen);

/** Generate random alphabetical string of length [min_len, max_len]*/
std::string RandomAString(size_t min_len, size_t max_len, std::mt19937 &gen);


/** Generate random numerical string of length [min_len, max_len] */
std::string RandomNString(size_t min_len, size_t max_len, std::mt19937 &gen);


}
#endif