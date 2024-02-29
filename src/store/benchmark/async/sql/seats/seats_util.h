#ifndef SEATS_SQL_UTILS_H
#define SEATS_SQL_UTILS_H

#include "store/benchmark/async/sql/seats/seats_constants.h"
#include "store/benchmark/async/sql/seats/cached_flight.h"
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


const char* FLIGHTS_AIRPORT_HISTO_FN = "./resources/histogram_flights_per_airport.json";
const char* FLIGHTS_TIME_HISTO_FN = "./resources/histogram_flights_per_time.json";
const char* AIRLINE_SEATS_TABLE = "./resources/table_airline_seats.csv";
const char* AIRPORT_SEATS_TABLE = "./resources/table_airport_seats.csv";
const char* COUNTRY_SEATS_TABLE = "./resources/table_country_seats.csv";


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


void skipCSVHeader(std::ifstream& iostr){
  //Skip header
  std::string columns;
  getline(iostr, columns); 
}

std::vector<std::string> readCSVRow(std::ifstream& iostr) {
  std::vector<std::string> res;

  std::string line; 
  std::getline(iostr, line);
  std::stringstream lineStream(line);
  std::string cell; 

  //std::cerr << std::endl << line << std::endl;
  std::string combination_cell = "";

  //TODO: Do not parse on entries that are of form "dallas, Texas". Those should be treated as one cell.
        //TODO: If split cell, but cell has only open quotes, then it must be part of a second word. Don't push back cell, until we get next cell, and then combine them...
      //Alternatively: Edit CSV to remove all such entries.

  while (std::getline(lineStream, cell, ',')) {
    // if (cell[0] == '"') cell = cell.substr(1);
    // if (cell[cell.size() - 1] == '"') cell = cell.substr(0, cell.size() - 1);
    bool add_to_res = true;
    if(cell.empty()){} //do nothing
    else if(cell[0] == '"' && cell[cell.size() - 1] != '"'){
      add_to_res = false;
    }
    else if(cell[0] != '"' && cell[cell.size() - 1] == '"'){
      cell = combination_cell + ";" + cell;
      combination_cell = "";
      //std::cerr << "combined: " << cell << std::endl;
    }

    //std::cerr << "cell before erase: " << cell << std::endl;
    cell.erase(std::remove(cell.begin(), cell.end(), '\"' ),cell.end()); //remove enclosing quotes
    cell.erase(std::remove(cell.begin(), cell.end(), '\'' ),cell.end()); //remove any internal single quotes (TODO: if one wants them, then they must be doubled)
    //std::cerr << "cell after erase: " << cell << std::endl;

    if(add_to_res) res.push_back(cell);
    else{
      combination_cell += cell;
     // std::cerr << "first part: " << combination_cell << std::endl;
    }
  }

  return res;
}

void writeCachedFlights(std::queue<seats_sql::CachedFlight> cf_q) {
  std::ofstream profile;
  profile.open(seats_sql::PROFILE_FILE_NAME);
  profile << "Flight Id, Airline Id, Arrive AP Id, Depart AP Id, Depart Time\n";
  while (!cf_q.empty()) {
    seats_sql::CachedFlight cf = cf_q.front();
    profile << cf.flight_id << "," << cf.airline_id << "," << cf.depart_ap_id << "," << cf.depart_time << "," << cf.arrive_ap_id << "\n";
    cf_q.pop();
  }
  profile.close();
}


using histogram = std::vector<std::pair<std::string, int>>;

histogram createFPAHistogram(std::ifstream& iostr) {
  std::vector<std::pair<std::string, int>> res; 
  std::string line; 
  // skip first few lines that aren't relevant data
  for (int i = 0; i < 4; i++) 
    std::getline(iostr, line); 

  std::stringstream lineStream(line);
  std::string cell; 
  
  while (std::getline(lineStream, cell, ':')) {
    //std::cerr << "line: " << line << std::endl;
    std::string key = cell; 
    std::getline(lineStream, cell, ',');
    std::string val = cell; 
    int v = stoi(val);
    if (!res.empty())
      v = stoi(val) + res.back().second;
    
    res.push_back(std::make_pair(key, v));

    // grab next line
    std::getline(iostr, line); 
    lineStream.str(line);
  }
  return res; 
}

histogram createFPTHistogram(std::ifstream& iostr) {
    std::vector<std::pair<std::string, int>> res; 
  std::string line; 
  // skip first few lines that aren't relevant data
  for (int i = 0; i < 4; i++) 
    std::getline(iostr, line); 

  std::stringstream lineStream(line);
  std::string cell; 
  
  while (std::getline(lineStream, cell, ':')) {
    std::string key = cell; 
    std::getline(lineStream, cell, ':');
    key += ":" + cell;
    std::getline(lineStream, cell, ',');
    std::string val = cell; 
    int v = stoi(val);
    if (!res.empty())
      v = stoi(val) + res.back().second;
    
    res.push_back(std::make_pair(key, v));

    // grab next line
    std::getline(iostr, line); 
    lineStream.str(line);
  }
  return res; 
}

bool inline compHist(const std::pair<std::string, int> &e1, const std::pair<std::string, int> &e2) {
  return e1.second < e2.second;
}

std::string getRandValFromHistogram(const histogram &hist, std::mt19937 gen) {
  int rand = std::uniform_int_distribution<int>(1, hist.back().second)(gen) - 1;
  std::string ret = std::upper_bound(hist.begin(), hist.end(), std::make_pair("", rand), compHist)->first; 
  return ret;
}

void FillColumnNamesWithGenericAttr( std::vector<std::pair<std::string, std::string>> &column_names_and_types,
    std::string generic_col_name, std::string generic_col_type, int num_cols) {
    std::ostringstream ss;
    for (int i = 0; i < num_cols; i++) {
        ss << std::setw(2) << std::setfill('0') << i;
        column_names_and_types.push_back(std::make_pair(generic_col_name + ss.str(), generic_col_type));
        ss.str(std::string()); 
        ss.clear();
    }
}

const char ALPHA_NUMERIC[] = "0123456789abcdefghjijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

/** Generate random alpha-numeric string of length [min_len, max_len] */
std::string RandomANString(size_t min_len, size_t max_len, std::mt19937 &gen) {
  std::string s;
  size_t length = std::uniform_int_distribution<size_t>(min_len,  max_len)(gen);
  for (size_t i = 0; i < length; ++i) {
    int j = std::uniform_int_distribution<size_t>(0, sizeof(ALPHA_NUMERIC) - 2)(gen);
    s.push_back(ALPHA_NUMERIC[j]);
  }
  return s;
}

/** Generate random alphabetical string of length [min_len, max_len]*/
std::string RandomAString(size_t min_len, size_t max_len, std::mt19937 &gen) {
  std::string s;
  size_t length = std::uniform_int_distribution<size_t>(min_len,  max_len)(gen);
  for (size_t i = 0; i < length; ++i) {
    int j = std::uniform_int_distribution<size_t>(10, sizeof(ALPHA_NUMERIC) - 2)(gen);
    s += ALPHA_NUMERIC[j];
  }
  return s;
}


/** Generate random numerical string of length [min_len, max_len] */
std::string RandomNString(size_t min_len, size_t max_len, std::mt19937 &gen) {
  std::string s;
  size_t length = std::uniform_int_distribution<size_t>(min_len,  max_len)(gen);
  for (size_t i = 0; i < length; ++i) {
    int j = std::uniform_int_distribution<size_t>(0, 9)(gen);
    s += ALPHA_NUMERIC[j];
  }
  return s;
}



#endif