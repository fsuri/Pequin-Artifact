#include "store/benchmark/async/json_table_writer.h"
#include "store/benchmark/async/sql/seats/seats_constants.h"
#include <gflags/gflags.h>
#include <random>
#include <sstream>
#include <iomanip>
#include <cmath>
#include <fstream>
#include <algorithm>
#include <queue>
#include <unordered_set>

const char* FLIGHTS_AIRPORT_HISTO_FN = "./resources/histogram_flights_per_airport.json";
const char* FLIGHTS_TIME_HISTO_FN = "./resources/histogram_flights_per_time.json";
const char* AIRLINE_SEATS_TABLE = "./resources/table_airline_seats.csv";
const char* AIRPORT_SEATS_TABLE = "./resources/table_airport_seats.csv";
const char* COUNTRY_SEATS_TABLE = "./resources/table_country_seats.csv";

std::vector<std::string> readCSVRow(std::ifstream& iostr) {
  std::vector<std::string> res;

  std::string line; 
  std::getline(iostr, line);
  std::stringstream lineStream(line);
  std::string cell; 

  while (std::getline(lineStream, cell, ',')) {
    if (cell[0] == '"') cell = cell.substr(1);
    if (cell[cell.size() - 1] == '"') cell = cell.substr(0, cell.size() - 1);
    res.push_back(cell);
  }

  return res;
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
  int rand = std::uniform_int_distribution<int>(1, hist.back().second)(gen);
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

std::unordered_map<std::string, int64_t> GenerateCountryTable(TableWriter &writer) {
    std::vector<std::pair<std::string, std::string>> column_names_and_types;
    column_names_and_types.push_back(std::make_pair("co_id", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("co_name", "TEXT"));
    column_names_and_types.push_back(std::make_pair("co_code_3", "TEXT")); // len 2
    column_names_and_types.push_back(std::make_pair("co_code_3", "TEXT")); // len 3
    const std::vector<uint32_t> primary_key_col_idx {0};

    std::string table_name = seats_sql::COUNTRY_TABLE;
    writer.add_table(table_name, column_names_and_types, primary_key_col_idx);
    std::ifstream file (COUNTRY_SEATS_TABLE);
    // skipped since first row is just column names
    std::vector<std::string> csv = readCSVRow(file);
    std::unordered_map<std::string, int64_t> ret;
    for (int co_id = 1; co_id <= seats_sql::NUM_COUNTRIES; co_id++) {
        std::vector<std::string> values; 
        csv = readCSVRow(file);

        values.push_back(std::to_string(co_id));
        values.push_back(csv[0]);
        values.push_back(csv[1]);
        values.push_back(csv[2]);
        ret[csv[2]] = co_id;
        writer.add_row(table_name, values);
        values.clear();
    }
    file.close();
    return ret;
}

std::vector<std::pair<double, double>> GenerateAirportTable(TableWriter &writer, std::unordered_map<std::string, int64_t> &co_code_to_id, std::unordered_map<std::string, int64_t> &ap_code_to_id) {
    std::vector<std::pair<std::string, std::string>> column_names_and_types;
    column_names_and_types.push_back(std::make_pair("ap_id", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("ap_code", "TEXT"));          // len 3
    column_names_and_types.push_back(std::make_pair("ap_name", "TEXT"));
    column_names_and_types.push_back(std::make_pair("ap_city", "TEXT"));
    column_names_and_types.push_back(std::make_pair("ap_postal_code", "TEXT"));   // len 12
    column_names_and_types.push_back(std::make_pair("ap_co_id", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("ap_longitude", "FLOAT"));
    column_names_and_types.push_back(std::make_pair("ap_latitude", "FLOAT"));
    column_names_and_types.push_back(std::make_pair("ap_gmt_offset", "FLOAT"));
    column_names_and_types.push_back(std::make_pair("ap_wac", "BIGINT"));
    FillColumnNamesWithGenericAttr(column_names_and_types, "ap_iattr", "BIGINT", 16);
  
    const std::vector<uint32_t> primary_key_col_idx {0};
    std::string table_name = seats_sql::AIRPORT_TABLE;
    writer.add_table(table_name, column_names_and_types, primary_key_col_idx);

    std::ifstream file (AIRPORT_SEATS_TABLE);
    std::vector<std::string> csv = readCSVRow(file);
    std::mt19937 gen;
    std::vector<std::pair<double, double>> airport_long_lats; 
    airport_long_lats.reserve(seats_sql::NUM_AIRPORTS);
    std::cerr << "prepared table" << std::endl;
    for (int ap_id = 1; ap_id <= seats_sql::NUM_AIRPORTS; ap_id++) {
      std::vector<std::string> values; 
      csv = readCSVRow(file);
      values.reserve(26);
      values.push_back(std::to_string(ap_id));
      values.push_back(csv[0]);                                      // ap_code
      ap_code_to_id[csv[0]] = ap_id;
      values.push_back(csv[1]);                                      // ap_name
      values.push_back(csv[2]);                                      // ap_city
      values.push_back(csv[3].substr(0, 8));                         // ap_postal_code
      if (co_code_to_id[csv[4]] == 0) values.push_back(std::to_string(std::uniform_int_distribution<int>(1, seats_sql::NUM_COUNTRIES)(gen)));
      else values.push_back(std::to_string(co_code_to_id[csv[4]]));    // ap_co_id
      std::string longitude = csv[5];
      std::string latitude = csv[6];
      std::cerr << "is alpha check" << std::endl;
      if (isalpha(longitude[1])) {
        longitude = csv[6];
        latitude = csv[7];
      }
      std::cerr << "past that" << std::endl;
      airport_long_lats.push_back(std::make_pair(stod(longitude), stod(latitude)));
      values.push_back(longitude);                                          // ap_longitude
      values.push_back(latitude);                                           // ap_latitude
      values.push_back(csv[7]);                                      // ap_gmt_offset
      values.push_back(csv[8]);                                      // ap_wac
      for (int iattr = 0; iattr < 16; iattr++) 
        values.push_back(std::to_string(std::uniform_int_distribution<int64_t>(1, std::numeric_limits<int64_t>::max())(gen)));
      std::cerr << "finished that" << std::endl;
      writer.add_row(table_name, values);
    }
    file.close();
    return airport_long_lats;
}

inline double rad2deg(double rad) {
  return (rad * 180.0) / M_PI;
}

inline double deg2rad(double deg) {
  return (deg * M_PI) / 180.0;
}

double calculateDistance(std::pair<double, double> ap_1, std::pair<double, double> ap_2) {
    auto dist = std::sin(deg2rad(ap_1.second)) * std::sin(deg2rad(ap_2.second)) + std::cos(deg2rad(ap_1.second)) * std::cos(deg2rad(ap_2.second)) * std::cos(deg2rad(ap_1.first - ap_1.second));
    dist = std::acos(dist);
    dist = rad2deg(dist);
    return (dist * 60 * 1.1515);
    //std::sqrt(std::pow(ap_1.first - ap_2.first, 2) + std::pow(ap_1.second - ap_2.second, 2))/6.0;
}

std::vector<std::vector<double>> GenerateAirportDistanceTable(TableWriter &writer, std::vector<std::pair<double, double>> &apid_long_lat) {
    std::vector<std::pair<std::string, std::string>> column_names_and_types;
    column_names_and_types.push_back(std::make_pair("d_ap_id0", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("d_ap_id1", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("d_distance", "FLOAT"));
    const std::vector<uint32_t> primary_key_col_idx {0, 1};
    std::string table_name = seats_sql::AIRPORT_DISTANCE_TABLE;
    writer.add_table(table_name, column_names_and_types, primary_key_col_idx);
    std::vector<std::vector<double>> dist_matrix;
    dist_matrix.reserve(seats_sql::NUM_AIRPORTS);
    for (int ap_id0 = 1; ap_id0 <= seats_sql::NUM_AIRPORTS; ap_id0++) {
      std::vector<double> dist_ap_0;
      dist_ap_0.reserve(seats_sql::NUM_AIRPORTS);
      for (int ap_id1 = 1; ap_id1 <= seats_sql::NUM_AIRPORTS; ap_id1++) {
        std::vector<std::string> values;
        values.push_back(std::to_string(ap_id0));
        values.push_back(std::to_string(ap_id1));
        double dist = 0;
        if (ap_id0 != ap_id1) 
          dist = calculateDistance(apid_long_lat[ap_id0 - 1], apid_long_lat[ap_id1 - 1]);
        values.push_back(std::to_string(dist));
        writer.add_row(table_name, values);

        dist_ap_0.push_back(dist);
      }
      dist_matrix.push_back(dist_ap_0);
    }
    return dist_matrix;
}

void GenerateAirlineTable(TableWriter &writer, std::unordered_map<std::string, int64_t> &co_code_to_id) {
    std::vector<std::pair<std::string, std::string>> column_names_and_types; 
    column_names_and_types.push_back(std::make_pair("al_id", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("al_iata_code", "TEXT"));
    column_names_and_types.push_back(std::make_pair("al_icao_code", "TEXT"));
    column_names_and_types.push_back(std::make_pair("al_call_sign", "TEXT"));
    column_names_and_types.push_back(std::make_pair("al_name", "TEXT"));
    column_names_and_types.push_back(std::make_pair("al_co_id", "BIGINT"));
    FillColumnNamesWithGenericAttr(column_names_and_types, "al_iattr", "BIGINT", 16);
    const std::vector<uint32_t> primary_key_col_idx {0};
    std::string table_name = seats_sql::AIRLINE_TABLE;
    writer.add_table(table_name, column_names_and_types, primary_key_col_idx);

    std::mt19937 gen;
    std::ifstream file (AIRLINE_SEATS_TABLE);
    std::vector<std::string> csv = readCSVRow(file);
    // generate data 
    for (int al_id = 1; al_id <= seats_sql::NUM_AIRLINES; al_id++) {
      std::vector<std::string> values; 
      csv = readCSVRow(file);
      values.reserve(22);
      values.push_back(std::to_string(al_id));
      values.push_back(csv[0]);
      values.push_back(csv[1]);
      values.push_back(csv[2]);
      values.push_back(csv[3]); 
      if (co_code_to_id[csv[4]] == 0) values.push_back(std::to_string(std::uniform_int_distribution<int>(1, seats_sql::NUM_COUNTRIES)(gen)));
      else values.push_back(std::to_string(co_code_to_id[csv[4]]));    // al_co_id
      for (int iattr = 0; iattr < 16; iattr++) 
        values.push_back(std::to_string(std::uniform_int_distribution<int64_t>(1, std::numeric_limits<int64_t>::max())(gen)));
      
      writer.add_row(table_name, values);
    }
    file.close();
}

void GenerateCustomerTable(TableWriter &writer) {
    std::vector<std::pair<std::string, std::string>> column_names_and_types; 
    column_names_and_types.push_back(std::make_pair("c_id", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("c_id_str", "TEXT"));
    column_names_and_types.push_back(std::make_pair("c_base_ap_id", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("c_balance", "FLOAT"));
    FillColumnNamesWithGenericAttr(column_names_and_types, "c_sattr", "TEXT", 20);
    FillColumnNamesWithGenericAttr(column_names_and_types, "c_iattr", "BIGINT", 20);
    const std::vector<uint32_t> primary_key_col_idx {0};
    std::string table_name = seats_sql::CUSTOMER_TABLE;
    writer.add_table(table_name, column_names_and_types, primary_key_col_idx);

     //Optional Index:
    const std::vector<uint32_t> index {1};
    writer.add_index(table_name, "customer_str_index", index);

    std::mt19937 gen;
    // generate data
    for (int c_id = 1; c_id <= seats_sql::NUM_CUSTOMERS; c_id++) {
      std::vector<std::string> values; 
      //values.reserve(44);
      values.push_back(std::to_string(c_id));       // c_id
      values.push_back(std::to_string(c_id));       // c_id_str
      values.push_back(std::to_string(std::uniform_int_distribution<int>(1, seats_sql::NUM_AIRPORTS)(gen)));
      values.push_back(std::to_string(std::uniform_real_distribution<float>(1000, 10000)(gen)));
      
      for (int sattr = 0; sattr < 20; sattr++) {
        values.push_back(RandomANString(8, 8, gen));
      }
      for (int iattr = 0; iattr < 20; iattr++) {
        values.push_back(std::to_string(std::uniform_int_distribution<int64_t>(1, 100000000)(gen)));
      }

      writer.add_row(table_name, values);
    }
}

bool containsId(std::vector<int64_t> &ids, int64_t to_find) {
  for (std::size_t i = 0; i < ids.size(); i++) {
    if (ids[i] == to_find) return true;
  }
  return false;
}

void GenerateFrequentFlyerTable(TableWriter &writer) {
    std::vector<std::pair<std::string, std::string>> column_names_and_types; 
    column_names_and_types.push_back(std::make_pair("ff_c_id", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("ff_al_id", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("ff_c_id_str", "TEXT"));
    FillColumnNamesWithGenericAttr(column_names_and_types, "ff_sattr", "TEXT", 4);
    FillColumnNamesWithGenericAttr(column_names_and_types, "ff_iattr", "BIGINT", 16);
    const std::vector<uint32_t> primary_key_col_idx {0, 1};
    std::string table_name = seats_sql::FREQUENT_FLYER_TABLE;
    writer.add_table(table_name, column_names_and_types, primary_key_col_idx);

    //Optional Index:
    const std::vector<uint32_t> index {0};
    writer.add_index(table_name, "ff_customer_index", index);

    std::mt19937 gen;
    // generate data
    for (int c_id = 1; c_id <= seats_sql::NUM_CUSTOMERS; c_id++) {
      std::vector<int64_t> al_per_customer;
      for (int al_num = 1; al_num <= 5; al_num++) {
        int64_t al_id = std::uniform_int_distribution<int64_t>(1, seats_sql::NUM_AIRLINES)(gen);
        while (containsId(al_per_customer, al_id)) {
          al_id = std::uniform_int_distribution<int64_t>(1, seats_sql::NUM_AIRLINES)(gen);
        }
        al_per_customer.push_back(al_id);
        std::vector<std::string> values; 
        values.push_back(std::to_string(c_id));
        values.push_back(std::to_string(al_id));
        values.push_back(std::to_string(c_id));
        for (int sattr = 0; sattr < 4; sattr++) {
          values.push_back(RandomANString(8, 32, gen));
        }
        for (int iattr = 0; iattr < 16; iattr++) {
          values.push_back(std::to_string(std::uniform_int_distribution<int64_t>(1, 100000000)(gen)));
        }

        writer.add_row(table_name, values);
      }
    }

}

inline int distToTime(double dist) {
  return (dist/seats_sql::FLIGHT_TRAVEL_RATE) * 3600000000L;
}

time_t convertStrToTime(std::string time) {
  std::stringstream sseam(time);
  std::string hour;
  std::string min;
  getline(sseam, hour, '"');
  getline(sseam, hour, ':');
  getline(sseam, min, '"');

  return 3600000 * stoi(hour) + 60000 * stoi(min);
} 

std::pair<std::string, std::string> convertAPConnToAirports(std::string apconn) {
  std::stringstream ss(apconn);
  std::pair<std::string, std::string> ret; 
  std::string temp; 
  getline(ss, temp, '"');
  getline(ss, ret.first, '-');
  getline(ss, ret.second, '"');

  return ret;
}

std::vector<int> GenerateFlightTable(TableWriter &writer, std::vector<std::vector<double>> &airport_distances, std::unordered_map<std::string, int64_t> ap_code_to_id, std::vector<std::pair<int64_t, int64_t>> &f_id_to_ap_conn) {
    std::vector<std::pair<std::string, std::string>> column_names_and_types; 
    column_names_and_types.push_back(std::make_pair("f_id", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("f_al_id", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("f_depart_ap_id", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("f_depart_time", "TIMESTAMP"));
    column_names_and_types.push_back(std::make_pair("f_arrive_ap_id", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("f_arrive_time", "TIMESTAMP"));
    column_names_and_types.push_back(std::make_pair("f_status", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("f_base_price", "FLOAT"));
    column_names_and_types.push_back(std::make_pair("f_seats_total", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("f_seats_left", "BIGINT"));
    FillColumnNamesWithGenericAttr(column_names_and_types, "f_iattr", "BIGINT", 30);
    const std::vector<uint32_t> primary_key_col_idx {0};
    std::string table_name = seats_sql::FLIGHT_TABLE;
    writer.add_table(table_name, column_names_and_types, primary_key_col_idx);
    const std::vector<uint32_t> index {3};
    writer.add_index(table_name, "f_depart_time_idx", index);
    // load histograms
    std::ifstream fa_hist (FLIGHTS_AIRPORT_HISTO_FN);
    histogram flight_airp_hist = createFPAHistogram(fa_hist);
    fa_hist.close(); 
    std::ifstream ft_hist (FLIGHTS_TIME_HISTO_FN);
    histogram flight_time_hist = createFPTHistogram(ft_hist);
    ft_hist.close();
    // generate data
    std::vector<int> flight_to_num_reserved;
    std::mt19937 gen;
    
    for (int f_id = 1; f_id <= seats_sql::NUM_FLIGHTS; f_id++) {
      std::vector<std::string> values; 
      values.push_back(std::to_string(f_id)); 
      values.push_back(std::to_string(std::binomial_distribution<int>(1, seats_sql::NUM_AIRLINES)(gen)));
      std::string ap_conn = getRandValFromHistogram(flight_airp_hist, gen);
      auto arr_dep_ap = convertAPConnToAirports(ap_conn);
      auto dep_ap_id = ap_code_to_id[arr_dep_ap.first];
      auto arr_ap_id = ap_code_to_id[arr_dep_ap.second];
      f_id_to_ap_conn.push_back(std::make_pair(dep_ap_id, arr_ap_id));
      auto dep_time =  std::binomial_distribution<std::time_t>(seats_sql::MIN_TS, seats_sql::MAX_TS - distToTime(airport_distances[dep_ap_id - 1][arr_ap_id - 1]))(gen);
      dep_time = (dep_time - (dep_time % seats_sql::MS_IN_DAY)) + convertStrToTime(getRandValFromHistogram(flight_time_hist, gen));
      values.push_back(std::to_string(dep_ap_id));
      values.push_back(std::to_string(dep_time));
      values.push_back(std::to_string(arr_ap_id));
      values.push_back(std::to_string(dep_time + distToTime(airport_distances[dep_ap_id - 1][arr_ap_id - 1])));
      values.push_back(std::to_string(std::uniform_int_distribution<int>(0, 1)(gen)));
      values.push_back(std::to_string(std::uniform_real_distribution<float>(100, 1000)(gen)));
      values.push_back(std::to_string(seats_sql::TOTAL_SEATS_PER_FLIGHT));
      auto seats_reserved = std::uniform_int_distribution<int>(seats_sql::MIN_SEATS_RESERVED, seats_sql::MAX_SEATS_RESERVED)(gen);
      values.push_back(std::to_string(seats_sql::TOTAL_SEATS_PER_FLIGHT - seats_reserved));
      flight_to_num_reserved.push_back(seats_reserved);
      for (int iattr = 0; iattr < 30; iattr++) {
        values.push_back(std::to_string(std::uniform_int_distribution<int64_t>(1, 100000000)(gen)));
      }

      writer.add_row(table_name, values);
    }
    return flight_to_num_reserved;
}

void GenerateReservationTable(TableWriter &writer, std::vector<int> flight_to_num_reserved, std::vector<std::pair<int64_t, int64_t>> &fl_to_ap_conn) {
    std::vector<std::pair<std::string, std::string>> column_names_and_types; 
    column_names_and_types.push_back(std::make_pair("r_id", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("r_c_id", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("r_f_id", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("r_seat", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("r_price", "FLOAT"));
    FillColumnNamesWithGenericAttr(column_names_and_types, "r_iattr", "BIGINT", 9);
    // column_names_and_types.push_back(std::make_pair("r_created", "TIMESTAMP"));
    // column_names_and_types.push_back(std::make_pair("r_updated", "TIMESTAMP"));
    const std::vector<uint32_t> primary_key_col_idx {0};
    std::string table_name = seats_sql::RESERVATION_TABLE;
    writer.add_table(table_name, column_names_and_types, primary_key_col_idx);

    //Optional Index:
     const std::vector<uint32_t> index {2};
    writer.add_index(table_name, "r_flight_index", index);
    
    // generate data
    std::mt19937 gen;   
    int r_id = 1;

    std::vector<std::queue<int64_t>> outbound_customers_per_ap_id(seats_sql::NUM_AIRPORTS, std::queue<int64_t>());

    for (int f_id = 1; f_id <= seats_sql::NUM_FLIGHTS; f_id++) {
      std::vector<int64_t> seat_ids;
      for (int r = 1; r <= flight_to_num_reserved[f_id]; r++) {
        std::vector<std::string> values; 
        values.push_back(std::to_string(r_id++));
        int64_t arr_ap_id = fl_to_ap_conn[f_id-1].second;
        int64_t c_id;
        if (outbound_customers_per_ap_id[arr_ap_id-1].empty()) {
          c_id = std::uniform_int_distribution<int>(1, seats_sql::NUM_CUSTOMERS)(gen);
          int64_t ret_ap_id = fl_to_ap_conn[f_id-1].first;
          outbound_customers_per_ap_id[ret_ap_id-1].push(c_id);
        } else {
          c_id = outbound_customers_per_ap_id[arr_ap_id-1].front();
          outbound_customers_per_ap_id[arr_ap_id-1].pop();
        }

        values.push_back(std::to_string(c_id));
        values.push_back(std::to_string(f_id));
        int64_t seat = std::uniform_int_distribution<int64_t>(1, seats_sql::TOTAL_SEATS_PER_FLIGHT)(gen);
        while (containsId(seat_ids, seat))
          seat = std::uniform_int_distribution<int64_t>(1, seats_sql::TOTAL_SEATS_PER_FLIGHT)(gen);
        seat_ids.push_back(seat);
        values.push_back(std::to_string(seat));
        values.push_back(std::to_string(std::uniform_int_distribution<int>(seats_sql::MIN_RESERVATION_PRICE, seats_sql::MAX_RESERVATION_PRICE)(gen)));
        for (int iattr = 0; iattr < 9; iattr++) {
          values.push_back(std::to_string(std::uniform_int_distribution<int64_t>(1, 100000000)(gen)));
        }
        // for r_updated, r_created
        //values.push_back(std::to_string(time(0)));
        //values.push_back(std::to_string(time(0)));

        writer.add_row(table_name, values);
      }
    }
}


int main(int argc, char *argv[]) {
    gflags::SetUsageMessage(
        "generates json file containing SQL tables for SEATS data\n");
    std::string file_name = "sql-seats";
    TableWriter writer = TableWriter(file_name);
    std::cerr << "Generating SEATS Tables" << std::endl;
    // generate all tables
    auto co_code_to_id = GenerateCountryTable(writer);
    std::cerr << "Finished Country" << std::endl;
    GenerateAirlineTable(writer, co_code_to_id);
    std::cerr << "Finished Airline" << std::endl;
    std::unordered_map<std::string, int64_t> ap_code_to_id;
    auto airport_coords = GenerateAirportTable(writer, co_code_to_id, ap_code_to_id);
    std::cerr << "Finished Airport" << std::endl;
    auto airport_dists = GenerateAirportDistanceTable(writer, airport_coords);
    //std::vector<std::vector<double>> airport_dists = std::vector<std::vector<double>>(seats_sql::NUM_AIRPORTS, std::vector<double>(seats_sql::NUM_AIRPORTS, 0.0));
    std::cerr << "Finished AD" << std::endl;
    std::vector<std::pair<int64_t, int64_t>> f_id_to_ap_conn;
    auto flight_reserves = GenerateFlightTable(writer, airport_dists, ap_code_to_id, f_id_to_ap_conn);
    std::cerr << "Finished Flights" << std::endl;
    GenerateReservationTable(writer, flight_reserves, f_id_to_ap_conn);
    std::cerr << "Finished Reservation" << std::endl;
    GenerateCustomerTable(writer);
    std::cerr << "Finished Customer" << std::endl;
    GenerateFrequentFlyerTable(writer);
    std::cerr << "Finished FF" << std::endl;
    writer.flush();
    std::cerr << "Finished SEATS Table Generation" << std::endl;
    return 0;
}