#include "store/benchmark/async/json_table_writer.h"
#include "store/benchmark/async/sql/seats/seats_constants.h"
#include <random>
#include <sstream>
#include <iomanip>
#include <cmath>

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

const char* ALPHA_NUMERIC = "0123456789abcdefghjijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

/** Generate random alpha-numeric string of length [min_len, max_len] */
std::string RandomANString(size_t min_len, size_t max_len, std::mt19937 &gen) {
  std::string s;
  size_t length = std::uniform_int_distribution<size_t>(min_len,  max_len)(gen);
  for (size_t i = 0; i < length; ++i) {
    int j = std::uniform_int_distribution<size_t>(0, sizeof(ALPHA_NUMERIC) - 2)(gen);
    s += ALPHA_NUMERIC[j];
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

void GenerateCountryTable(TableWriter &writer) {
    std::vector<std::pair<std::string, std::string>> column_names_and_types;
    column_names_and_types.push_back(std::make_pair("CO_ID", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("CO_NAME", "TEXT"));
    column_names_and_types.push_back(std::make_pair("CO_CODE_2", "TEXT")); // len 2
    column_names_and_types.push_back(std::make_pair("CO_CODE_3", "TEXT")); // len 3
    const std::vector<uint32_t> primary_key_col_idx {0};

    std::string table_name = seats_sql::COUNTRY_TABLE;
    writer.add_table(table_name, column_names_and_types, primary_key_col_idx);
    std::mt19937 gen;

    for (int co_id = 1; co_id <= seats_sql::NUM_COUNTRIES; co_id++) {
        std::vector<std::string> values; 
        values.push_back(std::to_string(co_id));
        values.push_back(RandomAString(5, 30, gen));  // co_name
        values.push_back(RandomANString(2, 2, gen));  // co_code_2 
        values.push_back(RandomANString(3, 3, gen));  // co_code_3

        writer.add_row(table_name, values);
    }
}

std::vector<std::pair<double, double>> GenerateAirportTable(TableWriter &writer) {
    std::vector<std::pair<std::string, std::string>> column_names_and_types;
    column_names_and_types.push_back(std::make_pair("AP_ID", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("AP_CODE", "TEXT"));          // len 3
    column_names_and_types.push_back(std::make_pair("AP_NAME", "TEXT"));
    column_names_and_types.push_back(std::make_pair("AP_CITY", "TEXT"));
    column_names_and_types.push_back(std::make_pair("AP_POSTAL_CODE", "TEXT"));   // len 12
    column_names_and_types.push_back(std::make_pair("AP_CO_ID", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("AP_LONGITUDE", "FLOAT"));
    column_names_and_types.push_back(std::make_pair("AP_LATITUDE", "FLOAT"));
    column_names_and_types.push_back(std::make_pair("AP_GMT_OFFSET", "FLOAT"));
    column_names_and_types.push_back(std::make_pair("AP_WAC", "BIGINT"));
    FillColumnNamesWithGenericAttr(column_names_and_types, "AP_IATTR", "BIGINT", 16);
    std::mt19937 gen;

    const std::vector<uint32_t> primary_key_col_idx {0};
    std::string table_name = seats_sql::AIRPORT_TABLE;
    writer.add_table(table_name, column_names_and_types, primary_key_col_idx);
    std::vector<std::pair<double, double>> airport_long_lats; 
    airport_long_lats.reserve(seats_sql::NUM_AIRPORTS);
    for (int ap_id = 1; ap_id <= seats_sql::NUM_AIRPORTS; ap_id++) {
      std::vector<std::string> values; 
      values.push_back(std::to_string(ap_id));
      std::string ap_code = "" + ALPHA_NUMERIC[36 + (((ap_id-1) / 676) % 26)] + ALPHA_NUMERIC[36 + (((ap_id-1)/26) % 26)] + ALPHA_NUMERIC[36 + ((ap_id-1) % 26)];
      values.push_back(ap_code);
      values.push_back(RandomAString(10, 60, gen));   // ap_name
      values.push_back(RandomAString(5, 20, gen));    // ap_city
      values.push_back(RandomNString(12, 12, gen));   // ap_postal_code
      values.push_back(std::to_string(std::uniform_int_distribution<int>(1, seats_sql::NUM_COUNTRIES)(gen)));   // ap_co_id
      auto longitude = std::uniform_real_distribution<double>(-180.0, 180.0)(gen);
      auto latitude = std::uniform_real_distribution<double>(-12, 12)(gen);
      airport_long_lats.push_back(std::make_pair(longitude, latitude));
      values.push_back(std::to_string(longitude));     // ap_longitude
      values.push_back(std::to_string(latitude));      // ap_latitude
      values.push_back(std::to_string(std::uniform_int_distribution<float>(-12, 12)(gen)));            // ap_gmt_offset
      values.push_back(RandomNString(3, 3, gen));     // ap_wac
      for (int iattr = 0; iattr < 16; iattr++) 
        values.push_back(std::to_string(std::uniform_int_distribution<int64_t>(1, 1000000)(gen)));
      
      writer.add_row(table_name, values);
    }
    return airport_long_lats;
}

float calculateDistance(std::pair<double, double> ap_1, std::pair<double, double> ap_2) {
    // divide by 6 to make distances less extreme
    return std::sqrt(std::pow(ap_1.first - ap_2.first, 2) + std::pow(ap_1.second - ap_2.second, 2))/6.0;
}

std::vector<std::vector<double>> GenerateAirportDistanceTable(TableWriter &writer, std::vector<std::pair<double, double>> &apid_long_lat) {
    std::vector<std::pair<std::string, std::string>> column_names_and_types;
    column_names_and_types.push_back(std::make_pair("D_AP_ID0", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("D_AP_ID1", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("D_DISTANCE", "FLOAT"));
    const std::vector<uint32_t> primary_key_col_idx {0, 1};
    std::string table_name = seats_sql::AIRPORT_DISTANCE_TABLE;
    writer.add_table(table_name, column_names_and_types, primary_key_col_idx);
    std::vector<std::vector<double>> dist_matrix;

    for (int ap_id0 = 1; ap_id0 <= seats_sql::NUM_AIRPORTS; ap_id0++) {
      std::vector<double> dist_ap_0;
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

void GenerateAirlineTable(TableWriter &writer) {
    std::vector<std::pair<std::string, std::string>> column_names_and_types; 
    column_names_and_types.push_back(std::make_pair("AL_ID", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("AL_IATA_CODE", "TEXT"));
    column_names_and_types.push_back(std::make_pair("AL_ICAO_CODE", "TEXT"));
    column_names_and_types.push_back(std::make_pair("AL_CALL_SIGN", "TEXT"));
    column_names_and_types.push_back(std::make_pair("AL_NAME", "TEXT"));
    column_names_and_types.push_back(std::make_pair("AL_CO_ID", "BIGINT"));
    FillColumnNamesWithGenericAttr(column_names_and_types, "AL_IATTR", "BIGINT", 16);
    const std::vector<uint32_t> primary_key_col_idx {0};
    std::string table_name = seats_sql::AIRLINE_TABLE;
    writer.add_table(table_name, column_names_and_types, primary_key_col_idx);

    std::mt19937 gen;
    // generate data 
    for (int al_id = 1; al_id <= seats_sql::NUM_AIRLINES; al_id++) {
      std::vector<std::string> values; 
      values.push_back(std::to_string(al_id));
      values.push_back(RandomAString(3, 3, gen));
      values.push_back(RandomAString(3, 3, gen));
      values.push_back(RandomAString(3, 20, gen));
      values.push_back(RandomAString(10, 30, gen)); 
      values.push_back(std::to_string(std::uniform_int_distribution<int>(1, seats_sql::NUM_COUNTRIES)(gen)));
      for (int iattr = 0; iattr < 16; iattr++) 
        values.push_back(std::to_string(std::uniform_int_distribution<int>(1, 1000000)(gen)));
      
      writer.add_row(table_name, values);
    }
}

void GenerateCustomerTable(TableWriter &writer) {
    std::vector<std::pair<std::string, std::string>> column_names_and_types; 
    column_names_and_types.push_back(std::make_pair("C_ID", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("C_ID_STR", "TEXT"));
    column_names_and_types.push_back(std::make_pair("C_BASE_AP_ID", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("C_BALANCE", "FLOAT"));
    FillColumnNamesWithGenericAttr(column_names_and_types, "C_SATTR", "TEXT", 20);
    FillColumnNamesWithGenericAttr(column_names_and_types, "C_IATTR", "BIGINT", 20);
    const std::vector<uint32_t> primary_key_col_idx {0};
    std::string table_name = seats_sql::CUSTOMER_TABLE;
    writer.add_table(table_name, column_names_and_types, primary_key_col_idx);

    std::mt19937 gen;
    // generate data
    for (int c_id = 1; c_id <= seats_sql::NUM_CUSTOMERS; c_id++) {
      std::vector<std::string> values; 
      values.push_back(std::to_string(c_id));       // c_id
      values.push_back(std::to_string(c_id));       // c_id_str
      values.push_back(std::to_string(std::uniform_int_distribution<int>(1, seats_sql::NUM_AIRPORTS)(gen)));
      values.push_back(std::to_string(std::uniform_real_distribution<float>(1000, 10000)(gen)));
      
      for (int sattr = 0; sattr < 20; sattr++) {
        values.push_back(RandomANString(3, 8, gen));
      }
      for (int iattr = 0; iattr < 20; iattr++) {
        values.push_back(std::to_string(std::uniform_int_distribution<int>(1, 1000000)(gen)));
      }

      writer.add_row(table_name, values);
    }
}

void GenerateFrequentFlyerTable(TableWriter &writer) {
    std::vector<std::pair<std::string, std::string>> column_names_and_types; 
    column_names_and_types.push_back(std::make_pair("FF_C_ID", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("FF_AL_ID", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("FF_C_ID_STR", "TEXT"));
    FillColumnNamesWithGenericAttr(column_names_and_types, "FF_SATTR", "TEXT", 4);
    FillColumnNamesWithGenericAttr(column_names_and_types, "FF_IATTR", "BIGINT", 16);
    const std::vector<uint32_t> primary_key_col_idx {0, 1};
    std::string table_name = seats_sql::FREQUENT_FLYER_TABLE;
    writer.add_table(table_name, column_names_and_types, primary_key_col_idx);

    std::mt19937 gen;
    // generate data
    for (int c_id = 1; c_id <= seats_sql::NUM_CUSTOMERS; c_id++) {
      for (int al_id = 1; al_id <= seats_sql::NUM_AIRLINES; al_id++) {
        std::vector<std::string> values; 
        values.push_back(std::to_string(c_id));
        values.push_back(std::to_string(al_id));
        values.push_back(std::to_string(c_id));
        for (int sattr = 0; sattr < 4; sattr++) {
          values.push_back(RandomANString(8, 32, gen));
        }
        for (int iattr = 0; iattr < 16; iattr++) {
          values.push_back(std::to_string(std::uniform_int_distribution<int>(1, 1000000)(gen)));
        }

        writer.add_row(table_name, values);
      }
    }

}

inline int distToTime(double dist) {
  // time to travel is (distance/5) hours
  return (dist * 3600000)/5;
}

std::vector<int> GenerateFlightTable(TableWriter &writer, const std::vector<std::vector<double>> &airport_distances) {
    std::vector<std::pair<std::string, std::string>> column_names_and_types; 
    column_names_and_types.push_back(std::make_pair("F_ID", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("F_AL_ID", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("F_DEPART_AP_ID", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("F_DEPART_TIME", "TIMESTAMP"));
    column_names_and_types.push_back(std::make_pair("F_ARRIVE_AP_ID", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("F_ARRIVE_TIME", "TIMESTAMP"));
    column_names_and_types.push_back(std::make_pair("F_BASE_PRICE", "FLOAT"));
    column_names_and_types.push_back(std::make_pair("F_SEATS_TOTAL", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("F_SEATS_LEFT", "BIGINT"));
    FillColumnNamesWithGenericAttr(column_names_and_types, "F_IATTR", "BIGINT", 30);
    const std::vector<uint32_t> primary_key_col_idx {0};
    std::string table_name = seats_sql::FLIGHT_TABLE;
    writer.add_table(table_name, column_names_and_types, primary_key_col_idx);

    // generate data
    std::vector<int> flight_to_num_reserved;
    std::mt19937 gen;
    for (int f_id = 0; f_id < seats_sql::NUM_FLIGHTS; f_id++) {
      std::vector<std::string> values; 
      values.push_back(std::to_string(f_id)); 
      values.push_back(std::to_string(std::uniform_int_distribution<int>(1, seats_sql::NUM_AIRLINES)(gen)));
      auto dep_ap_id = std::uniform_int_distribution<int>(1, seats_sql::NUM_AIRPORTS)(gen);
      auto arr_ap_id = std::uniform_int_distribution<int>(1, seats_sql::NUM_AIRPORTS)(gen);
      auto dep_time =  std::uniform_int_distribution<std::time_t>(seats_sql::MIN_TS, seats_sql::MAX_TS - distToTime(airport_distances[dep_ap_id][arr_ap_id]))(gen);
      values.push_back(std::to_string(dep_ap_id));
      values.push_back(std::to_string(dep_time));
      values.push_back(std::to_string(arr_ap_id));
      values.push_back(std::to_string(dep_time + distToTime(airport_distances[dep_ap_id][arr_ap_id])));
      values.push_back(std::to_string(std::uniform_real_distribution<float>(100, 1000)(gen)));
      values.push_back(std::to_string(seats_sql::TOTAL_SEATS_PER_FLIGHT));
      auto seats_reserved = std::uniform_int_distribution<int>(seats_sql::MIN_SEATS_RESERVED, seats_sql::MAX_SEATS_RESERVED)(gen);
      values.push_back(std::to_string(seats_sql::TOTAL_SEATS_PER_FLIGHT - seats_reserved));
      flight_to_num_reserved.push_back(seats_reserved);
      for (int iattr = 0; iattr < 30; iattr++) {
        values.push_back(std::to_string(std::uniform_int_distribution<int>(1, 1000000)(gen)));
      }

      writer.add_row(table_name, values);
    }
    return flight_to_num_reserved;
}

void GenerateReservationTable(TableWriter &writer, std::vector<int> flight_to_num_reserved) {
    std::vector<std::pair<std::string, std::string>> column_names_and_types; 
    column_names_and_types.push_back(std::make_pair("R_ID", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("R_C_ID", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("R_F_ID", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("R_SEAT", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("R_PRICE", "FLOAT"));
    FillColumnNamesWithGenericAttr(column_names_and_types, "R_IATTR", "BIGINT", 9);
    column_names_and_types.push_back(std::make_pair("R_CREATED", "TIMESTAMP"));
    column_names_and_types.push_back(std::make_pair("R_UPDATED", "TIMESTAMP"));
    const std::vector<uint32_t> primary_key_col_idx {0};
    std::string table_name = seats_sql::RESERVATION_TABLE;
    writer.add_table(table_name, column_names_and_types, primary_key_col_idx);

    // generate data
    std::mt19937 gen; 
    int r_id = 1;
    for (int f_id = 1; f_id <= seats_sql::NUM_FLIGHTS; f_id++) {
      for (int r = 1; r <= flight_to_num_reserved[f_id]; r++) {
        std::vector<std::string> values; 
        values.push_back(std::to_string(r_id++));
        values.push_back(std::to_string(std::uniform_int_distribution<int>(1, seats_sql::NUM_CUSTOMERS)(gen)));
        values.push_back(std::to_string(f_id));
        values.push_back(std::to_string(std::uniform_int_distribution<int>(1, seats_sql::TOTAL_SEATS_PER_FLIGHT)(gen)));
        values.push_back(std::to_string(std::uniform_int_distribution<float>(seats_sql::MIN_RESERVATION_PRICE, seats_sql::MAX_RESERVATION_PRICE)(gen)));
        for (int iattr = 0; iattr < 9; iattr++) {
          values.push_back(std::to_string(std::uniform_int_distribution<int>(1, 1000000)(gen)));
        }
        values.push_back(std::to_string(time(0)));
        values.push_back(std::to_string(time(0)));

        writer.add_row(table_name, values);
      }
    }
}


int main(int argc, char *argv[]) {
    /*gflags::SetUsageMessage(
        "generates json file containing SQL tables for SEATS data\n");
    
    gflags::ParseCommandLineFlags(&argc, &argv, true);*/
    std::string file_name = "sql-seats";
    TableWriter writer = TableWriter(file_name);
    uint32_t time = std::time(0);
    std::cerr << "Generating SEATS Tables" << std::endl;
    // generate all tables
    GenerateCountryTable(writer);
    GenerateAirlineTable(writer);
    auto airport_coords = GenerateAirportTable(writer);
    auto airport_dists = GenerateAirportDistanceTable(writer, airport_coords);
    auto flight_reserves = GenerateFlightTable(writer, airport_dists);
    GenerateReservationTable(writer, flight_reserves);
    GenerateCustomerTable(writer);
    GenerateFrequentFlyerTable(writer);
    writer.flush();
    std::cerr << "Finished SEATS Table Generation" << std::endl;
    return 0;
}