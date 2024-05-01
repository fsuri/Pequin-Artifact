#include "store/benchmark/async/json_table_writer.h"
#include "store/benchmark/async/sql/seats/seats_constants.h"
#include "store/benchmark/async/sql/seats/seats_profile.h"

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

//#include "seats_util.h"

DEFINE_int32(max_airports, -1, "number of airports (-1 == uncapped)");
DEFINE_int32(k_nearest_airports, 10, "number of distances stored (nearest k)");

DEFINE_int32(scale_factor, 1, "scaling factor"); 

//TABLE GENERATORS

namespace seats_sql {

std::unordered_map<std::string, int64_t> GenerateCountryTable(TableWriter &writer) {
    std::vector<std::pair<std::string, std::string>> column_names_and_types;
    column_names_and_types.push_back(std::make_pair("co_id", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("co_name", "TEXT"));
    column_names_and_types.push_back(std::make_pair("co_code_2", "TEXT")); // len 2
    column_names_and_types.push_back(std::make_pair("co_code_3", "TEXT")); // len 3
    const std::vector<uint32_t> primary_key_col_idx {0};

    std::string table_name = seats_sql::COUNTRY_TABLE;
    writer.add_table(table_name, column_names_and_types, primary_key_col_idx);
    std::ifstream file (seats_sql::COUNTRY_SEATS_TABLE);
   
    std::unordered_map<std::string, int64_t> ret;
    skipCSVHeader(file);  // skipped first row since it is just column names
    for (int co_id = 1; co_id <= seats_sql::NUM_COUNTRIES; co_id++) {
    
        std::vector<std::string> values; 
        std::vector<std::string> csv = readCSVRow(file);

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
    column_names_and_types.push_back(std::make_pair("ap_longitude", "FLOAT")); //TODO: Don't really need to support FLOAT for this.. INT will be fine.
    column_names_and_types.push_back(std::make_pair("ap_latitude", "FLOAT"));  
    column_names_and_types.push_back(std::make_pair("ap_gmt_offset", "FLOAT"));  
    column_names_and_types.push_back(std::make_pair("ap_wac", "BIGINT"));
    FillColumnNamesWithGenericAttr(column_names_and_types, "ap_iattr", "BIGINT", 16);
  
    const std::vector<uint32_t> primary_key_col_idx {0};
    std::string table_name = seats_sql::AIRPORT_TABLE;
    writer.add_table(table_name, column_names_and_types, primary_key_col_idx);

    std::ifstream file (seats_sql::AIRPORT_SEATS_TABLE);
    
    std::mt19937 gen;
    std::vector<std::pair<double, double>> airport_long_lats; 
    airport_long_lats.reserve(seats_sql::NUM_AIRPORTS);

    skipCSVHeader(file);  // skipped first row since it is just column names
    for (int ap_id = 1; ap_id <= seats_sql::NUM_AIRPORTS; ap_id++) {
      //std::cerr << "row# " << ap_id << std::endl;
      std::vector<std::string> values; 
      std::vector<std::string> csv = readCSVRow(file);
    
      if(csv.size() < 9){
        csv.push_back("");
      } 

      //values.reserve(26);
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

      // if (isalpha(longitude[1])) {
      //   std::cerr << "THIS CODE SHOULD NEVER GET TRIGGERED. Longitude: " << longitude << std::endl; 
      //   for(auto &val : csv){
      //     std::cerr << val << ", " << std::endl;
      //   }
      //   longitude = csv[6];
      //   latitude = csv[7];
      // }

      airport_long_lats.push_back(std::make_pair(stod(longitude), stod(latitude)));
      values.push_back(longitude);                                          // ap_longitude
      values.push_back(latitude);                                           // ap_latitude
      values.push_back(csv[7].empty() ? "0" : csv[7]);                                      // ap_gmt_offset
      values.push_back(csv[8].empty() ? "0" : csv[8]);                                      // ap_wac
      for (int iattr = 0; iattr < 16; iattr++) 
        values.push_back(std::to_string(std::uniform_int_distribution<int64_t>(1, std::numeric_limits<int64_t>::max())(gen)));

      writer.add_row(table_name, values);
    }
    file.close();

    std::cerr << "WRITE AP ID" << std::endl;
    writeAirportIDs(ap_code_to_id); //write to profile.

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
    //std::cerr << "distance: " << (dist * 60 * 1.1515) << std::endl;
    return (dist * 60 * 1.1515);
    //std::sqrt(std::pow(ap_1.first - ap_2.first, 2) + std::pow(ap_1.second - ap_2.second, 2))/6.0;
}


//Compute all Airport Distances + Store a Table that holds "nearby" airports and their distances
std::vector<std::vector<double>> GenerateAirportDistanceTableBounded(TableWriter &writer, std::vector<std::pair<double, double>> &apid_long_lat) {
    std::vector<std::pair<std::string, std::string>> column_names_and_types;
    // column_names_and_types.push_back(std::make_pair("d_ap_id0", "BIGINT"));
    // column_names_and_types.push_back(std::make_pair("d_distance", "FLOAT"));
    // column_names_and_types.push_back(std::make_pair("d_ap_id1", "BIGINT"));
    // const std::vector<uint32_t> primary_key_col_idx {0, 1, 2};

    column_names_and_types.push_back(std::make_pair("d_ap_id0", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("d_ap_id1", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("d_distance", "FLOAT"));
    const std::vector<uint32_t> primary_key_col_idx {0, 1};

    std::string table_name = seats_sql::AIRPORT_DISTANCE_TABLE;
    writer.add_table(table_name, column_names_and_types, primary_key_col_idx);
    std::vector<std::vector<double>> dist_matrix;
    dist_matrix.reserve(seats_sql::NUM_AIRPORTS);
    for (int ap_id0 = 1; ap_id0 <= seats_sql::NUM_AIRPORTS; ap_id0++) {
      //std::cerr << std::endl << "airport: " << ap_id0 << std::endl;

     // std::vector<double> nearest_dist();
      std::vector<double> dist_ap_0;

      std::priority_queue<std::pair<double, int>> nearest;
      // std::vector<std::pair<double, int>> nearest; //each item is a tuple (dist, ap1) 
      // std::make_heap(nearest.begin(), nearest.end()); //max heap.

      for (int ap_id1 = 1; ap_id1 <= seats_sql::NUM_AIRPORTS; ap_id1++) {
        

        //FIXME: Only include if there is a "flight between the two".

        double dist = calculateDistance(apid_long_lat[ap_id0 - 1], apid_long_lat[ap_id1 - 1]);
        assert(dist > 0);
        
        //std::cerr << " airport dest: " << ap_id1 << ". distance: "<< dist << std::endl;
        dist_ap_0.push_back(dist);
        if (ap_id0 == ap_id1) continue;

        //Alternatively: Only store the distances of airports under 100 //TODO: in this case, can make pkey only (id0, id1)
        if(dist <= seats_sql::MAX_NEAR_DISTANCE){
          nearest.push({dist, ap_id1});
        }

        //Alternatively: Only take the k nearest...   // in this case, might want the pkey to be (id0, dist, id2) 
        // if(nearest.size() < FLAGS_k_nearest_airports){
        //   nearest.push({dist, ap_id1});
        // }
        // else if(dist < nearest.top().first){
        //     nearest.pop();
        //     nearest.push({dist, ap_id1});
        // }
        
      }

      dist_matrix.push_back(std::move(dist_ap_0));

      while(nearest.size()){  
         //Keep vector sorted.
        auto conn = nearest.top();
        nearest.pop();
        double dist = conn.first;
        int ap_id1 = conn.second;

        std::vector<std::string> values;
        values.push_back(std::to_string(ap_id0));
       // values.push_back(std::to_string(dist)); //Order depending on Primary Key.
        values.push_back(std::to_string(ap_id1));
        values.push_back(std::to_string(dist));
        
        //if(dist > 0 && dist <= 100) std::cerr << "small dist: " << dist << std::endl;
        //std::cerr << "  nearest: " << dist << std::endl;
       
        writer.add_row(table_name, values);
      }
    }
    return dist_matrix;
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
        
        double dist = calculateDistance(apid_long_lat[ap_id0 - 1], apid_long_lat[ap_id1 - 1]);
        dist_ap_0.push_back(dist);

        if (ap_id0 != ap_id1) continue;

        std::vector<std::string> values;
        values.push_back(std::to_string(ap_id0));
        values.push_back(std::to_string(ap_id1));
        
        //if(dist > 0 && dist <= 100) std::cerr << "small dist: " << dist << std::endl;
        values.push_back(std::to_string(dist));
        writer.add_row(table_name, values);

      
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

    uint64_t num_customers = FLAGS_scale_factor * seats_sql::NUM_CUSTOMERS;

    std::mt19937 gen;
    // generate data
    for (int c_id = 1; c_id <= num_customers; c_id++) {
      std::vector<std::string> values; 
      //values.reserve(44);
      values.push_back(std::to_string(c_id));       // c_id
      values.push_back(std::to_string(c_id));       // c_id_str
      values.push_back(std::to_string(std::uniform_int_distribution<int>(1, seats_sql::NUM_AIRPORTS)(gen)));  //TODO: Create a map in profile: <airport_id -> max_customers> 
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
    const std::vector<uint32_t> index2 {2};
    writer.add_index(table_name, "ff_customer_str_index", index2);

    std::mt19937 gen;

    int max_per_customer = std::min(seats_sql::CUSTOMER_NUM_FREQUENTFLYERS_MAX * FLAGS_scale_factor, seats_sql::NUM_AIRLINES);
    ZipfianGenerator zipf = ZipfianGenerator(seats_sql::CUSTOMER_NUM_FREQUENTFLYERS_MIN, max_per_customer, seats_sql::CUSTOMER_NUM_FREQUENTFLYERS_SIGMA);
    // std::vector<int> ff_per_customer;
    // for (int i = 0; i < seats_sql::NUM_CUSTOMERS; i++) {
    //   int val = std::min(zipf.nextValue(gen), max_per_customer);
    //   ff_per_customer.push_back(val);
    // }

    uint64_t num_customers = FLAGS_scale_factor * seats_sql::NUM_CUSTOMERS;

    // generate data
    for (int c_id = 1; c_id <= num_customers; c_id++) {
      uint32_t num_ff = std::min(zipf.nextValue(gen), max_per_customer);
      std::set<int64_t> al_per_customer;
      //for (int al_num = 0; al_num < ff_per_customer[c_id - 1]; al_num++) {  
      for (int al_num = 0; al_num < num_ff; al_num++) {  
        //benchbase uses a flat histogram == uniform distribution
        int64_t al_id = std::uniform_int_distribution<int64_t>(1, seats_sql::NUM_AIRLINES)(gen);
        while (!al_per_customer.insert(al_id).second) {
          al_id = std::uniform_int_distribution<int64_t>(1, seats_sql::NUM_AIRLINES)(gen);
        }
      

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

inline uint64_t distToTime(double dist) {
  double flight_time_h = dist/seats_sql::FLIGHT_TRAVEL_RATE;
  //std::cerr << "flight time_h: " << flight_time_h << std::endl;
  uint64_t flight_time_ms = flight_time_h * 60 * 60 * 1000;
  //std::cerr << "flight time_ms: " << flight_time_ms << std::endl;
  return flight_time_ms;
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


std::vector<int> GenerateFlightTable(TableWriter &writer, std::vector<std::vector<double>> &airport_distances, std::unordered_map<std::string, int64_t> ap_code_to_id, 
            std::vector<std::pair<int64_t, int64_t>> &f_id_to_ap_conn) {
    std::vector<std::pair<std::string, std::string>> column_names_and_types; 
    column_names_and_types.push_back(std::make_pair("f_id", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("f_al_id", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("f_depart_ap_id", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("f_depart_time", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("f_arrive_ap_id", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("f_arrive_time", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("f_status", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("f_base_price", "FLOAT"));
    column_names_and_types.push_back(std::make_pair("f_seats_total", "BIGINT"));
    column_names_and_types.push_back(std::make_pair("f_seats_left", "BIGINT"));
    FillColumnNamesWithGenericAttr(column_names_and_types, "f_iattr", "BIGINT", 30);
    const std::vector<uint32_t> primary_key_col_idx {0};
    std::string table_name = seats_sql::FLIGHT_TABLE;
    writer.add_table(table_name, column_names_and_types, primary_key_col_idx);
    

    const std::vector<uint32_t> index {2, 3};
    writer.add_index(table_name, "f_departures_idx", index);

    // const std::vector<uint32_t> index {2, 3};
    // writer.add_index(table_name, "f_depart_airport_idx", index);


    // load histograms
    std::ifstream fa_hist (seats_sql::FLIGHTS_AIRPORT_HISTO_FN);
    histogram flight_airp_hist = createFPAHistogram(fa_hist); //flight frequencies (airport pairs)
    fa_hist.close(); 

    std::ifstream faf_hist (seats_sql::FLIGHTS_AIRPORT_HISTO_FN);
    std::map<std::string, histogram> airport_flights = createFPAHistograms(faf_hist); //flight frequencies *between* airports 
    writeAirportFlights(airport_flights);
    faf_hist.close();

    std::ifstream ft_hist (seats_sql::FLIGHTS_TIME_HISTO_FN);
    histogram flight_time_hist = createFPTHistogram(ft_hist);
    ft_hist.close();
    // generate data
    std::vector<int> flight_to_num_reserved;
    std::mt19937 gen;
    
    GaussGenerator num_flight_generator(seats_sql::FLIGHTS_PER_DAY_MIN, seats_sql::FLIGHTS_PER_DAY_MAX);
    GaussGenerator airline_generator(1, seats_sql::NUM_AIRLINES);

    // //generate past days?
    // for(uint64_t t = seats_sql::MIN_TS - (seats_sql::FLIGHTS_DAYS_PAST * seats_sql::MS_IN_DAY); t < seats_sql::MIN_TS; t+=seats_sql::MS_IN_DAY){
    //   int num_flights = generator();
    // }

    //std::map<uint64_t, uint64_t> flights_per_airline;

    int next_flight_id = 1;

    std::queue<seats_sql::CachedFlight> cached_flights;

    uint64_t today = seats_sql::TODAY;
    uint64_t min_past_day = today - seats_sql::MS_IN_DAY * seats_sql::FLIGHTS_DAYS_PAST * FLAGS_scale_factor; 
    uint64_t max_future_day = today + seats_sql::MS_IN_DAY * seats_sql::FLIGHTS_DAYS_FUTURES * FLAGS_scale_factor; 

    //generate flights for past and future days.
    for(uint64_t dep_time = min_past_day; dep_time <= max_future_day; dep_time+= seats_sql::MS_IN_DAY){
    //for(uint64_t dep_time = today; dep_time <= max_future_day; dep_time+= seats_sql::MS_IN_DAY){ // //generate future days only
      int num_flights = num_flight_generator();
      for (int i = 0; i <= num_flights; ++i) {
          int f_id = next_flight_id++;
          int f_al_id = airline_generator();

          std::vector<std::string> values; 
          values.push_back(std::to_string(f_id)); 
          values.push_back(std::to_string(f_al_id)); 
          //flights_per_airline[f_al_id]++;

          std::string ap_conn = getRandValFromHistogram(flight_airp_hist, gen);
          auto arr_dep_ap = convertAPConnToAirports(ap_conn);
          auto dep_ap_id = ap_code_to_id[arr_dep_ap.first];
          auto arr_ap_id = ap_code_to_id[arr_dep_ap.second];
          f_id_to_ap_conn.push_back(std::make_pair(dep_ap_id, arr_ap_id));

          uint64_t travel_time = distToTime(airport_distances[dep_ap_id - 1][arr_ap_id - 1]); 
        
          //normalize dep time to 00:00 and then set departure time based on histogram
          dep_time = (dep_time - (dep_time % seats_sql::MS_IN_DAY)) + convertStrToTime(getRandValFromHistogram(flight_time_hist, gen));
          uint64_t arrival_time = dep_time + travel_time;
          assert(arrival_time > dep_time);

          // cached flights are the latest departing flights, capped at CACHE_LIMIT_FLIGHTS 
          // https://github.com/cmu-db/benchbase/blob/main/src/main/java/com/oltpbenchmark/benchmarks/seats/procedures/LoadConfig.java#L55
          seats_sql::CachedFlight cf;
          cf.flight_id = f_id; cf.airline_id = f_al_id; cf.depart_ap_id = dep_ap_id; cf.arrive_ap_id = arr_ap_id; cf.depart_time = dep_time;
          if (cached_flights.size() == seats_sql::CACHE_LIMIT_FLIGHT_IDS) cached_flights.pop();
          cached_flights.push(cf);

          values.push_back(std::to_string(dep_ap_id));
          values.push_back(std::to_string(dep_time));
          values.push_back(std::to_string(arr_ap_id));
          values.push_back(std::to_string(arrival_time));

          values.push_back(std::to_string(std::uniform_int_distribution<int>(0, 1)(gen)));
          values.push_back(std::to_string(std::uniform_real_distribution<float>(100, 1000)(gen)));
          values.push_back(std::to_string(seats_sql::TOTAL_SEATS_PER_FLIGHT));

          //number of reserved seats
          //auto seats_reserved = std::uniform_int_distribution<int>(seats_sql::MIN_SEATS_RESERVED, seats_sql::MAX_SEATS_RESERVED)(gen);
          auto seats_reserved = std::binomial_distribution<int>(seats_sql::TOTAL_SEATS_PER_FLIGHT, 0.01 * seats_sql::PROB_SEAT_OCCUPIED)(gen);
          values.push_back(std::to_string(seats_sql::TOTAL_SEATS_PER_FLIGHT - seats_reserved)); //seats remaining
          flight_to_num_reserved.push_back(seats_reserved); //Generate a reservation for each occupied seat.

          for (int iattr = 0; iattr < 30; iattr++) {
            values.push_back(std::to_string(std::uniform_int_distribution<int64_t>(1, 100000000)(gen)));
          }

          writer.add_row(table_name, values);

      }
    }

    writeCachedFlights(cached_flights);
    return flight_to_num_reserved;
}

//TODO: Technically Benchbase generates Reservations in a bit more subtle way by trying to account for "returning" and "home-base" customers.
        //However, this appears to be irrelevant in the actual workload, so I don't think it actually matters.
void GenReservations(){
  //TODO: ScalingDataIterable for Reservations? total scales with scale_factor  => I don't think it is relevant..
  /*
  for all flights:
    //depart airport
    //arrive airport
  
    //depart_time
    //arrive_time

    //returning_customers: get Returning Customers(flight id)

    //booked_seats = flight_to_num_reserved

    //for seat [0.. booked_seats] 
       //airport_customer_count
       //local_customer
       //tries
       //while(tries > 0) pick a customer

          //if returning customer exist; pick it.
          //else: use local customer (from local airport)
          //else use random customer

          //if customer already has reservation. skip

      //if return flight: do nothing
      //if new outbound: randomly decide if and when customer will return  (rand_returns = gaussian, return flight days_min_max)
      
      //add customer to list for this flight

      //create reservation
  */
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
    const std::vector<uint32_t> primary_key_col_idx {0, 1, 2};
    std::string table_name = seats_sql::RESERVATION_TABLE;
    writer.add_table(table_name, column_names_and_types, primary_key_col_idx);

    //Optional Index:
     const std::vector<uint32_t> index {2, 1}; 
    writer.add_index(table_name, "r_flight_index", index);

    //   const std::vector<uint32_t> index2 {2, 3};
    // writer.add_index(table_name, "r_seat_index", index2);  //TODO: Check that this is currently a scan? //FIXME: This gets loader stuck?
    
    // generate data
    std::mt19937 gen;   
    int r_id = 1;

    //uint64_t total = (seats_sql::FLIGHTS_PER_DAY_MIN + seats_sql::FLIGHTS_PER_DAY_MAX) / 2 * FLAGS_scale_factor;

     uint64_t num_customers = FLAGS_scale_factor * seats_sql::NUM_CUSTOMERS;

    std::vector<std::queue<int64_t>> outbound_customers_per_ap_id(seats_sql::NUM_AIRPORTS, std::queue<int64_t>());
    //for (int f_id = 1; f_id <= 20; f_id++) {
    for (int f_id = 1; f_id <= flight_to_num_reserved.size(); f_id++) {  //For each flight: Create reservation for each occupied seat.
      //std::cerr << "flight id: " << f_id << std::endl;
      //std::vector<int64_t> seat_ids;
      std::set<uint32_t> used_seat_ids = {0};
      std::set<uint32_t> flight_customer_ids;

      //if(f_id == 68273) std::cerr << "RESERVED SEATS ON FLIGHT: " << flight_to_num_reserved[f_id-1] << std::endl;
      for (int r = 1; r <= flight_to_num_reserved[f_id-1]; r++) {
        //std::cerr << "res: " << r << std::endl;
        std::vector<std::string> values; 
        values.push_back(std::to_string(r_id++));
        int64_t arr_ap_id = fl_to_ap_conn[f_id-1].second;
        int64_t c_id;

        //TODO: Pick Customer in a more principled way (with returning and local customers). 
        //For now, just pick a random one.
        c_id = std::uniform_int_distribution<int>(1, num_customers)(gen);
        while(!flight_customer_ids.insert(c_id).second){  //Don't allow the same customer to have two seats.
           c_id = std::uniform_int_distribution<int>(1, num_customers)(gen);
        }

        // if (outbound_customers_per_ap_id[arr_ap_id-1].empty()) {
        //   c_id = std::uniform_int_distribution<int>(1, seats_sql::NUM_CUSTOMERS)(gen);
        //   int64_t ret_ap_id = fl_to_ap_conn[f_id-1].first;
        //   outbound_customers_per_ap_id[ret_ap_id-1].push(c_id);
        // } else {
        //   c_id = outbound_customers_per_ap_id[arr_ap_id-1].front();
        //   outbound_customers_per_ap_id[arr_ap_id-1].pop();
        // }

        values.push_back(std::to_string(c_id));
        values.push_back(std::to_string(f_id));

        uint32_t seat = 0;
        while(!used_seat_ids.insert(seat).second){ //pick a random flight from 1 to 150
           seat = std::uniform_int_distribution<int64_t>(1, seats_sql::TOTAL_SEATS_PER_FLIGHT)(gen);
        }
        //assert(seat != 0);
        // int64_t seat = std::uniform_int_distribution<int64_t>(1, seats_sql::TOTAL_SEATS_PER_FLIGHT)(gen);
        // while (containsId(seat_ids, seat))
        //   seat = std::uniform_int_distribution<int64_t>(1, seats_sql::TOTAL_SEATS_PER_FLIGHT)(gen);
        // seat_ids.push_back(seat);

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

}

int main(int argc, char *argv[]) {
    gflags::SetUsageMessage("generates json file containing SQL tables for SEATS data\n");
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    auto start_time = std::time(0);
    std::string file_name = "sql-seats";
    TableWriter writer = TableWriter(file_name);
    std::cerr << "Generating SEATS Tables" << std::endl;
    // generate all tables
    auto co_code_to_id = seats_sql::GenerateCountryTable(writer);
    std::cerr << "Finished Country" << std::endl;
    seats_sql::GenerateAirlineTable(writer, co_code_to_id);
    std::cerr << "Finished Airline" << std::endl;
    std::unordered_map<std::string, int64_t> ap_code_to_id;
    auto airport_coords =seats_sql::GenerateAirportTable(writer, co_code_to_id, ap_code_to_id);
    std::cerr << "Finished Airport" << std::endl;
    auto airport_dists = seats_sql::GenerateAirportDistanceTableBounded(writer, airport_coords);
    //std::vector<std::vector<double>> airport_dists = std::vector<std::vector<double>>(seats_sql::NUM_AIRPORTS, std::vector<double>(seats_sql::NUM_AIRPORTS, 0.0));
    std::cerr << "Finished AirportDistance" << std::endl;
    std::vector<std::pair<int64_t, int64_t>> f_id_to_ap_conn;
    auto flight_reserves = seats_sql::GenerateFlightTable(writer, airport_dists, ap_code_to_id, f_id_to_ap_conn);
    std::cerr << "Finished Flights" << std::endl;
    seats_sql::GenerateReservationTable(writer, flight_reserves, f_id_to_ap_conn);
    std::cerr << "Finished Reservation" << std::endl;
    seats_sql::GenerateCustomerTable(writer);
    std::cerr << "Finished Customer" << std::endl;
    seats_sql::GenerateFrequentFlyerTable(writer);
    std::cerr << "Finished FrequentFlyer" << std::endl;
    writer.flush();
    auto end_time = std::time(0);
    std::cerr << "Finished SEATS Table Generation. Took " << (end_time - start_time) << "seconds" << std::endl;
    return 0;
}
