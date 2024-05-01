
#include "store/benchmark/async/sql/seats/seats_profile.h"
#include <iostream>
#include "lib/assert.h"


namespace seats_sql {

 //TODO: parse into profile
void writeAirportIDs(const std::unordered_map<std::string, int64_t> &ap_code_to_id) {
    std::cerr <<"Writing AirportIDs" << std::endl;
    std::ofstream profile;
    profile.open(seats_sql::PROFILE_LOCATION + seats_sql::AIRPORT_CODE_ID_FILE_NAME);
    
    
    for(auto &[code, id]: ap_code_to_id){
      profile << code << "," << id << "\n";
    }
    profile.close();
}

////TODO: Parse and Write this in generator. => Avoid duplicate reading...
 //TODO: parse into profile
void writeAirportFlights(std::map<std::string, histogram> &airport_flights) {
    std::cerr <<"Writing AirportFlights" << std::endl;
    std::ofstream profile;
    profile.open(seats_sql::PROFILE_LOCATION + seats_sql::AIRPORT_FLIGHTS_FILE_NAME);
    for(auto &[dep_ap, arrive_freq]: airport_flights){
      for(auto &[arr_ap, freq]: arrive_freq){
          profile << dep_ap << "," << arr_ap << "," << freq << "\n";
      }
    }
    profile.close();
}

void writeCachedFlights(std::queue<CachedFlight> &cf_q) {
    std::cerr <<"Writing CachedFlights" << std::endl;
    std::ofstream profile;
    profile.open(seats_sql::PROFILE_LOCATION + seats_sql::CACHED_FLIGHTS_FILE_NAME);
    profile << "Flight Id, Airline Id, Arrive AP Id, Depart AP Id, Depart Time\n";
    while (!cf_q.empty()) {
      CachedFlight cf = cf_q.front();
      profile << cf.flight_id << "," << cf.airline_id << "," << cf.depart_ap_id << "," << cf.depart_time << "," << cf.arrive_ap_id << "\n";
      cf_q.pop();
    }
    profile.close();
}

void SeatsProfile::LoadProfile(const std::string &profile_file_path){
      std::cerr << "profile file path: " << profile_file_path << std::endl;

      //1) Load AirportCodeIds
      std::ifstream file1 (profile_file_path + seats_sql::AIRPORT_CODE_ID_FILE_NAME);
      for(int i = 0; i < seats_sql::NUM_AIRPORTS; ++i){
        std::vector<std::string> row = readCSVRow(file1);
        if (row.size() < 2) break;
        // std::cerr << "col 0: " << row[0] << std::endl;
        // std::cerr << "col 1: " << row[1] << std::endl;
        ap_code_to_id[row[0]] = std::stol(row[1]);
        ap_id_to_code[std::stol(row[1])] = row[0];
      }
      file1.close();

      //2) Load AirportFlight histogram
       std::ifstream file2 (profile_file_path + seats_sql::AIRPORT_FLIGHTS_FILE_NAME);
      std::set<std::string> airports_with_outbound_flights;
      while(true){ //read all rows.
        std::vector<std::string> row = readCSVRow(file2);
        if (row.size() < 3) break;      
        airports_with_outbound_flights.insert(row[0]);
        auto &hist = airport_flights[row[0]];
        hist.push_back(std::make_pair(row[1], std::stol(row[2])));
      }
      file2.close();
      for(auto &valid_airport: airports_with_outbound_flights){
        valid_airports.push_back(ap_code_to_id[valid_airport]);
      }

      //3) Load cached flights
     
      //TODO: Maybe every client should cache a different 10k flights?
  
      std::ifstream file3 (profile_file_path + seats_sql::CACHED_FLIGHTS_FILE_NAME);
      skipCSVHeader(file3);
      for (int i = 0; i < seats_sql::CACHE_LIMIT_FLIGHT_IDS; i++) {
        std::vector<std::string> row = readCSVRow(file3);
        if (row.size() < 5) break;

        CachedFlight cf;
        cf.flight_id = std::stol(row[0]); 
        cf.airline_id = std::stol(row[1]); 
        cf.depart_ap_id = std::stol(row[2]);
        cf.depart_time = std::stol(row[3]);
        cf.arrive_ap_id = std::stol(row[4]);

        cached_flights.push_back(cf);
      }
      file3.close();

      std::shuffle(cached_flights.begin(), cached_flights.end(), gen);
      UW_ASSERT(!cached_flights.empty());

        //PLACEHOLDER CODE:
      //TODO: Make this cleaner. Pass in actual path. Apply Flight cache more broadly
      
      /* std::string filename = "store/benchmark/async/sql/seats/sql-seats-data/flight.csv";
      std::ifstream file (filename);

      std::string row_line;
      getline(file, row_line); //skip header
      while(getline(file, row_line)){
        std::string value;
        std::stringstream row(row_line);
        std::vector<std::string> row_values;

        while (getline(row, value, ',')) {
          row_values.push_back(std::move(value));
          if(row_values.size() == 5) break;
        }

        CachedFlight flight;
        flight.flight_id = std::stol(row_values[0]);
        flight.airline_id = std::stol(row_values[1]);
        flight.depart_ap_id = std::stol(row_values[2]);
        flight.depart_time = std::stol(row_values[3]);
        flight.arrive_ap_id = std::stol(row_values[4]);

        profile.cached_flights.push_back(std::move(flight));

        if(profile.cached_flights.size() == seats_sql::CACHE_LIMIT_FLIGHT_IDS) break;  
        //TODO: Instead of reading the first 10k at every client: Each client should cache a random different 10k
      }*/

    //Panic("test Load Profile");
}

// // https://github.com/cmu-db/benchbase/blob/main/src/main/java/com/oltpbenchmark/benchmarks/seats/SEATSProfile.java#L417
// // https://github.com/cmu-db/benchbase/blob/main/src/main/java/com/oltpbenchmark/benchmarks/seats/SEATSProfile.java#L417
bool SeatsProfile::addFlightToCache(const CachedFlight &cf) {
  if (cached_flights.size() < seats_sql::CACHE_LIMIT_FLIGHT_IDS) {
    cached_flights.push_back(cf);
    return true;
  }
  if (std::uniform_int_distribution<int>(0, 1)(gen)) {
    cached_flights.pop_front();
    cached_flights.push_back(cf);
    return true;
  }
  return false;
}

 std::array<int, seats_sql::TOTAL_SEATS_PER_FLIGHT>& SeatsProfile::getSeatsBitSet(uint64_t flight_id){
  return cache_booked_seats[flight_id];
}


}
