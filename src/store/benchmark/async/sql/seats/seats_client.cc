#include "store/benchmark/async/sql/seats/seats_client.h"
#include "store/benchmark/async/sql/seats/delete_reservation.h"
#include "store/benchmark/async/sql/seats/find_flights.h"
#include "store/benchmark/async/sql/seats/find_open_seats.h"
#include "store/benchmark/async/sql/seats/new_reservation.h" 
#include "store/benchmark/async/sql/seats/update_customer.h"
#include "store/benchmark/async/sql/seats/update_reservation.h"
#include "store/benchmark/async/sql/seats/seats_constants.h"

#include "store/benchmark/async/sql/seats/seats_util.h"
#include <cmath>
#include <queue>



namespace seats_sql {


SEATSSQLClient::SEATSSQLClient(SyncClient &client, Transport &transport, const std::string &profile_file_path, uint32_t scale_factor,
      uint64_t id, uint64_t numRequests, uint64_t expDuration, uint64_t delay, uint64_t warmupSec,
      uint64_t cooldownSec, uint64_t tputInterval, uint32_t abortBackoff, bool retryAborted, 
      uint64_t maxBackoff, int64_t maxAttempts,
      uint64_t timeout, const std::string &latencyFilename) :       
      SyncTransactionBenchClient(client, transport, id, numRequests,
        expDuration, delay, warmupSec, cooldownSec, tputInterval, abortBackoff,
        retryAborted, maxBackoff, maxAttempts, timeout, latencyFilename), profile(id, scale_factor) { 
            gen.seed(id);
            started_workload = false;


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

     
      //TODO: Ideally every client should cache a random different 10k...
      std::cerr << "profile file path: " << profile_file_path << std::endl;
      std::ifstream file (profile_file_path);
      skipCSVHeader(file);
      for (int i = 0; i < CACHE_LIMIT_FLIGHT_IDS; i++) {
        std::vector<std::string> row = readCSVRow(file);
        if (row.size() < 5) break;

        CachedFlight cf;
        cf.flight_id = std::stol(row[0]); 
        cf.airline_id = std::stol(row[1]); 
        cf.depart_ap_id = std::stol(row[2]);
        cf.depart_time = std::stol(row[3]);
        cf.arrive_ap_id = std::stol(row[4]);

        profile.cached_flights.push_back(cf);
      }
      std::shuffle(profile.cached_flights.begin(), profile.cached_flights.end(), gen);

      UW_ASSERT(!profile.cached_flights.empty());
}

SEATSSQLClient::~SEATSSQLClient() {}

SyncTransaction* SEATSSQLClient::GetNextTransaction() {

  // need to populate reservations first
  std::cerr << "Select Next Transactions" << std::endl;
  if (!started_workload) {
    started_workload = true; 
    return new SQLFindOpenSeats(GetTimeout(), gen, profile);
  }

  // keep going until we get a valid operation
  while (true) {
    int64_t t_type = std::uniform_int_distribution<int64_t>(1, 100)(gen);
    std::cerr << "NEXT T_TYPE: " << t_type << std::endl;
    int freq = 0;
    if (t_type <= (freq = FREQUENCY_DELETE_RESERVATION)) {
      std::cerr << "Try Delete_Res. Is empty? " << (profile.delete_reservations.empty()) << std::endl; 
      if (profile.delete_reservations.empty()) 
        continue;
      last_op_ = "delete_reservation";
      return new SQLDeleteReservation(GetTimeout(), gen, profile);
    } 
    else if (t_type <= (freq += FREQUENCY_FIND_FLIGHTS)) {
      last_op_ = "find_flight";
      return new SQLFindFlights(GetTimeout(), gen, profile); 
    } 
    else if (t_type <= (freq += FREQUENCY_FIND_OPEN_SEATS)) {
      last_op_ = "find_open_seats";
      return new SQLFindOpenSeats(GetTimeout(), gen, profile);
    } 
    else if (t_type <= (freq += FREQUENCY_NEW_RESERVATION)) {
      if (profile.insert_reservations.empty())
        continue; 
      last_op_ = "new_reservation";
      int64_t r_id = ((int64_t) profile.seats_id | (profile.num_res_made++) << 32);
      return new SQLNewReservation(GetTimeout(), gen, r_id, profile);
    } 
    else if (t_type <= (freq += FREQUENCY_UPDATE_CUSTOMER)) {
      last_op_ = "update_customer";
      return new SQLUpdateCustomer(GetTimeout(), gen, profile);
    } 
    else {
      std::cerr << "Try Update_Res. Is empty? " << (profile.update_reservations.empty()) << std::endl; 
      if (profile.update_reservations.empty())
        continue;
      last_op_ = "update_reservation";
      return new SQLUpdateReservation(GetTimeout(), gen, profile);
    }
  }
}

std::string SEATSSQLClient::GetLastOp() const {
  return last_op_;
}

} 
