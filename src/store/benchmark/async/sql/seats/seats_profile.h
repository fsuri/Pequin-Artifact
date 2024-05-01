#ifndef SEATS_SQL_SEATS_PROFILE_H
#define SEATS_SQL_SEATS_PROFILE_H 
#include "store/benchmark/async/sql/seats/seats_constants.h"
#include "store/benchmark/async/sql/seats/seats_util.h"
#include <cstdint>
#include <vector>
#include <random>
#include <functional>
#include <queue>
#include <deque>
#include <set>
#include <map>
#include <iostream>
#include <cassert>


namespace seats_sql {

static const std::string PROFILE_LOCATION = "sql-seats-data/";

static const std::string CACHED_FLIGHTS_FILE_NAME = "cached_flights.csv";
static const std::string AIRPORT_FLIGHTS_FILE_NAME = "airport_flights.csv";
static const std::string AIRPORT_CODE_ID_FILE_NAME = "airport_ids.csv";

struct CachedFlight{
  CachedFlight(){}
  ~CachedFlight(){}
  uint64_t flight_id;
  uint64_t airline_id;
  uint64_t depart_ap_id;
  uint64_t arrive_ap_id;
  uint64_t depart_time;
};

struct SEATSReservation {
public: 
    SEATSReservation(int64_t r_id, int64_t c_id, CachedFlight flight, int64_t seat_num) 
    : r_id(r_id), c_id(c_id), flight(flight), seat_num(seat_num) {}
    ~SEATSReservation(){};

    int64_t r_id;   //reservation id
    int64_t c_id;   //customer id  //FIXME: CustomerID should be (id, depart_airport_id)
    CachedFlight flight;   //flight id    //FIXME: FlightID should be (airline_id, depart_airport_id, arrive_airport_id...)
    int64_t seat_num;
};

void writeAirportIDs(const std::unordered_map<std::string, int64_t> &ap_code_to_id);
void writeAirportFlights(std::map<std::string, histogram> &airport_flights);
void writeCachedFlights(std::queue<CachedFlight> &cf_q);

class SeatsProfile {
  public:
    SeatsProfile(uint64_t seats_id, uint32_t scale_factor, std::mt19937 &gen): seats_id(seats_id), num_res_made(0), scale_factor(scale_factor), gen(gen){

        num_customers = seats_sql::NUM_CUSTOMERS * scale_factor;
        max_future_day = seats_sql::TODAY + seats_sql::MS_IN_DAY * seats_sql::FLIGHTS_DAYS_FUTURES * scale_factor; 
    }
   ~SeatsProfile(){}

    void LoadProfile(const std::string &profile_file_path);

    std::mt19937 &gen;

    uint64_t seats_id;              // need this for generating res id
    int64_t num_res_made;       // number of reservations made by client

    uint32_t scale_factor; 

    uint32_t num_customers;
    uint64_t max_future_day;

    std::unordered_map<std::string, int64_t> ap_code_to_id;
    std::unordered_map<int64_t, std::string> ap_id_to_code;
    std::vector<int64_t> valid_airports; //list of airports with valid outbound flights;
    std::map<std::string, histogram> airport_flights;
    std::deque<CachedFlight> cached_flights;

    std::queue<SEATSReservation> insert_reservations; 
    std::queue<SEATSReservation> update_reservations;
    std::queue<SEATSReservation> delete_reservations;

    //Note: Benchbase never uses this properly. I tried to implement what seemed logical.
    std::map<uint64_t, std::set<uint64_t>> cache_customer_booked_flights; //map from flight to set of customers booked on the flight.

    std::map<uint64_t, std::array<int, seats_sql::TOTAL_SEATS_PER_FLIGHT>> cache_booked_seats;
    std::array<int, seats_sql::TOTAL_SEATS_PER_FLIGHT>& getSeatsBitSet(uint64_t flight_id);

    inline bool isFlightFull(const std::array<int, seats_sql::TOTAL_SEATS_PER_FLIGHT> &seats){
      return std::accumulate(seats.begin(), seats.end(), 0) == seats_sql::TOTAL_SEATS_PER_FLIGHT;

    }
    

    // // https://github.com/cmu-db/benchbase/blob/main/src/main/java/com/oltpbenchmark/benchmarks/seats/SEATSProfile.java#L417
    // // https://github.com/cmu-db/benchbase/blob/main/src/main/java/com/oltpbenchmark/benchmarks/seats/SEATSProfile.java#L417
    bool addFlightToCache(const CachedFlight &cf);

    inline int64_t getRandomAirportId(){ //only pick an airport that has outbound flights...
      auto rand_idx = std::uniform_int_distribution<int64_t>(1, valid_airports.size())(gen) - 1;
      return valid_airports[rand_idx];
  
      //TODO: Realistically, it makes more sense to pick a flight combination proportional to the flight heuristic used in generator.
    }


     inline int64_t getRandomOtherAirport(int64_t dep_aid){
      const std::string &dep_aid_code = ap_id_to_code[dep_aid];
      const histogram &flights = airport_flights[dep_aid_code];

      if(flights.empty()){ //This should never happen
        std::cerr << "airport: " << dep_aid_code << " has no outbound flights" << std::endl;
        assert(false);
      }

      auto arr_aid_code = getRandValFromHistogram(flights, gen);
      return ap_code_to_id[arr_aid_code];
    }

    //Completely random alternative:
    // inline int64_t getRandomAirportId(){
    //   return std::uniform_int_distribution<int64_t>(1, seats_sql::NUM_AIRPORTS)(gen); 
    // }

    // inline int64_t getRandomOtherAirport(int64_t dep_aid){
    //   //TODO: Original benchbase code uses a histogram for each airport, with a probability of flight to all other airports. `airport_histograms`
    //   int64_t other = getRandomAirportId();
    //   while(other == dep_aid){
    //     other = getRandomAirportId();
    //   }
    //   return other;
    // }

    inline int64_t getRandomUpcomingDate(){
       auto start_time = std::uniform_int_distribution<std::time_t>(seats_sql::TODAY, max_future_day)(gen);
       start_time = start_time - (start_time % seats_sql::MS_IN_DAY); //normalize to start of day
       return start_time;
    }

    inline int64_t getRandomCustomerId(){
         return std::uniform_int_distribution<int64_t>(1, num_customers)(gen);  
    }

    inline int64_t getRandomCustomerId(uint64_t airport_depart_id){
      //TODO: Original benchbase code uses a map for each airport, containing the number of customers for this airport. `airport_max_customer_id`
          //CustomerID is a struct consisting of (base_id, airport_id), where base id within [1, num_customers_for_this_airport]
      return std::uniform_int_distribution<int64_t>(1, num_customers)(gen);  
       
    }

    inline bool isCustomerBookedOnFlight(uint64_t customer_id, uint64_t flight_id){
      auto &customers = cache_customer_booked_flights[flight_id];
      return customers.count(customer_id);
    }
    inline void resetFlightCache(uint64_t flight_id){ 
      cache_booked_seats[flight_id].fill(0);
     
      cache_customer_booked_flights[flight_id].clear();
    }
    inline void cacheCustomerBooking(uint64_t customer_id, uint64_t flight_id){
      cache_customer_booked_flights[flight_id].insert(customer_id); 
    }
    inline void deleteCustomerBooking(uint64_t customer_id, uint64_t flight_id){
      cache_customer_booked_flights[flight_id].erase(customer_id);  
    }
    




    
  private:

};

//TODO: Add histograms:

//TODO: Read in table_airport_seats => generate "airport name -> id mapping"
//TODO: Create flight: histogram from id -> his(id, freq)

 // load histograms
    // std::ifstream fa_hist (FLIGHTS_AIRPORT_HISTO_FN);
    // histogram flight_airp_hist = createFPAHistogram(fa_hist);
    // fa_hist.close(); 
    // std::ifstream ft_hist (FLIGHTS_TIME_HISTO_FN);
    // histogram flight_time_hist = createFPTHistogram(ft_hist);


}
#endif