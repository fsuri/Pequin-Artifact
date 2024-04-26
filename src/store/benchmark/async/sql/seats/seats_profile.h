#ifndef SEATS_SQL_SEATS_PROFILE_H
#define SEATS_SQL_SEATS_PROFILE_H 
#include "store/benchmark/async/sql/seats/seats_constants.h"
#include <cstdint>
#include <vector>
#include <random>
#include <functional>
#include <queue>


namespace seats_sql {

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


class SeatsProfile {
  public:
    SeatsProfile(uint64_t seats_id, uint32_t scale_factor): seats_id(seats_id), num_res_made(0), scale_factor(scale_factor){

        num_customers = seats_sql::NUM_CUSTOMERS * scale_factor;
        max_future_day = seats_sql::TODAY + seats_sql::MS_IN_DAY * seats_sql::FLIGHTS_DAYS_FUTURES * scale_factor; 
    }
   ~SeatsProfile(){}

    uint64_t seats_id;              // need this for generating res id
    int64_t num_res_made;       // number of reservations made by client

    uint32_t scale_factor; 

    uint32_t num_customers;
    uint64_t max_future_day;

    std::vector<CachedFlight> cached_flights;

    std::queue<SEATSReservation> insert_reservations; 
    std::queue<SEATSReservation> update_reservations;
    std::queue<SEATSReservation> delete_reservations;

    // // https://github.com/cmu-db/benchbase/blob/main/src/main/java/com/oltpbenchmark/benchmarks/seats/SEATSProfile.java#L417
    // // https://github.com/cmu-db/benchbase/blob/main/src/main/java/com/oltpbenchmark/benchmarks/seats/SEATSProfile.java#L417
    bool addFlightToCache(CachedFlight cf, std::mt19937 &gen);

  private:

};



}
#endif