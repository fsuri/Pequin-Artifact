#ifndef SEATS_SQL_SEATS_CACHED_FLIGHT_H
#define SEATS_SQL_SEATS_CACHED_FLIGHT_H 
#include "store/benchmark/async/sql/seats/cached_flight.h"
#include <cstdint>
#include <vector>
#include <random>

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

// https://github.com/cmu-db/benchbase/blob/main/src/main/java/com/oltpbenchmark/benchmarks/seats/SEATSProfile.java#L417
bool addFlightToCache(std::vector<CachedFlight> &cached_flights, CachedFlight cf) {
  if (cached_flights.size() < seats_sql::CACHE_LIMIT_FLIGHT_IDS) {
    cached_flights.push_back(cf);
    return true;
  }
  if ((bool) std::bind(std::uniform_int_distribution<int>(0, 1), std::default_random_engine())()) {
    // just removing the back element over and over? sounds like it just uses the same flights then mostly
    cached_flights.pop_back();
    cached_flights.push_back(cf);
    return true;
  }
  return false;
}
}
#endif