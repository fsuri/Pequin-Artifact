#ifndef SEATS_SQL_SEATS_CACHED_FLIGHT_H
#define SEATS_SQL_SEATS_CACHED_FLIGHT_H 
#include "store/benchmark/async/sql/seats/seats_constants.h"
#include <cstdint>
#include <vector>
#include <random>
#include <functional>

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
bool addFlightToCache(std::vector<CachedFlight> &cached_flights, CachedFlight cf, std::mt19937 &gen);

}
#endif