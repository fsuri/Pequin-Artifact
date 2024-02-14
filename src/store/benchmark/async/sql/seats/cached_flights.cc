#include "store/benchmark/async/sql/seats/cached_flights.h"

namespace seats_sql {

// https://github.com/cmu-db/benchbase/blob/main/src/main/java/com/oltpbenchmark/benchmarks/seats/SEATSProfile.java#L417
bool addFlightToCache(std::vector<CachedFlight> &cached_flights, CachedFlight cf, std::mt19937 &gen) {
  if (cached_flights.size() < seats_sql::CACHE_LIMIT_FLIGHT_IDS) {
    cached_flights.push_back(cf);
    return true;
  }
  if (std::uniform_int_distribution<int>(0, 1)(gen)) {
  //if ((bool) std::bind(std::uniform_int_distribution<int>(0, 1), std::default_random_engine())()) {
    // just removing the back element over and over? sounds like it just uses the same flights then mostly
    cached_flights.pop_back();
    cached_flights.push_back(cf);
    return true;
  }
  return false;
}

}