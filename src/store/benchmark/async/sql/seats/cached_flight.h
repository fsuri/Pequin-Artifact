#ifndef SEATS_SQL_SEATS_CACHED_FLIGHT_H
#define SEATS_SQL_SEATS_CACHED_FLIGHT_H 

#include <cstdint>

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

}
#endif