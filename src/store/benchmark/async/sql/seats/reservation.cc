#include "store/benchmark/async/sql/seats/reservation.h"
#include "store/benchmark/async/sql/seats/seats_constants.h"

namespace seats_sql {

SEATSReservation::SEATSReservation(int64_t r_id, int64_t c_id, CachedFlight flight, int64_t seat_num) 
    : r_id(r_id), c_id(c_id), flight(flight), seat_num(seat_num) {}

SEATSReservation::~SEATSReservation() {}

}