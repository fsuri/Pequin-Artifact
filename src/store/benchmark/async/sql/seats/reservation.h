#ifndef SEATS_SQL_SEATS_RESERVATION_H
#define SEATS_SQL_SEATS_RESERVATION_H 

#include <cstdint>

namespace seats_sql {
struct SEATSReservation {
public: 
    SEATSReservation(int64_t r_id, int64_t c_id, int64_t f_id, int64_t seat_num);
    ~SEATSReservation();

    int64_t r_id; 
    int64_t c_id;
    int64_t f_id;
    int64_t seat_num;
};
}
#endif