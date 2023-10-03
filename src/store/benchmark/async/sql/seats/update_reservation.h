#ifndef SEATS_SQL_UPDATE_RESERVATION_H
#define SEATS_SQL_UPDATE_RESERVATION_H

#include "store/benchmark/async/sql/seats/seats_transaction.h"

namespace seats_sql {

class SQLUpdateReservation:public SEATSSQLTransaction {
    public: 
        SQLUpdateReservation(uint32_t timeout, int64_t r_id, int64_t c_id, int64_t f_id, int64_t seatnum, int64_t attr_idx, int64_t attr_val);
        virtual ~SQLUpdateReservation();
        virtual transaction_status_t Execute(SyncClient &client);
    private:
        int64_t r_id;       // current reservation on flight
        int64_t c_id;       // customer id
        int64_t f_id;       // flight id that customer has a reservation on
        int64_t seatnum;    // seat that the customer has a reservation on
        int64_t attr_idx;
        int64_t attr_val;
        std::vector<std::string> reserve_seats = {"R_IATTR00", "R_IATTR01", "R_IATTR02", "R_IATTR03"};
};
}

#endif

