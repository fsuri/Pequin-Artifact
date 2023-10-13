#ifndef SEATS_SQL_UPDATE_RESERVATION_H
#define SEATS_SQL_UPDATE_RESERVATION_H

#include "store/benchmark/async/sql/seats/seats_transaction.h"

namespace seats_sql {

class SQLUpdateReservation:public SEATSSQLTransaction {
    public: 
        SQLUpdateReservation(uint32_t timeout, std::mt19937_64 gen);
        virtual ~SQLUpdateReservation();
        virtual transaction_status_t Execute(SyncClient &client);
    private:
        int64_t r_id;       // current reservation on flight
        int64_t c_id;       // customer id
        int64_t f_id;       // flight id that customer has a reservation on
        int64_t seatnum;    // seat that the customer has a reservation on
        int64_t attr_idx;   // idx to index into reserve_seats; determine what attribute to update
        int64_t attr_val;   // value that attribute is updated to 
        std::vector<std::string> reserve_seats = {"R_IATTR00", "R_IATTR01", "R_IATTR02", "R_IATTR03"};
};
}

#endif

