#ifndef SEATS_SQL_NEW_RESERVATION_H
#define SEATS_SQL_NEW_RESERVATION_H 

#include "store/benchmark/async/sql/seats/seats_transaction.h"

#include "store/benchmark/async/sql/seats/seats_profile.h"

#include <queue>

namespace seats_sql {

class SQLNewReservation:public SEATSSQLTransaction {
    public: 
        SQLNewReservation(uint32_t timeout, std::mt19937 &gen, int64_t r_id, SeatsProfile &profile);
        virtual ~SQLNewReservation();
        virtual transaction_status_t Execute(SyncClient &client);
    private:
        int64_t r_id;  // reservation id
        int64_t c_id; 
        int64_t f_id;
        CachedFlight flight;
        int64_t seatnum; 
        double price;
        std::vector<int64_t> attributes;
        std::time_t time;
       
        std::mt19937 *gen_;
        SeatsProfile &profile;

        bool has_reservation;
};

}

#endif

