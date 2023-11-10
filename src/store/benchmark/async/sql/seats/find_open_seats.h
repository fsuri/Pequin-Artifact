#ifndef SEATS_SQL_FIND_OPEN_SEATS_H
#define SEATS_SQL_FIND_OPEN_SEATS_H 

#include "store/benchmark/async/sql/seats/seats_transaction.h"
#include "store/benchmark/async/sql/seats/reservation.h"
#include <random>
#include <queue>

namespace seats_sql {

class SQLFindOpenSeats: public SEATSSQLTransaction {
    public: 
        SQLFindOpenSeats(uint32_t timeout, std::mt19937_64 gen, std::queue<SEATSReservation> &new_res_queue);
        virtual ~SQLFindOpenSeats();
        virtual transaction_status_t Execute(SyncClient &client);
    private:
        int64_t f_id;  // flight id
        std::queue<SEATSReservation> *q;
};
}

#endif

