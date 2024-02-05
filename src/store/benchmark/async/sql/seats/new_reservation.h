#ifndef SEATS_SQL_NEW_RESERVATION_H
#define SEATS_SQL_NEW_RESERVATION_H 

#include "store/benchmark/async/sql/seats/seats_transaction.h"
#include "store/benchmark/async/sql/seats/reservation.h"
#include <queue>

namespace seats_sql {

class SQLNewReservation:public SEATSSQLTransaction {
    public: 
        SQLNewReservation(uint32_t timeout, std::mt19937 &gen, int64_t r_id, std::queue<SEATSReservation> &insert_res, std::queue<SEATSReservation> &update_res, std::queue<SEATSReservation> &delete_res);
        virtual ~SQLNewReservation();
        virtual transaction_status_t Execute(SyncClient &client);
    private:
        int64_t r_id;  // reservation id
        int64_t c_id; 
        int64_t f_id;
        int64_t seatnum; 
        double price;
        std::vector<int64_t> attributes;
        std::time_t time;
        std::queue<SEATSReservation> *update_q;
        std::queue<SEATSReservation> *delete_q;
        std::mt19937 *gen_;
};

}

#endif

