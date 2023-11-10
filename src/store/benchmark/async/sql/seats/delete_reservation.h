#ifndef SEATS_SQL_DELETE_RESERVATION_H
#define SEATS_SQL_DELETE_RESERVATION_H 

#include "store/benchmark/async/sql/seats/seats_transaction.h"
#include "store/benchmark/async/sql/seats/reservation.h"
#include <random>
#include <queue>

namespace seats_sql {

class SQLDeleteReservation: public SEATSSQLTransaction {
    public: 
        SQLDeleteReservation(uint32_t timeout, std::mt19937_64 gen, std::queue<SEATSReservation> &existing_res, std::queue<SEATSReservation> &insert_res);
        virtual ~SQLDeleteReservation();
        virtual transaction_status_t Execute(SyncClient &client);
    private:
        int64_t f_id;
        int64_t c_id;
        std::string c_id_str;
        std::string ff_c_id_str;
        std::queue<SEATSReservation> *q;            // insert reservation queue
};
}
#endif

