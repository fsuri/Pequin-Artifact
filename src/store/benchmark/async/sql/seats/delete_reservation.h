#ifndef SEATS_SQL_DELETE_RESERVATION_H
#define SEATS_SQL_DELETE_RESERVATION_H 

#include "store/benchmark/async/sql/seats/seats_transaction.h"

namespace seats_sql {

class SQLDeleteReservation: public SEATSSQLTransaction {
    public: 
        SQLDeleteReservation(uint32_t timeout, int64_t f_id, int64_t c_id, std::string c_id_str, std::string ff_c_id_str);
        virtual ~SQLDeleteReservation();
        virtual transaction_status_t Execute(SyncClient &client);
    private:
        int64_t f_id;
        int64_t c_id;
        std::string c_id_str;
        std::string ff_c_id_str;
};
}
#endif

