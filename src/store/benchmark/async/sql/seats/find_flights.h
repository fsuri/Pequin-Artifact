#ifndef SEATS_SQL_FIND_FLIGHTS_H
#define SEATS_SQL_FIND_FLIGHTS_H 

#include "store/benchmark/async/sql/seats/seats_transaction.h"
#include <random>

namespace seats_sql {

class SQLFindFlights: public SEATSSQLTransaction {
    public: 
        SQLFindFlights(uint32_t timeout, std::mt19937_64 gen);
        virtual ~SQLFindFlights();
        virtual transaction_status_t Execute(SyncClient &client);
    private:
        int64_t depart_aid;  // depart_ap_id, airport id of departure
        int64_t arrive_aid;   // arrive_ap_id, airport id of arrival
        int64_t start_time;  // unix timestamp of earliest departure
        int64_t end_time;    // unix timestamp of latest departure
        int64_t distance;    // max distance willing to travel

};
}
#endif

