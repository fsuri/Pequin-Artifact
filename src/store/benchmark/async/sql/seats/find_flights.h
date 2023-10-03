#ifndef SEATS_SQL_FIND_FLIGHTS_H
#define SEATS_SQL_FIND_FLIGHTS_H 

#include "store/benchmark/async/sql/seats/seats_transaction.h"

namespace seats_sql {

class SQLFindFlights: public SEATSSQLTransaction {
    public: 
        SQLFindFlights(uint32_t timeout, int64_t depart_aid, int64_t arrive_aid, int64_t start_time, int64_t end_time, int64_t distance);
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

