#ifndef SEATS_SQL_FIND_FLIGHTS_H
#define SEATS_SQL_FIND_FLIGHTS_H 

#include "store/benchmark/async/sql/seats/seats_transaction.h"
#include <random>
#include "cached_flight.h"

namespace seats_sql {

class SQLFindFlights: public SEATSSQLTransaction {
    public: 
        SQLFindFlights(uint32_t timeout, std::mt19937 &gen, std::vector<CachedFlight> &cached_flight_ids); 
        virtual ~SQLFindFlights();
        virtual transaction_status_t Execute(SyncClient &client);
    private:
        int64_t depart_aid;  // depart_ap_id, airport id of departure
        int64_t arrive_aid;   // arrive_ap_id, airport id of arrival
        int64_t start_time;  // unix timestamp of earliest departure
        int64_t end_time;    // unix timestamp of latest departure
        int64_t distance;    // max distance willing to travel

};

struct GetNearbyAirportsResultRow {
    GetNearbyAirportsResultRow() : dp_ap_id0(0), dp_ap_id1(0), d_distance(0) {} 
    ~GetNearbyAirportsResultRow() {} 
    int64_t dp_ap_id0;
    int64_t dp_ap_id1; 
    double d_distance;
};

void inline load_row(GetNearbyAirportsResultRow &store, std::unique_ptr<query_result::Row> row) {
    row->get(0, &store.dp_ap_id0);
    row->get(1, &store.dp_ap_id1);
    row->get(2, &store.d_distance);
}

struct GetFlightsResultRow {
public: 
    GetFlightsResultRow() : f_id(0), f_al_id(0), f_depart_ap_id(0), f_depart_time(0), f_arrive_ap_id(0), f_arrive_time(0), al_name(""), al_iattr00(0), al_iattr01(0) {}
    virtual ~GetFlightsResultRow() {} 
    int64_t f_id; 
    int64_t f_al_id;
    int64_t f_depart_ap_id;
    int64_t f_depart_time;
    int64_t f_arrive_ap_id;
    int64_t f_arrive_time;
    std::string al_name; 
    int64_t al_iattr00;
    int64_t al_iattr01;
};

void inline load_row(GetFlightsResultRow &store, std::unique_ptr<query_result::Row> row) {
    row->get(0, &store.f_id);
    row->get(1, &store.f_al_id);
    row->get(2, &store.f_depart_ap_id);
    row->get(3, &store.f_depart_time);
    row->get(4, &store.f_arrive_ap_id);
    row->get(5, &store.f_arrive_time); 
    row->get(6, &store.al_name);
    row->get(7, &store.al_iattr00);
    row->get(8, &store.al_iattr01);
}

struct GetAirportInfoResultRow {
public:
    GetAirportInfoResultRow() : ap_code(""), ap_name(""), ap_city(""), ap_longitude(0.0), ap_latitude(0.0), co_id(0), co_name(""), co_code_2(""), co_code_3("") {}
    virtual ~GetAirportInfoResultRow() {}
    std::string ap_code;
    std::string ap_name; 
    std::string ap_city;
    double ap_longitude;
    double ap_latitude;
    int64_t co_id;
    std::string co_name;
    std::string co_code_2;
    std::string co_code_3;
};

void inline load_row(GetAirportInfoResultRow &store, std::unique_ptr<query_result::Row> row) {
    row->get(0, &store.ap_code);
    row->get(1, &store.ap_name);
    row->get(2, &store.ap_city);
    row->get(3, &store.ap_longitude);
    row->get(4, &store.ap_latitude);
    row->get(5, &store.co_id); 
    row->get(6, &store.co_name);
    row->get(7, &store.co_code_2);
    row->get(8, &store.co_code_3);
   
}


}
#endif

