#ifndef SEATS_SQL_FIND_FLIGHTS_H
#define SEATS_SQL_FIND_FLIGHTS_H 

#include "store/benchmark/async/sql/seats/seats_transaction.h"
#include <random>

namespace seats_sql {

class SQLFindFlights: public SEATSSQLTransaction {
    public: 
        SQLFindFlights(uint32_t timeout, std::mt19937 &gen);
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
    GetFlightsResultRow() : f_id(0), f_al_id(0), f_depart_ap_id(0), f_depart_time(0), f_arrive_ap_id(0), f_arrive_time(0), ap_name(""), al_iattr00(0), al_iattr01(0) {}
    virtual ~GetFlightsResultRow() {} 
    virtual void SetFields(int64_t f_id, int64_t f_al_id, int64_t f_depart_ap_id, int64_t f_depart_time, int64_t f_arrive_ap_id, int64_t f_arrive_time, std::string ap_name, int64_t al_iattr00, int64_t al_iattr01) {
        f_id = f_id; 
        f_al_id = f_al_id; 
        f_depart_ap_id = f_depart_ap_id;
        f_depart_time = f_depart_time; 
        f_arrive_ap_id = f_arrive_ap_id;
        f_arrive_time = f_arrive_time; 
        ap_name = ap_name;
        al_iattr00 = al_iattr00; 
        al_iattr01 = al_iattr01;
    }
    int64_t f_id; 
    int64_t f_al_id;
    int64_t f_depart_ap_id;
    int64_t f_depart_time;
    int64_t f_arrive_ap_id;
    int64_t f_arrive_time;
    std::string ap_name; 
    int64_t al_iattr00;
    int64_t al_iattr01;
};

void inline load_row(GetFlightsResultRow &store, std::unique_ptr<query_result::Row> row) {
    int64_t f_id; 
    int64_t f_al_id;
    int64_t f_depart_ap_id;
    int64_t f_depart_time;
    int64_t f_arrive_ap_id;
    int64_t f_arrive_time;
    std::string ap_name; 
    int64_t al_iattr00;
    int64_t al_iattr01;
    row->get(0, &f_id);
    row->get(1, &f_al_id);
    row->get(2, &f_depart_ap_id);
    row->get(3, &f_depart_time);
    row->get(4, &f_arrive_ap_id);
    row->get(5, &f_arrive_time); 
    row->get(6, &ap_name);
    row->get(7, &al_iattr00);
    row->get(8, &al_iattr01);
    store.SetFields(f_id, f_al_id, f_depart_ap_id, f_depart_time, f_arrive_ap_id, f_arrive_time, ap_name, al_iattr00, al_iattr01);
}

struct GetAirportInfoResultRow {
public:
    GetAirportInfoResultRow() : ap_code(""), ap_name(""), ap_city(""), ap_longitude(0.0), ap_latitude(0.0), co_id(0), co_name(""), co_code_2(""), co_code_3("") {}
    virtual ~GetAirportInfoResultRow() {}
    virtual void SetFields(std::string ap_code, std::string ap_name, std::string ap_city, float ap_longitude, float ap_latitude, int64_t co_id, std::string co_name, std::string co_code_2, std::string co_code_3) {
        ap_code = ap_code; 
        ap_name = ap_name; 
        ap_city = ap_city; 
        ap_longitude = ap_longitude; 
        ap_latitude = ap_latitude; 
        co_id = co_id; 
        co_name = co_name; 
        co_code_2 = co_code_2; 
        co_code_3 = co_code_3;
    }
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
    std::string ap_code;
    std::string ap_name; 
    std::string ap_city;
    double ap_longitude;
    double ap_latitude;
    int64_t co_id;
    std::string co_name;
    std::string co_code_2;
    std::string co_code_3;
    row->get(0, &ap_code);
    row->get(1, &ap_name);
    row->get(2, &ap_city);
    row->get(3, &ap_longitude);
    row->get(4, &ap_latitude);
    row->get(5, &co_id); 
    row->get(6, &co_name);
    row->get(7, &co_code_2);
    row->get(8, &co_code_3);
    store.SetFields(ap_code, ap_name, ap_city, ap_longitude, ap_latitude, co_id, co_name, co_code_2, co_code_3);
}


}
#endif

