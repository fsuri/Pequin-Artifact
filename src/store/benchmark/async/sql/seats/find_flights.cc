#include "store/benchmark/async/sql/seats/find_flights.h"
#include "store/benchmark/async/sql/seats/seats_constants.h"

#include <fmt/core.h>
#include <random>

namespace seats_sql {

const int MAX_NUM_FLIGHTS = 10;

SQLFindFlights::SQLFindFlights(uint32_t timeout, std::mt19937_64 gen) :
    SEATSSQLTransaction(timeout), distance(distance)
    {
        depart_aid = std::uniform_int_distribution<int64_t>(1, NUM_AIRPORTS)(gen);
        arrive_aid = std::uniform_int_distribution<int64_t>(1, NUM_AIRPORTS)(gen);
        start_time = std::uniform_int_distribution<std::time_t>(MIN_TS, MAX_TS)(gen);
        end_time = std::uniform_int_distribution<std::time_t>(start_time, MAX_TS)(gen);
    }

SQLFindFlights::~SQLFindFlights() {}

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

transaction_status_t SQLFindFlights::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query;

    Debug("FIND_FLIGHTS");
    client.Begin(timeout);
    std::vector<std::string> nearby_airports;

    if (distance > 0) {
        Debug("FIND_NEARBY_AIRPORT");
        query = fmt::format("SELECT * FROM {} WHERE D_AP_ID0 = {} AND D_DISTANCE <= {} ORDER BY D_DISTANCE ASC", AIRPORT_DISTANCE_TABLE, depart_aid, distance); 
        client.Query(query, queryResult, timeout);

        GetNearbyAirportsResultRow ad_row;
        std::vector<int64_t> nearby_airports;
        for (int i = 0; i < std::min((int) queryResult->size(), 2); i++) {
            deserialize(ad_row, queryResult, i);
            nearby_airports.push_back(ad_row.dp_ap_id1);
        }
    } 

    if (nearby_airports.size() == 0) query = fmt::format("SELECT F_ID, F_AL_ID, F_DEPART_AP_ID, F_DEPART_TIME, F_ARRIVE_AP_ID, F_ARRIVE_TIME, AL_NAME, AL_IATTR00, AL_IATTR01 FROM {}, {} WHERE F_DEPART_AP_ID = {} AND F_DEPART_TIME >= {} AND F_DEPART_TIME <= {} AND F_AL_ID = AL_ID AND F_ARRIVE_AP_ID = {} LIMIT {}", 
                                                            FLIGHT_TABLE, AIRPORT_TABLE, depart_aid, start_time, end_time, arrive_aid, MAX_NUM_FLIGHTS);
    else if (nearby_airports.size() == 1) query = fmt::format("SELECT F_ID, F_AL_ID, F_DEPART_AP_ID, F_DEPART_TIME, F_ARRIVE_AP_ID, F_ARRIVE_TIME, AL_NAME, AL_IATTR00, AL_IATTR01 FROM {}, {} WHERE F_DEPART_AP_ID = {} AND F_DEPART_TIME >= {} AND F_DEPART_TIME <= {} AND F_AL_ID = AL_ID AND F_ARRIVE_AP_ID = {} OR F_ARRIVE_AP_ID = {} LIMIT {}", 
                                                            FLIGHT_TABLE, AIRPORT_TABLE, depart_aid, start_time, end_time, arrive_aid, nearby_airports[0], MAX_NUM_FLIGHTS);
    else query = fmt::format("SELECT F_ID, F_AL_ID, F_DEPART_AP_ID, F_DEPART_TIME, F_ARRIVE_AP_ID, F_ARRIVE_TIME, AL_NAME, AL_IATTR00, AL_IATTR01 FROM {}, {} WHERE F_DEPART_AP_ID = {} AND F_DEPART_TIME >= {} AND F_DEPART_TIME <= {} AND F_AL_ID = AL_ID AND F_ARRIVE_AP_ID = {} OR F_ARRIVE_AP_ID = {} OR F_ARRIVE_AP_ID = {} LIMIT {}", 
                            FLIGHT_TABLE, AIRPORT_TABLE, depart_aid, start_time, end_time, arrive_aid, nearby_airports[0], nearby_airports[1], MAX_NUM_FLIGHTS);

    client.Query(query, queryResult, timeout);

    GetFlightsResultRow flight_row = GetFlightsResultRow();
    std::unique_ptr<const query_result::QueryResult> queryResultAirportInfo;
    std::string getAirportInfoQuery = "SELECT AP_CODE, AP_NAME, AP_CITY, AP_LONGITUDE, AP_LATITUDE, CO_ID, CO_NAME, CO_CODE_2, CO_CODE_3 FROM {}, {} WHERE AP_ID = {} AND AP_CO_ID = CO_ID";
    std::vector<GetAirportInfoResultRow> airport_infos;
    // populate the infos of arriving / departing airports of flight
    for (int i = 0; i < queryResult->size(); i++) {
        deserialize(flight_row, queryResult, i);
        query = fmt::format(getAirportInfoQuery, AIRPORT_TABLE, COUNTRY_TABLE, flight_row.f_depart_ap_id);
        client.Query(query, queryResultAirportInfo, timeout);
        GetAirportInfoResultRow ai_row = GetAirportInfoResultRow();
        deserialize(ai_row, queryResultAirportInfo, 0);
        airport_infos.push_back(ai_row);

        query = fmt::format(getAirportInfoQuery, AIRPORT_TABLE, COUNTRY_TABLE, flight_row.f_arrive_ap_id);
        client.Query(query, queryResultAirportInfo, timeout);
        ai_row = GetAirportInfoResultRow();
        deserialize(ai_row, queryResultAirportInfo, 0);
        airport_infos.push_back(ai_row);
    }
    // print info of flight
    for (int i = 0; i < queryResult->size(); i++) {
        deserialize(flight_row, queryResult, i);
        int64_t f_id = flight_row.f_id; 
        int64_t depart_time = flight_row.f_depart_time; 
        int64_t arrival_time = flight_row.f_arrive_time;
        GetAirportInfoResultRow ai_row = airport_infos[2*i];
        std::string depart_ap = ai_row.ap_name;
        std::string depart_city = ai_row.ap_city;
        ai_row = airport_infos[2*i + 1];
        std::string arrive_ap = ai_row.ap_name;
        std::string arrive_city = ai_row.ap_city;

        Debug("Flight %d / dep time %d / %s, %s to  %s, %s / arr time %d", f_id, depart_time, depart_ap, depart_city, arrive_ap, arrive_city, arrival_time);
    }
    Debug("COMMIT");
    return client.Commit(timeout);

}
}
