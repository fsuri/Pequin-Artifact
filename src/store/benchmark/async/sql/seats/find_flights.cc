#include "store/benchmark/async/sql/seats/find_flights.h"
#include "store/benchmark/async/sql/seats/seats_constants.h"
#include <fmt/core.h>
#include <random>

namespace seats_sql {

const int MAX_NUM_FLIGHTS = 10;

SQLFindFlights::SQLFindFlights(uint32_t timeout, std::mt19937_64 gen) :
    SEATSSQLTransaction(timeout)
    {
        depart_aid = std::uniform_int_distribution<int64_t>(1, NUM_AIRPORTS)(gen);
        arrive_aid = std::uniform_int_distribution<int64_t>(1, NUM_AIRPORTS)(gen);
        start_time = std::uniform_int_distribution<std::time_t>(MIN_TS, MAX_TS)(gen);
        end_time = std::uniform_int_distribution<std::time_t>(start_time, MAX_TS)(gen);
        if (std::uniform_int_distribution<int>(1, 100)(gen) < PROB_FIND_FLIGHTS_NEARBY_AIRPORT) {
            distance = std::uniform_int_distribution<int>(6000, 9000)(gen);
        } else {
            distance = 0;
        }
    }

SQLFindFlights::~SQLFindFlights() {}

transaction_status_t SQLFindFlights::Execute(SyncClient &client) {
    std::unique_ptr<const query_result::QueryResult> queryResult;
    std::string query;

    Debug("FIND_FLIGHTS");
    client.Begin(timeout);
    std::vector<std::string> nearby_airports;

    if (distance > 0) {
        Debug("FIND_NEARBY_AIRPORT");
        query = fmt::format("SELECT * FROM {} WHERE d_ap_id0 = {} AND d_distance <= {} ORDER BY d_distance ASC", AIRPORT_DISTANCE_TABLE, depart_aid, distance); 
        client.Query(query, queryResult, timeout);

        GetNearbyAirportsResultRow ad_row;
        std::vector<int64_t> nearby_airports;
        for (int i = 0; i < std::min((int) queryResult->size(), 2); i++) {
            deserialize(ad_row, queryResult, i);
            nearby_airports.push_back(ad_row.dp_ap_id1);
        }
    } 

    if (nearby_airports.size() == 0){
        query = fmt::format("SELECT f_id, f_al_id, f_depart_ap_id, f_depart_time, f_arrive_ap_id, f_arrive_time, al_name, al_iattr00, al_iattr01 FROM {}, {} " 
                            "WHERE f_depart_ap_id = {} AND f_depart_time >= {} AND f_depart_time <= {} AND f_al_id = al_id AND f_arrive_ap_id = {} LIMIT {}", 
                            FLIGHT_TABLE, AIRLINE_TABLE, depart_aid, start_time, end_time, arrive_aid, MAX_NUM_FLIGHTS);
    } 
    else if (nearby_airports.size() == 1){ 
        query = fmt::format("SELECT f_id, f_al_id, f_depart_ap_id, f_depart_time, f_arrive_ap_id, f_arrive_time, al_name, al_iattr00, al_iattr01 FROM {}, {} " 
                            "WHERE f_depart_ap_id = {} AND f_depart_time >= {} AND f_depart_time <= {} AND f_al_id = al_id AND f_arrive_ap_id = {} OR f_arrive_ap_id = {} LIMIT {}", 
                            FLIGHT_TABLE, AIRLINE_TABLE, depart_aid, start_time, end_time, arrive_aid, nearby_airports[0], MAX_NUM_FLIGHTS);
    }
    else{
        query = fmt::format("SELECT f_id, f_al_id, f_depart_ap_id, f_depart_time, f_arrive_ap_id, f_arrive_time, al_name, al_iattr00, al_iattr01 FROM {}, {} "
                            "WHERE f_depart_ap_id = {} AND f_depart_time >= {} AND f_depart_time <= {} AND f_al_id = al_id AND f_arrive_ap_id = {} OR f_arrive_ap_id = {} OR f_arrive_ap_id = {} LIMIT {}", 
                            FLIGHT_TABLE, AIRLINE_TABLE, depart_aid, start_time, end_time, arrive_aid, nearby_airports[0], nearby_airports[1], MAX_NUM_FLIGHTS);
    }

    client.Query(query, queryResult, timeout);

    GetFlightsResultRow flight_row = GetFlightsResultRow();
    std::unique_ptr<const query_result::QueryResult> queryResultAirportInfo;
    std::string getAirportInfoQuery = "SELECT ap_code, ap_name, ap_city, ap_longitude, ap_latitude, co_id, co_name, co_code_2, co_code_3 FROM {}, {} WHERE ap_id = {} AND ap_co_id = co_id";
    std::vector<GetAirportInfoResultRow> airport_infos;
    // populate the infos of arriving / departing airports of flight
    for (std::size_t i = 0; i < queryResult->size(); i++) {
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

        //TODO: Parallelize all of the reads in the loop
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

        Debug("Flight %ld / dep time %ld / %s, %s to  %s, %s / arr time %ld", f_id, depart_time, depart_ap, depart_city, arrive_ap, arrive_city, arrival_time);
    }
    Debug("COMMIT");
    return client.Commit(timeout);

}
}
