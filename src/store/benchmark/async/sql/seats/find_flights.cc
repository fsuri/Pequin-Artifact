#include "store/benchmark/async/sql/seats/find_flights.h"
#include "store/benchmark/async/sql/seats/seats_constants.h"
#include <fmt/core.h>
#include <random>

namespace seats_sql {

const int MAX_NUM_FLIGHTS = 10;

SQLFindFlights::SQLFindFlights(uint32_t timeout, std::mt19937 &gen, SeatsProfile &profile) :
    SEATSSQLTransaction(timeout), gen(gen), profile(profile)
    {
        std::cerr << "FIND_FLIGHTS" << std::endl;
        //TODO: Implement FindRandomAirport ID vs getRandomFlightId (pick random flight from cached flights)
        //TODO implement profile.cached_flights.  
        //If we get a result > 1, try to cache the flights found.
                    //Load some initially too. (LoadProfile) (Load flights from the CSV, up to a cache limit flight ids)
    
        if (std::uniform_int_distribution<int>(1, 100)(gen) < PROB_FIND_FLIGHTS_RANDOM_AIRPORTS || profile.cached_flights.empty()) {
            //Select two random airport ids. 
            //Note: They might not actually fly to each other. In that case the query will return no flights.
            depart_aid = profile.getRandomAirportId();
            arrive_aid = profile.getRandomOtherAirport(depart_aid);
            start_time = profile.getRandomUpcomingDate();
            end_time = start_time + MS_IN_DAY * 2; //up to 2 days from start_time.
        }
        else{
            //Use an existing flight to guarantee to get back results.
               
            int64_t flight_index = std::uniform_int_distribution<int64_t>(1, profile.cached_flights.size())(gen) - 1;
            CachedFlight &flight = profile.cached_flights[flight_index];
            depart_aid = flight.depart_ap_id;
            arrive_aid = flight.arrive_ap_id;
            uint64_t range = seats_sql::MS_IN_DAY / 2;
            start_time = flight.depart_time - range; 
            end_time = flight.depart_time + range; 

            Debug("Select Flight From Cache. dep_ap: %d, arrive_ap: %d. Dep_time %lu. Sanity f_id: %lu\n", depart_aid, arrive_aid, start_time, flight.flight_id);

        }
       
        if (std::uniform_int_distribution<int>(1, 100)(gen) < PROB_FIND_FLIGHTS_NEARBY_AIRPORT) {
            distance = NEAR_DISTANCES[std::uniform_int_distribution<int>(0, NEAR_DISTANCES.size()-1)(gen)];
        } else {
            distance = 0;
        }

        UW_ASSERT(start_time < end_time);
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
        //GetNearbyAirports: Get the nearby airports for the departure and arrival cities
        query = fmt::format("SELECT * FROM {} WHERE d_ap_id0 = {} AND d_distance <= {} ORDER BY d_distance ASC", AIRPORT_DISTANCE_TABLE, depart_aid, distance); 
        client.Query(query, queryResult, timeout);

        GetNearbyAirportsResultRow ad_row;
        std::vector<int64_t> nearby_airports;
        for (int i = 0; i < std::min((int) queryResult->size(), 2); i++) {
            deserialize(ad_row, queryResult, i);
            nearby_airports.push_back(ad_row.dp_ap_id1);
        }
    } 

    //GetFlights - up to 2 nearby airports
    if (nearby_airports.size() == 0){
        query = fmt::format("SELECT f_id, f_al_id, f_depart_ap_id, f_depart_time, f_arrive_ap_id, f_arrive_time, al_name, al_iattr00, al_iattr01 FROM {}, {} " 
                            "WHERE f_depart_ap_id = {} AND f_depart_time >= {} AND f_depart_time <= {} AND f_al_id = al_id AND f_arrive_ap_id = {} "
                            "AND al_id = al_id " //REFLEXIVE ARG FOR DUMB PELOTON PLANNER
                            "LIMIT {} ",
                            FLIGHT_TABLE, AIRLINE_TABLE, depart_aid, start_time, end_time, arrive_aid, MAX_NUM_FLIGHTS);
    } 
    else if (nearby_airports.size() == 1){ 
        query = fmt::format("SELECT f_id, f_al_id, f_depart_ap_id, f_depart_time, f_arrive_ap_id, f_arrive_time, al_name, al_iattr00, al_iattr01 FROM {}, {} " 
                            "WHERE f_depart_ap_id = {} AND f_depart_time >= {} AND f_depart_time <= {} AND f_al_id = al_id " 
                            "AND al_id = al_id ", //REFLEXIVE ARG FOR DUMB PELOTON PLANNER
                            "AND (f_arrive_ap_id = {} OR f_arrive_ap_id = {}) LIMIT {}", 
                            FLIGHT_TABLE, AIRLINE_TABLE, depart_aid, start_time, end_time, arrive_aid, nearby_airports[0], MAX_NUM_FLIGHTS);
    }
    else{
        query = fmt::format("SELECT f_id, f_al_id, f_depart_ap_id, f_depart_time, f_arrive_ap_id, f_arrive_time, al_name, al_iattr00, al_iattr01 FROM {}, {} "
                            "WHERE f_depart_ap_id = {} AND f_depart_time >= {} AND f_depart_time <= {} AND f_al_id = al_id "
                            "AND al_id = al_id ", //REFLEXIVE ARG FOR DUMB PELOTON PLANNER
                            "AND (f_arrive_ap_id = {} OR f_arrive_ap_id = {} OR f_arrive_ap_id = {}) LIMIT {}", 
                            FLIGHT_TABLE, AIRLINE_TABLE, depart_aid, start_time, end_time, arrive_aid, nearby_airports[0], nearby_airports[1], MAX_NUM_FLIGHTS);
    }

    client.Query(query, queryResult, timeout);

    GetFlightsResultRow flight_row;
    //GetAirportInfo
    std::string getAirportInfoQuery = "SELECT ap_code, ap_name, ap_city, ap_longitude, ap_latitude, co_id, co_name, co_code_2, co_code_3 "
                                      "FROM {}, {} "
                                      "WHERE ap_id = {} AND ap_co_id = co_id "
                                      "AND co_id = co_id"; //REFLEXIVE ARG FOR DUMB PELOTON PLANNER
    std::vector<GetAirportInfoResultRow> airport_infos;

     // populate the infos of arriving / departing airports of flight

    //Parallel Read version
    std::vector<std::unique_ptr<const query_result::QueryResult>> results; 
    for (std::size_t i = 0; i < queryResult->size(); i++) {
        try{
            deserialize(flight_row, queryResult, i);
        }
        catch(const std::exception &e){
            Panic("failed to deserialize flight row: %s", e.what());
        }

        //Departure Airport
        query = fmt::format(getAirportInfoQuery, AIRPORT_TABLE, COUNTRY_TABLE, flight_row.f_depart_ap_id);
        client.Query(query, timeout);
       
        //Arrival Airport
        query = fmt::format(getAirportInfoQuery, AIRPORT_TABLE, COUNTRY_TABLE, flight_row.f_arrive_ap_id);
        client.Query(query,timeout);
    }
    //Collect all info (FIFO)
    client.Wait(results);
   
    for(auto &queryResultAirportInfo: results){
        GetAirportInfoResultRow ai_row;
       

        try{
             deserialize(ai_row, queryResultAirportInfo, 0);
        }
        catch(const std::exception &e){
            Panic("failed to deserialize Airport Info: %s", e.what());
        }

        airport_infos.push_back(ai_row);
    }

    //Sequential Read version
    // std::unique_ptr<const query_result::QueryResult> queryResultAirportInfo;
    // for (std::size_t i = 0; i < queryResult->size(); i++) {
    //     deserialize(flight_row, queryResult, i);

    //     //Departure Airport
    //     query = fmt::format(getAirportInfoQuery, AIRPORT_TABLE, COUNTRY_TABLE, flight_row.f_depart_ap_id);
    //     client.Query(query, queryResultAirportInfo, timeout);
    //     GetAirportInfoResultRow ai_row = GetAirportInfoResultRow();
    //     deserialize(ai_row, queryResultAirportInfo, 0);
    //     airport_infos.push_back(ai_row);

    //     //Arrival Airport
    //     query = fmt::format(getAirportInfoQuery, AIRPORT_TABLE, COUNTRY_TABLE, flight_row.f_arrive_ap_id);
    //     client.Query(query, queryResultAirportInfo, timeout);
    //     ai_row = GetAirportInfoResultRow();
    //     deserialize(ai_row, queryResultAirportInfo, 0);
    //     airport_infos.push_back(ai_row);
    // }
    
    Debug("COMMIT");
    auto result = client.Commit(timeout);
    if(result != transaction_status_t::COMMITTED) return result;

     //////////////// UPDATE PROFILE /////////////////////
        
    // Convert the data into FlightIds that other transactions can use
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

        CachedFlight cf;
        cf.flight_id = f_id; 
        cf.airline_id = flight_row.f_al_id; 
        cf.depart_ap_id = flight_row.f_depart_ap_id; 
        cf.depart_time = depart_time;
        cf.arrive_ap_id = flight_row.f_arrive_ap_id;
        profile.addFlightToCache(cf);
    }
    
    return result;

}
}
